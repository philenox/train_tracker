#!/usr/bin/env python3
"""
display.py — Train tracker OLED display.

Shows the next 3 trains predicted to pass the visible berths west of
Reading, sorted by ETA, with direction, time, and destination.

Layout (256x64, 3 rows of 21px):
  ┌─────────────────────────────────────────┐
  │ WB  22:29  Bristol Temple Meads         │
  ├─────────────────────────────────────────┤
  │ EB  22:30  London Paddington            │
  ├─────────────────────────────────────────┤
  │ WB  22:45  Oxford                       │
  └─────────────────────────────────────────┘

ETA shown as HH:MM when >90s away, or "XXs" countdown when imminent.
TD real-time detections update the display immediately when a train
crosses a visible berth.
"""

import os
import signal
import sys
import time
from datetime import datetime

import RPi.GPIO as GPIO
from dotenv import load_dotenv
from luma.core.interface.serial import spi
from luma.core.render import canvas
from luma.oled.device import ssd1322

import predict
import schedule_db
import td_client
import trust_client

GPIO.setwarnings(False)
load_dotenv()

REFRESH_SECS  = 5     # schedule prediction refresh
TD_REFRESH    = 1     # display redraw rate
ROW_H         = 21    # pixels per row
MAX_DEST_CHARS = 22   # destination truncation


def make_device():
    serial = spi(device=0, port=0, bus_speed_hz=2000000, transfer_size=4096,
                 gpio_DC=24, gpio_RST=25)
    device = ssd1322(serial, width=256, height=64, rotate=0, mode='1')
    device.contrast(255)
    return device


def _fmt_eta(eta: datetime) -> str:
    secs = int((eta - datetime.now()).total_seconds())
    if secs < 0:
        return "now"
    if secs < 90:
        return f"{secs}s"
    return eta.strftime("%H:%M")


def _truncate(s: str, n: int) -> str:
    return s if len(s) <= n else s[:n - 1] + "\u2026"  # ellipsis


def render(device, trains):
    """Render up to 3 upcoming trains onto the display."""
    now = datetime.now()

    with canvas(device) as draw:
        for row, train in enumerate(trains[:3]):
            y       = row * ROW_H
            eta_str = _fmt_eta(train["eta"])
            dest    = _truncate(train["destination"], MAX_DEST_CHARS)
            line    = f"{train['direction']}  {eta_str:<6} {dest}"

            # Highlight row if train is at/past its ETA (just crossed the berth)
            secs_until = int((train["eta"] - now).total_seconds())
            fill = "white"

            draw.text((2, y + 2), line, fill=fill)

            if row < 2:
                draw.line([(0, y + ROW_H - 1), (255, y + ROW_H - 1)], fill="white")

        # If fewer than 3 trains, fill remaining rows with placeholder
        for row in range(len(trains[:3]), 3):
            y = row * ROW_H
            draw.text((2, y + 2), "-- no data --", fill="white")
            if row < 2:
                draw.line([(0, y + ROW_H - 1), (255, y + ROW_H - 1)], fill="white")


def main():
    if not os.environ.get("NR_USERNAME") or not os.environ.get("NR_PASSWORD"):
        print("Error: NR_USERNAME/NR_PASSWORD not set in .env")
        sys.exit(1)

    schedule_db.refresh_if_stale()

    device = make_device()

    print("Starting TD feed listener...")
    td_client.start()

    print("Starting TRUST feed listener...")
    trust_client.start()

    trains   = []
    last_refresh = 0

    def shutdown(sig, frame):
        device.cleanup()
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    print("Running — press Ctrl+C to stop")
    while True:
        now = time.time()

        # Refresh prediction list periodically
        if now - last_refresh >= REFRESH_SECS:
            try:
                trains = predict.get_upcoming(n=3)
                last_refresh = now
            except Exception as e:
                print(f"[predict] Error: {e}")

        # Check if TD has a recent detection to inject/update
        for berth, direction in [
            (td_client.WESTBOUND_BERTH, "WB"),
            (td_client.EASTBOUND_BERTH, "EB"),
        ]:
            last = td_client.get_last(berth)
            if last:
                secs_ago = (datetime.now() - last["time"]).total_seconds()
                if secs_ago < 120:
                    # A train just crossed — bubble it to the top of the list
                    detected = {
                        "direction":   direction,
                        "headcode":    last["headcode"],
                        "eta":         last["time"],
                        "destination": f"{last['headcode']} (detected)",
                        "sched_reading": "--",
                        "atoc_code":   "??",
                        "uid":         "live",
                    }
                    # Replace matching entry or prepend
                    trains = [t for t in trains
                              if not (t["headcode"] == last["headcode"] and t["direction"] == direction)]
                    trains.insert(0, detected)
                    trains = trains[:3]

        try:
            render(device, trains)
        except Exception as e:
            print(f"[render] Error: {e}")
            try:
                with canvas(device) as draw:
                    draw.text((0, 24), f"Error: {str(e)[:30]}", fill="white")
            except Exception:
                pass

        # Log to stdout
        for t in trains[:3]:
            print(f"  {t['direction']}  {_fmt_eta(t['eta']):<6}  {t['destination']}")
        print()

        time.sleep(TD_REFRESH)


if __name__ == "__main__":
    main()
