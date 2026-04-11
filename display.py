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

import contextlib
import io
import os
import signal
import sys
import threading
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

REFRESH_SECS      = 5     # schedule prediction refresh
TD_REFRESH        = 1     # display redraw rate
ROW_H             = 21    # pixels per row
MAX_JOURNEY_CHARS = 28    # characters visible in journey field
TD_INJECT_TTL     = 120   # seconds to keep a TD-detected train visible after passing
DB_CHECK_INTERVAL = 3600  # check schedule staleness every hour
SCROLL_PERIOD     = 20    # seconds of static (truncated) display before scrolling
SCROLL_STEPS      = 8     # number of 1-second scroll steps
SCROLL_SPEED      = 2     # characters advanced per scroll step


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


def _journey_text(origin: str, dest: str, scroll_tick: int) -> str:
    """Return the journey string for the current scroll_tick.

    For short journeys (fits in MAX_JOURNEY_CHARS) always returns the full string.
    For long journeys: shows truncated text for SCROLL_PERIOD seconds, then slides
    a MAX_JOURNEY_CHARS-wide window across the full string over SCROLL_STEPS seconds,
    then repeats.
    """
    full = f"{origin} > {dest}"
    if len(full) <= MAX_JOURNEY_CHARS:
        return full
    overflow = len(full) - MAX_JOURNEY_CHARS
    phase = scroll_tick % (SCROLL_PERIOD + SCROLL_STEPS)
    if phase < SCROLL_PERIOD:
        return full[:MAX_JOURNEY_CHARS - 1] + "~"
    offset = min((phase - SCROLL_PERIOD) * SCROLL_SPEED, overflow)
    return full[offset:offset + MAX_JOURNEY_CHARS]


def render(device, trains, scroll_tick: int):
    """Render up to 3 upcoming trains onto the display."""
    now = datetime.now()

    with canvas(device) as draw:
        for row, train in enumerate(trains[:3]):
            y       = row * ROW_H
            eta_str = _fmt_eta(train["eta"])
            origin  = train.get("origin", "?")
            dest    = train["destination"]
            journey = _journey_text(origin, dest, scroll_tick)
            line    = f"{train['direction']}  {eta_str:<6} {journey}"

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

    trains        = []
    last_refresh  = 0
    last_db_check = 0

    def _bg_db_refresh():
        with contextlib.redirect_stdout(io.StringIO()):
            schedule_db.refresh_if_stale()

    def shutdown(sig, frame):
        device.cleanup()
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    print("Running — press Ctrl+C to stop")
    while True:
        now = time.time()

        if now - last_db_check >= DB_CHECK_INTERVAL:
            threading.Thread(target=_bg_db_refresh, daemon=True, name="db-refresh").start()
            last_db_check = now

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
            if not last:
                continue
            secs_ago = (datetime.now() - last["time"]).total_seconds()
            if secs_ago > TD_INJECT_TTL:
                continue
            existing = next(
                (t for t in trains
                 if t["headcode"] == last["headcode"] and t["direction"] == direction),
                None,
            )
            if existing:
                dest   = existing["destination"]
                origin = existing.get("origin", "?")
            else:
                sched  = predict.lookup_headcode(last["headcode"])
                dest   = sched["destination"] if sched else "not in CIF"
                origin = sched["origin"]      if sched else "?"
            detected = {
                "direction":     direction,
                "headcode":      last["headcode"],
                "eta":           last["time"],
                "origin":        origin,
                "destination":   dest,
                "sched_reading": "--",
                "atoc_code":     "??",
                "uid":           "td-visible",
                "delay_secs":    None,
                "source":        "TD",
            }
            trains = [t for t in trains
                      if not (t["headcode"] == last["headcode"]
                              and t["direction"] == direction)]
            trains.insert(0, detected)
            trains = trains[:3]

        try:
            render(device, trains, int(time.time()))
        except Exception as e:
            print(f"[render] Error: {e}")
            try:
                with canvas(device) as draw:
                    draw.text((0, 24), f"Error: {str(e)[:30]}", fill="white")
            except Exception:
                pass

        # Log to stdout
        for t in trains[:3]:
            origin = t.get("origin", "?")
            print(f"  {t['direction']}  {_fmt_eta(t['eta']):<6}  {origin} > {t['destination']}")
        print()

        time.sleep(TD_REFRESH)


if __name__ == "__main__":
    main()
