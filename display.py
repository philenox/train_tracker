#!/usr/bin/env python3
"""
Display upcoming trains passing the window west of Reading station.

Layout (256x64, 3 rows of 21px):
  Row 0: next westbound train  (WB  HH:MM  Destination)
  Row 1: next westbound train  (WB  HH:MM  Destination)
  Row 2: last eastbound seen   (EB  HH:MM  Xm ago) or next predicted EB

Darwin is polled every 60s for westbound predictions.
The TD feed (STOMP) runs in a background thread for real-time eastbound detection.
"""

import os
import signal
import sys
import time
from datetime import datetime, timedelta

import RPi.GPIO as GPIO
from dotenv import load_dotenv
from luma.core.interface.serial import spi
from luma.core.render import canvas
from luma.oled.device import ssd1322

import td_client
from trains import get_westbound_departures, effective_time

GPIO.setwarnings(False)
load_dotenv()

WB_OFFSET_MINS = 2   # minutes from Reading departure to berth 1757
REFRESH_SECS   = 60
ROW_H          = 21  # pixels per row


def make_device():
    serial = spi(device=0, port=0, bus_speed_hz=2000000, transfer_size=4096,
                 gpio_DC=24, gpio_RST=25)
    device = ssd1322(serial, width=256, height=64, rotate=0, mode='1')
    device.contrast(255)
    return device


def _berth_time(svc) -> str:
    """Predicted time at berth 1757 for a westbound Darwin service."""
    dep = effective_time(svc)
    if not dep:
        return "?"
    return (dep + timedelta(minutes=WB_OFFSET_MINS)).strftime("%H:%M")


def _dest(svc) -> str:
    return svc.get("destination", [{}])[0].get("locationName", "Unknown")


def _late(svc) -> bool:
    etd = svc.get("etd", "")
    return bool(etd and etd not in ("On time", "Cancelled", "") and ":" in etd)


def render(device, wb_services):
    now = datetime.now()

    # Eastbound: last seen at berth 1742
    eb = td_client.get_last(td_client.EASTBOUND_BERTH)

    with canvas(device) as draw:
        # --- Westbound rows ---
        for i, svc in enumerate(wb_services[:2]):
            y = i * ROW_H
            t = _berth_time(svc)
            dest = _dest(svc)[:16]
            late_mark = "*" if _late(svc) else ""
            draw.text((0,   y), "WB", fill='white')
            draw.text((20,  y), t,    fill='white')
            draw.text((58,  y), dest, fill='white')
            if late_mark:
                draw.text((248, y), late_mark, fill='white')
            if i == 0:
                draw.line([(0, ROW_H - 1), (255, ROW_H - 1)], fill='white')

        # --- Eastbound row ---
        y = 2 * ROW_H
        draw.line([(0, y - 1), (255, y - 1)], fill='white')
        draw.text((0, y), "EB", fill='white')

        if eb:
            mins_ago = int((now - eb["time"]).total_seconds() / 60)
            if mins_ago < 60:
                ago_str = f"{mins_ago}m ago"
            else:
                ago_str = eb["time"].strftime("%H:%M")
            draw.text((20,  y), eb["time"].strftime("%H:%M"), fill='white')
            draw.text((58,  y), f"{eb['headcode']}  ({ago_str})",  fill='white')
        else:
            draw.text((20, y), "no data yet", fill='white')


def main():
    api_key = os.environ.get("LDBWS_CONSUMER_KEY")
    if not api_key:
        print("Error: LDBWS_CONSUMER_KEY not set in .env")
        sys.exit(1)
    if not os.environ.get("NR_USERNAME") or not os.environ.get("NR_PASSWORD"):
        print("Warning: NR_USERNAME/NR_PASSWORD not set — eastbound detection disabled")
        nr_ok = False
    else:
        nr_ok = True

    device = make_device()

    if nr_ok:
        print("Starting TD feed listener...")
        td_client.start()

    def shutdown(sig, frame):
        device.cleanup()
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    print("Running — press Ctrl+C to stop")
    while True:
        try:
            wb = get_westbound_departures(api_key)
            render(device, wb)
            eb = td_client.get_last(td_client.EASTBOUND_BERTH)
            eb_str = eb["headcode"] if eb else "none"
            print(f"[{datetime.now().strftime('%H:%M:%S')}] WB:{len(wb)} services  EB last:{eb_str}")
        except Exception as e:
            print(f"Error: {e}")
            with canvas(device) as draw:
                draw.text((0, 24), f'Error: {str(e)[:30]}', fill='white')

        time.sleep(REFRESH_SECS)


if __name__ == "__main__":
    main()
