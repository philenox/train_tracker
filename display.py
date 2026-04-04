#!/usr/bin/env python3
"""
Display the next trains predicted to pass berth 1757 on the SSD1322 OLED.
Refreshes every 60 seconds.
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

from trains import get_westbound_departures, effective_time, format_departure

GPIO.setwarnings(False)

load_dotenv()

BERTH_1757_OFFSET_MINS = 2
NUM_TRAINS = 3
REFRESH_SECS = 60


def make_device():
    serial = spi(device=0, port=0, bus_speed_hz=2000000, transfer_size=4096,
                 gpio_DC=24, gpio_RST=25)
    device = ssd1322(serial, width=256, height=64, rotate=0, mode='1')
    device.contrast(255)
    return device


def render(device, services):
    with canvas(device) as draw:
        if not services:
            draw.text((0, 24), 'No trains found', fill='white')
            return

        row_h = 20
        for i, svc in enumerate(services[:NUM_TRAINS]):
            y = i * row_h

            dep_time = effective_time(svc)
            berth_time = (dep_time + timedelta(minutes=BERTH_1757_OFFSET_MINS)).strftime("%H:%M") if dep_time else "?"
            destination = svc.get("destination", [{}])[0].get("locationName", "Unknown")
            etd = svc.get("etd", "")
            late = etd not in ("On time", "Cancelled", "", None) and ":" in etd

            # Time
            draw.text((0, y), berth_time, fill='white')
            # Destination (truncate to fit)
            draw.text((45, y), destination[:18], fill='white')
            # Late indicator
            if late:
                draw.text((230, y), '*', fill='white')
            # Divider (except after last row)
            if i < NUM_TRAINS - 1:
                draw.line([(0, y + row_h - 2), (255, y + row_h - 2)], fill='white')


def main():
    api_key = os.environ.get("LDBWS_CONSUMER_KEY")
    if not api_key:
        print("Error: set LDBWS_CONSUMER_KEY in .env")
        sys.exit(1)

    device = make_device()

    def shutdown(sig, frame):
        device.cleanup()
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    print("Running — press Ctrl+C to stop")
    while True:
        try:
            services = get_westbound_departures(api_key)
            render(device, services)
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Refreshed, {len(services)} services")
        except Exception as e:
            print(f"Error: {e}")
            with canvas(device) as draw:
                draw.text((0, 24), f'Error: {str(e)[:30]}', fill='white')

        time.sleep(REFRESH_SECS)


if __name__ == "__main__":
    main()
