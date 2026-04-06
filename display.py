#!/usr/bin/env python3
"""
Display trains passing the window west of Reading station.

Layout (256x64, 3 rows of 21px):
  Row 0: WB approaching ETA (from trigger berth 1733) or blank
  Row 1: WB last seen at berth 1757
  Row 2: EB approaching ETA (from trigger berth 1772) or last seen at berth 1724

All data comes from the Network Rail TD feed — no Darwin dependency.
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

import td_client

GPIO.setwarnings(False)
load_dotenv()

REFRESH_SECS = 1   # display refresh rate
ROW_H        = 21  # pixels per row


def make_device():
    serial = spi(device=0, port=0, bus_speed_hz=2000000, transfer_size=4096,
                 gpio_DC=24, gpio_RST=25)
    device = ssd1322(serial, width=256, height=64, rotate=0, mode='1')
    device.contrast(255)
    return device


def _mins_ago(dt) -> str:
    secs = int((datetime.now() - dt).total_seconds())
    if secs < 60:
        return f"{secs}s ago"
    mins = secs // 60
    if mins < 60:
        return f"{mins}m ago"
    return dt.strftime("%H:%M")


def _secs_until(dt) -> str:
    secs = int((dt - datetime.now()).total_seconds())
    if secs <= 0:
        return "now"
    if secs < 60:
        return f"{secs}s"
    return f"{secs // 60}m {secs % 60:02d}s"


def render(device):
    now = datetime.now()

    wb_approaching = td_client.get_approaching("WB")
    wb_last        = td_client.get_last(td_client.WESTBOUND_BERTH)
    eb_approaching = td_client.get_approaching("EB")
    eb_last        = td_client.get_last(td_client.EASTBOUND_BERTH)

    with canvas(device) as draw:
        # --- Row 0: WB approaching ---
        y = 0
        if wb_approaching:
            eta_str  = wb_approaching["eta"].strftime("%H:%M:%S")
            secs_str = _secs_until(wb_approaching["eta"])
            hc       = wb_approaching["headcode"]
            draw.text((0,  y), "WB", fill='white')
            draw.text((20, y), eta_str, fill='white')
            draw.text((90, y), f"{hc}  in {secs_str}", fill='white')
        else:
            draw.text((0, y), "WB", fill='white')
            draw.text((20, y), "no train approaching", fill='white')

        draw.line([(0, ROW_H - 1), (255, ROW_H - 1)], fill='white')

        # --- Row 1: WB last seen ---
        y = ROW_H
        draw.text((0, y), "WB", fill='white')
        if wb_last:
            draw.text((20, y), wb_last["time"].strftime("%H:%M:%S"), fill='white')
            draw.text((90, y), f"{wb_last['headcode']}  ({_mins_ago(wb_last['time'])})", fill='white')
        else:
            draw.text((20, y), "no data yet", fill='white')

        draw.line([(0, 2 * ROW_H - 1), (255, 2 * ROW_H - 1)], fill='white')

        # --- Row 2: EB approaching or last seen ---
        y = 2 * ROW_H
        draw.text((0, y), "EB", fill='white')
        if eb_approaching:
            eta_str  = eb_approaching["eta"].strftime("%H:%M:%S")
            secs_str = _secs_until(eb_approaching["eta"])
            hc       = eb_approaching["headcode"]
            draw.text((20, y), eta_str, fill='white')
            draw.text((90, y), f"{hc}  in {secs_str}", fill='white')
        elif eb_last:
            mins_ago_str = _mins_ago(eb_last["time"])
            draw.text((20, y), eb_last["time"].strftime("%H:%M:%S"), fill='white')
            draw.text((90, y), f"{eb_last['headcode']}  ({mins_ago_str})", fill='white')
        else:
            draw.text((20, y), "no data yet", fill='white')


def main():
    if not os.environ.get("NR_USERNAME") or not os.environ.get("NR_PASSWORD"):
        print("Error: NR_USERNAME/NR_PASSWORD not set in .env")
        sys.exit(1)

    device = make_device()

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
            render(device)
            wb_a = td_client.get_approaching("WB")
            eb_a = td_client.get_approaching("EB")
            wb_l = td_client.get_last(td_client.WESTBOUND_BERTH)
            eb_l = td_client.get_last(td_client.EASTBOUND_BERTH)
            print(
                f"[{datetime.now().strftime('%H:%M:%S')}] "
                f"WB approaching:{wb_a['headcode'] if wb_a else 'none'}  "
                f"WB last:{wb_l['headcode'] if wb_l else 'none'}  "
                f"EB approaching:{eb_a['headcode'] if eb_a else 'none'}  "
                f"EB last:{eb_l['headcode'] if eb_l else 'none'}"
            )
        except Exception as e:
            print(f"Error: {e}")
            try:
                with canvas(device) as draw:
                    draw.text((0, 24), f'Error: {str(e)[:30]}', fill='white')
            except Exception:
                pass

        time.sleep(REFRESH_SECS)


if __name__ == "__main__":
    main()
