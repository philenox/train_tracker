#!/usr/bin/env python3
"""
display.py — Train tracker LED matrix display.

Shows the next 5 trains predicted to pass the visible berths west of
Reading, sorted by ETA, with direction, time, and destination.

Layout (128x64, 5 rows of 12px):
  ┌────────────────────────┐
  │ WB 22:29 Bristol TM    │
  ├────────────────────────┤
  │ EB 22:30 London Pad    │
  ├────────────────────────┤
  │ WB 22:45 Oxford        │
  ├────────────────────────┤
  │ EB 22:51 London Pad    │
  ├────────────────────────┤
  │ WB 23:10 Cardiff       │
  └────────────────────────┘

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

from dotenv import load_dotenv
from rgbmatrix import RGBMatrix, RGBMatrixOptions, graphics

import predict
import schedule_db
import td_client
import trust_client

load_dotenv()

REFRESH_SECS      = 5     # schedule prediction refresh
TD_REFRESH        = 1     # display redraw rate
ROW_H             = 12    # pixels per row — 5 rows fit in 64 physical rows
MAX_JOURNEY_CHARS = 16    # characters visible in journey field (128px wide display)
TD_INJECT_TTL     = 120   # seconds to keep a TD-detected train visible after passing
DB_CHECK_INTERVAL = 3600  # check schedule staleness every hour
SCROLL_PERIOD     = 20    # seconds of static (truncated) display before scrolling
SCROLL_STEPS      = 8     # number of 1-second scroll steps
SCROLL_SPEED      = 2     # characters advanced per scroll step

# Path to the rpi-rgb-led-matrix fonts directory
FONT_DIR = os.environ.get("LED_FONT_DIR", "/home/plenox/rpi-rgb-led-matrix/fonts")

# Authentic UK departure board amber — warm sodium-lamp orange, not yellow
COLOR_AMBER     = graphics.Color(255, 140, 0)
COLOR_DIM_AMBER = graphics.Color(160, 88, 0)


def make_matrix():
    options = RGBMatrixOptions()
    options.rows = 64
    options.cols = 128
    options.hardware_mapping = 'adafruit-hat'
    options.led_rgb_sequence = 'BRG'
    options.gpio_slowdown = 5
    options.multiplexing = 0
    options.disable_hardware_pulsing = False
    options.drop_privileges = False
    matrix = RGBMatrix(options=options)
    return matrix


def load_font(name="5x8.bdf"):
    font = graphics.Font()
    font.LoadFont(os.path.join(FONT_DIR, name))
    return font


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


def render(matrix, canvas, font, time_font, trains, scroll_tick: int):
    """Render up to 4 upcoming trains + current time onto the LED matrix canvas.

    Rows 0-3 show trains; row 4 always shows the current time.
    Returns the new canvas (after SwapOnVSync).
    """
    canvas.Clear()

    for row, train in enumerate(trains[:4]):
        # 5x7 font: ascent ~6px; baseline at row_top+8 keeps text within 12px row
        y_baseline = row * ROW_H + 8

        eta_str = _fmt_eta(train["eta"])
        origin  = train.get("origin", "?")
        dest    = train["destination"]
        journey = _journey_text(origin, dest, scroll_tick)
        line    = f"{train['direction']} {eta_str:<5} {journey}"

        graphics.DrawText(canvas, font, 2, y_baseline, COLOR_AMBER, line)

        div_y = (row + 1) * ROW_H - 1
        graphics.DrawLine(canvas, 0, div_y, 127, div_y, COLOR_DIM_AMBER)

    # Fill remaining train rows with placeholder if fewer than 4 trains
    for row in range(len(trains[:4]), 4):
        y_baseline = row * ROW_H + 8
        graphics.DrawText(canvas, font, 2, y_baseline, COLOR_DIM_AMBER, "-- no data --")
        div_y = (row + 1) * ROW_H - 1
        graphics.DrawLine(canvas, 0, div_y, 127, div_y, COLOR_DIM_AMBER)

    # Row 4: current time, centered (7x13 font: 8 chars × 7px = 56px → x=36, baseline=60)
    time_str = datetime.now().strftime("%H:%M:%S")
    graphics.DrawText(canvas, time_font, 36, 60, COLOR_AMBER, time_str)

    return matrix.SwapOnVSync(canvas)


def main():
    if not os.environ.get("NR_USERNAME") or not os.environ.get("NR_PASSWORD"):
        print("Error: NR_USERNAME/NR_PASSWORD not set in .env")
        sys.exit(1)

    schedule_db.refresh_if_stale()

    matrix    = make_matrix()
    canvas    = matrix.CreateFrameCanvas()
    font      = load_font("5x7.bdf")
    time_font = load_font("7x13.bdf")

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
        matrix.Clear()
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
                trains = predict.get_upcoming(n=4)
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
            trains = trains[:4]

        try:
            canvas = render(matrix, canvas, font, time_font, trains, int(time.time()))
        except Exception as e:
            print(f"[render] Error: {e}")
            try:
                canvas.Clear()
                err_font = load_font("4x6.bdf")
                graphics.DrawText(canvas, err_font, 0, 6, COLOR_AMBER,
                                  f"Err: {str(e)[:28]}")
                canvas = matrix.SwapOnVSync(canvas)
            except Exception:
                pass

        # Log to stdout
        for t in trains[:4]:
            origin = t.get("origin", "?")
            print(f"  {t['direction']}  {_fmt_eta(t['eta']):<6}  {origin} > {t['destination']}")
        print()

        time.sleep(TD_REFRESH)


if __name__ == "__main__":
    main()
