#!/usr/bin/env python3
"""
monitor.py — Terminal real-time train predictions.

Shows the next trains predicted to pass the visible berths west of Reading,
updated every second. TRUST delays are applied to ETAs when available.
TD feed detections override schedule predictions the moment a train is
physically detected at a trigger or visible berth.

Usage:
  venv/bin/python monitor.py
"""

import curses
import os
import signal
import sys
import time
from datetime import datetime

from dotenv import load_dotenv

import td_client
import trust_client
import predict
import schedule_db

load_dotenv()

N_TRAINS      = 8     # rows to show
SCHED_REFRESH = 5     # seconds between schedule re-queries
TD_INJECT_TTL = 120   # seconds to keep a TD-detected train visible after passing


def _fmt_eta(eta: datetime) -> str:
    secs = int((eta - datetime.now()).total_seconds())
    if secs < -60:
        return f"{abs(secs) // 60}m ago"
    if secs < 0:
        return "now"
    if secs < 90:
        return f"in {secs}s"
    if secs < 600:
        m, s = divmod(secs, 60)
        return f"in {m}m{s:02d}s"
    return eta.strftime("%H:%M")


def _fmt_delay(delay_secs) -> tuple:
    """Return (text, colour_attr_name) for a delay value."""
    if delay_secs is None:
        return ("      ", "dim")
    if abs(delay_secs) < 30:
        return ("on time", "green")
    mins = delay_secs // 60
    if delay_secs > 0:
        return (f"+{mins}min ", "yellow")
    return (f"{mins}min  ", "cyan")


def _inject_td(trains):
    """
    Overlay real-time TD detections onto the schedule list.
    - Approaching (trigger berth): replaces the matching schedule entry if found,
      otherwise prepends — ETA is precise (transit time from trigger).
    - Just passed (visible berth): prepended as a "passed" entry for TTL seconds.
    """
    # Approaching trains (trigger berths fired)
    for direction in ("WB", "EB"):
        approaching = td_client.get_approaching(direction)
        if not approaching:
            continue
        secs_until = (approaching["eta"] - datetime.now()).total_seconds()
        if secs_until < -30:
            continue   # stale
        dest = next(
            (t["destination"] for t in trains
             if t["headcode"] == approaching["headcode"] and t["direction"] == direction),
            None,
        )
        if dest is None:
            sched = predict.lookup_headcode(approaching["headcode"])
            dest  = sched["destination"] if sched else "not in CIF (freight/ECS?)"
        entry = {
            "direction":    direction,
            "headcode":     approaching["headcode"],
            "eta":          approaching["eta"],
            "destination":  dest,
            "sched_reading": "--",
            "atoc_code":    "??",
            "uid":          "td-trigger",
            "delay_secs":   None,
            "source":       "TD",
        }
        trains = [t for t in trains
                  if not (t["headcode"] == approaching["headcode"]
                          and t["direction"] == direction)]
        trains.insert(0, entry)

    # Trains that just passed a visible berth
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
        dest = next(
            (t["destination"] for t in trains
             if t["headcode"] == last["headcode"] and t["direction"] == direction),
            None,
        )
        if dest is None:
            sched = predict.lookup_headcode(last["headcode"])
            dest  = sched["destination"] if sched else "not in CIF (freight/ECS?)"
        entry = {
            "direction":    direction,
            "headcode":     last["headcode"],
            "eta":          last["time"],
            "destination":  dest,
            "sched_reading": "--",
            "atoc_code":    "??",
            "uid":          "td-visible",
            "delay_secs":   None,
            "source":       "TD",
        }
        trains = [t for t in trains
                  if not (t["headcode"] == last["headcode"]
                          and t["direction"] == direction)]
        trains.insert(0, entry)

    return trains[:N_TRAINS]


def draw(stdscr, trains, status_msg):
    colours = {
        "header":  curses.color_pair(1),
        "wb":      curses.color_pair(2),
        "eb":      curses.color_pair(3),
        "td":      curses.color_pair(4),
        "green":   curses.color_pair(5),
        "yellow":  curses.color_pair(6),
        "cyan":    curses.color_pair(7),
        "dim":     curses.color_pair(8),
        "normal":  curses.color_pair(0),
    }

    height, width = stdscr.getmaxyx()
    stdscr.erase()
    row = 0

    def addstr(y, x, text, attr=0):
        try:
            stdscr.addstr(y, x, text, attr)
        except curses.error:
            pass   # ignore writes past edge of terminal

    now = datetime.now()
    trust_n = trust_client.delay_count()

    # ── Header ────────────────────────────────────────────────────────────────
    header = (
        f" Train Tracker   {now.strftime('%H:%M:%S')}   "
        f"TRUST: {trust_n} trains   {status_msg}"
    )
    addstr(row, 0, header.ljust(width), colours["header"] | curses.A_BOLD)
    row += 1

    # ── Column headers ────────────────────────────────────────────────────────
    addstr(row, 0, f"  {'Dir':<4} {'ETA':<12} {'Code':<5}  {'Destination':<28}  {'Delay':<10}  {'Src'}")
    row += 1
    addstr(row, 0, "  " + "─" * (width - 4))
    row += 1

    # ── Train rows ────────────────────────────────────────────────────────────
    for train in trains[:N_TRAINS]:
        if row >= height - 2:
            break

        source      = train.get("source", "SCH")
        direction   = train["direction"]
        eta_str     = _fmt_eta(train["eta"])
        delay_txt, delay_colour = _fmt_delay(train.get("delay_secs"))
        dest        = train["destination"][:27]
        hc          = train["headcode"]

        dir_colour  = colours["wb"] if direction == "WB" else colours["eb"]
        src_colour  = colours["td"] if source == "TD" else colours["dim"]

        addstr(row, 0,  "  ")
        addstr(row, 2,  f"{direction:<4}", dir_colour | curses.A_BOLD)
        addstr(row, 6,  f" {eta_str:<12}")
        addstr(row, 19, f" {hc:<5} ")
        addstr(row, 25, f" {dest:<28} ")
        addstr(row, 55, f" {delay_txt:<10}", colours[delay_colour])
        addstr(row, 67, f"  {source:<3}", src_colour)
        row += 1

    # Pad remaining rows
    while row < height - 2:
        addstr(row, 0, "")
        row += 1

    # ── Footer ────────────────────────────────────────────────────────────────
    footer = "  TD: live berth detections   SCH: schedule+TRUST   Ctrl+C to quit"
    addstr(height - 1, 0, footer.ljust(width), colours["header"])

    stdscr.refresh()


def _init_colours():
    curses.start_color()
    curses.use_default_colors()
    curses.init_pair(1, curses.COLOR_BLACK,  curses.COLOR_YELLOW)  # header/footer
    curses.init_pair(2, curses.COLOR_CYAN,   -1)                   # WB direction
    curses.init_pair(3, curses.COLOR_GREEN,  -1)                   # EB direction
    curses.init_pair(4, curses.COLOR_YELLOW, -1)                   # TD source
    curses.init_pair(5, curses.COLOR_GREEN,  -1)                   # on time
    curses.init_pair(6, curses.COLOR_YELLOW, -1)                   # late
    curses.init_pair(7, curses.COLOR_CYAN,   -1)                   # early
    curses.init_pair(8, curses.COLOR_WHITE,  -1)                   # dim/no data


def run(stdscr):
    _init_colours()
    curses.curs_set(0)
    stdscr.nodelay(True)

    trains       = []
    last_refresh = 0
    status_msg   = "connecting..."

    while True:
        # Non-blocking key check — q or Ctrl+C exits
        key = stdscr.getch()
        if key in (ord("q"), ord("Q"), 3):   # 3 = Ctrl+C
            return

        now = time.time()

        if now - last_refresh >= SCHED_REFRESH:
            try:
                trains = predict.get_upcoming(n=N_TRAINS)
                status_msg = f"schedule OK ({len(trains)} trains)"
            except Exception as e:
                status_msg = f"schedule error: {e}"
            last_refresh = now

        display_trains = _inject_td(list(trains))
        draw(stdscr, display_trains, status_msg)
        time.sleep(1)


def main():
    for var in ("NR_USERNAME", "NR_PASSWORD"):
        if not os.environ.get(var):
            print(f"Error: {var} not set in .env")
            sys.exit(1)

    schedule_db.refresh_if_stale()

    print("Connecting to TD feed...", flush=True)
    td_client.start()

    print("Connecting to TRUST feed...", flush=True)
    trust_client.start()

    # Brief pause so connections can establish before curses takes over
    time.sleep(2)

    try:
        curses.wrapper(run)
    except KeyboardInterrupt:
        pass

    print("Stopped.")


if __name__ == "__main__":
    main()
