#!/usr/bin/env python3
"""
correlate.py — Stage 2: correlate TD berth observations with CIF schedule data.

For each train observed at our visible berths (1757 WB, 1724 EB), looks up
its schedule and computes the offset between scheduled Reading time and actual
berth time. This tells us how accurately we can predict berth pass times from
the schedule alone.

Usage:
  venv/bin/python correlate.py              # analyse td_data.csv
  venv/bin/python correlate.py --csv FILE   # use a different CSV
  venv/bin/python correlate.py --date DATE  # filter to a specific date (YYYY-MM-DD)
"""

import argparse
import csv
import sqlite3
from collections import defaultdict
from datetime import datetime, date, timedelta
from pathlib import Path

import schedule_db

WESTBOUND_BERTH = "1757"
EASTBOUND_BERTH = "1724"
WATCHED_BERTHS  = {WESTBOUND_BERTH, EASTBOUND_BERTH}

# TIPLOC used to anchor schedule time against observed berth time
READING_TIPLOC = "RDNGSTN"

DAY_NAMES = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]


def parse_time(t_str, ref_date):
    """Parse HHMM or HHMMS (half-minute) schedule time into a datetime."""
    if not t_str:
        return None
    t = t_str.replace("H", "").strip()
    if len(t) == 4:
        hh, mm = int(t[:2]), int(t[2:])
    else:
        return None
    dt = datetime(ref_date.year, ref_date.month, ref_date.day, hh, mm)
    # Handle overnight services: if scheduled time is much earlier than midnight
    # and observation is after midnight, advance by a day
    return dt


def load_observations(csv_path):
    """Load CSV, return list of (datetime, from_berth, to_berth, headcode)."""
    obs = []
    with open(csv_path) as f:
        for row in csv.reader(f):
            if len(row) < 5:
                continue
            try:
                dt        = datetime.fromisoformat(row[0])
                from_b    = row[2].strip()
                to_b      = row[3].strip()
                headcode  = row[4].strip()
                obs.append((dt, from_b, to_b, headcode))
            except ValueError:
                continue
    return obs


def get_reading_time(conn, uid, stp, start_date):
    """Return (arrival, departure, pass_time) tuple for RDNGSTN in this schedule."""
    row = conn.execute(
        """
        SELECT arrival, departure, pass_time
        FROM schedule_locations
        WHERE uid=? AND stp_indicator=? AND start_date=? AND tiploc=?
        """,
        (uid, stp, start_date, READING_TIPLOC),
    ).fetchone()
    return row if row else None


def find_schedule(conn, headcode, obs_dt):
    """
    Find the best matching schedule for a headcode on the observed date.
    Prefers Permanent (P) schedules; falls back to Overlay (O) or New (N).
    Returns list of matching schedule rows.
    """
    obs_date    = obs_dt.date().isoformat()
    day_idx     = obs_dt.weekday()  # 0=Mon

    rows = conn.execute(
        """
        SELECT * FROM schedules
        WHERE headcode = ?
          AND start_date <= ?
          AND end_date   >= ?
        ORDER BY
          CASE stp_indicator WHEN 'P' THEN 0 WHEN 'O' THEN 1 WHEN 'N' THEN 2 ELSE 3 END
        """,
        (headcode, obs_date, obs_date),
    ).fetchall()

    # Filter to schedules that run on this day of week
    matches = [r for r in rows if len(r["days_runs"]) > day_idx and r["days_runs"][day_idx] == "1"]
    return matches


def analyse(csv_path, filter_date=None):
    conn = schedule_db.db_connect()

    observations = load_observations(csv_path)
    if filter_date:
        observations = [o for o in observations if o[0].date() == filter_date]

    # Group observations at our visible berths by headcode
    berth_obs = [o for o in observations if o[2] in WATCHED_BERTHS]
    print(f"Total CSV rows: {len(observations):,}")
    print(f"Rows at watched berths (1757, 1724): {len(berth_obs)}")

    # Deduplicate: one entry per (headcode, berth, date)
    seen = set()
    unique_obs = []
    for obs in berth_obs:
        key = (obs[3], obs[2], obs[0].date())
        if key not in seen:
            seen.add(key)
            unique_obs.append(obs)

    print(f"Unique (headcode, berth, date) events: {len(unique_obs)}\n")

    results     = []
    no_schedule = []
    no_reading  = []

    for obs_dt, from_b, to_b, headcode in sorted(unique_obs):
        direction = "WB" if to_b == WESTBOUND_BERTH else "EB"
        matches   = find_schedule(conn, headcode, obs_dt)

        if not matches:
            no_schedule.append((headcode, obs_dt, direction))
            continue

        # Use the highest-priority match
        sched = matches[0]
        rdng  = get_reading_time(conn, sched["uid"], sched["stp_indicator"], sched["start_date"])

        if not rdng:
            no_reading.append((headcode, obs_dt, direction, sched["uid"]))
            continue

        # Pick the most useful time: departure for WB (leaving Reading), arrival for EB (arriving)
        if direction == "WB":
            sched_time_str = rdng["departure"] or rdng["pass_time"]
        else:
            sched_time_str = rdng["arrival"] or rdng["pass_time"]

        sched_dt = parse_time(sched_time_str, obs_dt.date()) if sched_time_str else None

        # Handle overnight: if scheduled time is much later than midnight wrap
        if sched_dt and abs((sched_dt - obs_dt).total_seconds()) > 6 * 3600:
            sched_dt += timedelta(days=1)

        offset_secs = int((obs_dt - sched_dt).total_seconds()) if sched_dt else None

        results.append({
            "headcode":   headcode,
            "direction":  direction,
            "obs_dt":     obs_dt,
            "sched_time": sched_time_str,
            "sched_dt":   sched_dt,
            "offset_sec": offset_secs,
            "uid":        sched["uid"],
            "atoc_code":  sched["atoc_code"],
            "power_type": sched["power_type"],
            "timing_load":sched["timing_load"],
            "stp":        sched["stp_indicator"],
        })

    # Print results table
    print(f"{'HC':>4}  {'Dir'}  {'Observed':>19}  {'Sched':>5}  {'Offset':>8}  {'UID':>8}  {'Op'}  {'Power'}")
    print("-" * 80)
    wb_offsets = []
    eb_offsets = []
    for r in results:
        off_str = f"{r['offset_sec']:+d}s" if r["offset_sec"] is not None else "  n/a"
        print(
            f"{r['headcode']:>4}  {r['direction']}  "
            f"{r['obs_dt'].strftime('%Y-%m-%d %H:%M:%S')}  "
            f"{r['sched_time'] or '????':>5}  {off_str:>8}  "
            f"{r['uid']:>8}  {r['atoc_code'] or '??':>2}  "
            f"{r['power_type'] or '??'}/{r['timing_load'] or '?'}"
        )
        if r["offset_sec"] is not None:
            if r["direction"] == "WB":
                wb_offsets.append(r["offset_sec"])
            else:
                eb_offsets.append(r["offset_sec"])

    # Summary statistics
    print()
    if wb_offsets:
        avg = sum(wb_offsets) / len(wb_offsets)
        mn, mx = min(wb_offsets), max(wb_offsets)
        spread = mx - mn
        print(f"WB offset from Reading departure: avg={avg:+.0f}s  min={mn:+d}s  max={mx:+d}s  spread={spread}s  (n={len(wb_offsets)})")
    if eb_offsets:
        avg = sum(eb_offsets) / len(eb_offsets)
        mn, mx = min(eb_offsets), max(eb_offsets)
        spread = mx - mn
        print(f"EB offset from Reading arrival:   avg={avg:+.0f}s  min={mn:+d}s  max={mx:+d}s  spread={spread}s  (n={len(eb_offsets)})")

    if no_schedule:
        print(f"\nNo schedule found ({len(no_schedule)}):")
        for hc, dt, d in no_schedule:
            print(f"  {hc} {d} {dt.strftime('%Y-%m-%d %H:%M:%S')}")

    if no_reading:
        print(f"\nSchedule found but no RDNGSTN location ({len(no_reading)}):")
        for hc, dt, d, uid in no_reading:
            print(f"  {hc} {d} {dt.strftime('%Y-%m-%d %H:%M:%S')} uid={uid}")


def main():
    parser = argparse.ArgumentParser(description="Correlate TD observations with CIF schedules")
    parser.add_argument("--csv",  default="td_data.csv", help="TD data CSV path")
    parser.add_argument("--date", help="Filter to date YYYY-MM-DD")
    args = parser.parse_args()

    filter_date = date.fromisoformat(args.date) if args.date else None
    analyse(args.csv, filter_date)


if __name__ == "__main__":
    main()
