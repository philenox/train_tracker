#!/usr/bin/env python3
"""
schedule_db.py — Download and import Network Rail CIF schedule data into SQLite.

Filters to schedules that physically run through the Reading area
(between Reading station and Tilehurst), covering both stopping and
express/freight services that don't stop at Tilehurst.

schedule_days_runs format: 7 chars, index 0=Mon ... 6=Sun, '1'=runs that day.

Usage:
  venv/bin/python schedule_db.py           # download and import full CIF
  venv/bin/python schedule_db.py --stats   # show database stats
  venv/bin/python schedule_db.py --lookup HEADCODE  # show schedules for headcode
"""

import argparse
import gzip
import io
import json
import os
import sqlite3
import sys
from datetime import datetime, date

import requests
from dotenv import load_dotenv

load_dotenv()

DB_PATH = "schedules.db"
CIF_URL = (
    "https://publicdatafeeds.networkrail.co.uk/ntrod/CifFileAuthenticate"
    "?type=CIF_ALL_FULL_DAILY&day=toc-full"
)

# Import any schedule that passes through at least one of these TIPLOCs.
# Broad enough to catch fast trains (no Tilehurst stop) and stopping services.
FILTER_TIPLOCS = {
    "RDNGSTN",  # Reading station (stopping)
    "RDNGKBJ",  # Kennet Bridge Junction (just east of Reading, all main-line trains)
    "RDNGMLW",  # Reading Main Line West (non-stopping pass-throughs)
    "TILHRST",  # Tilehurst station
    "TILHEJN",  # Tilehurst East Junction
    "PANGBRN",  # Pangbourne (one stop west of Tilehurst)
}

DAY_NAMES = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]


def db_connect(path=DB_PATH):
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def create_schema(conn):
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS schedules (
            uid            TEXT NOT NULL,
            stp_indicator  TEXT NOT NULL,
            start_date     TEXT NOT NULL,
            end_date       TEXT,
            headcode       TEXT,
            atoc_code      TEXT,
            power_type     TEXT,
            timing_load    TEXT,
            speed          INTEGER,
            days_runs      TEXT,
            imported_at    TEXT,
            PRIMARY KEY (uid, stp_indicator, start_date)
        );

        CREATE TABLE IF NOT EXISTS schedule_locations (
            uid            TEXT    NOT NULL,
            stp_indicator  TEXT    NOT NULL,
            start_date     TEXT    NOT NULL,
            seq            INTEGER NOT NULL,
            tiploc         TEXT    NOT NULL,
            location_type  TEXT,
            arrival        TEXT,
            departure      TEXT,
            pass_time      TEXT,
            platform       TEXT,
            line           TEXT,
            PRIMARY KEY (uid, stp_indicator, start_date, seq)
        );

        CREATE INDEX IF NOT EXISTS idx_headcode   ON schedules(headcode);
        CREATE INDEX IF NOT EXISTS idx_dates      ON schedules(start_date, end_date);
        CREATE INDEX IF NOT EXISTS idx_loc_tiploc ON schedule_locations(tiploc);
        CREATE INDEX IF NOT EXISTS idx_loc_uid    ON schedule_locations(uid, stp_indicator, start_date);
    """)
    conn.commit()


def import_cif(conn):
    username = os.environ["NR_USERNAME"]
    password = os.environ["NR_PASSWORD"]

    print("Connecting to Network Rail CIF feed...")
    r = requests.get(CIF_URL, auth=(username, password), stream=True, timeout=60)
    r.raise_for_status()
    r.raw.decode_content = True

    print("Clearing existing data...")
    conn.executescript("DROP TABLE IF EXISTS schedules; DROP TABLE IF EXISTS schedule_locations;")
    create_schema(conn)

    buf  = gzip.GzipFile(fileobj=r.raw)
    text = io.TextIOWrapper(buf, encoding="utf-8")

    n = total = imported = skipped = 0
    now = datetime.now().isoformat()

    print("Streaming and parsing CIF records (this takes a few minutes)...")
    try:
        for line in text:
            n += 1
            line = line.strip()
            if not line:
                continue

            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue

            if "JsonScheduleV1" not in obj:
                continue

            total += 1
            s   = obj["JsonScheduleV1"]
            seg = s.get("schedule_segment", {})
            if isinstance(seg, list):
                seg = seg[0] if seg else {}

            locs = seg.get("schedule_location", [])

            # Skip cancellation records (no location data)
            if s.get("CIF_stp_indicator") == "C":
                skipped += 1
                continue

            # Filter to Reading-area services
            tiploc_set = {l.get("tiploc_code", "") for l in locs}
            if not tiploc_set & FILTER_TIPLOCS:
                skipped += 1
                continue

            uid   = s.get("CIF_train_uid", "")
            stp   = s.get("CIF_stp_indicator", "")
            start = s.get("schedule_start_date", "")

            conn.execute(
                """
                INSERT OR REPLACE INTO schedules
                    (uid, stp_indicator, start_date, end_date, headcode,
                     atoc_code, power_type, timing_load, speed,
                     days_runs, imported_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    uid, stp, start,
                    s.get("schedule_end_date"),
                    seg.get("signalling_id"),
                    s.get("atoc_code"),
                    seg.get("CIF_power_type"),
                    seg.get("CIF_timing_load"),
                    seg.get("CIF_speed"),
                    s.get("schedule_days_runs"),
                    now,
                ),
            )

            for seq, loc in enumerate(locs):
                arr = loc.get("arrival") or loc.get("public_arrival")
                dep = loc.get("departure") or loc.get("public_departure")
                conn.execute(
                    """
                    INSERT OR REPLACE INTO schedule_locations
                        (uid, stp_indicator, start_date, seq, tiploc,
                         location_type, arrival, departure, pass_time, platform, line)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        uid, stp, start, seq,
                        loc.get("tiploc_code"),
                        loc.get("location_type"),
                        arr, dep,
                        loc.get("pass"),
                        loc.get("platform"),
                        loc.get("line"),
                    ),
                )

            imported += 1
            if imported % 500 == 0:
                conn.commit()
                print(f"  {n:>8,} lines read | {imported:>5,} imported | {skipped:>6,} skipped", end="\r")

    except Exception as e:
        print(f"\nError at line {n}: {e}")
        raise
    finally:
        r.close()
        conn.commit()

    print(f"\n  {n:>8,} lines read | {imported:>5,} imported | {skipped:>6,} skipped")
    print(f"Done. Database written to {DB_PATH}")


def cmd_stats(conn):
    row = conn.execute("SELECT COUNT(*) FROM schedules").fetchone()[0]
    print(f"Schedules:        {row:,}")
    row = conn.execute("SELECT COUNT(*) FROM schedule_locations").fetchone()[0]
    print(f"Locations:        {row:,}")

    print("\nBy STP indicator (P=permanent, O=overlay, N=new):")
    for r in conn.execute("SELECT stp_indicator, COUNT(*) n FROM schedules GROUP BY stp_indicator ORDER BY n DESC"):
        print(f"  {r['stp_indicator']}: {r['n']:,}")

    print("\nBy operator:")
    for r in conn.execute("SELECT atoc_code, COUNT(*) n FROM schedules GROUP BY atoc_code ORDER BY n DESC LIMIT 15"):
        print(f"  {r['atoc_code'] or '??':>4}: {r['n']:,}")

    print("\nBy power type:")
    for r in conn.execute("SELECT power_type, COUNT(*) n FROM schedules GROUP BY power_type ORDER BY n DESC LIMIT 10"):
        print(f"  {r['power_type'] or '??':>6}: {r['n']:,}")

    today = date.today().isoformat()
    active = conn.execute(
        "SELECT COUNT(*) FROM schedules WHERE start_date <= ? AND end_date >= ?",
        (today, today)
    ).fetchone()[0]
    print(f"\nActive today ({today}): {active:,}")


def cmd_lookup(conn, headcode):
    today      = date.today().isoformat()
    day_idx    = date.today().weekday()  # 0=Mon
    schedules  = conn.execute(
        """
        SELECT * FROM schedules
        WHERE headcode = ?
          AND start_date <= ?
          AND end_date   >= ?
        ORDER BY start_date
        """,
        (headcode, today, today),
    ).fetchall()

    if not schedules:
        print(f"No schedules found for headcode '{headcode}' active today.")
        return

    for s in schedules:
        days = s["days_runs"] or "       "
        day_str = " ".join(DAY_NAMES[i] for i in range(7) if i < len(days) and days[i] == "1")
        runs_today = len(days) > day_idx and days[day_idx] == "1"
        print(f"\nUID: {s['uid']}  STP: {s['stp_indicator']}  "
              f"Operator: {s['atoc_code']}  Power: {s['power_type']}/{s['timing_load']}  "
              f"Speed: {s['speed']}")
        print(f"Valid: {s['start_date']} → {s['end_date']}  Runs: {day_str}"
              f"  {'[RUNS TODAY]' if runs_today else '[not today]'}")

        locs = conn.execute(
            "SELECT * FROM schedule_locations WHERE uid=? AND stp_indicator=? AND start_date=? ORDER BY seq",
            (s["uid"], s["stp_indicator"], s["start_date"]),
        ).fetchall()
        for loc in locs:
            t = loc["pass_time"] or loc["departure"] or loc["arrival"] or "    "
            flag = "pass" if loc["pass_time"] else ("dep" if loc["departure"] else "arr")
            plat = f" plat {loc['platform']}" if loc["platform"] else ""
            print(f"  {t}  {flag}  {loc['tiploc']:<12}{plat}")


def main():
    parser = argparse.ArgumentParser(description="Network Rail CIF schedule database")
    parser.add_argument("--db",     default=DB_PATH, help="SQLite database path")
    parser.add_argument("--stats",  action="store_true", help="Show database stats")
    parser.add_argument("--lookup", metavar="HEADCODE",  help="Look up schedules for a headcode")
    args = parser.parse_args()

    conn = db_connect(args.db)
    create_schema(conn)

    if args.stats:
        cmd_stats(conn)
    elif args.lookup:
        cmd_lookup(conn, args.lookup.upper())
    else:
        import_cif(conn)
        print()
        cmd_stats(conn)


if __name__ == "__main__":
    main()
