#!/usr/bin/env python3
"""
collect.py — Long-running data collection for TD and TRUST feeds.

Writes two CSV files simultaneously:
  td_YYYY-MM-DD.csv    — every berth step in the Reading TD area (D1 + D2)
  trust_YYYY-MM-DD.csv — every TRUST movement report

Files rotate at midnight so each day's data is kept separate.

Usage:
  caffeinate -i venv/bin/python -u collect.py
  caffeinate -i venv/bin/python -u collect.py --td-dir data/td --trust-dir data/trust
  caffeinate -i venv/bin/python -u collect.py --duration 8   # stop after 8 hours

See README for caffeinate / stay-awake notes.
"""

import argparse
import csv
import json
import os
import signal
import sys
import threading
import time
import zlib
from datetime import datetime, date, timedelta
from pathlib import Path

import stomp
from dotenv import load_dotenv

load_dotenv()

HOST  = "publicdatafeeds.networkrail.co.uk"
PORT  = 61618

TD_TOPIC    = "/topic/TD_ALL_SIG_AREA"
TRUST_TOPIC = "/topic/TRAIN_MVT_ALL_TOC"

TD_AREAS = {"D1", "D2"}   # Reading signal box areas

TD_FIELDNAMES    = ["timestamp", "td_ts_ms", "area_id", "from_berth", "to_berth", "headcode"]
TRUST_FIELDNAMES = ["timestamp", "train_id", "headcode", "stanox",
                    "event_type", "actual_ts_ms", "planned_ts_ms",
                    "variation_mins", "variation_status"]

# ── Shared stats (updated by both listener threads) ───────────────────────────

_stats_lock   = threading.Lock()
_stats = {
    "td_rows":      0,
    "trust_rows":   0,
    "td_connected": False,
    "trust_connected": False,
    "td_errors":    0,
    "trust_errors": 0,
    "started_at":   datetime.now(),
}


def _inc(key, n=1):
    with _stats_lock:
        _stats[key] += n


def _set(key, val):
    with _stats_lock:
        _stats[key] = val


# ── CSV helpers with daily rotation ──────────────────────────────────────────

class RotatingCSV:
    """Opens a new dated CSV file each day; writes header once per file."""

    def __init__(self, directory: str, prefix: str, fieldnames: list):
        self.directory  = Path(directory)
        self.prefix     = prefix
        self.fieldnames = fieldnames
        self.directory.mkdir(parents=True, exist_ok=True)
        self._lock      = threading.Lock()
        self._date      = None
        self._file      = None
        self._writer    = None
        self._open_for_today()

    def _open_for_today(self):
        today = date.today()
        if today == self._date:
            return
        if self._file:
            self._file.close()
        path = self.directory / f"{self.prefix}_{today.isoformat()}.csv"
        is_new = not path.exists() or path.stat().st_size == 0
        self._file   = open(path, "a", newline="")
        self._writer = csv.DictWriter(self._file, fieldnames=self.fieldnames)
        if is_new:
            self._writer.writeheader()
        self._date = today
        print(f"[{self.prefix}] writing to {path}")

    def write(self, row: dict):
        with self._lock:
            self._open_for_today()
            self._writer.writerow(row)
            self._file.flush()

    def close(self):
        with self._lock:
            if self._file:
                self._file.close()


# ── TD listener ───────────────────────────────────────────────────────────────

class TDListener(stomp.ConnectionListener):
    def __init__(self, conn, csv_out: RotatingCSV):
        self.conn    = conn
        self.csv_out = csv_out

    def on_connected(self, frame):
        _set("td_connected", True)

    def on_disconnected(self):
        _set("td_connected", False)
        time.sleep(5)
        _td_connect(self.conn)

    def on_error(self, frame):
        _inc("td_errors")

    def on_message(self, frame):
        self.conn.ack(frame.headers["message-id"], frame.headers["subscription"])
        try:
            body = frame.body
            if isinstance(body, bytes):
                try:
                    body = zlib.decompress(body, zlib.MAX_WBITS | 16).decode()
                except zlib.error:
                    body = body.decode("utf-8", errors="replace")
            messages = json.loads(body)
            if not isinstance(messages, list):
                messages = [messages]
        except Exception:
            return

        now = datetime.now().isoformat()
        for msg in messages:
            for msg_type, b in msg.items():
                if msg_type != "CA_MSG":
                    continue
                if b.get("area_id", "") not in TD_AREAS:
                    continue
                headcode = b.get("descr", "").strip()
                if not headcode:
                    continue
                self.csv_out.write({
                    "timestamp":  now,
                    "td_ts_ms":   b.get("time", ""),
                    "area_id":    b.get("area_id", ""),
                    "from_berth": b.get("from", "").strip(),
                    "to_berth":   b.get("to", "").strip(),
                    "headcode":   headcode,
                })
                _inc("td_rows")


def _td_connect(conn):
    username = os.environ["NR_USERNAME"]
    password = os.environ["NR_PASSWORD"]
    conn.connect(username=username, passcode=password, wait=True,
                 headers={"client-id": f"{username}-collect-td"})
    conn.subscribe(destination=TD_TOPIC, id="collect-td",
                   ack="client-individual",
                   headers={"activemq.subscriptionName": f"{username}-collect-td"})


# ── TRUST listener ────────────────────────────────────────────────────────────

class TRUSTListener(stomp.ConnectionListener):
    def __init__(self, conn, csv_out: RotatingCSV):
        self.conn    = conn
        self.csv_out = csv_out

    def on_connected(self, frame):
        _set("trust_connected", True)

    def on_disconnected(self):
        _set("trust_connected", False)
        time.sleep(5)
        _trust_connect(self.conn)

    def on_error(self, frame):
        _inc("trust_errors")

    def on_message(self, frame):
        self.conn.ack(frame.headers["message-id"], frame.headers["subscription"])
        try:
            body = frame.body
            if isinstance(body, bytes):
                try:
                    body = zlib.decompress(body, zlib.MAX_WBITS | 16).decode()
                except zlib.error:
                    body = body.decode("utf-8", errors="replace")
            messages = json.loads(body)
            if not isinstance(messages, list):
                messages = [messages]
        except Exception:
            return

        now = datetime.now().isoformat()
        for msg in messages:
            header   = msg.get("header", {})
            body     = msg.get("body", {})
            if header.get("msg_type") != "0003":
                continue
            train_id = body.get("train_id", "").strip()
            headcode = train_id[2:6] if len(train_id) >= 6 else ""
            if not headcode:
                continue
            self.csv_out.write({
                "timestamp":      now,
                "train_id":       train_id,
                "headcode":       headcode,
                "stanox":         body.get("loc_stanox", "").strip(),
                "event_type":     body.get("event_type", "").strip(),
                "actual_ts_ms":   body.get("actual_timestamp", "").strip(),
                "planned_ts_ms":  body.get("planned_timestamp", "").strip(),
                "variation_mins": body.get("timetable_variation", "").strip(),
                "variation_status": body.get("variation_status", "").strip(),
            })
            _inc("trust_rows")


def _trust_connect(conn):
    username = os.environ["NR_USERNAME"]
    password = os.environ["NR_PASSWORD"]
    conn.connect(username=username, passcode=password, wait=True,
                 headers={"client-id": f"{username}-collect-trust"})
    conn.subscribe(destination=TRUST_TOPIC, id="collect-trust",
                   ack="client-individual",
                   headers={"activemq.subscriptionName": f"{username}-collect-trust"})


# ── Stats display ─────────────────────────────────────────────────────────────

def _stats_loop(stop_event: threading.Event):
    while not stop_event.is_set():
        with _stats_lock:
            td_ok    = "✓" if _stats["td_connected"]    else "✗"
            tr_ok    = "✓" if _stats["trust_connected"] else "✗"
            td_rows  = _stats["td_rows"]
            tr_rows  = _stats["trust_rows"]
            elapsed  = (datetime.now() - _stats["started_at"]).total_seconds()
            td_err   = _stats["td_errors"]
            tr_err   = _stats["trust_errors"]

        hrs, rem = divmod(int(elapsed), 3600)
        mins, secs = divmod(rem, 60)
        rate_td    = td_rows  / elapsed * 60 if elapsed > 0 else 0
        rate_trust = tr_rows  / elapsed * 60 if elapsed > 0 else 0

        print(
            f"[{datetime.now().strftime('%H:%M:%S')}]  "
            f"uptime {hrs:02d}:{mins:02d}:{secs:02d}  |  "
            f"TD {td_ok} {td_rows:>8,} rows ({rate_td:.0f}/min)  "
            f"err={td_err}  |  "
            f"TRUST {tr_ok} {tr_rows:>9,} rows ({rate_trust:.0f}/min)  "
            f"err={tr_err}",
            flush=True,
        )
        stop_event.wait(30)


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Log TD + TRUST feeds to CSV for offline analysis.",
        epilog="Tip: wrap with  caffeinate -i  to prevent MacBook sleep.",
    )
    parser.add_argument("--td-dir",    default="data", metavar="DIR",
                        help="Directory for TD CSV files (default: data/)")
    parser.add_argument("--trust-dir", default="data", metavar="DIR",
                        help="Directory for TRUST CSV files (default: data/)")
    parser.add_argument("--duration",  type=float, default=None, metavar="HOURS",
                        help="Stop automatically after this many hours")
    args = parser.parse_args()

    for var in ("NR_USERNAME", "NR_PASSWORD"):
        if not os.environ.get(var):
            print(f"Error: {var} not set in .env")
            sys.exit(1)

    td_csv    = RotatingCSV(args.td_dir,    "td",    TD_FIELDNAMES)
    trust_csv = RotatingCSV(args.trust_dir, "trust", TRUST_FIELDNAMES)

    # ── Start TD connection ───────────────────────────────────────────────────
    td_conn = stomp.Connection([(HOST, PORT)], heartbeats=(15000, 15000),
                               heart_beat_receive_scale=2.5)
    td_conn.set_listener("", TDListener(td_conn, td_csv))
    print("Connecting to TD feed...")
    _td_connect(td_conn)

    # ── Start TRUST connection ────────────────────────────────────────────────
    trust_conn = stomp.Connection([(HOST, PORT)], heartbeats=(15000, 15000),
                                  heart_beat_receive_scale=2.5)
    trust_conn.set_listener("", TRUSTListener(trust_conn, trust_csv))
    print("Connecting to TRUST feed...")
    _trust_connect(trust_conn)

    # ── Stats printer ─────────────────────────────────────────────────────────
    stop_event = threading.Event()
    threading.Thread(target=_stats_loop, args=(stop_event,),
                     daemon=True, name="stats").start()

    deadline = (datetime.now() + timedelta(hours=args.duration)
                if args.duration else None)

    def shutdown(sig, frame):
        print("\nShutting down...")
        stop_event.set()
        td_conn.disconnect()
        trust_conn.disconnect()
        td_csv.close()
        trust_csv.close()
        with _stats_lock:
            print(f"Collected {_stats['td_rows']:,} TD rows, "
                  f"{_stats['trust_rows']:,} TRUST rows.")
        sys.exit(0)

    signal.signal(signal.SIGINT,  shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    print("Collecting — Ctrl+C to stop.\n")
    while True:
        if deadline and datetime.now() >= deadline:
            print(f"\n--duration {args.duration}h reached.")
            shutdown(None, None)
        time.sleep(5)


if __name__ == "__main__":
    main()
