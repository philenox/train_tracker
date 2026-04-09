#!/usr/bin/env python3
"""
trust_debug.py — Inspect raw TRUST feed messages to diagnose delay tracking.

Listens for 2 minutes and reports:
  - Sample Type 1 (activation) message fields
  - All STANOX codes seen in Type 3 (movement) messages, with counts
  - Any Type 3 movements for headcodes in today's schedule (verifies the
    train_id → headcode link is working)

Usage:
  venv/bin/python trust_debug.py
  venv/bin/python trust_debug.py --duration 120   # listen for N seconds (default 120)
  venv/bin/python trust_debug.py --stanox 87701   # show all movements at a specific STANOX
"""

import argparse
import json
import os
import signal
import sys
import time
import zlib
from collections import Counter
from datetime import datetime
from dotenv import load_dotenv
import stomp

load_dotenv()

HOST  = "publicdatafeeds.networkrail.co.uk"
PORT  = 61618
TOPIC = "/topic/TRAIN_MVT_ALL_TOC"


def _decompress(body):
    if isinstance(body, bytes):
        try:
            return zlib.decompress(body, zlib.MAX_WBITS | 16).decode("utf-8")
        except zlib.error:
            return body.decode("utf-8", errors="replace")
    return body


def load_todays_headcodes():
    """Load headcodes active today from the schedule DB for cross-referencing."""
    try:
        import schedule_db
        from datetime import date
        conn     = schedule_db.db_connect()
        today    = date.today()
        day_idx  = today.weekday()
        rows = conn.execute(
            """
            SELECT DISTINCT headcode FROM schedules
            WHERE start_date <= ? AND end_date >= ?
              AND substr(days_runs, ?, 1) = '1'
              AND headcode IS NOT NULL
            """,
            (today.isoformat(), today.isoformat(), day_idx + 1),
        ).fetchall()
        conn.close()
        return {r["headcode"] for r in rows}
    except Exception as e:
        print(f"[warn] Could not load schedule headcodes: {e}")
        return set()


class DebugListener(stomp.ConnectionListener):
    def __init__(self, conn, args, known_headcodes):
        self.conn            = conn
        self.args            = args
        self.known_headcodes = known_headcodes

        self.activation_sample  = None   # first activation body seen
        self.movement_sample    = None   # first movement body seen
        self.stanox_counts      = Counter()
        self.activations        = {}     # train_id → headcode
        self.schedule_hits      = []     # movements matching known headcodes
        self.total_messages     = 0
        self.total_activations  = 0
        self.total_movements    = 0

    def on_connected(self, frame):
        print(f"Connected to TRUST feed — listening for {self.args.duration}s\n")

    def on_error(self, frame):
        print(f"STOMP error: {frame.body}")

    def on_message(self, frame):
        self.conn.ack(frame.headers["message-id"], frame.headers["subscription"])
        try:
            messages = json.loads(_decompress(frame.body))
        except Exception:
            return
        if not isinstance(messages, list):
            messages = [messages]

        for msg in messages:
            self.total_messages += 1
            header   = msg.get("header", {})
            body     = msg.get("body", {})
            msg_type = header.get("msg_type", "")

            if msg_type == "0001":
                self.total_activations += 1
                if self.activation_sample is None:
                    self.activation_sample = body
                train_id = body.get("train_id", "").strip()
                # Try common field names for headcode
                headcode = (
                    body.get("signalling_id") or
                    body.get("reporting_id") or
                    body.get("train_service_code", "")[:4]
                ).strip()
                if train_id and headcode:
                    self.activations[train_id] = headcode

            elif msg_type == "0003":
                self.total_movements += 1
                if self.movement_sample is None:
                    self.movement_sample = body

                stanox   = body.get("loc_stanox", "").strip()
                train_id = body.get("train_id", "").strip()
                self.stanox_counts[stanox] += 1

                # Show movements at a specific STANOX if requested
                if self.args.stanox and stanox == self.args.stanox:
                    headcode = self.activations.get(train_id, "????")
                    variation = body.get("timetable_variation", "?")
                    status    = body.get("variation_status", "?")
                    event     = body.get("event_type", "?")
                    print(f"  [{datetime.now().strftime('%H:%M:%S')}] "
                          f"stanox={stanox} train_id={train_id} headcode={headcode} "
                          f"event={event} variation={variation} status={status}")

                # Cross-reference with today's schedule headcodes
                headcode = self.activations.get(train_id)
                if headcode and headcode in self.known_headcodes:
                    self.schedule_hits.append({
                        "time":      datetime.now().strftime("%H:%M:%S"),
                        "headcode":  headcode,
                        "train_id":  train_id,
                        "stanox":    stanox,
                        "variation": body.get("timetable_variation", "?"),
                        "status":    body.get("variation_status", "?"),
                        "event":     body.get("event_type", "?"),
                    })


def report(listener):
    print("\n" + "=" * 70)
    print("TRUST FEED DIAGNOSTIC REPORT")
    print("=" * 70)

    print(f"\nMessages received:   {listener.total_messages:,}")
    print(f"  Type 1 activations: {listener.total_activations:,}")
    print(f"  Type 3 movements:   {listener.total_movements:,}")
    print(f"  Activations mapped (train_id→headcode): {len(listener.activations):,}")

    # ── Activation sample ────────────────────────────────────────────────────
    print("\n── Type 1 Activation sample fields ──")
    if listener.activation_sample:
        for k, v in sorted(listener.activation_sample.items()):
            print(f"  {k:<30} = {str(v)!r}")
    else:
        print("  (none received)")

    # ── Movement sample ──────────────────────────────────────────────────────
    print("\n── Type 3 Movement sample fields ──")
    if listener.movement_sample:
        for k, v in sorted(listener.movement_sample.items()):
            print(f"  {k:<30} = {str(v)!r}")
    else:
        print("  (none received)")

    # ── Top STANOX codes ─────────────────────────────────────────────────────
    print("\n── Top 20 STANOX codes in movements ──")
    print("  (cross-reference these against the Reading-area stations you expect)")
    for stanox, count in listener.stanox_counts.most_common(20):
        print(f"  {stanox}  {count:>6,} movements")

    # ── Schedule cross-reference hits ────────────────────────────────────────
    print(f"\n── Movements matching today's schedule headcodes "
          f"({len(listener.schedule_hits)} hits) ──")
    if listener.schedule_hits:
        print(f"  {'Time':<9} {'Headcode':<9} {'STANOX':<8} {'Event':<12} "
              f"{'Variation':<10} {'Status'}")
        for h in listener.schedule_hits[-30:]:   # last 30
            print(f"  {h['time']:<9} {h['headcode']:<9} {h['stanox']:<8} "
                  f"{h['event']:<12} {h['variation']:<10} {h['status']}")
        if len(listener.schedule_hits) > 30:
            print(f"  ... ({len(listener.schedule_hits) - 30} earlier hits not shown)")
    else:
        print("  None — activations may not be mapping train_id→headcode correctly,")
        print("  or the listen window was too short to catch any Reading-area trains.")

    print()


def main():
    parser = argparse.ArgumentParser(description="TRUST feed diagnostic")
    parser.add_argument("--duration", type=int, default=120,
                        help="How many seconds to listen (default: 120)")
    parser.add_argument("--stanox", default=None,
                        help="Print every movement at this STANOX in real time")
    args = parser.parse_args()

    for var in ("NR_USERNAME", "NR_PASSWORD"):
        if not os.environ.get(var):
            print(f"Error: {var} not set in .env")
            sys.exit(1)

    print("Loading today's schedule headcodes...", end=" ", flush=True)
    known_headcodes = load_todays_headcodes()
    print(f"{len(known_headcodes)} headcodes loaded")

    username = os.environ["NR_USERNAME"]
    password = os.environ["NR_PASSWORD"]

    conn     = stomp.Connection([(HOST, PORT)], heartbeats=(15000, 15000),
                                heart_beat_receive_scale=2.5)
    listener = DebugListener(conn, args, known_headcodes)
    conn.set_listener("", listener)
    conn.connect(username=username, passcode=password, wait=True,
                 headers={"client-id": f"{username}-trust-debug"})
    conn.subscribe(destination=TOPIC, id="trust-debug", ack="client-individual",
                   headers={"activemq.subscriptionName": f"{username}-trust-debug"})

    def _shutdown(sig, frame):
        report(listener)
        conn.disconnect()
        sys.exit(0)

    signal.signal(signal.SIGINT, _shutdown)

    deadline = time.time() + args.duration
    while time.time() < deadline:
        remaining = int(deadline - time.time())
        print(f"\r  {remaining:3d}s remaining — "
              f"msgs={listener.total_messages:,}  "
              f"act={listener.total_activations:,}  "
              f"mvt={listener.total_movements:,}  "
              f"hits={len(listener.schedule_hits)}   ", end="", flush=True)
        time.sleep(1)

    print()
    report(listener)
    conn.disconnect()


if __name__ == "__main__":
    main()
