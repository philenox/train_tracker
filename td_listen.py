#!/usr/bin/env python3
"""
Listen to the Network Rail TD feed and watch for trains passing the
section of track visible from the window west of Reading station.

Confirmed berths:
  1757 — westbound (down) line
  1742 — eastbound (up) line

Usage:
  venv/bin/python -u td_listen.py              # watch window berths only
  venv/bin/python -u td_listen.py --all        # log all Reading area berths
  venv/bin/python -u td_listen.py --csv FILE   # record all steps to CSV
"""

import argparse
import csv
import os
import signal
import sys
import time
from datetime import datetime

import td_client
from dotenv import load_dotenv

load_dotenv()

WESTBOUND_BERTH = td_client.WESTBOUND_BERTH
EASTBOUND_BERTH = td_client.EASTBOUND_BERTH


def main():
    parser = argparse.ArgumentParser(description="Reading TD feed — window berth watcher")
    parser.add_argument("--all", action="store_true",
                        help="Log all berth steps in the Reading area")
    parser.add_argument("--csv", metavar="FILE", default=None,
                        help="Record all Reading area berth steps to a CSV file")
    args = parser.parse_args()

    for var in ("NR_USERNAME", "NR_PASSWORD"):
        if not os.environ.get(var):
            print(f"Error: {var} not set in .env")
            sys.exit(1)

    csv_writer = None
    csv_file   = None
    if args.csv:
        csv_file   = open(args.csv, "a", newline="")
        csv_writer = csv.writer(csv_file)
        # Write header if file is new/empty
        if csv_file.tell() == 0:
            csv_writer.writerow(["timestamp", "td_ts_ms", "from_berth", "to_berth", "headcode"])
        print(f"Recording all steps to {args.csv}")

    # Register a print callback for window berth events (used in focused mode)
    def _on_event(berth, headcode, dt):
        if args.all:
            return  # all mode uses its own output from the raw handler
        direction = "WESTBOUND" if berth == WESTBOUND_BERTH else "EASTBOUND"
        print(f"[{dt.strftime('%H:%M:%S')}]  {direction:<10}  {headcode}  at berth {berth}")

    td_client.on_event(_on_event)

    # In --all mode, also hook into raw messages via a custom listener
    if args.all:
        import json, zlib, stomp

        class AllListener(stomp.ConnectionListener):
            def __init__(self, conn):
                self.conn = conn

            def on_connected(self, frame):
                print(f"Connected — logging ALL Reading area berth steps")
                print(f"Format: [time]  headcode  from → to\n")

            def on_disconnected(self, frame=None):
                print("Disconnected — reconnecting...")

            def on_error(self, frame):
                print(f"STOMP error: {frame.body}")

            def on_message(self, frame):
                self.conn.ack(frame.headers["message-id"], frame.headers["subscription"])
                try:
                    body = frame.body
                    if isinstance(body, bytes):
                        try: body = zlib.decompress(body, zlib.MAX_WBITS | 16).decode()
                        except: body = body.decode("utf-8", errors="replace")
                    messages = json.loads(body)
                    if not isinstance(messages, list):
                        messages = [messages]
                    for msg in messages:
                        for msg_type, b in msg.items():
                            if msg_type != "CA_MSG":
                                continue
                            if b.get("area_id", "") not in td_client.TARGET_AREAS:
                                continue
                            headcode = b.get("descr", "").strip()
                            if not headcode:
                                continue
                            fb = b.get("from", "").strip()
                            tb = b.get("to", "").strip()
                            ts = b.get("time", "")
                            now = datetime.now().strftime("%H:%M:%S")
                            print(f"[{now}]  {headcode:<6}  {fb:>6} → {tb:<6}  (td_ts={ts})")
                            if csv_writer:
                                csv_writer.writerow([datetime.now().isoformat(), ts, fb, tb, headcode])
                                csv_file.flush()
                except Exception:
                    pass

        import stomp as stomp_mod
        conn = stomp_mod.Connection(
            [(td_client.HOST, td_client.PORT)],
            heartbeats=(15000, 15000),
            heart_beat_receive_scale=2.5,
        )
        conn.set_listener("", AllListener(conn))
        username = os.environ["NR_USERNAME"]
        password = os.environ["NR_PASSWORD"]
        conn.connect(username=username, passcode=password, wait=True,
                     headers={"client-id": username})
        conn.subscribe(destination=td_client.TOPIC, id="td-all",
                       ack="client-individual",
                       headers={"activemq.subscriptionName": "td-all"})

        def shutdown_all(sig, frame):
            print("\nShutting down...")
            if csv_file:
                csv_file.close()
            conn.disconnect()
            sys.exit(0)

        signal.signal(signal.SIGINT, shutdown_all)
        print("Listening... press Ctrl+C to stop.")
        while conn.is_connected():
            time.sleep(1)

    else:
        # Focused mode — use td_client shared module
        conn = td_client.start(csv_writer=csv_writer)

        if not args.csv:
            print(f"Watching berth {WESTBOUND_BERTH} (westbound) and {EASTBOUND_BERTH} (eastbound)")
            print("Ctrl+C to stop.\n")

        def shutdown(sig, frame):
            print("\nShutting down...")
            if csv_file:
                csv_file.close()
            conn.disconnect()
            sys.exit(0)

        signal.signal(signal.SIGINT, shutdown)
        while True:
            time.sleep(1)


if __name__ == "__main__":
    main()
