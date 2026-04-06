"""
Background STOMP client for the Reading TD feed.
Maintains state of the last train seen at each watched berth.
Import this and call start() to run in a daemon thread.
"""

import json
import os
import threading
import time
import zlib
from datetime import datetime

import stomp

HOST         = "publicdatafeeds.networkrail.co.uk"
PORT         = 61618
TOPIC        = "/topic/TD_ALL_SIG_AREA"
TARGET_AREAS = {"D1", "D2"}

WESTBOUND_BERTH = "1757"
EASTBOUND_BERTH = "1742"
WATCH_BERTHS    = {WESTBOUND_BERTH, EASTBOUND_BERTH}

# Shared state — updated by the background thread, read by the display
_lock = threading.Lock()
_state = {
    WESTBOUND_BERTH: None,  # {"headcode": str, "time": datetime}
    EASTBOUND_BERTH: None,
}
_callbacks = []  # optional: list of callables to invoke on a berth event


def get_last(berth: str) -> dict | None:
    with _lock:
        return _state.get(berth)


def on_event(fn):
    """Register a callback fn(berth, headcode, dt) for berth events."""
    _callbacks.append(fn)


def _decompress(body):
    if isinstance(body, bytes):
        try:
            return zlib.decompress(body, zlib.MAX_WBITS | 16).decode("utf-8")
        except zlib.error:
            return body.decode("utf-8", errors="replace")
    return body


class _Listener(stomp.ConnectionListener):
    def __init__(self, conn, csv_writer=None):
        self.conn = conn
        self.csv_writer = csv_writer

    def on_disconnected(self):
        time.sleep(5)
        _connect(self.conn)

    def on_error(self, frame):
        print(f"[td_client] STOMP error: {frame.body}")

    def on_message(self, frame):
        self.conn.ack(frame.headers["message-id"], frame.headers["subscription"])
        try:
            messages = json.loads(_decompress(frame.body))
        except Exception:
            return
        if not isinstance(messages, list):
            messages = [messages]
        for msg in messages:
            for msg_type, body in msg.items():
                if msg_type == "CA_MSG":
                    self._handle(body)

    def _handle(self, body):
        if body.get("area_id", "") not in TARGET_AREAS:
            return
        to_berth  = body.get("to", "").strip()
        headcode  = body.get("descr", "").strip()
        from_berth = body.get("from", "").strip()
        ts_ms     = body.get("time", "")

        if not headcode:
            return

        # Log all steps to CSV if recording
        if self.csv_writer:
            now = datetime.now().isoformat()
            self.csv_writer.writerow([now, ts_ms, from_berth, to_berth, headcode])

        if to_berth not in WATCH_BERTHS:
            return

        dt = datetime.now()
        with _lock:
            _state[to_berth] = {"headcode": headcode, "time": dt}

        for fn in _callbacks:
            try:
                fn(to_berth, headcode, dt)
            except Exception:
                pass


def _connect(conn):
    username = os.environ["NR_USERNAME"]
    password = os.environ["NR_PASSWORD"]
    conn.connect(username=username, passcode=password, wait=True,
                 headers={"client-id": username})
    conn.subscribe(destination=TOPIC, id="td-reading", ack="client-individual",
                   headers={"activemq.subscriptionName": "td-reading"})


def start(csv_writer=None):
    """Start the TD client in a daemon thread. Returns the connection."""
    conn = stomp.Connection([(HOST, PORT)], heartbeats=(15000, 15000),
                            heart_beat_receive_scale=2.5)
    conn.set_listener("", _Listener(conn, csv_writer=csv_writer))
    _connect(conn)

    def _keepalive():
        while True:
            if not conn.is_connected():
                time.sleep(5)
            time.sleep(1)

    t = threading.Thread(target=_keepalive, daemon=True, name="td-keepalive")
    t.start()
    return conn
