"""
Background STOMP client for the Network Rail TRUST (Train Movement) feed.
Maintains real-time delay estimates for trains, keyed by headcode.

Processes two message types:
  Type 1 (Activation): train_id → headcode mapping
  Type 3 (Movement):   delay in minutes from timetable_variation field

Delay records expire after 4 hours to avoid stale data.
"""

import json
import os
import threading
import time
import zlib
from datetime import datetime, timedelta

import stomp

HOST  = "publicdatafeeds.networkrail.co.uk"
PORT  = 61618
TOPIC = "/topic/TRAIN_MVT_ALL_TOC"

_lock = threading.Lock()

# train_id (10-char TRUST ID) → headcode (4-char signalling ID)
_activations: dict = {}

# headcode → {"delay_secs": int, "stanox": str, "updated_at": datetime}
_delays: dict = {}

EXPIRY_SECS = 4 * 3600


def get_delay(headcode: str) -> int | None:
    """
    Return delay in seconds for a headcode (positive = late, negative = early).
    Returns None if no recent data is available.
    """
    with _lock:
        rec = _delays.get(headcode)
    if rec is None:
        return None
    age = (datetime.now() - rec["updated_at"]).total_seconds()
    if age > EXPIRY_SECS:
        return None
    return rec["delay_secs"]


def delay_count() -> int:
    """Number of trains with active delay records (for status display)."""
    with _lock:
        cutoff = datetime.now() - timedelta(seconds=EXPIRY_SECS)
        return sum(1 for r in _delays.values() if r["updated_at"] > cutoff)


def _decompress(body):
    if isinstance(body, bytes):
        try:
            return zlib.decompress(body, zlib.MAX_WBITS | 16).decode("utf-8")
        except zlib.error:
            return body.decode("utf-8", errors="replace")
    return body


class _Listener(stomp.ConnectionListener):
    def __init__(self, conn):
        self.conn = conn

    def on_disconnected(self):
        time.sleep(5)
        _connect(self.conn)

    def on_error(self, frame):
        print(f"[trust_client] STOMP error: {frame.body}")

    def on_message(self, frame):
        self.conn.ack(frame.headers["message-id"], frame.headers["subscription"])
        try:
            messages = json.loads(_decompress(frame.body))
        except Exception:
            return
        if not isinstance(messages, list):
            messages = [messages]
        for msg in messages:
            header   = msg.get("header", {})
            body     = msg.get("body", {})
            msg_type = header.get("msg_type", "")
            if msg_type == "0001":
                self._handle_activation(body)
            elif msg_type == "0003":
                self._handle_movement(body)

    def _handle_activation(self, body):
        train_id = body.get("train_id", "").strip()
        # schedule_wtt_id is the headcode (4 chars) + a check character, e.g. "1G331"
        wtt_id   = body.get("schedule_wtt_id", "").strip()
        headcode = wtt_id[:4] if len(wtt_id) >= 4 else wtt_id
        if not train_id or not headcode:
            return
        with _lock:
            _activations[train_id] = headcode
            # Bound memory — prune when we exceed 50k entries
            if len(_activations) > 50_000:
                for k in list(_activations)[:10_000]:
                    del _activations[k]

    def _handle_movement(self, body):
        train_id  = body.get("train_id", "").strip()
        variation = body.get("timetable_variation", "").strip()
        status    = body.get("variation_status", "").strip()

        # Headcode is embedded in train_id at [2:6], e.g. "845Z371Y09" → "5Z37".
        # Fall back to the activation map if we have it (slightly more reliable).
        headcode = _activations.get(train_id) or (train_id[2:6] if len(train_id) >= 6 else None)
        if not headcode or not variation:
            return

        try:
            minutes = int(variation)
        except ValueError:
            return

        # timetable_variation is the absolute value; status gives the sign
        if status == "EARLY":
            minutes = -minutes
        elif status == "ON TIME":
            minutes = 0

        with _lock:
            _delays[headcode] = {
                "delay_secs": minutes * 60,
                "updated_at": datetime.now(),
            }


def _connect(conn):
    username = os.environ["NR_USERNAME"]
    password = os.environ["NR_PASSWORD"]
    conn.connect(
        username=username,
        passcode=password,
        wait=True,
        headers={"client-id": f"{username}-trust"},
    )
    conn.subscribe(
        destination=TOPIC,
        id="trust-mvt",
        ack="client-individual",
        headers={"activemq.subscriptionName": f"{username}-trust-mvt"},
    )


def start():
    """Start the TRUST client in a daemon thread. Returns the connection."""
    conn = stomp.Connection(
        [(HOST, PORT)],
        heartbeats=(15000, 15000),
        heart_beat_receive_scale=2.5,
    )
    conn.set_listener("", _Listener(conn))
    _connect(conn)

    def _keepalive():
        while True:
            if not conn.is_connected():
                time.sleep(5)
            time.sleep(1)

    threading.Thread(target=_keepalive, daemon=True, name="trust-keepalive").start()
    return conn
