"""
Background STOMP client for the Reading TD feed.
Maintains state of the last train seen at each watched berth,
and approaching ETAs based on upstream trigger berths.

Berth layout (west → east, toward Reading):
  ... 1772 ... 1757 ... 1742 ... 1724 ...

Westbound (away from Reading, increasing berth numbers):
  trigger: 1733 → visible: 1757

Eastbound (toward Reading, decreasing berth numbers):
  trigger: 1772 → visible: 1724
"""

import json
import os
import threading
import time
import zlib
from datetime import datetime, timedelta

import stomp

HOST         = "publicdatafeeds.networkrail.co.uk"
PORT         = 61618
TOPIC        = "/topic/TD_ALL_SIG_AREA"
TARGET_AREAS = {"D1", "D2"}

WESTBOUND_BERTH   = "1757"
EASTBOUND_BERTH   = "1724"
WESTBOUND_TRIGGER = "1733"
EASTBOUND_TRIGGER = "1772"

# Measured from CSV: 1772→1724 consistently ~174s, 1733→1757 consistently ~52s
EB_TRANSIT_SECS = 174
WB_TRANSIT_SECS = 52

WATCH_BERTHS = {WESTBOUND_BERTH, EASTBOUND_BERTH, WESTBOUND_TRIGGER, EASTBOUND_TRIGGER}

POSITION_EXPIRY_SECS = 1800   # forget a train's position after 30 min of silence
CACHE_PATH           = ".td_cache.json"
CACHE_SAVE_INTERVAL  = 30     # seconds between cache saves

# Shared state — updated by the background thread, read by the display
_lock = threading.Lock()

# Last train actually seen at each visible berth
_state = {
    WESTBOUND_BERTH: None,  # {"headcode": str, "time": datetime}
    EASTBOUND_BERTH: None,
}

# Trains that crossed a trigger berth and are approaching the visible berth
_approaching = {
    "WB": None,  # {"headcode": str, "eta": datetime, "trigger_time": datetime}
    "EB": None,
}

# Most-recent berth for every headcode seen in the TD area
_positions: dict = {}   # headcode → {"berth": str, "ts": datetime}

_callbacks = []  # optional: list of callables to invoke on a visible berth event


def get_last(berth: str) -> dict | None:
    """Returns the last train seen at a visible berth, or None."""
    with _lock:
        return _state.get(berth)


def get_approaching(direction: str) -> dict | None:
    """
    Returns approaching train info for 'WB' or 'EB', or None.
    Dict has keys: headcode, eta (datetime), trigger_time (datetime).
    """
    with _lock:
        return _approaching.get(direction)


def _save_cache():
    with _lock:
        data = {
            hc: {"berth": p["berth"], "ts": p["ts"].isoformat()}
            for hc, p in _positions.items()
        }
    try:
        with open(CACHE_PATH, "w") as f:
            json.dump(data, f)
    except Exception:
        pass


def _load_cache():
    try:
        with open(CACHE_PATH) as f:
            data = json.load(f)
    except Exception:
        return
    now    = datetime.now()
    loaded = {}
    for hc, p in data.items():
        try:
            ts = datetime.fromisoformat(p["ts"])
            if (now - ts).total_seconds() < POSITION_EXPIRY_SECS:
                loaded[hc] = {"berth": p["berth"], "ts": ts}
        except Exception:
            pass
    with _lock:
        _positions.update(loaded)


def get_position(headcode: str) -> dict | None:
    """
    Return the most recent berth position for a headcode, or None if not seen
    recently (within POSITION_EXPIRY_SECS).  Dict has keys: berth (str), ts (datetime).
    """
    with _lock:
        pos = _positions.get(headcode)
    if not pos:
        return None
    age = (datetime.now() - pos["ts"]).total_seconds()
    return pos if age < POSITION_EXPIRY_SECS else None


def get_all_positions() -> dict:
    """Return a copy of all non-stale positions: headcode → {berth, ts}."""
    now = datetime.now()
    with _lock:
        return {
            hc: p for hc, p in _positions.items()
            if (now - p["ts"]).total_seconds() < POSITION_EXPIRY_SECS
        }


def on_event(fn):
    """Register a callback fn(berth, headcode, dt) for visible berth events."""
    _callbacks.append(fn)


def _decompress(body):
    if isinstance(body, bytes):
        try:
            return zlib.decompress(body, zlib.MAX_WBITS | 16).decode("utf-8")
        except zlib.error:
            return body.decode("utf-8", errors="replace")
    return body


class _Listener(stomp.ConnectionListener):
    def __init__(self, conn, csv_writer=None, csv_file=None):
        self.conn = conn
        self.csv_writer = csv_writer
        self.csv_file = csv_file

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
        to_berth   = body.get("to", "").strip()
        headcode   = body.get("descr", "").strip()
        from_berth = body.get("from", "").strip()
        ts_ms      = body.get("time", "")

        if not headcode:
            return

        dt = datetime.now()

        # Log all steps to CSV if recording
        if self.csv_writer:
            self.csv_writer.writerow([dt.isoformat(), ts_ms, from_berth, to_berth, headcode])
            self.csv_file.flush()

        # Track current position for every berth step (used by routing table lookups)
        if to_berth:
            with _lock:
                _positions[headcode] = {"berth": to_berth, "ts": dt}

        if to_berth not in WATCH_BERTHS:
            return

        if to_berth == EASTBOUND_TRIGGER:
            # Train entered the EB trigger berth — compute ETA for visible berth
            eta = dt + timedelta(seconds=EB_TRANSIT_SECS)
            with _lock:
                _approaching["EB"] = {"headcode": headcode, "eta": eta, "trigger_time": dt}

        elif to_berth == WESTBOUND_TRIGGER:
            # Train entered the WB trigger berth — compute ETA for visible berth
            eta = dt + timedelta(seconds=WB_TRANSIT_SECS)
            with _lock:
                _approaching["WB"] = {"headcode": headcode, "eta": eta, "trigger_time": dt}

        elif to_berth == EASTBOUND_BERTH:
            # Train arrived at visible EB berth
            with _lock:
                _state[EASTBOUND_BERTH] = {"headcode": headcode, "time": dt}
                _approaching["EB"] = None
            for fn in _callbacks:
                try:
                    fn(to_berth, headcode, dt)
                except Exception:
                    pass

        elif to_berth == WESTBOUND_BERTH:
            # Train arrived at visible WB berth
            with _lock:
                _state[WESTBOUND_BERTH] = {"headcode": headcode, "time": dt}
                _approaching["WB"] = None
            for fn in _callbacks:
                try:
                    fn(to_berth, headcode, dt)
                except Exception:
                    pass


def _connect(conn):
    username = os.environ["NR_USERNAME"]
    password = os.environ["NR_PASSWORD"]
    conn.connect(username=username, passcode=password, wait=True,
                 headers={"client-id": f"{username}-td"})
    conn.subscribe(destination=TOPIC, id="td-reading", ack="client-individual",
                   headers={"activemq.subscriptionName": f"{username}-td-reading"})


def start(csv_writer=None, csv_file=None):
    """Start the TD client in a daemon thread. Returns the connection."""
    _load_cache()

    conn = stomp.Connection([(HOST, PORT)], heartbeats=(15000, 15000),
                            heart_beat_receive_scale=2.5)
    conn.set_listener("", _Listener(conn, csv_writer=csv_writer, csv_file=csv_file))
    _connect(conn)

    def _keepalive():
        while True:
            if not conn.is_connected():
                time.sleep(5)
            time.sleep(1)

    def _cache_loop():
        while True:
            time.sleep(CACHE_SAVE_INTERVAL)
            _save_cache()

    threading.Thread(target=_keepalive,  daemon=True, name="td-keepalive").start()
    threading.Thread(target=_cache_loop, daemon=True, name="td-cache").start()
    return conn
