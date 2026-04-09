"""
routing.py — Load and query the berth routing table built by analyse_routes.py.

The table maps (berth, direction) → {p_visible, eta_mean, eta_std, n_trains, ...}.
Direction is 'WB' (→ berth 1757) or 'EB' (→ berth 1724), or '??' for trains
that were in the Reading TD area but never reached a visible berth.

If routing_table.json doesn't exist, all lookups return None / False and the
prediction engine falls back to TRUST + schedule as before.
"""

import json
import os
from datetime import datetime

ROUTING_TABLE_PATH  = "routing_table.json"
OFF_PATH_MIN_SAMPLES = 5   # min observations to confidently call a berth off-path

_table: dict      = {}
_loaded_at: datetime | None = None
_table_mtime: float | None  = None


def _load():
    global _table, _loaded_at, _table_mtime
    if not os.path.exists(ROUTING_TABLE_PATH):
        _table = {}
        return
    mtime = os.path.getmtime(ROUTING_TABLE_PATH)
    if mtime == _table_mtime:
        return   # already up to date
    with open(ROUTING_TABLE_PATH) as f:
        _table = json.load(f)
    _table_mtime = mtime
    _loaded_at   = datetime.now()


def _entry(berth: str, direction: str) -> dict | None:
    _load()
    return _table.get(f"{berth}__{direction}")


def lookup(berth: str, direction: str) -> dict | None:
    """Return the full routing table entry for (berth, direction), or None."""
    return _entry(berth, direction)


def eta_secs(berth: str, direction: str) -> float | None:
    """Return mean ETA in seconds from this berth to the visible berth, or None."""
    entry = _entry(berth, direction)
    return entry["eta_mean"] if entry and entry.get("eta_mean") is not None else None


def p_visible(berth: str, direction: str) -> float | None:
    """Return fraction of observed trains from this berth that reached the visible berth."""
    entry = _entry(berth, direction)
    return entry["p_visible"] if entry else None


def is_on_path(berth: str, direction: str) -> bool:
    """
    True if this berth is in a known WB or EB chain leading to a visible berth
    with high confidence (p_visible >= 0.8, at least 2 observed samples).
    """
    entry = _entry(berth, direction)
    if not entry:
        return False
    return entry.get("p_visible", 0) >= 0.8 and entry.get("n_trains", 0) >= 2


def is_off_path(berth: str) -> bool:
    """
    True if this berth was observed often enough in the Reading area but never
    led to a visible berth for either direction.  Used to drop trains from the
    display that have been confirmed as heading to a platform/depot/other route.

    Requires OFF_PATH_MIN_SAMPLES observations to avoid false negatives from
    berths we simply didn't observe often enough.
    """
    _load()
    has_directional = (
        f"{berth}__WB" in _table or
        f"{berth}__EB" in _table
    )
    if has_directional:
        return False
    unk = _table.get(f"{berth}__??")
    if unk and unk.get("n_trains", 0) >= OFF_PATH_MIN_SAMPLES:
        return True
    return False


def max_eta_secs(direction: str) -> float | None:
    """
    Return the largest eta_mean across all known berths for this direction.

    This is the ETA from the furthest-out berth in the chain to the visible berth.
    Used as a floor when a train has no TD position: if it hasn't been seen anywhere
    in the area it must still be at least this far away.
    """
    _load()
    values = [
        v["eta_mean"]
        for k, v in _table.items()
        if k.endswith(f"__{direction}") and v.get("eta_mean") is not None
    ]
    return max(values) if values else None


def loaded() -> bool:
    """Return True if a routing table has been successfully loaded."""
    _load()
    return bool(_table)
