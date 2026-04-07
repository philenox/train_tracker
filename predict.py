"""
predict.py — Schedule-first prediction engine.

Queries today's CIF schedules for trains physically passing through the
visible berths west of Reading (between Reading station and Tilehurst),
computes berth ETA from scheduled Reading time, and returns them sorted
by ETA.

Offsets measured empirically from td_data.csv vs CIF schedule:
  WB: berth 1757 ≈ Reading departure + 70s
  EB: berth 1724 ≈ Reading arrival  − 210s
"""

import sqlite3
from datetime import datetime, date, timedelta

import schedule_db

# Measured offsets (seconds)
WB_OFFSET_SECS =  70
EB_OFFSET_SECS = 210   # subtracted from Reading arrival

# Any schedule that contains one of these is on the main line west of Reading
WEST_TIPLOCS = {"GORASTR", "TILHRST", "RDNGMLW", "PANGBRN", "TILHEJN"}

# Passenger operators only — exclude freight/departmental (ZZ, etc.)
PASSENGER_ATOC = {"GW", "XC", "XR", "SW", "HX", "LS", "WR", "AW", "EM", "VT", "LM", "NT", "TP", "CS"}


def _parse_cif_time(t_str, ref_date: date) -> datetime | None:
    """Parse HHMM or HHMMH CIF time string into a datetime on ref_date."""
    if not t_str:
        return None
    t = t_str.replace("H", "").strip()
    if len(t) != 4:
        return None
    try:
        hh, mm = int(t[:2]), int(t[2:])
        return datetime(ref_date.year, ref_date.month, ref_date.day, hh, mm)
    except ValueError:
        return None


def _get_locations(conn, uid, stp, start_date):
    return conn.execute(
        "SELECT * FROM schedule_locations WHERE uid=? AND stp_indicator=? AND start_date=? ORDER BY seq",
        (uid, stp, start_date),
    ).fetchall()


def _get_direction(locs) -> str | None:
    """Return 'WB' or 'EB' based on position of RDNGSTN vs west-of-Reading TIPLOCs."""
    tiplocs = [l["tiploc"] for l in locs]
    rdng_idx  = next((i for i, t in enumerate(tiplocs) if t == "RDNGSTN"), None)
    west_idx  = next((i for i, t in enumerate(tiplocs) if t in WEST_TIPLOCS), None)
    if rdng_idx is None or west_idx is None:
        return None
    return "WB" if rdng_idx < west_idx else "EB"


def _get_reading_loc(locs, direction: str):
    """Return the schedule_location row for RDNGSTN."""
    for loc in locs:
        if loc["tiploc"] == "RDNGSTN":
            return loc
    return None


def _get_terminus(locs):
    """Return the TIPLOC code of the last scheduled stop."""
    for loc in reversed(locs):
        if loc["location_type"] in ("LT", "LO"):
            return loc["tiploc"]
    return locs[-1]["tiploc"] if locs else None


def get_upcoming(n: int = 6, lookahead_mins: int = 120) -> list[dict]:
    """
    Return up to n upcoming trains sorted by berth ETA.

    Each entry is a dict with:
      direction       'WB' or 'EB'
      headcode        e.g. '1G33'
      eta             datetime of predicted berth pass
      destination     human-readable station name
      sched_reading   scheduled Reading time string (HHMM)
      atoc_code       operator
      uid             CIF train UID
    """
    conn    = schedule_db.db_connect()
    now     = datetime.now()
    today   = now.date()
    day_idx = today.weekday()          # 0=Mon … 6=Sun
    cutoff  = now + timedelta(minutes=lookahead_mins)

    # Fetch active schedules for today, applying STP priority (N > O > P).
    # For each UID, only the highest-priority non-cancelled schedule is used.
    # A C record for a UID on this date cancels the entire service.
    schedules = conn.execute(
        """
        SELECT s.* FROM schedules s
        WHERE s.start_date <= ?
          AND s.end_date   >= ?
          AND substr(s.days_runs, ?, 1) = '1'
          AND s.stp_indicator != 'C'
          AND s.stp_indicator = (
              SELECT MIN(s2.stp_indicator)
              FROM schedules s2
              WHERE s2.uid          = s.uid
                AND s2.start_date  <= ?
                AND s2.end_date    >= ?
                AND substr(s2.days_runs, ?, 1) = '1'
                AND s2.stp_indicator != 'C'
          )
          AND NOT EXISTS (
              SELECT 1 FROM schedules sc
              WHERE sc.uid           = s.uid
                AND sc.stp_indicator = 'C'
                AND sc.start_date   <= ?
                AND sc.end_date     >= ?
          )
        """,
        (today.isoformat(), today.isoformat(), day_idx + 1,
         today.isoformat(), today.isoformat(), day_idx + 1,
         today.isoformat(), today.isoformat()),
    ).fetchall()

    results = []

    for s in schedules:
        # Passenger services only
        if s["atoc_code"] not in PASSENGER_ATOC:
            continue

        locs = _get_locations(conn, s["uid"], s["stp_indicator"], s["start_date"])
        tiploc_set = {l["tiploc"] for l in locs}

        # Must pass through Reading AND through the main line west of Reading
        if "RDNGSTN" not in tiploc_set or not (tiploc_set & WEST_TIPLOCS):
            continue

        direction = _get_direction(locs)
        if not direction:
            continue

        rdng_loc = _get_reading_loc(locs, direction)
        if not rdng_loc:
            continue

        if direction == "WB":
            t_str = rdng_loc["departure"] or rdng_loc["pass_time"]
            offset = timedelta(seconds=WB_OFFSET_SECS)
        else:
            t_str = rdng_loc["arrival"] or rdng_loc["pass_time"]
            offset = timedelta(seconds=-EB_OFFSET_SECS)

        sched_dt = _parse_cif_time(t_str, today)
        if not sched_dt:
            continue

        eta = sched_dt + offset

        # Handle overnight: if ETA is more than 12h in the past, it's tomorrow's service
        if (now - eta).total_seconds() > 12 * 3600:
            eta        += timedelta(days=1)
            sched_dt   += timedelta(days=1)

        # Only include trains within our window
        if eta < now - timedelta(minutes=1) or eta > cutoff:
            continue

        terminus = _get_terminus(locs)
        destination = schedule_db.tiploc_name(conn, terminus) if terminus else terminus or "?"

        results.append({
            "direction":    direction,
            "headcode":     s["headcode"] or "????",
            "eta":          eta,
            "destination":  destination,
            "sched_reading": t_str or "????",
            "atoc_code":    s["atoc_code"],
            "uid":          s["uid"],
        })

    conn.close()

    # Deduplicate by (headcode, ETA minute) — same physical train can appear
    # in multiple overlapping CIF schedule records
    seen = set()
    unique = []
    for r in sorted(results, key=lambda x: x["eta"]):
        key = (r["headcode"], r["eta"].replace(second=0, microsecond=0))
        if key not in seen:
            seen.add(key)
            unique.append(r)

    return unique[:n]


if __name__ == "__main__":
    print(f"Upcoming trains (next 2 hours):\n")
    trains = get_upcoming(n=10)
    if not trains:
        print("  None found.")
    else:
        print(f"  {'Dir'}  {'ETA':>8}  {'Sched':>5}  {'Headcode'}  {'Destination'}")
        print("  " + "-" * 60)
        for t in trains:
            mins = int((t["eta"] - datetime.now()).total_seconds() / 60)
            print(
                f"  {t['direction']}   {t['eta'].strftime('%H:%M:%S')}  "
                f"{t['sched_reading']:>5}  {t['headcode']:>4}      {t['destination']}"
            )
