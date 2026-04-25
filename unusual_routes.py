#!/usr/bin/env python3
"""
unusual_routes.py — Analyse train routes that crossed the visible window.

For every headcode seen at berth 1757 (WB) or 1724 (EB), looks up the full
CIF schedule to find origin, destination, operator, and calling points.
Ranks routes by how unusual/infrequent they are and prints a report.

Usage:
  venv/bin/python unusual_routes.py
  venv/bin/python unusual_routes.py --min-crossings 1   # include one-off sightings
  venv/bin/python unusual_routes.py --top 30            # show more routes
"""

import argparse
import collections
import csv
import glob
import sqlite3
from datetime import date, timedelta
from pathlib import Path

import schedule_db

VISIBLE_WB = "1757"
VISIBLE_EB = "1724"

# Operators that run scheduled passenger/freight services through Reading
ATOC_NAMES = {
    "GW": "GWR",
    "XC": "CrossCountry",
    "XR": "Elizabeth line",
    "HX": "Heathrow Express",
    "SW": "South Western",
    "EM": "East Midlands",
    "VT": "Avanti",
    "LM": "West Midlands",
    "TP": "TransPennine",
    "AW": "Avanti Wales",
    "WR": "West Midlands",
    "CS": "Caledonian",
    "ZZ": "Freight/Departmental",
    "??": "Unknown",
}


# ── Data loading ──────────────────────────────────────────────────────────────

def load_crossings(data_dir: str) -> list[dict]:
    """Return one dict per visible-berth crossing from all TD CSV files."""
    crossings = []
    for path in sorted(Path(data_dir).glob("td_*.csv")):
        date_str = path.stem.replace("td_", "")
        with open(path) as fh:
            for row in csv.DictReader(fh):
                berth = row.get("to_berth", "")
                hc    = row.get("headcode", "").strip()
                if berth in (VISIBLE_WB, VISIBLE_EB) and hc:
                    crossings.append({
                        "headcode":  hc,
                        "direction": "WB" if berth == VISIBLE_WB else "EB",
                        "date":      date_str,
                        "timestamp": row.get("timestamp", ""),
                    })
    return crossings


# ── Schedule lookup ───────────────────────────────────────────────────────────

def _find_schedule(conn, headcode: str, for_date: str) -> sqlite3.Row | None:
    """Find the highest-priority active schedule for headcode on for_date."""
    d = date.fromisoformat(for_date)
    day_idx = d.weekday()
    return conn.execute(
        """
        SELECT s.* FROM schedules s
        WHERE s.headcode    =  ?
          AND s.start_date  <= ?
          AND s.end_date    >= ?
          AND substr(s.days_runs, ?, 1) = '1'
          AND s.stp_indicator != 'C'
          AND s.stp_indicator = (
              SELECT MIN(s2.stp_indicator) FROM schedules s2
              WHERE s2.uid            = s.uid
                AND s2.headcode       = s.headcode
                AND s2.start_date    <= ?
                AND s2.end_date      >= ?
                AND substr(s2.days_runs, ?, 1) = '1'
                AND s2.stp_indicator != 'C'
          )
        LIMIT 1
        """,
        (headcode,
         for_date, for_date, day_idx + 1,
         for_date, for_date, day_idx + 1),
    ).fetchone()


def enrich_crossings(crossings: list[dict]) -> list[dict]:
    """
    Add origin, destination, operator, and calling-point list to each crossing.
    Tries the crossing date first, then ±3 days for weekend/holiday schedules.
    """
    conn = schedule_db.db_connect()
    enriched = []

    for c in crossings:
        sched = None
        # Try the crossing date and a few days either side
        d0 = date.fromisoformat(c["date"])
        for delta in [0, -1, 1, -2, 2, -3, 3]:
            d = (d0 + timedelta(days=delta)).isoformat()
            sched = _find_schedule(conn, c["headcode"], d)
            if sched:
                break

        if not sched:
            enriched.append({**c, "origin": "?", "destination": "?",
                              "atoc_code": "??", "uid": None,
                              "calling_points": [], "stp": "?"})
            continue

        locs = conn.execute(
            "SELECT tiploc, location_type, arrival, departure, pass_time "
            "FROM schedule_locations "
            "WHERE uid=? AND stp_indicator=? AND start_date=? ORDER BY seq",
            (sched["uid"], sched["stp_indicator"], sched["start_date"]),
        ).fetchall()

        origin_tip = next(
            (l["tiploc"] for l in locs if l["location_type"] == "LO"), None
        )
        dest_tip = next(
            (l["tiploc"] for l in reversed(locs) if l["location_type"] == "LT"), None
        )

        # Calling points = locations with a scheduled stop (arrival or departure)
        calling = [
            l["tiploc"] for l in locs
            if l["arrival"] or l["departure"]
        ]

        enriched.append({
            **c,
            "origin":         schedule_db.tiploc_name(conn, origin_tip) if origin_tip else "?",
            "destination":    schedule_db.tiploc_name(conn, dest_tip)   if dest_tip   else "?",
            "origin_tiploc":  origin_tip or "?",
            "dest_tiploc":    dest_tip   or "?",
            "atoc_code":      sched["atoc_code"] or "??",
            "uid":            sched["uid"],
            "stp":            sched["stp_indicator"],
            "calling_points": calling,
        })

    conn.close()
    return enriched


# ── Route analysis ────────────────────────────────────────────────────────────

def build_route_summary(enriched: list[dict]) -> list[dict]:
    """
    Group crossings by (headcode, origin, destination, direction) and compute
    a per-route summary with crossing count, operator, and calling points.
    """
    groups: dict[tuple, dict] = {}

    for c in enriched:
        key = (c["headcode"], c["origin"], c["destination"], c["direction"])
        if key not in groups:
            groups[key] = {
                "headcode":    c["headcode"],
                "direction":   c["direction"],
                "origin":      c["origin"],
                "destination": c["destination"],
                "atoc_code":   c["atoc_code"],
                "operator":    ATOC_NAMES.get(c["atoc_code"], c["atoc_code"]),
                "stp":         c["stp"],
                "calling_points": c["calling_points"],
                "dates":       [],
                "n":           0,
            }
        groups[key]["n"] += 1
        groups[key]["dates"].append(c["date"])

    return sorted(groups.values(), key=lambda r: r["n"])


def headcode_first_digit_class(hc: str) -> str:
    if not hc:
        return "?"
    d = hc[0]
    return {
        "0": "light engine",
        "1": "passenger",
        "2": "local passenger",
        "3": "postal/parcels",
        "4": "express freight",
        "5": "empty coaching stock",
        "6": "ordinary freight",
        "7": "light freight",
        "8": "departmental",
        "9": "special",
    }.get(d, f"({d}?)")


# ── Display ───────────────────────────────────────────────────────────────────

def fmt_route(r: dict, show_calling: bool = True) -> str:
    """Format a route summary as a multi-line string."""
    hc_class = headcode_first_digit_class(r["headcode"])
    dates_str = ", ".join(sorted(set(r["dates"]))[:5])
    if len(set(r["dates"])) > 5:
        dates_str += f" … +{len(set(r['dates']))-5} more"

    lines = [
        f"  {r['headcode']}  {r['direction']}  {r['origin']}  →  {r['destination']}",
        f"    operator: {r['operator']} ({r['atoc_code']})  |  type: {hc_class}  |  seen: {r['n']}×  |  dates: {dates_str}",
    ]

    if show_calling and r["calling_points"]:
        conn = schedule_db.db_connect()
        names = []
        for tip in r["calling_points"]:
            name = schedule_db.tiploc_name(conn, tip)
            if name and name not in (r["origin"], r["destination"]):
                names.append(name)
        conn.close()
        # Deduplicate while preserving order
        seen = set()
        unique_names = []
        for n in names:
            if n not in seen:
                seen.add(n)
                unique_names.append(n)
        if unique_names:
            route_str = " → ".join(unique_names)
            if len(route_str) > 90:
                route_str = route_str[:87] + "…"
            lines.append(f"    calls at: {route_str}")

    return "\n".join(lines)


def print_section(title: str, routes: list[dict], show_calling: bool = True):
    print()
    print("=" * 72)
    print(title)
    print("=" * 72)
    if not routes:
        print("  (none)")
        return
    for r in routes:
        print(fmt_route(r, show_calling=show_calling))
        print()


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Unusual route analysis")
    parser.add_argument("--data",         default="data", help="TD CSV directory")
    parser.add_argument("--top",          default=20, type=int,
                        help="Max routes to show per section")
    parser.add_argument("--min-crossings",default=1, type=int,
                        help="Min crossings to include a headcode (default 1)")
    args = parser.parse_args()

    print("Loading TD crossings from visible berths…")
    crossings = load_crossings(args.data)
    print(f"  {len(crossings)} crossings, {len({c['headcode'] for c in crossings})} unique headcodes")

    print("Enriching with CIF schedule data…")
    enriched = enrich_crossings(crossings)
    not_found = sum(1 for c in enriched if c["uid"] is None)
    print(f"  {not_found} headcodes not found in schedule DB")

    routes = build_route_summary(enriched)
    routes = [r for r in routes if r["n"] >= args.min_crossings]

    # ── Section 1: one-off sightings ──────────────────────────────────────────
    one_offs = [r for r in routes if r["n"] == 1]
    print_section(f"ONE-OFF SIGHTINGS  ({len(one_offs)} routes seen exactly once)", one_offs[:args.top])

    # ── Section 2: infrequent but recurring ───────────────────────────────────
    infrequent = [r for r in routes if 2 <= r["n"] <= 3]
    print_section(f"INFREQUENT ROUTES  ({len(infrequent)} routes seen 2–3 times)", infrequent[:args.top])

    # ── Section 3: freight and special services ───────────────────────────────
    freight_routes = [
        r for r in routes
        if r["headcode"][0] in "046" or r["atoc_code"] == "ZZ"
    ]
    print_section(
        f"FREIGHT & SPECIAL SERVICES  ({len(freight_routes)} routes)",
        sorted(freight_routes, key=lambda r: -r["n"])[:args.top],
    )

    # ── Section 4: ECS / light engine moves ───────────────────────────────────
    ecs_routes = [r for r in routes if r["headcode"][0] in "05"]
    print_section(
        f"EMPTY STOCK & LIGHT ENGINE MOVES  ({len(ecs_routes)} routes)",
        sorted(ecs_routes, key=lambda r: -r["n"])[:args.top],
    )

    # ── Section 5: unusual passenger routes ───────────────────────────────────
    # "Unusual" = passenger, appears ≤3 times, or unusual origin/destination
    common_origins = {
        "London Paddington", "Bristol Temple Meads", "Cardiff Central",
        "Swansea", "Oxford", "Weston-super-Mare", "Penzance", "Plymouth",
        "Exeter St Davids", "Bristol Parkway", "Cheltenham Spa", "Gloucester",
        "Newbury", "Reading",
    }
    unusual_pax = [
        r for r in routes
        if r["headcode"][0] in "12"
        and (r["n"] <= 3
             or r["origin"] not in common_origins
             and r["destination"] not in common_origins)
    ]
    print_section(
        f"UNUSUAL PASSENGER ROUTES  ({len(unusual_pax)} routes)",
        sorted(unusual_pax, key=lambda r: r["n"])[:args.top],
    )

    # ── Section 6: most common routes (for context) ───────────────────────────
    common = sorted(
        [r for r in routes if r["headcode"][0] in "12"],
        key=lambda r: -r["n"],
    )
    print_section(
        f"MOST COMMON PASSENGER ROUTES  (top {args.top} by frequency)",
        common[:args.top],
        show_calling=False,
    )

    # ── Summary stats ─────────────────────────────────────────────────────────
    print()
    print("=" * 72)
    print("SUMMARY")
    print("=" * 72)
    total_crossings = sum(r["n"] for r in routes)
    by_dir = collections.Counter(c["direction"] for c in enriched)
    by_atoc = collections.Counter(r["atoc_code"] for r in routes for _ in range(r["n"]))
    print(f"  Total visible crossings: {total_crossings}")
    print(f"  WB (→ Tilehurst): {by_dir['WB']},  EB (→ Reading): {by_dir['EB']}")
    print(f"  Unique routes: {len(routes)}")
    print()
    print("  Crossings by operator:")
    for atoc, n in by_atoc.most_common():
        print(f"    {ATOC_NAMES.get(atoc, atoc):25s} {n:4d}  ({100*n//total_crossings}%)")


if __name__ == "__main__":
    main()
