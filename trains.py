#!/usr/bin/env python3
"""
List the next trains predicted to pass berth 1757 (just west of Reading station).

Uses Darwin GetDepBoardWithDetails for all Reading departures, then filters to
westbound services only (excluding trains heading east towards London or south
towards Basingstoke/Wokingham).
"""

import os
import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

DEPARTURES_URL = "https://api1.raildata.org.uk/1010-live-departure-board-dep1_2/LDBWS/api/20220120/GetDepBoardWithDetails/{crs}"

READING_CRS = "RDG"
NUM_TRAINS = 3
BERTH_1757_OFFSET_MINS = 2  # estimated minutes from Reading departure to passing berth 1757

# First calling points that indicate a train is NOT heading west over the overpass.
# GWR eastbound (towards London Paddington):
#   Twyford, Maidenhead, Taplow, Burnham, Slough, Langley, Iver, West Drayton,
#   Hayes & Harlington, Southall, Ealing Broadway, West Ealing, Acton Main Line,
#   London Paddington
# SWR southbound (towards Basingstoke/Waterloo):
#   Earley, Winnersh, Winnersh Triangle, Wokingham, and beyond
NON_WESTBOUND_CRS = {
    # GWR eastbound (towards London Paddington)
    "TWY", "MAI", "TAP", "BNM", "SLO", "LNG", "IVR", "WDT",
    "HAY", "STL", "EAL", "WEA", "AML", "PAD",
    # SWR southbound (towards Basingstoke/Waterloo via Wokingham)
    "EAR", "WNS", "WTI", "WKM",
    # GWR/SWR southbound (towards Basingstoke via Reading West, or direct)
    "RDW", "BSK",
}

# Destinations that are unambiguously east/south of Reading — used as a
# secondary check to catch fast services that skip stations in NON_WESTBOUND_CRS.
NON_WESTBOUND_DESTINATIONS = {
    # Elizabeth line eastern destinations
    "ABW", "WWC", "SHF", "WOH", "CTM", "BKH", "NWX", "ELW", "PLU", "WLW",
    "CWH", "WWD", "CUS", "WHC", "STP", "FAR", "TCR", "BON", "LST", "LVS",
    "SHA", "IFD", "BRX", "SYD", "ELT", "WLN", "AGT",
    # London terminals
    "PAD", "WAT", "VIC", "CHX", "BFR", "LBG",
}


def get_departures(api_key: str, num_rows: int = 10) -> list:
    url = DEPARTURES_URL.format(crs=READING_CRS)
    response = requests.get(
        url,
        headers={"x-apikey": api_key, "User-Agent": "train-tracker/1.0"},
        params={"numRows": num_rows},
    )
    response.raise_for_status()
    return response.json().get("trainServices", []) or []


def is_westbound(service: dict) -> bool:
    """Return True if the service heads west over the Reading overpass."""
    # Check destination
    dest_crs = service.get("destination", [{}])[0].get("crs", "")
    if dest_crs in NON_WESTBOUND_DESTINATIONS:
        return False

    # Check first subsequent calling point
    calling_point_lists = service.get("subsequentCallingPoints", [])
    if not calling_point_lists:
        return False
    first_points = calling_point_lists[0].get("callingPoint", [])
    if not first_points:
        return False
    first_crs = first_points[0].get("crs", "")
    return first_crs not in NON_WESTBOUND_CRS


def effective_time(service: dict) -> datetime | None:
    """Return the best estimate of actual departure time as a datetime."""
    etd = service.get("etd", "")
    std = service.get("std", "")
    time_str = etd if (etd and etd not in ("On time", "Cancelled", "Delayed")) else std
    if not time_str:
        return None
    try:
        t = datetime.strptime(time_str, "%H:%M")
        now = datetime.now()
        return t.replace(year=now.year, month=now.month, day=now.day)
    except ValueError:
        return None


def format_departure(service: dict) -> str:
    std = service.get("std", "??:??")
    etd = service.get("etd", "")
    if etd and etd not in ("On time", "Cancelled", "Delayed"):
        return f"{std} (exp {etd})"
    if etd in ("Cancelled", "Delayed"):
        return f"{std} [{etd}]"
    return std


def get_westbound_departures(api_key: str) -> list:
    services = get_departures(api_key)
    return [s for s in services if s.get("etd") != "Cancelled" and is_westbound(s)]


def main():
    api_key = os.environ.get("LDBWS_CONSUMER_KEY")
    if not api_key:
        print("Error: set LDBWS_CONSUMER_KEY in .env")
        return

    print("Fetching departures...", end=" ", flush=True)
    services = get_westbound_departures(api_key)
    print("OK\n")

    if not services:
        print("No westbound services found.")
        return

    now = datetime.now()
    print(f"Next {NUM_TRAINS} trains predicted to pass berth 1757")
    print(f"(~{BERTH_1757_OFFSET_MINS} min west of Reading, as of {now.strftime('%H:%M:%S')})\n")
    print(f"  {'Departs RDG':<14} {'At berth 1757':<16} {'Destination':<28} {'Plat'}")
    print(f"  {'-'*14} {'-'*16} {'-'*28} {'-'*4}")

    for svc in services[:NUM_TRAINS]:
        dep_str = format_departure(svc)
        dep_time = effective_time(svc)
        berth_time = (dep_time + timedelta(minutes=BERTH_1757_OFFSET_MINS)).strftime("%H:%M") if dep_time else "?"
        destination = svc.get("destination", [{}])[0].get("locationName", "Unknown")
        platform = svc.get("platform") or "-"
        print(f"  {dep_str:<14} {berth_time:<16} {destination:<28} {platform}")

    print(f"\nAs of {now.strftime('%H:%M:%S')}")


if __name__ == "__main__":
    main()
