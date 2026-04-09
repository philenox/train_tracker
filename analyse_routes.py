#!/usr/bin/env python3
"""
analyse_routes.py — Build a berth routing and ETA lookup table from collected TD data.

For every berth seen in the Reading TD area, computes:
  - How many trains passed through it
  - What % of those went on to cross a visible berth (1757 WB or 1724 EB)
  - Average / std / min / max time from that berth to the visible crossing
  - Breakdown by destination

Also outputs a machine-readable JSON lookup table for use in the prediction engine.

Usage:
  venv/bin/python analyse_routes.py
  venv/bin/python analyse_routes.py --data data/   # directory with td_*.csv files
  venv/bin/python analyse_routes.py --min-samples 3
  venv/bin/python analyse_routes.py --out routing_table.json
"""

import argparse
import json
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
from tabulate import tabulate

import schedule_db

VISIBLE_WB = "1757"
VISIBLE_EB = "1724"
VISIBLE_BERTHS = {VISIBLE_WB, VISIBLE_EB}
RUN_GAP_SECS   = 3 * 3600   # gap between two appearances of same headcode = new run


# ── Data loading ──────────────────────────────────────────────────────────────

def load_td(data_dir: str) -> pd.DataFrame:
    paths = sorted(Path(data_dir).glob("td_*.csv"))
    if not paths:
        raise FileNotFoundError(f"No td_*.csv files found in {data_dir}")
    df = pd.concat([pd.read_csv(p) for p in paths], ignore_index=True)
    df["ts"] = pd.to_datetime(df["timestamp"])
    df = df.sort_values("ts").reset_index(drop=True)
    print(f"Loaded {len(df):,} TD rows from {len(paths)} file(s)  "
          f"({df['ts'].min().strftime('%Y-%m-%d %H:%M')} → "
          f"{df['ts'].max().strftime('%Y-%m-%d %H:%M')})")
    return df


def load_destinations(headcodes: set) -> dict:
    """Look up destination for each headcode from the schedule DB."""
    conn    = schedule_db.db_connect()
    dest_map = {}
    for hc in headcodes:
        # Try a range of recent dates in case data spans multiple days
        for offset in range(5):
            d = (date.today() - timedelta(days=offset)).isoformat()
            row = conn.execute(
                """
                SELECT s.uid, s.stp_indicator, s.start_date
                FROM schedules s
                WHERE s.headcode = ?
                  AND s.start_date <= ? AND s.end_date >= ?
                ORDER BY CASE s.stp_indicator WHEN 'N' THEN 0
                                              WHEN 'O' THEN 1
                                              WHEN 'P' THEN 2 ELSE 3 END
                LIMIT 1
                """,
                (hc, d, d),
            ).fetchone()
            if row:
                locs = conn.execute(
                    "SELECT tiploc, location_type FROM schedule_locations "
                    "WHERE uid=? AND stp_indicator=? AND start_date=? ORDER BY seq DESC LIMIT 5",
                    (row["uid"], row["stp_indicator"], row["start_date"]),
                ).fetchall()
                for loc in locs:
                    if loc["location_type"] in ("LT", "LO"):
                        dest_map[hc] = schedule_db.tiploc_name(conn, loc["tiploc"])
                        break
                if hc in dest_map:
                    break
    conn.close()
    return dest_map


# ── Run segmentation ──────────────────────────────────────────────────────────

def assign_runs(df: pd.DataFrame) -> pd.DataFrame:
    """
    Within each headcode, split on gaps > RUN_GAP_SECS to produce a unique
    run_id per distinct train journey.
    """
    df = df.sort_values(["headcode", "ts"]).copy()
    df["prev_ts"] = df.groupby("headcode")["ts"].shift(1)
    df["gap"]     = (df["ts"] - df["prev_ts"]).dt.total_seconds().fillna(0)
    df["new_run"] = (df["gap"] > RUN_GAP_SECS) | (df.groupby("headcode").cumcount() == 0)
    df["run_seq"] = df.groupby("headcode")["new_run"].cumsum()
    df["run_id"]  = df["headcode"] + "__" + df["run_seq"].astype(str)
    return df


# ── Core analysis ─────────────────────────────────────────────────────────────

def analyse(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Returns:
      berth_df   — per-(berth, direction) stats
      dest_df    — per-(berth, direction, destination) stats
    """
    df = assign_runs(df)

    # Find the first visible crossing per run (WB or EB)
    visible = df[df["to_berth"].isin(VISIBLE_BERTHS)].copy()
    visible["direction"] = visible["to_berth"].map(
        {VISIBLE_WB: "WB", VISIBLE_EB: "EB"}
    )
    # Keep only the first crossing per run (some trains hit both in the same run — unlikely but possible)
    visible_first = (
        visible.sort_values("ts")
               .drop_duplicates("run_id", keep="first")
        [["run_id", "ts", "direction"]]
        .rename(columns={"ts": "visible_ts", "direction": "direction"})
    )

    # Join crossing info back onto every row in those runs
    df = df.merge(visible_first, on="run_id", how="left")
    df["hit_visible"]    = df["visible_ts"].notna()
    df["secs_to_visible"] = (df["visible_ts"] - df["ts"]).dt.total_seconds()

    # For trains that hit visible: keep rows that occurred BEFORE crossing
    # For trains that didn't hit visible: keep all rows
    df_before = df[
        (~df["hit_visible"]) |
        (df["hit_visible"] & (df["secs_to_visible"] >= 0))
    ].copy()

    # Direction for non-visible trains: unknown ("??")
    df_before["direction"] = df_before["direction"].fillna("??")

    # Load destinations
    all_hc   = set(df_before["headcode"].unique())
    dest_map = load_destinations(all_hc)
    df_before["destination"] = df_before["headcode"].map(dest_map).fillna("(unknown)")

    # ── Per (berth, direction) stats ──────────────────────────────────────────
    def berth_agg(grp):
        runs     = grp["run_id"].unique()
        n_runs   = len(runs)
        vis_runs = grp.loc[grp["hit_visible"], "run_id"].unique()
        n_vis    = len(vis_runs)
        etas     = grp.loc[
            grp["hit_visible"] & grp["secs_to_visible"].notna() & (grp["secs_to_visible"] > 0),
            "secs_to_visible"
        ]
        return pd.Series({
            "n_trains":   n_runs,
            "n_visible":  n_vis,
            "p_visible":  n_vis / n_runs if n_runs else 0,
            "eta_mean":   etas.mean() if len(etas) else float("nan"),
            "eta_std":    etas.std()  if len(etas) > 1 else float("nan"),
            "eta_min":    etas.min()  if len(etas) else float("nan"),
            "eta_max":    etas.max()  if len(etas) else float("nan"),
        })

    berth_df = (
        df_before.groupby(["to_berth", "direction"])
                 .apply(berth_agg, include_groups=False)
                 .reset_index()
    )

    # ── Per (berth, direction, destination) stats ─────────────────────────────
    dest_df = (
        df_before.groupby(["to_berth", "direction", "destination"])
                 .apply(berth_agg, include_groups=False)
                 .reset_index()
    )

    return berth_df, dest_df, df_before


# ── Output helpers ────────────────────────────────────────────────────────────

def fmt_secs(s) -> str:
    if pd.isna(s):
        return "   —"
    m, sec = divmod(int(s), 60)
    return f"{m}m{sec:02d}s" if m else f"   {sec}s"


def print_berth_table(berth_df: pd.DataFrame, min_samples: int, direction_filter=None):
    df = berth_df.copy()
    if direction_filter:
        df = df[df["direction"] == direction_filter]
    df = df[df["n_trains"] >= min_samples]
    df = df.sort_values(["direction", "p_visible", "eta_mean"],
                        ascending=[True, False, True])

    rows = []
    for _, r in df.iterrows():
        rows.append([
            r["to_berth"],
            r["direction"],
            int(r["n_trains"]),
            int(r["n_visible"]),
            f"{r['p_visible']*100:.0f}%",
            fmt_secs(r["eta_mean"]),
            fmt_secs(r["eta_std"]),
            fmt_secs(r["eta_min"]),
            fmt_secs(r["eta_max"]),
        ])

    print(tabulate(rows,
        headers=["Berth", "Dir", "n_trains", "n_visible", "p_visible",
                 "eta_mean", "eta_std", "eta_min", "eta_max"],
        tablefmt="simple"))


def print_dest_table(dest_df: pd.DataFrame, berth: str, direction: str, min_samples: int):
    df = dest_df[
        (dest_df["to_berth"] == berth) &
        (dest_df["direction"] == direction) &
        (dest_df["n_trains"] >= min_samples)
    ].copy()
    if df.empty:
        print(f"  (no data for berth {berth} dir {direction} with ≥{min_samples} samples)")
        return

    df = df.sort_values("p_visible", ascending=False)
    rows = []
    for _, r in df.iterrows():
        rows.append([
            r["destination"][:35],
            int(r["n_trains"]),
            int(r["n_visible"]),
            f"{r['p_visible']*100:.0f}%",
            fmt_secs(r["eta_mean"]),
        ])
    print(tabulate(rows,
        headers=["Destination", "n", "n_vis", "p_vis", "eta_mean"],
        tablefmt="simple"))


def build_lookup_table(berth_df: pd.DataFrame, min_samples: int) -> dict:
    """
    Build a JSON-serialisable lookup table:
      { "BERTH__DIR": { p_visible, eta_mean, eta_std, n_trains } }

    Suitable for direct use in the prediction engine.
    """
    lookup = {}
    for _, r in berth_df.iterrows():
        if r["n_trains"] < min_samples:
            continue
        key = f"{r['to_berth']}__{r['direction']}"
        lookup[key] = {
            "berth":     r["to_berth"],
            "direction": r["direction"],
            "n_trains":  int(r["n_trains"]),
            "n_visible": int(r["n_visible"]),
            "p_visible": round(float(r["p_visible"]), 3),
            "eta_mean":  round(float(r["eta_mean"]), 1) if not pd.isna(r["eta_mean"]) else None,
            "eta_std":   round(float(r["eta_std"]),  1) if not pd.isna(r["eta_std"])  else None,
            "eta_min":   round(float(r["eta_min"]),  1) if not pd.isna(r["eta_min"])  else None,
            "eta_max":   round(float(r["eta_max"]),  1) if not pd.isna(r["eta_max"])  else None,
        }
    return lookup


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Berth routing + ETA analysis")
    parser.add_argument("--data",        default="data",          help="Directory with td_*.csv files")
    parser.add_argument("--min-samples", default=2, type=int,     help="Min trains per berth to include")
    parser.add_argument("--out",         default="routing_table.json", help="JSON output path")
    parser.add_argument("--dest-berths", default=None,            help="Comma-separated berths for destination breakdown")
    args = parser.parse_args()

    # ── Load & analyse ────────────────────────────────────────────────────────
    df = load_td(args.data)
    print("Analysing runs...")
    berth_df, dest_df, df_before = analyse(df)

    n_runs    = df_before["run_id"].nunique()
    n_visible = berth_df.loc[berth_df["to_berth"].isin(VISIBLE_BERTHS), "n_trains"].sum()
    print(f"Found {n_runs} distinct train runs, "
          f"{int(n_visible)} crossed a visible berth\n")

    # ── WB berth table ────────────────────────────────────────────────────────
    print("=" * 70)
    print(f"WESTBOUND — berths upstream of 1757  (≥{args.min_samples} samples)")
    print("=" * 70)
    print_berth_table(berth_df, args.min_samples, direction_filter="WB")

    print()
    print("=" * 70)
    print(f"EASTBOUND — berths upstream of 1724  (≥{args.min_samples} samples)")
    print("=" * 70)
    print_berth_table(berth_df, args.min_samples, direction_filter="EB")

    print()
    print("=" * 70)
    print(f"UNKNOWN DIRECTION — in Reading area but didn't reach a visible berth")
    print(f"(≥{args.min_samples} samples, top 30 by count)")
    print("=" * 70)
    df_unk = berth_df[berth_df["direction"] == "??"].copy()
    df_unk = df_unk[df_unk["n_trains"] >= args.min_samples].sort_values("n_trains", ascending=False).head(30)
    rows = [[r["to_berth"], int(r["n_trains"])] for _, r in df_unk.iterrows()]
    print(tabulate(rows, headers=["Berth", "n_trains"], tablefmt="simple"))

    # ── Destination breakdown for interesting berths ───────────────────────────
    # Auto-pick: WB and EB berths with 50–99% p_visible and ≥5 samples
    interesting = berth_df[
        (berth_df["p_visible"] > 0.4) &
        (berth_df["p_visible"] < 1.0) &
        (berth_df["n_trains"] >= 5) &
        (~berth_df["to_berth"].isin(VISIBLE_BERTHS))
        & (berth_df["direction"].isin(["WB", "EB"]))
    ]["to_berth"].tolist()

    # Also include any user-specified berths
    if args.dest_berths:
        interesting += [b.strip() for b in args.dest_berths.split(",")]
    interesting = sorted(set(interesting))

    if interesting:
        print()
        print("=" * 70)
        print("DESTINATION BREAKDOWN for ambiguous berths (40–99% visible)")
        print("These are where routing decisions happen — destination may predict pass/skip")
        print("=" * 70)
        for berth in interesting:
            for direction in ("WB", "EB"):
                sub = dest_df[
                    (dest_df["to_berth"] == berth) &
                    (dest_df["direction"] == direction) &
                    (dest_df["n_trains"] >= 1)
                ]
                if sub.empty:
                    continue
                total  = berth_df.loc[
                    (berth_df["to_berth"] == berth) & (berth_df["direction"] == direction),
                    "n_trains"
                ].sum()
                p = berth_df.loc[
                    (berth_df["to_berth"] == berth) & (berth_df["direction"] == direction),
                    "p_visible"
                ].values
                p_str = f"{p[0]*100:.0f}%" if len(p) else "?"
                print(f"\nBerth {berth} ({direction}, {p_str} visible, n={int(total)}):")
                print_dest_table(dest_df, berth, direction, min_samples=1)

    # ── JSON lookup table ─────────────────────────────────────────────────────
    lookup = build_lookup_table(berth_df, args.min_samples)
    with open(args.out, "w") as f:
        json.dump(lookup, f, indent=2)
    print(f"\nLookup table ({len(lookup)} entries) written to {args.out}")

    # ── Quick summary: best upstream indicators ───────────────────────────────
    print()
    print("=" * 70)
    print("KEY UPSTREAM BERTHS  (p_visible ≥ 90%, ≥5 samples, not the visible berth itself)")
    print("These are your best early-warning signals")
    print("=" * 70)
    top = berth_df[
        (berth_df["p_visible"] >= 0.9) &
        (berth_df["n_trains"] >= 5) &
        (~berth_df["to_berth"].isin(VISIBLE_BERTHS)) &
        (berth_df["direction"].isin(["WB", "EB"]))
    ].sort_values(["direction", "eta_mean"], ascending=[True, False])

    rows = []
    for _, r in top.iterrows():
        rows.append([
            r["to_berth"],
            r["direction"],
            int(r["n_trains"]),
            f"{r['p_visible']*100:.0f}%",
            fmt_secs(r["eta_mean"]),
            fmt_secs(r["eta_std"]),
        ])
    print(tabulate(rows,
        headers=["Berth", "Dir", "n", "p_vis", "eta_mean", "eta_std"],
        tablefmt="simple"))


if __name__ == "__main__":
    main()
