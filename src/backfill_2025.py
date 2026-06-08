#!/usr/bin/env python3
"""
Backfill 2025 MLB pitch-by-pitch data into the mlb-pitcher-data repo.

This reuses the EXACT extraction logic of the repo's own src/fetch_pitches.py
(live-feed source, identical schema) but iterates the entire 2025 regular season
so the output slots directly alongside data/raw/2026/ with no schema differences.

Run it once from the repo root:
    python src/backfill_2025.py
It writes:
    data/raw/2025/daily/YYYY-MM-DD.parquet
    data/raw/2025/monthly/MM_monthname.parquet

Notes:
- 2025 regular season ran Mar 27 - Sep 28 (plus the Mar 18-19 Tokyo Series).
  The schedule endpoint is the source of truth; we just iterate dates and keep
  whatever Final regular-season (gameType R) games exist.
- This is the same public statsapi live feed the repo already uses; no auth.
- Safe to re-run: existing daily files are skipped unless --refresh is passed.
"""

import argparse
import sys
from datetime import date, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

# Reuse the repo's own extraction so the schema is byte-for-byte identical.
sys.path.insert(0, str(Path(__file__).resolve().parent))
from fetch_pitches import get_schedule, get_pitch_data, MONTH_NAMES  # noqa: E402

SEASON = 2025
SEASON_START = date(2025, 3, 18)   # Tokyo Series opener
SEASON_END = date(2025, 9, 28)     # regular-season finale


def daterange(d0, d1):
    d = d0
    while d <= d1:
        yield d
        d += timedelta(days=1)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--refresh", action="store_true",
                    help="re-fetch days that already have a daily file")
    args = ap.parse_args()

    raw = Path("data") / "raw" / str(SEASON)
    daily_dir = raw / "daily"
    monthly_dir = raw / "monthly"
    daily_dir.mkdir(parents=True, exist_ok=True)
    monthly_dir.mkdir(parents=True, exist_ok=True)

    total_rows = 0
    for d in daterange(SEASON_START, SEASON_END):
        ds = d.isoformat()
        out = daily_dir / f"{ds}.parquet"
        if out.exists() and not args.refresh:
            continue

        game_ids = get_schedule(ds)
        if not game_ids:
            continue

        day_rows = []
        for gid in game_ids:
            day_rows.extend(get_pitch_data(gid))

        if not day_rows:
            continue

        table = pa.Table.from_pylist(day_rows)
        pq.write_table(table, out)
        total_rows += len(day_rows)
        print(f"  {ds}: {len(game_ids)} games, {len(day_rows):,} pitches")

    # Rebuild monthly files from dailies
    print("\nRebuilding monthly files...")
    by_month = {}
    for f in sorted(daily_dir.glob("*.parquet")):
        mo = int(f.stem[5:7])
        by_month.setdefault(mo, []).append(f)
    for mo, files in sorted(by_month.items()):
        tables = [pq.read_table(f) for f in files]
        combined = pa.concat_tables(tables, promote_options="default")
        name = f"{mo:02d}_{MONTH_NAMES[mo].lower()}.parquet"
        pq.write_table(combined, monthly_dir / name)
        print(f"  {name}: {combined.num_rows:,} pitches")

    print(f"\nDone. {total_rows:,} new pitches written for {SEASON}.")


if __name__ == "__main__":
    main()
