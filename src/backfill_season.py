#!/usr/bin/env python3
"""
Backfill any past MLB season's pitch-by-pitch data into this repo.

Generalizes src/backfill_2025.py: same live-feed source, same extraction
functions, identical schema — just parameterized by season so 2022, 2023,
2024 (and re-runs of 2025) all flow through one script.

Usage (from repo root):
    python src/backfill_season.py --season 2024
    python src/backfill_season.py --season 2023 --monthly-only
    python src/backfill_season.py --season 2022 --refresh

Writes:
    data/raw/{season}/daily/YYYY-MM-DD.parquet     (unless --monthly-only)
    data/raw/{season}/monthly/MM_monthname.parquet

Flags:
    --monthly-only  Build dailies as temp intermediates, rebuild monthlies,
                    then delete the dailies before exit. Halves the on-disk
                    footprint for archive seasons (~100MB instead of ~212MB).
                    Note: re-runs can't resume day-by-day without dailies.
    --refresh       Re-fetch days whose daily file already exists.

Season windows below are the regular-season bounds (international openers
included). The schedule endpoint + gameType == 'R' filter is the real gate,
so the bounds just limit how many empty dates we poll.
"""

import argparse
import shutil
import sys
from datetime import date, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

# Reuse the repo's own extraction so the schema is byte-for-byte identical.
sys.path.insert(0, str(Path(__file__).resolve().parent))
from fetch_pitches import get_schedule, get_pitch_data, MONTH_NAMES  # noqa: E402

SEASON_WINDOWS = {
    2022: (date(2022, 4, 7), date(2022, 10, 5)),    # lockout-delayed opener; season ran long
    2023: (date(2023, 3, 30), date(2023, 10, 1)),
    2024: (date(2024, 3, 20), date(2024, 9, 30)),   # Seoul Series opener Mar 20
    2025: (date(2025, 3, 18), date(2025, 9, 28)),   # Tokyo Series opener Mar 18
}


def daterange(d0, d1):
    d = d0
    while d <= d1:
        yield d
        d += timedelta(days=1)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--season", type=int, required=True,
                    help="season to backfill, e.g. 2024")
    ap.add_argument("--refresh", action="store_true",
                    help="re-fetch days that already have a daily file")
    ap.add_argument("--monthly-only", action="store_true",
                    help="delete daily files after rebuilding monthlies")
    args = ap.parse_args()

    season = args.season
    if season in SEASON_WINDOWS:
        start, end = SEASON_WINDOWS[season]
    else:
        # Generic fallback window; gameType filter keeps it safe.
        start, end = date(season, 3, 15), date(season, 10, 10)
        print(f"WARNING: no preset window for {season}; "
              f"polling {start} - {end} (gameType=R filter still applies)")

    raw = Path("data") / "raw" / str(season)
    daily_dir = raw / "daily"
    monthly_dir = raw / "monthly"
    daily_dir.mkdir(parents=True, exist_ok=True)
    monthly_dir.mkdir(parents=True, exist_ok=True)

    print(f"Backfilling {season}: {start} - {end}")
    total_rows = 0
    for d in daterange(start, end):
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

    if args.monthly_only:
        shutil.rmtree(daily_dir)
        print("Removed daily files (--monthly-only).")

    print(f"\nDone. {total_rows:,} new pitches written for {season}.")


if __name__ == "__main__":
    main()
