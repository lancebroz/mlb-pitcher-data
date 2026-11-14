#!/usr/bin/env python3
"""One-off check of the base/out and count extraction on a sample of dates (branch only, not part of the pipeline)."""
import gzip, json, sys
from pathlib import Path
import requests
import pyarrow as pa, pyarrow.parquet as pq
sys.path.insert(0, str(Path(__file__).resolve().parent))
from fetch_pitches import get_schedule, get_pitch_data  # noqa: E402

DATES = ['2026-03-28', '2026-04-05', '2026-04-18', '2026-07-20', '2026-08-09', '2026-09-02']
KEEP = ['game_pk', 'game_date', 'at_bat_number', 'pitch_number', 'inning', 'half_inning', 'balls', 'strikes', 'count',
        'outs', 'on_1b', 'on_2b', 'on_3b', 'call_description', 'events', 'pitcher_id', 'batter_id', 'pitch_type']
out = Path('validation'); out.mkdir(exist_ok=True)
rows, feeds = [], 0
for d in DATES:
    for gid in get_schedule(d):
        p = get_pitch_data(gid)
        rows.extend({k: r.get(k) for k in KEEP} | {'fetch_date': d} for r in p)
        extra = any((r.get('inning') or 0) >= 10 for r in p)
        if feeds < 6 and (extra or feeds < 2):
            feed = requests.get(f"https://statsapi.mlb.com/api/v1.1/game/{gid}/feed/live", timeout=30).json()
            plays = feed.get('liveData', {}).get('plays', {}).get('allPlays', [])
            for pl in plays:
                for ev in pl.get('playEvents', []):
                    ev.pop('pitchData', None); ev.pop('hitData', None)
            with gzip.open(out / f'feed_{gid}.json.gz', 'wt') as f:
                json.dump({'gamePk': gid, 'allPlays': plays}, f)
            feeds += 1
    print(d, len(rows))
pq.write_table(pa.Table.from_pylist(rows), out / 'pitches.parquet')
print('wrote', len(rows), 'pitches,', feeds, 'feeds')
