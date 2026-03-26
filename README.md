# MLB Pitch Data Repository

Automated collection of MLB pitch-by-pitch data from the Stats API live feed. Updated 6 times daily during the season.

## 📊 Data Available

### Raw Data (`data/raw/2026/`)
Full pitch-level Statcast-equivalent data in Parquet format, partitioned by month:
- `03_march.parquet`
- `04_april.parquet`
- `05_may.parquet`
- ... etc.

**Variables included per pitch:**

| Category | Fields |
|----------|--------|
| **Game Context** | game_pk, game_date, home_team, away_team, venue, inning, top_bottom |
| **Matchup** | pitcher_id, pitcher_name, pitcher_hand, batter_id, batter_name, batter_hand |
| **Count** | balls, strikes, outs, at_bat_number, pitch_number |
| **Pitch Type** | pitch_type (code), pitch_name |
| **Velocity** | start_speed, end_speed |
| **Location** | plate_x, plate_z, zone, sz_top, sz_bottom |
| **Release** | release_x, release_y, release_z, extension |
| **Movement** | pfx_x, pfx_z, vx0, vy0, vz0, ax, ay, az |
| **Spin** | spin_rate, spin_direction, break_angle, break_length, break_y |
| **Result** | call_code, call_description, is_strike, is_ball, is_in_play |
| **Batted Ball** | launch_speed, launch_angle, hit_distance, trajectory, hardness, hit_x, hit_y |

### Aggregated Data (`data/aggregated/`)

Pre-computed views for downstream applications:

- **`pitch_usage_by_count.json`** - Pitch usage rates by pitcher, batter hand, and count situation
  - Full season totals
  - Monthly breakdowns
  - Qualified pitchers only (150+ pitches season, 50+ pitches monthly)

## 🔄 Update Schedule

Data updates automatically 6 times daily (Central Time):
- 9:00 AM - Catch overnight West Coast games
- 3:00 PM - Early day games
- 5:00 PM - Afternoon games
- 7:00 PM - Early evening games
- 9:00 PM - Prime time games
- 11:00 PM - West Coast night games

## 📱 Using This Data

### In Python
```python
import pandas as pd

# Load a month of raw data
df = pd.read_parquet('data/raw/2026/04_april.parquet')

# Load all months
from pathlib import Path
all_dfs = [pd.read_parquet(f) for f in Path('data/raw/2026').glob('*.parquet')]
df = pd.concat(all_dfs, ignore_index=True)
```

### In JavaScript/React
```javascript
// Fetch aggregated data for apps
const response = await fetch(
  'https://raw.githubusercontent.com/YOUR_USERNAME/YOUR_REPO/main/data/aggregated/pitch_usage_by_count.json'
);
const data = await response.json();
```

## 🏗️ Project Structure

```
├── .github/workflows/
│   └── update_data.yml      # GitHub Actions schedule
├── src/
│   └── fetch_pitches.py     # Main ETL script
├── data/
│   ├── raw/                 # Full pitch-level Parquet files
│   │   └── 2026/
│   ├── aggregated/          # Pre-computed JSON views
│   └── last_update.json     # Tracking file
├── requirements.txt
└── README.md
```

## 🚀 Setup Your Own

1. Fork this repository
2. Enable GitHub Actions in your fork
3. The workflow will run automatically on schedule
4. Or trigger manually: Actions → Update MLB Pitch Data → Run workflow

## ⚾ Data Source

Data is pulled from the MLB Stats API live feed endpoint:
```
https://statsapi.mlb.com/api/v1.1/game/{game_pk}/feed/live
```

Only **regular season games** are included (no Spring Training, All-Star, or Postseason).

## 📝 License

This data is sourced from MLB's public API. Use of MLB data is subject to MLB's terms of service.
