#!/usr/bin/env python3
"""
Fetch MLB pitch data from the live feed API and aggregate for the pitch usage app.
Runs via GitHub Actions on a schedule.
"""

import json
import requests
from datetime import datetime, timedelta
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq

# Config
MIN_PITCHES_SEASON = 10   # Lowered for early season
MIN_PITCHES_MONTH = 10    # Lowered for early season
SEASON = 2026

# Valid counts only (filter out MLB API post-pitch count bug)
VALID_COUNTS = {'0-0', '0-1', '0-2', '1-0', '1-1', '1-2', '2-0', '2-1', '2-2', '3-0', '3-1', '3-2'}

MONTH_NAMES = {
    3: 'March', 4: 'April', 5: 'May', 6: 'June',
    7: 'July', 8: 'August', 9: 'September', 10: 'October', 11: 'November'
}

def get_schedule(date):
    """Get game IDs for a given date."""
    url = f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&date={date}"
    resp = requests.get(url)
    games = []
    if resp.ok:
        data = resp.json()
        for date_entry in data.get('dates', []):
            for game in date_entry.get('games', []):
                if game.get('status', {}).get('abstractGameState') == 'Final':
                    games.append(game['gamePk'])
    return games

def get_pitch_data(game_id):
    """Get all pitches from a game."""
    url = f"https://statsapi.mlb.com/api/v1.1/game/{game_id}/feed/live"
    resp = requests.get(url)
    pitches = []
    
    if not resp.ok:
        return pitches
    
    data = resp.json()
    game_date = data.get('gameData', {}).get('datetime', {}).get('officialDate', '')
    
    all_plays = data.get('liveData', {}).get('plays', {}).get('allPlays', [])
    
    for play in all_plays:
        batter = play.get('matchup', {}).get('batter', {})
        pitcher = play.get('matchup', {}).get('pitcher', {})
        bat_side = play.get('matchup', {}).get('batSide', {}).get('code', '')
        
        for event in play.get('playEvents', []):
            if event.get('isPitch', False):
                details = event.get('details', {})
                pitch_type = details.get('type', {}).get('code', '')
                
                # Get the count BEFORE this pitch (pre-pitch count)
                count = event.get('count', {})
                balls = count.get('balls', 0)
                strikes = count.get('strikes', 0)
                
                # The API gives post-pitch count, so we need to adjust
                # If this pitch was a ball, subtract 1 from balls
                # If this pitch was a strike (or foul with < 2 strikes), subtract 1 from strikes
                if details.get('isBall', False):
                    balls = max(0, balls - 1)
                elif details.get('isStrike', False):
                    strikes = max(0, strikes - 1)
                
                count_str = f"{balls}-{strikes}"
                
                # Only include valid counts
                if pitch_type and count_str in VALID_COUNTS:
                    pitches.append({
                        'game_date': game_date,
                        'pitcher_id': pitcher.get('id'),
                        'pitcher_name': pitcher.get('fullName', ''),
                        'batter_id': batter.get('id'),
                        'stand': bat_side,
                        'pitch_type': pitch_type,
                        'balls': balls,
                        'strikes': strikes,
                        'count': count_str
                    })
    
    return pitches

def main():
    base_path = Path('data')
    raw_path = base_path / 'raw' / str(SEASON)
    agg_path = base_path / 'aggregated'
    raw_path.mkdir(parents=True, exist_ok=True)
    agg_path.mkdir(parents=True, exist_ok=True)
    
    # Determine date range to fetch
    # Check last update
    tracker_file = base_path / 'last_update.json'
    if tracker_file.exists():
        with open(tracker_file) as f:
            tracker = json.load(f)
        last_date = datetime.strptime(tracker.get('last_date', '2026-03-01'), '%Y-%m-%d')
    else:
        last_date = datetime(2026, 3, 20)  # Season start
    
    # Fetch from last_date to yesterday
    today = datetime.now()
    yesterday = today - timedelta(days=1)
    
    all_pitches = []
    current_date = last_date
    
    while current_date <= yesterday:
        date_str = current_date.strftime('%Y-%m-%d')
        print(f"Fetching games for {date_str}...")
        
        game_ids = get_schedule(date_str)
        for game_id in game_ids:
            pitches = get_pitch_data(game_id)
            all_pitches.extend(pitches)
            print(f"  Game {game_id}: {len(pitches)} pitches")
        
        current_date += timedelta(days=1)
    
    if all_pitches:
        # Save to parquet by month
        df_new = pa.Table.from_pylist(all_pitches)
        
        # Group by month and save
        for month in set(p['game_date'][:7] for p in all_pitches):
            month_num = int(month.split('-')[1])
            month_name = MONTH_NAMES.get(month_num, f'Month{month_num}')
            month_file = raw_path / f"{month_num:02d}_{month_name.lower()}.parquet"
            
            month_pitches = [p for p in all_pitches if p['game_date'].startswith(month)]
            
            # Load existing if present
            if month_file.exists():
                existing = pq.read_table(month_file).to_pylist()
                # Merge avoiding duplicates (by game_date + pitcher_id + batter_id + count index)
                existing_keys = set((p['game_date'], p['pitcher_id'], p['batter_id'], p.get('count', '')) for p in existing)
                for p in month_pitches:
                    key = (p['game_date'], p['pitcher_id'], p['batter_id'], p.get('count', ''))
                    if key not in existing_keys:
                        existing.append(p)
                month_pitches = existing
            
            pq.write_table(pa.Table.from_pylist(month_pitches), month_file)
            print(f"Saved {len(month_pitches)} pitches to {month_file}")
    
    # Now aggregate all data
    print("\nAggregating data...")
    
    all_data = []
    for parquet_file in raw_path.glob('*.parquet'):
        table = pq.read_table(parquet_file)
        all_data.extend(table.to_pylist())
    
    print(f"Total pitches: {len(all_data)}")
    
    # Build aggregations
    season_data = {}  # pitcher -> stand -> pitch_type -> count -> total
    monthly_data = {}  # month -> pitcher -> stand -> pitch_type -> count -> total
    
    for pitch in all_data:
        pitcher = pitch['pitcher_name']
        stand = pitch['stand']
        pitch_type = pitch['pitch_type']
        count = pitch['count']
        month_num = int(pitch['game_date'].split('-')[1])
        month_name = MONTH_NAMES.get(month_num, f'Month{month_num}')
        
        # Season aggregation
        if pitcher not in season_data:
            season_data[pitcher] = {}
        if stand not in season_data[pitcher]:
            season_data[pitcher][stand] = {}
        if pitch_type not in season_data[pitcher][stand]:
            season_data[pitcher][stand][pitch_type] = {}
        season_data[pitcher][stand][pitch_type][count] = season_data[pitcher][stand][pitch_type].get(count, 0) + 1
        
        # Monthly aggregation
        if month_name not in monthly_data:
            monthly_data[month_name] = {}
        if pitcher not in monthly_data[month_name]:
            monthly_data[month_name][pitcher] = {}
        if stand not in monthly_data[month_name][pitcher]:
            monthly_data[month_name][pitcher][stand] = {}
        if pitch_type not in monthly_data[month_name][pitcher][stand]:
            monthly_data[month_name][pitcher][stand][pitch_type] = {}
        monthly_data[month_name][pitcher][stand][pitch_type][count] = monthly_data[month_name][pitcher][stand][pitch_type].get(count, 0) + 1
    
    # Filter by minimum pitches
    def count_pitches(pitcher_data):
        total = 0
        for stand_data in pitcher_data.values():
            for pitch_data in stand_data.values():
                for count_val in pitch_data.values():
                    total += count_val
        return total
    
    # Season qualified
    qualified_season = {p: d for p, d in season_data.items() if count_pitches(d) >= MIN_PITCHES_SEASON}
    
    # Monthly qualified
    qualified_monthly = {}
    for month, pitchers in monthly_data.items():
        qualified_monthly[month] = {p: d for p, d in pitchers.items() if count_pitches(d) >= MIN_PITCHES_MONTH}
    
    # Output
    output = {
        'season': SEASON,
        'last_updated': datetime.now().isoformat(),
        'total_pitches': len(all_data),
        'total_pitchers': len(qualified_season),
        'data': qualified_season,
        'monthly': qualified_monthly
    }
    
    output_file = agg_path / 'pitch_usage_by_count.json'
    with open(output_file, 'w') as f:
        json.dump(output, f)
    
    print(f"\nSaved aggregated data to {output_file}")
    print(f"Season qualified pitchers: {len(qualified_season)}")
    for month, pitchers in qualified_monthly.items():
        print(f"  {month}: {len(pitchers)} pitchers")
    
    # Update tracker
    with open(tracker_file, 'w') as f:
        json.dump({'last_date': yesterday.strftime('%Y-%m-%d'), 'last_run': datetime.now().isoformat()}, f)

if __name__ == '__main__':
    main()
