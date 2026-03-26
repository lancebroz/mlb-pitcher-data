"""
MLB Pitch Data Fetcher
======================
Fetches pitch-by-pitch data from the MLB Stats API live feed.
Stores raw data in Parquet format, partitioned by month.
Generates aggregated views for downstream apps.

Schedule: Runs 6x daily via GitHub Actions
"""

import requests
import pandas as pd
import json
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional
import pyarrow as pa
import pyarrow.parquet as pq

# === CONFIGURATION ===
SEASON = 2026
DATA_DIR = Path(__file__).parent.parent / "data"
RAW_DIR = DATA_DIR / "raw" / str(SEASON)
AGG_DIR = DATA_DIR / "aggregated"
TRACKER_FILE = DATA_DIR / "last_update.json"

# MLB API endpoints
SCHEDULE_URL = "https://statsapi.mlb.com/api/v1/schedule"
LIVE_FEED_URL = "https://statsapi.mlb.com/api/v1.1/game/{game_pk}/feed/live"

# Pitch type mapping for readable names
PITCH_NAMES = {
    'FF': '4-Seam Fastball', 'SI': 'Sinker', 'FC': 'Cutter',
    'SL': 'Slider', 'CU': 'Curveball', 'KC': 'Knuckle Curve',
    'CH': 'Changeup', 'FS': 'Splitter', 'KN': 'Knuckleball',
    'SC': 'Screwball', 'CS': 'Slow Curve', 'SV': 'Sweeper',
    'ST': 'Sweeping Curve', 'FA': 'Fastball', 'EP': 'Eephus'
}


def get_last_update() -> Optional[str]:
    """Get the last date we successfully processed."""
    if TRACKER_FILE.exists():
        with open(TRACKER_FILE, 'r') as f:
            data = json.load(f)
            return data.get('last_date')
    return None


def save_last_update(date_str: str):
    """Save the last successfully processed date."""
    TRACKER_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(TRACKER_FILE, 'w') as f:
        json.dump({'last_date': date_str, 'updated_at': datetime.now().isoformat()}, f)


def get_season_start(season: int) -> str:
    """Get the regular season start date (approximate)."""
    # 2026 regular season starts ~March 26
    # Adjust as needed for actual schedule
    return f"{season}-03-26"


def get_schedule(start_date: str, end_date: str) -> list:
    """Fetch game IDs from the MLB schedule API."""
    params = {
        'sportId': 1,  # MLB
        'startDate': start_date,
        'endDate': end_date,
        'gameType': 'R',  # Regular season only (excludes Spring Training)
        'hydrate': 'team'
    }
    
    try:
        response = requests.get(SCHEDULE_URL, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
    except requests.RequestException as e:
        print(f"Error fetching schedule: {e}")
        return []
    
    games = []
    for date_entry in data.get('dates', []):
        game_date = date_entry['date']
        for game in date_entry.get('games', []):
            # Only include completed games
            if game.get('status', {}).get('abstractGameState') == 'Final':
                games.append({
                    'game_pk': game['gamePk'],
                    'game_date': game_date,
                    'home_team': game.get('teams', {}).get('home', {}).get('team', {}).get('abbreviation', ''),
                    'away_team': game.get('teams', {}).get('away', {}).get('team', {}).get('abbreviation', ''),
                    'venue': game.get('venue', {}).get('name', '')
                })
    
    return games


def extract_pitch_data(game_pk: int, game_info: dict) -> list:
    """Extract all pitch data from a game's live feed."""
    try:
        response = requests.get(LIVE_FEED_URL.format(game_pk=game_pk), timeout=60)
        response.raise_for_status()
        data = response.json()
    except requests.RequestException as e:
        print(f"Error fetching game {game_pk}: {e}")
        return []
    
    pitches = []
    
    # Get player lookup for names
    players = data.get('gameData', {}).get('players', {})
    
    all_plays = data.get('liveData', {}).get('plays', {}).get('allPlays', [])
    
    for play in all_plays:
        # Get matchup info
        matchup = play.get('matchup', {})
        pitcher_id = matchup.get('pitcher', {}).get('id')
        batter_id = matchup.get('batter', {}).get('id')
        batter_hand = matchup.get('batSide', {}).get('code', '')
        pitcher_hand = matchup.get('pitchHand', {}).get('code', '')
        
        # Get pitcher/batter names from player lookup
        pitcher_info = players.get(f'ID{pitcher_id}', {})
        batter_info = players.get(f'ID{batter_id}', {})
        pitcher_name = pitcher_info.get('fullName', '')
        batter_name = batter_info.get('fullName', '')
        
        # Play context
        about = play.get('about', {})
        inning = about.get('inning', 0)
        top_bottom = about.get('halfInning', '')  # 'top' or 'bottom'
        at_bat_number = about.get('atBatIndex', 0)
        
        # Iterate through each pitch event
        play_events = play.get('playEvents', [])
        for event in play_events:
            # Only process actual pitches (not pickoffs, etc.)
            if not event.get('isPitch', False):
                continue
            
            details = event.get('details', {})
            pitch_data = event.get('pitchData', {})
            count = event.get('count', {})
            hit_data = event.get('hitData', {})
            
            # Build comprehensive pitch record
            pitch_record = {
                # === Game Context ===
                'game_pk': game_pk,
                'game_date': game_info['game_date'],
                'home_team': game_info['home_team'],
                'away_team': game_info['away_team'],
                'venue': game_info['venue'],
                'inning': inning,
                'top_bottom': top_bottom,
                'at_bat_number': at_bat_number,
                'pitch_number': event.get('pitchNumber', 0),
                
                # === Matchup ===
                'pitcher_id': pitcher_id,
                'pitcher_name': pitcher_name,
                'pitcher_hand': pitcher_hand,
                'batter_id': batter_id,
                'batter_name': batter_name,
                'batter_hand': batter_hand,
                
                # === Count (BEFORE this pitch) ===
                'balls': count.get('balls', 0),
                'strikes': count.get('strikes', 0),
                'outs': count.get('outs', 0),
                
                # === Pitch Type ===
                'pitch_type': details.get('type', {}).get('code', ''),
                'pitch_name': details.get('type', {}).get('description', ''),
                
                # === Pitch Result ===
                'call_code': details.get('call', {}).get('code', ''),
                'call_description': details.get('call', {}).get('description', ''),
                'is_strike': details.get('isStrike', False),
                'is_ball': details.get('isBall', False),
                'is_in_play': details.get('isInPlay', False),
                
                # === Velocity ===
                'start_speed': pitch_data.get('startSpeed'),
                'end_speed': pitch_data.get('endSpeed'),
                
                # === Location ===
                'plate_x': pitch_data.get('coordinates', {}).get('pX'),
                'plate_z': pitch_data.get('coordinates', {}).get('pZ'),
                'zone': pitch_data.get('zone'),
                'sz_top': pitch_data.get('strikeZoneTop'),
                'sz_bottom': pitch_data.get('strikeZoneBottom'),
                
                # === Release Point ===
                'release_x': pitch_data.get('coordinates', {}).get('x0'),
                'release_y': pitch_data.get('coordinates', {}).get('y0'),
                'release_z': pitch_data.get('coordinates', {}).get('z0'),
                
                # === Velocity Components ===
                'vx0': pitch_data.get('coordinates', {}).get('vX0'),
                'vy0': pitch_data.get('coordinates', {}).get('vY0'),
                'vz0': pitch_data.get('coordinates', {}).get('vZ0'),
                
                # === Acceleration Components ===
                'ax': pitch_data.get('coordinates', {}).get('aX'),
                'ay': pitch_data.get('coordinates', {}).get('aY'),
                'az': pitch_data.get('coordinates', {}).get('aZ'),
                
                # === Movement ===
                'pfx_x': pitch_data.get('coordinates', {}).get('pfxX'),
                'pfx_z': pitch_data.get('coordinates', {}).get('pfxZ'),
                
                # === Spin/Break ===
                'spin_rate': pitch_data.get('breaks', {}).get('spinRate'),
                'spin_direction': pitch_data.get('breaks', {}).get('spinDirection'),
                'break_angle': pitch_data.get('breaks', {}).get('breakAngle'),
                'break_length': pitch_data.get('breaks', {}).get('breakLength'),
                'break_y': pitch_data.get('breaks', {}).get('breakY'),
                
                # === Extension ===
                'extension': pitch_data.get('extension'),
                
                # === Batted Ball (if in play) ===
                'launch_speed': hit_data.get('launchSpeed'),
                'launch_angle': hit_data.get('launchAngle'),
                'hit_distance': hit_data.get('totalDistance'),
                'trajectory': hit_data.get('trajectory'),
                'hardness': hit_data.get('hardness'),
                'hit_x': hit_data.get('coordinates', {}).get('coordX'),
                'hit_y': hit_data.get('coordinates', {}).get('coordY'),
            }
            
            pitches.append(pitch_record)
    
    return pitches


def save_to_parquet(df: pd.DataFrame, month: int):
    """Save DataFrame to monthly Parquet file, appending if exists."""
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    
    month_name = datetime(2000, month, 1).strftime('%m_%B').lower()
    filepath = RAW_DIR / f"{month_name}.parquet"
    
    if filepath.exists():
        # Read existing and append
        existing_df = pd.read_parquet(filepath)
        
        # Remove any duplicate game_pk + pitch combinations
        existing_keys = set(zip(existing_df['game_pk'], existing_df['at_bat_number'], existing_df['pitch_number']))
        new_rows = df[~df.apply(lambda r: (r['game_pk'], r['at_bat_number'], r['pitch_number']) in existing_keys, axis=1)]
        
        if len(new_rows) > 0:
            combined_df = pd.concat([existing_df, new_rows], ignore_index=True)
            combined_df.to_parquet(filepath, index=False)
            print(f"  Appended {len(new_rows)} pitches to {filepath.name}")
        else:
            print(f"  No new pitches to add to {filepath.name}")
    else:
        df.to_parquet(filepath, index=False)
        print(f"  Created {filepath.name} with {len(df)} pitches")


def load_all_raw_data() -> pd.DataFrame:
    """Load all raw Parquet files into a single DataFrame."""
    all_dfs = []
    
    if RAW_DIR.exists():
        for parquet_file in RAW_DIR.glob("*.parquet"):
            df = pd.read_parquet(parquet_file)
            all_dfs.append(df)
    
    if all_dfs:
        return pd.concat(all_dfs, ignore_index=True)
    return pd.DataFrame()


def generate_pitch_usage_aggregation(df: pd.DataFrame):
    """Generate the pitch usage by count aggregation for the app."""
    AGG_DIR.mkdir(parents=True, exist_ok=True)
    
    if df.empty:
        print("No data to aggregate")
        return
    
    # Filter to pitchers with 150+ pitches
    pitcher_counts = df.groupby('pitcher_name').size()
    qualified_pitchers = pitcher_counts[pitcher_counts >= 150].index.tolist()
    df_qualified = df[df['pitcher_name'].isin(qualified_pitchers)]
    
    print(f"Aggregating data for {len(qualified_pitchers)} qualified pitchers")
    
    # Build the nested structure: pitcher -> batter_hand -> pitch_type -> count -> total
    result = {}
    
    for pitcher_name in qualified_pitchers:
        pitcher_df = df_qualified[df_qualified['pitcher_name'] == pitcher_name]
        result[pitcher_name] = {}
        
        for batter_hand in ['R', 'L']:
            hand_df = pitcher_df[pitcher_df['batter_hand'] == batter_hand]
            if hand_df.empty:
                continue
            
            result[pitcher_name][batter_hand] = {}
            
            for pitch_type in hand_df['pitch_type'].unique():
                if not pitch_type:  # Skip empty pitch types
                    continue
                    
                pitch_df = hand_df[hand_df['pitch_type'] == pitch_type]
                result[pitcher_name][batter_hand][pitch_type] = {}
                
                # Group by count
                for (balls, strikes), count_df in pitch_df.groupby(['balls', 'strikes']):
                    count_key = f"{balls}-{strikes}"
                    result[pitcher_name][batter_hand][pitch_type][count_key] = len(count_df)
    
    # Also create month-level aggregations
    df['month'] = pd.to_datetime(df['game_date']).dt.month
    
    monthly_result = {}
    for month in df['month'].unique():
        month_df = df[df['month'] == month]
        month_name = datetime(2000, int(month), 1).strftime('%B')
        monthly_result[month_name] = {}
        
        # Filter to pitchers with 50+ pitches that month
        month_pitcher_counts = month_df.groupby('pitcher_name').size()
        month_qualified = month_pitcher_counts[month_pitcher_counts >= 50].index.tolist()
        month_df_qualified = month_df[month_df['pitcher_name'].isin(month_qualified)]
        
        for pitcher_name in month_qualified:
            pitcher_df = month_df_qualified[month_df_qualified['pitcher_name'] == pitcher_name]
            monthly_result[month_name][pitcher_name] = {}
            
            for batter_hand in ['R', 'L']:
                hand_df = pitcher_df[pitcher_df['batter_hand'] == batter_hand]
                if hand_df.empty:
                    continue
                
                monthly_result[month_name][pitcher_name][batter_hand] = {}
                
                for pitch_type in hand_df['pitch_type'].unique():
                    if not pitch_type:
                        continue
                    
                    pitch_df = hand_df[hand_df['pitch_type'] == pitch_type]
                    monthly_result[month_name][pitcher_name][batter_hand][pitch_type] = {}
                    
                    for (balls, strikes), count_df in pitch_df.groupby(['balls', 'strikes']):
                        count_key = f"{balls}-{strikes}"
                        monthly_result[month_name][pitcher_name][batter_hand][pitch_type][count_key] = len(count_df)
    
    # Save full season aggregation
    output = {
        'season': SEASON,
        'last_updated': datetime.now().isoformat(),
        'total_pitches': len(df),
        'total_pitchers': len(qualified_pitchers),
        'data': result,
        'monthly': monthly_result
    }
    
    output_path = AGG_DIR / "pitch_usage_by_count.json"
    with open(output_path, 'w') as f:
        json.dump(output, f)
    
    print(f"Saved aggregation to {output_path}")
    print(f"  Total pitches: {len(df):,}")
    print(f"  Qualified pitchers (150+ pitches): {len(qualified_pitchers)}")


def main():
    """Main entry point for the data fetch pipeline."""
    print(f"=" * 50)
    print(f"MLB Pitch Data Fetcher - {datetime.now().isoformat()}")
    print(f"=" * 50)
    
    # Determine date range to fetch
    last_update = get_last_update()
    today = datetime.now().strftime('%Y-%m-%d')
    
    if last_update:
        # Start from the day after last update
        start_date = (datetime.strptime(last_update, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')
        print(f"Incremental update from {start_date} to {today}")
    else:
        # First run - start from season start
        start_date = get_season_start(SEASON)
        print(f"Initial fetch from {start_date} to {today}")
    
    # Don't fetch future dates
    if start_date > today:
        print("Already up to date!")
        # Still regenerate aggregations in case we want fresh output
        df = load_all_raw_data()
        if not df.empty:
            generate_pitch_usage_aggregation(df)
        return
    
    # Fetch schedule
    print(f"\nFetching schedule...")
    games = get_schedule(start_date, today)
    print(f"Found {len(games)} completed games")
    
    if not games:
        print("No new games to process")
        df = load_all_raw_data()
        if not df.empty:
            generate_pitch_usage_aggregation(df)
        return
    
    # Fetch pitch data for each game
    all_pitches = []
    for i, game in enumerate(games):
        print(f"  [{i+1}/{len(games)}] Fetching game {game['game_pk']} ({game['away_team']} @ {game['home_team']})...")
        pitches = extract_pitch_data(game['game_pk'], game)
        all_pitches.extend(pitches)
        print(f"    → {len(pitches)} pitches")
    
    print(f"\nTotal new pitches: {len(all_pitches)}")
    
    if all_pitches:
        # Convert to DataFrame
        df_new = pd.DataFrame(all_pitches)
        
        # Save by month
        df_new['month'] = pd.to_datetime(df_new['game_date']).dt.month
        for month, month_df in df_new.groupby('month'):
            save_to_parquet(month_df.drop(columns=['month']), month)
        
        # Update tracker
        save_last_update(today)
    
    # Regenerate aggregations with all data
    print(f"\nGenerating aggregations...")
    df_all = load_all_raw_data()
    generate_pitch_usage_aggregation(df_all)
    
    print(f"\n{'=' * 50}")
    print(f"Complete!")
    print(f"{'=' * 50}")


if __name__ == "__main__":
    main()
