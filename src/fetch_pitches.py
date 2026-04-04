#!/usr/bin/env python3
"""
Fetch MLB pitch data from the live feed API and aggregate for the pitch usage app.
Runs via GitHub Actions on a schedule.
Outputs: season totals, monthly breakdowns, and game-by-game data for each pitcher.
Stores both daily and monthly parquet files for flexible querying.
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
REGULAR_SEASON_START = '2026-03-26'  # Opening Day - exclude spring training

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

def safe_float(val):
    """Safely convert a value to float, returning None if not possible."""
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None

def get_pitch_data(game_id):
    """Get all pitches from a game with full Statcast-equivalent data."""
    url = f"https://statsapi.mlb.com/api/v1.1/game/{game_id}/feed/live"
    resp = requests.get(url)
    pitches = []
    
    if not resp.ok:
        return pitches
    
    data = resp.json()
    game_data = data.get('gameData', {})
    game_date = game_data.get('datetime', {}).get('officialDate', '')
    
    # Get team info
    teams = game_data.get('teams', {})
    home_team = teams.get('home', {}).get('abbreviation', '')
    away_team = teams.get('away', {}).get('abbreviation', '')
    venue = game_data.get('venue', {}).get('name', '')
    
    all_plays = data.get('liveData', {}).get('plays', {}).get('allPlays', [])
    
    for play in all_plays:
        batter = play.get('matchup', {}).get('batter', {})
        pitcher = play.get('matchup', {}).get('pitcher', {})
        bat_side = play.get('matchup', {}).get('batSide', {}).get('code', '')
        pitch_hand = play.get('matchup', {}).get('pitchHand', {}).get('code', '')
        
        # Get the play result for batted ball data
        play_result = play.get('result', {})
        
        ab_number = play.get('atBatIndex', 0)
        inning = play.get('about', {}).get('inning', 0)
        half = play.get('about', {}).get('halfInning', '')
        top_bottom = 'Top' if half == 'top' else 'Bot'
        outs = play.get('count', {}).get('outs', 0)
        
        play_events = play.get('playEvents', [])
        
        # Get the play result event type (e.g., "strikeout", "single", "walk")
        play_event_type = play_result.get('eventType', '')
        
        # Collect pitches for this play, then add events to the last one
        play_pitches = []
        
        for event in play_events:
            if not event.get('isPitch', False):
                continue
                
            details = event.get('details', {})
            pitch_type_obj = details.get('type', {})
            pitch_type = pitch_type_obj.get('code', '')
            pitch_name = pitch_type_obj.get('description', '')
            
            # Get the count BEFORE this pitch (pre-pitch count)
            count = event.get('count', {})
            balls = count.get('balls', 0)
            strikes = count.get('strikes', 0)
            
            # The API gives post-pitch count, so we need to adjust
            if details.get('isBall', False):
                balls = max(0, balls - 1)
            elif details.get('isStrike', False):
                strikes = max(0, strikes - 1)
            
            count_str = f"{balls}-{strikes}"
            
            # Only include valid counts and valid pitch types
            if not pitch_type or count_str not in VALID_COUNTS:
                continue
            
            # Pitch data from the event
            pitch_data_obj = event.get('pitchData', {})
            coordinates = pitch_data_obj.get('coordinates', {})
            breaks = pitch_data_obj.get('breaks', {})
            
            # Hit data (only on last pitch of at-bat if ball in play)
            hit_data = event.get('hitData', {})
            
            # Call/result info
            call_code = details.get('code', '')
            call_desc = details.get('description', '')
            is_strike = details.get('isStrike', False)
            is_ball = details.get('isBall', False)
            is_in_play = details.get('isInPlay', False)
            
            # Movement: convert from inches to feet (divide by 12)
            # breakVerticalInduced is IVB in inches, breakHorizontal is HB in inches
            raw_ivb = safe_float(breaks.get('breakVerticalInduced'))
            raw_hb = safe_float(breaks.get('breakHorizontal'))
            pfx_z = raw_ivb / 12.0 if raw_ivb is not None else None  # IVB inches -> feet
            pfx_x = -raw_hb / 12.0 if raw_hb is not None else None  # HB inches -> feet (negated)
            
            pitch_record = {
                # Game context
                'game_pk': game_id,
                'game_date': game_date,
                'home_team': home_team,
                'away_team': away_team,
                'venue': venue,
                'inning': inning,
                'top_bottom': top_bottom,
                
                # Matchup
                'pitcher_id': pitcher.get('id'),
                'pitcher_name': pitcher.get('fullName', ''),
                'pitcher_hand': pitch_hand,
                'batter_id': batter.get('id'),
                'batter_name': batter.get('fullName', ''),
                'batter_hand': bat_side,
                'stand': bat_side,
                
                # Count
                'balls': balls,
                'strikes': strikes,
                'count': count_str,
                'outs': outs,
                'at_bat_number': ab_number,
                'pitch_number': event.get('pitchNumber', 0),
                
                # Pitch type
                'pitch_type': pitch_type,
                'pitch_name': pitch_name,
                
                # Velocity
                'start_speed': safe_float(pitch_data_obj.get('startSpeed')),
                'end_speed': safe_float(pitch_data_obj.get('endSpeed')),
                
                # Location
                'plate_x': safe_float(coordinates.get('pX')),
                'plate_z': safe_float(coordinates.get('pZ')),
                'zone': safe_float(pitch_data_obj.get('zone')),
                'sz_top': safe_float(pitch_data_obj.get('strikeZoneTop')),
                'sz_bottom': safe_float(pitch_data_obj.get('strikeZoneBottom')),
                
                # Release point
                'release_x': safe_float(coordinates.get('x0')),
                'release_y': safe_float(coordinates.get('y0')),
                'release_z': safe_float(coordinates.get('z0')),
                'extension': safe_float(pitch_data_obj.get('extension')),
                
                # Movement (stored in feet, same as Savant)
                'pfx_x': pfx_x,
                'pfx_z': pfx_z,
                'vx0': safe_float(coordinates.get('vX0')),
                'vy0': safe_float(coordinates.get('vY0')),
                'vz0': safe_float(coordinates.get('vZ0')),
                'ax': safe_float(coordinates.get('aX')),
                'ay': safe_float(coordinates.get('aY')),
                'az': safe_float(coordinates.get('aZ')),
                
                # Spin
                'spin_rate': safe_float(breaks.get('spinRate')),
                'spin_direction': safe_float(breaks.get('spinDirection')),
                'break_angle': safe_float(breaks.get('breakAngle')),
                'break_length': safe_float(breaks.get('breakLength')),
                'break_y': safe_float(breaks.get('breakY')),
                
                # Result
                'call_code': call_code,
                'call_description': call_desc,
                'is_strike': is_strike,
                'is_ball': is_ball,
                'is_in_play': is_in_play,
                
                # Batted ball (only populated when is_in_play)
                'launch_speed': safe_float(hit_data.get('launchSpeed')),
                'launch_angle': safe_float(hit_data.get('launchAngle')),
                'hit_distance': safe_float(hit_data.get('totalDistance')),
                'trajectory': hit_data.get('trajectory', ''),
                'hardness': hit_data.get('hardness', ''),
                'hit_x': safe_float(hit_data.get('coordinates', {}).get('coordX')),
                'hit_y': safe_float(hit_data.get('coordinates', {}).get('coordY')),
                
                # Events field - will be set on last pitch of at-bat
                'events': '',
            }
            
            play_pitches.append(pitch_record)
        
        # Set the play result event on the last pitch of this at-bat
        if play_pitches and play_event_type:
            play_pitches[-1]['events'] = play_event_type
        
        pitches.extend(play_pitches)
    
    return pitches

def main():
    base_path = Path('data')
    raw_path = base_path / 'raw' / str(SEASON)
    daily_path = raw_path / 'daily'
    monthly_path = raw_path / 'monthly'
    agg_path = base_path / 'aggregated'
    
    # Create all directories
    daily_path.mkdir(parents=True, exist_ok=True)
    monthly_path.mkdir(parents=True, exist_ok=True)
    agg_path.mkdir(parents=True, exist_ok=True)
    
    # Determine date range to fetch
    tracker_file = base_path / 'last_update.json'
    if tracker_file.exists():
        with open(tracker_file) as f:
            tracker = json.load(f)
        last_date = datetime.strptime(tracker.get('last_date', '2026-03-25'), '%Y-%m-%d')
    else:
        last_date = datetime(2026, 3, 25)  # Day before opening day
    
    # Fetch from day after last_date to yesterday
    today = datetime.now()
    yesterday = today - timedelta(days=1)
    
    current_date = last_date + timedelta(days=1)
    
    # IMPORTANT: Delete old daily files so they get re-fetched with full data
    # This is needed once to upgrade from the old 9-column format
    existing_dailies = list(daily_path.glob('*.parquet'))
    if existing_dailies:
        # Check if first file has the new columns
        try:
            test_table = pq.read_table(existing_dailies[0])
            if 'start_speed' not in test_table.column_names:
                print("Detected old format daily files — deleting to re-fetch with full data...")
                for f in existing_dailies:
                    f.unlink()
                # Reset last_date to re-fetch everything
                last_date = datetime(2026, 3, 25)
                current_date = last_date + timedelta(days=1)
        except Exception:
            pass
    
    while current_date <= yesterday:
        date_str = current_date.strftime('%Y-%m-%d')
        
        # Skip spring training dates
        if date_str < REGULAR_SEASON_START:
            current_date += timedelta(days=1)
            continue
            
        print(f"Fetching games for {date_str}...")
        
        daily_file = daily_path / f"{date_str}.parquet"
        
        # Skip if we already have this day's data
        if daily_file.exists():
            print(f"  Already have data for {date_str}, skipping...")
            current_date += timedelta(days=1)
            continue
        
        day_pitches = []
        game_ids = get_schedule(date_str)
        for game_id in game_ids:
            pitches = get_pitch_data(game_id)
            day_pitches.extend(pitches)
            print(f"  Game {game_id}: {len(pitches)} pitches")
        
        # Save daily parquet
        if day_pitches:
            pq.write_table(pa.Table.from_pylist(day_pitches), daily_file)
            print(f"  Saved {len(day_pitches)} pitches to {daily_file}")
        
        current_date += timedelta(days=1)
    
    # Rebuild monthly parquets from daily files
    print("\nRebuilding monthly parquet files...")
    monthly_data = {}  # month_str -> list of pitches
    
    for daily_file in sorted(daily_path.glob('*.parquet')):
        try:
            date_str = daily_file.stem  # e.g., "2026-03-27"
            month_str = date_str[:7]    # e.g., "2026-03"
            
            table = pq.read_table(daily_file)
            rows = table.to_pylist()
            
            if month_str not in monthly_data:
                monthly_data[month_str] = []
            monthly_data[month_str].extend(rows)
        except Exception as e:
            print(f"  Error reading {daily_file}: {e}")
    
    # Save monthly parquets
    for month_str, pitches in monthly_data.items():
        month_num = int(month_str.split('-')[1])
        month_name = MONTH_NAMES.get(month_num, f'Month{month_num}')
        month_file = monthly_path / f"{month_num:02d}_{month_name.lower()}.parquet"
        pq.write_table(pa.Table.from_pylist(pitches), month_file)
        print(f"  Saved {len(pitches)} pitches to {month_file}")
    
    # Now aggregate all data from daily files
    print("\nAggregating data...")
    
    all_data = []
    for daily_file in sorted(daily_path.glob('*.parquet')):
        try:
            table = pq.read_table(daily_file)
            rows = table.to_pylist()
            if rows and 'stand' in rows[0] and 'pitch_type' in rows[0]:
                all_data.extend(rows)
        except Exception as e:
            print(f"  Error reading {daily_file}: {e}")
    
    print(f"Total pitches (regular season): {len(all_data)}")
    
    if not all_data:
        print("No valid data to aggregate. Exiting.")
        output = {
            'season': SEASON,
            'last_updated': datetime.now().isoformat(),
            'total_pitches': 0,
            'total_pitchers': 0,
            'data': {},
            'monthly': {},
            'games': {}
        }
        output_file = agg_path / 'pitch_usage_by_count.json'
        with open(output_file, 'w') as f:
            json.dump(output, f)
        
        # Update tracker
        with open(tracker_file, 'w') as f:
            json.dump({'last_date': yesterday.strftime('%Y-%m-%d'), 'last_run': datetime.now().isoformat()}, f)
        return
    
    # Build aggregations
    season_data = {}
    monthly_agg = {}
    games_data = {}  # pitcher -> {games: [{date, pitches, usage}]}
    
    for pitch in all_data:
        pitcher = pitch.get('pitcher_name', '')
        stand = pitch.get('stand', '')
        pitch_type = pitch.get('pitch_type', '')
        count = pitch.get('count', '')
        game_date = pitch.get('game_date', '')
        
        if not all([pitcher, stand, pitch_type, count, game_date]):
            continue
            
        month_num = int(game_date.split('-')[1])
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
        if month_name not in monthly_agg:
            monthly_agg[month_name] = {}
        if pitcher not in monthly_agg[month_name]:
            monthly_agg[month_name][pitcher] = {}
        if stand not in monthly_agg[month_name][pitcher]:
            monthly_agg[month_name][pitcher][stand] = {}
        if pitch_type not in monthly_agg[month_name][pitcher][stand]:
            monthly_agg[month_name][pitcher][stand][pitch_type] = {}
        monthly_agg[month_name][pitcher][stand][pitch_type][count] = monthly_agg[month_name][pitcher][stand][pitch_type].get(count, 0) + 1
        
        # Game-by-game aggregation
        if pitcher not in games_data:
            games_data[pitcher] = {}
        if game_date not in games_data[pitcher]:
            games_data[pitcher][game_date] = {}
        if stand not in games_data[pitcher][game_date]:
            games_data[pitcher][game_date][stand] = {}
        if pitch_type not in games_data[pitcher][game_date][stand]:
            games_data[pitcher][game_date][stand][pitch_type] = {}
        games_data[pitcher][game_date][stand][pitch_type][count] = games_data[pitcher][game_date][stand][pitch_type].get(count, 0) + 1
    
    # Convert games_data to list format (sorted by date, most recent last)
    games_output = {}
    for pitcher, dates in games_data.items():
        sorted_dates = sorted(dates.keys())
        games_list = []
        for date in sorted_dates:
            usage = dates[date]
            # Count total pitches for this game
            pitch_count = sum(
                count_val
                for stand_data in usage.values()
                for pitch_data in stand_data.values()
                for count_val in pitch_data.values()
            )
            games_list.append({
                'date': date,
                'pitches': pitch_count,
                'usage': usage
            })
        games_output[pitcher] = {'games': games_list}
    
    # Filter by minimum pitches
    def count_pitches(pitcher_data):
        total = 0
        for stand_data in pitcher_data.values():
            for pitch_data in stand_data.values():
                for count_val in pitch_data.values():
                    total += count_val
        return total
    
    qualified_season = {p: d for p, d in season_data.items() if count_pitches(d) >= MIN_PITCHES_SEASON}
    
    qualified_monthly = {}
    for month, pitchers in monthly_agg.items():
        qualified_monthly[month] = {p: d for p, d in pitchers.items() if count_pitches(d) >= MIN_PITCHES_MONTH}
    
    # Output
    output = {
        'season': SEASON,
        'last_updated': datetime.now().isoformat(),
        'total_pitches': len(all_data),
        'total_pitchers': len(qualified_season),
        'data': qualified_season,
        'monthly': qualified_monthly,
        'games': games_output
    }
    
    output_file = agg_path / 'pitch_usage_by_count.json'
    with open(output_file, 'w') as f:
        json.dump(output, f)
    
    print(f"\nSaved aggregated data to {output_file}")
    print(f"Season qualified pitchers: {len(qualified_season)}")
    for month, pitchers in qualified_monthly.items():
        print(f"  {month}: {len(pitchers)} pitchers")
    print(f"Pitchers with game data: {len(games_output)}")
    
    # Update tracker
    with open(tracker_file, 'w') as f:
        json.dump({'last_date': yesterday.strftime('%Y-%m-%d'), 'last_run': datetime.now().isoformat()}, f)

if __name__ == '__main__':
    main()
