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
from zoneinfo import ZoneInfo

# Config
MIN_PITCHES_SEASON = 10   # Lowered for early season
MIN_PITCHES_MONTH = 10    # Lowered for early season
SEASON = 2026
REGULAR_SEASON_START = '2026-03-27'  # Opening Day - exclude spring training
CENTRAL_TZ = ZoneInfo('America/Chicago')  # MLB games are scheduled in US timezones

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
        inning = play.get('about', {}).get('inning', 0)
        half_inning = play.get('about', {}).get('halfInning', '')  # 'top' or 'bottom'
        
        for event in play.get('playEvents', []):
            if event.get('isPitch', False):
                details = event.get('details', {})
                pitch_type = details.get('type', {}).get('code', '')
                
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
                        'count': count_str,
                        'inning': inning,
                        'half_inning': half_inning
                    })
    
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
        last_date = datetime.strptime(tracker.get('last_date', '2026-03-26'), '%Y-%m-%d')
    else:
        last_date = datetime(2026, 3, 26)  # Day before opening day
    
    # Fetch from day after last_date through TODAY (not yesterday)
    # Use Central Time since MLB games are scheduled in US timezones
    # GitHub Actions runs in UTC, so we need to convert
    today = datetime.now(CENTRAL_TZ).replace(tzinfo=None)
    
    current_date = last_date + timedelta(days=1)
    
    while current_date <= today:
        date_str = current_date.strftime('%Y-%m-%d')
        
        # Skip spring training dates
        if date_str < REGULAR_SEASON_START:
            current_date += timedelta(days=1)
            continue
            
        print(f"Fetching games for {date_str}...")
        
        daily_file = daily_path / f"{date_str}.parquet"
        
        # For past days, skip if we already have the file
        # For today, always re-fetch to get newly completed games
        is_today = (current_date.date() == today.date())
        if daily_file.exists() and not is_today:
            print(f"  Already have data for {date_str}, skipping...")
            current_date += timedelta(days=1)
            continue
        
        day_pitches = []
        game_ids = get_schedule(date_str)
        for game_id in game_ids:
            pitches = get_pitch_data(game_id)
            day_pitches.extend(pitches)
            print(f"  Game {game_id}: {len(pitches)} pitches")
        
        # Save daily parquet (overwrite for today to capture new games)
        if day_pitches:
            pq.write_table(pa.Table.from_pylist(day_pitches), daily_file)
            print(f"  Saved {len(day_pitches)} pitches to {daily_file}")
        elif is_today:
            print(f"  No completed games yet today")
        
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
    monthly_data = {}
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
        if month_name not in monthly_data:
            monthly_data[month_name] = {}
        if pitcher not in monthly_data[month_name]:
            monthly_data[month_name][pitcher] = {}
        if stand not in monthly_data[month_name][pitcher]:
            monthly_data[month_name][pitcher][stand] = {}
        if pitch_type not in monthly_data[month_name][pitcher][stand]:
            monthly_data[month_name][pitcher][stand][pitch_type] = {}
        monthly_data[month_name][pitcher][stand][pitch_type][count] = monthly_data[month_name][pitcher][stand][pitch_type].get(count, 0) + 1
        
        # Game-by-game aggregation
        if pitcher not in games_data:
            games_data[pitcher] = {}
        if game_date not in games_data[pitcher]:
            games_data[pitcher][game_date] = {'usage': {}, 'innings': set()}
        if stand not in games_data[pitcher][game_date]['usage']:
            games_data[pitcher][game_date]['usage'][stand] = {}
        if pitch_type not in games_data[pitcher][game_date]['usage'][stand]:
            games_data[pitcher][game_date]['usage'][stand][pitch_type] = {}
        games_data[pitcher][game_date]['usage'][stand][pitch_type][count] = games_data[pitcher][game_date]['usage'][stand][pitch_type].get(count, 0) + 1
        
        # Track unique innings pitched (inning + half_inning for uniqueness)
        inning = pitch.get('inning', 0)
        half_inning = pitch.get('half_inning', '')
        if inning and half_inning:
            games_data[pitcher][game_date]['innings'].add((inning, half_inning))
    
    # Convert games_data to list format (sorted by date, most recent last)
    games_output = {}
    for pitcher, dates in games_data.items():
        sorted_dates = sorted(dates.keys())
        games_list = []
        for date in sorted_dates:
            game_info = dates[date]
            usage = game_info['usage']
            innings_set = game_info['innings']
            # Count total pitches for this game
            pitch_count = sum(
                count_val
                for stand_data in usage.values()
                for pitch_data in stand_data.values()
                for count_val in pitch_data.values()
            )
            # Calculate innings pitched (count unique innings)
            innings_pitched = len(innings_set)
            games_list.append({
                'date': date,
                'pitches': pitch_count,
                'innings': innings_pitched,
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
    for month, pitchers in monthly_data.items():
        qualified_monthly[month] = {p: d for p, d in pitchers.items() if count_pitches(d) >= MIN_PITCHES_MONTH}
    
    # Calculate total innings from games data (sum of innings per pitcher per game)
    total_innings = sum(
        game.get('innings', 0) 
        for pitcher_data in games_output.values() 
        for game in pitcher_data.get('games', [])
    )
    
    # Output - use Central Time for last_updated
    output = {
        'season': SEASON,
        'last_updated': datetime.now(CENTRAL_TZ).isoformat(),
        'total_pitches': len(all_data),
        'total_pitchers': len(qualified_season),
        'total_innings': total_innings,
        'data': qualified_season,
        'monthly': qualified_monthly,
        'games': games_output
    }
    
    output_file = agg_path / 'pitch_usage_by_count.json'
    with open(output_file, 'w') as f:
        json.dump(output, f)
    
    print(f"\nSaved aggregated data to {output_file}")
    print(f"Season qualified pitchers: {len(qualified_season)}")
    print(f"Total innings: {total_innings}")
    for month, pitchers in qualified_monthly.items():
        print(f"  {month}: {len(pitchers)} pitchers")
    print(f"Pitchers with game data: {len(games_output)}")
    
    # Update tracker - set to yesterday so we always re-check today on next run
    yesterday = today - timedelta(days=1)
    with open(tracker_file, 'w') as f:
        json.dump({'last_date': yesterday.strftime('%Y-%m-%d'), 'last_run': datetime.now(CENTRAL_TZ).isoformat()}, f)

if __name__ == '__main__':
    main()
