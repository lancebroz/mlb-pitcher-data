#!/usr/bin/env python3
"""
Fetch MLB pitch data from the live feed API with FULL Statcast-equivalent schema.
Runs via GitHub Actions on a schedule.

Extracts every field the downstream apps expect:
  - Velocity (start_speed, end_speed)
  - Release point (release_x, release_y, release_z, extension)
  - Movement (pfx_x, pfx_z, vx0, vy0, vz0, ax, ay, az)
  - Spin (spin_rate, spin_direction, break_angle, break_length, break_y)
  - Location (plate_x, plate_z, zone, sz_top, sz_bottom)
  - Situation (balls, strikes and outs before the pitch; on_1b, on_2b, on_3b)
  - Result (call_code, call_description, is_strike, is_ball, is_in_play, type)
  - Batted ball (launch_speed, launch_angle, hit_distance, trajectory, hardness, hit_x, hit_y)

Outputs:
  - data/raw/2026/daily/YYYY-MM-DD.parquet  (one file per game day)
  - data/raw/2026/monthly/MM_monthname.parquet  (rebuilt from dailies)
  - data/aggregated/pitch_usage_by_count.json  (per-pitcher usage views)
  - data/last_update.json  (tracker file)
"""

import json
import os
import requests
from datetime import datetime, timedelta
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
from zoneinfo import ZoneInfo

# Config
MIN_PITCHES_SEASON = 10
MIN_PITCHES_MONTH = 10
SEASON = 2026
REGULAR_SEASON_START = '2026-03-25'  # Opening Day 2026 was March 25 (no Japan series this season)
CENTRAL_TZ = ZoneInfo('America/Chicago')

VALID_COUNTS = {'0-0', '0-1', '0-2', '1-0', '1-1', '1-2', '2-0', '2-1', '2-2', '3-0', '3-1', '3-2'}

MONTH_NAMES = {
    3: 'March', 4: 'April', 5: 'May', 6: 'June',
    7: 'July', 8: 'August', 9: 'September', 10: 'October', 11: 'November'
}


def _f(v):
    """Coerce to float, return None if not a number."""
    if v is None:
        return None
    try:
        f = float(v)
        if f != f:  # NaN check
            return None
        return f
    except (ValueError, TypeError):
        return None


def _i(v):
    """Coerce to int, return None if not a number."""
    if v is None:
        return None
    try:
        return int(v)
    except (ValueError, TypeError):
        return None


def _s(v, default=''):
    """Coerce to string, return default if None."""
    if v is None:
        return default
    s = str(v)
    return default if s in ('nan', 'None', 'NaN') else s


BASES = ('1B', '2B', '3B')


def _runner_moves(play):
    """Runner movements of a play, grouped by the index of the play event they happened on."""
    moves = {}
    for r in play.get('runners') or []:
        mv = r.get('movement') or {}
        det = r.get('details') or {}
        rid = (det.get('runner') or {}).get('id')
        moves.setdefault(det.get('playIndex'), []).append(
            (rid, mv.get('start'), mv.get('end'), bool(mv.get('isOut'))))
    return moves


def _apply_moves(pos, moves):
    """Advance the {runner_id: base} map by one play event's runner movements."""
    for rid, start, end, is_out in moves:
        key = rid
        if key not in pos and start in BASES:
            # Runner we only know by the base he started on
            key = next((k for k, b in pos.items() if b == start), rid)
        pos.pop(key, None)
        if not is_out and end in BASES:
            pos[rid if rid is not None else key] = end


def _post_play_bases(play):
    """{runner_id: base} after a play, from the feed's own post-play fields."""
    m = play.get('matchup') or {}
    pos = {}
    for field, base in (('postOnFirst', '1B'), ('postOnSecond', '2B'), ('postOnThird', '3B')):
        rid = (m.get(field) or {}).get('id')
        if rid is not None:
            pos[rid] = base
    return pos


# Game types to ingest: R = regular season, F = Wild Card, D = Division Series,
# L = League Championship Series, W = World Series. Spring (S), exhibitions (E)
# and the All-Star Game (A) stay excluded.
INGESTED_GAME_TYPES = {'R', 'F', 'D', 'L', 'W'}


def _get_json(url, timeout, attempts=3):
    """GET a JSON document, retrying transient failures. Returns None if every attempt fails."""
    import time
    for attempt in range(attempts):
        try:
            resp = requests.get(url, timeout=timeout)
            if resp.ok:
                return resp.json()
            print(f"  HTTP {resp.status_code} for {url} (attempt {attempt + 1}/{attempts})")
        except Exception as e:
            print(f"  Fetch failed for {url} (attempt {attempt + 1}/{attempts}): {e}")
        if attempt < attempts - 1:
            time.sleep(2 * (attempt + 1))
    return None


def get_schedule(date):
    """Get final-status game IDs for a given date (regular season + postseason)."""
    url = f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&date={date}"
    games = []
    data = _get_json(url, timeout=15)
    if data is not None:
        for date_entry in data.get('dates', []):
            for game in date_entry.get('games', []):
                # Only Final games
                if game.get('status', {}).get('abstractGameState') == 'Final':
                    # Regular season + postseason (see INGESTED_GAME_TYPES)
                    if game.get('gameType') in INGESTED_GAME_TYPES:
                        games.append(game['gamePk'])
    return games


# Games whose feed could not be fetched during this run (see main: a day with a failed
# game never overwrites a daily file that already exists).
FAILED_FEEDS = set()


def get_pitch_data(game_id):
    """Get all pitches from a game with FULL Statcast schema."""
    url = f"https://statsapi.mlb.com/api/v1.1/game/{game_id}/feed/live"
    data = _get_json(url, timeout=30)
    if data is None:
        print(f"  Feed fetch failed for game {game_id}")
        FAILED_FEEDS.add(game_id)
        return []

    pitches = []
    game_data = data.get('gameData', {})
    live_data = data.get('liveData', {})

    game_date = game_data.get('datetime', {}).get('officialDate', '')
    home_team = game_data.get('teams', {}).get('home', {}).get('abbreviation', '')
    away_team = game_data.get('teams', {}).get('away', {}).get('abbreviation', '')
    venue = game_data.get('venue', {}).get('name', '')

    all_plays = live_data.get('plays', {}).get('allPlays', [])
    bb_map = {
        'fly_ball': 'fly_ball', 'ground_ball': 'ground_ball',
        'line_drive': 'line_drive', 'popup': 'popup',
    }

    prev_half = None      # (inning, halfInning) of the previous play
    carried = {}          # {runner_id: base} after the previous play

    for play in all_plays:
        matchup = play.get('matchup', {})
        about = play.get('about', {})

        # Runners on base when this play starts: whoever the previous play of the
        # same half inning left on. Runner movements inside the play (steals, wild
        # pitches, the extra-inning placed runner) are applied event by event below.
        this_half = (about.get('inning'), about.get('halfInning'))
        pos = dict(carried) if this_half == prev_half else {}
        prev_half = this_half
        carried = _post_play_bases(play)
        moves_by_event = _runner_moves(play)
        # Count before the next pitch, tracked through every play event (pitches,
        # automatic balls/strikes, ...) rather than backed out of the post-pitch count.
        pre_balls, pre_strikes = 0, 0

        batter = matchup.get('batter', {})
        pitcher = matchup.get('pitcher', {})
        pitcher_id = pitcher.get('id')
        pitcher_name = pitcher.get('fullName', '')
        batter_id = batter.get('id')
        batter_name = batter.get('fullName', '')
        bat_side = matchup.get('batSide', {}).get('code', '')
        pitch_hand = matchup.get('pitchHand', {}).get('code', '')

        inning = about.get('inning', 0)
        half_inning = about.get('halfInning', '')
        top_bottom = 'Top' if half_inning == 'top' else ('Bot' if half_inning == 'bottom' else '')
        at_bat_index = play.get('atBatIndex', 0)

        # Final play result (events)
        result = play.get('result', {})
        events = result.get('eventType', '')

        for ev_index, event in enumerate(play.get('playEvents', [])):
            # State BEFORE this event, then advance the trackers for the next one.
            on_1b = '1B' in pos.values()
            on_2b = '2B' in pos.values()
            on_3b = '3B' in pos.values()
            balls, strikes = pre_balls, pre_strikes
            try:
                _apply_moves(pos, moves_by_event.get(event.get('index', ev_index), []))
            except Exception:
                pass
            if not event.get('isPitch', False):
                # Runners that appear or change identity without a runner movement:
                # the extra-inning placed runner, and pinch runners.
                try:
                    ev_base = event.get('base')
                    ev_base = {1: '1B', 2: '2B', 3: '3B'}.get(ev_base, ev_base if ev_base in BASES else None)
                    ev_player = (event.get('player') or {}).get('id')
                    ev_type = (event.get('details') or {}).get('eventType')
                    replaced = (event.get('replacedPlayer') or {}).get('id')
                    if ev_type == 'runner_placed' and ev_base and ev_player is not None:
                        pos[ev_player] = ev_base
                    elif replaced is not None and replaced in pos and ev_player is not None:
                        pos[ev_player] = pos.pop(replaced)
                except Exception:
                    pass
            ev_count = event.get('count') or {}
            if ev_count.get('balls') is not None and ev_count.get('strikes') is not None:
                pre_balls = min(int(ev_count['balls']), 3)
                pre_strikes = min(int(ev_count['strikes']), 2)

            if not event.get('isPitch', False):
                continue

            # Per-pitch UUID from the live feed. Links directly to MLB's research/video
            # tools: https://research.mlb.com/games/{game_pk}/plays/{play_id}
            play_id = event.get('playId', '') or ''

            details = event.get('details', {})
            pitch_data = event.get('pitchData') or {}
            hit_data = event.get('hitData') or {}
            coords = pitch_data.get('coordinates') or {}
            breaks = pitch_data.get('breaks') or {}

            pitch_type_obj = details.get('type', {}) or {}
            pitch_type_code = pitch_type_obj.get('code', '')
            pitch_name = pitch_type_obj.get('description', '')

            # Pre-pitch count: tracked above. (Backing it out of the post-pitch count
            # filed every two-strike foul as a one-strike pitch.)
            # `outs` in the feed is the number of outs when the batter came up.
            count = event.get('count', {}) or {}
            outs = count.get('outs', 0) or 0

            count_str = f"{balls}-{strikes}"

            # Skip pitch types we can't classify
            if not pitch_type_code:
                continue

            # call_description: human-readable; call_code is the short code
            call_desc_raw = details.get('description', '')  # e.g. "Called Strike"
            call_code = (details.get('call') or {}).get('code', '') or details.get('code', '')

            # Normalize call_description to snake_case for downstream consistency
            cd_lower = call_desc_raw.lower().replace(' ', '_').replace(',', '').replace('(', '').replace(')', '')
            cd_norm_map = {
                'called_strike': 'called_strike',
                'swinging_strike': 'swinging_strike',
                'swinging_strike_blocked': 'swinging_strike_blocked',
                'foul': 'foul',
                'foul_tip': 'foul_tip',
                'foul_bunt': 'foul_bunt',
                'ball': 'ball',
                'ball_in_dirt': 'blocked_ball',
                'blocked_ball': 'blocked_ball',
                'hit_by_pitch': 'hit_by_pitch',
                'missed_bunt': 'missed_bunt',
                'in_play_outs': 'hit_into_play',
                'in_play_out_s': 'hit_into_play',
                'in_play_runs': 'hit_into_play_score',
                'in_play_run_s': 'hit_into_play_score',
                'in_play_no_out': 'hit_into_play_no_out',
                'pitchout': 'pitchout',
                'intent_ball': 'intent_ball',
            }
            call_description = cd_norm_map.get(cd_lower, cd_lower)

            is_strike = bool(details.get('isStrike', False))
            is_ball = bool(details.get('isBall', False))
            is_in_play = bool(details.get('isInPlay', False))

            # Movement: use breaks.breakVerticalInduced + breakHorizontal which match
            # Savant's IVB/HB (in inches). pfxX/pfxZ from coordinates uses different units.
            # We divide by 12 to store in feet (matches Savant CSV convention used elsewhere).
            raw_ivb = breaks.get('breakVerticalInduced')
            raw_hb = breaks.get('breakHorizontal')
            pfx_z_ft = (raw_ivb / 12.0) if raw_ivb is not None else None
            pfx_x_ft = (-raw_hb / 12.0) if raw_hb is not None else None

            # Full Statcast-equivalent record
            row = {
                # Game context
                'game_pk': game_id,
                'game_date': game_date,
                'play_id': play_id,  # MLB per-pitch UUID for video/research deep links
                'home_team': home_team,
                'away_team': away_team,
                'venue': venue,
                'inning': inning,
                'top_bottom': top_bottom,
                'half_inning': half_inning,

                # Matchup
                'pitcher_id': pitcher_id,
                'pitcher_name': pitcher_name,
                'pitcher_hand': pitch_hand,
                'batter_id': batter_id,
                'batter_name': batter_name,
                'batter_hand': bat_side,
                'stand': bat_side,  # alias

                # Count
                'balls': balls,
                'strikes': strikes,
                'count': count_str,
                'outs': outs,                 # outs when the batter came up
                'on_1b': on_1b,               # runner on first before the pitch
                'on_2b': on_2b,
                'on_3b': on_3b,
                'at_bat_number': at_bat_index,
                'pitch_number': event.get('pitchNumber', 0),

                # Pitch type
                'pitch_type': pitch_type_code,
                'pitch_name': pitch_name,

                # Velocity
                'start_speed': _f(pitch_data.get('startSpeed')),
                'end_speed': _f(pitch_data.get('endSpeed')),

                # Location
                'plate_x': _f(coords.get('pX')),
                'plate_z': _f(coords.get('pZ')),
                'zone': _i(pitch_data.get('zone')),
                'sz_top': _f(pitch_data.get('strikeZoneTop')),
                'sz_bottom': _f(pitch_data.get('strikeZoneBottom')),

                # Release point
                'release_x': _f(coords.get('x0')),
                'release_y': _f(coords.get('y0')),
                'release_z': _f(coords.get('z0')),
                'extension': _f(pitch_data.get('extension')),

                # Movement (in feet, matches Savant CSV convention)
                'pfx_x': pfx_x_ft,
                'pfx_z': pfx_z_ft,
                'vx0': _f(coords.get('vX0')),
                'vy0': _f(coords.get('vY0')),
                'vz0': _f(coords.get('vZ0')),
                'ax': _f(coords.get('aX')),
                'ay': _f(coords.get('aY')),
                'az': _f(coords.get('aZ')),

                # Spin
                'spin_rate': _f(breaks.get('spinRate')),
                'spin_direction': _f(breaks.get('spinDirection')),
                'break_angle': _f(breaks.get('breakAngle')),
                'break_length': _f(breaks.get('breakLength')),
                'break_y': _f(breaks.get('breakY')),

                # Result
                'call_code': call_code,
                'call_description': call_description,
                'is_strike': is_strike,
                'is_ball': is_ball,
                'is_in_play': is_in_play,
                'events': events,

                # Batted ball
                'launch_speed': _f(hit_data.get('launchSpeed')),
                'launch_angle': _f(hit_data.get('launchAngle')),
                'hit_distance': _f(hit_data.get('totalDistance')),
                'trajectory': bb_map.get(hit_data.get('trajectory', ''), hit_data.get('trajectory', '')),
                'hardness': hit_data.get('hardness', ''),
                'hit_x': _f(hit_data.get('coordinates', {}).get('coordX')),
                'hit_y': _f(hit_data.get('coordinates', {}).get('coordY')),
            }
            pitches.append(row)

    return pitches


def main():
    base_path = Path('data')
    raw_path = base_path / 'raw' / str(SEASON)
    daily_path = raw_path / 'daily'
    monthly_path = raw_path / 'monthly'
    agg_path = base_path / 'aggregated'

    daily_path.mkdir(parents=True, exist_ok=True)
    monthly_path.mkdir(parents=True, exist_ok=True)
    agg_path.mkdir(parents=True, exist_ok=True)

    # Tracker file path (used by cleanup + main loop below)
    tracker_file = base_path / 'last_update.json'

    def rewind_tracker():
        """Forget the last fetched date (so fetching restarts at Opening Day) but keep the other flags."""
        if not tracker_file.exists():
            return False
        try:
            with open(tracker_file) as f:
                kept = json.load(f)
            kept.pop('last_date', None)
            with open(tracker_file, 'w') as f:
                json.dump(kept, f)
        except Exception:
            tracker_file.unlink()
        return True

    # ── Auto-cleanup: detect daily files written by the OLD broken script ──
    # The old version wrote ~11 columns. The new version writes ~50.
    # Any file with the old schema gets deleted so it'll be re-fetched fresh.
    # Also detects files where pfx_x/pfx_z were wrongly stored in inches (legacy bug).
    # 'play_id' added 2026-06: its absence triggers a one-time full-season re-fetch
    # so every historical pitch gains the video/research deep-link UUID.
    # 'on_1b' added 2026-11 (runners on base before each pitch, plus the corrected
    # pre-pitch count): its absence triggers the same one-time full-season re-fetch.
    REQUIRED_COLUMNS = {'ax', 'ay', 'az', 'start_speed', 'spin_rate', 'pfx_x', 'pfx_z', 'play_id', 'on_1b'}
    deleted_count = 0
    for daily_file in sorted(daily_path.glob('*.parquet')):
        try:
            schema = pq.read_schema(daily_file)
            cols = set(schema.names)
            if not REQUIRED_COLUMNS.issubset(cols):
                print(f"  [cleanup] Deleting {daily_file.name} - missing columns from old schema")
                daily_file.unlink()
                deleted_count += 1
                continue
            # Check if pfx values look wrong (in inches when they should be in feet).
            # MLB pfx values in feet are typically -2 to +2; in inches they'd be -24 to +24.
            try:
                t = pq.read_table(daily_file, columns=['pfx_x', 'pfx_z'])
                df = t.to_pandas()
                vals = df.dropna()
                if len(vals) > 10:
                    max_abs = max(vals['pfx_x'].abs().max(), vals['pfx_z'].abs().max())
                    if max_abs > 5:  # >5 feet means values are in inches (bug)
                        print(f"  [cleanup] Deleting {daily_file.name} - pfx values look like inches (max={max_abs:.1f})")
                        daily_file.unlink()
                        deleted_count += 1
                        continue
            except Exception as inner_e:
                print(f"  [cleanup] Could not check pfx values for {daily_file.name}: {inner_e}")
        except Exception as e:
            print(f"  [cleanup] Could not read {daily_file.name}: {e} - deleting")
            try:
                daily_file.unlink()
                deleted_count += 1
            except Exception:
                pass
    if deleted_count > 0:
        print(f"  [cleanup] Removed {deleted_count} stale daily files - they'll be re-fetched")
        # Rewind the tracker so the missing days are re-fetched (days that still have a file are skipped)
        if rewind_tracker():
            print(f"  [cleanup] Reset tracker - will re-fetch from {REGULAR_SEASON_START}")

    # ── Also force re-fetch if Opening Day (March 25-26) parquets are missing ──
    # This handles the case where the previous version of the script had the wrong
    # REGULAR_SEASON_START and skipped these dates entirely.
    expected_opening = ['2026-03-25', '2026-03-26']
    missing_opening = [d for d in expected_opening if not (daily_path / f"{d}.parquet").exists()]
    if missing_opening:
        print(f"  [cleanup] Missing Opening Day parquets: {missing_opening} - resetting tracker")
        rewind_tracker()

    # Read tracker to determine where to start fetching from
    if tracker_file.exists():
        with open(tracker_file) as f:
            tracker = json.load(f)
        last_date = datetime.strptime(tracker.get('last_date', '2026-03-24'), '%Y-%m-%d')
    else:
        tracker = {}
        last_date = datetime(2026, 3, 24)  # Day before Opening Day

    # ── One-time postseason backfill (Oct 2026) ──
    # Earlier versions only ingested gameType 'R', so every postseason game was
    # discarded while the tracker kept advancing day by day. If the tracker is
    # already past the end of the regular season and the backfill hasn't run,
    # rewind the start once to the final regular-season day. Existing daily
    # parquets are skipped by the loop below, so this only fetches the missing
    # postseason dates and then sets the flag.
    POSTSEASON_RESUME = datetime(2026, 9, 27)
    if last_date > POSTSEASON_RESUME and not tracker.get('postseason_backfill_done'):
        print("  [cleanup] Postseason backfill: rewinding fetch start to 2026-09-28")
        last_date = POSTSEASON_RESUME

    today = datetime.now(CENTRAL_TZ).replace(tzinfo=None)
    current_date = last_date + timedelta(days=1)

    # ── Every run re-fetches today and the previous 3 days ──
    # Catches games that were "Final" by the time we ran but had partial Statcast
    # measurements that get filled in later. Don't trust a final-game snapshot.
    refetch_window_start = (today - timedelta(days=3)).replace(hour=0, minute=0, second=0, microsecond=0)
    current_date = min(current_date, refetch_window_start)

    # ── Full-season refresh: weekly while games are being played, or on request ──
    # MLB revises older games too (pitch types get reclassified weeks later), so the
    # whole season is re-fetched once a week instead of on every run. FULL_REFRESH=1
    # (the "full_refresh" box on the workflow's Run button) forces one now.
    # Existing daily files are only ever replaced by a complete re-fetch of that day.
    existing_days = sorted(f.stem for f in daily_path.glob('*.parquet'))
    season_active = bool(existing_days) and (today - datetime.strptime(existing_days[-1], '%Y-%m-%d')).days <= 21
    last_full = tracker.get('last_full_refresh')
    days_since_full = (today - datetime.strptime(last_full, '%Y-%m-%d')).days if last_full else None
    full_refresh = os.environ.get('FULL_REFRESH', '').strip().lower() in ('1', 'true', 'yes')
    if not full_refresh and season_active and (days_since_full is None or days_since_full >= 7):
        full_refresh = True
    if full_refresh:
        print("Full-season refresh this run")
        current_date = datetime.strptime(REGULAR_SEASON_START, '%Y-%m-%d')
    refresh_complete = True

    # ── Fetch each day from the start date through today ──
    while current_date <= today:
        date_str = current_date.strftime('%Y-%m-%d')

        if date_str < REGULAR_SEASON_START:
            current_date += timedelta(days=1)
            continue

        daily_file = daily_path / f"{date_str}.parquet"
        is_today = (current_date.date() == today.date())
        is_recent = current_date >= refetch_window_start

        # Skip only if the file exists AND is older than the recent window
        if daily_file.exists() and not is_today and not is_recent and not full_refresh:
            print(f"Skipping {date_str} (already have, outside refetch window)")
            current_date += timedelta(days=1)
            continue

        print(f"Fetching games for {date_str}...")
        day_pitches = []
        failed_before = len(FAILED_FEEDS)
        game_ids = get_schedule(date_str)
        for game_id in game_ids:
            pitches = get_pitch_data(game_id)
            day_pitches.extend(pitches)
            print(f"  Game {game_id}: {len(pitches)} pitches")
        day_failed = len(FAILED_FEEDS) > failed_before

        if day_failed and daily_file.exists():
            # A game's feed could not be fetched: keep the file we already have rather
            # than replace it with a day that is missing a game.
            print(f"  Kept existing {daily_file.name} (a game feed failed this run)")
            refresh_complete = False
        elif day_pitches:
            pq.write_table(pa.Table.from_pylist(day_pitches), daily_file)
            print(f"  Wrote {len(day_pitches)} pitches to {daily_file}")
        elif is_today:
            print(f"  No completed games yet for {date_str}")
        else:
            print(f"  No games found for {date_str}")

        current_date += timedelta(days=1)

    # Stamp the weekly refresh only when every day came back whole; otherwise the next run retries.
    last_full_refresh = today.strftime('%Y-%m-%d') if (full_refresh and refresh_complete) else last_full

    # ── A postponed game stays on the schedule of its original date, so its pitches
    # land in two daily files: the original date and the date it was played. Find the
    # games that have a copy under their own official date; the stray copy is dropped
    # from the monthly files and the aggregates below (never from the daily files, so
    # no game can be lost).
    home_games = set()
    for daily_file in sorted(daily_path.glob('*.parquet')):
        try:
            t = pq.read_table(daily_file, columns=['game_pk', 'game_date'])
            for pk, gd in set(zip(t.column('game_pk').to_pylist(), t.column('game_date').to_pylist())):
                if gd == daily_file.stem:
                    home_games.add(pk)
        except Exception as e:
            print(f"  Error scanning {daily_file}: {e}")

    def own_rows(daily_file, rows):
        """Rows of a daily file minus games that belong to (and exist under) another date."""
        day = daily_file.stem
        return [r for r in rows if r.get('game_date') == day or r.get('game_pk') not in home_games]

    # ── Rebuild monthly parquets from all dailies ──
    print("\nRebuilding monthly parquets...")
    monthly_data = {}
    for daily_file in sorted(daily_path.glob('*.parquet')):
        try:
            date_str = daily_file.stem
            month_str = date_str[:7]
            table = pq.read_table(daily_file)
            rows = own_rows(daily_file, table.to_pylist())
            if month_str not in monthly_data:
                monthly_data[month_str] = []
            monthly_data[month_str].extend(rows)
        except Exception as e:
            print(f"  Error reading {daily_file}: {e}")

    for month_str, pitches in monthly_data.items():
        try:
            month_num = int(month_str.split('-')[1])
            month_name = MONTH_NAMES.get(month_num, f'Month{month_num}')
            month_file = monthly_path / f"{month_num:02d}_{month_name.lower()}.parquet"
            pq.write_table(pa.Table.from_pylist(pitches), month_file)
            print(f"  Wrote {len(pitches)} pitches to {month_file}")
        except Exception as e:
            print(f"  Error writing month {month_str}: {e}")

    # ── Build aggregated pitch_usage_by_count.json (downstream apps use this) ──
    print("\nBuilding aggregated pitch usage data...")
    all_data = []
    for daily_file in sorted(daily_path.glob('*.parquet')):
        try:
            table = pq.read_table(daily_file)
            rows = own_rows(daily_file, table.to_pylist())
            for r in rows:
                # Only include rows with the fields needed by aggregation
                if r.get('stand') and r.get('pitch_type') and r.get('count') in VALID_COUNTS:
                    all_data.append(r)
        except Exception as e:
            print(f"  Error reading {daily_file}: {e}")

    print(f"Total qualifying pitches: {len(all_data)}")

    if not all_data:
        print("No valid data to aggregate. Writing empty output.")
        output = {
            'season': SEASON,
            'last_updated': datetime.now(CENTRAL_TZ).isoformat(),
            'total_pitches': 0,
            'total_pitchers': 0,
            'data': {},
            'monthly': {},
            'games': {}
        }
        output_file = agg_path / 'pitch_usage_by_count.json'
        with open(output_file, 'w') as f:
            json.dump(output, f)
        yesterday = today - timedelta(days=1)
        with open(tracker_file, 'w') as f:
            json.dump({
                'last_date': yesterday.strftime('%Y-%m-%d'),
                'last_run': datetime.now(CENTRAL_TZ).isoformat(),
                'postseason_backfill_done': True,
                'last_full_refresh': last_full_refresh
            }, f)
        return

    # ── Aggregate by pitcher / hand / pitch_type / count ──
    season_data = {}
    monthly_data_agg = {}
    games_data = {}

    for pitch in all_data:
        pitcher = pitch.get('pitcher_name', '')
        stand = pitch.get('stand', '')
        pitch_type = pitch.get('pitch_type', '')
        count = pitch.get('count', '')
        game_date = pitch.get('game_date', '')

        if not all([pitcher, stand, pitch_type, count, game_date]):
            continue

        try:
            month_num = int(game_date.split('-')[1])
        except (ValueError, IndexError):
            continue
        month_name = MONTH_NAMES.get(month_num, f'Month{month_num}')

        # Season aggregation
        season_data.setdefault(pitcher, {}).setdefault(stand, {}).setdefault(pitch_type, {})
        season_data[pitcher][stand][pitch_type][count] = season_data[pitcher][stand][pitch_type].get(count, 0) + 1

        # Monthly aggregation
        monthly_data_agg.setdefault(month_name, {}).setdefault(pitcher, {}).setdefault(stand, {}).setdefault(pitch_type, {})
        monthly_data_agg[month_name][pitcher][stand][pitch_type][count] = monthly_data_agg[month_name][pitcher][stand][pitch_type].get(count, 0) + 1

        # Game-by-game aggregation (innings as a set)
        games_data.setdefault(pitcher, {}).setdefault(game_date, {'usage': {}, 'innings': set()})
        usage = games_data[pitcher][game_date]['usage']
        usage.setdefault(stand, {}).setdefault(pitch_type, {})
        usage[stand][pitch_type][count] = usage[stand][pitch_type].get(count, 0) + 1

        inning = pitch.get('inning', 0)
        half_inning = pitch.get('half_inning', '')
        if inning and half_inning:
            games_data[pitcher][game_date]['innings'].add((inning, half_inning))

    # Build games output
    games_output = {}
    for pitcher, dates in games_data.items():
        sorted_dates = sorted(dates.keys())
        games_list = []
        for date in sorted_dates:
            game_info = dates[date]
            usage = game_info['usage']
            innings_set = game_info['innings']
            pitch_count = sum(
                cv for sd in usage.values() for pd_ in sd.values() for cv in pd_.values()
            )
            innings_pitched = len(innings_set)
            games_list.append({
                'date': date,
                'pitches': pitch_count,
                'innings': innings_pitched,
                'usage': usage,
            })
        games_output[pitcher] = {'games': games_list}

    def count_pitches(pdata):
        total = 0
        for sd in pdata.values():
            for pt_data in sd.values():
                for cv in pt_data.values():
                    total += cv
        return total

    qualified_season = {p: d for p, d in season_data.items() if count_pitches(d) >= MIN_PITCHES_SEASON}
    qualified_monthly = {
        m: {p: d for p, d in pp.items() if count_pitches(d) >= MIN_PITCHES_MONTH}
        for m, pp in monthly_data_agg.items()
    }

    total_innings = sum(
        g.get('innings', 0)
        for pdata in games_output.values()
        for g in pdata.get('games', [])
    )

    output = {
        'season': SEASON,
        'last_updated': datetime.now(CENTRAL_TZ).isoformat(),
        'total_pitches': len(all_data),
        'total_pitchers': len(qualified_season),
        'total_innings': total_innings,
        'data': qualified_season,
        'monthly': qualified_monthly,
        'games': games_output,
    }

    output_file = agg_path / 'pitch_usage_by_count.json'
    with open(output_file, 'w') as f:
        json.dump(output, f)

    print(f"\nWrote aggregated data to {output_file}")
    print(f"  Season qualified pitchers: {len(qualified_season)}")
    print(f"  Total innings: {total_innings}")
    for month, pitchers in qualified_monthly.items():
        print(f"  {month}: {len(pitchers)} pitchers")

    # Update tracker
    yesterday = today - timedelta(days=1)
    with open(tracker_file, 'w') as f:
        json.dump({
            'last_date': yesterday.strftime('%Y-%m-%d'),
            'last_run': datetime.now(CENTRAL_TZ).isoformat(),
            'postseason_backfill_done': True,
            'last_full_refresh': last_full_refresh
        }, f)


if __name__ == '__main__':
    main()
