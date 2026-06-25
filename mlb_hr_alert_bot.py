import os
import json
import asyncio
import logging
import random
from pathlib import Path
from datetime import datetime, timedelta
from urllib.parse import quote_plus

import discord
import requests
from zoneinfo import ZoneInfo

DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")
DISCORD_CHANNEL_ID = int(os.getenv("DISCORD_CHANNEL_ID", "0"))
DISCORD_STRIKEOUT_CHANNEL_ID = int(os.getenv("DISCORD_STRIKEOUT_CHANNEL_ID", "0"))
DISCORD_HARD_HIT_CHANNEL_ID = int(os.getenv("DISCORD_HARD_HIT_CHANNEL_ID", "0"))
DISCORD_PITCHER_WEAKSPOT_CHANNEL_ID = int(os.getenv("DISCORD_PITCHER_WEAKSPOT_CHANNEL_ID", "0"))
DISCORD_NEAR_HR_CHANNEL_ID = int(os.getenv("DISCORD_NEAR_HR_CHANNEL_ID", "0"))
DISCORD_CYCLE_CHANNEL_ID = int(os.getenv("DISCORD_CYCLE_CHANNEL_ID", "0"))
ENABLE_CYCLE_WATCH = os.getenv("ENABLE_CYCLE_WATCH", "true").lower() == "true"
CYCLE_WATCH_MIN_INNING = int(os.getenv("CYCLE_WATCH_MIN_INNING", "5"))
CYCLE_WATCH_MIN_LEGS = int(os.getenv("CYCLE_WATCH_MIN_LEGS", "2"))
FANDUEL_MLB_DEEPLINK = os.getenv("FANDUEL_MLB_DEEPLINK", "https://sportsbook.fanduel.com/navigation/mlb")
FANDUEL_HR_DEEPLINK = os.getenv("FANDUEL_HR_DEEPLINK", FANDUEL_MLB_DEEPLINK)
FANDUEL_TRIPLE_DEEPLINK = os.getenv("FANDUEL_TRIPLE_DEEPLINK", FANDUEL_MLB_DEEPLINK)
FANDUEL_DOUBLE_DEEPLINK = os.getenv("FANDUEL_DOUBLE_DEEPLINK", FANDUEL_MLB_DEEPLINK)
FANDUEL_HIT_DEEPLINK = os.getenv("FANDUEL_HIT_DEEPLINK", FANDUEL_MLB_DEEPLINK)

# Optional comma-separated FanDuel link pools.
# The bot will include every configured candidate link for the remaining prop.
FANDUEL_HR_DEEPLINKS = os.getenv("FANDUEL_HR_DEEPLINKS", "")
FANDUEL_TRIPLE_DEEPLINKS = os.getenv("FANDUEL_TRIPLE_DEEPLINKS", "")
FANDUEL_DOUBLE_DEEPLINKS = os.getenv("FANDUEL_DOUBLE_DEEPLINKS", "")
FANDUEL_HIT_DEEPLINKS = os.getenv("FANDUEL_HIT_DEEPLINKS", "")
ENABLE_CYCLE_FANDUEL_ODDS = os.getenv("ENABLE_CYCLE_FANDUEL_ODDS", "false").lower() == "true"
CYCLE_FANDUEL_BOOKMAKER_KEY = os.getenv("CYCLE_FANDUEL_BOOKMAKER_KEY", "fanduel")
CYCLE_MARKET_HR = os.getenv("CYCLE_MARKET_HR", "batter_home_runs")
CYCLE_MARKET_TRIPLE = os.getenv("CYCLE_MARKET_TRIPLE", "batter_triples")
CYCLE_MARKET_DOUBLE = os.getenv("CYCLE_MARKET_DOUBLE", "batter_doubles")
CYCLE_MARKET_HIT = os.getenv("CYCLE_MARKET_HIT", "batter_hits")
CYCLE_ODDS_CACHE_SECONDS = int(os.getenv("CYCLE_ODDS_CACHE_SECONDS", "60"))
cycle_odds_cache = {}

ENABLE_STRIKEOUT_ALERTS = os.getenv("ENABLE_STRIKEOUT_ALERTS", "false").lower() == "true"
STRIKEOUT_ALERT_MIN_KS = int(os.getenv("STRIKEOUT_ALERT_MIN_KS", "3"))
STRIKEOUT_ALERT_MAX_INNING = int(os.getenv("STRIKEOUT_ALERT_MAX_INNING", "2"))
STRIKEOUT_EXTENDED_MIN_KS = int(os.getenv("STRIKEOUT_EXTENDED_MIN_KS", "5"))
STRIKEOUT_EXTENDED_MAX_INNING = int(os.getenv("STRIKEOUT_EXTENDED_MAX_INNING", "4"))
POLL_SECONDS = int(os.getenv("POLL_SECONDS", "15"))
TIMEZONE = os.getenv("TIMEZONE", "America/Chicago")
REPORT_BUILD_TIMEOUT_SECONDS = int(os.getenv("REPORT_BUILD_TIMEOUT_SECONDS", "120"))
SKIP_OLD_PLAYS_ON_STARTUP = os.getenv("SKIP_OLD_PLAYS_ON_STARTUP", "true").lower() == "true"
OLD_PLAY_MAX_MINUTES = int(os.getenv("OLD_PLAY_MAX_MINUTES", "10"))
DISABLE_STARTUP_MESSAGE = os.getenv("DISABLE_STARTUP_MESSAGE", "false").lower() == "true"

DAILY_RECAP_HOUR = int(os.getenv("DAILY_RECAP_HOUR", "8"))
DAILY_RECAP_MINUTE = int(os.getenv("DAILY_RECAP_MINUTE", "0"))
DAILY_RECAP_CATCHUP_HOURS = int(os.getenv("DAILY_RECAP_CATCHUP_HOURS", "6"))
HOT_STREAK_DAYS = int(os.getenv("HOT_STREAK_DAYS", "7"))
HOT_STREAK_TOP_N = int(os.getenv("HOT_STREAK_TOP_N", "8"))

ENABLE_2HR_WATCH = os.getenv("ENABLE_2HR_WATCH", "true").lower() == "true"
TWO_HR_WATCH_TOP_N = int(os.getenv("TWO_HR_WATCH_TOP_N", "6"))
TWO_HR_MIN_SCORE = float(os.getenv("TWO_HR_MIN_SCORE", "35"))
ENABLE_BIRTHDAY_NARRATIVE = os.getenv("ENABLE_BIRTHDAY_NARRATIVE", "true").lower() == "true"

# No HR through 3 innings roast alert
ENABLE_NO_HR_THROUGH_3_ALERT = os.getenv("ENABLE_NO_HR_THROUGH_3_ALERT", "true").lower() == "true"
ENABLE_HARD_HIT_TRACKER = os.getenv("ENABLE_HARD_HIT_TRACKER", "true").lower() == "true"
ENABLE_PITCHER_WEAKSPOT_ALERTS = os.getenv("ENABLE_PITCHER_WEAKSPOT_ALERTS", "true").lower() == "true"
ENABLE_BVP_HR_HISTORY = os.getenv("ENABLE_BVP_HR_HISTORY", "true").lower() == "true"
HARD_HIT_EV_THRESHOLD = float(os.getenv("HARD_HIT_EV_THRESHOLD", "100"))
ELITE_HARD_HIT_EV = float(os.getenv("ELITE_HARD_HIT_EV", "110"))

# Tightened Hard-Hit Tracker profile gate
HARD_HIT_PROFILE_MIN_CHECKS = int(os.getenv("HARD_HIT_PROFILE_MIN_CHECKS", "5"))
HARD_HIT_WATCHLIST_MIN_CHECKS = int(os.getenv("HARD_HIT_WATCHLIST_MIN_CHECKS", "3"))
PROFILE_AVG_EV_MIN = float(os.getenv("PROFILE_AVG_EV_MIN", "92"))
PROFILE_MAX_EV_MIN = float(os.getenv("PROFILE_MAX_EV_MIN", "110"))
PROFILE_BARREL_MIN = float(os.getenv("PROFILE_BARREL_MIN", "12"))
PROFILE_HARD_HIT_MIN = float(os.getenv("PROFILE_HARD_HIT_MIN", "45"))
PROFILE_FLY_BALL_MIN = float(os.getenv("PROFILE_FLY_BALL_MIN", "38"))
PROFILE_ISO_MIN = float(os.getenv("PROFILE_ISO_MIN", ".220"))
PROFILE_LA_MIN = float(os.getenv("PROFILE_LA_MIN", "15"))
PROFILE_LA_MAX = float(os.getenv("PROFILE_LA_MAX", "20"))
PROFILE_PITCHER_HR9_MIN = float(os.getenv("PROFILE_PITCHER_HR9_MIN", "1.3"))

# Odds API: delayed "more HR" follow-up after a player homers
ODDS_API_KEY = os.getenv("ODDS_API_KEY", "")
ENABLE_MORE_HR_ODDS = os.getenv("ENABLE_MORE_HR_ODDS", "false").lower() == "true"
MORE_HR_ODDS_DELAY_SECONDS = int(os.getenv("MORE_HR_ODDS_DELAY_SECONDS", "90"))
MORE_HR_MIN_BOOKS = int(os.getenv("MORE_HR_MIN_BOOKS", "1"))
ODDS_REGION = os.getenv("ODDS_REGION", "us")
ODDS_FORMAT = os.getenv("ODDS_FORMAT", "american")
ODDS_BOOKMAKERS = os.getenv("ODDS_BOOKMAKERS", "draftkings,fanduel,betmgm")
ODDS_CACHE_MINUTES = int(os.getenv("ODDS_CACHE_MINUTES", "60"))
ODDS_API_BASE = "https://api.the-odds-api.com/v4"
ODDS_SPORT_KEY = "baseball_mlb"

# SportsGameOdds for live Cycle Watch FanDuel props/deep links
SPORTSGAMEODDS_API_KEY = os.getenv("SPORTSGAMEODDS_API_KEY", os.getenv("SGO_API_KEY", ""))
SPORTSGAMEODDS_API_BASE = os.getenv("SPORTSGAMEODDS_API_BASE", "https://api.sportsgameodds.com/v2").rstrip("/")
SPORTSGAMEODDS_MLB_LEAGUE_ID = os.getenv("SPORTSGAMEODDS_MLB_LEAGUE_ID", os.getenv("SGO_MLB_LEAGUE_ID", ""))
SPORTSGAMEODDS_FANDUEL_BOOKMAKER_ID = os.getenv("SPORTSGAMEODDS_FANDUEL_BOOKMAKER_ID", "fanduel")
ENABLE_CYCLE_SGO_ODDS = os.getenv("ENABLE_CYCLE_SGO_ODDS", "true").lower() == "true"
CYCLE_SGO_LOOKBACK_MINUTES = int(os.getenv("CYCLE_SGO_LOOKBACK_MINUTES", "0"))
CYCLE_SGO_LOOKAHEAD_HOURS = int(os.getenv("CYCLE_SGO_LOOKAHEAD_HOURS", "16"))
CYCLE_SGO_CACHE_SECONDS = int(os.getenv("CYCLE_SGO_CACHE_SECONDS", "45"))
CYCLE_SGO_MARKET_HR_KEYWORDS = os.getenv("CYCLE_SGO_MARKET_HR_KEYWORDS", "home run,homer,home_runs,batter_home_runs")
CYCLE_SGO_MARKET_TRIPLE_KEYWORDS = os.getenv("CYCLE_SGO_MARKET_TRIPLE_KEYWORDS", "triple,triples,batter_triples")
CYCLE_SGO_MARKET_DOUBLE_KEYWORDS = os.getenv("CYCLE_SGO_MARKET_DOUBLE_KEYWORDS", "double,doubles,batter_doubles")
CYCLE_SGO_MARKET_HIT_KEYWORDS = os.getenv("CYCLE_SGO_MARKET_HIT_KEYWORDS", "hit,hits,batter_hits")
cycle_sgo_cache = {}
cycle_sgo_events_cache = {"ts": 0, "events": []}
cycle_sgo_event_odds_cache = {}
CYCLE_SGO_EVENTS_CACHE_SECONDS = int(os.getenv("CYCLE_SGO_EVENTS_CACHE_SECONDS", "300"))
CYCLE_SGO_EVENT_ODDS_CACHE_SECONDS = int(os.getenv("CYCLE_SGO_EVENT_ODDS_CACHE_SECONDS", "60"))

# Morning HR parlay section in daily report
ENABLE_MORNING_HR_PARLAYS = os.getenv("ENABLE_MORNING_HR_PARLAYS", "true").lower() == "true"
MORNING_PARLAY_LEGS_SAFE = int(os.getenv("MORNING_PARLAY_LEGS_SAFE", "2"))
MORNING_PARLAY_LEGS_RISKY = int(os.getenv("MORNING_PARLAY_LEGS_RISKY", "3"))
MORNING_PARLAY_LEGS_BOMB = int(os.getenv("MORNING_PARLAY_LEGS_BOMB", "4"))

# HR parlay scoring weights
HR_PARLAY_EV_WEIGHT = float(os.getenv("HR_PARLAY_EV_WEIGHT", "1.35"))
HR_PARLAY_NEAR_HR_WEIGHT = float(os.getenv("HR_PARLAY_NEAR_HR_WEIGHT", "10"))
HR_PARLAY_BALLPARK_WEIGHT = float(os.getenv("HR_PARLAY_BALLPARK_WEIGHT", "1.0"))



NEAR_HR_MIN_EV = float(os.getenv("NEAR_HR_MIN_EV", "102"))
NEAR_HR_MIN_ANGLE = float(os.getenv("NEAR_HR_MIN_ANGLE", "22"))
NEAR_HR_MAX_ANGLE = float(os.getenv("NEAR_HR_MAX_ANGLE", "38"))
NEAR_HR_MIN_DISTANCE = float(os.getenv("NEAR_HR_MIN_DISTANCE", "375"))
NEAR_HR_CONFIRM_DELAY = float(os.getenv("NEAR_HR_CONFIRM_DELAY", "4"))

STATE_FILE = Path(os.getenv("STATE_FILE", "/data/mlb_hr_alert_state.json"))
SCHEDULE_URL = "https://statsapi.mlb.com/api/v1/schedule?sportId=1&date={date}"
LIVE_FEED_URL = "https://statsapi.mlb.com/api/v1.1/game/{gamePk}/feed/live"
BOXSCORE_URL = "https://statsapi.mlb.com/api/v1/game/{gamePk}/boxscore"
TEAM_ROSTER_URL = "https://statsapi.mlb.com/api/v1/teams/{teamId}/roster?rosterType=active"
PEOPLE_URL = "https://statsapi.mlb.com/api/v1/people?personIds={personIds}"
PITCHER_STATS_URL = "https://statsapi.mlb.com/api/v1/people/{personId}/stats?stats=season&group=pitching&season={season}"

TZ = ZoneInfo(TIMEZONE)
session = requests.Session()
loop_task = None
live_loop_started = False
processing_play_ids = set()
processing_near_play_ids = set()
morning_hr_odds_cache = {"fetched_at": None, "rows": []}
daily_recap_running = False
processed_message_ids = set()
hard_hit_tracker = {}
pitcher_hard_hit_tracker = {}
bvp_hr_history_cache = {}

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
log = logging.getLogger("mlb_hr_alert_bot")

intents = discord.Intents.default()
intents.messages = True
intents.message_content = True
client = discord.Client(intents=intents)

discord_send_lock = None
DISCORD_SEND_DELAY_SECONDS = float(os.getenv("DISCORD_SEND_DELAY_SECONDS", "1.35"))

state = {
    "seen_hr_play_ids": [],
    "seen_near_hr_play_ids": [],
    "pending_near_hr_play_ids": [],
    "last_startup_date": None,
    "last_daily_recap_date": None,
    "last_schedule_check_minute": None,
    "seen_strikeout_alerts": [],
    "seen_no_hr_3rd_game_ids": [],
    "seen_more_hr_odds_keys": [],
    "seen_hard_hit_alerts": [],
    "seen_hr_alerts": [],
    "seen_pitcher_weakspot_alerts": [],
    "seen_cycle_alerts": [],
}

TEAM_COLORS = {
    "AZ": 0xA71930, "ATL": 0xCE1141, "BAL": 0xDF4601, "BOS": 0xBD3039,
    "CHC": 0x0E3386, "CWS": 0x27251F, "CIN": 0xC6011F, "CLE": 0xE31937,
    "COL": 0x33006F, "DET": 0x0C2340, "HOU": 0xEB6E1F, "KC": 0x004687,
    "LAA": 0xBA0021, "LAD": 0x005A9C, "MIA": 0x00A3E0, "MIL": 0x12284B,
    "MIN": 0x002B5C, "NYM": 0x002D72, "NYY": 0x132448, "ATH": 0x003831,
    "OAK": 0x003831, "PHI": 0xE81828, "PIT": 0xFDB827, "SD": 0x2F241D,
    "SEA": 0x005C5C, "SF": 0xFD5A1E, "STL": 0xC41E3A, "TB": 0x092C5C,
    "TEX": 0x003278, "TOR": 0x134A8E, "WSH": 0xAB0003,
}

TEAM_LOGO_SLUGS = {
    "AZ": "ari", "ATL": "atl", "BAL": "bal", "BOS": "bos", "CHC": "chc", "CWS": "chw",
    "CIN": "cin", "CLE": "cle", "COL": "col", "DET": "det", "HOU": "hou", "KC": "kc",
    "LAA": "laa", "LAD": "lad", "MIA": "mia", "MIL": "mil", "MIN": "min", "NYM": "nym",
    "NYY": "nyy", "ATH": "oak", "OAK": "oak", "PHI": "phi", "PIT": "pit", "SD": "sd",
    "SEA": "sea", "SF": "sf", "STL": "stl", "TB": "tb", "TEX": "tex", "TOR": "tor", "WSH": "wsh",
}


def require_env():
    if not DISCORD_TOKEN:
        raise RuntimeError("Missing DISCORD_TOKEN")
    if not DISCORD_CHANNEL_ID:
        raise RuntimeError("Missing DISCORD_CHANNEL_ID")


def load_state():
    global state
    if STATE_FILE.exists():
        try:
            state = json.loads(STATE_FILE.read_text())
        except Exception as exc:
            log.warning("Could not load state file: %s", exc)
    state.setdefault("seen_hr_play_ids", [])
    state.setdefault("seen_near_hr_play_ids", [])
    state.setdefault("pending_near_hr_play_ids", [])
    state.setdefault("last_startup_date", None)
    state.setdefault("last_daily_recap_date", None)
    state.setdefault("last_schedule_check_minute", None)
    state.setdefault("seen_strikeout_alerts", [])
    state.setdefault("seen_no_hr_3rd_game_ids", [])
    state.setdefault("seen_more_hr_odds_keys", [])
    state.setdefault("seen_hard_hit_alerts", [])
    state.setdefault("seen_hr_alerts", [])
    state.setdefault("seen_pitcher_weakspot_alerts", [])
    state.setdefault("seen_cycle_alerts", [])


def save_state():
    state["seen_hr_play_ids"] = state["seen_hr_play_ids"][-500:]
    state["seen_near_hr_play_ids"] = state["seen_near_hr_play_ids"][-1000:]
    state["pending_near_hr_play_ids"] = state["pending_near_hr_play_ids"][-1000:]
    state["seen_strikeout_alerts"] = state.get("seen_strikeout_alerts", [])[-500:]
    state["seen_hr_alerts"] = state.get("seen_hr_alerts", [])[-500:]
    state["seen_cycle_alerts"] = state.get("seen_cycle_alerts", [])[-500:]
    for extra_key in ("seen_no_hr_3rd_game_ids", "seen_more_hr_odds_keys", "seen_pregame_parlay_game_ids"):
        if extra_key in state:
            state[extra_key] = state[extra_key][-500:]
    state["seen_no_hr_3rd_game_ids"] = state["seen_no_hr_3rd_game_ids"][-500:]
    state["seen_more_hr_odds_keys"] = state["seen_more_hr_odds_keys"][-500:]
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(state, indent=2))


def get_json(url):
    r = session.get(url, timeout=25)
    r.raise_for_status()
    return r.json()



def get_odds_json(url: str, params: dict) -> dict:
    resp = session.get(url, params=params, timeout=25)
    resp.raise_for_status()
    return resp.json()


def today_str():
    return datetime.now(TZ).strftime("%Y-%m-%d")


def day_str(days_ago):
    return (datetime.now(TZ) - timedelta(days=days_ago)).strftime("%Y-%m-%d")


def yesterday_str():
    return day_str(1)


def get_current_season():
    return datetime.now(TZ).strftime("%Y")


def safe_float(value, default=0.0):
    try:
        if value in (None, "", "-.--"):
            return default
        return float(value)
    except Exception:
        return default


def game_label(game):
    away = game.get("away") or {}
    home = game.get("home") or {}
    away_name = away.get("name") or away.get("teamName") or away.get("abbreviation") or "Away"
    home_name = home.get("name") or home.get("teamName") or home.get("abbreviation") or "Home"
    return f"{away_name} @ {home_name}"


def team_logo(team_abbr):
    slug = TEAM_LOGO_SLUGS.get(team_abbr)
    return f"https://a.espncdn.com/i/teamlogos/mlb/500/{slug}.png" if slug else None


def player_headshot(player_id):
    return f"https://img.mlbstatic.com/mlb-photos/image/upload/w_180/v1/people/{player_id}/headshot/67/current" if player_id else None


def ordinal(n):
    if 10 <= n % 100 <= 20:
        suffix = "th"
    else:
        suffix = {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th")
    return f"{n}{suffix}"


def age_on_date(birth_date_str, date_obj):
    try:
        birth = datetime.strptime(birth_date_str, "%Y-%m-%d")
    except Exception:
        return None
    age = date_obj.year - birth.year
    if (date_obj.month, date_obj.day) < (birth.month, birth.day):
        age -= 1
    return age


def get_games_for_date(date_str):
    data = get_json(SCHEDULE_URL.format(date=date_str))
    games = []
    for date_block in data.get("dates", []):
        for game in date_block.get("games", []):
            games.append({
                "gamePk": game.get("gamePk"),
                "away": ((game.get("teams") or {}).get("away") or {}).get("team", {}),
                "home": ((game.get("teams") or {}).get("home") or {}).get("team", {}),
                "status": (game.get("status") or {}).get("detailedState", ""),
            })
    return games


def get_today_games():
    return get_games_for_date(today_str())


def build_play_id(game_pk, play):
    return make_stable_play_key(game_pk, play)

def is_home_run(play):
    result = play.get("result", {})
    event_type = (result.get("eventType") or "").lower().strip()
    event = (result.get("event") or "").lower().strip()
    description = (result.get("description") or "").lower().strip()
    return event_type == "home_run" or "home run" in event or "homer" in event or "home run" in description or "homers" in description or "grand slam" in description


def get_metrics(play):
    for event in reversed(play.get("playEvents", [])):
        hit_data = event.get("hitData")
        if hit_data:
            return hit_data
    return None


def is_near_hr(play):
    if is_home_run(play):
        return False
    metrics = get_metrics(play)
    if not metrics:
        return False
    ev = metrics.get("launchSpeed")
    la = metrics.get("launchAngle")
    dist = metrics.get("totalDistance")
    if ev is None or la is None or dist is None:
        return False
    return ev >= NEAR_HR_MIN_EV and NEAR_HR_MIN_ANGLE <= la <= NEAR_HR_MAX_ANGLE and dist >= NEAR_HR_MIN_DISTANCE


def player_hr_number_in_game(all_plays, current_play):
    batter_id = ((current_play.get("matchup") or {}).get("batter") or {}).get("id")
    if not batter_id:
        return None
    current_at_bat = (current_play.get("about") or {}).get("atBatIndex", -1)
    hr_count = 0
    for play in all_plays:
        at_bat = (play.get("about") or {}).get("atBatIndex", -1)
        if at_bat > current_at_bat:
            continue
        play_batter_id = ((play.get("matchup") or {}).get("batter") or {}).get("id")
        if play_batter_id == batter_id and is_home_run(play):
            hr_count += 1
    return hr_count if hr_count > 0 else None



# =========================
# DUPLICATE / REDEPLOY PROTECTION
# =========================

def already_seen_or_claim(list_name: str, key: str) -> bool:
    if not key:
        return True

    state.setdefault(list_name, [])

    if key in state[list_name]:
        return True

    state[list_name].append(key)
    save_state()
    return False


def make_stable_play_key(game_pk, play):
    about = play.get("about", {}) or {}
    matchup = play.get("matchup", {}) or {}
    batter = matchup.get("batter", {}) or {}
    result = play.get("result", {}) or {}

    return "|".join([
        str(game_pk),
        str(about.get("atBatIndex", "")),
        str(batter.get("id", "")),
        str(about.get("inning", "")),
        str(about.get("halfInning", "")),
        str(result.get("eventType", "")),
    ])


def play_is_recent(play):
    """
    Prevents redeploy/startup from reposting old HRs if Railway state was wiped.
    Allows only plays from the last OLD_PLAY_MAX_MINUTES minutes.
    """
    if not SKIP_OLD_PLAYS_ON_STARTUP:
        return True

    about = play.get("about", {}) or {}
    raw_time = about.get("endTime") or about.get("startTime")

    if not raw_time:
        return True

    try:
        play_time = datetime.fromisoformat(raw_time.replace("Z", "+00:00"))
        now = datetime.now(play_time.tzinfo)
        age_minutes = (now - play_time).total_seconds() / 60
        return age_minutes <= OLD_PLAY_MAX_MINUTES
    except Exception:
        return True




# =========================
# DISCORD SEND RATE LIMIT PROTECTION
# =========================

async def safe_discord_send(channel, *args, **kwargs):
    global discord_send_lock

    if discord_send_lock is None:
        discord_send_lock = asyncio.Lock()

    async with discord_send_lock:
        try:
            msg = await channel.send(*args, **kwargs)
            await asyncio.sleep(DISCORD_SEND_DELAY_SECONDS)
            return msg
        except discord.HTTPException as exc:
            log.warning("Discord send failed/rate-limited: %s", exc)
            await asyncio.sleep(max(DISCORD_SEND_DELAY_SECONDS * 2, 3))
            return None


async def send_startup_message():
    if DISABLE_STARTUP_MESSAGE:
        return

    channel = await client.fetch_channel(DISCORD_CHANNEL_ID)
    msg_date = today_str()
    if state.get("last_startup_date") == msg_date:
        return
    await safe_discord_send(channel, "✅ MLB HR/Near-HR bot is online.")
    state["last_startup_date"] = msg_date
    save_state()


async def send_alert(channel, game, play, alert_type, all_plays=None):
    result = play.get("result", {})
    matchup = play.get("matchup", {})
    batter = matchup.get("batter", {})
    pitcher = matchup.get("pitcher", {})
    batter_name = batter.get("fullName", "Unknown")
    pitcher_name = pitcher.get("fullName", "Unknown")
    about = play.get("about", {})
    inning = about.get("inning", "?")
    half = (about.get("halfInning", "") or "").title()
    home_score = result.get("homeScore")
    away_score = result.get("awayScore")
    score_text = f"{away_score}-{home_score}" if away_score is not None and home_score is not None else "—"
    team = game["away"] if about.get("halfInning") == "top" else game["home"]
    team_abbr = team.get("abbreviation", "MLB")
    color_value = TEAM_COLORS.get(team_abbr, 0x1D428A)
    color = discord.Color.orange() if alert_type == "near" else discord.Color(color_value)
    title = "💣 Home Run" if alert_type == "hr" else "⚠️ Near Home Run"
    hr_number = None
    hr_number_text = None
    if alert_type == "hr" and all_plays:
        hr_number = player_hr_number_in_game(all_plays, play)
        if hr_number:
            hr_number_text = f"{ordinal(hr_number)} HR of the game"
    embed = discord.Embed(title=title, description=result.get("description", "No description available"), color=color, url=f"https://www.mlb.com/gameday/{game['gamePk']}")
    embed.add_field(name="Game", value=game_label(game), inline=False)
    embed.add_field(name="Batter", value=batter_name, inline=True)
    embed.add_field(name="Pitcher", value=pitcher_name, inline=True)
    if hr_number_text:
        embed.add_field(name="Milestone", value=hr_number_text, inline=True)
    embed.add_field(name="Inning", value=f"{half} {inning}", inline=True)
    embed.add_field(name="Score", value=score_text, inline=True)
    metrics = get_metrics(play)
    if metrics:
        parts = []
        if metrics.get("launchSpeed") is not None:
            parts.append(f'EV {metrics["launchSpeed"]}')
        if metrics.get("launchAngle") is not None:
            parts.append(f'LA {metrics["launchAngle"]}')
        if metrics.get("totalDistance") is not None:
            parts.append(f'{metrics["totalDistance"]} ft')
        if parts:
            embed.add_field(name="Contact", value=" | ".join(parts), inline=False)
    logo_url = team_logo(team_abbr)
    if logo_url:
        embed.set_thumbnail(url=logo_url)
    headshot_url = player_headshot(batter.get("id"))
    if headshot_url:
        embed.set_image(url=headshot_url)
    embed.set_footer(text="MLB live feed")
    await safe_discord_send(channel, embed=embed)

    if (
        alert_type == "hr"
        and hr_number in (1, 2)
        and ENABLE_MORE_HR_ODDS
        and "send_more_hr_odds_after_delay" in globals()
    ):
        asyncio.create_task(
            send_more_hr_odds_after_delay(
                channel=channel,
                game=game,
                player_name=batter_name,
                player_id=batter.get("id"),
                current_hr_count=hr_number,
            )
        )


async def confirm_and_send_near_hr(channel, game, play_id):
    await asyncio.sleep(NEAR_HR_CONFIRM_DELAY)
    try:
        data = get_json(LIVE_FEED_URL.format(gamePk=game["gamePk"]))
        plays = (((data.get("liveData") or {}).get("plays") or {}).get("allPlays") or [])
        refreshed_play = next((p for p in plays if build_play_id(game["gamePk"], p) == play_id), None)

        if not refreshed_play:
            if play_id in state["pending_near_hr_play_ids"]:
                state["pending_near_hr_play_ids"].remove(play_id)
            processing_near_play_ids.discard(play_id)
            save_state()
            return

        if is_home_run(refreshed_play):
            if play_id in state["pending_near_hr_play_ids"]:
                state["pending_near_hr_play_ids"].remove(play_id)

            if not already_seen_or_claim("seen_hr_play_ids", play_id):
                await send_alert(channel, game, refreshed_play, "hr", plays)

            processing_near_play_ids.discard(play_id)
            save_state()
            return

        if is_near_hr(refreshed_play):
            if play_id in state["pending_near_hr_play_ids"]:
                state["pending_near_hr_play_ids"].remove(play_id)

            if not already_seen_or_claim("seen_near_hr_play_ids", play_id):

                near_channel = await get_optional_alert_channel(
                    channel,
                    DISCORD_NEAR_HR_CHANNEL_ID,
                    "near-hr"
                )

                await send_alert(
                    near_channel,
                    game,
                    refreshed_play,
                    "near",
                    plays
                )

            processing_near_play_ids.discard(play_id)
            save_state()
            return

        if play_id in state["pending_near_hr_play_ids"]:
            state["pending_near_hr_play_ids"].remove(play_id)

        processing_near_play_ids.discard(play_id)
        save_state()

    except Exception as exc:
        log.warning("Error confirming near HR %s: %s", play_id, exc)
        if play_id in state["pending_near_hr_play_ids"]:
            state["pending_near_hr_play_ids"].remove(play_id)
        processing_near_play_ids.discard(play_id)
        save_state()



# =========================
# NO HR THROUGH 3 ROAST ALERTS
# =========================

NO_HR_THROUGH_3_MESSAGES = [
    "The home run slips are currently in witness protection.",
    "Everyone who bet a homer is now staring at the TV like it owes them money.",
    "Three innings in and the ball still hasn’t filed a flight plan.",
    "The bats are giving warning-track energy only.",
    "HR bettors are already negotiating with the baseball gods.",
    "No homers yet. The sportsbook intern is feeling very safe.",
    "The wind is apparently working for the books today.",
    "Three innings, zero bombs. Pain.",
    "The balls are staying in the yard like rent is affordable there.",
    "Power hitters are currently loading... very slowly.",
    "The HR props are sweating less than we are.",
    "Somebody tell the bats this is not a contact-hitting support group.",
]


def game_has_any_hr(plays):
    return any(is_home_run(play) for play in plays)


def game_reached_end_of_3rd(plays):
    for play in plays:
        about = play.get("about", {}) or {}
        inning = about.get("inning")

        if isinstance(inning, int) and inning >= 4:
            return True

    return False


async def maybe_send_no_hr_3rd_alert(channel, game, plays):
    if not ENABLE_NO_HR_THROUGH_3_ALERT:
        return

    game_pk = game.get("gamePk")
    if not game_pk:
        return

    game_key = str(game_pk)

    state.setdefault("seen_no_hr_3rd_game_ids", [])

    if game_key in state["seen_no_hr_3rd_game_ids"]:
        return

    if not game_reached_end_of_3rd(plays):
        return

    if game_has_any_hr(plays):
        return

    msg = random.choice(NO_HR_THROUGH_3_MESSAGES)

    embed = discord.Embed(
        title="🫠 No HR Through 3",
        description=f"**{game_label(game)}**\n\n{msg}",
        color=discord.Color.dark_grey(),
    )
    embed.set_footer(text="HR bettors support group checking in")

    await safe_discord_send(channel, embed=embed)

    state["seen_no_hr_3rd_game_ids"].append(game_key)
    save_state()







# =========================
# HARD-HIT PROFILE GATE
# =========================

def build_hard_hit_profile_from_live(metrics, ev):
    """
    Lightweight profile gate using live contact plus safe estimates.
    This keeps alerts tighter without adding external APIs.
    """
    distance = metrics.get("totalDistance")
    launch_angle = metrics.get("launchAngle")

    try:
        distance = float(distance) if distance is not None else 0
    except Exception:
        distance = 0

    try:
        launch_angle = float(launch_angle) if launch_angle is not None else None
    except Exception:
        launch_angle = None

    # Estimates based on current contact. Conservative enough to reduce spam.
    avg_ev = ev - 14 if ev else 0
    max_ev = ev
    barrel_pct = 14 if ev >= 108 and launch_angle is not None and 15 <= launch_angle <= 30 else 8
    hard_hit_pct = 48 if ev >= 105 else 42 if ev >= 100 else 0
    fly_ball_pct = 40 if launch_angle is not None and launch_angle >= 15 else 30
    iso = 0.245 if ev >= 105 and distance >= 380 else 0.200
    pitcher_hr9 = 1.35 if ev >= 105 else 1.10
    pitch_matchup_positive = ev >= 105
    weather_positive = False

    checks = []

    if avg_ev >= PROFILE_AVG_EV_MIN:
        checks.append(f"✅ Avg EV profile: {avg_ev:.1f}")
    if max_ev >= PROFILE_MAX_EV_MIN:
        checks.append(f"✅ Max EV: {max_ev:.1f}")
    if barrel_pct >= PROFILE_BARREL_MIN:
        checks.append(f"✅ Barrel % profile: {barrel_pct:.1f}%")
    if hard_hit_pct >= PROFILE_HARD_HIT_MIN:
        checks.append(f"✅ Hard Hit % profile: {hard_hit_pct:.1f}%")
    if fly_ball_pct >= PROFILE_FLY_BALL_MIN:
        checks.append(f"✅ Fly Ball % profile: {fly_ball_pct:.1f}%")
    if iso >= PROFILE_ISO_MIN:
        checks.append(f"✅ ISO profile: {iso:.3f}")
    if launch_angle is not None and PROFILE_LA_MIN <= launch_angle <= PROFILE_LA_MAX:
        checks.append(f"✅ Launch Angle: {launch_angle:.0f}°")
    if pitch_matchup_positive:
        checks.append("✅ Pitch matchup: positive")
    if weather_positive:
        checks.append("✅ Weather: warm + wind out")
    if pitcher_hr9 >= PROFILE_PITCHER_HR9_MIN:
        checks.append(f"✅ Pitcher HR/9 profile: {pitcher_hr9:.2f}")

    if len(checks) >= 8:
        tier = "🔥 Perfect HR Profile"
    elif len(checks) >= HARD_HIT_PROFILE_MIN_CHECKS:
        tier = "✅ Strong HR Profile"
    elif len(checks) >= HARD_HIT_WATCHLIST_MIN_CHECKS:
        tier = "⚠️ Watchlist Profile"
    else:
        tier = None

    return {
        "checks": checks,
        "check_count": len(checks),
        "tier": tier,
    }




def player_game_key(game, play):
    matchup = play.get("matchup", {}) or {}
    batter = matchup.get("batter", {}) or {}
    batter_id = batter.get("id")
    batter_name = batter.get("fullName", "Unknown")
    return f"{game.get('gamePk')}:{batter_id or batter_name}"


# =========================
# HR IN X/30 PARKS ESTIMATOR
# =========================

def estimate_hr_parks(distance, ev, launch_angle=None):
    try:
        distance = float(distance or 0)
        ev = float(ev or 0)
        launch_angle = float(launch_angle or 0)
    except Exception:
        return 0

    if distance >= 430:
        return 30
    if distance >= 425:
        return 29
    if distance >= 420:
        return 27
    if distance >= 415:
        return 25
    if distance >= 410:
        return 23
    if distance >= 405:
        return 20
    if distance >= 400 and ev >= 108:
        return 18
    if distance >= 395 and ev >= 106:
        return 15
    if distance >= 390 and ev >= 104:
        return 12
    if distance >= 375 and ev >= 102 and 15 <= launch_angle <= 25:
        return 6

    return 0


# =========================
# HARD-HIT / PITCHER WEAKSPOT ALERTS
# =========================


async def get_optional_alert_channel(channel, channel_id, label):
    if not channel_id:
        return channel

    try:
        return await client.fetch_channel(channel_id)
    except Exception as exc:
        log.warning("Could not fetch %s channel %s: %s", label, channel_id, exc)
        return channel


async def maybe_send_hard_hit_tracker(channel, game, play, metrics):
    if not ENABLE_HARD_HIT_TRACKER or not metrics:
        return

    # Never send hard-hit tracker for the actual HR play
    if is_home_run(play):
        return

    ev = metrics.get("launchSpeed")
    if ev is None:
        return

    try:
        ev = float(ev)
    except Exception:
        return

    if ev < HARD_HIT_EV_THRESHOLD:
        return

    matchup = play.get("matchup", {}) or {}
    batter = matchup.get("batter", {}) or {}
    batter_name = batter.get("fullName", "Unknown")
    about = play.get("about", {}) or {}
    team = game["away"] if about.get("halfInning") == "top" else game["home"]
    team_abbr = team.get("abbreviation", "MLB")

    key = player_game_key(game, play)
    hard_hit_tracker[key] = hard_hit_tracker.get(key, 0) + 1

    state.setdefault("seen_hard_hit_alerts", [])

    if key in state["seen_hard_hit_alerts"]:
        return

    # Don't send hard-hit alerts after the player already homered
    state.setdefault("seen_hr_alerts", [])
    if key in state["seen_hr_alerts"]:
        return

    live_elite_trigger = ev >= ELITE_HARD_HIT_EV
    live_repeat_trigger = hard_hit_tracker[key] >= 2

    if not live_elite_trigger and not live_repeat_trigger:
        return

    profile = build_hard_hit_profile_from_live(metrics, ev)

    # Tightened gate:
    # - Normal alerts require 5+ profile matches.
    # - Elite 110+ EV can pass with 3+ watchlist matches.
    if profile["check_count"] < HARD_HIT_PROFILE_MIN_CHECKS:
        if not (live_elite_trigger and profile["check_count"] >= HARD_HIT_WATCHLIST_MIN_CHECKS):
            return

    lines = [
        "🔥 **Hard-Hit Tracker**",
        "",
        f"**{batter_name} ({team_abbr})**",
        f"EV: {ev:.1f} mph",
    ]

    distance = metrics.get("totalDistance")
    launch_angle = metrics.get("launchAngle")
    hr_parks = estimate_hr_parks(distance, ev, launch_angle)

    if distance is not None:
        lines.append(f"Distance: {float(distance):.0f} ft")

    if launch_angle is not None:
        lines.append(f"Launch Angle: {float(launch_angle):.0f}°")

    # Always show this when distance exists, even if estimated parks is 0.
    if distance is not None:
        lines.append(f"🏟️ HR in {hr_parks}/30 parks")

    if ev >= ELITE_HARD_HIT_EV:
        lines.append("Signal: 💣 Elite contact")
    else:
        lines.append(f"Signal: 🔥 {hard_hit_tracker[key]} hard-hit balls today")

    lines.append("")
    lines.append(f"Profile: {profile['check_count']}/10 matches")
    if profile.get("tier"):
        lines.append(profile["tier"])
    for check in profile["checks"][:8]:
        lines.append(check)

    target_channel = await get_optional_alert_channel(channel, DISCORD_HARD_HIT_CHANNEL_ID, "hard-hit")
    await safe_discord_send(target_channel, "\n".join(lines))

    state["seen_hard_hit_alerts"].append(key)
    save_state()


async def maybe_send_pitcher_weakspot_alert(channel, game, play, metrics):
    if not ENABLE_PITCHER_WEAKSPOT_ALERTS or not metrics:
        return

    ev = metrics.get("launchSpeed")
    if ev is None:
        return

    try:
        ev = float(ev)
    except Exception:
        return

    result = play.get("result", {}) or {}
    event_type = (result.get("eventType") or "").lower().strip()
    event_text = " ".join([
        str(result.get("event") or ""),
        str(result.get("description") or ""),
        event_type,
    ]).lower()

    # Do not count foul balls / foul tips as pitcher damage.
    if "foul" in event_text:
        return

    # Only count completed batted-ball outcomes or actual HRs.
    # This prevents random foul/K pitch hitData from polluting the alert.
    batted_ball_events = {
        "single", "double", "triple", "home_run",
        "field_out", "force_out", "grounded_into_double_play",
        "double_play", "fielders_choice", "fielders_choice_out",
        "sac_fly", "sac_bunt", "lineout", "flyout", "groundout",
        "pop_out", "field_error",
    }
    if event_type and event_type not in batted_ball_events and not is_home_run(play):
        return

    launch_angle = metrics.get("launchAngle")
    try:
        launch_angle = float(launch_angle)
    except Exception:
        return

    # More direct HR shape: avoid grounders and sky-high popups.
    if launch_angle < 10 or launch_angle > 38:
        return

    distance = metrics.get("totalDistance")
    try:
        distance = float(distance or 0)
    except Exception:
        distance = 0

    # Ignore short contact. This keeps weakspot alerts focused on real damage.
    if distance < 280:
        return

    # Require dangerous contact.
    if ev < 95:
        return

    matchup = play.get("matchup", {}) or {}
    batter = matchup.get("batter", {}) or {}
    pitcher = matchup.get("pitcher", {}) or {}

    pitcher_name = pitcher.get("fullName", "Unknown")
    key = f"{game.get('gamePk')}:{pitcher.get('id') or pitcher_name}"

    pdata = pitcher_hard_hit_tracker.get(
        key,
        {
            "count": 0,
            "elite_count": 0,
            "hardest_ev": 0.0,
            "targets": set(),
            "target_keys": {},
            "target_evs": {},
            "target_ids": {},
        },
    )
    pdata.setdefault("targets", set())
    pdata.setdefault("target_keys", {})
    pdata.setdefault("target_evs", {})
    pdata.setdefault("target_ids", {})

    pdata["count"] += 1

    if ev >= 100 and 15 <= launch_angle <= 32:
        pdata["elite_count"] += 1

    pdata["hardest_ev"] = max(float(pdata["hardest_ev"]), ev)
    batter_name = batter.get("fullName", "Unknown")
    batter_key = player_game_key(game, play)

    # Count the pitcher damage, but do not recommend the hitter from this play if he just homered.
    if not is_home_run(play):
        pdata["targets"].add(batter_name)
        pdata["target_keys"][batter_name] = batter_key
        pdata["target_evs"][batter_name] = max(float(pdata["target_evs"].get(batter_name, 0.0)), ev)
        pdata["target_ids"][batter_name] = batter.get("id")

    pitcher_hard_hit_tracker[key] = pdata

    state.setdefault("seen_pitcher_weakspot_alerts", [])

    if key in state["seen_pitcher_weakspot_alerts"]:
        return

    # Tightened Pitcher Weakspot trigger: HR-shaped damage only.
    if pdata["count"] < 4:
        return

    if pdata.get("elite_count", 0) < 2:
        return

    if pdata.get("hardest_ev", 0) < 103:
        return

    if len(pdata.get("targets", [])) < 2:
        return

    lines = [
        "🚨 **Pitcher Damage Watch**",
        "",
        f"**{pitcher_name}** is allowing HR-shaped contact",
        f"Danger balls allowed: {pdata['count']}",
        f"100+ EV / ideal-angle balls: {pdata.get('elite_count', 0)}",
        f"Hardest EV: {pdata['hardest_ev']:.1f} mph",
        "Filter: no foul balls, no grounders, no short contact",
        "",
        "Best HR targets:",
    ]

    state.setdefault("seen_hr_alerts", [])

    eligible_targets = []
    target_keys = pdata.get("target_keys", {})
    target_evs = pdata.get("target_evs", {})
    target_ids = pdata.get("target_ids", {})

    for hitter in list(pdata["targets"]):
        hitter_key = target_keys.get(hitter)
        if hitter_key and hitter_key in state["seen_hr_alerts"]:
            continue
        eligible_targets.append((hitter, float(target_evs.get(hitter, 0.0)), target_ids.get(hitter)))

    eligible_targets.sort(key=lambda row: (-row[1], row[0]))

    if not eligible_targets:
        return

    pitcher_id = pitcher.get("id")

    for hitter, hitter_ev, hitter_id in eligible_targets[:3]:
        history = get_batter_vs_pitcher_hr_history(hitter_id, pitcher_id)
        bvp_hr = int(history.get("home_runs", 0) or 0)
        at_bats = history.get("at_bats")

        parts = []
        if hitter_ev > 0:
            parts.append(f"{hitter_ev:.1f} EV")

        if bvp_hr > 0:
            if at_bats is not None:
                parts.append(f"{bvp_hr} career HR vs {pitcher_name} in {at_bats} AB")
            else:
                parts.append(f"{bvp_hr} career HR vs {pitcher_name}")
        else:
            parts.append(f"0 career HR vs {pitcher_name}")

        lines.append(f"• {hitter} — " + " | ".join(parts))

    target_channel = await get_optional_alert_channel(channel, DISCORD_PITCHER_WEAKSPOT_CHANNEL_ID, "pitcher damage watch")
    await safe_discord_send(target_channel, "\n".join(lines))

    state["seen_pitcher_weakspot_alerts"].append(key)
    save_state()




# =========================
# LIVE CYCLE PROP WATCH
# =========================

def hit_type_from_play(play):
    result = play.get("result", {}) or {}
    event_type = (result.get("eventType") or "").lower().strip()
    event = (result.get("event") or "").lower().strip()

    if event_type == "single" or event == "single":
        return "1B"
    if event_type == "double" or event == "double":
        return "2B"
    if event_type == "triple" or event == "triple":
        return "3B"
    if is_home_run(play):
        return "HR"
    return None


def current_inning_from_plays(plays):
    inning = 0
    for play in plays:
        about = play.get("about", {}) or {}
        try:
            inning = max(inning, int(about.get("inning") or 0))
        except Exception:
            continue
    return inning



def normalize_name_for_match(value):
    return "".join(ch for ch in str(value or "").lower() if ch.isalnum())

def american_odds_text(price):
    if price is None:
        return "N/A"
    try:
        price = int(price)
        return f"+{price}" if price > 0 else str(price)
    except Exception:
        return str(price)

def cycle_market_for_missing(missing):
    if missing == "HR":
        return CYCLE_MARKET_HR
    if missing == "3B":
        return CYCLE_MARKET_TRIPLE
    if missing == "2B":
        return CYCLE_MARKET_DOUBLE
    if missing == "1B":
        return CYCLE_MARKET_HIT
    return CYCLE_MARKET_HIT

def get_odds_api_events():
    if not ODDS_API_KEY:
        return []
    url = f"{ODDS_API_BASE}/sports/{ODDS_SPORT_KEY}/events"
    return get_odds_json(url, {"apiKey": ODDS_API_KEY})

def names_match(a, b):
    aa = normalize_name_for_match(a)
    bb = normalize_name_for_match(b)
    return bool(aa and bb and (aa in bb or bb in aa))

def find_odds_event_for_game(game):
    away = game.get("away") or {}
    home = game.get("home") or {}
    away_names = [
        away.get("name"),
        away.get("teamName"),
        away.get("abbreviation"),
    ]
    home_names = [
        home.get("name"),
        home.get("teamName"),
        home.get("abbreviation"),
    ]

    events = get_odds_api_events()
    for ev in events:
        ev_home = ev.get("home_team", "")
        ev_away = ev.get("away_team", "")

        away_ok = any(names_match(x, ev_away) for x in away_names if x)
        home_ok = any(names_match(x, ev_home) for x in home_names if x)

        # Some sources flip naming around; allow either team pair match.
        away_flip = any(names_match(x, ev_home) for x in away_names if x)
        home_flip = any(names_match(x, ev_away) for x in home_names if x)

        if (away_ok and home_ok) or (away_flip and home_flip):
            return ev

    return None

def extract_any_link(*items):
    # The Odds API may expose links at different levels depending on plan/book.
    for item in items:
        if not isinstance(item, dict):
            continue
        for key in ("link", "url", "deep_link", "deeplink"):
            val = item.get(key)
            if val:
                return val
    return None



def cycle_props_for_missing_list(missing_list, player_name=""):
    props = []
    for missing in missing_list:
        prop_name, links, stars = cycle_prop_for_missing(missing, player_name)
        props.append({
            "missing": missing,
            "prop": prop_name,
            "links": links,
            "stars": stars,
        })
    return props


def sgo_get_json(path, params=None):
    if not SPORTSGAMEODDS_API_KEY:
        return None
    url = f"{SPORTSGAMEODDS_API_BASE}{path}"
    headers = {
        "x-api-key": SPORTSGAMEODDS_API_KEY,
        "accept": "application/json",
    }
    r = session.get(url, params=params or {}, headers=headers, timeout=25)
    if r.status_code >= 400:
        log.warning("SportsGameOdds error %s url=%s params=%s body=%s", r.status_code, url, params, r.text[:500])
    r.raise_for_status()
    return r.json()

def sgo_keywords_for_missing(missing):
    if missing == "HR":
        return [x.strip().lower() for x in CYCLE_SGO_MARKET_HR_KEYWORDS.split(",") if x.strip()]
    if missing == "3B":
        return [x.strip().lower() for x in CYCLE_SGO_MARKET_TRIPLE_KEYWORDS.split(",") if x.strip()]
    if missing == "2B":
        return [x.strip().lower() for x in CYCLE_SGO_MARKET_DOUBLE_KEYWORDS.split(",") if x.strip()]
    if missing == "1B":
        return [x.strip().lower() for x in CYCLE_SGO_MARKET_HIT_KEYWORDS.split(",") if x.strip()]
    return []

def flatten_dicts(obj):
    if isinstance(obj, dict):
        yield obj
        for v in obj.values():
            yield from flatten_dicts(v)
    elif isinstance(obj, list):
        for item in obj:
            yield from flatten_dicts(item)

def sgo_text_blob(d):
    parts = []
    if isinstance(d, dict):
        for k, v in d.items():
            if isinstance(v, (str, int, float)):
                parts.append(str(k))
                parts.append(str(v))
    return " ".join(parts).lower()

def sgo_extract_price(d):
    for key in ("price", "odds", "americanOdds", "american", "bookOdds", "bookPrice"):
        if key in d and d.get(key) not in (None, ""):
            return d.get(key)
    return None

def sgo_extract_link(d):
    for key in (
        "deepLink", "deeplink", "deep_link", "link", "url", "betUrl", "betURL",
        "sportsbookUrl", "sportsbookURL", "appLink", "webUrl", "webURL"
    ):
        if isinstance(d, dict) and d.get(key):
            return d.get(key)
    return None

def sgo_is_fanduel(d):
    blob = sgo_text_blob(d)
    book = (SPORTSGAMEODDS_FANDUEL_BOOKMAKER_ID or "fanduel").lower()
    return "fanduel" in blob or book in blob

def sgo_market_matches(d, player_name, missing):
    blob = sgo_text_blob(d)
    if not names_match(player_name, blob):
        return False
    if not sgo_is_fanduel(d):
        return False
    keywords = sgo_keywords_for_missing(missing)
    return any(k in blob for k in keywords)

def sgo_event_matches_game(d, game):
    blob = sgo_text_blob(d)
    away = game.get("away") or {}
    home = game.get("home") or {}
    checks = [
        away.get("name"), away.get("teamName"), away.get("abbreviation"),
        home.get("name"), home.get("teamName"), home.get("abbreviation"),
    ]
    found = [x for x in checks if x and normalize_name_for_match(x) in normalize_name_for_match(blob)]
    return len(found) >= 2

def fetch_sgo_events():
    """
    Cache today's MLB events once instead of calling /events for every prop.
    SportsGameOdds max limit is 100.
    """
    now_ts = datetime.now(TZ).timestamp()
    cached = cycle_sgo_events_cache.get("events") or []

    if cached and now_ts - cycle_sgo_events_cache.get("ts", 0) <= CYCLE_SGO_EVENTS_CACHE_SECONDS:
        return cached

    params = {
        "leagueID": SPORTSGAMEODDS_MLB_LEAGUE_ID or "MLB",
        "oddsAvailable": "true",
        "bookmakerID": SPORTSGAMEODDS_FANDUEL_BOOKMAKER_ID or "fanduel",
        "limit": "100",
    }

    try:
        payload = sgo_get_json("/events/", params=params)
        events = []

        if isinstance(payload, dict):
            for key in ("events", "data", "results"):
                if isinstance(payload.get(key), list):
                    events = payload.get(key)
                    break
            if not events:
                events = [d for d in flatten_dicts(payload) if d.get("eventID") or d.get("id")]
        elif isinstance(payload, list):
            events = payload

        cycle_sgo_events_cache["ts"] = now_ts
        cycle_sgo_events_cache["events"] = events or []
        log.info("SportsGameOdds events cached: %s", len(events or []))
        return events or []

    except Exception as exc:
        log.warning("SportsGameOdds /events failed. Check SPORTSGAMEODDS_API_KEY and leagueID=MLB. Error: %s", exc)
        return cached or []

def sgo_event_id(d):
    if not isinstance(d, dict):
        return None
    for key in ("eventID", "eventId", "id", "gameID", "gameId"):
        if d.get(key):
            return d.get(key)
    return None

def find_sgo_event_for_game(game):
    events = fetch_sgo_events()
    for ev in events:
        if sgo_event_matches_game(ev, game):
            return ev
    return None

def fetch_sgo_event_odds(event_id):
    """
    Fetch and cache odds for one SportsGameOdds event.
    Uses eventID to avoid scanning the entire slate repeatedly.
    """
    if not event_id:
        return None

    now_ts = datetime.now(TZ).timestamp()
    cache_key = str(event_id)
    cached = cycle_sgo_event_odds_cache.get(cache_key)

    if cached and now_ts - cached.get("ts", 0) <= CYCLE_SGO_EVENT_ODDS_CACHE_SECONDS:
        return cached.get("payload")

    params = {
        "eventID": event_id,
        "bookmakerID": SPORTSGAMEODDS_FANDUEL_BOOKMAKER_ID or "fanduel",
        "oddsAvailable": "true",
        "limit": "100",
    }

    try:
        # Prefer odds endpoint. If the user's plan/endpoint differs, fallback to /events with eventID.
        try:
            payload = sgo_get_json("/odds/", params=params)
        except Exception:
            payload = sgo_get_json("/events/", params=params)

        cycle_sgo_event_odds_cache[cache_key] = {"ts": now_ts, "payload": payload}
        return payload

    except Exception as exc:
        log.warning("SportsGameOdds event odds failed eventID=%s error=%s", event_id, exc)
        cycle_sgo_event_odds_cache[cache_key] = {"ts": now_ts, "payload": None}
        return None


def fetch_sgo_player_prop(game, player_name, missing):
    if not ENABLE_CYCLE_SGO_ODDS or not SPORTSGAMEODDS_API_KEY:
        return None

    cache_key = f"sgo:{game.get('gamePk')}:{player_name}:{missing}"
    now_ts = datetime.now(TZ).timestamp()
    cached = cycle_sgo_cache.get(cache_key)

    if cached and now_ts - cached.get("ts", 0) <= CYCLE_SGO_CACHE_SECONDS:
        return cached.get("value")

    value = None

    try:
        sgo_event = find_sgo_event_for_game(game)
        event_id = sgo_event_id(sgo_event)

        if not event_id:
            log.warning("SportsGameOdds event match not found for %s", game_label(game))
            cycle_sgo_cache[cache_key] = {"ts": now_ts, "value": None}
            return None

        payload = fetch_sgo_event_odds(event_id)
        candidates = list(flatten_dicts(payload))

        matches = [
            d for d in candidates
            if sgo_market_matches(d, player_name, missing)
        ]

        matches.sort(
            key=lambda d: (
                1 if sgo_extract_link(d) else 0,
                1 if sgo_extract_price(d) is not None else 0,
            ),
            reverse=True,
        )

        if matches:
            best = matches[0]
            price = sgo_extract_price(best)
            link = sgo_extract_link(best)

            value = {
                "book": "FanDuel",
                "source": "SportsGameOdds",
                "player": player_name,
                "missing": missing,
                "price": price,
                "oddsText": american_odds_text(price),
                "link": link,
                "rawText": sgo_text_blob(best)[:500],
            }

    except Exception as exc:
        log.warning("Could not load SportsGameOdds FanDuel cycle prop for %s missing %s: %s", player_name, missing, exc)

    cycle_sgo_cache[cache_key] = {"ts": now_ts, "value": value}
    return value


def fanduel_prop_display(odds_row, fallback_links):
    if odds_row:
        link = odds_row.get("link") or (fallback_links[0] if fallback_links else FANDUEL_MLB_DEEPLINK)
        odds = odds_row.get("oddsText") or american_odds_text(odds_row.get("price"))
        outcome_name = odds_row.get("outcomeName") or "Yes/Over"
        desc = odds_row.get("description") or odds_row.get("player") or ""
        source = odds_row.get("source", "Odds API")
        return {
            "text": f"FanDuel {odds} — {desc} {outcome_name} ({source})".strip(),
            "link": link,
            "odds": odds,
        }

    return {
        "text": "FanDuel odds not found from SportsGameOdds yet",
        "link": fallback_links[0] if fallback_links else FANDUEL_MLB_DEEPLINK,
        "odds": "N/A",
    }


def split_links(value):
    return [x.strip() for x in str(value or "").split(",") if x.strip()]

def unique_links(links):
    out = []
    seen = set()
    for link in links:
        if not link or link in seen:
            continue
        seen.add(link)
        out.append(link)
    return out

def fanduel_search_link(player_name, prop_name):
    query = quote_plus(f"{player_name} {prop_name}")
    return f"https://sportsbook.fanduel.com/search?query={query}"

def cycle_prop_for_missing(missing, player_name=""):
    if missing == "HR":
        prop = "To Hit A Home Run"
        links = split_links(FANDUEL_HR_DEEPLINKS) + [FANDUEL_HR_DEEPLINK]
        stars = "⭐⭐⭐⭐⭐"
    elif missing == "3B":
        prop = "To Record A Triple"
        links = split_links(FANDUEL_TRIPLE_DEEPLINKS) + [FANDUEL_TRIPLE_DEEPLINK]
        stars = "⭐"
    elif missing == "2B":
        prop = "To Record A Double"
        links = split_links(FANDUEL_DOUBLE_DEEPLINKS) + [FANDUEL_DOUBLE_DEEPLINK]
        stars = "⭐⭐⭐"
    elif missing == "1B":
        prop = "To Record A Hit"
        links = split_links(FANDUEL_HIT_DEEPLINKS) + [FANDUEL_HIT_DEEPLINK]
        stars = "⭐⭐⭐⭐"
    else:
        prop = "Player Prop"
        links = [FANDUEL_MLB_DEEPLINK]
        stars = "—"

    # Always include MLB landing and a player/prop search fallback.
    links += [
        FANDUEL_MLB_DEEPLINK,
        fanduel_search_link(player_name, prop),
    ]

    return prop, unique_links(links), stars


def collect_cycle_watch_candidates(game, plays):
    current_inning = current_inning_from_plays(plays)
    if current_inning < CYCLE_WATCH_MIN_INNING:
        return []

    by_player = {}

    for play in plays:
        hit_type = hit_type_from_play(play)
        if not hit_type:
            continue

        matchup = play.get("matchup", {}) or {}
        batter = matchup.get("batter", {}) or {}
        batter_id = batter.get("id")
        batter_name = batter.get("fullName", "Unknown")

        if not batter_id and not batter_name:
            continue

        key = f"{game.get('gamePk')}:{batter_id or batter_name}"
        about = play.get("about", {}) or {}
        team = game["away"] if about.get("halfInning") == "top" else game["home"]
        team_abbr = team.get("abbreviation", "MLB")

        row = by_player.setdefault(
            key,
            {
                "key": key,
                "player_id": batter_id,
                "name": batter_name,
                "team_abbr": team_abbr,
                "hits": set(),
                "hit_events": [],
                "latest_inning": about.get("inning"),
            },
        )

        row["hits"].add(hit_type)
        row["hit_events"].append(hit_type)
        row["latest_inning"] = about.get("inning") or row.get("latest_inning")

    candidates = []
    full_cycle = {"1B", "2B", "3B", "HR"}

    for row in by_player.values():
        hits = set(row["hits"])
        hit_leg_count = len(hits)

        if hit_leg_count < CYCLE_WATCH_MIN_LEGS or hit_leg_count >= 4:
            continue

        missing = sorted(
            list(full_cycle - hits),
            key=lambda x: {"1B": 1, "2B": 2, "3B": 3, "HR": 4}.get(x, 9)
        )

        row["missing"] = missing
        row["hit_leg_count"] = hit_leg_count
        row["cycle_stage"] = f"{hit_leg_count}/4"
        candidates.append(row)

    return candidates

async def update_cycle_watch_message_with_odds(message, game, row, props, primary_link):
    if not message:
        return

    try:
        fd_rows = []
        link_lines = []
        best_link = primary_link or FANDUEL_MLB_DEEPLINK

        for prop in props:
            fanduel_odds = await asyncio.to_thread(
                fetch_sgo_player_prop,
                game,
                row["name"],
                prop["missing"]
            )

            fd_display = fanduel_prop_display(fanduel_odds, prop["links"])

            if best_link == FANDUEL_MLB_DEEPLINK and fd_display.get("link"):
                best_link = fd_display["link"]

            fd_rows.append(
                f"• **{prop['prop']}** — {fd_display['odds']}\n"
                f"  [FanDuel Link]({fd_display['link']})"
            )

            check_links = unique_links([fd_display["link"]] + prop["links"])
            for idx, link in enumerate(check_links[:3], start=1):
                link_lines.append(f"[{prop['missing']} Link {idx}]({link})")

        if not message.embeds:
            return

        embed = message.embeds[0]
        embed.url = best_link

        old_fields = list(embed.fields)
        embed.clear_fields()

        for field in old_fields:
            if field.name in ("FanDuel Odds / Deep Links", "FanDuel Links To Check"):
                continue
            embed.add_field(name=field.name, value=field.value, inline=field.inline)

        embed.add_field(
            name="FanDuel Odds / Deep Links",
            value="\n".join(fd_rows) if fd_rows else "FanDuel props not found yet.",
            inline=False,
        )

        if link_lines:
            embed.add_field(
                name="FanDuel Links To Check",
                value=" • ".join(link_lines[:8]),
                inline=False,
            )

        await message.edit(embed=embed)

    except Exception as exc:
        log.warning("Could not update cycle alert with FanDuel odds: %s", exc)


async def maybe_send_cycle_watch_alerts(channel, game, plays):
    if not ENABLE_CYCLE_WATCH:
        return

    state.setdefault("seen_cycle_alerts", [])

    target_channel = await get_optional_alert_channel(
        channel,
        DISCORD_CYCLE_CHANNEL_ID,
        "cycle-watch"
    )

    for row in collect_cycle_watch_candidates(game, plays):
        missing_list = row.get("missing", [])
        stage = row.get("cycle_stage", f"{row.get('hit_leg_count', len(row.get('hits', [])))}/4")

        alert_key = f"{row['key']}:cycle:{stage}"

        if alert_key in state["seen_cycle_alerts"]:
            continue

        props = cycle_props_for_missing_list(missing_list, row["name"])

        has_1b = "✅" if "1B" in row["hits"] else "❌"
        has_2b = "✅" if "2B" in row["hits"] else "❌"
        has_3b = "✅" if "3B" in row["hits"] else "❌"
        has_hr = "✅" if "HR" in row["hits"] else "❌"

        if row.get("hit_leg_count") == 3:
            title = "🚲 Live Cycle Prop Watch — 1 Leg Away"
            intro = f"**{row['name']} ({row['team_abbr']})** is one leg away from the cycle."
        else:
            title = "🚲 Live Cycle Prop Watch — 2 of 4"
            intro = f"**{row['name']} ({row['team_abbr']})** has 2 of 4 cycle legs."

        prop_lines = [
            f"• **{prop['prop']}** — looking up FanDuel odds/link..."
            for prop in props
        ]

        embed = discord.Embed(
            title=title,
            description=(
                f"{intro}\n\n"
                f"{has_1b} Single\n"
                f"{has_2b} Double\n"
                f"{has_3b} Triple\n"
                f"{has_hr} Home Run\n\n"
                f"🎯 **Remaining Props:**\n"
                + "\n".join(prop_lines)
            ),
            color=discord.Color.teal(),
            url=FANDUEL_MLB_DEEPLINK,
        )

        embed.add_field(name="Game", value=game_label(game), inline=False)
        embed.add_field(name="Cycle Progress", value=stage, inline=True)
        embed.add_field(name="Hits Today", value=str(len(row["hit_events"])), inline=True)
        embed.add_field(name="Missing Legs", value=", ".join(missing_list), inline=True)
        embed.add_field(
            name="FanDuel Odds / Deep Links",
            value="Looking up FanDuel odds and deep links from SportsGameOdds...",
            inline=False,
        )

        headshot_url = player_headshot(row.get("player_id"))
        if headshot_url:
            embed.set_image(url=headshot_url)

        logo_url = team_logo(row.get("team_abbr"))
        if logo_url:
            embed.set_thumbnail(url=logo_url)

        embed.set_footer(
            text="Real-time trigger from MLB live feed. FanDuel odds/links update after post."
        )

        state["seen_cycle_alerts"].append(alert_key)
        save_state()

        msg = await safe_discord_send(target_channel, embed=embed)

        asyncio.create_task(
            update_cycle_watch_message_with_odds(
                msg,
                game,
                row,
                props,
                FANDUEL_MLB_DEEPLINK,
            )
        )


# =========================
# STRIKEOUT ALERTS
# =========================

def innings_pitched_to_float(value):
    """Convert MLB innings notation. 1.1 = 1 and 1/3, 1.2 = 1 and 2/3."""
    if value in (None, ""):
        return 0.0

    try:
        text = str(value)

        if "." not in text:
            return float(text)

        whole, frac = text.split(".", 1)
        whole_num = int(whole or 0)

        if frac == "1":
            return whole_num + (1 / 3)

        if frac == "2":
            return whole_num + (2 / 3)

        return float(text)
    except Exception:
        return 0.0


def get_current_game_inning_from_feed(feed_data):
    linescore = ((feed_data.get("liveData") or {}).get("linescore") or {})
    inning = linescore.get("currentInning")

    try:
        return int(inning)
    except Exception:
        return None


def get_k_alert_signal(ks, pitch_count, on_pace, tier):
    """
    Simple signal label based on pitch efficiency and K pace.
    """
    if tier == "early":
        if pitch_count <= 35:
            return "🔥 Efficient K pace", "🔥 Early K Alert — Strong Pace", discord.Color.orange()
        if pitch_count >= 45:
            return "⚠️ Good K pace, but pitch count is getting high", "⚠️ Early K Alert — High Pitch Count", discord.Color.gold()
        return "✅ Good early K rhythm", "✅ Early K Alert — Solid Start", discord.Color.green()

    if pitch_count <= 65:
        return "🔥 Sustained dominance", "🔥 K Alert — Strong Pace", discord.Color.orange()
    if pitch_count >= 80:
        return "⚠️ Pace is good, but pitch count is climbing", "⚠️ K Alert — Watch Pitch Count", discord.Color.gold()
    return "✅ Strong K pace", "✅ K Alert — Solid Start", discord.Color.green()


def collect_strikeout_alert_pitchers(game, feed_data):
    """
    Two-tier K alerts:
    - Early: 3+ Ks by inning 2
    - Extended: 5+ Ks by inning 4
    """
    current_inning = get_current_game_inning_from_feed(feed_data)

    if current_inning is None:
        return []

    boxscore = ((feed_data.get("liveData") or {}).get("boxscore") or {})
    teams = boxscore.get("teams") or {}
    found = []

    for side in ("away", "home"):
        team_block = teams.get(side) or {}
        team_info = team_block.get("team") or {}
        team_abbr = team_info.get("abbreviation") or team_info.get("teamName") or team_info.get("name") or "MLB"
        players = team_block.get("players") or {}

        for player in players.values():
            person = player.get("person") or {}
            pitching = ((player.get("stats") or {}).get("pitching") or {})

            if not pitching:
                continue

            ks = int(pitching.get("strikeOuts", 0) or 0)
            pitch_count = int(pitching.get("pitchesThrown", 0) or 0)
            innings_pitched_raw = pitching.get("inningsPitched", "0")
            innings_pitched = innings_pitched_to_float(innings_pitched_raw)

            tiers = []

            if current_inning <= STRIKEOUT_ALERT_MAX_INNING and ks >= STRIKEOUT_ALERT_MIN_KS:
                tiers.append({
                    "tier": "early",
                    "min_ks": STRIKEOUT_ALERT_MIN_KS,
                    "max_inning": STRIKEOUT_ALERT_MAX_INNING,
                })

            if current_inning <= STRIKEOUT_EXTENDED_MAX_INNING and ks >= STRIKEOUT_EXTENDED_MIN_KS:
                tiers.append({
                    "tier": "extended",
                    "min_ks": STRIKEOUT_EXTENDED_MIN_KS,
                    "max_inning": STRIKEOUT_EXTENDED_MAX_INNING,
                })

            if not tiers:
                continue

            if innings_pitched <= 0:
                on_pace = 0
            else:
                on_pace = round((ks / innings_pitched) * 9, 1)

            for tier_info in tiers:
                found.append({
                    "player_id": person.get("id"),
                    "name": person.get("fullName", "Unknown Pitcher"),
                    "team_abbr": team_abbr,
                    "ks": ks,
                    "innings_pitched": innings_pitched_raw,
                    "pitch_count": pitch_count,
                    "on_pace": on_pace,
                    "current_inning": current_inning,
                    "game": game_label(game),
                    "game_pk": game.get("gamePk"),
                    **tier_info,
                })

    return found


async def maybe_send_strikeout_alerts(channel, game, feed_data):
    if not ENABLE_STRIKEOUT_ALERTS:
        return

    target_channel = channel

    if DISCORD_STRIKEOUT_CHANNEL_ID:
        try:
            target_channel = await client.fetch_channel(DISCORD_STRIKEOUT_CHANNEL_ID)
        except Exception as exc:
            log.warning("Could not fetch strikeout channel %s: %s", DISCORD_STRIKEOUT_CHANNEL_ID, exc)
            target_channel = channel

    pitchers = collect_strikeout_alert_pitchers(game, feed_data)

    for pitcher in pitchers:
        alert_key = f"{pitcher['game_pk']}-{pitcher.get('player_id')}-{pitcher['tier']}-{pitcher['min_ks']}ks"

        if alert_key in state["seen_strikeout_alerts"]:
            continue

        state["seen_strikeout_alerts"].append(alert_key)
        save_state()

        signal, title, color = get_k_alert_signal(
            pitcher["ks"],
            pitcher["pitch_count"],
            pitcher["on_pace"],
            pitcher["tier"],
        )

        embed = discord.Embed(
            title=title,
            description=(
                f"**{pitcher['name']} ({pitcher['team_abbr']})**\n"
                f"{pitcher['ks']} Ks through {pitcher['innings_pitched']} IP\n"
                f"Pitch Count: {pitcher['pitch_count']}\n"
                f"On Pace: {pitcher['on_pace']} Ks\n\n"
                f"Signal: {signal}\n"
                f"**Game:** {pitcher['game']}"
            ),
            color=color,
            url=f"https://www.mlb.com/gameday/{pitcher['game_pk']}",
        )

        if pitcher["tier"] == "early":
            footer = f"Trigger: {STRIKEOUT_ALERT_MIN_KS}+ Ks in innings 1-{STRIKEOUT_ALERT_MAX_INNING}"
        else:
            footer = f"Trigger: {STRIKEOUT_EXTENDED_MIN_KS}+ Ks by inning {STRIKEOUT_EXTENDED_MAX_INNING}"

        embed.set_footer(text=footer)

        await safe_discord_send(target_channel, embed=embed)


async def process_game(channel, game):
    data = get_json(LIVE_FEED_URL.format(gamePk=game["gamePk"]))
    plays = (((data.get("liveData") or {}).get("plays") or {}).get("allPlays") or [])

    await maybe_send_strikeout_alerts(channel, game, data)

    await maybe_send_no_hr_3rd_alert(channel, game, plays)

    await maybe_send_cycle_watch_alerts(channel, game, plays)

    for play in plays:
        if "play_is_recent" in globals() and not play_is_recent(play):
            continue

        pid = build_play_id(game["gamePk"], play)

        metrics = get_metrics(play)
        if metrics:
            await maybe_send_hard_hit_tracker(channel, game, play, metrics)
            await maybe_send_pitcher_weakspot_alert(channel, game, play, metrics)

        if is_home_run(play):
            state.setdefault("seen_hr_alerts", [])
            hr_player_key = player_game_key(game, play)
            if hr_player_key not in state["seen_hr_alerts"]:
                state["seen_hr_alerts"].append(hr_player_key)
                save_state()

            if pid in processing_play_ids:
                continue

            # Claim BEFORE sending. This prevents duplicate loops/reconnects from posting same HR.
            if already_seen_or_claim("seen_hr_play_ids", pid):
                continue

            processing_play_ids.add(pid)
            try:
                await send_alert(channel, game, play, "hr", plays)
            finally:
                processing_play_ids.discard(pid)

            if pid in state["pending_near_hr_play_ids"]:
                state["pending_near_hr_play_ids"].remove(pid)
                save_state()

            continue

        if is_near_hr(play):
            if (
                pid in processing_near_play_ids
                or pid in state["seen_near_hr_play_ids"]
                or pid in state["pending_near_hr_play_ids"]
                or pid in state["seen_hr_play_ids"]
            ):
                continue

            processing_near_play_ids.add(pid)
            state["pending_near_hr_play_ids"].append(pid)
            save_state()

            asyncio.create_task(confirm_and_send_near_hr(channel, game, pid))

    save_state()


def _collect_boxscore_hitters(game_pk):
    data = get_json(BOXSCORE_URL.format(gamePk=game_pk))
    results = []
    for side in ("away", "home"):
        team_block = (data.get("teams") or {}).get(side) or {}
        team_info = team_block.get("team") or {}
        team_abbr = team_info.get("abbreviation") or team_info.get("name") or ""
        players = team_block.get("players") or {}
        for player in players.values():
            person = player.get("person") or {}
            batting = ((player.get("stats") or {}).get("batting") or {})
            hr_count = batting.get("homeRuns", 0) or 0
            if hr_count > 0:
                results.append({"player_id": person.get("id"), "name": person.get("fullName", "Unknown"), "team_abbr": team_abbr, "hr_count": hr_count})
    return results


def build_yesterday_recap(target_date):
    recap_games = []
    total_hr = 0
    unique_players = set()
    for game in get_games_for_date(target_date):
        game_pk = game.get("gamePk")
        if not game_pk:
            continue
        try:
            hitters = _collect_boxscore_hitters(game_pk)
        except Exception as exc:
            log.warning("Could not load boxscore for game %s: %s", game_pk, exc)
            continue
        if not hitters:
            continue
        hitters.sort(key=lambda x: (-x["hr_count"], x["name"]))
        recap_games.append({"label": game_label(game), "hitters": hitters})
        for hitter in hitters:
            total_hr += hitter["hr_count"]
            unique_players.add(hitter["name"])
    return {"date": target_date, "games": recap_games, "total_hr": total_hr, "unique_players": len(unique_players)}


def build_hot_streaks(end_date_days_ago=1, window_days=HOT_STREAK_DAYS, top_n=HOT_STREAK_TOP_N):
    totals = {}
    daily_counts = []
    for days_ago in range(end_date_days_ago + window_days - 1, end_date_days_ago - 1, -1):
        date_str = day_str(days_ago)
        day_counts = {}
        for game in get_games_for_date(date_str):
            game_pk = game.get("gamePk")
            if not game_pk:
                continue
            try:
                hitters = _collect_boxscore_hitters(game_pk)
            except Exception as exc:
                log.warning("Could not load hot-streak boxscore for game %s: %s", game_pk, exc)
                continue
            for hitter in hitters:
                key = hitter["player_id"] or hitter["name"]
                if key not in totals:
                    totals[key] = {"player_id": hitter["player_id"], "name": hitter["name"], "team_abbr": hitter["team_abbr"], "total_hr": 0, "days": {}}
                totals[key]["total_hr"] += hitter["hr_count"]
                totals[key]["team_abbr"] = hitter["team_abbr"]
                totals[key]["days"][date_str] = totals[key]["days"].get(date_str, 0) + hitter["hr_count"]
                day_counts[key] = day_counts.get(key, 0) + hitter["hr_count"]
        daily_counts.append((date_str, day_counts))
    hot = []
    ordered_dates_desc = [date for date, _ in reversed(daily_counts)]
    for data in totals.values():
        streak_days = 0
        for date_str in ordered_dates_desc:
            if data["days"].get(date_str, 0) > 0:
                streak_days += 1
            else:
                break
        hot.append({"player_id": data["player_id"], "name": data["name"], "team_abbr": data["team_abbr"], "total_hr": data["total_hr"], "streak_days": streak_days})
    hot.sort(key=lambda x: (-x["total_hr"], -x["streak_days"], x["name"]))
    return hot[:top_n]


def get_active_roster_player_ids(team_id):
    data = get_json(TEAM_ROSTER_URL.format(teamId=team_id))
    return [row.get("person", {}).get("id") for row in data.get("roster", []) if row.get("person", {}).get("id")]


def get_people_by_ids(player_ids):
    if not player_ids:
        return []
    people = []
    for i in range(0, len(player_ids), 100):
        ids = ",".join(str(x) for x in player_ids[i:i + 100])
        data = get_json(PEOPLE_URL.format(personIds=ids))
        people.extend(data.get("people", []))
    return people


def build_birthday_narratives(today_games, hot_streaks):
    if not ENABLE_BIRTHDAY_NARRATIVE:
        return []
    today = datetime.now(TZ)
    today_mmdd = today.strftime("%m-%d")
    hot_lookup = {row["player_id"]: row for row in hot_streaks if row.get("player_id")}
    team_games = {}
    team_ids = set()
    for game in today_games:
        for team in (game.get("away") or {}, game.get("home") or {}):
            team_id = team.get("id")
            if not team_id:
                continue
            team_ids.add(team_id)
            team_games[team_id] = {"team_abbr": team.get("abbreviation") or team.get("teamName") or team.get("name") or "MLB", "game": game_label(game)}
    birthday_players = []
    for team_id in sorted(team_ids):
        try:
            people = get_people_by_ids(get_active_roster_player_ids(team_id))
        except Exception as exc:
            log.warning("Could not load birthday data for team %s: %s", team_id, exc)
            continue
        for person in people:
            birth_date = person.get("birthDate")
            if not birth_date or birth_date[5:] != today_mmdd:
                continue
            player_id = person.get("id")
            hot = hot_lookup.get(player_id)
            team_info = team_games.get(team_id, {})
            birthday_players.append({
                "player_id": player_id,
                "name": person.get("fullName", "Unknown"),
                "birth_date": birth_date,
                "age": age_on_date(birth_date, today),
                "team_abbr": team_info.get("team_abbr", "MLB"),
                "game": team_info.get("game", "Team plays today"),
                "last_7_hr": hot.get("total_hr", 0) if hot else 0,
                "streak_days": hot.get("streak_days", 0) if hot else 0,
            })
    birthday_players.sort(key=lambda x: (x["team_abbr"], x["name"]))
    return birthday_players


def get_probable_pitcher_for_team(game, batter_team_id):
    try:
        raw_schedule = get_json(SCHEDULE_URL.format(date=today_str()))
        raw_game = None
        for date_block in raw_schedule.get("dates", []):
            for g in date_block.get("games", []):
                if g.get("gamePk") == game.get("gamePk"):
                    raw_game = g
                    break
            if raw_game:
                break
        if not raw_game:
            return None
        teams = raw_game.get("teams") or {}
        away = teams.get("away") or {}
        home = teams.get("home") or {}
        away_id = ((away.get("team") or {}).get("id"))
        home_id = ((home.get("team") or {}).get("id"))
        if batter_team_id == away_id:
            return home.get("probablePitcher")
        if batter_team_id == home_id:
            return away.get("probablePitcher")
    except Exception as exc:
        log.warning("Could not load probable pitcher for game %s: %s", game.get("gamePk"), exc)
    return None


def get_pitcher_hr_per_9(pitcher_id):
    if not pitcher_id:
        return None
    try:
        data = get_json(PITCHER_STATS_URL.format(personId=pitcher_id, season=get_current_season()))
        stats = data.get("stats") or []
        if not stats:
            return None
        splits = stats[0].get("splits") or []
        if not splits:
            return None
        stat = splits[0].get("stat") or {}
        hr_allowed = safe_float(stat.get("homeRuns"), 0.0)
        innings = safe_float(stat.get("inningsPitched"), 0.0)
        if innings <= 0:
            return None
        return round((hr_allowed / innings) * 9, 2)
    except Exception as exc:
        log.warning("Could not load pitcher HR/9 for %s: %s", pitcher_id, exc)
        return None



def get_batter_vs_pitcher_hr_history(batter_id, pitcher_id):
    """
    Returns career batter-vs-pitcher HR history using MLB StatsAPI vsPlayer split.
    Cached in memory so weak-pitcher alerts do not repeatedly hit the API.
    """
    if not ENABLE_BVP_HR_HISTORY or not batter_id or not pitcher_id:
        return {"home_runs": 0, "at_bats": None, "hits": None}

    cache_key = f"{batter_id}:{pitcher_id}"
    if cache_key in bvp_hr_history_cache:
        return bvp_hr_history_cache[cache_key]

    result = {"home_runs": 0, "at_bats": None, "hits": None}

    try:
        url = (
            f"https://statsapi.mlb.com/api/v1/people/{batter_id}/stats"
            f"?stats=vsPlayer&group=hitting&opposingPlayerId={pitcher_id}"
        )
        data = get_json(url)

        for stat_group in data.get("stats", []) or []:
            for split in stat_group.get("splits", []) or []:
                stat = split.get("stat", {}) or {}
                result["home_runs"] += int(stat.get("homeRuns", 0) or 0)

                if stat.get("atBats") is not None:
                    result["at_bats"] = int(stat.get("atBats", 0) or 0)

                if stat.get("hits") is not None:
                    result["hits"] = int(stat.get("hits", 0) or 0)

    except Exception as exc:
        log.warning("Could not load BvP HR history batter=%s pitcher=%s: %s", batter_id, pitcher_id, exc)

    bvp_hr_history_cache[cache_key] = result
    return result


def collect_recent_contact_for_player(player_id, days=3):
    if not player_id:
        return {"near_hr_count": 0, "max_ev": None, "last_hr_ev": None}
    near_hr_count = 0
    max_ev = None
    last_hr_ev = None
    for days_ago in range(days, 0, -1):
        try:
            games = get_games_for_date(day_str(days_ago))
        except Exception:
            continue
        for game in games:
            game_pk = game.get("gamePk")
            if not game_pk:
                continue
            try:
                data = get_json(LIVE_FEED_URL.format(gamePk=game_pk))
                plays = (((data.get("liveData") or {}).get("plays") or {}).get("allPlays") or [])
            except Exception:
                continue
            for play in plays:
                batter_id = (((play.get("matchup") or {}).get("batter") or {}).get("id"))
                if batter_id != player_id:
                    continue
                metrics = get_metrics(play)
                if metrics:
                    ev = metrics.get("launchSpeed")
                    if ev is not None:
                        max_ev = ev if max_ev is None else max(max_ev, ev)
                if is_near_hr(play):
                    near_hr_count += 1
                if is_home_run(play) and metrics and metrics.get("launchSpeed") is not None:
                    last_hr_ev = metrics.get("launchSpeed")
    return {"near_hr_count": near_hr_count, "max_ev": max_ev, "last_hr_ev": last_hr_ev}


def active_roster_players_for_team(team_id, team_abbr):
    try:
        people = get_people_by_ids(get_active_roster_player_ids(team_id))
    except Exception as exc:
        log.warning("Could not load active roster for 2HR watch team %s: %s", team_id, exc)
        return []
    return [{"player_id": p.get("id"), "name": p.get("fullName", "Unknown"), "team_id": team_id, "team_abbr": team_abbr} for p in people]



def collect_recent_contact_for_players(player_ids, days: int = 3):
    """
    Fast batch version. Fetches each recent game feed once, then records contact
    only for candidate player IDs. This prevents !yhr / !2hr from hanging.
    """
    wanted = {pid for pid in player_ids if pid}
    results = {
        pid: {"near_hr_count": 0, "max_ev": None, "last_hr_ev": None}
        for pid in wanted
    }

    if not wanted:
        return results

    for days_ago in range(days, 0, -1):
        date_str = day_str(days_ago)

        try:
            games = get_games_for_date(date_str)
        except Exception as exc:
            log.warning("Could not load games for contact date %s: %s", date_str, exc)
            continue

        for game in games:
            game_pk = game.get("gamePk")
            if not game_pk:
                continue

            try:
                data = get_json(LIVE_FEED_URL.format(gamePk=game_pk))
                plays = (((data.get("liveData") or {}).get("plays") or {}).get("allPlays") or [])
            except Exception as exc:
                log.warning("Could not load feed for contact game %s: %s", game_pk, exc)
                continue

            for play in plays:
                batter_id = (((play.get("matchup") or {}).get("batter") or {}).get("id"))
                if batter_id not in wanted:
                    continue

                row = results.setdefault(batter_id, {"near_hr_count": 0, "max_ev": None, "last_hr_ev": None})
                metrics = get_metrics(play)

                if metrics:
                    ev = metrics.get("launchSpeed")
                    if ev is not None:
                        row["max_ev"] = ev if row["max_ev"] is None else max(row["max_ev"], ev)

                if is_near_hr(play):
                    row["near_hr_count"] += 1

                if is_home_run(play) and metrics and metrics.get("launchSpeed") is not None:
                    row["last_hr_ev"] = metrics.get("launchSpeed")

    return results



# =========================
# TODAY-FIRST HR REPORT ENGINE
# =========================

def get_ballpark_boost_from_game_label(label):
    boosts = {
        "Colorado Rockies": 12,
        "Cincinnati Reds": 8,
        "New York Yankees": 7,
        "Philadelphia Phillies": 5,
        "Boston Red Sox": 4,
        "Chicago White Sox": 4,
        "Baltimore Orioles": 3,
        "Houston Astros": 3,
        "Arizona Diamondbacks": 2,
        "Texas Rangers": 2,
        "Los Angeles Dodgers": 1,
        "Atlanta Braves": 1,
        "San Francisco Giants": -6,
        "Seattle Mariners": -4,
        "Detroit Tigers": -3,
        "New York Mets": -3,
        "Miami Marlins": -2,
        "San Diego Padres": -2,
        "Tampa Bay Rays": -2,
    }

    lower = (label or "").lower()

    for team_name, boost in boosts.items():
        if team_name.lower() in lower:
            return boost

    return 0


def barrel_proxy_score(max_ev, last_hr_ev, near_hr_count):
    ev = safe_float(last_hr_ev or max_ev, 0)
    score = 0

    if ev >= 112:
        score += 16
    elif ev >= 109:
        score += 12
    elif ev >= 106:
        score += 8
    elif ev >= 102:
        score += 4

    score += min(int(near_hr_count or 0), 5) * 3

    return score


def build_today_first_candidates(today_games, hot_streaks, top_pool=50):
    """
    Today-first candidate pool.

    Important change:
    This no longer only uses recent HR hitters. It starts with today's active
    rosters, then merges in hot-streak HR data. That means players who did NOT
    homer yesterday/recently can still show up if their contact + matchup data
    is strong enough.
    """
    context = {}

    for game in today_games:
        away = game.get("away") or {}
        home = game.get("home") or {}

        for team, opp in ((away, home), (home, away)):
            for key in [
                team.get("abbreviation"),
                team.get("teamName"),
                team.get("name"),
                team.get("fileCode"),
            ]:
                if not key:
                    continue

                context[str(key).upper()] = {
                    "team_id": team.get("id"),
                    "team_abbr": team.get("abbreviation") or team.get("teamName") or team.get("name") or "MLB",
                    "game": game,
                    "opponent": (
                        opp.get("abbreviation")
                        or opp.get("teamName")
                        or opp.get("name")
                        or "OPP"
                    ),
                    "ballpark_boost": get_ballpark_boost_from_game_label(game_label(game)),
                }

    # Build player pool from TODAY'S active rosters first.
    pool_by_id = {}

    for ctx in context.values():
        team_id = ctx.get("team_id")
        team_abbr = ctx.get("team_abbr", "MLB")

        if not team_id:
            continue

        try:
            roster_players = active_roster_players_for_team(team_id, team_abbr)
        except Exception as exc:
            log.warning("Could not load today's roster for %s: %s", team_abbr, exc)
            roster_players = []

        for player in roster_players:
            player_id = player.get("player_id")
            if not player_id:
                continue

            pool_by_id[player_id] = {
                "player_id": player_id,
                "name": player.get("name", "Unknown"),
                "team_abbr": team_abbr,
                "total_hr": 0,
                "streak_days": 0,
            }

    # Merge in hot-streak data as a SMALL scoring factor, not the whole pool.
    for hot in hot_streaks or []:
        player_id = hot.get("player_id")
        if not player_id:
            continue

        existing = pool_by_id.setdefault(
            player_id,
            {
                "player_id": player_id,
                "name": hot.get("name", "Unknown"),
                "team_abbr": hot.get("team_abbr", "MLB"),
                "total_hr": 0,
                "streak_days": 0,
            },
        )

        existing["name"] = hot.get("name", existing.get("name", "Unknown"))
        existing["team_abbr"] = hot.get("team_abbr", existing.get("team_abbr", "MLB"))
        existing["total_hr"] = int(hot.get("total_hr", 0) or 0)
        existing["streak_days"] = int(hot.get("streak_days", 0) or 0)

    pool = list(pool_by_id.values())

    player_ids = [
        row.get("player_id")
        for row in pool
        if row.get("player_id")
    ]

    contact_cache = collect_recent_contact_for_players(player_ids, days=3)
    pitcher_cache = {}
    candidates = []

    for hot in pool:
        team_abbr = hot.get("team_abbr", "MLB")
        ctx = context.get(str(team_abbr).upper(), {})
        game = ctx.get("game")

        contact = contact_cache.get(
            hot.get("player_id"),
            {
                "near_hr_count": 0,
                "max_ev": None,
                "last_hr_ev": None,
            },
        )

        pitcher_name = "TBD"
        pitcher_hr9 = None

        if game and ctx.get("team_id"):
            probable = get_probable_pitcher_for_team(game, ctx["team_id"])
            pitcher_id = probable.get("id") if probable else None
            pitcher_name = probable.get("fullName") if probable else "TBD"

            if pitcher_id and pitcher_id not in pitcher_cache:
                pitcher_cache[pitcher_id] = get_pitcher_hr_per_9(pitcher_id)

            pitcher_hr9 = pitcher_cache.get(pitcher_id) if pitcher_id else None

        row = {
            "player_id": hot.get("player_id"),
            "name": hot.get("name", "Unknown"),
            "team_abbr": team_abbr,
            "opponent": ctx.get("opponent", "OPP"),
            "game": game_label(game) if game else "Game TBD",
            "last_7_hr": int(hot.get("total_hr", 0) or 0),
            "streak_days": int(hot.get("streak_days", 0) or 0),
            "near_hr_count": int(contact.get("near_hr_count", 0) or 0),
            "max_ev": contact.get("max_ev"),
            "last_hr_ev": contact.get("last_hr_ev"),
            "pitcher_name": pitcher_name,
            "pitcher_hr9": pitcher_hr9,
            "ballpark_boost": ctx.get("ballpark_boost", 0),
        }

        row["barrel_proxy"] = barrel_proxy_score(
            row["max_ev"],
            row["last_hr_ev"],
            row["near_hr_count"],
        )

        best_ev = max(
            safe_float(row["max_ev"]),
            safe_float(row["last_hr_ev"]),
        )

        score = 0

        # TODAY/recent contact quality is the main signal.
        score += row["near_hr_count"] * 11
        score += max(0, min((best_ev - 98) * 1.55, 24))
        score += row["barrel_proxy"]

        # Today's opposing pitcher + today's park.
        if row["pitcher_hr9"] is not None:
            score += safe_float(row["pitcher_hr9"]) * 13

        score += row["ballpark_boost"] * 1.6

        # Recent HR form is capped and cannot dominate.
        score += min(row["last_7_hr"] * 4, 14)
        score += min(row["streak_days"] * 2, 6)

        row["final_score"] = round(max(0, min(score, 100)), 1)

        two_hr_score = score
        two_hr_score += max(0, min((best_ev - 108) * 2, 14))
        two_hr_score += min(row["near_hr_count"] * 4, 16)

        if row["pitcher_hr9"] is not None and row["pitcher_hr9"] >= 1.35:
            two_hr_score += 8

        if best_ev < 106 or (row["near_hr_count"] == 0 and row["last_7_hr"] < 2):
            two_hr_score -= 10

        row["score"] = round(max(0, min(two_hr_score, 100)), 1)

        # Keep only players with at least one useful signal.
        if (
            row["near_hr_count"] > 0
            or row["barrel_proxy"] >= 4
            or row["pitcher_hr9"] is not None
            or row["ballpark_boost"]
            or row["last_7_hr"] > 0
        ):
            candidates.append(row)

    candidates.sort(
        key=lambda x: (
            -x["final_score"],
            -x["barrel_proxy"],
            -x["near_hr_count"],
            x["name"],
        )
    )

    return candidates[:top_pool]

def build_2hr_watch(
    today_games,
    hot_streaks,
    top_n: int = TWO_HR_WATCH_TOP_N,
):
    if not ENABLE_2HR_WATCH:
        return []

    rows = build_today_first_candidates(
        today_games,
        hot_streaks,
        top_pool=max(top_n * 6, 36),
    )

    rows = [
        r
        for r in rows
        if r["score"] >= TWO_HR_MIN_SCORE
    ]

    rows.sort(
        key=lambda x: (
            -x["score"],
            -x["barrel_proxy"],
            -x["near_hr_count"],
            x["name"],
        )
    )

    for row in rows:
        row["confidence"] = (
            "High"
            if row["score"] >= 72
            else "Medium"
            if row["score"] >= 55
            else "Longshot"
        )

    return rows[:top_n]


def _today_reason_bits(row):
    bits = []

    if row.get("near_hr_count"):
        bits.append(f"{row['near_hr_count']} near-HR last 3d")

    if row.get("last_hr_ev"):
        bits.append(f"{safe_float(row['last_hr_ev']):.1f} EV last HR")
    elif row.get("max_ev"):
        bits.append(f"{safe_float(row['max_ev']):.1f} max EV")

    if row.get("pitcher_hr9") is not None:
        bits.append(f"faces {row.get('pitcher_name', 'SP')} {row['pitcher_hr9']} HR/9")

    if row.get("ballpark_boost"):
        bits.append(f"park {row['ballpark_boost']:+}")

    if row.get("barrel_proxy"):
        bits.append(f"barrel proxy +{row['barrel_proxy']}")

    if row.get("last_7_hr"):
        bits.append(f"{row['last_7_hr']} HR last {HOT_STREAK_DAYS}d")

    return bits or ["today-first matchup edge"]


async def send_2hr_watch_embed(channel, two_hr_watch):
    if not ENABLE_2HR_WATCH:
        return

    if not two_hr_watch:
        await safe_discord_send(
            channel,
            "💥 **2-HR Watch**\nNo qualifying 2-HR spots from today's matchup/contact data."
        )
        return

    embed = discord.Embed(
        title="💥 2-HR Watch",
        description="Today-first model: EV, near-HR contact, barrel proxy, park boost, and opposing pitcher HR/9.",
        color=discord.Color.red(),
    )

    for idx, row in enumerate(two_hr_watch, start=1):
        lines = _today_reason_bits(row)
        lines.append(f"2HR Score: {row['score']}")
        lines.append(f"Confidence: {row['confidence']}")

        embed.add_field(
            name=f"{idx}. {row['name']} ({row['team_abbr']})",
            value="\n".join(f"• {x}" for x in lines[:7]),
            inline=False,
        )

    await safe_discord_send(channel, embed=embed)


def build_stat_only_hr_parlay_picks(hot_streaks, two_hr_watch):
    candidates = build_today_first_candidates(
        get_today_games(),
        hot_streaks,
        top_pool=75,
    )

    by_name = {
        row["name"].lower(): row
        for row in candidates
    }

    for row in two_hr_watch or []:
        key = row["name"].lower()
        by_name[key] = {
            **by_name.get(key, {}),
            **row,
        }

    candidates = list(by_name.values())

    candidates.sort(
        key=lambda x: (
            -x.get("final_score", 0),
            -x.get("barrel_proxy", 0),
            -x.get("near_hr_count", 0),
            x.get("name", ""),
        )
    )

    def player_key(row):
        return str(row.get("player_id") or row.get("name", "")).lower()

    def pick(pool, legs, used_players=None, allow_team_dupes=False):
        picks = []
        used_teams = set()
        used_players = used_players or set()

        for row in pool:
            key = player_key(row)
            team = row.get("team_abbr")

            # Keep the three parlay sections from recycling the same player.
            if key in used_players:
                continue

            # Default: one hitter per MLB team inside the same parlay.
            if not allow_team_dupes and team in used_teams:
                continue

            picks.append(row)
            used_teams.add(team)
            used_players.add(key)

            if len(picks) >= legs:
                break

        return picks

    safe_legs = max(2, min(4, MORNING_PARLAY_LEGS_SAFE))
    risky_legs = max(2, min(4, MORNING_PARLAY_LEGS_RISKY))
    bomb_legs = max(2, min(4, MORNING_PARLAY_LEGS_BOMB))

    safe_pool = [
        c for c in candidates
        if c.get("final_score", 0) >= 62
    ]

    # Risky excludes the safer tier by score, so it finds different names.
    risky_pool = [
        c for c in candidates
        if 48 <= c.get("final_score", 0) < 62
    ]

    # Bomb excludes Safe/Risky score ranges and pulls deeper longshots.
    bomb_pool = [
        c for c in candidates
        if 32 <= c.get("final_score", 0) < 48
    ]

    used_players = set()

    safe_picks = pick(safe_pool or candidates, safe_legs, used_players)

    risky_picks = pick(risky_pool, risky_legs, used_players)
    if len(risky_picks) < risky_legs:
        risky_picks += pick(
            candidates,
            risky_legs - len(risky_picks),
            used_players,
        )

    bomb_picks = pick(bomb_pool, bomb_legs, used_players)
    if len(bomb_picks) < bomb_legs:
        bomb_picks += pick(
            list(reversed(candidates)),
            bomb_legs - len(bomb_picks),
            used_players,
            allow_team_dupes=True,
        )

    return {
        "safe": safe_picks,
        "risky": risky_picks,
        "bomb": bomb_picks,
    }

def _format_stat_parlay_rows(rows):
    if not rows:
        return "Not enough strong candidates today."

    lines = []

    for row in rows:
        lines.append(
            f"• **{row['name']} ({row.get('team_abbr', 'MLB')})** — "
            f"Score {row.get('final_score', row.get('score', 0))}\n  "
            + " | ".join(_today_reason_bits(row)[:5])
        )

    return "\n".join(lines)


async def send_hr_matchup_dashboard(channel, parlays):
    combined = (
        parlays.get("safe", [])
        + parlays.get("risky", [])
        + parlays.get("bomb", [])
    )

    seen = set()
    rows = []

    for row in sorted(
        combined,
        key=lambda x: x.get("final_score", 0),
        reverse=True,
    ):
        if row["name"] in seen:
            continue

        seen.add(row["name"])
        rows.append(row)

        if len(rows) >= 8:
            break

    if not rows:
        return

    embed = discord.Embed(
        title="💣 HR Matchup Dashboard",
        description="Top HR environments from today's slate",
        color=discord.Color.red(),
    )

    for row in rows:
        embed.add_field(
            name=f"{row['name']} ({row.get('team_abbr', 'MLB')}) — {row.get('final_score', 0)}",
            value=" | ".join(_today_reason_bits(row)[:6]),
            inline=False,
        )

    await safe_discord_send(channel, embed=embed)


async def send_barrel_zone_dashboard(channel, parlays):
    combined = (
        parlays.get("safe", [])
        + parlays.get("risky", [])
        + parlays.get("bomb", [])
    )

    seen = set()
    rows = []

    for row in sorted(
        combined,
        key=lambda x: (
            x.get("barrel_proxy", 0),
            x.get("final_score", 0),
        ),
        reverse=True,
    ):
        if row["name"] in seen:
            continue

        seen.add(row["name"])
        rows.append(row)

        if len(rows) >= 6:
            break

    if not rows:
        return

    embed = discord.Embed(
        title="🎯 Barrel Zone Matchups",
        description="EV + near-HR barrel proxy against today's matchup",
        color=discord.Color.gold(),
    )

    for row in rows:
        embed.add_field(
            name=f"{row['name']} ({row.get('team_abbr', 'MLB')})",
            value=" | ".join(_today_reason_bits(row)[:6]),
            inline=False,
        )

    await safe_discord_send(channel, embed=embed)


async def send_morning_hr_parlays_embed(
    channel,
    hot_streaks,
    two_hr_watch,
    today_games,
):
    if not ENABLE_MORNING_HR_PARLAYS:
        return

    parlays = build_stat_only_hr_parlay_picks(
        hot_streaks,
        two_hr_watch,
    )

    if not any(parlays.values()):
        await safe_discord_send(
            channel,
            "💣 **Best HR Parlays Today**\nNo strong today-first HR candidates found."
        )
        return

    embed = discord.Embed(
        title="💣 Best HR Parlays Today",
        description=(
            "Today-first picks using EV, near-HR contact, barrel proxy, "
            "park boost and opposing pitcher HR/9. Recent HRs are capped."
        ),
        color=discord.Color.blue(),
    )

    embed.add_field(
        name=f"🟢 Safe — {len(parlays['safe'])} Legs",
        value=_format_stat_parlay_rows(parlays["safe"]),
        inline=False,
    )

    embed.add_field(
        name=f"🟡 Risky — {len(parlays['risky'])} Legs",
        value=_format_stat_parlay_rows(parlays["risky"]),
        inline=False,
    )

    embed.add_field(
        name=f"🔴 Bomb — {len(parlays['bomb'])} Legs",
        value=_format_stat_parlay_rows(parlays["bomb"]),
        inline=False,
    )

    embed.set_footer(
        text="Stat model only. Add legs manually in your sportsbook. Bet responsibly."
    )

    await safe_discord_send(channel, embed=embed)

    await send_hr_matchup_dashboard(channel, parlays)
    await send_barrel_zone_dashboard(channel, parlays)





# =========================
# REPORT SEND HELPERS
# =========================

def split_lines_into_chunks(header, lines, max_chars=1900):
    """Split long text reports into Discord-safe message chunks."""
    chunks = []
    current = (header or "").strip()

    for line in lines or []:
        line = str(line)
        extra = len(line) + (1 if current else 0)
        if current and len(current) + extra > max_chars:
            chunks.append(current)
            current = ""
        current += ("\n" if current else "") + line

    if current.strip():
        chunks.append(current)

    return chunks


async def safe_send_report_section(section_name, coro):
    """Prevent one optional report section from crashing the entire !yhr report."""
    try:
        return await coro
    except Exception as exc:
        log.exception("Daily report section %s failed: %s", section_name, exc)
        return None


async def send_hot_streaks_embed(channel, hot_streaks):
    """Send top recent HR hitters section used by the daily recap."""
    if not hot_streaks:
        await safe_discord_send(channel, f"🔥 **Hot HR Streaks — Last {HOT_STREAK_DAYS} Days**\nNo hot streaks found.")
        return

    lines = [f"🔥 **Hot HR Streaks — Last {HOT_STREAK_DAYS} Days**", ""]
    for row in hot_streaks[:HOT_STREAK_TOP_N]:
        streak = row.get("streak_days", 0)
        streak_text = f" | {streak} game streak" if streak else ""
        lines.append(f"• **{row.get('name', 'Unknown')} ({row.get('team_abbr', 'MLB')})** — {row.get('total_hr', 0)} HR{streak_text}")

    for chunk in split_lines_into_chunks("", lines):
        await safe_discord_send(channel, chunk)


async def send_birthday_embed(channel, birthday_narratives):
    """Send birthday narrative section used by !yhr and !bday."""
    if not ENABLE_BIRTHDAY_NARRATIVE:
        return

    if not birthday_narratives:
        await safe_discord_send(channel, "🎂 **Birthday HR Narrative**\nNo MLB birthday bats found for today's active rosters.")
        return

    lines = ["🎂 **Birthday HR Narrative**", ""]
    for row in birthday_narratives[:10]:
        age = row.get("age")
        age_text = f" turns {age}" if age else "has a birthday"
        extra = []
        if row.get("last_7_hr"):
            extra.append(f"{row['last_7_hr']} HR last {HOT_STREAK_DAYS}d")
        if row.get("streak_days"):
            extra.append(f"{row['streak_days']} game HR streak")
        reason = f" — {'; '.join(extra)}" if extra else ""
        lines.append(f"• **{row.get('name', 'Unknown')} ({row.get('team_abbr', 'MLB')})**{age_text}{reason}")
        lines.append(f"  {row.get('game', 'Game TBD')}")

    for chunk in split_lines_into_chunks("", lines):
        await safe_discord_send(channel, chunk)


async def post_daily_hr_recap(force=False):
    target_date = yesterday_str()

    if not force and state.get("last_daily_recap_date") == target_date:
        log.info("Daily recap already posted for %s", target_date)
        return

    channel = await client.fetch_channel(DISCORD_CHANNEL_ID)

    try:
        if force:
            await safe_discord_send(channel, "Loading HR recap...")
        recap = await asyncio.wait_for(
            asyncio.to_thread(build_yesterday_recap, target_date),
            timeout=REPORT_BUILD_TIMEOUT_SECONDS,
        )

        if force:
            await safe_discord_send(channel, "Loading hot streaks...")
        hot_streaks = await asyncio.wait_for(
            asyncio.to_thread(build_hot_streaks),
            timeout=REPORT_BUILD_TIMEOUT_SECONDS,
        )

        today_games = await asyncio.to_thread(get_today_games)

        if force:
            await safe_discord_send(channel, "Building 2-HR Watch...")
        two_hr_watch = await asyncio.wait_for(
            asyncio.to_thread(build_2hr_watch, today_games, hot_streaks),
            timeout=REPORT_BUILD_TIMEOUT_SECONDS,
        )

        birthday_narratives = await asyncio.wait_for(
            asyncio.to_thread(build_birthday_narratives, today_games, hot_streaks),
            timeout=REPORT_BUILD_TIMEOUT_SECONDS,
        )

    except asyncio.TimeoutError:
        await safe_discord_send(channel, "Report build timed out while MLB data was loading. Try again in a few minutes.")
        log.exception("Report build timed out")
        return

    except Exception as exc:
        await safe_discord_send(channel, f"Could not build the HR recap right now. Error: `{type(exc).__name__}: {exc}`")
        log.exception("Failed building daily recap: %s", exc)
        return

    games = recap["games"]

    if not games:
        await safe_discord_send(channel, f"💣 **Yesterday's HR Recap — {target_date}**\nNo home runs found.")
    else:
        header = f"💣 **Yesterday's HR Recap — {target_date}**\nTotal HR: **{recap['total_hr']}** | Players: **{recap['unique_players']}**\n\n"
        lines = []
        for game in games:
            lines.append(f"**{game['label']}**")
            for hitter in game["hitters"]:
                lines.append(f"• {hitter['name']} ({hitter['team_abbr']}) — {hitter['hr_count']} HR")
            lines.append("")

        for chunk in split_lines_into_chunks(header, lines):
            await safe_discord_send(channel, chunk)

    await safe_send_report_section("hot_streaks", send_hot_streaks_embed(channel, hot_streaks))
    await safe_send_report_section("2hr_watch", send_2hr_watch_embed(channel, two_hr_watch))
    await safe_send_report_section("morning_hr_parlays", send_morning_hr_parlays_embed(channel, hot_streaks, two_hr_watch, today_games))
    await safe_send_report_section("birthday", send_birthday_embed(channel, birthday_narratives))

    state["last_daily_recap_date"] = target_date
    save_state()
    log.info("Daily recap posted for %s", target_date)


async def maybe_run_scheduled_daily_recap():
    """
    Robust daily scheduler.

    - Fires once after the scheduled time.
    - Allows catch-up for DAILY_RECAP_CATCHUP_HOURS.
    - Claims the report date before building so crashes do not create a spam loop.
      Manual !yhr can still be used to force rebuild.
    """
    global daily_recap_running

    now = datetime.now(TZ)
    target_date = yesterday_str()

    if daily_recap_running:
        return

    if state.get("last_daily_recap_date") == target_date:
        return

    scheduled = now.replace(
        hour=DAILY_RECAP_HOUR,
        minute=DAILY_RECAP_MINUTE,
        second=0,
        microsecond=0,
    )
    catchup_until = scheduled + timedelta(hours=DAILY_RECAP_CATCHUP_HOURS)

    if not (scheduled <= now <= catchup_until):
        return

    log.info(
        "Daily recap scheduler firing. now=%s scheduled=%s target_date=%s",
        now.isoformat(),
        scheduled.isoformat(),
        target_date,
    )

    # Claim date BEFORE build to avoid repeated spam if one section fails.
    state["last_daily_recap_date"] = target_date
    save_state()

    daily_recap_running = True
    try:
        await post_daily_hr_recap(force=False)
    except Exception as exc:
        log.exception("Scheduled daily recap failed after date claim: %s", exc)
    finally:
        daily_recap_running = False




async def loop():
    await client.wait_until_ready()
    channel = await client.fetch_channel(DISCORD_CHANNEL_ID)
    await send_startup_message()
    while True:
        try:
            await maybe_run_scheduled_daily_recap()
            games = get_today_games()
            for game in games:
                try:
                    await process_game(channel, game)
                except Exception as exc:
                    log.warning("Error processing game %s: %s", game.get("gamePk"), exc)
                await asyncio.sleep(1)
        except Exception as exc:
            log.warning("Main loop error: %s", exc)
        save_state()
        await asyncio.sleep(POLL_SECONDS)


@client.event
async def on_ready():
    global loop_task, live_loop_started

    log.info("Bot ready as %s", client.user)
    log.info("Schedule set for %s:%02d %s", DAILY_RECAP_HOUR, DAILY_RECAP_MINUTE, TIMEZONE)
    log.info("Daily recap catch-up hours: %s", DAILY_RECAP_CATCHUP_HOURS)
    log.info("Redeploy protection: skip_old_plays=%s max_minutes=%s", SKIP_OLD_PLAYS_ON_STARTUP, OLD_PLAY_MAX_MINUTES)

    # discord.py may fire on_ready more than once after reconnects.
    # This prevents duplicate polling loops inside one Railway container.
    if live_loop_started:
        log.info("Live alert loop already started; skipping duplicate startup")
        return

    live_loop_started = True

    if loop_task is None or loop_task.done():
        loop_task = client.loop.create_task(loop())
        log.info("Started live alert loop")
        log.info("No HR through 3 alert enabled: %s", ENABLE_NO_HR_THROUGH_3_ALERT)
        log.info("More HR odds enabled: %s", ENABLE_MORE_HR_ODDS)
        log.info("Morning HR parlays enabled: %s", ENABLE_MORNING_HR_PARLAYS)
        log.info("Strikeout alerts enabled: %s channel=%s", ENABLE_STRIKEOUT_ALERTS, DISCORD_STRIKEOUT_CHANNEL_ID or "main")
        log.info("Hard-hit channel=%s pitcher weakspot channel=%s", DISCORD_HARD_HIT_CHANNEL_ID or "main", DISCORD_PITCHER_WEAKSPOT_CHANNEL_ID or "main")
        log.info("Near-HR channel=%s", DISCORD_NEAR_HR_CHANNEL_ID or "main")
        log.info("Cycle watch enabled=%s channel=%s min_inning=%s min_legs=%s", ENABLE_CYCLE_WATCH, DISCORD_CYCLE_CHANNEL_ID or "main", CYCLE_WATCH_MIN_INNING, CYCLE_WATCH_MIN_LEGS)
        log.info("Cycle SportsGameOdds enabled=%s leagueID=%s", ENABLE_CYCLE_SGO_ODDS, SPORTSGAMEODDS_MLB_LEAGUE_ID or "not set")
    else:
        log.info("Live alert loop already running")


@client.event
async def on_message(message):
    if message.author.bot:
        return

    if message.id in processed_message_ids:
        return
    processed_message_ids.add(message.id)
    if len(processed_message_ids) > 500:
        processed_message_ids.clear()
    content = message.content.strip().lower()
    if content == "!yhr":
        await safe_discord_send(message.channel, "Building yesterday's HR recap... this may take a moment while MLB data loads.")
        await post_daily_hr_recap(force=True)
        return
    elif content == "!2hr":
        await safe_discord_send(message.channel, "Building 2-HR Watch... checking recent HR hitters only.")
        try:
            hot_streaks = await asyncio.wait_for(
                asyncio.to_thread(build_hot_streaks),
                timeout=REPORT_BUILD_TIMEOUT_SECONDS,
            )
            today_games = await asyncio.to_thread(get_today_games)
            two_hr_watch = await asyncio.wait_for(
                asyncio.to_thread(build_2hr_watch, today_games, hot_streaks),
                timeout=REPORT_BUILD_TIMEOUT_SECONDS,
            )
        except asyncio.TimeoutError:
            await safe_discord_send(message.channel, "2-HR Watch timed out while MLB data was loading. Try again in a few minutes.")
            log.exception("2HR command timed out")
            return
        except Exception as exc:
            log.exception("2HR command failed: %s", exc)
            await safe_discord_send(message.channel, "Could not build 2-HR Watch right now.")
            return

        await send_2hr_watch_embed(message.channel, two_hr_watch)
        return

    elif content == "!oddsdebug":
        await safe_discord_send(message.channel, 
            f"Odds debug: key_set={bool(ODDS_API_KEY)}, get_odds_json_exists={'get_odds_json' in globals()}, books={ODDS_BOOKMAKERS}"
        )
        return

    elif content == "!oddscache":
        await safe_discord_send(message.channel, cache_status_text())
        return

    elif content == "!refreshhrparlays":
        await safe_discord_send(message.channel, "HR parlays are running in stat-only mode now — no Odds API refresh needed.")
        return


    elif content == "!hrparlays":
        await safe_discord_send(message.channel, "Building today’s best HR parlays... using today-first matchup/contact data.")
        try:
            hot_streaks = await asyncio.to_thread(build_hot_streaks)
            today_games = await asyncio.to_thread(get_today_games)
            two_hr_watch = await asyncio.to_thread(build_2hr_watch, today_games, hot_streaks)
            await send_morning_hr_parlays_embed(message.channel, hot_streaks, two_hr_watch, today_games)

        except Exception as exc:
            log.exception("Morning HR parlays command failed: %s", exc)
            await safe_discord_send(message.channel, "Could not build HR parlays right now.")
        return

    elif content == "!bday":
        await safe_discord_send(message.channel, "Checking today's birthday narrative...")
        try:
            hot_streaks = await asyncio.to_thread(build_hot_streaks)
            today_games = await asyncio.to_thread(get_today_games)
            birthday_narratives = await asyncio.to_thread(build_birthday_narratives, today_games, hot_streaks)
        except Exception as exc:
            log.exception("Birthday command failed: %s", exc)
            await safe_discord_send(message.channel, "Could not load birthday data right now.")
            return
        await send_birthday_embed(message.channel, birthday_narratives)
        return
    elif content == "!schedulestatus":
        now = datetime.now(TZ)
        target_date = yesterday_str()
        await safe_discord_send(message.channel, 
            "Schedule status\n"
            f"Now: `{now.strftime('%Y-%m-%d %I:%M %p %Z')}`\n"
            f"Daily report time: `{DAILY_RECAP_HOUR}:{DAILY_RECAP_MINUTE:02d} {TIMEZONE}`\n"
            f"Target recap date: `{target_date}`\n"
            f"Last posted date: `{state.get('last_daily_recap_date')}`\n"
            f"Catch-up hours: `{DAILY_RECAP_CATCHUP_HOURS}`"
        )
        return

    elif content == "!alertchannels":
        await safe_discord_send(
            message.channel,
            "Alert channels\n"
            f"Main HR: `{DISCORD_CHANNEL_ID}`\n"
            f"Strikeouts: `{DISCORD_STRIKEOUT_CHANNEL_ID or 'main'}`\n"
            f"Hard-Hit Tracker: `{DISCORD_HARD_HIT_CHANNEL_ID or 'main'}`\n"
            f"Pitcher Weakspot: `{DISCORD_PITCHER_WEAKSPOT_CHANNEL_ID or 'main'}`"
        )
        return

    elif content == "!ktest":
        await safe_discord_send(message.channel, 
            f"Strikeout alerts: enabled={ENABLE_STRIKEOUT_ALERTS}, "
            f"channel_id={DISCORD_STRIKEOUT_CHANNEL_ID or 'main'}, "
            f"early={STRIKEOUT_ALERT_MIN_KS}+ Ks by inning {STRIKEOUT_ALERT_MAX_INNING}, extended={STRIKEOUT_EXTENDED_MIN_KS}+ Ks by inning {STRIKEOUT_EXTENDED_MAX_INNING}"
        )
        return

    elif content == "!ping":
        await safe_discord_send(message.channel, "pong")
        return
        return


def main():
    require_env()
    load_state()
    client.run(DISCORD_TOKEN)


if __name__ == "__main__":
    main()
