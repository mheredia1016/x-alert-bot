import os
import json
import asyncio
import logging
import random
from pathlib import Path
from datetime import datetime, timedelta

import discord
import requests
from zoneinfo import ZoneInfo

DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")
DISCORD_CHANNEL_ID = int(os.getenv("DISCORD_CHANNEL_ID", "0"))
DISCORD_STRIKEOUT_CHANNEL_ID = int(os.getenv("DISCORD_STRIKEOUT_CHANNEL_ID", "0"))
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

# Morning HR parlay section in daily report
ENABLE_MORNING_HR_PARLAYS = os.getenv("ENABLE_MORNING_HR_PARLAYS", "true").lower() == "true"
MORNING_PARLAY_LEGS_SAFE = int(os.getenv("MORNING_PARLAY_LEGS_SAFE", "2"))
MORNING_PARLAY_LEGS_RISKY = int(os.getenv("MORNING_PARLAY_LEGS_RISKY", "3"))
MORNING_PARLAY_LEGS_BOMB = int(os.getenv("MORNING_PARLAY_LEGS_BOMB", "4"))



NEAR_HR_MIN_EV = float(os.getenv("NEAR_HR_MIN_EV", "102"))
NEAR_HR_MIN_ANGLE = float(os.getenv("NEAR_HR_MIN_ANGLE", "22"))
NEAR_HR_MAX_ANGLE = float(os.getenv("NEAR_HR_MAX_ANGLE", "38"))
NEAR_HR_MIN_DISTANCE = float(os.getenv("NEAR_HR_MIN_DISTANCE", "375"))
NEAR_HR_CONFIRM_DELAY = float(os.getenv("NEAR_HR_CONFIRM_DELAY", "4"))

STATE_FILE = Path("mlb_hr_alert_state.json")
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

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
log = logging.getLogger("mlb_hr_alert_bot")

intents = discord.Intents.default()
intents.messages = True
intents.message_content = True
client = discord.Client(intents=intents)

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


def save_state():
    state["seen_hr_play_ids"] = state["seen_hr_play_ids"][-500:]
    state["seen_near_hr_play_ids"] = state["seen_near_hr_play_ids"][-1000:]
    state["pending_near_hr_play_ids"] = state["pending_near_hr_play_ids"][-1000:]
    state["seen_strikeout_alerts"] = state.get("seen_strikeout_alerts", [])[-500:]
    for extra_key in ("seen_no_hr_3rd_game_ids", "seen_more_hr_odds_keys", "seen_pregame_parlay_game_ids"):
        if extra_key in state:
            state[extra_key] = state[extra_key][-500:]
    state["seen_no_hr_3rd_game_ids"] = state["seen_no_hr_3rd_game_ids"][-500:]
    state["seen_more_hr_odds_keys"] = state["seen_more_hr_odds_keys"][-500:]
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



async def send_startup_message():
    if DISABLE_STARTUP_MESSAGE:
        return

    channel = await client.fetch_channel(DISCORD_CHANNEL_ID)
    msg_date = today_str()
    if state.get("last_startup_date") == msg_date:
        return
    await channel.send("✅ MLB HR/Near-HR bot is online.")
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
    await channel.send(embed=embed)

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
                await send_alert(channel, game, refreshed_play, "near", plays)

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

    await channel.send(embed=embed)

    state["seen_no_hr_3rd_game_ids"].append(game_key)
    save_state()




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

        await target_channel.send(embed=embed)


async def process_game(channel, game):
    data = get_json(LIVE_FEED_URL.format(gamePk=game["gamePk"]))
    plays = (((data.get("liveData") or {}).get("plays") or {}).get("allPlays") or [])

    await maybe_send_strikeout_alerts(channel, game, data)

    await maybe_send_no_hr_3rd_alert(channel, game, plays)

    for play in plays:
        if "play_is_recent" in globals() and not play_is_recent(play):
            continue

        pid = build_play_id(game["gamePk"], play)

        if is_home_run(play):
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


def build_2hr_watch(today_games, hot_streaks, top_n: int = TWO_HR_WATCH_TOP_N):
    """
    Fast MLB-only 2-HR Watch.
    Always shows top recent HR hitters when possible, but avoids per-player game-feed scans.
    """
    if not ENABLE_2HR_WATCH:
        return []

    candidates = []
    pitcher_hr9_cache = {}

    # Map team keys -> today's game/team id.
    team_context = {}
    for game in today_games:
        for team in (game.get("away") or {}, game.get("home") or {}):
            keys = [
                team.get("abbreviation"),
                team.get("teamName"),
                team.get("name"),
                team.get("fileCode"),
            ]
            team_id = team.get("id")
            for key in keys:
                if key:
                    team_context[str(key).upper()] = {
                        "team_id": team_id,
                        "game": game,
                    }

    # Only evaluate recent HR hitters, not every active player.
    pool = hot_streaks[: max(top_n * 4, 20)]
    candidate_ids = [row.get("player_id") for row in pool if row.get("player_id")]
    contact_cache = collect_recent_contact_for_players(candidate_ids, days=3)

    for hot in pool:
        player_id = hot.get("player_id")
        player_name = hot.get("name", "Unknown")
        team_abbr = hot.get("team_abbr", "MLB")
        last_7_hr = int(hot.get("total_hr", 0) or 0)
        streak_days = int(hot.get("streak_days", 0) or 0)

        if last_7_hr <= 0:
            continue

        context = team_context.get(str(team_abbr).upper())

        game = context["game"] if context else None
        team_id = context["team_id"] if context else None

        pitcher_name = "TBD"
        pitcher_hr9 = None

        if game and team_id:
            probable_pitcher = get_probable_pitcher_for_team(game, team_id)
            pitcher_id = probable_pitcher.get("id") if probable_pitcher else None
            pitcher_name = probable_pitcher.get("fullName") if probable_pitcher else "TBD"

            if pitcher_id and pitcher_id not in pitcher_hr9_cache:
                pitcher_hr9_cache[pitcher_id] = get_pitcher_hr_per_9(pitcher_id)

            pitcher_hr9 = pitcher_hr9_cache.get(pitcher_id) if pitcher_id else None

        contact = contact_cache.get(player_id, {"near_hr_count": 0, "max_ev": None, "last_hr_ev": None})
        near_hr_count = int(contact.get("near_hr_count", 0) or 0)
        max_ev = contact.get("max_ev")
        last_hr_ev = contact.get("last_hr_ev")

        score = 0
        score += last_7_hr * 12
        score += streak_days * 7
        score += near_hr_count * 8

        if last_hr_ev:
            score += max(0, last_hr_ev - 95) * 0.8
        elif max_ev:
            score += max(0, max_ev - 98) * 0.5

        if pitcher_hr9:
            score += pitcher_hr9 * 10

        score = round(score)

        if score >= 55:
            confidence = "High"
        elif score >= 30:
            confidence = "Medium"
        else:
            confidence = "Low"

        candidates.append({
            "name": player_name,
            "team_abbr": team_abbr,
            "game": game_label(game) if game else "Game match TBD",
            "last_7_hr": last_7_hr,
            "streak_days": streak_days,
            "near_hr_count": near_hr_count,
            "max_ev": max_ev,
            "last_hr_ev": last_hr_ev,
            "pitcher_name": pitcher_name,
            "pitcher_hr9": pitcher_hr9,
            "score": score,
            "confidence": confidence,
        })

    candidates.sort(key=lambda x: (-x["score"], -x["last_7_hr"], -x["near_hr_count"], x["name"]))
    return candidates[:top_n]


async def send_2hr_watch_embed(channel, two_hr_watch):
    if not ENABLE_2HR_WATCH:
        return
    if not two_hr_watch:
        await channel.send("💥 **2-HR Watch**\nNo recent HR hitters found for today’s slate.")
        return
    embed = discord.Embed(title="💥 2-HR Watch", color=discord.Color.red(), description="MLB-only model: recent HR form, near-HR contact, EV, and opposing probable pitcher HR/9")
    for idx, row in enumerate(two_hr_watch, start=1):
        lines = [f"• {row['last_7_hr']} HR last {HOT_STREAK_DAYS} days"]
        if row["near_hr_count"] > 0:
            lines.append(f"• {row['near_hr_count']} near-HR balls last 3 games")
        if row["last_hr_ev"]:
            lines.append(f"• {row['last_hr_ev']:.1f} EV last HR")
        elif row["max_ev"]:
            lines.append(f"• {row['max_ev']:.1f} max EV last 3 games")
        if row["pitcher_hr9"] is not None:
            lines.append(f"• Faces {row['pitcher_name']} allowing {row['pitcher_hr9']} HR/9")
        else:
            lines.append("• Opposing probable pitcher HR/9: TBD")
        lines.append(f"• Score: {row['score']}")
        if row.get("confidence"):
            lines.append(f"• Confidence: {row['confidence']}")
        embed.add_field(name=f"{idx}. {row['name']} ({row['team_abbr']})", value="\n".join(lines), inline=False)
    embed.set_footer(text="Not betting advice. Score is a simple signal model from MLB Stats API data.")
    await channel.send(embed=embed)


async def send_birthday_embed(channel, birthday_narratives):
    if not ENABLE_BIRTHDAY_NARRATIVE:
        return
    if not birthday_narratives:
        await channel.send("🎂 **Birthday Narrative Watch**\nNo active roster birthday matches found for teams playing today.")
        return
    embed = discord.Embed(title="🎂 Birthday Narrative Watch", color=discord.Color.magenta(), description="Active roster players with birthdays today whose team plays today")
    for player in birthday_narratives[:10]:
        age_text = f"turns {player['age']} today" if player["age"] else "birthday today"
        streak_text = ""
        if player["last_7_hr"] > 0:
            streak_text = f"\n{player['last_7_hr']} HR in last {HOT_STREAK_DAYS} days"
            if player["streak_days"] > 0:
                streak_text += f" | {player['streak_days']} straight-day streak"
        embed.add_field(name=f"{player['name']} ({player['team_abbr']})", value=f"{age_text}\n{player['game']}{streak_text}", inline=False)
    headshot = player_headshot(birthday_narratives[0].get("player_id"))
    if headshot:
        embed.set_thumbnail(url=headshot)
    embed.set_footer(text="Birthday note means active roster + team has a game today; lineup not guaranteed.")
    await channel.send(embed=embed)



def split_lines_into_chunks(header: str, lines: list[str], limit: int = 1900):
    chunks = []
    current = header

    for line in lines:
        addition = f"{line}\n"

        if len(current) + len(addition) > limit:
            chunks.append(current.rstrip())
            current = addition
        else:
            current += addition

    if current.strip():
        chunks.append(current.rstrip())

    return chunks



# =========================
# SAFE FALLBACK EMBEDS / OPTIONAL FEATURES
# =========================

async def send_hot_streaks_embed(channel, hot_streaks):
    """
    Safe fallback in case a previous build references this function.
    Posts hot streaks without crashing the scheduler.
    """
    if not hot_streaks:
        return

    embed = discord.Embed(
        title=f"🔥 Hot HR Streaks (Last {HOT_STREAK_DAYS} Days)",
        color=discord.Color.gold(),
    )

    for row in hot_streaks[:HOT_STREAK_TOP_N]:
        streak_days = int(row.get("streak_days", 0) or 0)
        streak_text = f"{streak_days} straight day" if streak_days == 1 else f"{streak_days} straight days"
        if streak_days == 0:
            streak_text = "No current streak"

        embed.add_field(
            name=f"{row.get('name', 'Unknown')} ({row.get('team_abbr', 'MLB')})",
            value=f"{row.get('total_hr', 0)} HR last {HOT_STREAK_DAYS} days\n{streak_text}",
            inline=False,
        )

    await channel.send(embed=embed)


async def send_more_hr_odds_after_delay(*args, **kwargs):
    """
    No-op fallback. Keeps HR alerts from crashing if MORE_HR odds are disabled
    or if the full Odds API follow-up module is not present.
    """
    return



async def safe_send_report_section(section_name, coro):
    try:
        await coro
    except Exception as exc:
        log.exception("Report section failed: %s: %s", section_name, exc)

async def post_daily_hr_recap(force=False):
    target_date = yesterday_str()

    if not force and state.get("last_daily_recap_date") == target_date:
        log.info("Daily recap already posted for %s", target_date)
        return

    channel = await client.fetch_channel(DISCORD_CHANNEL_ID)

    try:
        if force:
            await channel.send("Loading HR recap...")
        recap = await asyncio.wait_for(
            asyncio.to_thread(build_yesterday_recap, target_date),
            timeout=REPORT_BUILD_TIMEOUT_SECONDS,
        )

        if force:
            await channel.send("Loading hot streaks...")
        hot_streaks = await asyncio.wait_for(
            asyncio.to_thread(build_hot_streaks),
            timeout=REPORT_BUILD_TIMEOUT_SECONDS,
        )

        today_games = await asyncio.to_thread(get_today_games)

        if force:
            await channel.send("Building 2-HR Watch...")
        two_hr_watch = await asyncio.wait_for(
            asyncio.to_thread(build_2hr_watch, today_games, hot_streaks),
            timeout=REPORT_BUILD_TIMEOUT_SECONDS,
        )

        birthday_narratives = await asyncio.wait_for(
            asyncio.to_thread(build_birthday_narratives, today_games, hot_streaks),
            timeout=REPORT_BUILD_TIMEOUT_SECONDS,
        )

    except asyncio.TimeoutError:
        await channel.send("Report build timed out while MLB data was loading. Try again in a few minutes.")
        log.exception("Report build timed out")
        return

    except Exception as exc:
        await channel.send(f"Could not build the HR recap right now. Error: `{type(exc).__name__}: {exc}`")
        log.exception("Failed building daily recap: %s", exc)
        return

    games = recap["games"]

    if not games:
        await channel.send(f"💣 **Yesterday's HR Recap — {target_date}**\nNo home runs found.")
    else:
        header = f"💣 **Yesterday's HR Recap — {target_date}**\nTotal HR: **{recap['total_hr']}** | Players: **{recap['unique_players']}**\n\n"
        lines = []
        for game in games:
            lines.append(f"**{game['label']}**")
            for hitter in game["hitters"]:
                lines.append(f"• {hitter['name']} ({hitter['team_abbr']}) — {hitter['hr_count']} HR")
            lines.append("")

        for chunk in split_lines_into_chunks(header, lines):
            await channel.send(chunk)

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
    else:
        log.info("Live alert loop already running")


@client.event
async def on_message(message):
    if message.author.bot:
        return
    content = message.content.strip().lower()
    if content == "!yhr":
        await message.channel.send("Building yesterday's HR recap... this may take a moment while MLB data loads.")
        await post_daily_hr_recap(force=True)
    elif content == "!2hr":
        await message.channel.send("Building 2-HR Watch... checking recent HR hitters only.")
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
            await message.channel.send("2-HR Watch timed out while MLB data was loading. Try again in a few minutes.")
            log.exception("2HR command timed out")
            return
        except Exception as exc:
            log.exception("2HR command failed: %s", exc)
            await message.channel.send("Could not build 2-HR Watch right now.")
            return

        await send_2hr_watch_embed(message.channel, two_hr_watch)

    elif content == "!oddsdebug":
        await message.channel.send(
            f"Odds debug: key_set={bool(ODDS_API_KEY)}, get_odds_json_exists={'get_odds_json' in globals()}, books={ODDS_BOOKMAKERS}"
        )

    elif content == "!oddscache":
        await message.channel.send(cache_status_text())

    elif content == "!refreshhrparlays":
        await message.channel.send("Refreshing HR odds cache once...")
        try:
            today_games = await asyncio.to_thread(get_today_games)
            odds_rows = await asyncio.to_thread(build_today_hr_odds_rows, today_games, True)
            await message.channel.send(f"Refreshed HR odds cache: {len(odds_rows)} player lines.")
        except Exception as exc:
            log.exception("Refresh HR parlays failed: %s", exc)
            await message.channel.send(f"Could not refresh HR odds cache: `{type(exc).__name__}: {exc}`")

    elif content == "!hrparlays":
        await message.channel.send("Building today’s best HR parlays... using cached odds when available.")
        try:
            hot_streaks = await asyncio.to_thread(build_hot_streaks)
            today_games = await asyncio.to_thread(get_today_games)
            two_hr_watch = await asyncio.to_thread(build_2hr_watch, today_games, hot_streaks)
            await send_morning_hr_parlays_embed(message.channel, hot_streaks, two_hr_watch, today_games)

        except Exception as exc:
            log.exception("Morning HR parlays command failed: %s", exc)
            await message.channel.send("Could not build HR parlays right now.")

    elif content == "!bday":
        await message.channel.send("Checking today's birthday narrative...")
        try:
            hot_streaks = await asyncio.to_thread(build_hot_streaks)
            today_games = await asyncio.to_thread(get_today_games)
            birthday_narratives = await asyncio.to_thread(build_birthday_narratives, today_games, hot_streaks)
        except Exception as exc:
            log.exception("Birthday command failed: %s", exc)
            await message.channel.send("Could not load birthday data right now.")
            return
        await send_birthday_embed(message.channel, birthday_narratives)
    elif content == "!schedulestatus":
        now = datetime.now(TZ)
        target_date = yesterday_str()
        await message.channel.send(
            "Schedule status\n"
            f"Now: `{now.strftime('%Y-%m-%d %I:%M %p %Z')}`\n"
            f"Daily report time: `{DAILY_RECAP_HOUR}:{DAILY_RECAP_MINUTE:02d} {TIMEZONE}`\n"
            f"Target recap date: `{target_date}`\n"
            f"Last posted date: `{state.get('last_daily_recap_date')}`\n"
            f"Catch-up hours: `{DAILY_RECAP_CATCHUP_HOURS}`"
        )

    elif content == "!ktest":
        await message.channel.send(
            f"Strikeout alerts: enabled={ENABLE_STRIKEOUT_ALERTS}, "
            f"channel_id={DISCORD_STRIKEOUT_CHANNEL_ID or 'main'}, "
            f"early={STRIKEOUT_ALERT_MIN_KS}+ Ks by inning {STRIKEOUT_ALERT_MAX_INNING}, extended={STRIKEOUT_EXTENDED_MIN_KS}+ Ks by inning {STRIKEOUT_EXTENDED_MAX_INNING}"
        )

    elif content == "!ping":
        await message.channel.send("pong")


def main():
    require_env()
    load_state()
    client.run(DISCORD_TOKEN)


if __name__ == "__main__":
    main()
