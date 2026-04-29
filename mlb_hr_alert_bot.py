
import os
import json
import asyncio
import logging
from pathlib import Path
from datetime import datetime, timedelta

import discord
import requests
from zoneinfo import ZoneInfo

DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")
DISCORD_CHANNEL_ID = int(os.getenv("DISCORD_CHANNEL_ID", "0"))
POLL_SECONDS = int(os.getenv("POLL_SECONDS", "15"))
TIMEZONE = os.getenv("TIMEZONE", "America/Chicago")

DAILY_RECAP_HOUR = int(os.getenv("DAILY_RECAP_HOUR", "8"))
DAILY_RECAP_MINUTE = int(os.getenv("DAILY_RECAP_MINUTE", "0"))
HOT_STREAK_DAYS = int(os.getenv("HOT_STREAK_DAYS", "7"))
HOT_STREAK_TOP_N = int(os.getenv("HOT_STREAK_TOP_N", "8"))
TWO_HR_WATCH_TOP_N = int(os.getenv("TWO_HR_WATCH_TOP_N", "6"))
TWO_HR_MIN_SCORE = float(os.getenv("TWO_HR_MIN_SCORE", "35"))
ENABLE_2HR_WATCH = os.getenv("ENABLE_2HR_WATCH", "true").lower() == "true"
ENABLE_BIRTHDAY_NARRATIVE = os.getenv("ENABLE_BIRTHDAY_NARRATIVE", "true").lower() == "true"

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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
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
    "AZ": "ari", "ATL": "atl", "BAL": "bal", "BOS": "bos", "CHC": "chc",
    "CWS": "chw", "CIN": "cin", "CLE": "cle", "COL": "col", "DET": "det",
    "HOU": "hou", "KC": "kc", "LAA": "laa", "LAD": "lad", "MIA": "mia",
    "MIL": "mil", "MIN": "min", "NYM": "nym", "NYY": "nyy", "ATH": "oak",
    "OAK": "oak", "PHI": "phi", "PIT": "pit", "SD": "sd", "SEA": "sea",
    "SF": "sf", "STL": "stl", "TB": "tb", "TEX": "tex", "TOR": "tor", "WSH": "wsh",
}


def require_env() -> None:
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


def save_state():
    state["seen_hr_play_ids"] = state["seen_hr_play_ids"][-500:]
    state["seen_near_hr_play_ids"] = state["seen_near_hr_play_ids"][-1000:]
    state["pending_near_hr_play_ids"] = state["pending_near_hr_play_ids"][-1000:]
    STATE_FILE.write_text(json.dumps(state, indent=2))


def get_json(url: str) -> dict:
    resp = session.get(url, timeout=25)
    resp.raise_for_status()
    return resp.json()


def today_str() -> str:
    return datetime.now(TZ).strftime("%Y-%m-%d")


def day_str(days_ago: int) -> str:
    return (datetime.now(TZ) - timedelta(days=days_ago)).strftime("%Y-%m-%d")


def yesterday_str() -> str:
    return day_str(1)


def game_label(game):
    away = game["away"]
    home = game["home"]
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


def age_on_date(birth_date_str: str, date_obj: datetime):
    try:
        birth = datetime.strptime(birth_date_str, "%Y-%m-%d")
    except Exception:
        return None

    age = date_obj.year - birth.year
    if (date_obj.month, date_obj.day) < (birth.month, birth.day):
        age -= 1
    return age


def is_home_run(play):
    result = play.get("result", {})
    event_type = (result.get("eventType") or "").lower().strip()
    event = (result.get("event") or "").lower().strip()
    description = (result.get("description") or "").lower().strip()

    return (
        event_type == "home_run"
        or "home run" in event
        or "homer" in event
        or "home run" in description
        or "homers" in description
        or "grand slam" in description
    )


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


def get_games_for_date(date_str: str):
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
    about = play.get("about", {}) or {}
    return f"{game_pk}-{about.get('atBatIndex')}"


async def send_startup_message():
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

    hr_number_text = None
    if alert_type == "hr" and all_plays:
        hr_number = player_hr_number_in_game(all_plays, play)
        if hr_number:
            hr_number_text = f"{ordinal(hr_number)} HR of the game"

    embed = discord.Embed(
        title=title,
        description=result.get("description", "No description available"),
        color=color,
        url=f"https://www.mlb.com/gameday/{game['gamePk']}",
    )

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


async def confirm_and_send_near_hr(channel, game, play_id):
    await asyncio.sleep(NEAR_HR_CONFIRM_DELAY)

    try:
        data = get_json(LIVE_FEED_URL.format(gamePk=game["gamePk"]))
        plays = (((data.get("liveData") or {}).get("plays") or {}).get("allPlays") or [])

        refreshed_play = None
        for play in plays:
            if build_play_id(game["gamePk"], play) == play_id:
                refreshed_play = play
                break

        if not refreshed_play:
            if play_id in state["pending_near_hr_play_ids"]:
                state["pending_near_hr_play_ids"].remove(play_id)
                save_state()
            return

        if is_home_run(refreshed_play):
            if play_id in state["pending_near_hr_play_ids"]:
                state["pending_near_hr_play_ids"].remove(play_id)
            if play_id not in state["seen_hr_play_ids"]:
                await send_alert(channel, game, refreshed_play, "hr", plays)
                state["seen_hr_play_ids"].append(play_id)
            save_state()
            return

        if is_near_hr(refreshed_play):
            if play_id in state["pending_near_hr_play_ids"]:
                state["pending_near_hr_play_ids"].remove(play_id)
            if play_id not in state["seen_near_hr_play_ids"]:
                await send_alert(channel, game, refreshed_play, "near", plays)
                state["seen_near_hr_play_ids"].append(play_id)
            save_state()
            return

        if play_id in state["pending_near_hr_play_ids"]:
            state["pending_near_hr_play_ids"].remove(play_id)
        save_state()

    except Exception as exc:
        log.warning("Error confirming near HR %s: %s", play_id, exc)
        if play_id in state["pending_near_hr_play_ids"]:
            state["pending_near_hr_play_ids"].remove(play_id)
        save_state()


async def process_game(channel, game):
    data = get_json(LIVE_FEED_URL.format(gamePk=game["gamePk"]))
    plays = (((data.get("liveData") or {}).get("plays") or {}).get("allPlays") or [])

    for play in plays:
        pid = build_play_id(game["gamePk"], play)

        if is_home_run(play):
            if pid not in state["seen_hr_play_ids"]:
                await send_alert(channel, game, play, "hr", plays)
                state["seen_hr_play_ids"].append(pid)

            if pid in state["pending_near_hr_play_ids"]:
                state["pending_near_hr_play_ids"].remove(pid)

            continue

        if is_near_hr(play):
            if (
                pid not in state["seen_near_hr_play_ids"]
                and pid not in state["pending_near_hr_play_ids"]
                and pid not in state["seen_hr_play_ids"]
            ):
                state["pending_near_hr_play_ids"].append(pid)
                asyncio.create_task(confirm_and_send_near_hr(channel, game, pid))

    save_state()


def _collect_boxscore_hitters(game_pk: int):
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
                results.append({
                    "player_id": person.get("id"),
                    "name": person.get("fullName", "Unknown"),
                    "team_abbr": team_abbr,
                    "hr_count": hr_count,
                })

    return results


def build_yesterday_recap(target_date: str):
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


def build_hot_streaks(end_date_days_ago: int = 1, window_days: int = HOT_STREAK_DAYS, top_n: int = HOT_STREAK_TOP_N):
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
                    totals[key] = {
                        "player_id": hitter["player_id"],
                        "name": hitter["name"],
                        "team_abbr": hitter["team_abbr"],
                        "total_hr": 0,
                        "days": {},
                    }

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

        hot.append({
            "player_id": data["player_id"],
            "name": data["name"],
            "team_abbr": data["team_abbr"],
            "total_hr": data["total_hr"],
            "streak_days": streak_days,
        })

    hot.sort(key=lambda x: (-x["total_hr"], -x["streak_days"], x["name"]))
    return hot[:top_n]



def get_current_season() -> str:
    return datetime.now(TZ).strftime("%Y")


def safe_float(value, default=0.0):
    try:
        if value in (None, "", "-.--"):
            return default
        return float(value)
    except Exception:
        return default


def get_probable_pitcher_for_team(game: dict, batter_team_id: int):
    """
    Returns the opposing probable pitcher for the batter's team.
    MLB schedule data often includes probablePitcher when announced.
    """
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


def get_pitcher_hr_per_9(pitcher_id: int):
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


def collect_recent_contact_for_player(player_id: int, days: int = 3):
    """
    Pulls recent game feeds and calculates:
    - near-HR balls
    - max EV
    - last HR EV
    Uses the same hitData fields as the live alerts.
    """
    if not player_id:
        return {"near_hr_count": 0, "max_ev": None, "last_hr_ev": None}

    near_hr_count = 0
    max_ev = None
    last_hr_ev = None

    for days_ago in range(days, 0, -1):
        date_str = day_str(days_ago)

        try:
            games = get_games_for_date(date_str)
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

    return {
        "near_hr_count": near_hr_count,
        "max_ev": max_ev,
        "last_hr_ev": last_hr_ev,
    }


def active_roster_players_for_team(team_id: int, team_abbr: str):
    try:
        roster_ids = get_active_roster_player_ids(team_id)
        people = get_people_by_ids(roster_ids)
    except Exception as exc:
        log.warning("Could not load active roster for 2HR watch team %s: %s", team_id, exc)
        return []

    players = []
    for person in people:
        players.append({
            "player_id": person.get("id"),
            "name": person.get("fullName", "Unknown"),
            "team_id": team_id,
            "team_abbr": team_abbr,
        })

    return players


def build_2hr_watch(today_games: list[dict], hot_streaks: list[dict], top_n: int = TWO_HR_WATCH_TOP_N):
    """
    MLB-only 2-HR watch.
    Uses:
    - last 7 HR from hot streaks
    - near-HR balls last 3 days
    - last HR EV / max recent EV
    - opposing probable pitcher HR/9, when available
    """
    if not ENABLE_2HR_WATCH:
        return []

    hot_lookup = {row["player_id"]: row for row in hot_streaks if row.get("player_id")}
    candidates = []

    for game in today_games:
        for team in (game.get("away") or {}, game.get("home") or {}):
            team_id = team.get("id")
            team_abbr = team.get("abbreviation") or team.get("teamName") or team.get("name") or "MLB"
            if not team_id:
                continue

            players = active_roster_players_for_team(team_id, team_abbr)
            probable_pitcher = get_probable_pitcher_for_team(game, team_id)
            pitcher_id = probable_pitcher.get("id") if probable_pitcher else None
            pitcher_name = probable_pitcher.get("fullName") if probable_pitcher else "TBD"
            pitcher_hr9 = get_pitcher_hr_per_9(pitcher_id) if pitcher_id else None

            for player in players:
                player_id = player.get("player_id")
                hot = hot_lookup.get(player_id, {
                    "total_hr": 0,
                    "streak_days": 0,
                    "team_abbr": team_abbr,
                    "name": player["name"],
                    "player_id": player_id,
                })

                contact = collect_recent_contact_for_player(player_id, days=3)

                last_7_hr = int(hot.get("total_hr", 0) or 0)
                streak_days = int(hot.get("streak_days", 0) or 0)
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

                if score < TWO_HR_MIN_SCORE:
                    continue

                candidates.append({
                    "name": player["name"],
                    "team_abbr": team_abbr,
                    "game": game_label(game),
                    "last_7_hr": last_7_hr,
                    "streak_days": streak_days,
                    "near_hr_count": near_hr_count,
                    "max_ev": max_ev,
                    "last_hr_ev": last_hr_ev,
                    "pitcher_name": pitcher_name,
                    "pitcher_hr9": pitcher_hr9,
                    "score": round(score),
                })

    candidates.sort(key=lambda x: (-x["score"], -x["last_7_hr"], -x["near_hr_count"], x["name"]))
    return candidates[:top_n]


async def send_2hr_watch_embed(channel, two_hr_watch):
    if not ENABLE_2HR_WATCH:
        return

    if not two_hr_watch:
        await channel.send("💥 **2-HR Watch**\nNo strong 2-HR candidates found from MLB-only signals today.")
        return

    embed = discord.Embed(
        title="💥 2-HR Watch",
        color=discord.Color.red(),
        description="MLB-only model: recent HR form, near-HR contact, EV, and opposing probable pitcher HR/9",
    )

    for idx, row in enumerate(two_hr_watch, start=1):
        lines = []
        lines.append(f"• {row['last_7_hr']} HR last {HOT_STREAK_DAYS} days")

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

        embed.add_field(
            name=f"{idx}. {row['name']} ({row['team_abbr']})",
            value="\n".join(lines),
            inline=False,
        )

    embed.set_footer(text="Not betting advice. Score is a simple signal model from MLB Stats API data.")
    await channel.send(embed=embed)

def get_active_roster_player_ids(team_id: int):
    data = get_json(TEAM_ROSTER_URL.format(teamId=team_id))
    return [
        row.get("person", {}).get("id")
        for row in data.get("roster", [])
        if row.get("person", {}).get("id")
    ]


def get_people_by_ids(player_ids):
    people = []
    chunk_size = 100

    for i in range(0, len(player_ids), chunk_size):
        chunk = player_ids[i:i + chunk_size]
        ids = ",".join(str(x) for x in chunk)
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
            team_games[team_id] = {
                "team_abbr": team.get("abbreviation") or team.get("teamName") or team.get("name") or "MLB",
                "game": game_label(game),
            }

    birthday_players = []

    for team_id in sorted(team_ids):
        try:
            roster_ids = get_active_roster_player_ids(team_id)
            people = get_people_by_ids(roster_ids)
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


async def send_birthday_embed(channel, birthday_narratives):
    if not birthday_narratives:
        await channel.send("🎂 **Birthday Narrative Watch**\nNo active roster birthday matches found for teams playing today.")
        return

    embed = discord.Embed(
        title="🎂 Birthday Narrative Watch",
        color=discord.Color.magenta(),
        description="Active roster players with birthdays today whose team plays today",
    )

    for player in birthday_narratives[:10]:
        age_text = f"turns {player['age']} today" if player["age"] else "birthday today"
        streak_text = ""
        if player["last_7_hr"] > 0:
            streak_text = f"\n{player['last_7_hr']} HR in last {HOT_STREAK_DAYS} days"
            if player["streak_days"] > 0:
                streak_text += f" | {player['streak_days']} straight-day streak"

        embed.add_field(
            name=f"{player['name']} ({player['team_abbr']})",
            value=f"{age_text}\n{player['game']}{streak_text}",
            inline=False,
        )

    headshot = player_headshot(birthday_narratives[0].get("player_id"))
    if headshot:
        embed.set_thumbnail(url=headshot)

    embed.set_footer(text="Birthday note means active roster + team has a game today; lineup not guaranteed.")
    await channel.send(embed=embed)


async def post_daily_hr_recap(force: bool = False):
    target_date = yesterday_str()

    if not force and state.get("last_daily_recap_date") == target_date:
        log.info("Daily recap already posted for %s", target_date)
        return

    channel = await client.fetch_channel(DISCORD_CHANNEL_ID)

    try:
    recap = await asyncio.to_thread(build_yesterday_recap, target_date)
    hot_streaks = await asyncio.to_thread(build_hot_streaks)
    today_games = await asyncio.to_thread(get_today_games)
    birthday_narratives = await asyncio.to_thread(build_birthday_narratives, today_games, hot_streaks)
    two_hr_watch = await asyncio.to_thread(build_2hr_watch, today_games, hot_streaks)

except Exception as exc:
    log.exception("Failed building daily recap: %s", exc)
    return

    games = recap["games"]

    if not games:
        await channel.send(f"💣 **Yesterday's HR Recap — {target_date}**\nNo home runs found.")
    else:
        header = (
            f"💣 **Yesterday's HR Recap — {target_date}**\n"
            f"Total HR: **{recap['total_hr']}** | Players: **{recap['unique_players']}**\n\n"
        )

        lines = []
        for game in games:
            lines.append(f"**{game['label']}**")
            for hitter in game["hitters"]:
                lines.append(f"• {hitter['name']} ({hitter['team_abbr']}) — {hitter['hr_count']} HR")
            lines.append("")

        for chunk in split_lines_into_chunks(header, lines):
            await channel.send(chunk)

    if hot_streaks:
        embed = discord.Embed(
            title=f"🔥 Hot HR Streaks (Last {HOT_STREAK_DAYS} Days)",
            color=discord.Color.gold(),
            description="Top homer hitters heading into today",
        )

        for row in hot_streaks:
            streak_text = f"{row['streak_days']} straight day" if row["streak_days"] == 1 else f"{row['streak_days']} straight days"
            if row["streak_days"] == 0:
                streak_text = "No current streak"

            embed.add_field(
                name=f"{row['name']} ({row['team_abbr']})",
                value=f"{row['total_hr']} HR in last {HOT_STREAK_DAYS} days\n{streak_text}",
                inline=False,
            )

        top_logo = team_logo(hot_streaks[0]["team_abbr"])
        if top_logo:
            embed.set_thumbnail(url=top_logo)

        embed.set_footer(text=f"Updated daily at {DAILY_RECAP_HOUR}:{DAILY_RECAP_MINUTE:02d} {TIMEZONE}")
        await channel.send(embed=embed)

    await send_2hr_watch_embed(channel, two_hr_watch)

    await send_birthday_embed(channel, birthday_narratives)

    state["last_daily_recap_date"] = target_date
    save_state()
    log.info("Daily recap posted for %s", target_date)


async def maybe_run_scheduled_daily_recap():
    now = datetime.now(TZ)
    schedule_key = now.strftime("%Y-%m-%d-%H-%M")

    if (
        now.hour == DAILY_RECAP_HOUR
        and now.minute == DAILY_RECAP_MINUTE
        and state.get("last_schedule_check_minute") != schedule_key
    ):
        state["last_schedule_check_minute"] = schedule_key
        save_state()
        log.info("Scheduled daily recap trigger fired at %s", now.isoformat())
        await post_daily_hr_recap(force=False)


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
    global loop_task

    log.info("Bot ready as %s", client.user)
    log.info("Schedule set for %s:%02d %s", DAILY_RECAP_HOUR, DAILY_RECAP_MINUTE, TIMEZONE)

    if loop_task is None or loop_task.done():
        loop_task = client.loop.create_task(loop())
        log.info("Started live alert loop")
    else:
        log.info("Live alert loop already running")


@client.event
async def on_message(message):
    if message.author.bot:
        return

    content = message.content.strip().lower()

    if content == "!yhr":
        await message.channel.send("Building yesterday's HR recap...")
        await post_daily_hr_recap(force=True)

    elif content == "!bday":
        await message.channel.send("Checking today's birthday narrative...")
        try:
            hot_streaks = await asyncio.to_thread(build_hot_streaks)
            today_games = await asyncio.to_thread(get_today_games)
            birthday_narratives = await asyncio.to_thread(build_birthday_narratives, today_games, hot_streaks)
        two_hr_watch = await asyncio.to_thread(build_2hr_watch, today_games, hot_streaks)
        except Exception as exc:
            log.exception("Birthday command failed: %s", exc)
            await message.channel.send("Could not load birthday data right now.")
            return

        await send_birthday_embed(message.channel, birthday_narratives)

    elif content == "!2hr":
        await message.channel.send("Building 2-HR Watch...")
        try:
            hot_streaks = await asyncio.to_thread(build_hot_streaks)
            today_games = await asyncio.to_thread(get_today_games)
            two_hr_watch = await asyncio.to_thread(build_2hr_watch, today_games, hot_streaks)
        except Exception as exc:
            log.exception("2HR command failed: %s", exc)
            await message.channel.send("Could not build 2-HR Watch right now.")
            return

        await send_2hr_watch_embed(message.channel, two_hr_watch)

    elif content == "!ping":
        await message.channel.send("pong")


def main():
    require_env()
    load_state()
    client.run(DISCORD_TOKEN)


if __name__ == "__main__":
    main()
