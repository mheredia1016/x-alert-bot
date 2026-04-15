import os
import json
import asyncio
from pathlib import Path
from datetime import datetime

import discord
import requests
from zoneinfo import ZoneInfo

DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")
DISCORD_CHANNEL_ID = int(os.getenv("DISCORD_CHANNEL_ID", "0"))
POLL_SECONDS = int(os.getenv("POLL_SECONDS", "15"))
TIMEZONE = os.getenv("TIMEZONE", "America/Chicago")

# Tighter near-HR thresholds
NEAR_HR_MIN_EV = float(os.getenv("NEAR_HR_MIN_EV", "102"))
NEAR_HR_MIN_ANGLE = float(os.getenv("NEAR_HR_MIN_ANGLE", "22"))
NEAR_HR_MAX_ANGLE = float(os.getenv("NEAR_HR_MAX_ANGLE", "38"))
NEAR_HR_MIN_DISTANCE = float(os.getenv("NEAR_HR_MIN_DISTANCE", "375"))
NEAR_HR_CONFIRM_DELAY = float(os.getenv("NEAR_HR_CONFIRM_DELAY", "4"))

STATE_FILE = Path("mlb_hr_alert_state.json")
SCHEDULE_URL = "https://statsapi.mlb.com/api/v1/schedule?sportId=1&date={date}"
LIVE_FEED_URL = "https://statsapi.mlb.com/api/v1.1/game/{gamePk}/feed/live"

TZ = ZoneInfo(TIMEZONE)
session = requests.Session()

intents = discord.Intents.default()
client = discord.Client(intents=intents)

state = {
    "seen_hr_play_ids": [],
    "seen_near_hr_play_ids": [],
    "pending_near_hr_play_ids": [],
    "last_startup_date": None,
}

TEAM_COLORS = {
    "AZ": 0xA71930,
    "ATL": 0xCE1141,
    "BAL": 0xDF4601,
    "BOS": 0xBD3039,
    "CHC": 0x0E3386,
    "CWS": 0x27251F,
    "CIN": 0xC6011F,
    "CLE": 0xE31937,
    "COL": 0x33006F,
    "DET": 0x0C2340,
    "HOU": 0xEB6E1F,
    "KC": 0x004687,
    "LAA": 0xBA0021,
    "LAD": 0x005A9C,
    "MIA": 0x00A3E0,
    "MIL": 0x12284B,
    "MIN": 0x002B5C,
    "NYM": 0x002D72,
    "NYY": 0x132448,
    "ATH": 0x003831,
    "OAK": 0x003831,
    "PHI": 0xE81828,
    "PIT": 0xFDB827,
    "SD": 0x2F241D,
    "SEA": 0x005C5C,
    "SF": 0xFD5A1E,
    "STL": 0xC41E3A,
    "TB": 0x092C5C,
    "TEX": 0x003278,
    "TOR": 0x134A8E,
    "WSH": 0xAB0003,
}

TEAM_LOGO_SLUGS = {
    "AZ": "ari",
    "ATL": "atl",
    "BAL": "bal",
    "BOS": "bos",
    "CHC": "chc",
    "CWS": "chw",
    "CIN": "cin",
    "CLE": "cle",
    "COL": "col",
    "DET": "det",
    "HOU": "hou",
    "KC": "kc",
    "LAA": "laa",
    "LAD": "lad",
    "MIA": "mia",
    "MIL": "mil",
    "MIN": "min",
    "NYM": "nym",
    "NYY": "nyy",
    "ATH": "oak",
    "OAK": "oak",
    "PHI": "phi",
    "PIT": "pit",
    "SD": "sd",
    "SEA": "sea",
    "SF": "sf",
    "STL": "stl",
    "TB": "tb",
    "TEX": "tex",
    "TOR": "tor",
    "WSH": "wsh",
}


def load_state():
    global state
    if STATE_FILE.exists():
        try:
            state = json.loads(STATE_FILE.read_text())
        except Exception:
            pass

    state.setdefault("seen_hr_play_ids", [])
    state.setdefault("seen_near_hr_play_ids", [])
    state.setdefault("pending_near_hr_play_ids", [])
    state.setdefault("last_startup_date", None)


def save_state():
    state["seen_hr_play_ids"] = state["seen_hr_play_ids"][-500:]
    state["seen_near_hr_play_ids"] = state["seen_near_hr_play_ids"][-1000:]
    state["pending_near_hr_play_ids"] = state["pending_near_hr_play_ids"][-1000:]
    STATE_FILE.write_text(json.dumps(state, indent=2))


def get_json(url):
    r = session.get(url, timeout=20)
    r.raise_for_status()
    return r.json()


def today_str():
    return datetime.now(TZ).strftime("%Y-%m-%d")


def game_label(game):
    away = game["away"]
    home = game["home"]

    away_name = away.get("name") or away.get("teamName") or away.get("abbreviation") or "Away"
    home_name = home.get("name") or home.get("teamName") or home.get("abbreviation") or "Home"

    return f"{away_name} @ {home_name}"


def team_logo(team_abbr):
    slug = TEAM_LOGO_SLUGS.get(team_abbr)
    if not slug:
        return None
    return f"https://a.espncdn.com/i/teamlogos/mlb/500/{slug}.png"


def player_headshot(player_id):
    if not player_id:
        return None
    return f"https://img.mlbstatic.com/mlb-photos/image/upload/w_180/v1/people/{player_id}/headshot/67/current"


def ordinal(n):
    if 10 <= n % 100 <= 20:
        suffix = "th"
    else:
        suffix = {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th")
    return f"{n}{suffix}"


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

    return (
        ev >= NEAR_HR_MIN_EV
        and NEAR_HR_MIN_ANGLE <= la <= NEAR_HR_MAX_ANGLE
        and dist >= NEAR_HR_MIN_DISTANCE
    )


def player_hr_number_in_game(all_plays, current_play):
    matchup = current_play.get("matchup", {}) or {}
    batter = matchup.get("batter", {}) or {}
    batter_id = batter.get("id")

    if not batter_id:
        return None

    current_at_bat = (current_play.get("about", {}) or {}).get("atBatIndex", -1)

    hr_count = 0
    for play in all_plays:
        about = play.get("about", {}) or {}
        at_bat = about.get("atBatIndex", -1)

        if at_bat > current_at_bat:
            continue

        play_matchup = play.get("matchup", {}) or {}
        play_batter = play_matchup.get("batter", {}) or {}

        if play_batter.get("id") != batter_id:
            continue

        if is_home_run(play):
            hr_count += 1

    return hr_count if hr_count > 0 else None


def get_today_games():
    data = get_json(SCHEDULE_URL.format(date=today_str()))
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

    except Exception as e:
        print(f"Error confirming near HR {play_id}: {e}")
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


async def loop():
    await client.wait_until_ready()
    channel = await client.fetch_channel(DISCORD_CHANNEL_ID)
    await send_startup_message()

    while True:
        try:
            games = get_today_games()

            for game in games:
                try:
                    await process_game(channel, game)
                except Exception as e:
                    print(f"Error processing game {game.get('gamePk')}: {e}")
                await asyncio.sleep(1)

        except Exception as e:
            print("ERROR:", e)

        save_state()
        await asyncio.sleep(POLL_SECONDS)


@client.event
async def on_ready():
    print(f"Bot ready as {client.user}")
    client.loop.create_task(loop())


load_state()
client.run(DISCORD_TOKEN)
