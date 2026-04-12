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

NEAR_HR_MIN_EV = float(os.getenv("NEAR_HR_MIN_EV", "100"))
NEAR_HR_MIN_ANGLE = float(os.getenv("NEAR_HR_MIN_ANGLE", "20"))
NEAR_HR_MAX_ANGLE = float(os.getenv("NEAR_HR_MAX_ANGLE", "40"))
NEAR_HR_MIN_DISTANCE = float(os.getenv("NEAR_HR_MIN_DISTANCE", "360"))

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
    "last_startup_date": None
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

def logo_url_for_abbr(abbr: str) -> str | None:
    slug = TEAM_LOGO_SLUGS.get(abbr)
    if not slug:
        return None
    return f"https://a.espncdn.com/i/teamlogos/mlb/500/{slug}.png"

def load_state():
    global state
    if STATE_FILE.exists():
        try:
            state = json.loads(STATE_FILE.read_text())
        except Exception:
            pass
    state.setdefault("seen_hr_play_ids", [])
    state.setdefault("seen_near_hr_play_ids", [])
    state.setdefault("last_startup_date", None)

def save_state():
    state["seen_hr_play_ids"] = state["seen_hr_play_ids"][-500:]
    state["seen_near_hr_play_ids"] = state["seen_near_hr_play_ids"][-1000:]
    STATE_FILE.write_text(json.dumps(state, indent=2))

def get_json(url: str, timeout: int = 20):
    r = session.get(url, timeout=timeout)
    r.raise_for_status()
    return r.json()

def today_str() -> str:
    return datetime.now(TZ).strftime("%Y-%m-%d")

def get_today_games():
    data = get_json(SCHEDULE_URL.format(date=today_str()))
    games = []
    for date_block in data.get("dates", []):
        for game in date_block.get("games", []):
            games.append({
                "gamePk": game.get("gamePk"),
                "gameDate": game.get("gameDate"),
                "status": (game.get("status") or {}).get("detailedState"),
                "away": ((game.get("teams") or {}).get("away") or {}).get("team", {}),
                "home": ((game.get("teams") or {}).get("home") or {}).get("team", {}),
            })
    return games

def game_label(game):
    away_abbr = game["away"].get("abbreviation", "AWAY")
    home_abbr = game["home"].get("abbreviation", "HOME")
    return f"{away_abbr} @ {home_abbr}"

def likely_near_hr(play):
    result = ((play.get("result") or {}).get("eventType") or "").lower()
    if result == "home_run":
        return False

    play_events = play.get("playEvents") or []
    hit_data = None
    for event in reversed(play_events):
        hd = event.get("hitData")
        if hd:
            hit_data = hd
            break

    if not hit_data:
        return False

    launch_speed = hit_data.get("launchSpeed")
    launch_angle = hit_data.get("launchAngle")
    total_distance = hit_data.get("totalDistance")

    if launch_speed is None or launch_angle is None or total_distance is None:
        return False

    return (
        launch_speed >= NEAR_HR_MIN_EV and
        NEAR_HR_MIN_ANGLE <= launch_angle <= NEAR_HR_MAX_ANGLE and
        total_distance >= NEAR_HR_MIN_DISTANCE
    )

def play_metrics(play):
    play_events = play.get("playEvents") or []
    hit_data = None
    for event in reversed(play_events):
        hd = event.get("hitData")
        if hd:
            hit_data = hd
            break
    if not hit_data:
        return None
    return {
        "ev": hit_data.get("launchSpeed"),
        "la": hit_data.get("launchAngle"),
        "dist": hit_data.get("totalDistance"),
        "traj": hit_data.get("trajectory"),
        "hardness": hit_data.get("hardness"),
        "location": hit_data.get("location")
    }

def build_play_id(game_pk, play):
    about = play.get("about") or {}
    return f'{game_pk}:{about.get("atBatIndex")}:{about.get("halfInning")}:{about.get("inning")}'

def pick_batting_team(game, play):
    half = ((play.get("about") or {}).get("halfInning") or "").lower()
    return game["away"] if half == "top" else game["home"]

def score_line(play):
    result = play.get("result") or {}
    home = result.get("homeScore")
    away = result.get("awayScore")
    if home is None or away is None:
        return None
    return f"{away}-{home}"

async def send_startup_message():
    channel = await client.fetch_channel(DISCORD_CHANNEL_ID)
    msg_date = today_str()
    if state.get("last_startup_date") == msg_date:
        return
    await channel.send("✅ MLB HR/Near-HR bot is online.")
    state["last_startup_date"] = msg_date
    save_state()

async def send_alert(channel, game, play, alert_type: str):
    result = play.get("result") or {}
    matchup = play.get("matchup") or {}
    batter = (matchup.get("batter") or {}).get("fullName", "Unknown batter")
    pitcher = (matchup.get("pitcher") or {}).get("fullName", "Unknown pitcher")
    event = result.get("event", "Play")
    description = result.get("description") or event
    about = play.get("about") or {}
    inning = about.get("inning")
    half = (about.get("halfInning") or "").title()
    game_pk = game["gamePk"]

    batting_team = pick_batting_team(game, play)
    team_abbr = batting_team.get("abbreviation", "")
    team_name = batting_team.get("name", "Team")
    logo = logo_url_for_abbr(team_abbr)

    metrics = play_metrics(play)
    score = score_line(play)

    if alert_type == "hr":
        title = "💣 Home Run"
        color = discord.Color.red()
    else:
        title = "⚠️ Near Home Run"
        color = discord.Color.orange()

    embed = discord.Embed(
        title=title,
        description=description[:4096],
        color=color,
        url=f"https://www.mlb.com/gameday/{game_pk}",
    )
    embed.add_field(name="Batter", value=batter, inline=True)
    embed.add_field(name="Pitcher", value=pitcher, inline=True)
    embed.add_field(name="Game", value=game_label(game), inline=True)

    embed.add_field(name="Team", value=f"{team_name} ({team_abbr})", inline=True)
    embed.add_field(name="Inning", value=f"{half} {inning}", inline=True)
    embed.add_field(name="Score", value=score or "—", inline=True)

    if metrics:
        metric_parts = []
        if metrics.get("ev") is not None:
            metric_parts.append(f'EV: {metrics["ev"]:.1f} mph')
        if metrics.get("la") is not None:
            metric_parts.append(f'LA: {metrics["la"]:.1f}°')
        if metrics.get("dist") is not None:
            metric_parts.append(f'Dist: {metrics["dist"]:.0f} ft')
        if metric_parts:
            embed.add_field(name="Contact", value=" | ".join(metric_parts), inline=False)

    embed.set_footer(text="MLB live feed")

    if logo:
        embed.set_thumbnail(url=logo)

    await channel.send(embed=embed)

async def process_game(channel, game):
    game_pk = game["gamePk"]
    feed = await asyncio.to_thread(get_json, LIVE_FEED_URL.format(gamePk=game_pk))
    all_plays = (((feed.get("liveData") or {}).get("plays") or {}).get("allPlays") or [])

    new_hr_ids = []
    new_near_ids = []

    for play in all_plays:
        result = play.get("result") or {}
        event_type = (result.get("eventType") or "").lower()
        play_id = build_play_id(game_pk, play)

        if event_type == "home_run":
            if play_id not in state["seen_hr_play_ids"]:
                new_hr_ids.append((play_id, play))
        elif likely_near_hr(play):
            if play_id not in state["seen_near_hr_play_ids"]:
                new_near_ids.append((play_id, play))

    for play_id, play in new_hr_ids:
        await send_alert(channel, game, play, "hr")
        state["seen_hr_play_ids"].append(play_id)
        await asyncio.sleep(0.5)

    for play_id, play in new_near_ids:
        await send_alert(channel, game, play, "near_hr")
        state["seen_near_hr_play_ids"].append(play_id)
        await asyncio.sleep(0.5)

    if new_hr_ids or new_near_ids:
        save_state()

async def poll_loop():
    await client.wait_until_ready()
    channel = await client.fetch_channel(DISCORD_CHANNEL_ID)
    await send_startup_message()

    while not client.is_closed():
        try:
            games = await asyncio.to_thread(get_today_games)
            active_games = []
            for game in games:
                status = (game.get("status") or "").lower()
                if any(x in status for x in [
                    "in progress", "manager challenge", "review",
                    "delayed", "warmup", "pre-game", "scheduled"
                ]):
                    active_games.append(game)

            for game in active_games:
                try:
                    await process_game(channel, game)
                except Exception as e:
                    print(f"Error processing game {game.get('gamePk')}: {e}")
                await asyncio.sleep(0.25)

        except Exception as e:
            print(f"Polling loop error: {e}")

        await asyncio.sleep(POLL_SECONDS)

@client.event
async def on_ready():
    print(f"Logged in as {client.user} ({client.user.id})")
    client.loop.create_task(poll_loop())

def main():
    if not DISCORD_TOKEN:
        raise ValueError("Missing DISCORD_TOKEN")
    if DISCORD_CHANNEL_ID == 0:
        raise ValueError("Missing or invalid DISCORD_CHANNEL_ID")
    load_state()
    client.run(DISCORD_TOKEN)

if __name__ == "__main__":
    main()
