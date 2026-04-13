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
    "last_startup_date": None,
}

TEAM_COLORS = {
    "NYY": 0x132448, "BOS": 0xBD3039, "LAD": 0x005A9C, "CHC": 0x0E3386,
    "ATL": 0xCE1141, "HOU": 0xEB6E1F, "PHI": 0xE81828, "NYM": 0x002D72
}


# -----------------------------
# HELPERS
# -----------------------------

def load_state():
    global state
    if STATE_FILE.exists():
        try:
            state = json.loads(STATE_FILE.read_text())
        except:
            pass


def save_state():
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

    away_name = away.get("teamName") or away.get("name") or away.get("abbreviation") or "Away"
    home_name = home.get("teamName") or home.get("name") or home.get("abbreviation") or "Home"

    return f"{away_name} @ {home_name}"


def team_logo(team_id):
    return f"https://statsapi.mlb.com/api/v1/teams/{team_id}/logo"


def player_headshot(player_id):
    return f"https://img.mlbstatic.com/mlb-photos/image/upload/w_180/v1/people/{player_id}/headshot/67/current"


def is_home_run(play):
    text = f"{play.get('result',{})}".lower()
    return any(x in text for x in ["home_run", "home run", "homer", "grand slam"])


def get_metrics(play):
    for e in reversed(play.get("playEvents", [])):
        if e.get("hitData"):
            return e["hitData"]
    return None


def is_near_hr(play):
    if is_home_run(play):
        return False

    m = get_metrics(play)
    if not m:
        return False

    ev = m.get("launchSpeed")
    la = m.get("launchAngle")
    dist = m.get("totalDistance")

    if not all([ev, la, dist]):
        return False

    return ev >= NEAR_HR_MIN_EV and 20 <= la <= 40 and dist >= NEAR_HR_MIN_DISTANCE


# -----------------------------
# ALERT
# -----------------------------

async def send_alert(channel, game, play, alert_type):
    result = play.get("result", {})
    matchup = play.get("matchup", {})

    batter = matchup.get("batter", {})
    pitcher = matchup.get("pitcher", {})

    batter_name = batter.get("fullName", "Unknown")
    pitcher_name = pitcher.get("fullName", "Unknown")

    team = game["away"] if play["about"]["halfInning"] == "top" else game["home"]

    team_id = team.get("id")
    team_abbr = team.get("abbreviation", "MLB")

    color = TEAM_COLORS.get(team_abbr, 0x1D428A)

    title = "💣 Home Run" if alert_type == "hr" else "⚠️ Near Home Run"

    embed = discord.Embed(
        title=title,
        description=result.get("description"),
        color=color
    )

    embed.add_field(name="Game", value=game_label(game), inline=False)
    embed.add_field(name="Batter", value=batter_name)
    embed.add_field(name="Pitcher", value=pitcher_name)

    m = get_metrics(play)
    if m:
        embed.add_field(
            name="Contact",
            value=f"EV {m.get('launchSpeed')} | LA {m.get('launchAngle')} | {m.get('totalDistance')} ft",
            inline=False
        )

    embed.set_thumbnail(url=team_logo(team_id))
    embed.set_image(url=player_headshot(batter.get("id")))

    await channel.send(embed=embed)


# -----------------------------
# PROCESS
# -----------------------------

async def process_game(channel, game):
    data = get_json(LIVE_FEED_URL.format(gamePk=game["gamePk"]))
    plays = data["liveData"]["plays"]["allPlays"]

    for play in plays:
        pid = f"{game['gamePk']}-{play['about']['atBatIndex']}"

        if is_home_run(play):
            if pid not in state["seen_hr_play_ids"]:
                await send_alert(channel, game, play, "hr")
                state["seen_hr_play_ids"].append(pid)
        elif is_near_hr(play):
            if pid not in state["seen_near_hr_play_ids"]:
                await send_alert(channel, game, play, "near")
                state["seen_near_hr_play_ids"].append(pid)


# -----------------------------
# LOOP
# -----------------------------

async def loop():
    await client.wait_until_ready()
    channel = await client.fetch_channel(DISCORD_CHANNEL_ID)

    await channel.send("✅ MLB bot is live")

    while True:
        try:
            data = get_json(SCHEDULE_URL.format(date=today_str()))
            games = data["dates"][0]["games"]

            for g in games:
                game = {
                    "gamePk": g["gamePk"],
                    "away": g["teams"]["away"]["team"],
                    "home": g["teams"]["home"]["team"]
                }

                await process_game(channel, game)
                await asyncio.sleep(1)

        except Exception as e:
            print("ERROR:", e)

        save_state()
        await asyncio.sleep(POLL_SECONDS)


@client.event
async def on_ready():
    print("Bot ready")
    client.loop.create_task(loop())


load_state()
client.run(DISCORD_TOKEN)
