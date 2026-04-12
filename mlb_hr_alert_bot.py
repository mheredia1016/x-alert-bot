import os
import json
import asyncio
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import aiohttp
import discord

DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")
DISCORD_CHANNEL_ID = int(os.getenv("DISCORD_CHANNEL_ID", "0"))
TIMEZONE = os.getenv("TIMEZONE", "America/Chicago")
POLL_SECONDS = int(os.getenv("POLL_SECONDS", "15"))
STATE_FILE = Path("mlb_hr_state.json")
SCHEDULE_URL = "https://statsapi.mlb.com/api/v1/schedule"
LIVE_FEED_URL = "https://statsapi.mlb.com/api/v1.1/game/{game_pk}/feed/live"

intents = discord.Intents.default()
client = discord.Client(intents=intents)
state = {"alerted_play_ids": {}, "last_date": None}


def load_state() -> None:
    global state
    if STATE_FILE.exists():
        try:
            state = json.loads(STATE_FILE.read_text())
        except Exception:
            state = {"alerted_play_ids": {}, "last_date": None}


def save_state() -> None:
    STATE_FILE.write_text(json.dumps(state, indent=2))


def today_str() -> str:
    return datetime.now(ZoneInfo(TIMEZONE)).strftime("%Y-%m-%d")


def ensure_today_state() -> None:
    current = today_str()
    if state.get("last_date") != current:
        state["last_date"] = current
        state["alerted_play_ids"] = {}
        save_state()
        print(f"Rolled state to {current}")


async def fetch_json(session: aiohttp.ClientSession, url: str, params: dict | None = None) -> dict:
    async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=20)) as response:
        response.raise_for_status()
        return await response.json()


async def get_todays_games(session: aiohttp.ClientSession) -> list[dict]:
    params = {"sportId": 1, "date": today_str()}
    data = await fetch_json(session, SCHEDULE_URL, params=params)
    dates = data.get("dates", [])
    if not dates:
        return []
    return dates[0].get("games", [])


async def get_live_feed(session: aiohttp.ClientSession, game_pk: int) -> dict:
    return await fetch_json(session, LIVE_FEED_URL.format(game_pk=game_pk))


def build_play_key(game_pk: int, play: dict) -> str:
    about = play.get("about", {})
    at_bat_index = about.get("atBatIndex", "na")
    event_index = about.get("eventIndex", "na")
    return f"{game_pk}:{at_bat_index}:{event_index}"


def extract_new_home_runs(game_pk: int, feed: dict) -> list[dict]:
    game_data = feed.get("gameData", {})
    live_data = feed.get("liveData", {})
    all_plays = live_data.get("plays", {}).get("allPlays", [])
    home_team = game_data.get("teams", {}).get("home", {}).get("name", "Home")
    away_team = game_data.get("teams", {}).get("away", {}).get("name", "Away")

    new_alerts = []
    seen_for_game = state["alerted_play_ids"].setdefault(str(game_pk), [])
    seen_set = set(seen_for_game)

    for play in all_plays:
        result = play.get("result", {})
        event_type = (result.get("eventType") or "").lower()
        event = (result.get("event") or "").lower()
        if event_type != "home_run" and event != "home run":
            continue

        play_key = build_play_key(game_pk, play)
        if play_key in seen_set:
            continue

        about = play.get("about", {})
        batter = play.get("matchup", {}).get("batter", {}).get("fullName", "Unknown batter")
        pitcher = play.get("matchup", {}).get("pitcher", {}).get("fullName", "Unknown pitcher")
        description = result.get("description") or "Home run"
        inning = about.get("inning", "?")
        half = about.get("halfInning", "")
        scoring_team = result.get("team", {}).get("name") or play.get("team", {}).get("name")

        new_alerts.append({
            "play_key": play_key,
            "batter": batter,
            "pitcher": pitcher,
            "description": description,
            "inning": inning,
            "half": half,
            "home_team": home_team,
            "away_team": away_team,
            "scoring_team": scoring_team,
        })

    return new_alerts


async def send_home_run_alert(channel: discord.abc.Messageable, game_pk: int, alert: dict) -> None:
    title = "💣 MLB Home Run Alert"
    game_label = f"{alert['away_team']} at {alert['home_team']}"
    inning_text = f"{str(alert['half']).title()} {alert['inning']}"
    description = alert["description"]
    scoring_team = alert.get("scoring_team") or ""

    embed = discord.Embed(
        title=title,
        description=description[:4000],
    )
    embed.add_field(name="Batter", value=alert["batter"], inline=True)
    embed.add_field(name="Pitcher", value=alert["pitcher"], inline=True)
    embed.add_field(name="Inning", value=inning_text, inline=True)
    embed.add_field(name="Game", value=game_label, inline=False)
    if scoring_team:
        embed.add_field(name="Team", value=scoring_team, inline=True)
    embed.set_footer(text=f"GamePk {game_pk}")

    await channel.send(embed=embed)


async def poll_home_runs() -> None:
    await client.wait_until_ready()
    channel = await client.fetch_channel(DISCORD_CHANNEL_ID)
    if channel is None:
        print("Could not fetch Discord channel. Check DISCORD_CHANNEL_ID.")
        return

    await channel.send("✅ MLB home run bot is online.")
    print("MLB HR polling started.")

    async with aiohttp.ClientSession(headers={"User-Agent": "Mozilla/5.0"}) as session:
        while not client.is_closed():
            try:
                ensure_today_state()
                games = await get_todays_games(session)
                print(f"Found {len(games)} scheduled game(s) for {today_str()}")

                for game in games:
                    game_pk = game.get("gamePk")
                    detailed_state = game.get("status", {}).get("detailedState", "")
                    if not game_pk:
                        continue
                    if detailed_state in {"Final", "Completed Early", "Postponed", "Cancelled"}:
                        continue

                    try:
                        feed = await get_live_feed(session, int(game_pk))
                        alerts = extract_new_home_runs(int(game_pk), feed)
                        if alerts:
                            for alert in alerts:
                                await send_home_run_alert(channel, int(game_pk), alert)
                                state["alerted_play_ids"].setdefault(str(game_pk), []).append(alert["play_key"])
                                save_state()
                                print(f"Posted HR alert for {alert['batter']} in game {game_pk}")
                                await asyncio.sleep(1)
                    except Exception as game_error:
                        print(f"Game {game_pk} error: {game_error}")

            except Exception as poll_error:
                print(f"Polling error: {poll_error}")

            await asyncio.sleep(POLL_SECONDS)


@client.event
async def on_ready():
    print(f"Logged in as {client.user} ({client.user.id})")
    client.loop.create_task(poll_home_runs())


def main() -> None:
    if not DISCORD_TOKEN:
        raise ValueError("Missing DISCORD_TOKEN environment variable")
    if DISCORD_CHANNEL_ID == 0:
        raise ValueError("Missing or invalid DISCORD_CHANNEL_ID environment variable")

    load_state()
    ensure_today_state()
    client.run(DISCORD_TOKEN)


if __name__ == "__main__":
    main()
