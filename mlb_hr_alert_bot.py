
import os
import json
import asyncio
import logging
from pathlib import Path
from datetime import datetime, timedelta

import discord
import requests
from zoneinfo import ZoneInfo
from apscheduler.schedulers.asyncio import AsyncIOScheduler

DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")
DISCORD_CHANNEL_ID = int(os.getenv("DISCORD_CHANNEL_ID", "0"))
POLL_SECONDS = int(os.getenv("POLL_SECONDS", "15"))
TIMEZONE = os.getenv("TIMEZONE", "America/Chicago")

# Daily recap settings
DAILY_RECAP_HOUR = int(os.getenv("DAILY_RECAP_HOUR", "8"))
HOT_STREAK_DAYS = int(os.getenv("HOT_STREAK_DAYS", "7"))
HOT_STREAK_TOP_N = int(os.getenv("HOT_STREAK_TOP_N", "8"))

# The Odds API settings
ODDS_API_KEY = os.getenv("ODDS_API_KEY", "")
ODDS_REGION = os.getenv("ODDS_REGION", "us")
ODDS_FORMAT = os.getenv("ODDS_FORMAT", "american")
ODDS_BOOKMAKERS = os.getenv("ODDS_BOOKMAKERS", "")  # comma-separated optional filter
ODDS_TARGET_COUNT = int(os.getenv("ODDS_TARGET_COUNT", "8"))
ODDS_LONGSHOT_MIN = int(os.getenv("ODDS_LONGSHOT_MIN", "450"))

# Tighter near-HR thresholds
NEAR_HR_MIN_EV = float(os.getenv("NEAR_HR_MIN_EV", "102"))
NEAR_HR_MIN_ANGLE = float(os.getenv("NEAR_HR_MIN_ANGLE", "22"))
NEAR_HR_MAX_ANGLE = float(os.getenv("NEAR_HR_MAX_ANGLE", "38"))
NEAR_HR_MIN_DISTANCE = float(os.getenv("NEAR_HR_MIN_DISTANCE", "375"))
NEAR_HR_CONFIRM_DELAY = float(os.getenv("NEAR_HR_CONFIRM_DELAY", "4"))

STATE_FILE = Path("mlb_hr_alert_state.json")
SCHEDULE_URL = "https://statsapi.mlb.com/api/v1/schedule?sportId=1&date={date}"
LIVE_FEED_URL = "https://statsapi.mlb.com/api/v1.1/game/{gamePk}/feed/live"
BOXSCORE_URL = "https://statsapi.mlb.com/api/v1/game/{gamePk}/boxscore"

ODDS_API_BASE = "https://api.the-odds-api.com/v4"
ODDS_SPORT_KEY = "baseball_mlb"

TZ = ZoneInfo(TIMEZONE)
session = requests.Session()
scheduler = AsyncIOScheduler(timezone=TZ)
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


def save_state():
    state["seen_hr_play_ids"] = state["seen_hr_play_ids"][-500:]
    state["seen_near_hr_play_ids"] = state["seen_near_hr_play_ids"][-1000:]
    state["pending_near_hr_play_ids"] = state["pending_near_hr_play_ids"][-1000:]
    STATE_FILE.write_text(json.dumps(state, indent=2))


def get_json(url: str, params: dict | None = None) -> dict:
    resp = session.get(url, params=params, timeout=25)
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
        recap_games.append({
            "label": game_label(game),
            "hitters": hitters,
        })

        for hitter in hitters:
            total_hr += hitter["hr_count"]
            unique_players.add(hitter["name"])

    return {
        "date": target_date,
        "games": recap_games,
        "total_hr": total_hr,
        "unique_players": len(unique_players),
    }


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


def fetch_today_odds_events():
    if not ODDS_API_KEY:
        return []

    params = {
        "apiKey": ODDS_API_KEY,
        "regions": ODDS_REGION,
        "oddsFormat": ODDS_FORMAT,
    }

    if ODDS_BOOKMAKERS.strip():
        params["bookmakers"] = ODDS_BOOKMAKERS.strip()

    url = f"{ODDS_API_BASE}/sports/{ODDS_SPORT_KEY}/odds"
    return get_json(url, params=params)


def fetch_event_hr_prop_odds(event_id: str):
    if not ODDS_API_KEY:
        return {}

    params = {
        "apiKey": ODDS_API_KEY,
        "regions": ODDS_REGION,
        "markets": "batter_home_runs",
        "oddsFormat": ODDS_FORMAT,
    }

    if ODDS_BOOKMAKERS.strip():
        params["bookmakers"] = ODDS_BOOKMAKERS.strip()

    url = f"{ODDS_API_BASE}/sports/{ODDS_SPORT_KEY}/events/{event_id}/odds"
    return get_json(url, params=params)


def normalize_player_name(name: str) -> str:
    return " ".join((name or "").lower().replace(".", "").replace("'", "").split())


def parse_best_hr_prices_from_event(event_data: dict):
    best = {}

    for bookmaker in event_data.get("bookmakers", []):
        book_title = bookmaker.get("title", bookmaker.get("key", "Book"))
        for market in bookmaker.get("markets", []):
            if market.get("key") != "batter_home_runs":
                continue

            for outcome in market.get("outcomes", []):
                if outcome.get("name") != "Over":
                    continue
                if outcome.get("point") != 0.5:
                    continue

                player_name = outcome.get("description")
                price = outcome.get("price")
                if not player_name or price is None:
                    continue

                key = normalize_player_name(player_name)
                current = best.get(key)
                if current is None or price > current["price"]:
                    best[key] = {
                        "player_name": player_name,
                        "price": price,
                        "bookmaker": book_title,
                    }

    return best


def build_today_hr_targets(hot_streaks: list[dict], top_n: int = ODDS_TARGET_COUNT):
    if not ODDS_API_KEY:
        return {"targets": [], "longshots": [], "note": "ODDS_API_KEY not set"}

    events = fetch_today_odds_events()
    merged_prices = {}

    for event in events:
        event_id = event.get("id")
        if not event_id:
            continue

        try:
            event_odds = fetch_event_hr_prop_odds(event_id)
        except Exception as exc:
            log.warning("Could not load event odds for %s: %s", event_id, exc)
            continue

        event_best = parse_best_hr_prices_from_event(event_odds)
        for key, value in event_best.items():
            current = merged_prices.get(key)
            if current is None or value["price"] > current["price"]:
                merged_prices[key] = value

    ranked = []
    for row in hot_streaks:
        key = normalize_player_name(row["name"])
        price_info = merged_prices.get(key)
        if not price_info:
            continue

        score = (
            row["total_hr"] * 10
            + row["streak_days"] * 6
            + max(price_info["price"], 0) / 100.0
        )

        ranked.append({
            "name": row["name"],
            "team_abbr": row["team_abbr"],
            "total_hr": row["total_hr"],
            "streak_days": row["streak_days"],
            "best_price": price_info["price"],
            "best_bookmaker": price_info["bookmaker"],
            "score": score,
        })

    ranked.sort(key=lambda x: (-x["score"], -x["best_price"], x["name"]))

    targets = ranked[:top_n]
    longshots = [r for r in ranked if r["best_price"] >= ODDS_LONGSHOT_MIN][:top_n]

    return {"targets": targets, "longshots": longshots, "note": None}


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


async def post_daily_hr_recap(force: bool = False):
    target_date = yesterday_str()

    if not force and state.get("last_daily_recap_date") == target_date:
        log.info("Daily recap already posted for %s", target_date)
        return

    channel = await client.fetch_channel(DISCORD_CHANNEL_ID)

    try:
        recap = await asyncio.to_thread(build_yesterday_recap, target_date)
        hot_streaks = await asyncio.to_thread(build_hot_streaks)
        today_targets = await asyncio.to_thread(build_today_hr_targets, hot_streaks)
    except Exception as exc:
        log.exception("Failed building daily recap: %s", exc)
        return

    games = recap["games"]
    total_hr = recap["total_hr"]
    unique_players = recap["unique_players"]

    if not games:
        await channel.send(f"💣 **Yesterday's HR Recap — {target_date}**\nNo home runs found.")
        state["last_daily_recap_date"] = target_date
        save_state()
        return

    header = (
        f"💣 **Yesterday's HR Recap — {target_date}**\n"
        f"Total HR: **{total_hr}** | Players: **{unique_players}**\n\n"
    )

    lines = []
    for game in games:
        lines.append(f"**{game['label']}**")
        for hitter in game["hitters"]:
            hr_count = hitter["hr_count"]
            lines.append(f"• {hitter['name']} ({hitter['team_abbr']}) — {hr_count} HR")
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

        embed.set_footer(text=f"Updated daily at {DAILY_RECAP_HOUR}:00 {TIMEZONE}")
        await channel.send(embed=embed)

    if today_targets["note"]:
        await channel.send(f"🎯 Today's HR Targets unavailable: {today_targets['note']}")
    else:
        targets = today_targets["targets"]
        longshots = today_targets["longshots"]

        if targets:
            embed = discord.Embed(
                title="🎯 Today's HR Targets",
                color=discord.Color.blue(),
                description="Ranked from hot streak + best available HR price",
            )

            for row in targets:
                streak_text = f"{row['streak_days']} straight day" if row["streak_days"] == 1 else f"{row['streak_days']} straight days"
                if row["streak_days"] == 0:
                    streak_text = "No current streak"
                embed.add_field(
                    name=f"{row['name']} ({row['team_abbr']})",
                    value=(
                        f"Best HR odds: **{row['best_price']:+}** at {row['best_bookmaker']}\n"
                        f"{row['total_hr']} HR in last {HOT_STREAK_DAYS} days | {streak_text}"
                    ),
                    inline=False,
                )

            top_logo = team_logo(targets[0]["team_abbr"])
            if top_logo:
                embed.set_thumbnail(url=top_logo)

            embed.set_footer(text="The Odds API integration")
            await channel.send(embed=embed)

        if longshots:
            embed = discord.Embed(
                title=f"🧨 Longshot HR Looks (+{ODDS_LONGSHOT_MIN} or longer)",
                color=discord.Color.purple(),
                description="Hot bats with bigger prices",
            )

            for row in longshots:
                embed.add_field(
                    name=f"{row['name']} ({row['team_abbr']})",
                    value=(
                        f"Best HR odds: **{row['best_price']:+}** at {row['best_bookmaker']}\n"
                        f"{row['total_hr']} HR in last {HOT_STREAK_DAYS} days"
                    ),
                    inline=False,
                )

            await channel.send(embed=embed)

    state["last_daily_recap_date"] = target_date
    save_state()
    log.info("Daily recap posted for %s", target_date)


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

    if loop_task is None or loop_task.done():
        loop_task = client.loop.create_task(loop())
        log.info("Started live alert loop")

    if not scheduler.running:
        scheduler.add_job(
            post_daily_hr_recap,
            trigger="cron",
            hour=DAILY_RECAP_HOUR,
            minute=0,
            id="daily_hr_recap",
            replace_existing=True,
            max_instances=1,
            coalesce=True,
        )
        scheduler.start()
        log.info("Scheduled daily HR recap for %s:00 %s", DAILY_RECAP_HOUR, TIMEZONE)


@client.event
async def on_message(message):
    if message.author.bot:
        return

    content = message.content.strip().lower()

    if content == "!yhr":
        await message.channel.send("Building yesterday's HR recap...")
        await post_daily_hr_recap(force=True)

    elif content == "!ping":
        await message.channel.send("pong")


def main():
    require_env()
    load_state()
    client.run(DISCORD_TOKEN)


if __name__ == "__main__":
    main()
