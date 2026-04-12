import os
import json
import asyncio
from pathlib import Path

import discord
import snscrape.modules.twitter as sntwitter

DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")
DISCORD_CHANNEL_ID = int(os.getenv("DISCORD_CHANNEL_ID", "0"))

TRACKED_ACCOUNTS = ["MLBHR", "MLBNearHR"]
STATE_FILE = Path("x_alert_state.json")

intents = discord.Intents.default()
client = discord.Client(intents=intents)

state = {"last_ids": {}}


def load_state():
    global state
    if STATE_FILE.exists():
        try:
            state = json.loads(STATE_FILE.read_text())
        except Exception:
            state = {"last_ids": {}}


def save_state():
    STATE_FILE.write_text(json.dumps(state, indent=2))


def get_recent_original_tweets(username, limit=5):
    tweets = []
    scraper = sntwitter.TwitterSearchScraper(f"from:{username}")

    for tweet in scraper.get_items():
        if tweet.inReplyToTweetId is not None:
            continue
        if getattr(tweet, "retweetedTweet", None) is not None:
            continue

        tweets.append(tweet)
        if len(tweets) >= limit:
            break

    return tweets


async def send_tweet(channel, username, tweet):
    url = f"https://x.com/{username}/status/{tweet.id}"
    text = (tweet.rawContent or "").strip()

    if username.lower() == "mlbhr":
        title = "💣 Home Run Alert"
    elif username.lower() == "mlbnearhr":
        title = "⚠️ Near Home Run"
    else:
        title = f"New post from @{username}"

    embed = discord.Embed(
        title=title,
        description=text[:4000] if text else url,
        url=url
    )

    if tweet.date:
        embed.timestamp = tweet.date

    embed.set_footer(text="X Alert Monitor")

    await channel.send(content=url, embed=embed)


async def check_account(channel, username):
    tweets = await asyncio.to_thread(get_recent_original_tweets, username)

    if not tweets:
        return

    latest_id = str(tweets[0].id)
    last_seen = state["last_ids"].get(username)

    if last_seen is None:
        state["last_ids"][username] = latest_id
        save_state()
        print(f"Initialized {username}")
        return

    unseen = []
    for tweet in tweets:
        if str(tweet.id) == last_seen:
            break
        unseen.append(tweet)

    if not unseen:
        return

    unseen.reverse()

    for tweet in unseen:
        await send_tweet(channel, username, tweet)
        await asyncio.sleep(1)

    state["last_ids"][username] = latest_id
    save_state()
    print(f"Posted {len(unseen)} tweets for {username}")


async def poll():
    await client.wait_until_ready()
    channel = client.get_channel(DISCORD_CHANNEL_ID)

    if not channel:
        print("Channel not found")
        return

    print("Bot running...")

    while True:
        for username in TRACKED_ACCOUNTS:
            try:
                await check_account(channel, username)
            except Exception as e:
                print(f"Error with {username}: {e}")

            await asyncio.sleep(3)

        await asyncio.sleep(20)


@client.event
async def on_ready():
    print(f"Logged in as {client.user}")
    client.loop.create_task(poll())


load_state()
client.run(DISCORD_TOKEN)
