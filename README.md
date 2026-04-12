# MLB Home Run Discord Bot

Posts a Discord alert whenever MLB's live feed records a new home run.

## Railway variables

- `DISCORD_TOKEN`
- `DISCORD_CHANNEL_ID`
- Optional: `TIMEZONE` (default `America/Chicago`)
- Optional: `POLL_SECONDS` (default `15`)

## Files

- `mlb_hr_alert_bot.py` - main bot
- `requirements.txt` - Python dependencies
- `Procfile` - Railway worker command
- `runtime.txt` - pin Python 3.11.9

## Notes

- This bot does not use X.
- It resets its seen-play state each new day in the configured timezone.
- Railway may wipe the local state file on redeploy, which is fine for same-day polling.
