# MLB HR + Near-HR Discord Bot

Railway-ready Discord bot that watches MLB live game feeds and posts:

- 💣 Home Run alerts
- ⚠️ Near Home Run alerts (heuristic based on EV, launch angle, and projected distance)

## Required Railway variables

- `DISCORD_TOKEN`
- `DISCORD_CHANNEL_ID`

## Optional Railway variables

- `POLL_SECONDS=15`
- `TIMEZONE=America/Chicago`
- `NEAR_HR_MIN_EV=100`
- `NEAR_HR_MIN_ANGLE=20`
- `NEAR_HR_MAX_ANGLE=40`
- `NEAR_HR_MIN_DISTANCE=360`

## Files

- `mlb_hr_alert_bot.py`
- `requirements.txt`
- `Procfile`
- `runtime.txt`
- `.gitignore`
