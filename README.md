# MLB HR Alert Bot — Near HR Separate Channel + Cycle Watch

This is the live HR alert bot.

Near HR separate channel:
- Add DISCORD_NEAR_HR_CHANNEL_ID=<near HR channel id>
- Home run alerts stay in DISCORD_CHANNEL_ID
- Near home run alerts go to DISCORD_NEAR_HR_CHANNEL_ID

Cycle Watch is also included, but you can leave it disabled until ready.

Cycle required variables when ready:
- ENABLE_CYCLE_WATCH=true
- DISCORD_CYCLE_CHANNEL_ID=<cycle channel id>
- ODDS_API_KEY=<your The Odds API key>

Recommended cycle variables:
- ENABLE_CYCLE_FANDUEL_ODDS=true
- CYCLE_WATCH_MIN_INNING=5
- CYCLE_FANDUEL_BOOKMAKER_KEY=fanduel
- ODDS_REGION=us
- ODDS_FORMAT=american
