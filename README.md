# MLB HR Alert Bot — Real-Time Cycle Alerts

Cycle Watch now posts immediately from MLB live feed, then edits the same Discord message with FanDuel odds/deep links after SportsGameOdds returns.

Required:
- DISCORD_NEAR_HR_CHANNEL_ID
- ENABLE_CYCLE_WATCH=true
- DISCORD_CYCLE_CHANNEL_ID
- SPORTSGAMEODDS_API_KEY
- SPORTSGAMEODDS_MLB_LEAGUE_ID=MLB
- ENABLE_CYCLE_SGO_ODDS=true

Recommended:
- CYCLE_WATCH_MIN_INNING=5
- CYCLE_WATCH_MIN_LEGS=2
- SPORTSGAMEODDS_FANDUEL_BOOKMAKER_ID=fanduel
