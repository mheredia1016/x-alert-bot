# MLB HR Alert Bot — Cycle Alerts with Game Context

Adds inning/game context to live Cycle Watch alerts.

Cycle alert now includes:
- Current inning
- Outs
- Score
- Hits today
- AB
- RBI
- Missing legs
- FanDuel odds/deep links update after the alert posts

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
