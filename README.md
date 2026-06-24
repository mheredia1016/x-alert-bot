# MLB HR Alert Bot — Cycle Watch 2-of-4 with SportsGameOdds ONLY

This is the live HR alert bot.

Changes:
- Near HR can post to its own channel.
- Cycle Watch alerts at 2 of 4 and 3 of 4 cycle legs.
- Cycle Watch uses SportsGameOdds only for FanDuel odds/deep links.
- The Odds API fallback is disabled/removed for Cycle Watch.

Required:
- DISCORD_TOKEN
- DISCORD_CHANNEL_ID
- DISCORD_NEAR_HR_CHANNEL_ID=<near HR channel id>
- ENABLE_CYCLE_WATCH=true
- DISCORD_CYCLE_CHANNEL_ID=<cycle channel id>
- SPORTSGAMEODDS_API_KEY=<your SportsGameOdds key>
- SPORTSGAMEODDS_MLB_LEAGUE_ID=MLB
- ENABLE_CYCLE_SGO_ODDS=true

Recommended:
- CYCLE_WATCH_MIN_INNING=5
- CYCLE_WATCH_MIN_LEGS=2
- SPORTSGAMEODDS_FANDUEL_BOOKMAKER_ID=fanduel

Do NOT use these for Cycle Watch:
- ODDS_API_KEY
- ENABLE_CYCLE_FANDUEL_ODDS

SportsGameOdds request is now:
GET /v2/events/?leagueID=MLB&oddsAvailable=true&bookmakerID=fanduel&limit=500
