# MLB HR Alert Bot — Cycle Watch 2-of-4 with Cached SportsGameOdds

Live bot update.

Fixes:
- SportsGameOdds limit changed to 100.
- Caches MLB events instead of calling /events for every prop.
- Matches MLB game to SportsGameOdds event once.
- Fetches/caches event odds for that event.
- Cycle Watch remains SportsGameOdds-only.

Required:
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
- CYCLE_SGO_EVENTS_CACHE_SECONDS=300
- CYCLE_SGO_EVENT_ODDS_CACHE_SECONDS=60
- CYCLE_SGO_CACHE_SECONDS=45
