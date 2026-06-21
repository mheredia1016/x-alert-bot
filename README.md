# MLB HR Alert Bot — Near HR Channel + Cycle Watch with SportsGameOdds FanDuel Odds

This is for the LIVE HR alert bot.

Included:
- HR alerts unchanged
- Near HR alerts can post to their own channel
- Cycle Watch live alerts
- Cycle Watch uses SportsGameOdds first for FanDuel odds/deep links
- Falls back to The Odds API / FanDuel search only if SportsGameOdds does not find the prop

Required for Near HR channel:
- DISCORD_NEAR_HR_CHANNEL_ID=<near HR channel id>

Required for Cycle Watch:
- ENABLE_CYCLE_WATCH=true
- DISCORD_CYCLE_CHANNEL_ID=<cycle channel id>
- SPORTSGAMEODDS_API_KEY=<your SportsGameOdds key>
- SPORTSGAMEODDS_MLB_LEAGUE_ID=<MLB league ID from SportsGameOdds>

Recommended:
- ENABLE_CYCLE_SGO_ODDS=true
- SPORTSGAMEODDS_FANDUEL_BOOKMAKER_ID=fanduel
- CYCLE_WATCH_MIN_INNING=5

Optional fallback:
- ODDS_API_KEY=<The Odds API key>
- ENABLE_CYCLE_FANDUEL_ODDS=true

Market keyword vars if SportsGameOdds names differ:
- CYCLE_SGO_MARKET_HR_KEYWORDS=home run,homer,home_runs,batter_home_runs
- CYCLE_SGO_MARKET_TRIPLE_KEYWORDS=triple,triples,batter_triples
- CYCLE_SGO_MARKET_DOUBLE_KEYWORDS=double,doubles,batter_doubles
- CYCLE_SGO_MARKET_HIT_KEYWORDS=hit,hits,batter_hits
