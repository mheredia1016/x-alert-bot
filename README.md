# MLB HR Alert Bot — Cycle Watch 2-of-4 with SportsGameOdds

Live bot update.

Cycle alerts now trigger when a player has:
- 2 of 4 cycle legs
- 3 of 4 cycle legs

Each alert lists every remaining prop:
- Single -> To Record A Hit
- Double -> To Record A Double
- Triple -> To Record A Triple
- Home Run -> To Hit A Home Run

SportsGameOdds is used first for FanDuel odds/deep links.

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
