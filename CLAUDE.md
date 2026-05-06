# JUMPBOT NBA PROPS — Live Player-Prop Market Maker

## What this is
NO-side penny-jumping bot for Kalshi NBA player-prop markets (points, rebounds,
assists). 60-100+ markets simultaneously during live games. Blends FanDuel +
DraftKings implied probabilities for fair values, uses arb-% ceiling strategy
(not min-EV tiers), and reacts to NBA play-by-play for phase-aware trading.

## Architecture
- **`app.py`** — Flask backend (~6000 lines). Multi-game state, trading engine,
  WS orderbook, OB polling, NBA feed integration, inline HTML UI, SQLite
  fills DB, live + historical PnL dashboards.
- **`nba_feed.py`** — ESPN scoreboard + NBA CDN play-by-play + boxscore.
  Provides: lifecycle (Scheduled/Live/Final), phase detection (LIVE,
  TIMEOUT, QUARTER_BREAK, HALFTIME, FOUL_SHOT, OVERTIME), live player stats,
  score-change callbacks, on-court tracking, and the player directory
  (name→personId resolution).
- **`odds_feed.py`** — OddsBlaze REST polling for FD + DK HR-equivalent
  (player-prop) odds. Devig + blended implied.
- **`boltodds_feed.py`** — BoltOdds WebSocket for real-time lineup / line
  changes (past-posting protection).
- **`op_clocks.py`** — OddsPapi clock state (game clock stopped/running) for
  dead-ball confirmation. Secondary signal to NBA PBP.
- **`kalshi.py`** — Shared RSA-PSS auth (same as all jumpbots).
- Deployed on Railway: `Millertime07/Jumpbot-NBA-Prop`, WEB_CONCURRENCY=1.

## Trading Logic (NO-side only)

**Ceiling mode = `arb` (default), book mode = `min`.** Different from Homer —
we want guaranteed arb-%, not probabilistic min-EV.

```
arb mode:  ceil = min(fd_no, dk_no) - arb_pct + offset
book_mode=min uses the lower-priced book, most conservative
```

The min-EV tier system exists in code but is **not the active strategy**.
Stick to arb% + min book.

- **Penny-jump**: place at `best_bid + 1` capped at ceil
- **Self-exclusion**: `compute_best_bid(ob, exclude_price=our_price)` —
  never jump yourself. Note: the current impl excludes the entire level
  if there's a qty match; there's a pending improvement to subtract only
  our own size so we properly see others' quantity at our level.
- **Stepdown**: delay-protected, post-stepdown jump cooldown
- **Suspension pull**: when FD or DK drops odds for a player, cancel all
  open orders for that `player|line` key immediately (`SUSPENDED_PULL`)
- **Bond detection**: YES bid ≥ 99 → two-hit confirmation → `prop_done=True`,
  market killed

## Games Dict Structure (`games[game_id]`)
```
{
  "game_name": str, "nba_game_id": int/str, "kalshi_event": str,
  "prop_type": str ("points" | "rebounds" | "assists"),
  "paused": bool, "smart_mode": bool, "smart_status": str,
  "settings": {
    "max_exposure": int,
    "points":    {"size", "fill_target", "arb_pct", "offset", "player_max"},
    "rebounds":  {...same keys...},
    "assists":   {...same keys...},
  },
  "player_maxes": {player_name: total_contracts},
  "markets": {ticker: mkt_dict},
  "paused_tickers": set,
  "op_fixture_id": str (optional),
}
```

**`settings` is per-prop nested.** `player_max` is per-prop, not global.
Getting this wrong (e.g., calling `compute_ceiling` without `prop_type`)
causes REB/AST markets to silently use points odds — shipped bug 4/17.

## NBA Feed Phases (`nba_feed.get_game_phase`)
- `PRE`, `LIVE`, `OVERTIME` → active trading
- `TIMEOUT`, `QUARTER_BREAK`, `HALFTIME` → dead ball, safer fills
- `FOUL_SHOT` → free throws incoming, high risk of stat change
- `FINAL` → game over, stop trading
- `UNKNOWN` → feed lag, treat as PRE

PBP refresh 2s, ESPN fallback 5-15s. Live phase-aware settings were built and
then reverted 2026-04-20 — the scaffolding (`_get_phase_config`,
`phase_settings` dict, auto-match) is a known next feature. Current code uses
scattered flags: `deadball_only`, `inplay_enabled`, `inplay_size_pct`,
`inplay_ceiling_adj`, `post_score_window_secs`.

## Past-Posting Protection (5-gate)
Player-prop markets get past-posted when a stat event (basket, rebound,
assist) resolves the market before our stale order is cancelled. Gates:
1. BoltOdds WS line change detection (`line_version` bumps)
2. FD/DK suspension detection in OddsBlaze poll
3. NBA PBP score-change callback (`_on_score_change`)
4. NBA boxscore `get_player_stat` within N of line → freeze
5. Escalating freeze: longer pauses after each suspected past-post

This does NOT catch a book that sits stale (doesn't drop odds) while the
stat moves. See Schroder AST 2026-04-20 — FD held stale 3.5 line while he
dished his 4th assist, we got lifted at 60c on a NO that was actually worth
~20c. No pure technical fix — market-maker's curse.

## Fill Log + CLV (SQLite)
Local DB at `FILLS_DB` (default `/data/nba_fills.db` on Railway).

Schema (see `app.py:1368-1376` for CREATE TABLE):
```
fills (id, ts, ticker, player, line, contracts, fill_price, ceil_no,
       fair_no_at_fill, ev_at_fill, ceil_ev, has_under,
       fair_no_10s, clv_10s, move_10s,
       fair_no_60s, clv_60s, move_60s,
       devig_method, devig_exponent, prop_type, game_id)
```

CLV 10s / 60s backfilled by a background thread 10s and 60s after each fill
using the same `get_blended_implied` fair NO. `move_Xs` = fair_no_Xs -
fair_no_at_fill (market direction, not edge).

## Fills Flow — NBA → nba_dashboard (split out 2026-04-21)
Fills POST to `NBA_DASHBOARD_URL` via `_send_fill`, landing in `nba_fills`
Postgres table (shared Railway Postgres plugin with `homer_fills`, different
table). If `NBA_DASHBOARD_URL` is unset, falls back to legacy `FILLS_URL`
(homer_fills). Enriched payload includes per-book fd/dk over odds,
per-book fair_no, blended_fair_no, devig exponent used, game_phase.

**CRITICAL env-var lesson (2026-04-22):** `NBA_DASHBOARD_URL` MUST include
`https://`. When it was set to a bare domain, every POST silently failed
with `Invalid URL 'domain/api/fill': No scheme supplied`. One full session
of fills (302 rows) was lost — had to backfill from local SQLite.
**Defense in place:** `_normalize_url()` auto-prepends `https://` and
trims trailing slash, so bare domains now work. Still, always set env
vars correctly.

### Backfill endpoint (2026-04-22)
`POST /api/admin/backfill_fills` with `X-API-Key`. Reads local SQLite
where `fills_id IS NULL`, POSTs each to dashboard, writes remote id back.
Idempotent. Supports `?dry_run=true` and `?limit=N`. Stitches ticker
date for legacy rows with time-only `ts`.

## Live P&L Dashboard (added 2026-04-20)
- `GET /api/live-pnl/snapshot?ev_mode=devig|book` — one-call payload:
  `summary` + `games[].players[].lines[]` with live fair_no, EV%, unrealized,
  live_stat from nba_feed, `row_state` (approaching if stat within 3, hit if
  stat ≥ line), player_max bar %.
- `GET /api/live-pnl/performance` — today / 7d / all-time CLV + EV stats
  from the fills DB. Volume-weighted by contract notional
  (`SUM(metric * contracts * fill_price) / SUM(contracts * fill_price)`)
  so large fills dominate the average. The summary-bar CLV inside
  `/api/live-pnl/snapshot` uses the same weighting.
- `GET /live-pnl` — inline HTML page, polls `/api/live-pnl/snapshot` every 2s.

Pattern: snapshot games dict under `games_lock`, compute response outside.
**No new computation in the endpoint** — reuses `get_blended_implied`,
`_find_detail`, `nba_feed.get_player_stat`, `nba_feed.get_game_info`.

## Batch Orders
- **`kalshi.batch_cancel(order_ids)`** used during dead→live resize at
  `app.py:2162` — cancels N oversized orders in one call when entering
  LIVE phase with reduced size_pct.
- **`batch_create` is NOT yet used.** Every `place_order` is a separate
  Kalshi REST call. At 80+ markets + penny-jump churn, this is the main
  heartbeat bottleneck (0.2s target → observed 0.3-1.0s). Pending.

## Known Issues (open as of 2026-04-21)
- **WS fills dropping**: `FILL_POLL` warnings recur every session. Poller
  catches them (no lost fills), and fill price is recovered via
  `_last_known_price` so SQLite accuracy holds. Still worth a focused
  look at the WS subscription handler to reduce the volume of fallbacks.
- **Heartbeat slow at >80 markets**: sequential Kalshi REST in loop +
  no batch_create. 0.2-1.0s per heartbeat means jumps land 0.3-0.8s
  behind the bid move. Batch create would cut this by ~60%.

## Resolved (keeping context for future you)
- **Fill price poller fallback** (was `avg_no` blend): fixed. Fallback
  chain in `app.py:2093` is `order_price → _last_known_price → avg_no`.
  `_last_known_price` is written on every successful place at
  `app.py:1286` and `app.py:1343`.
- **409 Conflict retry loops**: fixed. `_place_fails` counter at
  `app.py:1289` increments on each failed place; at 5 fails the market
  transitions to `PLACE_GIVE_UP` with a 60s backoff (`app.py:1295-1301`).
  No more infinite retry on state-desynced tickers.
- **Ceiling stuck above fair_no — Clingan -400 / Maxey stale EV
  (2026-04-22)**: fixed. `_ceil_cache_key` at `app.py:~1159` now
  includes `fair_yes` from both books. Before, only `(arb, offset,
  fd_odds, dk_odds)` keyed the cache — so when `has_under` arrived late
  and flipped devig from power-one-way to two-way-norm, fair_yes
  recomputed but the cache didn't bust. `compute_ceiling` never re-ran,
  its safety clamp (`ceil >= fair_no → floor(fair_no) - 1`) never fired,
  and ceiling stayed at an old higher value.
- **Frozen orderbooks (2026-04-21/22)**: `_ob_staleness_poller` tightened:
  10s cycle, 10s threshold, 15 refetches per cycle, active markets only,
  oldest-first. Per-refetch log: `OB refetch {tk}: {age:.1f}s stale` so
  recurring offenders surface. Initial boot OB fetch rate-limited to
  15/s with 3-worker pool (was 10 parallel → 429 storms).
- **NBA CDN 403s (2026-04-22)**: headers beefed up in `nba_feed.py`.
  Minimal UA+Referer started getting rejected; now sends full Chrome-
  like header set (Accept, Accept-Language, Origin, Sec-Fetch-*,
  keep-alive). Cleared 403s immediately.

## Player Directory + On-Court Tracking (2026-04-21)
`nba_feed.py` maintains a per-game name→personId index built from every
boxscore fetch. Storage key for `on_court_state[gid]` is **personId**
(not family name) so same-family pairs (e.g., OKC's Jalen + Jaylin
Williams) stay distinct. Highlights:
- `resolve_player_in_game(gid, name) → pid | None` — normalizes
  (ASCII-fold, strip accents/suffixes/periods, collapse "O.G." → "og"),
  returns None for unknown *or* ambiguous names. Never guesses.
- Per-game index only contains keys that are unambiguous across the 2
  teams — ambiguous keys (e.g., "williams" on an OKC game) are omitted,
  forcing match by fullname.
- PBP substitution events resolve directly by `personId` from the event
  payload; no name-matching needed.
- Pregame boxscore pre-fetch (within 6h of tipoff) populates the
  directory before tip so name resolution is ready at game start.
- Manual alias overrides: `_manual_aliases = {}` (empty). Add normalized
  entries here when `player_dir miss:` log lines surface a feed name
  that doesn't match NBA CDN.
- Market rows cache `mkt["_pid"]` on first resolve so each tick is O(1).
- UI: small ON/OFF/— pill next to each player in both the main strike
  grid and the `/live-pnl` player summary row. Same color scheme
  (green/amber/grey). See `ocDotAttrs` in `INDEX_HTML`.
- Trading-logic integration (phase b) is **not wired yet** — the
  indicator is observe-only. Planned: `off_court_enabled` master toggle
  + `off_court_settings` mirroring phase-config shape (size_pct,
  arb_pct_delta), applied in `process_market` when player is confirmed
  on bench (`on_court is False`, not falsy).

## Per-Prop-Type Control
Each game card has PTS / REB / AST start/stop buttons (added 2026-04-17).
Use these to turn one prop type on without another — e.g., points-only.
They are per-game, not global.

## Settings (global defaults at `app.py:~60`)
```python
{
  "cooldown_secs": 3, "heartbeat_interval": 1, "stepdown_delay_secs": 15,
  "post_stepdown_jump_cooldown_secs": 10,
  "dk_weight": 50,
  "devig_exp_heavy_fav": 1.08,   # raw implied >= 0.80
  "devig_exp_mid_high": 1.10,    # 0.55-0.79
  "devig_exp_mid_low": 1.11,     # 0.40-0.54
  "devig_exp_longshot": 1.14,    # < 0.40
  "default_min_ev": 5,            # not active unless ceil_mode="tiers"
  "min_ev_tiers": {...},          # not active
  "inplay_enabled": True, "inplay_size_pct": 50, "inplay_ceiling_adj": -2,
  "inplay_refill_delay_secs": 5, "post_score_window_secs": 12,
  "deadball_only": False, "auto_stop_q4": False, "auto_start_live": False,
  "ceil_mode": "arb",             # ACTIVE — don't switch to "tiers"
  "book_mode": "min",             # ACTIVE — lower of fd_no / dk_no
}
```

## Env Vars
- `KALSHI_API_KEY`, `KALSHI_PEM` — Kalshi auth
- `OB_API_KEY` — OddsBlaze API key. **Shared with jumpbot_homer** — the same
  key counts against a single rate limit (OddsBlaze default 250/min on
  current tier). NBA at 2 books × 0.5s = 240/min + Homer at 7 books × 3s =
  140/min = 380/min total. Monitor `OddsBlaze error: 429` in logs. If you
  see throttling, dial NBA `ob_poll_interval` up or use a second key.
- `NBA_DASHBOARD_URL` — nba_dashboard service URL (MUST include `https://`
  or code auto-prepends). Fills route here when set.
- `NBA_DASHBOARD_API_KEY` — matches nba_dashboard's API key. Falls back to
  `FILLS_API_KEY` if unset.
- `FILLS_URL` — homer_fills legacy service URL (fallback if
  `NBA_DASHBOARD_URL` unset)
- `FILLS_API_KEY` — auth for fills tracker (default: homer123)
- `FILLS_DB` — local SQLite path (default `/data/nba_fills.db`)

## Deploy Notes
- **NBA redeploys during live games cost 30-60s of trading**. Orders survive
  on Kalshi (tracking resyncs via resting-order query), but fills during the
  restart window log via poller not WS, so price accuracy is worse. Avoid
  mid-game deploys if possible.
- **`WEB_CONCURRENCY=1`** is mandatory. The whole thing is single-process
  with threads.

## Naming Warning
`odds_feed.ob_poll_interval` here is **OddsBlaze odds polling** (same
convention as jumpbot_homer). If you see `OB_POLL_INTERVAL` used as a
constant for "Kalshi OrderBook REST" in jumpbot_homer, it does NOT apply
to this bot — NBA uses pure WS orderbook without a REST fallback knob.

## UI Notes
- **Strike row column order** (see `strikeHdrHtml` + `buildStrike` in
  `INDEX_HTML`): PLAYER | LINE | CEIL AM | BOOK | FAIR NO | CEIL | CEIL
  EV% | FAIR NO% | K YES | K NO | ORDER | STATUS | POS | ADJ | SIZE |
  TGT | ON.
- **BOOK column shows YES-side American** (the over odds from
  OddsBlaze). CEIL AM and FAIR NO are NO-side American. Side mismatch
  is intentional — matches book-native display.
- **NO-side American flip was tried + reverted 2026-04-22** (commits
  f7422ab then 654c395). The flip made all three odds columns NO-side
  consistent but confused live operation. If revisiting, also need to
  flip color convention and re-verify.
- **Book availability outline** on BOOK cell: green=DK only, blue=FD
  only, red=neither, none=both. Uses CSS `outline` (not border) so
  column widths stay stable.

## Sibling Projects
- **`jumpbot_homer/`** — Live MLB HR trading. Shares `OB_API_KEY`.
- **`homer_fills/`** — MLB HR fill tracker + PnL. NBA no longer sends
  here (2026-04-21 split); historical NBA rows remain as frozen archive.
- **`nba_dashboard/`** — NBA fill tracker + PnL. Receives fills from
  this bot. Shares Railway Postgres with homer_fills. See its CLAUDE.md
  for schema + endpoints.
- **`homer_pregame/`** — MLB HR pre-game pricing.
- **`jumpbot/`** — Manual trading terminal.
- **`pinnacle_bot/`** — Pinnacle-based multi-sport (KBO live trading).
