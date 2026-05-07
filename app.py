"""
JUMPBOT NBA PROPS — Automated player props market trading with odds-derived pricing.
Pulls OddsBlaze NBA points odds, tracks NBA game status, auto-prices NO-side orders.
"""

import os
import time
import json
import math
import base64
import asyncio
import threading
import logging
import sqlite3
import unicodedata
from datetime import datetime, timezone, timedelta
from flask import Flask, request, jsonify
from flask_cors import CORS
import websockets
import kalshi
import odds_feed
import boltodds_feed
import boltodds_ls_feed
import boltodds_pbp_feed
import phase_resolver
import ls_phase_manager
import clock_state_feed
import nba_feed
import op_clocks

# Odds source — always OddsBlaze REST dual-book (FD + DK)
ODDS_SOURCE = "ob"

# ---------------------------------------------------------------------------
# Sidecar Kalshi orderbook log — only written when env var is set so prod
# deploys don't burn disk. Lines align (by ts) with boltodds_nba_monitor's
# /tmp/nba_events.jsonl for cross-feed latency analysis.
# ---------------------------------------------------------------------------
OB_LOG_PATH = os.getenv("KALSHI_OB_LOG", "")  # e.g. /tmp/kalshi_nba_ob.jsonl
_ob_log_lock = threading.Lock()

def _ob_log(rec_type, ticker, payload):
    if not OB_LOG_PATH:
        return
    try:
        line = json.dumps({
            "ts": datetime.now(timezone.utc).isoformat(),
            "source": "kalshi",
            "type": rec_type,
            "ticker": ticker,
            "payload": payload,
        }, separators=(",", ":"))
        with _ob_log_lock:
            with open(OB_LOG_PATH, "a") as f:
                f.write(line + "\n")
    except Exception:
        pass  # Logging must not break trading

# ---------------------------------------------------------------------------
# App setup
# ---------------------------------------------------------------------------
app = Flask(__name__)
CORS(app)
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("nba_props")


# Make Flask's JSON serializer handle sets automatically
import flask.json.provider as _fjp
_orig_default = _fjp.DefaultJSONProvider.default
def _safe_default(self, o):
    if isinstance(o, set):
        return list(o)
    return _orig_default(self, o)
_fjp.DefaultJSONProvider.default = _safe_default

# ---------------------------------------------------------------------------
# State — multi-game architecture
# ---------------------------------------------------------------------------
# games: {game_id: {game_name, nba_game_id, kalshi_event, prop_type,
#          settings: {size, max_exposure, arb_pct, fill_target, offset},
#          markets: {ticker: {market dict}}}}
# all_markets: {ticker: ref into games} — flat index for WS/heartbeat
games = {}
all_markets = {}
games_lock = threading.Lock()

# Config defaults
DEFAULT_SIZE = 250
DEFAULT_ARB_PCT = 0
DEFAULT_OFFSET = 0
DEFAULT_MAX_EXPOSURE = 15000
DEFAULT_FILL_TARGET = 1500
DEFAULT_PLAYER_MAX = 5000  # max total contracts across all lines for one player
PROP_TYPES = ["points", "rebounds", "assists"]
PROP_SETTING_KEYS = ["size", "fill_target", "arb_pct", "offset", "player_max"]

active_odds_events = set()


def _default_prop_settings():
    """Default per-prop-type settings dict."""
    return {
        "size": DEFAULT_SIZE,
        "fill_target": DEFAULT_FILL_TARGET,
        "arb_pct": DEFAULT_ARB_PCT,
        "offset": DEFAULT_OFFSET,
        "player_max": DEFAULT_PLAYER_MAX,
    }


def _ensure_nested_settings(game):
    """Migrate flat settings to nested per-prop if needed. Idempotent."""
    gs = game.get("settings", {})
    if "points" in gs and isinstance(gs.get("points"), dict):
        return  # already nested
    flat = dict(gs)
    new_gs = {"max_exposure": flat.pop("max_exposure", DEFAULT_MAX_EXPOSURE)}
    for pt in PROP_TYPES:
        ps = {}
        for k in PROP_SETTING_KEYS:
            ps[k] = flat.get(k, _default_prop_settings().get(k, 0))
        new_gs[pt] = ps
    game["settings"] = new_gs


def _get_prop_settings(game, prop_type="points"):
    """Get settings for a specific prop type, with fallback."""
    _ensure_nested_settings(game)
    gs = game.get("settings", {})
    return gs.get(prop_type, gs.get("points", _default_prop_settings()))

# Paper mode — v2 default ON (validation fork, never live until explicitly flipped)
# Override with PAPER_MODE_DEFAULT=false in env if you really mean to ship live.
paper_mode = (os.environ.get("PAPER_MODE_DEFAULT", "true").lower() != "false")

# When 0 (or "false"/"no"), the auto-subscribe loop and manual subscribe
# endpoint will NOT call boltodds_ls_feed.subscribe / boltodds_pbp_feed.subscribe.
# This prevents prod from holding Bolt's 1-concurrent WS slot per feed when
# the LS phase resolver isn't actually being used in trading logic.
# Default ON so existing local dev / phase-compare workflows still work.
ENABLE_BOLT_FEEDS = os.environ.get("BOLT_LS_ENABLED", "1").lower() not in ("0", "false", "no", "off")

# Global smart mode
global_smart_mode = False
smart_refresh_thread = None

# Settings
settings = {
    "arb_pct": 0,
    "size": 250,
    "rounding": "down",
    "dk_weight": 50,
    "book_mode": "min",  # "blend" = weighted dk/fd, "min" = min(fd, dk) fair_no — safest against sharp moves
    "ob_poll_interval": 0.5,
    "cooldown_secs": 0.5,
    "heartbeat_interval": 0.2,
    "slow_heartbeat_interval": 5,
    "reconcile_interval": 30,
    "max_exposure_dollars": 15000,
    "ceiling_cap": 98,
    "ceiling_floor": 1,
    "max_implied_yes": 98,
    "ceil_mode": "arb",  # "arb" = homer-style (fair_no - arb%), "tiers" = min EV tiers
    "stepdown_enabled": False,
    "stepdown_delay_secs": 15,
    "post_stepdown_jump_cooldown_secs": 10,
    "floor_enabled": False,
    "floor_offset": 5,
    "join_ceil": False,
    # Withdraw entirely (no floor fallback) when qty resting ABOVE ceil exceeds this.
    # Different from a thin-queue hold: this is for stepping out of THICK walls
    # where ceil is firm. 0 = disabled. Default 500.
    "withdraw_above_ceil_threshold": 500,
    "devig_exp_heavy_fav": 1.08,   # raw >= 0.80 (heavy fav NO like -700)
    "devig_exp_mid_high": 1.10,    # raw 0.55-0.79
    "devig_exp_mid_low": 1.11,     # raw 0.40-0.54 (core range)
    "devig_exp_longshot": 1.14,    # raw < 0.40 (longshots)
    "default_min_ev": 5,
    "min_ceiling": 1,
    "min_ev_tiers": {"90": 2, "80": 3, "70": 3.5, "60": 4, "50": 5, "40": 6, "30": 7, "20": 8},
    # In-play adjusters — applied when NBA game is live (clock running Q1-Q4)
    "inplay_enabled": True,
    "inplay_size_pct": 50,            # % of normal size during live play (50 = half)
    "inplay_ceiling_adj": -2,         # add to ceiling (negative = lower, more conservative)
    "inplay_refill_delay_secs": 5,    # wait this long after a fill before reposting
    # Post-score window (separate from in-play) — short burst of extra tightness after a score
    "post_score_window_secs": 12,
    # Dead ball mode — only trade during timeouts, quarter breaks, halftime
    "deadball_only": False,
    # Auto-stop in 4th quarter — pulls all orders when Q4 starts
    "auto_stop_q4": False,
    "auto_stop_q3": False,  # Pull all orders at Q3 tipoff. 1H+halftime only.
    # Require FanDuel odds — if FD is missing for a player|line, refuse to trade.
    # FD is the more reliable book; trading single-book on DK only got us
    # past-posted on Fox + others 2026-04-24 when FD was stale/down. Default ON.
    "require_fanduel": True,
    # Max age (seconds) since FD's last data update. If FD's poller has been
    # silent longer than this, treat as suspended even if cached prices are
    # still in player_odds_detail. Closes the "stale FD" leak that
    # require_fanduel alone can't catch (cached values look real).
    "fd_max_age_secs": 30,
    # Distance-tier sizing — caps per-order size by how close player is to the line.
    # Catches late-game stat surges where a 250-contract NO fill becomes paper-loss
    # (e.g., Robinson 14.5 with stat=13 in Q4 → 1.5 distance, fills are nearly dead).
    # kill_thresh = pull/skip when distance <= this; t2/t3 = caps below those distances.
    "dist_tier_enabled": True,
    "dist_tier_caps": {
        "kill_thresh": 1.0,   # distance ≤ 1 stat-unit → no trade, pull resting orders
        "t2_thresh": 2.0,     # distance ≤ 2 → cap at cap_t2
        "t3_thresh": 4.0,     # distance ≤ 4 → cap at cap_t3
        "cap_t2": 25,
        "cap_t3": 100,
    },
    # Auto-start when game goes live — starts all bots on tipoff
    "auto_start_live": False,
    # Phase-aware trading (replaces scattered inplay_* flags as the primary mechanism).
    # Master switch — if False, trade uniformly regardless of phase (base settings only).
    "phase_settings_enabled": True,
    # Per-phase config. enabled=False → no trading in that phase (cancel on entry).
    # size_pct = % of base size. arb_pct_delta = cents added to base arb_pct
    # (positive = wider buffer / lower ceiling = more conservative).
    "phase_settings": {
        "PRE":           {"enabled": True,  "size_pct": 100, "arb_pct_delta": 0},
        "LIVE":          {"enabled": False, "size_pct": 50,  "arb_pct_delta": 2},
        "OVERTIME":      {"enabled": False, "size_pct": 50,  "arb_pct_delta": 2},
        "TIMEOUT":       {"enabled": True,  "size_pct": 100, "arb_pct_delta": 0},
        "QUARTER_BREAK": {"enabled": True,  "size_pct": 100, "arb_pct_delta": 0},
        "HALFTIME":      {"enabled": True,  "size_pct": 100, "arb_pct_delta": 0},
        "FOUL_SHOT":     {"enabled": False, "size_pct": 100, "arb_pct_delta": 0},
        "FINAL":         {"enabled": False, "size_pct": 0,   "arb_pct_delta": 0},
        "UNKNOWN":       {"enabled": False, "size_pct": 100, "arb_pct_delta": 0},
    },
}
PHASES_ALL = ["PRE", "LIVE", "OVERTIME", "TIMEOUT", "QUARTER_BREAK",
              "HALFTIME", "FOUL_SHOT", "FINAL", "UNKNOWN"]

# Session log
session_log = []
session_log_lock = threading.Lock()
MAX_LOG = 2000

# Pause state
all_paused = False
paused_tickers = set()

# WebSocket state
PROD_WS = "wss://api.elections.kalshi.com/trade-api/ws/v2"
ws_connected = False
ws_disconnected_at = None
ws_loop = None
ws_task = None
pending_ws_subscribes = set()

# ---------------------------------------------------------------------------
# WebSocket — Kalshi orderbook tracking (ported from JumpBot)
# ---------------------------------------------------------------------------

def price_to_cents(val):
    if val is None or val == 0:
        return 0
    f = float(val)
    return round(f * 100) if f < 1.0 else round(f)


def parse_orderbook_side(entries):
    result = {}
    for entry in entries:
        if isinstance(entry, list):
            price = price_to_cents(entry[0])
            qty = int(float(entry[1])) if len(entry) > 1 else 0
        elif isinstance(entry, dict):
            price = price_to_cents(entry.get("price", entry.get("price_fp", 0)))
            qty = int(entry.get("quantity", entry.get("quantity_fp",
                      entry.get("delta", entry.get("delta_fp", 0)))))
        else:
            continue
        if price > 0 and qty > 0:
            result[price] = qty
    return result


def compute_best_bid(book_side, exclude_price=None, exclude_size=0):
    # Subtract our resting size from its level; only drop the level if nothing else is there.
    prices = []
    for p, q in book_side.items():
        if q <= 0:
            continue
        if p == exclude_price and exclude_size > 0:
            if q - exclude_size > 0:
                prices.append(p)
        else:
            prices.append(p)
    return max(prices) if prices else 0


def fetch_and_apply_orderbook(ticker):
    if ticker not in all_markets:
        return
    try:
        # Capture pre-fetch timestamp so we can detect WS deltas that landed
        # *during* our REST roundtrip and avoid clobbering them.
        fetch_start_ts = time.time()
        snap = kalshi.kalshi_get(f"/markets/{ticker}/orderbook")
        ob = snap.get("orderbook_fp") or snap.get("orderbook") or snap
        raw_yes = (ob.get("yes_dollars_fp") or ob.get("yes_dollars")
                   or ob.get("yes") or ob.get("yes_fp", []))
        raw_no = (ob.get("no_dollars_fp") or ob.get("no_dollars")
                  or ob.get("no") or ob.get("no_fp", []))
        new_yes = parse_orderbook_side(raw_yes)
        new_no = parse_orderbook_side(raw_no)
        mkt = all_markets[ticker]
        with games_lock:
            # Skip REST overwrite ONLY if WS data is fresh (recent update).
            # The old "skip if any WS data exists" lock-out meant that once WS
            # got going, REST could never recover the book even after WS went
            # silent for minutes.
            last_ws = mkt.get("_ob_last_update", 0)
            if time.time() - last_ws < 5:
                return
            # If a WS delta landed during our REST call, it's newer than our
            # snapshot — don't blindly overwrite a delta-progressed book.
            if last_ws > fetch_start_ts:
                return
            mkt["orderbook"] = {"yes": new_yes, "no": new_no}
            mkt["best_bid_yes"] = compute_best_bid(mkt["orderbook"]["yes"])
            mkt["best_bid_no"] = compute_best_bid(mkt["orderbook"]["no"])
            mkt["_ob_last_update"] = time.time()
    except Exception as e:
        log.error(f"OB fetch error {ticker}: {e}")


def ws_sign_headers():
    if not kalshi.is_authenticated():
        return {}
    ts = str(int(datetime.now().timestamp() * 1000))
    message = (ts + "GET" + "/trade-api/ws/v2").encode("utf-8")
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.asymmetric import padding
    sig = kalshi._creds["key"].sign(
        message,
        padding.PSS(
            mgf=padding.MGF1(hashes.SHA256()),
            salt_length=padding.PSS.DIGEST_LENGTH,
        ),
        hashes.SHA256(),
    )
    return {
        "KALSHI-ACCESS-KEY": kalshi._creds["api_key_id"],
        "KALSHI-ACCESS-SIGNATURE": base64.b64encode(sig).decode("utf-8"),
        "KALSHI-ACCESS-TIMESTAMP": ts,
    }


async def ws_manager():
    global ws_connected, ws_disconnected_at
    while True:
        if not kalshi.is_authenticated():
            await asyncio.sleep(1)
            continue
        all_tickers = list(all_markets.keys())
        if not all_tickers and not pending_ws_subscribes:
            await asyncio.sleep(1)
            continue
        try:
            hdrs = ws_sign_headers()
            async with websockets.connect(
                PROD_WS, additional_headers=hdrs,
                ping_interval=20, ping_timeout=10,
            ) as ws:
                ws_connected = True
                ws_disconnected_at = None
                add_log("WS_CONNECTED", detail="orderbook tracking started")

                # Reconcile before subscribing
                try:
                    reconcile_resting_orders()
                    add_log("WS_RECONCILE", detail="reconciled on reconnect")
                except Exception as e:
                    log.error(f"WS reconnect reconcile failed: {e}")

                subscribe_tickers = list(set(all_tickers) | set(pending_ws_subscribes))
                active_tickers = list(subscribe_tickers)
                if subscribe_tickers:
                    sub_msg = {
                        "id": int(time.time() * 1000),
                        "cmd": "subscribe",
                        "params": {
                            "channels": ["orderbook_delta"],
                            "market_tickers": subscribe_tickers,
                        },
                    }
                    await ws.send(json.dumps(sub_msg))
                    log.info(f"WS subscribed to {len(subscribe_tickers)} tickers")

                # Subscribe to user fill channel — instant fill detection, no polling lag
                fill_sub = {
                    "id": int(time.time() * 1000) + 1,
                    "cmd": "subscribe",
                    "params": {"channels": ["fill"]},
                }
                await ws.send(json.dumps(fill_sub))
                log.info("WS subscribed to fill channel")

                # Fetch initial orderbook snapshots, rate-limited to stay
                # under Kalshi's 30 reads/sec. With 120 tickers at startup,
                # firing all at once blows past the limit, 429s some, and
                # those markets boot with empty books. Throttle to ~15/sec
                # (≈66ms between fetches) so we leave headroom for position
                # polls and resting-order syncs happening concurrently.
                # 120 markets → ~8s to fully populate. WS-delivered
                # orderbook_snapshot messages arrive in parallel and fill
                # in anything the REST path lags on.
                def _batch_fetch_obs(tickers_to_fetch):
                    from concurrent.futures import ThreadPoolExecutor
                    failed = 0
                    FETCH_PER_SEC = 15
                    SLEEP = 1.0 / FETCH_PER_SEC
                    # Use a tiny pool so we overlap latency (each REST call
                    # is ~100ms) but the rate is bounded by the sleep below.
                    with ThreadPoolExecutor(max_workers=3) as pool:
                        futures = []
                        for tk in tickers_to_fetch:
                            futures.append(pool.submit(fetch_and_apply_orderbook, tk))
                            time.sleep(SLEEP)
                        for f in futures:
                            try:
                                f.result()
                            except Exception:
                                failed += 1
                    log.info(
                        f"WS initial OB fetch: {len(tickers_to_fetch)} "
                        f"requested, {failed} failed "
                        f"(rate {FETCH_PER_SEC}/s). Staleness poller will "
                        f"cover any gaps."
                    )
                threading.Thread(target=_batch_fetch_obs, args=(list(subscribe_tickers),), daemon=True).start()
                pending_ws_subscribes.clear()

                while True:
                    if pending_ws_subscribes:
                        new_tickers = set(pending_ws_subscribes) - set(active_tickers)
                        pending_ws_subscribes.clear()
                        if new_tickers:
                            sub_msg = {
                                "id": int(time.time() * 1000),
                                "cmd": "subscribe",
                                "params": {
                                    "channels": ["orderbook_delta"],
                                    "market_tickers": list(new_tickers),
                                },
                            }
                            await ws.send(json.dumps(sub_msg))
                            active_tickers.extend(new_tickers)
                            # Fetch snapshots in background — don't block WS recv
                            _new_list = list(new_tickers)
                            threading.Thread(target=lambda tks: [fetch_and_apply_orderbook(t) for t in tks],
                                           args=(_new_list,), daemon=True).start()

                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=1.0)
                    except asyncio.TimeoutError:
                        continue

                    try:
                        msg = json.loads(raw)
                    except json.JSONDecodeError:
                        continue

                    msg_type = msg.get("type", "")

                    if msg_type == "orderbook_delta":
                        data = msg.get("msg", msg)
                        ticker = data.get("market_ticker", "")
                        if ticker not in all_markets:
                            continue
                        # Diagnostic: log first 5 raw deltas per market so we can
                        # confirm Kalshi field formats (delta vs delta_fp, price
                        # vs price_dollars, presence of seq). Remove once verified.
                        _mkt_diag = all_markets.get(ticker)
                        if _mkt_diag and _mkt_diag.get("_delta_log_count", 0) < 5:
                            log.info(f"WS_DELTA_RAW {ticker}: {json.dumps(data)}")
                            _mkt_diag["_delta_log_count"] = _mkt_diag.get("_delta_log_count", 0) + 1
                        side_key = data.get("side", "")
                        price_raw = data.get("price", data.get("price_fp", data.get("price_dollars", 0)))
                        delta_raw = data.get("delta", data.get("delta_fp", 0))
                        # Use 'is not None' so legitimate price=0 doesn't silently drop.
                        # (Kalshi prices are 1-99 today, but be defensive.)
                        if side_key in ("yes", "no") and price_raw is not None:
                            price = price_to_cents(price_raw)
                            delta = int(float(delta_raw))
                            seq = data.get("seq")
                            _ob_log("delta", ticker,
                                    {"side": side_key, "price": price, "delta": delta, "seq": seq})
                            # Sequence-gap detection: if Kalshi sends seq, watch for
                            # gaps. A gap means we missed a delta — book is now untrusted,
                            # trigger a force-refetch so we resync to truth.
                            seq_gap = False
                            if seq is not None and _mkt_diag is not None:
                                last_seq = _mkt_diag.get("_last_ob_seq")
                                if last_seq is not None and seq != last_seq + 1:
                                    log.warning(
                                        f"OB SEQ GAP {ticker}: {last_seq} → {seq} "
                                        f"(missed {seq - last_seq - 1})"
                                    )
                                    seq_gap = True
                                _mkt_diag["_last_ob_seq"] = seq
                            # Lock the read-modify-write so a REST snapshot or other
                            # reader can't see a half-applied book.
                            with games_lock:
                                mkt = all_markets.get(ticker)
                                if not mkt:
                                    continue
                                ob = mkt.get("orderbook") or {"yes": {}, "no": {}}
                                current = ob.get(side_key, {}).get(price, 0)
                                new_qty = max(0, current + delta)
                                if new_qty > 0:
                                    ob.setdefault(side_key, {})[price] = new_qty
                                else:
                                    ob.get(side_key, {}).pop(price, None)
                                mkt["orderbook"] = ob
                                mkt[f"best_bid_{side_key}"] = compute_best_bid(ob.get(side_key, {}))
                                mkt["_ob_last_update"] = time.time()
                            # On seq gap, fire a background resync so the book gets
                            # corrected by REST. Outside the lock — REST is HTTP.
                            if seq_gap:
                                threading.Thread(
                                    target=_force_refetch_orderbook,
                                    args=(ticker,),
                                    daemon=True,
                                ).start()
                            # Don't call process_market here — it holds games_lock for HTTP calls
                            # which blocks the WS receive loop. Fast heartbeat handles it every 200ms.

                    elif msg_type == "orderbook_snapshot":
                        data = msg.get("msg", msg)
                        ticker = data.get("market_ticker", "")
                        if ticker not in all_markets:
                            continue
                        ob = {"yes": {}, "no": {}}
                        for side_key in ["yes", "no"]:
                            entries = (data.get(f"{side_key}_dollars_fp")
                                       or data.get(f"{side_key}_dollars")
                                       or data.get(side_key)
                                       or data.get(f"{side_key}_fp", []))
                            ob[side_key] = parse_orderbook_side(entries)
                        snap_seq = data.get("seq")
                        _ob_log("snapshot", ticker,
                                {"yes": ob["yes"], "no": ob["no"], "seq": snap_seq})
                        with games_lock:
                            mkt = all_markets.get(ticker)
                            if not mkt:
                                continue
                            mkt["orderbook"] = ob
                            mkt["best_bid_yes"] = compute_best_bid(ob["yes"])
                            mkt["best_bid_no"] = compute_best_bid(ob["no"])
                            mkt["_ob_last_update"] = time.time()
                            # Reset sequence tracker — snapshot is fresh source of truth.
                            if snap_seq is not None:
                                mkt["_last_ob_seq"] = snap_seq

                    elif msg_type == "fill":
                        # Instant fill detection via Kalshi WS — no polling lag
                        data = msg.get("msg", msg)
                        _handle_ws_fill(data)

        except websockets.ConnectionClosed:
            ws_connected = False
            ws_disconnected_at = time.time()
            add_log("WS_DISCONNECT", detail="reconnecting in 3s...")
            await asyncio.sleep(3)
        except Exception as e:
            ws_connected = False
            ws_disconnected_at = time.time()
            log.error(f"WS error: {e}")
            await asyncio.sleep(3)


def start_ws_thread():
    global ws_loop, ws_task
    if ws_loop and ws_loop.is_running():
        ws_loop.call_soon_threadsafe(ws_loop.stop)
        time.sleep(0.5)
    def run():
        global ws_loop, ws_task
        ws_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(ws_loop)
        ws_task = ws_loop.create_task(ws_manager())
        ws_loop.run_forever()
    threading.Thread(target=run, daemon=True).start()
    log.info("WS thread started")


# ---------------------------------------------------------------------------
# Session logging
# ---------------------------------------------------------------------------

def add_log(action, ticker="", detail="", side="no", price=0, size=0):
    """Add entry to session log."""
    entry = {
        "ts": time.strftime("%H:%M:%S"),
        "action": action,
        "ticker": ticker,
        "detail": detail,
        "side": side,
        "price": price,
        "size": size,
    }
    with session_log_lock:
        session_log.append(entry)
        if len(session_log) > MAX_LOG:
            del session_log[:len(session_log) - MAX_LOG]


# ---------------------------------------------------------------------------
# Player name matching
# ---------------------------------------------------------------------------

def normalize_name(name):
    """Normalize player name for fuzzy matching — strips accents, punctuation."""
    nfkd = unicodedata.normalize("NFKD", name)
    ascii_name = "".join(c for c in nfkd if not unicodedata.combining(c))
    return ascii_name.lower().replace(".", "").replace("'", "").replace("-", "").replace("  ", " ").strip()


def match_player_to_odds(kalshi_name, odds_players, market_line=None):
    """Try to match a Kalshi market to an odds entry by player name + line.
    odds_players is keyed by 'player|line' (e.g. 'LaMelo Ball|24.5').
    If market_line provided, only matches entries with that exact line.
    """
    kn = normalize_name(kalshi_name)
    for key, data in odds_players.items():
        # Key is "player|line"
        parts = key.split("|")
        player_name = parts[0] if parts else key
        entry_line = float(parts[1]) if len(parts) > 1 else None

        # Check line match first if we have a market_line
        if market_line is not None and entry_line is not None:
            if abs(entry_line - market_line) > 0.6:
                continue  # wrong line

        pn = normalize_name(player_name)
        # Exact name match
        if kn == pn:
            return key
        # Last name + first initial match
        k_last = kn.split()[-1] if kn.split() else ""
        p_last = pn.split()[-1] if pn.split() else ""
        if k_last and k_last == p_last and len(k_last) > 2:
            k_first = kn[0] if kn else ""
            p_first = pn[0] if pn else ""
            if k_first == p_first:
                return key
    return None


# ---------------------------------------------------------------------------
# Trading engine — penny-jump NO side (identical logic to jumpbot_homer)
# ---------------------------------------------------------------------------

def compute_ceiling(player_name, manual_offset=0, arb_pct=None, market_line=None, prop_type="points"):
    """Compute NO ceiling using min EV system with tiered thresholds.
    ceil = floor(fair_no / (1 + min_ev/100)) + offset
    Uses 2-way devig fair_no when available, else exponent devig.
    Min EV selected by tier based on fair_no value."""
    # Min odds filter: skip lines where Over is too heavy (e.g., -1000+)
    max_implied = settings.get("max_implied_yes", 95)
    detail = _find_detail(player_name, market_line, prop_type)
    fd = detail.get("fanduel", {})
    dk = detail.get("draftkings", {})
    best_imp = fd.get("implied_yes") or dk.get("implied_yes") or 0
    if best_imp >= max_implied:
        return None

    # Require FanDuel: if FD has no odds for this player|line, OR FD's poller
    # has been silent longer than fd_max_age_secs, refuse to trade. Single-book
    # trading on DK alone past-posted us on Fox + others 2026-04-24.
    if settings.get("require_fanduel", True):
        if not (fd.get("implied_yes") or fd.get("odds")):
            return None
        max_age = settings.get("fd_max_age_secs", 30)
        if boltodds_feed.get_book_age("fanduel") > max_age:
            return None

    dk_w = settings.get("dk_weight", 50)
    blended_imp = get_blended_implied(player_name, dk_w, market_line=market_line, prop_type=prop_type)
    if blended_imp is None or blended_imp <= 0:
        return None
    fair_no = 100 - blended_imp

    # Ceiling mode: simple arb (homer-style) or min EV tiers
    if arb_pct is None:
        arb_pct = settings.get("arb_pct", 0)

    if settings.get("ceil_mode", "arb") == "arb":
        # Arb mode: ceil = book's raw implied NO - arb_pct
        # arb=0 means ceiling IS the book's price (closest to a true arb)
        # arb=1 means 1c better than arb, etc.
        detail = _find_detail(player_name, market_line, prop_type)
        dk = detail.get("draftkings", {})
        fd = detail.get("fanduel", {})
        fd_imp_no = (100 - fd.get("implied_yes", 0)) if fd.get("implied_yes") else 0
        dk_imp_no = (100 - dk.get("implied_yes", 0)) if dk.get("implied_yes") else 0

        book_mode = settings.get("book_mode", "min")
        if fd_imp_no and dk_imp_no:
            if book_mode == "min":
                book_no = min(fd_imp_no, dk_imp_no)  # most conservative
            else:
                w = settings.get("dk_weight", 50) / 100.0
                book_no = dk_imp_no * w + fd_imp_no * (1 - w)
        elif fd_imp_no:
            book_no = fd_imp_no
        elif dk_imp_no:
            book_no = dk_imp_no
        else:
            book_no = fair_no  # fallback to devigged

        raw_ceil = book_no - arb_pct + manual_offset
    else:
        # Min EV tiers: ceil = fair_no / (1 + min_ev%)
        min_ev_tiers = settings.get("min_ev_tiers", {})
        min_ev = float(settings.get("default_min_ev", 3))
        for floor_str in sorted(min_ev_tiers.keys(), key=lambda x: int(x), reverse=True):
            if fair_no >= int(floor_str):
                min_ev = float(min_ev_tiers[floor_str])
                break
        if min_ev > 0:
            raw_ceil = fair_no / (1 + min_ev / 100) + manual_offset
        else:
            raw_ceil = fair_no + manual_offset

    rounding = settings.get("rounding", "down")
    if rounding == "up":
        ceil = math.ceil(raw_ceil)
    elif rounding == "down":
        ceil = math.floor(raw_ceil)
    else:
        ceil = round(raw_ceil)

    cap = int(settings.get("ceiling_cap", 98))
    cap = max(1, min(99, cap))
    floor_ceil = int(settings.get("ceiling_floor", 1))
    floor_ceil = max(1, min(99, floor_ceil))

    rnd_ceil = ceil  # already rounded above
    ceil = min(cap, rnd_ceil)

    # Kill threshold: if computed ceiling is below floor, don't trade this market
    if ceil < floor_ceil:
        return None

    # Safety: never allow ceiling above fair_no
    if ceil >= fair_no:
        ceil = max(1, math.floor(fair_no) - 1)

    ceil = max(1, min(99, ceil))

    return ceil


def _odds_key(player_name, market_line):
    """Build the player|line key used in odds_feed."""
    if market_line is not None:
        return f"{player_name}|{market_line}"
    return player_name


def _get_odds_source(prop_type="points"):
    """Get the active odds data dicts for a given prop type."""
    return boltodds_feed.player_odds_detail.get(prop_type, {}), boltodds_feed.player_odds.get(prop_type, {})


def _find_detail(player_name, market_line, prop_type="points"):
    """Find the odds detail for a player+line, exact or fuzzy match.

    Fuzzy match (2026-04-25): pick the CLOSEST line, ties broken by
    preferring FD line BELOW the Kalshi line. Reason: Kalshi 'X+' markets
    (line=X) are equivalent to FD '(X-0.5) OVER' since rebounds/assists/
    points are integers — Kalshi 10+ ≡ FD 9.5 OVER. FD 10.5 OVER would be
    '11+' which is harder, wrong match. Old code took the FIRST entry
    within ±0.6 (dict iteration order = OddsBlaze response order), which
    silently picked the wrong line whenever FD had both X-0.5 and X+0.5
    listed. This caused massively wrong fair_no values (e.g., Wendell
    Carter rebounds showing 27c ceil vs ~13c real)."""
    detail_dict, _ = _get_odds_source(prop_type)
    key = _odds_key(player_name, market_line)
    detail = detail_dict.get(key, {})
    if not detail and market_line is not None:
        candidates = []
        # Snapshot — OddsBlaze poller mutates detail_dict from another thread,
        # was raising RuntimeError("dictionary changed size during iteration")
        # under live load (mirrors _find_best's pattern).
        for k, v in list(detail_dict.items()):
            parts = k.split("|")
            if len(parts) == 2 and normalize_name(parts[0]) == normalize_name(player_name):
                try:
                    line = float(parts[1])
                    diff = line - market_line  # negative if below Kalshi line
                    if abs(diff) <= 0.6:
                        candidates.append((diff, v))
                except (ValueError, TypeError):
                    continue
        if candidates:
            # Sort key: prefer below-line first (diff < 0), then closest by abs.
            # (diff >= 0) is False for below, True for above → False sorts first.
            candidates.sort(key=lambda x: (x[0] >= 0, abs(x[0])))
            detail = candidates[0][1]
    return detail


def _find_best(player_name, market_line, prop_type="points"):
    """Find best odds entry for a player+line. Same closest-line + below-bias
    fuzzy match as _find_detail (see its docstring for rationale)."""
    _, odds_dict = _get_odds_source(prop_type)
    key = _odds_key(player_name, market_line)
    best = odds_dict.get(key, {})
    if not best and market_line is not None:
        candidates = []
        for k, v in list(odds_dict.items()):
            parts = k.split("|")
            if len(parts) == 2 and normalize_name(parts[0]) == normalize_name(player_name):
                try:
                    line = float(parts[1])
                    diff = line - market_line
                    if abs(diff) <= 0.6:
                        candidates.append((diff, v))
                except (ValueError, TypeError):
                    continue
        if candidates:
            candidates.sort(key=lambda x: (x[0] >= 0, abs(x[0])))
            best = candidates[0][1]
    return best


def raw_prob(odds: int) -> float:
    """Convert American odds to raw implied probability (0-1)."""
    if odds > 0:
        return 100 / (odds + 100)
    else:
        return abs(odds) / (abs(odds) + 100)


def get_devig_exp(raw: float) -> float:
    """Dynamic power exponent based on raw implied probability."""
    if raw >= 0.80:
        return settings.get("devig_exp_heavy_fav", 1.08)
    elif raw >= 0.55:
        return settings.get("devig_exp_mid_high", 1.10)
    elif raw >= 0.40:
        return settings.get("devig_exp_mid_low", 1.11)
    else:
        return settings.get("devig_exp_longshot", 1.14)


def devig_one_way(odds: int) -> float:
    """Power devig for one-way lines. Returns fair YES probability (0-100)."""
    raw = raw_prob(odds)
    exp = get_devig_exp(raw)
    return raw ** exp * 100


def devig_two_way(over_odds: int, under_odds: int) -> tuple:
    """Normalization devig for two-way lines. Returns (fair_yes, fair_no) as 0-100."""
    raw_over = raw_prob(over_odds)
    raw_under = raw_prob(under_odds)
    total = raw_over + raw_under
    if total <= 0:
        return 0, 0
    return round(raw_over / total * 100, 2), round(raw_under / total * 100, 2)


def _devig_implied_yes(detail_book):
    """Get fair YES from a single book's detail dict.
    Returns (fair_yes, method, exponent).
    Two-way → normalization. One-way → dynamic power.
    """
    if not detail_book:
        return None, None, None
    if detail_book.get("has_under") and detail_book.get("fair_yes"):
        return detail_book["fair_yes"], "two_way_norm", None
    odds = detail_book.get("odds")
    if odds and odds != 0:
        raw = raw_prob(odds)
        exp = get_devig_exp(raw)
        fair_yes = raw ** exp * 100
        return round(fair_yes, 2), "power_one_way", exp
    imp = detail_book.get("implied_yes", 0) or 0
    if imp > 0:
        raw = imp / 100
        exp = get_devig_exp(raw)
        fair_yes = raw ** exp * 100
        return round(fair_yes, 2), "power_one_way", exp
    return None, None, None


def get_blended_implied(player_name, dk_weight_pct=50, market_line=None, prop_type="points"):
    """Get blended or min DK/FD implied YES probability for a specific player+line.
    book_mode="min": use MAX fair_yes (= min fair_no) — most conservative, safest on sharp moves.
    book_mode="blend": weighted average of DK/FD.
    Returns float (0-100) or None if no data.
    """
    detail = _find_detail(player_name, market_line, prop_type)
    dk = detail.get("draftkings", {})
    fd = detail.get("fanduel", {})

    dk_val, _, _ = _devig_implied_yes(dk)
    fd_val, _, _ = _devig_implied_yes(fd)

    # Require FanDuel: if FD missing OR stale, return None so caller pulls orders.
    if settings.get("require_fanduel", True):
        if not fd_val:
            return None
        max_age = settings.get("fd_max_age_secs", 30)
        if boltodds_feed.get_book_age("fanduel") > max_age:
            return None

    book_mode = settings.get("book_mode", "min")

    if dk_val and fd_val:
        if book_mode == "min":
            return max(dk_val, fd_val)
        else:
            w = dk_weight_pct / 100.0
            return dk_val * w + fd_val * (1 - w)
    elif dk_val:
        return dk_val
    elif fd_val:
        return fd_val

    best = _find_best(player_name, market_line, prop_type)
    best_val, _, _ = _devig_implied_yes(best)
    if best_val:
        return best_val
    return None


def get_blended_odds(player_name, dk_weight_pct=50, market_line=None, prop_type="points"):
    """Get blended DK/FD American odds for a specific player+line.
    Returns (blended_american_odds, dk_odds, fd_odds) or (None, None, None).
    """
    detail = _find_detail(player_name, market_line, prop_type)
    dk = detail.get("draftkings", {})
    fd = detail.get("fanduel", {})
    dk_odds = dk.get("odds") if dk else None
    fd_odds = fd.get("odds") if fd else None
    dk_imp = dk.get("implied_yes", 0) if dk else 0
    fd_imp = fd.get("implied_yes", 0) if fd else 0
    if dk_imp and fd_imp:
        w = dk_weight_pct / 100.0
        blended_imp = dk_imp * w + fd_imp * (1 - w)
        if blended_imp <= 0 or blended_imp >= 100:
            blended_am = None
        elif blended_imp > 50:
            blended_am = -int(round(blended_imp / (100 - blended_imp) * 100))
        else:
            blended_am = int(round((100 - blended_imp) / blended_imp * 100))
        return blended_am, dk_odds, fd_odds
    elif dk_imp:
        return dk_odds, dk_odds, None
    elif fd_imp:
        return fd_odds, None, fd_odds
    else:
        best = _find_best(player_name, market_line, prop_type)
        return best.get("odds"), None, None


# ---------------------------------------------------------------------------
# Smart Mode — auto start/stop games based on NBA game status
# ---------------------------------------------------------------------------

def on_game_start(nba_game_id):
    """NBA game went live (tipoff) — auto-start bots if smart_mode or auto_start_live."""
    nba_game_id = str(nba_game_id)
    auto_live = settings.get("auto_start_live", False)
    with games_lock:
        for gid, game in games.items():
            if str(game.get("nba_game_id", "")) != nba_game_id:
                continue
            if not game.get("smart_mode") and not auto_live:
                continue
            started = 0
            for tk, mkt in game["markets"].items():
                if not mkt["prop_done"] and not mkt["active"]:
                    mkt["active"] = True
                    mkt["status"] = "JUMPING"
                    mkt["last_order_time"] = 0
                    started += 1
            game["smart_status"] = "LIVE"
            label = "AUTO_START_LIVE" if auto_live else "SMART_START"
            add_log(label, detail=f"Auto-started {started} bots in {gid} — tipoff")
            log.info(f"{label} [{gid}] auto-started {started} bots on tipoff")


def on_game_end(nba_game_id):
    """NBA game went final — auto-stop bots, keep positions."""
    nba_game_id = str(nba_game_id)
    with games_lock:
        for gid, game in games.items():
            if str(game.get("nba_game_id", "")) == nba_game_id and game.get("smart_mode"):
                stopped = 0
                for tk, mkt in game["markets"].items():
                    if mkt["active"]:
                        mkt["active"] = False
                        if mkt.get("order_id") and not str(mkt["order_id"]).startswith("paper_"):
                            try:
                                kalshi.cancel_order(mkt["order_id"])
                            except Exception:
                                pass
                            mkt["order_id"] = None
                            mkt["order_price"] = None
                        if not mkt["prop_done"]:
                            mkt["status"] = "GAME_OVER"
                        stopped += 1
                game["smart_status"] = "FINAL"
                add_log("SMART_STOP", detail=f"Auto-stopped {stopped} bots in {gid} — game final")
                log.info(f"SMART_STOP [{gid}] auto-stopped {stopped} bots — game final")


def _on_score_change(game_id, total_score, diff):
    """Score change in a live game — log it. Markets in this game will use
    in-play tightened settings for post_score_window_secs."""
    add_log("SCORE_CHANGE", detail=f"game={game_id} +{diff}pts (total={total_score})")


# Set callbacks
nba_feed.set_callbacks(
    on_game_start=on_game_start,
    on_game_end=on_game_end,
    on_score_change=_on_score_change,
)


def _smart_mode_loop():
    """Background loop: checks NBA schedule, manages lifecycle."""
    _last_schedule_check = 0

    while True:
        if not global_smart_mode:
            time.sleep(5)
            continue

        now = time.time()

        # Every 5 min: check schedule for new games
        if now - _last_schedule_check > 300:
            _last_schedule_check = now
            try:
                todays = nba_feed.get_todays_games()
                for g in todays:
                    gid_nba = str(g.get("game_id", ""))
                    status = g.get("status", "")
                    if not gid_nba or status == "Final":
                        continue
                    # Check if already loaded
                    already = any(str(game.get("nba_game_id", "")) == gid_nba for game in games.values())
                    if already:
                        continue
                    # Auto-load not implemented for v1 — user loads manually
            except Exception as e:
                log.error(f"Smart schedule check failed: {e}")

        # Every 30s: check lifecycle of all loaded games
        for gid, game in list(games.items()):
            if not game.get("smart_mode"):
                continue
            nba_gid = game.get("nba_game_id")
            if not nba_gid:
                continue
            try:
                lifecycle = nba_feed.get_game_lifecycle(str(nba_gid))
                if lifecycle == "Live" and game.get("smart_status") == "WAITING":
                    on_game_start(nba_gid)
                if lifecycle == "Final" and game.get("smart_status") not in ("FINAL",):
                    on_game_end(nba_gid)
                    time.sleep(1)
            except Exception as e:
                log.error(f"Smart lifecycle check failed for {gid}: {e}")

        time.sleep(30)


# ---------------------------------------------------------------------------
# In-play adjusters — apply when clock is running (active game play)
# ---------------------------------------------------------------------------

def _is_inplay(mkt):
    """Returns True if market's NBA game is actively in live play (clock running).

    Primary source: `clock_state_feed.is_running(bolt_key)` — push-driven
    Bolt PBP+LS multiplexer, fires on the whistle (~0ms after WS push).
    Fallback: `nba_feed.is_clock_running()` — CDN-derived, 5-15s lag.

    Falls back when:
      - bolt_key not resolved for the game
      - clock_state_feed reports `dead=True` (no events for >180s)
      - clock_state_feed has no opinion (running is None)
    """
    if not settings.get("inplay_enabled", True):
        return False
    nba_gid = mkt.get("nba_game_id")
    if not nba_gid:
        return False
    # Prefer the fast feed
    bolt_key = nba_feed.get_bolt_key(str(nba_gid))
    if bolt_key:
        cf_state = clock_state_feed.get_state(bolt_key)
        if (cf_state and cf_state.get("running") is not None
                and not cf_state.get("dead")):
            cf_running = bool(cf_state["running"])
            # Shadow-log disagreements with CDN for telemetry
            cdn_running = nba_feed.is_clock_running(str(nba_gid))
            if cdn_running is not None and cf_running != cdn_running:
                log.info(f"clock_disagree [{nba_gid}] cf={cf_running} "
                         f"(via {cf_state.get('source')}) vs cdn={cdn_running}")
            return cf_running
    # Fallback path
    return nba_feed.is_clock_running(str(nba_gid))


def _is_post_score(mkt):
    """Returns True if this market's game just had a score (within window)."""
    nba_gid = mkt.get("nba_game_id")
    if not nba_gid:
        return False
    window = settings.get("post_score_window_secs", 12)
    elapsed = nba_feed.get_post_score_window(str(nba_gid), window_secs=window)
    return elapsed is not None


# ---------------------------------------------------------------------------
# Process market — penny-jump NO side (identical logic to homer)
# ---------------------------------------------------------------------------

def compute_market_action(ticker, mkt):
    """Pure computation — decides what to do for a market. No HTTP calls.
    Returns an action dict or None. MUST be called with games_lock held."""
    if not mkt["active"] or mkt["prop_done"]:
        return None

    # Backoff after consecutive place failures
    if mkt.get("_place_backoff_until") and time.time() < mkt["_place_backoff_until"]:
        return None

    # Post-suspension recovery window — see _on_suspension. Holds re-quote
    # for ~3s after a book flickered so blended price can stabilize.
    if mkt.get("_resume_after") and time.time() < mkt["_resume_after"]:
        return None

    # Dead ball only mode — only trade during dead ball phases
    if settings.get("deadball_only"):
        game = games.get(mkt.get("game_id", ""), {})
        espn_gid = game.get("nba_game_id")
        if espn_gid:
            phase = nba_feed.get_game_phase(str(espn_gid))
            if phase in ("LIVE", "OVERTIME"):
                # Live play — pull any existing order and wait
                order_id = mkt.get("order_id")
                if order_id and not str(order_id).startswith("paper_"):
                    mkt["status"] = "DEADBALL_WAIT"
                    mkt["last_order_time"] = time.time()
                    return {"type": "cancel", "order_id": order_id, "reason": "deadball_only"}
                mkt["status"] = "DEADBALL_WAIT"
                return None

    # Auto-stop Q3 / Q4 — pull all orders at the start of the configured quarter.
    # Q3 stop = trade only 1H + halftime (highest-edge buckets per CLV60 analysis).
    # Q4 stop = include 1H + halftime + Q3.
    stop_period = 0
    if settings.get("auto_stop_q3"):
        stop_period = 3
    elif settings.get("auto_stop_q4"):
        stop_period = 4
    if stop_period:
        game = games.get(mkt.get("game_id", ""), {})
        espn_gid = game.get("nba_game_id")
        if espn_gid:
            info = nba_feed.get_game_info(str(espn_gid))
            period = info.get("period", 0)
            phase = info.get("phase", "")
            if period >= stop_period and phase not in ("PRE", "FINAL", ""):
                stop_label = f"Q{stop_period}_STOPPED"
                stop_reason = f"auto_stop_q{stop_period}"
                order_id = mkt.get("order_id")
                if order_id and not str(order_id).startswith("paper_"):
                    mkt["status"] = stop_label
                    mkt["last_order_time"] = time.time()
                    add_log(f"Q{stop_period}_AUTO_STOP", ticker, f"period {period}, auto-stopped")
                    return {"type": "cancel", "order_id": order_id, "reason": stop_reason}
                mkt["status"] = stop_label
                return None

    player_name = mkt.get("player", "")

    # Per-game, per-prop-type settings
    game = games.get(mkt.get("game_id", ""), {})
    prop_type_for_settings = mkt.get("prop_type", "points")
    game_settings = _get_prop_settings(game, prop_type_for_settings)
    order_size = mkt.get("size", game_settings.get("size", DEFAULT_SIZE))

    # Phase-aware trading — primary mechanism when phase_settings_enabled is True.
    # Supersedes the legacy inplay_* flags to avoid double-applying adjustments.
    phase_active = None
    arb_pct_phase_delta = 0
    if settings.get("phase_settings_enabled", True):
        phase_active = _get_phase_for_game(game)
        # Per-game phase transition log: fires ONCE per transition per game so
        # you can see exactly when the engine switched phases and what
        # adjustment is now active. Log is at game level, not per-market, to
        # avoid flooding.
        last_phase = game.get("_current_phase")
        if last_phase != phase_active:
            cfg_preview = _get_phase_config(phase_active)
            log.info(
                f"PHASE [{game.get('game_name', game.get('game_id', '?'))}] "
                f"{last_phase or 'INIT'} → {phase_active}  "
                f"(enabled={cfg_preview.get('enabled', True)} "
                f"size_pct={cfg_preview.get('size_pct', 100)} "
                f"arb_delta={cfg_preview.get('arb_pct_delta', 0)})"
            )
            game["_current_phase"] = phase_active
        cfg = _get_phase_config(phase_active)
        effective_phase = phase_active

        # OFF-COURT OVERRIDE
        # If the player for this market is explicitly on the bench
        # (on_court === False — NOT None=unknown), apply the TIMEOUT phase
        # config regardless of actual game phase. Rationale: a benched
        # player can't accrue stats until they sub in, so from a trading
        # perspective it's structurally identical to a dead ball. Lets the
        # user tune one row (TIMEOUT) to govern both real timeouts AND
        # any off-court minutes — so setting LIVE=disabled + TIMEOUT=enabled
        # stops trading on-court players but keeps the off-court / bench
        # ones active.
        pid = mkt.get("_pid")
        nba_gid_fc = game.get("nba_game_id")
        # Resolve pid here if not already cached (first tick before the
        # snapshot API has populated mkt["_pid"]). Guarantees the off-court
        # check runs on every process_market call, not just post-snapshot.
        if pid is None and nba_gid_fc:
            player_full = (mkt.get("player", "") or "").strip()
            if player_full:
                pid = nba_feed.resolve_player_in_game(str(nba_gid_fc), player_full)
                if pid is not None:
                    mkt["_pid"] = pid
        if pid is not None and nba_gid_fc:
            oc_status = nba_feed.is_on_court(str(nba_gid_fc), pid)
            if oc_status is False:
                cfg = _get_phase_config("TIMEOUT")
                effective_phase = "OFF_COURT"

        # Per-market transition log — fires once when effective phase
        # changes. Only logs the OFF_COURT boundary (regular phase flips
        # are logged at game level above).
        _last_eff = mkt.get("_last_effective_phase")
        if _last_eff != effective_phase and ("OFF_COURT" in (str(_last_eff), effective_phase)):
            log.info(
                f"OFF_COURT [{ticker}] {_last_eff or 'INIT'} → {effective_phase}  "
                f"(enabled={cfg.get('enabled', True)} "
                f"size_pct={cfg.get('size_pct', 100)} "
                f"arb_delta={cfg.get('arb_pct_delta', 0)})"
            )
        mkt["_last_effective_phase"] = effective_phase

        if not cfg.get("enabled", True):
            # Phase trading is off — cancel any resting order and stand down.
            oid = mkt.get("order_id")
            status_label = f"PHASE_OFF_{effective_phase}"
            if mkt.get("status") != status_label:
                mkt["status"] = status_label
                add_log("PHASE_OFF", ticker, f"trading disabled for phase {effective_phase}")
            if oid and not str(oid).startswith("paper_"):
                return {"type": "cancel", "order_id": oid, "reason": f"phase_off_{effective_phase}"}
            return None
        size_pct_phase = int(cfg.get("size_pct", 100) or 100)
        if size_pct_phase != 100:
            order_size = max(1, int(order_size * size_pct_phase / 100))
        arb_pct_phase_delta = int(cfg.get("arb_pct_delta", 0) or 0)
        mkt["_phase"] = phase_active
        mkt["_effective_phase"] = effective_phase

    # Legacy in-play adjusters — only active when phase_settings is OFF
    in_play = (phase_active is None) and _is_inplay(mkt) and settings.get("inplay_enabled", True)
    ceiling_adj = 0
    refill_delay = 0
    if in_play:
        size_pct = settings.get("inplay_size_pct", 50)
        order_size = max(1, int(order_size * size_pct / 100))
        ceiling_adj = settings.get("inplay_ceiling_adj", -2)
        refill_delay = settings.get("inplay_refill_delay_secs", 5)
        mkt["status_inplay"] = True
    else:
        mkt["status_inplay"] = False

    # Refill delay
    if refill_delay > 0:
        last_fill_ts = mkt.get("last_fill_ts", 0)
        if last_fill_ts and (time.time() - last_fill_ts) < refill_delay and not mkt.get("order_id"):
            mkt["status"] = "REFILL_WAIT"
            return None

    # Fill target check
    fill_target = mkt.get("fill_target", game_settings.get("fill_target", DEFAULT_FILL_TARGET))
    position_no = mkt.get("position_no", 0)
    if position_no >= fill_target:
        oid = mkt.get("order_id")
        if oid and not str(oid).startswith("paper_"):
            if mkt["status"] != "FILLED":
                mkt["status"] = "FILLED"
                add_log("FILLED", ticker, f"position={position_no} >= target={fill_target}")
            return {"type": "cancel", "order_id": oid, "reason": "filled"}
        if mkt["status"] != "FILLED":
            mkt["status"] = "FILLED"
            add_log("FILLED", ticker, f"position={position_no} >= target={fill_target}")
        return None

    # Distance-tier sizing — cap per-order size by stat distance from line.
    # Pulls/skips when player is within kill_thresh of the line (NO is structurally
    # toast — one stat event resolves it), tiers the cap below t2/t3 thresholds.
    if settings.get("dist_tier_enabled", True):
        nba_gid_dt = game.get("nba_game_id")
        line_val_dt = mkt.get("line")
        live_stat_dt = None
        if nba_gid_dt and line_val_dt is not None:
            try:
                live_stat_dt = nba_feed.get_player_stat(
                    mkt.get("player", ""),
                    mkt.get("prop_type", "points"),
                    str(nba_gid_dt),
                )
            except Exception:
                live_stat_dt = None
        if live_stat_dt is not None and line_val_dt is not None:
            distance = float(line_val_dt) - float(live_stat_dt)
            tiers = settings.get("dist_tier_caps", {})
            kill_t = float(tiers.get("kill_thresh", 1.0))
            t2 = float(tiers.get("t2_thresh", 2.0))
            t3 = float(tiers.get("t3_thresh", 4.0))
            cap_t2 = int(tiers.get("cap_t2", 25))
            cap_t3 = int(tiers.get("cap_t3", 100))
            if distance <= kill_t:
                oid = mkt.get("order_id")
                if mkt.get("status") != "DIST_KILL":
                    mkt["status"] = "DIST_KILL"
                    add_log("DIST_KILL", ticker,
                            f"{mkt.get('player')} stat={live_stat_dt} line={line_val_dt} dist={distance:.1f} ≤ {kill_t}",
                            "no")
                if oid and not str(oid).startswith("paper_"):
                    return {"type": "cancel", "order_id": oid, "reason": "dist_kill"}
                return None
            if distance <= t2:
                order_size = min(order_size, cap_t2)
            elif distance <= t3:
                order_size = min(order_size, cap_t3)
            if mkt.get("status") == "DIST_KILL":
                mkt["status"] = "JUMPING"

    # Player-level max check
    player_name = (mkt.get("player") or "").lower()
    player_maxes = game.get("player_maxes", {})
    player_max = player_maxes.get(player_name, game_settings.get("player_max", DEFAULT_PLAYER_MAX))
    if player_max > 0 and player_name:
        player_total = 0
        for _tk, _m in game.get("markets", {}).items():
            if (_m.get("player") or "").lower() == player_name:
                player_total += _m.get("position_no", 0)
        if player_total >= player_max:
            oid = mkt.get("order_id")
            if oid and not str(oid).startswith("paper_"):
                if mkt["status"] != "PLAYER_MAX":
                    mkt["status"] = "PLAYER_MAX"
                    add_log("PLAYER_MAX", ticker, f"{mkt.get('player')} total={player_total} >= max={player_max}")
                return {"type": "cancel", "order_id": oid, "reason": "player_max"}
            if mkt["status"] != "PLAYER_MAX":
                mkt["status"] = "PLAYER_MAX"
                add_log("PLAYER_MAX", ticker, f"{mkt.get('player')} total={player_total} >= max={player_max}")
            return None
        if mkt.get("status") == "PLAYER_MAX":
            mkt["status"] = "JUMPING"

    # Refresh ceiling
    prop_type = mkt.get("prop_type", "points")
    game_arb = game_settings.get("arb_pct", settings.get("arb_pct", 0)) + arb_pct_phase_delta
    combined_offset = mkt.get("offset", 0) + ceiling_adj
    _player_odds_key = ""
    _detail = _find_detail(mkt["player"], mkt.get("line"), prop_type)
    if _detail:
        _fd = _detail.get("fanduel", {})
        _dk = _detail.get("draftkings", {})
        # Include fair_yes in key so cache busts when devig recomputes
        # (e.g., has_under arrives late and flips power-one-way → two-way-norm).
        # Odds-only key let ceil go stale when fair_yes moved underneath.
        _player_odds_key = (
            f"{_fd.get('odds')}:{_dk.get('odds')}:"
            f"{_fd.get('fair_yes')}:{_dk.get('fair_yes')}"
        )
    _ceil_key = f"{game_arb}:{combined_offset}:{_player_odds_key}"
    if mkt.get("_ceil_cache_key") == _ceil_key and mkt.get("ceil_no", 0) > 0:
        ceil = mkt["ceil_no"]
    else:
        ceil = compute_ceiling(mkt["player"], combined_offset, arb_pct=game_arb, market_line=mkt.get("line"), prop_type=prop_type)
        mkt["_ceil_cache_key"] = _ceil_key
    if ceil is not None:
        mkt["ceil_no"] = ceil
        if mkt.get("status") == "NO_ODDS":
            mkt["status"] = "JUMPING"
            add_log("ODDS_BACK", ticker, f"odds restored, resuming", "no")
    elif ceil is None:
        mkt["ceil_no"] = 0
        oid = mkt.get("order_id")
        cancel_oid = None
        if oid and not str(oid).startswith("paper_"):
            cancel_oid = oid
            add_log("CEIL_GONE", ticker, "ceiling returned None — order pulled", "no")
        dk_w = settings.get("dk_weight", 50)
        blended = get_blended_implied(mkt["player"], dk_w, market_line=mkt.get("line"), prop_type=prop_type)
        if blended is None:
            if mkt.get("status") != "NO_ODDS":
                mkt["status"] = "NO_ODDS"
                add_log("NO_ODDS", ticker, "all books dropped this player")
        else:
            mkt["status"] = "WAITING"
        return {"type": "cancel", "order_id": cancel_oid, "reason": "ceil_gone"} if cancel_oid else None

    ceil = mkt.get("ceil_no", 0)
    if ceil <= 0:
        oid = mkt.get("order_id")
        if oid and not str(oid).startswith("paper_"):
            add_log("PULL", ticker, f"ceil={ceil} <= 0, order pulled", "no")
            mkt["status"] = "WAITING"
            return {"type": "cancel", "order_id": oid, "reason": "ceil_zero"}
        mkt["status"] = "WAITING"
        return None

    # Cooldown check
    now = time.time()
    last_order_time = mkt.get("last_order_time", 0)
    cooldown = settings.get("cooldown_secs", 3)
    if now - last_order_time < cooldown:
        return None

    # Compute best bid
    my_price = mkt.get("order_price")
    my_size = mkt.get("_order_remaining", 0) or 0
    ob_no = mkt.get("orderbook", {}).get("no", {})
    best_bid = compute_best_bid(ob_no, exclude_price=my_price, exclude_size=my_size)
    mkt["best_bid_no"] = best_bid

    best_yes_bid = mkt.get("best_bid_yes", 0)
    maker_max = (99 - best_yes_bid) if best_yes_bid > 0 else 99

    # --- Withdraw guard: too much size resting above ceil → cancel and idle ---
    withdraw_threshold = settings.get("withdraw_above_ceil_threshold", 0)
    if withdraw_threshold > 0:
        qty_above_ceil = sum(q for p, q in ob_no.items() if p > ceil)
        if qty_above_ceil > withdraw_threshold:
            oid = mkt.get("order_id")
            if oid and not str(oid).startswith("paper_"):
                add_log("WITHDRAW", ticker,
                        f"qty_above_ceil={qty_above_ceil} > {withdraw_threshold}",
                        "no", my_price or 0, 0)
                mkt["last_order_time"] = time.time()
                mkt["status"] = "WAITING"
                return {"type": "cancel", "order_id": oid, "reason": "withdraw_above_ceil"}
            if oid and str(oid).startswith("paper_"):
                mkt["order_id"] = None
                mkt["order_price"] = None
                add_log("WITHDRAW", ticker,
                        f"qty_above_ceil={qty_above_ceil} > {withdraw_threshold} (paper)",
                        "no", 0, 0)
            mkt["status"] = "WAITING"
            return None

    # === WE HAVE AN ORDER ===
    order_id = mkt.get("order_id")
    if order_id:
        # Paper orders
        if str(order_id).startswith("paper_"):
            if best_bid > (my_price or 0) and best_bid < ceil:
                target = min(best_bid + 1, ceil)
                target = max(1, min(99, target))
                mkt["order_id"] = "paper_" + str(int(time.time() * 1000))
                mkt["order_price"] = target
                mkt["status"] = "JUMPING"
                add_log("PAPER_JUMP", ticker, f"jump to {target}c (bid={best_bid})", "no", target, order_size)
            elif best_bid >= ceil:
                mkt["order_id"] = None
                mkt["order_price"] = None
                mkt["status"] = "JUMPING"
                add_log("PAPER_PULL", ticker, f"bid={best_bid} >= ceil={ceil}", "no", 0, 0)
            else:
                mkt["status"] = "JUMPING"
            return None

        # PULL: order above ceiling (odds changed)
        if my_price and my_price > ceil:
            add_log("PULL", ticker, f"order={my_price} > ceil={ceil}, odds changed", "no", my_price, 0)
            mkt["last_order_time"] = time.time()
            mkt["status"] = "WAITING"
            return {"type": "cancel", "order_id": order_id, "reason": "above_ceil"}

        # PULL: best bid above ceiling
        join_ceil = settings.get("join_ceil", False)
        if best_bid > ceil:
            add_log("PULL", ticker, f"best_bid={best_bid} > ceil={ceil}", "no", my_price or 0, 0)
            mkt["status"] = "WAITING"
            return {"type": "cancel", "order_id": order_id, "reason": "bid_above_ceil"}
        if best_bid == ceil and not join_ceil:
            add_log("PULL", ticker, f"best_bid={best_bid} >= ceil={ceil}", "no", my_price or 0, 0)
            mkt["status"] = "WAITING"
            return {"type": "cancel", "order_id": order_id, "reason": "bid_at_ceil"}

        # Compute floor
        floor = max(1, ceil - settings["floor_offset"]) if settings["floor_enabled"] else 1

        # STEPDOWN
        if settings["stepdown_enabled"] and best_bid > 0 and best_bid < (my_price or 0) - 1:
            target = best_bid + 1
            if target > ceil:
                target = ceil
            if target < floor:
                target = floor
            target = max(1, min(99, target))
            if target == my_price:
                mkt["stepdown_since"] = 0
                mkt["status"] = "JUMPING"
                return None
            if mkt["stepdown_since"] == 0:
                mkt["stepdown_since"] = time.time()
            elapsed = time.time() - mkt["stepdown_since"]
            if elapsed >= settings["stepdown_delay_secs"]:
                add_log("STEPDOWN", ticker, f"from {my_price}c to {target}c (bid={best_bid}, floor={floor})", "no", target, order_size)
                mkt["last_order_time"] = time.time()
                mkt["stepdown_since"] = 0
                mkt["last_stepdown_at"] = time.time()
                return {"type": "cancel_and_place", "cancel_id": order_id, "place_ticker": ticker,
                        "side": "no", "price": target, "size": order_size}
            mkt["status"] = "JUMPING"
            return None
        else:
            mkt["stepdown_since"] = 0

        # JUMP: best bid > our price
        if best_bid > (my_price or 0):
            last_stepdown = mkt.get("last_stepdown_at", 0)
            post_step_cd = settings.get("post_stepdown_jump_cooldown_secs", 10)
            if last_stepdown and (time.time() - last_stepdown) < post_step_cd:
                mkt["status"] = "JUMPING"
                return None
            target = min(best_bid + 1, ceil, maker_max)
            if target < floor:
                target = floor
            target = max(1, min(99, target))
            if target == my_price:
                mkt["status"] = "JUMPING"
                return None
            if target > maker_max:
                mkt["status"] = "JUMPING"
                return None
            add_log("JUMP", ticker, f"from {my_price}c to {target}c (bid={best_bid})", "no", target, order_size)
            mkt["last_order_time"] = time.time()
            return {"type": "cancel_and_place", "cancel_id": order_id, "place_ticker": ticker,
                    "side": "no", "price": target, "size": order_size}

        # Top of book
        mkt["status"] = "JUMPING"
        return None

    # === NO ORDER — place one ===
    join_ceil = settings.get("join_ceil", False)
    if best_bid > ceil:
        mkt["status"] = "WAITING"
        return None
    if best_bid == ceil and not join_ceil:
        mkt["status"] = "WAITING"
        return None

    floor = max(1, ceil - settings["floor_offset"]) if settings["floor_enabled"] else 1
    if best_bid > 0:
        target = min(best_bid + 1, ceil, maker_max)
    else:
        target = min(ceil, maker_max)
    if target < floor:
        target = floor
    target = max(1, min(99, target))

    if target > maker_max:
        mkt["status"] = "WAITING"
        return None

    if target < 3:
        mkt["status"] = "WAITING"
        return None

    if paper_mode:
        mkt["order_id"] = "paper_" + str(int(time.time() * 1000))
        mkt["order_price"] = target
        mkt["status"] = "JUMPING"
        mkt["last_order_time"] = time.time()
        add_log("WOULD_PLACE", ticker, f"paper NO @{target}c", "no", target, order_size)
        return None

    mkt["last_order_time"] = time.time()
    return {"type": "place", "place_ticker": ticker, "side": "no", "price": target, "size": order_size}


def _execute_market_action(ticker, mkt, action):
    """Execute HTTP calls OUTSIDE games_lock, then re-acquire to update state."""
    atype = action["type"]

    if atype == "cancel":
        oid = action.get("order_id")
        if oid:
            try:
                kalshi.cancel_order(oid)
            except Exception as e:
                log.warning(f"Cancel failed {ticker}: {e}")
        with games_lock:
            # Clear tracking after cancel attempt (order is gone or will be caught by reconcile)
            if mkt.get("order_id") == oid or (oid and mkt.get("order_id") is None):
                mkt["order_id"] = None
                mkt["order_price"] = None

    elif atype == "place":
        order = kalshi.place_order(action["place_ticker"], action["side"], action["price"], action["size"])
        with games_lock:
            if order:
                mkt["order_id"] = order["order_id"]
                mkt["order_price"] = action["price"]
                mkt["_last_known_price"] = action["price"]
                mkt["_last_order_size"] = action["size"]
                mkt["_order_remaining"] = action["size"]
                mkt["_place_fails"] = 0
                mkt["status"] = "JUMPING"
                add_log("PLACED", ticker, "", "no", action["price"], action["size"])
            else:
                fails = mkt.get("_place_fails", 0) + 1
                mkt["_place_fails"] = fails
                if fails >= 5:
                    mkt["order_id"] = None
                    mkt["order_price"] = None
                    mkt["_place_backoff_until"] = time.time() + 60
                    mkt["_place_fails"] = 0
                    mkt["status"] = "PLACE_GIVE_UP"
                    add_log("PLACE_GIVE_UP", ticker, f"after {fails} failures at {action['price']}c — resync on next heartbeat", "no", action["price"], action["size"])
                else:
                    backoff = min(60, 5 * (2 ** (fails - 1)))
                    mkt["_place_backoff_until"] = time.time() + backoff
                    mkt["status"] = "WAITING"
                    if fails <= 3:
                        add_log("PLACE_FAILED", ticker, f"target={action['price']}c (retry in {backoff}s)", "no", action["price"], action["size"])

    elif atype == "cancel_and_place":
        # Cancel first
        cancel_ok = True
        try:
            if not kalshi.cancel_order(action["cancel_id"]):
                cancel_ok = False
                # Verify order is actually gone
                resting = kalshi.get_resting_orders(action["place_ticker"])
                if any(o.get("order_id") == action["cancel_id"] for o in resting):
                    # Order still resting — don't clear state, don't place
                    with games_lock:
                        mkt["last_order_time"] = time.time()
                    return
                cancel_ok = True  # order gone despite cancel returning false
        except Exception as e:
            log.warning(f"Cancel in jump failed {ticker}: {e}")
            cancel_ok = False

        if not cancel_ok:
            with games_lock:
                mkt["last_order_time"] = time.time()
            return

        # Cancel succeeded — clear old order state BEFORE placing new one
        with games_lock:
            mkt["order_id"] = None
            mkt["order_price"] = None

        # Place new order
        order = kalshi.place_order(action["place_ticker"], action["side"], action["price"], action["size"])
        with games_lock:
            if order:
                mkt["order_id"] = order["order_id"]
                mkt["order_price"] = action["price"]
                mkt["_last_known_price"] = action["price"]
                mkt["_last_order_size"] = action["size"]
                mkt["_order_remaining"] = action["size"]
                mkt["_place_fails"] = 0
                mkt["status"] = "JUMPING"
                add_log("PLACED", ticker, f"jumped to {action['price']}c", "no", action["price"], action["size"])
            else:
                mkt["order_id"] = None
                mkt["order_price"] = None
                mkt["status"] = "WAITING"
                add_log("PLACE_FAILED", ticker, f"target={action['price']}c", "no", action["price"], action["size"])


def process_market(ticker, mkt):
    """Legacy wrapper — calls compute + execute. For use by non-heartbeat callers.
    MUST be called with games_lock held (will release for HTTP)."""
    action = compute_market_action(ticker, mkt)
    if action:
        # Temporarily release lock for HTTP
        games_lock.release()
        try:
            _execute_market_action(ticker, mkt, action)
        finally:
            games_lock.acquire()


# ---------------------------------------------------------------------------
# Bond detection — YES bid >= 99 (prop hit via orderbook)
# ---------------------------------------------------------------------------

def _check_bond(ticker, mkt):
    """Check if YES bid >= 99 — indicates prop hit.
    Uses 2-consecutive-hit confirmation like JumpBot.
    """
    if mkt.get("prop_done"):
        return

    best_yes = mkt.get("best_bid_yes", 0)

    if best_yes >= 99:
        mkt["_mention_count"] = mkt.get("_mention_count", 0) + 1
        count = mkt["_mention_count"]

        if count == 1:
            add_log("MENTIONED", ticker, f"YES bid={best_yes} — auto-stopping")
            mkt["active"] = False
            if mkt.get("order_id"):
                if not str(mkt["order_id"]).startswith("paper_"):
                    kalshi.cancel_order(mkt["order_id"])
                mkt["order_id"] = None
                mkt["order_price"] = None
            mkt["status"] = "MENTIONED"

        elif count >= 2 and not mkt.get("_bonded"):
            mkt["_bonded"] = True
            mkt["prop_done"] = True
            mkt["active"] = False
            if mkt.get("order_id"):
                if not str(mkt["order_id"]).startswith("paper_"):
                    kalshi.cancel_order(mkt["order_id"])
                mkt["order_id"] = None
                mkt["order_price"] = None
            mkt["status"] = "PROP_HIT"
            add_log("BONDED", ticker, f"YES bid={best_yes} — prop confirmed, market killed")
    else:
        if mkt.get("_mention_count", 0) > 0 or mkt.get("_bonded"):
            was_bonded = mkt.get("_bonded", False)
            mkt["_mention_count"] = 0
            mkt["_bonded"] = False
            if not mkt.get("prop_done"):
                if was_bonded:
                    mkt["prop_done"] = False
                mkt["status"] = "OFF"
                add_log("UNBONDED", ticker, f"YES bid={best_yes} — misbond, price dropped")


# ---------------------------------------------------------------------------
# Position tracking
# ---------------------------------------------------------------------------

FILLS_URL = os.environ.get("FILLS_URL", "")
FILLS_API_KEY = os.environ.get("FILLS_API_KEY", "homer123")
# New dedicated NBA fill tracker. If set, fills go here and stop going to FILLS_URL.
NBA_DASHBOARD_URL = os.environ.get("NBA_DASHBOARD_URL", "")
NBA_DASHBOARD_API_KEY = os.environ.get("NBA_DASHBOARD_API_KEY", FILLS_API_KEY)


def _normalize_url(u):
    """Auto-prepend https:// if missing. Prevents silent POST failures when
    env var is set to a bare domain (a real bug we hit — cost a session of fills)."""
    if not u:
        return u
    if not u.startswith(("http://", "https://")):
        return "https://" + u
    return u.rstrip("/")


def _fills_target():
    """Returns (url, api_key) for fill POST/PATCH. Prefers NBA dashboard if configured."""
    if NBA_DASHBOARD_URL:
        return _normalize_url(NBA_DASHBOARD_URL), NBA_DASHBOARD_API_KEY
    return _normalize_url(FILLS_URL), FILLS_API_KEY


def _compute_fair_no(implied_yes, odds=None):
    """Devig implied YES to get fair NO using dynamic power method."""
    if not implied_yes or implied_yes <= 0:
        return None, None
    raw = implied_yes / 100
    exp = get_devig_exp(raw)
    fair_yes = raw ** exp * 100
    fair_no = 100 - fair_yes
    return fair_yes, fair_no


# Local fill log with CLV + SQLite persistence
fill_log = []
fill_log_lock = threading.Lock()
MAX_FILLS = 200
FILLS_DB = os.environ.get("FILLS_DB", "/data/nba_fills_v2.db" if os.path.isdir("/data") else "nba_fills_v2.db")


def _init_fills_db():
    conn = sqlite3.connect(FILLS_DB)
    conn.execute("""CREATE TABLE IF NOT EXISTS fills (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts TEXT, ticker TEXT, player TEXT, line REAL,
        contracts INTEGER, fill_price INTEGER, ceil_no INTEGER,
        fair_no_at_fill REAL, ev_at_fill REAL, ceil_ev REAL, has_under INTEGER,
        fair_no_10s REAL, clv_10s REAL, move_10s REAL,
        fair_no_60s REAL, clv_60s REAL, move_60s REAL,
        fills_id INTEGER
    )""")
    # Migration for existing DBs
    for col in ["ceil_no INTEGER", "ceil_ev REAL", "devig_method TEXT", "devig_exponent REAL",
                "prop_type TEXT", "game_id TEXT",
                "fd_under_odds INTEGER", "dk_under_odds INTEGER",
                "fd_fair_no REAL", "dk_fair_no REAL"]:
        try:
            conn.execute(f"ALTER TABLE fills ADD COLUMN {col}")
        except Exception:
            pass
    conn.commit()
    conn.close()


def _db_insert_fill(fill):
    try:
        conn = sqlite3.connect(FILLS_DB)
        conn.execute("""INSERT INTO fills
            (ts, ticker, player, line, contracts, fill_price, ceil_no,
             fair_no_at_fill, ev_at_fill, ceil_ev, has_under,
             devig_method, devig_exponent, prop_type, game_id,
             fd_under_odds, dk_under_odds, fd_fair_no, dk_fair_no)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (fill.get("ts_display"), fill.get("ticker"), fill.get("player"),
             fill.get("line"), fill.get("contracts"), fill.get("fill_price"),
             fill.get("ceil_no"),
             fill.get("fair_no_at_fill"), fill.get("ev_at_fill"),
             fill.get("ceil_ev"),
             1 if fill.get("has_under") else 0,
             fill.get("devig_method"), fill.get("devig_exponent"),
             fill.get("prop_type", "points"), fill.get("game_id", ""),
             fill.get("fd_under_odds"), fill.get("dk_under_odds"),
             fill.get("fair_no_fd"), fill.get("fair_no_dk")))
        fill["_db_id"] = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        conn.commit()
        conn.close()
    except Exception as e:
        log.error(f"db insert fill: {e}")


def _db_update_clv(fill, prefix):
    db_id = fill.get("_db_id")
    if not db_id:
        return
    try:
        conn = sqlite3.connect(FILLS_DB)
        conn.execute(f"""UPDATE fills SET fair_no_{prefix}=?, clv_{prefix}=?, move_{prefix}=?
            WHERE id=?""",
            (fill.get(f"fair_no_{prefix}"), fill.get(f"clv_{prefix}"),
             fill.get(f"move_{prefix}"), db_id))
        conn.commit()
        conn.close()
    except Exception as e:
        log.error(f"db update clv {prefix}: {e}")


def _db_load_fills():
    """Load recent fills from SQLite on boot."""
    try:
        conn = sqlite3.connect(FILLS_DB)
        conn.row_factory = sqlite3.Row
        rows = conn.execute("SELECT * FROM fills ORDER BY id DESC LIMIT ?", (MAX_FILLS,)).fetchall()
        conn.close()
        fills = []
        for r in reversed(rows):
            fills.append({
                "_db_id": r["id"],
                "ts_display": r["ts"] or "",
                "ticker": r["ticker"],
                "player": r["player"],
                "line": r["line"],
                "contracts": r["contracts"],
                "fill_price": r["fill_price"],
                "ceil_no": r["ceil_no"] if "ceil_no" in r.keys() else 0,
                "fair_no_at_fill": r["fair_no_at_fill"],
                "ev_at_fill": r["ev_at_fill"] or 0,
                "ceil_ev": r["ceil_ev"] if "ceil_ev" in r.keys() else 0,
                "has_under": bool(r["has_under"]),
                "fair_no_10s": r["fair_no_10s"],
                "clv_10s": r["clv_10s"],
                "move_10s": r["move_10s"],
                "fair_no_60s": r["fair_no_60s"],
                "clv_60s": r["clv_60s"],
                "move_60s": r["move_60s"],
                "fd_under_odds": r["fd_under_odds"] if "fd_under_odds" in r.keys() else None,
                "dk_under_odds": r["dk_under_odds"] if "dk_under_odds" in r.keys() else None,
                "fair_no_fd": r["fd_fair_no"] if "fd_fair_no" in r.keys() else None,
                "fair_no_dk": r["dk_fair_no"] if "dk_fair_no" in r.keys() else None,
                "_player": r["player"],
                "_line": r["line"],
                "_fills_id": r["fills_id"],
            })
        return fills
    except Exception as e:
        log.error(f"db load fills: {e}")
        return []


_init_fills_db()


def _get_best_fair_no_now(player, line):
    """Get the best available fair NO right now. Tries every source."""
    detail = _find_detail(player, line)
    # 1) FD 2-way (most accurate)
    fd = detail.get("fanduel", {})
    if fd.get("has_under") and fd.get("fair_no"):
        return fd["fair_no"]
    # 2) DK 2-way
    dk = detail.get("draftkings", {})
    if dk.get("has_under") and dk.get("fair_no"):
        return dk["fair_no"]
    # 3) Any book with 2-way
    for book_key, book_data in detail.items():
        if book_data.get("has_under") and book_data.get("fair_no"):
            return book_data["fair_no"]
    # 4) Blended (may use power devig)
    imp = get_blended_implied(player, settings.get("dk_weight", 50), market_line=line)
    if imp:
        return round(100 - imp, 2)
    # 5) Any book with odds at all
    for book_key, book_data in detail.items():
        val, _, _ = _devig_implied_yes(book_data)
        if val:
            return round(100 - val, 2)
    return 0


def _get_per_book_fair_no_now(player, line, prop_type="points"):
    """Per-book + blended fair_no snapshot at the current moment.
    Returns dict with fd, dk, blended keys (any may be None).
    Uses the same devig path as fill recording so CLV math is apples-to-apples.
    """
    detail = _find_detail(player, line, prop_type)
    fd = detail.get("fanduel", {})
    dk = detail.get("draftkings", {})
    fd_val, _, _ = _devig_implied_yes(fd)
    dk_val, _, _ = _devig_implied_yes(dk)
    fd_fair = round(100 - fd_val, 2) if fd_val else None
    dk_fair = round(100 - dk_val, 2) if dk_val else None
    blended = None
    imp = get_blended_implied(player, settings.get("dk_weight", 50),
                              market_line=line, prop_type=prop_type)
    if imp:
        blended = round(100 - imp, 2)
    return {"fd": fd_fair, "dk": dk_fair, "blended": blended}


def _clv_check(fill, ticker, delay_secs, field_prefix):
    """Wait N seconds, re-read per-book + blended fair_no, compute CLV.
    Stores per-book fair, per-book EV, blended EV, CLV, move on the fill.
    Retries once on failure.
    """
    time.sleep(delay_secs)
    pt = fill.get("prop_type", "points")
    snap = _get_per_book_fair_no_now(fill["_player"], fill["_line"], prop_type=pt)
    if snap["blended"] is None or snap["blended"] <= 0:
        time.sleep(2)
        snap = _get_per_book_fair_no_now(fill["_player"], fill["_line"], prop_type=pt)

    blended = snap["blended"]
    if blended and blended > 0:
        fp = fill["fill_price"]
        fd_fair = snap["fd"]
        dk_fair = snap["dk"]
        # EV = (fair_no - fill_price) / fill_price * 100 — same formula as at fill
        def _ev(fair):
            return round((fair - fp) / fp * 100, 2) if (fair and fp) else None
        blended_ev = _ev(blended)
        fd_ev = _ev(fd_fair)
        dk_ev = _ev(dk_fair)
        clv = round(blended - fp, 2)
        move = round(blended - fill["fair_no_at_fill"], 2)
        with fill_log_lock:
            # Keep the legacy single-value keys populated for the local UI
            fill[f"fair_no_{field_prefix}"] = round(blended, 2)
            fill[f"clv_{field_prefix}"] = clv
            fill[f"move_{field_prefix}"] = move
            # Per-book enrichment for nba_dashboard
            fill[f"fd_fair_no_{field_prefix}"] = fd_fair
            fill[f"dk_fair_no_{field_prefix}"] = dk_fair
            fill[f"blended_fair_no_{field_prefix}"] = round(blended, 2)
            fill[f"fd_ev_{field_prefix}"] = fd_ev
            fill[f"dk_ev_{field_prefix}"] = dk_ev
            fill[f"blended_ev_{field_prefix}"] = blended_ev
        direction = "+" if move >= 0 else ""
        add_log(f"CLV_{field_prefix}", ticker,
                f"fair_no {fill['fair_no_at_fill']:.1f}→{blended:.1f} (move={direction}{move:.1f}, clv={clv:.1f})")
        _db_update_clv(fill, field_prefix)
        _send_clv_update(fill, field_prefix, blended, fd_fair, dk_fair,
                         blended_ev, fd_ev, dk_ev, clv, move)
    else:
        with fill_log_lock:
            fill[f"fair_no_{field_prefix}"] = None
            fill[f"clv_{field_prefix}"] = None
            fill[f"move_{field_prefix}"] = None
        add_log(f"CLV_{field_prefix}_MISS", ticker,
                f"no odds data after retry for {fill['_player']}")


def _send_clv_update(fill, prefix, blended_fair, fd_fair, dk_fair,
                     blended_ev, fd_ev, dk_ev, clv, move):
    """PATCH CLV data back to whichever fill tracker holds this fill."""
    url, api_key = _fills_target()
    if not url:
        return
    fill_id = fill.get("_fills_id")
    if not fill_id:
        return
    # nba_dashboard expects per-book keys; homer_fills only knows the legacy trio.
    if NBA_DASHBOARD_URL:
        payload = {
            f"blended_fair_no_{prefix}": blended_fair,
            f"fd_fair_no_{prefix}": fd_fair,
            f"dk_fair_no_{prefix}": dk_fair,
            f"blended_ev_{prefix}": blended_ev,
            f"fd_ev_{prefix}": fd_ev,
            f"dk_ev_{prefix}": dk_ev,
            f"clv_{prefix}": clv,
            f"move_{prefix}": move,
        }
    else:
        payload = {
            f"fair_no_{prefix}": blended_fair,
            f"clv_{prefix}": clv,
            f"move_{prefix}": move,
        }
    try:
        import requests as _req
        _req.patch(
            f"{url}/api/fill/{fill_id}/clv",
            json=payload,
            headers={"X-API-Key": api_key},
            timeout=5,
        )
    except Exception as e:
        log.error(f"CLV update send failed: {e}")


def _get_fill_fair_no(player, line, prop_type="points"):
    """Get fair NO for fill recording — uses BLENDED (same source as ceiling).
    Also returns per-book fair_no and the exponent actually applied per book.
    Returns (blended_fair_no, devig_method, devig_exp, fd_fair_no, dk_fair_no,
             fd_exp, dk_exp).
    """
    detail = _find_detail(player, line, prop_type)
    dk_w = settings.get("dk_weight", 50)

    # Per-book values (for reference)
    fd = detail.get("fanduel", {})
    dk = detail.get("draftkings", {})
    fd_val, fd_method, fd_exp = _devig_implied_yes(fd)
    dk_val, dk_method, dk_exp = _devig_implied_yes(dk)
    fd_fair_no = round(100 - fd_val, 2) if fd_val else None
    dk_fair_no = round(100 - dk_val, 2) if dk_val else None

    # PRIMARY: blended — same as compute_ceiling uses
    imp = get_blended_implied(player, dk_w, market_line=line, prop_type=prop_type)
    if imp:
        fair_no = round(100 - imp, 2)
        # Determine method from whichever book(s) contributed
        if fd_val and dk_val:
            method = fd_method if fd.get("has_under") else (dk_method if dk.get("has_under") else "power_one_way")
        else:
            method = fd_method or dk_method or "power_one_way"
        exp = fd_exp or dk_exp
        return fair_no, method, exp, fd_fair_no, dk_fair_no, fd_exp, dk_exp

    # Fallback: any book
    for book_key, book_data in detail.items():
        val, method, exp = _devig_implied_yes(book_data)
        if val:
            return round(100 - val, 2), method, exp, fd_fair_no, dk_fair_no, fd_exp, dk_exp
    return 0, None, None, fd_fair_no, dk_fair_no, fd_exp, dk_exp


def _record_local_fill(ticker, mkt, new_contracts, fill_price):
    """Record fill locally with 10s + 60s CLV checks."""
    player = mkt.get("player", "")
    line = mkt.get("line")
    pt = mkt.get("prop_type", "points")
    fair_no, devig_method, devig_exp, fd_fair_no, dk_fair_no, fd_exp_used, dk_exp_used = _get_fill_fair_no(player, line, prop_type=pt)

    # Devigged EV — true edge vs fair value
    ev_pct = round((fair_no - fill_price) / fill_price * 100, 2) if fair_no and fill_price else 0

    # Book implied NO — for arb reference
    detail = _find_detail(player, line, pt)
    fd = detail.get("fanduel", {})
    dk = detail.get("draftkings", {})
    fd_imp_no = round(100 - fd.get("implied_yes", 0), 2) if fd.get("implied_yes") else None
    dk_imp_no = round(100 - dk.get("implied_yes", 0), 2) if dk.get("implied_yes") else None
    # American odds for both sides — over is the primary OddsBlaze source,
    # under is only set when the book posts a 2-way line.
    fd_over_odds = fd.get("odds")
    dk_over_odds = dk.get("odds")
    fd_under_odds = fd.get("under_odds")
    dk_under_odds = dk.get("under_odds")
    # Raw implied YES (pre-devig) — lets us backtest exponents against the book
    fd_implied_yes_raw = fd.get("implied_yes")
    dk_implied_yes_raw = dk.get("implied_yes")
    # Min book implied NO (what arb mode ceiling is based on)
    book_nos = [x for x in [fd_imp_no, dk_imp_no] if x]
    book_no = min(book_nos) if book_nos else None

    # Arb EV — edge vs book's raw price (not devigged)
    arb_ev = round((book_no - fill_price) / fill_price * 100, 2) if book_no and fill_price else 0

    # Recompute ceiling at fill time
    game = games.get(mkt.get("game_id", ""), {})
    game_arb = _get_prop_settings(game, pt).get("arb_pct", settings.get("arb_pct", 0))
    ceil_no = compute_ceiling(player, mkt.get("offset", 0), arb_pct=game_arb, market_line=line, prop_type=pt)
    if ceil_no is None:
        ceil_no = mkt.get("ceil_no", 0)

    # Ceil EV — always use devigged fair_no (matches UI display)
    ceil_ev = round((fair_no - ceil_no) / ceil_no * 100, 2) if fair_no and ceil_no else 0

    # Game phase at fill — for "is live trading viable" analysis on the dashboard
    game_phase = None
    try:
        nba_gid = game.get("nba_game_id")
        if nba_gid:
            game_phase = nba_feed.get_game_phase(str(nba_gid))
    except Exception:
        pass

    fill = {
        "ts_display": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        "ticker": ticker,
        "player": player,
        "line": line,
        "contracts": new_contracts,
        "fill_price": fill_price,
        "ceil_no": ceil_no,
        "fair_no_at_fill": round(fair_no, 2),
        "fair_no_fd": fd_fair_no,
        "fair_no_dk": dk_fair_no,
        "book_no": book_no,
        "fd_imp_no": fd_imp_no,
        "dk_imp_no": dk_imp_no,
        "fd_over_odds": fd_over_odds,
        "dk_over_odds": dk_over_odds,
        "fd_under_odds": fd_under_odds,
        "dk_under_odds": dk_under_odds,
        "fd_implied_yes_raw": fd_implied_yes_raw,
        "dk_implied_yes_raw": dk_implied_yes_raw,
        "fd_exponent_used": fd_exp_used,
        "dk_exponent_used": dk_exp_used,
        "game_phase": game_phase,
        "ev_at_fill": ev_pct,
        "arb_ev": arb_ev,
        "ceil_ev": ceil_ev,
        "has_under": fd.get("has_under", False),
        "devig_method": devig_method,
        "devig_exponent": devig_exp,
        "prop_type": mkt.get("prop_type", "points"),
        "game_id": mkt.get("game_id", ""),
        "fair_no_10s": None, "clv_10s": None, "move_10s": None,
        "fair_no_60s": None, "clv_60s": None, "move_60s": None,
        "_player": player,
        "_line": line,
    }
    with fill_log_lock:
        fill_log.append(fill)
        if len(fill_log) > MAX_FILLS:
            del fill_log[:len(fill_log) - MAX_FILLS]

    # Persist to SQLite
    try:
        _db_insert_fill(fill)
    except Exception as e:
        log.error(f"Fill SQLite insert failed: {e}")

    # Schedule CLV checks
    try:
        threading.Thread(target=_clv_check, args=(fill, ticker, 10, "10s"), daemon=True).start()
        threading.Thread(target=_clv_check, args=(fill, ticker, 60, "60s"), daemon=True).start()
    except Exception as e:
        log.error(f"CLV check thread failed: {e}")
    return fill


def _send_fill(ticker, mkt, new_contracts, fill_price):
    """Record locally + send to fills tracker (nba_dashboard if configured, else homer_fills)."""
    try:
        local_fill = _record_local_fill(ticker, mkt, new_contracts, fill_price)
    except Exception as e:
        log.error(f"FILL_RECORD_ERROR {ticker}: {e}")
        import traceback; traceback.print_exc()
        local_fill = {}
    url, api_key = _fills_target()
    if not url:
        return
    try:
        player = mkt.get("player", "")
        game = games.get(mkt.get("game_id", ""), {})
        game_name = game.get("game_name", mkt.get("game_id", ""))
        ceil_no = mkt.get("ceil_no") or 0
        ceil_ev = local_fill.get("ceil_ev", 0)
        fair_no = local_fill.get("fair_no_at_fill", 0)
        fair_yes = round(100 - fair_no, 2) if fair_no else 0
        ev_pct = local_fill.get("ev_at_fill", 0)
        ev_dollars = ev_pct / 100 * new_contracts * fill_price / 100 if ev_pct else 0

        if NBA_DASHBOARD_URL:
            # Enriched payload — nba_dashboard schema
            payload = {
                "ts": datetime.now(timezone.utc).isoformat(),
                "player": player,
                "game": game_name,
                "game_id": mkt.get("game_id", ""),
                "ticker": ticker,
                "line": local_fill.get("line"),
                "prop_type": mkt.get("prop_type", "points"),
                "contracts": new_contracts,
                "fill_price": fill_price,
                "side": "no",
                "role": "Maker",
                "fd_over_odds": local_fill.get("fd_over_odds"),
                "dk_over_odds": local_fill.get("dk_over_odds"),
                "fd_implied_yes": local_fill.get("fd_implied_yes_raw"),
                "dk_implied_yes": local_fill.get("dk_implied_yes_raw"),
                "fd_fair_no": local_fill.get("fair_no_fd"),
                "dk_fair_no": local_fill.get("fair_no_dk"),
                "blended_fair_no": round(fair_no, 2) if fair_no else None,
                "fd_exponent_used": local_fill.get("fd_exponent_used"),
                "dk_exponent_used": local_fill.get("dk_exponent_used"),
                "devig_method": local_fill.get("devig_method"),
                "ev_at_fill": round(ev_pct, 2) if ev_pct else 0,
                "ceil_no": ceil_no,
                "ceil_ev": round(ceil_ev, 2) if ceil_ev else 0,
                "game_phase": local_fill.get("game_phase"),
            }
        else:
            # Legacy homer_fills payload (kept for deploys without NBA_DASHBOARD_URL set yet)
            book_odds = mkt.get("blended_odds") or mkt.get("odds_american")
            payload = {
                "ts": datetime.now(timezone.utc).isoformat(),
                "player": player,
                "game": game_name,
                "ticker": ticker,
                "contracts": new_contracts,
                "fill_price": fill_price,
                "side": "no",
                "role": "Maker",
                "book_odds": book_odds,
                "implied_yes": round(fair_yes, 2) if fair_yes else None,
                "fair_yes": round(fair_yes, 2) if fair_yes else None,
                "fair_no": round(fair_no, 2) if fair_no else None,
                "ev_pct": round(ev_pct, 2) if ev_pct else 0,
                "ev_dollars": round(ev_dollars, 2) if ev_dollars else 0,
                "ceil_no": ceil_no,
                "devig_method": local_fill.get("devig_method"),
                "devig_exponent": local_fill.get("devig_exponent"),
                "prop_type": mkt.get("prop_type", "points"),
                "fd_under_odds": local_fill.get("fd_under_odds"),
                "dk_under_odds": local_fill.get("dk_under_odds"),
                "fd_fair_no": local_fill.get("fair_no_fd"),
                "dk_fair_no": local_fill.get("fair_no_dk"),
            }

        import requests as _req
        resp = _req.post(
            f"{url}/api/fill",
            json=payload,
            headers={"X-API-Key": api_key},
            timeout=5,
        )
        if resp.ok and local_fill:
            rid = resp.json().get("id")
            if rid:
                local_fill["_fills_id"] = rid
                db_id = local_fill.get("_db_id")
                if db_id:
                    try:
                        conn = sqlite3.connect(FILLS_DB)
                        conn.execute("UPDATE fills SET fills_id=? WHERE id=?", (rid, db_id))
                        conn.commit()
                        conn.close()
                    except Exception:
                        pass
    except Exception as e:
        log.error(f"Fill send failed: {e}")


def _handle_ws_fill(data):
    """Handle a fill event from the Kalshi WS fill channel.
    Instant fill recording with no polling lag.

    Expected fields: ticker/market_ticker, order_id, side, yes_price, no_price,
    count, action, is_taker, created_time, trade_id.
    """
    try:
        ticker = data.get("market_ticker") or data.get("ticker", "")
        if ticker not in all_markets:
            return

        # Log ALL fill events for debugging (first 20)
        _handle_ws_fill._debug_count = getattr(_handle_ws_fill, '_debug_count', 0) + 1
        if _handle_ws_fill._debug_count <= 20:
            log.info(f"WS_FILL_RAW: {ticker} side={data.get('side')} action={data.get('action')} "
                     f"count={data.get('count')} no_price={data.get('no_price')} "
                     f"trade_id={str(data.get('trade_id', ''))[:8]}")

        # Only track NO-side buys (our strategy)
        side = data.get("side", "")
        action = data.get("action", "buy")
        if side != "no" or action != "buy":
            return

        # Deduplicate by trade_id
        trade_id = data.get("trade_id", "")
        with games_lock:
            mkt = all_markets.get(ticker)
            if not mkt:
                return
            seen_trades = mkt.setdefault("_seen_trade_ids", set())
            if trade_id and trade_id in seen_trades:
                return
            if trade_id:
                seen_trades.add(trade_id)
                # Prevent memory growth — keep only last 200 per market
                if len(seen_trades) > 200:
                    # Simple trim — clear half
                    mkt["_seen_trade_ids"] = set(list(seen_trades)[-100:])

            # Parse fill price (cents)
            no_price = data.get("no_price")
            if no_price is None:
                no_price = data.get("no_price_dollars")
            if no_price is None:
                no_price = data.get("price_dollars")
            fill_price = price_to_cents(no_price) if no_price is not None else (mkt.get("order_price") or 0)

            count = int(data.get("count", 0) or 0)
            if count <= 0 or fill_price <= 0:
                return

            # Update position count (WS is source of truth now)
            mkt["position_no"] = mkt.get("position_no", 0) + count
            # Rolling avg price
            old_count = mkt.get("position_no", 0) - count
            old_avg = mkt.get("position_avg_no", 0)
            if old_count > 0 and old_avg > 0:
                new_avg = round((old_avg * old_count + fill_price * count) / (old_count + count))
            else:
                new_avg = fill_price
            mkt["position_avg_no"] = new_avg
            mkt["_position_synced"] = True
            mkt["last_fill_ts"] = time.time()

            # Track remaining contracts on this order
            orig_size = mkt.get("_last_order_size", 0) or 0
            prev_remaining = mkt.get("_order_remaining", orig_size)
            remaining = prev_remaining - count
            mkt["_order_remaining"] = max(0, remaining)
            if remaining <= 0:
                # Fully filled — order no longer exists on Kalshi
                mkt["order_id"] = None
                mkt["order_price"] = None
                mkt["last_order_time"] = time.time()
            # If partial fill, keep order_id — order still rests with remaining contracts

        # Record + schedule CLV (outside lock)
        add_log("FILL_WS", ticker,
                f"instant fill @{fill_price}c x{count} (trade={trade_id[:8] if trade_id else '?'})",
                side="no", price=fill_price, size=count)
        # Snapshot mkt — poller can mutate the live dict before the thread reads it
        mkt_snapshot = dict(mkt)
        threading.Thread(
            target=_send_fill,
            args=(ticker, mkt_snapshot, count, fill_price),
            daemon=True,
        ).start()
    except Exception as e:
        log.error(f"WS fill handler error: {e}")


def refresh_positions():
    """Fetch positions from Kalshi and update market state. Detect new fills."""
    if not kalshi.is_authenticated():
        return
    try:
        resp = kalshi.kalshi_get("/portfolio/positions")
        positions = resp.get("market_positions") or resp.get("positions") or []
        pos_map = {}
        for p in positions:
            tk = p.get("ticker") or p.get("market_ticker", "")
            raw_pos = float(p.get("position_fp") or p.get("position") or 0)
            count = abs(int(raw_pos))
            exposure = abs(float(p.get("market_exposure_dollars") or 0))
            avg_cents = round(exposure / count * 100) if count > 0 else 0
            side = "yes" if raw_pos > 0 else "no" if raw_pos < 0 else ""
            if tk:
                pos_map[tk] = {
                    "yes": count if side == "yes" else 0,
                    "no": count if side == "no" else 0,
                    "avg_yes": avg_cents if side == "yes" else 0,
                    "avg_no": avg_cents if side == "no" else 0,
                }
        with games_lock:
            for ticker, mkt in all_markets.items():
                pos = pos_map.get(ticker, {"yes": 0, "no": 0, "avg_yes": 0, "avg_no": 0})
                old_no = mkt.get("position_no", 0)
                new_no = pos["no"]
                # Only use polling as fill detection if WS isn't the primary source.
                # WS handles fills instantly; polling just syncs position counts.
                # Log discrepancies for debugging but don't double-record.
                if new_no > old_no and not mkt.get("_position_synced"):
                    # First sync after boot — don't treat as new fill
                    pass
                elif new_no > old_no:
                    diff = new_no - old_no
                    # Check if WS already recorded this fill (window widened — WS can arrive
                    # late during dropout recovery, and a tight window caused double-POSTs
                    # to homer_fills).
                    last_ws_fill = mkt.get("last_fill_ts", 0)
                    if time.time() - last_ws_fill < 30:
                        # WS caught it — just sync count, don't double-record
                        log.info(f"POSITION_SYNC {ticker}: +{diff} (WS already recorded)")
                    else:
                        # WS missed it — record via poller
                        log.warning(f"FILL_POLL {ticker}: +{diff} (WS missed, recording)")
                        add_log("FILL_DETECTED", ticker,
                                f"+{diff} @poll (WS missed)", "no", 0, diff)
                        # Prefer live order_price → last known place price → finally blended avg.
                        # avg_no is a last resort — it blends every historical fill on this ticker.
                        fill_price = (mkt.get("order_price")
                                      or mkt.get("_last_known_price")
                                      or pos["avg_no"] or 0)
                        mkt["last_fill_ts"] = time.time()
                        if fill_price > 0:
                            mkt_snapshot = dict(mkt)
                            threading.Thread(
                                target=_send_fill,
                                args=(ticker, mkt_snapshot, diff, fill_price),
                                daemon=True,
                            ).start()
                mkt["position_no"] = pos["no"]
                mkt["position_avg_no"] = pos["avg_no"]
                mkt["position_yes"] = pos["yes"]
                mkt["position_avg_yes"] = pos["avg_yes"]
                mkt["_position_synced"] = True
    except Exception as e:
        log.error(f"Position refresh failed: {e}")


def _position_poller():
    """Position poller — primary fill detection fallback.
    WS fill channel isn't catching fills reliably, so this polls every 5s
    to detect position changes and record fills."""
    while True:
        refresh_positions()
        time.sleep(5)


def _force_refetch_orderbook(ticker):
    """Forced REST OB re-fetch for staleness/seq-gap recovery. Bypasses the
    freshness guard in fetch_and_apply_orderbook BUT respects WS deltas that
    landed during the REST roundtrip — the snapshot is from before our HTTP
    request, so any WS update with a newer timestamp is more recent."""
    if ticker not in all_markets:
        return
    try:
        # Capture pre-fetch timestamp — anything newer in WS state wins.
        fetch_start_ts = time.time()
        snap = kalshi.kalshi_get(f"/markets/{ticker}/orderbook")
        ob = snap.get("orderbook_fp") or snap.get("orderbook") or snap
        raw_yes = (ob.get("yes_dollars_fp") or ob.get("yes_dollars")
                   or ob.get("yes") or ob.get("yes_fp", []))
        raw_no = (ob.get("no_dollars_fp") or ob.get("no_dollars")
                  or ob.get("no") or ob.get("no_fp", []))
        new_yes = parse_orderbook_side(raw_yes)
        new_no = parse_orderbook_side(raw_no)
        with games_lock:
            mkt = all_markets.get(ticker)
            if not mkt:
                return
            # If WS got an update DURING our REST call, the WS state is newer.
            # Don't overwrite — better to stay slightly stale one cycle than
            # to wipe a delta-progressed book.
            last_ws = mkt.get("_ob_last_update", 0)
            if last_ws > fetch_start_ts:
                log.info(
                    f"OB resync skipped {ticker}: WS newer "
                    f"(last_ws={last_ws:.3f} > fetch_start={fetch_start_ts:.3f})"
                )
                return
            mkt["orderbook"] = {"yes": new_yes, "no": new_no}
            mkt["best_bid_yes"] = compute_best_bid(new_yes)
            mkt["best_bid_no"] = compute_best_bid(new_no)
            mkt["_ob_last_update"] = time.time()
    except Exception as e:
        log.error(f"OB resync error {ticker}: {e}")


def _ob_staleness_poller():
    """Detect silent orderbook drift on active markets. If WS dropped a
    delta, the book sits wrong until the next organic update — the bot
    can place or hold orders against a stale best_bid/ask.

    Relaxed 2026-05-05: 15s cycle, 30s threshold. The previous 5s/5s combo
    fired on every quiet market every cycle (no deltas because nothing was
    trading), burning REST budget and spamming logs. Sequence-gap detection
    in the WS handler now triggers immediate force-refetch when a delta is
    actually missed — staleness poller is a slower safety net.
    """
    from concurrent.futures import ThreadPoolExecutor
    while True:
        time.sleep(15)
        try:
            now = time.time()
            stale = []
            with games_lock:
                for tk, mkt in all_markets.items():
                    if not mkt.get("active"):
                        continue  # don't burn REST budget on inactive markets
                    last = mkt.get("_ob_last_update", 0)
                    age = now - last
                    if age > 30:
                        # 2x weight for markets with an open order — past-post
                        # risk lives there. Idle markets can wait one cycle.
                        weight = age * 2 if mkt.get("order_id") else age
                        stale.append((weight, age, tk))
            # Highest weight first
            stale.sort(reverse=True)
            batch = stale[:20]
            for _, age, tk in batch:
                log.info(f"OB refetch {tk}: {age:.1f}s stale")
            if batch:
                with ThreadPoolExecutor(max_workers=4) as pool:
                    list(pool.map(lambda x: _force_refetch_orderbook(x[2]), batch))
        except Exception as e:
            log.error(f"OB staleness poller error: {e}")


def reconcile_resting_orders():
    """Safety net: pull ALL resting orders, detect anomalies.
    HTTP calls outside lock, state changes under lock (Bug #7 fix)."""
    try:
        all_resting = kalshi.get_resting_orders()
    except Exception as e:
        log.error(f"reconcile: get_resting_orders failed: {e}")
        return

    by_ticker = {}
    for o in all_resting or []:
        tk = o.get("ticker")
        if tk:
            by_ticker.setdefault(tk, []).append(o)

    # Phase 1: collect zombie cancels (need HTTP outside lock)
    zombie_cancels = []  # [(ticker, oid_to_cancel, keep_id)]

    with games_lock:
        for ticker, orders in by_ticker.items():
            if ticker not in all_markets:
                continue
            mkt = all_markets[ticker]
            tracked_id = mkt.get("order_id")

            if len(orders) == 1:
                actual_id = orders[0].get("order_id")
                if tracked_id != actual_id and not str(tracked_id or "").startswith("paper_"):
                    mkt["order_id"] = actual_id
                    actual_price = orders[0].get("no_price") or orders[0].get("yes_price")
                    if actual_price is not None:
                        try:
                            fp = float(actual_price)
                            mkt["order_price"] = round(fp * 100) if fp <= 1.0 else round(fp)
                        except (ValueError, TypeError):
                            pass
                    add_log("TRACKING_SYNC", ticker, f"synced to resting order {actual_id}")
                continue

            # ZOMBIE — collect for cancel outside lock
            keep_id = tracked_id if any(o.get("order_id") == tracked_id for o in orders) else None
            for o in orders:
                oid = o.get("order_id")
                if oid != keep_id:
                    zombie_cancels.append((ticker, oid, keep_id))
            if not keep_id:
                mkt["order_id"] = None
                mkt["order_price"] = None

        # Detect stale tracking
        now = time.time()
        for ticker, mkt in list(all_markets.items()):
            tracked = mkt.get("order_id")
            if tracked and ticker not in by_ticker:
                if str(tracked).startswith("paper_"):
                    continue
                if now - mkt.get("last_order_time", 0) < 10:
                    continue
                mkt["order_id"] = None
                mkt["order_price"] = None
                add_log("STALE_TRACKING", ticker, "cleared order_id that Kalshi doesn't have resting")

    # Phase 2: cancel zombies OUTSIDE lock
    for ticker, oid, keep_id in zombie_cancels:
        try:
            kalshi.cancel_order(oid)
        except Exception as e:
            log.error(f"reconcile cancel {oid} failed: {e}")
    if zombie_cancels:
        tickers_cleaned = set(t for t, _, _ in zombie_cancels)
        for t in tickers_cleaned:
            add_log("ZOMBIE_CLEANED", t, f"cancelled {sum(1 for x in zombie_cancels if x[0]==t)} duplicates")


def _on_odds_change(player, line, old_fair_no, new_fair_no, prop_type="points"):
    """Lane 1 — instant amend when ANY book's odds shift on a player.
    Collect actions under lock, execute HTTP calls OUTSIDE lock to avoid blocking.
    """
    if paper_mode:
        return

    # Phase 1: collect actions under lock (no HTTP calls)
    actions = []  # [(ticker, order_id, action, new_ceil, current_price, mkt_ref)]
    with games_lock:
        for ticker, mkt in all_markets.items():
            if not mkt.get("active") or mkt.get("prop_done"):
                continue
            if mkt.get("player") != player:
                continue
            if mkt.get("prop_type", "points") != prop_type:
                continue
            order_id = mkt.get("order_id")
            if not order_id or str(order_id).startswith("paper_"):
                continue

            game = games.get(mkt.get("game_id", ""), {})
            game_settings = _get_prop_settings(game, prop_type)
            game_arb = game_settings.get("arb_pct", settings.get("arb_pct", 0))
            new_ceil = compute_ceiling(player, mkt.get("offset", 0), arb_pct=game_arb, market_line=mkt.get("line"), prop_type=prop_type)

            if new_ceil is None or new_ceil <= 0:
                actions.append((ticker, order_id, "cancel", 0, mkt.get("order_price", 0), mkt))
                mkt["order_id"] = None
                mkt["order_price"] = None
                mkt["ceil_no"] = 0
                mkt["status"] = "NO_ODDS"
                mkt["last_order_time"] = time.time()
                continue

            mkt["ceil_no"] = new_ceil
            current_price = mkt.get("order_price", 0)

            if current_price and current_price > new_ceil:
                actions.append((ticker, order_id, "amend", new_ceil, current_price, mkt))
                # Pre-clear state — will be updated after HTTP call
                mkt["order_id"] = None
                mkt["order_price"] = None
                mkt["last_order_time"] = time.time()

    # Phase 2: execute HTTP calls OUTSIDE lock
    amended = 0
    for ticker, order_id, action, new_ceil, current_price, mkt in actions:
        try:
            if action == "cancel":
                try:
                    kalshi.cancel_order(order_id)
                except Exception:
                    pass
                add_log("LANE1_PULL", ticker, f"fair_no {old_fair_no:.1f}→{new_fair_no:.1f}, odds gone")
            elif action == "amend":
                # Cancel + place (same pattern as process_market)
                try:
                    kalshi.cancel_order(order_id)
                except Exception:
                    pass
                order = kalshi.place_order(ticker, "no", new_ceil, mkt.get("size", DEFAULT_SIZE))
                with games_lock:
                    if order:
                        mkt["order_id"] = order["order_id"]
                        mkt["order_price"] = new_ceil
                        mkt["_last_order_size"] = mkt.get("size", DEFAULT_SIZE)
                        mkt["_order_remaining"] = mkt.get("size", DEFAULT_SIZE)
                        amended += 1
                        add_log("LANE1_ADJUST", ticker,
                                f"fair_no {old_fair_no:.1f}→{new_fair_no:.1f}, {current_price}→{new_ceil}c")
                    else:
                        add_log("LANE1_PLACE_FAIL", ticker,
                                f"cancel+place failed for {current_price}→{new_ceil}c")
        except Exception as e:
            log.error(f"LANE1 HTTP error {ticker}: {e}")

    if amended > 0:
        log.info(f"LANE1: {player} fair_no {old_fair_no:.1f}→{new_fair_no:.1f}, amended {amended} orders")


def _on_suspension(player, line, book, prop_type="points"):
    """Book suspended/removed odds — collect order IDs under lock, cancel OUTSIDE lock."""
    if paper_mode:
        return

    # Phase 1: collect orders to cancel under lock
    to_cancel = []
    with games_lock:
        for ticker, mkt in all_markets.items():
            if not mkt.get("active") or mkt.get("prop_done"):
                continue
            if mkt.get("player") != player:
                continue
            if mkt.get("prop_type", "points") != prop_type:
                continue
            order_id = mkt.get("order_id")
            if not order_id or str(order_id).startswith("paper_"):
                continue
            to_cancel.append((ticker, order_id))
            mkt["status"] = "SUSPENDED"
            mkt["last_order_time"] = time.time()
            # Block re-quote for 3s after odds return — book flicker (DK
            # dropping a player and bringing them back) was producing
            # cancel/replace churn at unstable prices because the cache
            # repopulates mid-recovery.
            mkt["_resume_after"] = time.time() + 3.0

    # Phase 2: cancel OUTSIDE lock, then clear state
    for ticker, order_id in to_cancel:
        try:
            kalshi.cancel_order(order_id)
        except Exception:
            pass
    if to_cancel:
        with games_lock:
            for ticker, order_id in to_cancel:
                mkt = all_markets.get(ticker)
                if mkt and mkt.get("order_id") == order_id:
                    mkt["order_id"] = None
                    mkt["order_price"] = None

    if to_cancel:
        add_log("SUSPENDED_PULL", f"{player}|{line}",
                f"{book} dropped odds, cancelled {len(to_cancel)} orders")
        log.info(f"SUSPENDED_PULL: {player} disappeared from {book}, cancelled {len(to_cancel)} orders")


def fast_heartbeat_loop():
    """Fast trading loop (0.2s) — jump logic + ceiling check using cached WS data.
    Zero REST reads. Pure in-memory processing.
    Only iterates active markets for speed — inactive ones skip instantly."""
    while True:
        t0 = time.time()
        processed = 0
        resize_cancels = []  # collect oversized orders for batch cancel on phase transition
        # Process each market: compute under lock, HTTP outside, apply under lock
        with games_lock:
            active_tickers = [(tk, mkt) for tk, mkt in all_markets.items()
                              if mkt.get("active") and not mkt.get("prop_done")]
        for ticker, mkt in active_tickers:
            # Phase 1: compute what to do under lock (no HTTP)
            action = None
            with games_lock:
                if not mkt.get("active") or mkt.get("prop_done"):
                    continue
                # Check for phase transition
                was_inplay = mkt.get("status_inplay", False)
                will_be_inplay = _is_inplay(mkt)
                if will_be_inplay and not was_inplay:
                    oid = mkt.get("order_id")
                    if oid and not str(oid).startswith("paper_"):
                        game = games.get(mkt.get("game_id", ""), {})
                        pt_settings = _get_prop_settings(game, mkt.get("prop_type", "points"))
                        full_size = mkt.get("size", pt_settings.get("size", DEFAULT_SIZE))
                        inplay_size = max(1, int(full_size * settings.get("inplay_size_pct", 50) / 100))
                        current_size = mkt.get("_last_order_size", full_size)
                        if current_size > inplay_size:
                            resize_cancels.append((ticker, oid, current_size, inplay_size))
                try:
                    _check_bond(ticker, mkt)
                    action = compute_market_action(ticker, mkt)
                    processed += 1
                except Exception as e:
                    log.error(f"fast heartbeat error {ticker}: {e}")

            # Phase 2: execute HTTP outside lock
            if action:
                _execute_market_action(ticker, mkt, action)

        # Batch cancel oversized orders from phase transitions (1 API call for all)
        if resize_cancels:
            order_ids = [oid for _, oid, _, _ in resize_cancels]
            cancelled = kalshi.batch_cancel(order_ids)
            with games_lock:
                for ticker, oid, old_sz, new_sz in resize_cancels:
                    if ticker in all_markets:
                        all_markets[ticker]["order_id"] = None
                        all_markets[ticker]["order_price"] = None
                        all_markets[ticker]["_last_order_size"] = 0
            add_log("INPLAY_RESIZE", detail=f"batch cancelled {cancelled} oversized orders (dead→live transition)")
            log.info(f"INPLAY_RESIZE: batch cancelled {cancelled} orders on phase transition")

        elapsed = time.time() - t0
        interval = settings.get("heartbeat_interval", 0.2)
        if elapsed > interval:
            log.warning(f"fast heartbeat slow: {elapsed:.3f}s for {processed} active markets (target {interval}s)")
        sleep_time = max(0.01, interval - elapsed)
        time.sleep(sleep_time)


_CSF_TO_CDN_PHASE = {
    clock_state_feed.LIVE: "LIVE",
    clock_state_feed.TIMEOUT: "TIMEOUT",
    clock_state_feed.HALFTIME: "HALFTIME",
    clock_state_feed.QUARTER_END: "QUARTER_BREAK",
    clock_state_feed.FREE_THROW: "FOUL_SHOT",
    clock_state_feed.FINAL: "FINAL",
    # Micro-stops (foul/turnover/review/jumpball/generic dead-ball) map to
    # LIVE for phase-config purposes — they're brief and CDN treats them
    # as LIVE too. The dead→live transition that triggers batch-cancel
    # fires on TIMEOUT/HALFTIME/QUARTER_BREAK/FOUL_SHOT → LIVE only.
    clock_state_feed.DEAD_BALL_FOUL: "LIVE",
    clock_state_feed.DEAD_BALL_TURNOVER: "LIVE",
    clock_state_feed.DEAD_BALL_REVIEW: "LIVE",
    clock_state_feed.DEAD_BALL_JUMPBALL: "LIVE",
    clock_state_feed.DEAD_BALL: "LIVE",
}


def _get_phase_for_game(game):
    """Current NBA phase for a games[] entry. Returns 'UNKNOWN' on any failure.

    Primary source: clock_state_feed (Bolt LS+PBP, sub-second push).
    Fallback: nba_feed CDN poll (5-15s lag).

    Driving _phase_transition_loop from clock_state_feed means dead→live
    batch-cancellation fires on Bolt's `ls_game_dt` signal, typically
    3-6s ahead of CDN — closing the past-post window on first-shot-out-
    of-timeout. CDN remains the safety net when Bolt is dead/silent."""
    try:
        nba_gid = game.get("nba_game_id")
        bolt_key = game.get("_forced_bolt_key")
        if not bolt_key and nba_gid:
            bolt_key = nba_feed.get_bolt_key(str(nba_gid))
        if bolt_key:
            csf_phase = clock_state_feed.get_phase(bolt_key)
            mapped = _CSF_TO_CDN_PHASE.get(csf_phase)
            if mapped:
                return mapped
        # Fallback to CDN (also fires when csf returned UNKNOWN)
        if nba_gid:
            return nba_feed.get_game_phase(str(nba_gid)) or "UNKNOWN"
        return "UNKNOWN"
    except Exception:
        return "UNKNOWN"


def _get_phase_config(phase):
    """Return the effective per-phase settings dict, falling back to a neutral default."""
    ps = settings.get("phase_settings") or {}
    return ps.get(phase, {"enabled": True, "size_pct": 100, "arb_pct_delta": 0})


def _phase_configs_equivalent(phase_a, phase_b):
    """True if trading behavior is identical across the two phases.
    Compares only the fields that drive order placement: enabled, size_pct,
    arb_pct_delta. If these all match, a phase change is cosmetic and we can
    skip the batch-cancel/replace churn (e.g. LIVE↔FOUL_SHOT when both share
    the same config)."""
    a = _get_phase_config(phase_a)
    b = _get_phase_config(phase_b)
    return (
        bool(a.get("enabled", True)) == bool(b.get("enabled", True))
        and a.get("size_pct", 100) == b.get("size_pct", 100)
        and a.get("arb_pct_delta", 0) == b.get("arb_pct_delta", 0)
    )


def _cancel_all_for_game(game_id, reason="phase_transition"):
    """Batch-cancel every live order for a game.

    Race-safe: local state is cleared BEFORE the HTTP call so the 0.2s
    heartbeat doesn't try to amend a cancelled-but-not-yet-confirmed order
    (which would race the batch_cancel and spam 409s). If the HTTP cancel
    fails the zombie reconcile loop catches the orphan on its next pass.
    """
    to_cancel = []  # (ticker, order_id)
    with games_lock:
        game = games.get(game_id)
        if not game:
            return 0
        for ticker, mkt in game.get("markets", {}).items():
            oid = mkt.get("order_id")
            if oid and not str(oid).startswith("paper_"):
                to_cancel.append((ticker, oid))
                # Optimistic local clear — heartbeat sees "no orders" immediately.
                mkt["order_id"] = None
                mkt["order_price"] = None
                mkt["_order_remaining"] = 0
                mkt["status"] = "PHASE_CANCEL"
    if not to_cancel:
        return 0
    try:
        kalshi.batch_cancel([oid for _, oid in to_cancel])
    except Exception as e:
        log.error(f"phase batch_cancel failed for {game_id}: {e} — falling back to per-order")
        for _, oid in to_cancel:
            try:
                kalshi.cancel_order(oid)
            except Exception:
                pass
    add_log("PHASE_CANCEL", None,
            f"{reason}: cancelled {len(to_cancel)} orders in game {game_id}")
    return len(to_cancel)


def _auto_subscribe_live_games_loop():
    """Polls today's NBA scoreboard every 60s and ensures Bolt LS+PBP are
    subscribed to every live game (status='Live'), independent of Kalshi
    event loading. Creates an observation-only stub in `games` so the
    /v2/phase dashboard surfaces them.

    Bolt slot capacity (Pro tier): 1 connection per feed locally
    (Railway prod uses the other), 20 games per subscription. Plenty of
    headroom for a typical 4-8 game NBA night."""
    log.info("Auto-subscribe Bolt to all live NBA games — loop started")
    seen_subscribed = set()  # bolt_keys we've already subscribed
    while True:
        try:
            todays = nba_feed.get_todays_games() or []
            live_games = [g for g in todays if (g.get("status") or "").lower() == "live"]
            for g in live_games:
                espn_gid = str(g.get("game_id") or "")
                home_full = g.get("home", "")
                away_full = g.get("away", "")
                if not (home_full and away_full):
                    continue
                try:
                    bolt_key = boltodds_ls_feed.resolve_bolt_key(home_full, away_full)
                except Exception:
                    continue
                if not bolt_key:
                    continue
                if bolt_key in seen_subscribed:
                    # Already subscribed this game — make sure stub is in `games`
                    pass
                else:
                    if ENABLE_BOLT_FEEDS:
                        boltodds_ls_feed.subscribe(bolt_key)
                        boltodds_pbp_feed.subscribe(bolt_key)
                    nba_feed.register_bolt_key(espn_gid, bolt_key)
                    nba_cdn_id = None
                    try:
                        nba_cdn_id = nba_feed._nba_game_ids.get(espn_gid)
                    except Exception:
                        pass
                    if nba_cdn_id:
                        nba_feed.register_bolt_key(nba_cdn_id, bolt_key)
                    seen_subscribed.add(bolt_key)
                    log.info(f"Auto-subscribed Bolt: {bolt_key} (ESPN={espn_gid})")

                # Ensure observation-only stub exists in games dict so
                # /v2/phase shows it even if no Kalshi event is loaded.
                stub_id = f"live_{espn_gid}"
                with games_lock:
                    if stub_id not in games:
                        games[stub_id] = {
                            "game_id": stub_id,
                            "game_name": f"{g.get('away_abbr','?')} @ {g.get('home_abbr','?')}",
                            "nba_game_id": espn_gid,
                            "kalshi_event": None,
                            "prop_type": "points",
                            "smart_mode": False,
                            "smart_status": None,
                            "settings": {},
                            "markets": {},
                            "paused": True,
                            "_observation_only": True,
                            "_forced_bolt_key": bolt_key,
                        }
        except Exception as e:
            log.warning(f"auto_subscribe_live_games error: {e}")
        time.sleep(60)


def _phase_transition_loop():
    """Watches each loaded game's phase and batch-cancels ALL orders on any change.
    No cooldown — next heartbeat re-prices with the new phase's config."""
    while True:
        try:
            if settings.get("phase_settings_enabled", True):
                with games_lock:
                    game_ids = list(games.keys())
                for gid in game_ids:
                    with games_lock:
                        game = games.get(gid)
                        if not game:
                            continue
                        prev_phase = game.get("_last_phase")
                    curr_phase = _get_phase_for_game(game)
                    if prev_phase is None:
                        # First observation — seed without cancelling
                        with games_lock:
                            if gid in games:
                                games[gid]["_last_phase"] = curr_phase
                        continue
                    if curr_phase != prev_phase:
                        if _phase_configs_equivalent(prev_phase, curr_phase):
                            # Cosmetic transition — same enabled/size/arb. Skip cancel.
                            with games_lock:
                                if gid in games:
                                    games[gid]["_last_phase"] = curr_phase
                            add_log("PHASE_SKIP", None,
                                    f"{prev_phase}→{curr_phase} (config identical, no cancel)")
                        else:
                            _cancel_all_for_game(
                                gid, reason=f"phase {prev_phase}→{curr_phase}"
                            )
                            with games_lock:
                                if gid in games:
                                    games[gid]["_last_phase"] = curr_phase
                                    games[gid]["_last_phase_transition_ts"] = time.time()
        except Exception as e:
            log.error(f"phase transition loop error: {e}")
        time.sleep(1.0)


def slow_heartbeat_loop():
    """Slow safety loop (5s) — reconcile + refresh ceilings for inactive markets.
    REST reads happen here, not in the fast loop."""
    _last_reconcile = 0
    while True:
        try:
            now = time.time()
            reconcile_interval = settings.get("reconcile_interval", 30)
            if now - _last_reconcile >= reconcile_interval:
                reconcile_resting_orders()
                _last_reconcile = now

            # Refresh ceilings + implied for ALL markets (so UI shows current data)
            dk_w = settings.get("dk_weight", 50)
            with games_lock:
                for tk, mkt in all_markets.items():
                    if mkt.get("active") or mkt.get("prop_done"):
                        continue  # active markets handled by fast heartbeat
                    pt = mkt.get("prop_type", "points")
                    game = games.get(mkt.get("game_id", ""), {})
                    game_settings_local = _get_prop_settings(game, pt)
                    g_arb = game_settings_local.get("arb_pct", settings.get("arb_pct", 0))
                    blended_imp = get_blended_implied(mkt["player"], dk_w, market_line=mkt.get("line"), prop_type=pt)
                    if blended_imp is not None and blended_imp > 0:
                        mkt["implied_yes"] = round(blended_imp, 1)
                        mkt["implied_no"] = round(100 - blended_imp, 1)
                    new_ceil = compute_ceiling(mkt["player"], mkt.get("offset", 0), arb_pct=g_arb, market_line=mkt.get("line"), prop_type=pt)
                    if new_ceil is not None:
                        mkt["ceil_no"] = new_ceil
        except Exception as e:
            log.error(f"slow heartbeat error: {e}")
        time.sleep(settings.get("slow_heartbeat_interval", 5))


# ---------------------------------------------------------------------------
# API routes
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    return INDEX_HTML


@app.route("/api/status")
def get_status():
    games_active = 0
    games_live = 0
    games_waiting = 0
    games_final = 0
    for gid, game in games.items():
        has_active = any(m.get("active") for m in game.get("markets", {}).values())
        if has_active:
            games_active += 1
        ss = game.get("smart_status", "")
        if ss == "LIVE":
            games_live += 1
        elif ss == "WAITING":
            games_waiting += 1
        elif ss == "FINAL":
            games_final += 1
    return jsonify({
        "authenticated": kalshi.is_authenticated(),
        "active_markets": sum(1 for m in all_markets.values() if m["active"]),
        "total_markets": len(all_markets),
        "odds_last_poll": odds_feed.last_poll_time,
        "active_games": len(games),
        "games_active": games_active,
        "games_live": games_live,
        "games_waiting": games_waiting,
        "games_final": games_final,
        "balance": kalshi.get_balance(),
        "paper_mode": paper_mode,
        "paused": all_paused,
        "ws_connected": ws_connected,
        "global_smart_mode": global_smart_mode,
        "rate_stats": kalshi.get_rate_stats(),
    })


@app.route("/api/smart-mode", methods=["POST"])
def toggle_global_smart():
    global global_smart_mode, smart_refresh_thread
    global_smart_mode = not global_smart_mode
    if global_smart_mode:
        with games_lock:
            for gid, game in games.items():
                game["smart_mode"] = True
                nba_gid = game.get("nba_game_id")
                if nba_gid:
                    lifecycle = nba_feed.get_game_lifecycle(str(nba_gid))
                    if lifecycle == "Live":
                        game["smart_status"] = "LIVE"
                        on_game_start(nba_gid)
                    elif lifecycle == "Final":
                        game["smart_status"] = "FINAL"
                    else:
                        game["smart_status"] = "WAITING"
        if not smart_refresh_thread or not smart_refresh_thread.is_alive():
            smart_refresh_thread = threading.Thread(target=_smart_mode_loop, daemon=True)
            smart_refresh_thread.start()
        add_log("SMART_GLOBAL", detail="Global smart mode ON")
    else:
        with games_lock:
            for gid, game in games.items():
                game["smart_mode"] = False
                game["smart_status"] = None
        add_log("SMART_GLOBAL", detail="Global smart mode OFF")
    return jsonify({"ok": True, "global_smart_mode": global_smart_mode})


@app.route("/api/kalshi_rate_stats")
def kalshi_rate_stats():
    return jsonify(kalshi.get_rate_stats())


@app.route("/api/settings", methods=["GET", "POST"])
def api_settings():
    global settings
    if request.method == "GET":
        return jsonify(settings)
    data = request.json or {}
    for k in settings:
        if k in data:
            settings[k] = data[k]
    try:
        settings["ceiling_cap"] = max(1, min(99, int(settings.get("ceiling_cap", 98))))
    except (ValueError, TypeError):
        settings["ceiling_cap"] = 98
    return jsonify({"ok": True, "settings": settings})


@app.route("/api/settings/phase", methods=["GET", "POST"])
def api_phase_settings():
    """GET: full phase settings tree. POST: update master switch and/or per-phase configs.
    Request body (any subset):
      {"phase_settings_enabled": bool,
       "phase_settings": {"LIVE": {"enabled": bool, "size_pct": int, "arb_pct_delta": int}, ...}}
    """
    if request.method == "GET":
        return jsonify({
            "phase_settings_enabled": settings.get("phase_settings_enabled", True),
            "phase_settings": settings.get("phase_settings", {}),
            "phases": PHASES_ALL,
            "current_phases": {
                gid: game.get("_last_phase") or _get_phase_for_game(game)
                for gid, game in games.items()
            },
        })
    data = request.json or {}
    if "phase_settings_enabled" in data:
        settings["phase_settings_enabled"] = bool(data["phase_settings_enabled"])
    new_cfg = data.get("phase_settings")
    if isinstance(new_cfg, dict):
        ps = settings.setdefault("phase_settings", {})
        for phase, cfg in new_cfg.items():
            if phase not in PHASES_ALL or not isinstance(cfg, dict):
                continue
            current = ps.setdefault(phase, {"enabled": True, "size_pct": 100, "arb_pct_delta": 0})
            if "enabled" in cfg:
                current["enabled"] = bool(cfg["enabled"])
            if "size_pct" in cfg:
                try:
                    current["size_pct"] = max(0, min(1000, int(cfg["size_pct"])))
                except (ValueError, TypeError):
                    pass
            if "arb_pct_delta" in cfg:
                try:
                    current["arb_pct_delta"] = int(cfg["arb_pct_delta"])
                except (ValueError, TypeError):
                    pass
    return jsonify({
        "ok": True,
        "phase_settings_enabled": settings.get("phase_settings_enabled", True),
        "phase_settings": settings.get("phase_settings", {}),
    })


@app.route("/api/on-court/debug")
def api_on_court_debug():
    """Debug panel payload: per-loaded-game 5-on-5 + bench, with phase + clock context.
    Phase (a) — observation only. No trading impact.
    Shape: {games: [{game_id, game_name, phase, clock, period,
                     home: {tricode, on_court:[...], bench:[...]},
                     away: {...}, updated_at, stale, source}]}"""
    out_games = []
    with games_lock:
        game_items = list(games.items())
    for gid, game in game_items:
        nba_gid = game.get("nba_game_id")
        if not nba_gid:
            out_games.append({
                "game_id": gid,
                "game_name": game.get("game_name", ""),
                "error": "no nba_game_id — boxscore/PBP not mapped yet",
            })
            continue
        snap = nba_feed.get_on_court_snapshot(str(nba_gid))
        pbp = nba_feed.get_pbp_phase(str(nba_gid)) or {}
        entry = {
            "game_id": gid,
            "game_name": game.get("game_name", ""),
            "nba_game_id": str(nba_gid),
            "phase": pbp.get("phase") or _get_phase_for_game(game),
            "clock": pbp.get("clock", ""),
            "period": pbp.get("period", 0),
        }
        if snap:
            entry.update(snap)
        else:
            entry["error"] = "no boxscore yet — on-court unknown"
        out_games.append(entry)
    return jsonify({"games": out_games, "ts": time.time()})


@app.route("/api/mode/live", methods=["POST"])
def mode_live():
    global paper_mode
    data = request.json or {}
    if data.get("confirm") != "CONFIRM":
        return jsonify({"error": "Type CONFIRM to go live"}), 400
    paper_mode = False
    add_log("LIVE_MODE", detail="LIVE trading enabled")
    return jsonify({"ok": True})


@app.route("/api/mode/paper", methods=["POST"])
def mode_paper():
    global paper_mode
    paper_mode = True
    add_log("PAPER_MODE", detail="Paper mode enabled")
    return jsonify({"ok": True})


@app.route("/api/log")
def api_log():
    since = int(request.args.get("since", 0))
    with session_log_lock:
        total = len(session_log)
        entries = session_log[since:] if since < total else []
    return jsonify({"entries": entries, "total": total})


@app.route("/api/positions")
def api_positions():
    try:
        resp = kalshi.kalshi_get("/portfolio/positions")
        all_positions = resp.get("market_positions", resp.get("positions", []))
        our_tickers = set(all_markets.keys())
        positions = []
        for p in all_positions:
            tk = p.get("ticker", p.get("market_ticker", ""))
            if tk in our_tickers:
                positions.append(p)
        return jsonify({"positions": positions})
    except Exception as e:
        log.error(f"Positions error: {e}")
        return jsonify({"positions": []})


@app.route("/api/settlements")
def api_settlements():
    """Pull settled positions from Kalshi."""
    try:
        cursor = request.args.get("cursor")
        limit = int(request.args.get("limit", 200))
        params = {"limit": limit}
        if cursor:
            params["cursor"] = cursor
        resp = kalshi.kalshi_get("/portfolio/settlements", params=params)
        settlements = resp.get("settlements", [])
        # Group by event
        by_event = {}
        total_pnl = 0
        for s in settlements:
            ticker = s.get("ticker", s.get("market_ticker", ""))
            # Extract event from ticker (everything before the last dash-segment)
            parts = ticker.rsplit("-", 1)
            event = parts[0] if len(parts) > 1 else ticker
            pnl = float(s.get("revenue", s.get("pnl", 0)) or 0)
            total_pnl += pnl
            if event not in by_event:
                by_event[event] = {"event": event, "markets": [], "pnl": 0}
            by_event[event]["markets"].append({
                "ticker": ticker,
                "pnl": round(pnl, 2),
                "no_count": s.get("no_total_cost", s.get("no_count", 0)),
                "yes_count": s.get("yes_total_cost", s.get("yes_count", 0)),
                "settled_at": s.get("settled_time", s.get("settlement_time", "")),
            })
            by_event[event]["pnl"] = round(by_event[event]["pnl"] + pnl, 2)
        events_sorted = sorted(by_event.values(), key=lambda x: x["pnl"])
        return jsonify({
            "total_pnl": round(total_pnl, 2),
            "events": events_sorted,
            "count": len(settlements),
            "next_cursor": resp.get("cursor"),
        })
    except Exception as e:
        log.error(f"Settlements error: {e}")
        return jsonify({"error": str(e), "events": [], "total_pnl": 0})


@app.route("/api/v2/force_subscribe", methods=["POST", "GET"])
def api_v2_force_subscribe():
    """Test/admin endpoint — manually subscribe Bolt LS+PBP for a game without
    needing Kalshi events loaded. Pass either:
      ?bolt_key=<exact bolt key>   OR
      ?home=<home team>&away=<away team>   (will resolve via Bolt)

    Also creates a stub entry in `games` so the phase_compare dashboard sees it.
    Useful for validating Bolt connection independent of Kalshi auth.
    """
    bolt_key = request.args.get("bolt_key") or (request.json or {}).get("bolt_key") if request.method == "POST" else request.args.get("bolt_key")
    home = request.args.get("home") or (request.json or {}).get("home") if request.method == "POST" else request.args.get("home")
    away = request.args.get("away") or (request.json or {}).get("away") if request.method == "POST" else request.args.get("away")

    if not bolt_key and home and away:
        try:
            bolt_key = boltodds_ls_feed.resolve_bolt_key(home, away)
        except Exception as e:
            return jsonify({"ok": False, "error": f"resolve failed: {e}"}), 400

    if not bolt_key:
        return jsonify({"ok": False, "error": "provide ?bolt_key=... or ?home=...&away=..."}), 400

    # Subscribe both feeds (manual endpoint — always honors request, but logs
    # a warning if BOLT_LS_ENABLED=0 so the operator knows the global flag is off)
    if not ENABLE_BOLT_FEEDS:
        log.warning("Manual /api/v2/bolt_subscribe called while BOLT_LS_ENABLED=0 — subscribing anyway by explicit request")
    try:
        boltodds_ls_feed.subscribe(bolt_key)
        boltodds_pbp_feed.subscribe(bolt_key)
    except Exception as e:
        return jsonify({"ok": False, "error": f"subscribe failed: {e}"}), 500

    # Create a stub games entry so phase_compare dashboard sees it
    stub_id = f"forced_{bolt_key.replace(' ', '_').replace(',', '')[:40]}"
    with games_lock:
        if stub_id not in games:
            games[stub_id] = {
                "game_name": bolt_key,
                "nba_game_id": None,
                "kalshi_event": None,
                "_home_full": home or "",
                "_away_full": away or "",
                "_forced_bolt_key": bolt_key,
                "paused": True,  # safety — paper mode anyway, but extra paused
                "smart_mode": False,
                "smart_status": "FORCED_BOLT_TEST",
                "settings": {},
                "markets": {},
                "paused_tickers": set(),
            }
            # Register bolt_key for nba_feed lookups (under stub_id)
            try:
                nba_feed.register_bolt_key(stub_id, bolt_key)
            except Exception:
                pass

    return jsonify({
        "ok": True,
        "bolt_key": bolt_key,
        "stub_id": stub_id,
        "ls_connected": boltodds_ls_feed.is_connected(),
        "pbp_connected": boltodds_pbp_feed.is_connected(),
        "msg": "subscribed — wait 2-5s for WS handshake then refresh /v2/phase",
    })


@app.route("/api/v2/ceiling_audit")
def api_v2_ceiling_audit():
    """End-to-end math audit for a market — the per-book values, the
    book_mode-resolved 'used' odds, devigged fair, computed ceiling,
    and the EV at ceiling. Use to confirm the chain is consistent.

    Query params:
      ?ticker=<kalshi_ticker>     — audit a specific market (preferred)
      ?player=<name>&line=<X>     — fallback: lookup by player+line
      ?prop_type=points|rebounds|assists  (default: points)
    """
    ticker = request.args.get("ticker")
    player = request.args.get("player")
    line_arg = request.args.get("line")
    prop_type = request.args.get("prop_type", "points")
    line = float(line_arg) if line_arg else None
    if ticker:
        with games_lock:
            for gid, game in games.items():
                m = (game.get("markets") or {}).get(ticker)
                if m:
                    player = player or m.get("player")
                    line = line if line is not None else m.get("line")
                    prop_type = m.get("prop_type", prop_type)
                    break
    if not player:
        return jsonify({"error": "provide ?ticker=... or ?player=..."}), 400

    detail = _find_detail(player, line, prop_type)
    fd = detail.get("fanduel", {}) or {}
    dk = detail.get("draftkings", {}) or {}

    # Per-book raw (vigged) values
    fd_yes_am = fd.get("odds")
    dk_yes_am = dk.get("odds")
    fd_imp_yes = fd.get("implied_yes")  # vigged % from American odds
    dk_imp_yes = dk.get("implied_yes")
    fd_imp_no_raw = (100 - fd_imp_yes) if fd_imp_yes else None
    dk_imp_no_raw = (100 - dk_imp_yes) if dk_imp_yes else None

    # Per-book devigged fair_yes (the over-side true prob estimate)
    fd_fair_yes, fd_method, fd_exp = _devig_implied_yes(fd) if fd else (None, None, None)
    dk_fair_yes, dk_method, dk_exp = _devig_implied_yes(dk) if dk else (None, None, None)
    fd_fair_no = (100 - fd_fair_yes) if fd_fair_yes else None
    dk_fair_no = (100 - dk_fair_yes) if dk_fair_yes else None

    # Settings
    book_mode = settings.get("book_mode", "min")
    dk_weight = settings.get("dk_weight", 50)
    arb_pct = settings.get("arb_pct", 0)

    # USED (raw, vigged) — what compute_ceiling builds the ceiling against
    used_book_no_raw = None
    used_source = None
    if fd_imp_no_raw and dk_imp_no_raw:
        if book_mode == "min":
            used_book_no_raw = min(fd_imp_no_raw, dk_imp_no_raw)
            used_source = "FD" if fd_imp_no_raw <= dk_imp_no_raw else "DK"
        else:
            w = dk_weight / 100.0
            used_book_no_raw = dk_imp_no_raw * w + fd_imp_no_raw * (1 - w)
            used_source = f"BLEND(dk={dk_weight}%)"
    elif fd_imp_no_raw:
        used_book_no_raw = fd_imp_no_raw; used_source = "FD"
    elif dk_imp_no_raw:
        used_book_no_raw = dk_imp_no_raw; used_source = "DK"

    # USED (devigged) — what fair_no comes from
    used_fair_no = None
    if fd_fair_yes and dk_fair_yes:
        if book_mode == "min":
            # max(fair_yes) = min(fair_no) → most conservative
            used_fair_yes = max(fd_fair_yes, dk_fair_yes)
        else:
            w = dk_weight / 100.0
            used_fair_yes = dk_fair_yes * w + fd_fair_yes * (1 - w)
        used_fair_no = 100 - used_fair_yes
    elif fd_fair_yes:
        used_fair_no = 100 - fd_fair_yes
    elif dk_fair_yes:
        used_fair_no = 100 - dk_fair_yes

    # Compute ceiling via the same code the bot uses
    computed_ceil = compute_ceiling(player, manual_offset=0, arb_pct=arb_pct,
                                    market_line=line, prop_type=prop_type)
    # Manual recomputation (must match)
    manual_raw_ceil = (used_book_no_raw - arb_pct) if used_book_no_raw else None
    manual_ceil = math.floor(manual_raw_ceil) if manual_raw_ceil else None

    # EV at ceiling = (fair_no - ceil) / ceil
    ceil_ev = ((used_fair_no - computed_ceil) / computed_ceil * 100
               if used_fair_no and computed_ceil else None)

    # Validations
    checks = {
        "ceil_below_fair": (computed_ceil is not None and used_fair_no is not None
                            and computed_ceil < used_fair_no),
        "ceil_ev_positive": (ceil_ev is not None and ceil_ev >= 0),
        "ceil_matches_manual": (computed_ceil == manual_ceil
                                if manual_ceil is not None else None),
    }

    return jsonify({
        "ticker": ticker, "player": player, "line": line, "prop_type": prop_type,
        "settings": {
            "book_mode": book_mode, "dk_weight": dk_weight, "arb_pct": arb_pct,
            "ceil_mode": settings.get("ceil_mode", "arb"),
        },
        "books": {
            "fanduel": {
                "yes_american": fd_yes_am,
                "implied_yes_raw_pct": fd_imp_yes,
                "implied_no_raw_pct": fd_imp_no_raw,
                "fair_yes_devig_pct": fd_fair_yes,
                "fair_no_devig_pct": fd_fair_no,
                "devig_method": fd_method, "devig_exp": fd_exp,
            },
            "draftkings": {
                "yes_american": dk_yes_am,
                "implied_yes_raw_pct": dk_imp_yes,
                "implied_no_raw_pct": dk_imp_no_raw,
                "fair_yes_devig_pct": dk_fair_yes,
                "fair_no_devig_pct": dk_fair_no,
                "devig_method": dk_method, "devig_exp": dk_exp,
            },
        },
        "used": {
            "source": used_source,
            "book_no_raw_pct": used_book_no_raw,   # used to set ceiling
            "fair_no_devig_pct": used_fair_no,     # used for EV
        },
        "ceiling": {
            "manual_calc": f"{used_book_no_raw} - {arb_pct} = {manual_raw_ceil} → floor = {manual_ceil}" if used_book_no_raw else None,
            "computed_ceil": computed_ceil,
            "manual_ceil": manual_ceil,
        },
        "ev": {
            "formula": "(fair_no - ceil) / ceil * 100",
            "computation": (f"({used_fair_no:.2f} - {computed_ceil}) / {computed_ceil} * 100 = {ceil_ev:.2f}%"
                            if ceil_ev is not None else None),
            "ceil_ev_pct": ceil_ev,
        },
        "checks": checks,
    })


@app.route("/api/v2/pbp_recent")
def api_v2_pbp_recent():
    """Per-game recent Bolt PBP plays. Returns the last N normalized
    plays for the given bolt_key (or for ALL active games when no key).
    Use to render a per-game PBP feed in /v2/phase."""
    bolt_key = request.args.get("bolt_key")
    limit = int(request.args.get("limit", 25))
    out = {}
    keys = [bolt_key] if bolt_key else None
    if keys is None:
        # Pull all known bolt_keys from games
        keys = []
        with games_lock:
            for gid, game in games.items():
                bk = game.get("_forced_bolt_key")
                if not bk:
                    espn_gid = game.get("nba_game_id")
                    if espn_gid:
                        bk = nba_feed.get_bolt_key(str(espn_gid))
                if bk and bk not in keys:
                    keys.append(bk)
    for bk in keys:
        try:
            evs = boltodds_pbp_feed.get_recent_events(bk) or []
        except Exception:
            evs = []
        # Take last N
        evs = evs[-limit:]
        plays = []
        now = time.time()
        for e in evs:
            plays.append({
                "ts": e.get("ts"),
                "age_s": round(now - e["ts"], 1) if e.get("ts") else None,
                "type": e.get("type"),
                "team": e.get("team"),
                "scorer": e.get("scorer"),
                "player": e.get("player"),
                "fouled": e.get("fouled"),
                "fouling": e.get("fouling"),
                "points": e.get("points"),
                "awarded": e.get("awarded"),
                "seconds": e.get("seconds"),
                "score": e.get("score"),
                "description": (e.get("raw") or {}).get("name") or e.get("type"),
            })
        out[bk] = plays
    return jsonify({"ts": time.time(), "by_game": out})


@app.route("/api/v2/phase_compare")
def api_v2_phase_compare():
    """v2 — side-by-side comparison of CDN-derived phase vs Bolt-derived phase.
    Returns one row per active NBA game with both signals + diff flag."""
    rows = []
    with games_lock:
        snapshot = list(games.items())
    # Cache today's ESPN games for stub-id resolution
    espn_games = []
    try:
        espn_games = nba_feed.get_todays_games() or []
    except Exception:
        pass

    for gid, game in snapshot:
        espn_gid = game.get("nba_game_id")
        nba_cdn_id = nba_feed._espn_to_nba_ids.get(str(espn_gid)) if espn_gid else None
        # Resolve bolt_key — try forced-test key first, then nba_feed lookups
        bolt_key = game.get("_forced_bolt_key")
        if not bolt_key:
            bolt_key = nba_feed.get_bolt_key(str(espn_gid)) if espn_gid else None
        if not bolt_key and nba_cdn_id:
            bolt_key = nba_feed.get_bolt_key(str(nba_cdn_id))
        if not bolt_key:
            # last resort — try get_bolt_key on game_id itself (forced stub registers under it)
            try:
                bolt_key = nba_feed.get_bolt_key(str(gid))
            except Exception:
                pass

        # If this is a forced stub (no espn_gid), try team-name match against
        # ESPN scoreboard so cdn_phase can resolve.
        if not espn_gid and bolt_key and espn_games:
            bk_lower = bolt_key.lower()
            for eg in espn_games:
                home = (eg.get("home") or "").lower()
                away = (eg.get("away") or "").lower()
                if home and away and home in bk_lower and away in bk_lower:
                    espn_gid = eg.get("game_id")
                    nba_cdn_id = nba_feed._espn_to_nba_ids.get(str(espn_gid)) if espn_gid else None
                    break

        # CDN-derived phase
        cdn_phase = None
        try:
            cdn_phase = nba_feed.get_game_phase(str(nba_cdn_id or espn_gid)) if (nba_cdn_id or espn_gid) else None
        except Exception:
            cdn_phase = None

        # Bolt-derived phase
        bolt_summary = phase_resolver.get_phase_summary(bolt_key) if bolt_key else None

        bolt_phase = (bolt_summary or {}).get("phase")
        # Loose match: normalize both sides into the same vocabulary.
        cdn_norm = (cdn_phase or "").upper().replace("QUARTER_BREAK", "QUARTER_END").replace("FOUL_SHOT", "FREE_THROW")
        # Bolt may emit LIVE_PBP (PBP-only inferred LIVE) — treat as LIVE for match purposes.
        bolt_norm = (bolt_phase or "").upper().replace("LIVE_PBP", "LIVE")
        match = (cdn_norm == bolt_norm) if (cdn_phase and bolt_phase) else None

        # LS-first phase (new ls_phase resolver, beats CDN by 7.8s on score)
        ls_phase_summary = (ls_phase_manager.tick(bolt_key, nba_cdn_id)
                            if bolt_key else None)
        ls_recent_events = (ls_phase_manager.recent_events(bolt_key, 8)
                            if bolt_key else [])
        # Unified clock_state_feed — fastest possible clock_running signal
        clock_state = clock_state_feed.get_state(bolt_key) if bolt_key else {}

        rows.append({
            "game_id": gid,
            "game_name": game.get("game_name") or "",
            "nba_game_id": espn_gid,
            "nba_cdn_id": nba_cdn_id,
            "bolt_key": bolt_key,
            "cdn_phase": cdn_phase,
            "bolt_phase": bolt_phase,
            "bolt_reason": (bolt_summary or {}).get("reason"),
            "bolt_clock_running": (bolt_summary or {}).get("clock_running"),
            "bolt_clock_source": (bolt_summary or {}).get("clock_source"),
            "bolt_period": (bolt_summary or {}).get("period"),
            "bolt_remaining_sec": (bolt_summary or {}).get("remaining_sec"),
            "bolt_match_period": (bolt_summary or {}).get("match_period"),
            "bolt_score": (bolt_summary or {}).get("score"),
            "bolt_evidence": (bolt_summary or {}).get("evidence", []),
            "bolt_last_play": (bolt_summary or {}).get("last_play"),
            "ls_connected": (bolt_summary or {}).get("ls_connected", False),
            "pbp_connected": (bolt_summary or {}).get("pbp_connected", False),
            "phases_match": match,
            "ls_phase": ls_phase_summary,
            "ls_recent_events": ls_recent_events,
            "clock_feed": clock_state,
        })
    return jsonify({
        "ts": time.time(),
        "ls_connected": boltodds_ls_feed.is_connected(),
        "pbp_connected": boltodds_pbp_feed.is_connected(),
        "games": rows,
    })


@app.route("/api/pbp/live")
def api_pbp_live():
    """Live PBP stream — returns last 30 actions with phase detection."""
    import requests as req
    # Find live NBA CDN game ID
    nba_cdn_id = None
    game_name = ""
    for gid, game in games.items():
        espn_gid = game.get("nba_game_id")
        if espn_gid:
            cdn_id = nba_feed._espn_to_nba_ids.get(str(espn_gid))
            if cdn_id:
                # Check if live
                with nba_feed.game_states_lock:
                    gs = nba_feed.game_states.get(str(espn_gid), {})
                if gs.get("status") == "Live":
                    nba_cdn_id = cdn_id
                    game_name = game.get("game_name", gid)
                    break
    if not nba_cdn_id:
        # Try all known CDN IDs
        for eid, cid in nba_feed._espn_to_nba_ids.items():
            nba_cdn_id = cid
            game_name = f"ESPN {eid}"
            break
    if not nba_cdn_id:
        # Try to resolve now
        try:
            import requests as req2
            r2 = req2.get("https://cdn.nba.com/static/json/liveData/scoreboard/todaysScoreboard_00.json",
                         headers={"User-Agent": "Mozilla/5.0", "Referer": "https://www.nba.com/"}, timeout=5)
            if r2.status_code == 200:
                nba_games = r2.json().get("scoreboard", {}).get("games", [])
                for g in nba_games:
                    status = g.get("gameStatus", 0)
                    if status == 2:  # live
                        nba_cdn_id = g.get("gameId")
                        home = g.get("homeTeam", {}).get("teamTricode", "")
                        away = g.get("awayTeam", {}).get("teamTricode", "")
                        game_name = f"{away} @ {home}"
                        break
                if not nba_cdn_id and nba_games:
                    g = nba_games[-1]
                    nba_cdn_id = g.get("gameId")
                    home = g.get("homeTeam", {}).get("teamTricode", "")
                    away = g.get("awayTeam", {}).get("teamTricode", "")
                    game_name = f"{away} @ {home}"
        except Exception:
            pass
    if not nba_cdn_id:
        return jsonify({"error": "No live NBA game found"})

    try:
        r = req.get(f"https://cdn.nba.com/static/json/liveData/playbyplay/playbyplay_{nba_cdn_id}.json",
                    headers={"User-Agent": "Mozilla/5.0", "Referer": "https://www.nba.com/"}, timeout=5)
        if r.status_code != 200:
            return jsonify({"error": f"NBA CDN returned {r.status_code}"})
        actions = r.json().get("game", {}).get("actions", [])
        recent = actions[-30:]
        result = []
        for a in recent:
            at = (a.get("actionType") or "").lower()
            st = (a.get("subType") or "").lower()
            desc = a.get("description", "")
            phase = "LIVE"
            if at == "timeout": phase = "TIMEOUT"
            elif at == "period" and st == "end": phase = "QUARTER_END"
            elif at == "period" and st == "start": phase = "PERIOD_START"
            elif at == "freethrow": phase = "FREE_THROW"
            elif at == "foul": phase = "FOUL"
            elif at == "substitution": phase = "DEAD_BALL"
            elif at == "stoppage": phase = "STOPPAGE"
            elif at == "game" and st == "end": phase = "FINAL"
            result.append({
                "period": a.get("period"),
                "clock": a.get("clock"),
                "phase": phase,
                "type": a.get("actionType"),
                "sub": a.get("subType"),
                "team": a.get("teamTricode"),
                "player": a.get("playerNameI"),
                "desc": desc,
            })
        # Use the smart CDN phase logic (handles period-end detritus, timeout
        # detection across same-tick events, etc.) instead of the simple
        # last-action derivation. Falls back to last-action phase only if
        # get_pbp_phase has nothing.
        smart = nba_feed.get_pbp_phase(nba_cdn_id)
        if smart and smart.get("phase"):
            current_phase = smart["phase"]
        else:
            current_phase = result[-1]["phase"] if result else "UNKNOWN"

        # Bolt LS phase — independent comparison alongside CDN
        bolt_phase = "NO_KEY"
        bolt_period = None
        bolt_remaining = None
        bolt_clock_running = None
        bolt_age_s = None
        bolt_connected = boltodds_ls_feed.is_connected()
        bolt_key = nba_feed.get_bolt_key(nba_cdn_id)
        if bolt_key:
            state = boltodds_ls_feed.get_clock_state(bolt_key)
            if state is None:
                bolt_phase = "DISCONNECTED" if not bolt_connected else "NO_DATA"
            else:
                bolt_clock_running = state.get("clock_running")
                bolt_age_s = state.get("age_s")
                if bolt_clock_running is True:
                    bolt_phase = "LIVE"
                elif bolt_clock_running is False:
                    bolt_phase = "STOPPED"
                else:
                    bolt_phase = "UNCERTAIN"
                bolt_period = state.get("period")
                bolt_remaining = state.get("remaining_secs")

        return jsonify({
            "game": game_name,
            "nba_cdn_id": nba_cdn_id,
            "current_phase": current_phase,
            "bolt_phase": bolt_phase,
            "bolt_period": bolt_period,
            "bolt_remaining_secs": bolt_remaining,
            "bolt_clock_running": bolt_clock_running,
            "bolt_key": bolt_key,
            "bolt_age_s": bolt_age_s,
            "bolt_connected": bolt_connected,
            "bolt_api_key_source": getattr(boltodds_ls_feed, "_API_KEY_SOURCE", "?"),
            "total_actions": len(actions),
            "actions": result,
        })
    except Exception as e:
        return jsonify({"error": str(e)})


@app.route("/api/orders/resting")
def api_resting_orders():
    try:
        orders = kalshi.get_resting_orders()
        our_tickers = set(all_markets.keys())
        filtered = [o for o in orders if o.get("ticker", "") in our_tickers]
        return jsonify({"orders": filtered})
    except Exception as e:
        log.error(f"Resting orders error: {e}")
        return jsonify({"orders": []})


@app.route("/api/pause-all", methods=["POST"])
def pause_all():
    global all_paused, paused_tickers
    paused_tickers = set()
    to_cancel = []
    with games_lock:
        for tk, mkt in all_markets.items():
            if mkt["active"] and not mkt["prop_done"]:
                paused_tickers.add(tk)
                if mkt.get("order_id") and not str(mkt["order_id"]).startswith("paper_"):
                    to_cancel.append(mkt["order_id"])
                mkt["order_id"] = None
                mkt["order_price"] = None
                mkt["active"] = False
                mkt["status"] = "PAUSED"
    for oid in to_cancel:
        try:
            kalshi.cancel_order(oid)
        except Exception as e:
            log.warning(f"Cancel on pause failed: {e}")
    all_paused = True
    add_log("PAUSE_ALL", detail=f"Paused {len(paused_tickers)} bots")
    return jsonify({"ok": True, "paused": list(paused_tickers)})


@app.route("/api/resume-all", methods=["POST"])
def resume_all():
    global all_paused
    resumed = 0
    with games_lock:
        for tk in paused_tickers:
            if tk in all_markets:
                mkt = all_markets[tk]
                if not mkt["prop_done"]:
                    mkt["active"] = True
                    mkt["status"] = "JUMPING"
                    resumed += 1
    all_paused = False
    add_log("RESUME_ALL", detail=f"Resumed {resumed} bots")
    return jsonify({"ok": True, "resumed": resumed})


@app.route("/api/nba/games")
def get_nba_games():
    """Get today's NBA games from ESPN."""
    todays_games = nba_feed.get_todays_games()
    return jsonify({"games": todays_games})


@app.route("/api/nba/player-stats")
def get_nba_player_stats():
    """Get live player stats from NBA boxscore API."""
    nba_game_id = request.args.get("game_id")
    stats = nba_feed.get_player_stats(nba_game_id)
    return jsonify({"stats": stats, "ts": time.time()})


@app.route("/api/nba/game-info/<game_id>")
def get_nba_game_info(game_id):
    """Get game phase, clock, score for a specific game."""
    # Try ESPN game_id directly, or look up from our game's nba_game_id
    info = nba_feed.get_game_info(game_id)
    if info.get("phase") == "PRE" and game_id in games:
        nba_gid = games[game_id].get("nba_game_id")
        if nba_gid:
            info = nba_feed.get_game_info(str(nba_gid))
    return jsonify(info)


@app.route("/api/games/add", methods=["POST"])
def add_game():
    """Add a game — load Kalshi markets for an event ticker."""
    data = request.json
    kalshi_event_ticker = data.get("kalshi_event_ticker", "")
    nba_game_id = data.get("nba_game_id", "")
    prop_type = data.get("prop_type", "points")

    if not kalshi_event_ticker:
        return jsonify({"error": "kalshi_event_ticker required"}), 400

    game_id = kalshi_event_ticker
    if game_id in games:
        return jsonify({"error": "Game already loaded", "game_id": game_id}), 400

    # Fetch Kalshi markets for this event
    markets_data = []
    try:
        resp = kalshi.kalshi_get("/events/" + kalshi_event_ticker)
        for mkt in resp.get("markets", []):
            markets_data.append(mkt)
    except Exception as e:
        log.error(f"Kalshi event fetch error: {e}")
        return jsonify({"error": f"Failed to fetch Kalshi event: {e}"}), 500

    # Build game name
    game_name = kalshi_event_ticker
    game_time_iso = None

    # Try to get NBA game info for the name
    home_full = ""
    away_full = ""
    if nba_game_id:
        nba_games = nba_feed.get_todays_games()
        for g in nba_games:
            if str(g["game_id"]) == str(nba_game_id):
                game_name = f"{g['away_abbr']} @ {g['home_abbr']}"
                game_time_iso = g.get("startTime")
                home_full = g.get("home", "")
                away_full = g.get("away", "")
                break

    # Resolve Bolt LS key + subscribe so clock-state detection runs ahead
    # of CDN. Best-effort: a missing match silently falls back to CDN-only.
    if home_full and away_full:
        try:
            bolt_key = boltodds_ls_feed.resolve_bolt_key(home_full, away_full)
            if bolt_key:
                boltodds_ls_feed.subscribe(bolt_key)
                boltodds_pbp_feed.subscribe(bolt_key)  # v2: also subscribe PBP feed
                # Register under every ID we have to be tolerant of the
                # ESPN→NBA-CDN race (PBP poller may not have resolved yet).
                nba_cdn_id = None
                try:
                    nba_cdn_id = nba_feed._nba_game_ids.get(str(nba_game_id))
                except Exception:
                    pass
                nba_feed.register_bolt_key(nba_game_id, bolt_key)
                if nba_cdn_id and str(nba_cdn_id) != str(nba_game_id):
                    nba_feed.register_bolt_key(nba_cdn_id, bolt_key)
                log.info(f"Bolt LS+PBP subscribed: {bolt_key} -> espn={nba_game_id} cdn={nba_cdn_id or 'pending'}")
            else:
                log.info(f"Bolt LS: no match for {away_full} @ {home_full} today — CDN-only")
        except Exception as e:
            log.warning(f"Bolt LS setup failed for {game_id}: {e}")

    # Create game entry
    game = {
        "game_id": game_id,
        "game_name": game_name,
        "game_time": game_time_iso,
        "nba_game_id": nba_game_id,
        "kalshi_event": kalshi_event_ticker,
        "prop_type": prop_type,
        "smart_mode": False,
        "smart_status": None,
        "settings": {
            "max_exposure": DEFAULT_MAX_EXPOSURE,
            "points": dict(_default_prop_settings(), arb_pct=settings.get("arb_pct", DEFAULT_ARB_PCT)),
            "rebounds": dict(_default_prop_settings(), arb_pct=settings.get("arb_pct", DEFAULT_ARB_PCT)),
            "assists": dict(_default_prop_settings(), arb_pct=settings.get("arb_pct", DEFAULT_ARB_PCT)),
        },
        "markets": {},
    }

    # Create markets from Kalshi event
    new_tickers = []
    _, player_odds_map = _get_odds_source(prop_type)  # Bug #2 fix: use correct prop_type
    with games_lock:
        for mkt_info in markets_data:
            tk = mkt_info["ticker"]
            name = mkt_info.get("yes_sub_title") or mkt_info.get("subtitle") or mkt_info.get("title", tk)

            # Extract line FIRST (Bug #1 fix: line must be defined before match_player_to_odds)
            import re
            floor_strike = mkt_info.get("floor_strike")
            if floor_strike is not None:
                line = float(floor_strike)
            else:
                line_match = re.search(r'(\d+)\+', name)
                line = float(line_match.group(1)) - 0.5 if line_match else None

            # Parse player name from Kalshi market
            match_name = name.split(":")[0].strip() if ":" in name else name
            clean_name = re.sub(r'\s+\d+\+?\s*(Points|Rebounds|Assists|pts|reb|ast).*$', '', match_name, flags=re.IGNORECASE).strip()
            if not clean_name:
                clean_name = match_name

            matched_player = match_player_to_odds(clean_name, player_odds_map, market_line=line)
            odds = player_odds_map.get(matched_player, {}) if matched_player else {}

            if tk not in all_markets:
                mkt = {
                    "player": (matched_player.split("|")[0] if matched_player else clean_name),
                    "name": name,
                    "ticker": tk,
                    "game_id": game_id,
                    "nba_game_id": nba_game_id,
                    "prop_type": prop_type,
                    "line": line,
                    "ceil_no": odds.get("ceil_no", 0),
                    "offset": DEFAULT_OFFSET,
                    "size": DEFAULT_SIZE,
                    "active": False,
                    "order_id": None,
                    "order_price": None,
                    "last_order_time": 0,
                    "stepdown_since": 0,
                    "last_stepdown_at": 0,
                    "pulled": False,
                    "prop_done": False,
                    "status": "OFF",
                    "odds_american": odds.get("odds", None),
                    "implied_yes": odds.get("implied_yes", 0),
                    "implied_no": odds.get("implied_no", 0),
                    "book": odds.get("book", ""),
                    "orderbook": {"yes": {}, "no": {}},
                    "best_bid_yes": 0,
                    "best_bid_no": 0,
                    "fill_target": DEFAULT_FILL_TARGET,
                "player_max": DEFAULT_PLAYER_MAX,
                    "_mention_count": 0,
                    "_bonded": False,
                    "position_no": 0,
                    "position_avg_no": 0,
                    "position_yes": 0,
                    "position_avg_yes": 0,
                    "_position_synced": False,
                }
                game["markets"][tk] = mkt
                all_markets[tk] = mkt
                new_tickers.append(tk)

        games[game_id] = game

    # Start odds poller (OddsBlaze dual-book FD + DK)
    boltodds_feed.poll_interval = settings.get("ob_poll_interval", 0.5)
    # Lane 1 disabled — heartbeat handles odds changes naturally (like Homer)
    # boltodds_feed.on_odds_change = _on_odds_change
    boltodds_feed.on_suspension = _on_suspension
    boltodds_feed.start()

    # Start NBA feed poller
    nba_feed.start_game_poller()
    nba_feed.start_cdn_status_poller()

    # Start WS for orderbook tracking
    if new_tickers:
        pending_ws_subscribes.update(new_tickers)
    if not ws_connected:
        start_ws_thread()

    # Sync resting orders from Kalshi
    try:
        resting = kalshi.get_resting_orders()
        synced = 0
        with games_lock:
            for order in resting:
                otk = order.get("ticker", "")
                if otk in all_markets:
                    mkt = all_markets[otk]
                    if not mkt.get("order_id"):
                        mkt["order_id"] = order.get("order_id")
                        price = order.get("yes_price") or order.get("no_price") or 0
                        if isinstance(price, float) and price <= 1:
                            price = round(price * 100)
                        mkt["order_price"] = price
                        synced += 1
        if synced:
            log.info(f"Synced {synced} resting orders from Kalshi")
    except Exception as e:
        log.error(f"Resting order sync failed: {e}")

    # Bug #5 fix: fetch positions immediately so we know existing fills
    threading.Thread(target=refresh_positions, daemon=True).start()

    add_log("LOADED", detail=f"{len(new_tickers)} markets from {kalshi_event_ticker}")

    return jsonify({
        "ok": True,
        "game_id": game_id,
        "markets_loaded": len(new_tickers),
        "odds_matched": sum(1 for tk in new_tickers if all_markets[tk]["odds_american"] is not None),
    })


def _build_market_entry(mkt_info, game_id, prop_type):
    """Build a market dict from Kalshi market info. Shared by load_today and add_game."""
    import re
    tk = mkt_info["ticker"]
    name = mkt_info.get("yes_sub_title") or mkt_info.get("subtitle") or mkt_info.get("title", tk)

    # Parse player name — strip stat suffix
    match_name = name.split(":")[0].strip() if ":" in name else name
    clean_name = re.sub(r'\s+\d+\+?\s*(Points|Rebounds|Assists|pts|reb|ast).*$', '', match_name, flags=re.IGNORECASE).strip()
    if not clean_name:
        clean_name = match_name

    # Extract line
    floor_strike = mkt_info.get("floor_strike")
    if floor_strike is not None:
        line = float(floor_strike)
    else:
        line_match = re.search(r'(\d+)\+', name)
        line = float(line_match.group(1)) - 0.5 if line_match else None

    _, player_odds_map = _get_odds_source(prop_type)
    matched_player = match_player_to_odds(clean_name, player_odds_map, market_line=line)
    odds = player_odds_map.get(matched_player, {}) if matched_player else {}

    return {
        "player": (matched_player.split("|")[0] if matched_player else clean_name),
        "name": name,
        "ticker": tk,
        "game_id": game_id,
        "nba_game_id": None,
        "prop_type": prop_type,
        "line": line,
        "ceil_no": odds.get("ceil_no", 0),
        "offset": DEFAULT_OFFSET,
        "size": DEFAULT_SIZE,
        "active": False,
        "order_id": None,
        "order_price": None,
        "last_order_time": 0,
        "stepdown_since": 0,
        "last_stepdown_at": 0,
        "pulled": False,
        "prop_done": False,
        "status": "OFF",
        "odds_american": odds.get("odds", None),
        "implied_yes": odds.get("implied_yes", 0),
        "implied_no": odds.get("implied_no", 0),
        "book": odds.get("book", ""),
        "orderbook": {"yes": {}, "no": {}},
        "best_bid_yes": 0,
        "best_bid_no": 0,
        "fill_target": DEFAULT_FILL_TARGET,
        "player_max": DEFAULT_PLAYER_MAX,
        "_mention_count": 0,
        "_bonded": False,
        "position_no": 0,
        "position_avg_no": 0,
        "position_yes": 0,
        "position_avg_yes": 0,
        "_position_synced": False,
    }


@app.route("/api/games/load_today", methods=["POST"])
def load_today():
    """Auto-discover and load all today's NBA points events from Kalshi."""
    try:
        import re
        from datetime import date, datetime, timezone, timedelta

        # Series → prop_type mapping
        FULL_SERIES_MAP = {
            "KXNBAPTS": "points",
            "KXNBAREB": "rebounds",
            "KXNBAAST": "assists",
        }

        # Filter by requested prop types (default: points only)
        body = request.json or {}
        requested_types = body.get("prop_types", ["points"])  # ["points"], ["points","rebounds"], etc.
        SERIES_MAP = {k: v for k, v in FULL_SERIES_MAP.items() if v in requested_types}

        # Fetch events from requested series
        all_events = []  # [(event_dict, prop_type)]
        for series, pt in SERIES_MAP.items():
            cursor = None
            while True:
                params = {"series_ticker": series, "limit": 100}
                if cursor:
                    params["cursor"] = cursor
                resp = kalshi.kalshi_get("/events", params=params)
                events = resp.get("events", [])
                for ev in events:
                    all_events.append((ev, pt))
                cursor = resp.get("cursor", "")
                if not cursor or not events:
                    break

        # Filter to target date
        et = timezone(timedelta(hours=-4))
        now_utc = datetime.now(timezone.utc)
        date_str = body.get("date")
        if date_str:
            try:
                target_date = datetime.strptime(date_str, "%Y-%m-%d").date()
            except ValueError:
                target_date = now_utc.astimezone(et).date()
        else:
            date_offset = int(body.get("date_offset", 0))
            target_date = now_utc.astimezone(et).date() + timedelta(days=date_offset)
        today_kalshi = target_date.strftime("%y") + target_date.strftime("%b").upper() + target_date.strftime("%d")
        log.info(f"load_today: looking for date pattern '{today_kalshi}' in tickers (target={target_date})")

        today_events = [(ev, pt) for ev, pt in all_events if today_kalshi in ev.get("event_ticker", "")]
        log.info(f"load_today: found {len(today_events)} events across all prop types")

        # Group events by matchup (team codes from ticker) → merge into one game
        # Ticker: KXNBAPTS-26APR17GSWPHX → matchup key = "26APR17GSWPHX"
        matchup_groups = {}  # {matchup_key: [(event, prop_type), ...]}
        for ev, pt in today_events:
            tk = ev.get("event_ticker", "")
            # Strip series prefix to get matchup key: "KXNBAPTS-26APR17GSWPHX" → "26APR17GSWPHX"
            parts = tk.split("-", 1)
            matchup_key = parts[1] if len(parts) > 1 else tk
            if matchup_key not in matchup_groups:
                matchup_groups[matchup_key] = []
            matchup_groups[matchup_key].append((ev, pt))

        loaded = 0
        skipped = 0
        errors = 0
        for matchup_key, ev_list in matchup_groups.items():
            # Use the PTS event ticker as game_id (or first available)
            pts_ev = next((ev for ev, pt in ev_list if pt == "points"), None)
            first_ev, first_pt = ev_list[0]
            base_ev = pts_ev or first_ev
            game_id = base_ev.get("event_ticker", matchup_key)

            if game_id in games:
                # Game exists — merge any new prop types into it
                game = games[game_id]
                new_tickers = []
                for ev, pt in ev_list:
                    event_ticker = ev.get("event_ticker", "")
                    try:
                        evt_resp = kalshi.kalshi_get("/events/" + event_ticker)
                        for mkt_info in evt_resp.get("markets", []):
                            tk = mkt_info["ticker"]
                            if tk in all_markets:
                                continue
                            mkt = _build_market_entry(mkt_info, game_id, pt)
                            if mkt:
                                with games_lock:
                                    game["markets"][tk] = mkt
                                    all_markets[tk] = mkt
                                new_tickers.append(tk)
                    except Exception as e:
                        log.error(f"Error merging {event_ticker}: {e}")
                if new_tickers:
                    pending_ws_subscribes.update(new_tickers)
                    add_log("MERGED", detail=f"{len(new_tickers)} markets merged into {game_id}")
                skipped += 1
                continue

            try:
                sub = base_ev.get("sub_title", "")
                title = base_ev.get("title", "")
                game_name = sub or title or game_id
                # Clean prop type from game name: "PHX @ GSW: Points" → "PHX @ GSW"
                game_name = re.sub(r'\s*:\s*(Points|Rebounds|Assists)$', '', game_name, flags=re.IGNORECASE)

                game_time_iso = None
                kalshi_events = {}

                game = {
                    "game_id": game_id,
                    "game_name": game_name,
                    "game_time": game_time_iso,
                    "nba_game_id": None,
                    "kalshi_events": kalshi_events,
                    "smart_mode": False,
                    "smart_status": None,
                    "settings": {
                        "max_exposure": DEFAULT_MAX_EXPOSURE,
                        "points": dict(_default_prop_settings(), arb_pct=settings.get("arb_pct", DEFAULT_ARB_PCT)),
                        "rebounds": dict(_default_prop_settings(), arb_pct=settings.get("arb_pct", DEFAULT_ARB_PCT)),
                        "assists": dict(_default_prop_settings(), arb_pct=settings.get("arb_pct", DEFAULT_ARB_PCT)),
                    },
                    "player_maxes": {},
                    "markets": {},
                }

                new_tickers = []
                for ev, pt in ev_list:
                    event_ticker = ev.get("event_ticker", "")
                    kalshi_events[pt] = event_ticker
                    try:
                        evt_resp = kalshi.kalshi_get("/events/" + event_ticker)
                        markets_data = evt_resp.get("markets", [])

                        if not game_time_iso and markets_data:
                            close_time = markets_data[0].get("close_time") or markets_data[0].get("expected_expiration_time")
                            if close_time:
                                game["game_time"] = close_time
                                game_time_iso = close_time

                        with games_lock:
                            for mkt_info in markets_data:
                                tk = mkt_info["ticker"]
                                if tk in all_markets:
                                    continue
                                mkt = _build_market_entry(mkt_info, game_id, pt)
                                if mkt:
                                    game["markets"][tk] = mkt
                                    all_markets[tk] = mkt
                                    new_tickers.append(tk)
                    except Exception as e:
                        log.error(f"Error loading {event_ticker}: {e}")
                        errors += 1

                with games_lock:
                    games[game_id] = game

                if new_tickers:
                    pending_ws_subscribes.update(new_tickers)
                if not ws_connected:
                    start_ws_thread()

                prop_counts = {}
                for tk in new_tickers:
                    pt = all_markets[tk].get("prop_type", "?")
                    prop_counts[pt] = prop_counts.get(pt, 0) + 1
                add_log("LOADED", detail=f"{len(new_tickers)} markets ({prop_counts}) from {game_name}")
                loaded += 1

            except Exception as e:
                log.error(f"Error loading matchup {matchup_key}: {e}")
                errors += 1

        # Start pollers if not running
        boltodds_feed.poll_interval = settings.get("ob_poll_interval", 0.5)
        # Lane 1 disabled — heartbeat handles everything
        # boltodds_feed.on_odds_change = _on_odds_change
        boltodds_feed.start()
        nba_feed.start_game_poller()
        nba_feed.start_pbp_poller()
        nba_feed.start_cdn_status_poller()

        # Match ESPN game IDs for live phase detection
        try:
            espn_games = nba_feed.get_todays_games()
            with games_lock:
                for gid, game in games.items():
                    if game.get("nba_game_id"):
                        continue
                    gname = game.get("game_name", "").upper()
                    for eg in espn_games:
                        away = eg.get("away_abbr", "").upper()
                        home = eg.get("home_abbr", "").upper()
                        if away and home and away in gname and home in gname:
                            game["nba_game_id"] = eg["game_id"]
                            game["_home_full"] = eg.get("home", "")
                            game["_away_full"] = eg.get("away", "")
                            nba_feed.register_nba_game_id(gid, eg["game_id"])
                            log.info(f"ESPN matched: {game['game_name']} → ESPN {eg['game_id']}")
                            break
        except Exception as e:
            log.warning(f"ESPN game matching failed (non-fatal): {e}")

        # Resolve + subscribe Bolt LS for each matched game (clock-state lead).
        # Best-effort: a missing Bolt match silently falls back to CDN-only.
        try:
            with games_lock:
                bolt_targets = [
                    (gid, g.get("nba_game_id"), g.get("_home_full"), g.get("_away_full"))
                    for gid, g in games.items()
                    if g.get("nba_game_id") and g.get("_home_full") and g.get("_away_full")
                ]
            for gid, espn_gid, home_full, away_full in bolt_targets:
                try:
                    nba_cdn_id = nba_feed._nba_game_ids.get(str(espn_gid))
                    bolt_key = boltodds_ls_feed.resolve_bolt_key(home_full, away_full)
                    if bolt_key:
                        boltodds_ls_feed.subscribe(bolt_key)
                        boltodds_pbp_feed.subscribe(bolt_key)  # v2: also PBP
                        # Register under every ID we have so lookups are
                        # tolerant of ESPN→NBA-CDN resolution race.
                        nba_feed.register_bolt_key(espn_gid, bolt_key)
                        if nba_cdn_id and str(nba_cdn_id) != str(espn_gid):
                            nba_feed.register_bolt_key(nba_cdn_id, bolt_key)
                        log.info(f"Bolt LS+PBP subscribed: {bolt_key} -> espn={espn_gid} cdn={nba_cdn_id or 'pending'}")
                    else:
                        log.info(f"Bolt LS: no match for {away_full} @ {home_full} today — CDN-only")
                except Exception as e:
                    log.warning(f"Bolt LS setup failed for {gid}: {e}")
        except Exception as e:
            log.warning(f"Bolt LS bulk setup failed (non-fatal): {e}")

        # Match OddsPapi fixtures for clock tracking
        try:
            op_fixtures = op_clocks.find_nba_fixtures()
            if not op_fixtures:
                # Fallback: use WS-discovered fixtures (if WS already connected)
                op_fixtures = op_clocks.get_ws_fixtures()
                if op_fixtures:
                    log.info(f"OP fixture matching: using {len(op_fixtures)} WS-discovered fixtures (REST failed)")
            if op_fixtures:
                matched = _match_op_fixtures(op_fixtures)
                log.info(f"OP fixture matching: {matched} games matched")
                if op_clocks.fixture_map:
                    op_clocks.subscribe_fixtures(list(op_clocks.fixture_map.keys()))
            op_clocks.start()
        except Exception as e:
            log.warning(f"OP fixture matching failed (non-fatal): {e}")

        return jsonify({
            "ok": True,
            "events_found": len(today_events),
            "loaded": loaded,
            "skipped": skipped,
            "errors": errors,
            "total_markets": len(all_markets),
        })

    except Exception as e:
        log.error(f"load_today error: {e}")
        return jsonify({"error": str(e)}), 500


def _match_op_fixtures(op_fixtures):
    """Match OddsPapi fixtures to loaded games. Returns count matched."""
    matched = 0
    with games_lock:
        for gid, game in games.items():
            if game.get("op_fixture_id"):
                continue
            gname = game.get("game_name", "")
            fid = op_clocks.match_fixture_to_game(op_fixtures, gname)
            if fid:
                game["op_fixture_id"] = fid
                op_clocks.fixture_map[fid] = gname
                log.info(f"OP fixture matched: {gname} → {fid}")
                matched += 1
    return matched


@app.route("/api/op/rematch", methods=["GET", "POST"])
def op_rematch():
    """Re-attempt OddsPapi fixture matching. GET or POST."""
    rest_fixtures = op_clocks.find_nba_fixtures()
    ws_fixtures = op_clocks.get_ws_fixtures()
    all_fixtures = rest_fixtures or ws_fixtures or []
    matched = _match_op_fixtures(all_fixtures) if all_fixtures else 0
    # Subscribe WS to matched fixture IDs
    if op_clocks.fixture_map:
        op_clocks.subscribe_fixtures(list(op_clocks.fixture_map.keys()))
    # Show game names for debugging
    game_names = [g.get("game_name", gid) for gid, g in games.items()]
    fixture_names = [f"{f['name']} (id={f['id']})" for f in all_fixtures]
    return jsonify({"ok": True, "matched": matched, "rest_found": len(rest_fixtures), "ws_found": len(ws_fixtures),
                    "game_names": game_names, "fixture_names": fixture_names, "fixture_map": dict(op_clocks.fixture_map)})


@app.route("/api/op/status")
def op_status():
    """OddsPapi clocks status."""
    return jsonify({
        "clocks": op_clocks.get_status(),
        "all_clocks": op_clocks.get_all_clocks(),
        "ws_fixtures": op_clocks.get_ws_fixtures(),
        "fixture_map": dict(op_clocks.fixture_map),
    })


@app.route("/api/markets")
def get_markets():
    with games_lock:
        result = {}
        dk_w = settings.get("dk_weight", 50)
        for tk, mkt in all_markets.items():
            # Read cached values — ceiling computed by heartbeat, not here
            blended, dk_odds, fd_odds = get_blended_odds(mkt["player"], dk_w, market_line=mkt.get("line"), prop_type=mkt.get("prop_type","points"))

            row = dict(mkt)
            row["blended_odds"] = blended
            row["dk_odds"] = dk_odds
            row["fd_odds"] = fd_odds
            # 2-way devig info from odds_feed
            best_odds = odds_feed.player_odds.get(_odds_key(mkt["player"], mkt.get("line")), {})
            fd_detail = _find_detail(mkt["player"], mkt.get("line"), mkt.get("prop_type","points")).get("fanduel", {})
            row["fd_under_odds"] = fd_detail.get("under_odds")
            row["fd_vig"] = fd_detail.get("vig")
            row["has_under"] = fd_detail.get("has_under", False)
            row["fair_yes_2way"] = fd_detail.get("fair_yes") if fd_detail.get("has_under") else None
            row["fair_no_2way"] = fd_detail.get("fair_no") if fd_detail.get("has_under") else None
            row["best_bid_yes"] = mkt.get("best_bid_yes", 0)
            row["best_bid_no"] = mkt.get("best_bid_no", 0)
            row["best_no_bid"] = mkt.get("best_bid_no", 0) or None
            row["order_no_price"] = mkt.get("order_price")
            row["order_no_size"] = mkt.get("size") if mkt.get("order_id") else None
            row["position_no"] = mkt.get("position_no", 0)
            row["position_avg_no"] = mkt.get("position_avg_no", 0)
            result[tk] = row
        return jsonify(result)


@app.route("/api/markets/<ticker>/update", methods=["POST"])
def update_market(ticker):
    ticker = ticker.upper()
    data = request.json
    with games_lock:
        if ticker not in all_markets:
            return jsonify({"error": "Market not found"}), 404
        mkt = all_markets[ticker]
        if "offset" in data:
            mkt["offset"] = int(data["offset"])
        if "size" in data:
            mkt["size"] = int(data["size"])
        if "ceil_no" in data:
            mkt["ceil_no"] = int(data["ceil_no"])
        if "fill_target" in data:
            mkt["fill_target"] = int(data["fill_target"])
            if mkt["status"] == "FILLED" and mkt.get("position_no", 0) < mkt["fill_target"]:
                mkt["status"] = "JUMPING" if mkt["active"] else "OFF"
        cancel_oid = None
        if "active" in data:
            mkt["active"] = bool(data["active"])
            if mkt["active"]:
                mkt["status"] = "JUMPING"
                mkt["last_order_time"] = 0
                add_log("ACTIVATED", ticker)
            else:
                if mkt.get("order_id"):
                    if not str(mkt["order_id"]).startswith("paper_"):
                        cancel_oid = mkt["order_id"]
                    mkt["order_id"] = None
                    mkt["order_price"] = None
                mkt["status"] = "OFF"
                add_log("DEACTIVATED", ticker)
    if cancel_oid:
        try:
            kalshi.cancel_order(cancel_oid)
        except Exception as e:
            log.warning(f"Cancel on deactivate failed: {e}")
    return jsonify({"ok": True})


@app.route("/api/markets/<ticker>/pull", methods=["POST"])
def pull_market(ticker):
    ticker = ticker.upper()
    cancel_oid = None
    with games_lock:
        if ticker not in all_markets:
            return jsonify({"error": "Market not found"}), 404
        mkt = all_markets[ticker]
        pulled = 0
        if mkt.get("order_id"):
            if not str(mkt["order_id"]).startswith("paper_"):
                cancel_oid = mkt["order_id"]
            mkt["order_id"] = None
            mkt["order_price"] = None
            pulled = 1
            add_log("PULL_STRIKE", ticker)
    if cancel_oid:
        try:
            kalshi.cancel_order(cancel_oid)
        except Exception as e:
            log.warning(f"Cancel on pull failed: {e}")
    return jsonify({"ok": True, "pulled": pulled})


@app.route("/api/orders/<order_id>/cancel", methods=["POST"])
def cancel_single_order(order_id):
    try:
        ok = kalshi.cancel_order(order_id)
        with games_lock:
            for tk, mkt in all_markets.items():
                if mkt.get("order_id") == order_id:
                    mkt["order_id"] = None
                    break
        add_log("CANCELLED", detail=f"order {order_id}")
        return jsonify({"ok": ok})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/start-all", methods=["POST"])
def start_all():
    body = request.get_json(silent=True) or {}
    prop_filter = body.get("prop_type")  # None = all, "points"/"rebounds"/"assists"
    try:
        reconcile_resting_orders()
    except Exception as e:
        log.error(f"start_all reconcile failed: {e}")
    started = 0
    with games_lock:
        for tk, mkt in all_markets.items():
            if prop_filter and mkt.get("prop_type", "points") != prop_filter:
                continue
            if not mkt["prop_done"] and mkt["ceil_no"] > 0:
                mkt["active"] = True
                mkt["status"] = "JUMPING"
                started += 1
    label = prop_filter.upper() if prop_filter else "ALL"
    add_log("START_ALL", detail=f"Started {started} {label} bots")
    return jsonify({"ok": True, "started": started})


@app.route("/api/stop-all", methods=["POST"])
def stop_all():
    global all_paused
    body = request.get_json(silent=True) or {}
    prop_filter = body.get("prop_type")  # None = all
    stopped = 0
    to_cancel = []
    with games_lock:
        for tk, mkt in all_markets.items():
            if prop_filter and mkt.get("prop_type", "points") != prop_filter:
                continue
            mkt["active"] = False
            if mkt.get("order_id"):
                if not str(mkt["order_id"]).startswith("paper_"):
                    to_cancel.append(mkt["order_id"])
                mkt["order_id"] = None
                mkt["order_price"] = None
            if not mkt["prop_done"]:
                mkt["status"] = "STOPPED"
            stopped += 1
    # Cancel orders OUTSIDE lock so UI/heartbeat aren't blocked
    for oid in to_cancel:
        try:
            kalshi.cancel_order(oid)
        except Exception as e:
            log.warning(f"Cancel in stop-all failed: {e}")
    if not prop_filter:
        all_paused = False
    stop_time = time.time()
    label = prop_filter.upper() if prop_filter else "ALL"
    add_log("STOP_ALL", detail=f"Stopped {stopped} {label} bots")
    threading.Thread(target=_verify_clean, args=(None, stop_time), daemon=True).start()
    return jsonify({"ok": True, "stopped": stopped})


def _verify_clean(game_id=None, stop_time=None):
    """Poll resting orders 5s after stop to catch stragglers."""
    time.sleep(5)
    try:
        if stop_time and any(mkt.get("active") for mkt in all_markets.values()):
            add_log("VERIFY_SKIPPED", detail="bots restarted during verify wait — skipping sweep")
            return
        orders = kalshi.get_resting_orders()
        if game_id:
            target_tickers = set(games.get(game_id, {}).get("markets", {}).keys())
        else:
            target_tickers = set(all_markets.keys())
        stragglers = [o for o in orders if o.get("ticker", "") in target_tickers]
        if stragglers:
            for o in stragglers:
                tk = o.get("ticker", "")
                with games_lock:
                    mkt = all_markets.get(tk, {})
                    if mkt.get("active") and mkt.get("order_id") == o.get("order_id"):
                        continue
                kalshi.cancel_order(o["order_id"])
                add_log("STRAGGLER_KILL", tk, f"order {o['order_id']} cancelled in verification sweep")
                with games_lock:
                    if tk in all_markets:
                        all_markets[tk]["order_id"] = None
                        all_markets[tk]["order_price"] = None
    except Exception as e:
        log.error(f"Straggler verification failed: {e}")


# ---------------------------------------------------------------------------
# Per-game API endpoints
# ---------------------------------------------------------------------------

@app.route("/api/games/data")
def get_games_data():
    # Snapshot under lock, compute outside — minimize lock hold time
    with games_lock:
        snapshot = {}
        for gid, game in games.items():
            _skip_game_keys = {"markets", "paused_tickers"}
            g_snap = {"game": {k: (list(v) if isinstance(v, set) else v) for k, v in game.items() if k not in _skip_game_keys}, "markets": {}}
            for tk, mkt in game["markets"].items():
                m_copy = {k: (list(v) if isinstance(v, set) else v) for k, v in mkt.items() if k != "orderbook"}
                g_snap["markets"][tk] = m_copy
            snapshot[gid] = g_snap

    # Now build response WITHOUT holding the lock
    result = {}
    dk_w = settings.get("dk_weight", 50)
    for gid, g_snap in snapshot.items():
        game = g_snap["game"]
        game_markets = {}
        # Get per-game arb_pct for ceiling recompute
        g_settings = game.get("settings", {})
        # On-court lookup (phase a observe-only) — one snapshot per game
        nba_gid_for_oc = game.get("nba_game_id")
        oc_lookup = nba_feed.get_on_court_lookup(str(nba_gid_for_oc)) if nba_gid_for_oc else {}
        # Current game phase — needed on each market so the UI can decide
        # whether to show the PLAYING banner (on_court=True AND phase is
        # active play, i.e., LIVE or OVERTIME — not timeout / quarter break).
        _g_info = nba_feed.get_game_info(str(nba_gid_for_oc)) if nba_gid_for_oc else {}
        game_phase_now = _g_info.get("phase", "") or ""
        for tk, mkt in g_snap["markets"].items():
            pt = mkt.get("prop_type", "points")
            blended, dk_odds, fd_odds = get_blended_odds(mkt.get("player",""), dk_w, market_line=mkt.get("line"), prop_type=pt)
            # Recompute ceiling with current settings (so UI reflects changes immediately)
            prop_s = g_settings.get(pt, g_settings.get("points", {}))
            g_arb = prop_s.get("arb_pct", settings.get("arb_pct", 0))
            g_offset = mkt.get("offset", prop_s.get("offset", 0))
            new_ceil = compute_ceiling(mkt.get("player", ""), g_offset, arb_pct=g_arb, market_line=mkt.get("line"), prop_type=pt)
            if new_ceil is not None:
                mkt["ceil_no"] = new_ceil
            row = mkt
            row["blended_odds"] = blended
            row["dk_odds"] = dk_odds
            row["fd_odds"] = fd_odds
            row["best_bid_yes"] = mkt.get("best_bid_yes", 0)
            row["best_bid_no"] = mkt.get("best_bid_no", 0)
            row["best_no_bid"] = mkt.get("best_bid_no", 0) or None
            row["order_no_price"] = mkt.get("order_price")
            row["order_no_size"] = mkt.get("size") if mkt.get("order_id") else None
            row["position_no"] = mkt.get("position_no", 0)
            row["position_avg_no"] = mkt.get("position_avg_no", 0)
            # On-court status via player directory: resolve name → personId once
            # (cached on mkt["_pid"]) and then O(1) dict hit each tick. True=floor,
            # False=bench, None=unknown (unresolved name or no boxscore yet).
            pid = mkt.get("_pid")
            if pid is None and nba_gid_for_oc:
                player_full = (mkt.get("player", "") or "").strip()
                if player_full:
                    pid = nba_feed.resolve_player_in_game(str(nba_gid_for_oc), player_full)
                    if pid is not None:
                        mkt["_pid"] = pid
            row["on_court"] = oc_lookup.get(pid) if pid is not None else None
            # Stamp current game phase so UI can render PLAYING banner only
            # when the game is in active play (not paused/break/foul-shot).
            row["game_phase"] = game_phase_now
            game_markets[tk] = row

        # Aggregates
        mkts = list(g_snap["markets"].values())
        any_active = any(m.get("active") for m in mkts)
        jumping_count = sum(1 for m in mkts if (m.get("status") or "") == "JUMPING")
        total_count = sum(1 for m in mkts if not m.get("prop_done"))

        evs = []
        for m in mkts:
            imp_yes = m.get("implied_yes") or 0
            ceil_no = m.get("ceil_no") or 0
            if imp_yes <= 0 or ceil_no <= 0:
                continue
            # implied_yes is already devigged — use directly
            fair_no = 100 - imp_yes
            if fair_no and ceil_no:
                evs.append((fair_no - ceil_no) / ceil_no * 100)
        avg_min_ev = round(sum(evs) / len(evs), 2) if evs else None

        # Game phase from ESPN/NBA feed
        nba_gid = game.get("nba_game_id")
        game_info = nba_feed.get_game_info(str(nba_gid)) if nba_gid else {}

        # OddsPapi clock state
        op_fid = game.get("op_fixture_id")
        op_clock = op_clocks.get_clock(op_fid) if op_fid else None
        op_stopped = op_clock.get("stopped") if op_clock else None

        result[gid] = {
            "game_id": gid,
            "game_name": game.get("game_name", gid),
            "game_time": game.get("game_time"),
            "nba_game_id": nba_gid,
            "prop_type": game.get("prop_type", "points"),
            "paused": game.get("paused", False),
            "smart_mode": game.get("smart_mode", False),
            "smart_status": game.get("smart_status", ""),
            "any_active": any_active,
            "jumping_count": jumping_count,
            "total_count": total_count,
            "avg_min_ev": avg_min_ev,
            "settings": game.get("settings", {}),
            "player_maxes": game.get("player_maxes", {}),
            "game_phase": game_info.get("phase", ""),
            "game_clock": game_info.get("clock", ""),
            "game_period": game_info.get("period", 0),
            "game_detail": game_info.get("detail", ""),
            "game_score": f"{game_info.get('away_score', 0)}-{game_info.get('home_score', 0)}" if game_info.get("phase") not in ("PRE", "") else "",
            "op_clock_stopped": op_stopped,
            "op_clock_period": op_clock.get("period") if op_clock else None,
            "op_clock_remaining": op_clock.get("remaining_period") if op_clock else None,
            "markets": game_markets,
        }
    return jsonify(result)


@app.route("/api/games/<game_id>/start-all", methods=["POST"])
def game_start_all(game_id):
    if game_id not in games:
        return jsonify({"error": "Game not found"}), 404
    started = 0
    with games_lock:
        for tk, mkt in games[game_id]["markets"].items():
            if not mkt["prop_done"] and mkt["ceil_no"] > 0:
                mkt["active"] = True
                mkt["status"] = "JUMPING"
                mkt["last_order_time"] = 0
                started += 1
    add_log("START_ALL", detail=f"Started {started} bots in {game_id}")
    return jsonify({"ok": True, "started": started})


@app.route("/api/games/<game_id>/stop-all", methods=["POST"])
def game_stop_all(game_id):
    if game_id not in games:
        return jsonify({"error": "Game not found"}), 404
    # Phase 1: collect under lock, no HTTP
    to_cancel = []
    stopped = 0
    with games_lock:
        for tk, mkt in games[game_id]["markets"].items():
            mkt["active"] = False
            if mkt.get("order_id"):
                if not str(mkt["order_id"]).startswith("paper_"):
                    to_cancel.append(mkt["order_id"])
                mkt["order_id"] = None
                mkt["order_price"] = None
            if not mkt["prop_done"]:
                mkt["status"] = "STOPPED"
            stopped += 1
    # Phase 2: HTTP outside lock
    for oid in to_cancel:
        try:
            kalshi.cancel_order(oid)
        except Exception:
            pass
    add_log("STOP_ALL", detail=f"Stopped {stopped} bots in {game_id}")
    threading.Thread(target=_verify_clean, args=(game_id,), daemon=True).start()
    return jsonify({"ok": True, "stopped": stopped})


@app.route("/api/games/<game_id>/pause", methods=["POST"])
def game_pause(game_id):
    if game_id not in games:
        return jsonify({"error": "Game not found"}), 404
    game = games[game_id]
    is_paused = game.get("paused", False)
    count = 0
    to_cancel = []
    with games_lock:
        if not is_paused:
            game["paused_tickers"] = set()
            for tk, mkt in game["markets"].items():
                if mkt["active"] and not mkt["prop_done"]:
                    game["paused_tickers"].add(tk)
                    if mkt.get("order_id") and not str(mkt["order_id"]).startswith("paper_"):
                        to_cancel.append(mkt["order_id"])
                    mkt["order_id"] = None
                    mkt["order_price"] = None
                    mkt["active"] = False
                    mkt["status"] = "PAUSED"
                    count += 1
            game["paused"] = True
            add_log("PAUSE", detail=f"Paused {count} bots in {game_id}")
        else:
            for tk in game.get("paused_tickers", set()):
                if tk in game["markets"]:
                    mkt = game["markets"][tk]
                    if not mkt["prop_done"]:
                        mkt["active"] = True
                        mkt["status"] = "JUMPING"
                        count += 1
            game["paused"] = False
            game["paused_tickers"] = set()
            add_log("RESUME", detail=f"Resumed {count} bots in {game_id}")
    for oid in to_cancel:
        try:
            kalshi.cancel_order(oid)
        except Exception as e:
            log.warning(f"Cancel on game pause failed: {e}")
    return jsonify({"ok": True, "paused": game.get("paused", False), "count": count})


@app.route("/api/games/<game_id>/settings", methods=["POST"])
def game_settings(game_id):
    if game_id not in games:
        return jsonify({"error": "Game not found"}), 404
    data = request.json or {}
    prop_type = data.pop("prop_type", None)
    with games_lock:
        _ensure_nested_settings(games[game_id])
        gs = games[game_id]["settings"]
        # max_exposure is shared across all prop types
        if "max_exposure" in data:
            gs["max_exposure"] = int(data["max_exposure"])
        # Per-prop settings
        if prop_type and prop_type in PROP_TYPES:
            pt_gs = gs.setdefault(prop_type, dict(_default_prop_settings()))
            for k in PROP_SETTING_KEYS:
                if k in data:
                    pt_gs[k] = int(data[k]) if isinstance(data[k], (int, float)) else data[k]
        else:
            # No prop_type specified — apply to all (backward compat)
            for pt in PROP_TYPES:
                pt_gs = gs.setdefault(pt, dict(_default_prop_settings()))
                for k in PROP_SETTING_KEYS:
                    if k in data:
                        pt_gs[k] = int(data[k]) if isinstance(data[k], (int, float)) else data[k]
    # Immediately recompute ceilings for all markets in this game
    # so UI reflects the new settings without waiting for slow heartbeat
    dk_w = settings.get("dk_weight", 50)
    target_pt = prop_type or "points"
    with games_lock:
        for tk, mkt in games[game_id].get("markets", {}).items():
            pt = mkt.get("prop_type", "points")
            if prop_type and pt != target_pt:
                continue
            gs_local = _get_prop_settings(games[game_id], pt)
            g_arb = gs_local.get("arb_pct", settings.get("arb_pct", 0))
            blended_imp = get_blended_implied(mkt["player"], dk_w, market_line=mkt.get("line"), prop_type=pt)
            if blended_imp is not None and blended_imp > 0:
                mkt["implied_yes"] = round(blended_imp, 1)
                mkt["implied_no"] = round(100 - blended_imp, 1)
            new_ceil = compute_ceiling(mkt["player"], mkt.get("offset", 0), arb_pct=g_arb, market_line=mkt.get("line"), prop_type=pt)
            if new_ceil is not None:
                mkt["ceil_no"] = new_ceil
            elif new_ceil is None:
                mkt["ceil_no"] = 0
    return jsonify({"ok": True, "settings": games[game_id]["settings"]})


@app.route("/api/games/<game_id>/player-max", methods=["POST"])
def set_player_max(game_id):
    """Set per-player contract max. Body: {player: "name", max: 3000}"""
    if game_id not in games:
        return jsonify({"error": "Game not found"}), 404
    data = request.json or {}
    player = (data.get("player") or "").lower().strip()
    max_val = int(data.get("max", 0))
    if not player:
        return jsonify({"error": "player required"}), 400
    with games_lock:
        if "player_maxes" not in games[game_id]:
            games[game_id]["player_maxes"] = {}
        if max_val <= 0:
            games[game_id]["player_maxes"].pop(player, None)
        else:
            games[game_id]["player_maxes"][player] = max_val
        # Resume any PLAYER_MAX markets for this player if new cap is higher
        for tk, mkt in games[game_id]["markets"].items():
            if (mkt.get("player") or "").lower() == player and mkt.get("status") == "PLAYER_MAX":
                mkt["status"] = "JUMPING"
    return jsonify({"ok": True, "player_maxes": games[game_id].get("player_maxes", {})})


@app.route("/api/games/<game_id>/apply-all", methods=["POST"])
def game_apply_all(game_id):
    if game_id not in games:
        return jsonify({"error": "Game not found"}), 404
    data = request.json or {}
    prop_type = data.pop("prop_type", None)
    applied = 0
    with games_lock:
        for tk, mkt in games[game_id]["markets"].items():
            # Filter by prop_type if specified
            if prop_type and mkt.get("prop_type", "points") != prop_type:
                continue
            if "size" in data:
                mkt["size"] = int(data["size"])
            if "fill_target" in data:
                mkt["fill_target"] = int(data["fill_target"])
                if mkt["status"] == "FILLED" and mkt.get("position_no", 0) < mkt["fill_target"]:
                    mkt["status"] = "JUMPING" if mkt["active"] else "OFF"
            if "offset" in data:
                mkt["offset"] = int(data["offset"])
            if "ceil_no" in data:
                mkt["ceil_no"] = int(data["ceil_no"])
            applied += 1
    return jsonify({"ok": True, "applied": applied})


@app.route("/api/games/<game_id>/smart", methods=["POST"])
def game_smart_toggle(game_id):
    if game_id not in games:
        return jsonify({"error": "Game not found"}), 404
    with games_lock:
        game = games[game_id]
        game["smart_mode"] = not game.get("smart_mode", False)
        if game["smart_mode"]:
            nba_gid = game.get("nba_game_id")
            if nba_gid:
                lifecycle = nba_feed.get_game_lifecycle(str(nba_gid))
                if lifecycle == "Live":
                    game["smart_status"] = "LIVE"
                    on_game_start(nba_gid)
                elif lifecycle == "Final":
                    game["smart_status"] = "FINAL"
                else:
                    game["smart_status"] = "WAITING"
            else:
                game["smart_status"] = "WAITING"
        else:
            game["smart_status"] = None
    status = "ON" if game["smart_mode"] else "OFF"
    add_log("SMART_MODE", detail=f"Smart mode {status} for {game_id}")
    return jsonify({"ok": True, "smart_mode": game["smart_mode"], "smart_status": game.get("smart_status")})


@app.route("/api/games/<game_id>/remove", methods=["POST"])
def game_remove(game_id):
    if game_id not in games:
        return jsonify({"error": "Game not found"}), 404
    removed = 0
    to_cancel = []
    with games_lock:
        game = games[game_id]
        for tk, mkt in game["markets"].items():
            if mkt.get("order_id"):
                if not str(mkt["order_id"]).startswith("paper_"):
                    to_cancel.append(mkt["order_id"])
            mkt["active"] = False
            mkt["order_id"] = None
            mkt["order_price"] = None
            all_markets.pop(tk, None)
            removed += 1
        del games[game_id]
    for oid in to_cancel:
        try:
            kalshi.cancel_order(oid)
        except Exception as e:
            log.warning(f"Cancel on remove failed: {e}")
    add_log("REMOVED", detail=f"Removed {game_id} ({removed} markets)")
    threading.Thread(target=_verify_clean, args=(None,), daemon=True).start()
    return jsonify({"ok": True, "removed": removed})


@app.route("/api/debug/kalshi/<event_ticker>")
def debug_kalshi_event(event_ticker):
    try:
        resp = kalshi.kalshi_get("/events/" + event_ticker)
        return jsonify(resp)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/debug/state")
def debug_state():
    """Debug: full bot state dump — settings, lock status, positions, orders, fills."""
    import traceback
    try:
        # Check if lock is available (non-blocking)
        lock_free = games_lock.acquire(blocking=False)
        if lock_free:
            games_lock.release()

        # Resting orders from Kalshi
        try:
            resting = kalshi.get_resting_orders()
        except Exception as e:
            resting = {"error": str(e)}

        # Positions from Kalshi
        try:
            pos_resp = kalshi.kalshi_get("/portfolio/positions")
            positions = pos_resp.get("market_positions") or pos_resp.get("positions") or []
            # Filter to loaded tickers only
            loaded_tickers = set(all_markets.keys())
            positions = [p for p in positions if (p.get("ticker") or p.get("market_ticker", "")) in loaded_tickers]
        except Exception as e:
            positions = {"error": str(e)}

        # Bot's internal state for each market with an order or position
        market_state = {}
        for tk, mkt in all_markets.items():
            if mkt.get("order_id") or mkt.get("position_no", 0) > 0 or mkt.get("active"):
                market_state[tk] = {
                    "active": mkt.get("active"),
                    "status": mkt.get("status"),
                    "order_id": mkt.get("order_id"),
                    "order_price": mkt.get("order_price"),
                    "position_no": mkt.get("position_no", 0),
                    "position_avg_no": mkt.get("position_avg_no", 0),
                    "ceil_no": mkt.get("ceil_no", 0),
                    "best_bid_no": mkt.get("best_bid_no", 0),
                    "player": mkt.get("player"),
                    "prop_type": mkt.get("prop_type"),
                    "line": mkt.get("line"),
                    "_position_synced": mkt.get("_position_synced"),
                }

        return jsonify({
            "lock_free": lock_free,
            "games_loaded": list(games.keys()),
            "game_settings": {gid: g.get("settings", {}) for gid, g in games.items()},
            "total_markets": len(all_markets),
            "active_markets": sum(1 for m in all_markets.values() if m.get("active")),
            "orders_tracked": sum(1 for m in all_markets.values() if m.get("order_id")),
            "settings": settings,
            "fill_log_count": len(fill_log),
            "last_fill": fill_log[-1] if fill_log else None,
            "kalshi_resting": resting,
            "kalshi_positions": positions,
            "market_state": market_state,
            "ws_connected": ws_connected,
            "paper_mode": paper_mode,
        })
    except Exception as e:
        return jsonify({"error": str(e), "traceback": traceback.format_exc()}), 500


@app.route("/api/fills")
def api_fills():
    with fill_log_lock:
        return jsonify({"fills": list(reversed(fill_log))})


@app.route("/api/admin/backfill_fills", methods=["POST"])
def admin_backfill_fills():
    """One-shot backfill: read local SQLite, POST fills where fills_id IS NULL
    to the nba_dashboard (or FILLS_URL fallback). Idempotent — each successful
    POST writes back the remote id so re-runs skip already-synced rows.

    Auth: X-API-Key must match NBA_DASHBOARD_API_KEY (or FILLS_API_KEY)."""
    provided = request.headers.get("X-API-Key", "")
    if provided != NBA_DASHBOARD_API_KEY and provided != FILLS_API_KEY:
        return jsonify({"error": "unauthorized"}), 401

    url, api_key = _fills_target()
    if not url:
        return jsonify({"error": "no fills target configured"}), 400

    dry_run = request.args.get("dry_run", "false").lower() == "true"
    limit = int(request.args.get("limit", 1000))

    try:
        conn = sqlite3.connect(FILLS_DB)
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT * FROM fills WHERE fills_id IS NULL ORDER BY id ASC LIMIT ?",
            (limit,),
        ).fetchall()
    except Exception as e:
        return jsonify({"error": f"sqlite read failed: {e}"}), 500

    if dry_run:
        conn.close()
        return jsonify({"dry_run": True, "would_backfill": len(rows), "target": url})

    import requests as _req
    import re as _re
    sent, failed, errors = 0, 0, []
    for r in rows:
        ticker = r["ticker"] or ""
        if not ticker.startswith("KXNBA"):
            continue  # dashboard rejects non-NBA tickers
        # Recover ts: some older rows stored time-only (e.g. "03:17:07"). Parse
        # the ticker's YYMMMDD segment (KXNBAPTS-26APR21...) and stitch it.
        raw_ts = r["ts"] or ""
        ts_out = raw_ts
        if raw_ts and "T" not in raw_ts and " " not in raw_ts:
            m = _re.match(r"^KXNBA\w+-(\d{2})([A-Z]{3})(\d{2})", ticker)
            if m:
                yy, mon, dd = m.groups()
                mon_map = {"JAN":"01","FEB":"02","MAR":"03","APR":"04",
                           "MAY":"05","JUN":"06","JUL":"07","AUG":"08",
                           "SEP":"09","OCT":"10","NOV":"11","DEC":"12"}
                mm = mon_map.get(mon, "01")
                ts_out = f"20{yy}-{mm}-{dd}T{raw_ts}+00:00"
            else:
                ts_out = datetime.now(timezone.utc).isoformat()
        payload = {
            "ts": ts_out,
            "player": r["player"] or "",
            "game": "",
            "game_id": r["game_id"] if "game_id" in r.keys() else "",
            "ticker": ticker,
            "line": r["line"],
            "prop_type": r["prop_type"] if "prop_type" in r.keys() else "points",
            "contracts": r["contracts"],
            "fill_price": r["fill_price"],
            "side": "no",
            "role": "Maker",
            "fd_over_odds": None,
            "dk_over_odds": None,
            "fd_implied_yes": None,
            "dk_implied_yes": None,
            "fd_fair_no": r["fd_fair_no"] if "fd_fair_no" in r.keys() else None,
            "dk_fair_no": r["dk_fair_no"] if "dk_fair_no" in r.keys() else None,
            "blended_fair_no": r["fair_no_at_fill"],
            "fd_exponent_used": None,
            "dk_exponent_used": None,
            "devig_method": r["devig_method"] if "devig_method" in r.keys() else None,
            "ev_at_fill": r["ev_at_fill"],
            "ceil_no": r["ceil_no"],
            "ceil_ev": r["ceil_ev"],
            "game_phase": None,
        }
        try:
            resp = _req.post(
                f"{url}/api/fill",
                json=payload,
                headers={"X-API-Key": api_key},
                timeout=10,
            )
            if resp.ok:
                rid = resp.json().get("id")
                if rid:
                    try:
                        conn.execute(
                            "UPDATE fills SET fills_id=? WHERE id=?",
                            (rid, r["id"]),
                        )
                        conn.commit()
                    except Exception:
                        pass
                sent += 1
            else:
                failed += 1
                if len(errors) < 5:
                    errors.append(f"id={r['id']} status={resp.status_code} body={resp.text[:200]}")
        except Exception as e:
            failed += 1
            if len(errors) < 5:
                errors.append(f"id={r['id']} exception={str(e)[:200]}")

    conn.close()
    return jsonify({
        "target": url,
        "scanned": len(rows),
        "sent": sent,
        "failed": failed,
        "errors": errors,
    })


@app.route("/api/odds/bolt_status")
def bolt_status():
    return jsonify(boltodds_feed.get_status())


@app.route("/api/odds/latency")
def odds_latency():
    """Which sportsbook reacts fastest to odds changes."""
    return jsonify(boltodds_feed.get_latency_report())


@app.route("/api/odds/events")
def odds_events():
    """Recent scoring events for the UI ticker."""
    with boltodds_feed._scoring_lock:
        events = list(boltodds_feed.scoring_events[-15:])
    return jsonify({"events": list(reversed(events))})


@app.route("/api/odds/detail")
def odds_detail_dump():
    """Dump per-book odds detail for all players."""
    detail, _ = _get_odds_source()
    with boltodds_feed.odds_lock:
        return jsonify(dict(detail))


@app.route("/api/odds/raw")
def debug_odds_raw():
    """Show raw OddsBlaze response to debug market name format."""
    return jsonify(odds_feed.debug_raw_fetch())


@app.route("/api/odds/debug")
def debug_odds():
    with games_lock:
        market_odds = {}
        dk_w = settings.get("dk_weight", 50)
        for tk, mkt in all_markets.items():
            blended, dk, fd = get_blended_odds(mkt["player"], dk_w, market_line=mkt.get("line"), prop_type=mkt.get("prop_type","points"))
            blended_imp = get_blended_implied(mkt["player"], dk_w, market_line=mkt.get("line"), prop_type=mkt.get("prop_type","points"))
            detail = _find_detail(mkt["player"], mkt.get("line"), mkt.get("prop_type","points"))
            market_odds[tk] = {
                "player": mkt["player"],
                "blended_odds": blended,
                "dk_odds": dk,
                "fd_odds": fd,
                "blended_imp": blended_imp,
                "ceil_no": mkt.get("ceil_no"),
                "detail_keys": list(detail.keys()),
                "detail": detail,
            }
    detail_dict, _ = _get_odds_source()
    return jsonify({
        "markets": market_odds,
        "total_players_in_detail": len(detail_dict),
        "last_poll": odds_feed.last_poll_time,
    })


@app.route("/api/odds/debug2")
def debug_odds2():
    """Debug: show raw feed keys vs market keys per prop type."""
    out = {}
    dk_w = settings.get("dk_weight", 50)
    for pt in ["points", "rebounds", "assists"]:
        detail_dict, odds_dict = _get_odds_source(pt)
        feed_keys = sorted(detail_dict.keys())[:50]
        mkt_keys = []
        with games_lock:
            for tk, mkt in all_markets.items():
                if mkt.get("prop_type") == pt:
                    detail = _find_detail(mkt["player"], mkt.get("line"), pt)
                    fd = detail.get("fanduel", {})
                    dk = detail.get("draftkings", {})
                    blended_imp = get_blended_implied(mkt["player"], dk_w, market_line=mkt.get("line"), prop_type=pt)
                    g = games.get(mkt.get("game_id", ""), {})
                    g_arb = g.get("settings", {}).get("arb_pct", settings.get("arb_pct", 0))
                    ceil_result = compute_ceiling(mkt["player"], mkt.get("offset", 0), arb_pct=g_arb, market_line=mkt.get("line"), prop_type=pt)
                    max_imp = settings.get("max_implied_yes", 95)
                    best_imp = fd.get("implied_yes") or dk.get("implied_yes") or 0
                    mkt_keys.append({
                        "ticker": tk,
                        "player": mkt["player"],
                        "line": mkt.get("line"),
                        "found": bool(detail),
                        "ceil": mkt.get("ceil_no", 0),
                        "ceil_result": ceil_result,
                        "blended_imp": blended_imp,
                        "best_imp": best_imp,
                        "max_imp": max_imp,
                        "fd_odds": fd.get("odds"),
                        "dk_odds": dk.get("odds"),
                        "fd_imp": fd.get("implied_yes"),
                        "dk_imp": dk.get("implied_yes"),
                        "detail_keys": list(detail.keys()),
                        "arb": g_arb,
                    })
        out[pt] = {"feed_keys": feed_keys, "market_lookups": mkt_keys[:20], "feed_total": len(detail_dict)}
    return jsonify(out)


@app.route("/api/balance")
def get_balance():
    return jsonify({"balance": kalshi.get_balance()})


# ---------------------------------------------------------------------------
# P&L API — reads from SQLite fills DB with prop_type filter
# ---------------------------------------------------------------------------

def _today_ts_prefix():
    """Return today's date as YYYY-MM-DD for SQL LIKE filtering."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


@app.route("/api/pnl/summary")
def pnl_summary():
    """Top-bar aggregate stats for a prop type."""
    prop_type = request.args.get("prop_type", "points")
    today_only = request.args.get("today", "1") == "1"
    try:
        conn = sqlite3.connect(FILLS_DB)
        conn.row_factory = sqlite3.Row
        sql = "SELECT * FROM fills WHERE (prop_type = ? OR prop_type IS NULL)"
        params = [prop_type]
        if today_only:
            sql += " AND ts LIKE ?"
            params.append(_today_ts_prefix() + "%")
        rows = conn.execute(sql, params).fetchall()
        conn.close()

        total_fills = len(rows)
        total_contracts = sum((r["contracts"] or 0) for r in rows)
        total_volume_dollars = sum((r["fill_price"] or 0) * (r["contracts"] or 0) / 100 for r in rows)

        ev_fills = [r for r in rows if r["ev_at_fill"] is not None]
        clv10_fills = [r for r in rows if r["clv_10s"] is not None]
        clv60_fills = [r for r in rows if r["clv_60s"] is not None]

        avg_ev_at_fill = sum(r["ev_at_fill"] for r in ev_fills) / len(ev_fills) if ev_fills else 0
        avg_clv_10s = sum(r["clv_10s"] for r in clv10_fills) / len(clv10_fills) if clv10_fills else 0
        avg_clv_60s = sum(r["clv_60s"] for r in clv60_fills) / len(clv60_fills) if clv60_fills else 0

        pos_clv_60s = sum(1 for r in clv60_fills if r["clv_60s"] > 0)
        win_rate = (pos_clv_60s / len(clv60_fills) * 100) if clv60_fills else 0

        # Unrealized P&L estimate using fair_no_60s as best guess of current value
        pnl_est = 0.0
        for r in rows:
            fair = r["fair_no_60s"] if r["fair_no_60s"] else r["fair_no_at_fill"]
            if fair and r["fill_price"] and r["contracts"]:
                pnl_est += (fair - r["fill_price"]) * r["contracts"] / 100

        return jsonify({
            "prop_type": prop_type,
            "total_fills": total_fills,
            "total_contracts": total_contracts,
            "total_volume_dollars": round(total_volume_dollars, 2),
            "avg_ev_at_fill": round(avg_ev_at_fill, 2),
            "avg_clv_10s": round(avg_clv_10s, 2),
            "avg_clv_60s": round(avg_clv_60s, 2),
            "win_rate": round(win_rate, 1),
            "pnl_estimate_dollars": round(pnl_est, 2),
        })
    except Exception as e:
        log.error(f"pnl_summary error: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/pnl/top_positions")
def pnl_top_positions():
    """Top N players by total contracts held for this prop type.
    Uses live all_markets state for current positions + fair_no."""
    prop_type = request.args.get("prop_type", "points")
    limit = int(request.args.get("limit", 10))
    dk_w = settings.get("dk_weight", 50)

    player_agg = {}  # player → {contracts, cost, lines: []}
    with games_lock:
        for tk, mkt in all_markets.items():
            if mkt.get("prop_type", "points") != prop_type:
                continue
            pos = mkt.get("position_no", 0)
            if pos <= 0:
                continue
            player = mkt.get("player", "")
            avg_price = mkt.get("position_avg_no", 0)
            line = mkt.get("line")

            # Current fair NO
            blended = get_blended_implied(player, dk_w, market_line=line)
            current_fair_no = round(100 - blended, 2) if blended else None

            unrealized = None
            if current_fair_no and avg_price:
                unrealized = (current_fair_no - avg_price) * pos / 100

            if player not in player_agg:
                player_agg[player] = {
                    "player": player,
                    "total_contracts": 0,
                    "total_cost": 0,
                    "total_unrealized": 0,
                    "lines": [],
                    "game_id": mkt.get("game_id", ""),
                }
            player_agg[player]["total_contracts"] += pos
            player_agg[player]["total_cost"] += avg_price * pos / 100
            if unrealized is not None:
                player_agg[player]["total_unrealized"] += unrealized
            player_agg[player]["lines"].append({
                "ticker": tk,
                "line": line,
                "contracts": pos,
                "avg_price": avg_price,
                "fair_no": current_fair_no,
                "ceil_no": mkt.get("ceil_no", 0),
                "unrealized": round(unrealized, 2) if unrealized is not None else None,
            })

    for p in player_agg.values():
        p["avg_price"] = round(p["total_cost"] * 100 / p["total_contracts"], 1) if p["total_contracts"] else 0
        p["total_cost"] = round(p["total_cost"], 2)
        p["total_unrealized"] = round(p["total_unrealized"], 2)

    sorted_players = sorted(player_agg.values(), key=lambda x: x["total_contracts"], reverse=True)
    return jsonify({"positions": sorted_players[:limit]})


@app.route("/api/pnl/by_game")
def pnl_by_game():
    """Positions grouped by game, with player expansion data.
    Pulls from live all_markets (current state), not historical fills."""
    prop_type = request.args.get("prop_type", "points")
    dk_w = settings.get("dk_weight", 50)

    out = {}  # game_id → {game_name, players: {player: {total, lines: []}}}
    with games_lock:
        for gid, game in games.items():
            if game.get("prop_type", "points") != prop_type:
                continue
            players_map = {}
            for tk, mkt in game.get("markets", {}).items():
                pos = mkt.get("position_no", 0)
                if pos <= 0:
                    continue
                player = mkt.get("player", "")
                line = mkt.get("line")
                avg_price = mkt.get("position_avg_no", 0)

                blended = get_blended_implied(player, dk_w, market_line=line)
                current_fair_no = round(100 - blended, 2) if blended else None
                ev_now = None
                if current_fair_no and avg_price:
                    ev_now = round((current_fair_no - avg_price) / avg_price * 100, 2)
                unrealized = None
                if current_fair_no and avg_price:
                    unrealized = round((current_fair_no - avg_price) * pos / 100, 2)

                if player not in players_map:
                    players_map[player] = {
                        "player": player,
                        "total_contracts": 0,
                        "total_exposure": 0,
                        "total_unrealized": 0,
                        "lines": [],
                    }
                players_map[player]["total_contracts"] += pos
                players_map[player]["total_exposure"] += round(avg_price * pos / 100, 2)
                if unrealized is not None:
                    players_map[player]["total_unrealized"] += unrealized
                players_map[player]["lines"].append({
                    "ticker": tk,
                    "line": line,
                    "contracts": pos,
                    "avg_price": avg_price,
                    "fair_no": current_fair_no,
                    "ceil_no": mkt.get("ceil_no", 0),
                    "ev_pct": ev_now,
                    "unrealized": unrealized,
                })

            for p in players_map.values():
                p["total_exposure"] = round(p["total_exposure"], 2)
                p["total_unrealized"] = round(p["total_unrealized"], 2)
                p["lines"].sort(key=lambda x: x.get("line") or 0)

            if players_map:
                out[gid] = {
                    "game_id": gid,
                    "game_name": game.get("game_name", gid),
                    "game_time": game.get("game_time"),
                    "players": sorted(players_map.values(), key=lambda x: x["total_contracts"], reverse=True),
                }
    return jsonify({"games": out})


@app.route("/api/pnl/positions")
def pnl_positions():
    """Full position view: game → prop_type → players sorted by exposure. With live stats."""
    dk_w = settings.get("dk_weight", 50)
    player_stats_all = nba_feed.get_player_stats()

    result = []
    with games_lock:
        for gid, game in games.items():
            game_entry = {
                "game_id": gid,
                "game_name": game.get("game_name", gid),
                "game_phase": "",
                "game_score": "",
                "prop_types": {},
            }
            nba_gid = game.get("nba_game_id")
            if nba_gid:
                gi = nba_feed.get_game_info(str(nba_gid))
                game_entry["game_phase"] = gi.get("phase", "")
                if gi.get("phase") not in ("PRE", ""):
                    game_entry["game_score"] = f"{gi.get('away_score',0)}-{gi.get('home_score',0)}"

            for pt in PROP_TYPES:
                players_map = {}
                for tk, mkt in game.get("markets", {}).items():
                    if mkt.get("prop_type", "points") != pt:
                        continue
                    pos = mkt.get("position_no", 0)
                    if pos <= 0:
                        continue
                    player = mkt.get("player", "")
                    line = mkt.get("line")
                    avg_price = mkt.get("position_avg_no", 0)

                    fair_no = None
                    blended = get_blended_implied(player, dk_w, market_line=line, prop_type=pt)
                    if blended:
                        fair_no = round(100 - blended, 2)
                    ev_now = round((fair_no - avg_price) / avg_price * 100, 2) if fair_no and avg_price else None
                    unrealized = round((fair_no - avg_price) * pos / 100, 2) if fair_no and avg_price else None

                    # Live stat for this player
                    stat_key = {"points": "points", "rebounds": "rebounds", "assists": "assists"}.get(pt, "points")
                    live_stat = nba_feed.get_player_stat(player, stat_key)

                    if player not in players_map:
                        players_map[player] = {
                            "player": player,
                            "total_contracts": 0,
                            "total_exposure": 0,
                            "total_unrealized": 0,
                            "live_stat": live_stat,
                            "lines": [],
                        }
                    elif live_stat is not None:
                        players_map[player]["live_stat"] = live_stat

                    players_map[player]["total_contracts"] += pos
                    players_map[player]["total_exposure"] += round(avg_price * pos / 100, 2)
                    if unrealized is not None:
                        players_map[player]["total_unrealized"] += unrealized
                    players_map[player]["lines"].append({
                        "line": line,
                        "contracts": pos,
                        "avg_price": avg_price,
                        "fair_no": fair_no,
                        "ev_pct": ev_now,
                        "unrealized": unrealized,
                    })

                if players_map:
                    for p in players_map.values():
                        p["total_exposure"] = round(p["total_exposure"], 2)
                        p["total_unrealized"] = round(p["total_unrealized"], 2)
                        p["lines"].sort(key=lambda x: x.get("line") or 0)
                    game_entry["prop_types"][pt] = sorted(
                        players_map.values(), key=lambda x: x["total_exposure"], reverse=True)

            if any(game_entry["prop_types"].values()):
                result.append(game_entry)

    return jsonify({"games": result, "ts": time.time()})


# ---------------------------------------------------------------------------
# Live P&L — mark-to-market of every open position using live OddsBlaze odds.
# Single endpoint returns everything the page needs (summary + games + lines).
# Pattern: snapshot under games_lock, build response outside.
# ---------------------------------------------------------------------------

# NBA: 4 quarters × 12 min = 2880s regulation. OT periods are 5 min each.
NBA_REGULATION_SECS = 4 * 12 * 60
NBA_QUARTER_SECS = 12 * 60
NBA_OT_SECS = 5 * 60


def _seconds_remaining_in_game(period, clock, phase):
    """Estimate seconds left until game end. Returns None if undeterminable.
    For OT we only count the current OT period — can't predict further OTs."""
    if phase == "FINAL":
        return 0.0
    try:
        period = int(period or 0)
    except Exception:
        period = 0
    if period < 1:
        return None  # pre-game
    sec_in_period = 0.0
    try:
        s = str(clock or "").strip()
        if ":" in s:
            mm, ss = s.split(":", 1)
            sec_in_period = int(mm) * 60 + float(ss)
        elif s:
            sec_in_period = float(s)
    except Exception:
        return None
    if period <= 4:
        full_quarters_left = 4 - period
        return full_quarters_left * NBA_QUARTER_SECS + sec_in_period
    return sec_in_period  # OT


def _poisson_hit_prob(rate_per_sec, sec_remaining, need_more):
    """P(Poisson(lambda=rate*sec) >= need_more). Direct CDF for small need,
    normal approximation otherwise."""
    if need_more <= 0:
        return 1.0
    if sec_remaining <= 0 or rate_per_sec <= 0:
        return 0.0
    lam = rate_per_sec * sec_remaining
    if need_more <= 80 and lam <= 300:
        term = math.exp(-lam)
        cdf = term
        for k in range(1, need_more):
            term *= lam / k
            cdf += term
        return max(0.0, min(1.0, 1.0 - cdf))
    sigma = math.sqrt(lam) if lam > 0 else 1e-9
    z = (need_more - 0.5 - lam) / sigma
    return max(0.0, min(1.0, 0.5 * math.erfc(z / math.sqrt(2))))


def compute_hit_prob(live_stat, line, sec_remaining, sec_total=NBA_REGULATION_SECS, book_p_yes=None):
    """Probability the over-line hits (final stat > line). Blends Poisson-pace
    with book-implied for early-game stability — full pace weight after 12 min played.
    Returns float 0..1 or None if no signal available."""
    if line is None:
        return book_p_yes
    try:
        line_f = float(line)
        threshold = math.floor(line_f) + 1  # smallest int that beats the line
        if live_stat is None:
            # Pre-game / no PBP → fall back on book if available
            return book_p_yes
        ls = float(live_stat)
        need = threshold - int(math.floor(ls))
        if need <= 0:
            return 1.0
        if sec_remaining is None:
            return book_p_yes
        if sec_remaining <= 0:
            return 0.0
        sec_played = max(60.0, sec_total - sec_remaining)  # floor played-time to dampen early-game blowups
        rate = ls / sec_played
        p_pace = _poisson_hit_prob(rate, sec_remaining, need)
        if book_p_yes is None:
            return p_pace
        # Blend: pace weight ramps from 0 to 1 over first 12 min played
        blend_horizon = 12 * 60.0
        w_pace = min(1.0, max(0.0, (sec_total - sec_remaining)) / blend_horizon)
        return max(0.0, min(1.0, w_pace * p_pace + (1.0 - w_pace) * book_p_yes))
    except Exception:
        return book_p_yes


@app.route("/api/live-pnl/snapshot")
def api_live_pnl_snapshot():
    """One-call payload for the live-pnl page. Polled every 2s by the UI.
    ev_mode=devig (default) uses get_blended_implied → power-devigged fair NO.
    ev_mode=book uses min(fd_no, dk_no) → raw arb-style fair NO."""
    ev_mode = request.args.get("ev_mode", "devig")
    dk_w = settings.get("dk_weight", 50)

    # Phase 1: snapshot under lock (no HTTP, minimal CPU)
    with games_lock:
        snap_games = {}
        for gid, game in games.items():
            g_copy = {
                "game_id": gid,
                "game_name": game.get("game_name", gid),
                "nba_game_id": game.get("nba_game_id"),
                "settings": game.get("settings", {}),
                "paused": game.get("paused", False),
            }
            g_copy["markets"] = []
            for tk, mkt in game["markets"].items():
                pos = mkt.get("position_no", 0) or 0
                if pos <= 0 and not mkt.get("active"):
                    continue  # skip markets with no position AND not active
                g_copy["markets"].append({
                    "ticker": tk,
                    "player": mkt.get("player", ""),
                    "prop_type": mkt.get("prop_type", "points"),
                    "line": mkt.get("line"),
                    "position_no": pos,
                    "position_avg_no": mkt.get("position_avg_no", 0) or 0,
                    "ceil_no": mkt.get("ceil_no", 0) or 0,
                    "best_bid_no": mkt.get("best_bid_no", 0) or 0,
                    "best_bid_yes": mkt.get("best_bid_yes", 0) or 0,
                    "order_price": mkt.get("order_price"),
                    "status": mkt.get("status", ""),
                    "active": bool(mkt.get("active")),
                    "size": mkt.get("size", 0) or 0,
                    "prop_done": bool(mkt.get("prop_done")),
                })
            snap_games[gid] = g_copy

    # Phase 2: compute response outside lock
    out_games = []
    sum_exposure = 0.0
    sum_unrealized = 0.0
    sum_ev_dollars = 0.0
    sum_contracts = 0
    ev_pct_weighted_num = 0.0  # ev_pct * exposure
    ev_pct_weighted_den = 0.0  # exposure
    positions_count = 0

    for gid, g in snap_games.items():
        nba_gid = g.get("nba_game_id")
        game_info = nba_feed.get_game_info(str(nba_gid)) if nba_gid else {}
        player_stats = nba_feed.get_player_stats(str(nba_gid)) if nba_gid else {}
        oc_lookup = nba_feed.get_on_court_lookup(str(nba_gid)) if nba_gid else {}
        sec_remaining = _seconds_remaining_in_game(
            game_info.get("period", 0),
            game_info.get("clock", ""),
            game_info.get("phase", ""),
        )

        # Group markets by (player, prop_type)
        by_player = {}
        for mkt in g["markets"]:
            key = (mkt["player"], mkt["prop_type"])
            by_player.setdefault(key, []).append(mkt)

        players_out = []
        g_exposure = 0.0
        g_unrealized = 0.0

        for (player, pt), mkts in by_player.items():
            prop_s = g.get("settings", {}).get(pt, {})
            player_max = prop_s.get("player_max", 0) or 0
            live_stat = None
            if player_stats:
                live_stat = nba_feed.get_player_stat(player, pt, str(nba_gid)) if nba_gid else None
            # On-court status — resolve once per (player, game) via directory.
            on_court = None
            if nba_gid:
                pid = nba_feed.resolve_player_in_game(str(nba_gid), player)
                if pid is not None:
                    on_court = oc_lookup.get(pid)

            lines = []
            p_contracts = 0
            p_exposure = 0.0
            p_unrealized = 0.0
            p_ev_weighted_num = 0.0
            p_ev_weighted_den = 0.0
            lines_remaining = 0

            for mkt in mkts:
                line_val = mkt.get("line")
                avg_price = float(mkt["position_avg_no"] or 0)
                contracts = int(mkt["position_no"] or 0)

                # Live fair NO — two modes
                fair_no = None
                fair_no_method = None
                # Always compute blended (devigged) yes-implied for hit-prob
                # blending — independent of EV display mode.
                blended_yes = get_blended_implied(player, dk_w, market_line=line_val, prop_type=pt)
                book_p_yes = (blended_yes / 100.0) if (blended_yes is not None and blended_yes > 0) else None
                if ev_mode == "book":
                    detail = _find_detail(player, line_val, pt) or {}
                    fd = detail.get("fanduel", {})
                    dk = detail.get("draftkings", {})
                    fd_imp = fd.get("implied_yes") if fd else None
                    dk_imp = dk.get("implied_yes") if dk else None
                    fd_no = (100 - fd_imp) if fd_imp else None
                    dk_no = (100 - dk_imp) if dk_imp else None
                    candidates = [v for v in (fd_no, dk_no) if v is not None]
                    if candidates:
                        fair_no = round(min(candidates), 2)
                        fair_no_method = "book_min"
                else:
                    if blended_yes is not None and blended_yes > 0:
                        fair_no = round(100 - blended_yes, 2)
                        fair_no_method = "devig"

                exposure = (avg_price * contracts / 100.0) if contracts > 0 else 0.0
                unrealized = None
                ev_pct = None
                ev_dollars = None
                if contracts > 0 and fair_no is not None and avg_price > 0:
                    unrealized = round((fair_no - avg_price) * contracts / 100.0, 2)
                    ev_pct = round((fair_no - avg_price) / avg_price * 100.0, 2)
                    ev_dollars = unrealized  # same thing — EV on current position

                # Proximity alert helpers (computed client-side too, but easier here)
                row_state = None
                try:
                    if live_stat is not None and line_val is not None:
                        lf = float(line_val)
                        if live_stat >= lf:
                            row_state = "hit"
                        else:
                            gap = lf - live_stat
                            if gap <= 3:
                                row_state = "approaching"
                            lines_remaining += 1
                    else:
                        lines_remaining += 1
                except Exception:
                    pass

                # Hit probability — chance the over-line gets hit (bad for our NO).
                hit_prob = compute_hit_prob(
                    live_stat, line_val, sec_remaining,
                    sec_total=NBA_REGULATION_SECS,
                    book_p_yes=book_p_yes,
                )
                hit_prob_pct = round(hit_prob * 100.0, 1) if hit_prob is not None else None
                # Method tag for tooltip / debugging
                if line_val is None or hit_prob is None:
                    hit_prob_method = None
                elif live_stat is not None and float(live_stat) >= math.floor(float(line_val)) + 1:
                    hit_prob_method = "already_hit"
                elif sec_remaining is None or live_stat is None:
                    hit_prob_method = "book"
                elif book_p_yes is None:
                    hit_prob_method = "pace"
                else:
                    hit_prob_method = "blend"

                lines.append({
                    "ticker": mkt["ticker"],
                    "line": line_val,
                    "contracts": contracts,
                    "avg_price": round(avg_price, 1),
                    "fair_no": fair_no,
                    "fair_no_method": fair_no_method,
                    "ev_pct": ev_pct,
                    "ev_dollars": ev_dollars,
                    "unrealized": unrealized,
                    "exposure": round(exposure, 2),
                    "ceil_no": mkt["ceil_no"],
                    "best_bid_no": mkt["best_bid_no"],
                    "status": mkt["status"],
                    "active": mkt["active"],
                    "order_price": mkt["order_price"],
                    "row_state": row_state,
                    "hit_prob": hit_prob_pct,
                    "hit_prob_method": hit_prob_method,
                })

                if contracts > 0:
                    p_contracts += contracts
                    p_exposure += exposure
                    if unrealized is not None:
                        p_unrealized += unrealized
                    if ev_pct is not None and exposure > 0:
                        p_ev_weighted_num += ev_pct * exposure
                        p_ev_weighted_den += exposure
                    positions_count += 1

            avg_ev_pct = round(p_ev_weighted_num / p_ev_weighted_den, 2) if p_ev_weighted_den > 0 else None
            # Headline hit-prob for the player row: max across not-yet-hit lines
            # we still hold contracts on. (Lines already hit = 100 → not actionable;
            # filter them so the headline number tells us the *next* risk.)
            risk_probs = [
                ln["hit_prob"]
                for ln in lines
                if ln.get("hit_prob") is not None
                and ln.get("hit_prob_method") != "already_hit"
                and (ln.get("contracts") or 0) > 0
            ]
            max_hit_prob = max(risk_probs) if risk_probs else None
            players_out.append({
                "player": player,
                "prop_type": pt,
                "lines": lines,
                "total_contracts": p_contracts,
                "total_exposure": round(p_exposure, 2),
                "total_unrealized": round(p_unrealized, 2),
                "avg_ev_pct": avg_ev_pct,
                "live_stat": live_stat,
                "lines_remaining": lines_remaining,
                "player_max": player_max,
                "max_pct": round(100.0 * p_contracts / player_max, 1) if player_max > 0 else None,
                "on_court": on_court,
                "max_hit_prob": max_hit_prob,
            })

            g_exposure += p_exposure
            g_unrealized += p_unrealized
            sum_exposure += p_exposure
            sum_unrealized += p_unrealized
            sum_ev_dollars += p_unrealized  # mark-to-market $ EV = unrealized
            sum_contracts += p_contracts
            ev_pct_weighted_num += p_ev_weighted_num
            ev_pct_weighted_den += p_ev_weighted_den

        if not players_out:
            continue

        # Sort players by exposure desc
        players_out.sort(key=lambda p: -p["total_exposure"])

        out_games.append({
            "game_id": gid,
            "game_name": g.get("game_name", gid),
            "game_phase": game_info.get("phase", ""),
            "game_clock": game_info.get("clock", ""),
            "game_period": game_info.get("period", 0),
            "game_score": (
                f"{game_info.get('away_score', 0)}-{game_info.get('home_score', 0)}"
                if game_info.get("phase") not in ("PRE", "", None) else ""
            ),
            "total_exposure": round(g_exposure, 2),
            "total_unrealized": round(g_unrealized, 2),
            "paused": g.get("paused", False),
            "players": players_out,
        })

    # Sort games by exposure desc
    out_games.sort(key=lambda g: -g["total_exposure"])

    # Today's fill aggregates for summary bar (CLV etc.)
    fills_today = 0
    avg_clv_10s = None
    avg_clv_60s = None
    try:
        conn = sqlite3.connect(FILLS_DB)
        conn.row_factory = sqlite3.Row
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        # Volume-weighted by contract notional (contracts * fill_price) — one
        # 1000-contract fill shouldn't weigh the same as one 1-contract fill.
        rows = conn.execute(
            "SELECT COUNT(*) AS n, "
            "SUM(clv_10s * contracts * fill_price) / NULLIF(SUM(CASE WHEN clv_10s IS NOT NULL THEN contracts * fill_price ELSE 0 END), 0) AS clv10, "
            "SUM(clv_60s * contracts * fill_price) / NULLIF(SUM(CASE WHEN clv_60s IS NOT NULL THEN contracts * fill_price ELSE 0 END), 0) AS clv60 "
            "FROM fills WHERE ts LIKE ?",
            (today + "%",),
        ).fetchone()
        if rows:
            fills_today = rows["n"] or 0
            avg_clv_10s = round(rows["clv10"], 2) if rows["clv10"] is not None else None
            avg_clv_60s = round(rows["clv60"], 2) if rows["clv60"] is not None else None
        conn.close()
    except Exception as e:
        log.error(f"live-pnl clv query: {e}")

    avg_ev_pct = round(ev_pct_weighted_num / ev_pct_weighted_den, 2) if ev_pct_weighted_den > 0 else None

    return jsonify({
        "ts": int(time.time()),
        "ev_mode": ev_mode,
        "summary": {
            "total_exposure": round(sum_exposure, 2),
            "total_unrealized": round(sum_unrealized, 2),
            "total_ev_dollars": round(sum_ev_dollars, 2),
            "avg_ev_pct": avg_ev_pct,
            "total_contracts": sum_contracts,
            "positions_count": positions_count,
            "fills_today": fills_today,
            "avg_clv_10s": avg_clv_10s,
            "avg_clv_60s": avg_clv_60s,
        },
        "games": out_games,
    })


@app.route("/api/live-pnl/performance")
def api_live_pnl_performance():
    """Historical CLV/EV/win-rate from the fills table.
    Returns today / 7d / all-time aggregates + by-phase breakdown if column exists."""
    out = {"today": None, "seven_days": None, "all_time": None, "by_phase": []}
    try:
        conn = sqlite3.connect(FILLS_DB)
        conn.row_factory = sqlite3.Row
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        seven = (datetime.now(timezone.utc) - timedelta(days=7)).strftime("%Y-%m-%d")

        def _agg(sql, params):
            r = conn.execute(sql, params).fetchone()
            if not r:
                return None
            return {
                "fills": r["n"] or 0,
                "avg_ev_at_fill": round(r["ev_avg"], 2) if r["ev_avg"] is not None else None,
                "avg_clv_10s": round(r["clv10"], 2) if r["clv10"] is not None else None,
                "avg_clv_60s": round(r["clv60"], 2) if r["clv60"] is not None else None,
                "win_rate_60s": round(r["wr60"] * 100, 1) if r["wr60"] is not None else None,
                "volume": round(r["vol"], 2) if r["vol"] is not None else 0,
                "ev_dollars": round(r["ev_d"], 2) if r["ev_d"] is not None else 0,
            }

        base_cols = (
            "COUNT(*) AS n, "
            "SUM(ev_at_fill * contracts * fill_price) / NULLIF(SUM(CASE WHEN ev_at_fill IS NOT NULL THEN contracts * fill_price ELSE 0 END), 0) AS ev_avg, "
            "SUM(clv_10s    * contracts * fill_price) / NULLIF(SUM(CASE WHEN clv_10s    IS NOT NULL THEN contracts * fill_price ELSE 0 END), 0) AS clv10, "
            "SUM(clv_60s    * contracts * fill_price) / NULLIF(SUM(CASE WHEN clv_60s    IS NOT NULL THEN contracts * fill_price ELSE 0 END), 0) AS clv60, "
            "AVG(CASE WHEN clv_60s > 0 THEN 1.0 WHEN clv_60s IS NULL THEN NULL ELSE 0.0 END) AS wr60, "
            "SUM(contracts * fill_price / 100.0) AS vol, "
            "SUM(ev_at_fill * contracts * fill_price / 10000.0) AS ev_d "
        )
        out["today"] = _agg(f"SELECT {base_cols} FROM fills WHERE ts LIKE ?", (today + "%",))
        out["seven_days"] = _agg(f"SELECT {base_cols} FROM fills WHERE ts >= ?", (seven,))
        out["all_time"] = _agg(f"SELECT {base_cols} FROM fills", ())

        # By-phase (if column exists — soft fail)
        try:
            rows = conn.execute(
                f"SELECT phase, {base_cols} FROM fills "
                "WHERE phase IS NOT NULL AND phase != '' "
                "GROUP BY phase ORDER BY n DESC"
            ).fetchall()
            for r in rows:
                out["by_phase"].append({
                    "phase": r["phase"],
                    "fills": r["n"] or 0,
                    "avg_ev_at_fill": round(r["ev_avg"], 2) if r["ev_avg"] is not None else None,
                    "avg_clv_60s": round(r["clv60"], 2) if r["clv60"] is not None else None,
                })
        except Exception:
            # Column doesn't exist yet — that's fine, skip
            pass

        conn.close()
    except Exception as e:
        log.error(f"live-pnl performance: {e}")
        return jsonify({"error": str(e)}), 500

    return jsonify(out)


@app.route("/v2/phase")
def v2_phase_page():
    return V2_PHASE_PAGE


V2_PHASE_PAGE = """<!doctype html>
<html><head><meta charset="utf-8"><title>v2 — Phase Compare</title>
<style>
:root{--bg:#0d1117;--panel:#161b22;--panel2:#1c2128;--border:#30363d;
--g:#3fb950;--y:#d29922;--r:#f85149;--c:#58a6ff;--p:#bc8cff;--orange:#ff9d3d;
--t1:#e6edf3;--t2:#8b949e;--t3:#6e7681;--t4:#484f58;}
*{box-sizing:border-box;margin:0;padding:0;}
body{background:var(--bg);color:var(--t1);font-family:'SF Mono',Menlo,monospace;padding:20px;}
h1{font-size:14px;letter-spacing:2px;margin-bottom:6px;color:var(--c);}
.sub{font-size:11px;color:var(--t3);margin-bottom:14px;}
.status{font-size:11px;color:var(--t2);margin-bottom:16px;padding:10px 12px;background:var(--panel);border:1px solid var(--border);border-radius:6px;display:flex;gap:18px;flex-wrap:wrap;}
.dot{display:inline-block;width:8px;height:8px;border-radius:50%;margin-right:6px;vertical-align:middle;}
table{width:100%;border-collapse:collapse;background:var(--panel);border:1px solid var(--border);border-radius:6px;overflow:hidden;}
th,td{padding:9px 12px;text-align:left;font-size:11px;border-bottom:1px solid var(--border);vertical-align:middle;}
th{background:var(--panel2);color:var(--t3);font-size:9px;letter-spacing:1px;text-transform:uppercase;}
tr:last-child td{border-bottom:none;}
.phase-pill{display:inline-block;padding:3px 10px;border-radius:12px;font-size:10px;font-weight:bold;letter-spacing:0.5px;}
.phase-LIVE,.phase-LIVE_RUNNING{background:rgba(63,185,80,0.18);color:var(--g);}
.phase-DEAD_BALL_SHORT,.phase-DEAD_BALL_LONG,.phase-DEADBALL,.phase-DEAD_BALL{background:rgba(210,153,34,0.15);color:var(--y);}
.phase-FREE_THROW,.phase-FOUL_SHOT{background:rgba(248,81,73,0.18);color:var(--r);}
.phase-TIMEOUT{background:rgba(188,140,255,0.18);color:var(--p);}
.phase-QUARTER_END,.phase-QUARTER_BREAK,.phase-PERIOD_END{background:rgba(255,157,61,0.18);color:var(--orange);}
.phase-HALFTIME{background:rgba(88,166,255,0.18);color:var(--c);}
.phase-UNKNOWN,.phase-PRE,.phase-FINAL{background:rgba(110,118,129,0.18);color:var(--t3);}
.ls-events{font-size:9px;color:var(--t3);line-height:1.4;}
.ls-evt{display:inline-block;padding:1px 5px;border-radius:3px;margin-right:4px;}
.ls-evt-score_change{background:rgba(63,185,80,0.15);color:var(--g);}
.ls-evt-clock_started{background:rgba(63,185,80,0.10);color:var(--g);}
.ls-evt-clock_stopped{background:rgba(210,153,34,0.15);color:var(--y);}
.ls-evt-pending_stop{background:rgba(188,140,255,0.15);color:var(--p);}
.ls-evt-period_change{background:rgba(255,157,61,0.15);color:var(--orange);}
.ls-evt-stop_canceled{background:rgba(248,81,73,0.15);color:var(--r);}
.ls-degraded{color:var(--y);}
.pbp-toggle{cursor:pointer;color:var(--c);font-size:9px;letter-spacing:1px;user-select:none;}
.pbp-toggle:hover{color:var(--t1);}
.pbp-feed{background:var(--panel2);border:1px solid var(--border);padding:8px 12px;font-size:10px;max-height:240px;overflow-y:auto;display:none;}
.pbp-feed.open{display:block;}
.pbp-row{padding:3px 0;border-bottom:1px solid #1c2128;display:flex;gap:8px;align-items:baseline;}
.pbp-row:last-child{border-bottom:none;}
.pbp-time{color:var(--t4);font-size:9px;width:50px;flex-shrink:0;}
.pbp-type{font-weight:bold;width:100px;flex-shrink:0;}
.pbp-type-goal{color:var(--g);}
.pbp-type-foul{color:var(--y);}
.pbp-type-timeout{color:var(--p);}
.pbp-type-free_throws_awarded{color:var(--r);}
.pbp-type-attempt_missed{color:var(--t3);}
.pbp-type-rebound,.pbp-type-possession,.pbp-type-steal_basket,.pbp-type-block{color:var(--t2);}
.pbp-type-periodend,.pbp-type-periodscore{color:var(--orange);}
.pbp-desc{color:var(--t2);}
.pbp-team{color:var(--t4);font-size:9px;margin-left:auto;flex-shrink:0;}
.match-y{color:var(--g);}
.match-n{color:var(--r);font-weight:bold;}
.match-na{color:var(--t4);}
.evidence{color:var(--t3);font-size:9px;font-style:italic;}
.reason{color:var(--t3);font-size:9px;}
.game-name{color:var(--t1);font-weight:600;}
.muted{color:var(--t4);font-size:9px;}
</style></head>
<body>
<h1>V2 — PHASE COMPARE (CDN vs BOLT vs LS-FIRST)</h1>
<div class="sub">3-way: NBA CDN phase vs PBP-first Bolt phase vs new LS-first resolver. LS-first beats CDN by +7.8s on score, +14.6s on clock-start, +8.2s on clock-stop (validated 2026-04-30). Refresh 1.5s. Observation only.</div>
<div class="status" id="status">Loading...</div>
<div id="games"></div>

<script>
function esc(s){if(s==null)return '';var d=document.createElement('div');d.textContent=String(s);return d.innerHTML;}
function pill(p){if(!p)return '<span class="phase-pill phase-UNKNOWN">—</span>';return '<span class="phase-pill phase-'+esc(p)+'">'+esc(p)+'</span>';}
function matchCell(m){
  if(m===true)return '<span class="match-y">✓</span>';
  if(m===false)return '<span class="match-n">✗ DIFF</span>';
  return '<span class="match-na">—</span>';
}
function fmtClock(p){
  if(p==null)return '<span class="muted">—</span>';
  if(p.remaining_sec==null)return 'P'+(p.period||'?');
  var m=Math.floor(p.remaining_sec/60);
  var s=p.remaining_sec%60;
  return 'Q'+(p.period||'?')+' '+m+':'+(s<10?'0':'')+s;
}
function render(d){
  var dot=function(c){return '<span class="dot" style="background:'+c+';"></span>';};
  var lsClr=d.ls_connected?'var(--g)':'var(--t4)';
  var pbpClr=d.pbp_connected?'var(--g)':'var(--t4)';
  document.getElementById('status').innerHTML =
    '<div>'+dot(lsClr)+'BOLT LS '+(d.ls_connected?'connected':'disconnected')+'</div>'+
    '<div>'+dot(pbpClr)+'BOLT PBP '+(d.pbp_connected?'connected':'disconnected')+'</div>'+
    '<div>games: '+(d.games||[]).length+'</div>';

  if(!d.games||!d.games.length){
    document.getElementById('games').innerHTML='<div style="color:var(--t3);padding:20px;">No active NBA games loaded.</div>';
    return;
  }
  var html='<table><thead><tr>';
  html+='<th>GAME</th><th>CDN PHASE</th><th>BOLT PHASE</th><th>LS-FIRST</th><th>⚡ CLOCK FEED</th><th>MATCH</th><th>BOLT CLOCK</th><th>SCORE</th><th>LAST PLAY</th><th>WHY (Bolt)</th><th>LS EVENTS</th></tr></thead><tbody>';
  for(var i=0;i<d.games.length;i++){
    var g=d.games[i];
    var clkInfo={period:g.bolt_period, remaining_sec:g.bolt_remaining_sec};
    var clk = fmtClock(clkInfo);
    var running = g.bolt_clock_running;
    var clkBadge = running===true?'<span style="color:var(--g);">▶</span> ':running===false?'<span style="color:var(--r);">■</span> ':'<span class="muted">?</span> ';
    var clkSrc = g.bolt_clock_source ? '<span class="muted"> ['+esc(g.bolt_clock_source)+']</span>' : '';
    var sc = g.bolt_score;
    var scoreStr = sc ? esc(sc.away+'-'+sc.home) : '<span class="muted">—</span>';
    var lp = g.bolt_last_play;
    var lpHtml = '<span class="muted">—</span>';
    if(lp){
      var who = lp.scorer || lp.player || lp.fouled || '';
      var ageS = (lp.age_s||0).toFixed(0);
      var sub = (who?(' '+who):'') + (lp.points?(' +'+lp.points):'') + (lp.awarded?(' ('+lp.awarded+'FT)'):'');
      var ageColor = lp.age_s < 5 ? 'var(--g)' : lp.age_s < 30 ? 'var(--y)' : 'var(--t3)';
      lpHtml = '<div><b>'+esc(lp.type||lp.name||'?')+'</b>'+esc(sub)+'</div>'+
               '<div class="muted" style="color:'+ageColor+';">'+ageS+'s ago'+(lp.team?(' · '+esc(lp.team)):'')+'</div>';
    }
    // LS-first resolver cell
    var lp2 = g.ls_phase || {};
    var lsPill = pill(lp2.phase || 'UNKNOWN');
    var lsSrc = lp2.source ? '<div class="muted">'+esc(lp2.source)+'</div>' : '';
    var lsAge = (lp2.ls_age_s != null) ? '<div class="muted'+(lp2.degraded?' ls-degraded':'')+'">ls_age '+lp2.ls_age_s.toFixed(0)+'s</div>' : '';
    var lsPending = lp2.pending_stop ? '<div class="muted" style="color:var(--p);">⏳ pending stop</div>' : '';
    // Recent LS events
    var evs = g.ls_recent_events || [];
    var evsHtml = '';
    for(var j=Math.max(0,evs.length-5);j<evs.length;j++){
      var e = evs[j];
      var label = e.kind;
      if(e.kind === 'score_change'){
        var dh = (e.payload||{}).delta_home||0, da=(e.payload||{}).delta_away||0;
        label = 'score +'+(dh||da)+' ('+(dh?'H':'A')+')';
      } else if(e.kind === 'period_change'){
        label = 'period→' + ((e.payload||{}).match_period||'?');
      }
      evsHtml += '<span class="ls-evt ls-evt-'+esc(e.kind)+'">'+esc(label)+'</span>';
    }
    if(!evsHtml) evsHtml = '<span class="muted">—</span>';

    html+='<tr>';
    html+='<td><div class="game-name">'+esc(g.game_name||g.game_id)+'</div><div class="muted">bolt_key: '+esc(g.bolt_key||'(none)')+'</div></td>';
    html+='<td>'+pill(g.cdn_phase)+'</td>';
    html+='<td>'+pill(g.bolt_phase)+'</td>';
    html+='<td>'+lsPill+lsSrc+lsAge+lsPending+'</td>';
    // ⚡ Clock feed cell — push-driven multiplexer, fastest signal wins
    var cf = g.clock_feed || {};
    var cfRunning = cf.running;
    var cfBadge = cfRunning===true?'<span style="color:var(--g);font-size:13px;">▶ RUN</span>':cfRunning===false?'<span style="color:var(--r);font-size:13px;">■ STOP</span>':'<span class="muted">— ?</span>';
    var cfPhase = cf.phase ? '  '+pill(cf.phase) : '';
    var cfSrc = cf.source ? '<div class="muted">via '+esc(cf.source)+'</div>' : '';
    var cfPhaseSrc = (cf.phase_source && cf.phase_source !== cf.source) ? '<div class="muted">phase via '+esc(cf.phase_source)+'</div>' : '';
    var cfAge = (cf.since_age_s != null) ? '<div class="muted">'+cf.since_age_s.toFixed(1)+'s ago</div>' : '';
    var cfStale = '';
    if (cf.waiting) cfStale = '<div class="muted" style="font-size:9px;">waiting for events…</div>';
    else if (cf.dead) cfStale = '<div style="color:var(--r);font-size:9px;">⚠ FEED DEAD ('+(cf.last_event_age_s||0).toFixed(0)+'s)</div>';
    else if (cf.stale) cfStale = '<div style="color:var(--y);font-size:9px;">⚠ stale ('+(cf.last_event_age_s||0).toFixed(0)+'s)</div>';
    html+='<td>'+cfBadge+cfPhase+cfSrc+cfPhaseSrc+cfAge+cfStale+'</td>';
    html+='<td>'+matchCell(g.phases_match)+'</td>';
    html+='<td>'+clkBadge+clk+clkSrc+'</td>';
    html+='<td>'+scoreStr+'</td>';
    html+='<td>'+lpHtml+'</td>';
    html+='<td class="reason">'+esc(g.bolt_reason||'')+'</td>';
    html+='<td class="ls-events">'+evsHtml+'</td>';
    html+='</tr>';
    // Expandable PBP feed row (one per game). Use a slug for the DOM id
    // (bolt_keys contain spaces/commas/colons which break id selectors and
    // string-literal arguments in onclick handlers).
    var bk = g.bolt_key || '';
    var rowKey = (bk || g.game_id || ('row'+i)).replace(/[^a-zA-Z0-9]/g, '_');
    html+='<tr><td colspan="11" style="padding:0;border-top:none;">'+
      '<div style="padding:4px 12px;background:var(--bg);">'+
      '<span class="pbp-toggle" data-rowkey="'+rowKey+'">▶ SHOW PBP FEED</span>'+
      '</div>'+
      '<div class="pbp-feed" id="pbpfeed-'+rowKey+'" data-boltkey="'+esc(bk)+'"></div>'+
      '</td></tr>';
  }
  html+='</tbody></table>';
  document.getElementById('games').innerHTML=html;
}
function poll(){
  fetch('/api/v2/phase_compare').then(function(r){return r.json();}).then(render).catch(function(e){
    document.getElementById('status').innerHTML='<span style="color:var(--r);">poll error: '+esc(String(e))+'</span>';
  });
}
poll();
setInterval(poll, 1500);

// Per-game PBP feed — toggle + 2s refresh while open
var pbpOpen = {};
// Event delegation on the games container — clicks on .pbp-toggle bubble up
document.addEventListener('click', function(e){
  var t = e.target;
  if(t && t.classList && t.classList.contains('pbp-toggle')){
    var rk = t.getAttribute('data-rowkey');
    if(rk) togglePbp(rk);
  }
});
function togglePbp(rowKey){
  pbpOpen[rowKey] = !pbpOpen[rowKey];
  var div = document.getElementById('pbpfeed-'+rowKey);
  if(div){
    div.classList.toggle('open', pbpOpen[rowKey]);
    var toggle = div.previousElementSibling.querySelector('.pbp-toggle');
    if(toggle) toggle.innerHTML = pbpOpen[rowKey] ? '▼ HIDE PBP FEED' : '▶ SHOW PBP FEED';
    if(pbpOpen[rowKey]) refreshPbp(rowKey);
  }
}
function refreshPbp(rowKey){
  var div = document.getElementById('pbpfeed-'+rowKey);
  if(!div || !pbpOpen[rowKey]) return;
  var bk = div.getAttribute('data-boltkey');
  if(!bk){
    div.innerHTML = '<div style="color:var(--t3);">no bolt_key for this game</div>';
    return;
  }
  fetch('/api/v2/pbp_recent?limit=30&bolt_key='+encodeURIComponent(bk))
    .then(function(r){return r.json();}).then(function(d){
      var plays = (d.by_game||{})[bk] || [];
      if(!plays.length){
        div.innerHTML = '<div style="color:var(--t3);">no plays yet</div>';
        return;
      }
      var html = '';
      // Newest at top
      for(var i=plays.length-1;i>=0;i--){
        var p = plays[i];
        var t = p.type || '?';
        var who = p.scorer || p.player || p.fouled || '';
        var pts = p.points ? '+'+p.points : '';
        var ftAw = p.awarded ? ' ('+p.awarded+'FT)' : '';
        var sec = p.seconds!=null ? ' @'+p.seconds+'s' : '';
        var sc = p.score ? ' ['+p.score.away+'-'+p.score.home+']' : '';
        var age = p.age_s!=null ? p.age_s.toFixed(1)+'s' : '';
        html += '<div class="pbp-row">'+
          '<span class="pbp-time">'+age+'</span>'+
          '<span class="pbp-type pbp-type-'+esc(t)+'">'+esc(t)+'</span>'+
          '<span class="pbp-desc">'+esc(who)+esc(pts)+esc(ftAw)+esc(sec)+esc(sc)+'</span>'+
          '<span class="pbp-team">'+esc(p.team||'')+'</span>'+
        '</div>';
      }
      div.innerHTML = html;
    }).catch(function(){});
}
setInterval(function(){
  Object.keys(pbpOpen).forEach(function(k){if(pbpOpen[k])refreshPbp(k);});
}, 2000);
</script>
</body></html>"""


@app.route("/live-pnl")
def live_pnl_page():
    return LIVE_PNL_HTML


@app.route("/pnl")
def pnl_page():
    return PNL_PAGE_HTML


PNL_PAGE_HTML = r"""<!DOCTYPE html>
<html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>NBA Props P&L</title>
<style>
*{box-sizing:border-box;margin:0;padding:0;}
body{background:#080a0e;color:#f0f0f8;font-family:'SF Mono',monospace;font-size:12px;padding:10px;max-width:600px;margin:0 auto;}
.hdr{display:flex;align-items:center;gap:8px;padding:10px;background:#0d0f15;border:1px solid #1c1e2a;border-radius:6px;margin-bottom:8px;}
.logo{font-size:15px;font-weight:bold;color:#00e676;letter-spacing:2px;}
.stat-row{display:flex;gap:6px;margin-bottom:8px;flex-wrap:wrap;}
.stat-card{flex:1;min-width:80px;padding:8px;background:#0d0f15;border:1px solid #1c1e2a;border-radius:6px;text-align:center;}
.stat-lbl{font-size:8px;color:#4a5068;letter-spacing:1.5px;text-transform:uppercase;margin-bottom:4px;}
.stat-val{font-size:14px;font-weight:bold;color:#f0f0f8;}
.pos{color:#00e676;}.neg{color:#ff3d57;}
.game{background:#0d0f15;border:1px solid #1c1e2a;border-radius:6px;margin-bottom:8px;overflow:hidden;}
.game-hdr{padding:8px 12px;background:#12141c;border-bottom:1px solid #1c1e2a;display:flex;justify-content:space-between;align-items:center;}
.game-name{font-weight:bold;font-size:13px;}
.game-phase{font-size:10px;font-weight:bold;}
.phase-live{color:#00e676;animation:pulse 1.5s infinite;}
.phase-timeout{color:#ffb300;}
.phase-half{color:#82b1ff;}
.phase-final{color:#4a5068;}
@keyframes pulse{0%,100%{opacity:1;}50%{opacity:0.5;}}
.pt-section{border-top:1px solid #1c1e2a;}
.pt-label{padding:4px 12px;font-size:9px;font-weight:bold;letter-spacing:2px;color:#82b1ff;background:#0a0c10;}
.player{padding:8px 12px;border-top:1px solid #0a0c10;}
.player:first-child{border-top:none;}
.player-top{display:flex;justify-content:space-between;align-items:center;margin-bottom:4px;}
.player-name{font-weight:bold;font-size:12px;}
.player-stat{font-size:10px;color:#82b1ff;font-weight:bold;}
.player-meta{display:flex;gap:12px;font-size:10px;color:#8890aa;}
.player-meta .pm-val{font-weight:bold;color:#f0f0f8;}
.lines{margin-top:4px;padding-left:8px;border-left:2px solid #1c1e2a;}
.line-row{display:flex;gap:8px;font-size:10px;color:#8890aa;padding:2px 0;}
.line-row .ln{color:#f0f0f8;font-weight:bold;min-width:35px;}
.line-row .qty{min-width:40px;}
.line-row .ev{font-weight:bold;}
.nav{display:flex;gap:6px;margin-bottom:8px;}
.nav a{color:#8890aa;text-decoration:none;font-size:10px;padding:4px 8px;border:1px solid #1c1e2a;border-radius:3px;}
.nav a:hover{color:#00e676;border-color:#00e676;}
.loading{padding:30px;text-align:center;color:#4a5068;}
</style></head>
<body>
<div class="hdr"><span class="logo">NBA P&L</span></div>
<div class="nav"><a href="/">DASHBOARD</a></div>
<div class="stat-row">
  <div class="stat-card"><div class="stat-lbl">P&L EST</div><div class="stat-val" id="sPnl">--</div></div>
  <div class="stat-card"><div class="stat-lbl">VOLUME</div><div class="stat-val" id="sVol">--</div></div>
  <div class="stat-card"><div class="stat-lbl">FILLS</div><div class="stat-val" id="sFills">--</div></div>
  <div class="stat-card"><div class="stat-lbl">AVG EV</div><div class="stat-val" id="sEv">--</div></div>
  <div class="stat-card"><div class="stat-lbl">CLV 10s</div><div class="stat-val" id="sClv10">--</div></div>
  <div class="stat-card"><div class="stat-lbl">CLV 60s</div><div class="stat-val" id="sClv60">--</div></div>
</div>
<div id="content"><div class="loading">Loading...</div></div>

<script>
function esc(s){if(!s&&s!==0)return '';var d=document.createElement('div');d.textContent=s;return d.innerHTML;}
function fmtD(v){if(v==null||isNaN(v))return '$--';return (v>=0?'+':'-')+'$'+Math.abs(v).toFixed(2);}
function cls(v){return v>0?'pos':v<0?'neg':'';}

function loadSummary(){
  fetch('/api/pnl/summary').then(function(r){return r.json();}).then(function(d){
    document.getElementById('sPnl').textContent=fmtD(d.pnl_estimate_dollars);
    document.getElementById('sPnl').className='stat-val '+cls(d.pnl_estimate_dollars);
    document.getElementById('sVol').textContent='$'+(d.total_volume_dollars||0).toFixed(0);
    document.getElementById('sFills').textContent=d.total_fills||0;
    document.getElementById('sEv').textContent=(d.avg_ev_at_fill||0).toFixed(1)+'%';
    document.getElementById('sEv').className='stat-val '+cls(d.avg_ev_at_fill);
    document.getElementById('sClv10').textContent=(d.avg_clv_10s>=0?'+':'')+(d.avg_clv_10s||0).toFixed(1);
    document.getElementById('sClv10').className='stat-val '+cls(d.avg_clv_10s);
    document.getElementById('sClv60').textContent=(d.avg_clv_60s>=0?'+':'')+(d.avg_clv_60s||0).toFixed(1);
    document.getElementById('sClv60').className='stat-val '+cls(d.avg_clv_60s);
  });
}

function loadPositions(){
  fetch('/api/pnl/positions').then(function(r){return r.json();}).then(function(d){
    var games=d.games||[];
    if(!games.length){document.getElementById('content').innerHTML='<div class="loading">No positions</div>';return;}
    var html='';
    var ptLabels={points:'POINTS',rebounds:'REBOUNDS',assists:'ASSISTS'};
    games.forEach(function(g){
      var phCls=g.game_phase==='LIVE'?'phase-live':(g.game_phase==='TIMEOUT'?'phase-timeout':(g.game_phase==='HALFTIME'?'phase-half':'phase-final'));
      html+='<div class="game"><div class="game-hdr"><span class="game-name">'+esc(g.game_name)+'</span>';
      html+='<span class="game-phase '+phCls+'">'+(g.game_phase||'PRE')+(g.game_score?' '+esc(g.game_score):'')+'</span></div>';
      ['points','rebounds','assists'].forEach(function(pt){
        var players=g.prop_types[pt];
        if(!players||!players.length)return;
        html+='<div class="pt-section"><div class="pt-label">'+ptLabels[pt]+'</div>';
        players.forEach(function(p){
          var statTxt=p.live_stat!=null?p.live_stat+' '+pt.substring(0,3).toUpperCase():'';
          html+='<div class="player"><div class="player-top"><span class="player-name">'+esc(p.player)+'</span>';
          if(statTxt)html+='<span class="player-stat">'+statTxt+'</span>';
          html+='</div><div class="player-meta">';
          html+='<span>'+p.total_contracts+' <span class="pm-val">ctrs</span></span>';
          html+='<span>$'+p.total_exposure.toFixed(2)+' <span class="pm-val">exp</span></span>';
          html+='<span class="'+cls(p.total_unrealized)+'">'+fmtD(p.total_unrealized)+' <span class="pm-val">unrl</span></span>';
          html+='</div><div class="lines">';
          p.lines.forEach(function(ln){
            var evCls=cls(ln.ev_pct);
            html+='<div class="line-row">';
            html+='<span class="ln">'+(ln.line!=null?ln.line+'+':'--')+'</span>';
            html+='<span class="qty">'+ln.contracts+'@'+ln.avg_price+'c</span>';
            html+='<span>fair:'+(ln.fair_no!=null?ln.fair_no.toFixed(0)+'c':'--')+'</span>';
            html+='<span class="ev '+evCls+'">'+(ln.ev_pct!=null?(ln.ev_pct>=0?'+':'')+ln.ev_pct.toFixed(1)+'%':'--')+'</span>';
            html+='<span class="'+cls(ln.unrealized)+'">'+fmtD(ln.unrealized)+'</span>';
            html+='</div>';
          });
          html+='</div></div>';
        });
        html+='</div>';
      });
      html+='</div>';
    });
    document.getElementById('content').innerHTML=html;
  });
}

loadSummary();loadPositions();
setInterval(function(){loadSummary();loadPositions();},3000);
</script>
</body></html>"""


LIVE_PNL_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>NBA LIVE P&L</title>
<style>
*{box-sizing:border-box;margin:0;padding:0;}
:root{
  --bg:#080a0e;--panel:#0f1319;--panel2:#141a23;--border:#1e2533;--border2:#2a3344;
  --t1:#f0f0f8;--t2:#9aa3b5;--t3:#6a7388;--t4:#4a5368;
  --g:#00e676;--g2:#00e67620;--r:#ff4444;--r2:#ff444420;
  --y:#ffc107;--y2:#ffc10720;--b:#82b1ff;--b2:#82b1ff20;
  --p:#c084fc;--p2:#c084fc20;--o:#ff9800;--o2:#ff980020;
  --cyan:#22d3ee;--cyan2:#22d3ee20;
}
body{background:var(--bg);color:var(--t1);font-family:'SF Mono',Menlo,Consolas,monospace;font-size:12px;padding:12px;line-height:1.4;}
a{color:var(--b);text-decoration:none;}a:hover{color:var(--cyan);}
.nav{display:flex;gap:12px;margin-bottom:14px;padding-bottom:10px;border-bottom:1px solid var(--border);align-items:center;}
.nav h1{font-size:13px;letter-spacing:3px;color:var(--t1);}
.nav .pill{padding:3px 10px;border:1px solid var(--border2);border-radius:3px;font-size:10px;color:var(--t3);}
.nav .pill.active{border-color:var(--g);color:var(--g);background:var(--g2);}
.nav .ts{margin-left:auto;font-size:10px;color:var(--t4);}
.bar{display:grid;grid-template-columns:repeat(7,1fr);gap:8px;margin-bottom:14px;}
.stat{background:var(--panel);border:1px solid var(--border);border-radius:4px;padding:10px 12px;}
.stat .lbl{font-size:9px;color:var(--t3);letter-spacing:2px;text-transform:uppercase;}
.stat .val{font-size:18px;font-weight:bold;margin-top:3px;color:var(--t1);}
.stat .val.pos{color:var(--g);}.stat .val.neg{color:var(--r);}
.stat .sub{font-size:9px;color:var(--t4);}
.toggle{margin-bottom:12px;display:flex;gap:6px;align-items:center;}
.tog{background:var(--panel);border:1px solid var(--border2);color:var(--t2);padding:5px 12px;font-family:inherit;font-size:10px;cursor:pointer;border-radius:3px;letter-spacing:1px;}
.tog.on{background:var(--g2);border-color:var(--g);color:var(--g);}
.tog:hover{border-color:var(--t3);}
.game{background:var(--panel);border:1px solid var(--border);border-radius:5px;margin-bottom:10px;overflow:hidden;}
.game-hdr{padding:10px 14px;background:var(--panel2);border-bottom:1px solid var(--border);display:flex;align-items:center;gap:10px;cursor:pointer;user-select:none;}
.game-hdr:hover{background:#182030;}
.game-name{font-size:13px;font-weight:bold;color:var(--t1);min-width:110px;}
.phase{padding:2px 8px;border-radius:3px;font-size:9px;letter-spacing:1px;}
.phase.LIVE{background:var(--g2);color:var(--g);border:1px solid var(--g);animation:pulse 1.5s infinite;}
.phase.OVERTIME{background:var(--r2);color:var(--r);border:1px solid var(--r);animation:pulse 1.2s infinite;}
.phase.TIMEOUT{background:var(--y2);color:var(--y);border:1px solid var(--y);}
.phase.HALFTIME,.phase.QUARTER_BREAK{background:var(--b2);color:var(--b);border:1px solid var(--b);}
.phase.FOUL_SHOT{background:var(--o2);color:var(--o);border:1px solid var(--o);}
.phase.PRE,.phase.UNKNOWN{background:#ffffff10;color:var(--t3);border:1px solid var(--border2);}
.phase.FINAL{color:var(--t4);border:1px solid var(--border2);}
@keyframes pulse{0%,100%{opacity:1;}50%{opacity:0.55;}}
.clock{font-size:11px;color:var(--t2);min-width:70px;}
.score{font-size:13px;color:var(--t1);font-weight:bold;}
.game-totals{margin-left:auto;display:flex;gap:18px;}
.game-totals span{font-size:10px;color:var(--t3);}
.game-totals b{color:var(--t1);font-weight:bold;}
.game-totals b.pos{color:var(--g);}.game-totals b.neg{color:var(--r);}
table{width:100%;border-collapse:collapse;}
th{font-size:9px;color:var(--t4);letter-spacing:1px;text-transform:uppercase;text-align:left;padding:6px 8px;border-bottom:1px solid var(--border);background:var(--panel2);cursor:pointer;user-select:none;}
th:hover{color:var(--t2);}
th.active{color:var(--g);}
td{padding:5px 8px;font-size:11px;border-bottom:1px solid var(--border);}
.player-row{background:var(--panel2);}
.player-row td{font-weight:bold;color:var(--t1);}
.line-row td{color:var(--t2);}
.line-row.approaching{background:#ff980015;}
.line-row.approaching td{color:var(--o);}
.line-row.hit{background:#c084fc18;}
.line-row.hit td{color:var(--p);text-decoration:line-through;}
.ev.good{color:var(--g);font-weight:bold;}
.ev.mid{color:var(--y);}
.ev.bad{color:var(--r);font-weight:bold;}
.pos{color:var(--g);}.neg{color:var(--r);}
.hp-mid{color:var(--y);}
.mutedcell{color:var(--t4);}
.bar-wrap{width:80px;height:6px;background:var(--border);border-radius:2px;overflow:hidden;display:inline-block;vertical-align:middle;}
.bar-fill{height:100%;background:var(--b);}
.bar-fill.hot{background:var(--o);}
.bar-fill.max{background:var(--r);}
.empty{padding:60px;text-align:center;color:var(--t4);font-size:12px;}
.fills-strip{background:var(--panel);border:1px solid var(--border);border-radius:5px;padding:8px 12px;margin-top:14px;max-height:230px;overflow:auto;}
.fills-strip h3{font-size:10px;letter-spacing:2px;color:var(--t3);margin-bottom:6px;cursor:pointer;}
.fill-row{font-size:10px;padding:3px 0;border-bottom:1px solid var(--border);color:var(--t2);display:grid;grid-template-columns:60px 140px 50px 50px 50px 60px 60px 50px;gap:8px;}
.perf{display:grid;grid-template-columns:1fr 1fr 1fr;gap:10px;margin-top:14px;}
.perf-card{background:var(--panel);border:1px solid var(--border);border-radius:5px;padding:10px 12px;}
.perf-card h4{font-size:10px;letter-spacing:2px;color:var(--t3);margin-bottom:8px;}
.perf-card .row{display:flex;justify-content:space-between;padding:3px 0;font-size:11px;}
.perf-card .row span:first-child{color:var(--t3);}
.perf-card .row span:last-child{color:var(--t1);font-weight:bold;}
</style>
</head>
<body>

<div class="nav">
  <h1>⚡ NBA LIVE P&amp;L</h1>
  <a href="/live-pnl" class="pill active">LIVE</a>
  <a href="/pnl" class="pill">HISTORICAL</a>
  <a href="/" class="pill">DASHBOARD</a>
  <span class="ts" id="ts">—</span>
</div>

<div class="toggle">
  <span style="font-size:10px;color:var(--t3);letter-spacing:2px;">EV MODE:</span>
  <button class="tog on" id="modeDevig" onclick="setMode('devig')">DEVIG EV</button>
  <button class="tog" id="modeBook" onclick="setMode('book')">BOOK EV (ARB)</button>
  <button class="tog" id="perfTog" onclick="togglePerf()" style="margin-left:auto;">📊 PERFORMANCE</button>
</div>

<div class="bar" id="sumBar">
  <div class="stat"><div class="lbl">EXPOSURE</div><div class="val" id="s_exposure">$0.00</div></div>
  <div class="stat"><div class="lbl">UNREALIZED</div><div class="val" id="s_unrealized">$0.00</div></div>
  <div class="stat"><div class="lbl">AVG EV%</div><div class="val" id="s_avg_ev">—</div></div>
  <div class="stat"><div class="lbl">$ EV</div><div class="val" id="s_ev_d">$0</div></div>
  <div class="stat"><div class="lbl">FILLS TODAY</div><div class="val" id="s_fills">0</div><div class="sub" id="s_positions"></div></div>
  <div class="stat"><div class="lbl">CLV 10s</div><div class="val" id="s_clv10">—</div></div>
  <div class="stat"><div class="lbl">CLV 60s</div><div class="val" id="s_clv60">—</div></div>
</div>

<div id="perfPane" style="display:none;">
  <div class="perf" id="perfGrid"></div>
</div>

<div id="games"><div class="empty">loading…</div></div>

<div class="fills-strip">
  <h3 onclick="document.getElementById('fillsBody').style.display=document.getElementById('fillsBody').style.display==='none'?'block':'none';">▼ RECENT FILLS</h3>
  <div id="fillsBody"></div>
</div>

<script>
var evMode='devig';
var sortKey='ev_pct';
var sortDir=-1;
var collapsed={};

function setMode(m){
  evMode=m;
  document.getElementById('modeDevig').className='tog'+(m==='devig'?' on':'');
  document.getElementById('modeBook').className='tog'+(m==='book'?' on':'');
  load();
}

function fmt$(n){ if(n==null)return '—'; var s=(n>=0?'+':'-')+'$'+Math.abs(n).toFixed(2); return s; }
function fmt$2(n){ if(n==null)return '$0'; return '$'+n.toFixed(2); }
function fmtPct(n){ if(n==null)return '—'; return (n>=0?'+':'')+n.toFixed(1)+'%'; }
function cls(n){ if(n==null)return 'mutedcell'; return n>0?'pos':(n<0?'neg':''); }
function evCls(n){ if(n==null)return 'mutedcell'; if(n>5)return 'ev good'; if(n>0)return 'ev mid'; return 'ev bad'; }
// Hit-prob colors: NO holders WANT line to NOT hit. Low hit% = green (safe), high = red (line likely to hit).
function hpFmt(n){ if(n==null)return '—'; return n.toFixed(0)+'%'; }
function hpCls(n){ if(n==null)return 'mutedcell'; if(n>=70)return 'neg'; if(n>=40)return 'hp-mid'; return 'pos'; }

function load(){
  fetch('/api/live-pnl/snapshot?ev_mode='+evMode).then(function(r){return r.json();}).then(function(d){
    renderSummary(d.summary);
    renderGames(d.games||[]);
    document.getElementById('ts').textContent=new Date(d.ts*1000).toLocaleTimeString();
  }).catch(function(e){console.error(e);});
  fetch('/api/pnl/fill_log?prop_type=points&limit=20').then(function(r){return r.json();}).then(function(d){
    renderFills(d.fills||[]);
  }).catch(function(){});
}

function renderSummary(s){
  if(!s)return;
  var el=function(id){return document.getElementById(id);};
  var unreal=s.total_unrealized||0;
  var evd=s.total_ev_dollars||0;
  el('s_exposure').textContent=fmt$2(s.total_exposure||0);
  el('s_unrealized').textContent=fmt$(unreal);
  el('s_unrealized').className='val '+(unreal>0?'pos':(unreal<0?'neg':''));
  el('s_avg_ev').textContent=fmtPct(s.avg_ev_pct);
  el('s_avg_ev').className='val '+(s.avg_ev_pct>0?'pos':(s.avg_ev_pct<0?'neg':''));
  el('s_ev_d').textContent=fmt$(evd);
  el('s_ev_d').className='val '+(evd>0?'pos':(evd<0?'neg':''));
  el('s_fills').textContent=s.fills_today||0;
  el('s_positions').textContent=(s.positions_count||0)+' positions · '+(s.total_contracts||0)+' contracts';
  el('s_clv10').textContent=s.avg_clv_10s==null?'—':(s.avg_clv_10s>=0?'+':'')+s.avg_clv_10s.toFixed(1);
  el('s_clv10').className='val '+(s.avg_clv_10s>0?'pos':(s.avg_clv_10s<0?'neg':''));
  el('s_clv60').textContent=s.avg_clv_60s==null?'—':(s.avg_clv_60s>=0?'+':'')+s.avg_clv_60s.toFixed(1);
  el('s_clv60').className='val '+(s.avg_clv_60s>0?'pos':(s.avg_clv_60s<0?'neg':''));
}

function renderGames(games){
  var root=document.getElementById('games');
  if(!games.length){ root.innerHTML='<div class="empty">No open positions.</div>'; return; }
  var html='';
  for(var i=0;i<games.length;i++){
    var g=games[i];
    var phase=g.game_phase||'PRE';
    var urCls=(g.total_unrealized||0)>0?'pos':((g.total_unrealized||0)<0?'neg':'');
    var isCollapsed=collapsed[g.game_id];
    html+='<div class="game">';
    html+= '<div class="game-hdr" onclick="toggleGame(\''+g.game_id+'\')">';
    html+=  '<span class="game-name">'+g.game_name+'</span>';
    html+=  '<span class="phase '+phase+'">'+phase+'</span>';
    if(g.game_clock) html+= '<span class="clock">Q'+(g.game_period||'?')+' '+g.game_clock+'</span>';
    if(g.game_score) html+= '<span class="score">'+g.game_score+'</span>';
    html+=  '<span class="game-totals">';
    html+=    '<span>EXP <b>'+fmt$2(g.total_exposure)+'</b></span>';
    html+=    '<span>UNRL <b class="'+urCls+'">'+fmt$(g.total_unrealized)+'</b></span>';
    html+=    '<span>'+(isCollapsed?'▶':'▼')+'</span>';
    html+=  '</span>';
    html+= '</div>';
    if(!isCollapsed){
      html+=renderPlayers(g.players||[]);
    }
    html+='</div>';
  }
  root.innerHTML=html;
}

function toggleGame(gid){ collapsed[gid]=!collapsed[gid]; load(); }

function renderPlayers(players){
  // Sort per current sortKey
  players.sort(function(a,b){
    var va=a[sortKey]==null?-Infinity:a[sortKey];
    var vb=b[sortKey]==null?-Infinity:b[sortKey];
    if(sortKey==='avg_ev_pct'){ va=a.avg_ev_pct==null?-Infinity:a.avg_ev_pct; vb=b.avg_ev_pct==null?-Infinity:b.avg_ev_pct; }
    return (va-vb)*sortDir;
  });
  var h='<table><thead><tr>';
  h+='<th onclick="setSort(\'player\')">PLAYER</th>';
  h+='<th>PROP</th>';
  h+='<th>LINE</th>';
  h+='<th>STAT</th>';
  h+='<th title="Probability the line gets hit (over). High = bad for our NO position.">HIT%</th>';
  h+='<th>QTY</th>';
  h+='<th>AVG</th>';
  h+='<th>FAIR NO</th>';
  h+='<th class="'+(sortKey==='avg_ev_pct'?'active':'')+'" onclick="setSort(\'avg_ev_pct\')">EV%</th>';
  h+='<th>$ EV</th>';
  h+='<th class="'+(sortKey==='total_unrealized'?'active':'')+'" onclick="setSort(\'total_unrealized\')">UNRL</th>';
  h+='<th>BID</th>';
  h+='<th>STATUS</th>';
  h+='<th>MAX</th>';
  h+='</tr></thead><tbody>';
  for(var i=0;i<players.length;i++){
    var p=players[i];
    // Player summary row
    var evAvg=p.avg_ev_pct;
    var unr=p.total_unrealized||0;
    var maxBar='';
    if(p.player_max && p.player_max>0){
      var pct=Math.min(100,(p.total_contracts||0)/p.player_max*100);
      var barCls=pct>=90?'max':(pct>=70?'hot':'');
      maxBar='<div class="bar-wrap"><div class="bar-fill '+barCls+'" style="width:'+pct+'%"></div></div> <span style="font-size:9px;color:var(--t4)">'+Math.round(pct)+'%</span>';
    }
    // On-court pill: ON (green) / OFF (amber) / — (grey). Same scheme as main grid.
    var ocColor=(p.on_court===true)?'#00e676':((p.on_court===false)?'#ffb300':'var(--t4)');
    var ocText=(p.on_court===true)?'ON':((p.on_court===false)?'OFF':'—');
    var ocTitle=(p.on_court===true)?'on court':((p.on_court===false)?'bench':'on-court unknown');
    var ocPill='<span style="color:'+ocColor+';font-size:9px;font-weight:bold;letter-spacing:0.3px;margin-right:6px;display:inline-block;width:22px;text-align:center;" title="'+ocTitle+'">'+ocText+'</span>';
    h+='<tr class="player-row">';
    h+='<td>'+ocPill+p.player+'</td>';
    h+='<td style="text-transform:uppercase;color:var(--t3);">'+p.prop_type+'</td>';
    h+='<td>—</td>';
    h+='<td style="color:var(--b)">'+(p.live_stat==null?'—':p.live_stat)+'</td>';
    h+='<td class="'+hpCls(p.max_hit_prob)+'" style="font-weight:bold;">'+hpFmt(p.max_hit_prob)+'</td>';
    h+='<td>'+(p.total_contracts||0)+'</td>';
    h+='<td>—</td>';
    h+='<td>—</td>';
    h+='<td class="'+evCls(evAvg)+'">'+fmtPct(evAvg)+'</td>';
    h+='<td class="'+cls(unr)+'">'+fmt$(unr)+'</td>';
    h+='<td class="'+cls(unr)+'">'+fmt$(unr)+'</td>';
    h+='<td>—</td>';
    h+='<td>—</td>';
    h+='<td>'+maxBar+'</td>';
    h+='</tr>';
    // Per-line rows
    for(var j=0;j<p.lines.length;j++){
      var L=p.lines[j];
      var rowCls='line-row'+(L.row_state==='approaching'?' approaching':'')+(L.row_state==='hit'?' hit':'');
      h+='<tr class="'+rowCls+'">';
      h+='<td style="padding-left:20px;color:var(--t4);">└</td>';
      h+='<td></td>';
      h+='<td>'+(L.line==null?'—':L.line)+'</td>';
      h+='<td></td>';
      h+='<td class="'+hpCls(L.hit_prob)+'" title="'+(L.hit_prob_method||'')+'">'+hpFmt(L.hit_prob)+'</td>';
      h+='<td>'+(L.contracts||0)+'</td>';
      h+='<td>'+(L.avg_price==null?'—':L.avg_price+'c')+'</td>';
      h+='<td>'+(L.fair_no==null?'—':L.fair_no+'c')+'</td>';
      h+='<td class="'+evCls(L.ev_pct)+'">'+fmtPct(L.ev_pct)+'</td>';
      h+='<td class="'+cls(L.ev_dollars)+'">'+fmt$(L.ev_dollars)+'</td>';
      h+='<td class="'+cls(L.unrealized)+'">'+fmt$(L.unrealized)+'</td>';
      h+='<td>'+(L.best_bid_no||'—')+'</td>';
      h+='<td style="font-size:9px;color:var(--t3)">'+(L.status||'')+'</td>';
      h+='<td></td>';
      h+='</tr>';
    }
  }
  h+='</tbody></table>';
  return h;
}

function setSort(k){
  if(sortKey===k) sortDir=-sortDir; else { sortKey=k; sortDir=-1; }
  load();
}

function renderFills(fills){
  var body=document.getElementById('fillsBody');
  if(!fills.length){ body.innerHTML='<div style="color:var(--t4);font-size:10px;padding:6px;">No fills.</div>'; return; }
  var h='<div class="fill-row" style="color:var(--t4);"><span>TIME</span><span>PLAYER</span><span>LINE</span><span>QTY</span><span>PRICE</span><span>FAIR@FILL</span><span>CLV 10s</span><span>CLV 60s</span></div>';
  for(var i=0;i<fills.length;i++){
    var f=fills[i];
    var t=f.ts?f.ts.split('T')[1]||f.ts:'';
    var clv10=f.clv_10s==null?'—':(f.clv_10s>=0?'+':'')+Number(f.clv_10s).toFixed(1);
    var clv60=f.clv_60s==null?'—':(f.clv_60s>=0?'+':'')+Number(f.clv_60s).toFixed(1);
    var clv10cls=f.clv_10s==null?'mutedcell':(f.clv_10s>0?'pos':(f.clv_10s<0?'neg':''));
    var clv60cls=f.clv_60s==null?'mutedcell':(f.clv_60s>0?'pos':(f.clv_60s<0?'neg':''));
    h+='<div class="fill-row">';
    h+='<span>'+t.substr(0,8)+'</span>';
    h+='<span>'+f.player+'</span>';
    h+='<span>'+(f.line==null?'—':f.line)+'</span>';
    h+='<span>'+(f.contracts||0)+'</span>';
    h+='<span>'+(f.fill_price||0)+'c</span>';
    h+='<span>'+(f.fair_no_at_fill==null?'—':Number(f.fair_no_at_fill).toFixed(1)+'c')+'</span>';
    h+='<span class="'+clv10cls+'">'+clv10+'</span>';
    h+='<span class="'+clv60cls+'">'+clv60+'</span>';
    h+='</div>';
  }
  body.innerHTML=h;
}

function togglePerf(){
  var pane=document.getElementById('perfPane');
  var btn=document.getElementById('perfTog');
  if(pane.style.display==='none'){
    pane.style.display='block';
    btn.className='tog on';
    loadPerf();
  } else {
    pane.style.display='none';
    btn.className='tog';
  }
}

function loadPerf(){
  fetch('/api/live-pnl/performance').then(function(r){return r.json();}).then(function(d){
    var grid=document.getElementById('perfGrid');
    function card(title,a){
      if(!a) return '<div class="perf-card"><h4>'+title+'</h4><div class="row"><span>—</span></div></div>';
      var clv60cls=a.avg_clv_60s==null?'':(a.avg_clv_60s>0?'pos':'neg');
      return '<div class="perf-card"><h4>'+title+'</h4>'+
        '<div class="row"><span>FILLS</span><span>'+a.fills+'</span></div>'+
        '<div class="row"><span>AVG EV@FILL</span><span>'+(a.avg_ev_at_fill==null?'—':a.avg_ev_at_fill+'%')+'</span></div>'+
        '<div class="row"><span>AVG CLV 10s</span><span>'+(a.avg_clv_10s==null?'—':(a.avg_clv_10s>=0?'+':'')+a.avg_clv_10s)+'</span></div>'+
        '<div class="row"><span>AVG CLV 60s</span><span class="'+clv60cls+'">'+(a.avg_clv_60s==null?'—':(a.avg_clv_60s>=0?'+':'')+a.avg_clv_60s)+'</span></div>'+
        '<div class="row"><span>WIN RATE 60s</span><span>'+(a.win_rate_60s==null?'—':a.win_rate_60s+'%')+'</span></div>'+
        '<div class="row"><span>VOLUME</span><span>$'+(a.volume||0).toFixed(0)+'</span></div>'+
        '<div class="row"><span>$ EV</span><span>$'+(a.ev_dollars||0).toFixed(0)+'</span></div>'+
      '</div>';
    }
    var html=card('TODAY',d.today)+card('7 DAYS',d.seven_days)+card('ALL TIME',d.all_time);
    if(d.by_phase && d.by_phase.length){
      html+='<div class="perf-card" style="grid-column:1/-1;"><h4>BY PHASE</h4>';
      html+='<table style="width:100%;font-size:11px;"><thead><tr><th>PHASE</th><th>FILLS</th><th>AVG EV%</th><th>AVG CLV 60s</th></tr></thead><tbody>';
      for(var i=0;i<d.by_phase.length;i++){
        var p=d.by_phase[i];
        html+='<tr><td>'+p.phase+'</td><td>'+p.fills+'</td><td>'+(p.avg_ev_at_fill==null?'—':p.avg_ev_at_fill+'%')+'</td><td>'+(p.avg_clv_60s==null?'—':p.avg_clv_60s)+'</td></tr>';
      }
      html+='</tbody></table></div>';
    }
    grid.innerHTML=html;
  }).catch(function(){});
}

load();
setInterval(load, 2000);
</script>
</body></html>"""


@app.route("/api/pnl/fill_log")
def pnl_fill_log():
    """Historical fill log with prop_type filter. Includes CLV data."""
    prop_type = request.args.get("prop_type", "points")
    days = int(request.args.get("days", 7))
    limit = int(request.args.get("limit", 500))
    try:
        conn = sqlite3.connect(FILLS_DB)
        conn.row_factory = sqlite3.Row
        # Date filter: last N days
        cutoff = (datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
                  - timedelta(days=days)).strftime("%Y-%m-%d")
        sql = """SELECT * FROM fills
                 WHERE (prop_type = ? OR prop_type IS NULL)
                 AND ts >= ?
                 ORDER BY id DESC
                 LIMIT ?"""
        rows = conn.execute(sql, (prop_type, cutoff, limit)).fetchall()
        conn.close()
        fills = []
        for r in rows:
            fills.append({
                "id": r["id"],
                "ts": r["ts"],
                "ticker": r["ticker"],
                "player": r["player"],
                "line": r["line"],
                "contracts": r["contracts"],
                "fill_price": r["fill_price"],
                "ceil_no": r["ceil_no"] if "ceil_no" in r.keys() else None,
                "fair_no_at_fill": r["fair_no_at_fill"],
                "ev_at_fill": r["ev_at_fill"],
                "ceil_ev": r["ceil_ev"] if "ceil_ev" in r.keys() else None,
                "fair_no_10s": r["fair_no_10s"],
                "clv_10s": r["clv_10s"],
                "fair_no_60s": r["fair_no_60s"],
                "clv_60s": r["clv_60s"],
                "devig_method": r["devig_method"] if "devig_method" in r.keys() else None,
            })
        return jsonify({"fills": fills})
    except Exception as e:
        log.error(f"pnl_fill_log error: {e}")
        return jsonify({"error": str(e)}), 500


# ---------------------------------------------------------------------------
# UI — Inline HTML (same CSS variables and dark terminal look as Homer)
# ---------------------------------------------------------------------------

INDEX_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>JUMPBOT NBA PROPS</title>
<style>
:root {
  --bg:#080a0e; --panel:#0d0f15; --panel2:#12141c; --panel3:#181a24;
  --border:#1c1e2a; --border2:#252838;
  --t1:#f0f0f8; --t2:#8890aa; --t3:#4a5068; --t4:#2a2e40;
  --g:#00e676; --g2:#00e67618; --g3:#00e67630;
  --r:#ff3d57; --r2:#ff3d5718; --r3:#ff3d5730;
  --y:#ffb300; --y2:#ffb30018;
  --b:#2979ff; --b2:#2979ff18;
  --o:#ff6d00; --o2:#ff6d0018;
  --p:#b388ff; --p2:#b388ff18;
}
*{margin:0;padding:0;box-sizing:border-box;}
input[type=number]{-moz-appearance:textfield;}
input[type=number]::-webkit-inner-spin-button,
input[type=number]::-webkit-outer-spin-button{-webkit-appearance:none;margin:0;}
html,body{background:var(--bg);color:var(--t1);font-family:'SF Mono','Fira Code','Consolas',monospace;font-size:12px;}
body{padding-bottom:20px;}

.flash{position:fixed;top:0;left:0;right:0;z-index:999;padding:8px;font-size:12px;font-weight:bold;letter-spacing:1px;text-align:center;transform:translateY(-100%);transition:transform .15s;pointer-events:none;}
.flash.show{transform:translateY(0);}
.flash.ok{background:var(--g);color:#000;}
.flash.err{background:var(--r);color:#fff;}

.odds-ticker{background:#1a1a2e;border-bottom:1px solid var(--border);padding:4px 10px;font-size:10px;max-height:60px;overflow-y:auto;display:none;}
.odds-ticker .ot-row{display:flex;gap:8px;padding:2px 0;align-items:center;}
.odds-ticker .ot-time{color:var(--t4);min-width:55px;}
.odds-ticker .ot-player{color:var(--t1);font-weight:bold;min-width:140px;}
.odds-ticker .ot-book{color:var(--cyan);min-width:65px;text-transform:uppercase;font-size:9px;font-weight:bold;}
.odds-ticker .ot-move{font-weight:bold;}
.odds-ticker .ot-move.down{color:var(--r);}
.odds-ticker .ot-move.up{color:var(--g);}
.odds-ticker .ot-followers{color:var(--t3);font-size:9px;}

.hdr{position:sticky;top:0;z-index:100;background:var(--panel);border-bottom:1px solid var(--border);padding:8px 12px;}
.hdr-row{display:flex;align-items:center;gap:8px;}
.hdr-row+.hdr-row{margin-top:6px;}
.logo{font-size:16px;font-weight:bold;letter-spacing:3px;color:var(--g);}
.env-badge{font-size:8px;font-weight:bold;padding:2px 6px;border-radius:3px;letter-spacing:1px;background:var(--g2);border:1px solid var(--g);color:var(--g);}
.mode-badge{font-size:8px;font-weight:bold;padding:2px 6px;border-radius:3px;letter-spacing:1px;}
.mode-badge.live{background:var(--r3);border:1px solid var(--r);color:var(--r);animation:pulse .8s infinite;}
.mode-badge.paper{background:var(--y2);border:1px solid var(--y);color:var(--y);}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.5}}
.spacer{flex:1;}
.bal{font-size:13px;font-weight:bold;color:var(--g);}
.hbtn{font-size:9px;font-weight:bold;padding:4px 8px;border-radius:3px;border:1px solid var(--border2);background:var(--panel3);color:var(--t3);cursor:pointer;letter-spacing:1px;}
.hbtn:hover{border-color:var(--t3);}
.hbtn.stop{border-color:var(--r);color:var(--r);background:var(--r2);}
.hbtn.stop:hover{background:var(--r);color:#fff;}
.hbtn.gear{font-size:12px;padding:3px 7px;}
.evt-input{flex:1;min-width:0;background:var(--panel3);border:1px solid var(--border2);border-radius:4px;color:var(--t1);font-family:inherit;font-size:11px;font-weight:bold;padding:6px 8px;outline:none;text-transform:uppercase;letter-spacing:1px;}
.evt-input:focus{border-color:var(--g);}
.evt-input.sm{max-width:120px;flex:0 0 120px;}
.evt-go{padding:6px 14px;background:var(--g);border:none;border-radius:4px;color:#000;font-family:inherit;font-size:11px;font-weight:bold;cursor:pointer;letter-spacing:1px;}

.container{padding:6px 10px;}

.tab-bar{display:flex;gap:2px;padding:4px 10px;background:var(--panel);border-bottom:1px solid var(--border);overflow-x:auto;margin-bottom:6px;}
.tab{padding:6px 12px;font-size:10px;font-weight:bold;border:1px solid var(--border2);border-radius:4px 4px 0 0;background:var(--panel3);color:var(--t3);cursor:pointer;font-family:inherit;white-space:nowrap;}
.tab:hover{border-color:var(--t3);}
.tab.active{background:var(--panel);border-color:var(--b);color:var(--b);border-bottom-color:var(--panel);}
.tab.all-active{border-color:var(--g);color:var(--g);}
.tab.all-active.active{background:var(--g2);}
.tab-close{margin-left:6px;font-size:14px;color:var(--t3);cursor:pointer;line-height:1;}
.tab-close:hover{color:var(--r);}
.tab .smart-dot{display:inline-block;width:6px;height:6px;border-radius:50%;margin-right:5px;vertical-align:middle;}
.tab .smart-dot.waiting{background:var(--y);}
.tab .smart-dot.live{background:var(--g);animation:pulse .8s infinite;}
.tab .smart-dot.final{background:var(--t3);}
.tab .tab-time{font-size:8px;color:var(--t3);margin-left:4px;letter-spacing:.5px;}

.game-hdr{display:flex;align-items:center;gap:8px;padding:8px 10px;background:var(--panel2);border:1px solid var(--border);border-radius:6px;margin-bottom:4px;flex-wrap:wrap;}
.game-title{font-size:12px;font-weight:bold;color:var(--t1);flex:1;}
.collapse-chev{font-size:10px;color:var(--t3);margin-right:2px;}
.game-settings{display:flex;align-items:center;gap:12px;padding:6px 10px;background:var(--panel);border:1px solid var(--border);border-radius:4px;margin-bottom:4px;font-size:10px;flex-wrap:wrap;}
.gs-label{color:var(--t3);font-weight:bold;letter-spacing:1px;text-transform:uppercase;font-size:8px;}
.gs-input{width:52px;background:var(--panel3);border:1px solid var(--border2);color:var(--y);font-family:inherit;font-size:11px;padding:4px;border-radius:3px;text-align:center;}
.gs-item{display:flex;align-items:center;gap:4px;}
.gs-btn{padding:4px 8px;background:var(--g2);border:1px solid var(--g);color:var(--g);border-radius:3px;font-family:inherit;font-size:8px;font-weight:bold;cursor:pointer;letter-spacing:1px;}
.gs-apply{display:inline-flex;align-items:center;justify-content:center;width:16px;height:16px;background:var(--g2);border:1px solid var(--g);color:var(--g);border-radius:2px;font-size:10px;cursor:pointer;line-height:1;}.gs-apply:hover{background:var(--g);color:#000;}

.strike-table{border:1px solid var(--border);border-radius:6px;margin-bottom:6px;overflow-x:auto;min-width:0;}
.strike-hdr-row{display:flex;align-items:center;padding:7px 10px;background:var(--panel2);border-bottom:1px solid var(--border);font-size:9px;font-weight:bold;color:var(--t3);letter-spacing:1.5px;text-transform:uppercase;min-width:1200px;gap:0;white-space:nowrap;}
.strike-row{display:flex;align-items:center;padding:10px 10px;background:var(--panel);border-bottom:1px solid var(--border);min-height:50px;min-width:1200px;gap:0;}
.strike-row:hover{background:var(--panel2);}
.strike-row.row-jumping{background:rgba(0,230,118,0.06);}
.strike-row.row-stopped{background:rgba(100,100,100,0.06);opacity:0.5;}
.strike-row.row-prop-hit{background:rgba(160,100,255,0.12);opacity:0.6;}
.strike-row.row-waiting{background:rgba(255,179,0,0.06);}
.strike-row.row-filled{background:rgba(41,121,255,0.06);}

.seg{display:flex;align-items:center;gap:5px;padding:0 10px;border-right:1px solid var(--border);flex-shrink:0;}
.seg:last-child{border-right:none;}
.seg-name{min-width:150px;max-width:300px;flex:1;cursor:pointer;}
.seg-line{width:55px;justify-content:center;}
.seg-ctrl{min-width:220px;flex-shrink:0;gap:6px;}
.seg-odds{width:75px;justify-content:center;}
.seg-fair{width:65px;justify-content:center;border-right:1px solid var(--border);}
.seg-fairpct{width:65px;justify-content:center;border-right:1px solid var(--border);}
.seg-ev{width:70px;justify-content:center;border-right:1px solid var(--border);}
.seg-ceil{width:50px;justify-content:center;}
.seg-price{width:50px;justify-content:center;}
.seg-order{width:100px;justify-content:center;}
.seg-status{width:90px;justify-content:center;}
.seg-pos{min-width:140px;justify-content:center;gap:4px;}
.seg-tgt{width:60px;justify-content:center;}
.seg-toggle{width:40px;justify-content:center;border-right:none;}
.shdr{display:flex;align-items:center;padding:0 10px;border-right:1px solid var(--border);flex-shrink:0;}
.shdr:last-child{border-right:none;}
.player-sep{border-top:2px solid var(--border2);margin-top:2px;}
.adj-bar{display:flex;gap:12px;padding:3px 10px;font-size:9px;letter-spacing:0.5px;color:var(--t3);background:var(--bg);border-bottom:1px solid var(--border);}
.adj-bar .adj-item{display:flex;align-items:center;gap:4px;}
.adj-bar .adj-label{color:var(--t4);text-transform:uppercase;font-weight:bold;}
.adj-bar .adj-val{font-weight:bold;}
.adj-bar .adj-live{color:#82b1ff;}
.adj-bar .adj-dead{color:var(--t3);}
.ip-toggle{display:inline-flex;align-items:center;width:28px;height:14px;border-radius:7px;padding:2px;cursor:pointer;transition:background 0.2s;}
.ip-on{background:#82b1ff;}
.ip-off{background:var(--border2);}
.ip-dot{width:10px;height:10px;border-radius:50%;background:#fff;transition:transform 0.2s;}
.ip-on .ip-dot{transform:translateX(14px);}
.ip-off .ip-dot{transform:translateX(0);}
.prop-sel{padding:4px 8px;background:var(--panel);}
.prop-select{background:var(--bg);border:1px solid var(--g);color:var(--g);font-family:inherit;font-size:10px;font-weight:bold;letter-spacing:1.5px;padding:5px 24px 5px 10px;border-radius:4px;cursor:pointer;appearance:none;-webkit-appearance:none;background-image:url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='10' height='6'%3E%3Cpath d='M0 0l5 6 5-6z' fill='%2300e676'/%3E%3C/svg%3E");background-repeat:no-repeat;background-position:right 8px center;outline:none;}
.prop-select:hover{border-color:var(--g);background-color:var(--g2);}
.prop-select option{background:var(--bg);color:var(--t1);}
.player-summary{display:flex;gap:14px;padding:3px 12px;background:var(--bg2);border-top:1px solid var(--border);font-size:10px;color:var(--t3);}
.player-summary .ps-max{color:var(--y);font-weight:bold;}

.s-name{font-size:12px;font-weight:bold;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;flex:1;}
.s-line{font-size:13px;font-weight:bold;color:var(--y);}
.s-american{font-size:14px;font-weight:bold;color:#fff;}
.s-ceil{font-size:16px;font-weight:bold;color:#fff;}
.s-price{font-size:14px;font-weight:bold;color:var(--r);}
.s-order{font-size:10px;font-weight:bold;}
.s-order.active{color:var(--y);}
.s-order.none{color:var(--t4);}
.s-param{background:var(--panel3);border:1px solid var(--border2);color:var(--y);font-family:inherit;font-size:13px;font-weight:bold;padding:6px 5px;border-radius:3px;outline:none;width:50px;text-align:center;}
.s-param:focus{border-color:var(--y);}
.s-param.sz{color:var(--t2);width:44px;}
.p-box{display:flex;flex-direction:column;align-items:center;gap:1px;}
.p-lbl{font-size:6px;font-weight:bold;color:var(--t4);letter-spacing:.5px;text-transform:uppercase;line-height:1;}
.dir-btn{padding:4px 8px;font-family:inherit;font-size:9px;font-weight:bold;border:1px solid var(--border2);cursor:pointer;background:var(--panel3);color:var(--t3);border-radius:3px;}
.dir-btn.an{background:var(--r);color:#fff;border-color:var(--r);}
.s-ft-big{background:var(--panel3);border:1px solid var(--border2);color:var(--t2);font-family:inherit;font-size:12px;font-weight:bold;padding:6px 4px;border-radius:3px;outline:none;width:52px;text-align:center;}
.s-ft-big:focus{border-color:var(--y);}

.s-badge{font-size:7px;font-weight:bold;padding:2px 4px;border-radius:2px;letter-spacing:.5px;}
.s-badge.y{background:var(--g2);color:var(--g);}
.s-badge.n{background:var(--r2);color:var(--r);}
.s-badge.fill{background:var(--b2);color:var(--b);}
.s-badge.wait{background:var(--y2);color:var(--y);}
.s-badge.mention{background:var(--y2);color:var(--y);animation:pulse .5s infinite;}
.s-badge.bonded{background:rgba(160,100,255,0.2);color:#b07fff;animation:pulse .5s infinite;}
.s-badge.off{background:var(--panel3);color:var(--t4);}
.s-badge.pull{background:var(--o2);color:var(--o);}
.s-badge.no-odds{background:var(--o2);color:var(--o);animation:pulse .8s infinite;}
.row-no-odds{background:var(--o2) !important;}

.s-slider{position:relative;width:32px;height:16px;border-radius:8px;background:var(--panel3);border:1px solid var(--border2);cursor:pointer;flex-shrink:0;}
.s-slider.on{background:var(--g);border-color:var(--g);}
.s-slider.paused{background:var(--y);border-color:var(--y);}
.s-slider .dot{position:absolute;top:2px;left:2px;width:10px;height:10px;border-radius:50%;background:var(--t3);transition:left .15s;}
.s-slider.on .dot{left:18px;background:#000;}
.s-slider.paused .dot{left:18px;background:#000;}

.s-expand{display:none;padding:6px 10px;background:var(--panel2);border-bottom:1px solid var(--border);min-width:1200px;}
.s-expand.open{display:flex;gap:6px;align-items:center;flex-wrap:wrap;}
.s-btn{padding:4px 10px;border-radius:3px;font-family:inherit;font-size:9px;font-weight:bold;letter-spacing:1px;cursor:pointer;border:1px solid;}
.s-btn.go{background:var(--g2);border-color:var(--g);color:var(--g);}
.s-btn.go:hover{background:var(--g);color:#000;}
.s-btn.stp{background:var(--r2);border-color:var(--r);color:var(--r);}
.s-btn.stp:hover{background:var(--r);color:#fff;}
.s-btn.pll{background:var(--o2);border-color:var(--o);color:var(--o);}
.s-btn.pll:hover{background:var(--o);color:#000;}

.sec{background:var(--panel);border:1px solid var(--border);border-radius:6px;overflow:hidden;margin-bottom:4px;}
.sec-hdr{display:flex;align-items:center;justify-content:space-between;padding:7px 10px;background:var(--panel2);border-bottom:1px solid var(--border);cursor:pointer;}
.sec-title{font-size:10px;font-weight:bold;letter-spacing:2px;color:var(--t3);}
.sec-chev{color:var(--t3);transition:transform .2s;font-size:10px;}
.sec.collapsed .sec-chev{transform:rotate(-90deg);}
.sec.collapsed .sec-body{display:none;}
.sec-body{font-size:10px;}

.settings-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(130px,1fr));gap:6px;padding:10px;font-size:10px;}
.settings-grid label{display:flex;flex-direction:column;gap:2px;}
.settings-grid input,.settings-grid select{background:var(--panel3);border:1px solid var(--border2);color:var(--t1);font-family:inherit;font-size:10px;padding:4px 6px;border-radius:3px;outline:none;width:100%;}
.set-row{display:flex;gap:8px;align-items:center;padding:0 10px 8px;font-size:10px;}
.set-tog{padding:3px 8px;border-radius:3px;border:1px solid var(--border2);cursor:pointer;font-size:9px;font-weight:bold;font-family:inherit;}
.set-tog.on{background:var(--g);color:#000;border-color:var(--g);}
.set-save{padding:5px 14px;background:var(--g);color:#000;border:none;border-radius:3px;cursor:pointer;font-weight:bold;font-size:10px;font-family:inherit;margin:0 10px 8px;}

.pos-grid-hdr{display:grid;grid-template-columns:2fr 50px 60px 60px 70px;padding:5px 10px;font-size:8px;font-weight:bold;color:var(--t4);letter-spacing:1px;text-transform:uppercase;border-bottom:1px solid var(--border);}
.pos-grid-row{display:grid;grid-template-columns:2fr 50px 60px 60px 70px;padding:5px 10px;font-size:11px;border-bottom:1px solid var(--border);align-items:center;}
.pos-grid-row:last-child{border-bottom:none;}
.pos-empty{padding:12px;text-align:center;color:var(--t4);}

.order-row{display:flex;align-items:center;padding:5px 10px;border-bottom:1px solid var(--border);font-size:10px;gap:6px;}
.order-row:last-child{border-bottom:none;}
.order-info{flex:1;}
.order-name{font-weight:bold;font-size:10px;}
.pull-btn{background:none;border:1px solid var(--r);border-radius:3px;color:var(--r);font-family:inherit;font-size:9px;font-weight:bold;padding:2px 8px;cursor:pointer;}
.pull-btn:hover{background:var(--r);color:#fff;}

.log-body{max-height:250px;overflow-y:auto;}

.fill-table{width:100%;border-collapse:collapse;font-size:10px;}
.fill-table th{font-size:8px;color:var(--t4);letter-spacing:1px;font-weight:bold;text-transform:uppercase;padding:5px 8px;text-align:left;border-bottom:1px solid var(--border);}
.fill-table td{padding:4px 8px;border-bottom:1px solid var(--border);white-space:nowrap;}
.fill-table .num{text-align:right;font-variant-numeric:tabular-nums;}
.log-entry{padding:3px 10px;border-bottom:1px solid var(--border);display:flex;gap:6px;align-items:baseline;font-size:10px;}

/* P&L Page */
.pnl-top{display:grid;grid-template-columns:repeat(auto-fit,minmax(130px,1fr));gap:8px;padding:12px;background:var(--panel2);border:1px solid var(--border);border-radius:6px;margin-bottom:8px;}
.pnl-stat{padding:10px;background:var(--panel);border:1px solid var(--border2);border-radius:4px;text-align:center;}
.pnl-stat-lbl{font-size:8px;font-weight:bold;letter-spacing:1.5px;color:var(--t3);text-transform:uppercase;margin-bottom:5px;}
.pnl-stat-val{font-size:18px;font-weight:bold;color:var(--t1);font-variant-numeric:tabular-nums;}
.pnl-stat-val.pos{color:var(--g);}
.pnl-stat-val.neg{color:var(--r);}
.pnl-type-bar{display:flex;gap:4px;margin-bottom:8px;border-bottom:1px solid var(--border);padding:0 6px;}
.pnl-type-tab{padding:8px 16px;font-size:10px;font-weight:bold;letter-spacing:1.5px;color:var(--t3);background:var(--panel3);border:1px solid var(--border2);border-bottom:none;border-radius:4px 4px 0 0;cursor:pointer;}
.pnl-type-tab:hover{color:var(--t1);}
.pnl-type-tab.active{background:var(--g2);color:var(--g);border-color:var(--g);}
.pnl-table{width:100%;border-collapse:collapse;font-size:11px;}
.pnl-table th{font-size:8px;color:var(--t4);letter-spacing:1.5px;font-weight:bold;text-transform:uppercase;padding:7px 10px;text-align:left;border-bottom:1px solid var(--border);background:var(--panel2);}
.pnl-table td{padding:6px 10px;border-bottom:1px solid var(--border);font-variant-numeric:tabular-nums;}
.pnl-table td.num,.pnl-table th.num{text-align:right;}
.pnl-table tr:hover{background:var(--panel2);}
.pnl-gamesel{width:100%;max-width:400px;background:var(--panel3);border:1px solid var(--border2);color:var(--t1);font-family:inherit;font-size:11px;padding:6px 10px;border-radius:4px;margin:10px;outline:none;}
.pnl-player-row{display:flex;align-items:center;padding:8px 12px;border-bottom:1px solid var(--border);cursor:pointer;background:var(--panel);}
.pnl-player-row:hover{background:var(--panel2);}
.pnl-player-row .chev{width:14px;color:var(--t3);margin-right:8px;transition:transform .15s;}
.pnl-player-row.open .chev{transform:rotate(90deg);}
.pnl-player-name{flex:1;font-weight:bold;color:var(--t1);}
.pnl-player-stat{padding:0 12px;text-align:right;font-variant-numeric:tabular-nums;min-width:90px;}
.pnl-player-stat-lbl{font-size:8px;color:var(--t4);letter-spacing:1px;text-transform:uppercase;}
.pnl-lines{padding:8px 12px 12px 40px;background:var(--panel3);display:none;}
.pnl-lines.open{display:block;}
.pos{color:var(--g);}
.neg{color:var(--r);}
.log-entry:last-child{border-bottom:none;}
.log-ts{color:var(--t4);font-size:8px;flex-shrink:0;}
.log-act{font-weight:bold;flex-shrink:0;min-width:80px;}
.log-act.PLACED{color:var(--g);}
.log-act.CANCELLED{color:var(--t3);}
.log-act.WOULD_PLACE{color:var(--y);}
.log-act.START_ALL{color:var(--g);}
.log-act.STOP_ALL{color:var(--r);}
.log-act.PAUSE_ALL{color:var(--y);}
.log-act.RESUME_ALL{color:var(--g);}
.log-act.LIVE_MODE{color:var(--r);}
.log-act.PAPER_MODE{color:var(--y);}
.log-act.ACTIVATED{color:var(--g);}
.log-act.DEACTIVATED{color:var(--t3);}
.log-act.PULL_STRIKE{color:var(--o);}
.log-act.JUMP{color:var(--g);}
.log-act.PULL{color:var(--o);}
.log-act.STEPDOWN{color:var(--y);}
.log-act.ORDER_GONE{color:var(--o);}
.log-act.STRAGGLER_KILL{color:var(--r);}
.log-act.PAPER_JUMP{color:var(--y);}
.log-act.PAPER_PULL{color:var(--o);}
.log-act.LOADED{color:var(--b);}
.log-act.BONDED{color:var(--p);}
.log-act.MENTIONED{color:var(--y);}
.log-act.UNBONDED{color:var(--o);}
.log-det{color:var(--t3);flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;}
</style>
</head>
<body>

<div class="flash" id="flash"></div>
<div class="odds-ticker" id="oddsTicker"></div>

<div class="hdr">
  <div class="hdr-row">
    <span class="logo">JUMPBOT NBA PROPS</span>
    <span class="env-badge">PROD</span>
    <span class="mode-badge live" id="modeBadge">LIVE</span>
    <span class="spacer"></span>
    <span class="bal" id="bal">--</span>
    <button class="hbtn" id="liveBtn" onclick="toggleLive()">PAPER</button>
    <button class="hbtn" id="pnlBtn" onclick="togglePnLView()" style="border-color:var(--g);color:var(--g);background:var(--g2);">P&amp;L</button>
    <button class="hbtn gear" onclick="toggleSection('settingsSec')">&#9881;</button>
    <button class="hbtn" onclick="doStartAll()" style="border-color:var(--g);color:var(--g);">START ALL</button>
    <button class="hbtn" onclick="doStartAll('points')" style="border-color:#82b1ff;color:#82b1ff;font-size:9px;">PTS</button>
    <button class="hbtn" onclick="doStartAll('rebounds')" style="border-color:#ff9800;color:#ff9800;font-size:9px;">REB</button>
    <button class="hbtn" onclick="doStartAll('assists')" style="border-color:#ce93d8;color:#ce93d8;font-size:9px;">AST</button>
    <button class="hbtn" id="pauseBtn" onclick="togglePauseAll()" style="border-color:var(--y);color:var(--y);">PAUSE</button>
    <button class="hbtn stop" id="stopAllBtn" onclick="doStopAll()">STOP ALL</button>
    <button class="hbtn stop" onclick="doStopAll('points')" style="font-size:9px;">PTS</button>
    <button class="hbtn stop" onclick="doStopAll('rebounds')" style="font-size:9px;">REB</button>
    <button class="hbtn stop" onclick="doStopAll('assists')" style="font-size:9px;">AST</button>
    <button class="hbtn" id="smartBtn" onclick="toggleSmartMode()" style="border-color:var(--p);color:var(--p);">SMART MODE</button>
    <span id="smartStatus" style="font-size:10px;color:var(--t3);letter-spacing:1px;"></span>
    <span id="gamesActive" style="font-size:10px;color:var(--g);letter-spacing:1px;font-weight:bold;"></span>
  </div>
  <div class="hdr-row">
    <input class="evt-input" id="kalshiTicker" placeholder="Kalshi event ticker (e.g. KXNBAPRP-...)" autocomplete="off" spellcheck="false">
    <input class="evt-input sm" id="nbaGameId" placeholder="NBA game ID" autocomplete="off">
    <button class="evt-go" onclick="loadGame()">GO</button>
    <button class="hbtn" onclick="fetchNbaGames()" style="border-color:var(--b);color:var(--b);background:var(--b2);">SHOW NBA GAMES</button>
    <input type="date" id="loadDateInput" style="background:var(--panel3);border:1px solid var(--border2);color:var(--t1);font-family:inherit;font-size:11px;padding:4px 8px;border-radius:3px;">
    <button class="hbtn" onclick="loadByDate(['points'])" style="border-color:#82b1ff;color:#82b1ff;font-weight:bold;">LOAD PTS</button>
    <button class="hbtn" onclick="loadByDate(['rebounds'])" style="border-color:#ff9800;color:#ff9800;font-size:9px;">+REB</button>
    <button class="hbtn" onclick="loadByDate(['assists'])" style="border-color:#ce93d8;color:#ce93d8;font-size:9px;">+AST</button>
    <button class="hbtn" onclick="loadByDate(['points','rebounds','assists'])" style="border-color:var(--g);color:var(--g);background:var(--g2);font-size:9px;">ALL</button>
    <span id="loadTodayStatus" style="font-size:10px;color:var(--t3);"></span>
    <select id="gameSelect" style="display:none;background:var(--panel3);border:1px solid var(--border2);color:var(--t1);font-family:inherit;font-size:10px;padding:4px;border-radius:3px;max-width:300px;" onchange="selectGame()"></select>
  </div>
</div>

<!-- PHASE INDICATOR STRIP -->
<div id="phaseStrip" style="display:flex;gap:8px;padding:6px 10px;background:#0a0a0a;border-bottom:1px solid var(--border2);font-size:10px;font-family:inherit;color:var(--t3);overflow-x:auto;white-space:nowrap;align-items:center;">
  <span style="color:var(--t4);letter-spacing:1px;font-weight:bold;">PHASE:</span>
  <span id="phaseMaster" style="padding:2px 8px;border-radius:3px;font-weight:bold;letter-spacing:.5px;">—</span>
  <span id="phaseGames" style="display:flex;gap:6px;"></span>
  <span style="flex:1"></span>
  <button onclick="toggleOnCourtPanel()" id="onCourtToggle" style="background:transparent;border:1px solid var(--border2);color:var(--t3);font-family:inherit;font-size:10px;padding:2px 8px;border-radius:3px;cursor:pointer;letter-spacing:.5px;">ON-COURT ▾</button>
</div>

<!-- ON-COURT DEBUG PANEL (phase a — observe only) -->
<div id="onCourtPanel" style="display:none;padding:8px 12px;background:#070707;border-bottom:1px solid var(--border2);font-family:inherit;font-size:11px;color:var(--t2);">
  <div style="color:var(--t4);letter-spacing:1px;font-weight:bold;font-size:9px;margin-bottom:6px;">ON-COURT TRACKER · debug · updates every 2s</div>
  <div id="onCourtGames" style="display:flex;flex-direction:column;gap:8px;"></div>
</div>

<div class="container">

  <div class="tab-bar" id="tabBar" style="display:none;"></div>
  <div id="gameContent"></div>

  <!-- P&L PAGE -->
  <div id="pnlPage" style="display:none;">
    <div class="pnl-top">
      <div class="pnl-stat"><div class="pnl-stat-lbl">P&L (EST)</div><div class="pnl-stat-val" id="pnlTop_pnl">$--</div></div>
      <div class="pnl-stat"><div class="pnl-stat-lbl">VOLUME</div><div class="pnl-stat-val" id="pnlTop_vol">$--</div></div>
      <div class="pnl-stat"><div class="pnl-stat-lbl">FILLS</div><div class="pnl-stat-val" id="pnlTop_fills">--</div></div>
      <div class="pnl-stat"><div class="pnl-stat-lbl">AVG FILL EV%</div><div class="pnl-stat-val" id="pnlTop_ev">--</div></div>
      <div class="pnl-stat"><div class="pnl-stat-lbl">$ EV</div><div class="pnl-stat-val" id="pnlTop_dollarEv">$--</div></div>
      <div class="pnl-stat"><div class="pnl-stat-lbl">ROI%</div><div class="pnl-stat-val" id="pnlTop_roi">--</div></div>
      <div class="pnl-stat"><div class="pnl-stat-lbl">AVG CLV 10s</div><div class="pnl-stat-val" id="pnlTop_clv10">--</div></div>
      <div class="pnl-stat"><div class="pnl-stat-lbl">AVG CLV 60s</div><div class="pnl-stat-val" id="pnlTop_clv60">--</div></div>
      <div class="pnl-stat"><div class="pnl-stat-lbl">WIN RATE</div><div class="pnl-stat-val" id="pnlTop_win">--</div></div>
    </div>

    <div class="pnl-type-bar">
      <div class="pnl-type-tab active" data-type="points" onclick="pnlSelectType('points')">POINTS</div>
      <div class="pnl-type-tab" data-type="rebounds" onclick="pnlSelectType('rebounds')">REBOUNDS</div>
      <div class="pnl-type-tab" data-type="assists" onclick="pnlSelectType('assists')">ASSISTS</div>
    </div>

    <!-- TOP 10 POSITIONS -->
    <div class="sec" id="pnlTop10Sec">
      <div class="sec-hdr" onclick="toggleSection('pnlTop10Sec')">
        <span class="sec-title">TOP 10 POSITIONS BY EXPOSURE</span>
        <span class="sec-chev">&#9662;</span>
      </div>
      <div class="sec-body">
        <table class="pnl-table" id="pnlTop10Table">
          <thead><tr>
            <th>PLAYER</th><th class="num">CONTRACTS</th><th class="num">AVG PRICE</th><th class="num">EXPOSURE</th><th class="num">UNREALIZED</th>
          </tr></thead>
          <tbody></tbody>
        </table>
      </div>
    </div>

    <!-- BY GAME -->
    <div class="sec" id="pnlGamesSec">
      <div class="sec-hdr" onclick="toggleSection('pnlGamesSec')">
        <span class="sec-title">POSITIONS BY GAME</span>
        <span class="sec-chev">&#9662;</span>
      </div>
      <div class="sec-body">
        <select id="pnlGameSel" class="pnl-gamesel" onchange="pnlRenderGame()"></select>
        <div id="pnlGameContent"></div>
      </div>
    </div>

    <!-- FILL LOG -->
    <div class="sec" id="pnlFillLogSec">
      <div class="sec-hdr" onclick="toggleSection('pnlFillLogSec')">
        <span class="sec-title">FILL LOG</span>
        <span class="sec-chev">&#9662;</span>
      </div>
      <div class="sec-body">
        <table class="pnl-table" id="pnlFillLogTable">
          <thead><tr>
            <th>TIME</th><th>PLAYER</th><th class="num">LINE</th><th class="num">QTY</th>
            <th class="num">PRICE</th><th class="num">CEIL</th><th class="num">FAIR</th>
            <th class="num">EV%</th><th class="num">$ EV</th><th class="num">CLV 10s</th><th class="num">CLV 60s</th>
          </tr></thead>
          <tbody></tbody>
        </table>
      </div>
    </div>
  </div>

  <!-- SETTINGS -->
  <div class="sec collapsed" id="settingsSec">
    <div class="sec-hdr" onclick="toggleSection('settingsSec')">
      <span class="sec-title">SETTINGS</span><span class="sec-chev">&#9662;</span>
    </div>
    <div class="sec-body">
      <div class="settings-grid">
        <label>Cooldown (s) <input type="number" id="set_cooldown_secs" step="1" min="0"></label>
        <label>Heartbeat (s) <input type="number" id="set_heartbeat_interval" step="0.1" min="0.1"></label>
        <label>Max Exposure ($) <input type="number" id="set_max_exposure_dollars" step="10" min="1"></label>
        <label>DK Weight (0-100) <input type="number" id="set_dk_weight" min="0" max="100"></label>
        <label>Rounding
          <select id="set_rounding" style="background:var(--panel3);border:1px solid var(--border2);color:var(--t1);font-family:inherit;font-size:10px;padding:4px 6px;border-radius:3px;">
            <option value="up">Up</option><option value="down">Down</option><option value="dynamic">Dynamic</option>
          </select>
        </label>
        <label>Stepdown Delay (s) <input type="number" id="set_stepdown_delay_secs" step="1" min="0"></label>
        <label>Floor Offset <input type="number" id="set_floor_offset" step="1" min="1"></label>
        <label>Max Implied % <input type="number" id="set_max_implied_yes" step="1" min="50" max="99" title="Skip lines where Over implied is above this (e.g., 90 = skip -900+)"></label>
        <label>Ceiling Cap <input type="number" id="set_ceiling_cap" step="1" min="1" max="99" title="Max ceiling — pulls orders above this"></label>
        <label>Ceiling Floor <input type="number" id="set_ceiling_floor" step="1" min="1" max="99" title="Min ceiling — pulls orders below this"></label>
        <label>Slow HB (s) <input type="number" id="set_slow_heartbeat_interval" step="1" min="1"></label>
        <label>Reconcile (s) <input type="number" id="set_reconcile_interval" step="5" min="5"></label>
        <label>OB Poll (s) <input type="number" id="set_ob_poll_interval" step="0.1" min="0.2" max="5" title="OddsBlaze polling interval for FD + DK"></label>
        <label>Withdraw Thresh <input type="number" id="set_withdraw_above_ceil_threshold" step="100" min="0" title="If qty resting ABOVE ceil exceeds this, withdraw entirely (no floor fallback). 0 = disabled. Default 500."></label>
      </div>
      <div class="set-row">
        <button class="set-tog" id="stepdownTog" onclick="togSet('stepdown_enabled')">STEPDOWN: OFF</button>
        <button class="set-tog" id="floorTog" onclick="togSet('floor_enabled')">FLOOR: OFF</button>
        <button class="set-tog" id="joinCeilTog" onclick="togSet('join_ceil')">JOIN CEIL: OFF</button>
        <button class="set-tog" id="ceilModeTog" onclick="toggleCeilMode()" style="min-width:120px;">CEIL: ARB</button>
        <button class="set-tog" id="bookModeTog" onclick="toggleBookMode()" style="min-width:120px;">BOOK: MIN</button>
      </div>
      <div style="margin-top:10px;padding:10px;background:var(--panel3);border:1px solid var(--border2);border-radius:4px;">
        <div style="font-size:9px;font-weight:bold;letter-spacing:2px;text-transform:uppercase;color:var(--p);margin-bottom:8px;">1-WAY POWER DEVIG EXPONENTS</div>
        <div style="display:flex;gap:8px;flex-wrap:wrap;">
          <label style="display:flex;flex-direction:column;gap:2px;font-size:9px;color:var(--t3);">Heavy Fav (≥80%) <input type="number" id="set_devig_exp_heavy_fav" step="0.005" min="1.0" max="1.3" style="width:60px;background:var(--bg);border:1px solid var(--p);color:var(--p);font-family:inherit;font-size:12px;font-weight:bold;text-align:center;padding:4px;border-radius:3px;outline:none;"></label>
          <label style="display:flex;flex-direction:column;gap:2px;font-size:9px;color:var(--t3);">Mid High (55-79%) <input type="number" id="set_devig_exp_mid_high" step="0.005" min="1.0" max="1.3" style="width:60px;background:var(--bg);border:1px solid var(--p);color:var(--p);font-family:inherit;font-size:12px;font-weight:bold;text-align:center;padding:4px;border-radius:3px;outline:none;"></label>
          <label style="display:flex;flex-direction:column;gap:2px;font-size:9px;color:var(--t3);">Core (40-54%) <input type="number" id="set_devig_exp_mid_low" step="0.005" min="1.0" max="1.3" style="width:60px;background:var(--bg);border:1px solid var(--p);color:var(--p);font-family:inherit;font-size:12px;font-weight:bold;text-align:center;padding:4px;border-radius:3px;outline:none;"></label>
          <label style="display:flex;flex-direction:column;gap:2px;font-size:9px;color:var(--t3);">Longshot (<40%) <input type="number" id="set_devig_exp_longshot" step="0.005" min="1.0" max="1.3" style="width:60px;background:var(--bg);border:1px solid var(--p);color:var(--p);font-family:inherit;font-size:12px;font-weight:bold;text-align:center;padding:4px;border-radius:3px;outline:none;"></label>
        </div>
        <div style="font-size:8px;color:var(--t4);margin-top:6px;">raw^exp — lower exp = more aggressive vig strip. Two-way lines use normalization instead.</div>
      </div>

      <div style="margin-top:10px;padding:12px;background:var(--panel3);border:1px solid var(--o);border-radius:4px;">
        <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:10px;">
          <div style="font-size:9px;font-weight:bold;letter-spacing:2px;text-transform:uppercase;color:var(--o);">IN-PLAY ADJUSTERS <span style="font-size:8px;color:var(--t4);font-weight:normal;letter-spacing:0;margin-left:6px;text-transform:none;">applied when clock is running</span></div>
          <button class="set-tog" id="inplayTog2" onclick="togSet('inplay_enabled')" style="min-width:80px;">ENABLED</button>
        </div>
        <div style="display:flex;gap:8px;flex-wrap:wrap;">
          <label style="display:flex;flex-direction:column;gap:2px;font-size:9px;color:var(--t3);flex:1;min-width:100px;">
            SIZE %
            <input type="number" id="set_inplay_size_pct" step="5" min="10" max="200" title="% of normal size during live play — e.g. 50 = half size" style="width:100%;background:var(--bg);border:1px solid var(--o);color:var(--o);font-family:inherit;font-size:14px;font-weight:bold;text-align:center;padding:6px;border-radius:3px;outline:none;">
            <span style="font-size:8px;color:var(--t4);">e.g. 50 = half size</span>
          </label>
          <label style="display:flex;flex-direction:column;gap:2px;font-size:9px;color:var(--t3);flex:1;min-width:100px;">
            CEILING ADJ
            <input type="number" id="set_inplay_ceiling_adj" step="1" min="-20" max="20" title="Add to ceiling — negative lowers it" style="width:100%;background:var(--bg);border:1px solid var(--o);color:var(--o);font-family:inherit;font-size:14px;font-weight:bold;text-align:center;padding:6px;border-radius:3px;outline:none;">
            <span style="font-size:8px;color:var(--t4);">-2 lowers ceiling by 2c</span>
          </label>
          <label style="display:flex;flex-direction:column;gap:2px;font-size:9px;color:var(--t3);flex:1;min-width:100px;">
            REFILL DELAY (s)
            <input type="number" id="set_inplay_refill_delay_secs" step="1" min="0" max="60" title="Wait this long after a fill before reposting" style="width:100%;background:var(--bg);border:1px solid var(--o);color:var(--o);font-family:inherit;font-size:14px;font-weight:bold;text-align:center;padding:6px;border-radius:3px;outline:none;">
            <span style="font-size:8px;color:var(--t4);">wait after fill</span>
          </label>
          <label style="display:flex;flex-direction:column;gap:2px;font-size:9px;color:var(--t3);flex:1;min-width:100px;">
            POST-SCORE WINDOW (s)
            <input type="number" id="set_post_score_window_secs" step="1" min="0" max="60" title="Duration of extra-tight window after each score" style="width:100%;background:var(--bg);border:1px solid var(--o);color:var(--o);font-family:inherit;font-size:14px;font-weight:bold;text-align:center;padding:6px;border-radius:3px;outline:none;">
            <span style="font-size:8px;color:var(--t4);">score detection window</span>
          </label>
        </div>
      </div>

      <div style="margin-top:10px;padding:12px;background:var(--panel3);border:1px solid var(--border2);border-radius:4px;">
        <div style="font-size:9px;font-weight:bold;letter-spacing:2px;text-transform:uppercase;color:#00e676;margin-bottom:10px;">LIVE TRADING MODE <span style="font-size:8px;color:var(--t4);font-weight:normal;letter-spacing:0;text-transform:none;margin-left:6px;">powered by NBA play-by-play</span></div>
        <div style="display:flex;gap:10px;align-items:center;flex-wrap:wrap;">
          <button class="set-tog" id="deadballTog" onclick="togSet('deadball_only')" style="min-width:120px;">DEADBALL ONLY: OFF</button>
          <span style="font-size:9px;color:var(--t3);max-width:200px;">Only trade during timeouts, quarter breaks, halftime. Pulls orders when clock is running.</span>
          <button class="set-tog" id="q4StopTog" onclick="togSet('auto_stop_q4')" style="min-width:120px;">Q4 AUTO-STOP: OFF</button>
          <button class="set-tog" id="q3StopTog" onclick="togSet('auto_stop_q3')" style="min-width:120px;" title="Pull all orders at Q3 tip — trade only 1H + halftime (highest-CLV bucket)">Q3 AUTO-STOP: OFF</button>
          <span style="font-size:9px;color:var(--t3);max-width:200px;">Auto-stop all bots when 4th quarter starts.</span>
          <button class="set-tog" id="autoLiveTog" onclick="togSet('auto_start_live')" style="min-width:120px;">AUTO-START LIVE: OFF</button>
          <span style="font-size:9px;color:var(--t3);max-width:200px;">Auto-start all bots when game tips off. Load games pregame, bot starts on tipoff.</span>
          <button class="set-tog" id="reqFdTog" onclick="togSet('require_fanduel')" style="min-width:140px;" title="Refuse to trade any player|line that FanDuel doesn't have odds for. Prevents single-book DK-only trading when FD is suspended/down.">REQUIRE FANDUEL: ON</button>
          <span style="font-size:9px;color:var(--t3);max-width:200px;">Pulls orders when FD has no odds for a player|line. Safer than trading DK alone.</span>
          <button class="set-tog" id="distTierTog" onclick="togSet('dist_tier_enabled')" style="min-width:140px;" title="Cap per-order size by stat distance from the line. Distance ≤1 → no trade (pull orders); ≤2 → cap 25; ≤4 → cap 100; else uncapped. Catches late-game stat surges.">DIST TIER: ON</button>
          <span style="font-size:9px;color:var(--t3);max-width:200px;">Caps fills when player is close to the line. ≤1 stat away = no trade.</span>
        </div>
      </div>

      <div style="margin-top:10px;padding:12px;background:var(--panel3);border:1px solid var(--border2);border-radius:4px;">
        <div style="font-size:9px;font-weight:bold;letter-spacing:2px;text-transform:uppercase;color:var(--cyan);margin-bottom:10px;">MIN EV% BY CEILING PRICE</div>
        <div style="display:flex;gap:3px;">
          <div style="flex:1;display:flex;flex-direction:column;align-items:center;background:var(--panel);border:1px solid var(--border2);border-radius:4px;padding:8px 2px;">
            <span style="font-size:10px;color:var(--t1);font-weight:bold;margin-bottom:4px;">20-29c</span>
            <input type="number" class="tier-input" data-floor="20" step="0.5" value="8" style="width:48px;background:var(--bg);border:1px solid var(--border2);color:var(--cyan);font-family:inherit;font-size:14px;font-weight:bold;text-align:center;padding:6px 2px;border-radius:3px;outline:none;">
          </div>
          <div style="flex:1;display:flex;flex-direction:column;align-items:center;background:var(--panel);border:1px solid var(--border2);border-radius:4px;padding:8px 2px;">
            <span style="font-size:10px;color:var(--t1);font-weight:bold;margin-bottom:4px;">30-39c</span>
            <input type="number" class="tier-input" data-floor="30" step="0.5" value="7" style="width:48px;background:var(--bg);border:1px solid var(--border2);color:var(--cyan);font-family:inherit;font-size:14px;font-weight:bold;text-align:center;padding:6px 2px;border-radius:3px;outline:none;">
          </div>
          <div style="flex:1;display:flex;flex-direction:column;align-items:center;background:var(--panel);border:1px solid var(--border2);border-radius:4px;padding:8px 2px;">
            <span style="font-size:10px;color:var(--t1);font-weight:bold;margin-bottom:4px;">40-49c</span>
            <input type="number" class="tier-input" data-floor="40" step="0.5" value="6" style="width:48px;background:var(--bg);border:1px solid var(--border2);color:var(--cyan);font-family:inherit;font-size:14px;font-weight:bold;text-align:center;padding:6px 2px;border-radius:3px;outline:none;">
          </div>
          <div style="flex:1;display:flex;flex-direction:column;align-items:center;background:var(--panel);border:1px solid var(--border2);border-radius:4px;padding:8px 2px;">
            <span style="font-size:10px;color:var(--t1);font-weight:bold;margin-bottom:4px;">50-59c</span>
            <input type="number" class="tier-input" data-floor="50" step="0.5" value="5" style="width:48px;background:var(--bg);border:1px solid var(--border2);color:var(--cyan);font-family:inherit;font-size:14px;font-weight:bold;text-align:center;padding:6px 2px;border-radius:3px;outline:none;">
          </div>
          <div style="flex:1;display:flex;flex-direction:column;align-items:center;background:var(--panel);border:1px solid var(--border2);border-radius:4px;padding:8px 2px;">
            <span style="font-size:10px;color:var(--t1);font-weight:bold;margin-bottom:4px;">60-69c</span>
            <input type="number" class="tier-input" data-floor="60" step="0.5" value="4" style="width:48px;background:var(--bg);border:1px solid var(--border2);color:var(--cyan);font-family:inherit;font-size:14px;font-weight:bold;text-align:center;padding:6px 2px;border-radius:3px;outline:none;">
          </div>
          <div style="flex:1;display:flex;flex-direction:column;align-items:center;background:var(--panel);border:1px solid var(--border2);border-radius:4px;padding:8px 2px;">
            <span style="font-size:10px;color:var(--t1);font-weight:bold;margin-bottom:4px;">70-79c</span>
            <input type="number" class="tier-input" data-floor="70" step="0.5" value="3.5" style="width:48px;background:var(--bg);border:1px solid var(--border2);color:var(--cyan);font-family:inherit;font-size:14px;font-weight:bold;text-align:center;padding:6px 2px;border-radius:3px;outline:none;">
          </div>
          <div style="flex:1;display:flex;flex-direction:column;align-items:center;background:var(--panel);border:1px solid var(--border2);border-radius:4px;padding:8px 2px;">
            <span style="font-size:10px;color:var(--t1);font-weight:bold;margin-bottom:4px;">80-89c</span>
            <input type="number" class="tier-input" data-floor="80" step="0.5" value="3" style="width:48px;background:var(--bg);border:1px solid var(--border2);color:var(--cyan);font-family:inherit;font-size:14px;font-weight:bold;text-align:center;padding:6px 2px;border-radius:3px;outline:none;">
          </div>
          <div style="flex:1;display:flex;flex-direction:column;align-items:center;background:var(--panel);border:1px solid var(--border2);border-radius:4px;padding:8px 2px;">
            <span style="font-size:10px;color:var(--t1);font-weight:bold;margin-bottom:4px;">90c+</span>
            <input type="number" class="tier-input" data-floor="90" step="0.5" value="2" style="width:48px;background:var(--bg);border:1px solid var(--border2);color:var(--cyan);font-family:inherit;font-size:14px;font-weight:bold;text-align:center;padding:6px 2px;border-radius:3px;outline:none;">
          </div>
        </div>
        <div style="font-size:9px;color:var(--t3);margin-top:8px;">Default: <input type="number" id="set_default_min_ev" step="0.5" value="5" style="width:44px;background:var(--bg);border:1px solid var(--border2);color:var(--cyan);font-family:inherit;font-size:11px;font-weight:bold;padding:3px 4px;border-radius:3px;text-align:center;">% (fallback when no tier matches)</div>
      </div>

      <!-- PHASE-AWARE TRADING -->
      <div style="margin-top:12px;padding:12px;background:var(--panel3);border:1px solid var(--border2);border-radius:4px;">
        <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:10px;">
          <div style="font-size:10px;font-weight:bold;letter-spacing:2px;text-transform:uppercase;color:var(--cyan);">Phase-Aware Trading</div>
          <button id="phaseMasterTog" class="set-tog" onclick="togglePhaseMaster()" style="min-width:100px;">PHASE: ON</button>
        </div>
        <div style="font-size:9px;color:var(--t3);margin-bottom:8px;line-height:1.4;">
          On every phase change (tip-off, whistle, timeout, FTs), all orders are cancelled. The next heartbeat re-prices with that phase's config below. Disable a phase to stop trading during it entirely.
        </div>
        <table id="phaseTable" style="width:100%;border-collapse:collapse;font-size:10px;">
          <thead>
            <tr style="color:var(--t3);letter-spacing:.5px;">
              <th style="text-align:left;padding:4px 6px;font-weight:600;font-size:9px;text-transform:uppercase;">Phase</th>
              <th style="text-align:center;padding:4px 6px;font-weight:600;font-size:9px;text-transform:uppercase;">Trade</th>
              <th style="text-align:right;padding:4px 6px;font-weight:600;font-size:9px;text-transform:uppercase;">Size %</th>
              <th style="text-align:right;padding:4px 6px;font-weight:600;font-size:9px;text-transform:uppercase;">Arb ∆</th>
            </tr>
          </thead>
          <tbody id="phaseTbody"></tbody>
        </table>
        <button class="set-save" onclick="savePhaseSettings()" style="margin-top:10px;">SAVE PHASE SETTINGS</button>
      </div>

      <button class="set-save" onclick="saveSettings()" style="margin-top:8px;">SAVE</button>
    </div>
  </div>

  <!-- POSITIONS -->
  <div class="sec collapsed" id="posSec" style="display:none">
    <div class="sec-hdr" onclick="toggleSection('posSec')">
      <span class="sec-title">POSITIONS</span><span class="sec-chev">&#9662;</span>
    </div>
    <div class="sec-body">
      <div id="posList"><div class="pos-empty">No positions</div></div>
    </div>
  </div>

  <!-- RESTING ORDERS -->
  <div class="sec collapsed" id="restSec" style="display:none">
    <div class="sec-hdr" onclick="toggleSection('restSec')">
      <span class="sec-title">RESTING ORDERS</span><span class="sec-chev">&#9662;</span>
    </div>
    <div class="sec-body">
      <div id="restList"><div class="pos-empty">No resting orders</div></div>
    </div>
  </div>

  <!-- FILL LOG -->
  <div class="sec collapsed" id="fillSec" style="display:none">
    <div class="sec-hdr" onclick="toggleSection('fillSec')">
      <span class="sec-title">FILL LOG</span><span class="sec-chev">&#9662;</span>
    </div>
    <div class="sec-body">
      <div id="fillsBody" style="max-height:300px;overflow-y:auto;"><div class="pos-empty">No fills yet</div></div>
    </div>
  </div>

  <!-- PBP LIVE -->
  <div class="sec" id="pbpSec">
    <div class="sec-hdr" onclick="toggleSection('pbpSec')">
      <span class="sec-title">LIVE PLAY-BY-PLAY</span>
      <span id="pbpPhase" style="margin-left:8px;font-size:11px;font-weight:bold;"></span>
      <span class="sec-chev">&#9662;</span>
    </div>
    <div class="sec-body">
      <div id="pbpBody" style="font-family:monospace;font-size:11px;max-height:300px;overflow-y:auto;padding:4px;"></div>
    </div>
  </div>

  <!-- SESSION LOG -->
  <div class="sec" id="logSec">
    <div class="sec-hdr" onclick="toggleSection('logSec')">
      <span class="sec-title">SESSION LOG</span><span class="sec-chev">&#9662;</span>
    </div>
    <div class="sec-body">
      <div class="log-body" id="logBody"><div class="pos-empty">No actions yet</div></div>
    </div>
  </div>
</div>

<script>
var paperMode=false, isPaused=false, logSeenCount=0;
var pollIv=null, posIv=null, balIv=null, logIv=null, statusIv=null, fillIv=null, tickerIv=null, pbpIv=null, phaseIv=null;

function pollPBP(){
  fetch('/api/pbp/live').then(function(r){return r.json();}).then(function(d){
    if(d.error){document.getElementById('pbpBody').innerHTML='<span style="color:var(--t3)">'+d.error+'</span>';return;}
    var phEl=document.getElementById('pbpPhase');
    var cp=d.current_phase||'';
    if(cp==='TIMEOUT'){phEl.innerHTML='<span style="color:#00e676;background:#00e67620;padding:1px 6px;border-radius:3px;">⏸ TIMEOUT</span>';}
    else if(cp==='FOUL'||cp==='FREE_THROW'){phEl.innerHTML='<span style="color:#ffb300;background:#ffb30020;padding:1px 6px;border-radius:3px;">⏸ '+cp+'</span>';}
    else if(cp==='DEAD_BALL'){phEl.innerHTML='<span style="color:#82b1ff;background:#82b1ff20;padding:1px 6px;border-radius:3px;">⏸ DEAD BALL</span>';}
    else if(cp==='QUARTER_END'){phEl.innerHTML='<span style="color:#82b1ff;background:#82b1ff20;padding:1px 6px;border-radius:3px;">⏸ QUARTER END</span>';}
    else if(cp==='FINAL'){phEl.innerHTML='<span style="color:var(--t4);background:#ffffff10;padding:1px 6px;border-radius:3px;">FINAL</span>';}
    else{phEl.innerHTML='<span style="color:#ff4444;background:#ff444420;padding:1px 6px;border-radius:3px;">▶ LIVE</span>';}
    var body=document.getElementById('pbpBody');
    var html='<div style="color:var(--t3);margin-bottom:4px;">'+d.game+' ('+d.total_actions+' actions)</div>';
    var actions=(d.actions||[]).slice().reverse();
    for(var i=0;i<actions.length;i++){
      var a=actions[i];var clk=a.clock||'';
      if(clk.startsWith('PT')){try{var m=clk.match(/PT(\d+)M([\d.]+)S/);if(m)clk=parseInt(m[1])+':'+('0'+parseInt(parseFloat(m[2]))).slice(-2);}catch(e){}}
      var col='var(--t1)';
      if(a.phase==='TIMEOUT')col='#00e676';
      else if(a.phase==='FOUL')col='#ffb300';
      else if(a.phase==='FREE_THROW')col='#ffb300';
      else if(a.phase==='DEAD_BALL')col='#82b1ff';
      html+='<div style="color:'+col+';padding:1px 0;border-bottom:1px solid #ffffff08;">Q'+a.period+' '+clk+' | '+(a.team||'')+' | '+a.desc+'</div>';
    }
    body.innerHTML=html;
  }).catch(function(){});
}
pbpIv=setInterval(pollPBP,2000);
pollPBP();
var _lastEventTs=0;
var gamesData={};
var marketsData={};
var activeTab='__all__';
var _collapsedGames={};
var strikesOpen={};
var activeSubTab={};  // {gameId: "points"|"rebounds"|"assists"}
var currentSettings={};
var settingsFields=['cooldown_secs','heartbeat_interval','slow_heartbeat_interval','reconcile_interval','ob_poll_interval','max_exposure_dollars','dk_weight','stepdown_delay_secs','floor_offset','max_implied_yes','ceiling_cap','ceiling_floor','devig_exp_heavy_fav','devig_exp_mid_high','devig_exp_mid_low','devig_exp_longshot','inplay_size_pct','inplay_ceiling_adj','inplay_refill_delay_secs','post_score_window_secs','withdraw_above_ceil_threshold'];

function esc(s){if(!s)return '';var d=document.createElement('div');d.textContent=s;return d.innerHTML;}
function flash(m,t){var el=document.getElementById('flash');el.textContent=m;el.className='flash '+(t||'ok')+' show';setTimeout(function(){el.classList.remove('show');},2000);}
function toggleSection(id){document.getElementById(id).classList.toggle('collapsed');}
function safe(tk){return tk.replace(/[^a-zA-Z0-9]/g,'_');}

function toggleLive(){
  if(paperMode){
    var c=prompt('Type CONFIRM to enable live trading:');
    if(c!=='CONFIRM'){flash('CANCELLED','err');return;}
    fetch('/api/mode/live',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({confirm:'CONFIRM'})}).then(function(r){return r.json();}).then(function(d){
      if(d.ok){paperMode=false;updateModeUI();flash('LIVE MODE','err');}else flash(d.error||'Failed','err');
    }).catch(function(){flash('Error','err');});
  }else{
    fetch('/api/mode/paper',{method:'POST'}).then(function(r){return r.json();}).then(function(d){
      if(d.ok){paperMode=true;updateModeUI();flash('PAPER MODE','ok');}
    }).catch(function(){});
  }
}

function updateModeUI(){
  var badge=document.getElementById('modeBadge'),btn=document.getElementById('liveBtn');
  if(paperMode){badge.textContent='PAPER';badge.className='mode-badge paper';btn.textContent='GO LIVE';}
  else{badge.textContent='LIVE';badge.className='mode-badge live';btn.textContent='PAPER';}
}

// ======================= P&L PAGE =======================
var pnlActiveType='points';
var pnlOpenPlayers={};
var pnlIv=null;

function togglePnLView(){
  var pnl=document.getElementById('pnlPage');
  var gc=document.getElementById('gameContent');
  var tb=document.getElementById('tabBar');
  var btn=document.getElementById('pnlBtn');
  if(pnl.style.display==='none'){
    pnl.style.display='block';
    gc.style.display='none';
    tb.style.display='none';
    btn.style.background='var(--g)';btn.style.color='#000';
    pnlRefreshAll();
    if(!pnlIv)pnlIv=setInterval(pnlRefreshAll,3000);
  }else{
    pnl.style.display='none';
    gc.style.display='';
    tb.style.display='';
    btn.style.background='var(--g2)';btn.style.color='var(--g)';
    if(pnlIv){clearInterval(pnlIv);pnlIv=null;}
  }
}

function pnlSelectType(t){
  pnlActiveType=t;
  var tabs=document.querySelectorAll('.pnl-type-tab');
  for(var i=0;i<tabs.length;i++){tabs[i].classList.toggle('active',tabs[i].getAttribute('data-type')===t);}
  pnlRefreshAll();
}

function pnlRefreshAll(){
  pnlFetchSummary();
  pnlFetchTop10();
  pnlFetchByGame();
  pnlFetchFillLog();
}

function fmtDollar(v){if(v==null||isNaN(v))return '$--';var s=v>=0?'+':'-';return s+'$'+Math.abs(v).toFixed(2);}
function signClass(v){if(v==null)return '';return v>0?'pos':v<0?'neg':'';}

function pnlFetchSummary(){
  fetch('/api/pnl/summary?prop_type='+pnlActiveType).then(function(r){return r.json();}).then(function(d){
    if(d.error)return;
    var el=function(id){return document.getElementById(id);};
    el('pnlTop_pnl').textContent=fmtDollar(d.pnl_estimate_dollars);
    el('pnlTop_pnl').className='pnl-stat-val '+signClass(d.pnl_estimate_dollars);
    el('pnlTop_vol').textContent='$'+(d.total_volume_dollars||0).toFixed(0);
    el('pnlTop_fills').textContent=d.total_fills||0;
    el('pnlTop_ev').textContent=(d.avg_ev_at_fill||0).toFixed(2)+'%';
    el('pnlTop_ev').className='pnl-stat-val '+signClass(d.avg_ev_at_fill);
    // $ EV = volume * avg_ev / 100
    var dollarEv=(d.total_volume_dollars||0)*(d.avg_ev_at_fill||0)/100;
    el('pnlTop_dollarEv').textContent=fmtDollar(dollarEv);
    el('pnlTop_dollarEv').className='pnl-stat-val '+signClass(dollarEv);
    // ROI = pnl / volume * 100
    var roi=(d.total_volume_dollars>0)?((d.pnl_estimate_dollars||0)/d.total_volume_dollars*100):0;
    el('pnlTop_roi').textContent=(roi>=0?'+':'')+roi.toFixed(2)+'%';
    el('pnlTop_roi').className='pnl-stat-val '+signClass(roi);
    el('pnlTop_clv10').textContent=(d.avg_clv_10s>=0?'+':'')+(d.avg_clv_10s||0).toFixed(2);
    el('pnlTop_clv10').className='pnl-stat-val '+signClass(d.avg_clv_10s);
    el('pnlTop_clv60').textContent=(d.avg_clv_60s>=0?'+':'')+(d.avg_clv_60s||0).toFixed(2);
    el('pnlTop_clv60').className='pnl-stat-val '+signClass(d.avg_clv_60s);
    el('pnlTop_win').textContent=(d.win_rate||0).toFixed(1)+'%';
  });
}

function pnlFetchTop10(){
  fetch('/api/pnl/top_positions?prop_type='+pnlActiveType+'&limit=10').then(function(r){return r.json();}).then(function(d){
    var tbody=document.querySelector('#pnlTop10Table tbody');
    var html='';
    (d.positions||[]).forEach(function(p){
      var un=p.total_unrealized;
      html+='<tr>'+
        '<td>'+esc(p.player)+'</td>'+
        '<td class="num">'+p.total_contracts+'</td>'+
        '<td class="num">'+(p.avg_price||0).toFixed(1)+'c</td>'+
        '<td class="num">$'+(p.total_cost||0).toFixed(2)+'</td>'+
        '<td class="num '+signClass(un)+'">'+(un!=null?fmtDollar(un):'--')+'</td>'+
        '</tr>';
    });
    if(!html)html='<tr><td colspan="5" style="text-align:center;color:var(--t4);padding:20px;">No open positions</td></tr>';
    tbody.innerHTML=html;
  });
}

function pnlFetchByGame(){
  fetch('/api/pnl/by_game?prop_type='+pnlActiveType).then(function(r){return r.json();}).then(function(d){
    var sel=document.getElementById('pnlGameSel');
    var prevVal=sel.value;
    var gs=d.games||{};
    var opts='<option value="">-- Select Game --</option>';
    Object.keys(gs).forEach(function(gid){
      var g=gs[gid];
      opts+='<option value="'+esc(gid)+'">'+esc(g.game_name)+' ('+g.players.length+' players)</option>';
    });
    sel.innerHTML=opts;
    if(prevVal && gs[prevVal])sel.value=prevVal;
    window._pnlGameData=gs;
    pnlRenderGame();
  });
}

function pnlRenderGame(){
  var sel=document.getElementById('pnlGameSel');
  var gid=sel.value;
  var container=document.getElementById('pnlGameContent');
  if(!gid||!window._pnlGameData||!window._pnlGameData[gid]){
    container.innerHTML='';return;
  }
  var g=window._pnlGameData[gid];
  var html='';
  g.players.forEach(function(p){
    var isOpen=pnlOpenPlayers[gid+'|'+p.player];
    html+='<div class="pnl-player-row'+(isOpen?' open':'')+'" onclick="pnlTogglePlayer(\''+esc(gid)+'\',\''+esc(p.player)+'\')">'+
      '<span class="chev">&#9656;</span>'+
      '<span class="pnl-player-name">'+esc(p.player)+'</span>'+
      '<div class="pnl-player-stat"><div class="pnl-player-stat-lbl">Contracts</div><div>'+p.total_contracts+'</div></div>'+
      '<div class="pnl-player-stat"><div class="pnl-player-stat-lbl">Exposure</div><div>$'+p.total_exposure.toFixed(2)+'</div></div>'+
      '<div class="pnl-player-stat"><div class="pnl-player-stat-lbl">Unrealized</div><div class="'+signClass(p.total_unrealized)+'">'+fmtDollar(p.total_unrealized)+'</div></div>'+
      '</div>'+
      '<div class="pnl-lines'+(isOpen?' open':'')+'" id="lines-'+esc(gid+p.player)+'">'+
        '<table class="pnl-table"><thead><tr>'+
          '<th class="num">LINE</th><th class="num">QTY</th><th class="num">AVG</th><th class="num">CEIL</th>'+
          '<th class="num">FAIR NO</th><th class="num">EV%</th><th class="num">UNREALIZED</th>'+
        '</tr></thead><tbody>';
    p.lines.forEach(function(ln){
      html+='<tr>'+
        '<td class="num">'+(ln.line!=null?ln.line:'--')+'</td>'+
        '<td class="num">'+ln.contracts+'</td>'+
        '<td class="num">'+(ln.avg_price||0).toFixed(1)+'c</td>'+
        '<td class="num">'+(ln.ceil_no||'--')+'c</td>'+
        '<td class="num">'+(ln.fair_no!=null?ln.fair_no.toFixed(1)+'c':'--')+'</td>'+
        '<td class="num '+signClass(ln.ev_pct)+'">'+(ln.ev_pct!=null?ln.ev_pct.toFixed(1)+'%':'--')+'</td>'+
        '<td class="num '+signClass(ln.unrealized)+'">'+(ln.unrealized!=null?fmtDollar(ln.unrealized):'--')+'</td>'+
        '</tr>';
    });
    html+='</tbody></table></div>';
  });
  if(!html)html='<div style="padding:20px;text-align:center;color:var(--t4);">No positions in this game</div>';
  container.innerHTML=html;
}

function pnlTogglePlayer(gid,player){
  var key=gid+'|'+player;
  pnlOpenPlayers[key]=!pnlOpenPlayers[key];
  pnlRenderGame();
}

function pnlFetchFillLog(){
  fetch('/api/pnl/fill_log?prop_type='+pnlActiveType+'&limit=200').then(function(r){return r.json();}).then(function(d){
    var tbody=document.querySelector('#pnlFillLogTable tbody');
    var html='';
    (d.fills||[]).forEach(function(f){
      var ts=f.ts?f.ts.split(' ')[1]||f.ts:'--';
      var clv10=f.clv_10s;var clv60=f.clv_60s;
      var dolEv=(f.ev_at_fill!=null&&f.contracts&&f.fill_price)?((f.ev_at_fill/100)*f.contracts*f.fill_price/100):null;
      html+='<tr>'+
        '<td>'+esc(ts)+'</td>'+
        '<td>'+esc(f.player||'')+'</td>'+
        '<td class="num">'+(f.line!=null?f.line:'--')+'</td>'+
        '<td class="num">'+f.contracts+'</td>'+
        '<td class="num">'+f.fill_price+'c</td>'+
        '<td class="num">'+(f.ceil_no||'--')+'c</td>'+
        '<td class="num">'+(f.fair_no_at_fill!=null?f.fair_no_at_fill.toFixed(1):'--')+'</td>'+
        '<td class="num '+signClass(f.ev_at_fill)+'">'+(f.ev_at_fill!=null?f.ev_at_fill.toFixed(1)+'%':'--')+'</td>'+
        '<td class="num '+signClass(dolEv)+'">'+(dolEv!=null?fmtDollar(dolEv):'--')+'</td>'+
        '<td class="num '+signClass(clv10)+'">'+(clv10!=null?(clv10>=0?'+':'')+clv10.toFixed(1):'--')+'</td>'+
        '<td class="num '+signClass(clv60)+'">'+(clv60!=null?(clv60>=0?'+':'')+clv60.toFixed(1):'--')+'</td>'+
        '</tr>';
    });
    if(!html)html='<tr><td colspan="11" style="text-align:center;color:var(--t4);padding:20px;">No fills</td></tr>';
    tbody.innerHTML=html;
  });
}
// ======================= /P&L PAGE =======================

function fetchBalance(){
  fetch('/api/balance').then(function(r){return r.json();}).then(function(d){
    var b=d.balance||0;document.getElementById('bal').textContent='$'+b.toFixed(2);
  }).catch(function(){});
}

function pollStatus(){
  fetch('/api/status').then(function(r){return r.json();}).then(function(d){
    if(d.paper_mode!==paperMode){paperMode=d.paper_mode;updateModeUI();}
    var sb=document.getElementById('smartBtn');
    var ss=document.getElementById('smartStatus');
    var ga=document.getElementById('gamesActive');
    if(d.global_smart_mode){
      sb.style.background='rgba(187,134,252,.2)';sb.textContent='SMART: ON';
      var parts=[];
      if(d.games_live)parts.push(d.games_live+' LIVE');
      if(d.games_waiting)parts.push(d.games_waiting+' WAITING');
      if(d.games_final)parts.push(d.games_final+' FINAL');
      ss.textContent=parts.join(' | ');
    }else{sb.style.background='';sb.textContent='SMART MODE';ss.textContent='';}
    if(d.games_active)ga.textContent=d.games_active+' GAMES ACTIVE';
    else ga.textContent='';
  }).catch(function(){});
}

function toggleSmartMode(){
  fetch('/api/smart-mode',{method:'POST'}).then(function(r){return r.json();}).then(function(d){
    if(d.global_smart_mode)flash('SMART MODE ON','ok');
    else flash('SMART MODE OFF','err');
    pollStatus();
  });
}

function loadSettings(){
  return fetch('/api/settings').then(function(r){return r.json();}).then(function(s){
    currentSettings=s;
    for(var i=0;i<settingsFields.length;i++){
      var el=document.getElementById('set_'+settingsFields[i]);
      if(el)el.value=s[settingsFields[i]];
    }
    var rndEl=document.getElementById('set_rounding');if(rndEl)rndEl.value=s.rounding||'up';
    updSetTog('stepdownTog','STEPDOWN',s.stepdown_enabled);
    updSetTog('floorTog','FLOOR',s.floor_enabled);
    updSetTog('joinCeilTog','JOIN CEIL',s.join_ceil);
    var ipTog=document.getElementById('inplayTog2');
    if(ipTog){var ipOn=s.inplay_enabled!==false;ipTog.textContent=ipOn?'ENABLED':'DISABLED';ipTog.className='set-tog'+(ipOn?' on':'');}
    var dbTog=document.getElementById('deadballTog');
    if(dbTog){dbTog.textContent='DEADBALL ONLY: '+(s.deadball_only?'ON':'OFF');dbTog.className='set-tog'+(s.deadball_only?' on':'');}
    var q4Tog=document.getElementById('q4StopTog');
    if(q4Tog){q4Tog.textContent='Q4 AUTO-STOP: '+(s.auto_stop_q4?'ON':'OFF');q4Tog.className='set-tog'+(s.auto_stop_q4?' on':'');}
    var q3Tog=document.getElementById('q3StopTog');
    if(q3Tog){q3Tog.textContent='Q3 AUTO-STOP: '+(s.auto_stop_q3?'ON':'OFF');q3Tog.className='set-tog'+(s.auto_stop_q3?' on':'');}
    var fdTog=document.getElementById('reqFdTog');
    if(fdTog){fdTog.textContent='REQUIRE FANDUEL: '+(s.require_fanduel?'ON':'OFF');fdTog.className='set-tog'+(s.require_fanduel?' on':'');}
    var dtTog=document.getElementById('distTierTog');
    if(dtTog){var dtOn=s.dist_tier_enabled!==false;dtTog.textContent='DIST TIER: '+(dtOn?'ON':'OFF');dtTog.className='set-tog'+(dtOn?' on':'');}
    var alTog=document.getElementById('autoLiveTog');
    if(alTog){alTog.textContent='AUTO-START LIVE: '+(s.auto_start_live?'ON':'OFF');alTog.className='set-tog'+(s.auto_start_live?' on':'');}
    // Ceil mode button
    var cmBtn=document.getElementById('ceilModeTog');
    if(cmBtn){var cm=s.ceil_mode||'arb';cmBtn.textContent='CEIL: '+(cm==='arb'?'ARB':'TIERS');cmBtn.style.background=cm==='arb'?'var(--g2)':'var(--cyan2)';cmBtn.style.borderColor=cm==='arb'?'var(--g)':'var(--cyan)';cmBtn.style.color=cm==='arb'?'var(--g)':'var(--cyan)';}
    var bmBtn=document.getElementById('bookModeTog');
    if(bmBtn){var bm=s.book_mode||'min';bmBtn.textContent='BOOK: '+(bm==='min'?'MIN':'BLEND');bmBtn.style.background=bm==='min'?'var(--r2)':'var(--b2)';bmBtn.style.borderColor=bm==='min'?'var(--r)':'var(--b)';bmBtn.style.color=bm==='min'?'var(--r)':'var(--b)';}
    // Load min EV tiers
    var mevEl=document.getElementById('set_default_min_ev');if(mevEl)mevEl.value=s.default_min_ev||5;
    var tiers=s.min_ev_tiers||{};
    document.querySelectorAll('.tier-input').forEach(function(inp){
      var floor=inp.getAttribute('data-floor');
      if(tiers[floor]!=null)inp.value=tiers[floor];
    });
  }).catch(function(){});
}

function updSetTog(id,lbl,on){var b=document.getElementById(id);b.textContent=lbl+': '+(on?'ON':'OFF');b.className='set-tog'+(on?' on':'');}
function togSet(k){currentSettings[k]=!currentSettings[k];if(k==='stepdown_enabled')updSetTog('stepdownTog','STEPDOWN',currentSettings[k]);if(k==='floor_enabled')updSetTog('floorTog','FLOOR',currentSettings[k]);if(k==='join_ceil')updSetTog('joinCeilTog','JOIN CEIL',currentSettings[k]);if(k==='inplay_enabled'){var t=document.getElementById('inplayTog2');if(t){t.textContent=currentSettings[k]?'ENABLED':'DISABLED';t.className='set-tog'+(currentSettings[k]?' on':'');}}if(k==='deadball_only'){var d=document.getElementById('deadballTog');if(d){d.textContent='DEADBALL ONLY: '+(currentSettings[k]?'ON':'OFF');d.className='set-tog'+(currentSettings[k]?' on':'');}}if(k==='auto_stop_q4'){var q=document.getElementById('q4StopTog');if(q){q.textContent='Q4 AUTO-STOP: '+(currentSettings[k]?'ON':'OFF');q.className='set-tog'+(currentSettings[k]?' on':'');}}if(k==='auto_stop_q3'){var q3=document.getElementById('q3StopTog');if(q3){q3.textContent='Q3 AUTO-STOP: '+(currentSettings[k]?'ON':'OFF');q3.className='set-tog'+(currentSettings[k]?' on':'');}}if(k==='require_fanduel'){var fd=document.getElementById('reqFdTog');if(fd){fd.textContent='REQUIRE FANDUEL: '+(currentSettings[k]?'ON':'OFF');fd.className='set-tog'+(currentSettings[k]?' on':'');}}if(k==='dist_tier_enabled'){var dt=document.getElementById('distTierTog');if(dt){dt.textContent='DIST TIER: '+(currentSettings[k]?'ON':'OFF');dt.className='set-tog'+(currentSettings[k]?' on':'');}}if(k==='auto_start_live'){var al=document.getElementById('autoLiveTog');if(al){al.textContent='AUTO-START LIVE: '+(currentSettings[k]?'ON':'OFF');al.className='set-tog'+(currentSettings[k]?' on':'');}}}
function toggleCeilMode(){currentSettings.ceil_mode=currentSettings.ceil_mode==='arb'?'tiers':'arb';var btn=document.getElementById('ceilModeTog');btn.textContent='CEIL: '+(currentSettings.ceil_mode==='arb'?'ARB':'TIERS');btn.style.background=currentSettings.ceil_mode==='arb'?'var(--g2)':'var(--cyan2)';btn.style.borderColor=currentSettings.ceil_mode==='arb'?'var(--g)':'var(--cyan)';btn.style.color=currentSettings.ceil_mode==='arb'?'var(--g)':'var(--cyan)';}
function toggleBookMode(){currentSettings.book_mode=currentSettings.book_mode==='min'?'blend':'min';var btn=document.getElementById('bookModeTog');var m=currentSettings.book_mode;btn.textContent='BOOK: '+(m==='min'?'MIN':'BLEND');btn.style.background=m==='min'?'var(--r2)':'var(--b2)';btn.style.borderColor=m==='min'?'var(--r)':'var(--b)';btn.style.color=m==='min'?'var(--r)':'var(--b)';}

// Phase-aware trading UI
var PHASES_ALL = ["PRE","LIVE","OVERTIME","TIMEOUT","QUARTER_BREAK","HALFTIME","FOUL_SHOT","FINAL","UNKNOWN"];
var phaseSettingsLocal = null;    // cache of server phase_settings
var phaseMasterLocal = true;

function phaseColorClass(phase){
  if(phase==='LIVE'||phase==='OVERTIME') return 'var(--g)';
  if(phase==='PRE') return '#82b1ff';
  if(phase==='TIMEOUT'||phase==='QUARTER_BREAK'||phase==='HALFTIME') return 'var(--y)';
  if(phase==='FOUL_SHOT') return '#ce93d8';
  if(phase==='FINAL') return 'var(--t3)';
  return 'var(--t3)';
}

function loadPhaseSettings(){
  fetch('/api/settings/phase').then(function(r){return r.json()}).then(function(d){
    phaseSettingsLocal = d.phase_settings || {};
    phaseMasterLocal = !!d.phase_settings_enabled;
    renderPhaseTable();
    renderPhaseStrip(d.current_phases || {});
    var mBtn = document.getElementById('phaseMasterTog');
    if(mBtn){
      mBtn.textContent = 'PHASE: ' + (phaseMasterLocal ? 'ON' : 'OFF');
      mBtn.className = 'set-tog' + (phaseMasterLocal ? ' on' : '');
    }
  }).catch(function(){});
}

function renderPhaseTable(){
  var tbody = document.getElementById('phaseTbody');
  if(!tbody) return;
  // Don't clobber the table while the user is editing a cell. Re-render
  // only when no input inside the table currently has focus.
  if(tbody.contains(document.activeElement)) return;
  tbody.innerHTML = PHASES_ALL.map(function(ph){
    var cfg = (phaseSettingsLocal && phaseSettingsLocal[ph]) || {enabled:true, size_pct:100, arb_pct_delta:0};
    var onLbl = cfg.enabled ? 'ON' : 'OFF';
    var onCls = cfg.enabled ? 'on' : '';
    var color = phaseColorClass(ph);
    var phEsc = ph.replace(/'/g,"\\'");
    return '<tr style="border-top:1px solid var(--border);">'+
      '<td style="padding:5px 6px;color:'+color+';font-weight:600;letter-spacing:.5px;">'+ph+'</td>'+
      '<td style="padding:3px 6px;text-align:center;">'+
        '<button class="set-tog '+onCls+'" style="min-width:60px;padding:3px 8px;font-size:9px;" onclick="togglePhaseEnabled(\''+phEsc+'\')" id="phEn_'+ph+'">'+onLbl+'</button>'+
      '</td>'+
      '<td style="padding:3px 6px;text-align:right;">'+
        '<input type="number" id="phSize_'+ph+'" value="'+(cfg.size_pct||100)+'" min="0" max="1000" step="5" onchange="updatePhaseField(\''+phEsc+'\',\'size_pct\',this.value)" style="width:54px;background:var(--bg);border:1px solid var(--border2);color:var(--t1);font-family:inherit;font-size:11px;text-align:right;padding:3px 5px;border-radius:3px;">'+
      '</td>'+
      '<td style="padding:3px 6px;text-align:right;">'+
        '<input type="number" id="phArb_'+ph+'" value="'+(cfg.arb_pct_delta||0)+'" step="1" onchange="updatePhaseField(\''+phEsc+'\',\'arb_pct_delta\',this.value)" style="width:54px;background:var(--bg);border:1px solid var(--border2);color:var(--t1);font-family:inherit;font-size:11px;text-align:right;padding:3px 5px;border-radius:3px;">'+
      '</td>'+
    '</tr>';
  }).join('');
}

// Push the current phase_settings/master to the server. Called by any of
// the instant-save handlers below. Fire-and-forget — on failure the next
// poll will reveal divergence.
function syncPhaseSettings(){
  fetch('/api/settings/phase', {
    method:'POST',
    headers:{'Content-Type':'application/json'},
    body:JSON.stringify({
      phase_settings_enabled: phaseMasterLocal,
      phase_settings: phaseSettingsLocal || {},
    })
  }).catch(function(){});
}

function updatePhaseField(phase, field, raw){
  if(!phaseSettingsLocal) phaseSettingsLocal = {};
  var cur = phaseSettingsLocal[phase] || {enabled:true, size_pct:100, arb_pct_delta:0};
  var v = parseInt(raw);
  if(isNaN(v)) v = (field==='size_pct') ? 100 : 0;
  cur[field] = v;
  phaseSettingsLocal[phase] = cur;
  syncPhaseSettings();
}

function togglePhaseEnabled(phase){
  if(!phaseSettingsLocal) phaseSettingsLocal = {};
  var cur = phaseSettingsLocal[phase] || {enabled:true, size_pct:100, arb_pct_delta:0};
  cur.enabled = !cur.enabled;
  phaseSettingsLocal[phase] = cur;
  var btn = document.getElementById('phEn_'+phase);
  if(btn){ btn.textContent = cur.enabled?'ON':'OFF'; btn.className = 'set-tog'+(cur.enabled?' on':''); }
  syncPhaseSettings();
}

function togglePhaseMaster(){
  phaseMasterLocal = !phaseMasterLocal;
  var btn = document.getElementById('phaseMasterTog');
  btn.textContent = 'PHASE: ' + (phaseMasterLocal ? 'ON' : 'OFF');
  btn.className = 'set-tog' + (phaseMasterLocal ? ' on' : '');
  syncPhaseSettings();
}

function savePhaseSettings(){
  var cfg = {};
  PHASES_ALL.forEach(function(ph){
    var szEl = document.getElementById('phSize_'+ph);
    var arEl = document.getElementById('phArb_'+ph);
    var enabled = (phaseSettingsLocal && phaseSettingsLocal[ph]) ? !!phaseSettingsLocal[ph].enabled : true;
    cfg[ph] = {
      enabled: enabled,
      size_pct: szEl ? parseInt(szEl.value)||0 : 100,
      arb_pct_delta: arEl ? parseInt(arEl.value)||0 : 0,
    };
  });
  fetch('/api/settings/phase', {
    method:'POST',
    headers:{'Content-Type':'application/json'},
    body: JSON.stringify({phase_settings_enabled: phaseMasterLocal, phase_settings: cfg})
  }).then(function(r){return r.json()}).then(function(d){
    phaseSettingsLocal = d.phase_settings || {};
    phaseMasterLocal = !!d.phase_settings_enabled;
    if(typeof flash === 'function') flash('phase settings saved');
  }).catch(function(){});
}

// On-court debug panel — phase (a) observe-only. No trading impact.
var onCourtOpen = false;
var onCourtTimer = null;

function toggleOnCourtPanel(){
  onCourtOpen = !onCourtOpen;
  var panel = document.getElementById('onCourtPanel');
  var btn = document.getElementById('onCourtToggle');
  if(!panel) return;
  panel.style.display = onCourtOpen ? 'block' : 'none';
  if(btn) btn.textContent = onCourtOpen ? 'ON-COURT ▴' : 'ON-COURT ▾';
  if(onCourtOpen){
    pollOnCourt();
    onCourtTimer = setInterval(pollOnCourt, 2000);
  } else if(onCourtTimer){
    clearInterval(onCourtTimer);
    onCourtTimer = null;
  }
}

function pollOnCourt(){
  fetch('/api/on-court/debug').then(function(r){return r.json()}).then(function(d){
    renderOnCourtPanel(d.games || []);
  }).catch(function(){});
}

function renderOnCourtPanel(games){
  var el = document.getElementById('onCourtGames');
  if(!el) return;
  if(!games.length){
    el.innerHTML = '<span style="color:var(--t4);font-style:italic;">no games loaded</span>';
    return;
  }
  el.innerHTML = games.map(function(g){
    if(g.error){
      return '<div style="padding:6px 8px;background:#111;border-radius:4px;border-left:3px solid var(--t4);">' +
        '<div style="color:var(--t3);font-weight:bold;">'+(g.game_name||g.game_id)+'</div>' +
        '<div style="color:var(--t4);font-style:italic;font-size:10px;">'+g.error+'</div>' +
      '</div>';
    }
    var home = g.home || {tricode:'?',on_court:[],bench:[]};
    var away = g.away || {tricode:'?',on_court:[],bench:[]};
    var phaseTxt = g.phase || '';
    var clockTxt = g.period ? ('Q'+g.period+(g.clock?' '+g.clock:'')) : '';
    var ageSec = g.updated_at ? ((Date.now()/1000) - g.updated_at) : 0;
    var stale = g.stale || ageSec > 30;
    var srcTxt = g.source || '';
    var staleTxt = stale ? ' <span style="color:var(--r);">·STALE '+Math.round(ageSec)+'s</span>' : '';
    var renderSide = function(side, label){
      var onLine = (side.on_court||[]).join(', ') || '<i style="color:var(--t4)">(empty)</i>';
      var benchLine = (side.bench||[]).join(', ') || '<i style="color:var(--t4)">(empty)</i>';
      return '<div style="margin-top:4px;">' +
        '<div style="color:var(--t4);font-size:9px;letter-spacing:1px;">'+label+' · '+side.tricode+'</div>' +
        '<div><span style="color:var(--g);font-weight:bold;font-size:10px;">ON COURT:</span> '+onLine+'</div>' +
        '<div><span style="color:var(--o);font-weight:bold;font-size:10px;">BENCH:</span> <span style="color:var(--t2);">'+benchLine+'</span> ' +
          '<span style="color:var(--t4);font-size:9px;font-style:italic;">← full size eligible</span></div>' +
      '</div>';
    };
    return '<div style="padding:6px 10px;background:#111;border-radius:4px;border-left:3px solid var(--g);">' +
      '<div style="display:flex;justify-content:space-between;align-items:baseline;">' +
        '<div style="color:var(--t1);font-weight:bold;">'+away.tricode+' @ '+home.tricode+
          ' <span style="color:var(--t3);font-weight:normal;font-size:10px;">— '+phaseTxt+(clockTxt?' '+clockTxt:'')+'</span></div>' +
        '<div style="color:var(--t4);font-size:9px;">src:'+srcTxt+staleTxt+'</div>' +
      '</div>' +
      renderSide(away, 'AWAY') + renderSide(home, 'HOME') +
    '</div>';
  }).join('');
}

function renderPhaseStrip(currentPhases){
  var master = document.getElementById('phaseMaster');
  if(master){
    master.textContent = phaseMasterLocal ? 'ON' : 'OFF';
    master.style.background = phaseMasterLocal ? 'rgba(74,222,128,.12)' : 'rgba(100,100,100,.15)';
    master.style.color = phaseMasterLocal ? 'var(--g)' : 'var(--t3)';
  }
  var el = document.getElementById('phaseGames');
  if(!el) return;
  var entries = Object.keys(currentPhases || {});
  if(!entries.length){
    el.innerHTML = '<span style="color:var(--t4);font-style:italic;">no games loaded</span>';
    return;
  }
  el.innerHTML = entries.map(function(gid){
    var phase = currentPhases[gid] || 'UNKNOWN';
    var cfg = (phaseSettingsLocal && phaseSettingsLocal[phase]) || {};
    var trading = cfg.enabled === false ? ' OFF' : '';
    var sizeTxt = cfg.size_pct != null && cfg.size_pct !== 100 ? (' '+cfg.size_pct+'%') : '';
    var arbTxt = cfg.arb_pct_delta ? (' '+(cfg.arb_pct_delta>0?'+':'')+cfg.arb_pct_delta+'¢') : '';
    var color = phaseColorClass(phase);
    return '<span style="padding:2px 7px;border-radius:3px;background:'+color+'1f;color:'+color+';font-weight:600;">'+gid+': '+phase+trading+sizeTxt+arbTxt+'</span>';
  }).join('');
}

function saveSettings(){
  var p=Object.assign({},currentSettings);
  for(var i=0;i<settingsFields.length;i++){
    var el=document.getElementById('set_'+settingsFields[i]);
    if(el&&el.value!==''){var v=Number(el.value);if(!isNaN(v))p[settingsFields[i]]=v;}
  }
  var rndEl=document.getElementById('set_rounding');if(rndEl)p.rounding=rndEl.value;
  p.stepdown_enabled=currentSettings.stepdown_enabled;
  p.floor_enabled=currentSettings.floor_enabled;
  p.ceil_mode=currentSettings.ceil_mode||'arb';
  // Save min EV tiers
  var mevEl=document.getElementById('set_default_min_ev');if(mevEl)p.default_min_ev=parseFloat(mevEl.value)||5;
  var tiers={};
  document.querySelectorAll('.tier-input').forEach(function(inp){
    var floor=inp.getAttribute('data-floor');
    var val=parseFloat(inp.value);
    if(floor&&!isNaN(val))tiers[floor]=val;
  });
  p.min_ev_tiers=tiers;
  fetch('/api/settings',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(p)}).then(function(r){return r.json();}).then(function(d){
    if(d.ok){flash('SAVED','ok');currentSettings=d.settings;localStorage.setItem('nba_global_settings',JSON.stringify(d.settings));}else flash('FAILED','err');
  }).catch(function(){flash('FAILED','err');});
}

function fetchNbaGames(){
  fetch('/api/nba/games').then(function(r){return r.json();}).then(function(d){
    var sel=document.getElementById('gameSelect');
    sel.innerHTML='<option value="">Select a game...</option>';
    (d.games||[]).forEach(function(g){
      var opt=document.createElement('option');
      opt.value=g.game_id;
      var t='';try{var dt=new Date(g.startTime);t=' '+dt.toLocaleTimeString([],{hour:'numeric',minute:'2-digit'});}catch(e){}
      opt.textContent=g.away_abbr+' @ '+g.home_abbr+' ('+g.status+')'+t;
      sel.appendChild(opt);
    });
    sel.style.display='';
  });
}
function selectGame(){document.getElementById('nbaGameId').value=document.getElementById('gameSelect').value;}

function loadGame(){
  var kt=document.getElementById('kalshiTicker').value.trim();
  var nid=document.getElementById('nbaGameId').value.trim();
  if(!kt){flash('Enter Kalshi event ticker','err');return;}
  var payload={kalshi_event_ticker:kt,nba_game_id:nid,prop_type:'points'};
  fetch('/api/games/add',{method:'POST',headers:{'Content-Type':'application/json'},
    body:JSON.stringify(payload)
  }).then(function(r){return r.json();}).then(function(d){
    if(d.error){flash(d.error,'err');return;}
    flash('Loaded '+d.markets_loaded+' markets, '+d.odds_matched+' odds matched','ok');
    activeTab=d.game_id;
    var saved=JSON.parse(localStorage.getItem('nba_game_mappings')||'{}');
    saved[d.game_id]=payload;
    localStorage.setItem('nba_game_mappings',JSON.stringify(saved));
    document.getElementById('posSec').style.display='';
    document.getElementById('restSec').style.display='';
    document.getElementById('fillSec').style.display='';
    startPolling();
  }).catch(function(e){flash('Failed: '+e,'err');});
}

function loadByDate(propTypes){
  var dateInput=document.getElementById('loadDateInput');
  var dateVal=dateInput.value;
  var el=document.getElementById('loadTodayStatus');
  var label=(propTypes||['points']).map(function(p){return p.toUpperCase();}).join('+');
  el.textContent='Loading '+label+'...';
  var body=dateVal?{date:dateVal}:{date_offset:0};
  if(propTypes)body.prop_types=propTypes;
  fetch('/api/games/load_today',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)}).then(function(r){return r.json();}).then(function(d){
    if(d.error){flash(d.error,'err');el.textContent='Error';return;}
    flash('Loaded '+d.loaded+' games, '+d.total_markets+' '+label+' markets','ok');
    el.textContent=d.loaded+' games, '+d.total_markets+' mkts ('+label+')';
    document.getElementById('posSec').style.display='';
    document.getElementById('restSec').style.display='';
    document.getElementById('fillSec').style.display='';
    startPolling();
  }).catch(function(e){flash('Failed: '+e,'err');el.textContent='Error';});
}
// Set date picker to today (ET) by default
(function(){var d=new Date();d.setHours(d.getHours()-4);document.getElementById('loadDateInput').value=d.toISOString().split('T')[0];})();

function startPolling(){
  stopPolling();
  pollMarkets();pollIv=setInterval(pollMarkets,1000);
  pollPositions();posIv=setInterval(pollPositions,3000);
  pollStatus();statusIv=setInterval(pollStatus,2000);
  loadPhaseSettings();phaseIv=setInterval(loadPhaseSettings,3000);
  pollLog();logIv=setInterval(pollLog,1000);
  pollFills();fillIv=setInterval(pollFills,5000);
  pollTicker();tickerIv=setInterval(pollTicker,3000);
  fetchBalance();balIv=setInterval(fetchBalance,15000);
}

function stopPolling(){
  [pollIv,posIv,balIv,logIv,statusIv,fillIv,tickerIv,phaseIv].forEach(function(iv){if(iv)clearInterval(iv);});
  pollIv=posIv=balIv=logIv=statusIv=fillIv=tickerIv=phaseIv=null;
}

function pollMarkets(){
  fetch('/api/games/data').then(function(r){return r.json();}).then(function(data){
    gamesData=data;
    marketsData={};
    for(var gid in data){for(var tk in data[gid].markets){marketsData[tk]=data[gid].markets[tk];}}
    renderTabs();renderContent();
  }).catch(function(){});
}

function pollPositions(){
  Promise.all([
    fetch('/api/positions').then(function(r){return r.json();}),
    fetch('/api/orders/resting').then(function(r){return r.json();})
  ]).then(function(r){renderPositions(r[0]);renderResting(r[1]);}).catch(function(){});
}

function pollTicker(){
  fetch('/api/odds/events').then(function(r){return r.json();}).then(function(d){
    var el=document.getElementById('oddsTicker');
    if(!d.events||!d.events.length){el.style.display='none';return;}
    el.style.display='';
    var html='';
    var isNew=d.events.length>0&&d.events[0].ts>_lastEventTs;
    if(d.events.length>0)_lastEventTs=d.events[0].ts;
    d.events.slice(0,8).forEach(function(ev){
      var drop=ev.trigger_drop.toFixed(1);
      var followers='';
      if(ev.reactions&&ev.reactions.length){
        followers=ev.reactions.map(function(r){return r.book+' +'+r.delay_ms+'ms';}).join(', ');
        followers=' <span class="ot-followers">→ '+followers+'</span>';
      }
      html+='<div class="ot-row"><span class="ot-time">'+ev.ts_display+'</span>';
      html+='<span class="ot-player">'+esc(ev.player)+' '+ev.line+'+</span>';
      html+='<span class="ot-book">'+ev.trigger_book+'</span>';
      html+='<span class="ot-move down">▼'+drop+'</span>';
      html+='<span style="color:var(--t3);font-size:9px">'+ev.trigger_old_fair.toFixed(0)+'→'+ev.trigger_new_fair.toFixed(0)+'</span>';
      html+=followers+'</div>';
    });
    el.innerHTML=html;
    if(isNew)el.style.background='#2a1a1a';
    setTimeout(function(){el.style.background='';},1500);
  }).catch(function(){});
}

function pollFills(){
  fetch('/api/fills').then(function(r){return r.json();}).then(function(d){
    var fb=document.getElementById('fillsBody');
    if(!d.fills||!d.fills.length){fb.innerHTML='<div class="pos-empty">No fills yet</div>';return;}
    var html='<table class="fill-table"><thead><tr><th>TIME</th><th>PLAYER</th><th class="num">LINE</th><th class="num">QTY</th><th class="num">PRICE</th><th class="num">CEIL</th><th class="num">FAIR NO</th><th class="num">FD ODDS</th><th class="num">DK ODDS</th><th class="num">FILL EV%</th><th class="num">CEIL EV%</th><th class="num">FD 10s</th><th class="num">CLV 10s</th><th class="num">MOVE 10s</th><th class="num">FD 60s</th><th class="num">CLV 60s</th><th class="num">MOVE 60s</th></tr></thead><tbody>';
    d.fills.forEach(function(f){
      var fd10=f.fair_no_10s!==null?f.fair_no_10s.toFixed(1):'...';
      var clv10=f.clv_10s!==null?f.clv_10s.toFixed(1):'...';
      var move10=f.move_10s!==null?f.move_10s.toFixed(1):'...';
      var fd60=f.fair_no_60s!==null?f.fair_no_60s.toFixed(1):'...';
      var clv60=f.clv_60s!==null?f.clv_60s.toFixed(1):'...';
      var move60=f.move_60s!==null?f.move_60s.toFixed(1):'...';
      var clv10n=parseFloat(clv10),clv60n=parseFloat(clv60),move10n=parseFloat(move10),move60n=parseFloat(move60);
      var ceilEv=f.ceil_ev?f.ceil_ev.toFixed(1)+'%':'--';
      var ceilEvN=f.ceil_ev||0;
      var fmtOdds=function(o){if(o===null||o===undefined)return '--';return (o>0?'+':'')+o;};
      html+='<tr><td style="color:var(--t3)">'+esc(f.ts_display)+'</td><td>'+esc(f.player)+'</td>';
      html+='<td class="num">'+f.line+'</td><td class="num">'+f.contracts+'</td>';
      html+='<td class="num" style="color:var(--g)">'+f.fill_price+'c</td>';
      html+='<td class="num" style="color:var(--y)">'+(f.ceil_no||'--')+'c</td>';
      html+='<td class="num" style="color:var(--cyan)">'+(f.fair_no_at_fill?f.fair_no_at_fill.toFixed(1):'--')+'</td>';
      html+='<td class="num" style="color:var(--t3)">'+fmtOdds(f.fd_under_odds)+'</td>';
      html+='<td class="num" style="color:var(--t3)">'+fmtOdds(f.dk_under_odds)+'</td>';
      html+='<td class="num" style="color:'+(f.ev_at_fill>0?'var(--g)':f.ev_at_fill<0?'var(--r)':'')+'">'+f.ev_at_fill.toFixed(1)+'%</td>';
      html+='<td class="num" style="color:'+(ceilEvN>0?'var(--g)':ceilEvN<0?'var(--r)':'')+'">'+ceilEv+'</td>';
      html+='<td class="num">'+fd10+'</td>';
      html+='<td class="num" style="color:'+(clv10n>0?'var(--g)':clv10n<0?'var(--r)':'')+'">'+clv10+'</td>';
      html+='<td class="num" style="color:'+(move10n>0?'var(--g)':move10n<0?'var(--r)':'')+'">'+move10+'</td>';
      html+='<td class="num">'+fd60+'</td>';
      html+='<td class="num" style="color:'+(clv60n>0?'var(--g)':clv60n<0?'var(--r)':'')+'">'+clv60+'</td>';
      html+='<td class="num" style="color:'+(move60n>0?'var(--g)':move60n<0?'var(--r)':'')+'">'+move60+'</td></tr>';
    });
    html+='</tbody></table>';fb.innerHTML=html;
  }).catch(function(){});
}

function pollLog(){
  fetch('/api/log?since='+logSeenCount).then(function(r){return r.json();}).then(function(d){
    if(!d.entries||!d.entries.length){if(d.total&&logSeenCount>d.total)logSeenCount=0;return;}
    var body=document.getElementById('logBody');
    if(logSeenCount===0)body.innerHTML='';
    for(var i=0;i<d.entries.length;i++){
      var e=d.entries[i];var row=document.createElement('div');row.className='log-entry';
      var det=e.ticker||'';if(e.side)det+=' '+e.side;if(e.price)det+=' @'+e.price+'c';if(e.size)det+=' x'+e.size;
      if(e.detail)det+=' '+e.detail;
      row.innerHTML='<span class="log-ts">'+esc(e.ts)+'</span><span class="log-act '+esc(e.action)+'">'+esc(e.action)+'</span><span class="log-det">'+esc(det)+'</span>';
      body.appendChild(row);
    }
    logSeenCount=d.total;body.scrollTop=body.scrollHeight;
  }).catch(function(){});
}

function renderTabs(){
  var bar=document.getElementById('tabBar');
  var keys=Object.keys(gamesData);
  if(!keys.length){bar.style.display='none';return;}
  keys.sort(function(a,b){
    var at=gamesData[a].game_time?new Date(gamesData[a].game_time).getTime():9e15;
    var bt=gamesData[b].game_time?new Date(gamesData[b].game_time).getTime():9e15;
    return at-bt;
  });
  bar.style.display='';
  var html='<div class="tab all-active'+(activeTab==='__all__'?' active':'')+'" onclick="selectTab(\'__all__\')">ALL ACTIVE</div>';
  for(var i=0;i<keys.length;i++){
    var gid=keys[i];var g=gamesData[gid];
    var cls='tab'+(activeTab===gid?' active':'');
    var dot='';
    var ss=(g.smart_status||'').toLowerCase();
    if(g.smart_mode&&ss){dot='<span class="smart-dot '+ss+'"></span>';}
    var timeStr='';
    if(g.game_time){try{var d=new Date(g.game_time);timeStr='<span class="tab-time">'+d.toLocaleTimeString([],{hour:'numeric',minute:'2-digit'})+'</span>';}catch(e){}}
    // Phase badge on tab — icon + text so the current phase is visible
    // at a glance without hunting in logs. Colors mirror the PBP panel
    // (green=timeout, red=live, amber=foul_shot, blue=quarter/half break).
    var tbadge='';
    var tph=g.game_phase||'';
    function phBadge(ico, txt, col){
      return '<span style="font-size:9px;color:'+col+';background:'+col+'20;padding:1px 5px;border-radius:3px;margin-left:5px;font-weight:bold;letter-spacing:0.3px;">'+ico+' '+txt+'</span>';
    }
    if(tph==='TIMEOUT'||g.op_clock_stopped===true){tbadge=phBadge('⏸','TIMEOUT','#00e676');}
    else if(tph==='FOUL_SHOT'){tbadge=phBadge('⏸','FOUL','#ffb300');}
    else if(tph==='HALFTIME'){tbadge=phBadge('⏸','HALF','#82b1ff');}
    else if(tph==='QUARTER_BREAK'){tbadge=phBadge('⏸','QTR','#82b1ff');}
    else if(tph==='LIVE'){tbadge=phBadge('▶','LIVE','#ff4444');}
    else if(tph==='OVERTIME'){tbadge=phBadge('▶','OT','#ff4444');}
    else if(tph==='FINAL'){tbadge=phBadge('✓','FINAL','var(--t4)');}
    else if(tph==='PRE'){tbadge=phBadge('·','PRE','var(--t3)');}
    else if(tph){tbadge=phBadge('?',tph,'var(--t3)');}
    html+='<div class="'+cls+'" onclick="selectTab(\''+esc(gid)+'\')">'+dot+esc(g.game_name||gid)+tbadge+timeStr+
      '<span class="tab-close" onclick="event.stopPropagation();removeGame(\''+esc(gid)+'\')">&times;</span></div>';
  }
  bar.innerHTML=html;
}

function selectTab(id){activeTab=id;renderTabs();renderContent();}
function toggleGameCollapse(gid){_collapsedGames[gid]=!_collapsedGames[gid];_lastContentKey='';renderContent();}

function removeGame(gid){
  if(!confirm('Remove '+gid+'? This will cancel all orders for this game.'))return;
  fetch('/api/games/'+encodeURIComponent(gid)+'/remove',{method:'POST'}).then(function(r){return r.json();}).then(function(d){
    if(d.ok){
      flash('Removed '+gid,'ok');
      if(activeTab===gid)activeTab='__all__';
      _lastContentKey='';
      var saved=JSON.parse(localStorage.getItem('nba_game_mappings')||'{}');
      delete saved[gid];
      localStorage.setItem('nba_game_mappings',JSON.stringify(saved));
    }else flash(d.error||'Failed','err');
  }).catch(function(){flash('ERROR','err');});
}

var _lastContentKey='';

function renderContent(){
  var gc=document.getElementById('gameContent');
  if(activeTab==='__all__'){renderAllActive(gc);}
  else if(gamesData[activeTab]){renderGameSection(gc,activeTab,gamesData[activeTab]);}
  else{gc.innerHTML='<div style="padding:20px;color:var(--t3);">Select a game tab or load a game.</div>';}
}

function renderAllActive(gc){
  var active={};
  for(var gid in gamesData){
    var mkts=gamesData[gid].markets;
    for(var tk in mkts){var m=mkts[tk];if(m.active||m.order_id||m.position_no>0||m.status==='STOPPED'||m.status==='FILLED'||m.status==='WAITING'||m.status==='PULLED'){active[tk]=m;active[tk]._game_name=gamesData[gid].game_name||gid;}}
  }
  var tickers=Object.keys(active).sort(function(a,b){
    var pa=(active[a].player||active[a].name||a).toLowerCase();
    var pb=(active[b].player||active[b].name||b).toLowerCase();
    if(pa!==pb)return pa.localeCompare(pb);
    return (active[a].line||0)-(active[b].line||0);
  });
  var key='__all__:'+tickers.join(',');
  if(key!==_lastContentKey){
    _lastContentKey=key;
    gc.innerHTML='<div class="strike-table"><div class="strike-hdr-row">'+strikeHdrHtml()+'</div><div id="strikes__all__"></div></div>';
    var c=document.getElementById('strikes__all__');
    for(var i=0;i<tickers.length;i++){var tk=tickers[i];var m=active[tk];var sf=safe(tk);
      var div=document.createElement('div');div.id='sr-'+sf;div.innerHTML=buildStrike(m,sf,tk);c.appendChild(div);attachHandlers(tk,sf);}
  }
  for(var i=0;i<tickers.length;i++){updateStrike(active[tickers[i]],safe(tickers[i]),tickers[i]);}
}

function switchPropTab(gameId,pt){
  activeSubTab[gameId]=pt;
  _lastContentKey='';  // force full re-render
  renderContent();
}

function renderGameSection(gc,gameId,game){
  var mkts=game.markets||{};
  var activePT=activeSubTab[gameId]||'points';
  // Count markets per prop type
  var ptCounts={points:0,rebounds:0,assists:0};
  for(var tk in mkts){ptCounts[mkts[tk].prop_type||'points']=(ptCounts[mkts[tk].prop_type||'points']||0)+1;}
  // Filter to active prop type
  var allTickers=Object.keys(mkts);
  var tickers=allTickers.filter(function(tk){return (mkts[tk].prop_type||'points')===activePT;}).sort(function(a,b){
    var pa=(mkts[a].player||mkts[a].name||a).toLowerCase();
    var pb=(mkts[b].player||mkts[b].name||b).toLowerCase();
    if(pa!==pb)return pa.localeCompare(pb);
    return (mkts[a].line||0)-(mkts[b].line||0);
  });
  var key=gameId+':'+activePT+':'+tickers.length;
  var gs=game.settings||{};
  var sgid=safe(gameId);
  if(key!==_lastContentKey){
    _lastContentKey=key;
    var collapsed=_collapsedGames[gameId];
    // Game phase badge — also update dynamically via span ID
    var phase=game.game_phase||'';
    var phaseHtml='<span id="gphase_'+sgid+'">';
    if(phase==='LIVE'||phase==='OVERTIME'){
      var q=game.game_period||0;var ql=phase==='OVERTIME'?'OT':'Q'+q;
      phaseHtml='<span style="color:#00e676;font-size:10px;font-weight:bold;animation:pulse 1.5s infinite;">'+ql+' '+esc(game.game_clock||'')+'</span>';
      if(game.game_score)phaseHtml+='<span style="color:var(--t1);font-size:11px;font-weight:bold;margin-left:6px;">'+esc(game.game_score)+'</span>';
    }else if(phase==='TIMEOUT'){
      phaseHtml='<span style="color:#ffb300;font-size:10px;font-weight:bold;">TIMEOUT Q'+(game.game_period||'?')+'</span>';
      if(game.game_score)phaseHtml+='<span style="color:var(--t1);font-size:11px;font-weight:bold;margin-left:6px;">'+esc(game.game_score)+'</span>';
    }else if(phase==='HALFTIME'){
      phaseHtml='<span style="color:#82b1ff;font-size:10px;font-weight:bold;">HALFTIME</span>';
      if(game.game_score)phaseHtml+='<span style="color:var(--t1);font-size:11px;font-weight:bold;margin-left:6px;">'+esc(game.game_score)+'</span>';
    }else if(phase==='QUARTER_BREAK'){
      phaseHtml='<span style="color:#82b1ff;font-size:10px;font-weight:bold;">END Q'+(game.game_period||'?')+'</span>';
      if(game.game_score)phaseHtml+='<span style="color:var(--t1);font-size:11px;font-weight:bold;margin-left:6px;">'+esc(game.game_score)+'</span>';
    }else if(phase==='FINAL'){
      phaseHtml='<span style="color:var(--t4);font-size:10px;font-weight:bold;">FINAL</span>';
      if(game.game_score)phaseHtml+='<span style="color:var(--t3);font-size:11px;margin-left:6px;">'+esc(game.game_score)+'</span>';
    }

    phaseHtml+='</span>';

    // Status badge — prominent game state indicator
    var opClockHtml='<span id="opclock_'+sgid+'" style="font-size:11px;font-weight:bold;margin-left:8px;padding:2px 8px;border-radius:4px;letter-spacing:0.5px;">';
    if(game.op_clock_stopped===true||phase==='TIMEOUT'){opClockHtml+='<span style="color:#00e676;background:#00e67620;padding:2px 8px;border-radius:4px;border:1px solid #00e67640;">⏸ TIMEOUT Q'+(game.game_period||'?')+'</span>';}
    else if(phase==='HALFTIME'){opClockHtml+='<span style="color:#82b1ff;background:#82b1ff20;padding:2px 8px;border-radius:4px;border:1px solid #82b1ff40;">⏸ HALFTIME</span>';}
    else if(phase==='QUARTER_BREAK'){opClockHtml+='<span style="color:#82b1ff;background:#82b1ff20;padding:2px 8px;border-radius:4px;border:1px solid #82b1ff40;">⏸ END Q'+(game.game_period||'?')+'</span>';}
    else if(game.op_clock_stopped===false||phase==='LIVE'||phase==='OVERTIME'){var ql=phase==='OVERTIME'?'OT':'Q'+(game.game_period||'?');opClockHtml+='<span style="color:#ff4444;background:#ff444420;padding:2px 8px;border-radius:4px;border:1px solid #ff444440;animation:pulse 1.5s infinite;">▶ IN PLAY '+ql+'</span>';}
    else if(phase==='FINAL'){opClockHtml+='<span style="color:var(--t4);background:#ffffff10;padding:2px 8px;border-radius:4px;border:1px solid #ffffff20;">FINAL</span>';}
    else if(phase==='PRE'||!phase){opClockHtml+='<span style="color:var(--t3);background:#ffffff08;padding:2px 8px;border-radius:4px;border:1px solid #ffffff15;">PRE-GAME</span>';}
    opClockHtml+='</span>';

    var html='<div class="game-hdr"><div class="game-title" onclick="toggleGameCollapse(\''+esc(gameId)+'\')" style="cursor:pointer;">'+
      '<span class="collapse-chev">'+(collapsed?'&#9656;':'&#9662;')+'</span> '+esc(game.game_name||gameId)+'</div>'+
      '<div style="display:flex;align-items:center;gap:4px;">'+phaseHtml+opClockHtml+'</div>'+
      '<button class="hbtn" style="border-color:var(--g);color:var(--g);" onclick="gameStartAll(\''+esc(gameId)+'\')">START ALL</button>'+
      '<button class="hbtn" style="border-color:var(--y);color:var(--y);" onclick="gamePauseToggle(\''+esc(gameId)+'\')">PAUSE</button>'+
      '<button class="hbtn stop" onclick="gameStopAll(\''+esc(gameId)+'\')">STOP ALL</button></div>';
    if(collapsed){gc.innerHTML=html;return;}
    // In-play adjustment indicator + toggle
    html+='<div id="adjbar_'+sgid+'" class="adj-bar"></div>';

    // Prop type dropdown
    html+='<div class="prop-sel"><select class="prop-select" onchange="switchPropTab(\''+esc(gameId)+'\',this.value)">';
    [['points','POINTS'],['rebounds','REBOUNDS'],['assists','ASSISTS']].forEach(function(pt){
      var cnt=ptCounts[pt[0]]||0;
      var label=pt[1]+(cnt?' ('+cnt+')':'');
      html+='<option value="'+pt[0]+'"'+(pt[0]===activePT?' selected':'')+'>'+label+'</option>';
    });
    html+='</select></div>';
    // Settings bar — reads from active prop type's settings
    var ptgs=gs[activePT]||gs.points||{};
    html+='<div class="game-settings">'+
      '<div class="gs-item"><span class="gs-label">ARB %</span><input type="number" class="gs-input" id="gs_arb_'+sgid+'" value="'+(ptgs.arb_pct||0)+'"></div>'+
      '<div class="gs-item"><span class="gs-label">EXPOSURE</span><input type="number" class="gs-input" id="gs_maxexp_'+sgid+'" value="'+(gs.max_exposure||15000)+'" style="width:55px;"></div>'+
      '<div class="gs-item"><span class="gs-label">OFFSET</span><input type="number" class="gs-input" id="gs_offset_'+sgid+'" value="'+(ptgs.offset||0)+'"></div>'+
      '<button class="gs-btn" onclick="gameUpdateSettings(\''+esc(gameId)+'\')" style="background:var(--b2);border-color:var(--b);color:var(--b);">SAVE</button>'+
      '<span style="width:1px;height:20px;background:var(--border);margin:0 4px;"></span>'+
      '<div class="gs-item"><span class="gs-label">SIZE</span><input type="number" class="gs-input" id="gs_size_'+sgid+'" value="'+(ptgs.size||250)+'"><span class="gs-apply" onclick="gameApplyField(\''+esc(gameId)+'\',\'size\')">&darr;</span></div>'+
      '<div class="gs-item"><span class="gs-label">TARGET</span><input type="number" class="gs-input" id="gs_ft_'+sgid+'" value="'+(ptgs.fill_target||1500)+'"><span class="gs-apply" onclick="gameApplyField(\''+esc(gameId)+'\',\'fill_target\')">&darr;</span></div>'+
      '<div class="gs-item"><span class="gs-label">P.MAX</span><input type="number" class="gs-input" id="gs_pmax_'+sgid+'" value="'+(ptgs.player_max||5000)+'" style="width:55px;"><span class="gs-apply" onclick="gameApplyField(\''+esc(gameId)+'\',\'player_max\')">&darr;</span></div>'+
    '</div>';
    html+='<div class="strike-table"><div class="strike-hdr-row">'+strikeHdrHtml()+'</div><div id="strikes_'+sgid+'"></div></div>';
    gc.innerHTML=html;
    var c=document.getElementById('strikes_'+sgid);
    // Group tickers by player for summary bars
    var playerGroups={};var playerOrder=[];
    for(var i=0;i<tickers.length;i++){
      var tk=tickers[i];var m=mkts[tk];
      var p=(m.player||m.name||tk).toLowerCase();
      if(!playerGroups[p]){playerGroups[p]=[];playerOrder.push(p);}
      playerGroups[p].push(tk);
    }
    for(var pi=0;pi<playerOrder.length;pi++){
      var player=playerOrder[pi];var ptks=playerGroups[player];
      if(pi>0){var sep=document.createElement('div');sep.className='player-sep';c.appendChild(sep);}
      for(var j=0;j<ptks.length;j++){
        var tk=ptks[j];var m=mkts[tk];var sf=safe(tk);
        var div=document.createElement('div');div.id='sr-'+sf;
        div.innerHTML=buildStrike(m,sf,tk);c.appendChild(div);attachHandlers(tk,sf);
      }
      var sumDiv=document.createElement('div');sumDiv.className='player-summary';sumDiv.id='psum-'+safe(player);
      c.appendChild(sumDiv);
    }
  }
  for(var i=0;i<tickers.length;i++){updateStrike(mkts[tickers[i]],safe(tickers[i]),tickers[i]);}
  updatePlayerSummaries(mkts,tickers,gameId);
  // Settings bar inputs only update on DOM rebuild (tab switch / market count change)
  // Do NOT auto-refresh from server — causes race condition where poll resets
  // user's typed values before they click SAVE
  // Dynamic game phase update (no DOM rebuild needed)
  var gpe=document.getElementById('gphase_'+sgid);
  if(gpe){
    var ph=game.game_phase||'';var gph='';
    if(ph==='LIVE'||ph==='OVERTIME'){var ql=ph==='OVERTIME'?'OT':'Q'+(game.game_period||0);gph='<span style="color:#00e676;font-size:10px;font-weight:bold;animation:pulse 1.5s infinite;">'+ql+' '+esc(game.game_clock||'')+'</span>'+(game.game_score?'<span style="color:var(--t1);font-size:11px;font-weight:bold;margin-left:6px;">'+esc(game.game_score)+'</span>':'');}
    else if(ph==='TIMEOUT'){gph='<span style="color:#ffb300;font-size:10px;font-weight:bold;">TIMEOUT Q'+(game.game_period||'?')+'</span>'+(game.game_score?'<span style="color:var(--t1);font-size:11px;font-weight:bold;margin-left:6px;">'+esc(game.game_score)+'</span>':'');}
    else if(ph==='HALFTIME'){gph='<span style="color:#82b1ff;font-size:10px;font-weight:bold;">HALFTIME</span>'+(game.game_score?'<span style="color:var(--t1);font-size:11px;font-weight:bold;margin-left:6px;">'+esc(game.game_score)+'</span>':'');}
    else if(ph==='QUARTER_BREAK'){gph='<span style="color:#82b1ff;font-size:10px;font-weight:bold;">END Q'+(game.game_period||'?')+'</span>'+(game.game_score?'<span style="color:var(--t1);font-size:11px;font-weight:bold;margin-left:6px;">'+esc(game.game_score)+'</span>':'');}
    else if(ph==='FINAL'){gph='<span style="color:var(--t4);font-size:10px;font-weight:bold;">FINAL</span>'+(game.game_score?'<span style="color:var(--t3);font-size:11px;margin-left:6px;">'+esc(game.game_score)+'</span>':'');}
    gpe.innerHTML=gph;
  }
  // Status badge (dynamic update)
  var opce=document.getElementById('opclock_'+sgid);
  if(opce){
    var ph=game.game_phase||'';
    if(game.op_clock_stopped===true||ph==='TIMEOUT'){opce.innerHTML='<span style="color:#00e676;background:#00e67620;padding:2px 8px;border-radius:4px;border:1px solid #00e67640;">⏸ TIMEOUT Q'+(game.game_period||'?')+'</span>';}
    else if(ph==='HALFTIME'){opce.innerHTML='<span style="color:#82b1ff;background:#82b1ff20;padding:2px 8px;border-radius:4px;border:1px solid #82b1ff40;">⏸ HALFTIME</span>';}
    else if(ph==='QUARTER_BREAK'){opce.innerHTML='<span style="color:#82b1ff;background:#82b1ff20;padding:2px 8px;border-radius:4px;border:1px solid #82b1ff40;">⏸ END Q'+(game.game_period||'?')+'</span>';}
    else if(game.op_clock_stopped===false||ph==='LIVE'||ph==='OVERTIME'){var ql=ph==='OVERTIME'?'OT':'Q'+(game.game_period||'?');opce.innerHTML='<span style="color:#ff4444;background:#ff444420;padding:2px 8px;border-radius:4px;border:1px solid #ff444440;animation:pulse 1.5s infinite;">▶ IN PLAY '+ql+'</span>';}
    else if(ph==='FINAL'){opce.innerHTML='<span style="color:var(--t4);background:#ffffff10;padding:2px 8px;border-radius:4px;border:1px solid #ffffff20;">FINAL</span>';}
    else if(ph==='PRE'||!ph){opce.innerHTML='<span style="color:var(--t3);background:#ffffff08;padding:2px 8px;border-radius:4px;border:1px solid #ffffff15;">PRE-GAME</span>';}
  }
  // In-play adjustment indicator + toggle
  var adjEl=document.getElementById('adjbar_'+sgid);
  if(adjEl){
    var ph=game.game_phase||'';
    var ipEnabled=currentSettings.inplay_enabled!==false;
    var isLive=(ph==='LIVE'||ph==='OVERTIME')&&ipEnabled;
    var sizePct=isLive?(currentSettings.inplay_size_pct||50):100;
    var ceilAdj=isLive?(currentSettings.inplay_ceiling_adj||0):0;
    var refill=isLive?(currentSettings.inplay_refill_delay_secs||0):0;
    var cls=isLive?'adj-live':'adj-dead';
    var phLabel=ph||'PRE';
    // Toggle slider
    var slOn=ipEnabled;
    adjEl.innerHTML=
      '<span class="adj-item" style="cursor:pointer;" onclick="toggleInplay()">'+
        '<span class="adj-label">IN-PLAY</span>'+
        '<span class="ip-toggle '+(slOn?'ip-on':'ip-off')+'"><span class="ip-dot"></span></span>'+
      '</span>'+
      '<span class="adj-item"><span class="adj-label">PHASE:</span><span class="adj-val '+cls+'">'+phLabel+'</span></span>'+
      '<span class="adj-item"><span class="adj-label">SIZE:</span><span class="adj-val '+cls+'">'+sizePct+'%</span></span>'+
      '<span class="adj-item"><span class="adj-label">CEIL ADJ:</span><span class="adj-val '+cls+'">'+(ceilAdj>=0?'+':'')+ceilAdj+'</span></span>'+
      '<span class="adj-item"><span class="adj-label">REFILL:</span><span class="adj-val '+cls+'">'+refill+'s</span></span>';
  }
}

function strikeHdrHtml(){
  return '<div class="shdr seg-name">PLAYER</div>'+
    '<div class="shdr seg-line">LINE</div>'+
    '<div class="shdr seg-odds">CEIL AM</div>'+
    '<div class="shdr seg-odds">DK</div>'+
    '<div class="shdr seg-odds">FD</div>'+
    '<div class="shdr seg-odds">USED</div>'+
    '<div class="shdr seg-odds">FAIR NO</div>'+
    '<div class="shdr seg-ceil">CEIL</div>'+
    '<div class="shdr seg-ev">CEIL EV%</div>'+
    '<div class="shdr seg-fairpct">FAIR NO%</div>'+
    '<div class="shdr seg-price" style="margin-left:auto;">K YES</div>'+
    '<div class="shdr seg-price">K NO</div>'+
    '<div class="shdr seg-order">ORDER</div>'+
    '<div class="shdr seg-status">STATUS</div>'+
    '<div class="shdr seg-pos">POS</div>'+
    '<div class="shdr seg-tgt">ADJ</div>'+
    '<div class="shdr seg-tgt">SIZE</div>'+
    '<div class="shdr seg-tgt">TGT</div>'+
    '<div class="shdr seg-toggle">ON</div>';
}

function updatePlayerSummaries(mkts,tickers,gameId){
  var byPlayer={};
  for(var i=0;i<tickers.length;i++){
    var m=mkts[tickers[i]];
    var p=(m.player||m.name||'').toLowerCase();
    if(!byPlayer[p])byPlayer[p]={contracts:0,dollars:0,maxFill:0,weightedSum:0,lines:0};
    var pos=m.position_no||0;
    var avg=m.position_avg_no||0;
    byPlayer[p].contracts+=pos;
    byPlayer[p].dollars+=pos*avg/100;
    byPlayer[p].weightedSum+=pos*avg;
    byPlayer[p].lines++;
    if(pos>byPlayer[p].maxFill)byPlayer[p].maxFill=pos;
  }
  // Get player_max from game settings + per-player overrides
  var gs=(gamesData[gameId]&&gamesData[gameId].settings)||{};
  var defaultMax=gs.player_max||5000;
  var playerMaxes=(gamesData[gameId]&&gamesData[gameId].player_maxes)||{};
  for(var p in byPlayer){
    var el=document.getElementById('psum-'+safe(p));
    if(!el)continue;
    var d=byPlayer[p];
    var sp=safe(p);
    var thisMax=playerMaxes[p]||defaultMax;
    var pctFull=thisMax>0?Math.round(d.contracts/thisMax*100):0;
    var capColor=pctFull>=100?'var(--r)':(pctFull>=80?'var(--y)':'var(--t3)');
    // Check if the input is focused — skip full rebuild to avoid killing focus
    var existingInput=document.getElementById('pmax-'+sp);
    if(existingInput&&existingInput===document.activeElement){
      // Only update the text spans, leave input alone
      var statsEl=document.getElementById('pstats-'+sp);
      if(statsEl){
        var wtdAvg=d.contracts>0?(d.weightedSum/d.contracts).toFixed(1):'--';
        statsEl.innerHTML=d.contracts>0?
          '<span>$'+d.dollars.toFixed(2)+'</span>'+
          '<span>'+d.contracts+' ctrs</span>'+
          '<span>avg '+wtdAvg+'c</span>'+
          '<span class="ps-max">max line: '+d.maxFill+'</span>'+
          '<span style="color:'+capColor+';font-weight:bold;">'+d.contracts+'/'+thisMax+'</span>':
          '<span style="color:var(--t4);">no position</span>';
      }
      continue;
    }
    var wtdAvg=d.contracts>0?(d.weightedSum/d.contracts).toFixed(1):'--';
    el.innerHTML='<span id="pstats-'+sp+'" style="display:flex;gap:14px;">'+(d.contracts>0?
      '<span>$'+d.dollars.toFixed(2)+'</span>'+
      '<span>'+d.contracts+' ctrs</span>'+
      '<span>avg '+wtdAvg+'c</span>'+
      '<span class="ps-max">max line: '+d.maxFill+'</span>'+
      '<span style="color:'+capColor+';font-weight:bold;">'+d.contracts+'/'+thisMax+'</span>':
      '<span style="color:var(--t4);">no position</span>')+'</span>'+
      '<span style="margin-left:auto;display:flex;align-items:center;gap:4px;">'+
        '<span style="color:var(--t4);font-size:9px;">PLAYER MAX:</span>'+
        '<input type="number" id="pmax-'+sp+'" value="'+thisMax+'" min="0" style="width:55px;background:var(--bg);border:1px solid var(--border);color:var(--t1);font-family:inherit;font-size:10px;padding:2px 4px;border-radius:3px;text-align:center;" onclick="event.stopPropagation()" onchange="setPlayerMax(\''+esc(gameId)+'\',\''+esc(p)+'\',this.value)">'+
      '</span>';
  }
}

function toggleInplay(){
  var cur=currentSettings.inplay_enabled!==false;
  var next=!cur;
  fetch('/api/settings',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({inplay_enabled:next})}).then(function(r){return r.json();}).then(function(d){
    if(d.ok){currentSettings.inplay_enabled=next;flash('In-play adjusters '+(next?'ON':'OFF'),'ok');localStorage.setItem('nba_global_settings',JSON.stringify(d.settings));}
  });
}

function setPlayerMax(gameId,player,val){
  var v=parseInt(val)||0;
  fetch('/api/games/'+encodeURIComponent(gameId)+'/player-max',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({player:player,max:v})}).then(function(r){return r.json();}).then(function(d){
    if(d.ok)flash(player+' max set to '+v,'ok');
    else flash(d.error||'Error','err');
  }).catch(function(){flash('Error','err');});
}

function centsToAmerican(cents){
  if(!cents||cents<=0||cents>=100)return '--';
  if(cents>50)return '-'+Math.round(cents/(100-cents)*100);
  return '+'+Math.round((100-cents)/cents*100);
}

function getDevigExp(raw){
  if(raw>=0.80)return currentSettings.devig_exp_heavy_fav||1.08;
  if(raw>=0.55)return currentSettings.devig_exp_mid_high||1.10;
  if(raw>=0.40)return currentSettings.devig_exp_mid_low||1.11;
  return currentSettings.devig_exp_longshot||1.14;
}
function getFairValues(impliedYes,bookOdds){
  if(!impliedYes||impliedYes<=0)return null;
  var raw=impliedYes/100;
  var exp=getDevigExp(raw);
  var fairYes=Math.pow(raw,exp)*100;
  var fairNo=100-fairYes;
  var fairAm;
  if(fairYes>=50)fairAm=-Math.round(fairYes/(100-fairYes)*100);
  else fairAm=Math.round((100-fairYes)/fairYes*100);
  return {american:fairAm,fairYes:fairYes,fairNo:fairNo};
}

function ocDotAttrs(on){
  if(on===true)  return {color:'#00e676', title:'on court', text:'ON'};
  if(on===false) return {color:'#ffb300', title:'bench · off-court eligible', text:'OFF'};
  return {color:'var(--t4)', title:'on-court unknown', text:'—'};
}

// Convert YES-side American odds → NO-side American odds so the book column
// reads on the same side as ceil / fair NO. Book odds from OddsBlaze are
// stored YES-side; flipping here keeps the three odds columns consistent.
function yesAmToNoAm(am){
  if(!am) return null;
  var pYes = am < 0 ? (-am)/((-am)+100) : 100/(am+100);
  var pNo = 1 - pYes;
  if(pNo <= 0 || pNo >= 1) return null;
  if(pNo >= 0.5) return -Math.round(pNo/(1-pNo)*100);
  return Math.round((1-pNo)/pNo*100);
}

// PLAYING banner — shown when the player is on the floor AND the game is
// in active play. Hidden on timeout / halftime / quarter break / foul
// shot because those are dead-ball phases (no stats accruing right now).
function playingBannerHtml(m, sf){
  var live = (m.game_phase === 'LIVE' || m.game_phase === 'OVERTIME');
  var show = (m.on_court === true) && live;
  if(show){
    return '<span id="pb-'+sf+'" style="color:#fff;background:#ff4444;font-weight:bold;padding:1px 7px;border-radius:3px;font-size:9px;margin-right:6px;letter-spacing:0.6px;box-shadow:0 0 6px rgba(255,68,68,0.5);">▶ PLAYING</span>';
  }
  return '<span id="pb-'+sf+'" style="display:none;"></span>';
}

function buildStrike(m,sf,tk){
  var oc=ocDotAttrs(m.on_court);
  return '<div class="strike-row" id="sh-'+sf+'">'+
    '<div class="seg seg-name"><span id="oc-'+sf+'" style="color:'+oc.color+';font-size:9px;font-weight:bold;letter-spacing:0.3px;margin-right:6px;display:inline-block;width:22px;text-align:center;" title="'+oc.title+'">'+oc.text+'</span>'+playingBannerHtml(m,sf)+'<span class="s-name">'+esc(m.player||m.name)+'</span></div>'+
    '<div class="seg seg-line"><span class="s-line" id="ln-'+sf+'">'+(m.line||'--')+'</span></div>'+
    '<div class="seg seg-odds"><span id="clam-'+sf+'" style="font-size:11px;font-weight:bold;">--</span></div>'+
    '<div class="seg seg-odds"><span class="s-american" id="ao-dk-'+sf+'" style="font-size:11px;">--</span></div>'+
    '<div class="seg seg-odds"><span class="s-american" id="ao-fd-'+sf+'" style="font-size:11px;">--</span></div>'+
    '<div class="seg seg-odds"><span class="s-american" id="ao-'+sf+'" style="font-size:11px;font-weight:bold;">--</span></div>'+
    '<div class="seg seg-odds"><span id="fair-am-'+sf+'" style="font-size:11px;font-weight:bold;">--</span></div>'+
    '<div class="seg seg-ceil"><div style="text-align:center"><span class="s-ceil" id="cl-'+sf+'">'+(m.ceil_no||'--')+'c</span></div></div>'+
    '<div class="seg seg-ev"><span id="fair-ev-'+sf+'" style="font-size:11px;font-weight:bold;">--</span></div>'+
    '<div class="seg seg-fairpct"><span id="fair-no-'+sf+'" style="font-size:11px;font-weight:bold;">--</span></div>'+
    '<div class="seg seg-price" style="margin-left:auto;"><span id="py-'+sf+'" style="font-size:11px;color:var(--g);">--</span></div>'+
    '<div class="seg seg-price"><span class="s-price" id="pr-'+sf+'">--</span></div>'+
    '<div class="seg seg-order"><span class="s-order none" id="no-'+sf+'">--</span></div>'+
    '<div class="seg seg-status"><span class="s-status" id="sb-'+sf+'"></span></div>'+
    '<div class="seg seg-pos"><span id="sf-'+sf+'" style="display:flex;align-items:center;gap:4px;font-size:11px;font-weight:bold;"></span></div>'+
    '<div class="seg seg-tgt"><span id="adjsz-'+sf+'" style="font-size:10px;color:var(--t4);">--</span></div>'+
    '<div class="seg seg-tgt"><input class="s-ft-big" id="sz-'+sf+'" type="number" value="'+(m.size||250)+'" min="1" onclick="event.stopPropagation()" style="width:44px;"></div>'+
    '<div class="seg seg-tgt"><input class="s-ft-big" id="ft-'+sf+'" type="number" value="'+(m.fill_target||1500)+'" min="1" onclick="event.stopPropagation()"></div>'+
    '<div class="seg seg-toggle"><div class="s-slider" id="sl-'+sf+'" onclick="event.stopPropagation();togSlider(this,&quot;'+esc(tk)+'&quot;)"><div class="dot"></div></div></div>'+
  '</div>'+
  '<div class="s-expand" id="sx-'+sf+'">'+
    '<span id="sorders-'+sf+'" style="font-size:9px;color:var(--t3);margin-right:auto;"></span>'+
    '<div id="sbtns-'+sf+'" style="display:flex;gap:4px;"></div>'+
  '</div>';
}

function attachHandlers(tk,sf){
  document.getElementById('sh-'+sf).addEventListener('click',function(e){
    if(e.target.tagName==='SELECT'||e.target.tagName==='OPTION'||e.target.tagName==='INPUT'||e.target.tagName==='BUTTON')return;
    strikesOpen[tk]=!strikesOpen[tk];
    document.getElementById('sx-'+sf).classList.toggle('open',!!strikesOpen[tk]);
  });
  if(strikesOpen[tk])document.getElementById('sx-'+sf).classList.add('open');
  var szEl=document.getElementById('sz-'+sf);
  if(szEl)szEl.addEventListener('change',function(){
    var val=parseInt(this.value)||250;
    fetch('/api/markets/'+encodeURIComponent(tk)+'/update',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({size:val})}).catch(function(){});
  });
  var ftEl=document.getElementById('ft-'+sf);
  if(ftEl)ftEl.addEventListener('change',function(){
    var val=parseInt(this.value)||1500;
    fetch('/api/markets/'+encodeURIComponent(tk)+'/update',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({fill_target:val})}).catch(function(){});
  });
}

function updateStrike(m,sf,tk){
  var row=document.getElementById('sh-'+sf);
  if(row){
    row.className='strike-row';
    if(m.prop_done)row.classList.add('row-prop-hit');
    else if(m.status==='MENTIONED')row.classList.add('row-waiting');
    else if(m.status==='NO_ODDS')row.classList.add('row-no-odds');
    else if(m.status==='FILLED')row.classList.add('row-filled');
    else if(m.status==='STOPPED'||m.status==='OFF')row.classList.add('row-stopped');
    else if(m.status==='WAITING'||m.status==='PULLED')row.classList.add('row-waiting');
    else if(m.active)row.classList.add('row-jumping');
  }

  var ln=document.getElementById('ln-'+sf);
  if(ln)ln.textContent=m.line||'--';

  // On-court dot — refresh color+tooltip when state flips
  var ocEl=document.getElementById('oc-'+sf);
  if(ocEl){
    var oc=ocDotAttrs(m.on_court);
    ocEl.style.color=oc.color;
    ocEl.title=oc.title;
    ocEl.textContent=oc.text;
  }
  // PLAYING banner — on court AND game in live play
  var pbEl=document.getElementById('pb-'+sf);
  if(pbEl){
    var live=(m.game_phase==='LIVE'||m.game_phase==='OVERTIME');
    if(m.on_court===true && live){
      pbEl.style.display='';
      pbEl.style.color='#fff';
      pbEl.style.background='#ff4444';
      pbEl.style.fontWeight='bold';
      pbEl.style.padding='1px 7px';
      pbEl.style.borderRadius='3px';
      pbEl.style.fontSize='9px';
      pbEl.style.marginRight='6px';
      pbEl.style.letterSpacing='0.6px';
      pbEl.style.boxShadow='0 0 6px rgba(255,68,68,0.5)';
      pbEl.textContent='▶ PLAYING';
    }else{
      pbEl.style.display='none';
      pbEl.textContent='';
    }
  }

  // Helper — render American odds into a span
  function renderAm(spanId, am){
    var el=document.getElementById(spanId);
    if(!el)return;
    if(am==null||am===0){el.textContent='--';el.style.color='var(--t3)';return;}
    el.textContent=(am>0?'+':'')+am;
    el.style.color=am<0?'var(--r)':'var(--g)';
  }
  // DK / FD individual columns — raw YES American odds per book
  renderAm('ao-dk-'+sf, m.dk_odds);
  renderAm('ao-fd-'+sf, m.fd_odds);

  // USED column — what the bot's ceiling actually consults, per book_mode
  // book_mode=min: the book whose YES odds carry HIGHER implied_yes
  //   = lower fair_no = more conservative for selling NO.
  //   On American, that's Math.min(fd, dk) (most-juicy YES).
  // book_mode=blend: weighted YES American by dk_weight.
  var ao=document.getElementById('ao-'+sf);
  if(ao){
    var used=null, usedSrc='';
    var fd=m.fd_odds, dk=m.dk_odds;
    var bm=currentSettings.book_mode||'min';
    if(fd&&dk){
      if(bm==='min'){
        used=Math.min(fd,dk);
        usedSrc=(fd<=dk?'FD':'DK');
      }else{
        // blend: convert each to implied_yes, weight, convert back
        function impY(a){return a>0?100/(a+100)*100:(-a)/(-a+100)*100;}
        var w=(currentSettings.dk_weight||50)/100.0;
        var iy=impY(dk)*w + impY(fd)*(1-w);
        if(iy>0&&iy<100){
          used = iy>=50 ? -Math.round(iy/(100-iy)*100) : Math.round((100-iy)/iy*100);
        }
        usedSrc='BLEND';
      }
    }else if(fd){used=fd;usedSrc='FD';}
    else if(dk){used=dk;usedSrc='DK';}
    if(used!=null){
      var amStr=(used>0?'+':'')+used;
      var srcLabel=usedSrc?'<span style="font-size:8px;color:var(--t3);letter-spacing:0.5px;margin-left:4px;">'+usedSrc+'</span>':'';
      ao.innerHTML=amStr+srcLabel;
      ao.style.color=used<0?'var(--r)':'var(--g)';
      ao.title='used='+usedSrc+' (book_mode='+bm+(bm==='blend'?', dk_weight='+(currentSettings.dk_weight||50):'')+')';
    }else{
      ao.textContent='--';ao.style.color='var(--t3)';ao.title='';
    }
    // Availability outline on USED cell
    var hasFd=!!fd, hasDk=!!dk;
    var boxColor='';
    if(hasFd&&hasDk)boxColor='';
    else if(hasDk)boxColor='#00e676';
    else if(hasFd)boxColor='#448aff';
    else boxColor='#ff4444';
    if(boxColor){
      ao.style.outline='1.5px solid '+boxColor;
      ao.style.outlineOffset='1px';
      ao.style.borderRadius='3px';
    }else{ao.style.outline='none';}
  }

  // Fair values — implied_yes from backend is already devigged, just use directly
  var fairNo=document.getElementById('fair-no-'+sf);
  var fairEv=document.getElementById('fair-ev-'+sf);
  var fairYes, fairNoVal;
  if(m.has_under && m.fair_yes_2way){
    fairYes=m.fair_yes_2way;
    fairNoVal=m.fair_no_2way;
  } else {
    var iy=m.implied_yes||0;
    if(iy>0){fairYes=iy;fairNoVal=100-iy;}
    else{fairYes=null;fairNoVal=null;}
  }
  // FAIR NO — American odds
  var fairAm=document.getElementById('fair-am-'+sf);
  if(fairAm&&fairNoVal&&fairNoVal>0&&fairNoVal<100){
    var am;
    if(fairNoVal>=50)am='-'+Math.round(fairNoVal/(100-fairNoVal)*100);
    else am='+'+Math.round((100-fairNoVal)/fairNoVal*100);
    fairAm.textContent=am;fairAm.style.color=fairNoVal>=50?'var(--r)':'var(--g)';
  }else if(fairAm){fairAm.textContent='--';fairAm.style.color='var(--t3)';}

  // CEIL — blue when in-play adjusted
  var cl=document.getElementById('cl-'+sf);
  if(cl){cl.textContent=(m.ceil_no||'--')+'c';cl.style.color=m.status_inplay?'#82b1ff':'#fff';}

  // CEIL AMERICAN — convert ceil cents to American odds
  var clam=document.getElementById('clam-'+sf);
  if(clam){
    var cn=m.ceil_no||0;
    if(cn>0&&cn<100){
      var ceilAm;
      if(cn>=50)ceilAm='-'+Math.round(cn/(100-cn)*100);
      else ceilAm='+'+Math.round((100-cn)/cn*100);
      clam.textContent=ceilAm;clam.style.color=cn>=50?'var(--r)':'var(--g)';
    }else{clam.textContent='--';clam.style.color='var(--t3)';}
  }

  // CEIL EV% = (fair_no - ceil) / ceil * 100
  if(fairNoVal&&fairEv&&m.ceil_no&&m.ceil_no>0){
    var evPct=(fairNoVal-m.ceil_no)/m.ceil_no*100;
    fairEv.textContent=evPct.toFixed(1)+'%';
    fairEv.style.color=evPct>=0?'var(--g)':'var(--r)';
  }else if(fairEv){fairEv.textContent='--';fairEv.style.color='var(--t3)';}

  // FAIR NO% — fair NO as percentage
  if(fairNoVal&&fairNo){fairNo.textContent=fairNoVal.toFixed(1)+'%';}else if(fairNo){fairNo.textContent='--';}

  // K YES bid
  var py=document.getElementById('py-'+sf);
  if(py){var yesBid=m.best_bid_yes;py.textContent=yesBid?yesBid+'c':'--';}

  // K NO bid
  var pr=document.getElementById('pr-'+sf);
  if(pr){var noBid=m.best_no_bid||m.best_bid_no;pr.textContent=noBid?noBid+'c':'--';}

  var no_=document.getElementById('no-'+sf);
  if(no_){var np=m.order_no_price;no_.textContent=np?np+'c':'--';no_.className='s-order '+(np?'active':'none');}

  var sb=document.getElementById('sb-'+sf);
  if(sb){
    var st=m.status||'OFF';var badge='';
    if(st==='PROP_HIT')badge='<span class="s-badge bonded">PROP HIT</span>';
    else if(st==='MENTIONED')badge='<span class="s-badge mention">MENTIONED</span>';
    else if(st==='FILLED')badge='<span class="s-badge fill">FILLED</span>';
    else if(st==='PLAYER_MAX')badge='<span class="s-badge fill" style="background:#ff6d0020;color:#ff6d00;border-color:#ff6d00;">PLAYER MAX</span>';
    else if(st==='NO_ODDS')badge='<span class="s-badge no-odds">NO ODDS</span>';
    else if(st==='WAITING'||st==='PULLED')badge='<span class="s-badge wait">'+st+'</span>';
    else if(st==='JUMPING')badge='<span class="s-badge y">JUMPING</span>';
    else if(st==='STOPPED')badge='<span class="s-badge n">STOPPED</span>';
    else if(st==='PAUSED')badge='<span class="s-badge wait">PAUSED</span>';
    else if(st==='SUSPENDED')badge='<span class="s-badge n" style="background:#ff000030;color:#ff4444;border-color:#ff4444;animation:pulse 1s infinite;">SUSPENDED</span>';
    else if(st==='DEADBALL_WAIT')badge='<span class="s-badge wait" style="background:#00e67620;color:#00e676;border-color:#00e676;">⏸ WAITING</span>';
    else if(st==='Q4_STOPPED')badge='<span class="s-badge n" style="background:#ff660030;color:#ff6600;border-color:#ff6600;">Q4 STOPPED</span>';
    else if(st==='GAME_OVER')badge='<span class="s-badge off">GAME OVER</span>';
    else badge='<span class="s-badge off">'+st+'</span>';
    sb.innerHTML=badge;
  }

  var sfl=document.getElementById('sf-'+sf);
  if(sfl){
    var pn=m.position_no||0;var avn=m.position_avg_no||0;
    if(pn>0){sfl.innerHTML='<span style="color:var(--r)">NO</span> <span style="color:var(--t1);font-size:13px">'+pn+'</span>'+(avn?' <span style="color:var(--y);font-size:10px">@'+avn+'c</span>':'');}
    else{sfl.innerHTML='<span style="color:var(--t4)">--</span>';}
  }

  // Adjusted size — shows in-play reduced size when clock running
  var adjEl=document.getElementById('adjsz-'+sf);
  if(adjEl){
    var baseSize=m.size||250;
    var inplaySizePct=currentSettings.inplay_size_pct||50;
    var isLivePhase=m.status_inplay;
    if(isLivePhase){
      var adj=Math.max(1,Math.round(baseSize*inplaySizePct/100));
      adjEl.textContent=adj;
      adjEl.style.color='#82b1ff';adjEl.style.fontWeight='bold';
    }else{
      adjEl.textContent=baseSize;
      adjEl.style.color='var(--t4)';adjEl.style.fontWeight='normal';
    }
  }

  var szEl=document.getElementById('sz-'+sf);
  if(szEl&&!szEl.matches(':focus'))szEl.value=m.size||250;

  var ftEl=document.getElementById('ft-'+sf);
  if(ftEl&&!ftEl.matches(':focus'))ftEl.value=m.fill_target||1500;

  var sl=document.getElementById('sl-'+sf);
  if(sl){sl.classList.remove('on','paused');if(m.active)sl.classList.add('on');else if(m.status==='PAUSED')sl.classList.add('paused');}

  var btns=document.getElementById('sbtns-'+sf);
  if(btns){
    if(m.active){btns.innerHTML='<button class="s-btn pll" onclick="pullStrike(&quot;'+esc(tk)+'&quot;)">PULL</button><button class="s-btn stp" onclick="stopBot(&quot;'+esc(tk)+'&quot;)">STOP</button>';}
    else if(!m.prop_done){btns.innerHTML='<button class="s-btn go" onclick="activateBot(&quot;'+esc(tk)+'&quot;)">GO</button>';}
    else{btns.innerHTML='';}
  }

  var sord=document.getElementById('sorders-'+sf);
  if(sord){
    if(m.order_no_price){sord.innerHTML='<span style="color:var(--r)">NO: '+m.order_no_price+'c x'+(m.order_no_size||m.size||'?')+'</span>';}
    else if(m.active){sord.innerHTML='no orders';}
    else{sord.innerHTML='';}
  }
}

var _sliderDebounce={};
function togSlider(el,tk){
  if(_sliderDebounce[tk])return;
  _sliderDebounce[tk]=true;setTimeout(function(){_sliderDebounce[tk]=false;},500);
  var sf=safe(tk);var sl=document.getElementById('sl-'+sf);
  if(sl.classList.contains('on')){stopBot(tk);}else{activateBot(tk);}
}

function activateBot(tk){
  fetch('/api/markets/'+encodeURIComponent(tk)+'/update',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({active:true})}).then(function(r){return r.json();}).then(function(d){
    if(d.error)flash(d.error,'err');else flash('ACTIVATED','ok');
  }).catch(function(){flash('ERROR','err');});
}

function stopBot(tk){
  fetch('/api/markets/'+encodeURIComponent(tk)+'/update',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({active:false})}).then(function(r){return r.json();}).then(function(){
    flash('STOPPED','ok');
  }).catch(function(){flash('ERROR','err');});
}

function pullStrike(tk){
  fetch('/api/markets/'+encodeURIComponent(tk)+'/pull',{method:'POST'}).then(function(r){return r.json();}).then(function(d){
    if(d.ok)flash('PULLED '+d.pulled+' orders','ok');else flash('PULL FAILED','err');
  }).catch(function(){flash('PULL FAILED','err');});
}

function doStartAll(pt){
  var label=pt?pt.toUpperCase():'ALL';
  if(!confirm('Start '+label+' NBA prop markets?'))return;
  var opts={method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(pt?{prop_type:pt}:{})};
  fetch('/api/start-all',opts).then(function(r){return r.json();}).then(function(d){
    flash('Started '+d.started+' '+label+' markets','ok');
  }).catch(function(){flash('ERROR','err');});
}

function doStopAll(pt){
  var label=pt?pt.toUpperCase():'ALL';
  var opts={method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(pt?{prop_type:pt}:{})};
  fetch('/api/stop-all',opts).then(function(r){return r.json();}).then(function(d){
    if(!pt){isPaused=false;var pbtn=document.getElementById('pauseBtn');pbtn.textContent='PAUSE';pbtn.style.background='';}
    flash('Stopped '+d.stopped+' '+label+' markets','ok');
  }).catch(function(){flash('STOP FAILED','err');});
}

function togglePauseAll(){
  var btn=document.getElementById('pauseBtn');
  if(!isPaused){
    btn.textContent='...';
    fetch('/api/pause-all',{method:'POST'}).then(function(r){return r.json();}).then(function(d){
      isPaused=true;btn.textContent='RESUME';btn.style.background='var(--y2)';btn.style.borderColor='var(--y)';btn.style.color='var(--y)';
      flash('PAUSED '+d.paused.length+' BOTS','ok');
    }).catch(function(){flash('PAUSE FAILED','err');btn.textContent='PAUSE';});
  }else{
    btn.textContent='...';
    fetch('/api/resume-all',{method:'POST'}).then(function(r){return r.json();}).then(function(d){
      isPaused=false;btn.textContent='PAUSE';btn.style.background='';btn.style.borderColor='var(--y)';btn.style.color='var(--y)';
      flash('RESUMED '+d.resumed+' BOTS','ok');
    }).catch(function(){flash('RESUME FAILED','err');btn.textContent='RESUME';});
  }
}

function gameStartAll(gid){
  fetch('/api/games/'+encodeURIComponent(gid)+'/start-all',{method:'POST'}).then(function(r){return r.json();}).then(function(d){
    flash('Started '+d.started+' in '+gid,'ok');
  }).catch(function(){flash('ERROR','err');});
}

function gameStopAll(gid){
  fetch('/api/games/'+encodeURIComponent(gid)+'/stop-all',{method:'POST'}).then(function(r){return r.json();}).then(function(d){
    flash('Stopped '+d.stopped+' in '+gid,'ok');
  }).catch(function(){flash('ERROR','err');});
}

function gamePauseToggle(gid){
  fetch('/api/games/'+encodeURIComponent(gid)+'/pause',{method:'POST'}).then(function(r){return r.json();}).then(function(d){
    flash(d.paused?'Paused':'Resumed'+' '+d.count+' in '+gid,'ok');
  }).catch(function(){flash('ERROR','err');});
}

function gameApplyField(gid,field){
  var sgid=safe(gid);
  var pt=activeSubTab[gid]||'points';
  var body={prop_type:pt};
  var label=field;
  if(field==='size'){var el=document.getElementById('gs_size_'+sgid);if(el)body.size=parseInt(el.value)||250;label='SIZE '+body.size;}
  else if(field==='fill_target'){var el=document.getElementById('gs_ft_'+sgid);if(el)body.fill_target=parseInt(el.value)||1500;label='FILL TARGET '+body.fill_target;}
  else if(field==='player_max'){
    var el=document.getElementById('gs_pmax_'+sgid);
    var v=el?parseInt(el.value)||5000:5000;
    fetch('/api/games/'+encodeURIComponent(gid)+'/settings',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({prop_type:pt,player_max:v})});
    flash('Player max set to '+v+' ('+pt.toUpperCase()+')','ok');
    return;
  }
  fetch('/api/games/'+encodeURIComponent(gid)+'/apply-all',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)}).then(function(r){return r.json();}).then(function(d){
    flash(label+' applied to '+d.applied+' '+pt.toUpperCase()+' markets','ok');
  }).catch(function(){flash('ERROR','err');});
}

function gameUpdateSettings(gid){
  var sgid=safe(gid);var body={};
  var pt=activeSubTab[gid]||'points';
  body.prop_type=pt;
  var arb=document.getElementById('gs_arb_'+sgid);if(arb)body.arb_pct=parseInt(arb.value)||0;
  var mx=document.getElementById('gs_maxexp_'+sgid);if(mx)body.max_exposure=parseInt(mx.value)||15000;
  var of=document.getElementById('gs_offset_'+sgid);if(of)body.offset=parseInt(of.value)||0;
  fetch('/api/games/'+encodeURIComponent(gid)+'/settings',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)}).then(function(r){return r.json();}).then(function(d){
    flash('Settings saved ('+pt.toUpperCase()+')','ok');
    // Persist to localStorage so settings survive redeploy
    var ss=JSON.parse(localStorage.getItem('nba_game_settings')||'{}');
    if(!ss[gid])ss[gid]={};
    ss[gid][pt]=body;
    localStorage.setItem('nba_game_settings',JSON.stringify(ss));
  }).catch(function(){flash('ERROR','err');});
}

function renderPositions(data){
  var pos=data.positions||[];var list=document.getElementById('posList');
  if(!pos.length){list.innerHTML='<div class="pos-empty">No positions</div>';return;}
  var html='<div class="pos-grid-hdr"><span>MARKET</span><span>SIDE</span><span>COUNT</span><span>AVG</span><span>COST</span></div>';
  for(var i=0;i<pos.length;i++){
    var p=pos[i];var tk=p.ticker||p.market_ticker||'';
    var yesQ=0,noQ=0,yesAvg=0,noAvg=0;
    if(p.market_positions){for(var j=0;j<p.market_positions.length;j++){var mp=p.market_positions[j];if(mp.side==='yes'){yesQ=mp.total_traded||0;yesAvg=mp.average_price||0;}else{noQ=mp.total_traded||0;noAvg=mp.average_price||0;}}}
    else{yesQ=p.position||p.position_fp||0;if(yesQ<0){noQ=Math.abs(yesQ);yesQ=0;}noQ=noQ||p.total_no||0;yesAvg=p.avg_yes||0;noAvg=p.avg_no||0;}
    if(noQ>0){var cost=noQ*(noAvg||0);html+='<div class="pos-grid-row"><span style="font-weight:bold">'+esc(tk.split("-").pop())+'</span><span style="color:var(--r);font-weight:bold">NO</span><span>'+noQ+'</span><span>'+(noAvg?Math.round(noAvg*100)+'c':'--')+'</span><span>$'+(cost>0?cost.toFixed(2):'--')+'</span></div>';}
    if(yesQ>0){var cost=yesQ*(yesAvg||0);html+='<div class="pos-grid-row"><span style="font-weight:bold">'+esc(tk.split("-").pop())+'</span><span style="color:var(--g);font-weight:bold">YES</span><span>'+yesQ+'</span><span>'+(yesAvg?Math.round(yesAvg*100)+'c':'--')+'</span><span>$'+(cost>0?cost.toFixed(2):'--')+'</span></div>';}
  }
  list.innerHTML=html;
}

function renderResting(data){
  var orders=data.orders||[];var list=document.getElementById('restList');
  if(!orders.length){list.innerHTML='<div class="pos-empty">No resting orders</div>';return;}
  var html='';
  for(var i=0;i<orders.length;i++){
    var o=orders[i];var tk=o.ticker||'';var side=o.side||'no';
    var price=o.yes_price||o.no_price||0;
    if(typeof price==='number'&&price<1)price=Math.round(price*100);
    var remaining=o.remaining_count||o.count||0;var oid=o.order_id||'';
    html+='<div class="order-row"><div class="order-info"><span class="order-name">'+esc(tk.split("-").pop())+'</span> '+
      '<span style="color:'+(side==='no'?'var(--r)':'var(--g)')+';font-weight:bold">'+side.toUpperCase()+'</span> '+
      '<span style="color:var(--y)">@'+price+'c</span> x'+remaining+'</div>'+
      '<button class="pull-btn" onclick="cancelOrder(&quot;'+esc(oid)+'&quot;)">PULL</button></div>';
  }
  list.innerHTML=html;
}

function cancelOrder(oid){
  fetch('/api/orders/'+encodeURIComponent(oid)+'/cancel',{method:'POST'}).then(function(r){return r.json();}).then(function(d){
    if(d.ok)flash('CANCELLED','ok');else flash('CANCEL FAILED','err');
  }).catch(function(){flash('ERROR','err');});
}

// ---- INIT ----
fetchBalance();
// Restore global settings from localStorage (survives redeploy)
(function(){
  var saved=localStorage.getItem('nba_global_settings');
  if(saved){try{var gs=JSON.parse(saved);
    fetch('/api/settings',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(gs)}).then(function(r){return r.json();}).then(function(d){
      if(d.ok){currentSettings=d.settings;loadSettings();flash('Global settings restored','ok');}
    }).catch(function(){});
  }catch(e){}}else{loadSettings();}
})();
updateModeUI();
pollTicker();tickerIv=setInterval(pollTicker,3000);
fetch('/api/status').then(function(r){return r.json();}).then(function(d){
  if(!d.authenticated)flash('Not authenticated - check KALSHI env vars','err');
  paperMode=d.paper_mode!==undefined?d.paper_mode:true;
  updateModeUI();
});
fetch('/api/games/data').then(function(r){return r.json();}).then(function(data){
  if(Object.keys(data).length>0){
    gamesData=data;marketsData={};
    for(var gid in data){for(var tk in data[gid].markets){marketsData[tk]=data[gid].markets[tk];}}
    _lastContentKey='';  // force full DOM rebuild
    renderTabs();renderContent();
    document.getElementById('posSec').style.display='';
    document.getElementById('restSec').style.display='';
    document.getElementById('fillSec').style.display='';
    startPolling();
  }else{
    var saved=JSON.parse(localStorage.getItem('nba_game_mappings')||'{}');
    var keys=Object.keys(saved);
    if(keys.length>0){
      flash('Restoring '+keys.length+' games from saved mappings...','ok');
      var loaded=0;
      keys.forEach(function(gid){
        var p=saved[gid];
        fetch('/api/games/add',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(p)})
          .then(function(r){return r.json();})
          .then(function(d){loaded++;if(loaded===keys.length){
            _lastContentKey='';  // force full DOM rebuild after restore
            document.getElementById('posSec').style.display='';document.getElementById('restSec').style.display='';document.getElementById('fillSec').style.display='';
            startPolling();
            // Restore saved per-game settings after all games loaded
            var ss=JSON.parse(localStorage.getItem('nba_game_settings')||'{}');
            for(var sg in ss){for(var spt in ss[sg]){
              var sb=ss[sg][spt];sb.prop_type=spt;
              fetch('/api/games/'+encodeURIComponent(sg)+'/settings',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(sb)}).catch(function(){});
            }}
            if(Object.keys(ss).length>0)flash('Restored saved settings','ok');
          }}).catch(function(){loaded++;});
      });
    }
  }
}).catch(function(err){
  console.error('Init fetch failed, trying localStorage restore:', err);
  var saved=JSON.parse(localStorage.getItem('nba_game_mappings')||'{}');
  var keys=Object.keys(saved);
  if(keys.length>0){
    flash('Reconnecting '+keys.length+' games...','ok');
    var loaded=0;
    keys.forEach(function(gid){
      var p=saved[gid];
      fetch('/api/games/add',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(p)})
        .then(function(r){return r.json();})
        .then(function(){loaded++;if(loaded===keys.length){_lastContentKey='';startPolling();}})
        .catch(function(){loaded++;});
    });
  }
});
</script>
</body>
</html>"""


# ---------------------------------------------------------------------------
# Boot
# ---------------------------------------------------------------------------

def boot():
    """Initialize on startup."""
    if kalshi.init_creds():
        log.info("NBA Props ready — Kalshi authenticated")
    else:
        log.warning("NBA Props running without Kalshi auth — orders will fail")

    # Load persisted fills
    loaded = _db_load_fills()
    if loaded:
        global fill_log
        fill_log = loaded
        log.info(f"Loaded {len(loaded)} fills from SQLite")

    # Start heartbeat loops — fast (0.2s, zero reads) + slow (5s, reconcile/safety)
    threading.Thread(target=fast_heartbeat_loop, daemon=True).start()
    threading.Thread(target=slow_heartbeat_loop, daemon=True).start()
    # Start position poller
    threading.Thread(target=_position_poller, daemon=True).start()
    # Start OB staleness safety-net poller (resyncs markets with no update in >90s)
    threading.Thread(target=_ob_staleness_poller, daemon=True).start()
    # Start phase transition watcher — batch-cancels on every LIVE↔dead phase change
    threading.Thread(target=_phase_transition_loop, daemon=True).start()
    # Auto-subscribe Bolt PBP+LS to every live NBA game on the slate.
    # Independent of Kalshi event loading so /v2/phase shows clock state
    # for ALL live games, not just markets we're trading.
    threading.Thread(target=_auto_subscribe_live_games_loop, daemon=True).start()
    # 1Hz heartbeat for clock_state_feed — fires pending stops when Bolt
    # goes silent during stoppages (no pbp_timeout + no further LS frames).
    # Also performs continuous Bolt-vs-CDN disagreement measurement so we
    # can quantify how far ahead Bolt is on each transition (independent
    # of whether markets are being processed).
    _last_cf_running = {}
    _last_cdn_running = {}
    _disagree_since = {}
    def _csf_tick_loop():
        while True:
            try:
                clock_state_feed.tick_all()
                # Measurement: compare Bolt-only vs CDN per active game
                with games_lock:
                    snap = list(games.values())
                for game in snap:
                    bk = game.get("_forced_bolt_key")
                    espn_gid = game.get("nba_game_id")
                    if not bk and espn_gid:
                        bk = nba_feed.get_bolt_key(str(espn_gid))
                    if not bk:
                        continue
                    csf = clock_state_feed.get_state(bk)
                    cdn = _csf_cdn_fallback(bk)
                    cf_run = csf.get("running")
                    cdn_run = (cdn or {}).get("running")
                    prev_cf = _last_cf_running.get(bk)
                    prev_cdn = _last_cdn_running.get(bk)
                    now = time.time()
                    # Detect transitions. When cf flips first while cdn lags
                    # (or vice versa), log time to converge.
                    if cf_run != prev_cf:
                        if cdn_run is not None and cf_run != cdn_run:
                            _disagree_since[bk] = ("cf_first", now, cf_run, cdn_run)
                            log.info(f"cf_lead [{bk[:40]}] cf flipped to {cf_run} "
                                     f"(via {csf.get('source')}); cdn still {cdn_run}")
                        _last_cf_running[bk] = cf_run
                    if cdn_run != prev_cdn:
                        if cf_run is not None and cdn_run != cf_run:
                            _disagree_since[bk] = ("cdn_first", now, cf_run, cdn_run)
                            log.info(f"cdn_lead [{bk[:40]}] cdn flipped to {cdn_run}; "
                                     f"cf still {cf_run}")
                        _last_cdn_running[bk] = cdn_run
                    # Resolution: when they reconverge, log how long
                    if (bk in _disagree_since and cf_run is not None
                            and cdn_run is not None and cf_run == cdn_run):
                        kind, since_ts, _, _ = _disagree_since.pop(bk)
                        elapsed = now - since_ts
                        leader = "bolt" if kind == "cf_first" else "cdn"
                        log.info(f"converge [{bk[:40]}] {leader} led by {elapsed:.1f}s")
            except Exception as e:
                log.debug(f"clock_state_feed tick err: {e}")
            time.sleep(1.0)
    threading.Thread(target=_csf_tick_loop, daemon=True).start()
    # CDN fallback for clock_state_feed: when Bolt is dead/silent or has no
    # opinion, the feed transparently falls back to NBA CDN. Source
    # transitions per game are logged ("clock_feed source [...]: bolt → cdn").
    _CDN_TO_CSF_PHASE = {
        "LIVE": clock_state_feed.LIVE,
        "OVERTIME": clock_state_feed.LIVE,
        "TIMEOUT": clock_state_feed.TIMEOUT,
        "QUARTER_BREAK": clock_state_feed.QUARTER_END,
        "HALFTIME": clock_state_feed.HALFTIME,
        "FOUL_SHOT": clock_state_feed.FREE_THROW,
        "FINAL": clock_state_feed.FINAL,
    }
    def _csf_cdn_fallback(bolt_key):
        if not bolt_key:
            return None
        # Find the game whose bolt_key matches. Match either via
        # nba_feed.get_bolt_key (organic games auto-subscribed) or via
        # the games dict's _forced_bolt_key field (forced stubs from
        # /api/v2/force_subscribe).
        nba_gid = None
        try:
            with games_lock:
                snap = list(games.values())
        except Exception:
            return None
        for game in snap:
            if game.get("_forced_bolt_key") == bolt_key:
                nba_gid = game.get("nba_game_id") or game.get("_espn_gid")
                if nba_gid:
                    break
            espn_gid = game.get("nba_game_id")
            if espn_gid and nba_feed.get_bolt_key(str(espn_gid)) == bolt_key:
                nba_gid = espn_gid
                break
        if not nba_gid:
            return None
        cdn_phase = nba_feed.get_game_phase(str(nba_gid))
        if not cdn_phase or cdn_phase in ("UNKNOWN", "PRE"):
            return None
        return {
            "running": cdn_phase in ("LIVE", "OVERTIME"),
            "phase": _CDN_TO_CSF_PHASE.get(cdn_phase, clock_state_feed.UNKNOWN),
        }
    clock_state_feed.set_cdn_fallback(_csf_cdn_fallback)
    log.info("clock_state_feed CDN fallback registered")
    # Start NBA feed poller
    nba_feed.start_game_poller()
    nba_feed.start_cdn_status_poller()
    nba_feed.start_pbp_poller()
    # Start odds poller (OddsBlaze dual-book FD + DK)
    boltodds_feed.poll_interval = settings.get("ob_poll_interval", 0.5)
    # Lane 1 disabled — heartbeat handles odds changes naturally (like Homer)
    # boltodds_feed.on_odds_change = _on_odds_change
    boltodds_feed.on_suspension = _on_suspension
    boltodds_feed.start()
    log.info("NBA Props heartbeat + position poller + OB dual-book feeds started (heartbeat only, no Lane 1, suspension pull ON)")
    # Start OddsPapi clocks WS (if API key configured)
    op_clocks.start()


boot()

if __name__ == "__main__":
    # use_reloader=False — avoid the parent-watcher process double-running
    # WS-background threads, which would otherwise cause our own duplicate
    # Bolt connections to fight for the single Growth-tier slot per feed.
    debug_on = os.environ.get("FLASK_DEBUG", "0") == "1"
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5052)),
            debug=debug_on, use_reloader=False)
