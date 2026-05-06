"""Bolt Livescores WebSocket feed — primary NBA clock-state detection.

Bolt LS is 3-6s ahead of NBA CDN on clock-state transitions (validated
2026-04-30 in boltodds_nba_monitor logs). Used as the PRIMARY signal
for clock_running detection, closing the timeout-end past-post window.

Bolt's `clockRunning` field on NBA is broken (always False), so we
derive clock_running from `remainingTimeSecondsInPeriod` decrement vs
wall clock — see _derive_clock_running.

Usage from app.py:
    import boltodds_ls_feed
    bolt_key = boltodds_ls_feed.resolve_bolt_key("Atlanta Hawks", "New York Knicks")
    if bolt_key:
        boltodds_ls_feed.subscribe(bolt_key)
        nba_feed.register_bolt_key(nba_game_id, bolt_key)
"""

import asyncio
import json
import logging
import os
import threading
import time
from datetime import datetime

import requests
import websockets

log = logging.getLogger("bolt_ls")

_DEFAULT_API_KEY = "0c97fafd-3455-484a-9ba5-16f0a100a457"
if os.environ.get("BOLT_API_KEY"):
    API_KEY = os.environ["BOLT_API_KEY"]
    _API_KEY_SOURCE = "BOLT_API_KEY env"
elif os.environ.get("BOLT_KEY"):
    API_KEY = os.environ["BOLT_KEY"]
    _API_KEY_SOURCE = "BOLT_KEY env"
else:
    API_KEY = _DEFAULT_API_KEY
    _API_KEY_SOURCE = "BUNDLED DEFAULT"

# Source-log fires lazily on first subscribe() — at import time, app.py's
# logging.basicConfig() hasn't been called yet, so log.info calls would
# be swallowed (Python default = WARNING-only).
_source_logged = False

def _log_source_once():
    global _source_logged
    if _source_logged:
        return
    _source_logged = True
    if _API_KEY_SOURCE == "BUNDLED DEFAULT":
        log.warning(
            "!!! Bolt LS using BUNDLED DEFAULT API key — neither "
            "BOLT_API_KEY nor BOLT_KEY env var is set. Analysis will be "
            "on the shared default key, which may rate-limit or conflict "
            "with other clients. Set BOLT_API_KEY=... in your env."
        )
    else:
        log.info(f"Bolt LS API key source: {_API_KEY_SOURCE} ({API_KEY[:8]}…)")
LS_WS_URL = f"wss://spro.agency/api/livescores?key={API_KEY}"
HTTP_BASE = "https://spro.agency/api"

# NO time-based freshness check. Bolt LS is event-driven (only emits
# when state changes), so a "fresh threshold" inappropriately invalidates
# perfectly good last-known state during quiet stretches. Instead we track
# WS connection liveness explicitly: state is valid iff the WS is connected.

# Per-bolt-key state cache.
# Bolt key example: "Atlanta Hawks vs New York Knicks, 2026-04-30, 07"
_states: dict = {}
_states_lock = threading.Lock()

# Active subscription set. Synced to WS on next loop tick.
_active_keys: set = set()
_active_lock = threading.Lock()
_subs_dirty = threading.Event()

_ws_loop = None
_ws_thread_started = False
_start_lock = threading.Lock()
_ws_connected = False  # True iff WS handshake done and not yet errored
_ws_connected_lock = threading.Lock()


def _set_connected(v: bool):
    global _ws_connected
    with _ws_connected_lock:
        _ws_connected = v


def is_connected() -> bool:
    with _ws_connected_lock:
        return _ws_connected


def _derive_clock_running(prev: dict, cur: dict, wall_delta_s: float):
    """Bolt NBA `clockRunning` is unreliable. Derive from
    remainingTimeSecondsInPeriod decrement vs wall clock.
    Returns True (running), False (stopped), or None (uncertain).

    Hard overrides (don't need a prev sample):
      remaining == 0     → False (clock can't run with 0 time)
      matchPeriod has 'BREAK', 'END', 'HALF' (and not 'IN_') → False
    """
    cr = cur.get("remainingTimeSecondsInPeriod")
    cp_raw = cur.get("matchPeriod")
    cp_str = ""
    if isinstance(cp_raw, list) and len(cp_raw) >= 2:
        cp_str = str(cp_raw[1]).upper()
    elif cp_raw:
        cp_str = str(cp_raw).upper()

    # Hard stops — no need for prev sample.
    if cr is not None and cr == 0:
        return False
    if cp_str and not cp_str.startswith("IN_") and any(
        kw in cp_str for kw in ("HALFTIME", "HALF_TIME", "BREAK", "END_OF", "END_")
    ):
        return False

    if prev is None or wall_delta_s < 0.5:
        return None
    pr = prev.get("remainingTimeSecondsInPeriod")
    pp = prev.get("matchPeriod")
    if pr is None or cr is None:
        return None
    if pp != cp_raw:
        # Period transition — can't derive from delta, but cur is fresh so
        # we don't want to stick on stale True. Return None and let caller
        # decide; the hard-overrides above already caught break states.
        return None
    game_delta = pr - cr  # positive = clock decreased
    return game_delta > 0 and (game_delta / wall_delta_s) > 0.5


def _extract_period(state: dict):
    mp = state.get("matchPeriod")
    if isinstance(mp, list) and len(mp) >= 2:
        s = mp[1]
        for n, key in [(1, "FIRST"), (2, "SECOND"), (3, "THIRD"), (4, "FOURTH")]:
            if key in s:
                return n
        if "OVER" in s:
            return 5
    return state.get("period")


async def _ls_loop():
    last_subscribed = set()
    while True:
        try:
            async with websockets.connect(LS_WS_URL, max_size=None,
                                          ping_interval=20, ping_timeout=10) as ws:
                ack = await ws.recv()
                log.info(f"Bolt LS connected: {str(ack)[:120]}")
                _set_connected(True)
                last_subscribed = set()
                while True:
                    with _active_lock:
                        cur = set(_active_keys)
                    if cur != last_subscribed:
                        await ws.send(json.dumps({
                            "action": "subscribe",
                            "filters": {"games": list(cur)},
                        }))
                        last_subscribed = cur
                        log.info(f"Bolt LS subscribed to {len(cur)} games: {sorted(cur)}")
                    try:
                        msg = await asyncio.wait_for(ws.recv(), timeout=1.0)
                    except asyncio.TimeoutError:
                        continue
                    try:
                        data = json.loads(msg)
                    except Exception:
                        continue
                    items = data if isinstance(data, list) else [data]
                    for it in items:
                        if it.get("action") != "match_update":
                            continue
                        st = it.get("state") or {}
                        bolt_key = it.get("game") or ""
                        if not bolt_key:
                            continue
                        with _states_lock:
                            existing = _states.get(bolt_key, {})
                            now = time.time()
                            prev_st = existing.get("_raw_state")
                            prev_ts = existing.get("ts", 0)
                            wall_delta = (now - prev_ts) if prev_ts else 0
                            running = _derive_clock_running(prev_st, st, wall_delta)
                            # Sticky: keep previous derived value when current
                            # tick can't decide — but ONLY if the period hasn't
                            # changed. Across a period boundary the previous
                            # running=True is stale (e.g. Q2→halftime), so we
                            # let the value go to None and the hard-overrides
                            # in _derive_clock_running carry it.
                            prev_period = (prev_st or {}).get("matchPeriod")
                            cur_period = st.get("matchPeriod")
                            same_period = prev_period == cur_period
                            # Treat REST-seeded prev as same-period so we don't
                            # lose the authoritative match_info.running value
                            # on the first WS event after a seed.
                            seeded_from_rest = isinstance(prev_st, dict) and \
                                prev_st.get("_seeded_from") in ("rest", "boxscore")
                            if running is None and (same_period or seeded_from_rest):
                                running = existing.get("clock_running")
                            _states[bolt_key] = {
                                "ts": now,
                                "clock_running": running,
                                "period": _extract_period(st),
                                "remaining_secs": st.get("remainingTimeSecondsInPeriod"),
                                "elapsed_secs": st.get("elapsedTimeSeconds"),
                                "points_a": st.get("pointsA"),
                                "points_b": st.get("pointsB"),
                                "match_period": st.get("matchPeriod"),
                                "_raw_state": st,
                            }
                            # Log clock transition for past-post protection observability
                            prev_running = existing.get("clock_running")
                            if running is not None and prev_running is not None and running != prev_running:
                                log.info(f"Bolt LS clock {'STARTED' if running else 'STOPPED'} "
                                         f"[{bolt_key}] period={_extract_period(st)} "
                                         f"remaining={st.get('remainingTimeSecondsInPeriod')}")
                        # Feed the LS-first phase resolver outside the states lock
                        try:
                            import ls_phase_manager
                            ls_phase_manager.ingest_ls_frame(bolt_key, st)
                        except Exception as _e:
                            log.debug(f"ls_phase_manager ingest err: {_e}")
                        # Feed the unified clock_state_feed (push-driven)
                        try:
                            import clock_state_feed
                            clock_state_feed.on_ls_frame(bolt_key, st)
                        except Exception as _e:
                            log.debug(f"clock_state_feed ls err: {_e}")
        except Exception as e:
            _set_connected(False)
            log.warning(f"Bolt LS WS error: {e}; reconnect in 5s")
            await asyncio.sleep(5)


def _ensure_ws_thread():
    global _ws_loop, _ws_thread_started
    with _start_lock:
        if _ws_thread_started:
            return
        _ws_thread_started = True

    def _run():
        global _ws_loop
        _ws_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(_ws_loop)
        _ws_loop.create_task(_ls_loop())
        _ws_loop.run_forever()

    threading.Thread(target=_run, daemon=True, name="bolt-ls").start()
    log.info("Bolt LS thread started")


def subscribe(bolt_key: str):
    """Add a game to the LS subscription set. WS thread auto-starts on first call.
    Also fetches a one-shot boxscore snapshot so we have current state immediately —
    Bolt LS is event-driven (no snapshot on subscribe), so without this we'd be
    blind until the next clock transition. This is the 'lock in best state' path."""
    if not bolt_key:
        return
    _log_source_once()
    with _active_lock:
        already = bolt_key in _active_keys
        _active_keys.add(bolt_key)
    _ensure_ws_thread()
    # First-time subscribe: pull a snapshot so the resolver has period+clock
    # immediately, even if subscribed mid-timeout.
    if not already:
        try:
            threading.Thread(target=_seed_from_boxscore, args=(bolt_key,),
                             daemon=True, name=f"bolt-ls-seed").start()
        except Exception as e:
            log.warning(f"Bolt LS snapshot seed failed for {bolt_key}: {e}")


_BOXSCORES_URL = f"https://spro.agency/api/boxscores?key={API_KEY}"
_MATCHINFO_URL = f"https://spro.agency/api/match_info?key={API_KEY}"


def _parse_clock_field(clock: str):
    """Parse Bolt boxscore `clock` string → (clock_running_hint, remaining_secs).
    Examples:
      'Halftime'           → (False, 0)
      'End of 3rd Quarter' → (False, 0)
      'Final'              → (False, 0)
      '5:32'               → (None, 332)   # can't tell running from string alone
      ''  / None           → (None, None)
    """
    if not clock or not isinstance(clock, str):
        return None, None
    c = clock.strip()
    cl = c.lower()
    if "halftime" in cl or "final" in cl:
        return False, 0
    if "end of" in cl or "end " in cl:
        return False, 0
    # MM:SS — extract the last "MM:SS"-shaped token (e.g. "Q4 - 11:47" → "11:47")
    import re
    m = re.search(r"(\d{1,2}):(\d{2})", c)
    if m:
        try:
            return None, int(m.group(1)) * 60 + int(m.group(2))
        except Exception:
            return None, None
    return None, None


def _fetch_match_info(bolt_key: str) -> dict | None:
    """Pull match_info REST. Returns parsed dict or None on error/missing.
    For NBA, this is the authoritative source for `clock_running` (timeinfo.running),
    plus current period status name, score, and remaining seconds."""
    try:
        r = requests.post(_MATCHINFO_URL, json={"games": [bolt_key]}, timeout=5)
        if r.status_code != 200:
            return None
        data = r.json() or {}
        wrap = data.get(bolt_key) or {}
        docs = wrap.get("doc") or []
        if not docs:
            return None
        m = (docs[0].get("data") or {}).get("match") or {}
        if not m:
            return None
        ti = m.get("timeinfo") or {}
        st = m.get("status") or {}
        result = m.get("result") or {}
        return {
            "running": ti.get("running"),
            "remaining": int(ti.get("remaining") or 0) if ti.get("remaining") is not None else None,
            "played": int(ti.get("played") or 0) if ti.get("played") is not None else None,
            "ended": ti.get("ended"),
            "status_name": st.get("name") or "",
            "status_id": st.get("_id"),
            "period": m.get("p"),
            "result_home": result.get("home"),
            "result_away": result.get("away"),
            "winner": result.get("winner"),
            "periods": m.get("periods") or {},
        }
    except Exception as e:
        log.warning(f"Bolt match_info error for {bolt_key}: {e}")
        return None


def _apply_boxscore_state(bolt_key: str, force: bool = False) -> dict | None:
    """Fetch boxscore + match_info REST, merge, write into _states.

    match_info supplies `clock_running` (authoritative for NBA, since LS WS
    only emits on transitions) and the score; boxscore supplies the
    human-readable clock string used to flag boundaries (Halftime/Final/End).

    If `force` is False, skip when WS-derived state is already present.
    If `force` is True, overwrite — used by the UNKNOWN-fallback path.
    """
    # Fire both REST calls (could parallelize with threads but timeouts are 5s each)
    mi = _fetch_match_info(bolt_key)
    bs_data = None
    try:
        rb = requests.post(_BOXSCORES_URL, json={"games": [bolt_key]}, timeout=5)
        if rb.status_code == 200:
            bs_data = (rb.json() or {}).get(bolt_key)
    except Exception as e:
        log.warning(f"Bolt LS boxscore error for {bolt_key}: {e}")

    if not mi and not bs_data:
        return None

    # Boxscore-derived signals
    clock_str = ""
    spp = {}
    bs_period = 0
    if bs_data:
        bg = bs_data.get("game") or {}
        clock_str = bg.get("clock") or bg.get("newScoreboardClock") or ""
        spp = bg.get("scorePerPeriod") or {}
        try:
            bs_period = max(int(k) for k in spp.keys()) if spp else 0
        except Exception:
            bs_period = 0

    # match_info wins for clock_running and remaining when present
    mi_running = mi.get("running") if mi else None
    mi_remaining = mi.get("remaining") if mi else None
    mi_period = mi.get("period") if mi else None
    mi_status = (mi.get("status_name") or "") if mi else ""

    # Period: prefer match_info > boxscore-derived
    period = mi_period if isinstance(mi_period, int) and mi_period > 0 else bs_period

    # remaining_secs: match_info > parsed boxscore string
    if mi_remaining is not None:
        remaining = mi_remaining
    else:
        _, remaining = _parse_clock_field(clock_str)

    # clock_running: match_info > parsed boxscore (boxscore returns False/0 for boundaries only)
    if mi_running is not None:
        clk_running = bool(mi_running)
    else:
        clk_running, _ = _parse_clock_field(clock_str)  # may still be None

    # match_period synthesis: prefer status name from match_info, else clock string
    cl = (mi_status or clock_str).lower()
    if "halftime" in cl or "half time" in cl:
        mp = ["NBAMatchPeriod", "HALFTIME"]
    elif "final" in cl or (mi and mi.get("ended")):
        mp = ["NBAMatchPeriod", "FINAL"]
    elif "end" in cl:
        mp = ["NBAMatchPeriod", f"END_OF_QUARTER_{period}"]
    elif period:
        mp = ["NBAMatchPeriod", f"IN_QUARTER_{period}"]
    else:
        mp = []

    # Score: match_info.result > boxscore.home/away
    points_a = (mi.get("result_home") if mi else None)
    points_b = (mi.get("result_away") if mi else None)
    if points_a is None and bs_data:
        bg = bs_data.get("game") or {}
        points_a = bg.get("home")
        points_b = bg.get("away")

    new_state = {
        "ts": time.time(),
        "clock_running": clk_running,
        "period": period,
        "remaining_secs": remaining,
        "elapsed_secs": (mi.get("played") if mi else None),
        "points_a": points_a,
        "points_b": points_b,
        "match_period": mp,
        "_raw_state": {
            "_seeded_from": "rest",
            "clock": clock_str,
            "status_name": mi_status,
            "scorePerPeriod": spp,
            "match_info_running": mi_running,
        },
    }
    with _states_lock:
        existing = _states.get(bolt_key, {})
        if not force and existing.get("_raw_state") and \
           existing["_raw_state"].get("_seeded_from") not in ("boxscore", "rest"):
            return existing
        _states[bolt_key] = new_state

    # Also push the seed into clock_state_feed in LS-WS shape so games
    # subscribed-during-halftime/timeout immediately have a phase, instead
    # of UNKNOWN until the next WS frame (which can be 60+ seconds away).
    try:
        import clock_state_feed
        if remaining is not None:
            ws_shape = {
                "remainingTimeSecondsInPeriod": remaining,
                "matchPeriod": mp,
            }
            clock_state_feed.on_ls_frame(bolt_key, ws_shape)
    except Exception as _e:
        log.debug(f"clock_state_feed seed push err: {_e}")
    return new_state


def _seed_from_boxscore(bolt_key: str):
    """One-shot snapshot on subscribe (boxscore + match_info merged)."""
    s = _apply_boxscore_state(bolt_key, force=False)
    if s and s.get("_raw_state", {}).get("_seeded_from") in ("rest", "boxscore"):
        rs = s.get("_raw_state", {})
        log.info(f"Bolt LS seeded from REST [{bolt_key}] "
                 f"period={s['period']} clock='{rs.get('clock')}' "
                 f"status='{rs.get('status_name')}' running={s['clock_running']} "
                 f"remaining={s['remaining_secs']} "
                 f"score={s.get('points_a')}-{s.get('points_b')}")


# Cache to throttle UNKNOWN-fallback boxscore refetches
_unknown_refresh_ts: dict = {}
_unknown_refresh_lock = threading.Lock()
UNKNOWN_REFRESH_THROTTLE = 8  # seconds — at most 1 boxscore call per game per 8s


def refresh_for_unknown(bolt_key: str) -> bool:
    """Hot-path fallback: when phase resolver hits UNKNOWN, call this to
    re-fetch a fresh boxscore snapshot. Throttled per-game to avoid hammering
    the REST API. Returns True if a refresh was actually performed."""
    if not bolt_key:
        return False
    now = time.time()
    with _unknown_refresh_lock:
        last = _unknown_refresh_ts.get(bolt_key, 0)
        if (now - last) < UNKNOWN_REFRESH_THROTTLE:
            return False
        _unknown_refresh_ts[bolt_key] = now
    s = _apply_boxscore_state(bolt_key, force=True)
    if s:
        log.info(f"Bolt LS UNKNOWN-refresh boxscore [{bolt_key}] period={s['period']} "
                 f"clock='{s['_raw_state'].get('clock')}' "
                 f"running={s['clock_running']} remaining={s['remaining_secs']}")
        return True
    return False


def unsubscribe(bolt_key: str):
    if not bolt_key:
        return
    with _active_lock:
        _active_keys.discard(bolt_key)


def get_clock_state(bolt_key: str):
    """Returns dict with clock_running/period/etc.

    Returns None ONLY when:
    - bolt_key is missing
    - we've never received any state for this game

    Does NOT return None just because the cached state is old — Bolt LS
    is event-driven, so 'old' state is still authoritative as long as the
    WS is connected. Use is_connected() to detect a real outage.

    Adds 'age_s' (seconds since last update) and 'connected' for callers
    that want to render staleness UX themselves.
    """
    if not bolt_key:
        return None
    with _states_lock:
        s = _states.get(bolt_key)
    if not s:
        return None
    out = {k: v for k, v in s.items() if not k.startswith("_")}
    out["age_s"] = time.time() - s["ts"]
    out["connected"] = is_connected()
    return out


def is_clock_running(bolt_key: str):
    """True/False if known and fresh, None otherwise."""
    s = get_clock_state(bolt_key)
    if s is None:
        return None
    return s.get("clock_running")


def resolve_bolt_key(home_full: str, away_full: str, date_str: str = None):
    """Find today's Bolt game key matching the team names. Returns None if unmatched.

    Date matching is timezone-tolerant: Bolt stamps games with the local game
    day (typically US/Eastern), but our server may run UTC where the day has
    already rolled over. Try today + yesterday + tomorrow as candidate dates.
    """
    from datetime import timedelta
    if not (home_full and away_full):
        return None

    candidate_dates = []
    if date_str:
        candidate_dates.append(date_str)
    now = datetime.now()
    for delta in (0, -1, 1):
        candidate_dates.append((now + timedelta(days=delta)).strftime("%Y-%m-%d"))
    # Dedupe preserving order
    seen = set()
    candidate_dates = [d for d in candidate_dates if not (d in seen or seen.add(d))]

    try:
        r = requests.get(f"{HTTP_BASE}/get_games", params={"key": API_KEY}, timeout=10)
        if r.status_code != 200:
            log.warning(f"Bolt /get_games returned {r.status_code}")
            return None
        all_games = r.json()
        # Pass 1: prefer date+name match across candidate dates
        for ds in candidate_dates:
            for k, v in all_games.items():
                if v.get("sport") != "NBA":
                    continue
                if ds not in k:
                    continue
                if home_full in k and away_full in k:
                    return k
        # Pass 2: name-only match across all NBA games (last resort)
        # NBA teams play at most once per day, so name match is safe.
        for k, v in all_games.items():
            if v.get("sport") != "NBA":
                continue
            if home_full in k and away_full in k:
                log.info(f"Bolt LS resolved by name-only (no date match): {k}")
                return k
    except Exception as e:
        log.warning(f"resolve_bolt_key failed: {e}")
    return None
