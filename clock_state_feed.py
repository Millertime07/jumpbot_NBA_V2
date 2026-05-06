"""clock_state_feed — fastest possible clock_running signal.

Push-driven multiplexer over Bolt PBP + Bolt LS WS streams. Whichever feed
fires first wins. Maintains <1ms internal latency between WS frame arrival
and observable state flip.

Signal precedence (per-event-arrival, not per-game):

  STOPPED:
    1. PBP stopping action type    (foul/timeout/turnover/freethrow/...)
       → fires INSTANTLY on whistle (~0ms after WS push)
    2. LS frozen-frame derivation  (game_dt=0 over ≥4s wall)
       → 4-8s after whistle (slower fallback)

  STARTED:
    1. LS game_dt > 0              (clock advanced ≥1s in ≥1s wall)
       → fires on first LS frame after restart, 1-3s typical
    2. PBP `live` action type      (goal/rebound/possession/etc)
       → fires on first play after restart, 1-3s typical
       Race winner depends on which WS pushes first.

Public API:
    register_callback(fn)                  — fn(bolt_key, running, source, ts)
    on_pbp_event(bolt_key, ev_dict)        — call from PBP feed loop
    on_ls_frame(bolt_key, raw_state)       — call from LS feed loop
    is_running(bolt_key) -> bool|None      — instant query
    get_state(bolt_key) -> dict            — full state + telemetry
    last_change_ts(bolt_key) -> float|None — when state last flipped

State flip is debounced: identical successive signals are dropped at the
state level, but ALL raw events are recorded for telemetry.
"""
from __future__ import annotations

import logging
import threading
import time
from collections import deque

log = logging.getLogger("clock_state_feed")

# ─── Tunables ────────────────────────────────────────────────────────────
LS_STOP_FROZEN_WINDOW_S = 4.0     # frozen frames over ≥this → stopped
LS_START_MIN_WALL_S = 1.0         # minimum wall delta to confirm start
LS_START_GAME_RATIO = 0.5         # game_dt / wall_dt > this → running
PBP_POSSESSION_RESTART_DELAY_S = 1.5  # `possession` events must arrive
                                  # ≥this long after last stop to flip
                                  # (filters out possession-arrow events
                                  # during FT setup)
LATE_GAME_PERIOD = 4              # period at/after which made FG can stop clock
LATE_GAME_REMAINING_S = 120       # last 2 min of Q4/OT — made FG stops clock
STALE_EVENT_AGE_S = 60.0          # past this, we mark state as stale
DEAD_EVENT_AGE_S = 180.0          # past this, state is dead/unknown

# Bolt PBP action types that DETERMINISTICALLY stop the clock
_STOPPING = {
    "foul", "turnover_basket", "timeout", "free_throws_awarded",
    "videoreview", "periodscore", "periodend",
    "player_disqualified", "won_jump_ball",
}
# PBP action types that imply the clock is RUNNING (live action observed).
# These CONFIRM running but don't FLIP from stopped → running, because
# they can fire mid-FT-sequence (the FT shot is `attempt_missed` / `goal`,
# the rebound after a missed FT still fires `rebound`, etc.). Allowing
# them to flip causes flicker during free-throw stoppages.
# `goal` excluded — last 2 min of Q4/OT a made FG stops the clock.
_RUNNING = {
    "attempt_missed", "rebound", "possession", "steal_basket", "block",
}
# ONLY these PBP types are allowed to flip from STOPPED → RUNNING.
# `possession` is the most reliable "ball back in play" PBP signal; FT
# sequences don't typically emit explicit possession events. Subject to
# PBP_POSSESSION_RESTART_DELAY_S so the immediate post-foul possession-
# arrow setting doesn't trigger.
_PBP_RESTART_TYPES = {"possession", "steal_basket"}

# Phase enum — what's actually happening during a stoppage
LIVE = "LIVE"
TIMEOUT = "TIMEOUT"
FREE_THROW = "FREE_THROW"
DEAD_BALL_FOUL = "DEAD_BALL_FOUL"
DEAD_BALL_TURNOVER = "DEAD_BALL_TURNOVER"
DEAD_BALL_REVIEW = "DEAD_BALL_REVIEW"
DEAD_BALL_JUMPBALL = "DEAD_BALL_JUMPBALL"
QUARTER_END = "QUARTER_END"
HALFTIME = "HALFTIME"
FINAL = "FINAL"
DEAD_BALL = "DEAD_BALL"            # generic stoppage with no PBP context
UNKNOWN = "UNKNOWN"

# Maps the source string (e.g. "pbp_timeout") to a phase classification.
# A specific event (free_throws_awarded) UPGRADES a generic earlier event
# (foul) when it arrives later — see _maybe_upgrade_phase_locked.
# Note: pbp_periodend / pbp_periodscore are remapped at runtime by period
# (HALFTIME at period 2, QUARTER_END at periods 1/3, FINAL or QUARTER_END
# at period ≥ 4). The mapping below is the default fallback.
_PHASE_BY_SOURCE = {
    "pbp_timeout": TIMEOUT,
    "pbp_free_throws_awarded": FREE_THROW,
    "pbp_foul": DEAD_BALL_FOUL,
    "pbp_turnover_basket": DEAD_BALL_TURNOVER,
    "pbp_videoreview": DEAD_BALL_REVIEW,
    "pbp_won_jump_ball": DEAD_BALL_JUMPBALL,
    "pbp_player_disqualified": DEAD_BALL_FOUL,
    "pbp_periodend": QUARTER_END,
    "pbp_periodscore": QUARTER_END,
    "pbp_late_goal": DEAD_BALL,        # last 2-min Q4/OT made FG
    "ls_remaining=0": QUARTER_END,
    "ls_frozen": DEAD_BALL,
}


def _classify_period_boundary(period: int | None) -> str:
    """Period boundary phase based on which period just ended."""
    if period == 2:
        return HALFTIME
    if period and period >= 4:
        # End of Q4 or OT — could be FINAL or going to OT.
        # Default to QUARTER_END; LS HALFTIME/FINAL matchPeriod can upgrade.
        return QUARTER_END
    return QUARTER_END

# Specificity ranking — when multiple stopping events fire in a window,
# the most specific phase wins. FREE_THROW > FOUL because once we see
# free_throws_awarded, we know it's a shooting foul going to the line.
_PHASE_SPECIFICITY = {
    FREE_THROW: 100,
    TIMEOUT: 90,
    DEAD_BALL_REVIEW: 85,
    QUARTER_END: 80,
    HALFTIME: 80,
    FINAL: 80,
    DEAD_BALL_JUMPBALL: 70,
    DEAD_BALL_FOUL: 60,
    DEAD_BALL_TURNOVER: 60,
    DEAD_BALL: 10,
    UNKNOWN: 0,
}

# Window during which a more-specific event upgrades the prior phase
# (e.g. foul fires, then 1.5s later free_throws_awarded → upgrade)
PHASE_UPGRADE_WINDOW_S = 8.0


# ─── Per-game state ──────────────────────────────────────────────────────
class _GameState:
    __slots__ = ("running", "running_ts", "running_source",
                 "ls_prev_remaining", "ls_prev_ts", "ls_frozen_since",
                 "last_pbp_stop_ts", "last_pbp_run_ts",
                 "pbp_prev_seconds", "pbp_prev_seconds_ts",
                 "last_event_ts",
                 "last_pbp_period", "last_pbp_seconds",  # ordering guard
                 "history",  # deque of (ts, running, source) for telemetry
                 "period", "remaining_sec", "match_period",
                 "phase", "phase_source", "phase_ts")

    def __init__(self):
        self.running: bool | None = None
        self.running_ts: float | None = None
        self.running_source: str | None = None
        self.ls_prev_remaining: int | None = None
        self.ls_prev_ts: float | None = None
        self.ls_frozen_since: float | None = None
        self.last_pbp_stop_ts: float | None = None
        self.last_pbp_run_ts: float | None = None
        self.pbp_prev_seconds: float | None = None  # game-clock secs from last PBP play
        self.pbp_prev_seconds_ts: float | None = None
        self.last_event_ts: float | None = None  # any event from any feed
        # Chronological ordering: track the most-recent ACCEPTED play's
        # (period, seconds). Any later-arriving play with an earlier
        # game-clock position is stale (Bolt PBP can deliver plays
        # out of order — observed in production logs).
        self.last_pbp_period: int | None = None
        self.last_pbp_seconds: float | None = None
        self.history = deque(maxlen=50)
        self.period: int | None = None
        self.remaining_sec: int | None = None
        self.match_period: str = ""
        self.phase: str = UNKNOWN
        self.phase_source: str = ""
        self.phase_ts: float | None = None


def _is_pbp_stale(s: _GameState, ev_period, ev_seconds) -> bool:
    """Return True if this play arrived out of chronological order.

    Bolt PBP `seconds` is GAME-ELAPSED (increases over time, e.g. Q2
    runs 720→1440). The stale-detector exists to filter LATE-arriving
    Q2 plays that show up DURING halftime (large cross-period drift).

    Within the SAME period, Bolt frequently delivers events 5-15s out
    of order (e.g. turnover at sec=1687 arriving after rebound at
    sec=1692). These are NOT stale — they're concurrent plays with
    micro-reorder. Rejecting them silently drops legitimate stoppage
    signals. So same-period events are always accepted; cross-period
    drift and large in-period drift are the only real staleness cases.

    Sentinel `seconds=-1` (periodstart, periodscore, etc.) is always
    accepted — those are authoritative state-transition events."""
    if ev_period is None or ev_seconds is None:
        return False
    if isinstance(ev_seconds, (int, float)) and ev_seconds < 0:
        return False  # sentinel — period-boundary event
    if s.last_pbp_period is None or s.last_pbp_seconds is None:
        return False  # first event we've seen
    # Earlier period = always stale
    if ev_period < s.last_pbp_period:
        return True
    # Later period = always fresh (accept; period rolled forward)
    if ev_period > s.last_pbp_period:
        return False
    # SAME period — only flag as stale if WAY out of order (>30s drift),
    # which would indicate a far-back-in-time replay, not micro-reorder.
    # The LS boundary guard handles the halftime case authoritatively.
    return (s.last_pbp_seconds - ev_seconds) > 30.0


_states: dict[str, _GameState] = {}
_lock = threading.RLock()
_callbacks: list = []

# CDN fallback. Set once at startup via set_cdn_fallback(fn).
# Callable(bolt_key) -> {"running": bool|None, "phase": str|None} | None
_cdn_fallback_fn = None
# Per-game record of which source served the last query: "bolt" | "cdn" | "none"
_active_source: dict[str, str] = {}


# ─── Public API ──────────────────────────────────────────────────────────
def register_callback(fn):
    """fn(bolt_key, running: bool, source: str, ts: float). Called on every flip."""
    with _lock:
        if fn not in _callbacks:
            _callbacks.append(fn)


def set_cdn_fallback(fn):
    """Register a CDN fallback resolver. fn(bolt_key) should return
    {"running": bool|None, "phase": str|None} or None when no CDN data
    is available. Used by is_running/get_phase/get_state when bolt-side
    data is missing or dead."""
    global _cdn_fallback_fn
    _cdn_fallback_fn = fn


def get_active_source(bolt_key: str) -> str | None:
    """Returns the source that last served this game's query: 'bolt',
    'cdn', or 'none'. Useful for telemetry."""
    return _active_source.get(bolt_key)


def _bolt_alive_locked(s) -> bool:
    """Caller holds _lock. Is bolt-derived state usable?"""
    if s is None or s.running is None:
        return False
    if s.last_event_ts is None:
        return False
    if (time.time() - s.last_event_ts) > DEAD_EVENT_AGE_S:
        return False
    return True


def _set_active_source(bolt_key: str, source: str, reason: str = ""):
    """Track active source per game; log on transition."""
    prev = _active_source.get(bolt_key)
    if prev != source:
        _active_source[bolt_key] = source
        if prev is not None:
            extra = f" ({reason})" if reason else ""
            log.info(f"clock_feed source [{bolt_key}]: {prev} → {source}{extra}")


def _try_cdn(bolt_key: str):
    """Call the registered CDN fallback. Returns None on missing or error."""
    fn = _cdn_fallback_fn
    if fn is None:
        return None
    try:
        return fn(bolt_key)
    except Exception as e:
        log.debug(f"clock_feed cdn fallback err [{bolt_key}]: {e}")
        return None


def is_running(bolt_key: str) -> bool | None:
    """Authoritative running flag with CDN fallback.

    Source priority:
      1. Bolt clock_state_feed (push-driven, sub-second latency)
      2. CDN fallback (registered via set_cdn_fallback) — used when
         bolt has no opinion (running=None) or its state is dead
         (no events for >DEAD_EVENT_AGE_S=180s)
      3. None when both fail

    Logs source transitions per-game via `clock_feed source [key]: a → b`."""
    if not bolt_key:
        return None
    with _lock:
        s = _states.get(bolt_key)
        bolt_ok = _bolt_alive_locked(s)
        bolt_running = s.running if bolt_ok else None
        bolt_dead = (s is not None and s.last_event_ts is not None
                     and (time.time() - s.last_event_ts) > DEAD_EVENT_AGE_S)
    if bolt_ok:
        _set_active_source(bolt_key, "bolt")
        return bolt_running
    cdn = _try_cdn(bolt_key)
    if cdn and cdn.get("running") is not None:
        reason = "bolt_dead" if bolt_dead else "bolt_no_data"
        _set_active_source(bolt_key, "cdn", reason)
        return bool(cdn["running"])
    _set_active_source(bolt_key, "none")
    return None


def get_phase(bolt_key: str) -> str:
    """Authoritative phase enum with CDN fallback. See is_running for
    source priority and transition logging."""
    if not bolt_key:
        return UNKNOWN
    with _lock:
        s = _states.get(bolt_key)
        bolt_ok = _bolt_alive_locked(s)
        bolt_phase = s.phase if (bolt_ok and s.phase != UNKNOWN) else None
        bolt_dead = (s is not None and s.last_event_ts is not None
                     and (time.time() - s.last_event_ts) > DEAD_EVENT_AGE_S)
    if bolt_ok and bolt_phase:
        _set_active_source(bolt_key, "bolt")
        return bolt_phase
    cdn = _try_cdn(bolt_key)
    if cdn and cdn.get("phase"):
        reason = "bolt_dead" if bolt_dead else "bolt_no_data"
        _set_active_source(bolt_key, "cdn", reason)
        return cdn["phase"]
    _set_active_source(bolt_key, "none")
    return UNKNOWN


def last_change_ts(bolt_key: str) -> float | None:
    if not bolt_key:
        return None
    with _lock:
        s = _states.get(bolt_key)
        return s.running_ts if s else None


def get_state(bolt_key: str) -> dict:
    """Returns full state including active source. When bolt is dead/empty
    and CDN fallback is in use, surfaces CDN's running/phase under
    `running` and `phase` keys with `active_source='cdn'`."""
    if not bolt_key:
        return {}
    now = time.time()
    with _lock:
        s = _states.get(bolt_key)
        if s is None:
            # No bolt events yet — still try CDN fallback
            cdn = _try_cdn(bolt_key)
            if cdn and (cdn.get("running") is not None or cdn.get("phase")):
                _set_active_source(bolt_key, "cdn", "no_bolt_state")
                return {
                    "running": cdn.get("running"),
                    "phase": cdn.get("phase") or UNKNOWN,
                    "source": "cdn",
                    "active_source": "cdn",
                    "phase_source": "cdn",
                    "stale": False, "dead": False, "waiting": False,
                }
            _set_active_source(bolt_key, "none")
            return {"running": None, "phase": UNKNOWN, "active_source": "none",
                    "stale": False, "dead": False, "waiting": True}
        age = (now - s.running_ts) if s.running_ts else None
        phase_age = (now - s.phase_ts) if s.phase_ts else None
        last_event_age = (now - s.last_event_ts) if s.last_event_ts else None
        stale = (last_event_age is not None and last_event_age > STALE_EVENT_AGE_S)
        dead = (last_event_age is not None and last_event_age > DEAD_EVENT_AGE_S)
        bolt_ok = _bolt_alive_locked(s)
        bolt_phase = s.phase if (bolt_ok and s.phase != UNKNOWN) else None
        bolt_running = s.running if bolt_ok else None
        bolt_running_source = s.running_source
        bolt_phase_source = s.phase_source
    # Decide what to surface as the headline running/phase
    active = "bolt"
    out_running = bolt_running
    out_phase = bolt_phase or UNKNOWN
    out_source = bolt_running_source or ""
    out_phase_source = bolt_phase_source or ""
    if not bolt_ok or out_phase == UNKNOWN:
        cdn = _try_cdn(bolt_key)
        if cdn and (cdn.get("running") is not None or cdn.get("phase")):
            reason = "bolt_dead" if dead else ("bolt_no_phase" if bolt_ok else "bolt_no_data")
            _set_active_source(bolt_key, "cdn", reason)
            active = "cdn"
            if cdn.get("running") is not None:
                out_running = bool(cdn["running"])
            if cdn.get("phase"):
                out_phase = cdn["phase"]
            out_source = "cdn"
            out_phase_source = "cdn"
        else:
            _set_active_source(bolt_key, "none" if not bolt_ok else "bolt")
            active = _active_source.get(bolt_key, "bolt")
    else:
        _set_active_source(bolt_key, "bolt")
    return {
        "running": out_running,
        "source": out_source,
        "active_source": active,
        "since_ts": s.running_ts,
        "since_age_s": age,
        "period": s.period,
        "remaining_sec": s.remaining_sec,
        "match_period": s.match_period,
        "phase": out_phase,
        "phase_source": out_phase_source,
        "phase_age_s": phase_age,
        "last_event_age_s": last_event_age,
        "stale": stale,
        "dead": dead,
        "history": [
            {"ts": t, "running": r, "source": src}
            for (t, r, src) in list(s.history)[-10:]
        ],
    }


def tick_all():
    """Periodic check — fires pending stops that need wall-time confirmation
    even when no new WS event arrived. Critical for the case where Bolt
    sends one frozen frame then goes silent for 30+ seconds during a
    stoppage (no `pbp_timeout` emitted, no further LS frames).

    Should be called ~1Hz from a heartbeat thread."""
    now = time.time()
    callbacks_to_fire = []
    with _lock:
        for bolt_key, s in list(_states.items()):
            # If LS gave us one frozen-frame signal and ≥STOP_FRAME_WINDOW_S
            # has elapsed since (with no further frames either way), fire
            # the stop. This handles "Bolt PBP missed the timeout AND LS
            # WS went silent" — we still flip to STOPPED based on the
            # frozen evidence we have.
            if (s.ls_frozen_since is not None
                    and s.running is not False
                    and (now - s.ls_frozen_since) >= LS_STOP_FROZEN_WINDOW_S):
                _flip_running_locked(s, False, "ls_frozen_tick", now,
                                     bolt_key, callbacks_to_fire)
                s.last_pbp_stop_ts = now
                s.ls_frozen_since = None
    for cb_args in callbacks_to_fire:
        for cb in list(_callbacks):
            try:
                cb(*cb_args)
            except Exception:
                pass


def reset(bolt_key: str | None = None):
    with _lock:
        if bolt_key is None:
            _states.clear()
        else:
            _states.pop(bolt_key, None)


# ─── Ingestion ───────────────────────────────────────────────────────────
def on_pbp_event(bolt_key: str, ev: dict):
    """Called from boltodds_pbp_feed for every normalized event."""
    if not bolt_key or not isinstance(ev, dict):
        return
    etype = ev.get("type")
    if not etype:
        return
    now = time.time()
    src = f"pbp_{etype}"
    callbacks_to_fire = []  # (bolt_key, running, source, ts) tuples

    with _lock:
        s = _states.setdefault(bolt_key, _GameState())
        s.last_event_ts = now

        # ── Out-of-order guard ────────────────────────────────────────
        # Bolt PBP delivers plays in non-strict order — a play from before
        # a stoppage can arrive after the stop is already reported.
        # Reject stale plays before any signal logic so they can't
        # un-stop a correctly-stopped clock.
        ev_period = (ev.get("raw") or {}).get("period")
        ev_seconds = ev.get("seconds")
        if _is_pbp_stale(s, ev_period, ev_seconds):
            s.history.append((now, s.running, src + ":stale_ordering"))
            return  # ignore entirely — don't pollute state with old data
        # Accept this play — advance the chronological cursor only when
        # this event is strictly newer. Bolt sometimes delivers events
        # within a few seconds of each other out of order; we accept
        # them but the cursor tracks the max seen so future cross-period
        # checks remain meaningful.
        if isinstance(ev_period, int) and ev_period > 0:
            if s.last_pbp_period is None or ev_period > s.last_pbp_period:
                s.last_pbp_period = ev_period
                s.last_pbp_seconds = None  # reset on period advance
        if isinstance(ev_seconds, (int, float)) and ev_seconds >= 0:
            if (s.last_pbp_seconds is None
                    or ev_seconds > s.last_pbp_seconds):
                s.last_pbp_seconds = float(ev_seconds)

        # ── LS-confirmed period boundary guard ────────────────────────
        # When LS authoritatively reports we're at HALFTIME / QUARTER_END
        # / FINAL, NO PBP signal can flip running=true. Period boundaries
        # are determined by the official league clock, not inferred from
        # plays. Stale plays from late in the prior period arrive 30-60s
        # into the break and would otherwise re-start the clock. Ignore
        # them entirely until LS leaves the boundary state.
        mp = s.match_period or ""
        if (mp and not mp.startswith("IN_") and
                any(t in mp for t in ("END", "HALF", "BREAK", "FINAL"))):
            s.history.append((now, s.running, src + ":boundary_ignored"))
            return

        # PBP `seconds` advance → tertiary clock-running signal. Bolt PBP
        # `seconds` is GAME-ELAPSED (increases monotonically while clock
        # runs, e.g. Q2 plays run 720→1440). Two consecutive plays with
        # ascending seconds = clock advanced between them = clock running.
        # GUARDS:
        #   - same_segment: prev reading must be AFTER the most recent
        #     stop. Otherwise the delta reflects clock that ran BEFORE
        #     the stop and is meaningless for restart detection.
        #   - ≥0.5x near-real-time advancement, filters out artifacts.
        #   - Skip sentinel cur_secs<0 (periodstart/score don't carry
        #     a usable clock value).
        cur_secs = ev.get("seconds")
        if isinstance(cur_secs, (int, float)) and cur_secs >= 0:
            prev_secs = s.pbp_prev_seconds
            prev_ts = s.pbp_prev_seconds_ts
            last_stop = s.last_pbp_stop_ts
            same_segment = (prev_ts is not None and last_stop is not None
                            and prev_ts > last_stop)
            s.pbp_prev_seconds = float(cur_secs)
            s.pbp_prev_seconds_ts = now
            if (s.running is False and prev_secs is not None
                    and prev_ts is not None
                    and same_segment
                    and (now - last_stop) >= PBP_POSSESSION_RESTART_DELAY_S
                    and (now - prev_ts) >= 1.0
                    and cur_secs > prev_secs
                    and ((cur_secs - prev_secs) / (now - prev_ts)) >= 0.5):
                _flip_running_locked(s, True, "pbp_seconds_dt", now,
                                     bolt_key, callbacks_to_fire)

        # ── Bolt fires `type=timeout` for BOTH timeout start AND end.
        # Distinguish by description: "Timeout over" = the timeout has
        # ended. Treat as an explicit RESTART signal (flip running=True,
        # phase=LIVE) — Bolt sends it 15-20s before the next possession
        # event, so this is a major past-post protection improvement.
        description = (ev.get("raw") or {}).get("description") or ""
        is_timeout_over = (etype == "timeout"
                           and "over" in description.lower())
        if is_timeout_over:
            _flip_running_locked(s, True, "pbp_timeout_over", now,
                                 bolt_key, callbacks_to_fire)
        # ── Handle late-game made FG (period ≥ 4 AND remaining ≤ 120):
        # in this window a made FG stops the clock for inbound. Outside
        # the window, `goal` neither stops nor flips running on its own
        # (other live signals are present).
        elif etype == "goal":
            in_late_window = (s.period is not None and s.period >= LATE_GAME_PERIOD
                              and s.remaining_sec is not None
                              and s.remaining_sec <= LATE_GAME_REMAINING_S)
            if in_late_window:
                _flip_running_locked(s, False, "pbp_late_goal", now,
                                     bolt_key, callbacks_to_fire)
                s.last_pbp_stop_ts = now
                _maybe_upgrade_phase_locked(s, "pbp_late_goal", now)
            else:
                # Confirm running state if already running, but do NOT
                # use as restart-from-stopped (could be a FT made shot
                # mid-FT-sequence which doesn't actually start the clock).
                if s.running is True:
                    _flip_running_locked(s, True, src, now, bolt_key,
                                         callbacks_to_fire)
                else:
                    s.history.append((now, s.running, src + ":suppressed_goal"))

        elif etype in _STOPPING:
            _flip_running_locked(s, False, src, now, bolt_key, callbacks_to_fire)
            s.last_pbp_stop_ts = now
            # Period-boundary events get reclassified by current period
            if etype in ("periodend", "periodscore"):
                phase = _classify_period_boundary(s.period)
                _force_phase_locked(s, phase, src, now)
            else:
                _maybe_upgrade_phase_locked(s, src, now)

        elif etype in _RUNNING:
            currently_stopped = (s.running is False)
            s.last_pbp_run_ts = now
            if not currently_stopped:
                # Confirm running (dup will be recorded inside)
                _flip_running_locked(s, True, src, now, bolt_key, callbacks_to_fire)
            elif etype in _PBP_RESTART_TYPES:
                # Restart-type — only flip if past the suppression window
                last_stop = s.last_pbp_stop_ts
                if (last_stop is not None
                        and (now - last_stop) < PBP_POSSESSION_RESTART_DELAY_S):
                    s.history.append((now, s.running, src + ":too_soon"))
                else:
                    _flip_running_locked(s, True, src, now, bolt_key,
                                         callbacks_to_fire)
            else:
                # Non-restart live action while stopped — suppress (FT
                # shot, post-FT rebound, etc.)
                s.history.append((now, s.running, src + ":suppressed"))

    # Fire callbacks outside lock
    for cb_args in callbacks_to_fire:
        for cb in list(_callbacks):
            try:
                cb(*cb_args)
            except Exception:
                pass


def on_ls_frame(bolt_key: str, state: dict):
    """Called from boltodds_ls_feed for every match_update frame.
    `state` is the raw LS state dict (not the derived clock_state)."""
    if not bolt_key or not isinstance(state, dict):
        return
    cur_remaining = state.get("remainingTimeSecondsInPeriod")
    if cur_remaining is None:
        return
    now = time.time()
    mp_raw = state.get("matchPeriod")
    mp_str = ""
    if isinstance(mp_raw, list) and len(mp_raw) >= 2:
        mp_str = str(mp_raw[1]).upper()
    elif mp_raw:
        mp_str = str(mp_raw).upper()

    callbacks_to_fire = []
    with _lock:
        s = _states.setdefault(bolt_key, _GameState())
        s.last_event_ts = now
        s.remaining_sec = cur_remaining
        s.match_period = mp_str
        # period extraction
        for n, key in [(1, "FIRST"), (2, "SECOND"), (3, "THIRD"), (4, "FOURTH")]:
            if key in mp_str:
                s.period = n
                break
        else:
            if "OVER" in mp_str:
                s.period = 5

        # Hard overrides (no prev needed)
        if cur_remaining == 0:
            s.ls_frozen_since = None
            s.ls_prev_remaining = cur_remaining
            s.ls_prev_ts = now
            src = "ls_remaining=0"
            _flip_running_locked(s, False, src, now, bolt_key, callbacks_to_fire)
            s.last_pbp_stop_ts = now
            # remaining=0 boundary phase by period
            phase = _classify_period_boundary(s.period)
            _force_phase_locked(s, phase, src, now)
        elif (mp_str and not mp_str.startswith("IN_") and
                ("END" in mp_str or "HALF" in mp_str or "BREAK" in mp_str
                 or "FINAL" in mp_str)):
            s.ls_frozen_since = None
            s.ls_prev_remaining = cur_remaining
            s.ls_prev_ts = now
            src = f"ls_mp={mp_str}"
            _flip_running_locked(s, False, src, now, bolt_key, callbacks_to_fire)
            s.last_pbp_stop_ts = now
            # Map LS matchPeriod to phase
            if "HALF" in mp_str:
                _force_phase_locked(s, HALFTIME, src, now)
            elif "FINAL" in mp_str:
                _force_phase_locked(s, FINAL, src, now)
            elif s.period == 2:
                _force_phase_locked(s, HALFTIME, src, now)
            else:
                _force_phase_locked(s, QUARTER_END, src, now)
        else:
            prev_r = s.ls_prev_remaining
            prev_t = s.ls_prev_ts
            s.ls_prev_remaining = cur_remaining
            s.ls_prev_ts = now
            if prev_r is not None and prev_t is not None:
                wall_dt = now - prev_t
                if wall_dt >= LS_START_MIN_WALL_S:
                    game_dt = prev_r - cur_remaining
                    if game_dt > 0 and (game_dt / wall_dt) > LS_START_GAME_RATIO:
                        s.ls_frozen_since = None
                        _flip_running_locked(s, True, "ls_game_dt", now,
                                             bolt_key, callbacks_to_fire)
                    elif game_dt == 0:
                        if s.ls_frozen_since is None:
                            s.ls_frozen_since = prev_t
                        elif (now - s.ls_frozen_since) >= LS_STOP_FROZEN_WINDOW_S:
                            _flip_running_locked(s, False, "ls_frozen", now,
                                                 bolt_key, callbacks_to_fire)
                            s.last_pbp_stop_ts = now
                            _maybe_upgrade_phase_locked(s, "ls_frozen", now)

    # Fire callbacks outside lock
    for cb_args in callbacks_to_fire:
        for cb in list(_callbacks):
            try:
                cb(*cb_args)
            except Exception:
                pass


# ─── Internal flip dispatch ──────────────────────────────────────────────
def _flip_running_locked(s, running, source, ts, bolt_key, callbacks_out):
    """Internal flip handler. Caller holds _lock and provides a list to
    accumulate callbacks (fired outside lock by caller). Returns True if
    state actually changed."""
    if running == s.running:
        s.history.append((ts, running, source + ":dup"))
        return False
    s.running = running
    s.running_ts = ts
    s.running_source = source
    s.history.append((ts, running, source))
    if running:
        _force_phase_locked(s, LIVE, source, ts)
    else:
        # Period-boundary events get period-specific classification
        if source in ("pbp_periodend", "pbp_periodscore", "ls_remaining=0"):
            _force_phase_locked(s, _classify_period_boundary(s.period),
                                source, ts)
        else:
            _force_phase_locked(s, _PHASE_BY_SOURCE.get(source, DEAD_BALL),
                                source, ts)
    callbacks_out.append((bolt_key, running, source, ts))
    return True


def _maybe_upgrade_phase_locked(s, source, ts):
    """A more-specific stopping signal arrived within the upgrade window.
    e.g. foul fires → DEAD_BALL_FOUL, then 1.5s later free_throws_awarded
    arrives → upgrade to FREE_THROW. Only upgrades, never downgrades."""
    new_phase = _PHASE_BY_SOURCE.get(source)
    if new_phase is None or s.running:
        return
    # Outside upgrade window? Replace anyway (newer event is more current).
    if s.phase_ts is not None and (ts - s.phase_ts) > PHASE_UPGRADE_WINDOW_S:
        _force_phase_locked(s, new_phase, source, ts)
        return
    cur_spec = _PHASE_SPECIFICITY.get(s.phase, 0)
    new_spec = _PHASE_SPECIFICITY.get(new_phase, 0)
    if new_spec >= cur_spec:
        _force_phase_locked(s, new_phase, source, ts)


def _force_phase_locked(s, phase, source, ts):
    if phase == s.phase and source == s.phase_source:
        return
    s.phase = phase
    s.phase_source = source
    s.phase_ts = ts
