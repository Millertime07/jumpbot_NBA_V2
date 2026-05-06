"""ls_phase.py — Bolt LiveScores-driven NBA phase resolver.

Designed to beat the NBA CDN on speed and accuracy where LS leads, while
deferring to CDN on the noisy signals (clock stops). Empirical leads
measured from the 2026-04-30 NYK@ATL game (see _notes/ls_phase_analysis.md):

  Score events    : LS leads CDN by +7.8 s median, 100% positive (n=37)
  Clock-restart   : LS leads CDN by +5.7 s median, 100% positive (n=15)
  Clock-stop      : LS leads CDN ~50% raw — needs cross-check + debounce
  Period boundary : LS exposes AT_*_END transient state CDN doesn't have

Stateful, per-game. Caller feeds raw frames in; resolver returns resolved
phase + change events with lead-time metadata. Score-change events carry an
estimated past-post window the bot can use to size its racing window.
"""
from __future__ import annotations

from dataclasses import dataclass, field


# ── Phase enum ────────────────────────────────────────────────────────────
PRE = "PRE"
LIVE_RUNNING = "LIVE_RUNNING"   # clock confirmed running
DEAD_BALL = "DEAD_BALL"         # clock confirmed stopped, in-period
PERIOD_END = "PERIOD_END"       # AT_*_END or remaining=0 mid-game
HALFTIME = "HALFTIME"
FINAL = "FINAL"
UNKNOWN = "UNKNOWN"

# ── Tunables (from analysis) ─────────────────────────────────────────────
# Stricter than monitor's existing 0.5 ratio derivation. Two frames frozen
# over ≥STOP_FRAME_WINDOW_S of wall = tentative stop. CDN must confirm
# (clockRunning False) within STOP_CDN_CONFIRM_TIMEOUT_S, OR we fire anyway.
STOP_FRAME_WINDOW_S = 4.0
STOP_CDN_CONFIRM_TIMEOUT_S = 6.0
START_MIN_WALL_DELTA_S = 1.0       # need ≥1s wall to confirm advance
START_GAME_DELTA_RATIO = 0.5       # game_dt / wall_dt > this → running
LS_MAX_FRESH_AGE_S = 60.0          # past this, LS is stale-degraded
LS_MAX_TRUST_AGE_S = 120.0         # past this, LS is dead → CDN-only
SCORE_LEAD_ESTIMATE_S = 7.8        # empirical median, used by past-post sizer
START_LEAD_ESTIMATE_S = 5.7

# Match-period substrings → boundary phase
_HALF_KEYWORDS = ("HALFTIME", "HALF_TIME")
_FINAL_KEYWORDS = ("FINAL", "MATCH_COMPLETED", "GAME_ENDED")
_END_KEYWORDS = ("END_OF", "AT_", "BREAK", "END_")


@dataclass
class _LSEvent:
    kind: str                    # score_change | clock_started | clock_stopped
                                 # | period_change | pending_stop | stop_canceled
    ts: float
    payload: dict = field(default_factory=dict)


_CDN_STOPPING_ACTIONS = {
    "foul", "turnover", "timeout", "freethrow", "violation",
    "ejection", "review", "videoreview", "jumpball",
}

import re as _re

_CDN_CLOCK_RE = _re.compile(r"PT(\d+)M([\d.]+)S")


def _parse_cdn_clock(s):
    """'PT07M42.00S' → 462.0 seconds, or None."""
    if not s or not isinstance(s, str):
        return None
    m = _CDN_CLOCK_RE.match(s)
    if not m:
        return None
    return int(m.group(1)) * 60 + float(m.group(2))


def _mp_str(mp_raw):
    if isinstance(mp_raw, list) and len(mp_raw) >= 2:
        return str(mp_raw[1]).upper()
    if mp_raw:
        return str(mp_raw).upper()
    return ""


def _classify_mp(mp: str, period: int | None):
    """Return phase string from matchPeriod, or '' if not a boundary."""
    if not mp:
        return ""
    if any(k in mp for k in _FINAL_KEYWORDS):
        return FINAL
    if any(k in mp for k in _HALF_KEYWORDS):
        return HALFTIME
    if mp.startswith("IN_"):
        return ""  # in-period — not a boundary phase
    if mp == "PREMATCH":
        return PRE
    if any(k in mp for k in _END_KEYWORDS):
        return PERIOD_END
    return ""


def _extract_period(mp: str, fallback):
    if not mp:
        return fallback
    for n, key in [(1, "FIRST"), (2, "SECOND"), (3, "THIRD"), (4, "FOURTH")]:
        if key in mp:
            return n
    if "OVER" in mp:
        return 5
    return fallback


class LSPhaseResolver:
    """Per-game phase resolver. Construct one per Bolt universal_id.

    Lifecycle:
        r = LSPhaseResolver()
        r.ingest_ls(ts, state_dict)      # call on every LS match_update
        r.ingest_cdn(ts, snapshot_dict)  # call on every CDN box-score poll
        r.tick(now)                      # call periodically (e.g. 1 Hz)
        r.get_phase(now)                 # query

    `ingest_ls` and `tick` return a list of _LSEvent — drain for past-posting
    triggers. `tick` exists so we still fire pending stops when a CDN
    confirmation timeout elapses without any new frames.
    """

    def __init__(self):
        # LS frame history
        self._prev_state: dict | None = None
        self._prev_ls_ts: float | None = None
        self._last_ls_ts: float | None = None

        # Clock derivation state
        self._frozen_since: float | None = None     # wall ts when remaining last advanced
        self._frozen_remaining: int | None = None
        self._derived_running: bool | None = None   # confirmed clock state
        self._pending_stop_ts: float | None = None  # tentative stop awaiting CDN

        # CDN cross-check state
        self._cdn_running: bool | None = None
        self._cdn_running_ts: float | None = None
        self._cdn_period: int | None = None
        self._cdn_clock: str | None = None

        # CDN clock-string decrement tracking (CDN's clockRunning bool is
        # unreliable — it's True whenever gameStatus==2 and clock != 00:00,
        # so it stays True through timeouts. We derive a real clock_running
        # from the `clock` string ourselves: PT07M42.00S → 462.0 seconds.
        self._cdn_clock_secs: float | None = None
        self._cdn_clock_secs_ts: float | None = None
        self._cdn_clock_frozen_since: float | None = None
        self._cdn_action_stop_ts: float | None = None  # ts of last stopping action

        # Resolved snapshot
        self._period: int | None = None
        self._mp: str = ""
        self._mp_phase: str = ""        # boundary phase from mp ("" if in-period)
        self._remaining_sec: int | None = None
        self._score_a: int | None = None
        self._score_b: int | None = None
        self._last_score_change_ts: float | None = None

    # ── Ingestion ────────────────────────────────────────────────────────

    def ingest_ls(self, ts: float, state: dict) -> list[_LSEvent]:
        events: list[_LSEvent] = []
        if not isinstance(state, dict):
            return events

        cur_remaining = state.get("remainingTimeSecondsInPeriod")
        cur_mp = _mp_str(state.get("matchPeriod"))
        cur_period = _extract_period(cur_mp, state.get("period"))
        cur_a = state.get("pointsA")
        cur_b = state.get("pointsB")

        prev = self._prev_state
        prev_ts = self._prev_ls_ts
        wall_delta = (ts - prev_ts) if prev_ts is not None else 0.0

        # ── 1. Score change (LS lead = +7.8s median, 100% reliable) ──
        if prev is not None:
            pa, pb = prev.get("pointsA"), prev.get("pointsB")
            d_home = (cur_a or 0) - (pa or 0) if pa is not None else 0
            d_away = (cur_b or 0) - (pb or 0) if pb is not None else 0
            if d_home != 0 or d_away != 0:
                events.append(_LSEvent(
                    kind="score_change", ts=ts,
                    payload={
                        "score_home": cur_a, "score_away": cur_b,
                        "delta_home": d_home, "delta_away": d_away,
                        "remaining_sec": cur_remaining,
                        "period": cur_period,
                        "lead_estimate_s": SCORE_LEAD_ESTIMATE_S,
                    },
                ))
                self._last_score_change_ts = ts

        # ── 2. Period / matchPeriod change ──
        if prev is None or _mp_str(prev.get("matchPeriod")) != cur_mp:
            old_phase = self._mp_phase
            new_phase = _classify_mp(cur_mp, cur_period)
            if new_phase != old_phase or prev is None:
                events.append(_LSEvent(
                    kind="period_change", ts=ts,
                    payload={
                        "match_period": cur_mp,
                        "phase": new_phase or "IN_PERIOD",
                        "period": cur_period,
                    },
                ))
            self._mp = cur_mp
            self._mp_phase = new_phase

        # ── 3. Clock-running derivation ──
        running_event = self._update_clock_state(
            ts=ts, prev=prev, cur_remaining=cur_remaining, cur_mp=cur_mp,
            wall_delta=wall_delta,
        )
        if running_event is not None:
            events.append(running_event)

        # ── 4. Commit snapshot ──
        self._prev_state = dict(state)
        self._prev_ls_ts = ts
        self._last_ls_ts = ts
        self._period = cur_period or self._period
        self._remaining_sec = cur_remaining
        self._score_a = cur_a if cur_a is not None else self._score_a
        self._score_b = cur_b if cur_b is not None else self._score_b

        return events

    def _update_clock_state(self, *, ts, prev, cur_remaining, cur_mp,
                            wall_delta) -> _LSEvent | None:
        # Hard overrides — no prev needed.
        if cur_remaining == 0:
            return self._set_running(False, ts, source="remaining=0")
        if cur_mp and not cur_mp.startswith("IN_") and any(
            k in cur_mp for k in _HALF_KEYWORDS + _END_KEYWORDS + _FINAL_KEYWORDS
        ):
            return self._set_running(False, ts, source=f"matchPeriod={cur_mp}")

        if prev is None or wall_delta < START_MIN_WALL_DELTA_S:
            return None
        prev_remaining = prev.get("remainingTimeSecondsInPeriod")
        if prev_remaining is None or cur_remaining is None:
            return None
        if _mp_str(prev.get("matchPeriod")) != cur_mp:
            # Period transition — clock state ambiguous until next frame.
            self._frozen_since = None
            return None

        game_delta = prev_remaining - cur_remaining

        # CLOCK STARTED — confident, fire immediately
        if game_delta > 0 and (game_delta / wall_delta) > START_GAME_DELTA_RATIO:
            self._frozen_since = None
            if self._pending_stop_ts is not None:
                # We had a tentative stop; cancel it.
                self._pending_stop_ts = None
                # Don't return here — still fire start.
            return self._set_running(True, ts, source="game_dt>0")

        # CLOCK STOPPED CANDIDATE — game_delta == 0, clock didn't advance
        if game_delta == 0:
            if self._frozen_since is None:
                self._frozen_since = ts
                self._frozen_remaining = cur_remaining
            elif (ts - self._frozen_since) >= STOP_FRAME_WINDOW_S:
                # 2nd frame still frozen, ≥4s elapsed → tentative stop
                if self._derived_running is not False and self._pending_stop_ts is None:
                    # Check CDN — if it already says stopped, confirm immediately
                    if self._cdn_running is False:
                        return self._set_running(False, ts,
                                                 source="frozen+cdn_confirm")
                    self._pending_stop_ts = ts
                    return _LSEvent(
                        kind="pending_stop", ts=ts,
                        payload={"awaiting_cdn_confirm": True},
                    )
            return None
        # game_delta != 0 but ratio low (out-of-band noise) — ignore
        return None

    def _set_running(self, running: bool, ts: float, source: str) -> _LSEvent | None:
        if running == self._derived_running:
            return None
        prev = self._derived_running
        self._derived_running = running
        self._pending_stop_ts = None
        if running:
            self._frozen_since = None
        return _LSEvent(
            kind="clock_started" if running else "clock_stopped",
            ts=ts,
            payload={
                "from": prev, "source": source,
                "lead_estimate_s": START_LEAD_ESTIMATE_S if running else 0.0,
            },
        )

    def ingest_cdn(self, ts: float, snapshot: dict) -> list[_LSEvent]:
        """CDN box-score poll. Used to:
          (a) confirm pending LS stops via CDN clock-string decrement
          (b) fall back to CDN when LS is stale/dead
        Note: snapshot['clockRunning'] is deliberately ignored — it's True
        whenever gameStatus==2, even during timeouts. Use clock-string
        decrement instead for a real signal.
        """
        events: list[_LSEvent] = []
        clock_str = snapshot.get("clock")
        cur_secs = _parse_cdn_clock(clock_str)
        if cur_secs is not None:
            prev_secs = self._cdn_clock_secs
            prev_ts = self._cdn_clock_secs_ts
            cdn_running = None
            if prev_secs is not None and prev_ts is not None:
                wall_dt = ts - prev_ts
                game_dt = prev_secs - cur_secs
                if wall_dt >= 1.0:
                    if game_dt > 0:
                        cdn_running = True
                        self._cdn_clock_frozen_since = None
                    elif game_dt == 0:
                        if self._cdn_clock_frozen_since is None:
                            self._cdn_clock_frozen_since = prev_ts
                        # ≥4s frozen → CDN-confirmed stop
                        if (ts - self._cdn_clock_frozen_since) >= 4.0:
                            cdn_running = False
                    # game_dt < 0 (clock reset between periods): no signal
            if cdn_running is not None:
                self._cdn_running = cdn_running
                self._cdn_running_ts = ts
                # Confirm pending LS stop
                if (self._pending_stop_ts is not None
                        and cdn_running is False
                        and self._derived_running is not False):
                    ev = self._set_running(False, self._pending_stop_ts,
                                           source="cdn_clock_frozen")
                    if ev is not None:
                        events.append(ev)
                # CDN clock advancing → if LS thinks stopped, override
                elif (cdn_running is True
                      and self._pending_stop_ts is not None
                      and (ts - self._pending_stop_ts) > 10.0):
                    # Long-pending and CDN clock is moving → likely false stop
                    self._pending_stop_ts = None
                    self._frozen_since = None
                    events.append(_LSEvent(
                        kind="stop_canceled", ts=ts,
                        payload={"reason": "cdn_clock_advanced"},
                    ))
            self._cdn_clock_secs = cur_secs
            self._cdn_clock_secs_ts = ts

        self._cdn_period = snapshot.get("period") or self._cdn_period
        self._cdn_clock = clock_str or self._cdn_clock
        return events

    def ingest_cdn_action(self, ts: float, action: dict) -> list[_LSEvent]:
        """CDN PBP action stream. Stopping action types confirm any pending
        LS stop instantly (more reliable than the snapshot bool)."""
        events: list[_LSEvent] = []
        atype = (action.get("actionType") or "").lower()
        if atype in _CDN_STOPPING_ACTIONS:
            self._cdn_action_stop_ts = ts
            if (self._pending_stop_ts is not None
                    and self._derived_running is not False):
                ev = self._set_running(False, self._pending_stop_ts,
                                       source=f"cdn_action_{atype}")
                if ev is not None:
                    events.append(ev)
        return events

    def tick(self, now: float) -> list[_LSEvent]:
        """Periodic poll. Fires pending stops on CDN-confirmation timeout."""
        events: list[_LSEvent] = []
        if (self._pending_stop_ts is not None
                and (now - self._pending_stop_ts) >= STOP_CDN_CONFIRM_TIMEOUT_S
                and self._derived_running is not False):
            ev = self._set_running(False, self._pending_stop_ts,
                                   source="timeout_no_cdn")
            if ev is not None:
                events.append(ev)
        return events

    # ── Query ───────────────────────────────────────────────────────────

    def get_phase(self, now: float) -> dict:
        """Resolve current phase. Caller passes wall-clock now."""
        ls_age = (now - self._last_ls_ts) if self._last_ls_ts is not None else None
        ls_dead = ls_age is None or ls_age > LS_MAX_TRUST_AGE_S
        ls_stale = ls_age is not None and ls_age > LS_MAX_FRESH_AGE_S

        # No LS data at all → CDN fallback if we have it
        if ls_dead:
            return self._cdn_fallback_phase(now, reason=("no_ls" if ls_age is None
                                                          else f"ls_dead_{ls_age:.0f}s"))

        # Boundary phases from matchPeriod take priority
        if self._mp_phase in (FINAL, HALFTIME, PERIOD_END, PRE):
            return self._build(self._mp_phase, ls_age,
                               source=f"matchPeriod={self._mp}")

        # remaining=0 mid-period (LS hasn't flipped to AT_*_END yet)
        if (self._remaining_sec == 0 and self._derived_running is False
                and self._period is not None):
            phase = HALFTIME if self._period == 2 else PERIOD_END
            return self._build(phase, ls_age, source="remaining=0")

        if self._derived_running is True:
            return self._build(LIVE_RUNNING, ls_age,
                               source="ls_clock_running",
                               degraded=ls_stale)
        if self._derived_running is False:
            return self._build(DEAD_BALL, ls_age,
                               source="ls_clock_stopped",
                               degraded=ls_stale)

        # LS connected but clock state indeterminate — try CDN as tiebreak
        if self._cdn_running is True:
            return self._build(LIVE_RUNNING, ls_age,
                               source="cdn_clock_running",
                               degraded=True)
        if self._cdn_running is False:
            return self._build(DEAD_BALL, ls_age,
                               source="cdn_clock_stopped",
                               degraded=True)

        return self._build(UNKNOWN, ls_age,
                           source="indeterminate", degraded=True)

    def _cdn_fallback_phase(self, now: float, reason: str) -> dict:
        cdn_age = ((now - self._cdn_running_ts)
                   if self._cdn_running_ts is not None else None)
        if self._cdn_running is True:
            phase = LIVE_RUNNING
        elif self._cdn_running is False:
            phase = DEAD_BALL
        else:
            phase = UNKNOWN
        return self._build(phase, ls_age=None,
                           source=f"cdn_only ({reason})",
                           degraded=True, cdn_age=cdn_age)

    def _build(self, phase, ls_age, *, source, degraded=False, cdn_age=None):
        return {
            "phase": phase,
            "source": source,
            "degraded": degraded,
            "clock_running": self._derived_running,
            "period": self._period,
            "remaining_sec": self._remaining_sec,
            "match_period": self._mp,
            "score_home": self._score_a,
            "score_away": self._score_b,
            "last_score_change_ts": self._last_score_change_ts,
            "pending_stop": self._pending_stop_ts is not None,
            "ls_age_s": ls_age,
            "cdn_age_s": cdn_age,
            "cdn_clock_running": self._cdn_running,
        }
