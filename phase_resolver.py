"""phase_resolver.py — Bolt-derived NBA game phase resolver for v2.

Maps every available Bolt signal into the 7-phase taxonomy:
  LIVE, DEAD_BALL_SHORT, DEAD_BALL_LONG, FREE_THROW, TIMEOUT, QUARTER_END, HALFTIME
  + FINAL, UNKNOWN

Signal hierarchy (fastest→slowest, used as priority cascade):

  1. PBP `new_play` event types (millisecond-fresh):
       free_throws_awarded → FREE_THROW
       timeout             → TIMEOUT
       periodscore         → period boundary (HALFTIME / QUARTER_END / FINAL)
       periodend           → period boundary
       foul, turnover_basket → DEAD_BALL_LONG trigger
       goal, attempt_missed, rebound, possession, steal_basket → LIVE evidence

  2. PBP `seconds` field decrement vs wall-clock (independent clock-running derivation):
       2+ plays in 5s with descending `seconds` → clock_running=True

  3. LS `matchPeriod` string (authoritative for HALFTIME/QUARTER_END/FINAL):
       "HALFTIME"            → HALFTIME
       "END_OF_X_QUARTER"    → QUARTER_END / FINAL (period 4)
       "FINAL"               → FINAL

  4. LS derived clock_running (from remaining_sec decrement):
       True  → LIVE
       False → DEAD_BALL_SHORT/LONG (classified by recent foul/turnover)
       None  → fall to PBP-derived

  5. PBP recency (last-resort presence signal):
       last play <30s ago → LIVE_PBP

NOT a trading-decision engine. Returns the phase string + a small breakdown
dict for debugging. Caller (app.py) decides what to do with the phase.
"""
from __future__ import annotations

import time

import boltodds_ls_feed
import boltodds_pbp_feed

# Window sizes for "recent event" lookups (seconds back from now)
TIMEOUT_WINDOW = 130        # PBP timeouts can extend 60-120s; cap at 130 for buffer
FT_WINDOW = 60              # FT awarded → all shots resolved typically <50s
PERIOD_BOUNDARY_WINDOW = 30 # periodscore/periodend recency for boundary classification
DEAD_TRIGGER_WINDOW = 10    # foul/turnover lookup before classifying dead-ball
LIVE_RECENCY = 8            # PBP-only "game is live" presence signal (tight — real
                            # live action emits a play every 1-3s; >8s gap = clock
                            # almost certainly stopped, don't infer LIVE from it)
PBP_CLK_WINDOW = 6          # window for deriving clock_running from PBP `seconds` decrement

# Event types that count as "live action" for PBP-only LIVE inference
_LIVE_EVENT_TYPES = {
    "goal", "attempt_missed", "rebound", "possession",
    "steal_basket", "block", "won_jump_ball",
}

# Event types that deterministically STOP the game clock in NBA.
# Arriving via PBP, they let us call clock_running=False instantly,
# without waiting for LS to emit its clockStatus transition event.
_CLOCK_STOPPING_EVENTS = {
    "foul", "turnover_basket", "timeout", "free_throws_awarded",
    "videoreview", "periodscore", "periodend",
    "player_disqualified", "won_jump_ball",  # jump ball stops the clock until tip
}

# Event types that imply the clock is RUNNING (we observed ongoing live action).
# `goal` excluded — last-2min of Q4/OT a made FG also stops the clock.
_CLOCK_RUNNING_EVENTS = {
    "attempt_missed", "rebound", "possession", "steal_basket", "block",
}


def _derive_clock_running_from_pbp(recent_plays: list, now: float):
    """Try to derive clock_running by checking if PBP `seconds` (game clock)
    is decrementing across recent plays. Returns True / False / None.

    `seconds` in NBA Stream-4 plays = remaining seconds in the period at the
    play's moment. If two plays within wall-clock window have descending
    seconds, the clock was ticking between them.
    """
    if not recent_plays:
        return None
    # Filter to plays in the last PBP_CLK_WINDOW seconds with valid seconds
    candidates = [p for p in recent_plays
                  if (now - p["ts"]) < PBP_CLK_WINDOW
                  and isinstance(p.get("seconds"), (int, float))
                  and p.get("seconds", -1) >= 0]
    if len(candidates) < 2:
        return None
    # Compare oldest to newest in window
    a, b = candidates[0], candidates[-1]
    if (now - b["ts"]) > 4:  # last play stale → can't infer
        return None
    sec_drop = (a.get("seconds") or 0) - (b.get("seconds") or 0)
    wall_delta = b["ts"] - a["ts"]
    if wall_delta < 1:
        return None
    # Clock decremented at all → likely running
    if sec_drop > 0:
        return True
    # Multiple plays in window but clock not decrementing → clock stopped
    if sec_drop == 0 and wall_delta > 2:
        return False
    return None


_CSF_TO_SPEC_PHASE = {
    "LIVE": "LIVE",
    "TIMEOUT": "TIMEOUT",
    "FREE_THROW": "FREE_THROW",
    "DEAD_BALL_FOUL": "DEAD_BALL_LONG",
    "DEAD_BALL_TURNOVER": "DEAD_BALL_LONG",
    "DEAD_BALL_REVIEW": "DEAD_BALL_LONG",
    "DEAD_BALL_JUMPBALL": "DEAD_BALL_SHORT",
    "QUARTER_END": "QUARTER_END",
    "HALFTIME": "HALFTIME",
    "FINAL": "FINAL",
    "DEAD_BALL": "DEAD_BALL_SHORT",
    "UNKNOWN": "UNKNOWN",
    "PRE": "UNKNOWN",
}


def resolve_phase(bolt_key: str) -> dict:
    """Resolve the current game phase using all available Bolt signals.

    Returns a dict with phase, reason, and all evidence used.
    """
    out = {
        "phase": "UNKNOWN",
        "reason": "",
        "clock_running": None,
        "clock_source": None,         # which signal derived clock_running
        "period": None,
        "remaining_sec": None,
        "match_period": "",
        "score": None,                # {"home":..,"away":..} from latest PBP
        "evidence": [],
        "last_play": None,            # {"type":..,"name":..,"team":..,"age_s":..,"seconds":..}
    }
    if not bolt_key:
        out["reason"] = "no bolt_key"
        return out

    now = time.time()

    # ─── Pull all signals ────────────────────────────────────────────────
    recent_plays = boltodds_pbp_feed.get_recent_events(
        bolt_key, since_ts=now - max(TIMEOUT_WINDOW, FT_WINDOW)
    )
    out["evidence"] = [e["type"] for e in recent_plays[-12:]]

    if recent_plays:
        last = recent_plays[-1]
        out["last_play"] = {
            "type": last.get("type"),
            "name": last.get("raw", {}).get("name") or last.get("type"),
            "team": last.get("team"),
            "scorer": last.get("scorer"),
            "player": last.get("player"),
            "points": last.get("points"),
            "awarded": last.get("awarded"),
            "fouled": last.get("fouled"),
            "fouling": last.get("fouling"),
            "seconds": last.get("seconds"),
            "age_s": now - last["ts"],
        }
        # Score from any play that includes it (most plays do)
        for p in reversed(recent_plays):
            sc = p.get("score") or p.get("raw", {}).get("score")
            if sc:
                out["score"] = sc
                break

    # Fallback: if PBP didn't carry score (no recent plays), pull from LS state
    # (boxscore/match_info REST sets points_a / points_b on seed).
    if out["score"] is None:
        ls_pre = boltodds_ls_feed.get_clock_state(bolt_key)
        if ls_pre and (ls_pre.get("points_a") is not None or ls_pre.get("points_b") is not None):
            out["score"] = {"home": ls_pre.get("points_a"), "away": ls_pre.get("points_b")}

    ls = boltodds_ls_feed.get_clock_state(bolt_key)
    if ls:
        out["period"] = ls.get("period")
        out["remaining_sec"] = ls.get("remaining_secs")
        mp_raw = ls.get("match_period") or []
        mp_str = mp_raw[1] if isinstance(mp_raw, list) and len(mp_raw) >= 2 else str(mp_raw or "")
        out["match_period"] = mp_str

    mp_upper = (out["match_period"] or "").upper()

    # ─── Pre-compute event windows ───────────────────────────────────────
    fts_recent = [e for e in recent_plays
                  if e["type"] == "free_throws_awarded" and (now - e["ts"]) < FT_WINDOW]
    timeouts_recent = [e for e in recent_plays
                       if e["type"] == "timeout" and (now - e["ts"]) < TIMEOUT_WINDOW]
    period_boundary_recent = [e for e in recent_plays
                              if e["type"] in ("periodscore", "periodend")
                              and (now - e["ts"]) < PERIOD_BOUNDARY_WINDOW]
    dead_trigger_recent = [e for e in recent_plays
                           if e["type"] in ("foul", "turnover_basket")
                           and (now - e["ts"]) < DEAD_TRIGGER_WINDOW]

    # ─── Clock-running: read the unified clock_state_feed ──────────────
    # Single source of truth. Multiplexes Bolt PBP + LS + CDN cross-check
    # with proper out-of-order guard, late-game-FG handling, FT-shot
    # suppression, and 14 unit tests. See clock_state_feed.py.
    # Falls back to LS or PBP-seconds when clock_state_feed has no opinion
    # (e.g. fresh subscription before first WS event).
    try:
        import clock_state_feed
        cf_running = clock_state_feed.is_running(bolt_key)
        cf_state = clock_state_feed.get_state(bolt_key) if bolt_key else {}
    except Exception:
        cf_running = None
        cf_state = {}
    ls_running = ls.get("clock_running") if ls else None

    if cf_running is not None and not cf_state.get("dead"):
        out["clock_running"] = cf_running
        out["clock_source"] = f"csf_{cf_state.get('source','?')}"
    elif ls_running is not None:
        out["clock_running"] = ls_running
        out["clock_source"] = "ls"
    else:
        pbp_running = _derive_clock_running_from_pbp(recent_plays, now)
        if pbp_running is not None:
            out["clock_running"] = pbp_running
            out["clock_source"] = "pbp_seconds"

    # Infer period from PBP if LS hasn't reported one yet
    if out["period"] is None:
        for p in reversed(recent_plays):
            pp = p.get("raw", {}).get("period")
            if isinstance(pp, int) and pp > 0:
                out["period"] = pp
                break

    # ─── 0. PRIMARY: clock_state_feed.get_phase() ─────────────────────────
    # Single push-driven multiplexer over Bolt PBP + LS, with FT-shot
    # suppression, ordering guard, late-game-FG handling, and 14 unit
    # tests. We consult it FIRST and short-circuit when it has a real
    # answer — eliminates the FREE_THROW-window lag bug and the
    # attempt_missed flicker bug present in the legacy cascade below.
    if cf_state and not cf_state.get("dead") and not cf_state.get("waiting"):
        cf_phase_raw = cf_state.get("phase") or "UNKNOWN"
        spec_phase = _CSF_TO_SPEC_PHASE.get(cf_phase_raw, cf_phase_raw)
        if spec_phase and spec_phase != "UNKNOWN":
            out["phase"] = spec_phase
            out["reason"] = (f"clock_state_feed phase={cf_phase_raw} "
                             f"src={cf_state.get('phase_source','?')}")
            return out

    # ─── 1. matchPeriod string (LS authoritative for boundaries) ─────────
    if "HALF" in mp_upper:
        out["phase"] = "HALFTIME"
        out["reason"] = f"matchPeriod={out['match_period']}"
        return out
    if "FINAL" in mp_upper:
        out["phase"] = "FINAL"
        out["reason"] = f"matchPeriod={out['match_period']}"
        return out
    if "BREAK" in mp_upper or "END_OF" in mp_upper or "END_" in mp_upper:
        # Period 4 end → FINAL only if no OT signaled; otherwise QUARTER_END (going to OT)
        if out["period"] and out["period"] >= 4 and "OT" not in mp_upper:
            out["phase"] = "QUARTER_END"  # treat end of regulation same as q-end (could be OT)
        else:
            out["phase"] = "QUARTER_END"
        out["reason"] = f"matchPeriod={out['match_period']}"
        return out

    # ─── 2. periodscore / periodend PBP event (boundary fast path) ───────
    if period_boundary_recent:
        last_b = period_boundary_recent[-1]
        period_at_event = last_b.get("raw", {}).get("period") or out["period"] or 0
        age = now - last_b["ts"]
        if period_at_event == 2:
            out["phase"] = "HALFTIME"
            out["reason"] = f"PBP {last_b['type']} at period={period_at_event}, {age:.0f}s ago"
            return out
        if period_at_event in (1, 3):
            out["phase"] = "QUARTER_END"
            out["reason"] = f"PBP {last_b['type']} at period={period_at_event}, {age:.0f}s ago"
            return out
        if period_at_event == 4:
            # End of regulation → could be FINAL or going to OT. Without LS hint,
            # call QUARTER_END until matchPeriod confirms FINAL.
            out["phase"] = "QUARTER_END"
            out["reason"] = f"PBP {last_b['type']} at period=4 (end of regulation)"
            return out

    # ─── 3. FREE_THROW (PBP fast) ────────────────────────────────────────
    if fts_recent:
        last_fta = fts_recent[-1]
        ftage = now - last_fta["ts"]
        # Exit if clock has restarted after FT and ≥15s elapsed (FT sequence done)
        if out["clock_running"] is True and ftage > 15:
            pass
        else:
            out["phase"] = "FREE_THROW"
            out["reason"] = f"free_throws_awarded {ftage:.0f}s ago, awarded={last_fta.get('awarded')}"
            return out

    # ─── 4. TIMEOUT (PBP fast) ───────────────────────────────────────────
    if timeouts_recent and out["clock_running"] is not True:
        ts_age = now - timeouts_recent[-1]["ts"]
        out["phase"] = "TIMEOUT"
        out["reason"] = f"timeout called {ts_age:.0f}s ago, clock not started"
        return out

    # ─── 5. End-of-period via remaining_sec=0 ────────────────────────────
    # The clock can't show 0 mid-period unless we're at a boundary.
    # Treat any 0-remaining as period boundary even when clock_running is None
    # (Bolt LS often lags the matchPeriod flag flip from "IN_QUARTER_X" to
    # "END_OF_QUARTER_X" / "HALFTIME").
    if out["remaining_sec"] == 0 and out["clock_running"] is not True:
        if out["period"] == 2:
            out["phase"] = "HALFTIME"
            out["reason"] = "remaining_sec=0 + period=2 (matchPeriod lag)"
        elif out["period"] and out["period"] >= 1:
            out["phase"] = "QUARTER_END"
            out["reason"] = f"remaining_sec=0 + period={out['period']} (matchPeriod lag)"
        return out

    # ─── 6. LIVE / DEAD_BALL ──────────────────────────────────────────────
    if out["clock_running"] is True:
        out["phase"] = "LIVE"
        out["reason"] = f"clock_running=True (src={out['clock_source']})"
        return out

    if out["clock_running"] is False:
        if dead_trigger_recent:
            out["phase"] = "DEAD_BALL_LONG"
            last_trig = dead_trigger_recent[-1]
            out["reason"] = (f"clock stopped, recent {last_trig['type']} "
                             f"{now - last_trig['ts']:.0f}s ago (src={out['clock_source']})")
        else:
            out["phase"] = "DEAD_BALL_SHORT"
            out["reason"] = f"clock stopped, no foul/turnover (src={out['clock_source']})"
        return out

    # ─── PBP-only fallback only fires when LS is genuinely unavailable ───
    # If we have ANY LS state at all (including REST-seeded), that signal is
    # authoritative for LIVE vs DEAD_BALL. We don't overrule it with stale
    # PBP "I saw a possession 7s ago" inference. PBP-only inference is for
    # the no-LS case (e.g. LS slot held by another consumer).
    ls_age = (ls.get("age_s") if ls else None)
    ls_fresh = ls is not None and (ls_age is None or ls_age < 60)

    if not ls_fresh and recent_plays:
        last_play = recent_plays[-1]
        last_age = now - last_play["ts"]
        last_type = last_play["type"]
        if dead_trigger_recent and last_age < DEAD_TRIGGER_WINDOW:
            out["phase"] = "DEAD_BALL_LONG"
            out["reason"] = f"PBP-only (LS stale): {last_type} {last_age:.0f}s ago"
            return out
        if last_type in _LIVE_EVENT_TYPES and last_age < LIVE_RECENCY:
            out["phase"] = "LIVE_PBP"
            out["reason"] = f"PBP-only (LS stale): '{last_type}' {last_age:.0f}s ago"
            return out
        out["phase"] = "UNKNOWN"
        out["reason"] = f"PBP-only (LS stale): last play '{last_type}' {last_age:.0f}s ago"
        return out

    # We have LS state but it didn't give us LIVE/DEAD_BALL — likely
    # clock_running is None and remaining_sec is None. Stay UNKNOWN so the
    # boxscore/match_info auto-refresh in get_phase_summary kicks in.
    out["phase"] = "UNKNOWN"
    if ls is None:
        out["reason"] = "no LS state, no PBP events"
    else:
        out["reason"] = (f"LS state present but clock indeterminate "
                         f"(period={out['period']}, remaining={out['remaining_sec']}, "
                         f"age={ls_age:.0f}s)" if ls_age is not None
                         else "LS state present but clock indeterminate")
    return out


def get_phase_summary(bolt_key: str) -> dict:
    """Lightweight wrapper for UI — returns just phase + 1-line reason + connected flag.

    If the first resolution returns UNKNOWN, attempts a throttled boxscore re-poll
    via boltodds_ls_feed.refresh_for_unknown(), then resolves once more. This
    closes the gap when LS WS hasn't received a state-change event yet.
    """
    p = resolve_phase(bolt_key)
    if p["phase"] == "UNKNOWN" and bolt_key:
        try:
            if boltodds_ls_feed.refresh_for_unknown(bolt_key):
                p = resolve_phase(bolt_key)
                if p.get("reason"):
                    p["reason"] += " (refreshed via boxscore)"
        except Exception:
            pass
    return {
        "phase": p["phase"],
        "reason": p["reason"],
        "ls_connected": boltodds_ls_feed.is_connected(),
        "pbp_connected": boltodds_pbp_feed.is_connected(),
        "period": p["period"],
        "remaining_sec": p["remaining_sec"],
        "clock_running": p["clock_running"],
        "clock_source": p["clock_source"],
        "match_period": p["match_period"],
        "score": p["score"],
        "evidence": p["evidence"],
        "last_play": p["last_play"],
    }
