"""Unit tests for clock_state_feed — validates the optimizations that
can't easily be observed live (late-game FG, halftime classification,
seconds-decrement, staleness, callback safety, suppression behavior).
"""
import time

import clock_state_feed as cf


def setup():
    cf.reset()
    return "test_game"


# ─── Late-game made FG (period ≥ 4 AND remaining ≤ 120) ───────────────────
def test_late_game_made_fg_stops_clock():
    g = setup()
    now = time.time()
    # Establish state: Q4, 1:30 remaining, running
    cf.on_ls_frame(g, {
        "matchPeriod": ["BasketballMatchPeriod", "IN_FOURTH_QUARTER"],
        "remainingTimeSecondsInPeriod": 91, "elapsedTimeSeconds": 629,
        "pointsA": 100, "pointsB": 100,
    })
    time.sleep(1.1)
    cf.on_ls_frame(g, {
        "matchPeriod": ["BasketballMatchPeriod", "IN_FOURTH_QUARTER"],
        "remainingTimeSecondsInPeriod": 90, "elapsedTimeSeconds": 630,
        "pointsA": 100, "pointsB": 100,
    })
    assert cf.is_running(g) is True, f"expected running, got {cf.is_running(g)}"
    # Made FG in last 2 min — should STOP
    cf.on_pbp_event(g, {"type": "goal", "seconds": 90})
    assert cf.is_running(g) is False, f"expected stopped, got {cf.is_running(g)}"
    assert cf.get_phase(g) == cf.DEAD_BALL, f"expected DEAD_BALL, got {cf.get_phase(g)}"
    src = cf.get_state(g)["source"]
    assert src == "pbp_late_goal", f"expected pbp_late_goal, got {src}"
    print("✓ late_game_made_fg_stops_clock")


def test_normal_made_fg_doesnt_stop_clock():
    g = setup()
    # Q2 running, made FG should NOT stop
    cf.on_ls_frame(g, {
        "matchPeriod": ["BasketballMatchPeriod", "IN_SECOND_QUARTER"],
        "remainingTimeSecondsInPeriod": 400, "elapsedTimeSeconds": 320,
        "pointsA": 50, "pointsB": 50,
    })
    time.sleep(1.1)
    cf.on_ls_frame(g, {
        "matchPeriod": ["BasketballMatchPeriod", "IN_SECOND_QUARTER"],
        "remainingTimeSecondsInPeriod": 399, "elapsedTimeSeconds": 321,
        "pointsA": 50, "pointsB": 50,
    })
    assert cf.is_running(g) is True
    cf.on_pbp_event(g, {"type": "goal", "seconds": 399})
    assert cf.is_running(g) is True, "non-late-game goal should not stop clock"
    print("✓ normal_made_fg_doesnt_stop_clock")


# ─── Halftime classification ──────────────────────────────────────────────
def test_periodend_at_q2_is_halftime():
    g = setup()
    cf.on_ls_frame(g, {
        "matchPeriod": ["BasketballMatchPeriod", "IN_SECOND_QUARTER"],
        "remainingTimeSecondsInPeriod": 5, "elapsedTimeSeconds": 715,
        "pointsA": 60, "pointsB": 58,
    })
    cf.on_pbp_event(g, {"type": "periodend", "seconds": 0})
    assert cf.get_phase(g) == cf.HALFTIME, f"expected HALFTIME, got {cf.get_phase(g)}"
    print("✓ periodend_at_q2_is_halftime")


def test_periodend_at_q1_is_quarter_end():
    g = setup()
    cf.on_ls_frame(g, {
        "matchPeriod": ["BasketballMatchPeriod", "IN_FIRST_QUARTER"],
        "remainingTimeSecondsInPeriod": 1, "elapsedTimeSeconds": 719,
        "pointsA": 30, "pointsB": 28,
    })
    cf.on_pbp_event(g, {"type": "periodend", "seconds": 0})
    assert cf.get_phase(g) == cf.QUARTER_END
    print("✓ periodend_at_q1_is_quarter_end")


# ─── PBP seconds-decrement as restart fallback ────────────────────────────
def test_pbp_seconds_decrement_restart():
    g = setup()
    now_base = time.time()
    # Stop the clock via foul
    cf.on_pbp_event(g, {"type": "foul", "seconds": 500})
    assert cf.is_running(g) is False
    # First post-stop play with seconds — too soon to flip
    time.sleep(0.5)
    cf.on_pbp_event(g, {"type": "goal", "seconds": 500})  # FT make, suppressed
    assert cf.is_running(g) is False
    # Wait past restart delay, then a play with descending seconds
    time.sleep(1.1)  # total >1.5s since stop
    # PBP goal with seconds=499 (clock advanced 1 from 500)
    # In our setup, prev was at .5s ago, this is 1.1s later — that's
    # 1.1s wall, prev=500, cur=499 → game_dt=1, wall_dt>=1 → restart
    cf.on_pbp_event(g, {"type": "rebound", "seconds": 499})
    # Note: rebound is in _RUNNING but not _PBP_RESTART_TYPES — so it
    # should be SUPPRESSED as a non-restart event ... HOWEVER the
    # seconds-decrement check runs FIRST in on_pbp_event and can flip
    # to running independently of the action type.
    assert cf.is_running(g) is True, f"expected restart via seconds_dt, got {cf.is_running(g)}"
    src = cf.get_state(g)["source"]
    assert src == "pbp_seconds_dt", f"expected pbp_seconds_dt source, got {src}"
    print("✓ pbp_seconds_decrement_restart")


# ─── Suppression behavior ─────────────────────────────────────────────────
def test_attempt_missed_during_ft_suppressed():
    g = setup()
    cf.on_pbp_event(g, {"type": "foul", "seconds": 200})
    cf.on_pbp_event(g, {"type": "free_throws_awarded", "awarded": 2})
    assert cf.is_running(g) is False
    # FT shot arrives — should be suppressed
    cf.on_pbp_event(g, {"type": "attempt_missed"})
    assert cf.is_running(g) is False, "attempt_missed during FT should not flip"
    history_sources = [h[2] for h in [(t, r, s) for (t, r, s) in cf._states[g].history]]
    assert any(":suppressed" in src for src in history_sources), \
        f"expected suppression marker, got {history_sources}"
    print("✓ attempt_missed_during_ft_suppressed")


def test_possession_too_soon_after_stop():
    g = setup()
    cf.on_pbp_event(g, {"type": "foul", "seconds": 100})
    # Possession event arrives within 1.5s window — too soon
    cf.on_pbp_event(g, {"type": "possession"})
    assert cf.is_running(g) is False, "possession <1.5s after stop should not flip"
    history_sources = [h[2] for h in [(t, r, s) for (t, r, s) in cf._states[g].history]]
    assert any(":too_soon" in src for src in history_sources), \
        f"expected too_soon marker, got {history_sources}"
    print("✓ possession_too_soon_after_stop")


def test_possession_after_delay_restarts():
    g = setup()
    cf.on_pbp_event(g, {"type": "foul", "seconds": 100})
    time.sleep(1.6)
    cf.on_pbp_event(g, {"type": "possession"})
    assert cf.is_running(g) is True
    print("✓ possession_after_delay_restarts")


# ─── Phase upgrade behavior ───────────────────────────────────────────────
def test_foul_then_ft_upgrades():
    g = setup()
    cf.on_pbp_event(g, {"type": "foul"})
    assert cf.get_phase(g) == cf.DEAD_BALL_FOUL
    cf.on_pbp_event(g, {"type": "free_throws_awarded", "awarded": 2})
    assert cf.get_phase(g) == cf.FREE_THROW
    # Specificity: FREE_THROW (100) > DEAD_BALL_FOUL (60), so it upgrades
    print("✓ foul_then_ft_upgrades")


def test_ft_doesnt_downgrade_to_foul():
    g = setup()
    cf.on_pbp_event(g, {"type": "free_throws_awarded", "awarded": 2})
    assert cf.get_phase(g) == cf.FREE_THROW
    cf.on_pbp_event(g, {"type": "foul"})  # later foul during FT — shouldn't downgrade
    # Note: in real life this'd be a separate foul, but our specificity
    # check requires new_spec >= cur_spec; FOUL (60) < FREE_THROW (100),
    # so phase stays FREE_THROW.
    assert cf.get_phase(g) == cf.FREE_THROW
    print("✓ ft_doesnt_downgrade_to_foul")


# ─── Staleness detection ──────────────────────────────────────────────────
def test_staleness_thresholds():
    g = setup()
    cf.on_pbp_event(g, {"type": "possession"})
    state = cf.get_state(g)
    assert state["stale"] is False
    assert state["dead"] is False
    # Manually backdate last_event_ts
    with cf._lock:
        cf._states[g].last_event_ts = time.time() - 70  # > STALE (60s)
    state = cf.get_state(g)
    assert state["stale"] is True, f"expected stale, got {state}"
    assert state["dead"] is False
    with cf._lock:
        cf._states[g].last_event_ts = time.time() - 200  # > DEAD (180s)
    state = cf.get_state(g)
    assert state["stale"] is True
    assert state["dead"] is True
    print("✓ staleness_thresholds")


# ─── Callback safety ──────────────────────────────────────────────────────
def test_callbacks_fired_outside_lock():
    """Verify callbacks can re-enter clock_state_feed without deadlock."""
    g = setup()
    fired = []
    def cb(bk, running, source, ts):
        # Re-enter — read state from inside callback (would deadlock if
        # callback fires while still holding _lock with non-reentrant lock)
        cf.is_running(bk)
        cf.get_state(bk)
        fired.append((bk, running, source))
    cf.register_callback(cb)
    try:
        cf.on_pbp_event(g, {"type": "foul"})
        assert len(fired) == 1, f"expected 1 callback, got {len(fired)}"
        cf.on_pbp_event(g, {"type": "free_throws_awarded"})  # phase upgrade, no flip
        cf.on_pbp_event(g, {"type": "foul"})  # dup, no flip
        # Still 1 — phase upgrades and dups don't fire flip callbacks
        assert len(fired) == 1
        # Restart
        time.sleep(1.6)
        cf.on_pbp_event(g, {"type": "possession"})
        assert len(fired) == 2
        print("✓ callbacks_fired_outside_lock")
    finally:
        cf._callbacks.remove(cb)


# ─── Out-of-order play guard ──────────────────────────────────────────────
def test_stale_play_arriving_after_stop_ignored():
    """Bolt PBP can deliver a goal-from-the-past after a timeout. The
    stale goal must NOT un-stop the clock."""
    g = setup()
    # Live possession at game-clock 200s remaining
    cf.on_pbp_event(g, {
        "type": "possession", "seconds": 200,
        "raw": {"period": 4},
    })
    assert cf.is_running(g) is True
    # Now timeout at 198s
    cf.on_pbp_event(g, {
        "type": "timeout", "seconds": 198,
        "raw": {"period": 4},
    })
    assert cf.is_running(g) is False
    assert cf.get_phase(g) == cf.TIMEOUT
    # NOW a stale `goal` arrives — but its game-clock is 199s (from
    # before the timeout). Must be rejected.
    cf.on_pbp_event(g, {
        "type": "goal", "seconds": 199,
        "raw": {"period": 4},
    })
    assert cf.is_running(g) is False, \
        f"stale goal should not un-stop, got running={cf.is_running(g)}"
    assert cf.get_phase(g) == cf.TIMEOUT, \
        f"phase should stay TIMEOUT, got {cf.get_phase(g)}"
    history_sources = [h["source"] for h in cf.get_state(g).get("history", [])]
    assert any(":stale_ordering" in src for src in history_sources), \
        f"expected stale_ordering marker, got {history_sources}"
    print("✓ stale_play_arriving_after_stop_ignored")


def test_period_advance_accepted():
    """A play in a NEW period must NOT be marked stale even though its
    seconds value is higher than the previous period's last seconds."""
    g = setup()
    cf.on_pbp_event(g, {"type": "possession", "seconds": 5,
                        "raw": {"period": 1}})
    cf.on_pbp_event(g, {"type": "periodend", "seconds": 0,
                        "raw": {"period": 1}})
    # Q2 starts — seconds resets to ~720; goal at start of Q2.
    cf.on_pbp_event(g, {"type": "possession", "seconds": 715,
                        "raw": {"period": 2}})
    history = cf.get_state(g).get("history", [])
    last_src = history[-1]["source"]
    assert ":stale_ordering" not in last_src, \
        f"Q2 play wrongly marked stale: {last_src}"
    print("✓ period_advance_accepted")


# ─── Run all ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    test_late_game_made_fg_stops_clock()
    test_normal_made_fg_doesnt_stop_clock()
    test_periodend_at_q2_is_halftime()
    test_periodend_at_q1_is_quarter_end()
    test_pbp_seconds_decrement_restart()
    test_attempt_missed_during_ft_suppressed()
    test_possession_too_soon_after_stop()
    test_possession_after_delay_restarts()
    test_foul_then_ft_upgrades()
    test_ft_doesnt_downgrade_to_foul()
    test_staleness_thresholds()
    test_callbacks_fired_outside_lock()
    test_stale_play_arriving_after_stop_ignored()
    test_period_advance_accepted()
    print()
    print("ALL TESTS PASSED ✓")
