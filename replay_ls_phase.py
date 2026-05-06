"""Replay validator for ls_phase.py.

Feeds /tmp/nba_events.jsonl chronologically through the resolver and
compares LS-derived score / clock-start / clock-stop events against the
CDN ground truth.

Reports:
  - Score-event lead time vs CDN PBP made-shot actions
  - Clock-start lead time vs CDN clock_change(start)
  - Clock-stop lead time vs CDN PBP stopping actions
  - False-positive / false-negative counts on stops (target: <14% / <38%)
  - Phase distribution
"""
from __future__ import annotations

import json
import statistics
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import ls_phase as lsp  # noqa: E402


UNIFIED_LOG = "/tmp/nba_events.jsonl"

# CDN action types that stop the NBA clock
STOPPING_ACTION_TYPES = {
    "foul", "turnover", "timeout", "freethrow", "violation",
    "rebound",  # only def rebound after FT — coarse but ok for ground truth
    "ejection", "review",
}
# Score-event types
SCORE_ACTION_TYPES = {"2pt", "3pt", "freethrow"}


def parse_ts(s):
    return datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()


def load_events(path):
    events = []
    with open(path) as f:
        for line in f:
            try:
                ev = json.loads(line)
            except Exception:
                continue
            ev["_ts"] = parse_ts(ev["ts"])
            events.append(ev)
    events.sort(key=lambda e: e["_ts"])
    return events


def stats(label, values):
    if not values:
        print(f"  {label}: n=0")
        return
    n = len(values)
    med = statistics.median(values)
    p25 = statistics.quantiles(values, n=4)[0] if n >= 4 else min(values)
    p75 = statistics.quantiles(values, n=4)[2] if n >= 4 else max(values)
    pos = sum(1 for v in values if v > 0)
    print(f"  {label}: n={n} median={med:+.2f}s "
          f"p25={p25:+.2f}s p75={p75:+.2f}s "
          f"min={min(values):+.2f}s max={max(values):+.2f}s "
          f"LS_lead={pos}/{n} ({100*pos/n:.0f}%)")


def main():
    events = load_events(UNIFIED_LOG)
    print(f"Loaded {len(events)} events from {UNIFIED_LOG}")

    resolver = lsp.LSPhaseResolver()

    # Collected resolver outputs
    ls_score_events = []      # list of (ts, delta_home, delta_away)
    ls_clock_started = []     # list of ts
    ls_clock_stopped = []     # list of ts
    ls_clock_stopped_sources = []  # parallel list of source strings
    ls_clock_started_sources = []
    ls_pending_stops = []     # list of ts
    phase_log = []            # (ts, phase) sampled at each LS frame

    # Ground truth from CDN — derive scores via scoreHome/scoreAway deltas
    # across actions (no shotResult field present in monitor's emit).
    cdn_score_events = []     # list of ts when cumulative score changed
    cdn_stopping_actions = [] # list of (ts, atype)
    cdn_snapshot_clock_run_ts = []   # list of (ts, derived_running) from clock-string
    prev_score = (None, None)
    prev_cdn_clock_secs = None
    prev_cdn_clock_ts = None
    cdn_clock_frozen_since = None
    cdn_running = None

    # Periodic tick to fire pending-stop timeouts
    last_tick = 0

    for ev in events:
        ts = ev["_ts"]
        src = ev["source"]
        kind = ev["kind"]
        payload = ev.get("payload") or {}

        # Drive resolver from LS frames
        if src == "livescore" and kind == "match_update":
            state = payload.get("state") or {}
            new_events = resolver.ingest_ls(ts, state)
            for nev in new_events:
                if nev.kind == "score_change":
                    ls_score_events.append((nev.ts, nev.payload))
                elif nev.kind == "clock_started":
                    ls_clock_started.append(nev.ts)
                    ls_clock_started_sources.append(nev.payload.get("source", "?"))
                elif nev.kind == "clock_stopped":
                    ls_clock_stopped.append(nev.ts)
                    ls_clock_stopped_sources.append(nev.payload.get("source", "?"))
                elif nev.kind == "pending_stop":
                    ls_pending_stops.append(nev.ts)
            phase_log.append((ts, resolver.get_phase(ts)["phase"]))

        # Cross-check from CDN snapshots
        elif src == "cdn" and kind == "snapshot":
            tick_events = resolver.ingest_cdn(ts, payload)
            for nev in tick_events:
                if nev.kind == "clock_stopped":
                    ls_clock_stopped.append(nev.ts)
                    ls_clock_stopped_sources.append(nev.payload.get("source", "?"))
            # Ground-truth CDN clock_running via clock-string decrement
            from ls_phase import _parse_cdn_clock
            cur_secs = _parse_cdn_clock(payload.get("clock"))
            if cur_secs is not None and prev_cdn_clock_secs is not None:
                wall_dt = ts - prev_cdn_clock_ts
                game_dt = prev_cdn_clock_secs - cur_secs
                if wall_dt >= 1.0:
                    if game_dt > 0:
                        if cdn_running is not True:
                            cdn_snapshot_clock_run_ts.append((ts, True))
                        cdn_running = True
                        cdn_clock_frozen_since = None
                    elif game_dt == 0:
                        if cdn_clock_frozen_since is None:
                            cdn_clock_frozen_since = prev_cdn_clock_ts
                        if (ts - cdn_clock_frozen_since) >= 4.0:
                            if cdn_running is not False:
                                cdn_snapshot_clock_run_ts.append(
                                    (cdn_clock_frozen_since + 4.0, False))
                            cdn_running = False
            if cur_secs is not None:
                prev_cdn_clock_secs = cur_secs
                prev_cdn_clock_ts = ts

        elif src == "cdn" and kind == "action":
            tick_events = resolver.ingest_cdn_action(ts, payload)
            for nev in tick_events:
                if nev.kind == "clock_stopped":
                    ls_clock_stopped.append(nev.ts)
                    ls_clock_stopped_sources.append(nev.payload.get("source", "?"))
            # Ground-truth: CDN scores via scoreHome/scoreAway delta
            try:
                sh = int(payload.get("scoreHome") or 0)
                sa = int(payload.get("scoreAway") or 0)
            except (TypeError, ValueError):
                sh, sa = None, None
            if sh is not None and prev_score != (None, None):
                if (sh, sa) != prev_score:
                    cdn_score_events.append(ts)
            if sh is not None:
                prev_score = (sh, sa)
            atype = (payload.get("actionType") or "").lower()
            if atype in STOPPING_ACTION_TYPES:
                cdn_stopping_actions.append((ts, atype))

        # Periodic tick (1Hz wall-equivalent)
        if ts - last_tick > 1.0:
            tick_events = resolver.tick(ts)
            for nev in tick_events:
                if nev.kind == "clock_stopped":
                    ls_clock_stopped.append(nev.ts)
            last_tick = ts

    # Count canceled stops by re-running with a counter — easier path: count
    # distinct ls_pending_stops without a corresponding ls_clock_stopped.
    confirmed_stops = set(ls_clock_stopped)
    pending_no_confirm = 0
    for pts in ls_pending_stops:
        # Confirmed if any ls_clock_stopped within 30s after pending
        if not any(0 <= (cs - pts) <= 30 for cs in confirmed_stops):
            pending_no_confirm += 1

    print()
    print("═" * 70)
    print("RESOLVER OUTPUT COUNTS")
    print("═" * 70)
    print(f"  LS score_change events     : {len(ls_score_events)}")
    from collections import Counter
    started_src = Counter(ls_clock_started_sources)
    stopped_src = Counter(ls_clock_stopped_sources)
    print(f"  LS clock_started events    : {len(ls_clock_started)}  sources={dict(started_src)}")
    print(f"  LS clock_stopped events    : {len(ls_clock_stopped)}  sources={dict(stopped_src)}")
    print(f"  LS pending_stop events     : {len(ls_pending_stops)} "
          f"({pending_no_confirm} never confirmed)")
    print()
    cdn_clock_started = [t for t, r in cdn_snapshot_clock_run_ts if r is True]
    cdn_clock_stopped = [t for t, r in cdn_snapshot_clock_run_ts if r is False]
    print(f"  CDN score events (delta)   : {len(cdn_score_events)}")
    print(f"  CDN stopping actions       : {len(cdn_stopping_actions)}")
    print(f"  CDN clock_started (derived): {len(cdn_clock_started)}")
    print(f"  CDN clock_stopped (derived): {len(cdn_clock_stopped)}")

    # ── Lead-time analysis ────────────────────────────────────────────
    print()
    print("═" * 70)
    print("LEAD TIMES — positive = LS earlier than CDN (good)")
    print("═" * 70)

    # Score: match each LS score event to nearest CDN score-delta within 20s
    score_leads = []
    used_cdn = set()
    for ls_ts, _ in ls_score_events:
        best = None
        best_idx = -1
        for i, cdn_ts in enumerate(cdn_score_events):
            if i in used_cdn:
                continue
            delta = cdn_ts - ls_ts
            if -5 < delta < 20:
                if best is None or abs(delta) < abs(best):
                    best = delta
                    best_idx = i
        if best is not None:
            score_leads.append(best)
            used_cdn.add(best_idx)
    stats("Score-event lead", score_leads)

    # Clock-start: match LS clock_started to nearest CDN clock_started ±15s
    start_leads = []
    used_cdn = set()
    for ls_ts in ls_clock_started:
        best = None; best_idx = -1
        for i, cdn_ts in enumerate(cdn_clock_started):
            if i in used_cdn:
                continue
            delta = cdn_ts - ls_ts
            if -15 < delta < 30:
                if best is None or abs(delta) < abs(best):
                    best = delta; best_idx = i
        if best is not None:
            start_leads.append(best)
            used_cdn.add(best_idx)
    stats("Clock-start lead", start_leads)

    # Clock-stop: match LS clock_stopped to CDN stopping action OR CDN
    # clock_stopped (derived) — whichever is earlier
    cdn_stop_truth = sorted(set([t for t, _ in cdn_stopping_actions] + cdn_clock_stopped))
    stop_leads = []
    used_cdn = set()
    for ls_ts in ls_clock_stopped:
        best = None; best_idx = -1
        for i, cdn_ts in enumerate(cdn_stop_truth):
            if i in used_cdn:
                continue
            delta = cdn_ts - ls_ts
            if -30 < delta < 30:
                if best is None or abs(delta) < abs(best):
                    best = delta; best_idx = i
        if best is not None:
            stop_leads.append(best)
            used_cdn.add(best_idx)
    stats("Clock-stop lead vs CDN truth", stop_leads)

    # Phase distribution
    print()
    print("═" * 70)
    print("PHASE DISTRIBUTION (sampled at each LS frame)")
    print("═" * 70)
    counts = {}
    for _, p in phase_log:
        counts[p] = counts.get(p, 0) + 1
    total = max(1, sum(counts.values()))
    for p, n in sorted(counts.items(), key=lambda x: -x[1]):
        print(f"  {p:20s}: {n:5d} ({100*n/total:5.1f}%)")

    # Comparison vs baseline (analyst's CDN-only numbers)
    print()
    print("═" * 70)
    print("VS BASELINE")
    print("═" * 70)
    if score_leads:
        med = statistics.median(score_leads)
        print(f"  Score lead       resolver={med:+.2f}s   baseline_target=+7.84s")
    if start_leads:
        med = statistics.median(start_leads)
        print(f"  Clock-start lead resolver={med:+.2f}s   baseline_target=+5.70s")
    if stop_leads:
        med = statistics.median(stop_leads)
        print(f"  Clock-stop lead  resolver={med:+.2f}s   baseline (raw monitor)=-1.34s")
    print(f"  Stop FP rate     resolver={pending_no_confirm}/{len(ls_pending_stops) or 1} pending unconfirmed   "
          f"baseline=14% (3/21)")


if __name__ == "__main__":
    main()
