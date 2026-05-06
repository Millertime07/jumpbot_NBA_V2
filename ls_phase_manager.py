"""ls_phase_manager — per-game LSPhaseResolver lifecycle + glue.

Holds one LSPhaseResolver per Bolt universal_id, fed in real time from
the LS WS frame callback. CDN snapshots are pulled on each tick() call
from nba_feed's cached PBP state.

Public API:
    ingest_ls_frame(bolt_key, raw_state)  — called by boltodds_ls_feed on every frame
    tick(bolt_key, nba_cdn_id)            — refresh CDN cross-check + return phase
    get_phase(bolt_key)                   — read-only current phase
    drain_events(bolt_key)                — pop accumulated change events
"""
from __future__ import annotations

import re
import threading
import time

import ls_phase

_resolvers: dict[str, ls_phase.LSPhaseResolver] = {}
_pending_events: dict[str, list] = {}
_lock = threading.RLock()

_CLOCK_RE = re.compile(r"PT(\d+)M([\d.]+)S")


def _ensure(bolt_key: str) -> ls_phase.LSPhaseResolver:
    r = _resolvers.get(bolt_key)
    if r is None:
        r = ls_phase.LSPhaseResolver()
        _resolvers[bolt_key] = r
        _pending_events[bolt_key] = []
    return r


def ingest_ls_frame(bolt_key: str, raw_state: dict):
    """Called from boltodds_ls_feed for every match_update frame."""
    if not bolt_key or not isinstance(raw_state, dict):
        return
    now = time.time()
    with _lock:
        r = _ensure(bolt_key)
        events = r.ingest_ls(now, raw_state)
        if events:
            _pending_events[bolt_key].extend(events)


def tick(bolt_key: str, nba_cdn_id: str | None = None) -> dict:
    """Pull CDN cross-check + tick + return current phase summary."""
    if not bolt_key:
        return {"phase": ls_phase.UNKNOWN, "source": "no_bolt_key"}
    now = time.time()
    with _lock:
        r = _ensure(bolt_key)
        # CDN cross-check from cached PBP state
        if nba_cdn_id:
            try:
                import nba_feed
                pbp = nba_feed.get_pbp_phase(str(nba_cdn_id))
                if pbp:
                    snap = {
                        "period": pbp.get("period"),
                        "clock": pbp.get("clock"),
                    }
                    events = r.ingest_cdn(now, snap)
                    if events:
                        _pending_events[bolt_key].extend(events)
            except Exception:
                pass
        events = r.tick(now)
        if events:
            _pending_events[bolt_key].extend(events)
        return r.get_phase(now)


def get_phase(bolt_key: str) -> dict:
    if not bolt_key:
        return {"phase": ls_phase.UNKNOWN, "source": "no_bolt_key"}
    with _lock:
        r = _resolvers.get(bolt_key)
        if r is None:
            return {"phase": ls_phase.UNKNOWN, "source": "no_resolver"}
        return r.get_phase(time.time())


def drain_events(bolt_key: str, limit: int = 50) -> list:
    """Pop accumulated change events. Returns list of dicts."""
    with _lock:
        if bolt_key not in _pending_events:
            return []
        evs = _pending_events[bolt_key]
        out = evs[-limit:]
        _pending_events[bolt_key] = []
        return [{"kind": e.kind, "ts": e.ts, "payload": e.payload} for e in out]


def recent_events(bolt_key: str, limit: int = 20) -> list:
    """Peek at pending events without draining (for UI display)."""
    with _lock:
        evs = _pending_events.get(bolt_key, [])[-limit:]
        return [{"kind": e.kind, "ts": e.ts, "payload": e.payload} for e in evs]
