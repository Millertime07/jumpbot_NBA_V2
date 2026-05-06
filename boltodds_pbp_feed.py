"""boltodds_pbp_feed.py — Bolt PBP WebSocket client for v2.

Subscribes to /api/playbyplay for NBA games and maintains a per-game
ring buffer of recent play events. Used by phase_resolver.py to detect
fouls, free_throws_awarded, timeouts, goals, and period boundaries
faster than NBA CDN.

Bolt's official PBP play_info types observed (NBA):
  possession, attempt_missed, goal, foul, rebound, turnover_basket,
  free_throws_awarded, steal_basket, timeout, periodstart,
  block, player_disqualified, won_jump_ball, videoreview, periodscore

Concurrency: Pro tier = 2 PBP WS slots per key. Don't run another PBP
client (e.g., boltodds_nba_monitor) on the same key during NBA hours
without that headroom.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import threading
import time
from collections import deque
from datetime import datetime

import websockets

log = logging.getLogger("bolt_pbp")

_DEFAULT_API_KEY = "0c97fafd-3455-484a-9ba5-16f0a100a457"
API_KEY = os.environ.get("BOLT_API_KEY") or os.environ.get("BOLT_KEY") or _DEFAULT_API_KEY
PBP_WS_URL = f"wss://spro.agency/api/playbyplay?key={API_KEY}"

# State per bolt-game-key
_events = {}              # bolt_key → deque[(ts, payload)] (most recent 200 events)
_events_lock = threading.Lock()
_active_keys: set = set()
_active_lock = threading.Lock()

_ws_thread_started = False
_ws_started_lock = threading.Lock()
_connected = False
_connected_lock = threading.Lock()

MAX_EVENTS_PER_GAME = 200


def is_connected() -> bool:
    with _connected_lock:
        return _connected


def _set_connected(v: bool):
    global _connected
    with _connected_lock:
        _connected = v


def subscribe(bolt_key: str):
    """Add a game to the PBP subscription set. Auto-starts the WS thread."""
    if not bolt_key:
        return
    with _active_lock:
        if bolt_key in _active_keys:
            return
        _active_keys.add(bolt_key)
    with _events_lock:
        _events.setdefault(bolt_key, deque(maxlen=MAX_EVENTS_PER_GAME))
    _ensure_ws_thread()


def unsubscribe(bolt_key: str):
    if not bolt_key:
        return
    with _active_lock:
        _active_keys.discard(bolt_key)


def get_recent_events(bolt_key: str, since_ts: float = None, types: tuple = None):
    """Return recent PBP events for a game.

    since_ts: only events with ts >= since_ts (default: all in buffer)
    types: filter to specific play_info types (e.g., ('foul', 'goal'))

    Each event: {"ts": float, "type": str, "team": str, "player": str|None,
                 "scorer": str|None, "raw": dict}
    """
    with _events_lock:
        buf = _events.get(bolt_key, deque())
        out = list(buf)  # snapshot
    if since_ts is not None:
        out = [e for e in out if e["ts"] >= since_ts]
    if types:
        out = [e for e in out if e.get("type") in types]
    return out


def get_status():
    with _events_lock:
        per_game_counts = {k: len(v) for k, v in _events.items()}
    with _active_lock:
        active = list(_active_keys)
    return {
        "connected": is_connected(),
        "active_keys": active,
        "events_buffered": per_game_counts,
        "api_key_prefix": API_KEY[:8],
    }


def _normalize_event(action: str, payload: dict) -> dict | None:
    """Flatten Bolt PBP payload into a normalized event record we cache.
    Returns None for non-data actions (ping/connected/error/etc)."""
    if action != "new_play":
        return None
    pi = payload.get("play_info") or []
    if not pi or not isinstance(pi[0], dict):
        return None
    info = pi[0]
    typ = info.get("type")
    if not typ:
        return None
    # Pull scorer/player names if present
    scorer = (info.get("scorer") or {}).get("name") if isinstance(info.get("scorer"), dict) else None
    player = (info.get("player") or {}).get("name") if isinstance(info.get("player"), dict) else None
    fouled = (info.get("fouled") or {}).get("name") if isinstance(info.get("fouled"), dict) else None
    fouling = (info.get("fouling") or {}).get("name") if isinstance(info.get("fouling"), dict) else None
    awarded = info.get("awarded")  # int — number of FTs awarded
    return {
        "ts": time.time(),
        "type": typ,
        "team": info.get("team"),
        "points": info.get("points"),
        "scorer": scorer,
        "player": player,
        "fouled": fouled,
        "fouling": fouling,
        "awarded": awarded,
        "seconds": info.get("seconds"),
        "score": payload.get("score"),  # outer-payload score: {"home": int, "away": int}
        "league": payload.get("league"),
        "raw": info,
    }


async def _pbp_loop():
    last_subscribed: set = set()
    while True:
        try:
            async with websockets.connect(PBP_WS_URL, max_size=None,
                                          ping_interval=20, ping_timeout=10) as ws:
                ack = await ws.recv()
                _set_connected(True)
                log.info(f"Bolt PBP connected: {str(ack)[:160]}")
                last_subscribed = set()
                while True:
                    # Sync subscriptions if changed
                    with _active_lock:
                        cur = set(_active_keys)
                    if cur != last_subscribed:
                        if cur:
                            await ws.send(json.dumps({
                                "action": "subscribe",
                                "filters": {"games": list(cur)},
                            }))
                            log.info(f"Bolt PBP subscribed to {len(cur)} games: {sorted(cur)}")
                        last_subscribed = cur

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
                        action = it.get("action", "?")
                        if action == "ping":
                            continue
                        if action in ("subscription_updated", "socket_connected", "connected"):
                            log.info(f"Bolt PBP {action}: {str(it)[:240]}")
                            continue
                        if action == "error":
                            log.warning(f"Bolt PBP error: {it.get('message') or it.get('error')}")
                            continue
                        # `event` field carries the bolt_key on this feed
                        bolt_key = it.get("event") or it.get("game") or ""
                        ev = _normalize_event(action, it)
                        if ev and bolt_key:
                            with _events_lock:
                                buf = _events.setdefault(bolt_key, deque(maxlen=MAX_EVENTS_PER_GAME))
                                buf.append(ev)
                            log.info(f"Bolt PBP play: {ev['type']} bolt_key={bolt_key[:40]}")
                            # Push to unified clock_state_feed (fastest path)
                            try:
                                import clock_state_feed
                                clock_state_feed.on_pbp_event(bolt_key, ev)
                            except Exception as _e:
                                log.debug(f"clock_state_feed pbp err: {_e}")
                        else:
                            log.info(f"Bolt PBP unhandled action={action} keys={list(it.keys())[:8]}")
        except Exception as e:
            _set_connected(False)
            log.warning(f"Bolt PBP WS error: {e}; reconnect in 5s")
            await asyncio.sleep(5)


def _ensure_ws_thread():
    global _ws_thread_started
    with _ws_started_lock:
        if _ws_thread_started:
            return
        _ws_thread_started = True

    def _run():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.create_task(_pbp_loop())
        loop.run_forever()

    threading.Thread(target=_run, daemon=True, name="bolt-pbp").start()
    log.info("Bolt PBP thread started")
