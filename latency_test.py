"""
Latency comparison tool — 3 sources:
  1. BoltOdds WS (FD)
  2. OddsBlaze REST 0.5s (FD)
  3. OddsBlaze REST 0.5s (DK)

Also logs FD vs DK move timing — which book moves first on each player.

Usage: python latency_test.py
"""

import os
import re
import json
import time
import threading
import asyncio
import websockets
import requests
from collections import defaultdict
from datetime import datetime, timezone

BOLT_KEY = os.environ.get("BOLT_KEY", "0c97fafd-3455-484a-9ba5-16f0a100a457")
OB_KEY = os.environ.get("OB_API_KEY", "90618d6b-8ac1-492f-b2c8-ef518e418d92")
BOLT_WS = "wss://spro.agency/api"
OB_URL = "https://odds.oddsblaze.com"

# Per-source state: {player|line: fair_no}
_bolt_fd_state = {}
_ob_fd_state = {}
_ob_dk_state = {}
_state_lock = threading.Lock()

# ---------------------------------------------------------------------------
# Feed latency tracking (BOLT vs OB for FD)
# ---------------------------------------------------------------------------
_feed_pending = {}  # {key: {source, ts, old_fair, new_fair}}
_feed_events = []
_MATCH_WINDOW = 5.0

_feed_stats = {
    "bolt_first": 0, "ob_first": 0,
    "bolt_only": 0, "ob_only": 0,
    "bolt_avg_ms": [], "ob_avg_ms": [],
    "bolt_msgs": 0, "ob_fd_polls": 0,
}

# ---------------------------------------------------------------------------
# Book speed tracking (FD vs DK via OddsBlaze)
# ---------------------------------------------------------------------------
_book_pending = {}  # {key: {book, ts, old_fair, new_fair}}
_book_events = []

_book_stats = {
    "fd_first": 0, "dk_first": 0,
    "fd_only": 0, "dk_only": 0,
    "fd_avg_ms": [], "dk_avg_ms": [],
    "ob_dk_polls": 0,
}

_events_lock = threading.Lock()


def american_to_implied(odds):
    if odds is None or odds == 0:
        return 0
    if odds > 0:
        return round(100 / (odds + 100) * 100, 2)
    else:
        return round(abs(odds) / (abs(odds) + 100) * 100, 2)


def _make_key(player, line):
    return f"{player}|{line}"


def _log_feed_move(player, line, source, old_fair, new_fair):
    """Track BOLT vs OB_FD latency."""
    now = time.time()
    key = _make_key(player, line)

    with _events_lock:
        pending = _feed_pending.get(key)
        if pending and pending["source"] != source and (now - pending["ts"]) < _MATCH_WINDOW:
            lead_ms = round((now - pending["ts"]) * 1000)
            first = pending["source"]
            second = source

            if first == "BOLT":
                _feed_stats["bolt_first"] += 1
                _feed_stats["bolt_avg_ms"].append(lead_ms)
            else:
                _feed_stats["ob_first"] += 1
                _feed_stats["ob_avg_ms"].append(lead_ms)

            drop = round(old_fair - new_fair, 1)
            direction = "DROP" if drop > 0 else "RISE"
            ts_str = datetime.now(timezone.utc).strftime("%H:%M:%S.%f")[:-3]
            print(f"  {ts_str}  FEED  {player} {line}  {direction} {abs(drop):.1f}  "
                  f"1st={first} 2nd={second} lead={lead_ms}ms")
            del _feed_pending[key]
        else:
            _feed_pending[key] = {"source": source, "ts": now, "old_fair": old_fair, "new_fair": new_fair}


def _log_book_move(player, line, book, old_fair, new_fair):
    """Track FD vs DK book speed (both via OddsBlaze REST)."""
    now = time.time()
    key = _make_key(player, line)

    with _events_lock:
        pending = _book_pending.get(key)
        if pending and pending["book"] != book and (now - pending["ts"]) < _MATCH_WINDOW:
            lead_ms = round((now - pending["ts"]) * 1000)
            first = pending["book"]
            second = book
            # Check moves are same direction
            first_drop = pending["old_fair"] - pending["new_fair"]
            second_drop = old_fair - new_fair
            if (first_drop > 0) != (second_drop > 0):
                # Different directions — not the same event, skip
                _book_pending[key] = {"book": book, "ts": now, "old_fair": old_fair, "new_fair": new_fair}
                return

            if first == "FD":
                _book_stats["fd_first"] += 1
                _book_stats["fd_avg_ms"].append(lead_ms)
            else:
                _book_stats["dk_first"] += 1
                _book_stats["dk_avg_ms"].append(lead_ms)

            drop = round(old_fair - new_fair, 1)
            direction = "DROP" if drop > 0 else "RISE"
            ts_str = datetime.now(timezone.utc).strftime("%H:%M:%S.%f")[:-3]
            print(f"  {ts_str}  BOOK  {player} {line}  {direction} {abs(drop):.1f}  "
                  f"1st={first} 2nd={second} lead={lead_ms}ms")

            _book_events.append({
                "ts": now, "player": player, "line": line,
                "first": first, "second": second, "lead_ms": lead_ms,
                "direction": direction, "drop": abs(drop),
            })
            del _book_pending[key]
        else:
            _book_pending[key] = {"book": book, "ts": now, "old_fair": old_fair, "new_fair": new_fair}


def _flush_pending():
    """Expire unmatched pending moves."""
    now = time.time()
    with _events_lock:
        for store, stats, label in [
            (_feed_pending, _feed_stats, "FEED"),
            (_book_pending, _book_stats, "BOOK"),
        ]:
            expired = []
            for key, p in store.items():
                if now - p["ts"] >= _MATCH_WINDOW:
                    expired.append(key)
                    src = p.get("source") or p.get("book")
                    if label == "FEED":
                        if src == "BOLT":
                            stats["bolt_only"] += 1
                        else:
                            stats["ob_only"] += 1
                    else:
                        if src == "FD":
                            stats["fd_only"] += 1
                        else:
                            stats["dk_only"] += 1
            for key in expired:
                del store[key]


# ---------------------------------------------------------------------------
# BoltOdds WS (FD only)
# ---------------------------------------------------------------------------

def _bolt_process(outcomes, sportsbook):
    if sportsbook not in ("fanduel", "fanatics"):
        return
    _feed_stats["bolt_msgs"] += 1

    overs = {}
    unders = {}
    for outcome_key, data in outcomes.items():
        odds_str = data.get("odds")
        if not odds_str or odds_str == '':
            continue
        try:
            odds = int(odds_str)
        except (ValueError, TypeError):
            continue

        name = data.get("outcome_target", "")
        ou = data.get("outcome_over_under", "")
        line_str = data.get("outcome_line")

        if not name or not ou or not line_str:
            m = re.match(r'^(.+?)\s+(Over|Under)\s+(\d+\.?\d*)', outcome_key, re.IGNORECASE)
            if not m:
                continue
            name = m.group(1).strip()
            ou = m.group(2).lower()
            line_str = m.group(3)
        else:
            ou = ou.lower()

        try:
            line = float(line_str)
        except (ValueError, TypeError):
            continue

        implied = american_to_implied(odds)
        key = _make_key(name, line)
        if ou == "over":
            overs[key] = {"player": name, "line": line, "implied_yes": implied}
        elif ou == "under":
            unders[key] = {"implied_no": implied}

    with _state_lock:
        for key, over in overs.items():
            over_imp = over["implied_yes"]
            under = unders.get(key)
            if under:
                total = over_imp + under["implied_no"]
                fair_no = round(under["implied_no"] / total * 100, 2) if total > 0 else round(100 - over_imp, 1)
            else:
                fair_no = round(100 - over_imp, 1)

            old = _bolt_fd_state.get(key, 0)
            _bolt_fd_state[key] = fair_no
            if old and fair_no != old:
                _log_feed_move(over["player"], over["line"], "BOLT", old, fair_no)


async def _bolt_ws():
    uri = f"{BOLT_WS}?key={BOLT_KEY}"
    while True:
        try:
            async with websockets.connect(uri, max_size=None) as ws:
                ack = await ws.recv()
                print(f"[BOLT] Connected: {ack}")
                sub = {
                    "action": "subscribe",
                    "filters": {
                        "sports": ["NBA"],
                        "sportsbooks": ["fanduel"],
                        "markets": ["Points"],
                    }
                }
                await ws.send(json.dumps(sub))
                print("[BOLT] Subscribed: NBA / FanDuel / Points")
                while True:
                    raw = await ws.recv()
                    try:
                        messages = json.loads(raw)
                    except json.JSONDecodeError:
                        continue
                    if not isinstance(messages, list):
                        messages = [messages]
                    for msg in messages:
                        action = msg.get("action", "")
                        if action == "ping":
                            continue
                        if action in ("initial_state", "game_update", "line_update"):
                            data = msg.get("data", {})
                            sportsbook = data.get("sportsbook", "fanduel")
                            outcomes = data.get("outcomes", {})
                            if outcomes:
                                _bolt_process(outcomes, sportsbook)
                    _flush_pending()
        except websockets.ConnectionClosed:
            print("[BOLT] Disconnected, reconnecting in 3s...")
            await asyncio.sleep(3)
        except Exception as e:
            print(f"[BOLT] Error: {e}, reconnecting in 3s...")
            await asyncio.sleep(3)


def _run_bolt():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(_bolt_ws())


# ---------------------------------------------------------------------------
# OddsBlaze REST pollers (FD + DK, both 0.5s)
# ---------------------------------------------------------------------------

def _ob_parse(data):
    """Parse OddsBlaze response into {key: {player, line, fair_no}}."""
    overs = {}
    unders = {}
    for event in data.get("events", []):
        for odd in event.get("odds", []):
            name = odd.get("name", "")
            price_str = odd.get("price", "")
            if not price_str:
                continue
            try:
                price = int(str(price_str).replace("+", ""))
            except (ValueError, TypeError):
                continue
            is_over = "Over" in name
            is_under = "Under" in name
            if not is_over and not is_under:
                continue
            clean = name.replace("Under", "Over")
            m = re.match(r'^(.+?)\s+Over\s+(\d+\.?\d*)', clean, re.IGNORECASE)
            if not m:
                continue
            player = m.group(1).strip()
            line = float(m.group(2))
            key = _make_key(player, line)
            implied = american_to_implied(price)
            if is_over:
                overs[key] = {"player": player, "line": line, "implied_yes": implied}
            else:
                unders[key] = {"implied_no": implied}

    results = {}
    for key, over in overs.items():
        over_imp = over["implied_yes"]
        under = unders.get(key)
        if under:
            total = over_imp + under["implied_no"]
            fair_no = round(under["implied_no"] / total * 100, 2) if total > 0 else round(100 - over_imp, 1)
        else:
            fair_no = round(100 - over_imp, 1)
        results[key] = {"player": over["player"], "line": over["line"], "fair_no": fair_no}
    return results


def _ob_poll_fd():
    """Poll OddsBlaze FD 0.5s."""
    while True:
        try:
            resp = requests.get(OB_URL, params={
                "key": OB_KEY, "sportsbook": "fanduel",
                "league": "nba", "market": "player-points",
            }, timeout=10)
            resp.raise_for_status()
            results = _ob_parse(resp.json())
            _feed_stats["ob_fd_polls"] += 1

            with _state_lock:
                for key, r in results.items():
                    old = _ob_fd_state.get(key, 0)
                    _ob_fd_state[key] = r["fair_no"]
                    if old and r["fair_no"] != old:
                        _log_feed_move(r["player"], r["line"], "OB_FD", old, r["fair_no"])
                        # Also feed into book comparison
                        _log_book_move(r["player"], r["line"], "FD", old, r["fair_no"])
            _flush_pending()
        except Exception as e:
            print(f"[OB_FD] Poll error: {e}")
        time.sleep(0.5)


def _ob_poll_dk():
    """Poll OddsBlaze DK 0.5s."""
    while True:
        try:
            resp = requests.get(OB_URL, params={
                "key": OB_KEY, "sportsbook": "draftkings",
                "league": "nba", "market": "player-points",
            }, timeout=10)
            resp.raise_for_status()
            results = _ob_parse(resp.json())
            _book_stats["ob_dk_polls"] += 1

            with _state_lock:
                for key, r in results.items():
                    old = _ob_dk_state.get(key, 0)
                    _ob_dk_state[key] = r["fair_no"]
                    if old and r["fair_no"] != old:
                        _log_book_move(r["player"], r["line"], "DK", old, r["fair_no"])
            _flush_pending()
        except Exception as e:
            print(f"[OB_DK] Poll error: {e}")
        time.sleep(0.5)


# ---------------------------------------------------------------------------
# Stats printer
# ---------------------------------------------------------------------------

def _avg(lst):
    return round(sum(lst) / len(lst)) if lst else 0


def _print_stats():
    while True:
        time.sleep(30)
        ft = _feed_stats
        bt = _book_stats
        total_feed = ft["bolt_first"] + ft["ob_first"] + ft["bolt_only"] + ft["ob_only"]
        total_book = bt["fd_first"] + bt["dk_first"] + bt["fd_only"] + bt["dk_only"]

        print(f"\n{'='*70}")
        print(f"  LATENCY REPORT  |  {datetime.now(timezone.utc).strftime('%H:%M:%S')} UTC")
        print(f"{'='*70}")
        print(f"  --- FEED: BoltOdds WS vs OddsBlaze REST (FD) ---")
        print(f"  Total: {total_feed}")
        print(f"  BOLT first:  {ft['bolt_first']:>4}   avg lead: {_avg(ft['bolt_avg_ms'])}ms")
        print(f"  OB first:    {ft['ob_first']:>4}   avg lead: {_avg(ft['ob_avg_ms'])}ms")
        print(f"  BOLT only:   {ft['bolt_only']:>4}")
        print(f"  OB only:     {ft['ob_only']:>4}")
        print(f"  BOLT msgs:   {ft['bolt_msgs']:>4}   OB FD polls: {ft['ob_fd_polls']}")
        print(f"")
        print(f"  --- BOOK: FD vs DK (both OddsBlaze REST) ---")
        print(f"  Total: {total_book}")
        print(f"  FD first:    {bt['fd_first']:>4}   avg lead: {_avg(bt['fd_avg_ms'])}ms")
        print(f"  DK first:    {bt['dk_first']:>4}   avg lead: {_avg(bt['dk_avg_ms'])}ms")
        print(f"  FD only:     {bt['fd_only']:>4}")
        print(f"  DK only:     {bt['dk_only']:>4}")
        print(f"  OB DK polls: {bt['ob_dk_polls']}")
        print(f"{'='*70}\n")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("=" * 70)
    print("  LATENCY TEST v2")
    print("  Feed: BoltOdds WS vs OddsBlaze REST (FD)")
    print("  Book: FD vs DK (both OddsBlaze REST 0.5s)")
    print("  Logging ANY fair_no change")
    print("=" * 70)
    print()

    threading.Thread(target=_run_bolt, daemon=True).start()
    threading.Thread(target=_ob_poll_fd, daemon=True).start()
    threading.Thread(target=_ob_poll_dk, daemon=True).start()
    threading.Thread(target=_print_stats, daemon=True).start()

    print("[MAIN] All 3 feeds running. Ctrl+C to stop.\n")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n\nFinal report:")
        _print_stats()
