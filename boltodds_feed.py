"""
OddsBlaze dual-book poller — polls FD + DK NBA player props via REST.
Replaces BoltOdds WS with faster OddsBlaze REST polling.
Maintains same interface (player_odds, player_odds_detail, on_odds_change callback).
"""

import os
import json
import time
import re
import threading
import logging
import requests as _http
from collections import defaultdict

log = logging.getLogger("odds_feed")

OB_API_KEY = os.environ.get("OB_API_KEY", "")
OB_BASE_URL = "https://odds.oddsblaze.com"

# Configurable poll interval — set from app.py settings
poll_interval = 0.5

# Storage: nested by prop_type, keyed by "player|line"
# player_odds["points"]["player|line"] = best odds entry
# player_odds_detail["points"]["player|line"] = {fanduel: {...}, draftkings: {...}}
PROP_TYPES = ["points", "rebounds", "assists"]
player_odds = {pt: {} for pt in PROP_TYPES}
player_odds_detail = {pt: {} for pt in PROP_TYPES}
odds_lock = threading.Lock()

# OddsBlaze market names → our prop_type mapping
OB_MARKET_MAP = {
    "player-points": "points",
    "player-rebounds": "rebounds",
    "player-assists": "assists",
}
OB_MARKETS = ",".join(OB_MARKET_MAP.keys())
_last_update = 0
_message_count = 0

# Per-book stats
book_stats = defaultdict(lambda: {"messages": 0, "last_update": 0, "players": set()})

# Scoring event detection
_last_fair = {}
_last_fair_lock = threading.Lock()
scoring_events = []
_scoring_lock = threading.Lock()
MAX_SCORING_EVENTS = 200
_pending_events = {}
_SCORING_WINDOW = 5.0

# Callbacks
on_odds_change = None
on_suspension = None  # Called with (player, line, book) when a player disappears from a book

# Per-book last known keys — for suspension detection
_last_keys = {}  # {sportsbook: set(keys)}

# Per-book empty-poll counter — single empty = hiccup (absorb); 2+ = real outage
# (fire suspension for every prev_key so app pulls orders).
_empty_poll_count = {}  # {last_key: int}
EMPTY_POLL_TOLERANCE = 1  # absorb 1 empty, fire on 2nd consecutive


def american_to_implied(odds):
    if odds is None or odds == 0:
        return 0
    if odds > 0:
        return round(100 / (odds + 100) * 100, 2)
    else:
        return round(abs(odds) / (abs(odds) + 100) * 100, 2)


def _parse_name_and_line(outcome_name):
    m = re.match(r'^(.+?)\s+(Over|Under)\s+(\d+\.?\d*)', outcome_name, re.IGNORECASE)
    if m:
        return m.group(1).strip(), float(m.group(3)), m.group(2).lower()
    return None, None, None


def _make_key(player, line):
    return f"{player}|{line}"


def _detect_scoring_event(key, player, line, book_key, new_fair_no):
    """Detect when a book adjusts odds after a scoring play."""
    now = time.time()
    with _last_fair_lock:
        if key not in _last_fair:
            _last_fair[key] = {}
        old_fair = _last_fair[key].get(book_key)
        _last_fair[key][book_key] = new_fair_no

    if old_fair is None:
        return

    drop = old_fair - new_fair_no
    if drop < 2.0:
        with _scoring_lock:
            pending = _pending_events.get(key)
            if pending and drop > 0.5 and book_key != pending["trigger_book"]:
                delay_ms = round((now - pending["trigger_ts"]) * 1000)
                if not any(r["book"] == book_key for r in pending["reactions"]):
                    pending["reactions"].append({
                        "book": book_key, "delay_ms": delay_ms,
                        "old_fair": round(old_fair, 1), "new_fair": round(new_fair_no, 1),
                        "drop": round(old_fair - new_fair_no, 1),
                    })
        return

    with _scoring_lock:
        pending = _pending_events.get(key)
        if pending and (now - pending["trigger_ts"]) < _SCORING_WINDOW:
            if book_key != pending["trigger_book"]:
                delay_ms = round((now - pending["trigger_ts"]) * 1000)
                if not any(r["book"] == book_key for r in pending["reactions"]):
                    pending["reactions"].append({
                        "book": book_key, "delay_ms": delay_ms,
                        "old_fair": round(old_fair, 1), "new_fair": round(new_fair_no, 1),
                        "drop": round(drop, 1),
                    })
        else:
            _pending_events[key] = {
                "trigger_ts": now, "trigger_book": book_key,
                "trigger_old_fair": round(old_fair, 1),
                "trigger_new_fair": round(new_fair_no, 1),
                "trigger_drop": round(drop, 1),
                "player": player, "line": line, "reactions": [],
            }


def _flush_scoring_events():
    now = time.time()
    with _scoring_lock:
        to_remove = []
        for key, pending in _pending_events.items():
            if now - pending["trigger_ts"] >= _SCORING_WINDOW:
                event = {
                    "ts": pending["trigger_ts"],
                    "ts_display": time.strftime("%H:%M:%S", time.gmtime(pending["trigger_ts"])),
                    "player": pending["player"], "line": pending["line"],
                    "trigger_book": pending["trigger_book"],
                    "trigger_drop": pending["trigger_drop"],
                    "trigger_old_fair": pending["trigger_old_fair"],
                    "trigger_new_fair": pending["trigger_new_fair"],
                    "reactions": sorted(pending["reactions"], key=lambda r: r["delay_ms"]),
                    "books_reacted": len(pending["reactions"]),
                }
                scoring_events.append(event)
                if len(scoring_events) > MAX_SCORING_EVENTS:
                    del scoring_events[:len(scoring_events) - MAX_SCORING_EVENTS]
                to_remove.append(key)
                log.debug(f"SCORING_EVENT: {pending['player']} {pending['line']}+ | "
                          f"first={pending['trigger_book']} (-{pending['trigger_drop']:.1f}) | "
                          f"{len(pending['reactions'])} books followed")
        for key in to_remove:
            del _pending_events[key]


# ---------------------------------------------------------------------------
# Generic OddsBlaze book poller
# ---------------------------------------------------------------------------

def _poll_book(sportsbook):
    """Poll one sportsbook from OddsBlaze for all prop types. Returns {prop_type: (overs, unders)}."""
    try:
        resp = _http.get(OB_BASE_URL, params={
            "key": OB_API_KEY,
            "sportsbook": sportsbook,
            "league": "nba",
            "market": OB_MARKETS,
        }, timeout=10)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        log.error(f"OB {sportsbook} poll error: {e}")
        return {}

    # Parse by prop type
    by_prop = {pt: ({}  , {}) for pt in PROP_TYPES}  # {prop_type: (overs, unders)}

    for event in data.get("events", []):
        for odd in event.get("odds", []):
            name = odd.get("name", "")
            market = odd.get("market", "")
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

            # Determine prop type from OB market field
            prop_type = None
            market_lower = market.lower()
            if "point" in market_lower:
                prop_type = "points"
            elif "rebound" in market_lower:
                prop_type = "rebounds"
            elif "assist" in market_lower:
                prop_type = "assists"
            if not prop_type:
                continue

            clean = name.replace("Under", "Over")
            m = re.match(r'^(.+?)\s+Over\s+(\d+\.?\d*)', clean, re.IGNORECASE)
            if not m:
                continue
            player = m.group(1).strip()
            line = float(m.group(2))
            key = _make_key(player, line)
            implied = american_to_implied(price)

            overs, unders = by_prop[prop_type]
            if is_over:
                overs[key] = {"player": player, "line": line, "odds": price, "implied_yes": implied}
            else:
                unders[key] = {"odds": price, "implied_no": implied}

    return by_prop


def _process_book(sportsbook, prop_type, overs, unders):
    """Process parsed book data for one prop type into player_odds_detail.
    Returns list of changes for Lane 1: [(player, line, old_fair_no, new_fair_no, prop_type)]."""
    global _last_update, _message_count
    now = time.time()
    _message_count += 1
    _last_update = now

    book_key = "fanduel" if sportsbook in ("fanduel", "fanatics") else sportsbook
    book_stats[book_key]["messages"] += 1
    book_stats[book_key]["last_update"] = now

    _flush_scoring_events()

    changed = []

    pt_odds = player_odds[prop_type]
    pt_detail = player_odds_detail[prop_type]

    with odds_lock:
        for key, over in overs.items():
            player = over["player"]
            line_val = over["line"]
            over_imp = over["implied_yes"]
            over_odds = over["odds"]
            under = unders.get(key)

            if under:
                under_imp = under["implied_no"]
                total = over_imp + under_imp
                if total > 0:
                    fair_yes = round(over_imp / total * 100, 2)
                    fair_no = round(under_imp / total * 100, 2)
                    vig = round(total - 100, 2)
                else:
                    fair_yes = over_imp
                    fair_no = round(100 - over_imp, 1)
                    vig = None
                under_odds = under.get("odds")
            else:
                fair_yes = over_imp
                fair_no = round(100 - over_imp, 1)
                vig = None
                under_odds = None

            old_detail = pt_detail.get(key, {}).get(book_key, {})
            old_fair_no = old_detail.get("fair_no", 0)

            if key not in pt_detail:
                pt_detail[key] = {}
            pt_detail[key][book_key] = {
                "odds": over_odds,
                "under_odds": under_odds,
                "implied_yes": over_imp,
                "implied_no": round(100 - over_imp, 1),
                "fair_yes": fair_yes,
                "fair_no": fair_no,
                "vig": vig,
                "has_under": under is not None,
                "line": line_val,
            }

            # Update best odds
            if key not in pt_odds or over_odds < pt_odds[key].get("odds", 99999):
                pt_odds[key] = {
                    "odds": over_odds, "under_odds": under_odds,
                    "implied_yes": over_imp, "implied_no": round(100 - over_imp, 1),
                    "fair_yes": fair_yes, "fair_no": fair_no,
                    "vig": vig, "has_under": under is not None,
                    "player": player, "line": line_val,
                    "book": book_key, "updated": now,
                }

            book_stats[book_key]["players"].add(player)
            _detect_scoring_event(key, player, line_val, book_key, fair_no)

            # Track changes for Lane 1
            if old_fair_no and abs(fair_no - old_fair_no) >= 1.0:
                changed.append((player, line_val, old_fair_no, fair_no, prop_type))

    return changed


def _poller_loop(sportsbook):
    """Background poller for one sportsbook. Polls all prop types in one request."""
    book_key = "fanduel" if sportsbook in ("fanduel", "fanatics") else sportsbook

    while True:
        try:
            by_prop = _poll_book(sportsbook)
            if by_prop:
                all_changed = []
                for prop_type, (overs, unders) in by_prop.items():
                    # Suspension detection per prop type
                    last_key = f"{book_key}:{prop_type}"
                    current_keys = set(overs.keys())
                    prev_keys = _last_keys.get(last_key, set())

                    # Glitch guard (2026-04-25, tightened 2026-04-25 b): single empty
                    # poll while prev had data = absorb (likely OB hiccup). Two
                    # consecutive empties = real outage — fire suspension for every
                    # prev_key so the app pulls all open orders on this book.
                    # Single-hiccup protection preserved; sustained outage no longer
                    # waits for fd_max_age_secs + heartbeat cycle.
                    if not current_keys and prev_keys:
                        cnt = _empty_poll_count.get(last_key, 0) + 1
                        _empty_poll_count[last_key] = cnt
                        if cnt <= EMPTY_POLL_TOLERANCE:
                            log.warning(
                                f"OB {sportsbook}/{prop_type}: 0 keys returned "
                                f"(had {len(prev_keys)}) empty#{cnt} — absorbing as glitch"
                            )
                            continue
                        log.error(
                            f"OB {sportsbook}/{prop_type}: 0 keys for {cnt} consecutive polls "
                            f"(had {len(prev_keys)}) — firing FULL-BOOK suspension"
                        )
                        if on_suspension:
                            for key in prev_keys:
                                parts = key.split("|")
                                player = parts[0] if parts else key
                                line = float(parts[1]) if len(parts) > 1 else 0
                                try:
                                    on_suspension(player, line, book_key, prop_type)
                                except Exception as e:
                                    log.error(f"suspension callback error: {e}")
                        _last_keys[last_key] = set()
                        continue

                    # Reset empty counter on any non-empty poll
                    if current_keys:
                        _empty_poll_count[last_key] = 0

                    if prev_keys:
                        disappeared = prev_keys - current_keys
                        if disappeared and on_suspension:
                            for key in disappeared:
                                parts = key.split("|")
                                player = parts[0] if parts else key
                                line = float(parts[1]) if len(parts) > 1 else 0
                                try:
                                    on_suspension(player, line, book_key, prop_type)
                                except Exception as e:
                                    log.error(f"suspension callback error: {e}")
                    _last_keys[last_key] = current_keys

                    changed = _process_book(sportsbook, prop_type, overs, unders)
                    all_changed.extend(changed)

                # Fire Lane 1 callbacks OUTSIDE lock
                if all_changed and on_odds_change:
                    for player, line_val, old_fn, new_fn, pt in all_changed:
                        try:
                            on_odds_change(player, line_val, old_fn, new_fn, pt)
                        except Exception as e:
                            log.error(f"{sportsbook} odds_change callback error: {e}")
        except Exception as e:
            log.error(f"OB {sportsbook} poller error: {e}")

        time.sleep(poll_interval)


# ---------------------------------------------------------------------------
# Public interface (same as before — drop-in replacement)
# ---------------------------------------------------------------------------

_started = False
# Keep _connected for backward compat with status checks
_connected = True  # REST polling is always "connected"


def _poller_loop_delayed(sportsbook, delay):
    """Start poller with an initial delay for staggering."""
    time.sleep(delay)
    _poller_loop(sportsbook)


def start():
    """Start OddsBlaze REST pollers for FD + DK, staggered by half-interval.
    FD fires at t=0, 0.5, 1.0...  DK fires at t=0.25, 0.75, 1.25...
    Result: fresh odds every 0.25s instead of both at once."""
    global _started
    if _started:
        return
    if not OB_API_KEY:
        log.warning("No OB_API_KEY set — odds pollers disabled")
        return
    _started = True

    threading.Thread(target=_poller_loop, args=("fanduel",), daemon=True).start()
    log.info(f"OddsBlaze FD poller started ({poll_interval}s)")

    # Offset DK by half the poll interval so FD and DK alternate
    offset = poll_interval / 2
    threading.Thread(target=_poller_loop_delayed, args=("draftkings", offset), daemon=True).start()
    log.info(f"OddsBlaze DK poller started ({poll_interval}s, offset {offset}s)")


def get_book_age(book_key):
    """Seconds since this book last produced data. Returns inf if never updated.
    Note: book_stats[book_key]["last_update"] is touched by _process_book, which
    is only called for prop_types with non-empty overs (the glitch guard in
    _poller_loop skips empty prop_types entirely). So this is effectively
    'time since this book had at least one prop_type with real data'."""
    last = book_stats.get(book_key, {}).get("last_update", 0)
    if not last:
        return float("inf")
    return time.time() - last


def get_player_odds(prop_type="points"):
    with odds_lock:
        return dict(player_odds.get(prop_type, {}))


def get_player_detail(prop_type="points"):
    with odds_lock:
        return dict(player_odds_detail.get(prop_type, {}))


def get_status():
    bs = {}
    for book, stats in book_stats.items():
        bs[book] = {
            "messages": stats["messages"],
            "players": len(stats["players"]),
            "last_update": stats["last_update"],
        }
    pt_counts = {pt: len(player_odds.get(pt, {})) for pt in PROP_TYPES}
    return {
        "connected": _connected,
        "last_update": _last_update,
        "message_count": _message_count,
        "player_count": sum(pt_counts.values()),
        "player_count_by_prop": pt_counts,
        "key_set": bool(OB_API_KEY),
        "books": bs,
    }


def get_latency_report():
    from collections import Counter
    first_mover = Counter()
    with _scoring_lock:
        events = list(scoring_events)
    for ev in events:
        first_mover[ev["trigger_book"]] += 1
    book_delays = defaultdict(list)
    for ev in events:
        for r in ev["reactions"]:
            book_delays[r["book"]].append(r["delay_ms"])
    avg_delays = {}
    for book, delays in book_delays.items():
        avg_delays[book] = {
            "avg_ms": round(sum(delays) / len(delays)),
            "min_ms": min(delays), "max_ms": max(delays),
            "count": len(delays),
        }
    return {
        "total_events": len(events),
        "first_mover": dict(first_mover.most_common()),
        "avg_reaction_delays": avg_delays,
        "recent_events": events[-20:],
    }
