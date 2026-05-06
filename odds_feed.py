"""
OddsBlaze integration — polls NBA player points props across multiple sportsbooks.
Converts American odds to implied probability and derives NO-side ceiling.
"""

import os
import re
import time
import threading
import logging
import requests
from concurrent.futures import ThreadPoolExecutor

log = logging.getLogger("nba_props")

# OddsBlaze config
OB_API_KEY = os.environ.get("OB_API_KEY", "")
OB_BASE_URL = "https://odds.oddsblaze.com"
OB_BOOKS = ["fanduel"]  # FD only for live — saves rate limit
OB_POLL_INTERVAL = 0.5  # 0.5s for live trading

# Player odds store: {player_name: {odds, implied_yes, implied_no, ceil_no, book, line, updated}}
player_odds = {}
# Per-book odds: {player_name: {book_key: {odds, implied_yes, implied_no, line}}}
player_odds_detail = {}
odds_lock = threading.Lock()
api_remaining = "?"
last_poll_time = None


def american_to_implied(odds):
    """Convert American odds to implied probability (0-100)."""
    if odds is None or odds == 0:
        return 0
    if odds > 0:
        return round(100 / (odds + 100) * 100, 1)
    else:
        return round(abs(odds) / (abs(odds) + 100) * 100, 1)


def implied_to_ceil_no(implied_yes, arb_pct=5):
    """Derive NO ceiling from YES implied probability.
    arb_pct: how many percentage points below the book's implied NO price to target.
    e.g., if book says NO implied = 60%, we target 55% (60 - 5) = 55c ceiling.
    """
    implied_no = 100 - implied_yes
    ceil = int(implied_no - arb_pct)
    return max(1, min(99, ceil))


def _parse_american(s):
    """Parse American odds string to int."""
    if not s:
        return None
    try:
        return int(str(s).replace("+", ""))
    except (ValueError, TypeError):
        return None


def _parse_player_and_line(name):
    """Parse player name and line from OddsBlaze name field.
    Input: 'LeBron James Over 24.5'
    Output: ('LeBron James', 24.5)
    """
    # Match pattern: "Player Name Over X.5"
    match = re.match(r'^(.+?)\s+Over\s+(\d+\.?\d*)', name, re.IGNORECASE)
    if match:
        player = match.group(1).strip()
        line = float(match.group(2))
        return player, line
    return name, None


# ---------------------------------------------------------------------------
# OddsBlaze
# ---------------------------------------------------------------------------

def _ob_fetch_book(book):
    """Fetch NBA Player Points props from one OddsBlaze sportsbook."""
    try:
        resp = requests.get(OB_BASE_URL, params={
            "key": OB_API_KEY,
            "sportsbook": book,
            "league": "nba",
            "market": "player-points",
        }, timeout=15)
        resp.raise_for_status()
        return book, resp.json()
    except Exception as e:
        log.error(f"OddsBlaze error ({book}): {e}")
        return book, {}


def _make_key(player, line):
    """Create a unique key for player + line combo."""
    return f"{player}|{line}"


def _ob_fetch_all_books():
    """Fetch Points props from all OddsBlaze sportsbooks in parallel.
    Returns {player|line: {odds, implied_yes, implied_no, ceil_no, book, line, player, updated}}.
    Keyed by player+line so each alt line gets its own entry.
    """
    with ThreadPoolExecutor(max_workers=len(OB_BOOKS)) as pool:
        results = dict(pool.map(lambda b: _ob_fetch_book(b), OB_BOOKS))

    players = {}
    detail = {}
    now = time.time()

    for book, data in results.items():
        # Merge fanduel + fanatics
        book_label = "FanDuel" if book in ("fanduel", "fanatics") else book.title()
        book_key = "fanduel" if book in ("fanduel", "fanatics") else book

        for event in data.get("events", []):
            # Skip future events — use 10am UTC as the day boundary
            event_date = event.get("date", "")
            if event_date:
                try:
                    from datetime import datetime, timezone, timedelta
                    event_dt = datetime.fromisoformat(event_date.replace("Z", "+00:00"))
                    now_utc = datetime.now(timezone.utc)
                    today_start = now_utc.replace(hour=10, minute=0, second=0, microsecond=0)
                    if now_utc.hour < 10:
                        today_start -= timedelta(days=1)
                    cutoff = today_start + timedelta(hours=24)
                    if event_dt >= cutoff:
                        continue
                except Exception:
                    pass

            # First pass: collect all over/under odds by player|line|book
            _book_overs = {}   # key -> {odds, implied_yes}
            _book_unders = {}  # key -> {odds, implied_no}
            for odd in event.get("odds", []):
                name = odd.get("name", "")
                price = _parse_american(odd.get("price", ""))
                if price is None:
                    continue

                is_over = "Over" in name
                is_under = "Under" in name
                if not is_over and not is_under:
                    continue

                # Parse player and line from either Over or Under name
                clean = name.replace("Under", "Over")  # normalize for parsing
                player, line = _parse_player_and_line(clean)
                if line is None:
                    continue

                key = _make_key(player, line)
                implied = american_to_implied(price)

                if is_over:
                    _book_overs[key] = {"odds": price, "implied_yes": implied, "player": player, "line": line}
                elif is_under:
                    _book_unders[key] = {"odds": price, "implied_no": implied}

            # Second pass: build entries with 2-way devig when both sides available
            for key, over in _book_overs.items():
                player = over["player"]
                line = over["line"]
                over_imp = over["implied_yes"]
                over_odds = over["odds"]

                under = _book_unders.get(key)
                if under:
                    # 2-way devig: strip vig proportionally
                    under_imp = under["implied_no"]
                    total_imp = over_imp + under_imp
                    if total_imp > 0:
                        fair_yes = round(over_imp / total_imp * 100, 2)
                        fair_no = round(under_imp / total_imp * 100, 2)
                        vig = round(total_imp - 100, 2)
                    else:
                        fair_yes = over_imp
                        fair_no = round(100 - over_imp, 1)
                        vig = None
                    under_odds = under.get("odds")
                else:
                    # No under — raw implied only
                    fair_yes = over_imp
                    fair_no = round(100 - over_imp, 1)
                    vig = None
                    under_odds = None

                # Store per-book detail
                if key not in detail:
                    detail[key] = {}
                detail[key][book_key] = {
                    "odds": over_odds,
                    "under_odds": under_odds,
                    "implied_yes": over_imp,
                    "implied_no": round(100 - over_imp, 1),
                    "fair_yes": fair_yes,
                    "fair_no": fair_no,
                    "vig": vig,
                    "has_under": under is not None,
                    "book_label": book_label,
                    "line": line,
                }

                # Keep best per player+line
                if key not in players or over_odds < players[key]["odds"]:
                    players[key] = {
                        "odds": over_odds,
                        "under_odds": under_odds,
                        "implied_yes": over_imp,
                        "implied_no": round(100 - over_imp, 1),
                        "fair_yes": fair_yes,
                        "fair_no": fair_no,
                        "vig": vig,
                        "has_under": under is not None,
                        "ceil_no": implied_to_ceil_no(fair_yes if under else over_imp),
                        "book": book_label,
                        "line": line,
                        "player": player,
                        "updated": now,
                    }

    # Store per-book detail globally
    with odds_lock:
        player_odds_detail.update(detail)

    return players


def _ob_fetch_events():
    """Fetch NBA events from OddsBlaze (uses DK as reference for game list)."""
    try:
        resp = requests.get(OB_BASE_URL, params={
            "key": OB_API_KEY,
            "sportsbook": "draftkings",
            "league": "nba",
        }, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        events = []
        for event in data.get("events", []):
            teams = event.get("teams", {})
            events.append({
                "id": event.get("id", ""),
                "home_team": teams.get("home", {}).get("name", "?"),
                "away_team": teams.get("away", {}).get("name", "?"),
                "commence_time": event.get("date", ""),
            })
        return events
    except Exception as e:
        log.error(f"OddsBlaze events error: {e}")
        return []


# ---------------------------------------------------------------------------
# Public interface
# ---------------------------------------------------------------------------

def refresh_odds():
    """Fetch latest odds — OddsBlaze all books."""
    if OB_API_KEY:
        return _ob_fetch_all_books()
    return {}


def get_player_odds():
    """Get current odds snapshot."""
    with odds_lock:
        return dict(player_odds)


def get_player_detail():
    """Get per-book odds detail."""
    with odds_lock:
        return dict(player_odds_detail)


def get_all_odds():
    """Get both player_odds and player_odds_detail."""
    with odds_lock:
        return dict(player_odds), dict(player_odds_detail)


def get_status():
    """Get odds feed status."""
    return {
        "last_poll": last_poll_time,
        "player_count": len(player_odds),
        "api_remaining": api_remaining,
        "ob_key_set": bool(OB_API_KEY),
    }


def debug_raw_fetch():
    """Fetch raw OddsBlaze response for debugging — shows what markets/names come back."""
    if not OB_API_KEY:
        return {"error": "No OB_API_KEY set"}
    results = {}
    # Try multiple market filters to find player props
    for market_filter in ["Points", "Player Points", "Player", "Pts", ""]:
        try:
            params = {
                "key": OB_API_KEY,
                "sportsbook": "draftkings",
                "league": "nba",
            }
            if market_filter:
                params["market_contains"] = market_filter
            resp = requests.get(OB_BASE_URL, params=params, timeout=15)
            resp.raise_for_status()
            data = resp.json()
            events = data.get("events", [])
            # Collect all unique market names
            all_markets = set()
            sample_odds = []
            for ev in events:
                for o in ev.get("odds", []):
                    all_markets.add(o.get("market", ""))
                    if len(sample_odds) < 20:
                        sample_odds.append({
                            "market": o.get("market"),
                            "name": o.get("name"),
                            "price": o.get("price"),
                        })
            results[market_filter or "(no filter)"] = {
                "events": len(events),
                "unique_markets": sorted(all_markets),
                "sample_odds": sample_odds[:10],
            }
        except Exception as e:
            results[market_filter or "(no filter)"] = {"error": str(e)}
    return results


def _poll_loop_ob():
    """OddsBlaze poller — fetches all books for all games.
    Replaces stale odds: players not in the fresh response get removed.
    """
    global last_poll_time
    while True:
        try:
            players = _ob_fetch_all_books()
            with odds_lock:
                # Track which players disappeared
                old_keys = set(player_odds.keys())
                new_keys = set(players.keys())
                dropped = old_keys - new_keys
                if dropped:
                    for name in dropped:
                        player_odds.pop(name, None)
                        player_odds_detail.pop(name, None)
                    log.info(f"OB poll: {len(dropped)} players dropped (no odds)")
                player_odds.update(players)
            last_poll_time = time.time()
            log.info(f"OB poll: {len(players)} players updated")
        except Exception as e:
            log.error(f"OB poll error (will retry): {e}")
        time.sleep(OB_POLL_INTERVAL)


_poller_started = False


def start_poller():
    """Start background odds polling thread (only once)."""
    global _poller_started
    if _poller_started:
        return
    _poller_started = True
    if OB_API_KEY:
        t = threading.Thread(target=_poll_loop_ob, daemon=True)
        t.start()
        log.info(f"OddsBlaze poller started, interval={OB_POLL_INTERVAL}s, books={len(OB_BOOKS)}")
        return
    _poller_started = False
    log.warning("No OB_API_KEY set — odds poller disabled")
