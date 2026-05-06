"""
OddsPapi WebSocket — NBA live clock detection.
Subscribes to the clocks channel to detect timeouts, dead balls, quarter breaks.
Exposes get_clock(fixture_id) for the trading engine.
"""

import os
import json
import time
import asyncio
import threading
import logging

try:
    import websockets
except ImportError:
    websockets = None

log = logging.getLogger("nba_props")

WS_URL = "wss://v5.oddspapi.io/ws"
OP_KEY = os.environ.get("OP_API_KEY", os.environ.get("ODDSPAPI_API_KEY", "")).strip()

# Clock state per fixture: {fixture_id: {clock dict, updated_at}}
_clocks = {}
_clocks_lock = threading.Lock()

# Connection state
_connected = False
_started = False
_server_epoch = None
_last_seen_ids = {}
_last_error = None
_ws_ref = None  # reference to active websocket for re-subscribing
_pending_fixture_ids = []  # fixture IDs to subscribe to

# Fixture ID mapping: {fixture_id: game_name} — set by app.py
fixture_map = {}
# WS-discovered fixtures: {fixture_id: {id, participant1, participant2, ...}}
_ws_fixtures = {}


def get_clock(fixture_id):
    """Get current clock state for a fixture.
    Returns {stopped, period, remaining, remaining_period, updated_at} or None."""
    with _clocks_lock:
        return _clocks.get(str(fixture_id))


def get_all_clocks():
    """Get all clock states. Returns dict copy."""
    with _clocks_lock:
        return dict(_clocks)


def is_timeout(fixture_id):
    """Check if the game clock is stopped (timeout/dead ball/quarter break)."""
    c = get_clock(fixture_id)
    if not c:
        return None  # unknown
    return c.get("stopped", None)


def is_connected():
    return _connected


def get_status():
    return {
        "connected": _connected,
        "fixtures_tracked": len(_clocks),
        "last_error": _last_error,
    }


async def _ws_loop():
    global _connected, _server_epoch, _last_error

    if not websockets:
        log.error("websockets package not installed — op_clocks disabled")
        return

    while True:
        try:
            log.info(f"OP Clocks: connecting to {WS_URL}...")
            async with websockets.connect(WS_URL, max_size=None, ping_interval=20, ping_timeout=10) as ws:
                _ws_ref = ws
                # Subscribe with specific fixture IDs if available, else broad sport filter
                fids = list(fixture_map.keys()) + list(_pending_fixture_ids)
                login = {
                    "type": "login",
                    "apiKey": OP_KEY,
                    "channels": ["clocks", "fixtures", "scores"],
                    "receiveType": "json",
                }
                if fids:
                    login["fixtureIds"] = fids
                    log.info(f"OP Clocks: subscribing to {len(fids)} specific fixtures")
                else:
                    login["sportIds"] = [11]  # fallback: all basketball
                if _server_epoch:
                    login["serverEpoch"] = _server_epoch
                if _last_seen_ids:
                    login["lastSeenId"] = dict(_last_seen_ids)

                await ws.send(json.dumps(login))
                log.info(f"OP Clocks: login sent ({'fixtures: ' + str(len(fids)) if fids else 'all basketball'})")

                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                    except json.JSONDecodeError:
                        continue

                    msg_type = msg.get("type", "")

                    if msg_type == "login_ok":
                        _connected = True
                        _last_error = None
                        epoch = msg.get("serverEpoch")
                        if epoch:
                            _server_epoch = epoch
                        log.info(f"OP Clocks: connected, serverEpoch={epoch}")
                        continue

                    if msg_type == "error":
                        _last_error = msg.get("message", str(msg))
                        log.error(f"OP Clocks error: {_last_error}")
                        continue

                    if msg_type == "snapshot_required":
                        log.warning("OP Clocks: snapshot_required — cursors stale")
                        _last_seen_ids.clear()
                        continue

                    if msg_type == "resume_complete":
                        log.info("OP Clocks: resume complete")
                        continue

                    # Track message IDs for resumption
                    channel = msg.get("channel", "")
                    msg_id = msg.get("id")
                    if msg_id and channel:
                        _last_seen_ids[channel] = msg_id

                    if channel == "clocks":
                        payload = msg.get("payload", {})
                        fid = str(payload.get("fixtureId", ""))
                        clock = payload.get("clock", {})
                        if fid and clock:
                            with _clocks_lock:
                                _clocks[fid] = {
                                    "stopped": clock.get("stopped"),
                                    "period": clock.get("currentPeriod"),
                                    "remaining": clock.get("remainingTime"),
                                    "remaining_period": clock.get("remainingTimeInPeriod"),
                                    "current_time": clock.get("currentTime"),
                                    "updated_at": time.time(),
                                }
                            # Log state changes
                            stopped = clock.get("stopped")
                            period = clock.get("currentPeriod", "?")
                            remaining = clock.get("remainingTimeInPeriod", "?")
                            game_name = fixture_map.get(fid, fid[:12])
                            if stopped:
                                log.debug(f"OP CLOCK: {game_name} STOPPED (timeout/dead ball) {period} {remaining}")
                            else:
                                log.debug(f"OP CLOCK: {game_name} RUNNING {period} {remaining}")

                    elif channel == "fixtures":
                        payload = msg.get("payload", {})
                        fid = str(payload.get("fixtureId", ""))
                        status = payload.get("status", {})
                        parts = payload.get("participants", {})
                        tourn = payload.get("tournament", {})
                        clock = payload.get("clock", {})
                        p1 = parts.get("participant1Name", "")
                        p2 = parts.get("participant2Name", "")
                        p1_short = parts.get("participant1ShortName", "")
                        p2_short = parts.get("participant2ShortName", "")
                        tournament = tourn.get("tournamentName", "") if isinstance(tourn, dict) else ""
                        if fid:
                            with _clocks_lock:
                                if fid not in _clocks:
                                    _clocks[fid] = {}
                                if status:
                                    _clocks[fid]["fixture_status"] = status.get("statusName", "")
                                    _clocks[fid]["is_live"] = status.get("live", False)
                                if clock:
                                    _clocks[fid]["stopped"] = clock.get("stopped")
                                    _clocks[fid]["period"] = clock.get("currentPeriod")
                                    _clocks[fid]["remaining_period"] = clock.get("remainingTimeInPeriod")
                                    _clocks[fid]["updated_at"] = time.time()
                                if p1 and p2:
                                    _clocks[fid]["participant1"] = p1
                                    _clocks[fid]["participant2"] = p2
                                    _clocks[fid]["p1_short"] = p1_short
                                    _clocks[fid]["p2_short"] = p2_short
                                    _clocks[fid]["tournament"] = tournament
                                    _ws_fixtures[fid] = {"id": fid, "participant1": p1, "participant2": p2,
                                                         "p1_short": p1_short, "p2_short": p2_short,
                                                         "name": f"{p1} vs {p2}", "tournament": tournament,
                                                         "is_live": status.get("live", False) if status else False}
                            if p1 and p2 and "NBA" in (tournament or "").upper():
                                log.info(f"OP WS fixture: {p1_short or p1} vs {p2_short or p2} (id={fid}, live={status.get('live', '?')})")

        except Exception as e:
            _connected = False
            _last_error = f"{type(e).__name__}: {str(e)[:100]}"
            log.warning(f"OP Clocks WS error: {e}")
            await asyncio.sleep(5)


def _run_ws():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(_ws_loop())


def subscribe_fixtures(fixture_ids):
    """Queue fixture IDs for subscription. Triggers reconnect to pick them up."""
    global _pending_fixture_ids, _ws_ref
    _pending_fixture_ids = list(fixture_ids)
    log.info(f"OP Clocks: queued {len(fixture_ids)} fixture IDs for subscription")
    # Force reconnect to re-login with new fixture IDs
    if _ws_ref:
        import asyncio
        try:
            asyncio.run_coroutine_threadsafe(
                _ws_ref.close(),
                asyncio.get_event_loop()
            )
        except Exception:
            pass  # will reconnect naturally


def get_ws_fixtures():
    """Get fixtures discovered via WS (no REST needed). Returns list of fixture dicts."""
    with _clocks_lock:
        return [f for f in _ws_fixtures.values() if "NBA" in (f.get("tournament", "") or "").upper()]


def find_nba_fixtures():
    """Find today's NBA fixtures from OddsPapi v5 REST API.
    Returns list of {id, name, participant1, participant2, status}."""
    if not OP_KEY:
        return []
    try:
        import requests
        resp = requests.get(
            "https://v5.oddspapi.io/en/fixtures/today",
            params={
                "apiKey": OP_KEY,
                "sportId": 11,  # basketball
            },
            timeout=15,
        )
        if resp.status_code != 200:
            log.warning(f"OP fixtures fetch failed: {resp.status_code}")
            return []
        data = resp.json()
        fixtures = data if isinstance(data, list) else data.get("data", data.get("fixtures", []))
        nba = []
        for f in fixtures:
            # v5 nests participants and tournament
            parts = f.get("participants", {})
            tournament_obj = f.get("tournament", {})
            tournament = (tournament_obj.get("tournamentName", "") if isinstance(tournament_obj, dict) else "")
            if "NBA" not in tournament.upper():
                continue
            p1 = parts.get("participant1Name", "?")
            p2 = parts.get("participant2Name", "?")
            p1s = parts.get("participant1ShortName", "")
            p2s = parts.get("participant2ShortName", "")
            fid = str(f.get("fixtureId", f.get("id", "")))
            status = f.get("status", {})
            nba.append({
                "id": fid,
                "name": f"{p1} vs {p2}",
                "participant1": p1,
                "participant2": p2,
                "p1_short": p1s,
                "p2_short": p2s,
                "status": status.get("statusName", "") if isinstance(status, dict) else str(status),
                "is_live": status.get("live", False) if isinstance(status, dict) else False,
            })
        if not nba and fixtures:
            # Log first fixture to debug structure
            log.info(f"OP fixtures: 0 NBA in {len(fixtures)} total. Sample keys: {list(fixtures[0].keys()) if fixtures else '?'}")
            # Try without NBA filter as fallback
            sample = fixtures[0]
            t_info = sample.get("tournament", sample.get("tournamentName", "?"))
            log.info(f"OP fixtures sample tournament: {t_info}")
        for g in nba:
            log.info(f"OP NBA fixture: {g['name']} (id={g['id']}, live={g['is_live']})")
        log.info(f"OP fixtures: found {len(nba)} NBA games out of {len(fixtures)} total")
        if not nba:
            # Try /fixtures/live as fallback
            try:
                resp2 = requests.get(
                    "https://v5.oddspapi.io/en/fixtures/live",
                    params={"apiKey": OP_KEY, "sportId": 11},
                    timeout=15,
                )
                if resp2.status_code == 200:
                    live_data = resp2.json()
                    live_fixtures = live_data if isinstance(live_data, list) else live_data.get("data", live_data.get("fixtures", []))
                    for f in live_fixtures:
                        parts = f.get("participants", {})
                        p1 = parts.get("participant1Name", f.get("participant1Name", "?"))
                        p2 = parts.get("participant2Name", f.get("participant2Name", "?"))
                        fid = str(f.get("fixtureId", f.get("id", "")))
                        nba.append({
                            "id": fid, "name": f"{p1} vs {p2}",
                            "participant1": p1, "participant2": p2,
                            "status": "Live", "is_live": True,
                        })
                    log.info(f"OP fixtures/live fallback: found {len(nba)} live basketball games")
            except Exception as e2:
                log.warning(f"OP fixtures/live fallback failed: {e2}")
        return nba
    except Exception as e:
        log.error(f"OP fixtures error: {e}")
        return []


def match_fixture_to_game(fixtures, game_name):
    """Match an OddsPapi fixture to a game by team name overlap.
    game_name is like 'MIN vs DEN' or 'ATL @ NYK'.
    Returns fixture_id or None."""
    if not fixtures or not game_name:
        return None
    # Extract team codes from game name
    parts = game_name.upper().replace("@", "VS").split("VS")
    if len(parts) < 2:
        return None
    team1 = parts[0].strip()[:3]
    team2 = parts[1].strip()[:3]

    # NBA team name mapping (abbreviation → common name fragments)
    team_names = {
        "ATL": "hawks", "BOS": "celtics", "BKN": "nets", "CHA": "hornets",
        "CHI": "bulls", "CLE": "cavaliers", "DAL": "mavericks", "DEN": "nuggets",
        "DET": "pistons", "GSW": "warriors", "HOU": "rockets", "IND": "pacers",
        "LAC": "clippers", "LAL": "lakers", "MEM": "grizzlies", "MIA": "heat",
        "MIL": "bucks", "MIN": "timberwolves", "NOP": "pelicans", "NYK": "knicks",
        "OKC": "thunder", "ORL": "magic", "PHI": "76ers", "PHX": "suns",
        "POR": "blazers", "SAC": "kings", "SAS": "spurs", "TOR": "raptors",
        "UTA": "jazz", "WAS": "wizards",
    }
    t1_name = team_names.get(team1, team1.lower())
    t2_name = team_names.get(team2, team2.lower())

    for f in fixtures:
        p1 = f["participant1"].lower()
        p2 = f["participant2"].lower()
        p1s = f.get("p1_short", "").lower()
        p2s = f.get("p2_short", "").lower()
        all_names = f"{p1} {p2} {p1s} {p2s}"
        if t1_name in all_names and t2_name in all_names:
            return f["id"]
    return None


def start():
    """Start the OddsPapi clocks WebSocket in a background thread.
    Disabled by default — set NBA_OP_CLOCKS_ENABLED=1 to opt in. NBA bot
    does not use clock signal in trading logic, so it stays off and avoids
    auth-spam when the OP key isn't valid for clocks.
    """
    global _started
    if _started:
        return
    if os.environ.get("NBA_OP_CLOCKS_ENABLED", "").strip().lower() not in ("1", "true", "yes"):
        log.info("OddsPapi clocks disabled (set NBA_OP_CLOCKS_ENABLED=1 to enable)")
        _started = True  # block subsequent calls
        return
    if not OP_KEY:
        log.warning("No OP_API_KEY set — OddsPapi clocks disabled")
        return
    _started = True
    threading.Thread(target=_run_ws, name="op_clocks_ws", daemon=True).start()
    log.info("OddsPapi clocks WS started (basketball)")
