"""
NBA game status feed — polls ESPN scoreboard for today's games.
Provides game status (Scheduled, Live, Final, Halftime) for smart mode.
No play-by-play needed for v1.
"""

import time
import threading
import logging
import re
import unicodedata
import requests
from datetime import datetime, timezone, timedelta

log = logging.getLogger("nba_props")

# Bolt LS feed for primary clock-state detection. Imported lazily to avoid
# circular import at module load time when boltodds_ls_feed isn't shipped.
try:
    import boltodds_ls_feed as _bolt_ls
except Exception:
    _bolt_ls = None

# Per-game Bolt LS key registry. Populated by app.py on add_game.
# Maps nba_game_id (str) -> bolt_key (str). Bolt LS uses keys like
# "Atlanta Hawks vs New York Knicks, 2026-04-30, 07".
_bolt_keys = {}
_bolt_keys_lock = threading.Lock()


def register_bolt_key(nba_game_id, bolt_key):
    """Register Bolt LS subscription key for a game. Pass None/empty to unregister."""
    with _bolt_keys_lock:
        if bolt_key:
            _bolt_keys[str(nba_game_id)] = bolt_key
        else:
            _bolt_keys.pop(str(nba_game_id), None)


def _get_bolt_key(nba_game_id):
    """Race-tolerant lookup: tries the passed ID directly, then walks
    the ESPN↔NBA-CDN mapping in both directions. Handles the boot race
    where Bolt subscribe registers under one ID before the PBP poller
    has resolved the corresponding mapping."""
    gid_str = str(nba_game_id)
    with _bolt_keys_lock:
        if gid_str in _bolt_keys:
            return _bolt_keys[gid_str]
        # Forward: passed ESPN ID, key registered under NBA CDN ID
        cdn_id = _nba_game_ids.get(gid_str)
        if cdn_id and str(cdn_id) in _bolt_keys:
            return _bolt_keys[str(cdn_id)]
        # Reverse: passed NBA CDN ID, key registered under ESPN ID
        for espn_id, mapped_cdn in _nba_game_ids.items():
            if str(mapped_cdn) == gid_str and str(espn_id) in _bolt_keys:
                return _bolt_keys[str(espn_id)]
    return None


def get_bolt_key(nba_game_id):
    """Public accessor for callers (UI endpoints, etc.)."""
    return _get_bolt_key(nba_game_id)

ESPN_SCOREBOARD = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard"
NBA_CDN_BOXSCORE = "https://cdn.nba.com/static/json/liveData/boxscore/boxscore_{}.json"
NBA_CDN_PBP = "https://cdn.nba.com/static/json/liveData/playbyplay/playbyplay_{}.json"
NBA_CDN_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Origin": "https://www.nba.com",
    "Referer": "https://www.nba.com/",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-site",
    "Connection": "keep-alive",
}
POLL_INTERVAL = 30  # seconds for lifecycle (Live/Final transitions)
SCORE_POLL_INTERVAL = 3  # seconds for score change detection (live games only)
STATS_POLL_INTERVAL = 5  # seconds for live player stats
PBP_POLL_INTERVAL = 2  # seconds for play-by-play (timeout detection)
CDN_STATUS_POLL_INTERVAL = 10  # seconds for NBA CDN scoreboard (primary status source)
NBA_CDN_SCOREBOARD = "https://cdn.nba.com/static/json/liveData/scoreboard/todaysScoreboard_00.json"

# Per-game last score change timestamp
# {game_id: {ts: float, last_score: int, period: int}}
last_score_change = {}
last_score_lock = threading.Lock()
_on_score_change = None  # Callback (game_id, total_score, period)

# Game state tracking
# {game_id: {status, away, home, away_score, home_score, startTime}}
game_states = {}
game_states_lock = threading.Lock()

# Game lifecycle: {game_id: "Scheduled" | "Live" | "Final" | "Halftime"}
game_lifecycle = {}
game_lifecycle_lock = threading.Lock()

# Live player stats from NBA boxscore
# {nba_game_id: {player_name: {points, rebounds, assists, minutes, ...}}}
player_stats = {}
player_stats_lock = threading.Lock()
_nba_game_ids = {}  # {our_game_id: nba_game_id} — mapping for boxscore lookups

# Play-by-play state for fast timeout detection
# {nba_game_id: {phase, period, clock, last_event, updated_at}}
pbp_state = {}
pbp_state_lock = threading.Lock()
_pbp_poller_started = False

# NBA CDN scoreboard status — primary source for Live/Final transitions
# (faster than ESPN, which lags real tipoff by 30-90s).
# {cdn_id: {status, updated_at}}  status: Scheduled|Live|Final
cdn_game_status = {}
cdn_game_status_lock = threading.Lock()
_cdn_status_poller_started = False

# On-court / bench tracking — phase (a): observe only, no trading impact.
# Authoritative source: boxscore `oncourt` field (5s). Fast incremental: PBP
# Substitution events (2s). Keyed by NBA CDN game id.
# Shape:
#   on_court_state[nba_game_id] = {
#     "home": {"tricode": "CLE",
#              "on_court": {personId: {"familyName": str, "fullName": str, "personId": int}},
#              "bench":    {personId: {...}}},
#     "away": {...},
#     "updated_at": float, "source": "boxscore"|"pbp"|"mixed",
#     "_sub_cursor": int,
#   }
# Personid-keyed (not name) so same-family pairs like OKC's Jalen + Jaylin
# Williams stay distinct. Name → personId resolution is handled by the
# player directory below.
on_court_state = {}
on_court_lock = threading.Lock()

# Player directory — name-to-personId resolution.
# Built incrementally from every boxscore fetch. _players_by_pid is the
# global roster; _game_name_to_pid is a per-game lookup that only contains
# unambiguous keys (within that game's 2 teams) — ambiguous keys are omitted
# so resolve() returns None rather than guess.
# Shape:
#   _players_by_pid[pid] = {fullName, familyName, firstName, teamTricode, gameId}
#   _game_name_to_pid[game_id] = {name_key: pid}  # only unique keys
_players_by_pid = {}
_game_name_to_pid = {}
_players_lock = threading.Lock()
# Manual alias overrides for known mismatches between feed names and NBA CDN.
# Keys are normalized (lowercase, no accents, no suffixes). Add entries as
# they surface in logs. Example: "nick claxton": "nicolas claxton".
_manual_aliases = {}
# De-dupe "unknown player" log spam — one warning per (game_id, name).
_resolve_misses = set()

# Callbacks
_on_game_start = None  # (game_id) — game went live (tipoff)
_on_game_end = None    # (game_id) — game went Final


def set_callbacks(on_game_start=None, on_game_end=None, on_score_change=None):
    global _on_game_start, _on_game_end, _on_score_change
    _on_game_start = on_game_start
    _on_game_end = on_game_end
    if on_score_change is not None:
        _on_score_change = on_score_change


def get_post_score_window(game_id, window_secs=10):
    """Return seconds since last score change for this game, or None if no recent score."""
    with last_score_lock:
        rec = last_score_change.get(str(game_id))
    if not rec:
        return None
    elapsed = time.time() - rec["ts"]
    return elapsed if elapsed < window_secs else None


def get_game_lifecycle(game_id):
    """Get current lifecycle state for a game."""
    with game_lifecycle_lock:
        return game_lifecycle.get(str(game_id))


def _parse_status(competition):
    """Parse ESPN competition status into our simplified status."""
    status_obj = competition.get("status", {})
    status_type = status_obj.get("type", {})
    state = status_type.get("state", "")
    detail = status_obj.get("type", {}).get("detail", "")
    short_detail = status_obj.get("type", {}).get("shortDetail", "")

    if state == "pre":
        return "Scheduled"
    elif state == "post":
        return "Final"
    elif state == "in":
        if "halftime" in detail.lower() or "halftime" in short_detail.lower():
            return "Halftime"
        return "Live"
    return "Scheduled"


def get_game_phase(game_id):
    """Get detailed game phase. Bolt LS is the PRIMARY clock-state authority
    (3-6s ahead of CDN on timeout-end transitions, validated 2026-04-30).
    NBA CDN PBP supplies the stoppage REASON (TIMEOUT vs QUARTER_BREAK vs
    HALFTIME) and the FINAL/PRE lifecycle.

    Returns one of: PRE, LIVE, TIMEOUT, QUARTER_BREAK, HALFTIME, FINAL,
    OVERTIME, FOUL_SHOT, UNKNOWN. When neither feed has recent state,
    returns UNKNOWN — whose phase_settings config defaults to neutral.

    Override matrix (only fires when Bolt LS data is fresh):
      Bolt running=True  + CDN in TIMEOUT/QUARTER_BREAK/HALFTIME → LIVE
        (timeout-end past-post protection: clock resumed before CDN saw it)
      Bolt running=False + CDN LIVE → no override
        (brief dead-balls don't warrant phase flap; CDN catches real timeouts)
    """
    nba_gid = _nba_game_ids.get(str(game_id)) or str(game_id)
    cdn_phase = "UNKNOWN"
    if nba_gid:
        pbp = get_pbp_phase(nba_gid)
        if pbp:
            cdn_phase = pbp["phase"]

    # Bolt LS clock-state override — OPT-IN via env var. Default OFF after
    # 2026-04-30 incident where Bolt's halftime detection wasn't reliable
    # and trading saw LIVE during halftime breaks. Re-enable with
    # BOLT_LS_OVERRIDE=1 only after offline validation confirms Bolt's
    # clock derivation is correct across all phase transitions.
    import os as _os
    if _os.environ.get("BOLT_LS_OVERRIDE") == "1" and _bolt_ls is not None:
        bolt_key = _get_bolt_key(nba_gid) or _get_bolt_key(game_id)
        if bolt_key:
            bolt_running = _bolt_ls.is_clock_running(bolt_key)
            if bolt_running is True and cdn_phase in ("TIMEOUT", "QUARTER_BREAK", "HALFTIME"):
                return "LIVE"

    return cdn_phase


def is_clock_running(game_id):
    """Check if game clock is actively running. Shortcut for get_game_phase() == LIVE."""
    phase = get_game_phase(game_id)
    return phase in ("LIVE", "OVERTIME")


def get_game_info(game_id):
    """Game info sourced from NBA CDN PBP only for phase/period/clock/detail.
    ESPN remains the source for team abbreviations and scores (those don't
    produce misleading phase reads). When PBP has no recent state, the
    live-play fields come back empty/UNKNOWN rather than being filled in
    with stale ESPN guesses like 'End of 1st Quarter' that disagreed with
    the actual PBP phase."""
    with game_states_lock:
        g = game_states.get(str(game_id), {})
    phase = get_game_phase(game_id)  # PBP-only; returns UNKNOWN if no PBP
    nba_gid = _nba_game_ids.get(str(game_id))
    pbp = get_pbp_phase(nba_gid) if nba_gid else None
    return {
        "phase": phase,
        "period": pbp["period"] if pbp else 0,
        "clock": pbp["clock"] if pbp else "",
        "detail": (pbp.get("last_event", "")[:60] if pbp else ""),
        # Team abbreviations + scores still come from ESPN — that part is
        # fine, scores are accurate and we'd need a separate NBA CDN
        # scoreboard poll to replace them. Not phase-related.
        "away": g.get("away_abbr", ""),
        "home": g.get("home_abbr", ""),
        "away_score": g.get("away_score", 0),
        "home_score": g.get("home_score", 0),
    }


def get_todays_games():
    """Get today's NBA games from ESPN scoreboard.
    Returns list of {game_id, away, home, status, startTime, away_score, home_score}.
    """
    try:
        resp = requests.get(ESPN_SCOREBOARD, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        games = []
        for event in data.get("events", []):
            game_id = event.get("id", "")
            competition = event.get("competitions", [{}])[0] if event.get("competitions") else {}
            competitors = competition.get("competitors", [])
            away = {}
            home = {}
            for c in competitors:
                if c.get("homeAway") == "home":
                    home = c
                else:
                    away = c

            status = _parse_status(competition)
            start_time = event.get("date", "")
            status_obj = competition.get("status", {})
            detail = status_obj.get("type", {}).get("detail", "")
            display_clock = status_obj.get("displayClock", "")
            period = status_obj.get("period", 0)

            games.append({
                "game_id": game_id,
                "away": away.get("team", {}).get("displayName", away.get("team", {}).get("name", "?")),
                "away_abbr": away.get("team", {}).get("abbreviation", "?"),
                "home": home.get("team", {}).get("displayName", home.get("team", {}).get("name", "?")),
                "home_abbr": home.get("team", {}).get("abbreviation", "?"),
                "status": status,
                "status_detail": detail,
                "display_clock": display_clock,
                "period": period,
                "startTime": start_time,
                "away_score": int(away.get("score", 0) or 0),
                "home_score": int(home.get("score", 0) or 0),
            })
        return games
    except Exception as e:
        log.error(f"ESPN scoreboard error: {e}")
        return []


def get_game_status(game_id):
    """Get current status for a specific game.
    Returns 'Scheduled', 'Live', 'Final', or 'Halftime'.
    """
    with game_lifecycle_lock:
        status = game_lifecycle.get(str(game_id))
        if status:
            return status
    # Fallback: fetch from ESPN
    games = get_todays_games()
    for g in games:
        if str(g["game_id"]) == str(game_id):
            return g["status"]
    return "Scheduled"


def _poll_loop():
    """Background poller — checks ESPN scoreboard for status changes."""
    while True:
        try:
            games = get_todays_games()
            with game_lifecycle_lock:
                for g in games:
                    gid = str(g["game_id"])
                    new_status = g["status"]
                    old_status = game_lifecycle.get(gid)

                    if new_status != old_status:
                        game_lifecycle[gid] = new_status
                        # Detect transitions
                        if new_status == "Live" and old_status in (None, "Scheduled"):
                            log.info(f"GAME_START [{gid}] {g['away_abbr']} @ {g['home_abbr']} — tipoff")
                            if _on_game_start:
                                _on_game_start(gid)
                        elif new_status == "Final" and old_status != "Final":
                            log.info(f"GAME_END [{gid}] {g['away_abbr']} @ {g['home_abbr']} — final")
                            if _on_game_end:
                                _on_game_end(gid)

            with game_states_lock:
                for g in games:
                    game_states[str(g["game_id"])] = g

        except Exception as e:
            log.error(f"NBA poll error: {e}")
        time.sleep(POLL_INTERVAL)


def _score_poll_loop():
    """Fast poller (3s) for live games — detects score changes for in-play tightening."""
    while True:
        try:
            # Only poll if we have any live games loaded
            with game_lifecycle_lock:
                live_gids = [gid for gid, status in game_lifecycle.items() if status == "Live"]
            if not live_gids:
                time.sleep(SCORE_POLL_INTERVAL * 2)
                continue

            games = get_todays_games()
            now = time.time()
            with last_score_lock:
                for g in games:
                    gid = str(g["game_id"])
                    if gid not in live_gids:
                        continue
                    total = g["away_score"] + g["home_score"]
                    rec = last_score_change.get(gid)
                    if rec is None:
                        last_score_change[gid] = {"ts": now, "last_score": total, "period": 0}
                    elif total > rec["last_score"]:
                        diff = total - rec["last_score"]
                        last_score_change[gid] = {"ts": now, "last_score": total, "period": rec.get("period", 0)}
                        log.info(f"SCORE_CHANGE [{gid}] +{diff} pts → {g['away_abbr']} {g['away_score']}-{g['home_score']} {g['home_abbr']}")
                        if _on_score_change:
                            try:
                                _on_score_change(gid, total, diff)
                            except Exception as e:
                                log.error(f"score change callback error: {e}")
        except Exception as e:
            log.error(f"Score poll error: {e}")
        time.sleep(SCORE_POLL_INTERVAL)


_poller_started = False
_score_poller_started = False
_stats_poller_started = False


def register_nba_game_id(our_game_id, nba_game_id):
    """Register mapping from our game_id to NBA's game_id for boxscore lookups."""
    if nba_game_id:
        _nba_game_ids[str(our_game_id)] = str(nba_game_id)
        log.info(f"Registered NBA game ID: {our_game_id} → {nba_game_id}")


# ---------------------------------------------------------------------------
# Player directory — name resolution
# ---------------------------------------------------------------------------

_SUFFIX_RE = re.compile(r"\s+(jr|sr|ii|iii|iv|v)\.?$", re.IGNORECASE)


def _normalize_player_name(name):
    """Fold to ASCII lowercase, strip accents/suffixes/periods, collapse space.
    Also collapses leading single-char tokens into one initialism so
    "O.G. Anunoby" → "og anunoby" matches NBA CDN's "OG Anunoby".
    Deterministic — same input always yields same output."""
    if not name:
        return ""
    s = str(name)
    # NFD split accents into combining marks, then drop them (Mn category).
    s = "".join(c for c in unicodedata.normalize("NFD", s)
                if unicodedata.category(c) != "Mn")
    s = s.lower().replace(".", " ").replace("-", " ").replace("'", "")
    s = re.sub(r"\s+", " ", s).strip()
    s = _SUFFIX_RE.sub("", s).strip()
    tokens = s.split()
    if len(tokens) > 2:
        lead = []
        rest = list(tokens)
        while rest and len(rest[0]) == 1:
            lead.append(rest.pop(0))
        if lead and rest:
            s = "".join(lead) + " " + " ".join(rest)
    return _manual_aliases.get(s, s)


def _generate_name_keys(first_name, family_name, full_name, name_i):
    """Produce the set of normalized keys we'll index a player under.
    - full name ("jalen williams")
    - family alone ("williams")         — may be ambiguous, filtered later
    - first-initial + family ("j williams")
    - boxscore nameI ("j williams")     — redundant with above, still safe
    - first + family (dropping middle) if full has >2 tokens
    """
    keys = set()
    full_n = _normalize_player_name(full_name)
    fam_n = _normalize_player_name(family_name)
    first_n = _normalize_player_name(first_name)
    namei_n = _normalize_player_name(name_i)
    if full_n:
        keys.add(full_n)
    if fam_n:
        keys.add(fam_n)
    if first_n and fam_n:
        keys.add(f"{first_n[0]} {fam_n}")
        keys.add(f"{first_n} {fam_n}")
    if namei_n:
        keys.add(namei_n)
    return keys


def _canonical_nba_id(gid):
    """Callers may pass either an ESPN game id (e.g. '401869396') or an NBA
    CDN game id (e.g. '0042500112'). Everything in this module is keyed by
    the NBA CDN id (that's what /boxscore uses). Translate transparently so
    callers don't have to care which flavour they have."""
    if not gid:
        return ""
    s = str(gid)
    # If this is an ESPN id mapped via register_nba_game_id / auto-discover,
    # swap to the NBA CDN id.
    if s in _nba_game_ids:
        mapped = _nba_game_ids[s]
        # Only translate if the target looks like an NBA CDN id (starts with
        # "00" — ESPN ids are pure integers). Otherwise the mapping might be
        # ticker→ESPN which we want to leave alone.
        if mapped and str(mapped).startswith("00"):
            return str(mapped)
    return s


def resolve_player_in_game(nba_game_id, name):
    """Return personId for `name` within this game, or None if unknown/ambiguous.
    The per-game index only contains keys that are unambiguous across the
    game's 2 teams — so None means 'cannot safely resolve', not 'never seen'.
    Logs unknowns once per (game_id, name) so misses can be aliased."""
    if not name:
        return None
    key = _normalize_player_name(name)
    if not key:
        return None
    canon = _canonical_nba_id(nba_game_id)
    with _players_lock:
        pid = _game_name_to_pid.get(canon, {}).get(key)
    if pid is None:
        miss_key = (canon, key)
        if miss_key not in _resolve_misses:
            _resolve_misses.add(miss_key)
            log.info(f"player_dir miss: game={nba_game_id} (canon={canon}) name='{name}' key='{key}'")
    return pid


def get_player(pid):
    """Return {fullName, familyName, teamTricode, gameId} or None."""
    if pid is None:
        return None
    with _players_lock:
        return dict(_players_by_pid.get(pid, {})) or None


def _fetch_boxscore(nba_game_id):
    """Fetch live boxscore from NBA CDN. Returns {player_name: {points, rebounds, assists, ...}}.
    Side effect: updates on_court_state and player directory for this game."""
    try:
        resp = requests.get(NBA_CDN_BOXSCORE.format(nba_game_id),
                          headers=NBA_CDN_HEADERS, timeout=10)
        if resp.status_code != 200:
            return None
        game = resp.json().get("game", {})
        stats_out = {}
        on_court_sides = {}  # team_key → parsed side entry
        dir_updates = {}     # pid → directory entry (applied after parse)
        key_counts = {}      # name_key → count (for ambiguity detection)
        key_to_pid = {}      # name_key → pid (last seen)
        for team_key in ("homeTeam", "awayTeam"):
            team = game.get(team_key, {})
            team_code = team.get("teamTricode", "")
            on_players = {}
            bench_players = {}
            for p in team.get("players", []):
                s = p.get("statistics", {})
                name = p.get("nameI", p.get("name", ""))  # "J. Tatum" format
                full_name = p.get("name", name)           # "Jayson Tatum" format
                family = (p.get("familyName") or "").strip()
                first_name = (p.get("firstName") or "").strip()
                person_id = p.get("personId")
                on_flag = str(p.get("oncourt", "")).strip()
                full_clean = (full_name or "").strip()
                # Directory always gets the player (even inactive) so later
                # feeds can resolve them. on_court/bench dicts are personId-
                # keyed and only hold active (on=1) / bench (on=0) players.
                if person_id is not None:
                    dir_updates[person_id] = {
                        "fullName": full_clean,
                        "familyName": family,
                        "firstName": first_name,
                        "teamTricode": team_code,
                        "gameId": str(nba_game_id),
                    }
                    for k in _generate_name_keys(first_name, family, full_clean, name):
                        key_counts[k] = key_counts.get(k, 0) + 1
                        key_to_pid[k] = person_id
                    entry = {
                        "familyName": family,
                        "fullName": full_clean,
                        "personId": person_id,
                    }
                    if on_flag == "1":
                        on_players[person_id] = entry
                    elif on_flag == "0":
                        bench_players[person_id] = entry
                    # on_flag == "" (inactive) → not tracked either side
                if not name:
                    continue
                stats_out[name] = {
                    "full_name": full_name,
                    "team": team_code,
                    "points": s.get("points", 0),
                    "rebounds": s.get("reboundsTotal", 0),
                    "assists": s.get("assists", 0),
                    "minutes": s.get("minutesCalculated", s.get("minutes", "")),
                    "fgm": s.get("fieldGoalsMade", 0),
                    "fga": s.get("fieldGoalsAttempted", 0),
                    "ftm": s.get("freeThrowsMade", 0),
                    "fta": s.get("freeThrowsAttempted", 0),
                    "three_pm": s.get("threePointersMade", 0),
                    "steals": s.get("steals", 0),
                    "blocks": s.get("blocks", 0),
                    "turnovers": s.get("turnovers", 0),
                    "plus_minus": s.get("plusMinusPoints", 0),
                }
            on_court_sides["home" if team_key == "homeTeam" else "away"] = {
                "tricode": team_code,
                "on_court": on_players,
                "bench": bench_players,
            }

        if on_court_sides.get("home") and on_court_sides.get("away"):
            with on_court_lock:
                prev = on_court_state.get(str(nba_game_id), {})
                on_court_state[str(nba_game_id)] = {
                    "home": on_court_sides["home"],
                    "away": on_court_sides["away"],
                    "updated_at": time.time(),
                    "source": "boxscore",
                    # Preserve cursor across boxscore refresh so PBP doesn't replay history.
                    "_sub_cursor": prev.get("_sub_cursor", 0),
                }
            # Update global directory + per-game name index. Only keys that
            # are unique across this game's 2 teams are registered; ambiguous
            # keys (e.g., "williams" on OKC) are omitted so resolve() returns
            # None rather than guessing wrong.
            game_index = {k: key_to_pid[k] for k, cnt in key_counts.items() if cnt == 1}
            with _players_lock:
                _players_by_pid.update(dir_updates)
                _game_name_to_pid[str(nba_game_id)] = game_index
        return stats_out
    except Exception as e:
        log.error(f"NBA boxscore error for {nba_game_id}: {e}")
        return None


def _apply_substitution_events(nba_game_id, actions):
    """Walk PBP actions past our cursor and apply Substitution events to on_court_state.
    In-place. Safe to call every PBP poll — cursor keeps us idempotent."""
    if not actions:
        return
    with on_court_lock:
        state = on_court_state.get(str(nba_game_id))
        if not state:
            # No boxscore bootstrap yet — can't tell which team a personId belongs to
            # with full confidence. Skip; we'll catch up after first boxscore.
            return
        cursor = state.get("_sub_cursor", 0)
        max_seen = cursor
        flipped = 0
        for a in actions:
            action_num = a.get("actionNumber", 0) or 0
            if action_num <= cursor:
                continue
            if action_num > max_seen:
                max_seen = action_num
            atype = (a.get("actionType") or "").lower()
            if atype != "substitution":
                continue
            subtype = (a.get("subType") or "").lower()
            if subtype not in ("in", "out"):
                continue
            family = (a.get("playerName") or "").strip()
            team_tricode = (a.get("teamTricode") or "").strip()
            person_id = a.get("personId")
            # Storage is personId-keyed, and PBP always provides personId.
            # If the event has no personId (defensive), skip — we won't guess.
            if person_id is None:
                continue
            # Find the side by tricode
            side = None
            for s in ("home", "away"):
                if state.get(s, {}).get("tricode") == team_tricode:
                    side = s
                    break
            if side is None:
                continue
            on_map = state[side].setdefault("on_court", {})
            bn_map = state[side].setdefault("bench", {})
            # Prefer canonical names from existing entry if present.
            existing = on_map.get(person_id) or bn_map.get(person_id)
            fullname = ""
            if existing:
                fullname = existing.get("fullName", "")
                family = existing.get("familyName") or family
            entry = {"familyName": family, "fullName": fullname, "personId": person_id}
            if subtype == "in":
                bn_map.pop(person_id, None)
                on_map[person_id] = entry
            else:  # out
                on_map.pop(person_id, None)
                bn_map[person_id] = entry
            flipped += 1
        state["_sub_cursor"] = max_seen
        if flipped:
            state["updated_at"] = time.time()
            state["source"] = "pbp"
            log.info(f"on_court [{nba_game_id}]: {flipped} sub events applied, cursor={max_seen}")


def _fetch_pbp(nba_game_id):
    """Fetch play-by-play from NBA CDN. Returns game phase from last events."""
    try:
        resp = requests.get(NBA_CDN_PBP.format(nba_game_id),
                          headers=NBA_CDN_HEADERS, timeout=5)
        if resp.status_code != 200:
            return None
        data = resp.json()
        game = data.get("game", {})
        actions = game.get("actions", [])
        if not actions:
            return None

        # On-court tracker — process substitution events incrementally.
        _apply_substitution_events(nba_game_id, actions)

        # Get last few events to determine phase
        recent = actions[-5:] if len(actions) >= 5 else actions
        last = actions[-1]
        action_type = (last.get("actionType", "") or "").lower()
        sub_type = (last.get("subType", "") or "").lower()
        description = (last.get("description", "") or "").lower()
        period = last.get("period", 0)
        clock = last.get("clock", "")
        # Remove PT prefix from clock: "PT05M30.00S" → "5:30"
        display_clock = clock
        if clock.startswith("PT"):
            try:
                import re
                m = re.match(r'PT(\d+)M([\d.]+)S', clock)
                if m:
                    mins = int(m.group(1))
                    secs = int(float(m.group(2)))
                    display_clock = f"{mins}:{secs:02d}"
            except Exception:
                pass

        # Determine phase from action type.
        # NOTE (2026-04-21): Per user request, stoppages/fouls/free throws/
        # reviews/ejections/dead-ball events all keep phase=LIVE. Rationale:
        # our trading engine reacts too slowly to those brief dead balls
        # right now — switching to TIMEOUT cancels + replaces orders, and
        # by the time we react the ball is back in play and we get
        # past-posted. Only *real* timeouts and period breaks flip out of
        # LIVE. Revisit once reaction time improves.
        # NOTE (2026-04-24): Subs during a timeout post AFTER the timeout
        # event at the same game clock, so `last = actions[-1]` lands on
        # a substitution and misses the timeout entirely. Fix: scan all
        # actions sharing the latest action's (period, clock) — if any is
        # a timeout, phase = TIMEOUT. Subs cluster at the timeout's clock;
        # real play advances the clock.
        phase = "LIVE"
        same_tick = [
            a for a in actions[-12:]
            if a.get("period", 0) == period and a.get("clock", "") == clock
        ]
        timeout_at_tick = any(
            (a.get("actionType", "") or "").lower() == "timeout"
            or "timeout" in (a.get("description", "") or "").lower()
            for a in same_tick
        )
        if timeout_at_tick:
            phase = "TIMEOUT"
        elif action_type == "period" and sub_type == "end":
            if period == 2:
                phase = "HALFTIME"
            else:
                phase = "QUARTER_BREAK"
        elif action_type == "period" and sub_type == "start":
            phase = "LIVE"
        elif action_type == "game" and sub_type == "end":
            phase = "FINAL"
        # Removed: stoppage / dead-ball / official / freethrow / review /
        # ejection previously mapped to TIMEOUT. Now stay LIVE.

        # NOTE (2026-04-24): NBA CDN PBP can lag 30-90s on the period:end
        # event. The buzzer fires, but the last event in the feed is a
        # benign artifact like a TEAM rebound / heave / block at clock
        # 0:00.X — phase stays LIVE while the game is actually at the
        # break. Trades placed in that window get past-posted when
        # period:end finally lands. Defensive heuristic: if the clock
        # is at/near 0:00 and the last event is end-of-period detritus
        # (no real scoring chance), preemptively flip to HALFTIME (P2)
        # or QUARTER_BREAK (P1/P3). P4+ stays LIVE — game might go to
        # OT and we can't distinguish from feed alone.
        if phase == "LIVE":
            try:
                import re as _re
                m = _re.match(r'PT(\d+)M([\d.]+)S', clock or '')
                clock_secs = int(m.group(1)) * 60 + float(m.group(2)) if m else 999
            except Exception:
                clock_secs = 999
            END_OF_PERIOD_DETRITUS = {"rebound", "heave", "block", "turnover", "violation"}
            if clock_secs <= 0.5 and action_type in END_OF_PERIOD_DETRITUS:
                if period == 2:
                    phase = "HALFTIME"
                elif period in (1, 3):
                    phase = "QUARTER_BREAK"

        return {
            "phase": phase,
            "period": period,
            "clock": display_clock,
            "last_event": last.get("description", ""),
            "action_type": action_type,
        }
    except Exception as e:
        log.debug(f"NBA PBP error for {nba_game_id}: {e}")
        return None


_espn_to_nba_ids = {}  # {espn_game_id: nba_cdn_game_id}


def _resolve_nba_cdn_id(espn_game_id):
    """Convert ESPN game ID to NBA CDN game ID. Caches results."""
    if espn_game_id in _espn_to_nba_ids:
        return _espn_to_nba_ids[espn_game_id]
    # Look up from ESPN game state
    with game_states_lock:
        g = game_states.get(str(espn_game_id), {})
    if g:
        nba_id = _try_find_nba_game_id(g)
        if nba_id:
            _espn_to_nba_ids[espn_game_id] = nba_id
            log.info(f"PBP resolved: ESPN {espn_game_id} ({g.get('away_abbr', '?')}@{g.get('home_abbr', '?')}) → NBA CDN {nba_id}")
            return nba_id
        else:
            log.debug(f"PBP resolve failed for ESPN {espn_game_id}: {g.get('away_abbr', '?')}@{g.get('home_abbr', '?')}, cache has {len(getattr(_try_find_nba_game_id, '_cache', {}))} entries")
    else:
        log.debug(f"PBP resolve: ESPN {espn_game_id} not found in game_states (keys: {list(game_states.keys())[:5]})")
    return None


def _pbp_poller_loop():
    """Poll NBA CDN play-by-play every 2s for live games. PBP is the sole
    source of truth for phase now that ESPN fallback has been removed, so
    this loop MUST populate pbp_state successfully for each live game.

    Fixed 2026-04-21: old code treated the value from _nba_game_ids as an
    ESPN id and re-resolved it via _resolve_nba_cdn_id → that resolve
    call was being fed a CDN id and always returned None → pbp_state was
    never populated → every caller of get_pbp_phase got None → every
    get_game_phase returned UNKNOWN (after ESPN fallback removal).

    _nba_game_ids stores two key types in one dict:
      - ticker → ESPN id  (from register_nba_game_id, Kalshi side)
      - ESPN id → CDN id  (from stats_poll_loop auto-discovery)
    When iterating game_states (keyed by ESPN id), the second mapping is
    what we want. We already have the CDN id directly — no resolve step
    needed."""
    while True:
        try:
            live_games = []
            with game_states_lock:
                for espn_id, state in game_states.items():
                    cdn_id = _nba_game_ids.get(espn_id)
                    # Only accept entries that look like a CDN id (NBA CDN
                    # ids are 10-digit strings starting with "00"; ESPN ids
                    # are 9-digit integers). Guards against edge cases
                    # where _nba_game_ids holds a ticker→ESPN mapping.
                    if not cdn_id or not str(cdn_id).startswith("00"):
                        continue

                    # Primary status source: NBA CDN (10s poll, fastest signal).
                    # CDN says Live → poll. CDN says Final → don't.
                    cdn_status_val = get_cdn_status(cdn_id)
                    if cdn_status_val == "Live":
                        live_games.append((espn_id, cdn_id))
                        continue
                    if cdn_status_val == "Final":
                        continue

                    # Fallback to ESPN when CDN is silent or stale.
                    status = state.get("status", "")
                    if status in ("Final", "Cancelled", "Postponed"):
                        continue
                    if status == "Scheduled":
                        # ESPN can lag real tipoff by 30-90s. If startTime is
                        # already in the past (within 4h), poll PBP regardless
                        # so we don't miss the tipoff window. Outside that
                        # window we treat 'Scheduled' literally — game probably
                        # postponed and ESPN just hasn't updated.
                        secs_until = _seconds_until_tipoff(state.get("startTime", ""))
                        if secs_until is None or secs_until > 0 or secs_until < -4 * 3600:
                            continue
                    # status == Live or Halftime, or post-tipoff Scheduled fallback
                    live_games.append((espn_id, cdn_id))

            for espn_id, cdn_id in live_games:
                result = _fetch_pbp(cdn_id)
                if not result:
                    continue
                with pbp_state_lock:
                    old = pbp_state.get(cdn_id, {})
                    if old.get("phase") != result["phase"]:
                        log.info(
                            f"PBP phase change [{espn_id}→{cdn_id}]: "
                            f"{old.get('phase', '?')} → {result['phase']} "
                            f"({result.get('last_event', '')[:50]})"
                        )
                    entry = {**result, "updated_at": time.time()}
                    pbp_state[cdn_id] = entry
                    pbp_state[espn_id] = entry  # also key by ESPN so
                                                # callers with either ID land
        except Exception as e:
            log.error(f"PBP poller error: {e}")
        time.sleep(PBP_POLL_INTERVAL)


def get_pbp_phase(nba_game_id):
    """Get game phase from play-by-play (faster than ESPN). Returns dict or None.

    Stale window widened from 15s → 60s so transient NBA CDN hiccups
    (e.g., during halftime transitions) don't collapse phase to UNKNOWN
    and cause trading behavior to flip. If PBP is genuinely down for a
    full minute we'll still fall through to UNKNOWN, which is the
    correct safety behavior."""
    with pbp_state_lock:
        state = pbp_state.get(str(nba_game_id))
        if state and time.time() - state.get("updated_at", 0) < 60:
            return state
    return None


def start_pbp_poller():
    """Start the play-by-play poller for fast timeout detection."""
    global _pbp_poller_started
    if _pbp_poller_started:
        return
    _pbp_poller_started = True
    threading.Thread(target=_pbp_poller_loop, daemon=True).start()
    log.info(f"NBA PBP poller started, interval={PBP_POLL_INTERVAL}s")


def get_cdn_status(cdn_id):
    """Return NBA CDN game status if fresh (<30s), else None.
    status: 'Scheduled' | 'Live' | 'Final'."""
    with cdn_game_status_lock:
        rec = cdn_game_status.get(str(cdn_id))
        if rec and time.time() - rec.get("updated_at", 0) < 30:
            return rec.get("status")
    return None


def _find_espn_id_by_cdn(cdn_id):
    """Reverse lookup espn_id from cdn_id via _nba_game_ids."""
    target = str(cdn_id)
    for espn_id, mapped in _nba_game_ids.items():
        if str(mapped) == target:
            return espn_id
    return None


def _cdn_status_poller_loop():
    """Poll NBA CDN scoreboard every 10s for authoritative live/final status.
    NBA CDN is the source of truth — ESPN derives from it but lags. By
    polling CDN directly we cut 30-90s off tipoff detection and 30s+ off
    final detection. Fires on_game_start / on_game_end on transitions."""
    while True:
        try:
            r = requests.get(NBA_CDN_SCOREBOARD, headers=NBA_CDN_HEADERS, timeout=10)
            if r.status_code == 200:
                games = r.json().get("scoreboard", {}).get("games", [])
                now = time.time()
                transitions = []  # (espn_id, old_status, new_status)
                with cdn_game_status_lock:
                    for g in games:
                        gid = str(g.get("gameId", ""))
                        if not gid:
                            continue
                        status_num = g.get("gameStatus", 0)
                        # NBA gameStatus: 1=Scheduled, 2=Live (incl Halftime), 3=Final
                        if status_num == 1:
                            status = "Scheduled"
                        elif status_num == 2:
                            status = "Live"
                        elif status_num == 3:
                            status = "Final"
                        else:
                            continue
                        old = cdn_game_status.get(gid, {})
                        old_status = old.get("status")
                        cdn_game_status[gid] = {"status": status, "updated_at": now}
                        if old_status and old_status != status:
                            espn_id = _find_espn_id_by_cdn(gid)
                            transitions.append((gid, espn_id, old_status, status))
                # Fire callbacks OUTSIDE lock
                for gid, espn_id, old_status, status in transitions:
                    log.info(f"CDN status [{gid}]: {old_status} → {status}")
                    if not espn_id:
                        continue
                    if status == "Live" and old_status == "Scheduled" and _on_game_start:
                        try:
                            _on_game_start(espn_id)
                        except Exception as e:
                            log.error(f"CDN game_start callback: {e}")
                    elif status == "Final" and old_status != "Final" and _on_game_end:
                        try:
                            _on_game_end(espn_id)
                        except Exception as e:
                            log.error(f"CDN game_end callback: {e}")
        except Exception as e:
            log.error(f"CDN status poller error: {e}")
        time.sleep(CDN_STATUS_POLL_INTERVAL)


def start_cdn_status_poller():
    """Start the NBA CDN scoreboard status poller (primary status source)."""
    global _cdn_status_poller_started
    if _cdn_status_poller_started:
        return
    _cdn_status_poller_started = True
    threading.Thread(target=_cdn_status_poller_loop, daemon=True).start()
    log.info(f"NBA CDN status poller started, interval={CDN_STATUS_POLL_INTERVAL}s")


def _try_find_nba_game_id(espn_game):
    """Try to find the NBA CDN game ID for an ESPN game.
    ESPN IDs and NBA IDs are different — NBA uses 10-digit IDs like 0052500101.
    We try to match by looking up the NBA schedule for today."""
    # NBA CDN schedule is huge — cache it
    if not hasattr(_try_find_nba_game_id, "_cache"):
        _try_find_nba_game_id._cache = {}
        _try_find_nba_game_id._cache_ts = 0

    now = time.time()
    if now - _try_find_nba_game_id._cache_ts > 3600:  # refresh hourly
        try:
            r = requests.get("https://cdn.nba.com/static/json/liveData/scoreboard/todaysScoreboard_00.json",
                headers=NBA_CDN_HEADERS, timeout=10)
            if r.status_code == 200:
                nba_games = r.json().get("scoreboard", {}).get("games", [])
                cache = {}
                for g in nba_games:
                    home = g.get("homeTeam", {}).get("teamTricode", "")
                    away = g.get("awayTeam", {}).get("teamTricode", "")
                    gid = g.get("gameId", "")
                    if home and away and gid:
                        cache[f"{away}@{home}"] = gid
                        # Also store individual team keys for fuzzy matching
                        cache[f"home:{home}"] = gid
                        cache[f"away:{away}"] = gid
                _try_find_nba_game_id._cache = cache
                _try_find_nba_game_id._cache_ts = now
                log.info(f"NBA CDN schedule loaded: {len(nba_games)} games, keys: {[k for k in cache if '@' in k]}")
        except Exception as e:
            log.warning(f"NBA CDN schedule fetch failed: {e}")

    # Match by team abbreviations — try exact, then fuzzy
    home = espn_game.get("home_abbr", "")
    away = espn_game.get("away_abbr", "")
    cache = _try_find_nba_game_id._cache
    # Exact match
    result = cache.get(f"{away}@{home}")
    if result:
        return result
    # Try NY vs NYK mismatch
    for key, gid in cache.items():
        if "@" in key:
            parts = key.split("@")
            if len(parts) == 2:
                ca, ch = parts
                if (ca in away or away in ca) and (ch in home or home in ch):
                    return gid
    return None


_PREGAME_BOXSCORE_WINDOW_SECS = 21600  # fetch pregame boxscores up to 6h before tipoff (NBA CDN returns empty until rosters publish ~1-2h out, which is fine)


def _seconds_until_tipoff(start_time_iso):
    """Return seconds until tipoff, or None if unparseable. Negative if past."""
    if not start_time_iso:
        return None
    try:
        ts = start_time_iso.replace("Z", "+00:00")
        dt = datetime.fromisoformat(ts)
        return (dt - datetime.now(timezone.utc)).total_seconds()
    except Exception:
        return None


def _stats_poll_loop():
    """Poll NBA CDN boxscores for live (and imminent pregame) games every 5s.

    Pregame fetch (Scheduled + within 15 min of tipoff) populates the player
    directory early so name resolution is ready at tipoff. On-court status
    will read as bench/unknown pre-tipoff, which is semantically correct."""
    while True:
        try:
            with game_lifecycle_lock:
                lifecycle_snap = dict(game_lifecycle)
            with game_states_lock:
                states_snap = dict(game_states)
            # Include anything in-progress — Live AND Halftime (and any
            # other non-terminal status ESPN might set). If we only check
            # 'Live' and the game is at halftime when the bot boots, the
            # NBA CDN ID never auto-discovers, _nba_game_ids stays empty
            # for that game, and the PBP poller has nothing to look up.
            # Phase stays UNKNOWN forever for that game.
            TERMINAL_OR_PREGAME = {"Scheduled", "Final", "Cancelled", "Postponed"}
            live_gids = [gid for gid, st in lifecycle_snap.items()
                         if st and st not in TERMINAL_OR_PREGAME]
            # Pregame window: Scheduled games within 15 min of tipoff (or past
            # tipoff but not yet flipped to Live by ESPN).
            pregame_gids = []
            for gid, st in lifecycle_snap.items():
                if st != "Scheduled":
                    continue
                secs = _seconds_until_tipoff(states_snap.get(gid, {}).get("startTime"))
                if secs is not None and secs <= _PREGAME_BOXSCORE_WINDOW_SECS:
                    pregame_gids.append(gid)
            poll_gids = list(dict.fromkeys(live_gids + pregame_gids))
            if not poll_gids:
                time.sleep(STATS_POLL_INTERVAL * 2)
                continue

            # Auto-discover NBA game IDs from ESPN game IDs if not registered
            for gid in poll_gids:
                if gid not in _nba_game_ids:
                    espn_game = states_snap.get(gid, {})
                    nba_gid = _try_find_nba_game_id(espn_game)
                    if nba_gid:
                        _nba_game_ids[gid] = nba_gid
                        log.info(f"Auto-discovered NBA game ID: ESPN {gid} → NBA {nba_gid}")

            for gid in poll_gids:
                nba_gid = _nba_game_ids.get(gid)
                if not nba_gid:
                    continue
                stats = _fetch_boxscore(nba_gid)
                if stats:
                    with player_stats_lock:
                        player_stats[nba_gid] = stats
        except Exception as e:
            log.error(f"Stats poll error: {e}")
        time.sleep(STATS_POLL_INTERVAL)


def get_player_stats(nba_game_id=None):
    """Get live player stats. If nba_game_id given, returns that game's stats.
    Otherwise returns all games' stats merged."""
    with player_stats_lock:
        if nba_game_id:
            return dict(player_stats.get(str(nba_game_id), {}))
        # Merge all games
        merged = {}
        for gid, stats in player_stats.items():
            merged.update(stats)
        return merged


def get_player_stat(player_name, stat="points", nba_game_id=None):
    """Get a specific stat for a player. Returns int or None."""
    stats = get_player_stats(nba_game_id)
    # Try exact match first, then fuzzy
    for name, s in stats.items():
        if name.lower() == player_name.lower() or s.get("full_name", "").lower() == player_name.lower():
            return s.get(stat)
    # Partial match (last name)
    player_lower = player_name.lower()
    for name, s in stats.items():
        if player_lower in name.lower() or player_lower in s.get("full_name", "").lower():
            return s.get(stat)
    return None


def get_on_court_snapshot(nba_game_id):
    """Return debug-panel-ready snapshot for a single game, or None if unknown.
    Shape: {home:{tricode,on_court:[familyName],bench:[familyName]}, away:{...},
            updated_at, stale, source}"""
    canon = _canonical_nba_id(nba_game_id)
    with on_court_lock:
        state = on_court_state.get(canon)
        if not state:
            return None
        out = {
            "updated_at": state.get("updated_at", 0),
            "source": state.get("source", ""),
        }
        for side in ("home", "away"):
            s = state.get(side, {})
            on_list = sorted(v["familyName"] for v in s.get("on_court", {}).values())
            bn_list = sorted(v["familyName"] for v in s.get("bench", {}).values())
            out[side] = {"tricode": s.get("tricode", ""), "on_court": on_list, "bench": bn_list}
    out["stale"] = (time.time() - out["updated_at"]) > 30
    return out


def get_on_court_lookup(nba_game_id):
    """Return {personId: bool} for O(1) lookup. True=on floor, False=bench.
    Inactive players are absent. Empty dict if no state yet.

    Callers should resolve `name → pid` via resolve_player_in_game() once
    (cache on their row) and then hit this dict each tick."""
    out = {}
    canon = _canonical_nba_id(nba_game_id)
    with on_court_lock:
        state = on_court_state.get(canon)
        if not state:
            return out
        for side in ("home", "away"):
            s = state.get(side, {})
            for pid in s.get("on_court", {}):
                out[pid] = True
            for pid in s.get("bench", {}):
                out[pid] = False
    return out


def is_on_court(nba_game_id, player, team_tricode=None):
    """Per-player on-court check. Returns True/False/None (None = unknown).

    `player` may be a personId (int) or a name string. Names resolve via
    the player directory — ambiguous/unknown names return None. If
    team_tricode is given, restricts the check to that team."""
    if player is None or player == "":
        return None
    # If a string that looks like a name, resolve to pid first.
    if isinstance(player, str):
        pid = resolve_player_in_game(nba_game_id, player)
    else:
        pid = player
    if pid is None:
        return None
    canon = _canonical_nba_id(nba_game_id)
    with on_court_lock:
        state = on_court_state.get(canon)
        if not state:
            return None
        for side in ("home", "away"):
            s = state.get(side, {})
            if team_tricode and s.get("tricode") != team_tricode:
                continue
            if pid in s.get("on_court", {}):
                return True
            if pid in s.get("bench", {}):
                return False
    return None


def start_game_poller():
    """Start background NBA game polling thread (only once)."""
    global _poller_started, _score_poller_started, _stats_poller_started
    if _score_poller_started is False:
        _score_poller_started = True
        threading.Thread(target=_score_poll_loop, daemon=True).start()
        log.info(f"NBA score poller started, interval={SCORE_POLL_INTERVAL}s")
    if not _stats_poller_started:
        _stats_poller_started = True
        threading.Thread(target=_stats_poll_loop, name="nba_stats_poller", daemon=True).start()
        log.info(f"NBA stats poller started, interval={STATS_POLL_INTERVAL}s")
    if _poller_started:
        return
    _poller_started = True
    t = threading.Thread(target=_poll_loop, daemon=True)
    t.start()
    log.info(f"NBA poller started, interval={POLL_INTERVAL}s")


def get_game_states():
    """Get current game states snapshot."""
    with game_states_lock:
        return dict(game_states)
