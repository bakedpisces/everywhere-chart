"""
Spotify Playlist Seeder
-----------------------
Searches Spotify for playlists matching discovery keywords ("new",
"hot", "hottest", "trending"), filters to those with ≥ 50k followers,
pulls their tracks, and upserts every song into the catalog.

No signal_events are written — this is purely catalog seeding so that
other collectors (Reddit, Shazam, YouTube, TikTok, ScrapeCreators) can
match against a much wider song universe.

Strategy:
  1. Search for each keyword (searches title + description)
  2. Paginate up to SEARCH_PAGES results per keyword
  3. Batch-fetch full playlist objects (followers, name) — up to
     FOLLOWER_FETCH_LIMIT per keyword to cap API calls
  4. Deduplicate playlists by Spotify playlist ID across keywords
  5. Fetch tracks from each qualifying playlist (up to TRACKS_PER_PLAYLIST)
     — requires a user-level token; 403 is detected and logged clearly
  6. Upsert artist + song; pull full metadata for new songs
  7. Record seeded playlists in seeded_playlists table to skip on re-runs

Called from spotify_collector.run() after chart data is written, with a
user-level token extracted from the Playwright browser session.
Can also run standalone (python -m collectors.spotify_playlist_seeder)
using the sp_dc cookie or client credentials as fallback.
"""

import os
import re
import time
import logging
import requests
import psycopg2
import psycopg2.extras
from datetime import datetime, timezone
from typing import Optional

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("spotify_playlist_seeder")

DB_URL         = os.environ["DATABASE_URL"]
SPOTIFY_CLIENT = os.environ.get("SPOTIFY_CLIENT_ID")
SPOTIFY_SECRET = os.environ.get("SPOTIFY_CLIENT_SECRET")
SPOTIFY_SP_DC  = os.environ.get("SPOTIFY_SP_DC")

SEARCH_KEYWORDS      = ["new", "hot", "hottest", "trending"]
MIN_FOLLOWERS        = 50_000
TRACKS_PER_PLAYLIST  = 100
SEARCH_LIMIT         = 10    # Spotify playlist search max per page
SEARCH_PAGES         = 20    # pages per keyword → up to 200 candidates
FOLLOWER_FETCH_LIMIT = 100   # cap full-playlist fetches per keyword to limit API calls
MAX_PLAYLISTS_PER_RUN = 50   # cap total playlists seeded per run

# ── Helpers ───────────────────────────────────────────────────────────────────

def normalize(s: str) -> str:
    return re.sub(r"[^\w\s]", "", s.lower().strip())

def _normalize_release_date(raw: Optional[str]) -> Optional[str]:
    if not raw:
        return None
    parts = raw.split("-")
    if len(parts) == 1:
        return f"{parts[0]}-01-01"
    if len(parts) == 2:
        return f"{parts[0]}-{parts[1]}-01"
    return raw

# ── Spotify auth ──────────────────────────────────────────────────────────────

_token_cache: dict = {}

def get_token() -> Optional[str]:
    """
    Priority:
      1. Injected user token (from Spotify collector's Playwright session)
      2. sp_dc session cookie → user-level token via web endpoint
      3. Client credentials (can discover playlists but NOT read their tracks)
    """
    now = time.time()
    if _token_cache.get("expires_at", 0) > now + 30:
        return _token_cache["access_token"]

    # Try sp_dc
    if SPOTIFY_SP_DC:
        try:
            resp = requests.get(
                "https://open.spotify.com/get_access_token",
                params={"reason": "transport", "productType": "web_player"},
                cookies={"sp_dc": SPOTIFY_SP_DC},
                headers={"User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
                )},
                timeout=10,
            )
            if resp.status_code == 200:
                data = resp.json()
                token = data.get("accessToken")
                expires_ms = data.get("accessTokenExpirationTimestampMs", 0)
                if token:
                    _token_cache["access_token"] = token
                    _token_cache["expires_at"]   = expires_ms / 1000 if expires_ms else now + 3600
                    log.info("Using sp_dc user token")
                    return token
        except Exception as e:
            log.warning(f"sp_dc token failed: {e} — falling back to client credentials")

    # Client credentials fallback
    if not SPOTIFY_CLIENT or not SPOTIFY_SECRET:
        log.error("No SPOTIFY_CLIENT_ID / SPOTIFY_CLIENT_SECRET — cannot get token")
        return None
    try:
        resp = requests.post(
            "https://accounts.spotify.com/api/token",
            data={"grant_type": "client_credentials"},
            auth=(SPOTIFY_CLIENT, SPOTIFY_SECRET),
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
        _token_cache["access_token"] = data["access_token"]
        _token_cache["expires_at"]   = now + data["expires_in"]
        log.warning(
            "Using client credentials token — playlist TRACK fetching will 403. "
            "Provide SPOTIFY_SP_DC or inject a user token to seed tracks."
        )
        return _token_cache["access_token"]
    except Exception as e:
        log.error(f"Spotify token error: {e}")
        return None


def _get(url: str, params: dict = None) -> Optional[dict]:
    """
    Authenticated GET with rate-limit handling.
    Returns None on any error. 403 is logged distinctly (auth issue,
    not retried) so callers can detect it via the log.
    """
    token = get_token()
    if not token:
        return None

    for attempt in range(3):
        try:
            resp = requests.get(
                url,
                headers={"Authorization": f"Bearer {token}"},
                params=params,
                timeout=15,
            )
            if resp.status_code == 429:
                wait = int(resp.headers.get("Retry-After", 10))
                log.warning(f"Rate limited — waiting {wait}s")
                time.sleep(wait)
                continue
            if resp.status_code == 401:
                injected = _token_cache.get("injected_token")
                _token_cache.clear()
                if injected:
                    log.warning("Injected user token returned 401 — falling back to client credentials")
                token = get_token()
                if not token:
                    return None
                continue
            if resp.status_code == 403:
                log.warning(
                    f"403 Forbidden: {url[:80]} — "
                    "token lacks permission (need user OAuth, not client credentials)"
                )
                return None   # don't retry — auth won't fix itself mid-run
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            log.warning(f"GET {url[:80]} attempt {attempt+1} failed: {e}")
            time.sleep(2)
    return None


def _has_user_token() -> bool:
    """True if current token is user-level (can read playlist tracks)."""
    return bool(_token_cache.get("injected_token") or SPOTIFY_SP_DC)


# ── Playlist discovery ────────────────────────────────────────────────────────

def search_playlists(keyword: str) -> list[dict]:
    """
    Search Spotify for playlists matching keyword.

    Optimised: collects all candidate IDs from search pages first, then
    batch-fetches full playlist objects (for accurate follower counts) only
    up to FOLLOWER_FETCH_LIMIT — avoiding hundreds of individual API calls.
    """
    # Phase 1: collect candidate playlist IDs from search pages
    candidate_ids: list[str] = []
    seen_ids: set[str] = set()

    for page in range(SEARCH_PAGES):
        data = _get(
            "https://api.spotify.com/v1/search",
            params={
                "q":      keyword,
                "type":   "playlist",
                "limit":  SEARCH_LIMIT,
                "offset": page * SEARCH_LIMIT,
            },
        )
        if not data:
            break

        items = data.get("playlists", {}).get("items", []) or []
        if not items:
            break

        for pl in items:
            if not pl or not pl.get("id"):
                continue
            pid = pl["id"]
            if pid not in seen_ids:
                seen_ids.add(pid)
                # Quick check: if follower count is already present in search
                # result and clearly too small, skip the full fetch
                fl = (pl.get("followers") or {}).get("total", 0) or 0
                if fl > 0 and fl < MIN_FOLLOWERS:
                    continue
                candidate_ids.append(pid)

        if len(items) < SEARCH_LIMIT:
            break
        time.sleep(0.2)

    log.info(f"  [{keyword}] {len(candidate_ids)} candidates from search")

    if not candidate_ids:
        return []

    # Phase 2: fetch full playlist objects for follower counts
    # Cap to FOLLOWER_FETCH_LIMIT to avoid excessive API calls
    to_fetch = candidate_ids[:FOLLOWER_FETCH_LIMIT]
    found: dict[str, dict] = {}

    for pl_id in to_fetch:
        full = _get(
            f"https://api.spotify.com/v1/playlists/{pl_id}",
            params={"fields": "id,name,description,followers,owner"},
        )
        if not full:
            time.sleep(0.1)
            continue

        followers = full.get("followers", {}).get("total", 0) or 0
        if followers < MIN_FOLLOWERS:
            time.sleep(0.1)
            continue

        found[pl_id] = {
            "id":        pl_id,
            "name":      full.get("name", ""),
            "followers": followers,
            "owner":     (full.get("owner") or {}).get("id", ""),
        }
        log.info(
            f"  [{keyword}] '{full.get('name', '')}' "
            f"({followers:,} followers) ✓"
        )
        time.sleep(0.1)

    return list(found.values())


def fetch_playlist_tracks(playlist_id: str) -> list[dict]:
    """
    Fetch up to TRACKS_PER_PLAYLIST tracks.
    Returns empty list on 403 (logged clearly by _get).
    """
    tracks = []
    url    = f"https://api.spotify.com/v1/playlists/{playlist_id}/tracks"
    params = {
        "fields": "next,items(track(id,name,artists,album,external_ids))",
        "limit":  50,
        "offset": 0,
    }

    while url and len(tracks) < TRACKS_PER_PLAYLIST:
        data = _get(url, params=params)
        if not data:
            break

        for item in data.get("items", []) or []:
            track = item.get("track")
            if not track or not track.get("id"):
                continue
            artists = track.get("artists", [])
            if not artists:
                continue
            tracks.append({
                "spotify_id":   track["id"],
                "title":        track.get("name", "").strip(),
                "artist":       artists[0].get("name", "").strip(),
                "artist_id":    artists[0].get("id", ""),
                "isrc":         track.get("external_ids", {}).get("isrc"),
                "release_date": _normalize_release_date(
                    track.get("album", {}).get("release_date")
                ),
            })

        url    = data.get("next")
        params = None
        time.sleep(0.1)

    return tracks[:TRACKS_PER_PLAYLIST]


# ── Catalog upserts ───────────────────────────────────────────────────────────

def fetch_artist_genres(artist_spotify_id: str) -> list[str]:
    data = _get(f"https://api.spotify.com/v1/artists/{artist_spotify_id}")
    return data.get("genres", []) if data else []


def upsert_track(cur, track: dict) -> Optional[str]:
    """Upsert artist + song. Returns song_id or None."""
    title_norm  = normalize(track["title"])
    artist_norm = normalize(track["artist"])
    if not title_norm or not artist_norm:
        return None

    artist_key = track["artist_id"] or f"unknown_{artist_norm}"

    cur.execute(
        "SELECT id, genre_tags FROM artists WHERE spotify_artist_id = %s",
        (artist_key,)
    )
    existing_artist = cur.fetchone()

    if existing_artist:
        artist_id = str(existing_artist["id"])
        genres    = existing_artist["genre_tags"] or []
    else:
        genres = fetch_artist_genres(track["artist_id"]) if track["artist_id"] else []
        time.sleep(0.1)
        cur.execute("""
            INSERT INTO artists (name, name_normalized, spotify_artist_id, genre_tags)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (spotify_artist_id) DO UPDATE SET
                name       = EXCLUDED.name,
                genre_tags = CASE
                    WHEN array_length(EXCLUDED.genre_tags, 1) > 0
                    THEN EXCLUDED.genre_tags
                    ELSE artists.genre_tags
                END,
                updated_at = NOW()
            RETURNING id
        """, (track["artist"], artist_norm, artist_key, genres))
        row = cur.fetchone()
        if row:
            artist_id = str(row["id"])
        else:
            cur.execute(
                "SELECT id FROM artists WHERE spotify_artist_id = %s", (artist_key,)
            )
            artist_id = str(cur.fetchone()["id"])

    cur.execute("""
        INSERT INTO songs (
            title, title_normalized, artist_id,
            spotify_track_id, isrc, genre_tags, release_date
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (spotify_track_id) DO NOTHING
        RETURNING id
    """, (
        track["title"], title_norm, artist_id,
        track["spotify_id"], track["isrc"], genres, track["release_date"],
    ))
    row = cur.fetchone()
    if row:
        return str(row["id"])

    cur.execute(
        "SELECT id FROM songs WHERE spotify_track_id = %s", (track["spotify_id"],)
    )
    row = cur.fetchone()
    return str(row["id"]) if row else None


# ── Seeded-playlist tracking ──────────────────────────────────────────────────

def ensure_seeded_playlists_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS seeded_playlists (
                playlist_id   TEXT PRIMARY KEY,
                name          TEXT,
                followers     INTEGER,
                last_seeded   TIMESTAMPTZ DEFAULT NOW(),
                tracks_added  INTEGER DEFAULT 0
            )
        """)
    conn.commit()


def already_seeded_today(cur, playlist_id: str) -> bool:
    cur.execute("""
        SELECT 1 FROM seeded_playlists
        WHERE playlist_id = %s
          AND last_seeded >= NOW() - INTERVAL '23 hours'
    """, (playlist_id,))
    return cur.fetchone() is not None


def mark_seeded(cur, playlist: dict, tracks_added: int):
    cur.execute("""
        INSERT INTO seeded_playlists (playlist_id, name, followers, last_seeded, tracks_added)
        VALUES (%s, %s, %s, NOW(), %s)
        ON CONFLICT (playlist_id) DO UPDATE SET
            last_seeded  = NOW(),
            followers    = EXCLUDED.followers,
            tracks_added = EXCLUDED.tracks_added
    """, (playlist["id"], playlist["name"], playlist["followers"], tracks_added))


# ── Main ──────────────────────────────────────────────────────────────────────

def run(user_token: Optional[str] = None, conn=None):
    """
    Main entry point.

    When called from spotify_collector.run() (normal case):
      - user_token: user-level token extracted from the Playwright browser
      - conn:       existing DB connection (not closed here)

    When run standalone (python -m collectors.spotify_playlist_seeder):
      - Both default to None; connection and token acquired internally.
    """
    # Inject user token so get_token() returns it immediately
    if user_token:
        _token_cache["injected_token"] = user_token
        _token_cache["access_token"]   = user_token
        _token_cache["expires_at"]     = time.time() + 3600
        log.info("Playlist seeder: using injected user token from Spotify collector")
    else:
        log.info("Playlist seeder: no injected token — will attempt sp_dc / client credentials")

    owns_conn = conn is None
    if owns_conn:
        conn = psycopg2.connect(DB_URL)
        conn.autocommit = False
        psycopg2.extras.register_uuid()

    ensure_seeded_playlists_table(conn)

    # Warn early if we probably can't read tracks
    if not _has_user_token():
        log.warning(
            "No user-level token available. Playlist discovery will work but "
            "track fetching will likely 403. To fix: ensure SPOTIFY_SP_DC is set "
            "or the Spotify collector's Playwright session is providing a token."
        )

    # ── Discover playlists ────────────────────────────────────────────────────
    all_playlists: dict[str, dict] = {}
    for keyword in SEARCH_KEYWORDS:
        log.info(f"Searching playlists for keyword: '{keyword}'")
        results = search_playlists(keyword)
        for pl in results:
            all_playlists[pl["id"]] = pl
        log.info(f"  '{keyword}': {len(results)} qualifying playlists found")
        time.sleep(0.5)

    log.info(f"Total unique qualifying playlists: {len(all_playlists)}")

    # Sort by followers desc; cap to MAX_PLAYLISTS_PER_RUN
    ranked = sorted(all_playlists.values(), key=lambda x: x["followers"], reverse=True)
    ranked = ranked[:MAX_PLAYLISTS_PER_RUN]
    log.info(f"Processing top {len(ranked)} playlists this run")

    # ── Seed tracks ───────────────────────────────────────────────────────────
    total_new     = 0
    playlists_run = 0
    first_403     = True   # flag to give a one-time clear explanation

    for pl in ranked:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            if already_seeded_today(cur, pl["id"]):
                log.info(f"Skipping '{pl['name']}' — seeded in last 23h")
                continue

        log.info(f"Seeding '{pl['name']}' ({pl['followers']:,} followers) …")
        tracks = fetch_playlist_tracks(pl["id"])

        if not tracks:
            if first_403:
                log.warning(
                    "No tracks returned. If you see 403 warnings above, the token "
                    "doesn't have playlist read permission. Check that SPOTIFY_SP_DC "
                    "is valid or that the Spotify collector is successfully injecting "
                    "a user token."
                )
                first_403 = False
            # Still mark as seeded so we don't hammer it on every run
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                mark_seeded(cur, pl, 0)
            conn.commit()
            continue

        first_403 = False  # at least one playlist returned tracks — token is working
        new_this_playlist = 0

        for track in tracks:
            if not track["title"] or not track["artist"] or not track["spotify_id"]:
                continue
            try:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    song_id = upsert_track(cur, track)
                    if song_id:
                        new_this_playlist += 1
                conn.commit()
            except Exception as e:
                conn.rollback()
                log.warning(f"  Failed upserting '{track['title']}': {e}")

        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            mark_seeded(cur, pl, new_this_playlist)
        conn.commit()

        total_new     += new_this_playlist
        playlists_run += 1
        log.info(f"  ✓ {new_this_playlist} tracks upserted")
        time.sleep(0.5)

    if owns_conn:
        conn.close()

    log.info(
        f"Playlist seeder complete — "
        f"{playlists_run} playlists processed, {total_new} songs upserted"
    )


if __name__ == "__main__":
    run()
