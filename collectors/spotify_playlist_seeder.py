"""
Spotify Playlist Seeder
-----------------------
Searches Spotify for playlists matching discovery keywords ("new",
"hot", "hottest", "trending"), filters to those with ≥ 50k followers,
pulls their tracks, and upserts every song into the catalog.

No signal_events are written — this is purely catalog seeding so that
other collectors (Reddit, Shazam, YouTube, TikTok) can match against
a much wider song universe.

Strategy:
  1. Search for each keyword (searches title + description)
  2. Paginate up to 200 results per keyword
  3. Keep playlists with followers >= MIN_FOLLOWERS
  4. Deduplicate playlists by Spotify playlist ID across keywords
  5. Fetch tracks from each qualifying playlist (up to TRACKS_PER_PLAYLIST)
  6. Upsert artist + song; pull full metadata for new songs
  7. Record seeded playlists in seeded_playlists table to skip on re-runs

Schedule: daily at 07:00 UTC (before Spotify collector at 08:00)
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
SPOTIFY_SP_DC  = os.environ.get("SPOTIFY_SP_DC")   # session cookie → user-level token

SEARCH_KEYWORDS   = ["new", "hot", "hottest", "trending"]
MIN_FOLLOWERS     = 50_000
TRACKS_PER_PLAYLIST = 100   # max tracks pulled per playlist
SEARCH_LIMIT      = 10      # results per search page (Spotify playlist search max is 10)
SEARCH_PAGES      = 20      # pages per keyword → up to 200 results per keyword

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
    Get a Spotify access token.
    First checks if an injected user token was provided (from Spotify collector's
    Playwright session). Otherwise tries sp_dc cookie, then client credentials.
    """
    now = time.time()
    if _token_cache.get("expires_at", 0) > now + 30:
        return _token_cache["access_token"]

    # Injected user token from Spotify collector's Playwright session (Option 2)
    if _token_cache.get("injected_token"):
        # Use it directly — treat as valid for 1 hour (it was just extracted)
        return _token_cache["injected_token"]

    # Try sp_dc first — gives user-level access needed for playlist tracks
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

    # Client credentials fallback (metadata only, can't read playlist tracks)
    if not SPOTIFY_CLIENT or not SPOTIFY_SECRET:
        log.error("Missing SPOTIFY_CLIENT_ID / SPOTIFY_CLIENT_SECRET")
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
        log.info("Using client credentials token (playlist tracks may be 403)")
        return _token_cache["access_token"]
    except Exception as e:
        log.error(f"Spotify token error: {e}")
        return None

def _get(url: str, params: dict = None) -> Optional[dict]:
    """Authenticated GET with rate-limit handling."""
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
                    # Injected token expired mid-run; fall through to sp_dc/client-creds
                    log.warning("Injected user token returned 401 — falling back to client credentials")
                token = get_token()
                continue
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            log.warning(f"GET {url} attempt {attempt+1} failed: {e}")
            time.sleep(2)
    return None

# ── Playlist discovery ────────────────────────────────────────────────────────

def search_playlists(keyword: str) -> list[dict]:
    """
    Search Spotify for playlists matching keyword (title + description).
    Returns playlists with followers >= MIN_FOLLOWERS.
    """
    found: dict[str, dict] = {}

    for page in range(SEARCH_PAGES):
        offset = page * SEARCH_LIMIT
        data = _get(
            "https://api.spotify.com/v1/search",
            params={
                "q":      keyword,
                "type":   "playlist",
                "limit":  SEARCH_LIMIT,
                "offset": offset,
            },
        )
        if not data:
            break

        items = data.get("playlists", {}).get("items", [])
        if not items:
            break

        for pl in items:
            if not pl or not pl.get("id"):
                continue

            followers = pl.get("followers", {}).get("total", 0)

            # followers may be 0/null in search results — fetch full playlist
            # only if it looks promising based on name/owner first
            pl_id = pl["id"]
            if pl_id in found:
                continue

            # Fetch full playlist object to get accurate follower count
            full = _get(f"https://api.spotify.com/v1/playlists/{pl_id}",
                        params={"fields": "id,name,description,followers,owner"})
            if not full:
                continue

            followers = full.get("followers", {}).get("total", 0)
            if followers < MIN_FOLLOWERS:
                continue

            found[pl_id] = {
                "id":          pl_id,
                "name":        full.get("name", ""),
                "description": full.get("description", ""),
                "followers":   followers,
                "owner":       full.get("owner", {}).get("id", ""),
            }
            log.info(
                f"  [{keyword}] '{full.get('name', '')}' "
                f"by {full.get('owner', {}).get('display_name', '?')} "
                f"— {followers:,} followers"
            )
            time.sleep(0.1)

        # Stop paging if we got fewer results than the limit
        if len(items) < SEARCH_LIMIT:
            break
        time.sleep(0.2)

    return list(found.values())


def fetch_playlist_tracks(playlist_id: str) -> list[dict]:
    """Fetch up to TRACKS_PER_PLAYLIST tracks from a playlist."""
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

        for item in data.get("items", []):
            track = item.get("track")
            if not track or not track.get("id"):
                continue
            artists = track.get("artists", [])
            if not artists:
                continue
            tracks.append({
                "spotify_id":  track["id"],
                "title":       track.get("name", "").strip(),
                "artist":      artists[0].get("name", "").strip(),
                "artist_id":   artists[0].get("id", ""),
                "isrc":        track.get("external_ids", {}).get("isrc"),
                "release_date": _normalize_release_date(
                    track.get("album", {}).get("release_date")
                ),
            })

        url    = data.get("next")
        params = None   # next URL already has params baked in
        time.sleep(0.1)

    return tracks[:TRACKS_PER_PLAYLIST]


# ── Catalog upserts ───────────────────────────────────────────────────────────

def fetch_artist_genres(artist_spotify_id: str) -> list[str]:
    data = _get(f"https://api.spotify.com/v1/artists/{artist_spotify_id}")
    return data.get("genres", []) if data else []


def upsert_track(cur, track: dict) -> Optional[str]:
    """
    Upsert artist + song. Returns song_id or None on error.
    Fetches artist genres for new artists.
    """
    title_norm  = normalize(track["title"])
    artist_norm = normalize(track["artist"])
    if not title_norm or not artist_norm:
        return None

    # ── Artist ────────────────────────────────────────────────────────────────
    artist_key = track["artist_id"] or f"unknown_{artist_norm}"

    # Check if artist exists already
    cur.execute(
        "SELECT id, genre_tags FROM artists WHERE spotify_artist_id = %s",
        (artist_key,)
    )
    existing_artist = cur.fetchone()

    if existing_artist:
        artist_id = str(existing_artist["id"])
        genres    = existing_artist["genre_tags"] or []
    else:
        # New artist — fetch genres
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

    # ── Song ──────────────────────────────────────────────────────────────────
    cur.execute("""
        INSERT INTO songs (
            title, title_normalized, artist_id,
            spotify_track_id, isrc, genre_tags, release_date
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (spotify_track_id) DO NOTHING
        RETURNING id
    """, (
        track["title"],
        title_norm,
        artist_id,
        track["spotify_id"],
        track["isrc"],
        genres,
        track["release_date"],
    ))
    row = cur.fetchone()
    if row:
        return str(row["id"])

    # Already existed
    cur.execute(
        "SELECT id FROM songs WHERE spotify_track_id = %s", (track["spotify_id"],)
    )
    row = cur.fetchone()
    return str(row["id"]) if row else None


# ── Seeded-playlist tracking ──────────────────────────────────────────────────

def ensure_seeded_playlists_table(conn):
    """Create tracking table if it doesn't exist yet."""
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

    When called from the Spotify collector (Option 2), pass:
      - user_token: valid user-level Spotify access token extracted from
                    the collector's Playwright browser session
      - conn:       an open psycopg2 connection to reuse (not closed here)

    When run standalone, both default to None and are set up internally.
    """
    # Inject user token so get_token() uses it immediately
    if user_token:
        _token_cache["injected_token"] = user_token
        # Give it a generous TTL — it was just fetched
        _token_cache["expires_at"] = time.time() + 3600
        _token_cache["access_token"] = user_token
        log.info("Playlist seeder using injected user token from Spotify collector")

    owns_conn = conn is None
    if owns_conn:
        conn = psycopg2.connect(DB_URL)
        conn.autocommit = False
        psycopg2.extras.register_uuid()

    ensure_seeded_playlists_table(conn)

    # ── Discover playlists ────────────────────────────────────────────────────
    all_playlists: dict[str, dict] = {}
    for keyword in SEARCH_KEYWORDS:
        log.info(f"Searching playlists for keyword: '{keyword}'")
        results = search_playlists(keyword)
        for pl in results:
            all_playlists[pl["id"]] = pl
        log.info(f"  '{keyword}': {len(results)} qualifying playlists found")
        time.sleep(0.5)

    log.info(f"Total unique playlists to seed: {len(all_playlists)}")

    # ── Seed tracks ───────────────────────────────────────────────────────────
    total_new     = 0
    total_skipped = 0
    playlists_run = 0

    for pl in sorted(all_playlists.values(), key=lambda x: x["followers"], reverse=True):
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            if already_seeded_today(cur, pl["id"]):
                log.info(f"Skipping '{pl['name']}' — seeded recently")
                continue

        log.info(
            f"Seeding '{pl['name']}' ({pl['followers']:,} followers) …"
        )
        tracks = fetch_playlist_tracks(pl["id"])
        if not tracks:
            log.warning(f"  No tracks returned for '{pl['name']}'")
            continue

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
        log.info(f"  ✓ {new_this_playlist} tracks upserted from '{pl['name']}'")
        time.sleep(0.5)

    if owns_conn:
        conn.close()
    log.info(
        f"Playlist seeder complete — "
        f"{playlists_run} playlists, {total_new} songs upserted, "
        f"{total_skipped} skipped"
    )


if __name__ == "__main__":
    run()
