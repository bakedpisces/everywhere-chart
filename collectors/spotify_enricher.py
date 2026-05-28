"""
Spotify Enricher
----------------
Retroactively upgrades catalog entries that were created by the YouTube,
TikTok, or Shazam collectors with placeholder IDs (yt_*, tiktok_*,
shazam_*) to real Spotify track IDs.

For each un-enriched song it:
  1. Searches Spotify for "track:<title> artist:<artist>"
  2. Picks the best hit using title + artist similarity (pg_trgm-style
     comparison in Python — no DB round-trip per candidate)
  3. If confidence ≥ 0.80, updates songs.spotify_track_id,
     songs.isrc, songs.genre_tags, songs.release_date and upgrades
     the artist record with the real spotify_artist_id / genre_tags

Songs that fail enrichment are skipped and retried on the next run
(they keep their placeholder ID, which acts as a "not yet enriched" flag).

Schedule: daily, after all platform collectors have run (e.g. 11:00 UTC).
Can also be run manually: python -m collectors.spotify_enricher
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
log = logging.getLogger("spotify_enricher")

DB_URL         = os.environ["DATABASE_URL"]
SPOTIFY_CLIENT = os.environ.get("SPOTIFY_CLIENT_ID")
SPOTIFY_SECRET = os.environ.get("SPOTIFY_CLIENT_SECRET")

# Minimum combined title+artist similarity to accept a Spotify search hit
MIN_MATCH_CONFIDENCE = 0.80

# Batch size — how many songs to process per run
BATCH_SIZE = 200

# Prefixes that mark placeholder IDs we want to upgrade
PLACEHOLDER_PREFIXES = ("yt_", "tiktok_", "shazam_")

# ── Helpers ───────────────────────────────────────────────────────────────────

def normalize(s: str) -> str:
    return re.sub(r"[^\w\s]", "", s.lower().strip())


def clean_artist_name(artist: str) -> str:
    """Strip YouTube channel suffixes that pollute Spotify searches."""
    artist = re.sub(r"\s*-\s*topic$",  "", artist, flags=re.IGNORECASE)
    artist = re.sub(r"vevo$",          "", artist, flags=re.IGNORECASE)
    artist = re.sub(r"\s*official$",   "", artist, flags=re.IGNORECASE)
    return artist.strip()


def clean_title_for_search(title: str) -> str:
    """Strip YouTube video noise from song titles before searching Spotify."""
    # Remove trailing parenthetical noise
    title = re.sub(
        r'\s*[\(\[]\s*(?:official\s*(?:music\s*)?(?:video|audio|mv)?|'
        r'lyrics?|visualizer|4k|hd|explicit|clean|live|music\s*video|'
        r'version|letra|dir\.?[^)\]]*)\s*[\)\]]',
        "", title, flags=re.IGNORECASE,
    ).strip()
    # If "Artist - Song" pattern, take only the song part
    parts = re.split(r"\s*[-–—]\s*", title, maxsplit=1)
    if len(parts) == 2 and len(parts[1]) > 2:
        title = parts[1]
    # Strip feat. suffix
    title = re.sub(r"\s+(?:feat\.?|ft\.?|featuring)\s+.*$", "", title, flags=re.IGNORECASE)
    return title.strip()


def title_similarity(a: str, b: str) -> float:
    """
    Lightweight trigram-style similarity between two normalized strings.
    Not as sophisticated as pg_trgm but fast enough for candidate ranking.
    """
    if not a or not b:
        return 0.0
    set_a = {a[i:i+3] for i in range(len(a) - 2)} if len(a) >= 3 else {a}
    set_b = {b[i:i+3] for i in range(len(b) - 2)} if len(b) >= 3 else {b}
    if not set_a or not set_b:
        return 1.0 if a == b else 0.0
    return len(set_a & set_b) / len(set_a | set_b)


# ── Spotify API ───────────────────────────────────────────────────────────────

_token_cache: dict = {}


def get_token() -> Optional[str]:
    if not SPOTIFY_CLIENT or not SPOTIFY_SECRET:
        return None
    now = time.time()
    if _token_cache.get("expires_at", 0) > now + 30:
        return _token_cache["access_token"]
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
        _token_cache["expires_at"]   = time.time() + data["expires_in"]
        return _token_cache["access_token"]
    except Exception as e:
        log.warning(f"Spotify token error: {e}")
        return None


def search_spotify(title: str, artist: str) -> Optional[dict]:
    """
    Search Spotify for a track. Returns the best-matching track dict or None.
    Uses the 'track:"title" artist:"artist"' query for precision.
    Falls back to a looser query if the strict search returns nothing.
    """
    token = get_token()
    if not token:
        return None

    headers = {"Authorization": f"Bearer {token}"}

    # Clean both title and artist before searching
    search_title  = clean_title_for_search(title)
    search_artist = clean_artist_name(artist)

    title_n  = normalize(title)        # original for similarity scoring
    artist_n = normalize(search_artist)

    def _search(q: str) -> list:
        try:
            resp = requests.get(
                "https://api.spotify.com/v1/search",
                headers=headers,
                params={"q": q, "type": "track", "limit": 5},
                timeout=10,
            )
            if resp.status_code == 429:
                wait = int(resp.headers.get("Retry-After", 10))
                log.warning(f"Spotify rate limit — waiting {wait}s")
                time.sleep(wait)
                return []
            resp.raise_for_status()
            return resp.json().get("tracks", {}).get("items", [])
        except Exception as e:
            log.warning(f"Spotify search error: {e}")
            return []

    # Strict query first (cleaned title + artist)
    items = _search(f'track:"{search_title}" artist:"{search_artist}"')

    # Looser fallback
    if not items:
        items = _search(f"{search_title} {search_artist}")

    if not items:
        return None

    # Score each candidate and pick the best
    # Compare using cleaned title so "ITZY - Motto (Official Video)" → "Motto" scores correctly
    search_title_n = normalize(search_title)
    best_item  = None
    best_score = 0.0
    for item in items:
        item_title  = normalize(item.get("name", ""))
        item_artist = normalize(item.get("artists", [{}])[0].get("name", ""))
        t_sim = max(
            title_similarity(title_n, item_title),
            title_similarity(search_title_n, item_title),
        )
        a_sim = title_similarity(artist_n, item_artist)
        score = (t_sim + a_sim) / 2
        if score > best_score:
            best_score = score
            best_item  = item

    if best_item and best_score >= MIN_MATCH_CONFIDENCE:
        return {"item": best_item, "confidence": round(best_score, 3)}

    return None


def fetch_artist_genres(artist_id: str, token: str) -> list:
    """Fetch genre tags for a Spotify artist ID."""
    try:
        resp = requests.get(
            f"https://api.spotify.com/v1/artists/{artist_id}",
            headers={"Authorization": f"Bearer {token}"},
            timeout=10,
        )
        if resp.status_code == 200:
            return resp.json().get("genres", [])
    except Exception:
        pass
    return []


def _normalize_release_date(raw: Optional[str]) -> Optional[str]:
    if not raw:
        return None
    parts = raw.split("-")
    if len(parts) == 1:
        return f"{parts[0]}-01-01"
    if len(parts) == 2:
        return f"{parts[0]}-{parts[1]}-01"
    return raw


# ── DB helpers ────────────────────────────────────────────────────────────────

def load_unenriched(conn, batch_size: int) -> list[dict]:
    """Return songs with placeholder IDs that haven't been enriched yet."""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        placeholders = " OR ".join(
            f"s.spotify_track_id LIKE '{p}%%'" for p in PLACEHOLDER_PREFIXES
        )
        cur.execute(f"""
            SELECT
                s.id::text     AS song_id,
                s.title,
                s.title_normalized,
                s.spotify_track_id AS placeholder_id,
                a.id::text     AS artist_id,
                a.name         AS artist_name,
                a.name_normalized,
                a.spotify_artist_id
            FROM songs s
            JOIN artists a ON s.artist_id = a.id
            WHERE ({placeholders})
            ORDER BY s.created_at DESC
            LIMIT %s
        """, (batch_size,))
        return [dict(r) for r in cur.fetchall()]


def upgrade_song(conn, row: dict, spotify_item: dict, confidence: float):
    """
    Update song + artist records with real Spotify data.
    Merges any existing signal_events to the new song row if a collision
    occurs (another catalog entry with the real spotify_track_id already exists).
    """
    track      = spotify_item
    track_id   = track["id"]
    album      = track.get("album", {})
    artists    = track.get("artists", [])
    isrc       = track.get("external_ids", {}).get("isrc")
    rel_date   = _normalize_release_date(album.get("release_date"))

    token = get_token()
    genres: list = []
    real_artist_spotify_id: Optional[str] = None
    if artists:
        real_artist_spotify_id = artists[0]["id"]
        if token:
            genres = fetch_artist_genres(real_artist_spotify_id, token)
            time.sleep(0.1)

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        # Check if a song with this real spotify_track_id already exists
        cur.execute(
            "SELECT id::text FROM songs WHERE spotify_track_id = %s",
            (track_id,)
        )
        existing = cur.fetchone()

        if existing and existing["id"] != row["song_id"]:
            # Real catalog entry already exists — remap signals and delete placeholder
            real_song_id = existing["id"]
            cur.execute(
                "UPDATE signal_events SET song_id = %s::uuid WHERE song_id = %s::uuid",
                (real_song_id, row["song_id"])
            )
            cur.execute("DELETE FROM songs WHERE id = %s::uuid", (row["song_id"],))
            log.info(
                f"Merged placeholder {row['placeholder_id']} → existing {track_id} "
                f"({row['title']})"
            )
        else:
            # Upgrade the placeholder song in place
            cur.execute("""
                UPDATE songs SET
                    spotify_track_id = %s,
                    isrc             = COALESCE(%s, isrc),
                    genre_tags       = CASE WHEN %s::text[] <> '{}' THEN %s ELSE genre_tags END,
                    release_date     = COALESCE(%s::date, release_date),
                    updated_at       = NOW()
                WHERE id = %s::uuid
            """, (
                track_id,
                isrc,
                genres, genres,
                rel_date,
                row["song_id"],
            ))

            # Upgrade artist if we now have a real spotify_artist_id
            if real_artist_spotify_id and row["spotify_artist_id"].startswith(PLACEHOLDER_PREFIXES):
                # Check for artist collision
                cur.execute(
                    "SELECT id::text FROM artists WHERE spotify_artist_id = %s",
                    (real_artist_spotify_id,)
                )
                existing_artist = cur.fetchone()
                if existing_artist and existing_artist["id"] != row["artist_id"]:
                    # Real artist exists — point song to real artist, delete placeholder
                    cur.execute(
                        "UPDATE songs SET artist_id = %s::uuid WHERE id = %s::uuid",
                        (existing_artist["id"], row["song_id"])
                    )
                    cur.execute(
                        "DELETE FROM artists WHERE id = %s::uuid", (row["artist_id"],)
                    )
                else:
                    cur.execute("""
                        UPDATE artists SET
                            spotify_artist_id = %s,
                            genre_tags = CASE WHEN %s::text[] <> '{}' THEN %s ELSE genre_tags END,
                            updated_at = NOW()
                        WHERE id = %s::uuid
                    """, (
                        real_artist_spotify_id,
                        genres, genres,
                        row["artist_id"],
                    ))

            log.info(
                f"Enriched: {row['title']} — {row['artist_name']} "
                f"[{row['placeholder_id']} → {track_id}] (conf={confidence})"
            )

    conn.commit()


# ── Label backfill ────────────────────────────────────────────────────────────

def load_label_missing(conn, batch_size: int) -> list[dict]:
    """Songs with a real Spotify ID but no label data yet."""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        placeholders = " AND ".join(
            f"s.spotify_track_id NOT LIKE '{p}%%'" for p in PLACEHOLDER_PREFIXES
        )
        cur.execute(f"""
            SELECT s.id::text AS song_id, s.spotify_track_id, s.title,
                   a.name AS artist_name
            FROM songs s
            JOIN artists a ON a.id = s.artist_id
            WHERE s.label IS NULL
              AND s.spotify_track_id IS NOT NULL
              AND {placeholders}
            ORDER BY s.created_at DESC
            LIMIT %s
        """, (batch_size,))
        return [dict(r) for r in cur.fetchall()]


def backfill_labels(batch_size: int = BATCH_SIZE):
    """
    Fetch label + label_tier for songs that have a real Spotify track ID but
    no label data. Uses batched API calls:
      - /v1/tracks?ids=...  (50 per call) → extract album IDs
      - /v1/albums?ids=...  (20 per call) → extract label strings
    Then classifies and writes label + label_tier to songs table.
    """
    from collectors.label_utils import classify_label_tier

    conn = psycopg2.connect(DB_URL)
    conn.autocommit = False
    psycopg2.extras.register_uuid()

    songs = load_label_missing(conn, batch_size)
    log.info(f"Label backfill: {len(songs)} songs missing label data")

    if not songs:
        log.info("Nothing to backfill.")
        conn.close()
        return

    token = get_token()
    if not token:
        log.error("No Spotify token — cannot backfill labels")
        conn.close()
        return

    headers = {"Authorization": f"Bearer {token}"}

    # Index by spotify_track_id for easy lookup
    by_track_id = {s["spotify_track_id"]: s for s in songs}
    track_ids   = list(by_track_id.keys())

    # ── Step 1: batch-fetch tracks to get album IDs (50 per call) ─────────
    album_id_for_song: dict[str, str] = {}   # song_id → album_id

    for i in range(0, len(track_ids), 50):
        batch = track_ids[i:i+50]
        try:
            resp = requests.get(
                "https://api.spotify.com/v1/tracks",
                headers=headers,
                params={"ids": ",".join(batch)},
                timeout=15,
            )
            if resp.status_code == 429:
                wait = int(resp.headers.get("Retry-After", 10))
                log.warning(f"Rate limited — waiting {wait}s")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            for track in resp.json().get("tracks") or []:
                if not track:
                    continue
                tid    = track.get("id")
                row    = by_track_id.get(tid)
                alb_id = (track.get("album") or {}).get("id")
                if row and alb_id:
                    album_id_for_song[row["song_id"]] = alb_id
        except Exception as e:
            log.warning(f"Track batch fetch failed: {e}")
        time.sleep(0.2)

    log.info(f"  Got album IDs for {len(album_id_for_song)}/{len(songs)} songs")

    # ── Step 2: batch-fetch albums for label strings (20 per call) ────────
    label_for_album: dict[str, str] = {}   # album_id → label string

    unique_album_ids = list(set(album_id_for_song.values()))
    for i in range(0, len(unique_album_ids), 20):
        batch = unique_album_ids[i:i+20]
        try:
            resp = requests.get(
                "https://api.spotify.com/v1/albums",
                headers=headers,
                params={"ids": ",".join(batch)},
                timeout=15,
            )
            if resp.status_code == 429:
                wait = int(resp.headers.get("Retry-After", 10))
                log.warning(f"Rate limited — waiting {wait}s")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            for album in resp.json().get("albums") or []:
                if not album:
                    continue
                aid   = album.get("id")
                label = album.get("label") or None
                if aid:
                    label_for_album[aid] = label
        except Exception as e:
            log.warning(f"Album batch fetch failed: {e}")
        time.sleep(0.2)

    log.info(f"  Got labels for {len(label_for_album)}/{len(unique_album_ids)} albums")

    # ── Step 3: write label + label_tier to songs table ───────────────────
    updated = skipped = 0
    with conn.cursor() as cur:
        for song in songs:
            alb_id = album_id_for_song.get(song["song_id"])
            if not alb_id:
                skipped += 1
                continue
            label      = label_for_album.get(alb_id)
            label_tier = classify_label_tier(label)
            try:
                cur.execute("""
                    UPDATE songs SET label = %s, label_tier = %s
                    WHERE id = %s::uuid AND label IS NULL
                """, (label, label_tier, song["song_id"]))
                updated += cur.rowcount
            except Exception as e:
                log.warning(f"  Write failed for '{song['title']}': {e}")
                skipped += 1

    conn.commit()
    conn.close()
    log.info(
        f"Label backfill complete — {updated} updated, {skipped} skipped "
        f"(no album ID or write error)"
    )


# ── Genre backfill ────────────────────────────────────────────────────────────

def backfill_genres(batch_size: int = BATCH_SIZE):
    """
    Fetch genre_tags for artists that have an empty or null genre list.
    Uses the /v1/artists?ids=... batch endpoint (50 per call).
    Updates both the artists table and the songs table (genre_tags is
    denormalised onto songs for query convenience).
    """
    conn = psycopg2.connect(DB_URL)
    conn.autocommit = False
    psycopg2.extras.register_uuid()

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT id::text AS artist_id, name, spotify_artist_id
            FROM artists
            WHERE (genre_tags IS NULL OR genre_tags = '{}')
              AND spotify_artist_id IS NOT NULL
              AND spotify_artist_id NOT LIKE 'unknown_%%'
            ORDER BY created_at DESC
            LIMIT %s
        """, (batch_size,))
        artists = [dict(r) for r in cur.fetchall()]

    log.info(f"Genre backfill: {len(artists)} artists missing genre data")
    if not artists:
        log.info("Nothing to backfill.")
        conn.close()
        return

    token = get_token()
    if not token:
        log.error("No Spotify token — cannot backfill genres")
        conn.close()
        return

    headers = {"Authorization": f"Bearer {token}"}
    by_spotify_id = {a["spotify_artist_id"]: a for a in artists}

    updated = skipped = 0

    for i in range(0, len(artists), 50):
        batch_ids = [a["spotify_artist_id"] for a in artists[i:i+50]]
        try:
            resp = requests.get(
                "https://api.spotify.com/v1/artists",
                headers=headers,
                params={"ids": ",".join(batch_ids)},
                timeout=15,
            )
            if resp.status_code == 429:
                wait = int(resp.headers.get("Retry-After", 10))
                log.warning(f"Rate limited — waiting {wait}s")
                time.sleep(wait)
                continue
            resp.raise_for_status()

            for artist_obj in resp.json().get("artists") or []:
                if not artist_obj:
                    skipped += 1
                    continue
                sid    = artist_obj["id"]
                genres = artist_obj.get("genres") or []
                row    = by_spotify_id.get(sid)
                if not row or not genres:
                    skipped += 1
                    continue

                with conn.cursor() as cur:
                    # Update artist
                    cur.execute("""
                        UPDATE artists SET genre_tags = %s, updated_at = NOW()
                        WHERE id = %s::uuid
                    """, (genres, row["artist_id"]))
                    # Propagate to songs (denormalised copy)
                    cur.execute("""
                        UPDATE songs SET genre_tags = %s
                        WHERE artist_id = %s::uuid
                          AND (genre_tags IS NULL OR genre_tags = '{}')
                    """, (genres, row["artist_id"]))
                    updated += 1

        except Exception as e:
            log.warning(f"Artist batch fetch failed: {e}")

        conn.commit()
        time.sleep(0.2)

    conn.close()
    log.info(f"Genre backfill complete — {updated} artists updated, {skipped} skipped")


# ── Main ──────────────────────────────────────────────────────────────────────

def run(batch_size: int = BATCH_SIZE):
    conn = psycopg2.connect(DB_URL)
    conn.autocommit = False
    psycopg2.extras.register_uuid()

    songs = load_unenriched(conn, batch_size)
    log.info(f"Enricher: {len(songs)} placeholder songs to process")

    enriched = 0
    skipped  = 0

    for row in songs:
        try:
            result = search_spotify(row["title"], row["artist_name"])
            if result:
                upgrade_song(conn, row, result["item"], result["confidence"])
                enriched += 1
            else:
                skipped += 1
                log.debug(f"No Spotify match: {row['title']} — {row['artist_name']}")

            time.sleep(0.15)  # ~6 req/s, well under the 180 req/min limit

        except Exception as e:
            conn.rollback()
            log.error(f"Failed enriching '{row['title']}': {e}")
            skipped += 1

    conn.close()
    log.info(f"Enricher complete — {enriched} enriched, {skipped} skipped/not found")


if __name__ == "__main__":
    import sys
    args = sys.argv[1:]

    if args and args[0] == "--labels":
        size = int(args[1]) if len(args) > 1 else BATCH_SIZE
        backfill_labels(size)
    elif args and args[0] == "--genres":
        size = int(args[1]) if len(args) > 1 else BATCH_SIZE
        backfill_genres(size)
    elif args and args[0] == "--all":
        size = int(args[1]) if len(args) > 1 else BATCH_SIZE
        run(size)
        backfill_labels(size)
        backfill_genres(size)
    else:
        size = int(args[0]) if args else BATCH_SIZE
        run(size)
