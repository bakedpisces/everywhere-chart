"""
ScrapeCreators TikTok Usage Collector
--------------------------------------
For songs in the catalog, queries TikTok via the ScrapeCreators API to find
the official sound for each song and record how many TikTok videos use it.

This supplements the TikTok Creative Center collector by covering the full
catalog — not just top-200 trending songs. Every song seeded from Spotify
playlists, Shazam charts, or YouTube can now get a TikTok sound-use signal.

Strategy:
  1. Pull up to MAX_SONGS songs that haven't been looked up recently,
     prioritising songs that already have at least one signal (they matter)
  2. For each song: search "{title} {artist}" on ScrapeCreators keyword search
  3. Among results, find the sound whose title/author best matches the song
  4. Record the sound's user_count as a signal_event (source_platform='tiktok',
     signal_type='sound_use') — same schema as the TikTok Creative Center data

Schedule: daily at 10:00 UTC  (after Spotify seeder at 07:00, Spotify chart at 08:00)
"""

import math
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
log = logging.getLogger("scrapecreators_collector")

DB_URL     = os.environ["DATABASE_URL"]
SC_API_KEY = os.environ.get("SCRAPECREATORS_API_KEY", "")
SC_BASE    = "https://api.scrapecreators.com"

MAX_SONGS       = 1000   # credit budget per run
RECHECK_DAYS    = 7      # skip songs searched within this window
MIN_USER_COUNT  = 10     # ignore sounds with fewer videos (noise)
MATCH_THRESHOLD = 0.40   # minimum token-overlap score to accept a sound

INTENTIONALITY  = 0.80   # matches TikTok Creative Center collector

CHART_NAME      = "tiktok_sound_usage"   # stored in context_snapshot


# ── Normalisation helpers ─────────────────────────────────────────────────────

_NOISE = re.compile(
    r"\b(official|video|audio|lyrics?|hd|4k|mv|music|feat\.?|ft\.?|"
    r"featuring|prod\.?|remix|edit|version|slowed|sped\s*up|reverb)\b",
    re.IGNORECASE,
)
_PUNCT = re.compile(r"[^\w\s]")
_WS    = re.compile(r"\s+")


def _norm(s: str) -> str:
    s = _NOISE.sub(" ", s.lower())
    s = _PUNCT.sub(" ", s)
    return _WS.sub(" ", s).strip()


def _tokens(s: str) -> set[str]:
    return set(t for t in _norm(s).split() if len(t) > 1)


def match_score(catalog_title: str, catalog_artist: str,
                sound_title: str, sound_author: str) -> float:
    """
    Returns a 0–1 score: 60% title-token overlap + 40% artist-token overlap.
    Returns 0 if no title tokens overlap (mandatory).
    """
    ct = _tokens(catalog_title)
    st = _tokens(sound_title)
    if not ct or not st:
        return 0.0

    title_overlap = len(ct & st) / max(len(ct), len(st))
    if title_overlap == 0:
        return 0.0

    ca = _tokens(catalog_artist)
    sa = _tokens(sound_author)
    artist_overlap = len(ca & sa) / max(len(ca), len(sa)) if ca and sa else 0.0

    return 0.6 * title_overlap + 0.4 * artist_overlap


# ── ScrapeCreators API ────────────────────────────────────────────────────────

def search_sounds(title: str, artist: str) -> list[dict]:
    """
    Search TikTok for '{title} {artist}', return deduplicated sounds sorted
    by user_count descending.
    Raises RuntimeError('out_of_credits') or RuntimeError('invalid_api_key')
    on fatal API errors.
    """
    query = f"{title} {artist}"
    try:
        resp = requests.get(
            f"{SC_BASE}/v1/tiktok/search/keyword",
            params={"query": query},
            headers={"x-api-key": SC_API_KEY},
            timeout=20,
        )
        if resp.status_code == 402:
            log.error("ScrapeCreators: out of credits (402). Stopping run.")
            raise RuntimeError("out_of_credits")
        if resp.status_code == 401:
            log.error("ScrapeCreators: invalid API key (401).")
            raise RuntimeError("invalid_api_key")
        if not resp.ok:
            log.warning(f"ScrapeCreators search failed ({resp.status_code}) for '{query}'")
            return []

        # parse_constant=None avoids float precision loss on large TikTok IDs
        data = resp.json()
        items = data.get("search_item_list", [])

        seen: dict[str, dict] = {}
        for item in items:
            music = item.get("aweme_info", {}).get("music", {})
            # IDs may lose precision as floats — convert through string
            mid = str(int(float(str(music.get("id", 0))))) if music.get("id") else ""
            if not mid or mid in seen:
                continue
            uc = int(music.get("user_count", 0))
            if uc < MIN_USER_COUNT:
                continue
            seen[mid] = {
                "sound_id":   mid,
                "title":      music.get("title", ""),
                "author":     music.get("author", ""),
                "user_count": uc,
            }

        return sorted(seen.values(), key=lambda x: x["user_count"], reverse=True)

    except RuntimeError:
        raise
    except Exception as e:
        log.warning(f"ScrapeCreators error for '{query}': {e}")
        return []


def find_best_sound(title: str, artist: str,
                    sounds: list[dict]) -> Optional[dict]:
    """Pick the best-matching sound or None if nothing clears MATCH_THRESHOLD."""
    best, best_score = None, 0.0
    for sound in sounds:
        score = match_score(title, artist, sound["title"], sound["author"])
        if score > best_score:
            best_score, best = score, sound
    if best and best_score >= MATCH_THRESHOLD:
        return best
    return None


# ── Database helpers ──────────────────────────────────────────────────────────

def load_songs(cur, limit: int) -> list[dict]:
    """
    Fetch up to `limit` songs that haven't had a ScrapeCreators lookup in
    the last RECHECK_DAYS days. Songs already on some chart come first.
    """
    cur.execute(f"""
        WITH last_sc AS (
            SELECT song_id, MAX(observed_at) AS last_at
            FROM signal_events
            WHERE source_platform = 'tiktok'
              AND external_id     LIKE 'sc_%%'
            GROUP BY song_id
        )
        SELECT
            s.id               AS song_id,
            s.title,
            s.title_normalized,
            a.name             AS artist,
            a.name_normalized  AS artist_normalized,
            EXISTS (
                SELECT 1 FROM signal_events se
                WHERE se.song_id = s.id
            ) AS has_signal
        FROM songs s
        JOIN artists a ON a.id = s.artist_id
        LEFT JOIN last_sc lc ON lc.song_id = s.id
        WHERE lc.song_id IS NULL
           OR lc.last_at < NOW() - INTERVAL '{RECHECK_DAYS} days'
        ORDER BY has_signal DESC, s.created_at DESC
        LIMIT %s
    """, (limit,))
    return cur.fetchall()


def write_signal(cur, song_id: str, sound: dict, now: datetime):
    """Write a tiktok / sound_use signal for a matched sound."""
    usage = sound["user_count"]
    # Same engagement_multiplier formula as TikTok Creative Center collector
    eng_mult = round(min(2.5, 1 + math.log10(max(usage, 1)) / 6), 3)
    weighted = round(INTENTIONALITY * eng_mult, 4)

    cur.execute("""
        INSERT INTO signal_events (
            observed_at, song_id,
            source_platform, signal_type,
            intentionality_score,
            raw_engagement, engagement_multiplier, weighted_score,
            resolution_confidence, is_home_community,
            external_id, context_snapshot
        )
        VALUES (%s, %s,
                'tiktok', 'sound_use',
                %s,
                %s, %s, %s,
                1.0, FALSE,
                %s, %s)
        ON CONFLICT DO NOTHING
    """, (
        now,
        song_id,
        INTENTIONALITY,
        psycopg2.extras.Json({"usage_count": usage}),
        eng_mult,
        weighted,
        f"sc_{sound['sound_id']}",   # prefix so we can filter SC lookups
        psycopg2.extras.Json({
            "chart_name":   CHART_NAME,
            "sound_id":     sound["sound_id"],
            "sound_title":  sound["title"],
            "sound_author": sound["author"],
            "usage_count":  usage,
        }),
    ))


def write_no_match(cur, song_id: str, now: datetime):
    """Sentinel row so we skip this song for RECHECK_DAYS even with no match."""
    cur.execute("""
        INSERT INTO signal_events (
            observed_at, song_id,
            source_platform, signal_type,
            intentionality_score,
            raw_engagement, engagement_multiplier, weighted_score,
            resolution_confidence, is_home_community,
            external_id, context_snapshot
        )
        VALUES (%s, %s,
                'tiktok', 'sound_use',
                0.0,
                %s, 1.0, 0.0,
                0.0, FALSE,
                'sc_no_match', %s)
        ON CONFLICT DO NOTHING
    """, (
        now,
        song_id,
        psycopg2.extras.Json({"usage_count": 0}),
        psycopg2.extras.Json({"chart_name": CHART_NAME, "result": "no_match"}),
    ))


# ── Main ──────────────────────────────────────────────────────────────────────

def run():
    if not SC_API_KEY:
        log.error("SCRAPECREATORS_API_KEY not set — exiting")
        return

    conn = psycopg2.connect(DB_URL)
    conn.autocommit = False
    psycopg2.extras.register_uuid()

    now = datetime.now(timezone.utc)

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        songs = load_songs(cur, MAX_SONGS)

    log.info(f"ScrapeCreators collector: processing {len(songs)} songs")

    hits = misses = errors = 0

    for song in songs:
        song_id = str(song["song_id"])
        title   = song["title"]
        artist  = song["artist"]
        log.info(f"  Searching: '{title}' — {artist}")

        try:
            sounds = search_sounds(title, artist)
        except RuntimeError as e:
            if str(e) in ("out_of_credits", "invalid_api_key"):
                log.error("Fatal API error — stopping run early")
                break
            sounds = []
            errors += 1

        best = find_best_sound(title, artist, sounds) if sounds else None

        try:
            with conn.cursor() as cur:
                if best:
                    write_signal(cur, song_id, best, now)
                    log.info(
                        f"    ✓ {best['user_count']:>10,} TikTok videos | "
                        f"'{best['title']}' by '{best['author']}'"
                    )
                    hits += 1
                else:
                    write_no_match(cur, song_id, now)
                    reason = f"among {len(sounds)} sounds" if sounds else "no sounds returned"
                    log.info(f"    — no match {reason}")
                    misses += 1
            conn.commit()
        except Exception as e:
            conn.rollback()
            log.warning(f"    DB error for '{title}': {e}")
            errors += 1

        time.sleep(0.3)   # ~3 req/sec

    conn.close()
    log.info(
        f"ScrapeCreators collector complete — "
        f"{hits} hits, {misses} misses, {errors} errors "
        f"out of {hits + misses + errors} songs processed"
    )


if __name__ == "__main__":
    run()
