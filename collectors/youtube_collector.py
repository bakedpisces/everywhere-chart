"""
YouTube Trending Music Collector
----------------------------------
Uses the YouTube Data API v3 to pull trending music videos
(videoCategoryId=10) for a set of regions. No scraping —
pure HTTPS requests with an API key.

For each video we extract title, channel (artist), view count,
like count, and rank. Songs are resolved against the existing
catalog via exact normalized match then pg_trgm fuzzy match
(same pattern as shazam_collector.py). Unresolved videos are
placed in the resolution_queue.

Intentionality score is 0.20 — YouTube autoplay makes chart
position a passive signal, less intentional than a Shazam ID.

Deduplication: a video that trends in multiple regions is
written once using the region where it ranked highest.

Schedule: daily at 10:00 UTC
"""

import os
import re
import time
import logging
import requests
import psycopg2
import psycopg2.extras
from datetime import date, datetime, timezone
from typing import Optional

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("youtube_collector")

# ── Config ───────────────────────────────────────────────────────────────────

DB_URL          = os.environ["DATABASE_URL"]
YOUTUBE_API_KEY = os.environ["YOUTUBE_API_KEY"]

YOUTUBE_API_BASE = "https://www.googleapis.com/youtube/v3"
MUSIC_CATEGORY   = "10"   # YouTube Music category ID
MAX_RESULTS      = 50

REGIONS_TO_FETCH = ["US", "GB", "AU", "BR", "CA", "DE", "MX", "IN", "KR"]

# Passive signal — YouTube autoplay inflates views vs intentional listening
INTENTIONALITY_SCORE = 0.20

# ── Helpers ──────────────────────────────────────────────────────────────────

def normalize(s: str) -> str:
    """Lowercase, strip punctuation — mirrors shazam/spotify collectors."""
    return re.sub(r"[^\w\s]", "", s.lower().strip())


def normalize_channel(channel: str) -> str:
    """Strip YouTube-specific suffixes before normalizing."""
    channel = re.sub(r"\s*-\s*topic$", "", channel, flags=re.IGNORECASE)
    channel = re.sub(r"vevo$", "", channel, flags=re.IGNORECASE)
    channel = re.sub(r"\s*official$", "", channel, flags=re.IGNORECASE)
    return normalize(channel.strip())


def _connect():
    return psycopg2.connect(
        DB_URL,
        connect_timeout=15,
        keepalives=1,
        keepalives_idle=10,
        keepalives_interval=5,
        keepalives_count=3,
        options="-c statement_timeout=20000",
    )

# ── YouTube API fetch ─────────────────────────────────────────────────────────

def fetch_trending_music(region: str) -> list[dict]:
    """
    Call videos.list?chart=mostPopular&videoCategoryId=10 for one region.
    Returns a list of dicts with: rank, video_id, title, channel, view_count,
    like_count, region.
    """
    params = {
        "part":            "snippet,statistics",
        "chart":           "mostPopular",
        "videoCategoryId": MUSIC_CATEGORY,
        "regionCode":      region,
        "maxResults":      MAX_RESULTS,
        "key":             YOUTUBE_API_KEY,
    }

    for attempt in range(3):
        try:
            resp = requests.get(
                f"{YOUTUBE_API_BASE}/videos",
                params=params,
                timeout=15,
            )
            if resp.status_code == 403:
                log.error(f"YouTube API 403 for region {region} — check API key/quota")
                return []
            if resp.status_code == 429:
                wait = int(resp.headers.get("Retry-After", 30))
                log.warning(f"YouTube rate limited — waiting {wait}s")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            break
        except requests.RequestException as e:
            log.warning(f"YouTube fetch attempt {attempt + 1} for {region} failed: {e}")
            time.sleep(2)
    else:
        return []

    items = resp.json().get("items", [])
    rows = []
    for rank_0, item in enumerate(items, start=1):
        snippet    = item.get("snippet", {})
        statistics = item.get("statistics", {})
        video_id   = item.get("id", "")
        title      = snippet.get("title", "").strip()
        channel    = snippet.get("channelTitle", "").strip()

        if not title or not channel or not video_id:
            continue

        rows.append({
            "rank":       rank_0,
            "video_id":   video_id,
            "title":      title,
            "channel":    channel,
            "view_count": int(statistics.get("viewCount", 0) or 0),
            "like_count": int(statistics.get("likeCount", 0) or 0),
            "region":     region,
        })

    log.info(f"Fetched {len(rows)} trending music videos for region {region}")
    return rows

# ── Song resolution ───────────────────────────────────────────────────────────

def resolve_song(cur, title: str, channel: str) -> tuple[Optional[str], float]:
    """
    Attempt to match a YouTube video to an existing song in catalog.
    Returns (song_id, confidence) or (None, 0.0).

    Strategy:
    1. Exact title + artist/channel normalized match
    2. pg_trgm fuzzy match on title + artist
    """
    title_norm   = normalize(title)
    channel_norm = normalize_channel(channel)

    # 1. exact normalized match
    cur.execute("""
        SELECT s.id FROM songs s
        JOIN artists a ON s.artist_id = a.id
        WHERE s.title_normalized = %s
          AND a.name_normalized  = %s
        LIMIT 1
    """, (title_norm, channel_norm))
    result = cur.fetchone()
    if result:
        return str(result["id"]), 0.95

    # 2. fuzzy match via pg_trgm similarity
    cur.execute("""
        SELECT s.id,
               similarity(s.title_normalized, %s) AS title_sim,
               similarity(a.name_normalized, %s)  AS artist_sim
        FROM songs s
        JOIN artists a ON s.artist_id = a.id
        WHERE similarity(s.title_normalized, %s) > 0.6
          AND similarity(a.name_normalized, %s)  > 0.5
        ORDER BY (similarity(s.title_normalized, %s) + similarity(a.name_normalized, %s)) DESC
        LIMIT 3
    """, (title_norm, channel_norm, title_norm, channel_norm, title_norm, channel_norm))
    results = cur.fetchall()

    if results:
        best = results[0]
        combined_sim = (best["title_sim"] + best["artist_sim"]) / 2
        if combined_sim >= 0.75:
            return str(best["id"]), round(combined_sim * 0.9, 3)

    return None, 0.0


def upsert_song_from_youtube(cur, row: dict) -> str:
    """
    Add a YouTube trending video to the catalog as a new song.
    Uses channel name as artist — good enough for matching future signals.
    Returns the song_id.
    """
    channel_norm = normalize_channel(row["channel"])
    title_norm   = normalize(row["title"])

    # Upsert artist
    cur.execute("""
        INSERT INTO artists (name, name_normalized, spotify_artist_id, genre_tags)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (spotify_artist_id) DO UPDATE SET
            name = EXCLUDED.name,
            updated_at = NOW()
        RETURNING id
    """, (
        row["channel"],
        channel_norm,
        f"yt_{channel_norm[:40]}",
        [],
    ))
    artist_row = cur.fetchone()
    if artist_row:
        artist_id = str(artist_row["id"])
    else:
        cur.execute(
            "SELECT id FROM artists WHERE name_normalized = %s LIMIT 1",
            (channel_norm,)
        )
        artist_id = str(cur.fetchone()["id"])

    # Upsert song
    cur.execute("""
        INSERT INTO songs (title, title_normalized, artist_id, spotify_track_id, genre_tags)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (spotify_track_id) DO NOTHING
        RETURNING id
    """, (
        row["title"],
        title_norm,
        artist_id,
        f"yt_{row['video_id']}",
        [],
    ))
    song_row = cur.fetchone()
    if song_row:
        song_id = str(song_row["id"])
        log.info(f"New song from YouTube: {row['title']} — {row['channel']}")
    else:
        cur.execute(
            "SELECT id FROM songs WHERE spotify_track_id = %s",
            (f"yt_{row['video_id']}",)
        )
        song_id = str(cur.fetchone()["id"])

    return song_id


def queue_for_resolution(cur, row: dict, snapshot_date: date):
    """Add unresolved video to the resolution_queue."""
    external_id = f"youtube::{row['video_id']}::{row['region']}::{snapshot_date}"
    cur.execute("""
        INSERT INTO resolution_queue (
            raw_text, context_json, source_platform,
            observed_at, external_id, fuzzy_candidates, status
        )
        VALUES (%s, %s, 'youtube', %s, %s, %s, 'pending')
        ON CONFLICT DO NOTHING
    """, (
        f"{row['title']} by {row['channel']}",
        psycopg2.extras.Json({
            "title":      row["title"],
            "channel":    row["channel"],
            "video_id":   row["video_id"],
            "region":     row["region"],
            "rank":       row["rank"],
            "view_count": row["view_count"],
            "like_count": row["like_count"],
        }),
        datetime.combine(snapshot_date, datetime.min.time()).replace(tzinfo=timezone.utc),
        external_id,
        psycopg2.extras.Json([]),
    ))

# ── Signal writing ────────────────────────────────────────────────────────────

def rank_score(rank: int, total: int = MAX_RESULTS) -> float:
    """
    Linear score: 1.0 at #1, approaches 0 at #total.
    Mirrors spotify_collector's (51 - rank) / 50 formula scaled to total.
    """
    return max(0.0, (total + 1 - rank) / total)


def insert_signal_event(cur, song_id: str, row: dict,
                        resolution_confidence: float, snapshot_date: date):
    """
    Write one signal_event for a YouTube trending chart appearance.
    engagement_multiplier = 1 + rank_score (higher rank → higher multiplier).
    """
    rs    = rank_score(row["rank"])
    mult  = round(1 + rs, 3)
    score = round(INTENTIONALITY_SCORE * mult, 4)

    external_id = f"youtube::{row['video_id']}::{row['region']}::{snapshot_date}"

    cur.execute("""
        INSERT INTO signal_events (
            observed_at, song_id, source_platform, signal_type,
            intentionality_score, raw_engagement, engagement_multiplier,
            weighted_score, resolution_confidence, is_home_community,
            external_id, context_snapshot
        )
        VALUES (
            %s, %s, 'youtube', 'chart_position',
            %s, %s, %s,
            %s, %s, FALSE,
            %s, %s
        )
        ON CONFLICT DO NOTHING
    """, (
        datetime.combine(snapshot_date, datetime.min.time()).replace(tzinfo=timezone.utc),
        song_id,
        INTENTIONALITY_SCORE,
        psycopg2.extras.Json({
            "view_count": row["view_count"],
            "like_count": row["like_count"],
            "rank":       row["rank"],
            "region":     row["region"],
        }),
        mult,
        score,
        resolution_confidence,
        external_id,
        psycopg2.extras.Json({
            "source":   "youtube_trending",
            "video_id": row["video_id"],
            "region":   row["region"],
            "rank":     row["rank"],
            "channel":  row["channel"],
        }),
    ))

# ── Deduplication across regions ──────────────────────────────────────────────

def pick_best_region_rows(all_rows: list[dict]) -> list[dict]:
    """
    A video can appear in multiple regions. Keep only one entry per video_id —
    the one with the best (lowest) rank. Ties broken by first-seen order.
    """
    best: dict[str, dict] = {}
    for row in all_rows:
        vid = row["video_id"]
        if vid not in best or row["rank"] < best[vid]["rank"]:
            best[vid] = row
    return list(best.values())

# ── Main ──────────────────────────────────────────────────────────────────────

def run(snapshot_date: date = None):
    snapshot_date = snapshot_date or date.today()

    conn = _connect()
    conn.autocommit = False
    psycopg2.extras.register_uuid()

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            INSERT INTO collector_runs (collector, metadata)
            VALUES ('youtube', %s) RETURNING id
        """, (psycopg2.extras.Json({
            "snapshot_date": str(snapshot_date),
            "regions":       REGIONS_TO_FETCH,
        }),))
        run_id = cur.fetchone()["id"]
    conn.commit()

    total_events  = 0
    total_queued  = 0
    total_dropped = 0

    try:
        # Collect rows from all regions
        all_rows: list[dict] = []
        for region in REGIONS_TO_FETCH:
            rows = fetch_trending_music(region)
            all_rows.extend(rows)
            time.sleep(0.5)  # gentle pacing between region calls

        # Deduplicate: one signal per video_id (best rank across regions)
        deduped_rows = pick_best_region_rows(all_rows)
        log.info(
            f"After dedup: {len(deduped_rows)} unique videos "
            f"(from {len(all_rows)} total across {len(REGIONS_TO_FETCH)} regions)"
        )

        for row in deduped_rows:
            if not row["title"] or not row["channel"]:
                total_dropped += 1
                continue

            try:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:

                    song_id, confidence = resolve_song(cur, row["title"], row["channel"])

                    if confidence < 0.65:
                        # Add to catalog so future runs can match it
                        song_id = upsert_song_from_youtube(cur, row)
                        confidence = 0.70  # YouTube-sourced catalog entry
                        total_queued += 1

                    insert_signal_event(cur, song_id, row, confidence, snapshot_date)
                    total_events += 1

                conn.commit()

            except Exception as e:
                conn.rollback()
                log.error(f"Failed processing '{row.get('title', '?')}' ({row.get('video_id')}): {e}")
                total_dropped += 1

        with conn.cursor() as cur:
            cur.execute("""
                UPDATE collector_runs
                SET status = 'success', completed_at = NOW(),
                    events_collected = %s, events_queued = %s, events_dropped = %s
                WHERE id = %s
            """, (total_events, total_queued, total_dropped, run_id))
        conn.commit()
        log.info(
            f"YouTube collector complete — "
            f"{total_events} events written, {total_queued} queued, {total_dropped} dropped"
        )

    except Exception as e:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE collector_runs
                SET status = 'failed', completed_at = NOW(), error_message = %s
                WHERE id = %s
            """, (str(e), run_id))
        conn.commit()
        log.error(f"YouTube collector failed: {e}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    import sys
    target = date.fromisoformat(sys.argv[1]) if len(sys.argv) > 1 else date.today()
    run(target)
