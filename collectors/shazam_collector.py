"""
Shazam Collector
----------------
Scrapes Shazam's public CSV chart endpoint — no API key required.
URL: https://www.shazam.com/services/charts/csv/top-200/{region}/

Resolves songs against existing catalog (Spotify collector runs first)
or queues unknown songs for resolution.

Schedule: every 6 hours — Shazam updates weekly but is your
highest-intentionality signal (Shazam = someone actively identified a song).
"""

import io
import csv
import os
import time
import logging
import requests
import psycopg2
import psycopg2.extras
from datetime import datetime, date, timezone
from typing import Optional

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("shazam_collector")

# ── Config ──────────────────────────────────────────────────────────────────

DB_URL = os.environ["DATABASE_URL"]

SHAZAM_CSV_BASE = "https://www.shazam.com/services/charts/csv/top-200"
SHAZAM_HEADERS  = {
    "user-agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36"
    )
}

CHARTS_TO_FETCH = [
    {"region": "global", "genre": "all", "slug": "world"},
    {"region": "us",     "genre": "all", "slug": "united-states"},
]

# Shazam chart position intentionality score —
# highest in the pipeline because Shazam = someone actively identified a song
INTENTIONALITY_SHAZAM = 0.92

# ── CSV fetch ────────────────────────────────────────────────────────────────

def fetch_shazam_chart(chart: dict) -> list[dict]:
    """
    Download the Shazam top-200 CSV for a region and return normalized rows.
    CSV format (after two preamble lines): Rank, Artist, Title
    Returns [{rank, shazam_id, song_title, artist_name, shazam_url}]
    """
    url = f"{SHAZAM_CSV_BASE}/{chart['slug']}/"
    for attempt in range(3):
        try:
            resp = requests.get(url, headers=SHAZAM_HEADERS, timeout=15)
            if resp.status_code == 429:
                wait = int(resp.headers.get("Retry-After", 30))
                log.warning(f"Shazam rate limited — waiting {wait}s")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            break
        except requests.RequestException as e:
            log.warning(f"Shazam CSV fetch attempt {attempt + 1} failed: {e}")
            time.sleep(2)
    else:
        return []

    # CSV has a BOM + 2 preamble lines before the actual headers
    reader = csv.reader(io.StringIO(resp.content.decode("utf-8-sig")))
    all_rows = list(reader)

    # Find the header row (contains "Rank")
    header_idx = next((i for i, r in enumerate(all_rows) if r and r[0].strip() == "Rank"), None)
    if header_idx is None:
        log.warning(f"Could not find header row in Shazam CSV for {chart['slug']}")
        return []

    rows = []
    for data_row in all_rows[header_idx + 1:]:
        if len(data_row) < 3:
            continue
        rank_str, artist, title = data_row[0].strip(), data_row[1].strip(), data_row[2].strip()
        if not rank_str.isdigit() or not title or not artist:
            continue
        rows.append({
            "rank":        int(rank_str),
            "shazam_id":   "",   # not in CSV; resolved via title+artist match
            "song_title":  title,
            "artist_name": artist,
            "shazam_url":  f"https://www.shazam.com/charts/top-200/{chart['slug']}",
        })

    log.info(f"Fetched {len(rows)} tracks from Shazam {chart['region']} chart")
    return rows

# ── Song resolution ──────────────────────────────────────────────────────────

def resolve_song(cur, row: dict) -> tuple[Optional[str], float]:
    """
    Attempt to match a Shazam row to an existing song in catalog.
    Returns (song_id, confidence) or (None, 0.0).

    Strategy:
    1. Exact shazam_id match
    2. Exact title + artist match (normalized)
    3. Fuzzy title + artist (pg_trgm)
    4. If no match → return None for queue
    """
    import re

    def normalize(s: str) -> str:
        return re.sub(r"[^\w\s]", "", s.lower().strip())

    # 1. shazam_id match
    if row["shazam_id"]:
        cur.execute("SELECT id FROM songs WHERE shazam_id = %s", (row["shazam_id"],))
        result = cur.fetchone()
        if result:
            return str(result["id"]), 1.0

    title_norm  = normalize(row["song_title"])
    artist_norm = normalize(row["artist_name"])

    # 2. exact normalized match
    cur.execute("""
        SELECT s.id FROM songs s
        JOIN artists a ON s.artist_id = a.id
        WHERE s.title_normalized = %s
          AND a.name_normalized = %s
        LIMIT 1
    """, (title_norm, artist_norm))
    result = cur.fetchone()
    if result:
        # update shazam_id on the song for future fast lookups
        cur.execute(
            "UPDATE songs SET shazam_id = %s WHERE id = %s AND shazam_id IS NULL",
            (row["shazam_id"], result["id"])
        )
        return str(result["id"]), 0.95

    # 3. fuzzy match via pg_trgm similarity
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
    """, (title_norm, artist_norm, title_norm, artist_norm, title_norm, artist_norm))
    results = cur.fetchall()

    if results:
        best = results[0]
        combined_sim = (best["title_sim"] + best["artist_sim"]) / 2
        if combined_sim >= 0.75:
            return str(best["id"]), round(combined_sim * 0.9, 3)  # cap at 0.9 for fuzzy

    return None, 0.0

def upsert_song_from_shazam(cur, row: dict) -> str:
    """
    Add a Shazam charting song to the catalog when resolution fails.
    Uses shazam_ prefix IDs so future runs can match via shazam_id.
    Returns song_id.
    """
    import re
    def _norm(s): return re.sub(r"[^\w\s]", "", s.lower().strip())

    artist_norm = _norm(row["artist_name"])
    title_norm  = _norm(row["song_title"])
    fake_artist_id = f"shazam_{artist_norm[:40]}"

    cur.execute("""
        INSERT INTO artists (name, name_normalized, spotify_artist_id, genre_tags)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (spotify_artist_id) DO UPDATE SET
            name = EXCLUDED.name, updated_at = NOW()
        RETURNING id
    """, (row["artist_name"], artist_norm, fake_artist_id, []))
    artist_row = cur.fetchone()
    if artist_row:
        artist_id = str(artist_row["id"])
    else:
        cur.execute("SELECT id FROM artists WHERE name_normalized = %s LIMIT 1", (artist_norm,))
        artist_id = str(cur.fetchone()["id"])

    fake_track_id = f"shazam_{row['shazam_id']}" if row["shazam_id"] else f"shazam_{artist_norm[:20]}_{title_norm[:20]}"

    cur.execute("""
        INSERT INTO songs (title, title_normalized, artist_id, spotify_track_id, genre_tags)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (spotify_track_id) DO NOTHING
        RETURNING id
    """, (row["song_title"], title_norm, artist_id, fake_track_id, []))
    song_row = cur.fetchone()
    if song_row:
        song_id = str(song_row["id"])
        log.info(f"New song from Shazam: {row['song_title']} — {row['artist_name']}")
    else:
        cur.execute("SELECT id FROM songs WHERE spotify_track_id = %s", (fake_track_id,))
        song_id = str(cur.fetchone()["id"])

    return song_id


def queue_for_resolution(cur, row: dict, chart: dict,
                          snapshot_date: date, candidates: list) -> str:
    """Add unresolved song to the resolution queue."""
    cur.execute("""
        INSERT INTO resolution_queue (
            raw_text, context_json, source_platform,
            observed_at, external_id, fuzzy_candidates, status
        )
        VALUES (%s, %s, 'shazam', %s, %s, %s, 'pending')
        ON CONFLICT DO NOTHING
        RETURNING id
    """, (
        f"{row['song_title']} by {row['artist_name']}",
        psycopg2.extras.Json({
            "song_title":  row["song_title"],
            "artist_name": row["artist_name"],
            "chart_region": chart["region"],
            "chart_genre":  chart["genre"],
            "rank":         row["rank"],
            "shazam_url":   row["shazam_url"],
        }),
        datetime.combine(snapshot_date, datetime.min.time()).replace(tzinfo=timezone.utc),
        row["shazam_id"],
        psycopg2.extras.Json(candidates),
    ))

# ── Shazam rank bonus ─────────────────────────────────────────────────────

def shazam_rank_bonus(rank: int, spotify_rank: Optional[int]) -> float:
    """
    Compute the Shazam intentionality bonus.
    Higher when Shazam rank >> Spotify rank (discovering faster than streaming).
    """
    base_bonus = max(0, (201 - rank) / 200)  # 1.0 at #1, ~0 at #200

    if spotify_rank:
        # delta: positive means Shazam is higher (more discovery-driven)
        rank_delta = spotify_rank - rank
        discovery_factor = min(1.5, max(0.5, 1 + (rank_delta / 100)))
        return round(base_bonus * discovery_factor, 3)

    # no Spotify presence yet — pure discovery signal, full bonus
    return round(base_bonus * 1.5, 3)

def get_spotify_rank(cur, song_id: str, snapshot_date: date) -> Optional[int]:
    """Look up today's Spotify rank for a song, if available."""
    cur.execute("""
        SELECT rank FROM spotify_chart_snapshots
        WHERE song_id = %s
          AND snapshot_date = %s
          AND region = 'global'
        LIMIT 1
    """, (song_id, snapshot_date))
    row = cur.fetchone()
    return row["rank"] if row else None

# ── Main collector ────────────────────────────────────────────────────────

def run(snapshot_date: date = None):
    snapshot_date = snapshot_date or date.today()
    now = datetime.now(timezone.utc)

    conn = psycopg2.connect(DB_URL)
    conn.autocommit = False
    psycopg2.extras.register_uuid()

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            INSERT INTO collector_runs (collector, metadata)
            VALUES ('shazam', %s) RETURNING id
        """, (psycopg2.extras.Json({"snapshot_date": str(snapshot_date)}),))
        run_id = cur.fetchone()["id"]
    conn.commit()

    total_events = 0
    total_queued = 0
    total_dropped = 0

    try:
        for chart in CHARTS_TO_FETCH:
            rows = fetch_shazam_chart(chart)
            if not rows:
                continue

            for row in rows:
                if not row["song_title"] or not row["artist_name"]:
                    total_dropped += 1
                    continue

                try:
                    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:

                        song_id, confidence = resolve_song(cur, row)

                        if confidence < 0.65:
                            # Add to catalog so the signal is counted now
                            song_id = upsert_song_from_shazam(cur, row)
                            confidence = 0.70
                            total_queued += 1

                        # insert snapshot
                        cur.execute("""
                            INSERT INTO shazam_chart_snapshots (
                                snapshot_date, region, genre, rank,
                                shazam_id, song_title, artist_name, song_id
                            )
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (snapshot_date, region, genre, rank) DO NOTHING
                        """, (
                            snapshot_date,
                            chart["region"],
                            chart["genre"],
                            row["rank"],
                            row["shazam_id"] or None,
                            row["song_title"],
                            row["artist_name"],
                            song_id,
                        ))

                        # compute rank bonus vs Spotify
                        spotify_rank = get_spotify_rank(cur, song_id, snapshot_date)
                        bonus = shazam_rank_bonus(row["rank"], spotify_rank)
                        final_score = round(INTENTIONALITY_SHAZAM * bonus, 4)

                        # signal event — no community (streaming signal)
                        cur.execute("""
                            INSERT INTO signal_events (
                                observed_at, song_id, source_platform, signal_type,
                                intentionality_score, raw_engagement,
                                engagement_multiplier, weighted_score,
                                resolution_confidence, is_home_community,
                                context_snapshot
                            )
                            VALUES (%s, %s, 'shazam', 'shazam', %s, %s, %s, %s, %s, FALSE, %s)
                            ON CONFLICT DO NOTHING
                        """, (
                            now,
                            song_id,
                            INTENTIONALITY_SHAZAM,
                            psycopg2.extras.Json({
                                "rank":          row["rank"],
                                "spotify_rank":  spotify_rank,
                            }),
                            bonus,
                            final_score,
                            confidence,
                            psycopg2.extras.Json({
                                "chart_region": chart["region"],
                                "chart_genre":  chart["genre"],
                                "rank":         row["rank"],
                                "shazam_url":   row["shazam_url"],
                                "shazam_id":    row["shazam_id"],
                            }),
                        ))

                        total_events += 1
                    conn.commit()

                except Exception as e:
                    conn.rollback()
                    log.error(f"Failed processing Shazam row {row}: {e}")
                    total_dropped += 1

            time.sleep(2)  # pause between chart types

        with conn.cursor() as cur:
            cur.execute("""
                UPDATE collector_runs
                SET status = 'success', completed_at = NOW(),
                    events_collected = %s, events_queued = %s, events_dropped = %s
                WHERE id = %s
            """, (total_events, total_queued, total_dropped, run_id))
        conn.commit()
        log.info(f"Shazam collector complete — {total_events} events, "
                 f"{total_queued} queued, {total_dropped} dropped")

    except Exception as e:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE collector_runs
                SET status = 'failed', completed_at = NOW(), error_message = %s
                WHERE id = %s
            """, (str(e), run_id))
        conn.commit()
        log.error(f"Shazam collector failed: {e}")
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    run()
