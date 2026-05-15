"""
Scoring Aggregator
------------------
Rolls up raw signal_events into song_scores per chart category
and window. Computes penetration_score, diversity_multiplier,
momentum_delta, and rank. Triggers narrative generation.

This is idempotent — safe to rerun with different weights.
Rerunning with a new home_inclusion_coefficient will recompute
all historical windows correctly from the raw signal_events.

Schedule: nightly at 03:00 UTC (after all collectors have run)

Usage:
    python aggregator.py                    # score today's window
    python aggregator.py --date 2026-05-01 # score a specific date
    python aggregator.py --backfill 30     # backfill last 30 days
    python aggregator.py --category crossover  # one category only
"""

import os
import sys
import math
import logging
import argparse
import psycopg2
import psycopg2.extras
from datetime import date, timedelta, datetime, timezone
from typing import Optional

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("aggregator")

DB_URL = os.environ["DATABASE_URL"]

# ── SQL: core score aggregation ───────────────────────────────────────────────
#
# Run as a single query per (window_date, chart_category).
# Reads from signal_events, joins to communities and songs.

SCORE_QUERY = """
WITH

-- 1. filter signal events to the rolling window
window_events AS (
    SELECT
        se.song_id,
        se.community_id,
        se.source_platform,
        se.signal_type,
        se.weighted_score,
        se.intentionality_score,
        se.engagement_multiplier,
        se.community_casual_weight,
        se.effective_distance,
        se.home_distance,
        se.is_home_community,
        se.resolution_confidence,
        se.observed_at,
        se.context_snapshot,
        c.community_type,
        c.casual_weight,
        COALESCE(c.external_id, se.source_platform) AS community_name
    FROM signal_events se
    LEFT JOIN communities c ON se.community_id = c.id
    JOIN songs s ON se.song_id = s.id
    WHERE
        se.observed_at >= %(window_start)s
        AND se.observed_at <  %(window_end)s
        AND se.resolution_confidence >= %(min_confidence)s
        -- community filters: NULL community_id means a streaming signal (Spotify/Shazam)
        -- which always passes community-type filters
        AND (%(min_casual_weight)s IS NULL
             OR c.casual_weight IS NULL
             OR c.casual_weight >= %(min_casual_weight)s)
        AND (c.community_type IS NULL
             OR c.community_type NOT IN
                (SELECT unnest(%(exclude_types)s::text[])))
        AND (%(include_types)s IS NULL
             OR c.community_type IS NULL
             OR c.community_type = ANY(%(include_types)s::text[]))
),

-- 2. split home vs out-of-home
split_events AS (
    SELECT
        song_id,
        community_id,
        community_type,
        community_name,
        source_platform,
        weighted_score,
        effective_distance,
        context_snapshot,
        is_home_community,
        CASE WHEN is_home_community THEN weighted_score ELSE 0 END AS home_score,
        CASE WHEN NOT is_home_community THEN weighted_score ELSE 0 END AS out_score
    FROM window_events
),

-- 3. per-song, per-community aggregates
community_aggs AS (
    SELECT
        song_id,
        community_id,
        community_type,
        community_name,
        SUM(out_score)  AS community_out_score,
        SUM(home_score) AS community_home_score,
        COUNT(*)        AS event_count,
        -- top post title for narrative generation
        (
            SELECT context_snapshot->>'post_title'
            FROM split_events se2
            WHERE se2.song_id = se.song_id
              AND se2.community_id = se.community_id
              AND se2.context_snapshot->>'post_title' IS NOT NULL
            ORDER BY weighted_score DESC
            LIMIT 1
        ) AS top_post_title,
        MAX(effective_distance) AS max_distance
    FROM split_events se
    GROUP BY song_id, community_id, community_type, community_name
),

-- 4. song-level aggregates
song_aggs AS (
    SELECT
        song_id,
        SUM(community_out_score)   AS out_of_home_weighted_score,
        SUM(community_home_score)  AS home_weighted_score,
        COUNT(DISTINCT community_id)        AS community_count,
        COUNT(DISTINCT community_type)      AS community_type_count,
        -- JSON array of top communities (for UI and narratives)
        jsonb_agg(
            jsonb_build_object(
                'community_id',   community_id,
                'community_name', community_name,
                'community_type', community_type,
                'out_score',      community_out_score,
                'event_count',    event_count,
                'top_post_title', top_post_title,
                'max_distance',   max_distance
            )
            ORDER BY community_out_score DESC
        ) AS top_communities_raw,
        -- signal source breakdown
        jsonb_object_agg(
            platform_grp,
            platform_score
        ) AS signal_breakdown
    FROM community_aggs
    CROSS JOIN LATERAL (
        -- pivot source breakdown
        SELECT source_platform AS platform_grp,
               SUM(out_score + home_score) AS platform_score
        FROM split_events se2
        WHERE se2.song_id = community_aggs.song_id
        GROUP BY source_platform
    ) AS platform_pivot
    GROUP BY song_id
)

SELECT
    sa.song_id,
    sa.out_of_home_weighted_score,
    sa.home_weighted_score,
    sa.community_count,
    sa.community_type_count,
    sa.top_communities_raw,
    sa.signal_breakdown,
    s.home_confidence
FROM song_aggs sa
JOIN songs s ON sa.song_id = s.id
"""

# ── Scoring math ──────────────────────────────────────────────────────────────

def diversity_multiplier(community_type_count: int, coeff: float = 0.25) -> float:
    """1 + coeff * (type_count - 1) — linear bonus for breadth."""
    return round(1 + coeff * max(0, community_type_count - 1), 4)

def penetration_score(
    out_of_home: float,
    home: float,
    home_coeff: float,
    div_mult: float,
) -> float:
    """
    (out_of_home + home * coeff) * diversity_multiplier
    / log10(home + 1)

    home_coeff=0.0 → home community excluded from numerator entirely
    home_coeff=0.1 → soft inclusion (default)
    """
    numerator   = out_of_home + home * home_coeff
    denominator = math.log10(home + 1) if home > 0 else 1.0
    return round((numerator * div_mult) / denominator, 4)

# ── New communities detection ─────────────────────────────────────────────────

def get_new_communities(cur, song_id: str, window_date: date,
                        chart_category: str) -> list[dict]:
    """Communities that appeared in this window but not the prior window."""
    cur.execute("""
        SELECT top_communities
        FROM song_scores
        WHERE song_id = %s
          AND window_date = %s
          AND chart_category = %s
    """, (song_id, window_date - timedelta(days=7), chart_category))
    row = cur.fetchone()
    if not row or not row["top_communities"]:
        return []

    prev_ids = {c["community_id"] for c in row["top_communities"]}
    return []  # populated by caller with filtered top_communities

# ── Rank assignment ───────────────────────────────────────────────────────────

def assign_ranks(scores: list[dict]) -> list[dict]:
    """Sort by penetration_score descending, assign rank."""
    sorted_scores = sorted(scores, key=lambda x: x["penetration_score"], reverse=True)
    for i, s in enumerate(sorted_scores, 1):
        s["rank"] = i
    return sorted_scores

# ── Main aggregation for one window + category ────────────────────────────────

def aggregate_window(conn, window_date: date, category: dict):
    """
    Compute song_scores for one (window_date, chart_category).
    Idempotent — deletes and rewrites the window.
    """
    category_id = category["id"]
    window_days = category["window_days"]
    home_coeff  = category["home_inclusion_coefficient"]
    div_coeff   = category["diversity_multiplier_coeff"]
    min_conf    = 0.65
    filters     = category.get("filters") or {}

    window_start = datetime.combine(
        window_date - timedelta(days=window_days), datetime.min.time()
    ).replace(tzinfo=timezone.utc)
    window_end = datetime.combine(
        window_date, datetime.max.time()
    ).replace(tzinfo=timezone.utc)

    exclude_types = filters.get("exclude_community_types", [])
    include_types = filters.get("include_community_types", None)
    min_casual    = filters.get("min_casual_weight", None)

    log.info(f"Aggregating {category_id} for {window_date} "
             f"({window_days}d window, home_coeff={home_coeff})")

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:

        # run core aggregation query
        cur.execute(SCORE_QUERY, {
            "window_start":    window_start,
            "window_end":      window_end,
            "min_confidence":  min_conf,
            "min_casual_weight": min_casual,
            "exclude_types":   exclude_types or [],
            "include_types":   include_types,
        })
        raw_rows = cur.fetchall()

        if not raw_rows:
            log.info(f"No signal events found for {category_id} {window_date}")
            return 0

        # fetch previous window scores for momentum
        prev_date = window_date - timedelta(days=7)
        cur.execute("""
            SELECT song_id, penetration_score, rank
            FROM song_scores
            WHERE window_date = %s AND chart_category = %s
        """, (prev_date, category_id))
        prev_scores = {str(r["song_id"]): r for r in cur.fetchall()}

        # compute scores
        scored = []
        for row in raw_rows:
            song_id = str(row["song_id"])
            out_score = float(row["out_of_home_weighted_score"] or 0)
            home_score = float(row["home_weighted_score"] or 0)
            type_count = int(row["community_type_count"] or 1)
            comm_count = int(row["community_count"] or 1)

            div_mult = diversity_multiplier(type_count, div_coeff)
            pen_score = penetration_score(out_score, home_score, home_coeff, div_mult)

            # momentum
            prev = prev_scores.get(song_id, {})
            prev_pen  = float(prev.get("penetration_score") or 0)
            prev_rank = prev.get("rank")
            mom_delta = round(pen_score - prev_pen, 4)
            mom_pct   = round((mom_delta / prev_pen * 100) if prev_pen > 0 else 0, 2)

            # top communities — cap at 10 for storage
            top_comms = (row["top_communities_raw"] or [])[:10]

            # new communities this window
            prev_comm_ids = set()
            if prev.get("penetration_score"):
                # rough proxy — communities in prev window
                cur.execute("""
                    SELECT jsonb_array_elements(top_communities)->>'community_id'
                    FROM song_scores
                    WHERE song_id = %s AND window_date = %s AND chart_category = %s
                """, (song_id, prev_date, category_id))
                prev_comm_ids = {r[0] for r in cur.fetchall()}

            new_comms = [
                c for c in top_comms
                if c.get("community_id") not in prev_comm_ids
            ]

            # apply chart-specific filters
            if filters.get("min_community_types") and type_count < filters["min_community_types"]:
                continue
            if filters.get("min_signal_count"):
                total_events = sum(c.get("event_count", 0) for c in top_comms)
                if total_events < filters["min_signal_count"]:
                    continue

            # story length heuristic
            rank_change = abs((prev_rank or 999) - 1)  # placeholder until ranks assigned
            if prev_rank is None:
                story_length = "long"      # new entry always gets a note
            elif abs(mom_delta) > pen_score * 0.15 or type_count >= 5:
                story_length = "long"
            elif abs(mom_delta) < pen_score * 0.03 and not new_comms:
                story_length = "none"      # holding steady with nothing new
            else:
                story_length = "short"

            scored.append({
                "song_id":                    song_id,
                "window_date":                window_date,
                "chart_category":             category_id,
                "out_of_home_weighted_score": out_score,
                "home_weighted_score":        home_score,
                "community_count":            comm_count,
                "community_type_count":       type_count,
                "diversity_multiplier":       div_mult,
                "home_inclusion_coefficient": home_coeff,
                "penetration_score":          pen_score,
                "prev_penetration_score":     prev_pen or None,
                "momentum_delta":             mom_delta,
                "momentum_pct":               mom_pct,
                "prev_rank":                  prev_rank,
                "signal_breakdown":           psycopg2.extras.Json(row["signal_breakdown"] or {}),
                "top_communities":            psycopg2.extras.Json(top_comms),
                "new_communities_this_week":  psycopg2.extras.Json(new_comms),
                "story_length":               story_length,
                "total_signal_count":         sum(c.get("event_count",0) for c in top_comms),
            })

        # apply chart sort and assign ranks
        sort_by = category.get("sort_by", "penetration_score")
        if sort_by == "momentum_delta":
            scored.sort(key=lambda x: x["momentum_delta"], reverse=True)
        elif sort_by == "community_type_count":
            scored.sort(key=lambda x: x["community_type_count"], reverse=True)
        else:
            scored.sort(key=lambda x: x["penetration_score"], reverse=True)

        chart_size = category.get("size", 25)
        scored = scored[:chart_size]
        for i, s in enumerate(scored, 1):
            s["rank"] = i

        # mark #1 overall mover for featured story
        if scored:
            top_mover = max(scored, key=lambda x: x["momentum_delta"])
            top_mover["story_length"] = "featured"

        # upsert into song_scores
        written = 0
        for s in scored:
            cur.execute("""
                INSERT INTO song_scores (
                    song_id, window_date, chart_category,
                    total_signal_count,
                    out_of_home_weighted_score, home_weighted_score,
                    community_count, community_type_count,
                    diversity_multiplier, home_inclusion_coefficient,
                    penetration_score, prev_penetration_score,
                    momentum_delta, momentum_pct,
                    rank, prev_rank,
                    signal_breakdown, top_communities,
                    new_communities_this_week, story_length,
                    computed_at
                )
                VALUES (
                    %(song_id)s, %(window_date)s, %(chart_category)s,
                    %(total_signal_count)s,
                    %(out_of_home_weighted_score)s, %(home_weighted_score)s,
                    %(community_count)s, %(community_type_count)s,
                    %(diversity_multiplier)s, %(home_inclusion_coefficient)s,
                    %(penetration_score)s, %(prev_penetration_score)s,
                    %(momentum_delta)s, %(momentum_pct)s,
                    %(rank)s, %(prev_rank)s,
                    %(signal_breakdown)s, %(top_communities)s,
                    %(new_communities_this_week)s, %(story_length)s,
                    NOW()
                )
                ON CONFLICT (song_id, window_date, chart_category)
                DO UPDATE SET
                    total_signal_count          = EXCLUDED.total_signal_count,
                    out_of_home_weighted_score  = EXCLUDED.out_of_home_weighted_score,
                    home_weighted_score         = EXCLUDED.home_weighted_score,
                    community_count             = EXCLUDED.community_count,
                    community_type_count        = EXCLUDED.community_type_count,
                    diversity_multiplier        = EXCLUDED.diversity_multiplier,
                    home_inclusion_coefficient  = EXCLUDED.home_inclusion_coefficient,
                    penetration_score           = EXCLUDED.penetration_score,
                    prev_penetration_score      = EXCLUDED.prev_penetration_score,
                    momentum_delta              = EXCLUDED.momentum_delta,
                    momentum_pct               = EXCLUDED.momentum_pct,
                    rank                        = EXCLUDED.rank,
                    prev_rank                   = EXCLUDED.prev_rank,
                    signal_breakdown            = EXCLUDED.signal_breakdown,
                    top_communities             = EXCLUDED.top_communities,
                    new_communities_this_week   = EXCLUDED.new_communities_this_week,
                    story_length                = EXCLUDED.story_length,
                    computed_at                 = NOW()
            """, s)
            written += 1

        conn.commit()
        log.info(f"  {category_id} {window_date}: {written} songs scored, "
                 f"{len(raw_rows) - written} filtered out")
        return written

# ── Market trend snapshot ─────────────────────────────────────────────────────

def compute_market_trend(conn, window_date: date, chart_category: str = "crossover"):
    """
    Aggregate chart-wide stats for the MarketTrendCard.
    Runs after all chart categories are scored.
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:

        # chart-wide averages
        cur.execute("""
            SELECT
                AVG(penetration_score)      AS avg_pen,
                AVG(community_type_count)   AS avg_types,
                COUNT(*)                    AS total_songs
            FROM song_scores
            WHERE window_date = %s AND chart_category = %s
        """, (window_date, chart_category))
        agg = cur.fetchone()

        # prior week
        prev_date = window_date - timedelta(days=7)
        cur.execute("""
            SELECT
                AVG(penetration_score)    AS avg_pen,
                AVG(community_type_count) AS avg_types
            FROM song_scores
            WHERE window_date = %s AND chart_category = %s
        """, (prev_date, chart_category))
        prev_agg = cur.fetchone()

        # community type activity
        cur.execute("""
            SELECT
                c.community_type,
                COUNT(DISTINCT se.song_id) AS song_count
            FROM signal_events se
            JOIN communities c ON se.community_id = c.id
            WHERE se.observed_at >= NOW() - INTERVAL '7 days'
              AND NOT se.is_home_community
              AND se.resolution_confidence >= 0.65
            GROUP BY c.community_type
            ORDER BY song_count DESC
        """)
        type_rows = cur.fetchall()

        # prior week type activity for comparison
        cur.execute("""
            SELECT
                c.community_type,
                COUNT(DISTINCT se.song_id) AS song_count
            FROM signal_events se
            JOIN communities c ON se.community_id = c.id
            WHERE se.observed_at >= NOW() - INTERVAL '14 days'
              AND se.observed_at <  NOW() - INTERVAL '7 days'
              AND NOT se.is_home_community
            GROUP BY c.community_type
        """)
        prev_type_rows = {r["community_type"]: r["song_count"] for r in cur.fetchall()}

        community_type_counts = {}
        for r in type_rows:
            ct = r["community_type"]
            prev_count = prev_type_rows.get(ct, 0)
            community_type_counts[ct] = {
                "songs":      r["song_count"],
                "prev_songs": prev_count,
                "delta":      r["song_count"] - prev_count,
            }

        # dominant signal type
        cur.execute("""
            SELECT source_platform, COUNT(*) AS ct
            FROM signal_events
            WHERE observed_at >= NOW() - INTERVAL '7 days'
              AND resolution_confidence >= 0.65
            GROUP BY source_platform
            ORDER BY ct DESC
            LIMIT 1
        """)
        dom_row = cur.fetchone()
        dominant_signal = dom_row["source_platform"] if dom_row else "reddit"

        # genre crossover health
        cur.execute("""
            SELECT
                unnest(s.genre_tags) AS genre,
                COUNT(DISTINCT se.song_id) AS song_count,
                AVG(se.effective_distance) AS avg_distance
            FROM signal_events se
            JOIN songs s ON se.song_id = s.id
            WHERE se.observed_at >= NOW() - INTERVAL '7 days'
              AND NOT se.is_home_community
              AND se.resolution_confidence >= 0.65
            GROUP BY genre
            ORDER BY song_count DESC
            LIMIT 20
        """)
        genre_rows = cur.fetchall()
        genre_crossover = {
            r["genre"]: {
                "songs":        r["song_count"],
                "avg_distance": round(float(r["avg_distance"] or 0), 3),
            }
            for r in genre_rows
        }

        # notable absences — genres with home activity but no crossover
        cur.execute("""
            SELECT DISTINCT unnest(genre_tags) AS genre
            FROM songs
            WHERE id IN (
                SELECT DISTINCT song_id FROM signal_events
                WHERE observed_at >= NOW() - INTERVAL '7 days'
                  AND is_home_community = TRUE
            )
        """)
        active_genres = {r["genre"] for r in cur.fetchall()}
        crossover_genres = set(genre_crossover.keys())
        notable_absences = list(active_genres - crossover_genres)[:5]

        # trailing 4 weeks for trend detection
        cur.execute("""
            SELECT window_date,
                   AVG(penetration_score)    AS avg_pen,
                   AVG(community_type_count) AS avg_types
            FROM song_scores
            WHERE window_date >= %s
              AND chart_category = %s
            GROUP BY window_date
            ORDER BY window_date DESC
            LIMIT 4
        """, (window_date - timedelta(days=28), chart_category))
        trailing = [
            {
                "date":      str(r["window_date"]),
                "avg_pen":   round(float(r["avg_pen"] or 0), 2),
                "avg_types": round(float(r["avg_types"] or 0), 2),
            }
            for r in cur.fetchall()
        ]

        # upsert market trend snapshot
        cur.execute("""
            INSERT INTO market_trend_snapshots (
                window_date, chart_category,
                avg_penetration_score, avg_community_type_count,
                prev_avg_penetration_score, prev_avg_community_type_count,
                community_type_counts, dominant_signal_type,
                genre_crossover, notable_absences, trailing_weeks,
                computed_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (window_date)
            DO UPDATE SET
                avg_penetration_score       = EXCLUDED.avg_penetration_score,
                avg_community_type_count    = EXCLUDED.avg_community_type_count,
                prev_avg_penetration_score  = EXCLUDED.prev_avg_penetration_score,
                prev_avg_community_type_count = EXCLUDED.prev_avg_community_type_count,
                community_type_counts       = EXCLUDED.community_type_counts,
                dominant_signal_type        = EXCLUDED.dominant_signal_type,
                genre_crossover             = EXCLUDED.genre_crossover,
                notable_absences            = EXCLUDED.notable_absences,
                trailing_weeks              = EXCLUDED.trailing_weeks,
                computed_at                 = NOW()
        """, (
            window_date, chart_category,
            round(float(agg["avg_pen"] or 0), 2),
            round(float(agg["avg_types"] or 0), 2),
            round(float(prev_agg["avg_pen"] or 0), 2) if prev_agg else None,
            round(float(prev_agg["avg_types"] or 0), 2) if prev_agg else None,
            psycopg2.extras.Json(community_type_counts),
            dominant_signal,
            psycopg2.extras.Json(genre_crossover),
            psycopg2.extras.Json(notable_absences),
            psycopg2.extras.Json(trailing),
        ))
        conn.commit()
        log.info(f"Market trend snapshot written for {window_date}")

# ── Main ──────────────────────────────────────────────────────────────────────

def run(
    target_date: date = None,
    backfill_days: int = 0,
    category_filter: str = None,
):
    target_date = target_date or date.today()
    conn = psycopg2.connect(DB_URL)
    conn.autocommit = False
    psycopg2.extras.register_uuid()

    # load chart categories
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        query = "SELECT * FROM chart_categories WHERE enabled = TRUE"
        params = []
        if category_filter:
            query += " AND id = %s"
            params.append(category_filter)
        cur.execute(query, params)
        categories = cur.fetchall()

    if not categories:
        log.error("No enabled chart categories found")
        sys.exit(1)

    # determine dates to process
    dates = [target_date - timedelta(days=i) for i in range(backfill_days + 1)]
    dates.reverse()  # oldest first

    log.info(f"Scoring {len(dates)} date(s) × {len(categories)} categories")

    for window_date in dates:
        for category in categories:
            try:
                aggregate_window(conn, window_date, dict(category))
            except Exception as e:
                log.error(f"Failed {category['id']} {window_date}: {e}")
                conn.rollback()

        # market trend after all categories for this date
        try:
            compute_market_trend(conn, window_date)
        except Exception as e:
            log.error(f"Market trend failed for {window_date}: {e}")
            conn.rollback()

    conn.close()
    log.info("Aggregator complete")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Score signal events into song_scores")
    parser.add_argument("--date",     type=date.fromisoformat,
                        help="Window date to score (default: today)")
    parser.add_argument("--backfill", type=int, default=0,
                        help="Also score N days before the target date")
    parser.add_argument("--category", type=str,
                        help="Only score one chart category")
    args = parser.parse_args()
    run(
        target_date=args.date,
        backfill_days=args.backfill,
        category_filter=args.category,
    )
