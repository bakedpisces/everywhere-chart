"""
TikTok Trending Sounds Collector
---------------------------------
Scrapes TikTok Creative Center's trending music page using Playwright.
No login or API key required — the Creative Center is public (built for
advertisers) and its internal JSON API is intercepted in-flight.

Source: https://ads.tiktok.com/business/creativecenter/inspiration/popular/music/pc/en
Internal API: ads.tiktok.com/business/creativecenter/api/v1/tp/music/list

Captures top 50 trending sounds per region. Usage count is the raw
engagement signal — TikTok usage is intentional (a creator explicitly
chose this sound), making it a high-intentionality crossover indicator.

Schedule: daily at 09:00 UTC (after Spotify runs at 08:00)
"""

import os
import re
import time
import logging
import psycopg2
import psycopg2.extras
from datetime import datetime, date, timezone
from typing import Optional

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("tiktok_collector")

DB_URL = os.environ["DATABASE_URL"]

CREATIVE_CENTER_URL = (
    "https://ads.tiktok.com/business/creativecenter/inspiration/popular/music/pc/en"
)
API_LIST_URL = "https://ads.tiktok.com/creative_radar_api/v1/popular_trend/sound/rank_list"

REGIONS = [
    {"country_code": "US", "label": "us"},
    {"country_code": "GB", "label": "uk"},
    {"country_code": "AU", "label": "au"},
    {"country_code": "BR", "label": "br"},
]

# TikTok usage = creator intentionally chose this sound — high intentionality
INTENTIONALITY_TIKTOK = 0.80

# ── Chart fetch ───────────────────────────────────────────────────────────────


def _fetch_via_browser(page, country_code: str, page_num: int = 1) -> Optional[dict]:
    """
    Call the rank_list API from inside the browser using fetch() with
    credentials:include — this automatically carries whatever cookies/headers
    TikTok set during page load, bypassing the need to intercept requests.
    """
    url = (
        f"{API_LIST_URL}"
        f"?rank_type=popular&period=7&page={page_num}&limit=20"
        f"&new_on_board=false&country_code={country_code}"
    )
    try:
        result = page.evaluate(f"""
            async () => {{
                try {{
                    const resp = await fetch("{url}", {{
                        credentials: "include",
                        headers: {{
                            "Accept": "application/json, text/plain, */*",
                        }}
                    }});
                    const data = await resp.json();
                    return {{ status: resp.status, data: data }};
                }} catch(e) {{
                    return {{ error: e.toString() }};
                }}
            }}
        """)
        if not result:
            return None
        if result.get("error"):
            log.warning(f"Browser fetch error for {country_code} p{page_num}: {result['error']}")
            return None
        if result.get("status") != 200:
            log.warning(f"Browser fetch status {result.get('status')} for {country_code} p{page_num}")
            return None
        data = result.get("data", {})
        if data.get("code") != 0:
            log.warning(
                f"rank_list API error for {country_code} p{page_num}: "
                f"code={data.get('code')} msg={data.get('msg')}"
            )
            return None
        return data
    except Exception as e:
        log.warning(f"_fetch_via_browser failed for {country_code} p{page_num}: {e}")
        return None


def fetch_all_regions() -> dict[str, list[dict]]:
    """
    Launch ONE browser, load the Creative Center page to establish a valid
    session, then call the rank_list API from within the browser context
    using page.evaluate() + fetch(credentials:include).

    This sidesteps the request-interception approach entirely — the browser
    already has the right cookies from page load, so in-browser fetch()
    calls are automatically authenticated.

    Returns {region_label: [rows]}.
    """
    from playwright.sync_api import sync_playwright

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            )
        )
        pg = context.new_page()

        # Log all TikTok API calls for diagnostics (don't need to intercept now)
        def on_request(request):
            url = request.url
            if "ads.tiktok.com" in url and any(
                k in url for k in ("rank_list", "sound", "music", "trend", "radar")
            ):
                log.info(f"[diag] TikTok API call: {url[:120]}")

        pg.on("request", on_request)

        try:
            # Load the music page to establish session cookies
            log.info("Loading Creative Center music page to establish session …")
            try:
                pg.goto(
                    f"{CREATIVE_CENTER_URL}?country_code=US",
                    wait_until="domcontentloaded",
                    timeout=30_000,
                )
                # Brief pause to let JS execute and set session cookies
                pg.wait_for_timeout(4_000)
            except Exception as e:
                log.warning(f"Page load warning (continuing anyway): {e}")

            # Now call the API from inside the browser for each region
            results = {}
            for region in REGIONS:
                cc    = region["country_code"]
                label = region["label"]
                all_items: list[dict] = []

                for page_num in range(1, 4):
                    data = _fetch_via_browser(pg, cc, page_num)
                    if not data:
                        break
                    items = data.get("data", {}).get("sound_list") or []
                    if not items:
                        break
                    all_items.extend(items)
                    log.info(f"  {label} p{page_num}: {len(items)} tracks (total {len(all_items)})")
                    time.sleep(1)

                if all_items:
                    results[label] = [
                        {"rank": i + 1, **item, "region": label}
                        for i, item in enumerate(all_items)
                    ]
                    log.info(f"  {label}: {len(all_items)} total tracks ✓")
                else:
                    log.warning(f"  {label}: no tracks returned")
                time.sleep(2)

            return results

        except Exception as e:
            log.warning(f"TikTok browser session failed: {e}")
            return {}
        finally:
            context.close()
            browser.close()


def _parse_items(raw_items: list, region_label: str) -> list[dict]:
    """Normalise raw sound_list items into track row dicts."""
    rows = []
    for i, item in enumerate(raw_items, start=1):
        title  = (item.get("music_name") or item.get("title") or item.get("name") or "").strip()
        artist = (item.get("author") or item.get("artist_name") or item.get("artist") or "").strip()
        if not title or not artist:
            continue
        usage_count = int(
            item.get("item_count")
            or item.get("use_count")
            or item.get("video_count")
            or item.get("rank_value")
            or 0
        )
        rows.append({
            "rank":        item.get("rank", i),
            "title":       title,
            "artist":      artist,
            "tiktok_id":   str(item.get("music_id") or item.get("id") or ""),
            "usage_count": usage_count,
            "region":      region_label,
        })
    return rows


# ── Song resolution ───────────────────────────────────────────────────────────

def normalize(s: str) -> str:
    return re.sub(r"[^\w\s]", "", s.lower().strip())


def resolve_song(cur, row: dict) -> tuple[Optional[str], float]:
    """Match TikTok track to existing catalog entry. Returns (song_id, confidence)."""

    title_norm  = normalize(row["title"])
    artist_norm = normalize(row["artist"])

    # 1. Exact normalized match
    cur.execute("""
        SELECT s.id FROM songs s
        JOIN artists a ON s.artist_id = a.id
        WHERE s.title_normalized = %s
          AND a.name_normalized  = %s
        LIMIT 1
    """, (title_norm, artist_norm))
    result = cur.fetchone()
    if result:
        return str(result["id"]), 0.95

    # 2. Fuzzy match via pg_trgm
    cur.execute("""
        SELECT s.id,
               similarity(s.title_normalized, %s) AS tsim,
               similarity(a.name_normalized,  %s) AS asim
        FROM songs s
        JOIN artists a ON s.artist_id = a.id
        WHERE similarity(s.title_normalized, %s) > 0.55
          AND similarity(a.name_normalized,  %s) > 0.45
        ORDER BY (similarity(s.title_normalized, %s) + similarity(a.name_normalized, %s)) DESC
        LIMIT 1
    """, (title_norm, artist_norm, title_norm, artist_norm, title_norm, artist_norm))
    result = cur.fetchone()
    if result:
        combined = (result["tsim"] + result["asim"]) / 2
        if combined >= 0.70:
            return str(result["id"]), round(combined * 0.90, 3)

    return None, 0.0


def upsert_song_from_tiktok(cur, row: dict) -> str:
    """
    Add a TikTok trending sound to the catalog when resolution fails.
    Returns song_id.
    """
    artist_norm = normalize(row["artist"])
    title_norm  = normalize(row["title"])
    fake_artist_id = f"tiktok_{artist_norm[:40]}"

    cur.execute("""
        INSERT INTO artists (name, name_normalized, spotify_artist_id, genre_tags)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (spotify_artist_id) DO UPDATE SET
            name = EXCLUDED.name, updated_at = NOW()
        RETURNING id
    """, (row["artist"], artist_norm, fake_artist_id, []))
    artist_row = cur.fetchone()
    if artist_row:
        artist_id = str(artist_row["id"])
    else:
        cur.execute("SELECT id FROM artists WHERE name_normalized = %s LIMIT 1", (artist_norm,))
        artist_id = str(cur.fetchone()["id"])

    fake_track_id = f"tiktok_{row['tiktok_id']}" if row["tiktok_id"] else f"tiktok_{artist_norm[:20]}_{title_norm[:20]}"

    cur.execute("""
        INSERT INTO songs (title, title_normalized, artist_id, spotify_track_id, genre_tags)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (spotify_track_id) DO NOTHING
        RETURNING id
    """, (row["title"], title_norm, artist_id, fake_track_id, []))
    song_row = cur.fetchone()
    if song_row:
        song_id = str(song_row["id"])
        log.info(f"New song from TikTok: {row['title']} — {row['artist']}")
    else:
        cur.execute("SELECT id FROM songs WHERE spotify_track_id = %s", (fake_track_id,))
        song_id = str(cur.fetchone()["id"])

    return song_id


def queue_unresolved(cur, row: dict, snapshot_date: date):
    """Queue unmatched TikTok tracks for future resolution."""
    external_id = f"tiktok::{row['tiktok_id']}::{row['region']}::{snapshot_date}"
    cur.execute("""
        INSERT INTO resolution_queue (
            raw_text, context_json, source_platform,
            observed_at, external_id, status
        )
        VALUES (%s, %s, 'tiktok', %s, %s, 'pending')
        ON CONFLICT DO NOTHING
    """, (
        f"{row['title']} by {row['artist']}",
        psycopg2.extras.Json({
            "title":       row["title"],
            "artist":      row["artist"],
            "tiktok_id":   row["tiktok_id"],
            "usage_count": row["usage_count"],
            "region":      row["region"],
            "rank":        row["rank"],
        }),
        datetime.combine(snapshot_date, datetime.min.time()).replace(tzinfo=timezone.utc),
        external_id,
    ))


# ── Signal writer ─────────────────────────────────────────────────────────────

def write_signal(cur, song_id: str, row: dict,
                 snapshot_date: date, confidence: float):
    """Write a signal_events row for a TikTok trending sound."""
    # Scale engagement: 1M+ uses → multiplier ~2.0, 10k uses → ~1.0
    usage = max(row["usage_count"], 1)
    import math
    engagement_mult = round(min(2.5, 1 + math.log10(usage) / 6), 3)
    weighted        = round(INTENTIONALITY_TIKTOK * engagement_mult, 4)

    external_id = f"tiktok::{row['tiktok_id']}::{row['region']}::{snapshot_date}"

    cur.execute("""
        INSERT INTO signal_events (
            observed_at, song_id, source_platform, signal_type,
            intentionality_score, raw_engagement,
            engagement_multiplier, weighted_score,
            resolution_confidence, is_home_community,
            external_id, context_snapshot
        )
        VALUES (%s, %s, 'tiktok', 'sound_use',
                %s, %s, %s, %s, %s, FALSE, %s, %s)
        ON CONFLICT DO NOTHING
    """, (
        datetime.combine(snapshot_date, datetime.min.time()).replace(tzinfo=timezone.utc),
        song_id,
        INTENTIONALITY_TIKTOK,
        psycopg2.extras.Json({"usage_count": usage, "rank": row["rank"]}),
        engagement_mult,
        weighted,
        confidence,
        external_id,
        psycopg2.extras.Json({
            "tiktok_id":   row["tiktok_id"],
            "title":       row["title"],
            "artist":      row["artist"],
            "region":      row["region"],
            "rank":        row["rank"],
            "usage_count": usage,
        }),
    ))


# ── Main ──────────────────────────────────────────────────────────────────────

def run(snapshot_date: date = None):
    snapshot_date = snapshot_date or date.today()

    conn = psycopg2.connect(DB_URL)
    conn.autocommit = False
    psycopg2.extras.register_uuid()

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            INSERT INTO collector_runs (collector, metadata)
            VALUES ('tiktok', %s) RETURNING id
        """, (psycopg2.extras.Json({"snapshot_date": str(snapshot_date)}),))
        run_id = cur.fetchone()["id"]
    conn.commit()

    total_events  = 0
    total_queued  = 0
    total_dropped = 0

    try:
        all_region_data = fetch_all_regions()
        if not all_region_data:
            log.warning("No data returned from TikTok — aborting")
            raise RuntimeError("fetch_all_regions returned empty")

        for region_label, raw_items in all_region_data.items():
            rows = _parse_items(raw_items, region_label)
            if not rows:
                log.warning(f"No parseable rows for TikTok {region_label}")
                continue

            for row in rows:
                try:
                    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                        song_id, confidence = resolve_song(cur, row)

                        if confidence < 0.65:
                            # Add to catalog so the signal is counted now
                            song_id = upsert_song_from_tiktok(cur, row)
                            confidence = 0.70
                            total_queued += 1

                        write_signal(cur, song_id, row, snapshot_date, confidence)
                        total_events += 1
                    conn.commit()

                except Exception as e:
                    conn.rollback()
                    log.error(f"Failed processing TikTok row '{row.get('title')}': {e}")
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
            f"TikTok collector complete — "
            f"{total_events} events, {total_queued} queued, {total_dropped} dropped"
        )

    except Exception as e:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE collector_runs
                SET status = 'failed', completed_at = NOW(), error_message = %s
                WHERE id = %s
            """, (str(e), run_id))
        conn.commit()
        log.error(f"TikTok collector failed: {e}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    import sys
    target = date.fromisoformat(sys.argv[1]) if len(sys.argv) > 1 else date.today()
    run(target)
