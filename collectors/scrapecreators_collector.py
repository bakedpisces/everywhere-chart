"""
ScrapeCreators Catalog Enrichment Collector
--------------------------------------------
For songs in the catalog, queries TikTok and YouTube via ScrapeCreators to
fill in signals for songs that aren't in the top-200 trending charts.

Per song, two lookups are made (2 credits total):
  1. TikTok — keyword search → find official sound → record user_count
     (# TikTok videos created using that sound)
     signal: source_platform='tiktok', signal_type='sound_use'

  2. YouTube — video search → find official video → record viewCountInt
     (cumulative view count; tracked daily to measure growth over time)
     signal: source_platform='youtube', signal_type='chart_position'

Both use the same schema as their respective chart collectors, so dashboard
scoring works identically. external_id is prefixed with 'sc_' to distinguish
ScrapeCreators-sourced signals from chart-scraped ones.

Priority: songs already on some chart come first (most likely to match).
Rechecked every RECHECK_DAYS days — daily runs build a view-count time series.

Schedule: daily at 10:00 UTC
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

MAX_SONGS           = 500    # songs per run (2 credits each = 1000 credits/day)
RECHECK_DAYS        = 7      # reprocess songs after this many days
MIN_TT_USER_COUNT   = 10     # ignore TikTok sounds with fewer videos
TT_MATCH_THRESHOLD  = 0.40   # token-overlap score to accept a TikTok sound
YT_MATCH_THRESHOLD  = 0.35   # slightly looser — video titles have more noise

INTENTIONALITY_TT   = 0.80   # matches TikTok Creative Center collector
INTENTIONALITY_YT   = 0.20   # matches YouTube chart collector

# Noise words to strip when classifying YouTube videos
_YT_OFFICIAL = re.compile(
    r"\b(official\s*(music\s*)?video|official\s*audio|official\s*lyric|"
    r"lyric\s*video|lyrics|audio|visualizer|mv|hd|4k)\b",
    re.IGNORECASE,
)
_YT_SKIP = re.compile(
    r"\b(cover|covers|covered|reaction|reacts|tutorial|karaoke|"
    r"instrumental|nightcore|remix|slowed|sped\s*up|reverb|parody)\b",
    re.IGNORECASE,
)

# ── Shared normalisation ──────────────────────────────────────────────────────

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
    return {t for t in _norm(s).split() if len(t) > 1}


def _overlap(a: set, b: set) -> float:
    if not a or not b:
        return 0.0
    return len(a & b) / max(len(a), len(b))


# ── TikTok helpers ────────────────────────────────────────────────────────────

def tt_match_score(catalog_title: str, catalog_artist: str,
                   sound_title: str, sound_author: str) -> float:
    """60% title overlap + 40% artist overlap. Returns 0 if no title overlap."""
    title_score = _overlap(_tokens(catalog_title), _tokens(sound_title))
    if title_score == 0:
        return 0.0
    artist_score = _overlap(_tokens(catalog_artist), _tokens(sound_author))
    return 0.6 * title_score + 0.4 * artist_score


def search_tiktok_sounds(title: str, artist: str) -> list[dict]:
    """
    Search TikTok for '{title} {artist}', return deduplicated sounds sorted
    by user_count descending. Raises RuntimeError on fatal API errors.
    """
    try:
        resp = requests.get(
            f"{SC_BASE}/v1/tiktok/search/keyword",
            params={"query": f"{title} {artist}"},
            headers={"x-api-key": SC_API_KEY},
            timeout=20,
        )
        _check_fatal(resp, "TikTok search")
        if not resp.ok:
            log.warning(f"TikTok search failed ({resp.status_code}) for '{title}'")
            return []

        items = resp.json().get("search_item_list", [])
        seen: dict[str, dict] = {}
        for item in items:
            music = item.get("aweme_info", {}).get("music", {})
            mid   = str(music.get("id", "")) if music.get("id") else ""
            if not mid or mid in seen:
                continue
            uc = int(music.get("user_count", 0))
            if uc < MIN_TT_USER_COUNT:
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
        log.warning(f"TikTok search error for '{title}': {e}")
        return []


def find_best_sound(title: str, artist: str, sounds: list[dict]) -> Optional[dict]:
    best, best_score = None, 0.0
    for s in sounds:
        score = tt_match_score(title, artist, s["title"], s["author"])
        if score > best_score:
            best_score, best = score, s
    return best if best and best_score >= TT_MATCH_THRESHOLD else None


# ── YouTube helpers ───────────────────────────────────────────────────────────

def yt_match_score(catalog_title: str, catalog_artist: str,
                   video_title: str, channel_name: str) -> float:
    """
    Score a YouTube video against the catalog entry.
    Title overlap is mandatory. Boosts for artist-in-channel match.
    Penalises covers, reactions, remixes.
    """
    if _YT_SKIP.search(video_title):
        return 0.0

    title_score = _overlap(_tokens(catalog_title), _tokens(video_title))
    if title_score == 0:
        return 0.0

    artist_score = _overlap(_tokens(catalog_artist), _tokens(channel_name))
    return 0.6 * title_score + 0.4 * artist_score


def search_youtube_video(title: str, artist: str) -> Optional[dict]:
    """
    Search YouTube for '{title} {artist}', return the best-matching video
    with its view count, or None if nothing clears YT_MATCH_THRESHOLD.
    Prefers official music videos / official audio over lyric videos.
    """
    try:
        resp = requests.get(
            f"{SC_BASE}/v1/youtube/search",
            params={"query": f"{title} {artist}"},
            headers={"x-api-key": SC_API_KEY},
            timeout=20,
        )
        _check_fatal(resp, "YouTube search")
        if not resp.ok:
            log.warning(f"YouTube search failed ({resp.status_code}) for '{title}'")
            return None

        videos = resp.json().get("videos", [])
        if not videos:
            return None

        ct = _tokens(catalog_title := title)
        ca = _tokens(catalog_artist := artist)

        best, best_score, best_tier = None, 0.0, 99

        for v in videos:
            vt    = v.get("title", "")
            ch    = v.get("channel", {}).get("title", "")
            views = v.get("viewCountInt") or 0

            score = yt_match_score(title, artist, vt, ch)
            if score < YT_MATCH_THRESHOLD:
                continue

            # Tier: 0=official MV, 1=official audio, 2=lyric video, 3=other
            vt_lo = vt.lower()
            if "official music video" in vt_lo or ("official" in vt_lo and "video" in vt_lo):
                tier = 0
            elif "official audio" in vt_lo or "official" in vt_lo:
                tier = 1
            elif "lyric" in vt_lo:
                tier = 2
            else:
                tier = 3

            # Pick by: best tier first, then highest score, then most views
            if (tier < best_tier or
                    (tier == best_tier and score > best_score) or
                    (tier == best_tier and score == best_score and views > (best["viewCountInt"] or 0) if best else False)):
                best_tier  = tier
                best_score = score
                best       = v

        if not best:
            return None

        return {
            "video_id":   best.get("id", ""),
            "title":      best.get("title", ""),
            "channel":    best.get("channel", {}).get("title", ""),
            "view_count": best.get("viewCountInt") or 0,
            "url":        best.get("url", ""),
            "published":  best.get("publishedTimeText", ""),
        }

    except RuntimeError:
        raise
    except Exception as e:
        log.warning(f"YouTube search error for '{title}': {e}")
        return None


# ── API error handling ────────────────────────────────────────────────────────

def _check_fatal(resp: requests.Response, context: str):
    if resp.status_code == 402:
        log.error(f"ScrapeCreators: out of credits (402) during {context}. Stopping.")
        raise RuntimeError("out_of_credits")
    if resp.status_code == 401:
        log.error(f"ScrapeCreators: invalid API key (401) during {context}.")
        raise RuntimeError("invalid_api_key")


# ── Database helpers ──────────────────────────────────────────────────────────

def load_songs(cur, limit: int) -> list[dict]:
    """
    Songs that haven't had any ScrapeCreators lookup in the last RECHECK_DAYS.

    Priority order:
      1. Under-radar songs (new + UGC playlist + not on Spotify chart)
      2. Top songs — on a Spotify or Shazam chart right now
      3. Everything else (catalog songs with some signals, or none)
         sorted by playlist follower count so the most-playlisted come first
    """
    cur.execute(f"""
        WITH last_sc AS (
            SELECT song_id, MAX(observed_at) AS last_at
            FROM signal_events
            WHERE external_id LIKE 'sc_%%'
            GROUP BY song_id
        ),
        on_chart AS (
            SELECT DISTINCT song_id
            FROM signal_events
            WHERE source_platform IN ('spotify', 'shazam')
              AND signal_type = 'chart_position'
              AND observed_at >= NOW() - INTERVAL '14 days'
        )
        SELECT
            s.id              AS song_id,
            s.title,
            s.title_normalized,
            a.name            AS artist,
            a.name_normalized AS artist_normalized,
            s.under_radar,
            s.release_date,
            s.playlist_follower_count,
            (oc.song_id IS NOT NULL) AS on_chart
        FROM songs s
        JOIN artists a ON a.id = s.artist_id
        LEFT JOIN last_sc lc ON lc.song_id = s.id
        LEFT JOIN on_chart oc ON oc.song_id = s.id
        WHERE lc.song_id IS NULL
           OR lc.last_at < NOW() - INTERVAL '{RECHECK_DAYS} days'
        ORDER BY
            s.under_radar DESC,                          -- tier 1: under-radar
            (oc.song_id IS NOT NULL) DESC,               -- tier 2: charting songs
            s.playlist_follower_count DESC NULLS LAST,   -- tier 3: by playlist reach
            s.created_at DESC
        LIMIT %s
    """, (limit,))
    return cur.fetchall()


def write_tiktok_signal(cur, song_id: str, sound: dict, now: datetime):
    usage    = sound["user_count"]
    eng_mult = round(min(2.5, 1 + math.log10(max(usage, 1)) / 6), 3)
    weighted = round(INTENTIONALITY_TT * eng_mult, 4)
    cur.execute("""
        INSERT INTO signal_events (
            observed_at, song_id, source_platform, signal_type,
            intentionality_score, raw_engagement, engagement_multiplier,
            weighted_score, resolution_confidence, is_home_community,
            external_id, context_snapshot
        ) VALUES (%s, %s, 'tiktok', 'sound_use',
                  %s, %s, %s, %s, 1.0, FALSE, %s, %s)
        ON CONFLICT DO NOTHING
    """, (
        now, song_id,
        INTENTIONALITY_TT,
        psycopg2.extras.Json({"usage_count": usage}),
        eng_mult, weighted,
        f"sc_{sound['sound_id']}",
        psycopg2.extras.Json({
            "source":       "scrapecreators",
            "sound_id":     sound["sound_id"],
            "sound_title":  sound["title"],
            "sound_author": sound["author"],
            "usage_count":  usage,
        }),
    ))


def write_tiktok_no_match(cur, song_id: str, now: datetime):
    cur.execute("""
        INSERT INTO signal_events (
            observed_at, song_id, source_platform, signal_type,
            intentionality_score, raw_engagement, engagement_multiplier,
            weighted_score, resolution_confidence, is_home_community,
            external_id, context_snapshot
        ) VALUES (%s, %s, 'tiktok', 'sound_use',
                  0.0, %s, 1.0, 0.0, 0.0, FALSE, 'sc_tt_no_match', %s)
        ON CONFLICT DO NOTHING
    """, (
        now, song_id,
        psycopg2.extras.Json({"usage_count": 0}),
        psycopg2.extras.Json({"source": "scrapecreators", "result": "no_match"}),
    ))


def write_youtube_signal(cur, song_id: str, video: dict, now: datetime):
    views    = video["view_count"]
    # Log-scale multiplier: 1M views → ~1.67, 100M → ~1.89, 1B → ~2.0
    eng_mult = round(min(2.5, 1 + math.log10(max(views, 1)) / 9), 3)
    weighted = round(INTENTIONALITY_YT * eng_mult, 4)
    cur.execute("""
        INSERT INTO signal_events (
            observed_at, song_id, source_platform, signal_type,
            intentionality_score, raw_engagement, engagement_multiplier,
            weighted_score, resolution_confidence, is_home_community,
            external_id, external_url, context_snapshot
        ) VALUES (%s, %s, 'youtube', 'chart_position',
                  %s, %s, %s, %s, 1.0, FALSE, %s, %s, %s)
        ON CONFLICT DO NOTHING
    """, (
        now, song_id,
        INTENTIONALITY_YT,
        psycopg2.extras.Json({"view_count": views}),
        eng_mult, weighted,
        f"sc_yt_{video['video_id']}",
        video["url"],
        psycopg2.extras.Json({
            "source":      "scrapecreators",
            "video_id":    video["video_id"],
            "video_title": video["title"],
            "channel":     video["channel"],
            "view_count":  views,
            "published":   video["published"],
        }),
    ))


def write_youtube_no_match(cur, song_id: str, now: datetime):
    cur.execute("""
        INSERT INTO signal_events (
            observed_at, song_id, source_platform, signal_type,
            intentionality_score, raw_engagement, engagement_multiplier,
            weighted_score, resolution_confidence, is_home_community,
            external_id, context_snapshot
        ) VALUES (%s, %s, 'youtube', 'chart_position',
                  0.0, %s, 1.0, 0.0, 0.0, FALSE, 'sc_yt_no_match', %s)
        ON CONFLICT DO NOTHING
    """, (
        now, song_id,
        psycopg2.extras.Json({"view_count": 0}),
        psycopg2.extras.Json({"source": "scrapecreators", "result": "no_match"}),
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

    log.info(f"ScrapeCreators collector: {len(songs)} songs to process "
             f"(~{len(songs) * 2} credits)")

    tt_hits = tt_misses = yt_hits = yt_misses = errors = 0

    for song in songs:
        song_id = str(song["song_id"])
        title   = song["title"]
        artist  = song["artist"]
        log.info(f"  '{title}' — {artist}")

        # ── TikTok lookup ──────────────────────────────────────────────────
        try:
            sounds   = search_tiktok_sounds(title, artist)
            tt_match = find_best_sound(title, artist, sounds)
        except RuntimeError as e:
            log.error(f"Fatal: {e} — stopping run")
            break
        except Exception as e:
            log.warning(f"    TikTok error: {e}")
            sounds   = []
            tt_match = None
            errors  += 1

        # ── YouTube lookup ─────────────────────────────────────────────────
        time.sleep(0.2)
        try:
            yt_match = search_youtube_video(title, artist)
        except RuntimeError as e:
            log.error(f"Fatal: {e} — stopping run")
            break
        except Exception as e:
            log.warning(f"    YouTube error: {e}")
            yt_match = None
            errors  += 1

        # ── Write both signals ─────────────────────────────────────────────
        try:
            with conn.cursor() as cur:
                if tt_match:
                    write_tiktok_signal(cur, song_id, tt_match, now)
                    log.info(
                        f"    TT ✓ {tt_match['user_count']:>10,} videos | "
                        f"'{tt_match['title']}' by '{tt_match['author']}'"
                    )
                    tt_hits += 1
                else:
                    write_tiktok_no_match(cur, song_id, now)
                    log.info(f"    TT — no match ({len(sounds)} sounds checked)")
                    tt_misses += 1

                if yt_match:
                    write_youtube_signal(cur, song_id, yt_match, now)
                    log.info(
                        f"    YT ✓ {yt_match['view_count']:>12,} views | "
                        f"'{yt_match['title']}'"
                    )
                    yt_hits += 1
                else:
                    write_youtube_no_match(cur, song_id, now)
                    log.info(f"    YT — no match")
                    yt_misses += 1

            conn.commit()
        except Exception as e:
            conn.rollback()
            log.warning(f"    DB error for '{title}': {e}")
            errors += 1

        time.sleep(0.3)

    conn.close()
    log.info(
        f"ScrapeCreators collector done — "
        f"TikTok: {tt_hits} hits / {tt_misses} misses | "
        f"YouTube: {yt_hits} hits / {yt_misses} misses | "
        f"{errors} errors"
    )


if __name__ == "__main__":
    run()
