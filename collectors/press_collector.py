"""
Press / Google News Collector
------------------------------
Queries Google News RSS for each tracked song and writes signal_events when
mentions surface in non-music or crossover contexts.

The key insight: a mention in ESPN or Vogue carries far more crossover weight
than a mention in Pitchfork. Publication type drives the intentionality score.

Also polls a small set of fixed RSS feeds for publications that don't surface
well in Google News (Bandcamp Daily, Pitchfork, etc.).

Schedule: every 6 hours. Queries top ~200 songs by recent signal activity.
"""

import re
import os
import math
import time
import logging
import feedparser
import requests
import psycopg2
import psycopg2.extras
from collections import defaultdict
from datetime import datetime, date, timezone, timedelta
from urllib.parse import quote_plus, urlparse
from typing import Optional

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("press_collector")

DB_URL = os.environ["DATABASE_URL"]

RSS_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36"
    )
}

# ── Publication type classification ──────────────────────────────────────────
#
# casual_weight: how likely this audience is to be a non-fan discovering the song
# intentionality: editorial precision (press mentions are deliberate)
# crossover_bonus: multiplier on top of base when this type appears (vs music press)

PUB_TYPES = {
    # type                  casual_weight  intentionality  crossover_bonus
    "general_news":         (0.95,         0.85,           1.4),
    "entertainment":        (0.88,         0.82,           1.2),
    "sports":               (0.95,         0.80,           1.5),
    "lifestyle":            (0.90,         0.78,           1.4),
    "tech":                 (0.85,         0.75,           1.3),
    "general_music":        (0.70,         0.80,           1.0),  # baseline — expected
    "industry":             (0.60,         0.85,           0.9),  # trade press
}

# Domain → publication type.  Everything else falls through to keyword heuristics.
DOMAIN_TYPE_MAP = {
    # General news
    "nytimes.com":          "general_news",
    "washingtonpost.com":   "general_news",
    "theguardian.com":      "general_news",
    "bbc.com":              "general_news",
    "bbc.co.uk":            "general_news",
    "cnn.com":              "general_news",
    "apnews.com":           "general_news",
    "reuters.com":          "general_news",
    "npr.org":              "general_news",
    "time.com":             "general_news",
    "theatlantic.com":      "general_news",
    "newyorker.com":        "general_news",
    "usatoday.com":         "general_news",
    "latimes.com":          "general_news",
    "chicagotribune.com":   "general_news",
    # Entertainment
    "variety.com":          "entertainment",
    "hollywoodreporter.com":"entertainment",
    "ew.com":               "entertainment",
    "people.com":           "entertainment",
    "tmz.com":              "entertainment",
    "deadline.com":         "entertainment",
    "vulture.com":          "entertainment",
    "indiewire.com":        "entertainment",
    # Sports
    "espn.com":             "sports",
    "si.com":               "sports",
    "bleacherreport.com":   "sports",
    "theathletic.com":      "sports",
    "cbssports.com":        "sports",
    "nbcsports.com":        "sports",
    "sportingnews.com":     "sports",
    # Lifestyle / fashion / culture
    "vogue.com":            "lifestyle",
    "teenvogue.com":        "lifestyle",
    "gq.com":               "lifestyle",
    "elle.com":             "lifestyle",
    "cosmopolitan.com":     "lifestyle",
    "glamour.com":          "lifestyle",
    "harpersbazaar.com":    "lifestyle",
    "buzzfeed.com":         "lifestyle",
    "refinery29.com":       "lifestyle",
    "complex.com":          "lifestyle",
    "hypebeast.com":        "lifestyle",
    "highsnobiety.com":     "lifestyle",
    "yahoo.com":            "entertainment",  # Yahoo Entertainment / Yahoo Music
    # Tech
    "wired.com":            "tech",
    "techcrunch.com":       "tech",
    "theverge.com":         "tech",
    "vice.com":             "tech",
    # Music press — these are the baseline, lower crossover weight
    "pitchfork.com":        "general_music",
    "rollingstone.com":     "general_music",
    "stereogum.com":        "general_music",
    "consequence.net":      "general_music",
    "nme.com":              "general_music",
    "uproxx.com":           "general_music",
    "spin.com":             "general_music",
    "theneedle.com":        "general_music",
    "diymag.com":           "general_music",
    # Industry trade
    "billboard.com":        "industry",
    "musicweek.com":        "industry",
    "musicbusiness.com":    "industry",
}

# Keyword heuristics for unknown domains
DOMAIN_KEYWORD_TYPES = [
    (["sport", "football", "basketball", "soccer", "nfl", "nba", "mlb"], "sports"),
    (["fashion", "style", "beauty", "lifestyle", "wellness"],             "lifestyle"),
    (["tech", "digital", "gaming", "game"],                               "tech"),
    (["music", "sound", "audio", "chart", "hiphop", "rap", "indie"],     "general_music"),
    (["entertain", "celebrity", "movie", "film", "tv", "television"],    "entertainment"),
]

def classify_domain(domain: str) -> str:
    domain = domain.lower().lstrip("www.")
    if domain in DOMAIN_TYPE_MAP:
        return DOMAIN_TYPE_MAP[domain]
    for keywords, pub_type in DOMAIN_KEYWORD_TYPES:
        if any(kw in domain for kw in keywords):
            return pub_type
    return "general_news"   # default: assume broad audience

# ── Fixed supplemental feeds ──────────────────────────────────────────────────
# Publications that don't surface well in Google News queries but are valuable
# for their editorial depth (Bandcamp, Pitchfork long-form, etc.)

FIXED_FEEDS = [
    {
        "name":     "Pitchfork",
        "slug":     "pitchfork",
        "feed_url": "https://pitchfork.com/feed/feed-news/rss",
        "pub_type": "general_music",
    },
    {
        "name":     "Bandcamp Daily",
        "slug":     "bandcamp-daily",
        "feed_url": "https://daily.bandcamp.com/feed/",
        "pub_type": "general_music",
    },
    {
        "name":     "NPR Music",
        "slug":     "npr-music",
        "feed_url": "https://feeds.npr.org/1039/rss.xml",
        "pub_type": "general_news",
    },
    {
        "name":     "Rolling Stone",
        "slug":     "rolling-stone",
        "feed_url": "https://www.rollingstone.com/music/music-news/feed/",
        "pub_type": "general_music",
    },
    {
        "name":     "Stereogum",
        "slug":     "stereogum",
        "feed_url": "https://www.stereogum.com/feed/",
        "pub_type": "general_music",
    },
    {
        "name":     "The Guardian Music",
        "slug":     "guardian-music",
        "feed_url": "https://www.theguardian.com/music/rss",
        "pub_type": "general_news",
    },
    {
        "name":     "Billboard",
        "slug":     "billboard",
        "feed_url": "https://www.billboard.com/feed/",
        "pub_type": "industry",
    },
    {
        "name":     "DIY Magazine",
        "slug":     "diy-magazine",
        "feed_url": "https://diymag.com/feed",
        "pub_type": "general_music",
    },
    {
        "name":     "Uproxx",
        "slug":     "uproxx",
        "feed_url": "https://uproxx.com/music/feed/",
        "pub_type": "general_music",
    },
]

MAX_SONGS_PER_RUN  = 200   # how many catalog songs to query Google News for
GOOGLE_NEWS_DELAY  = 2.5   # seconds between GN requests (be polite)
MIN_ARTICLE_AGE_DAYS = 7   # ignore articles older than this

# ── Community upsert ──────────────────────────────────────────────────────────

def upsert_community(cur, slug: str, name: str, pub_type: str) -> str:
    casual_weight, _, _ = PUB_TYPES.get(pub_type, PUB_TYPES["general_news"])
    # Map pub_type to the DB community_type enum
    db_type = {
        "general_news":  "entertainment",
        "entertainment": "entertainment",
        "sports":        "entertainment",
        "lifestyle":     "lifestyle",
        "tech":          "entertainment",
        "general_music": "general_music",
        "industry":      "general_music",
    }.get(pub_type, "entertainment")

    cur.execute("""
        INSERT INTO communities (
            platform, external_id, display_name, community_type, casual_weight
        )
        VALUES ('other', %s, %s, %s, %s)
        ON CONFLICT (platform, external_id) DO UPDATE
            SET display_name  = EXCLUDED.display_name,
                casual_weight = EXCLUDED.casual_weight
        RETURNING id
    """, (slug, name, db_type, casual_weight))
    return str(cur.fetchone()["id"])

# ── Song catalog ──────────────────────────────────────────────────────────────

def get_songs_to_query(cur, limit: int) -> list[dict]:
    """
    Return the top `limit` songs by recent signal activity — these are the ones
    worth querying Google News for.  Falls back to all songs if signal table is sparse.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(days=30)
    cur.execute("""
        SELECT s.id, s.title, a.name AS artist
        FROM songs s
        JOIN artists a ON s.artist_id = a.id
        WHERE EXISTS (
            SELECT 1 FROM signal_events se
            WHERE se.song_id = s.id AND se.observed_at >= %s
        )
        ORDER BY (
            SELECT MAX(se.weighted_score) FROM signal_events se WHERE se.song_id = s.id
        ) DESC NULLS LAST
        LIMIT %s
    """, (cutoff, limit))
    rows = cur.fetchall()

    # If catalog is small enough just query everything
    if len(rows) < 20:
        cur.execute("""
            SELECT s.id, s.title, a.name AS artist
            FROM songs s
            JOIN artists a ON s.artist_id = a.id
            LIMIT %s
        """, (limit,))
        rows = cur.fetchall()

    return [{"id": str(r["id"]), "title": r["title"], "artist": r["artist"]}
            for r in rows]

# ── Google News RSS query ─────────────────────────────────────────────────────

def query_google_news(song: dict, since_days: int = MIN_ARTICLE_AGE_DAYS) -> list[dict]:
    """
    Search Google News for mentions of this song. Returns article dicts.
    """
    q = quote_plus(f'"{song["artist"]}" "{song["title"]}"')
    url = f"https://news.google.com/rss/search?q={q}&hl=en-US&gl=US&ceid=US:en"

    try:
        resp = requests.get(url, headers=RSS_HEADERS, timeout=15)
        resp.raise_for_status()
    except Exception as e:
        log.warning(f"Google News fetch failed for {song['title']}: {e}")
        return []

    feed = feedparser.parse(resp.content)
    cutoff = datetime.now(timezone.utc) - timedelta(days=since_days)

    articles = []
    for entry in feed.entries:
        published_parsed = getattr(entry, "published_parsed", None)
        if published_parsed:
            observed_at = datetime(*published_parsed[:6], tzinfo=timezone.utc)
        else:
            observed_at = datetime.now(timezone.utc)

        if observed_at < cutoff:
            continue

        link = getattr(entry, "link", "") or ""
        source_title = entry.get("source", {}).get("title", "") if hasattr(entry, "get") else ""
        if not source_title:
            source_title = urlparse(link).netloc.lstrip("www.")

        domain = urlparse(link).netloc.lstrip("www.")

        articles.append({
            "title":        getattr(entry, "title", ""),
            "link":         link,
            "observed_at":  observed_at,
            "external_id":  link,
            "source_name":  source_title,
            "domain":       domain,
            "pub_type":     classify_domain(domain),
        })

    return articles

# ── Signal writer ─────────────────────────────────────────────────────────────

REVIEW_RE = re.compile(
    r"\breview\b|\balbum\b|\bep\b|\bsingle\b|\btrack\b|\bsong\b|\brelease\b",
    re.IGNORECASE,
)

def write_grouped_signal(cur, song_id: str, community_id: str,
                         domain: str, source_name: str,
                         articles: list[dict], pub_type: str,
                         confidence: float, week_start: date):
    """
    One signal per (song, publication domain) per week.

    Scoring:
      weighted = intentionality × crossover_bonus × volume_scale × review_bonus

    volume_scale = min(2.0, 1 + log10(article_count))
      — 1 article → ×1.0,  3 → ×1.48,  10+ → ×2.0 (capped)

    This keeps a song with 100 Yahoo articles from outscoring
    a song with 1 NYT + 1 ESPN mention.
    """
    _, intentionality, crossover_bonus = PUB_TYPES.get(pub_type, PUB_TYPES["general_news"])

    article_count = len(articles)
    volume_scale  = min(2.0, 1 + math.log10(article_count))
    has_review    = any(REVIEW_RE.search(a["title"]) for a in articles)
    review_bonus  = 1.1 if has_review else 1.0

    engagement_multiplier = round(crossover_bonus * volume_scale * review_bonus, 3)
    weighted = round(intentionality * engagement_multiplier, 4)

    # Use the most recent article's date; best article = first review found (or first)
    observed_at = max(a["observed_at"] for a in articles)
    best = next((a for a in articles if REVIEW_RE.search(a["title"])), articles[0])

    # Dedup key: one row per (song, domain, week) — safe to rerun
    external_id = f"{song_id}::{domain}::gn::{week_start}"

    cur.execute("""
        INSERT INTO signal_events (
            observed_at, song_id, source_platform, signal_type,
            community_id, intentionality_score,
            raw_engagement, engagement_multiplier, weighted_score,
            resolution_confidence, is_home_community,
            external_id, context_snapshot
        )
        VALUES (%s, %s, 'press', 'mention', %s, %s, %s, %s, %s, %s, FALSE, %s, %s)
        ON CONFLICT DO NOTHING
    """, (
        observed_at,
        song_id,
        community_id,
        intentionality,
        psycopg2.extras.Json({"article_count": article_count, "top_url": best["link"]}),
        engagement_multiplier,
        weighted,
        confidence,
        external_id,
        psycopg2.extras.Json({
            "publication":   source_name,
            "domain":        domain,
            "pub_type":      pub_type,
            "article_count": article_count,
            "article_title": best["title"][:200],
            "article_url":   best["link"],
            "has_review":    has_review,
        }),
    ))

def write_feed_signal(cur, song_id: str, community_id: str, article: dict,
                      pub_type: str, confidence: float):
    """One signal per article for fixed feeds (low volume, high precision)."""
    _, intentionality, crossover_bonus = PUB_TYPES.get(pub_type, PUB_TYPES["general_news"])
    has_review = bool(REVIEW_RE.search(article["title"]))
    engagement_multiplier = round(crossover_bonus * (1.1 if has_review else 1.0), 3)
    weighted = round(intentionality * engagement_multiplier, 4)

    cur.execute("""
        INSERT INTO signal_events (
            observed_at, song_id, source_platform, signal_type,
            community_id, intentionality_score,
            raw_engagement, engagement_multiplier, weighted_score,
            resolution_confidence, is_home_community,
            external_id, context_snapshot
        )
        VALUES (%s, %s, 'press', 'mention', %s, %s, %s, %s, %s, %s, FALSE, %s, %s)
        ON CONFLICT DO NOTHING
    """, (
        article["observed_at"],
        song_id,
        community_id,
        intentionality,
        psycopg2.extras.Json({"article_url": article["link"]}),
        engagement_multiplier,
        weighted,
        confidence,
        article["link"],
        psycopg2.extras.Json({
            "publication":   article["source_name"],
            "domain":        article["domain"],
            "pub_type":      pub_type,
            "article_title": article["title"][:200],
            "article_url":   article["link"],
            "has_review":    has_review,
        }),
    ))

# ── Fixed feed processing ─────────────────────────────────────────────────────

HTML_TAG_RE = re.compile(r"<[^>]+>")
STOPWORDS = {
    "that", "this", "with", "have", "from", "they", "will", "been", "when",
    "what", "were", "their", "said", "each", "which", "about", "there",
    "then", "more", "also", "into", "just", "over", "only", "most", "after",
    "first", "very", "like", "make", "even", "back", "down", "than", "such",
    "both", "some", "time", "year", "your", "them", "well", "come",
}

def normalize(s: str) -> str:
    return re.sub(r"[^\w\s]", "", s.lower().strip())

def fetch_feed(feed_cfg: dict) -> list[dict]:
    try:
        resp = requests.get(feed_cfg["feed_url"], headers=RSS_HEADERS, timeout=15)
        resp.raise_for_status()
        feed = feedparser.parse(resp.content)
    except Exception as e:
        log.warning(f"[{feed_cfg['slug']}] fetch failed: {e}")
        return []

    if feed.bozo and not feed.entries:
        log.warning(f"[{feed_cfg['slug']}] feed parse error: {feed.bozo_exception}")
        return []

    cutoff = datetime.now(timezone.utc) - timedelta(days=MIN_ARTICLE_AGE_DAYS)
    articles = []
    for entry in feed.entries:
        published_parsed = getattr(entry, "published_parsed", None)
        observed_at = (
            datetime(*published_parsed[:6], tzinfo=timezone.utc)
            if published_parsed else datetime.now(timezone.utc)
        )
        if observed_at < cutoff:
            continue
        title   = getattr(entry, "title",   "") or ""
        summary = HTML_TAG_RE.sub(" ", getattr(entry, "summary", "") or "")
        link    = getattr(entry, "link",    "") or ""
        articles.append({
            "title":       title,
            "summary":     summary,
            "link":        link,
            "observed_at": observed_at,
            "external_id": link or title,
            "source_name": feed_cfg["name"],
            "domain":      urlparse(link).netloc.lstrip("www."),
            "pub_type":    feed_cfg["pub_type"],
        })

    log.info(f"[{feed_cfg['slug']}] {len(articles)} recent articles")
    return articles

def find_catalog_match(cur, text: str) -> list[tuple[str, float]]:
    """
    Full-text search catalog for songs mentioned in this article.
    Returns [(song_id, confidence), ...].
    """
    words = [w for w in re.findall(r"[a-z]{4,}", text.lower()) if w not in STOPWORDS]
    if not words:
        return []
    tsquery = " | ".join(words[:20])
    try:
        cur.execute("""
            SELECT s.id, s.title, a.name AS artist
            FROM songs s
            JOIN artists a ON s.artist_id = a.id
            WHERE s.search_vector @@ to_tsquery('english', %s)
            ORDER BY ts_rank(s.search_vector, to_tsquery('english', %s)) DESC
            LIMIT 5
        """, (tsquery, tsquery))
    except Exception:
        return []

    text_norm = normalize(text)
    matches = []
    for row in cur.fetchall():
        if normalize(row["title"]) in text_norm and normalize(row["artist"]) in text_norm:
            is_review = bool(REVIEW_RE.search(text))
            matches.append((str(row["id"]), 0.90 if is_review else 0.80))
    return matches

# ── Main collector ────────────────────────────────────────────────────────────

def run(snapshot_date: date = None):
    snapshot_date = snapshot_date or date.today()

    conn = psycopg2.connect(DB_URL)
    conn.autocommit = False
    psycopg2.extras.register_uuid()

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            INSERT INTO collector_runs (collector, metadata)
            VALUES ('press', %s) RETURNING id
        """, (psycopg2.extras.Json({"snapshot_date": str(snapshot_date)}),))
        run_id = cur.fetchone()["id"]
    conn.commit()

    total_events  = 0
    total_dropped = 0

    # community_id cache: slug → id
    community_cache: dict[str, str] = {}

    def get_community(slug: str, name: str, pub_type: str) -> str:
        if slug not in community_cache:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                community_cache[slug] = upsert_community(cur, slug, name, pub_type)
            conn.commit()
        return community_cache[slug]

    try:
        # ── Phase 1: Google News queries for top catalog songs ────────────────
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            songs = get_songs_to_query(cur, MAX_SONGS_PER_RUN)

        log.info(f"Querying Google News for {len(songs)} songs ...")

        for song in songs:
            articles = query_google_news(song)
            if not articles:
                time.sleep(GOOGLE_NEWS_DELAY)
                continue

            # Deduplicate to one signal per publication domain.
            # Multiple articles from the same outlet count once — breadth matters.
            # Keep the article most likely to be a review/feature as representative.
            by_domain: dict[str, dict] = {}
            for article in articles:
                domain = article["domain"]
                if domain not in by_domain:
                    by_domain[domain] = article
                elif REVIEW_RE.search(article["title"]) and not REVIEW_RE.search(by_domain[domain]["title"]):
                    by_domain[domain] = article  # prefer review articles as representative

            for domain, article in by_domain.items():
                try:
                    slug    = re.sub(r"[^\w-]", "-", domain)[:60]
                    comm_id = get_community(slug, article["source_name"], article["pub_type"])

                    with conn.cursor() as cur:
                        write_signal(cur, song["id"], comm_id, article,
                                     article["pub_type"], confidence=0.95)
                        total_events += 1
                    conn.commit()
                except Exception as e:
                    conn.rollback()
                    log.error(f"Error writing GN signal for {song['title']}: {e}")
                    total_dropped += 1

            time.sleep(GOOGLE_NEWS_DELAY)

        # ── Phase 2: Fixed supplemental feeds ─────────────────────────────────
        log.info("Processing fixed supplemental feeds ...")

        for feed_cfg in FIXED_FEEDS:
            articles = fetch_feed(feed_cfg)
            comm_id  = get_community(
                feed_cfg["slug"], feed_cfg["name"], feed_cfg["pub_type"]
            )

            for article in articles:
                try:
                    text = f"{article['title']} {article.get('summary','')}"
                    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                        matches = find_catalog_match(cur, text)

                    for song_id, confidence in matches:
                        try:
                            with conn.cursor() as cur:
                                write_signal(cur, song_id, comm_id, article,
                                             feed_cfg["pub_type"], confidence)
                                total_events += 1
                            conn.commit()
                        except Exception as e:
                            conn.rollback()
                            log.error(f"Error writing feed signal: {e}")
                            total_dropped += 1

                except Exception as e:
                    conn.rollback()
                    log.error(f"[{feed_cfg['slug']}] Error on article: {e}")
                    total_dropped += 1

            time.sleep(1)

        # ── Finalize ──────────────────────────────────────────────────────────
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE collector_runs
                SET status = 'success', completed_at = NOW(),
                    events_collected = %s, events_dropped = %s
                WHERE id = %s
            """, (total_events, total_dropped, run_id))
        conn.commit()
        log.info(f"Press collector complete — {total_events} events, {total_dropped} errors")

    except Exception as e:
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE collector_runs
                    SET status = 'failed', completed_at = NOW(), error_message = %s
                    WHERE id = %s
                """, (str(e), run_id))
            conn.commit()
        except Exception:
            pass
        log.error(f"Press collector failed: {e}")
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    run()
