"""
Reddit Collector — Public JSON API
------------------------------------
Uses Reddit's public .json endpoints instead of OAuth.
No credentials required. Rate limit: 1 request/second.

When Reddit API credentials are approved, swap this for
the OAuth version with minimal changes.

Polls new posts and comments from the curated subreddit list.
Extracts song mentions via entity resolution, assigns community
context, and writes scored signal_events.

Schedule: every 6 hours per subreddit batch
"""

import os
import re
import time
import math
import logging
import requests
import psycopg2
import psycopg2.extras
import threading
from datetime import datetime, timezone
from functools import lru_cache
from typing import Optional

from wordfreq import word_frequency

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("reddit_collector")

# ── Config ──────────────────────────────────────────────────────────────────

DB_URL = os.environ["DATABASE_URL"]

# Reddit OAuth app credentials (script-type app from reddit.com/prefs/apps)
REDDIT_CLIENT_ID     = os.environ.get("REDDIT_CLIENT_ID", "")
REDDIT_CLIENT_SECRET = os.environ.get("REDDIT_CLIENT_SECRET", "")
REDDIT_USERNAME      = os.environ.get("REDDIT_USERNAME", "")    # optional, for script apps
REDDIT_PASSWORD      = os.environ.get("REDDIT_PASSWORD", "")    # optional, for script apps

# Reddit requires a specific UA format: platform:app_id:version (by /u/username)
_ua_user = f" (by /u/{REDDIT_USERNAME})" if REDDIT_USERNAME else " (contact: chart@example.com)"
REDDIT_USER_AGENT = os.environ.get(
    "REDDIT_USER_AGENT",
    f"linux:everywhere-chart:v1.0{_ua_user}"
)

# Intentionality scores by signal type
INTENTIONALITY = {
    "post":    0.65,
    "comment": 0.45,
}

POLL_LIMIT = 100
MIN_RESOLUTION_CONFIDENCE = 0.65

# ── Reddit OAuth ──────────────────────────────────────────────────────────────

_oauth_token_cache: dict = {}

def _get_oauth_token() -> Optional[str]:
    """
    Get an application-only OAuth token using client credentials.
    Works without a user login — just needs REDDIT_CLIENT_ID + SECRET.
    Token lasts ~1 hour; cached and refreshed automatically.
    """
    if not REDDIT_CLIENT_ID or not REDDIT_CLIENT_SECRET:
        return None
    now = time.time()
    if _oauth_token_cache.get("expires_at", 0) > now + 60:
        return _oauth_token_cache["token"]
    try:
        resp = requests.post(
            "https://www.reddit.com/api/v1/access_token",
            auth=(REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET),
            data={"grant_type": "client_credentials"},
            headers={"User-Agent": REDDIT_USER_AGENT},
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
        token = data.get("access_token")
        if token:
            _oauth_token_cache["token"]      = token
            _oauth_token_cache["expires_at"] = now + data.get("expires_in", 3600)
            log.info("Reddit OAuth token acquired (application-only)")
            return token
    except Exception as e:
        log.warning(f"Reddit OAuth token request failed: {e}")
    return None


# ── Reddit API (OAuth preferred, public JSON fallback) ────────────────────────

def reddit_get(path: str, params: dict = None) -> dict:
    """
    GET from Reddit API.
    Uses OAuth (oauth.reddit.com) when credentials are available —
    bypasses the IP-level 403 blocks on the public API.
    Falls back to www.reddit.com public JSON endpoint.
    """
    token = _get_oauth_token()
    if token:
        url     = f"https://oauth.reddit.com{path}"
        headers = {
            "User-Agent":    REDDIT_USER_AGENT,
            "Authorization": f"bearer {token}",
        }
    else:
        url     = f"https://www.reddit.com{path}.json"
        headers = {"User-Agent": REDDIT_USER_AGENT}
    for attempt in range(3):
        result: dict = {}
        error: list = []

        def _fetch():
            try:
                resp = requests.get(
                    url,
                    headers=headers,
                    params={"raw_json": 1, **(params or {})},
                    timeout=(8, 12),  # (connect, read-per-chunk)
                )
                result["resp"] = resp
            except Exception as e:
                error.append(e)

        t = threading.Thread(target=_fetch, daemon=True)
        t.start()
        t.join(timeout=25)  # hard wall-clock limit per request

        if t.is_alive():
            log.warning(f"Reddit hard timeout for {path} — skipping")
            return {}

        if error:
            log.warning(f"Reddit request failed (attempt {attempt + 1}): {error[0]}")
            time.sleep(2)
            continue

        resp = result.get("resp")
        if resp is None:
            continue

        if resp.status_code == 429:
            wait = int(resp.headers.get("Retry-After", 60))
            log.warning(f"Reddit rate limited — waiting {wait}s")
            time.sleep(wait)
            continue
        if resp.status_code in (403, 404):
            log.warning(f"Reddit {resp.status_code} for {path} — skipping")
            return {}
        try:
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            log.warning(f"Reddit request failed (attempt {attempt + 1}): {e}")
            time.sleep(2)
    return {}

# ── Subreddit polling ────────────────────────────────────────────────────────

def fetch_new_posts(subreddit: str, after: Optional[str] = None) -> tuple[list, str]:
    """
    Fetch new posts from a subreddit.
    Returns (posts, last_fullname) for pagination cursor.
    """
    params = {"limit": POLL_LIMIT}
    if after:
        params["after"] = after

    data = reddit_get(f"/r/{subreddit}/new", params)
    if not data or "data" not in data:

        return [], ""

    posts = []
    for child in data["data"].get("children", []):
        p = child.get("data", {})
        posts.append({
            "id":           p.get("id"),
            "fullname":     p.get("name"),           # t3_xxxxx
            "title":        p.get("title", ""),
            "body":         p.get("selftext", ""),
            "score":        p.get("score", 0),
            "num_comments": p.get("num_comments", 0),
            "created_utc":  p.get("created_utc"),
            "url":          p.get("url"),
            "permalink":    p.get("permalink"),
        })

    last = data["data"].get("after", "")
    return posts, last

def fetch_new_comments(subreddit: str, after: Optional[str] = None) -> tuple[list, str]:
    """
    Fetch recent comments from a subreddit's comment stream.
    Includes parent post title via link_title field.
    """
    params = {"limit": POLL_LIMIT}
    if after:
        params["after"] = after

    data = reddit_get(f"/r/{subreddit}/comments", params)
    if not data or "data" not in data:

        return [], ""

    comments = []
    for child in data["data"].get("children", []):
        c = child.get("data", {})
        comments.append({
            "id":             c.get("id"),
            "fullname":       c.get("name"),         # t1_xxxxx
            "body":           c.get("body", ""),
            "score":          c.get("score", 0),
            "created_utc":    c.get("created_utc"),
            "permalink":      c.get("permalink"),
            "link_title":     c.get("link_title", ""),   # parent post title
            "link_id":        c.get("link_id", ""),      # parent post fullname
        })

    last = data["data"].get("after", "")
    return comments, last

# ── Entity extraction ────────────────────────────────────────────────────────

# Communities where all posts are inherently music-related —
# skip the music pre-filter and use catalog-first matching.
MUSIC_COMMUNITY_TYPES = {"artist", "genre", "general_music"}

# Patterns that suggest music discussion — used to pre-filter posts in
# non-music communities (lifestyle, sports, etc.) before expensive resolution.
MUSIC_SIGNALS = [
    r"\bsong\b", r"\btrack\b", r"\balbum\b", r"\blyrics?\b",
    r"\blistening to\b", r"\bmusic\b", r"\bbop\b", r"\bslaps\b",
    r"\bsoundtrack\b", r"\bshazam\b", r"\bspotify\b", r"\bapple music\b",
    r"\bremix\b", r"\bcover version\b", r"\bnow playing\b",
    r"\bmusic video\b", r"\bfeat\b", r"\bfeaturing\b",
]
MUSIC_PATTERN = re.compile("|".join(MUSIC_SIGNALS), re.IGNORECASE)

def has_music_signal(text: str, context: str = "") -> bool:
    """Quick check — does the text plausibly mention music?"""
    combined = f"{text} {context}"
    return bool(MUSIC_PATTERN.search(combined))

# Frequency threshold for ambiguous title detection.
# Single-word titles with English frequency above this require artist corroboration.
# 1e-5 cleanly separates common words (perfect: 1.6e-4, baby: 1.8e-4)
# from distinctive song titles (fortnight: 2.6e-6, espresso: 2.5e-6).
_AMBIGUITY_FREQ_THRESHOLD = 5e-5

@lru_cache(maxsize=2048)
def _title_is_ambiguous(title_norm: str) -> bool:
    """
    Returns True if the title is likely to appear incidentally in posts
    that have nothing to do with the song.

    Only applies to single-word titles — multi-word titles appearing verbatim
    as a word-boundary match are already strong enough signal on their own.
    """
    words = title_norm.split()
    if len(words) > 1:
        return False
    return word_frequency(title_norm, "en") > _AMBIGUITY_FREQ_THRESHOLD


def scan_catalog_in_text(cur, text: str) -> list[tuple[str, float]]:
    """
    Catalog-first matching for music communities.

    Instead of extracting candidates from text and resolving them, we flip it:
    search the catalog for songs whose titles appear in the post text.
    This catches bare title mentions ("Fortnight is incredible") that the
    candidate extraction approach completely misses.

    Returns [(song_id, confidence), ...] — deduplicated by song_id.
    """
    text_norm = normalize(text)
    if not text_norm:
        return []

    # Use FTS to quickly surface candidate songs from the catalog,
    # then verify the title is actually a substring of the text.
    words = [w for w in re.findall(r"[a-z]{3,}", text_norm)
             if w not in _SCAN_STOPWORDS]
    if not words:
        return []

    tsquery = " | ".join(f"'{w}'" for w in words[:20])
    try:
        cur.execute("""
            SELECT s.id::text, s.title_normalized, a.name_normalized AS artist_norm
            FROM songs s
            JOIN artists a ON s.artist_id = a.id
            WHERE s.search_vector @@ to_tsquery('english', %s)
            LIMIT 30
        """, (tsquery,))
    except Exception:
        return []

    seen: dict[str, float] = {}
    for row in cur.fetchall():
        title_norm  = row["title_normalized"]
        artist_norm = row["artist_norm"]
        song_id     = row["id"]

        if len(title_norm) < 3:
            continue

        title_match = bool(re.search(r"\b" + re.escape(title_norm) + r"\b", text_norm))
        if not title_match:
            continue

        artist_present = artist_norm in text_norm

        if _title_is_ambiguous(title_norm):
            # Common English word/phrase: require artist name in post to confirm.
            if not artist_present:
                continue
            confidence = 0.80
        else:
            # Distinctive title: word-boundary match is sufficient.
            confidence = 0.92 if artist_present else 0.85

        # Keep highest confidence if song matched via multiple paths
        if song_id not in seen or seen[song_id] < confidence:
            seen[song_id] = confidence

    return list(seen.items())


_SCAN_STOPWORDS = {
    "that", "this", "with", "have", "from", "they", "will", "been", "when",
    "what", "were", "their", "said", "each", "which", "about", "there",
    "then", "more", "also", "into", "just", "over", "only", "most", "after",
    "first", "very", "like", "make", "even", "back", "down", "than", "such",
    "both", "some", "time", "year", "your", "them", "well", "come", "going",
    "really", "think", "know", "still", "never", "always", "every", "right",
    "could", "would", "should", "being", "doing", "want", "need", "feel",
}

def extract_candidate_mentions(text: str, context: str = "") -> list[str]:
    """
    Extract potential song/artist references from text.
    Returns list of candidate strings for resolution.

    Strategies:
    1. Quoted strings: "Song Name" or 'Song Name'
    2. Capitalized proper noun sequences (Title Case phrases)
    3. Artist name patterns: "by Artist", "from Artist"
    4. Explicit patterns: "listening to X", "playing X"
    """
    candidates = set()

    # 1. Quoted strings — most reliable
    quoted = re.findall(r'["\u201c\u201d]([^"\u201c\u201d]{3,60})["\u201c\u201d]', text)
    candidates.update(quoted)

    # 2. "listening to / playing / love" followed by title
    patterns = [
        r"(?:listening to|playing|love|obsessed with|can't stop playing)\s+([A-Z][^\.\!\?,]{3,50})",
        r"([A-Z][a-zA-Z\s']{3,40})\s+(?:by|from)\s+([A-Z][a-zA-Z\s']{2,30})",
        r"(?:song|track)\s+(?:called|titled|named)\s+['\"]?([A-Z][^\.\!\?,]{3,50})['\"]?",
    ]
    for p in patterns:
        matches = re.findall(p, text)
        for m in matches:
            if isinstance(m, tuple):
                candidates.update([part.strip() for part in m if part.strip()])
            else:
                candidates.add(m.strip())

    return [c for c in candidates if 3 < len(c) < 100]

# ── Entity resolution ─────────────────────────────────────────────────────────

def normalize(s: str) -> str:
    return re.sub(r"[^\w\s]", "", s.lower().strip())

def resolve_candidate(cur, candidate: str, context: dict) -> tuple[Optional[str], float, str]:
    """
    Try to resolve a candidate string to a song_id.
    Returns (song_id, confidence, method).

    Steps:
    1. Exact title match
    2. Exact title + artist from context
    3. Fuzzy match via pg_trgm
    4. Full-text search on search_vector (catches lyric fragments)
    """
    cand_norm = normalize(candidate)
    if len(cand_norm) < 3:
        return None, 0.0, "too_short"

    # 1. exact title match
    cur.execute("""
        SELECT s.id, a.name as artist_name
        FROM songs s JOIN artists a ON s.artist_id = a.id
        WHERE s.title_normalized = %s
        ORDER BY s.first_signal_at DESC NULLS LAST
        LIMIT 3
    """, (cand_norm,))
    results = cur.fetchall()
    if len(results) == 1:
        return str(results[0]["id"]), 0.95, "exact_title"
    if len(results) > 1:
        # ambiguous exact match — check if artist name is in context
        ctx_text = f"{context.get('post_title','')} {context.get('body','')}".lower()
        for r in results:
            if r["artist_name"].lower() in ctx_text:
                return str(r["id"]), 0.92, "exact_title_artist_context"
        # still ambiguous — lower confidence, take most recent
        return str(results[0]["id"]), 0.70, "exact_title_ambiguous"

    # 2. fuzzy match
    cur.execute("""
        SELECT s.id,
               similarity(s.title_normalized, %s) AS sim
        FROM songs s
        WHERE similarity(s.title_normalized, %s) > 0.65
        ORDER BY sim DESC
        LIMIT 5
    """, (cand_norm, cand_norm))
    fuzzy_results = cur.fetchall()
    if fuzzy_results:
        best = fuzzy_results[0]
        if best["sim"] >= 0.85:
            return str(best["id"]), round(best["sim"] * 0.9, 3), "fuzzy_title"
        if best["sim"] >= 0.70 and len(fuzzy_results) == 1:
            return str(best["id"]), round(best["sim"] * 0.85, 3), "fuzzy_title_unique"

    # 3. full-text search (lyric fragments, descriptions)
    tokens = [t for t in re.split(r"\s+", cand_norm) if t]
    if not tokens:
        return None, 0.0, "unresolved"
    tsquery = " & ".join(tokens)
    cur.execute("""
        SELECT id, ts_rank(search_vector, query) AS rank
        FROM songs, to_tsquery('english', %s) query
        WHERE search_vector @@ query
        ORDER BY rank DESC
        LIMIT 3
    """, (tsquery,))
    ft_results = cur.fetchall()
    if ft_results and ft_results[0]["rank"] > 0.05:
        return str(ft_results[0]["id"]), 0.68, "fulltext"

    return None, 0.0, "unresolved"

# ── Scoring ──────────────────────────────────────────────────────────────────

def compute_engagement_multiplier(score: int, num_comments: int = 0) -> float:
    """1 + log10(likes + replies*2 + 1) — log-scaled to control for virality."""
    raw = max(0, score) + max(0, num_comments) * 2
    return round(1 + math.log10(raw + 1), 3)

def compute_effective_distance(raw_distance: float, home_confidence: float) -> float:
    """Blend distance toward 0.5 when home context is uncertain."""
    return round(raw_distance * home_confidence + 0.5 * (1 - home_confidence), 4)

def get_community_distance(cur, community_id: str, song_id: str) -> tuple[float, float]:
    """
    Return (raw_distance, home_confidence) for a community/song pair.
    Falls back to taxonomy rules if no computed distance exists.
    """
    # get song's home community type and confidence
    cur.execute("""
        SELECT s.home_confidence, s.home_community_ids,
               c_home.community_type as home_type
        FROM songs s
        LEFT JOIN communities c_home
            ON c_home.id = ANY(s.home_community_ids)
        WHERE s.id = %s
        LIMIT 1
    """, (song_id,))
    song_row = cur.fetchone()
    if not song_row:
        return 0.5, 0.3

    home_confidence = song_row["home_confidence"] or 0.3
    home_type       = song_row["home_type"] or "genre"

    # get this community's type
    cur.execute(
        "SELECT community_type, casual_weight FROM communities WHERE id = %s",
        (community_id,)
    )
    community_row = cur.fetchone()
    if not community_row:
        return 0.5, home_confidence

    community_type = community_row["community_type"]

    # check for computed distance in graph
    cur.execute("""
        SELECT distance FROM community_distances
        WHERE (community_a_id = %s OR community_b_id = %s)
        ORDER BY computed_at DESC LIMIT 1
    """, (community_id, community_id))
    dist_row = cur.fetchone()
    if dist_row:
        return dist_row["distance"], home_confidence

    # taxonomy rule fallback
    DISTANCE_RULES = {
        ("artist",        "artist"):        0.0,
        ("artist",        "genre"):         0.2,
        ("genre",         "genre"):         0.1,
        ("genre",         "general_music"): 0.25,
        ("general_music", "general_music"): 0.15,
        ("general_music", "entertainment"): 0.5,
        ("general_music", "sports"):        0.75,
        ("general_music", "lifestyle"):     0.8,
        ("general_music", "non_music"):     0.9,
        ("entertainment", "entertainment"): 0.4,
        ("entertainment", "sports"):        0.65,
        ("entertainment", "lifestyle"):     0.7,
        ("entertainment", "non_music"):     0.85,
        ("sports",        "sports"):        0.5,
        ("sports",        "lifestyle"):     0.6,
        ("sports",        "non_music"):     0.8,
        ("lifestyle",     "lifestyle"):     0.55,
        ("lifestyle",     "non_music"):     0.75,
        ("non_music",     "non_music"):     0.7,
    }

    key = tuple(sorted([home_type, community_type]))
    raw_distance = DISTANCE_RULES.get(key, 0.5)
    return raw_distance, home_confidence

# ── Database write ────────────────────────────────────────────────────────────

def write_signal_event(cur, song_id: str, community_id: str,
                        signal_type: str, item: dict,
                        resolution_confidence: float,
                        context: dict) -> bool:
    """
    Write a scored signal_event. Returns True if written, False if skipped.
    """
    # get community weights + type
    cur.execute(
        "SELECT casual_weight, community_type, external_id FROM communities WHERE id = %s",
        (community_id,)
    )
    comm_row = cur.fetchone()
    if not comm_row:
        return False

    casual_weight    = comm_row["casual_weight"]
    community_type   = comm_row["community_type"]
    subreddit_name   = comm_row["external_id"]
    intentionality   = INTENTIONALITY[signal_type]

    # engagement
    score    = item.get("score", 0) or 0
    comments = item.get("num_comments", 0) or 0
    eng_mult = compute_engagement_multiplier(score, comments)

    # distance
    raw_distance, home_confidence = get_community_distance(cur, community_id, song_id)
    eff_distance = compute_effective_distance(raw_distance, home_confidence)

    # Determine is_home_community.
    # home_community_ids is rarely populated, so we infer from community type:
    #   artist  → home if song's artist name fuzzy-matches the subreddit name
    #   genre   → home if song has a matching genre tag
    #   other   → always out-of-home (crossover territory)
    is_home = False
    if community_type == "artist":
        sub_norm = normalize(subreddit_name)
        cur.execute("""
            SELECT TRUE FROM songs s
            JOIN artists a ON s.artist_id = a.id
            WHERE s.id = %s
              AND similarity(a.name_normalized, %s) > 0.55
        """, (song_id, sub_norm))
        is_home = bool(cur.fetchone())
    elif community_type == "genre":
        cur.execute("""
            SELECT TRUE FROM songs
            WHERE id = %s AND %s = ANY(genre_tags)
        """, (song_id, subreddit_name))
        is_home = bool(cur.fetchone())

    # final weighted score
    weighted = round(
        intentionality * eng_mult * casual_weight * eff_distance * resolution_confidence,
        5
    )

    observed_at = datetime.fromtimestamp(
        item["created_utc"], tz=timezone.utc
    ) if item.get("created_utc") else datetime.now(timezone.utc)

    external_id = item.get("fullname") or item.get("id")

    cur.execute("""
        INSERT INTO signal_events (
            observed_at, song_id, community_id,
            source_platform, signal_type,
            intentionality_score, raw_engagement, engagement_multiplier,
            community_casual_weight, home_distance, home_confidence,
            effective_distance, weighted_score, resolution_confidence,
            is_home_community, external_id, external_url, context_snapshot
        )
        VALUES (
            %s, %s, %s, 'reddit', %s,
            %s, %s, %s,
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s
        )
        ON CONFLICT DO NOTHING
        RETURNING id
    """, (
        observed_at, song_id, community_id, signal_type,
        intentionality,
        psycopg2.extras.Json({
            "score":        score,
            "replies":      comments,
            "upvote_ratio": item.get("upvote_ratio"),
        }),
        eng_mult,
        casual_weight,
        raw_distance, home_confidence, eff_distance,
        weighted, resolution_confidence,
        is_home,
        external_id,
        f"https://reddit.com{item.get('permalink','')}" if item.get("permalink") else None,
        psycopg2.extras.Json(context),
    ))

    return bool(cur.fetchone())

# ── Per-subreddit processing ──────────────────────────────────────────────────

def _resolve_items(conn, items: list[dict], community_id: str,
                   signal_type: str, community_type: str,
                   text_key: str, context_extra: dict) -> tuple[int, int]:
    """
    Shared resolution loop for posts and comments.
    Branches on community type: catalog-scan for music communities,
    candidate-extraction for crossover communities.
    """
    is_music_community = community_type in MUSIC_COMMUNITY_TYPES
    written = queued = 0

    for item in items:
        text    = item.get(text_key, "") or ""
        context = {
            **context_extra,
            "body":        text[:500],
            "signal_type": signal_type,
        }

        # Non-music communities: require an explicit music signal in the text
        if not is_music_community and not has_music_signal(text, context.get("post_title", "")):
            continue

        if is_music_community:
            # Catalog-first: find any known song titles present in the text.
            # Catches "Fortnight is incredible", "that Anti-Hero bridge", etc.
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                matches = scan_catalog_in_text(cur, text)
            for song_id, confidence in matches:
                if confidence >= MIN_RESOLUTION_CONFIDENCE:
                    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                        if write_signal_event(cur, song_id, community_id,
                                              signal_type, item, confidence, context):
                            written += 1
                    conn.commit()
        else:
            # Crossover communities: extract candidates then resolve.
            title   = item.get("title", "") or item.get("link_title", "") or ""
            candidates = extract_candidate_mentions(text, title)
            for candidate in candidates:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    song_id, confidence, method = resolve_candidate(cur, candidate, context)

                    # Non-music communities require stronger evidence
                    min_conf = MIN_RESOLUTION_CONFIDENCE if is_music_community else 0.85
                    if confidence >= min_conf and song_id:
                        if write_signal_event(cur, song_id, community_id,
                                              signal_type, item, confidence, context):
                            written += 1

                    elif 0.40 <= confidence < MIN_RESOLUTION_CONFIDENCE:
                        cur.execute("""
                            INSERT INTO resolution_queue (
                                raw_text, context_json, source_platform,
                                community_id, observed_at, external_id,
                                resolution_confidence, resolution_method, status
                            )
                            VALUES (%s, %s, 'reddit', %s, %s, %s, %s, %s, 'pending')
                            ON CONFLICT DO NOTHING
                        """, (
                            candidate,
                            psycopg2.extras.Json(context),
                            community_id,
                            datetime.fromtimestamp(item["created_utc"], tz=timezone.utc)
                                if item.get("created_utc") else datetime.now(timezone.utc),
                            item.get("fullname"),
                            confidence,
                            method,
                        ))
                        queued += 1

                conn.commit()

        time.sleep(0.05)

    return written, queued


def process_subreddit(conn, community_id: str, subreddit: str,
                      community_type: str = "general_music"):
    """Poll posts and comments from one subreddit, extract and score mentions."""
    posts, _    = fetch_new_posts(subreddit)
    comments, _ = fetch_new_comments(subreddit)

    post_context    = {"post_title": "", "subreddit": subreddit}
    comment_context = {"subreddit": subreddit}

    # Enrich posts with their own title for context
    for p in posts:
        p["_text"] = f"{p['title']} {p['body']}"

    # Comments use body as main text; link_title as context
    for c in comments:
        c["_text"]      = c["body"]
        c["post_title"] = c["link_title"]

    p_written, p_queued = _resolve_items(
        conn, posts, community_id, "post", community_type,
        "_text", {"subreddit": subreddit},
    )
    c_written, c_queued = _resolve_items(
        conn, comments, community_id, "comment", community_type,
        "_text", {"subreddit": subreddit},
    )

    return p_written + c_written, p_queued + c_queued

# ── Main collector ────────────────────────────────────────────────────────────

def _connect():
    return psycopg2.connect(
        DB_URL,
        connect_timeout=15,
        keepalives=1,
        keepalives_idle=10,
        keepalives_interval=5,
        keepalives_count=3,
        options="-c statement_timeout=20000",  # cancel any query > 20s
    )

def run():
    conn = _connect()
    conn.autocommit = False
    psycopg2.extras.register_uuid()

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            INSERT INTO collector_runs (collector) VALUES ('reddit') RETURNING id
        """)
        run_id = cur.fetchone()["id"]
    conn.commit()

    # load all active reddit communities
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT id, external_id, community_type, casual_weight
            FROM communities
            WHERE platform = 'reddit'
            ORDER BY casual_weight DESC, subscriber_count DESC NULLS LAST
        """)
        communities = cur.fetchall()

    if not REDDIT_CLIENT_ID:
        log.warning(
            "REDDIT_CLIENT_ID not set — Reddit API requires OAuth credentials. "
            "Collector will run but all requests will 403 (Railway IPs are blocked "
            "from the public API). Set REDDIT_CLIENT_ID + REDDIT_CLIENT_SECRET to enable."
        )

    log.info(f"Polling {len(communities)} subreddits")

    total_events  = 0
    total_queued  = 0
    total_dropped = 0
    subreddits_polled = 0

    for community in communities:
        subreddit    = community["external_id"]
        community_id = str(community["id"])

        try:
            written, queued = process_subreddit(
                conn, community_id, subreddit,
                community_type=community["community_type"],
            )
            total_events  += written
            total_queued  += queued
            subreddits_polled += 1
            log.info(f"r/{subreddit}: {written} events written, {queued} queued")

        except Exception as e:
            try:
                conn.rollback()
            except Exception:
                # Connection may have dropped; reconnect for subsequent subreddits
                try:
                    conn.close()
                except Exception:
                    pass
                conn = _connect()
                conn.autocommit = False
                psycopg2.extras.register_uuid()
            log.error(f"Failed processing r/{subreddit}: {e}")
            total_dropped += 1

        # Reddit free tier: ~100 req/min
        # With posts + comments per subreddit = ~2 requests
        # 300 subreddits = ~600 requests — spread over ~6 minutes minimum
        time.sleep(1.2)

    try:
        if conn.closed:
            conn = _connect()
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE collector_runs
                SET status = 'success', completed_at = NOW(),
                    events_collected = %s, events_queued = %s, events_dropped = %s,
                    metadata = %s
                WHERE id = %s
            """, (
                total_events, total_queued, total_dropped,
                psycopg2.extras.Json({"subreddits_polled": subreddits_polled}),
                run_id,
            ))
        conn.commit()
    except Exception as e:
        log.warning(f"Could not update collector_runs: {e}")
    finally:
        try:
            conn.close()
        except Exception:
            pass
    log.info(
        f"Reddit collector complete — {total_events} events, "
        f"{total_queued} queued, {total_dropped} errors, "
        f"{subreddits_polled} subreddits polled"
    )

if __name__ == "__main__":
    run()
