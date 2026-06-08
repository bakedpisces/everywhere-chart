"""
Spotify Charts Collector
------------------------
Uses Playwright to load charts.spotify.com and intercept the internal
charts API response (same data the website displays). The direct API
endpoint requires session cookies set by the browser, so headless
Playwright is used to handle that automatically.

Spotify Web API credentials (SPOTIFY_CLIENT_ID / SPOTIFY_CLIENT_SECRET)
are used only for track metadata enrichment on newly discovered songs:
genre tags, ISRC, release date, artist info.

Discovers artist subreddits automatically for each new artist
and adds them to the communities table.

Schedule: daily at 08:00 UTC
"""

import math
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
log = logging.getLogger("spotify_collector")

DB_URL         = os.environ["DATABASE_URL"]
SPOTIFY_CLIENT = os.environ.get("SPOTIFY_CLIENT_ID")
SPOTIFY_SECRET = os.environ.get("SPOTIFY_CLIENT_SECRET")
SPOTIFY_SP_DC  = os.environ.get("SPOTIFY_SP_DC")  # logged-in session cookie → unlocks 200-song auth endpoint

CHARTS_API_HOST     = "charts-spotify-com-service.spotify.com"
CHARTS_AUTH_PATH    = "/auth/v0/charts"    # 200-song chart (requires sp_dc cookie)
CHARTS_PUBLIC_PATH  = "/public/v0/charts"  # 50-song fallback (no auth needed)

CHARTS_TO_FETCH = [
    {
        "page_url":   "https://charts.spotify.com/charts/view/regional-global-weekly/latest",
        "chart_path": "regional-global-weekly/latest",
        "name":       "global_weekly",
        "region":     "global",
    },
    {
        "page_url":   "https://charts.spotify.com/charts/view/regional-us-weekly/latest",
        "chart_path": "regional-us-weekly/latest",
        "name":       "us_weekly",
        "region":     "us",
    },
]

# Chart position is a passive signal — low intentionality score
INTENTIONALITY_CHART_POSITION = 0.15

# Max artist pages to load per run for play-count scraping (each ~5-10s)
MAX_ARTIST_STREAM_FETCHES = 20

# ── Chart fetching ────────────────────────────────────────────────────────────

def _parse_entries(entries: list, chart: dict) -> list[dict]:
    """Convert raw chart entry list to normalized row dicts."""
    rows = []
    for entry in entries:
        entry_data = entry.get("chartEntryData", {})
        track      = entry.get("trackMetadata", {})
        artists    = track.get("artists", [])
        title      = track.get("trackName", "")
        artist     = artists[0].get("name", "") if artists else ""
        track_uri  = track.get("trackUri", "")
        spotify_id = track_uri.replace("spotify:track:", "") if track_uri else ""

        if not title or not artist:
            continue

        # Stream count is in rankingMetric (auth/v0 only; None on public/v0)
        ranking_metric = entry_data.get("rankingMetric", {})
        streams = (
            int(ranking_metric.get("value", 0))
            if ranking_metric.get("type") == "STREAMS"
            else None
        )

        rows.append({
            "rank":           entry_data.get("currentRank", 0),
            "prev_rank":      entry_data.get("previousRank"),
            "peak_rank":      entry_data.get("peakRank"),
            "weeks_on_chart": entry_data.get("appearancesOnChart", 0),
            "entry_status":   entry_data.get("entryStatus", ""),
            "streams":        streams,
            "title":          title,
            "artist":         artist,
            "all_artists":    [a.get("name") for a in artists],
            "spotify_id":     spotify_id,
            "region":         chart["region"],
            "chart_name":     chart["name"],
        })
    return rows


def _parse_chart_response(data: dict, chart: dict) -> list[dict]:
    """
    Handle both response shapes:
      auth/v0  → top-level 'entries' list (200 songs)
      public/v0 → 'chartEntryViewResponses' list; pick the TRACK block (50 songs)
    """
    # auth/v0 shape
    if "entries" in data:
        return _parse_entries(data["entries"], chart)

    # public/v0 shape — find the TRACK block
    for block in data.get("chartEntryViewResponses", []):
        meta = block.get("displayChart", {}).get("chartMetadata", {})
        if meta.get("entityType") == "TRACK":
            return _parse_entries(block.get("entries", []), chart)

    return []


def fetch_all_charts(artist_spotify_ids: list[str] = None) -> tuple:
    """
    Launch one Playwright browser, optionally inject the sp_dc session
    cookie, load each chart page, and intercept the API response.

    With sp_dc: captures auth/v0 response (200 songs per chart).
    Without:    captures public/v0 response (50 songs per chart).

    If artist_spotify_ids is provided, also loads each artist's page on
    open.spotify.com to intercept play counts from the internal GraphQL
    endpoint — used to get stream data for non-chart (under-radar) songs.

    Returns (chart_results, user_token, artist_stream_data) where:
      chart_results      = {chart_name: [rows]}
      user_token         = str | None
      artist_stream_data = {artist_spotify_id: [{track_id, track_name, playcount}]}
    """
    from playwright.sync_api import sync_playwright

    results = {}

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)

        ctx_kwargs = {}
        if SPOTIFY_SP_DC:
            ctx_kwargs["storage_state"] = {
                "cookies": [{
                    "name":     "sp_dc",
                    "value":    SPOTIFY_SP_DC,
                    "domain":   ".spotify.com",
                    "path":     "/",
                    "secure":   True,
                    "httpOnly": True,
                    "sameSite": "Lax",
                }]
            }
            log.info("sp_dc cookie injected — using auth/v0 endpoint (200 songs)")
        else:
            log.info("No sp_dc — using public/v0 endpoint (50 songs)")

        context = browser.new_context(**ctx_kwargs)

        # Shared across chart pages — grab the Bearer token from the first
        # authenticated chart API request (guaranteed user-level when sp_dc active).
        intercepted_token: dict = {}

        for chart in CHARTS_TO_FETCH:
            page     = context.new_page()
            captured = {}

            def on_request(request, intercepted_token=intercepted_token):
                if CHARTS_API_HOST not in request.url:
                    return
                if intercepted_token.get("token"):
                    return  # already captured
                auth = request.headers.get("authorization", "")
                if auth.startswith("Bearer "):
                    intercepted_token["token"] = auth[len("Bearer "):]
                    log.info("Bearer token captured from chart API request")

            def on_response(response, captured=captured):
                if CHARTS_API_HOST not in response.url or response.status != 200:
                    return
                if CHARTS_AUTH_PATH in response.url or CHARTS_PUBLIC_PATH in response.url:
                    if CHARTS_AUTH_PATH in response.url or "data" not in captured:
                        try:
                            captured["data"] = response.json()
                            captured["url"]  = response.url
                        except Exception:
                            pass

            page.on("request", on_request)
            page.on("response", on_response)

            try:
                page.goto(chart["page_url"], wait_until="networkidle", timeout=45_000)
            except Exception as e:
                log.warning(f"Page load failed for {chart['name']}: {e}")
                page.close()
                results[chart["name"]] = []
                continue

            if not captured:
                log.warning(f"No API response captured for {chart['name']}")
                page.close()
                results[chart["name"]] = []
                continue

            rows = _parse_chart_response(captured["data"], chart)
            src  = "auth/v0" if CHARTS_AUTH_PATH in captured.get("url", "") else "public/v0"
            log.info(f"Fetched {len(rows)} entries from {chart['name']} via {src}")
            results[chart["name"]] = rows
            page.close()

        # ── Extract user access token ─────────────────────────────────────
        # Use the Bearer token intercepted from chart API requests — those
        # are definitely user-level when sp_dc is active, no extra page load needed.
        user_token: Optional[str] = intercepted_token.get("token")
        if user_token:
            log.info("User token ready for playlist seeder (from chart request headers)")
        elif SPOTIFY_SP_DC:
            log.warning(
                "sp_dc was set but no Bearer token intercepted from chart requests — "
                "playlist seeder will fall back to sp_dc HTTP call"
            )

        # ── Artist page play counts (under-radar / non-chart songs) ──────────
        artist_stream_data: dict[str, list[dict]] = {}
        if SPOTIFY_SP_DC and artist_spotify_ids:
            to_fetch = artist_spotify_ids[:MAX_ARTIST_STREAM_FETCHES]
            log.info(f"Fetching play counts for {len(to_fetch)} artists via artist pages")
            for artist_id in to_fetch:
                page = context.new_page()
                try:
                    with page.expect_response(
                        lambda r: (
                            "api-partner.spotify.com/pathfinder" in r.url
                            and "queryArtistOverview" in r.url
                        ),
                        timeout=15_000,
                    ) as resp_info:
                        page.goto(
                            f"https://open.spotify.com/artist/{artist_id}",
                            wait_until="domcontentloaded",
                            timeout=30_000,
                        )
                    data = resp_info.value.json()
                    top_tracks = (
                        data.get("data", {})
                            .get("artistUnion", {})
                            .get("discography", {})
                            .get("topTracks", {})
                            .get("items", [])
                    )
                    tracks = []
                    for item in top_tracks:
                        track = item.get("track", {})
                        uri   = track.get("uri", "")
                        tid   = uri.replace("spotify:track:", "") if uri else track.get("id", "")
                        try:
                            playcount = int(track.get("playcount") or 0)
                        except (ValueError, TypeError):
                            playcount = 0
                        if tid and playcount > 0:
                            tracks.append({
                                "track_id":   tid,
                                "track_name": track.get("name", ""),
                                "playcount":  playcount,
                            })
                    if tracks:
                        artist_stream_data[artist_id] = tracks
                        log.info(f"  Artist {artist_id}: {len(tracks)} tracks with play counts")
                    else:
                        log.info(f"  Artist {artist_id}: no play count data in response")
                except Exception as e:
                    log.warning(f"  Artist {artist_id} page failed: {e}")
                finally:
                    page.close()
                time.sleep(1.0)

        context.close()
        browser.close()

    return results, user_token, artist_stream_data

# ── Spotify Web API — metadata enrichment only ────────────────────────────────

_token_cache: dict = {}

def get_spotify_token() -> Optional[str]:
    """Client credentials token for Spotify Web API metadata calls."""
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
        log.info("Spotify Web API token refreshed")
        return _token_cache["access_token"]
    except Exception as e:
        log.warning(f"Spotify token failed: {e} — metadata enrichment skipped")
        return None

def fetch_track_metadata(spotify_id: str) -> Optional[dict]:
    """
    Fetch genre tags, ISRC, release date, and artist info
    from the Spotify Web API. Called once per newly discovered song.
    Returns None if credentials are missing or the call fails.
    """
    token = get_spotify_token()
    if not token:
        return None

    try:
        # fetch track
        track_resp = requests.get(
            f"https://api.spotify.com/v1/tracks/{spotify_id}",
            headers={"Authorization": f"Bearer {token}"},
            timeout=10,
        )
        track_resp.raise_for_status()
        track = track_resp.json()

        # fetch artist genres (genres live on the artist object, not the track)
        # Try batch endpoint first; fall back to individual calls if 403
        artist_ids = [a["id"] for a in track.get("artists", [])]
        genres = []
        if artist_ids:
            try:
                artists_resp = requests.get(
                    "https://api.spotify.com/v1/artists",
                    headers={"Authorization": f"Bearer {token}"},
                    params={"ids": ",".join(artist_ids[:10])},
                    timeout=10,
                )
                artists_resp.raise_for_status()
                for artist in artists_resp.json().get("artists", []):
                    genres.extend(artist.get("genres", []))
            except Exception:
                # Batch endpoint can 403 under client credentials; try one-by-one
                for aid in artist_ids[:3]:
                    try:
                        ar = requests.get(
                            f"https://api.spotify.com/v1/artists/{aid}",
                            headers={"Authorization": f"Bearer {token}"},
                            timeout=10,
                        )
                        if ar.status_code == 200:
                            genres.extend(ar.json().get("genres", []))
                        time.sleep(0.1)
                    except Exception:
                        pass

        # fetch album label (requires full album object, not simplified)
        album_id = track.get("album", {}).get("id")
        from collectors.label_utils import fetch_album_label, classify_label_tier
        label     = fetch_album_label(album_id, token) if album_id else None
        label_tier = classify_label_tier(label)

        time.sleep(0.1)  # gentle rate limiting
        return {
            "isrc":         track.get("external_ids", {}).get("isrc"),
            "release_date": track.get("album", {}).get("release_date"),
            "genre_tags":   list(set(genres)),
            "label":        label,
            "label_tier":   label_tier,
            "artists": [
                {"id": a["id"], "name": a["name"]}
                for a in track.get("artists", [])
            ],
        }

    except Exception as e:
        log.warning(f"Metadata fetch failed for {spotify_id}: {e}")
        return None

# ── Catalog upserts ───────────────────────────────────────────────────────────

def normalize(s: str) -> str:
    """Lowercase, strip punctuation — used for fuzzy matching."""
    return re.sub(r"[^\w\s]", "", s.lower().strip())

def upsert_artist(cur, name: str, spotify_id: str = None,
                  genres: list = None) -> str:
    """Insert or update artist record. Returns internal UUID."""
    spotify_key = spotify_id or f"unknown_{normalize(name)}"
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
    """, (name, normalize(name), spotify_key, genres or []))
    row = cur.fetchone()
    if row:
        return str(row["id"])
    cur.execute(
        "SELECT id FROM artists WHERE name_normalized = %s LIMIT 1",
        (normalize(name),)
    )
    return str(cur.fetchone()["id"])

def _normalize_release_date(raw: Optional[str]) -> Optional[str]:
    """Spotify returns 'YYYY', 'YYYY-MM', or 'YYYY-MM-DD'. Postgres needs full date."""
    if not raw:
        return None
    parts = raw.split("-")
    if len(parts) == 1:
        return f"{parts[0]}-01-01"
    if len(parts) == 2:
        return f"{parts[0]}-{parts[1]}-01"
    return raw  # already YYYY-MM-DD

def upsert_song(cur, title: str, artist_id: str, spotify_id: str,
                meta: Optional[dict]) -> str:
    """Insert or update song record. Returns internal UUID."""
    isrc       = meta.get("isrc") if meta else None
    genres     = meta.get("genre_tags", []) if meta else []
    rel_date   = _normalize_release_date(meta.get("release_date") if meta else None)
    label      = meta.get("label") if meta else None
    label_tier = meta.get("label_tier") if meta else None
    track_key  = spotify_id or f"unknown_{normalize(title)}"

    cur.execute("""
        INSERT INTO songs (
            title, title_normalized, artist_id,
            spotify_track_id, isrc, genre_tags, release_date, label, label_tier
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (spotify_track_id) DO UPDATE SET
            title      = EXCLUDED.title,
            genre_tags = CASE
                WHEN array_length(EXCLUDED.genre_tags, 1) > 0
                THEN EXCLUDED.genre_tags
                ELSE songs.genre_tags
            END,
            label      = COALESCE(EXCLUDED.label, songs.label),
            label_tier = COALESCE(EXCLUDED.label_tier, songs.label_tier),
            updated_at = NOW()
        RETURNING id
    """, (title, normalize(title), artist_id, track_key, isrc, genres, rel_date,
          label, label_tier))
    song_id = str(cur.fetchone()["id"])

    cur.execute("""
        UPDATE songs SET first_signal_at = NOW()
        WHERE id = %s AND first_signal_at IS NULL
    """, (song_id,))

    return song_id

def insert_chart_signal_event(cur, song_id: str, row: dict,
                              snapshot_date: date):
    """
    Write a signal_event for a chart appearance.
    Intentionality is low (0.15) — chart position is a passive signal.

    Engagement multiplier blends rank score with stream count (when available):
      - Rank score: 1.0 at #1, 0.0 at #201 (linear)
      - Stream bonus: log10(streams)/9 capped at 1.0 (matches YouTube formula)
    If streams unavailable, falls back to rank-only multiplier.
    """
    rank_score = max(0.0, (201 - row["rank"]) / 200)   # 1.0 at #1, 0.0 at #201

    streams = row.get("streams")
    if streams and streams > 0:
        stream_score = min(1.0, math.log10(streams) / 9)  # 1M→0.67, 10M→0.78, 100M→0.89
        mult = round(1 + (rank_score * 0.5 + stream_score * 0.5), 3)
    else:
        mult = round(1 + rank_score, 3)

    cur.execute("""
        INSERT INTO signal_events (
            observed_at, song_id, source_platform, signal_type,
            intentionality_score, raw_engagement, engagement_multiplier,
            weighted_score, is_home_community, context_snapshot
        )
        VALUES (%s, %s, 'spotify', 'chart_position', %s, %s, %s, %s, FALSE, %s)
        ON CONFLICT DO NOTHING
    """, (
        datetime.combine(snapshot_date, datetime.min.time()).replace(tzinfo=timezone.utc),
        song_id,
        INTENTIONALITY_CHART_POSITION,
        psycopg2.extras.Json({
            "rank":           row["rank"],
            "prev_rank":      row["prev_rank"],
            "weeks_on_chart": row["weeks_on_chart"],
            "entry_status":   row["entry_status"],
            "streams":        streams,
        }),
        mult,
        round(INTENTIONALITY_CHART_POSITION * mult, 4),
        psycopg2.extras.Json({
            "source":     "spotify_charts",
            "chart_name": row["chart_name"],
            "region":     row["region"],
            "rank":       row["rank"],
            "streams":    streams,
        }),
    ))

def insert_stream_count_signal(cur, song_id: str, track_data: dict,
                               snapshot_date: date):
    """
    Write a signal_event for an artist-page play count.

    Scores on the DELTA (new plays since last snapshot) rather than the
    raw cumulative total — this measures velocity, not catalogue size.
    Falls back to the full playcount on first observation (no baseline yet).
    Stores both total and delta in context_snapshot for trend analysis.
    """
    playcount = track_data["playcount"]

    # Look up the most recent prior playcount for this track
    cur.execute("""
        SELECT (context_snapshot->>'playcount_total')::bigint AS prev_total
        FROM signal_events
        WHERE song_id = %s::uuid
          AND signal_type = 'stream_count'
          AND source_platform = 'spotify'
          AND observed_at < %s
        ORDER BY observed_at DESC
        LIMIT 1
    """, (song_id, datetime.combine(snapshot_date, datetime.min.time()).replace(tzinfo=timezone.utc)))
    row = cur.fetchone()

    prev_total  = row[0] if row and row[0] else None
    delta       = (playcount - prev_total) if prev_total is not None else playcount
    delta       = max(delta, 0)   # guard against Spotify corrections / resets
    score_basis = delta if delta > 0 else playcount  # use total only on first run

    stream_score = min(1.0, math.log10(max(score_basis, 1)) / 9)
    mult         = round(1 + stream_score, 3)

    cur.execute("""
        INSERT INTO signal_events (
            observed_at, song_id, source_platform, signal_type,
            intentionality_score, raw_engagement, engagement_multiplier,
            weighted_score, is_home_community, external_id, context_snapshot
        )
        VALUES (%s, %s, 'spotify', 'stream_count', %s, %s, %s, %s, FALSE, %s, %s)
        ON CONFLICT DO NOTHING
    """, (
        datetime.combine(snapshot_date, datetime.min.time()).replace(tzinfo=timezone.utc),
        song_id,
        INTENTIONALITY_CHART_POSITION,
        psycopg2.extras.Json({"playcount_delta": delta, "playcount_total": playcount}),
        mult,
        round(INTENTIONALITY_CHART_POSITION * mult, 4),
        f"sp_pc_{track_data['track_id']}_{snapshot_date}",
        psycopg2.extras.Json({
            "source":          "spotify_artist_page",
            "track_id":        track_data["track_id"],
            "track_name":      track_data["track_name"],
            "playcount_total": playcount,
            "playcount_delta": delta,
            "prev_total":      prev_total,
        }),
    ))


# ── Artist subreddit discovery ────────────────────────────────────────────────

REDDIT_HEADERS = {"User-Agent": "everywhere-chart/0.1"}

def find_artist_subreddit(artist_name: str) -> Optional[str]:
    """
    Check whether r/{artist_name} exists with >1k subscribers.
    Tries common name variants. Returns subreddit name or None.
    """
    candidates = [
        artist_name.replace(" ", ""),
        artist_name.replace(" ", "_"),
        artist_name.lower().replace(" ", ""),
    ]
    seen = set()
    candidates = [c for c in candidates if not (c in seen or seen.add(c))]

    for name in candidates:
        try:
            resp = requests.get(
                f"https://www.reddit.com/r/{name}/about.json",
                headers=REDDIT_HEADERS,
                timeout=10,
            )
            if resp.status_code == 200:
                data = resp.json().get("data", {})
                if data.get("subscribers", 0) >= 1000:
                    log.info(f"Found r/{name} for artist '{artist_name}'")
                    return name
            time.sleep(0.6)
        except Exception:
            pass
    return None

def upsert_artist_community(cur, subreddit: str, artist_id: str):
    """Add artist subreddit to communities table and link to artist."""
    cur.execute("""
        INSERT INTO communities (
            platform, external_id, display_name, community_type,
            casual_weight, weight_source, auto_discovered
        )
        VALUES ('reddit', %s, %s, 'artist', 0.15, 'auto', TRUE)
        ON CONFLICT (platform, external_id) DO NOTHING
        RETURNING id
    """, (subreddit.lower(), f"r/{subreddit}"))
    row = cur.fetchone()
    if row:
        cur.execute("""
            UPDATE artists
            SET home_subreddit = %s, home_subreddit_id = %s
            WHERE id = %s AND home_subreddit IS NULL
        """, (subreddit, str(row["id"]), artist_id))

# ── Main ──────────────────────────────────────────────────────────────────────

def run(snapshot_date: date = None):
    snapshot_date = snapshot_date or date.today()

    conn = psycopg2.connect(DB_URL)
    conn.autocommit = False
    psycopg2.extras.register_uuid()

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            INSERT INTO collector_runs (collector, metadata)
            VALUES ('spotify_charts', %s) RETURNING id
        """, (psycopg2.extras.Json({"snapshot_date": str(snapshot_date)}),))
        run_id = cur.fetchone()["id"]
    conn.commit()

    total_events  = 0
    total_dropped = 0

    try:
        # Query under-radar artist IDs before launching Playwright so we can
        # fetch their play counts in the same browser session.
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT DISTINCT a.spotify_artist_id
                FROM songs s
                JOIN artists a ON a.id = s.artist_id
                WHERE s.under_radar = TRUE
                  AND a.spotify_artist_id IS NOT NULL
                  AND a.spotify_artist_id NOT LIKE 'unknown_%%'
                ORDER BY a.spotify_artist_id
                LIMIT %s
            """, (MAX_ARTIST_STREAM_FETCHES,))
            under_radar_artist_ids = [r["spotify_artist_id"] for r in cur.fetchall()]
        log.info(f"Under-radar artists to stream-count: {len(under_radar_artist_ids)}")

        all_chart_rows, user_token, artist_stream_data = fetch_all_charts(
            artist_spotify_ids=under_radar_artist_ids,
        )

        seen_spotify_ids: set[str] = set()  # dedupe songs appearing in multiple charts

        for chart in CHARTS_TO_FETCH:
            rows = all_chart_rows.get(chart["name"], [])
            if not rows:
                log.warning(f"No rows returned for {chart['name']} — skipping")
                continue

            for row in rows:
                # skip songs we already processed from another chart this run
                if row["spotify_id"] and row["spotify_id"] in seen_spotify_ids:
                    continue
                if row["spotify_id"]:
                    seen_spotify_ids.add(row["spotify_id"])

                try:
                    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:

                        # check if song already in catalog
                        cur.execute("""
                            SELECT s.id FROM songs s
                            JOIN artists a ON s.artist_id = a.id
                            WHERE s.title_normalized = %s
                              AND a.name_normalized  = %s
                            LIMIT 1
                        """, (normalize(row["title"]), normalize(row["artist"])))
                        existing = cur.fetchone()

                        if existing:
                            song_id = str(existing["id"])

                        else:
                            log.info(f"New song: {row['title']} — {row['artist']}")

                            # enrich with Spotify Web API metadata
                            meta = None
                            if row["spotify_id"]:
                                meta = fetch_track_metadata(row["spotify_id"])

                            # upsert primary artist
                            spotify_artist_id = None
                            genres = []
                            if meta and meta.get("artists"):
                                spotify_artist_id = meta["artists"][0]["id"]
                                genres = meta.get("genre_tags", [])

                            artist_id = upsert_artist(
                                cur,
                                name       = row["artist"],
                                spotify_id = spotify_artist_id,
                                genres     = genres,
                            )

                            # discover and store artist subreddit
                            cur.execute(
                                "SELECT home_subreddit FROM artists WHERE id = %s",
                                (artist_id,)
                            )
                            artist_row = cur.fetchone()
                            if artist_row and artist_row["home_subreddit"] is None:
                                subreddit = find_artist_subreddit(row["artist"])
                                if subreddit:
                                    upsert_artist_community(cur, subreddit, artist_id)

                            song_id = upsert_song(
                                cur,
                                title      = row["title"],
                                artist_id  = artist_id,
                                spotify_id = row["spotify_id"],
                                meta       = meta,
                            )

                        insert_chart_signal_event(cur, song_id, row, snapshot_date)
                        total_events += 1

                    conn.commit()

                except Exception as e:
                    conn.rollback()
                    log.error(f"Failed processing '{row.get('title', '?')}': {e}")
                    total_dropped += 1


        with conn.cursor() as cur:
            cur.execute("""
                UPDATE collector_runs
                SET status = 'success', completed_at = NOW(),
                    events_collected = %s, events_dropped = %s
                WHERE id = %s
            """, (total_events, total_dropped, run_id))
        conn.commit()
        log.info(
            f"Spotify collector complete — "
            f"{total_events} events written, {total_dropped} dropped"
        )

        # ── Artist play counts for under-radar / non-chart songs ──────────
        # artist_stream_data: {artist_spotify_id: [{track_id, track_name, playcount}]}
        if artist_stream_data:
            stream_hits = stream_skipped = 0
            for artist_spotify_id, tracks in artist_stream_data.items():
                for track_data in tracks:
                    try:
                        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                            cur.execute(
                                "SELECT id FROM songs WHERE spotify_track_id = %s",
                                (track_data["track_id"],)
                            )
                            row = cur.fetchone()
                            if row:
                                insert_stream_count_signal(
                                    cur, str(row["id"]), track_data, snapshot_date
                                )
                                stream_hits += 1
                            else:
                                stream_skipped += 1
                        conn.commit()
                    except Exception as e:
                        conn.rollback()
                        log.warning(
                            f"Stream count write failed for "
                            f"'{track_data['track_name']}': {e}"
                        )
            log.info(
                f"Artist play counts — {stream_hits} signals written, "
                f"{stream_skipped} tracks not in catalog"
            )

        # ── Playlist seeder — runs after chart data is safely committed ───
        # Pass the Playwright-extracted user token if we got one; otherwise
        # the seeder falls back to sp_dc directly (also works for track reads).
        try:
            from collectors.spotify_playlist_seeder import run as seed_playlists
            if user_token:
                log.info("Starting playlist seeder with user token from Playwright session")
            else:
                log.info("Starting playlist seeder with sp_dc fallback (no Playwright token)")
            seed_playlists(user_token=user_token, conn=conn)
        except Exception as e:
            log.warning(f"Playlist seeder failed (non-fatal): {e}")

    except Exception as e:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE collector_runs
                SET status = 'failed', completed_at = NOW(), error_message = %s
                WHERE id = %s
            """, (str(e), run_id))
        conn.commit()
        log.error(f"Spotify collector failed: {e}")
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    import sys
    target = date.fromisoformat(sys.argv[1]) if len(sys.argv) > 1 else date.today()
    run(target)
