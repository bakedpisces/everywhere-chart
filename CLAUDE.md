# The Everywhere Chart — CLAUDE.md

Music discovery chart that ranks songs by cultural penetration: intentional engagement
across communities far from a song's home fanbase.

## Architecture

### Services (all deployed on Railway, auto-deploy from GitHub `main`)

| Service | Dockerfile | Schedule | Purpose |
|---------|-----------|----------|---------|
| dashboard | (Nixpacks, detects `dashboard.py`) | always-on | Streamlit chart UI |
| spotify | `Dockerfile.spotify` | daily 08:00 UTC | Spotify charts + playlist seeder + artist stream counts |
| scrapecreators | `Dockerfile.scrapecreators` | daily 10:00 UTC | TikTok sound counts + YouTube views via ScrapeCreators API |
| enricher | `Dockerfile.enricher` | daily 11:00 UTC | Upgrades placeholder Spotify IDs; backfills labels/genres |
| shazam | (Nixpacks) | every 6h | Shazam trending charts via RapidAPI |
| youtube | (Nixpacks) | daily | YouTube trending charts |
| press | (Nixpacks) | every 6h | Google News RSS + fixed publication feeds |
| reddit | (Nixpacks) | dormant | Blocked by Railway IPs; OAuth code ready, awaiting credentials |
| tiktok | `Dockerfile.tiktok` | dormant | Replaced by ScrapeCreators for TikTok data |

### Database
PostgreSQL on Railway. Extensions: `pg_trgm`, `unaccent`.

Key tables: `artists`, `songs`, `signal_events`, `communities`, `song_scores`, `playlist_memberships`.

`signal_events` is the core fact table — every data point written here with `source_platform`,
`signal_type`, `weighted_score`, `resolution_confidence`, `observed_at`.

## Collectors

### spotify_collector.py
- Uses Playwright (`mcr.microsoft.com/playwright/python:v1.44.0-jammy` base image)
- Intercepts `Authorization: Bearer` header from chart API requests via `page.on("request")` — NOT `get_access_token` (returns anonymous token)
- Captures artist stream counts from `api-partner.spotify.com/pathfinder/v2/query` via passive `page.on("response")` + 10s wait (not `expect_response` — causes timeouts)
- `MAX_ARTIST_STREAM_FETCHES = 100` under-radar artists per run
- Runs `spotify_playlist_seeder` inline after charts, passing the intercepted user token
- Randomised delays between artist page visits to reduce bot detection risk
- Uses a dedicated throwaway Spotify account (`sp_dc` cookie env var)

### spotify_playlist_seeder.py
- Searches for discovery playlists using keywords like "new music 2026", "viral hits", etc.
- Filters: static/retrospective playlists (regex), artist-named playlists (DB name lookup), playlists with <5 tracks
- Dedup: 0-track playlists retry after 1h; successful playlists retry after 23h
- Writes `playlist_reach` signal events daily
- Increments `songs.playlist_count` on new membership rows

### scrapecreators_collector.py
- 2 credits per song (1 TikTok + 1 YouTube)
- **Tiered recheck windows**: under-radar=3 days, recently-active=5 days, cold catalog=14 days
- `MAX_SONGS = 500` per run (~1,000 credits/day)
- Stops entire run on 402 (out of credits) or 401 (bad key) — check ScrapeCreators account if TikTok/YouTube signals stop
- No-match records written with `external_id = 'sc_tt_no_match'` / `'sc_yt_no_match'`, `resolution_confidence=0.0` (excluded from dashboard scoring)

### spotify_enricher.py
- CLI flags: `--labels`, `--genres`, `--all`, or positional batch size
- Batch `/v1/tracks` (50/call) → album IDs → batch `/v1/albums` (20/call) → labels
- Batch `/v1/artists` (50/call) for genre backfill; propagates to `songs.genre_tags`

### reddit_collector.py
- Railway IPs blocked from public `.json` API
- OAuth client_credentials flow implemented and ready (`oauth.reddit.com`)
- Dormant until `REDDIT_CLIENT_ID` + `REDDIT_CLIENT_SECRET` env vars set
- Reddit requires a special API access request (no longer self-serve app creation)

## Dashboard (dashboard.py)

Streamlit app. Two chart tabs:
- **Crossover**: penetration score = out-of-home signals × diversity multiplier / log(home+1)
- **Stream Velocity**: daily stream % increase (delta / prev_total), with min-delta threshold slider

### Scoring formula
```
penetration_score = (out_of_home + home * home_coeff) * diversity_mult / log10(home + 1)
diversity_mult    = 1 + diversity_coeff * (community_type_count - 1)
```

Platform weights applied first; signals from zero-weight platforms are fully dropped before
aggregation (including community type counts).

Press signals deduplicated to one per publication per song (breadth over volume).

`COALESCE(c.community_type, se.source_platform)` — platform signals count as community
contexts so Spotify/Shazam/YouTube each contribute to `community_type_count`.

### Running locally
```bash
cd /Users/sachindoshi/Developer/Charts_Project
source venv/bin/activate
streamlit run dashboard.py
```
Password: set in `APP_PASSWORD` env var (default: `civilwar`).

## Signal types

| signal_type | platform | meaning |
|-------------|----------|---------|
| `chart_position` | spotify, shazam, youtube | position on a trending chart |
| `sound_use` | tiktok | # videos using the sound |
| `article` | press | publication mention |
| `playlist_membership` | spotify | added to a playlist |
| `stream_count` | spotify | artist page play count snapshot |
| `playlist_reach` | spotify | daily follower-weighted playlist signal |

## Key gotchas & hard-won knowledge

### Railway deployments
- Services with `Dockerfile.*` use that file; others use Nixpacks auto-detection
- Nixpacks reads `requirements.txt` — **do not put `playwright` in requirements.txt** (greenlet C++ build fails). Only `Dockerfile.spotify` installs playwright explicitly
- `DATABASE_URL` is set per-service in Railway env vars. Never let a local `.env` shadow it when debugging

### Cursors
- Always use `psycopg2.extras.RealDictCursor` for row dict access (`row["col"]` not `row[0]`)
- Missing this causes silent `None` returns — was the root cause of stream delta always being null

### Stream count deltas
- `insert_stream_count_signal()` looks up previous `context_snapshot->>'playcount_total'` to compute daily delta
- `external_id = f"sp_pc_{track_id}_{snapshot_date}"` — one record per track per day
- Delta velocity is what scores, not cumulative total

### Under-radar flag
Criteria (all required):
- `release_date IS NOT NULL`
- Real Spotify track ID (not placeholder `SP_PLACEHOLDER_*`)
- `<10M lifetime plays` (from stream_count signals)
- On a UGC playlist
- Not on Spotify chart in last 30 days

Songs age out when: track > 18 months old, or lifetime plays > 10M, or appears on Spotify chart.

### Signal dedup
`ON CONFLICT DO NOTHING` on `external_id` — most signal types include date in the external_id for daily dedup. No-match ScrapeCreators records use a fixed external_id (`sc_tt_no_match`) — this is intentional, they serve only to mark "we checked this song recently".

### label_utils.py
`classify_label_tier(label)` → `'major'`, `'indie'`, `'unsigned'`, or `'unknown'`
Fetches label via `/v1/albums/{album_id}?fields=label`.

## Migrations run on Railway DB
- `migrations/003_playlist_tracking.sql` — playlist_memberships table
- `migrations/004_label_tracking.sql` — label, label_tier on songs
- `migrations/005_playlist_count.sql` — playlist_count on songs
- Manual: added `'stream_count'` and `'playlist_reach'` to `signal_events_signal_type_check` constraint
- Manual: cleared ~50 bad under_radar flags (Taylor Swift catalog + other false positives)

## Environment variables

| Var | Used by |
|-----|---------|
| `DATABASE_URL` | all services |
| `SPOTIFY_CLIENT_ID` / `SPOTIFY_CLIENT_SECRET` | spotify_collector, enricher |
| `SPOTIFY_SP_DC` | spotify_collector (user session cookie for Playwright) |
| `RAPIDAPI_KEY` | shazam_collector |
| `SCRAPECREATORS_API_KEY` | scrapecreators_collector |
| `REDDIT_CLIENT_ID` / `REDDIT_CLIENT_SECRET` / `REDDIT_USERNAME` | reddit_collector (dormant) |
| `APP_PASSWORD` | dashboard |

## Current data status (as of July 2026)
- ~7,300+ songs in catalog
- Active platforms: Spotify, Shazam, YouTube, Press, ScrapeCreators (TikTok+YT)
- Dormant: Reddit (IP blocked), TikTok direct collector (replaced by ScrapeCreators)
- Stream count deltas tracking correctly since June 2026 run
- ScrapeCreators: 25,000 credit balance as of July 7 2026 (~25 days at current rate)
