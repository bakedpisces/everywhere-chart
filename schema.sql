-- ============================================================
-- THE EVERYWHERE CHART — DATABASE SCHEMA
-- PostgreSQL + TimescaleDB
-- ============================================================

-- Enable TimescaleDB for time-series optimization
CREATE EXTENSION IF NOT EXISTS pg_trgm;   -- fuzzy string matching
CREATE EXTENSION IF NOT EXISTS unaccent;  -- normalize accented chars in entity resolution

-- ============================================================
-- ARTISTS
-- ============================================================

CREATE TABLE artists (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name                TEXT NOT NULL,
    name_normalized     TEXT NOT NULL,          -- lowercase, no punctuation
    name_aliases        TEXT[] DEFAULT '{}',    -- known alternate names
    spotify_artist_id   TEXT UNIQUE,
    musicbrainz_id      TEXT,
    genre_tags          TEXT[] DEFAULT '{}',    -- from Spotify / MusicBrainz
    home_subreddit      TEXT,                   -- e.g. 'taylorswift' — populated on discovery
    home_subreddit_id   UUID,                   -- FK to communities, set after subreddit check
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX artists_name_norm_idx ON artists (name_normalized);
CREATE INDEX artists_spotify_id_idx ON artists (spotify_artist_id);
CREATE INDEX artists_name_trgm_idx ON artists USING GIN (name_normalized gin_trgm_ops);

-- ============================================================
-- SONGS
-- ============================================================

CREATE TABLE songs (
    id                      UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    -- identity
    title                   TEXT NOT NULL,
    title_normalized        TEXT NOT NULL,
    title_aliases           TEXT[] DEFAULT '{}',
    artist_id               UUID NOT NULL REFERENCES artists(id),
    featured_artist_ids     UUID[] DEFAULT '{}',

    -- catalog cross-references
    spotify_track_id        TEXT UNIQUE,
    musicbrainz_id          TEXT,
    isrc                    TEXT,
    shazam_id               TEXT,

    -- genre / home context
    genre_tags              TEXT[] DEFAULT '{}',    -- raw from Spotify
    genre_community_ids     UUID[] DEFAULT '{}',    -- mapped to communities table
    home_community_ids      UUID[] DEFAULT '{}',    -- current best estimate
    home_confidence         FLOAT DEFAULT 0.3,      -- 0.3 (genre tag) → 0.95 (settled)
    home_source             TEXT DEFAULT 'genre_tag'
                                CHECK (home_source IN (
                                    'genre_tag','artist_history',
                                    'early_signal','settled'
                                )),
    home_updated_at         TIMESTAMPTZ,

    -- entity resolution catalog
    notable_lyrics          TEXT,                   -- hook + first line for lyric matching
    search_vector           TSVECTOR,               -- full-text search

    -- timeline
    release_date            DATE,
    first_signal_at         TIMESTAMPTZ,            -- when we first observed it
    created_at              TIMESTAMPTZ DEFAULT NOW(),
    updated_at              TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX songs_title_norm_idx ON songs (title_normalized);
CREATE INDEX songs_spotify_id_idx ON songs (spotify_track_id);
CREATE INDEX songs_artist_idx ON songs (artist_id);
CREATE INDEX songs_trgm_idx ON songs USING GIN (title_normalized gin_trgm_ops);
CREATE INDEX songs_search_idx ON songs USING GIN (search_vector);

-- auto-update search vector
CREATE OR REPLACE FUNCTION songs_search_vector_update() RETURNS TRIGGER AS $$
BEGIN
    NEW.search_vector :=
        setweight(to_tsvector('english', COALESCE(NEW.title, '')), 'A') ||
        setweight(to_tsvector('english', COALESCE(NEW.notable_lyrics, '')), 'B');
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER songs_search_vector_trigger
    BEFORE INSERT OR UPDATE ON songs
    FOR EACH ROW EXECUTE FUNCTION songs_search_vector_update();

-- ============================================================
-- COMMUNITIES
-- ============================================================

CREATE TABLE communities (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    -- identity
    platform            TEXT NOT NULL CHECK (platform IN (
                            'reddit','twitter','tiktok',
                            'youtube','forum','other'
                        )),
    external_id         TEXT NOT NULL,              -- subreddit name, channel ID, etc.
    display_name        TEXT,
    description         TEXT,
    url                 TEXT,

    -- taxonomy
    community_type      TEXT NOT NULL CHECK (community_type IN (
                            'artist','genre','general_music',
                            'entertainment','sports','lifestyle','non_music'
                        )),
    casual_weight       FLOAT NOT NULL DEFAULT 0.5
                            CHECK (casual_weight BETWEEN 0 AND 1),
    weight_source       TEXT DEFAULT 'auto'
                            CHECK (weight_source IN ('auto','behavioral','manual')),

    -- size signal
    subscriber_count    INTEGER,
    daily_active_users  INTEGER,                    -- if available

    -- classification metadata
    classified_at       TIMESTAMPTZ,
    home_confidence     FLOAT DEFAULT 0.5,
    auto_discovered     BOOLEAN DEFAULT FALSE,      -- found in the wild vs. seeded

    created_at          TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW(),

    UNIQUE (platform, external_id)
);

CREATE INDEX communities_type_idx ON communities (community_type);
CREATE INDEX communities_platform_idx ON communities (platform);
CREATE INDEX communities_weight_idx ON communities (casual_weight);

-- ============================================================
-- COMMUNITY DISTANCES
-- Sparse graph — only pairs that have co-occurred
-- ============================================================

CREATE TABLE community_distances (
    community_a_id      UUID NOT NULL REFERENCES communities(id),
    community_b_id      UUID NOT NULL REFERENCES communities(id),
    distance            FLOAT NOT NULL CHECK (distance BETWEEN 0 AND 1),
    distance_method     TEXT NOT NULL CHECK (distance_method IN (
                            'taxonomy_rule','co_mention','embedding','manual'
                        )),
    computed_at         TIMESTAMPTZ DEFAULT NOW(),

    PRIMARY KEY (community_a_id, community_b_id),
    CHECK (community_a_id < community_b_id)  -- store once, undirected
);

-- ============================================================
-- SIGNAL EVENTS
-- Atomic unit — one mention, play, save, sound-use, etc.
-- Never aggregate before storing.
-- ============================================================

CREATE TABLE signal_events (
    id                      UUID DEFAULT gen_random_uuid(),
    observed_at             TIMESTAMPTZ NOT NULL,   -- partition key

    -- what
    song_id                 UUID REFERENCES songs(id),
    resolution_confidence   FLOAT NOT NULL DEFAULT 1.0
                                CHECK (resolution_confidence BETWEEN 0 AND 1),

    -- where
    community_id            UUID REFERENCES communities(id),
    source_platform         TEXT NOT NULL CHECK (source_platform IN (
                                'reddit','spotify','shazam','twitter',
                                'tiktok','youtube','press','forum'
                            )),

    -- type of signal
    signal_type             TEXT NOT NULL CHECK (signal_type IN (
                                'mention','post','comment','save','share',
                                'sound_use','shazam','playlist_add',
                                'chart_position','upvote'
                            )),

    -- intentionality
    intentionality_score    FLOAT NOT NULL CHECK (intentionality_score BETWEEN 0 AND 1),

    -- engagement
    raw_engagement          JSONB DEFAULT '{}',     -- {likes, replies, reposts, views}
    engagement_multiplier   FLOAT DEFAULT 1.0,      -- 1 + log10(likes + replies*2 + reposts*3 + 1)

    -- scoring
    community_casual_weight FLOAT,                  -- snapshot of community.casual_weight at event time
    home_distance           FLOAT,                  -- distance from song's home communities
    home_confidence         FLOAT,                  -- snapshot of song.home_confidence
    effective_distance      FLOAT,                  -- distance * confidence + 0.5 * (1 - confidence)
    weighted_score          FLOAT,                  -- intentionality * engagement_mult * casual_weight * effective_distance * resolution_confidence
    is_home_community       BOOLEAN DEFAULT FALSE,

    -- deduplication
    external_id             TEXT,                   -- platform-native ID (Reddit post ID, etc.)
    external_url            TEXT,

    -- context snapshot for entity resolution audit
    context_snapshot        JSONB DEFAULT '{}',     -- {post_title, subreddit, parent_text, ...}

    PRIMARY KEY (id, observed_at)
);


-- Indexes on hypertable
CREATE INDEX signal_events_song_idx
    ON signal_events (song_id, observed_at DESC);
CREATE INDEX signal_events_community_idx
    ON signal_events (community_id, observed_at DESC);
CREATE INDEX signal_events_platform_idx
    ON signal_events (source_platform, observed_at DESC);
CREATE INDEX signal_events_external_idx
    ON signal_events (source_platform, external_id)
    WHERE external_id IS NOT NULL;             -- deduplication lookup

-- ============================================================
-- SONG SCORES
-- Materialized per window — recomputed nightly
-- ============================================================

CREATE TABLE song_scores (
    song_id                     UUID NOT NULL REFERENCES songs(id),
    window_date                 DATE NOT NULL,
    chart_category              TEXT NOT NULL,      -- 'crossover','rising','mainstream','longevity'

    -- raw aggregates
    total_signal_count          INTEGER DEFAULT 0,
    out_of_home_signal_count    INTEGER DEFAULT 0,
    home_signal_count           INTEGER DEFAULT 0,

    -- weighted scores (disaggregated — never lose these)
    out_of_home_weighted_score  FLOAT DEFAULT 0,
    home_weighted_score         FLOAT DEFAULT 0,

    -- diversity
    community_count             INTEGER DEFAULT 0,  -- distinct communities
    community_type_count        INTEGER DEFAULT 0,  -- distinct community types
    diversity_multiplier        FLOAT DEFAULT 1.0,  -- 1 + 0.25 * (type_count - 1)

    -- home inclusion (tunable coefficient)
    home_inclusion_coefficient  FLOAT DEFAULT 0.1,

    -- final score
    penetration_score           FLOAT DEFAULT 0,
    -- (out_of_home + home * coeff) * diversity / log(home + 1)

    -- momentum
    prev_penetration_score      FLOAT,
    momentum_delta              FLOAT,              -- penetration_score - prev
    momentum_pct                FLOAT,              -- % change

    -- chart position
    rank                        INTEGER,
    prev_rank                   INTEGER,

    -- breakdown by source
    signal_breakdown            JSONB DEFAULT '{}', -- {reddit: x, spotify: y, shazam: z}

    -- top communities this window (for UI and narratives)
    top_communities             JSONB DEFAULT '[]', -- [{community_id, score, distance, top_post_title}]
    new_communities_this_week   JSONB DEFAULT '[]', -- communities not seen in prior window

    -- narrative (generated after scoring)
    narrative_short             TEXT,
    narrative_long              TEXT,
    narrative_generated_at      TIMESTAMPTZ,
    story_length                TEXT CHECK (story_length IN ('featured','long','short','none')),

    computed_at                 TIMESTAMPTZ DEFAULT NOW(),

    PRIMARY KEY (song_id, window_date, chart_category)
);

CREATE INDEX song_scores_date_idx ON song_scores (window_date DESC, chart_category);
CREATE INDEX song_scores_rank_idx ON song_scores (chart_category, window_date DESC, rank);

-- ============================================================
-- CHART CATEGORIES
-- Config-driven — add new chart types without code changes
-- ============================================================

CREATE TABLE chart_categories (
    id                          TEXT PRIMARY KEY,   -- 'crossover', 'rising', etc.
    name                        TEXT NOT NULL,
    description                 TEXT,

    -- scoring config
    window_days                 INTEGER DEFAULT 7,
    size                        INTEGER DEFAULT 25,
    home_inclusion_coefficient  FLOAT DEFAULT 0.1,
    diversity_multiplier_coeff  FLOAT DEFAULT 0.25,

    -- filters (JSONB for flexibility)
    filters                     JSONB DEFAULT '{}',
    -- {min_casual_weight, min_community_types, exclude_community_types,
    --  include_community_types, recency_days, min_signal_count}

    -- sort
    sort_by                     TEXT DEFAULT 'penetration_score'
                                    CHECK (sort_by IN (
                                        'penetration_score','momentum_delta',
                                        'community_type_count','longevity_score',
                                        'out_of_home_weighted_score'
                                    )),

    -- per-source weight overrides
    source_weights              JSONB DEFAULT '{"reddit":0.7,"spotify":0.1,"shazam":0.2}',

    -- operational
    refresh_cadence             TEXT DEFAULT 'daily'
                                    CHECK (refresh_cadence IN ('hourly','daily','weekly')),
    enabled                     BOOLEAN DEFAULT TRUE,
    tags                        TEXT[] DEFAULT '{}',

    created_at                  TIMESTAMPTZ DEFAULT NOW()
);

-- Seed starter chart categories
INSERT INTO chart_categories (id, name, description, sort_by, filters) VALUES
    ('crossover', 'Crossover',
     'Songs reaching the most distinct audiences outside their home fanbase',
     'penetration_score',
     '{"min_community_types": 2, "exclude_community_types": ["artist"]}'
    ),
    ('rising', 'Rising',
     'Biggest week-on-week momentum — songs in active expansion',
     'momentum_delta',
     '{"min_signal_count": 5}'
    ),
    ('mainstream', 'Mainstream',
     'Overall cultural presence — broad reach, sustained engagement',
     'out_of_home_weighted_score',
     '{}'
    ),
    ('longevity', 'Longevity',
     'Songs maintaining casual-fan presence 30+ days after release',
     'penetration_score',
     '{"min_weeks_on_chart": 4}'
    );

-- ============================================================
-- SPOTIFY CHART SNAPSHOTS
-- Raw daily chart data — source of truth for catalog
-- ============================================================

CREATE TABLE spotify_chart_snapshots (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    snapshot_date   DATE NOT NULL,
    region          TEXT NOT NULL DEFAULT 'global',
    chart_type      TEXT NOT NULL DEFAULT 'top200',  -- 'top200','viral50'
    rank            INTEGER NOT NULL,
    spotify_track_id TEXT NOT NULL,
    song_title      TEXT NOT NULL,
    artist_name     TEXT NOT NULL,
    stream_count    BIGINT,
    song_id         UUID REFERENCES songs(id),       -- populated after entity resolution
    fetched_at      TIMESTAMPTZ DEFAULT NOW(),

    UNIQUE (snapshot_date, region, chart_type, rank)
);

CREATE INDEX spotify_snapshots_date_idx ON spotify_chart_snapshots (snapshot_date DESC);
CREATE INDEX spotify_snapshots_track_idx ON spotify_chart_snapshots (spotify_track_id);

-- ============================================================
-- SHAZAM CHART SNAPSHOTS
-- ============================================================

CREATE TABLE shazam_chart_snapshots (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    snapshot_date   DATE NOT NULL,
    region          TEXT NOT NULL DEFAULT 'global',
    genre           TEXT DEFAULT 'all',
    rank            INTEGER NOT NULL,
    shazam_id       TEXT,
    song_title      TEXT NOT NULL,
    artist_name     TEXT NOT NULL,
    song_id         UUID REFERENCES songs(id),
    fetched_at      TIMESTAMPTZ DEFAULT NOW(),

    UNIQUE (snapshot_date, region, genre, rank)
);

CREATE INDEX shazam_snapshots_date_idx ON shazam_chart_snapshots (snapshot_date DESC);

-- ============================================================
-- MARKET TREND SNAPSHOTS
-- Chart-wide aggregate stats per week — feeds MarketTrendCard
-- ============================================================

CREATE TABLE market_trend_snapshots (
    window_date                 DATE PRIMARY KEY,
    chart_category              TEXT NOT NULL DEFAULT 'crossover',

    -- chart-wide aggregates
    avg_penetration_score       FLOAT,
    avg_community_type_count    FLOAT,
    prev_avg_penetration_score  FLOAT,
    prev_avg_community_type_count FLOAT,

    -- community type activity
    community_type_counts       JSONB DEFAULT '{}',
    -- {lifestyle: {songs: 14, prev: 11}, sports: {songs: 9, prev: 9}, ...}

    -- signal type breakdown
    dominant_signal_type        TEXT,
    signal_type_breakdown       JSONB DEFAULT '{}',

    -- genre crossover health
    genre_crossover             JSONB DEFAULT '{}',
    -- {country: {songs: 0, avg_distance: 0.2}, pop: {songs: 8, avg_distance: 0.7}, ...}

    -- notable absences (genres/types quiet this week)
    notable_absences            JSONB DEFAULT '[]',

    -- trailing history for trend detection (4 weeks)
    trailing_weeks              JSONB DEFAULT '[]',

    -- generated narrative
    narrative                   TEXT,
    narrative_generated_at      TIMESTAMPTZ,

    computed_at                 TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================
-- COLLECTOR RUN LOG
-- Track each collection job for monitoring
-- ============================================================

CREATE TABLE collector_runs (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    collector       TEXT NOT NULL,  -- 'reddit','spotify_charts','shazam'
    started_at      TIMESTAMPTZ DEFAULT NOW(),
    completed_at    TIMESTAMPTZ,
    status          TEXT DEFAULT 'running'
                        CHECK (status IN ('running','success','partial','failed')),
    events_collected INTEGER DEFAULT 0,
    events_dropped   INTEGER DEFAULT 0,     -- below confidence threshold
    events_queued    INTEGER DEFAULT 0,     -- awaiting entity resolution
    error_message    TEXT,
    metadata        JSONB DEFAULT '{}'     -- {subreddits_polled, rate_limit_hits, ...}
);

CREATE INDEX collector_runs_collector_idx ON collector_runs (collector, started_at DESC);

-- ============================================================
-- ENTITY RESOLUTION QUEUE
-- Events awaiting confident song resolution
-- ============================================================

CREATE TABLE resolution_queue (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    raw_text            TEXT NOT NULL,
    context_json        JSONB DEFAULT '{}',     -- post title, subreddit, surrounding text
    source_platform     TEXT NOT NULL,
    community_id        UUID REFERENCES communities(id),
    observed_at         TIMESTAMPTZ NOT NULL,
    external_id         TEXT,

    -- resolution attempts
    fuzzy_candidates    JSONB DEFAULT '[]',     -- top 3-5 fuzzy matches with scores
    resolution_step     INTEGER DEFAULT 1,      -- which step reached (1=exact,2=fuzzy,3=context,4=llm)
    resolution_confidence FLOAT,
    resolved_song_id    UUID REFERENCES songs(id),
    resolved_at         TIMESTAMPTZ,
    resolution_method   TEXT,

    -- queue management
    attempts            INTEGER DEFAULT 0,
    status              TEXT DEFAULT 'pending'
                            CHECK (status IN ('pending','resolved','dropped','needs_review')),
    created_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX resolution_queue_status_idx ON resolution_queue (status, created_at);
