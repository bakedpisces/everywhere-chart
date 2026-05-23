-- Migration 003: Playlist follower tracking + under-the-radar flag
-- Run once against the live database.

-- ── New columns on songs ──────────────────────────────────────────────────────

ALTER TABLE songs
    ADD COLUMN IF NOT EXISTS playlist_follower_count BIGINT  DEFAULT 0,
    ADD COLUMN IF NOT EXISTS under_radar             BOOLEAN DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS under_radar_since       TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS songs_release_date_idx
    ON songs (release_date DESC NULLS LAST);

CREATE INDEX IF NOT EXISTS songs_under_radar_idx
    ON songs (under_radar) WHERE under_radar = TRUE;

CREATE INDEX IF NOT EXISTS songs_playlist_followers_idx
    ON songs (playlist_follower_count DESC);

-- ── Song → playlist membership tracking ──────────────────────────────────────
-- Tracks which song has been found in which Spotify playlist.
-- Prevents double-counting follower contributions on re-runs.

CREATE TABLE IF NOT EXISTS song_playlist_memberships (
    song_id       UUID  NOT NULL REFERENCES songs(id) ON DELETE CASCADE,
    playlist_id   TEXT  NOT NULL,
    is_editorial  BOOLEAN NOT NULL DEFAULT FALSE,   -- TRUE if owner = 'spotify'
    followers     INTEGER NOT NULL DEFAULT 0,
    first_seen_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (song_id, playlist_id)
);

CREATE INDEX IF NOT EXISTS spm_song_idx      ON song_playlist_memberships (song_id);
CREATE INDEX IF NOT EXISTS spm_editorial_idx ON song_playlist_memberships (song_id, is_editorial);
