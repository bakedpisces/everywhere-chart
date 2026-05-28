-- Migration 004: Label tracking on songs
-- Run once against the live database.

ALTER TABLE songs
    ADD COLUMN IF NOT EXISTS label      TEXT,
    ADD COLUMN IF NOT EXISTS label_tier TEXT
        CHECK (label_tier IN ('major', 'indie', 'unsigned', 'unknown'));

CREATE INDEX IF NOT EXISTS songs_label_tier_idx ON songs (label_tier);
