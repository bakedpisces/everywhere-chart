-- Migration 005: Add playlist_count column to songs
ALTER TABLE songs
    ADD COLUMN IF NOT EXISTS playlist_count INTEGER DEFAULT 0;

-- Back-fill from existing memberships
UPDATE songs s
SET playlist_count = (
    SELECT COUNT(*) FROM song_playlist_memberships m WHERE m.song_id = s.id
);
