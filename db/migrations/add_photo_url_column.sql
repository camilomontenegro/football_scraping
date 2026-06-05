-- MIGRATION: player photo URLs on dim_player.
-- Idempotent — safe to re-run.

ALTER TABLE dim_player ADD COLUMN IF NOT EXISTS photo_url TEXT;

CREATE INDEX IF NOT EXISTS idx_player_photo_url
ON dim_player (canonical_id)
WHERE photo_url IS NOT NULL;
