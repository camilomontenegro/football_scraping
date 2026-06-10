-- MIGRATION: add team context to player_review for easier disambiguation.
-- Idempotent — safe to re-run.

ALTER TABLE player_review ADD COLUMN IF NOT EXISTS source_team_id   VARCHAR(50);
ALTER TABLE player_review ADD COLUMN IF NOT EXISTS source_team_name VARCHAR(150);
