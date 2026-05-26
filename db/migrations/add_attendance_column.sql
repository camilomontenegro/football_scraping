-- MIGRATION: add attendance column to dim_match.
-- Idempotent — safe to re-run.

ALTER TABLE dim_match ADD COLUMN IF NOT EXISTS attendance INTEGER;
