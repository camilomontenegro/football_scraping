-- ══════════════════════════════════════════════════════════
-- Migración: vincular dim_match con dim_stadium
-- ══════════════════════════════════════════════════════════
-- Añade stadium_id FK a dim_match para saber en qué estadio
-- se jugó cada partido.
--
-- Solo crea la columna. Para poblarla con lógica multinivel
-- (SofaScore venue → WhoScored venue → home team fallback):
--   python -m scripts.backfill_stadium_match
--
-- Uso de esta migración:
--   psql -d football -f db/migrations/add_stadium_to_match.sql
-- ══════════════════════════════════════════════════════════

ALTER TABLE dim_match
    ADD COLUMN IF NOT EXISTS stadium_id INTEGER
    REFERENCES dim_stadium (stadium_id) ON DELETE SET NULL;

CREATE INDEX IF NOT EXISTS idx_match_stadium ON dim_match (stadium_id);
