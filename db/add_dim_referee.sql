-- ═══════════════════════════════════════════════════════════════════════
-- add_dim_referee.sql
-- ---------------------------------------------------------------------
-- Crea dim_referee (un árbitro = una fila) y enlaza dim_match via FK.
-- Idempotente. Fuente: Sofascore /api/v1/event/{id} campo "referee".
--
-- Uso:
--   psql -U postgres -d football_db -f db/add_dim_referee.sql
-- ═══════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS dim_referee (
    referee_id      SERIAL PRIMARY KEY,
    canonical_name  VARCHAR(150) NOT NULL,
    country         VARCHAR(80),
    id_sofascore    INTEGER UNIQUE,
    -- otros IDs que podamos enganchar a futuro
    id_whoscored    INTEGER,
    id_transfermarkt INTEGER,
    data_source     VARCHAR(50) DEFAULT 'sofascore',
    created_at      TIMESTAMP DEFAULT NOW(),
    updated_at      TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_referee_name_lower
    ON dim_referee (LOWER(canonical_name));

-- FK desde dim_match
ALTER TABLE dim_match
    ADD COLUMN IF NOT EXISTS referee_id INTEGER
        REFERENCES dim_referee (referee_id) ON DELETE SET NULL;

CREATE INDEX IF NOT EXISTS idx_match_referee
    ON dim_match (referee_id) WHERE referee_id IS NOT NULL;
