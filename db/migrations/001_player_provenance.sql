-- Migración incremental: trazabilidad de jugadores scrapeados.
-- Ejecutar sobre una BD existente sin recrear todo el schema:
--   psql -d football_db -f db/migrations/001_player_provenance.sql

ALTER TABLE player_review
    ADD COLUMN IF NOT EXISTS competition VARCHAR(100);

ALTER TABLE player_review
    ADD COLUMN IF NOT EXISTS season VARCHAR(20);

CREATE TABLE IF NOT EXISTS player_scrape_provenance (
    id               SERIAL PRIMARY KEY,
    source_system    VARCHAR(50)  NOT NULL,
    source_player_id VARCHAR(50)  NOT NULL,
    scraped_name     VARCHAR(150) NOT NULL,
    competition      VARCHAR(100) NOT NULL DEFAULT '',
    season           VARCHAR(20)  NOT NULL DEFAULT '',
    team_name        VARCHAR(150),
    team_id          VARCHAR(50)  NOT NULL DEFAULT '',
    canonical_id     INTEGER REFERENCES dim_player (canonical_id),
    scraped_at       TIMESTAMP DEFAULT NOW(),
    UNIQUE (source_system, source_player_id, competition, season, team_id)
);

CREATE INDEX IF NOT EXISTS idx_player_provenance_canonical
    ON player_scrape_provenance (canonical_id);

CREATE INDEX IF NOT EXISTS idx_player_provenance_name
    ON player_scrape_provenance (LOWER(scraped_name));
