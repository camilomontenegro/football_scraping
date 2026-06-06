-- Elimina player_scrape_provenance si existía en BDs antiguas del repo.
-- Uso: psql -d football_db -f db/migrations/002_drop_player_scrape_provenance.sql

DROP TABLE IF EXISTS player_scrape_provenance CASCADE;
