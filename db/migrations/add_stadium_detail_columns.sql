-- ══════════════════════════════════════════════════════════
-- MIGRACION: columnas de detalle de estadio (Transfermarkt loader)
-- ══════════════════════════════════════════════════════════
-- Alinea dim_stadium con loaders/stadium_loader.py y el scraper TM.
-- Idempotente: ADD COLUMN IF NOT EXISTS.
-- ══════════════════════════════════════════════════════════

ALTER TABLE dim_stadium ADD COLUMN IF NOT EXISTS seats_covered     INTEGER;
ALTER TABLE dim_stadium ADD COLUMN IF NOT EXISTS seats_vip         INTEGER;
ALTER TABLE dim_stadium ADD COLUMN IF NOT EXISTS vip_boxes         SMALLINT;
ALTER TABLE dim_stadium ADD COLUMN IF NOT EXISTS seats_standing    INTEGER;
ALTER TABLE dim_stadium ADD COLUMN IF NOT EXISTS inaugurated_year  SMALLINT;
ALTER TABLE dim_stadium ADD COLUMN IF NOT EXISTS refurbished_year  SMALLINT;
ALTER TABLE dim_stadium ADD COLUMN IF NOT EXISTS construction_cost VARCHAR(120);

-- Usado por wikidata_stadium_enricher / scripts de auditoria
ALTER TABLE dim_stadium ADD COLUMN IF NOT EXISTS is_current        BOOLEAN DEFAULT TRUE;
