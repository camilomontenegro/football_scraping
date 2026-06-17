-- Estadio real del partido (independiente del equipo local).
-- Poblado desde JSON SofaScore (data/raw/attendance) y CSV WhoScored (match_enrichment).
--
--   python -m scripts.backfill_stadium_match --force
--   python -m scripts.backfill_stadium_match --data-root "C:/Users/Ivan/Desktop/football_scraping_backup"

ALTER TABLE dim_match
    ADD COLUMN IF NOT EXISTS match_stadium_id INTEGER
        REFERENCES dim_stadium (stadium_id) ON DELETE SET NULL;

ALTER TABLE dim_match
    ADD COLUMN IF NOT EXISTS match_venue_source VARCHAR(32);

CREATE INDEX IF NOT EXISTS idx_match_match_stadium
    ON dim_match (match_stadium_id);

COMMENT ON COLUMN dim_match.match_stadium_id IS
    'Estadio donde se jugó el partido (venue SS/WS), no el del equipo local.';
COMMENT ON COLUMN dim_match.stadium_id IS
    'Legacy: enlace por equipo local; preferir match_stadium_id.';
COMMENT ON COLUMN dim_match.match_venue_source IS
    'sofascore_json | whoscored_csv | venue_name | home_fallback';
