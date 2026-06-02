-- ══════════════════════════════════════════════════════════
-- MIGRACION: anade columnas Wikidata a dim_stadium
-- ══════════════════════════════════════════════════════════
-- Suma 7 columnas no destructivas para enriquecimiento via Wikidata:
--   * latitude / longitude (DECIMAL): coordenadas (P625).
--   * altitude_m (INTEGER): elevacion sobre el nivel del mar.
--   * timezone (VARCHAR): zona IANA derivada de lat/lon (timezonefinder).
--   * wikidata_qid (VARCHAR): Q-ID del estadio en Wikidata.
--   * wikipedia_url (VARCHAR): URL del articulo en Wikipedia.
--   * image_url (VARCHAR): foto principal del estadio (Wikimedia Commons).
--
-- Idempotente: usa ADD COLUMN IF NOT EXISTS.
-- ══════════════════════════════════════════════════════════

ALTER TABLE dim_stadium ADD COLUMN IF NOT EXISTS latitude      DECIMAL(9,6);
ALTER TABLE dim_stadium ADD COLUMN IF NOT EXISTS longitude     DECIMAL(9,6);
ALTER TABLE dim_stadium ADD COLUMN IF NOT EXISTS altitude_m    INTEGER;
ALTER TABLE dim_stadium ADD COLUMN IF NOT EXISTS timezone      VARCHAR(64);
ALTER TABLE dim_stadium ADD COLUMN IF NOT EXISTS roof_type     VARCHAR(20);
ALTER TABLE dim_stadium ADD COLUMN IF NOT EXISTS wikidata_qid  VARCHAR(20);
ALTER TABLE dim_stadium ADD COLUMN IF NOT EXISTS wikipedia_url VARCHAR(500);
ALTER TABLE dim_stadium ADD COLUMN IF NOT EXISTS image_url     VARCHAR(500);

CREATE INDEX IF NOT EXISTS idx_stadium_wikidata_qid
    ON dim_stadium (wikidata_qid)
    WHERE wikidata_qid IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_stadium_latlon
    ON dim_stadium (latitude, longitude)
    WHERE latitude IS NOT NULL AND longitude IS NOT NULL;
