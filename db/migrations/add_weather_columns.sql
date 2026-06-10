-- MIGRATION: add weather columns to dim_match (Open-Meteo enrichment).
-- Idempotent — safe to re-run.

ALTER TABLE dim_match ADD COLUMN IF NOT EXISTS temperature_c    DECIMAL(4,1);
ALTER TABLE dim_match ADD COLUMN IF NOT EXISTS humidity_pct     SMALLINT;
ALTER TABLE dim_match ADD COLUMN IF NOT EXISTS precipitation_mm DECIMAL(5,1);
ALTER TABLE dim_match ADD COLUMN IF NOT EXISTS wind_speed_kmh   DECIMAL(5,1);
ALTER TABLE dim_match ADD COLUMN IF NOT EXISTS weather_code     SMALLINT;
