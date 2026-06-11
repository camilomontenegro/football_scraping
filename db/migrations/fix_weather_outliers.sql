-- ═══════════════════════════════════════════════════════════════════════
-- fix_weather_outliers.sql
-- ---------------------------------------------------------------------
-- NULL out weather values that are physically impossible.
-- NASA POWER sometimes returns garbage for locations with poor coverage
-- or for future/very recent dates.
--
-- Reasonable bounds:
--   temperature_c:    -60 to +60  (record extremes: -89.2 / +56.7)
--   humidity_pct:       0 to 100
--   wind_speed_kmh:     0 to 300  (strongest on record: ~407 km/h)
--   precipitation_mm:   0 to 500
--
-- Usage:
--   psql -U postgres -d football_db -f db/migrations/fix_weather_outliers.sql
-- ═══════════════════════════════════════════════════════════════════════

-- Show how many bad rows exist before fixing
SELECT 'Bad temperature rows' AS metric,
       COUNT(*) AS count
FROM dim_match
WHERE temperature_c IS NOT NULL
  AND (temperature_c < -60 OR temperature_c > 60);

-- NULL out bad temperature (and all related weather columns, since they
-- likely came from the same bad API response)
UPDATE dim_match
SET temperature_c    = NULL,
    humidity_pct     = NULL,
    precipitation_mm = NULL,
    wind_speed_kmh   = NULL,
    weather_code     = NULL
WHERE temperature_c IS NOT NULL
  AND (temperature_c < -60 OR temperature_c > 60);

-- Also fix isolated bad humidity/wind/precip on otherwise valid rows
UPDATE dim_match
SET humidity_pct = NULL
WHERE humidity_pct IS NOT NULL
  AND (humidity_pct < 0 OR humidity_pct > 100);

UPDATE dim_match
SET wind_speed_kmh = NULL
WHERE wind_speed_kmh IS NOT NULL
  AND (wind_speed_kmh < 0 OR wind_speed_kmh > 300);

UPDATE dim_match
SET precipitation_mm = NULL
WHERE precipitation_mm IS NOT NULL
  AND (precipitation_mm < 0 OR precipitation_mm > 500);

-- Summary after fix
SELECT 'Remaining bad temps' AS metric,
       COUNT(*) AS count
FROM dim_match
WHERE temperature_c IS NOT NULL
  AND (temperature_c < -60 OR temperature_c > 60);
