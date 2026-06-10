-- ═══════════════════════════════════════════════════════════════════════
-- drop_statsbomb.sql
-- ---------------------------------------------------------------------
-- Elimina las columnas id_statsbomb de todas las tablas.
-- StatsBomb no ofrece datos gratuitos para ligas comerciales.
--
-- Idempotente (IF EXISTS).
--
-- Uso:
--   psql -U postgres -d football_db -f db/drop_statsbomb.sql
-- ═══════════════════════════════════════════════════════════════════════

-- Eliminar índices únicos primero
DROP INDEX IF EXISTS ux_team_statsbomb;
DROP INDEX IF EXISTS ux_player_statsbomb;
DROP INDEX IF EXISTS ux_match_statsbomb;

-- Eliminar columnas
ALTER TABLE dim_team        DROP COLUMN IF EXISTS id_statsbomb;
ALTER TABLE dim_player      DROP COLUMN IF EXISTS id_statsbomb;
ALTER TABLE dim_match       DROP COLUMN IF EXISTS id_statsbomb;
ALTER TABLE dim_competition DROP COLUMN IF EXISTS id_statsbomb;
