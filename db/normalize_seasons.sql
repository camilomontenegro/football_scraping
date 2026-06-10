-- ══════════════════════════════════════════════════════════
-- normalize_seasons.sql
-- ══════════════════════════════════════════════════════════
--
-- Script idempotente que:
--   1. Borra los partidos residuales con data_source='understat'
--      (Understat NO es fuente principal de dim_match, solo enriquece).
--   2. Normaliza el campo `season` al formato canónico 'YYYY/YYYY'
--      en TODOS los registros existentes de dim_match.
--
-- Tras ejecutar este script, todos los formatos heterogéneos
-- ("20/21", "LaLiga 20/21", "Bundesliga 25/26", "2025") quedan
-- unificados a "2020/2021", "2025/2026", etc.
--
-- Aplica también la misma normalización en fact_injuries.season.
--
-- USO:
--   psql -U postgres -d football_db -f db/normalize_seasons.sql
-- O bien:
--   python -m scripts.normalize_db_seasons   (desde Python)
-- ══════════════════════════════════════════════════════════


-- ── 1. Limpieza: partidos residuales de Understat ─────────
-- Estos partidos solo existen porque Understat los insertó
-- como fallback al no encontrar match en dim_match. La fuente
-- canónica son SofaScore o WhoScored. Borramos events/shots
-- asociados primero por las FKs.

-- (Selecciónalos antes para confirmar si quieres):
-- SELECT match_id, competition, season, match_date
-- FROM dim_match
-- WHERE data_source = 'understat';

DELETE FROM fact_events
 WHERE match_id IN (SELECT match_id FROM dim_match WHERE data_source = 'understat');

DELETE FROM fact_shots
 WHERE match_id IN (SELECT match_id FROM dim_match WHERE data_source = 'understat');

DELETE FROM dim_match
 WHERE data_source = 'understat';


-- ── 2. Estado actual antes de normalizar (informativo) ────
-- SELECT DISTINCT season, COUNT(*) AS n
-- FROM dim_match
-- GROUP BY season ORDER BY season;


-- ── 3. Normalización a 'YYYY/YYYY' ────────────────────────
-- Cada UPDATE cubre las variantes que las distintas fuentes
-- guardaban (sin prefijo de competición, con prefijo, formato
-- corto, sólo año, etc.).

UPDATE dim_match SET season = '2020/2021'
 WHERE season IN ('20/21', 'LaLiga 20/21', 'Bundesliga 20/21',
                  'Premier League 20/21', 'Serie A 20/21',
                  'Ligue 1 20/21', '2020', '2020/21');

UPDATE dim_match SET season = '2021/2022'
 WHERE season IN ('21/22', 'LaLiga 21/22', 'Bundesliga 21/22',
                  'Premier League 21/22', 'Serie A 21/22',
                  'Ligue 1 21/22', '2021', '2021/22');

UPDATE dim_match SET season = '2022/2023'
 WHERE season IN ('22/23', 'LaLiga 22/23', 'Bundesliga 22/23',
                  'Premier League 22/23', 'Serie A 22/23',
                  'Ligue 1 22/23', '2022', '2022/23');

UPDATE dim_match SET season = '2023/2024'
 WHERE season IN ('23/24', 'LaLiga 23/24', 'Bundesliga 23/24',
                  'Premier League 23/24', 'Serie A 23/24',
                  'Ligue 1 23/24', '2023', '2023/24');

UPDATE dim_match SET season = '2024/2025'
 WHERE season IN ('24/25', 'LaLiga 24/25', 'Bundesliga 24/25',
                  'Premier League 24/25', 'Serie A 24/25',
                  'Ligue 1 24/25', '2024', '2024/25');

UPDATE dim_match SET season = '2025/2026'
 WHERE season IN ('25/26', 'LaLiga 25/26', 'Bundesliga 25/26',
                  'Premier League 25/26', 'Serie A 25/26',
                  'Ligue 1 25/26', '2025', '2025/26');

UPDATE dim_match SET season = '2026/2027'
 WHERE season IN ('26/27', 'LaLiga 26/27', '2026', '2026/27');


-- ── 4. fact_injuries.season (Transfermarkt usa 'YY/YY') ───
UPDATE fact_injuries SET season = '2020/2021' WHERE season IN ('20/21', '2020/21', '2020');
UPDATE fact_injuries SET season = '2021/2022' WHERE season IN ('21/22', '2021/22', '2021');
UPDATE fact_injuries SET season = '2022/2023' WHERE season IN ('22/23', '2022/23', '2022');
UPDATE fact_injuries SET season = '2023/2024' WHERE season IN ('23/24', '2023/24', '2023');
UPDATE fact_injuries SET season = '2024/2025' WHERE season IN ('24/25', '2024/25', '2024');
UPDATE fact_injuries SET season = '2025/2026' WHERE season IN ('25/26', '2025/26', '2025');


-- ── 5. Resultado final ────────────────────────────────────
-- Tras el script deberías ver una única fila por temporada:
-- SELECT DISTINCT season, COUNT(*) AS n
-- FROM dim_match
-- GROUP BY season ORDER BY season;
