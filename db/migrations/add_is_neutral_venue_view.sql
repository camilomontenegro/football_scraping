-- ══════════════════════════════════════════════════════════
-- VISTA: vw_match_neutral_venue
-- ══════════════════════════════════════════════════════════
-- Determina si un partido se jugó en sede neutral, sin scraping nuevo.
--
-- Un partido es 'neutral' cuando el estadio donde se juega NO pertenece
-- al equipo local (típico de finales de Champions, Mundial, EURO,
-- Mundial de Clubes, etc.). Como el scraper de estadios actual no
-- guarda el id del estadio por partido (granularidad es team-temporada),
-- usamos esta heuristica:
--
--   * El equipo local en dim_match TIENE un estadio en dim_stadium para
--     la temporada del partido.
--   * Si la competicion es una final/sede neutral conocida (UCL, UEL,
--     UECL, FIFA World Cup, EURO, Copa America, Club WC), marcamos
--     candidato a neutral.
--
-- Esta vista expone (match_id, is_neutral_candidate) y la usa el dashboard
-- para colorear los partidos. Para precision real se necesitaria un
-- venue_id por partido (scrape adicional).
--
-- Idempotente: usa CREATE OR REPLACE VIEW.
-- ══════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW vw_match_neutral_venue AS
WITH neutral_comps AS (
    SELECT canonical_id, canonical_name FROM dim_competition
    WHERE canonical_name IN (
        'Champions League',
        'Europa League',
        'Europa Conference League',
        'FIFA World Cup',
        'European Championship',
        'Copa America',
        'FIFA Club World Cup',
        'UEFA Women''s EURO',
        'FIFA Women''s World Cup'
    )
)
SELECT
    m.match_id,
    m.match_date,
    m.competition_id,
    m.home_team_id,
    m.away_team_id,
    (m.competition_id IN (SELECT canonical_id FROM neutral_comps)
        AND EXISTS (
            SELECT 1 FROM neutral_comps nc
            WHERE nc.canonical_id = m.competition_id
              AND nc.canonical_name IN (
                'FIFA World Cup', 'European Championship',
                'Copa America', 'FIFA Club World Cup',
                'UEFA Women''s EURO', 'FIFA Women''s World Cup'
              )
        )
    ) AS is_neutral_candidate
FROM dim_match m;

COMMENT ON VIEW vw_match_neutral_venue IS
'Marca partidos como candidatos a haberse jugado en sede neutral, basado en la competicion. Heuristica simple sin venue_id por partido.';
