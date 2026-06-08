-- ═══════════════════════════════════════════════════════════════════════
-- Muestra de datos extraídos del match centre de WhoScored
-- Ejecutar: psql -U postgres -d football_db -f reports/match_centre_sample.sql
-- ═══════════════════════════════════════════════════════════════════════

\echo '=== 1. RESUMEN GLOBAL ==='
SELECT
    (SELECT COUNT(*) FROM fact_player_match_stats WHERE data_source = 'whoscored')
        AS filas_stats_jugador,
    (SELECT COUNT(DISTINCT match_id) FROM fact_player_match_stats WHERE data_source = 'whoscored')
        AS partidos_con_stats,
    (SELECT COUNT(*) FROM fact_formations WHERE data_source = 'whoscored')
        AS filas_formaciones,
    (SELECT COUNT(DISTINCT match_id) FROM fact_formations WHERE data_source = 'whoscored')
        AS partidos_con_formaciones,
    (SELECT COUNT(*) FROM dim_referee WHERE id_whoscored IS NOT NULL)
        AS arbitros_whoscored,
    (SELECT COUNT(*) FROM dim_match WHERE venue_name IS NOT NULL)
        AS partidos_con_estadio,
    (SELECT COUNT(*) FROM dim_match WHERE manager_home IS NOT NULL OR manager_away IS NOT NULL)
        AS partidos_con_entrenadores,
    (SELECT COUNT(*) FROM dim_match WHERE ht_score IS NOT NULL OR ft_score IS NOT NULL)
        AS partidos_con_marcador_ws,
    (SELECT COUNT(*) FROM dim_match WHERE referee_id IS NOT NULL)
        AS partidos_con_arbitro;

\echo ''
\echo '=== 2. COBERTURA POR COMPETICIÓN / TEMPORADA ==='
SELECT
    m.competition,
    m.season,
    COUNT(DISTINCT m.match_id) AS partidos_whoscored,
    COUNT(DISTINCT CASE WHEN m.venue_name IS NOT NULL THEN m.match_id END) AS con_estadio,
    COUNT(DISTINCT CASE WHEN m.manager_home IS NOT NULL THEN m.match_id END) AS con_entrenadores,
    COUNT(DISTINCT CASE WHEN m.referee_id IS NOT NULL THEN m.match_id END) AS con_arbitro,
    COUNT(DISTINCT p.match_id) AS con_stats_jugador,
    COUNT(DISTINCT f.match_id) AS con_formaciones
FROM dim_match m
LEFT JOIN fact_player_match_stats p
    ON p.match_id = m.match_id AND p.data_source = 'whoscored'
LEFT JOIN fact_formations f
    ON f.match_id = m.match_id AND f.data_source = 'whoscored'
WHERE m.id_whoscored IS NOT NULL
GROUP BY m.competition, m.season
ORDER BY m.competition, m.season DESC;

\echo ''
\echo '=== 3. MUESTRA DE PARTIDOS ENRIQUECIDOS (últimos 5) ==='
SELECT
    m.match_id,
    m.id_whoscored AS ws_match_id,
    m.competition,
    m.season,
    m.match_date,
    ht.canonical_name AS equipo_local,
    at.canonical_name AS equipo_visitante,
    m.home_score,
    m.away_score,
    m.ht_score AS marcador_descanso,
    m.ft_score AS marcador_final_ws,
    m.venue_name AS estadio,
    m.manager_home AS entrenador_local,
    m.manager_away AS entrenador_visitante,
    m.attendance AS asistencia,
    r.canonical_name AS arbitro
FROM dim_match m
JOIN dim_team ht ON ht.canonical_id = m.home_team_id
JOIN dim_team at ON at.canonical_id = m.away_team_id
LEFT JOIN dim_referee r ON r.referee_id = m.referee_id
WHERE m.venue_name IS NOT NULL
  AND EXISTS (
      SELECT 1 FROM fact_player_match_stats p
      WHERE p.match_id = m.match_id AND p.data_source = 'whoscored'
  )
ORDER BY m.match_date DESC NULLS LAST
LIMIT 5;

\echo ''
\echo '=== 4. DETALLE UN PARTIDO: contexto + formaciones + top jugadores ==='
-- Cambia match_id si quieres otro partido
WITH sample AS (
    SELECT m.match_id
    FROM dim_match m
    WHERE m.venue_name IS NOT NULL
      AND EXISTS (
          SELECT 1 FROM fact_player_match_stats p
          WHERE p.match_id = m.match_id AND p.data_source = 'whoscored'
      )
    ORDER BY m.match_date DESC NULLS LAST
    LIMIT 1
)
SELECT '--- PARTIDO ---' AS seccion, m.match_id::text, m.competition, m.season::text,
       ht.canonical_name || ' ' || COALESCE(m.home_score::text,'') || '-' ||
       COALESCE(m.away_score::text,'') || ' ' || at.canonical_name AS detalle,
       m.venue_name, m.manager_home, m.manager_away, r.canonical_name AS arbitro
FROM dim_match m
JOIN sample s ON s.match_id = m.match_id
JOIN dim_team ht ON ht.canonical_id = m.home_team_id
JOIN dim_team at ON at.canonical_id = m.away_team_id
LEFT JOIN dim_referee r ON r.referee_id = m.referee_id;

\echo ''
\echo '--- FORMACIONES (mismo partido) ---'
WITH sample AS (
    SELECT m.match_id
    FROM dim_match m
    WHERE m.venue_name IS NOT NULL
      AND EXISTS (
          SELECT 1 FROM fact_player_match_stats p
          WHERE p.match_id = m.match_id AND p.data_source = 'whoscored'
      )
    ORDER BY m.match_date DESC NULLS LAST
    LIMIT 1
)
SELECT ff.side AS bando, t.canonical_name AS equipo, ff.formation_name AS formacion,
       ff.start_minute, ff.end_minute, p.canonical_name AS capitan
FROM fact_formations ff
JOIN sample s ON s.match_id = ff.match_id
JOIN dim_team t ON t.canonical_id = ff.team_id
LEFT JOIN dim_player p ON p.canonical_id = ff.captain_player_id
WHERE ff.data_source = 'whoscored'
ORDER BY ff.side, ff.start_minute;

\echo ''
\echo '--- STATS JUGADORES top 15 por nota (mismo partido) ---'
WITH sample AS (
    SELECT m.match_id
    FROM dim_match m
    WHERE m.venue_name IS NOT NULL
      AND EXISTS (
          SELECT 1 FROM fact_player_match_stats p
          WHERE p.match_id = m.match_id AND p.data_source = 'whoscored'
      )
    ORDER BY m.match_date DESC NULLS LAST
    LIMIT 1
)
SELECT p.canonical_name AS jugador, t.canonical_name AS equipo,
       fpms.position, fpms.is_starter AS titular, fpms.rating AS nota,
       fpms.touches AS toques, fpms.passes_total AS pases,
       fpms.passes_accurate AS pases_ok, fpms.shots_total AS tiros,
       fpms.shots_on_target AS a_puerta, fpms.tackles_total AS entradas,
       fpms.interceptions, fpms.is_man_of_the_match AS mvp
FROM fact_player_match_stats fpms
JOIN sample s ON s.match_id = fpms.match_id
JOIN dim_player p ON p.canonical_id = fpms.player_id
JOIN dim_team t ON t.canonical_id = fpms.team_id
WHERE fpms.data_source = 'whoscored'
ORDER BY fpms.rating DESC NULLS LAST
LIMIT 15;

\echo ''
\echo '=== 5. TOP ÁRBITROS (WhoScored) ==='
SELECT r.canonical_name, r.id_whoscored AS ws_id,
       COUNT(m.match_id) AS partidos
FROM dim_referee r
LEFT JOIN dim_match m ON m.referee_id = r.referee_id
WHERE r.id_whoscored IS NOT NULL
GROUP BY r.referee_id, r.canonical_name, r.id_whoscored
ORDER BY partidos DESC
LIMIT 15;
