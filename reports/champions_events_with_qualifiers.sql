-- Eventos de los 3 últimos partidos de Champions CON qualifiers (JSONB)
WITH ultimos_tres AS (
    SELECT match_id
    FROM dim_match
    WHERE competition ILIKE '%champions%'
    ORDER BY match_date DESC NULLS LAST, match_id DESC
    LIMIT 3
)
SELECT
    m.match_date,
    ht.canonical_name || ' vs ' || at.canonical_name AS partido,
    p.canonical_name   AS jugador,
    e.event_type,
    e.minute,
    e.second,
    e.x, e.y, e.end_x, e.end_y,
    e.outcome,
    e.qualifiers,
    e.qualifiers->>'Length'  AS longitud_m,
    e.qualifiers->>'Zone'    AS zona,
    e.qualifiers->>'Angle'   AS angulo,
    (e.qualifiers ? 'KeyPass') AS es_pase_clave,
    (e.qualifiers ? 'Cross')   AS es_centro
FROM ultimos_tres u
JOIN dim_match m   ON m.match_id = u.match_id
JOIN dim_team ht   ON ht.canonical_id = m.home_team_id
JOIN dim_team at   ON at.canonical_id = m.away_team_id
JOIN fact_events e ON e.match_id = m.match_id
JOIN dim_player p  ON p.canonical_id = e.player_id
WHERE e.data_source = 'whoscored'
ORDER BY m.match_date DESC, e.minute, COALESCE(e.second, -1);
