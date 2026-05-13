
-- Consultas frecuentes para análisis y verificación de  datos cargados de una competición
-- Permite  fiscalizar  el proceso de carga  de datos 

--------------------------------------------------------------------------------------------
-- JUGADORES  DE UNA COMPETICION 
--obtiene los jugadores de una competicion concreta (en este caso, Premier League) que han tenido eventos o tiros registrados en la base de datos
--No hay forma de saber la competicion del juegador sin utilizar los tiros o eventos 

SELECT DISTINCT dp.canonical_name, dp.position, dp.nationality
FROM dim_player dp
WHERE dp.canonical_id IN (
    SELECT player_id FROM fact_events fe
    JOIN dim_match dm ON fe.match_id = dm.match_id
    WHERE dm.competition = 'Premier League'
    UNION
    SELECT player_id FROM fact_shots fs
    JOIN dim_match dm ON fs.match_id = dm.match_id
    WHERE dm.competition = 'Premier League'
)
ORDER BY canonical_name;

-- Comprobar el numero de jugadores insertados en un dia 
-- Ten en cuenta que habra que registros que no se cuenten al insertar los jugadores de uan liga porque ya existian en la base de datos ( se cargaron en la champiomns por ejemplo) y solo se actualizan 
select count(*) from dim_player
where created_at ::date =CURRENT_DATE


--------------------------------------------------------------------------------------------
-- TOTAL JUGADORES DE UAN COMPETICION  - CON Y SIN ID EN CADA FUENTE
-- Obtiene el número total de jugadores   de una competicion concreta
-- Muestra cuantos jugadores tienen ID en cada fuente (sofascore, whoscored, understat, transfermarkt) y cuantos no lo tienen

SELECT 
    COUNT(*) as total_jugadores,
    COUNT(id_sofascore)      as con_sofascore,
    COUNT(*) - COUNT(id_sofascore)      as sin_sofascore,
    COUNT(id_whoscored)      as con_whoscored,
    COUNT(*) - COUNT(id_whoscored)      as sin_whoscored,
    COUNT(id_understat)      as con_understat,
    COUNT(*) - COUNT(id_understat)      as sin_understat,
    COUNT(id_transfermarkt)  as con_transfermarkt,
    COUNT(*) - COUNT(id_transfermarkt)  as sin_transfermarkt
FROM dim_player dp
WHERE dp.canonical_id IN (
    SELECT player_id FROM fact_events fe
    JOIN dim_match dm ON fe.match_id = dm.match_id
    WHERE dm.competition = 'Premier League'
    UNION
    SELECT player_id FROM fact_shots fs
    JOIN dim_match dm ON fs.match_id = dm.match_id
    WHERE dm.competition = 'Premier League'
);


--------------------------------------------------------------------------------------------
-- TOTAL EQUIPOS DE UNA COMPETICION - CON Y SIN ID EN CADA FUENTE
-- Comprueba cuántos equipos tienen cada ID de fuente enlazado
-- COUNT(columna) ignora los NULL, por lo que la diferencia con COUNT(*) da los NULL
SELECT 
    COUNT(*) as total_equipos,
    COUNT(id_sofascore)     as con_sofascore,
    COUNT(*) - COUNT(id_sofascore)     as sin_sofascore,
    COUNT(id_whoscored)     as con_whoscored,
    COUNT(*) - COUNT(id_whoscored)     as sin_whoscored,
    COUNT(id_understat)     as con_understat,
    COUNT(*) - COUNT(id_understat)     as sin_understat,
    COUNT(id_transfermarkt) as con_transfermarkt,
    COUNT(*) - COUNT(id_transfermarkt) as sin_transfermarkt
FROM dim_team
WHERE country = 'Inglaterra';



--------------------------------------------------------------------------------------------
-- PARTIDOS DE UNA COMPETICION - CON Y SIN ID EN CADA FUENTE
-- Comprueba cuántos partidos de la competicion  tienen cada ID de fuente enlazado
-- COUNT(columna) ignora los NULL, por lo que la diferencia con COUNT(*) da los NULL
SELECT 
    COUNT(*) as total_partidos,
    COUNT(id_sofascore)     as con_sofascore,
    COUNT(*) - COUNT(id_sofascore)     as sin_sofascore,
    COUNT(id_whoscored)     as con_whoscored,
    COUNT(*) - COUNT(id_whoscored)     as sin_whoscored,
    COUNT(id_understat)     as con_understat,
    COUNT(*) - COUNT(id_understat)     as sin_understat,
    COUNT(id_statsbomb)     as con_statsbomb,
    COUNT(*) - COUNT(id_statsbomb)     as sin_statsbomb
FROM dim_match
WHERE competition = 'Premier League';

-- puedes buscar tb por comepticion_id
SELECT 
    COUNT(*) as total_partidos,
    COUNT(id_sofascore)     as con_sofascore,
    COUNT(*) - COUNT(id_sofascore)     as sin_sofascore,
    COUNT(id_whoscored)     as con_whoscored,
    COUNT(*) - COUNT(id_whoscored)     as sin_whoscored,
    COUNT(id_understat)     as con_understat,
    COUNT(*) - COUNT(id_understat)     as sin_understat,
    COUNT(id_statsbomb)     as con_statsbomb,
    COUNT(*) - COUNT(id_statsbomb)     as sin_statsbomb
FROM dim_match
WHERE competition_id = ;



--------------------------------------------------------------------------------------------
-- SHOTS 
-- Conteo de tiros por fuente
SELECT 
    fs.data_source,
    COUNT(*) as total_tiros
FROM fact_shots fs
JOIN dim_match dm ON fs.match_id = dm.match_id
WHERE dm.competition_id = 
GROUP BY fs.data_source;


-- Verificación de coordenadas por fuente
SELECT
    fs.data_source,
    ROUND(MIN(x)::numeric, 4) as min_x,
    ROUND(MAX(x)::numeric, 4) as max_x,
    ROUND(MIN(y)::numeric, 4) as min_y,
    ROUND(MAX(y)::numeric, 4) as max_y
FROM fact_shots fs
JOIN dim_match dm ON fs.match_id = dm.match_id
WHERE dm.competition_id = 
GROUP BY fs.data_source;

-- NO ESTAN NORMALIZADAS LAS DE SOFASCORE 
--data_source|min_x |max_x  |min_y |max_y  |
-----------+------+-------+------+-------+
--sofascore  |0.3000|70.3000|0.5000|98.7000|
--understat  |0.0040| 0.9970|0.0080| 0.9950|

Mostrar menos

--------------------------------------------------------------------------------------------
--EVENTS 
-- Conteo de eventos por fuente
SELECT 
    fe.data_source,
    COUNT(*) as total_eventos
FROM fact_events fe
JOIN dim_match dm ON fe.match_id = dm.match_id
WHERE dm.competition_id = 
GROUP BY fe.data_source;

-- Verificación de coordenadas por fuente
-- Verificación de coordenadas por fuente
SELECT
    fe.data_source,
    ROUND(MIN(x)::numeric, 4) as min_x,
    ROUND(MAX(x)::numeric, 4) as max_x,
    ROUND(MIN(y)::numeric, 4) as min_y,
    ROUND(MAX(y)::numeric, 4) as max_y
FROM fact_events fe
JOIN dim_match dm ON fe.match_id = dm.match_id
WHERE dm.competition_id =
GROUP BY fe.data_source;

--data_source|min_x |max_x |min_y |max_y |
-----------+------+------+------+------+
--sofascore  |      |      |      |      |
--whoscored  |0.0000|1.0000|0.0000|1.0000|
--WhoScored está normalizado correctamente — coordenadas entre 0 y 1.
--SofaScore muestra NULL en todas las coordenadas — significa que los eventos de SofaScore no tienen coordenadas x e y


-- Tipos de eventos más frecuentes por fuente
SELECT 
    fe.data_source,
    fe.event_type,
    COUNT(*) as total
FROM fact_events fe
JOIN dim_match dm ON fe.match_id = dm.match_id
WHERE dm.competition_id = 
GROUP BY fe.data_source, event_type
ORDER BY fe.data_source, total DESC
LIMIT 20;




--------------------------------------------------------------------------------------------
-- PLAYER REVIEW 
-- La mayoría de los jugadores en player_review aparecen una sola vez — simplemente son jugadores que el sistema no pudo enlazar automáticamente por diferencias de nombre entre fuentes. No son duplicados, son jugadores pendientes de resolución manual.


--hay que convertir a date porque el campo created_at es timestamp
select count(*) from player_review
where created_at ::date =CURRENT_DATE;

-- Jugadores duplicados que apuntan al mismo canonical_id (mismo jugador, IDs distintos)
-- jugadores que tienen distintos ids en la fuente, pero  apuntan al mismo suggested_canonical_id
--a veces el mismo jugador tiene dos perfiles en la misma fuente  
SELECT source_name, source_system, source_id, suggested_canonical_id, similarity_score
FROM player_review
WHERE suggested_canonical_id IS NOT NULL
AND source_name IN (
    SELECT source_name 
    FROM player_review 
    WHERE suggested_canonical_id IS NOT NULL
    GROUP BY source_name, source_system, suggested_canonical_id
    HAVING COUNT(*) > 1
)
ORDER BY source_name, source_system;

-- Jugadores duplicados sin canonical_id sugerido (posiblemente personas distintas)
SELECT source_name, source_system, COUNT(*) as veces
FROM player_review
WHERE suggested_canonical_id IS NULL
GROUP BY source_name, source_system
HAVING COUNT(*) > 1
ORDER BY veces DESC;