
-- Comprueba si hay eventos duplicados 
SELECT match_id, player_id, event_type, minute, 
       COALESCE(second, -1), COALESCE(x, -1.0), COALESCE(y, -1.0), 
       data_source, COUNT(*)
FROM fact_events
GROUP BY match_id, player_id, event_type, minute, 
         COALESCE(second, -1), COALESCE(x, -1.0), COALESCE(y, -1.0), 
         data_source
HAVING COUNT(*) > 1;

-- solo ejecutar si hay eventos duplicados 
DELETE FROM fact_events
WHERE event_id NOT IN (
    SELECT MIN(event_id)
    FROM fact_events
    GROUP BY match_id, player_id, event_type, minute, 
             COALESCE(second, -1), 
             COALESCE(x, -1.0), 
             COALESCE(y, -1.0), 
             data_source
);

-- si hay eventos dudplciados la eliminacion y creacion del UNIQUE INDEZ FALLA 
DROP INDEX ux_events_unique;

CREATE UNIQUE INDEX ux_events_unique 
ON fact_events (match_id, player_id, event_type, minute, 
                COALESCE(second, -1), 
                COALESCE(x, -1.0), 
                COALESCE(y, -1.0), 
                data_source);
