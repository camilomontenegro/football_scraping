-- Script para normalizar coordenadas de tiros en fact_shots
-- En la base de datos las coordenadas de SofaScore y  las coordenadas de los  registros de  Understat provenientes de la liga no estaban normnalizadas al formato 0-1 

-- Verificar el estado de las coordenadas antes de la actualzación 
-- Si hay registros con coordenadas fuera del rango 0-1, entonces  hay que ejeturar los updates de normalización
SELECT 
    data_source,
    ROUND(MIN(x)::numeric, 4) as min_x,
    ROUND(MAX(x)::numeric, 4) as max_x,
    ROUND(MIN(y)::numeric, 4) as min_y,
    ROUND(MAX(y)::numeric, 4) as max_y
FROM fact_shots
GROUP BY data_source;


-- Normalizar coordenadas de SofaScore (rango 0-100 → 0-1)
UPDATE fact_shots 
SET x = ROUND(x / 100, 4),
    y = ROUND(y / 100, 4)
WHERE data_source = 'sofascore'
AND (x > 1 OR y > 1);

-- Normalizar coordenadas de Understat (algunos CSVs venían en 0-100)
UPDATE fact_shots 
SET x = ROUND(x / 100, 4),
    y = ROUND(y / 100, 4)
WHERE data_source = 'understat'
AND (x > 1 OR y > 1);

