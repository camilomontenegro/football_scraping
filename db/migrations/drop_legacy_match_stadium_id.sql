-- Unifica estadio de partido: match_stadium_id es la columna canónica.
-- Ejecutar via: python -m scripts.stadium_hygiene

UPDATE dim_match
SET match_stadium_id = stadium_id
WHERE match_stadium_id IS NULL AND stadium_id IS NOT NULL;

ALTER TABLE dim_match DROP COLUMN IF EXISTS stadium_id;

COMMENT ON COLUMN dim_match.match_stadium_id IS
    'Estadio donde se jugó el partido (dim_stadium.stadium_id).';
