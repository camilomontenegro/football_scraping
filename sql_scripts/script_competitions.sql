-- Crea la tabla dim competition con indices 
-- Añade la clave foránea en dim_match  y crea un índice sobre  la fk
-- El  archivo create _tables esta actualziado con estas modificacion
-- Solo ejecutar eset script  si el el esquema de la base de datos que se tnga no tiene la tabla dim_competition

CREATE TABLE dim_competition (
    canonical_id      SERIAL PRIMARY KEY,
    canonical_name    VARCHAR(150) NOT NULL,
    id_sofascore      INTEGER,
    --el 'id' de understat en la pagina no es numerico, sino texto
    --lo mismo ocurre en transfermarkt 
    id_understat      VARCHAR(50)
    id_transfermarkt  VARCHAR(50),
    id_statsbomb      INTEGER,
    id_whoscored      INTEGER,
    created_at        TIMESTAMP DEFAULT NOW()
);

-- garantiza que no haya dos competiciones con el mismo nombre
CREATE UNIQUE INDEX idx_dim_competition_name_unique
ON dim_competition(canonical_name);

-- garantiza que no haya dos competiciones con el mismo ID en cada fuente
CREATE UNIQUE INDEX idx_dim_competition_sofascore_unique
ON dim_competition(id_sofascore)
WHERE id_sofascore IS NOT NULL;

CREATE UNIQUE INDEX idx_dim_competition_understat_unique
ON dim_competition(id_understat)
WHERE id_understat IS NOT NULL;

CREATE UNIQUE INDEX idx_dim_competition_transfermarkt_unique
ON dim_competition(id_transfermarkt)
WHERE id_transfermarkt IS NOT NULL;

CREATE UNIQUE INDEX idx_dim_competition_statsbomb_unique
ON dim_competition(id_statsbomb)
WHERE id_statsbomb IS NOT NULL;

CREATE UNIQUE INDEX idx_dim_competition_whoscored_unique
ON dim_competition(id_whoscored)
WHERE id_whoscored IS NOT NULL;

-- clave foránea en dim_match hacia dim_competition
ALTER TABLE dim_match
ADD COLUMN competition_id INTEGER REFERENCES dim_competition(canonical_id);

-- índice sobre la clave foránea para acelerar los JOINs
CREATE INDEX idx_dim_match_competition_id
ON dim_match(competition_id);