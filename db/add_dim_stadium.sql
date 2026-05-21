-- ══════════════════════════════════════════════════════════
-- MIGRACIÓN ADITIVA: dim_stadium
-- ══════════════════════════════════════════════════════════
-- Ejecutar sobre una BBDD ya existente para añadir la tabla
-- de estadios sin tener que dropear el resto del schema.
--
--   psql -U postgres -d db_football_completa -f db/add_dim_stadium.sql

CREATE TABLE IF NOT EXISTS dim_stadium (
    stadium_id            SERIAL PRIMARY KEY,
    canonical_team_id     INTEGER REFERENCES dim_team (canonical_id) ON DELETE CASCADE,
    id_transfermarkt_team INTEGER,
    team_slug             VARCHAR(150),
    season                VARCHAR(20),
    stadium_name          VARCHAR(200),
    capacity              INTEGER,
    seats_total           INTEGER,
    seats_covered         INTEGER,
    seats_vip             INTEGER,
    vip_boxes             INTEGER,
    seats_standing        INTEGER,
    inaugurated_year      SMALLINT,
    built_year            SMALLINT,
    refurbished_year      SMALLINT,
    owner                 VARCHAR(200),
    operator              VARCHAR(200),
    address               VARCHAR(300),
    city                  VARCHAR(120),
    country               VARCHAR(80),
    construction_cost     VARCHAR(120),
    surface               VARCHAR(80),
    architect             VARCHAR(200),
    tm_url                VARCHAR(400),
    data_source           VARCHAR(50) DEFAULT 'transfermarkt',
    created_at            TIMESTAMP DEFAULT NOW(),
    updated_at            TIMESTAMP DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_stadium_team_season
    ON dim_stadium (id_transfermarkt_team, season)
    WHERE id_transfermarkt_team IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_stadium_team       ON dim_stadium (canonical_team_id);
CREATE INDEX IF NOT EXISTS idx_stadium_season     ON dim_stadium (season);
CREATE INDEX IF NOT EXISTS idx_stadium_name_lower ON dim_stadium (LOWER(stadium_name));
