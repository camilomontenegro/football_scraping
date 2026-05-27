-- ══════════════════════════════════════════════════════════
-- SCHEMA football_db
-- ══════════════════════════════════════════════════════════

-- ── Limpieza previa (idempotente) ─────────────────────────
-- DROP en orden inverso de dependencias por las FKs.
-- CASCADE elimina índices y constraints asociados.
DROP TABLE IF EXISTS player_scrape_provenance CASCADE;
DROP TABLE IF EXISTS fact_injuries   CASCADE;
DROP TABLE IF EXISTS fact_events     CASCADE;
DROP TABLE IF EXISTS fact_shots      CASCADE;
DROP TABLE IF EXISTS dim_match       CASCADE;
DROP TABLE IF EXISTS dim_competition CASCADE;
DROP TABLE IF EXISTS player_review   CASCADE;
DROP TABLE IF EXISTS dim_player      CASCADE;
DROP TABLE IF EXISTS dim_stadium     CASCADE;
DROP TABLE IF EXISTS dim_team        CASCADE;

-- ══════════════════════════════════════════════════════════
-- DIMENSIONES
-- ══════════════════════════════════════════════════════════


-- ── dim_team ──────────────────────────────────────────────
CREATE TABLE dim_team (
    canonical_id SERIAL PRIMARY KEY,
    canonical_name VARCHAR(150) NOT NULL,
    country VARCHAR(80),
    id_sofascore INTEGER,
    id_understat INTEGER,
    id_statsbomb VARCHAR(50),
    id_whoscored INTEGER,
    id_transfermarkt INTEGER,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE UNIQUE INDEX ux_team_sofascore ON dim_team (id_sofascore)
WHERE
    id_sofascore IS NOT NULL;

CREATE UNIQUE INDEX ux_team_understat ON dim_team (id_understat)
WHERE
    id_understat IS NOT NULL;

CREATE UNIQUE INDEX ux_team_statsbomb ON dim_team (id_statsbomb)
WHERE
    id_statsbomb IS NOT NULL;

CREATE UNIQUE INDEX ux_team_whoscored ON dim_team (id_whoscored)
WHERE
    id_whoscored IS NOT NULL;

CREATE UNIQUE INDEX ux_team_transfermarkt ON dim_team (id_transfermarkt)
WHERE
    id_transfermarkt IS NOT NULL;

-- ── dim_player ────────────────────────────────────────────
CREATE TABLE dim_player (
    canonical_id SERIAL PRIMARY KEY,
    canonical_name VARCHAR(150) NOT NULL,
    nationality VARCHAR(80),
    birth_date DATE,
    position VARCHAR(50),
    id_sofascore INTEGER,
    id_understat INTEGER,
    id_transfermarkt INTEGER,
    id_statsbomb VARCHAR(50),
    id_whoscored INTEGER,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE UNIQUE INDEX ux_player_sofascore ON dim_player (id_sofascore)
WHERE
    id_sofascore IS NOT NULL;

CREATE UNIQUE INDEX ux_player_understat ON dim_player (id_understat)
WHERE
    id_understat IS NOT NULL;

CREATE UNIQUE INDEX ux_player_statsbomb ON dim_player (id_statsbomb)
WHERE
    id_statsbomb IS NOT NULL;

CREATE UNIQUE INDEX ux_player_whoscored ON dim_player (id_whoscored)
WHERE
    id_whoscored IS NOT NULL;

CREATE UNIQUE INDEX ux_player_transfermkt ON dim_player (id_transfermarkt)
WHERE
    id_transfermarkt IS NOT NULL;

-- ── player_review (Sistema de desambiguación) ─────────────
CREATE TABLE player_review (
    id SERIAL PRIMARY KEY,
    source_name VARCHAR(150) NOT NULL,
    source_system VARCHAR(50) NOT NULL,
    source_id VARCHAR(50) NOT NULL,
    suggested_canonical_id INTEGER REFERENCES dim_player (canonical_id),
    similarity_score SMALLINT,
    resolved BOOLEAN DEFAULT FALSE,
    canonical_id_assigned INTEGER REFERENCES dim_player (canonical_id),
    created_at TIMESTAMP DEFAULT NOW(),
    reviewed_at TIMESTAMP,
    source_team_id VARCHAR(50),
    source_team_name VARCHAR(150),
    competition VARCHAR(100),
    season VARCHAR(20)
);

CREATE INDEX IF NOT EXISTS idx_player_review_source ON player_review (source_system, source_id);

CREATE INDEX IF NOT EXISTS idx_player_review_suggested ON player_review (suggested_canonical_id);

CREATE INDEX IF NOT EXISTS idx_player_review_assigned ON player_review (canonical_id_assigned);

CREATE INDEX IF NOT EXISTS idx_player_review_unresolved ON player_review (resolved)
WHERE
    resolved IS FALSE;


-- ── dim_competition ────────────────────────────────────────────
CREATE TABLE dim_competition(
    canonical_id SERIAL PRIMARY KEY,
    canonical_name VARCHAR(150) NOT NULL,
    country VARCHAR(80),
    country_code VARCHAR(10),
    id_sofascore INTEGER,
    id_understat VARCHAR(50),
    -- Transfermarkt usa códigos alfanuméricos como ES1, GB1, CL.
    id_transfermarkt VARCHAR(50),
    id_statsbomb VARCHAR(50),
    id_whoscored INTEGER,
    created_at TIMESTAMP DEFAULT NOW()
);
-- garantiza que no haya dos competiciones con el mismo nombre
CREATE UNIQUE INDEX idx_dim_competition_name_unique
ON dim_competition(canonical_name);


CREATE UNIQUE INDEX idx_dim_competition_transfermarkt_unique  ON dim_competition(id_transfermarkt) 
WHERE id_transfermarkt IS NOT NULL;

CREATE UNIQUE INDEX idx_dim_competition_sofascore_unique ON dim_competition(id_sofascore) 
WHERE id_sofascore IS NOT NULL;  

CREATE UNIQUE INDEX idx_dim_competition_whoscored_unique
ON dim_competition(id_whoscored) WHERE id_whoscored IS NOT NULL;



-- ── dim_match ─────────────────────────────────────────────
CREATE TABLE dim_match (
    match_id SERIAL PRIMARY KEY,
    match_date DATE,
    competition VARCHAR(100),
    season VARCHAR(20),
    home_team_id INTEGER REFERENCES dim_team (canonical_id),
    away_team_id INTEGER REFERENCES dim_team (canonical_id),
    competition_id INTEGER REFERENCES dim_competition (canonical_id),
    home_score SMALLINT,
    away_score SMALLINT,
    data_source VARCHAR(50),
    id_sofascore INTEGER,
    id_understat INTEGER,
    id_statsbomb VARCHAR(50),
    id_whoscored INTEGER,
    -- Enrichment columns (populated post-load)
    attendance INTEGER,
    temperature_c DECIMAL(4,1),
    humidity_pct SMALLINT,
    precipitation_mm DECIMAL(5,1),
    wind_speed_kmh DECIMAL(5,1),
    weather_code SMALLINT
);

CREATE UNIQUE INDEX ux_match_sofascore ON dim_match (id_sofascore)
WHERE
    id_sofascore IS NOT NULL;

CREATE UNIQUE INDEX ux_match_understat ON dim_match (id_understat)
WHERE
    id_understat IS NOT NULL;

CREATE UNIQUE INDEX ux_match_statsbomb ON dim_match (id_statsbomb)
WHERE
    id_statsbomb IS NOT NULL;

CREATE UNIQUE INDEX ux_match_whoscored ON dim_match (id_whoscored)
WHERE
    id_whoscored IS NOT NULL;

CREATE INDEX idx_match_home_team ON dim_match (home_team_id);

CREATE INDEX idx_match_away_team ON dim_match (away_team_id);

CREATE INDEX idx_match_date ON dim_match (match_date);

-- índice sobre la clave foránea para acelerar los JOINs
CREATE INDEX idx_dim_match_competition_id ON dim_match(competition_id);


-- ══════════════════════════════════════════════════════════
-- HECHOS
-- ══════════════════════════════════════════════════════════

-- ── fact_shots ────────────────────────────────────────────
CREATE TABLE fact_shots (
    shot_id SERIAL PRIMARY KEY,
    match_id INTEGER NOT NULL REFERENCES dim_match (match_id),
    player_id INTEGER NOT NULL REFERENCES dim_player (canonical_id),
    team_id INTEGER NOT NULL REFERENCES dim_team (canonical_id),
    minute SMALLINT,
    x DECIMAL(7, 4),
    y DECIMAL(7, 4),
    xg DECIMAL(7, 4),
    result VARCHAR(30),
    shot_type VARCHAR(30),
    situation VARCHAR(50),
    data_source VARCHAR(30)
);

CREATE UNIQUE INDEX ux_shots_unique ON fact_shots (
    match_id,
    player_id,
    minute,
    x,
    y,
    data_source
);

CREATE INDEX idx_shots_match ON fact_shots (match_id);

CREATE INDEX idx_shots_player ON fact_shots (player_id);

CREATE INDEX idx_shots_team ON fact_shots (team_id);

-- ── fact_events ───────────────────────────────────────────
CREATE TABLE fact_events (
    event_id SERIAL PRIMARY KEY,
    match_id INTEGER NOT NULL REFERENCES dim_match (match_id),
    player_id INTEGER NOT NULL REFERENCES dim_player (canonical_id),
    team_id INTEGER NOT NULL REFERENCES dim_team (canonical_id),
    event_type VARCHAR(50),
    minute SMALLINT,
    second SMALLINT,
    x DECIMAL(7, 4),
    y DECIMAL(7, 4),
    end_x DECIMAL(7, 4),
    end_y DECIMAL(7, 4),
    outcome VARCHAR(50),
    data_source VARCHAR(30)
);

-- se modifica el indez para que los campos second, x e y , que estan en null en algunso eventos, tengan un valor 
-- y  se puedan itenticar  como registros unicos y evitar la inserccion duplicada  de eventos 
CREATE UNIQUE INDEX ux_events_unique 
ON fact_events (match_id, player_id, event_type, minute, 
                COALESCE(second, -1), 
                COALESCE(x, -1.0), 
                COALESCE(y, -1.0), 
                data_source);

CREATE INDEX idx_events_match ON fact_events (match_id);

CREATE INDEX idx_events_player ON fact_events (player_id);

CREATE INDEX idx_events_team ON fact_events (team_id);

CREATE INDEX idx_events_type ON fact_events (event_type);

-- ── fact_injuries ─────────────────────────────────────────
CREATE TABLE fact_injuries (
    injury_id SERIAL PRIMARY KEY,
    player_id INTEGER NOT NULL REFERENCES dim_player (canonical_id),
    season VARCHAR(20),
    injury_type VARCHAR(200),
    date_from DATE,
    date_until DATE,
    days_absent INTEGER,
    matches_missed SMALLINT
);

CREATE UNIQUE INDEX ux_injuries_unique ON fact_injuries (
    player_id,
    season,
    injury_type,
    date_from
);

CREATE INDEX idx_injuries_player ON fact_injuries (player_id);


-- ── player_scrape_provenance ──────────────────────────────
-- Trazabilidad: de qué comp/temporada/equipo salió cada ID de jugador
-- en cada fuente. Clave para desambiguar homónimos ("Pedro", "Koke"…).
CREATE TABLE player_scrape_provenance (
    id               SERIAL PRIMARY KEY,
    source_system    VARCHAR(50)  NOT NULL,
    source_player_id VARCHAR(50)  NOT NULL,
    scraped_name     VARCHAR(150) NOT NULL,
    competition      VARCHAR(100) NOT NULL DEFAULT '',
    season           VARCHAR(20)  NOT NULL DEFAULT '',
    team_name        VARCHAR(150),
    team_id          VARCHAR(50)  NOT NULL DEFAULT '',
    canonical_id     INTEGER REFERENCES dim_player (canonical_id),
    scraped_at       TIMESTAMP DEFAULT NOW(),
    UNIQUE (source_system, source_player_id, competition, season, team_id)
);

CREATE INDEX idx_player_provenance_canonical
    ON player_scrape_provenance (canonical_id);

CREATE INDEX idx_player_provenance_name
    ON player_scrape_provenance (LOWER(scraped_name));


-- ══════════════════════════════════════════════════════════
-- DIMENSIÓN: ESTADIOS
-- ══════════════════════════════════════════════════════════
-- Estadios de cada equipo por temporada (Transfermarkt como fuente).
-- Granularidad: (team, season). Un equipo puede cambiar de estadio
-- entre temporadas (obras, mudanza, etc.).

-- SCD2: una fila por ESTADO del estadio, no por temporada. Si la
-- información no cambia entre 2020 y 2025, hay UNA sola fila con
-- valid_from_season='2020/2021' y valid_to_season='2024/2025'. Cuando
-- algún campo cambia (capacity, nombre, reforma…) se cierra la fila
-- antigua y se abre una nueva.
CREATE TABLE dim_stadium (
    stadium_id            SERIAL PRIMARY KEY,
    canonical_team_id     INTEGER REFERENCES dim_team (canonical_id) ON DELETE CASCADE,
    id_transfermarkt_team INTEGER NOT NULL,
    team_slug             VARCHAR(150),

    -- Rango de temporadas en las que este estado es válido
    valid_from_season     VARCHAR(20) NOT NULL,
    valid_to_season       VARCHAR(20) NOT NULL,

    -- Datos del estadio (Transfermarkt + Wikidata enrichment)
    stadium_name          VARCHAR(200),
    capacity              INTEGER,
    capacity_intl         INTEGER,
    seats_total           INTEGER,
    built_year            SMALLINT,
    owner                 VARCHAR(200),
    operator              VARCHAR(200),
    address               VARCHAR(300),
    city                  VARCHAR(120),
    country               VARCHAR(80),
    surface               VARCHAR(80),
    architect             VARCHAR(200),
    naming_rights         VARCHAR(200),
    previous_names_raw    TEXT,
    pitch_length_m        SMALLINT,
    pitch_width_m         SMALLINT,
    has_pitch_heating     BOOLEAN,
    tm_url                VARCHAR(400),

    -- Wikidata enrichment
    wikidata_qid          VARCHAR(20),
    latitude              DECIMAL(9,6),
    longitude             DECIMAL(9,6),
    image_url             TEXT,

    -- SHA1 hex de los campos comparables, para detectar cambios rápido
    data_hash             CHAR(40),

    data_source           VARCHAR(50) DEFAULT 'transfermarkt',
    created_at            TIMESTAMP DEFAULT NOW(),
    updated_at            TIMESTAMP DEFAULT NOW(),

    CHECK (valid_from_season <= valid_to_season)
);

-- Un equipo no puede tener dos rangos que empiecen en la misma temporada.
CREATE UNIQUE INDEX ux_stadium_team_validfrom
    ON dim_stadium (id_transfermarkt_team, valid_from_season);

CREATE INDEX idx_stadium_team      ON dim_stadium (canonical_team_id);
CREATE INDEX idx_stadium_team_tm   ON dim_stadium (id_transfermarkt_team);
CREATE INDEX idx_stadium_data_hash ON dim_stadium (id_transfermarkt_team, data_hash);
CREATE INDEX idx_stadium_name_lower ON dim_stadium (LOWER(stadium_name));
