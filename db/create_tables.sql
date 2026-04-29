-- ══════════════════════════════════════════════════════════
-- SCHEMA football_db
-- ══════════════════════════════════════════════════════════

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
    reviewed_at TIMESTAMP
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
    id_sofascore INTEGER,
    id_understat VARCHAR(50),
    # problema TF usa  string como codigo para las competiciones 
    id_transfermarkt INTEGER,
    id_statsbomb VARCHAR(50),
    id_whoscored INTEGER,
    created_at TIMESTAMP DEFAULT NOW(),
)
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
    competition_id INTEGER REFERENDES dim_competition (canonical_id),
    home_score SMALLINT,
    away_score SMALLINT,
    data_source VARCHAR(50),
    id_sofascore INTEGER,
    id_understat INTEGER,
    id_statsbomb VARCHAR(50),
    id_whoscored INTEGER
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