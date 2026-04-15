-- ══════════════════════════════════════════════════════════
-- SCHEMA football_db
-- Versión actualizada con IDs de todas las fuentes
-- ══════════════════════════════════════════════════════════
-- ══════════════════════════════════════════════════════════
-- DIMENSIONES
-- ══════════════════════════════════════════════════════════

-- ── dim_team ──────────────────────────────────────────────
CREATE TABLE dim_team (
    team_id          SERIAL PRIMARY KEY,
    name_canonical   VARCHAR(150)        NOT NULL,
    country          VARCHAR(80),
    id_sofascore     INTEGER,
    created_at       TIMESTAMP           DEFAULT NOW()
);

CREATE UNIQUE INDEX ux_team_sofascore    ON dim_team(id_sofascore)    WHERE id_sofascore    IS NOT NULL;


-- ── dim_player ────────────────────────────────────────────
CREATE TABLE dim_player (
    player_id        SERIAL PRIMARY KEY,
    name_canonical   VARCHAR(150)        NOT NULL,
    nationality      VARCHAR(80),
    birth_date       DATE,
    player_position  VARCHAR(50),
    id_sofascore     INTEGER,
    id_understat     INTEGER,
    id_transfermarkt INTEGER,
    id_statsbomb     VARCHAR(50),
    id_whoscored     INTEGER,
    created_at       TIMESTAMP           DEFAULT NOW()
);

CREATE UNIQUE INDEX ux_player_sofascore   ON dim_player(id_sofascore)    WHERE id_sofascore    IS NOT NULL;
CREATE UNIQUE INDEX ux_player_understat   ON dim_player(id_understat)    WHERE id_understat    IS NOT NULL;
CREATE UNIQUE INDEX ux_player_statsbomb   ON dim_player(id_statsbomb)    WHERE id_statsbomb    IS NOT NULL;
CREATE UNIQUE INDEX ux_player_whoscored   ON dim_player(id_whoscored)    WHERE id_whoscored    IS NOT NULL;
CREATE UNIQUE INDEX ux_player_transfermkt ON dim_player(id_transfermarkt) WHERE id_transfermarkt IS NOT NULL;

-- ── dim_match ─────────────────────────────────────────────
CREATE TABLE dim_match (
    match_id         SERIAL PRIMARY KEY,
    match_date       DATE,
    competition      VARCHAR(100),
    season           VARCHAR(20),
    home_team        VARCHAR(100),
    away_team        VARCHAR(100),
    home_score       SMALLINT,
    away_score       SMALLINT,
    data_source      VARCHAR(50),
    id_sofascore     INTEGER,
    id_understat     INTEGER,
    id_statsbomb     INTEGER,
    id_whoscored     INTEGER
);

CREATE UNIQUE INDEX ux_match_sofascore ON dim_match(id_sofascore) WHERE id_sofascore IS NOT NULL;
CREATE UNIQUE INDEX ux_match_understat ON dim_match(id_understat) WHERE id_understat IS NOT NULL;
CREATE UNIQUE INDEX ux_match_statsbomb ON dim_match(id_statsbomb) WHERE id_statsbomb IS NOT NULL;
CREATE UNIQUE INDEX ux_match_whoscored ON dim_match(id_whoscored) WHERE id_whoscored IS NOT NULL;

-- ══════════════════════════════════════════════════════════
-- HECHOS
-- ══════════════════════════════════════════════════════════

-- ── fact_shots ────────────────────────────────────────────
CREATE TABLE fact_shots (
    shot_id      SERIAL PRIMARY KEY,
    match_id     INTEGER REFERENCES dim_match(match_id),
    player_id    INTEGER REFERENCES dim_player(player_id),
    team_id      INTEGER REFERENCES dim_team(team_id),
    minute       SMALLINT,
    x            DECIMAL(6,4),
    y            DECIMAL(6,4),
    xg           DECIMAL(6,4),
    result       VARCHAR(30),
    shot_type    VARCHAR(30),
    situation    VARCHAR(50),
    data_source  VARCHAR(30)
);

-- Deduplicación de tiros: mismo partido, jugador, minuto y coordenadas (aprox)
CREATE UNIQUE INDEX ux_shots_unique ON fact_shots (match_id, player_id, minute, x, y, data_source);

CREATE INDEX idx_shots_match  ON fact_shots(match_id);
CREATE INDEX idx_shots_player ON fact_shots(player_id);
CREATE INDEX idx_shots_team   ON fact_shots(team_id);

-- ── fact_events ───────────────────────────────────────────
CREATE TABLE fact_events (
    event_id     SERIAL PRIMARY KEY,
    match_id     INTEGER REFERENCES dim_match(match_id),
    player_id    INTEGER REFERENCES dim_player(player_id),
    team_id      INTEGER REFERENCES dim_team(team_id),
    event_type   VARCHAR(50),
    minute       SMALLINT,
    second       SMALLINT,
    x            DECIMAL(6,4),
    y            DECIMAL(6,4),
    end_x        DECIMAL(6,4),
    end_y        DECIMAL(6,4),
    outcome      VARCHAR(50),
    data_source  VARCHAR(30)
);

-- Deduplicación de eventos
CREATE UNIQUE INDEX ux_events_unique ON fact_events (match_id, player_id, event_type, minute, second, x, y, data_source);

CREATE INDEX idx_events_match  ON fact_events(match_id);
CREATE INDEX idx_events_player ON fact_events(player_id);
CREATE INDEX idx_events_team   ON fact_events(team_id);
CREATE INDEX idx_events_type   ON fact_events(event_type);

-- ── fact_injuries ─────────────────────────────────────────
CREATE TABLE fact_injuries (
    injury_id      SERIAL PRIMARY KEY,
    player_id      INTEGER REFERENCES dim_player(player_id),
    season         VARCHAR(20),
    injury_type    VARCHAR(200),
    date_from      DATE,
    date_until     DATE,
    days_absent    INTEGER,
    matches_missed SMALLINT
);

-- Deduplicación de lesiones
CREATE UNIQUE INDEX ux_injuries_unique ON fact_injuries (player_id, season, injury_type, date_from);

CREATE INDEX idx_injuries_player ON fact_injuries(player_id);