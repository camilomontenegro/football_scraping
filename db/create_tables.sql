-- ══════════════════════════════════════════════════════════
-- FOOTBALL_DB v5.3 — PRODUCCIÓN LIMPIA
-- Dimensiones → External IDs → Mapping → Alignment → Facts → Staging
-- ══════════════════════════════════════════════════════════


-- ══════════════════════════════════════════════════════════
-- DIMENSIONES BASE
-- ══════════════════════════════════════════════════════════

CREATE TABLE dim_season (
    season_id    SERIAL PRIMARY KEY,
    label        VARCHAR(20) NOT NULL,
    year_start   SMALLINT NOT NULL,
    year_end     SMALLINT NOT NULL,
    created_at   TIMESTAMP DEFAULT NOW()
);
CREATE UNIQUE INDEX ux_season_label ON dim_season(label);


CREATE TABLE dim_injury_type (
    injury_type_id SERIAL PRIMARY KEY,
    name           VARCHAR(200) NOT NULL,
    category       VARCHAR(80),
    created_at     TIMESTAMP DEFAULT NOW()
);
CREATE UNIQUE INDEX ux_injury_type_name ON dim_injury_type(name);


CREATE TABLE dim_team (
    team_id        SERIAL PRIMARY KEY,
    name_canonical VARCHAR(150) NOT NULL,
    country        VARCHAR(80),
    created_at     TIMESTAMP DEFAULT NOW()
);
CREATE UNIQUE INDEX ux_team_name ON dim_team(name_canonical);


CREATE TABLE dim_player (
    player_id       SERIAL PRIMARY KEY,
    name_canonical  VARCHAR(150) NOT NULL,
    nationality     VARCHAR(80),
    birth_date      DATE,
    player_position VARCHAR(50),
    created_at      TIMESTAMP DEFAULT NOW()
);
CREATE UNIQUE INDEX ux_player_name ON dim_player(name_canonical);


CREATE TABLE dim_match (
    match_id       SERIAL PRIMARY KEY,
    match_date     DATE NOT NULL,
    season_id      INTEGER NOT NULL REFERENCES dim_season(season_id),
    home_team_id   INTEGER REFERENCES dim_team(team_id),
    away_team_id   INTEGER REFERENCES dim_team(team_id),
    home_score     SMALLINT,
    away_score     SMALLINT,
    data_source    VARCHAR(30),
    created_at     TIMESTAMP DEFAULT NOW()
);

CREATE UNIQUE INDEX ux_match_natural
ON dim_match (season_id, match_date, home_team_id, away_team_id, data_source);

CREATE INDEX idx_match_date   ON dim_match(match_date);
CREATE INDEX idx_match_season ON dim_match(season_id);


-- ══════════════════════════════════════════════════════════
-- PLAYER NAME ALIAS (MDM LAYER)
-- ══════════════════════════════════════════════════════════

CREATE TABLE player_name_alias (
    alias_id   SERIAL PRIMARY KEY,
    player_id  INTEGER NOT NULL REFERENCES dim_player(player_id) ON DELETE CASCADE,
    alias_name VARCHAR(200) NOT NULL,
    source     VARCHAR(30),
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_alias_player ON player_name_alias(player_id);
CREATE INDEX idx_alias_name   ON player_name_alias(alias_name);
CREATE UNIQUE INDEX ux_player_alias_source ON player_name_alias(player_id, alias_name, source);


-- ══════════════════════════════════════════════════════════
-- PLAYER RESOLUTION LOG (AUDITORÍA MDM)
-- ══════════════════════════════════════════════════════════

CREATE TABLE player_resolution_log (
    id         SERIAL PRIMARY KEY,
    raw_name   TEXT NOT NULL,
    player_id  INTEGER REFERENCES dim_player(player_id),
    source     VARCHAR(30),
    match_type VARCHAR(50),
    confidence SMALLINT,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_resolution_log_player  ON player_resolution_log(player_id);
CREATE INDEX idx_resolution_log_source  ON player_resolution_log(source);
CREATE INDEX idx_resolution_log_created ON player_resolution_log(created_at);


-- ══════════════════════════════════════════════════════════
-- TEAM NAME ALIAS (MDM LAYER)
-- ══════════════════════════════════════════════════════════

CREATE TABLE team_name_alias (
    alias_id   SERIAL PRIMARY KEY,
    team_id    INTEGER NOT NULL REFERENCES dim_team(team_id) ON DELETE CASCADE,
    alias_name VARCHAR(200) NOT NULL,
    source     VARCHAR(30),
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_team_alias_id   ON team_name_alias(team_id);
CREATE INDEX idx_team_alias_name ON team_name_alias(alias_name);
CREATE UNIQUE INDEX ux_team_alias_source ON team_name_alias(team_id, alias_name, source);


-- ══════════════════════════════════════════════════════════
-- EXTERNAL IDS
-- ══════════════════════════════════════════════════════════

CREATE TABLE player_external_ids (
    player_id   INTEGER NOT NULL REFERENCES dim_player(player_id) ON DELETE CASCADE,
    source      VARCHAR(30) NOT NULL,
    external_id TEXT NOT NULL,
    PRIMARY KEY (player_id, source)
);
CREATE UNIQUE INDEX ux_player_ext ON player_external_ids(source, external_id);


CREATE TABLE team_external_ids (
    team_id     INTEGER NOT NULL REFERENCES dim_team(team_id) ON DELETE CASCADE,
    source      VARCHAR(30) NOT NULL,
    external_id TEXT NOT NULL,
    PRIMARY KEY (team_id, source)
);
CREATE UNIQUE INDEX ux_team_ext ON team_external_ids(source, external_id);


CREATE TABLE match_external_ids (
    match_id    INTEGER NOT NULL REFERENCES dim_match(match_id) ON DELETE CASCADE,
    source      VARCHAR(30) NOT NULL,
    external_id TEXT NOT NULL,
    PRIMARY KEY (match_id, source)
);
CREATE UNIQUE INDEX ux_match_ext ON match_external_ids(source, external_id);


-- ══════════════════════════════════════════════════════════
-- PLAYER MAPPING (CLAVE DEL SISTEMA)
-- ══════════════════════════════════════════════════════════

CREATE TABLE transfermarkt_player_mapping (
    player_id_tm TEXT PRIMARY KEY,
    player_id    INTEGER NOT NULL REFERENCES dim_player(player_id),
    created_at   TIMESTAMP DEFAULT NOW(),
    updated_at   TIMESTAMP DEFAULT NOW()
);

CREATE UNIQUE INDEX ux_tm_player_reverse ON transfermarkt_player_mapping(player_id);


-- ══════════════════════════════════════════════════════════
-- ALIGNMENT (CROSS SOURCE)
-- ══════════════════════════════════════════════════════════

CREATE TABLE match_alignment (
    alignment_id SERIAL PRIMARY KEY,
    match_id     INTEGER NOT NULL REFERENCES dim_match(match_id) ON DELETE CASCADE,
    source       VARCHAR(30) NOT NULL,
    external_id  TEXT NOT NULL,
    match_date   DATE,
    home_team    VARCHAR(150),
    away_team    VARCHAR(150),
    confidence   VARCHAR(10) DEFAULT 'auto'
        CHECK (confidence IN ('auto','fuzzy','manual')),
    aligned_at   TIMESTAMP DEFAULT NOW(),
    aligned_by   VARCHAR(80) DEFAULT 'pipeline'
);

CREATE UNIQUE INDEX ux_match_alignment    ON match_alignment(match_id, source, external_id);
CREATE INDEX        idx_match_align_lookup ON match_alignment(source, external_id);


CREATE TABLE player_alignment (
    alignment_id SERIAL PRIMARY KEY,
    player_id    INTEGER NOT NULL REFERENCES dim_player(player_id) ON DELETE CASCADE,
    source       VARCHAR(30) NOT NULL,
    external_id  TEXT NOT NULL,
    confidence   VARCHAR(10) DEFAULT 'auto'
        CHECK (confidence IN ('auto','fuzzy','manual')),
    aligned_at   TIMESTAMP DEFAULT NOW(),
    aligned_by   VARCHAR(80) DEFAULT 'pipeline'
);

CREATE UNIQUE INDEX ux_player_alignment ON player_alignment(player_id, source, external_id);


-- ══════════════════════════════════════════════════════════
-- FACTS
-- ══════════════════════════════════════════════════════════

CREATE TABLE fact_shots (
    shot_id     SERIAL PRIMARY KEY,
    match_id    INTEGER REFERENCES dim_match(match_id),   -- nullable: fuentes sin dim_match
    player_id   INTEGER REFERENCES dim_player(player_id), -- nullable: MDM puede no resolver
    team_id     INTEGER REFERENCES dim_team(team_id),
    minute      SMALLINT,
    x           DECIMAL(5,2),
    y           DECIMAL(5,2),
    xg          DECIMAL(5,4),
    result      VARCHAR(30),
    shot_type   VARCHAR(30),
    situation   VARCHAR(50),
    data_source VARCHAR(30) NOT NULL,
    external_id TEXT
);

CREATE UNIQUE INDEX ux_shots
ON fact_shots(match_id, external_id, data_source)
WHERE external_id IS NOT NULL;


CREATE TABLE fact_events (
    event_id    SERIAL PRIMARY KEY,
    match_id    INTEGER REFERENCES dim_match(match_id),   -- nullable: fuentes sin dim_match
    player_id   INTEGER REFERENCES dim_player(player_id),
    team_id     INTEGER REFERENCES dim_team(team_id),
    event_type  VARCHAR(50),
    minute      SMALLINT,
    second      SMALLINT,
    x           DECIMAL(5,2),
    y           DECIMAL(5,2),
    end_x       DECIMAL(5,2),
    end_y       DECIMAL(5,2),
    outcome     VARCHAR(50),
    data_source VARCHAR(30) NOT NULL,
    external_id TEXT
);

CREATE UNIQUE INDEX ux_events
ON fact_events(match_id, external_id, data_source)
WHERE external_id IS NOT NULL;


CREATE TABLE fact_injuries (
    injury_id      SERIAL PRIMARY KEY,
    player_id      INTEGER NOT NULL REFERENCES dim_player(player_id),
    team_id        INTEGER REFERENCES dim_team(team_id),
    injury_type_id INTEGER REFERENCES dim_injury_type(injury_type_id),
    season_id      INTEGER REFERENCES dim_season(season_id),
    date_from      DATE,
    date_until     DATE,
    days_absent    INTEGER,
    matches_missed SMALLINT,
    CONSTRAINT chk_dates
        CHECK (date_until IS NULL OR date_until >= date_from)
);

CREATE UNIQUE INDEX ux_injuries ON fact_injuries(player_id, injury_type_id, date_from);
CREATE INDEX idx_injuries_player_season ON fact_injuries(player_id, season_id);


-- ══════════════════════════════════════════════════════════
-- STAGING (IDEMPOTENTE)
-- ══════════════════════════════════════════════════════════

CREATE TABLE stg_transfermarkt_players (
    player_name      VARCHAR(150),
    id_transfermarkt TEXT NOT NULL,
    team_name        VARCHAR(150),
    team_country     VARCHAR(80),
    position         VARCHAR(50),
    nationality      VARCHAR(80),
    birth_date       DATE,
    raw_json         JSONB,
    batch_id         TEXT NOT NULL,
    loaded_at        TIMESTAMP DEFAULT NOW()
);

CREATE UNIQUE INDEX ux_stg_players
ON stg_transfermarkt_players(id_transfermarkt, batch_id);


CREATE TABLE stg_transfermarkt_injuries (
    player_id_tm   TEXT NOT NULL,
    season         VARCHAR(10),
    injury_type    VARCHAR(200),
    date_from      DATE,
    date_until     DATE,
    days_absent    INTEGER,
    matches_missed SMALLINT,
    raw_json       JSONB,
    batch_id       TEXT NOT NULL,
    loaded_at      TIMESTAMP DEFAULT NOW()
);

CREATE UNIQUE INDEX ux_stg_injuries
ON stg_transfermarkt_injuries(player_id_tm, injury_type, date_from, batch_id);


CREATE TABLE stg_sofascore_shots (
    id            SERIAL PRIMARY KEY,
    match_id_ext  INTEGER,
    player_id_ext INTEGER,
    player_name   VARCHAR(150),
    team_name     VARCHAR(150),
    minute        VARCHAR(10),
    x             VARCHAR(20),
    y             VARCHAR(20),
    xg            VARCHAR(20),
    result        VARCHAR(30),
    shot_type     VARCHAR(30),
    raw_json      JSONB,
    batch_id      TEXT,
    loaded_at     TIMESTAMP DEFAULT NOW()
);


CREATE TABLE stg_sofascore_events (
    id            SERIAL PRIMARY KEY,
    match_id_ext  INTEGER,
    player_id_ext INTEGER,
    incident_type VARCHAR(50),
    minute        VARCHAR(10),
    player_name   VARCHAR(150),
    team_name     VARCHAR(150),
    raw_json      JSONB,
    batch_id      TEXT,
    loaded_at     TIMESTAMP DEFAULT NOW()
);


CREATE TABLE stg_statsbomb_events (
    id           SERIAL PRIMARY KEY,
    match_id_ext INTEGER,
    event_uuid   TEXT,
    event_type   VARCHAR(50),
    minute       SMALLINT,
    second       SMALLINT,
    player_name  VARCHAR(150),
    team_name    VARCHAR(150),
    raw_json     JSONB,
    batch_id     TEXT,
    loaded_at    TIMESTAMP DEFAULT NOW()
);

CREATE UNIQUE INDEX ux_statsbomb_uuid
ON stg_statsbomb_events(event_uuid)
WHERE event_uuid IS NOT NULL;


CREATE TABLE stg_whoscored_events (
    id           SERIAL PRIMARY KEY,
    match_id_ext INTEGER,
    event_id_ext INTEGER,
    event_type   VARCHAR(50),
    minute       VARCHAR(10),
    player_name  VARCHAR(150),
    team_name    VARCHAR(150),
    x            VARCHAR(20),
    y            VARCHAR(20),
    raw_json     JSONB,
    batch_id     TEXT,
    loaded_at    TIMESTAMP DEFAULT NOW()
);


CREATE TABLE stg_understat_shots (
    id           SERIAL PRIMARY KEY,
    match_id_ext TEXT,
    player_id_ext TEXT,
    player_name  VARCHAR(150),
    team_name    VARCHAR(150),
    h_team       VARCHAR(150),
    a_team       VARCHAR(150),
    season       VARCHAR(10),
    minute       SMALLINT,
    x            DECIMAL(5,2),
    y            DECIMAL(5,2),
    xg           DECIMAL(5,4),
    result       VARCHAR(30),
    shot_type    VARCHAR(30),
    situation    VARCHAR(50),
    raw_json     JSONB,
    batch_id     TEXT NOT NULL,
    loaded_at    TIMESTAMP DEFAULT NOW()
);

CREATE UNIQUE INDEX ux_stg_understat
ON stg_understat_shots(match_id_ext, player_id_ext, minute, batch_id)
WHERE match_id_ext IS NOT NULL;


-- ══════════════════════════════════════════════════════════
-- UTILIDAD
-- ══════════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION generate_batch_id()
RETURNS TEXT AS $$
BEGIN
    RETURN 'batch_' || to_char(NOW(), 'YYYYMMDD_HH24MISS');
END;
$$ LANGUAGE plpgsql;