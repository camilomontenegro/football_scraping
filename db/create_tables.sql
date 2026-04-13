-- MySQL DDL — informational reference only.
-- Authoritative schema creation: python -c "from db.models import get_engine, Base; Base.metadata.create_all(get_engine())"

CREATE TABLE IF NOT EXISTS dim_player (
    canonical_id     INT AUTO_INCREMENT PRIMARY KEY,
    canonical_name   VARCHAR(150) NOT NULL,
    birth_date       DATE,
    nationality      VARCHAR(80),
    position         VARCHAR(50),
    id_understat     INT,
    id_statsbomb     VARCHAR(50),
    id_sofascore     INT,
    id_whoscored     INT,
    id_transfermarkt INT,
    created_at       TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS player_review (
    id                     INT AUTO_INCREMENT PRIMARY KEY,
    source_name            VARCHAR(150),
    source_system          VARCHAR(50),
    source_id              VARCHAR(50),
    suggested_canonical_id INT REFERENCES dim_player(canonical_id),
    similarity_score       SMALLINT,
    resolved               BOOLEAN DEFAULT FALSE,
    canonical_id_assigned  INT REFERENCES dim_player(canonical_id)
);

CREATE TABLE IF NOT EXISTS dim_match (
    match_id    INT AUTO_INCREMENT PRIMARY KEY,
    date        DATE,
    competition VARCHAR(100),
    season      VARCHAR(20),
    home_team   VARCHAR(100),
    away_team   VARCHAR(100),
    home_score  SMALLINT,
    away_score  SMALLINT,
    source      VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS fact_shots (
    shot_id   INT AUTO_INCREMENT PRIMARY KEY,
    match_id  INT REFERENCES dim_match(match_id),
    player_id INT REFERENCES dim_player(canonical_id),
    minute    SMALLINT,
    x         DECIMAL(6,4),
    y         DECIMAL(6,4),
    xg        DECIMAL(6,4),
    result    VARCHAR(30),
    shot_type VARCHAR(30),
    situation VARCHAR(50),
    source    VARCHAR(30)
);

CREATE TABLE IF NOT EXISTS fact_events (
    event_id   INT AUTO_INCREMENT PRIMARY KEY,
    match_id   INT REFERENCES dim_match(match_id),
    player_id  INT REFERENCES dim_player(canonical_id),
    event_type VARCHAR(50),
    minute     SMALLINT,
    second     SMALLINT,
    x          DECIMAL(6,4),
    y          DECIMAL(6,4),
    end_x      DECIMAL(6,4),
    end_y      DECIMAL(6,4),
    outcome    VARCHAR(50),
    source     VARCHAR(30)
);

CREATE TABLE IF NOT EXISTS fact_injuries (
    injury_id      INT AUTO_INCREMENT PRIMARY KEY,
    player_id      INT REFERENCES dim_player(canonical_id),
    season         VARCHAR(20),
    injury_type    VARCHAR(200),
    date_from      DATE,
    date_until     DATE,
    days_absent    INT,
    matches_missed SMALLINT
);
