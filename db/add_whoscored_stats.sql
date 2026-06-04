-- ═══════════════════════════════════════════════════════════════════════
-- add_whoscored_stats.sql
-- ---------------------------------------------------------------------
-- Nuevas tablas para explotar el JSON completo de WhoScored /live:
--   1. fact_player_match_stats  — stats agregadas por jugador y partido
--   2. fact_formations          — formaciones con rango temporal
--   3. Enriquecimiento de dim_match: venue_name, manager_home, manager_away
--   4. Enriquecimiento de dim_referee con id_whoscored
--
-- Idempotente (IF NOT EXISTS / ADD COLUMN IF NOT EXISTS).
--
-- Uso:
--   psql -U postgres -d football_db -f db/add_whoscored_stats.sql
-- ═══════════════════════════════════════════════════════════════════════

-- ── 1. fact_player_match_stats ───────────────────────────────────────

CREATE TABLE IF NOT EXISTS fact_player_match_stats (
    id                  SERIAL PRIMARY KEY,
    match_id            INTEGER NOT NULL REFERENCES dim_match (match_id),
    player_id           INTEGER NOT NULL REFERENCES dim_player (canonical_id),
    team_id             INTEGER NOT NULL REFERENCES dim_team (canonical_id),

    -- Metadata del jugador en ese partido
    is_starter          BOOLEAN,
    position            VARCHAR(10),
    shirt_no            SMALLINT,
    age                 SMALLINT,
    height_cm           SMALLINT,
    weight_kg           SMALLINT,
    is_man_of_the_match BOOLEAN,

    -- Sustitución
    subbed_in_minute    SMALLINT,
    subbed_out_minute   SMALLINT,

    -- Rating (valor final del partido)
    rating              DECIMAL(4,2),

    -- Pases
    passes_total        SMALLINT,
    passes_accurate     SMALLINT,
    passes_key          SMALLINT,
    pass_success_pct    DECIMAL(5,2),

    -- Tiros
    shots_total         SMALLINT,
    shots_on_target     SMALLINT,
    shots_off_target    SMALLINT,
    shots_blocked       SMALLINT,

    -- Regates
    dribbles_attempted  SMALLINT,
    dribbles_won        SMALLINT,
    dribbles_lost       SMALLINT,

    -- Defensa
    tackles_total       SMALLINT,
    tackles_successful  SMALLINT,
    interceptions       SMALLINT,
    clearances          SMALLINT,

    -- Aéreo
    aerials_total       SMALLINT,
    aerials_won         SMALLINT,

    -- Disciplina y otros
    fouls_committed     SMALLINT,
    was_dribbled_past   SMALLINT,
    dispossessed        SMALLINT,
    touches             SMALLINT,
    offsides_caught     SMALLINT,

    -- Corners y saques de banda
    corners_total       SMALLINT,
    corners_accurate    SMALLINT,
    throw_ins_total     SMALLINT,
    throw_ins_accurate  SMALLINT,

    -- Portero
    saves_total         SMALLINT,
    saves_parried_safe  SMALLINT,
    saves_parried_danger SMALLINT,
    claims_high         SMALLINT,
    collected           SMALLINT,

    -- Posesión (touches por minuto, acumulado)
    possession_pct      DECIMAL(5,2),

    data_source         VARCHAR(30) DEFAULT 'whoscored',
    created_at          TIMESTAMP DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_player_match_stats
    ON fact_player_match_stats (match_id, player_id, data_source);

CREATE INDEX IF NOT EXISTS idx_pms_match  ON fact_player_match_stats (match_id);
CREATE INDEX IF NOT EXISTS idx_pms_player ON fact_player_match_stats (player_id);
CREATE INDEX IF NOT EXISTS idx_pms_team   ON fact_player_match_stats (team_id);


-- ── 2. fact_formations ──────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS fact_formations (
    id                  SERIAL PRIMARY KEY,
    match_id            INTEGER NOT NULL REFERENCES dim_match (match_id),
    team_id             INTEGER NOT NULL REFERENCES dim_team (canonical_id),
    side                VARCHAR(4) NOT NULL,          -- 'home' / 'away'
    formation_name      VARCHAR(20) NOT NULL,         -- '4231', '433', etc.
    captain_player_id   INTEGER REFERENCES dim_player (canonical_id),
    start_minute        SMALLINT NOT NULL DEFAULT 0,
    end_minute          SMALLINT,
    data_source         VARCHAR(30) DEFAULT 'whoscored',
    created_at          TIMESTAMP DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_formations_unique
    ON fact_formations (match_id, team_id, start_minute, data_source);

CREATE INDEX IF NOT EXISTS idx_formations_match ON fact_formations (match_id);
CREATE INDEX IF NOT EXISTS idx_formations_team  ON fact_formations (team_id);


-- ── 3. Enriquecer dim_match ──────────────────────────────────────────

ALTER TABLE dim_match ADD COLUMN IF NOT EXISTS venue_name VARCHAR(200);
ALTER TABLE dim_match ADD COLUMN IF NOT EXISTS manager_home VARCHAR(150);
ALTER TABLE dim_match ADD COLUMN IF NOT EXISTS manager_away VARCHAR(150);
ALTER TABLE dim_match ADD COLUMN IF NOT EXISTS ht_score VARCHAR(10);
ALTER TABLE dim_match ADD COLUMN IF NOT EXISTS ft_score VARCHAR(10);


-- ── 4. Asegurar id_whoscored en dim_referee ──────────────────────────

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'dim_referee' AND column_name = 'id_whoscored'
    ) THEN
        ALTER TABLE dim_referee ADD COLUMN id_whoscored INTEGER;
    END IF;
END $$;

CREATE UNIQUE INDEX IF NOT EXISTS ux_referee_whoscored
    ON dim_referee (id_whoscored) WHERE id_whoscored IS NOT NULL;
