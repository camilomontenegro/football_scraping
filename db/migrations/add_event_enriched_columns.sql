-- Columnas de qualifiers individuales en fact_events para consultas directas.
-- Complementan la columna JSONB (raw completo) con campos indexables.
-- Uso: psql -U postgres -d football_db -f db/migrations/add_event_enriched_columns.sql

-- ── Parte del cuerpo ─────────────────────────────────────────────
ALTER TABLE fact_events
    ADD COLUMN IF NOT EXISTS body_part VARCHAR(20);
    -- RightFoot, LeftFoot, Head, OtherBodyPart

-- ── Goal-mouth placement (0-100 scale from WhoScored) ───────────
ALTER TABLE fact_events
    ADD COLUMN IF NOT EXISTS goal_mouth_y DECIMAL(7,2),
    ADD COLUMN IF NOT EXISTS goal_mouth_z DECIMAL(7,2);

-- ── Geometría del pase/tiro ─────────────────────────────────────
ALTER TABLE fact_events
    ADD COLUMN IF NOT EXISTS angle DECIMAL(7,2),
    ADD COLUMN IF NOT EXISTS length DECIMAL(7,2);

-- ── Pass destination (0-100) ────────────────────────────────────
ALTER TABLE fact_events
    ADD COLUMN IF NOT EXISTS pass_end_x DECIMAL(7,4),
    ADD COLUMN IF NOT EXISTS pass_end_y DECIMAL(7,4);

-- ── Flags booleanos clave ───────────────────────────────────────
ALTER TABLE fact_events
    ADD COLUMN IF NOT EXISTS is_assisted BOOLEAN,
    ADD COLUMN IF NOT EXISTS is_individual_play BOOLEAN,
    ADD COLUMN IF NOT EXISTS is_big_chance BOOLEAN,
    ADD COLUMN IF NOT EXISTS is_key_pass BOOLEAN,
    ADD COLUMN IF NOT EXISTS is_fast_break BOOLEAN;

-- ── Shot zone (posición del tiro en el campo) ───────────────────
ALTER TABLE fact_events
    ADD COLUMN IF NOT EXISTS shot_zone VARCHAR(30);
    -- BoxCentre, BoxLeft, BoxRight, OutOfBoxCentre, SmallBoxCentre, etc.

-- ── Shot placement (dónde va el tiro a portería) ────────────────
ALTER TABLE fact_events
    ADD COLUMN IF NOT EXISTS shot_placement VARCHAR(20);
    -- LowLeft, LowCentre, LowRight, HighLeft, HighCentre, HighRight

-- ── Situación de juego detallada ────────────────────────────────
ALTER TABLE fact_events
    ADD COLUMN IF NOT EXISTS situation_detail VARCHAR(30);
    -- RegularPlay, FromCorner, SetPiece, FastBreak, Penalty, DirectFreekick

-- ── Blocked coordinates ─────────────────────────────────────────
ALTER TABLE fact_events
    ADD COLUMN IF NOT EXISTS blocked_x DECIMAL(7,4),
    ADD COLUMN IF NOT EXISTS blocked_y DECIMAL(7,4);

-- ── Related player (assister / fouled player) ───────────────────
ALTER TABLE fact_events
    ADD COLUMN IF NOT EXISTS related_player_id INTEGER;
    -- WhoScored relatedPlayerId (raw, sin resolver a canonical_id)

-- ── Índices para consultas frecuentes ───────────────────────────
CREATE INDEX IF NOT EXISTS idx_events_body_part
    ON fact_events (body_part) WHERE body_part IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_events_assisted
    ON fact_events (is_assisted) WHERE is_assisted IS TRUE;

CREATE INDEX IF NOT EXISTS idx_events_big_chance
    ON fact_events (is_big_chance) WHERE is_big_chance IS TRUE;

CREATE INDEX IF NOT EXISTS idx_events_shot_zone
    ON fact_events (shot_zone) WHERE shot_zone IS NOT NULL;

COMMENT ON COLUMN fact_events.goal_mouth_y IS 'Posición horizontal del tiro en portería (0=izq, 100=der), WhoScored GoalMouthY';
COMMENT ON COLUMN fact_events.goal_mouth_z IS 'Posición vertical del tiro en portería (0=suelo, 100=arriba), WhoScored GoalMouthZ';
COMMENT ON COLUMN fact_events.body_part IS 'Parte del cuerpo: RightFoot, LeftFoot, Head, OtherBodyPart';
COMMENT ON COLUMN fact_events.shot_zone IS 'Zona del campo desde donde se tira: BoxCentre, OutOfBoxCentre, etc.';
COMMENT ON COLUMN fact_events.shot_placement IS 'Zona de portería a la que va el tiro: LowLeft, HighRight, etc.';
COMMENT ON COLUMN fact_events.situation_detail IS 'Contexto del tiro: RegularPlay, FromCorner, FastBreak, Penalty, etc.';
