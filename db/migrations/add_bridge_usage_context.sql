-- Contexto de uso en bridge_team_season_stadium (liga vs UEFA, sede principal, etc.)

ALTER TABLE bridge_team_season_stadium
    ADD COLUMN IF NOT EXISTS usage_context VARCHAR(20) NOT NULL DEFAULT 'primary';

COMMENT ON COLUMN bridge_team_season_stadium.usage_context IS
    'primary | domestic | european | rental — sede habitual vs competición';

-- Permite mismo equipo+temporada con dos edificios distintos (p. ej. liga vs UEFA)
ALTER TABLE bridge_team_season_stadium
    DROP CONSTRAINT IF EXISTS bridge_team_season_stadium_canonical_team_id_stadium_id_sea_key;

DROP INDEX IF EXISTS bridge_team_season_stadium_canonical_team_id_stadium_id_sea_key;

CREATE UNIQUE INDEX IF NOT EXISTS ux_bridge_team_stadium_season_ctx
    ON bridge_team_season_stadium (
        canonical_team_id,
        stadium_id,
        season_start,
        season_end,
        usage_context
    );
