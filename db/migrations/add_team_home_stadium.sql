-- Sede habitual actual del equipo → edificio en dim_stadium_master.
-- Se mantiene vía scripts/sync_team_home_stadium.py (desde bridge).

ALTER TABLE dim_team
    ADD COLUMN IF NOT EXISTS home_stadium_master_id INTEGER
        REFERENCES dim_stadium_master (stadium_id);

CREATE INDEX IF NOT EXISTS idx_team_home_stadium
    ON dim_team (home_stadium_master_id)
    WHERE home_stadium_master_id IS NOT NULL;

COMMENT ON COLUMN dim_team.home_stadium_master_id IS
    'FK a dim_stadium_master: sede habitual actual (última temporada en bridge).';
