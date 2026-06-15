-- MIGRATION: add fact_transfers and fact_market_value tables.
-- Idempotent — safe to re-run.

-- ══════════════════════════════════════════════════════════
-- fact_transfers: historial de fichajes por jugador
-- ══════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS fact_transfers (
    transfer_id   SERIAL PRIMARY KEY,
    -- relación obligatorio con jugador. Cada registro de valor de transferencia  debe tener un jugador asociado.
    player_id     INTEGER NOT NULL REFERENCES dim_player (canonical_id),

    -- Temporada del fichaje (formato DB: 'YYYY/YYYY')
    season        VARCHAR(20),
    transfer_date DATE,

    -- Equipos origen / destino (referencia a dim_team si existe)
    from_team_id  INTEGER REFERENCES dim_team (canonical_id),
    from_team_name VARCHAR(200),
    to_team_id    INTEGER REFERENCES dim_team (canonical_id),
    to_team_name  VARCHAR(200),

    -- Datos económicos
    fee_raw       VARCHAR(100),   -- texto original: "25 mill. €", "Cesión", "Libre"
    fee_euros     BIGINT,         -- valor numérico en euros (NULL si cesión/libre/desconocido)
    fee_currency  VARCHAR(10) DEFAULT '€',

    -- Tipo de operación
    transfer_type VARCHAR(50),    -- 'transfer', 'loan', 'free', 'end_of_loan', 'unknown'
    is_loan       BOOLEAN DEFAULT FALSE,

    -- IDs de Transfermarkt (para deduplicación y trazabilidad)
    id_tm_from_team INTEGER,
    id_tm_to_team   INTEGER,

    created_at    TIMESTAMP DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_transfers_unique
    ON fact_transfers (player_id, season, transfer_date, COALESCE(id_tm_from_team, -1), COALESCE(id_tm_to_team, -1));

CREATE INDEX IF NOT EXISTS idx_transfers_player   ON fact_transfers (player_id);
CREATE INDEX IF NOT EXISTS idx_transfers_season    ON fact_transfers (season);
CREATE INDEX IF NOT EXISTS idx_transfers_from_team ON fact_transfers (from_team_id);
CREATE INDEX IF NOT EXISTS idx_transfers_to_team   ON fact_transfers (to_team_id);


-- ══════════════════════════════════════════════════════════
-- fact_market_value: serie temporal de valor de mercado
-- ══════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS fact_market_value (
    mv_id         SERIAL PRIMARY KEY,
    -- relación obligatorio con jugador. Cada registro de valor de mercado debe tener un jugador asociado.
    player_id     INTEGER NOT NULL REFERENCES dim_player (canonical_id),

    -- Fecha de la valoración
    value_date    DATE NOT NULL,

    -- Valor de mercado
    market_value  BIGINT NOT NULL,        -- en euros
    market_value_raw VARCHAR(100),        -- texto original: "80 mill. €"

    -- Club en ese momento
    -- relacion opcional con dim_team. Como  habra equipos que no estén en dim_team , habra registros de valor de mercado sin equipo asociado
    club_id       INTEGER REFERENCES dim_team (canonical_id),
    club_name     VARCHAR(200),
    id_tm_club    INTEGER,                -- id de TM para trazabilidad

    created_at    TIMESTAMP DEFAULT NOW()
);

-- Un jugador puede tener múltiples valoraciones a lo largo del tiempo, pero no debería haber duplicados para el mismo jugador en la misma fecha. Por eso usamos un índice único que combina player_id y value_date.
CREATE UNIQUE INDEX IF NOT EXISTS ux_market_value_unique
    ON fact_market_value (player_id, value_date);

CREATE INDEX IF NOT EXISTS idx_market_value_player ON fact_market_value (player_id);
CREATE INDEX IF NOT EXISTS idx_market_value_date   ON fact_market_value (value_date);
CREATE INDEX IF NOT EXISTS idx_market_value_club   ON fact_market_value (club_id);
