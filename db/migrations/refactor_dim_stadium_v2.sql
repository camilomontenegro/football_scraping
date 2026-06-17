-- ═══════════════════════════════════════════════════════════════════════
-- refactor_dim_stadium_v2.sql
-- ─────────────────────────────────────────────────────────────────────
-- Refactoriza dim_stadium (SCD2 monolítica) en 3 tablas:
--
--   1. dim_stadium_master     → 1 fila por edificio físico
--   2. dim_stadium_names      → histórico de nombres (SCD2 real)
--   3. bridge_team_stadium    → qué equipo jugó en qué estadio por temporada
--
-- Clave de deduplicación: wikidata_qid (fallback: team + solapamiento temporal)
--
-- IMPORTANTE: este script CREA las tablas nuevas y MIGRA los datos desde
-- dim_stadium. NO borra dim_stadium — eso se hace manualmente tras verificar.
--
-- Uso:
--   psql -U postgres -d football_db -f db/migrations/refactor_dim_stadium_v2.sql
-- ═══════════════════════════════════════════════════════════════════════

BEGIN;

-- ══════════════════════════════════════════════════════════════════════
-- 1. dim_stadium_master — UN registro por edificio físico
-- ══════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS dim_stadium_master (
    stadium_id          SERIAL PRIMARY KEY,

    -- Identificadores externos
    wikidata_qid        VARCHAR(20) UNIQUE,       -- clave natural de dedup (Q1234567)
    tm_url              VARCHAR(400),              -- URL Transfermarkt (puede cambiar)

    -- Nombre actual (el que se muestra por defecto)
    canonical_name      VARCHAR(200) NOT NULL,

    -- Datos físicos del edificio (NO se duplican por cambio de nombre)
    capacity            INTEGER,
    seats_total         INTEGER,
    vip_boxes           SMALLINT,
    built_year          SMALLINT,
    construction_cost   VARCHAR(120),
    owner               VARCHAR(200),
    operator            VARCHAR(200),
    address             VARCHAR(300),
    city                VARCHAR(120),
    country             VARCHAR(80),
    surface             VARCHAR(80),
    architect           VARCHAR(200),

    -- Geolocalización
    latitude            DECIMAL(9,6),
    longitude           DECIMAL(9,6),
    altitude_m          INTEGER,
    timezone            VARCHAR(64),

    -- Enlaces
    wikipedia_url       VARCHAR(500),
    image_url           TEXT,

    -- Estado
    is_active           BOOLEAN DEFAULT TRUE,     -- FALSE si demolido/abandonado

    -- Metadatos
    data_source         VARCHAR(50) DEFAULT 'transfermarkt',
    created_at          TIMESTAMP DEFAULT NOW(),
    updated_at          TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_stm_name_lower
    ON dim_stadium_master (LOWER(canonical_name));
CREATE INDEX IF NOT EXISTS idx_stm_city
    ON dim_stadium_master (LOWER(city));
CREATE INDEX IF NOT EXISTS idx_stm_country
    ON dim_stadium_master (country);


-- ══════════════════════════════════════════════════════════════════════
-- 2. dim_stadium_names — Histórico de nombres (SCD2 real)
-- ══════════════════════════════════════════════════════════════════════
-- Cada cambio de nombre = 1 fila. También captura cambios de capacidad
-- asociados a reformas (ej: ampliación del Bernabéu).

CREATE TABLE IF NOT EXISTS dim_stadium_names (
    name_id             SERIAL PRIMARY KEY,
    stadium_id          INTEGER NOT NULL REFERENCES dim_stadium_master (stadium_id) ON DELETE CASCADE,

    stadium_name        VARCHAR(200) NOT NULL,
    capacity            INTEGER,                  -- NULL = sin cambio respecto a master

    valid_from_season   VARCHAR(20) NOT NULL,     -- '2020/2021'
    valid_to_season     VARCHAR(20) NOT NULL,     -- '2024/2025'
    is_current          BOOLEAN DEFAULT FALSE,

    created_at          TIMESTAMP DEFAULT NOW(),

    CHECK (valid_from_season <= valid_to_season)
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_stnames_stadium_from
    ON dim_stadium_names (stadium_id, valid_from_season);
CREATE INDEX IF NOT EXISTS idx_stnames_stadium
    ON dim_stadium_names (stadium_id);
CREATE INDEX IF NOT EXISTS idx_stnames_name_lower
    ON dim_stadium_names (LOWER(stadium_name));


-- ══════════════════════════════════════════════════════════════════════
-- 3. bridge_team_stadium — Relación equipo↔estadio por temporada
-- ══════════════════════════════════════════════════════════════════════
-- Granularidad: 1 fila por (equipo, estadio, temporada).
-- Permite rastrear mudanzas (Calderón → Wanda) y venues neutrales.

CREATE TABLE IF NOT EXISTS bridge_team_stadium (
    id                  SERIAL PRIMARY KEY,
    canonical_team_id   INTEGER NOT NULL REFERENCES dim_team (canonical_id) ON DELETE CASCADE,
    stadium_id          INTEGER NOT NULL REFERENCES dim_stadium_master (stadium_id) ON DELETE CASCADE,

    season              VARCHAR(20) NOT NULL,     -- '2024/2025'
    is_home             BOOLEAN DEFAULT TRUE,     -- FALSE = venue neutral/temporal

    -- IDs de Transfermarkt para trazabilidad
    id_transfermarkt_team INTEGER,
    team_slug           VARCHAR(150),

    created_at          TIMESTAMP DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_bts_team_stadium_season
    ON bridge_team_stadium (canonical_team_id, stadium_id, season);
CREATE INDEX IF NOT EXISTS idx_bts_team
    ON bridge_team_stadium (canonical_team_id);
CREATE INDEX IF NOT EXISTS idx_bts_stadium
    ON bridge_team_stadium (stadium_id);
CREATE INDEX IF NOT EXISTS idx_bts_season
    ON bridge_team_stadium (season);


-- ══════════════════════════════════════════════════════════════════════
-- 4. MIGRACIÓN DE DATOS desde dim_stadium
-- ══════════════════════════════════════════════════════════════════════

-- 4a. Poblar dim_stadium_master
-- Agrupamos por wikidata_qid (cuando existe).
-- Para los que no tienen QID, agrupamos por (canonical_team_id, city).
-- Dentro de cada grupo, tomamos los datos más recientes (is_current=TRUE
-- o valid_to_season más alto).

INSERT INTO dim_stadium_master (
    wikidata_qid, tm_url, canonical_name,
    capacity, seats_total, vip_boxes, built_year, construction_cost,
    owner, operator, address, city, country, surface, architect,
    latitude, longitude, altitude_m, timezone,
    wikipedia_url, image_url, is_active, data_source
)
SELECT DISTINCT ON (COALESCE(wikidata_qid, 'no_qid_' || canonical_team_id::text || '_' || COALESCE(city,'')))
    wikidata_qid,
    tm_url,
    stadium_name AS canonical_name,
    capacity, seats_total, vip_boxes, built_year, construction_cost,
    owner, operator, address, city, country, surface, architect,
    latitude, longitude, altitude_m, timezone,
    wikipedia_url, image_url,
    is_current AS is_active,
    data_source
FROM dim_stadium
ORDER BY
    COALESCE(wikidata_qid, 'no_qid_' || canonical_team_id::text || '_' || COALESCE(city,'')),
    is_current DESC NULLS LAST,
    valid_to_season DESC;


-- 4b. Poblar dim_stadium_names
-- Cada fila de dim_stadium vieja = una entrada en el histórico de nombres.
-- Vinculamos al master por wikidata_qid (o fallback).

INSERT INTO dim_stadium_names (
    stadium_id, stadium_name, capacity,
    valid_from_season, valid_to_season, is_current
)
SELECT
    sm.stadium_id,
    ds.stadium_name,
    ds.capacity,
    ds.valid_from_season,
    ds.valid_to_season,
    ds.is_current
FROM dim_stadium ds
JOIN dim_stadium_master sm
    ON sm.wikidata_qid IS NOT NULL
   AND sm.wikidata_qid = ds.wikidata_qid
WHERE ds.stadium_name IS NOT NULL

UNION ALL

-- Fallback para los que no tienen wikidata_qid
SELECT
    sm.stadium_id,
    ds.stadium_name,
    ds.capacity,
    ds.valid_from_season,
    ds.valid_to_season,
    ds.is_current
FROM dim_stadium ds
JOIN dim_stadium_master sm
    ON sm.wikidata_qid IS NULL
   AND sm.canonical_name = ds.stadium_name
   AND COALESCE(sm.city,'') = COALESCE(ds.city,'')
WHERE ds.wikidata_qid IS NULL
  AND ds.stadium_name IS NOT NULL;


-- 4c. Poblar bridge_team_stadium
-- Expandimos los rangos SCD2 en filas por temporada.

INSERT INTO bridge_team_stadium (
    canonical_team_id, stadium_id, season,
    is_home, id_transfermarkt_team, team_slug
)
SELECT
    ds.canonical_team_id,
    sm.stadium_id,
    (y::text || '/' || (y + 1)::text) AS season,
    TRUE AS is_home,
    ds.id_transfermarkt_team,
    ds.team_slug
FROM dim_stadium ds
JOIN dim_stadium_master sm
    ON (sm.wikidata_qid IS NOT NULL AND sm.wikidata_qid = ds.wikidata_qid)
    OR (sm.wikidata_qid IS NULL AND sm.canonical_name = ds.stadium_name AND COALESCE(sm.city,'') = COALESCE(ds.city,''))
CROSS JOIN LATERAL generate_series(
    CAST(SPLIT_PART(ds.valid_from_season, '/', 1) AS INTEGER),
    CAST(SPLIT_PART(ds.valid_to_season,   '/', 1) AS INTEGER)
) AS y
WHERE ds.canonical_team_id IS NOT NULL
ON CONFLICT (canonical_team_id, stadium_id, season) DO NOTHING;


-- ══════════════════════════════════════════════════════════════════════
-- 5. Re-apuntar dim_match.stadium_id al nuevo master
-- ══════════════════════════════════════════════════════════════════════
-- Creamos columna nueva, mapeamos los IDs viejos → nuevos, y luego
-- renombramos. NOTA: esto se hace en un paso posterior con Python
-- porque requiere mapeo old_id → new_id que depende de la migración.

-- Por ahora solo documentamos el mapping:
-- dim_match.stadium_id (viejo) → dim_stadium.wikidata_qid → dim_stadium_master.stadium_id (nuevo)

COMMIT;

-- ══════════════════════════════════════════════════════════════════════
-- NOTAS POST-MIGRACIÓN (ejecutar con Python)
-- ══════════════════════════════════════════════════════════════════════
--
-- 1. Crear mapping old→new:
--    SELECT ds.stadium_id AS old_id, sm.stadium_id AS new_id
--    FROM dim_stadium ds
--    JOIN dim_stadium_master sm ON sm.wikidata_qid = ds.wikidata_qid
--                               OR (sm.canonical_name = ds.stadium_name AND sm.city = ds.city);
--
-- 2. Actualizar dim_match:
--    ALTER TABLE dim_match ADD COLUMN stadium_master_id INTEGER REFERENCES dim_stadium_master(stadium_id);
--    UPDATE dim_match SET stadium_master_id = mapping.new_id FROM mapping WHERE dim_match.stadium_id = mapping.old_id;
--
-- 3. Verificar, luego:
--    ALTER TABLE dim_match DROP COLUMN stadium_id;
--    ALTER TABLE dim_match RENAME COLUMN stadium_master_id TO stadium_id;
--
-- 4. DROP TABLE dim_stadium;  -- solo cuando todo esté verificado
