"""
db/sync_teammate_db.py
======================
Script de migración para poner al día una base de datos desactualizada.

Ejecuta todos los cambios de forma IDEMPOTENTE (se puede lanzar muchas
veces sin romper nada). Cubre:

    1. Tablas nuevas:  dim_competition, dim_stadium, player_review
    2. Columnas nuevas en dim_match:  competition_id (FK), attendance,
                       temperature_c, humidity_pct, precipitation_mm,
                       wind_speed_kmh, weather_code
    3. Columnas nuevas en player_review: competition, season,
                       source_team_id, source_team_name
    4. Columnas nuevas en fact_injuries: club_name, club_id_tm, club_slug
    5. Índices faltantes
    6. Vista vw_match_neutral_venue
    7. Seed de dim_competition

Uso:
    # Asegúrate de tener el .env con DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
    python db/sync_teammate_db.py

    # Dry-run: solo muestra lo que haría sin tocar nada
    python db/sync_teammate_db.py --dry-run
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from dotenv import load_dotenv
from sqlalchemy import create_engine, text, inspect

load_dotenv()

DB_HOST     = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT     = os.getenv("DB_PORT", "5432")
DB_NAME     = os.getenv("DB_NAME", "football_db")
DB_USER     = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD")

if not DB_PASSWORD:
    print("ERROR: DB_PASSWORD no definido en .env")
    raise SystemExit(1)

URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"


# ═══════════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════════

def table_exists(conn, name: str) -> bool:
    r = conn.execute(text(
        "SELECT 1 FROM information_schema.tables "
        "WHERE table_schema='public' AND table_name=:t"
    ), {"t": name})
    return r.fetchone() is not None


def column_exists(conn, table: str, column: str) -> bool:
    r = conn.execute(text(
        "SELECT 1 FROM information_schema.columns "
        "WHERE table_schema='public' AND table_name=:t AND column_name=:c"
    ), {"t": table, "c": column})
    return r.fetchone() is not None


def index_exists(conn, name: str) -> bool:
    r = conn.execute(text(
        "SELECT 1 FROM pg_indexes WHERE schemaname='public' AND indexname=:i"
    ), {"i": name})
    return r.fetchone() is not None


def view_exists(conn, name: str) -> bool:
    r = conn.execute(text(
        "SELECT 1 FROM information_schema.views "
        "WHERE table_schema='public' AND table_name=:v"
    ), {"v": name})
    return r.fetchone() is not None


class MigrationLog:
    """Acumula acciones para resumen final."""
    def __init__(self):
        self.actions: list[str] = []
        self.skipped: list[str] = []

    def added(self, msg: str):
        self.actions.append(msg)
        print(f"  ✓ {msg}")

    def skip(self, msg: str):
        self.skipped.append(msg)

    def summary(self):
        print(f"\n{'='*55}")
        print(f"  Cambios aplicados: {len(self.actions)}")
        print(f"  Ya existían:       {len(self.skipped)}")
        print(f"{'='*55}")
        if self.actions:
            print("\n  Detalle de cambios:")
            for a in self.actions:
                print(f"    • {a}")
        print()


# ═══════════════════════════════════════════════════════════════════
# PASO 1: TABLAS NUEVAS
# ═══════════════════════════════════════════════════════════════════

SQL_DIM_COMPETITION = """
CREATE TABLE dim_competition(
    canonical_id SERIAL PRIMARY KEY,
    canonical_name VARCHAR(150) NOT NULL,
    country VARCHAR(80),
    country_code VARCHAR(10),
    id_sofascore INTEGER,
    id_understat VARCHAR(50),
    id_transfermarkt VARCHAR(50),
    id_statsbomb VARCHAR(50),
    id_whoscored INTEGER,
    created_at TIMESTAMP DEFAULT NOW()
);
CREATE UNIQUE INDEX idx_dim_competition_name_unique
    ON dim_competition(canonical_name);
CREATE UNIQUE INDEX idx_dim_competition_transfermarkt_unique
    ON dim_competition(id_transfermarkt) WHERE id_transfermarkt IS NOT NULL;
CREATE UNIQUE INDEX idx_dim_competition_sofascore_unique
    ON dim_competition(id_sofascore) WHERE id_sofascore IS NOT NULL;
CREATE UNIQUE INDEX idx_dim_competition_whoscored_unique
    ON dim_competition(id_whoscored) WHERE id_whoscored IS NOT NULL;
"""

SQL_DIM_STADIUM = """
CREATE TABLE dim_stadium (
    stadium_id            SERIAL PRIMARY KEY,
    canonical_team_id     INTEGER REFERENCES dim_team (canonical_id),
    id_transfermarkt_team INTEGER NOT NULL,
    team_slug             VARCHAR(150),
    valid_from_season     VARCHAR(20) NOT NULL,
    valid_to_season       VARCHAR(20) NOT NULL,
    stadium_name          VARCHAR(200),
    capacity              INTEGER,
    seats_total           INTEGER,
    vip_boxes             SMALLINT,
    built_year            SMALLINT,
    construction_cost     VARCHAR(120),
    owner                 VARCHAR(200),
    operator              VARCHAR(200),
    address               VARCHAR(300),
    city                  VARCHAR(120),
    country               VARCHAR(80),
    surface               VARCHAR(80),
    architect             VARCHAR(200),
    tm_url                VARCHAR(400),
    wikidata_qid          VARCHAR(20),
    latitude              DECIMAL(9,6),
    longitude             DECIMAL(9,6),
    altitude_m            INTEGER,
    timezone              VARCHAR(64),
    wikipedia_url         VARCHAR(500),
    image_url             TEXT,
    is_current            BOOLEAN DEFAULT TRUE,
    data_hash             CHAR(40),
    data_source           VARCHAR(50) DEFAULT 'transfermarkt',
    created_at            TIMESTAMP DEFAULT NOW(),
    updated_at            TIMESTAMP DEFAULT NOW(),
    CHECK (valid_from_season <= valid_to_season)
);
CREATE UNIQUE INDEX ux_stadium_team_validfrom
    ON dim_stadium (id_transfermarkt_team, valid_from_season);
CREATE INDEX idx_stadium_team        ON dim_stadium (canonical_team_id);
CREATE INDEX idx_stadium_team_tm     ON dim_stadium (id_transfermarkt_team);
CREATE INDEX idx_stadium_data_hash   ON dim_stadium (id_transfermarkt_team, data_hash);
CREATE INDEX idx_stadium_name_lower  ON dim_stadium (LOWER(stadium_name));
CREATE INDEX idx_stadium_wikidata_qid ON dim_stadium (wikidata_qid)
    WHERE wikidata_qid IS NOT NULL;
CREATE INDEX idx_stadium_latlon      ON dim_stadium (latitude, longitude)
    WHERE latitude IS NOT NULL AND longitude IS NOT NULL;
"""

SQL_PLAYER_REVIEW = """
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
    reviewed_at TIMESTAMP,
    source_team_id VARCHAR(50),
    source_team_name VARCHAR(150),
    competition VARCHAR(100),
    season VARCHAR(20)
);
CREATE INDEX IF NOT EXISTS idx_player_review_source
    ON player_review (source_system, source_id);
CREATE INDEX IF NOT EXISTS idx_player_review_suggested
    ON player_review (suggested_canonical_id);
CREATE INDEX IF NOT EXISTS idx_player_review_assigned
    ON player_review (canonical_id_assigned);
CREATE INDEX IF NOT EXISTS idx_player_review_unresolved
    ON player_review (resolved) WHERE resolved IS FALSE;
"""

def step_create_tables(conn, log: MigrationLog):
    """Crea tablas que no existan."""
    tables = [
        ("dim_competition",          SQL_DIM_COMPETITION),
        ("dim_stadium",              SQL_DIM_STADIUM),
        ("player_review",            SQL_PLAYER_REVIEW),
    ]
    for name, ddl in tables:
        if table_exists(conn, name):
            log.skip(f"Tabla {name} ya existe")
        else:
            conn.execute(text(ddl))
            log.added(f"Tabla {name} creada")


# ═══════════════════════════════════════════════════════════════════
# PASO 2: COLUMNAS NUEVAS
# ═══════════════════════════════════════════════════════════════════

NEW_COLUMNS: list[tuple[str, str, str]] = [
    # (tabla, columna, definición SQL)

    # --- dim_match: FK a competición ---
    ("dim_match", "competition_id",
     "INTEGER REFERENCES dim_competition(canonical_id)"),

    # --- dim_match: asistencia ---
    ("dim_match", "attendance", "INTEGER"),

    # --- dim_match: meteorología ---
    ("dim_match", "temperature_c",    "DECIMAL(4,1)"),
    ("dim_match", "humidity_pct",     "SMALLINT"),
    ("dim_match", "precipitation_mm", "DECIMAL(5,1)"),
    ("dim_match", "wind_speed_kmh",   "DECIMAL(5,1)"),
    ("dim_match", "weather_code",     "SMALLINT"),

    # --- player_review: contexto de equipo ---
    ("player_review", "source_team_id",   "VARCHAR(50)"),
    ("player_review", "source_team_name", "VARCHAR(150)"),
    ("player_review", "competition",      "VARCHAR(100)"),
    ("player_review", "season",           "VARCHAR(20)"),

    # --- fact_injuries: club durante la lesión ---
    ("fact_injuries", "club_name",   "VARCHAR(200)"),
    ("fact_injuries", "club_id_tm",  "INTEGER"),
    ("fact_injuries", "club_slug",   "VARCHAR(150)"),
]


def step_add_columns(conn, log: MigrationLog):
    """Añade columnas que no existan."""
    for table, col, definition in NEW_COLUMNS:
        if not table_exists(conn, table):
            log.skip(f"Tabla {table} no existe, columna {col} omitida")
            continue
        if column_exists(conn, table, col):
            log.skip(f"{table}.{col} ya existe")
        else:
            conn.execute(text(
                f"ALTER TABLE {table} ADD COLUMN {col} {definition}"
            ))
            log.added(f"{table}.{col} añadida ({definition})")


# ═══════════════════════════════════════════════════════════════════
# PASO 3: ÍNDICES FALTANTES
# ═══════════════════════════════════════════════════════════════════

NEW_INDEXES: list[tuple[str, str]] = [
    ("idx_dim_match_competition_id",
     "CREATE INDEX IF NOT EXISTS idx_dim_match_competition_id "
     "ON dim_match(competition_id)"),
]


def step_add_indexes(conn, log: MigrationLog):
    """Crea índices que no existan."""
    for name, ddl in NEW_INDEXES:
        if index_exists(conn, name):
            log.skip(f"Índice {name} ya existe")
        else:
            conn.execute(text(ddl))
            log.added(f"Índice {name} creado")


# ═══════════════════════════════════════════════════════════════════
# PASO 4: VISTAS
# ═══════════════════════════════════════════════════════════════════

SQL_VIEW_NEUTRAL = """
CREATE VIEW vw_match_neutral_venue AS
WITH neutral_comps AS (
    SELECT canonical_id, canonical_name FROM dim_competition
    WHERE canonical_name IN (
        'Champions League',
        'Europa League',
        'Europa Conference League',
        'FIFA World Cup',
        'European Championship',
        'Copa America',
        'FIFA Club World Cup'
    )
)
SELECT
    m.match_id,
    m.match_date,
    m.competition_id,
    m.home_team_id,
    m.away_team_id,
    (m.competition_id IN (SELECT canonical_id FROM neutral_comps)
        AND EXISTS (
            SELECT 1 FROM neutral_comps nc
            WHERE nc.canonical_id = m.competition_id
              AND nc.canonical_name IN (
                'FIFA World Cup', 'European Championship',
                'Copa America', 'FIFA Club World Cup'
              )
        )
    ) AS is_neutral_candidate
FROM dim_match m;
"""


def step_create_views(conn, log: MigrationLog):
    """Crea vistas solo si no existen (no sobreescribe)."""
    if not table_exists(conn, "dim_competition"):
        log.skip("Vista vw_match_neutral_venue omitida (requiere dim_competition)")
        return
    if view_exists(conn, "vw_match_neutral_venue"):
        log.skip("Vista vw_match_neutral_venue ya existe")
        return
    conn.execute(text(SQL_VIEW_NEUTRAL))
    log.added("Vista vw_match_neutral_venue creada")


# ═══════════════════════════════════════════════════════════════════
# PASO 2b: COLUMNAS ELIMINADAS DEL SCHEMA
# ═══════════════════════════════════════════════════════════════════

_DROP_STADIUM_COLUMNS_SQL = Path(__file__).resolve().parent / "migrations" / "drop_stadium_sparse_columns.sql"


def step_drop_removed_columns(conn, log: MigrationLog):
    """Elimina columnas sparse de dim_stadium retiradas del schema definitivo."""
    if not _DROP_STADIUM_COLUMNS_SQL.is_file() or not table_exists(conn, "dim_stadium"):
        return
    for line in _DROP_STADIUM_COLUMNS_SQL.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line.startswith("ALTER TABLE"):
            continue
        col = line.split("EXISTS")[-1].strip().rstrip(";")
        if column_exists(conn, "dim_stadium", col):
            conn.execute(text(line))
            log.added(f"dim_stadium.{col} eliminada")
        else:
            log.skip(f"dim_stadium.{col} ya ausente")


# ═══════════════════════════════════════════════════════════════════
# PASO 5: SEED dim_competition
# ═══════════════════════════════════════════════════════════════════

def step_seed_competitions(conn, log: MigrationLog):
    """Inserta competiciones canónicas si la tabla está vacía."""
    if not table_exists(conn, "dim_competition"):
        return
    r = conn.execute(text("SELECT COUNT(*) FROM dim_competition"))
    count = r.scalar()
    if count > 0:
        log.skip(f"dim_competition ya tiene {count} filas")
        return
    try:
        from loaders.competition_loader import load_competitions
        inserted = load_competitions(conn)
        log.added(f"dim_competition sembrada ({inserted} competiciones)")
    except Exception as e:
        log.added(f"dim_competition seed fallido: {e}")


# ═══════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════

def run(dry_run: bool = False):
    engine = create_engine(URL)
    log = MigrationLog()

    print("=" * 55)
    print(f"  Sincronización de schema — {DB_NAME}@{DB_HOST}")
    if dry_run:
        print("  *** MODO DRY-RUN: no se aplican cambios ***")
    print("=" * 55)
    print()

    # Verificar que la BD existe y tiene las tablas base
    with engine.connect() as conn:
        if not table_exists(conn, "dim_team"):
            print("ERROR: la tabla dim_team no existe.")
            print("       Este script es para actualizar una BD existente,")
            print("       no para crearla desde cero. Usa db/setup_db.py.")
            raise SystemExit(1)

    if dry_run:
        # En dry-run abrimos conexión sin transacción real
        with engine.connect() as conn:
            print("[1/5] Tablas nuevas...")
            for name, _ in [
                ("dim_competition", None), ("dim_stadium", None),
                ("player_review", None),
            ]:
                if table_exists(conn, name):
                    log.skip(f"Tabla {name} ya existe")
                else:
                    log.added(f"Tabla {name} SE CREARÍA")

            print("\n[2/5] Columnas nuevas...")
            for table, col, definition in NEW_COLUMNS:
                if not table_exists(conn, table):
                    log.skip(f"Tabla {table} no existe")
                elif column_exists(conn, table, col):
                    log.skip(f"{table}.{col} ya existe")
                else:
                    log.added(f"{table}.{col} SE AÑADIRÍA ({definition})")

            print("\n[2b/5] Columnas eliminadas del schema...")
            if _DROP_STADIUM_COLUMNS_SQL.is_file() and table_exists(conn, "dim_stadium"):
                for line in _DROP_STADIUM_COLUMNS_SQL.read_text(encoding="utf-8").splitlines():
                    line = line.strip()
                    if not line.startswith("ALTER TABLE"):
                        continue
                    col = line.split("EXISTS")[-1].strip().rstrip(";")
                    if column_exists(conn, "dim_stadium", col):
                        log.added(f"dim_stadium.{col} SE ELIMINARÍA")
                    else:
                        log.skip(f"dim_stadium.{col} ya ausente")

            print("\n[3/5] Índices...")
            for name, _ in NEW_INDEXES:
                if index_exists(conn, name):
                    log.skip(f"Índice {name} ya existe")
                else:
                    log.added(f"Índice {name} SE CREARÍA")

            print("\n[4/5] Vistas...")
            log.added("Vista vw_match_neutral_venue SE CREARÍA/ACTUALIZARÍA")

            print("\n[5/5] Seed dim_competition...")
            if table_exists(conn, "dim_competition"):
                r = conn.execute(text("SELECT COUNT(*) FROM dim_competition"))
                if r.scalar() > 0:
                    log.skip("dim_competition ya tiene datos")
                else:
                    log.added("dim_competition SE SEMBRARÍA")

        log.summary()
        print("  Ejecuta sin --dry-run para aplicar los cambios.\n")
        return

    # Ejecución real dentro de una transacción
    with engine.begin() as conn:
        print("[1/5] Tablas nuevas...")
        step_create_tables(conn, log)

        print("\n[2/5] Columnas nuevas...")
        step_add_columns(conn, log)

        print("\n[2b/5] Columnas eliminadas del schema...")
        step_drop_removed_columns(conn, log)

        print("\n[3/5] Índices...")
        step_add_indexes(conn, log)

        print("\n[4/5] Vistas...")
        step_create_views(conn, log)

        print("\n[5/5] Seed dim_competition...")
        step_seed_competitions(conn, log)

    log.summary()
    print("  Base de datos sincronizada correctamente.\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Sincroniza el schema de una BD desactualizada con el actual.",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Muestra los cambios que se harían sin aplicarlos.",
    )
    args = parser.parse_args()
    run(dry_run=args.dry_run)
