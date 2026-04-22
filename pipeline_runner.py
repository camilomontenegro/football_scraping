"""
pipeline_runner.py
==================
Orquestador completo del ETL de fútbol.

Flujo completo:
  1. Transfermarkt  → extract → staging → dims → fact_injuries
  2. SofaScore      → (staging desde raw ya existente) → dim_match
  3. Understat      → extract → staging
  4. StatsBomb      → extract → staging
  5. WhoScored      → staging (desde raw existente, o extract si hay match_ids)
  6. fact_shots     (unifica sofascore + understat + statsbomb)
  7. fact_events    (unifica sofascore + statsbomb + whoscored)
  8. Validaciones de integridad multi-fuente
  9. Métricas finales

Uso:
    # Pipeline completo
    python pipeline_runner.py

    # Solo algunas fuentes
    python pipeline_runner.py --sources transfermarkt sofascore statsbomb

    # Ver qué haría (sin escribir en DB)
    python pipeline_runner.py --dry-run

    # Solo transformaciones (si ya tienes el raw)
    python pipeline_runner.py --skip-extract
"""
from __future__ import annotations

import argparse
import logging
import sys
import os
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from utils.db import engine
from utils.batch import generate_batch_id

# ── Staging loaders ──────────────────────────────────
from staging.load_transfermarkt import run_transfermarkt_loader
from staging.load_sofascore import run_sofascore_loader
from staging.load_understat import run_understat_loader
from staging.load_statsbomb import run_statsbomb_loader
from staging.load_whoscored import run_whoscored_loader

# ── Extract ──────────────────────────────────────────
from extract.transfermarkt_extract import extract_transfermarkt
from extract.understat_extract import run_understat_extract
from extract.statsbomb_extract import run_statsbomb_extract
from extract.sofascore_extract import run_sofascore_extract

# ── Transform: Dimensiones ───────────────────────────
from transform.dim_players import load_dim_players
from transform.dim_teams import load_dim_teams
from transform.dim_seasons import load_dim_season
from transform.dim_injury_types import load_dim_injury_types
from transform.dim_match import load_dim_match
from transform.player_mapping import load_player_mapping
from transform.external_ids import run_external_ids

# ── Transform: Facts ─────────────────────────────────
from transform.fact_injuries import load_fact_injuries
from transform.fact_shots import load_fact_shots
from transform.fact_events import load_fact_events

from sqlalchemy import text


# ─────────────────────────────────────────────────────
# CONFIGURACIÓN GLOBAL (Temporada 2020/2021)
# ─────────────────────────────────────────────────────

# Transfermarkt
TM_LEAGUE_CODE = "ES1"
TM_SEASON      = 2020

# Understat
UNDERSTAT_LEAGUE = "La_Liga"
UNDERSTAT_SEASON = "2020"

# StatsBomb
STATSBOMB_COMPETITION_ID = 11
STATSBOMB_SEASON_ID      = 90

# SofaScore
SOFASCORE_TOURNAMENT_ID = 8
SOFASCORE_SEASON_NAME   = "20/21"
SOFASCORE_RAW_DIR       = "data/raw/sofascore"

# Etiquetas para DB
SEASON_LABEL = "2020/2021"
SEASON_START = 2020
SEASON_END   = 2021

SOURCES_ALL = ["transfermarkt", "sofascore", "understat", "statsbomb", "whoscored"]


# ─────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("pipeline")


# ─────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────

def _step(name: str, fn, *args, dry_run: bool = False, **kwargs):
    """Ejecuta un paso del pipeline con logging y control de errores."""
    log.info("▶  %s", name)
    if dry_run:
        log.info("   [DRY-RUN] omitido")
        return None
    try:
        result = fn(*args, **kwargs)
        log.info("✓  %s → %s", name, result)
        return result
    except Exception as exc:
        log.error("✗  %s → ERROR: %s", name, exc)
        return None


# ─────────────────────────────────────────────────────
# VALIDACIONES MULTI-FUENTE
# ─────────────────────────────────────────────────────

def run_integrity_checks(conn) -> dict:
    """Ejecuta validaciones de integridad sobre todas las tablas."""
    checks = {}

    # Shots sin jugador resuelto
    checks["fact_shots_null_player"] = conn.execute(text("""
        SELECT COUNT(*) FROM fact_shots WHERE player_id IS NULL
    """)).scalar()

    # Events sin jugador resuelto
    checks["fact_events_null_player"] = conn.execute(text("""
        SELECT COUNT(*) FROM fact_events WHERE player_id IS NULL
    """)).scalar()

    # Injuries con fechas inválidas
    checks["fact_injuries_invalid_dates"] = conn.execute(text("""
        SELECT COUNT(*) FROM fact_injuries
        WHERE date_until IS NOT NULL AND date_until < date_from
    """)).scalar()

    # Players sin mapeo transfermarkt
    checks["players_without_tm_mapping"] = conn.execute(text("""
        SELECT COUNT(*) FROM dim_player dp
        LEFT JOIN transfermarkt_player_mapping tm ON tm.player_id = dp.player_id
        WHERE tm.player_id IS NULL
    """)).scalar()

    # Shots por fuente
    sources_shots = conn.execute(text("""
        SELECT data_source, COUNT(*) FROM fact_shots GROUP BY data_source
    """)).fetchall()
    checks["fact_shots_by_source"] = {r[0]: r[1] for r in sources_shots}

    # Events por fuente
    sources_events = conn.execute(text("""
        SELECT data_source, COUNT(*) FROM fact_events GROUP BY data_source
    """)).fetchall()
    checks["fact_events_by_source"] = {r[0]: r[1] for r in sources_events}

    return checks


# ─────────────────────────────────────────────────────
# PIPELINE PRINCIPAL
# ─────────────────────────────────────────────────────

def run_pipeline(
    sources: list[str] | None = None,
    skip_extract: bool = False,
    dry_run: bool = False,
) -> dict:
    start_time = datetime.now()
    batch_id = generate_batch_id()
    sources = sources or SOURCES_ALL
    log.info("=" * 65)
    log.info("PIPELINE START  |  batch=%s  |  sources=%s", batch_id, sources)
    log.info("=" * 65)

    metrics = {
        "batch_id":  batch_id,
        "start_at":  start_time.isoformat(),
        "dry_run":   dry_run,
        "sources":   sources,
        "steps":     {},
        "errors":    [],
    }

    # ══════════════════════════════════════════════
    # BLOQUE 1: EXTRACT (opcional)
    # ══════════════════════════════════════════════
    if not skip_extract:

        if "transfermarkt" in sources:
            log.info("── TRANSFERMARKT EXTRACT (Temporada Completa) ────────")
            stats = _step(
                "transfermarkt_extract",
                extract_transfermarkt,
                league_code=TM_LEAGUE_CODE,
                season=TM_SEASON,
                dry_run=dry_run,
            )
            metrics["steps"]["transfermarkt_extract"] = stats

        if "understat" in sources:
            log.info("── UNDERSTAT EXTRACT (Temporada Completa) ────────────")
            stats = _step(
                "understat_extract",
                run_understat_extract,
                league=UNDERSTAT_LEAGUE,
                season=UNDERSTAT_SEASON,
                teams=None,  # Todos los equipos
                dry_run=dry_run,
            )
            metrics["steps"]["understat_extract"] = stats

        if "statsbomb" in sources:
            log.info("── STATSBOMB EXTRACT ─────────────────────────────────")
            stats = _step(
                "statsbomb_extract",
                run_statsbomb_extract,
                STATSBOMB_COMPETITION_ID,
                STATSBOMB_SEASON_ID,
                dry_run=dry_run,
            )
            metrics["steps"]["statsbomb_extract"] = stats

        if "sofascore" in sources:
            log.info("── SOFASCORE EXTRACT (Temporada Completa) ────────────")
            stats = _step(
                "sofascore_extract_season",
                run_sofascore_extract,
                season_name=SOFASCORE_SEASON_NAME,
                tournament_id=SOFASCORE_TOURNAMENT_ID,
                dry_run=dry_run,
            )
            metrics["steps"]["sofascore_extract_season"] = stats

        # WhoScored: se asume raw ya descargado

    # ══════════════════════════════════════════════
    # BLOQUE 2: STAGING + DIMENSIONES + FACTS
    # ══════════════════════════════════════════════
    with engine.begin() as conn:

        # ── STAGING LAYER ─────────────────────────────
        log.info("── STAGING ───────────────────────────────────────────")

        # ── Transfermarkt staging ─────────────────
        if "transfermarkt" in sources:
            _step("transfermarkt_staging", run_transfermarkt_loader, conn, batch_id=batch_id,
                dry_run=dry_run)

        # ── SofaScore staging ─────────────────────
        if "sofascore" in sources:
            _step("sofascore_staging", run_sofascore_loader, conn, SOFASCORE_RAW_DIR, batch_id,
                dry_run=dry_run)

        # ── Understat staging ─────────────────────
        if "understat" in sources:
            _step("understat_staging", run_understat_loader, conn, dry_run=dry_run)

        # ── StatsBomb staging ─────────────────────
        if "statsbomb" in sources:
            _step("statsbomb_staging", run_statsbomb_loader, conn, dry_run=dry_run)

        # ── WhoScored staging ─────────────────────
        if "whoscored" in sources:
            _step("whoscored_staging", run_whoscored_loader, conn, dry_run=dry_run)


        # ── DIMENSION LAYER ───────────────────────────
        log.info("── DIMENSIONES ───────────────────────────────────────")

        _step("dim_season", load_dim_season, conn, SEASON_LABEL, SEASON_START, SEASON_END,
            dry_run=dry_run)

        if "transfermarkt" in sources or "sofascore" in sources:
            _step("dim_player",     load_dim_players,     conn, dry_run=dry_run)
            _step("dim_team",       load_dim_teams,        conn, dry_run=dry_run)
            _step("dim_injury_type", load_dim_injury_types, conn, dry_run=dry_run)
            _step("player_mapping", load_player_mapping,   conn, dry_run=dry_run)
            _step("external_ids",   run_external_ids,      conn, dry_run=dry_run)

        if "sofascore" in sources:
            _step("dim_match", load_dim_match, conn, dry_run=dry_run)

        # ── FACTS ─────────────────────────────────
        log.info("── FACTS ─────────────────────────────────────────────")

        if "transfermarkt" in sources:
            _step("fact_injuries", load_fact_injuries, conn, dry_run=dry_run)

        _step("fact_shots",  load_fact_shots,  conn, dry_run=dry_run)
        _step("fact_events", load_fact_events, conn, dry_run=dry_run)

        # ══════════════════════════════════════════
        # BLOQUE 3: VALIDACIONES
        # ══════════════════════════════════════════
        if not dry_run:
            log.info("── VALIDACIONES ──────────────────────────────────────")
            checks = run_integrity_checks(conn)
            metrics["integrity"] = checks

            log.info("  fact_shots_null_player  : %d", checks.get("fact_shots_null_player", 0))
            log.info("  fact_events_null_player : %d", checks.get("fact_events_null_player", 0))
            log.info("  fact_injuries_bad_dates : %d", checks.get("fact_injuries_invalid_dates", 0))
            log.info("  shots  por fuente       : %s", checks.get("fact_shots_by_source", {}))
            log.info("  events por fuente       : %s", checks.get("fact_events_by_source", {}))

            # ── Métricas finales ──────────────────
            log.info("── MÉTRICAS FINALES ──────────────────────────────────")
            for table in ["dim_player", "dim_team", "dim_match", "fact_shots", "fact_events", "fact_injuries"]:
                count = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()
                log.info("  %-20s: %d filas", table, count)
                metrics[table] = count

    end_time = datetime.now()
    metrics["end_at"] = end_time.isoformat()
    metrics["duration_s"] = (end_time - start_time).total_seconds()

    log.info("=" * 65)
    log.info("PIPELINE COMPLETED  |  duration=%.1fs", metrics["duration_s"])
    log.info("=" * 65)

    return metrics


# ─────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────

def _parse_args():
    parser = argparse.ArgumentParser(
        description="ETL Pipeline de Fútbol — Transfermarkt, SofaScore, Understat, StatsBomb, WhoScored"
    )
    parser.add_argument(
        "--sources", nargs="+",
        choices=SOURCES_ALL,
        default=SOURCES_ALL,
        help="Fuentes a ejecutar (default: todas)",
    )
    parser.add_argument(
        "--skip-extract", action="store_true",
        help="Omitir extracción (usa raw ya descargado)",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="No escribe en DB, solo muestra qué haría",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    metrics = run_pipeline(
        sources=args.sources,
        skip_extract=args.skip_extract,
        dry_run=args.dry_run,
    )
    sys.exit(0 if not metrics.get("errors") else 1)
