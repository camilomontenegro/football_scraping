import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging
from sqlalchemy import text

from utils.db import engine

from transform.dim_players import load_dim_players
from transform.dim_teams import load_dim_teams
from transform.dim_seasons import load_dim_season
from transform.dim_injury_types import load_dim_injury_types
from transform.player_mapping import load_player_mapping
from transform.external_ids import run_external_ids
from transform.fact_injuries import load_fact_injuries


logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s - %(message)s'
)


def run_pipeline():
    logging.info("START ETL PIPELINE")

    with engine.begin() as conn:

        # ─────────────────────────────
        # DIMENSION / MDM LAYER
        # ─────────────────────────────
        steps = [
            ("dim_player", load_dim_players),
            ("dim_team", load_dim_teams),
            ("dim_season", lambda c: load_dim_season(c, "2020/2021", 2020, 2021)),
            ("dim_injury_type", load_dim_injury_types),
            ("player_mapping", load_player_mapping),
            ("external_ids", run_external_ids),
        ]

        for name, step in steps:
            logging.info(f"Ejecutando: {name}")
            try:
                result = step(conn)
                logging.info(f"Completado: {name} | resultado: {result}")
            except Exception as e:
                logging.error(f"ERROR en {name}: {e}")
                raise

        # ─────────────────────────────
        # VALIDACIÓN MDM
        # ─────────────────────────────
        logging.info("Validando integridad MDM...")

        missing_players = conn.execute(text("""
            SELECT COUNT(*)
            FROM stg_transfermarkt_injuries i
            LEFT JOIN transfermarkt_player_mapping pm
                ON pm.player_id_tm = i.player_id_tm
            WHERE pm.player_id IS NULL
        """)).scalar()

        missing_teams = conn.execute(text("""
            SELECT COUNT(*)
            FROM stg_transfermarkt_injuries i
            JOIN stg_transfermarkt_players tm
                ON tm.id_transfermarkt = i.player_id_tm
            LEFT JOIN dim_team dt
                ON LOWER(dt.name_canonical) = LOWER(tm.team_name)
            WHERE dt.team_id IS NULL
        """)).scalar()

        logging.info(f"Missing player mappings: {missing_players}")
        logging.info(f"Missing team mappings: {missing_teams}")

        if missing_players > 0 or missing_teams > 0:
            raise Exception("MDM Integrity check failed: missing mappings")

        # ─────────────────────────────
        # FACT LAYER
        # ─────────────────────────────
        logging.info("Ejecutando: fact_injuries")

        try:
            rows = load_fact_injuries(conn)
        except Exception as e:
            logging.error(f"Error en fact_injuries: {e}")
            raise

        logging.info(f"Completado: fact_injuries | filas: {rows}")

        # ─────────────────────────────
        # DATA QUALITY CHECKS
        # ─────────────────────────────
        logging.info("Ejecutando checks de calidad...")

        invalid_dates = conn.execute(text("""
            SELECT COUNT(*)
            FROM fact_injuries
            WHERE date_until IS NOT NULL
            AND date_until < date_from
        """)).scalar()

        orphan_players = conn.execute(text("""
            SELECT COUNT(*)
            FROM fact_injuries fi
            LEFT JOIN dim_player dp ON dp.player_id = fi.player_id
            WHERE dp.player_id IS NULL
        """)).scalar()

        logging.info(f"Invalid date ranges: {invalid_dates}")
        logging.info(f"Orphan player references: {orphan_players}")

        if invalid_dates > 0 or orphan_players > 0:
            raise Exception("Data quality validation failed")

        # ─────────────────────────────
        # FINAL METRICS
        # ─────────────────────────────
        total_injuries = conn.execute(text("""
            SELECT COUNT(*) FROM fact_injuries
        """)).scalar()

        total_players = conn.execute(text("""
            SELECT COUNT(*) FROM dim_player
        """)).scalar()

        total_teams = conn.execute(text("""
            SELECT COUNT(*) FROM dim_team
        """)).scalar()

        logging.info("=" * 50)
        logging.info("PIPELINE SUMMARY")
        logging.info(f"Players: {total_players}")
        logging.info(f"Teams: {total_teams}")
        logging.info(f"Injuries: {total_injuries}")
        logging.info("=" * 50)

    logging.info("PIPELINE COMPLETED SUCCESSFULLY")


if __name__ == "__main__":
    run_pipeline()