"""
Elimina duplicados de La Liga 2025/2026 en dim_match.

Problema: 383 filas con id_sofascore + 380 filas solo WhoScored (mismo partido
fecha+local+visitante) = 763 filas. Este script fusiona cada par en la fila
SofaScore (master), mueve fact_shots/fact_events y borra la fila WS duplicada.

Uso:
    python -m scripts.cleanup_la_liga_duplicate_matches --dry-run
    python -m scripts.cleanup_la_liga_duplicate_matches
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from sqlalchemy import text

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from loaders.common import engine

log = logging.getLogger(__name__)

TM_CODE = "ES1"
SEASON = "2025/2026"


def _competition_id(conn) -> int:
    cid = conn.execute(
        text("SELECT canonical_id FROM dim_competition WHERE id_transfermarkt = :c"),
        {"c": TM_CODE},
    ).scalar()
    if cid is None:
        raise RuntimeError("No se encontró La Liga (id_transfermarkt=ES1) en dim_competition")
    return int(cid)


def _find_merge_pairs(conn, competition_id: int) -> list[tuple[int, int]]:
    """Devuelve (keeper_match_id, dup_match_id) — keeper tiene id_sofascore."""
    rows = conn.execute(
        text("""
            SELECT
                MAX(m.match_id) FILTER (WHERE m.id_sofascore IS NOT NULL) AS keeper_id,
                MAX(m.match_id) FILTER (WHERE m.id_sofascore IS NULL AND m.id_whoscored IS NOT NULL) AS dup_id
            FROM dim_match m
            WHERE m.competition_id = :cid
              AND m.season = :season
              AND m.match_date IS NOT NULL
              AND m.home_team_id IS NOT NULL
              AND m.away_team_id IS NOT NULL
            GROUP BY m.match_date, m.home_team_id, m.away_team_id
            HAVING COUNT(*) > 1
               AND COUNT(*) FILTER (WHERE m.id_sofascore IS NOT NULL) = 1
               AND COUNT(*) FILTER (WHERE m.id_sofascore IS NULL AND m.id_whoscored IS NOT NULL) >= 1
        """),
        {"cid": competition_id, "season": SEASON},
    ).fetchall()

    pairs: list[tuple[int, int]] = []
    for keeper_id, dup_id in rows:
        if keeper_id is None or dup_id is None:
            continue
        if int(keeper_id) == int(dup_id):
            continue
        pairs.append((int(keeper_id), int(dup_id)))
    return pairs


def _reassign_facts(conn, keeper_id: int, dup_id: int, dry_run: bool) -> tuple[int, int]:
    """Mueve hechos de dup → keeper; elimina conflictos de unicidad."""
    if dry_run:
        ev = conn.execute(
            text("SELECT COUNT(*) FROM fact_events WHERE match_id = :d"),
            {"d": dup_id},
        ).scalar()
        sh = conn.execute(
            text("SELECT COUNT(*) FROM fact_shots WHERE match_id = :d"),
            {"d": dup_id},
        ).scalar()
        return int(ev or 0), int(sh or 0)

    # Eventos: borrar en dup lo que ya existiría en keeper tras el UPDATE
    conn.execute(
        text("""
            DELETE FROM fact_events fe_dup
            WHERE fe_dup.match_id = :dup
              AND EXISTS (
                SELECT 1 FROM fact_events fe_k
                WHERE fe_k.match_id = :keeper
                  AND fe_k.player_id = fe_dup.player_id
                  AND fe_k.event_type = fe_dup.event_type
                  AND fe_k.minute IS NOT DISTINCT FROM fe_dup.minute
                  AND COALESCE(fe_k.second, -1) = COALESCE(fe_dup.second, -1)
                  AND COALESCE(fe_k.x, -1.0) = COALESCE(fe_dup.x, -1.0)
                  AND COALESCE(fe_k.y, -1.0) = COALESCE(fe_dup.y, -1.0)
                  AND fe_k.data_source = fe_dup.data_source
              )
        """),
        {"keeper": keeper_id, "dup": dup_id},
    )
    ev_moved = conn.execute(
        text("""
            UPDATE fact_events SET match_id = :keeper WHERE match_id = :dup
        """),
        {"keeper": keeper_id, "dup": dup_id},
    ).rowcount

    conn.execute(
        text("""
            DELETE FROM fact_shots fs_dup
            WHERE fs_dup.match_id = :dup
              AND EXISTS (
                SELECT 1 FROM fact_shots fs_k
                WHERE fs_k.match_id = :keeper
                  AND fs_k.player_id = fs_dup.player_id
                  AND fs_k.minute IS NOT DISTINCT FROM fs_dup.minute
                  AND COALESCE(fs_k.x, -1.0) = COALESCE(fs_dup.x, -1.0)
                  AND COALESCE(fs_k.y, -1.0) = COALESCE(fs_dup.y, -1.0)
                  AND fs_k.data_source = fs_dup.data_source
              )
        """),
        {"keeper": keeper_id, "dup": dup_id},
    )
    sh_moved = conn.execute(
        text("""
            UPDATE fact_shots SET match_id = :keeper WHERE match_id = :dup
        """),
        {"keeper": keeper_id, "dup": dup_id},
    ).rowcount

    return int(ev_moved or 0), int(sh_moved or 0)


def _merge_match_row(conn, keeper_id: int, dup_id: int, dry_run: bool) -> None:
    if dry_run:
        return

    dup = conn.execute(
        text("""
            SELECT id_whoscored, attendance, home_score, away_score, match_date
            FROM dim_match WHERE match_id = :dup
        """),
        {"dup": dup_id},
    ).mappings().one()

    # Liberar índices únicos en la fila duplicada antes de copiar IDs al keeper
    conn.execute(
        text("""
            UPDATE dim_match
            SET id_whoscored = NULL, id_understat = NULL, id_sofascore = NULL
            WHERE match_id = :dup
        """),
        {"dup": dup_id},
    )

    ws_id = dup["id_whoscored"]
    if ws_id is not None:
        taken = conn.execute(
            text("""
                SELECT 1 FROM dim_match
                WHERE id_whoscored = :ws AND match_id != :keeper
                LIMIT 1
            """),
            {"ws": ws_id, "keeper": keeper_id},
        ).scalar()
        if not taken:
            conn.execute(
                text("""
                    UPDATE dim_match
                    SET id_whoscored = :ws
                    WHERE match_id = :keeper AND id_whoscored IS NULL
                """),
                {"ws": ws_id, "keeper": keeper_id},
            )

    conn.execute(
        text("""
            UPDATE dim_match
            SET
                attendance = COALESCE(attendance, :att),
                home_score = COALESCE(home_score, :hs),
                away_score = COALESCE(away_score, :as),
                match_date = COALESCE(match_date, CAST(:md AS DATE))
            WHERE match_id = :keeper
        """),
        {
            "keeper": keeper_id,
            "att": dup["attendance"],
            "hs": dup["home_score"],
            "as": dup["away_score"],
            "md": dup["match_date"],
        },
    )

    conn.execute(
        text("DELETE FROM dim_match WHERE match_id = :dup"),
        {"dup": dup_id},
    )


def run(dry_run: bool) -> None:
    with engine.connect() as conn:
        cid = _competition_id(conn)
        before = conn.execute(
            text(
                "SELECT COUNT(*) FROM dim_match WHERE competition_id = :c AND season = :s"
            ),
            {"c": cid, "s": SEASON},
        ).scalar()
        pairs = _find_merge_pairs(conn, cid)

    log.info("La Liga dim_match antes: %s", before)
    log.info("Pares SS+WS a fusionar: %d", len(pairs))

    if not pairs:
        log.info("Nada que fusionar.")
        return

    total_ev = total_sh = 0
    if dry_run:
        with engine.connect() as conn:
            for keeper_id, dup_id in pairs:
                ev, sh = _reassign_facts(conn, keeper_id, dup_id, dry_run=True)
                total_ev += ev
                total_sh += sh
        log.info(
            "[DRY-RUN] Se fusionarían %d pares; ~%d eventos y ~%d tiros en filas dup",
            len(pairs), total_ev, total_sh,
        )
        log.info("[DRY-RUN] dim_match quedaría en ~%d filas", int(before) - len(pairs))
        return

    with engine.begin() as conn:
        cid = _competition_id(conn)
        for i, (keeper_id, dup_id) in enumerate(pairs, 1):
            ev, sh = _reassign_facts(conn, keeper_id, dup_id, dry_run=False)
            total_ev += ev
            total_sh += sh
            _merge_match_row(conn, keeper_id, dup_id, dry_run=False)
            if i % 50 == 0:
                log.info("  fusionados %d/%d", i, len(pairs))

        after = conn.execute(
            text(
                "SELECT COUNT(*) FROM dim_match WHERE competition_id = :c AND season = :s"
            ),
            {"c": cid, "s": SEASON},
        ).scalar()

    log.info("Hechos movidos: %d eventos, %d tiros", total_ev, total_sh)
    log.info("dim_match después: %s (eliminadas %d filas duplicadas)", after, int(before) - int(after))


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Fusionar duplicados La Liga en dim_match")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Solo informar, sin modificar la BD",
    )
    args = parser.parse_args()
    run(dry_run=args.dry_run)


if __name__ == "__main__":
    main()
