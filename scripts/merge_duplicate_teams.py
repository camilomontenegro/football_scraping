"""
Fusiona filas duplicadas en dim_team (mismo club, distinto canonical_id).

Casos típicos: fila SofaScore/TM (keeper) + fila WhoScored huérfana (loser).
Para FC Alverca también fusiona partidos SS+WS duplicados tras reasignar team_id.

Uso:
    python -m scripts.merge_duplicate_teams --dry-run
    python -m scripts.merge_duplicate_teams
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

# (keeper_id, loser_id) — keeper = nombre canónico del proyecto (SofaScore/TM)
TEAM_MERGE_PAIRS: list[tuple[int, int]] = [
    (204, 1756),   # Beşiktaş JK ← Besiktas
    (1653, 1760),  # Brøndby IF ← Brondby IF
    (1529, 1755),  # Basel ← FC Basel 1893
    (233, 1758),   # AC Sparta Praha ← Sparta Prague
    (215, 1754),   # GNK Dinamo Zagreb ← Dinamo Zagreb
    (1532, 1765),  # KAA Gent ← Gent
    (227, 1761),   # Royal Antwerp FC ← Royal Antwerp
    (1531, 1764),  # Standard Liège ← Standard Liege
    (216, 1752),   # FC Viktoria Plzeň ← Viktoria Plzen
    (207, 1757),   # Sheriff Tiraspol ← FC Sheriff
    (188, 1762),   # Zenit St. Petersburg ← Zenit
    (1741, 1742),  # FC Alverca ← Alverca (+ merge partidos)
    (829, 1739),   # SC Telstar ← Telstar
    (189, 1759),   # Shakhtar Donetsk ← Shakhtar
    (234, 1753),   # SK Sturm Graz ← Sturm Graz
    (844, 852),    # AVS - Futebol SAD ← AVS Futebol SAD
    (773, 1736),   # Hamburger SV ← Hamburg
    (1582, 1763),  # CFR 1907 Cluj ← Cluj
]

_ID_COLS = ("id_sofascore", "id_understat", "id_whoscored", "id_transfermarkt")

_TEAM_FK_UPDATES: list[tuple[str, str]] = [
    ("dim_match", "home_team_id"),
    ("dim_match", "away_team_id"),
    ("fact_shots", "team_id"),
    ("fact_events", "team_id"),
    ("fact_player_match_stats", "team_id"),
    ("fact_formations", "team_id"),
    ("fact_transfers", "from_team_id"),
    ("fact_transfers", "to_team_id"),
    ("dim_stadium", "canonical_team_id"),
]

_BRIDGE_FK = "bridge_team_season_stadium"

_OPTIONAL_FK = [("fact_market_value", "club_id")]


def _table_exists(conn, table: str) -> bool:
    return bool(
        conn.execute(
            text(
                "SELECT 1 FROM information_schema.tables "
                "WHERE table_schema = 'public' AND table_name = :t"
            ),
            {"t": table},
        ).scalar()
    )


def _transfer_external_ids(conn, keeper: int, loser: int, dry_run: bool) -> None:
    loser_row = conn.execute(
        text(
            "SELECT id_sofascore, id_understat, id_whoscored, id_transfermarkt "
            "FROM dim_team WHERE canonical_id = :lid"
        ),
        {"lid": loser},
    ).mappings().one()
    keeper_row = conn.execute(
        text(
            "SELECT id_sofascore, id_understat, id_whoscored, id_transfermarkt "
            "FROM dim_team WHERE canonical_id = :kid"
        ),
        {"kid": keeper},
    ).mappings().one()

    to_transfer: list[str] = []
    for col in _ID_COLS:
        loser_val = loser_row[col]
        keeper_val = keeper_row[col]
        if loser_val is None:
            continue
        if keeper_val is None:
            to_transfer.append(col)
        elif keeper_val != loser_val:
            log.warning(
                "  conflicto %s: keeper=%s loser=%s (se conserva keeper)",
                col, keeper_val, loser_val,
            )

    if not to_transfer:
        return

    if not dry_run:
        conn.execute(
            text(
                "UPDATE dim_team SET "
                "id_sofascore = NULL, id_understat = NULL, "
                "id_whoscored = NULL, id_transfermarkt = NULL "
                "WHERE canonical_id = :lid"
            ),
            {"lid": loser},
        )

    for col in to_transfer:
        loser_val = loser_row[col]
        if not dry_run:
            conn.execute(
                text(f"UPDATE dim_team SET {col} = :v WHERE canonical_id = :kid"),
                {"v": loser_val, "kid": keeper},
            )
        log.info("  transfer %s=%s → keeper %s", col, loser_val, keeper)


def _reassign_team_fks(conn, keeper: int, loser: int, dry_run: bool) -> dict[str, int]:
    moved: dict[str, int] = {}
    updates = list(_TEAM_FK_UPDATES)
    for tbl, col in _OPTIONAL_FK:
        if _table_exists(conn, tbl):
            updates.append((tbl, col))

    for tbl, col in updates:
        count = conn.execute(
            text(f"SELECT COUNT(*) FROM {tbl} WHERE {col} = :lid"),
            {"lid": loser},
        ).scalar()
        if not count:
            continue
        key = f"{tbl}.{col}"
        moved[key] = int(count)
        if not dry_run:
            conn.execute(
                text(f"UPDATE {tbl} SET {col} = :kid WHERE {col} = :lid"),
                {"kid": keeper, "lid": loser},
            )

    if _table_exists(conn, _BRIDGE_FK):
        count = conn.execute(
            text(f"SELECT COUNT(*) FROM {_BRIDGE_FK} WHERE canonical_team_id = :lid"),
            {"lid": loser},
        ).scalar()
        if count:
            key = f"{_BRIDGE_FK}.canonical_team_id"
            moved[key] = int(count)
            if not dry_run:
                conn.execute(
                    text(
                        f"UPDATE {_BRIDGE_FK} SET canonical_team_id = :kid "
                        f"WHERE canonical_team_id = :lid"
                    ),
                    {"kid": keeper, "lid": loser},
                )
    return moved


def _find_ss_ws_match_pairs(conn, team_id: int) -> list[tuple[int, int]]:
    rows = conn.execute(
        text("""
            SELECT
                MAX(m.match_id) FILTER (WHERE m.id_sofascore IS NOT NULL) AS keeper_id,
                MAX(m.match_id) FILTER (
                    WHERE m.id_sofascore IS NULL AND m.id_whoscored IS NOT NULL
                ) AS dup_id
            FROM dim_match m
            WHERE m.match_date IS NOT NULL
              AND m.home_team_id IS NOT NULL
              AND m.away_team_id IS NOT NULL
              AND :tid IN (m.home_team_id, m.away_team_id)
            GROUP BY m.match_date, m.home_team_id, m.away_team_id
            HAVING COUNT(*) > 1
               AND COUNT(*) FILTER (WHERE m.id_sofascore IS NOT NULL) = 1
               AND COUNT(*) FILTER (
                   WHERE m.id_sofascore IS NULL AND m.id_whoscored IS NOT NULL
               ) >= 1
        """),
        {"tid": team_id},
    ).fetchall()
    pairs: list[tuple[int, int]] = []
    for keeper_id, dup_id in rows:
        if keeper_id and dup_id and int(keeper_id) != int(dup_id):
            pairs.append((int(keeper_id), int(dup_id)))
    return pairs


def _reassign_match_facts(conn, keeper_id: int, dup_id: int, dry_run: bool) -> None:
    if dry_run:
        return
    for tbl in ("fact_events", "fact_shots", "fact_player_match_stats", "fact_formations"):
        if not _table_exists(conn, tbl):
            continue
        conn.execute(
            text(f"UPDATE {tbl} SET match_id = :keeper WHERE match_id = :dup"),
            {"keeper": keeper_id, "dup": dup_id},
        )


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
            text(
                "SELECT 1 FROM dim_match "
                "WHERE id_whoscored = :ws AND match_id != :keeper LIMIT 1"
            ),
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
    conn.execute(text("DELETE FROM dim_match WHERE match_id = :dup"), {"dup": dup_id})


def _merge_ss_ws_matches_for_team(conn, team_id: int, dry_run: bool) -> int:
    pairs = _find_ss_ws_match_pairs(conn, team_id)
    for keeper_id, dup_id in pairs:
        _reassign_match_facts(conn, keeper_id, dup_id, dry_run)
        _merge_match_row(conn, keeper_id, dup_id, dry_run)
    return len(pairs)


def merge_pair(conn, keeper: int, loser: int, dry_run: bool) -> None:
    names = conn.execute(
        text(
            "SELECT canonical_id, canonical_name FROM dim_team "
            "WHERE canonical_id IN (:k, :l) ORDER BY canonical_id"
        ),
        {"k": keeper, "l": loser},
    ).fetchall()
    if len(names) != 2:
        log.info("Saltar %s ← %s (ya fusionado o no existe)", keeper, loser)
        return

    log.info(
        "Fusionar %s (%s) ← %s (%s)",
        keeper, names[0][1] if names[0][0] == keeper else names[1][1],
        loser, names[1][1] if names[1][0] == loser else names[0][1],
    )

    moved = _reassign_team_fks(conn, keeper, loser, dry_run)
    if moved:
        log.info("  FK reasignadas: %s", moved)

    if any(k.startswith("dim_match.") for k in moved):
        n = _merge_ss_ws_matches_for_team(conn, keeper, dry_run)
        if n:
            log.info("  partidos SS+WS fusionados: %d", n)

    _transfer_external_ids(conn, keeper, loser, dry_run)

    if not dry_run:
        conn.execute(
            text("DELETE FROM dim_team WHERE canonical_id = :lid"),
            {"lid": loser},
        )
    log.info("  eliminado loser %s", loser)


def run(dry_run: bool) -> None:
    with engine.begin() as conn:
        before = conn.execute(text("SELECT COUNT(*) FROM dim_team")).scalar()
        for keeper, loser in TEAM_MERGE_PAIRS:
            merge_pair(conn, keeper, loser, dry_run)
        after = conn.execute(text("SELECT COUNT(*) FROM dim_team")).scalar()

    mode = "DRY-RUN" if dry_run else "OK"
    log.info("[%s] dim_team: %s → %s (−%d)", mode, before, after, int(before) - int(after))


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Fusionar equipos duplicados en dim_team")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    run(dry_run=args.dry_run)


if __name__ == "__main__":
    main()
