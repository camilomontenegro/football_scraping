"""Helpers para bridge_team_season_stadium y overrides validados."""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Iterable

from sqlalchemy import text
from sqlalchemy.engine import Connection

log = logging.getLogger(__name__)

USAGE_PRIMARY = "primary"
USAGE_DOMESTIC = "domestic"
USAGE_EUROPEAN = "european"
USAGE_RENTAL = "rental"


@dataclass(frozen=True)
class BridgeEntry:
    team_id: int
    master_id: int
    season_start: str
    season_end: str
    usage_context: str = USAGE_PRIMARY


@dataclass(frozen=True)
class NameHistoryEntry:
    master_id: int
    stadium_name: str
    valid_from_year: int | None = None
    valid_to_year: int | None = None
    is_current: bool = False


def ensure_master(
    conn: Connection,
    name: str,
    *,
    wikidata_qid: str | None = None,
    city: str | None = None,
    country: str | None = None,
    dry_run: bool = False,
) -> int:
    """Devuelve stadium_id en dim_stadium_master (inserta si no existe)."""
    if wikidata_qid:
        row = conn.execute(
            text(
                "SELECT stadium_id FROM dim_stadium_master "
                "WHERE wikidata_qid = :qid LIMIT 1"
            ),
            {"qid": wikidata_qid},
        ).scalar()
        if row:
            return int(row)

    row = conn.execute(
        text(
            "SELECT stadium_id FROM dim_stadium_master "
            "WHERE LOWER(canonical_name) = LOWER(:name) LIMIT 1"
        ),
        {"name": name},
    ).scalar()
    if row:
        return int(row)

    if dry_run:
        log.info("[DRY] insertar master: %s (%s)", name, wikidata_qid or "sin QID")
        return -1

    master_id = conn.execute(
        text("""
            INSERT INTO dim_stadium_master (
                canonical_name, wikidata_qid, city, country, is_current
            ) VALUES (
                :name, :qid, :city, :country, TRUE
            )
            RETURNING stadium_id
        """),
        {"name": name, "qid": wikidata_qid, "city": city, "country": country},
    ).scalar()
    log.info("Master creado: %s → id %s", name, master_id)
    return int(master_id)


def rename_master(
    conn: Connection,
    master_id: int,
    new_name: str,
    *,
    history_names: Iterable[str] | None = None,
    city: str | None = None,
    country: str | None = None,
    dry_run: bool = False,
) -> None:
    """Actualiza canonical_name y registra nombres anteriores en history."""
    row = conn.execute(
        text(
            "SELECT canonical_name FROM dim_stadium_master "
            "WHERE stadium_id = :sid"
        ),
        {"sid": master_id},
    ).scalar()
    if not row:
        raise ValueError(f"master {master_id} no existe")

    old_name = str(row)
    if old_name.lower() != new_name.lower():
        upsert_name_history(
            conn,
            NameHistoryEntry(master_id, old_name, is_current=False),
            dry_run=dry_run,
        )
    for name in history_names or []:
        if name.lower() not in {old_name.lower(), new_name.lower()}:
            upsert_name_history(
                conn,
                NameHistoryEntry(master_id, name, is_current=False),
                dry_run=dry_run,
            )

    if dry_run:
        log.info("[DRY] rename master %s: %r → %r", master_id, old_name, new_name)
        return

    params: dict = {"sid": master_id, "name": new_name}
    sets = ["canonical_name = :name"]
    if city is not None:
        sets.append("city = :city")
        params["city"] = city
    if country is not None:
        sets.append("country = :country")
        params["country"] = country
    conn.execute(
        text(
            f"UPDATE dim_stadium_master SET {', '.join(sets)} WHERE stadium_id = :sid"
        ),
        params,
    )
    upsert_name_history(
        conn,
        NameHistoryEntry(master_id, new_name, is_current=True),
        dry_run=False,
    )


def upsert_name_history(
    conn: Connection,
    entry: NameHistoryEntry,
    *,
    dry_run: bool = False,
) -> bool:
    exists = conn.execute(
        text("""
            SELECT 1 FROM dim_stadium_names_history
            WHERE stadium_id = :sid
              AND LOWER(stadium_name) = LOWER(:name)
            LIMIT 1
        """),
        {"sid": entry.master_id, "name": entry.stadium_name},
    ).scalar()
    if exists:
        return False

    if dry_run:
        log.info(
            "[DRY] name history master=%s: %r (%s–%s)",
            entry.master_id,
            entry.stadium_name,
            entry.valid_from_year,
            entry.valid_to_year,
        )
        return True

    conn.execute(
        text("""
            INSERT INTO dim_stadium_names_history (
                stadium_id, stadium_name,
                valid_from_year, valid_to_year, is_current
            ) VALUES (
                :sid, :name, :vf, :vt, :cur
            )
        """),
        {
            "sid": entry.master_id,
            "name": entry.stadium_name,
            "vf": entry.valid_from_year,
            "vt": entry.valid_to_year,
            "cur": entry.is_current,
        },
    )
    return True


def ensure_bridge_schema(conn: Connection) -> None:
    conn.execute(text("""
        ALTER TABLE bridge_team_season_stadium
            ADD COLUMN IF NOT EXISTS usage_context VARCHAR(20) NOT NULL DEFAULT 'primary'
    """))
    conn.execute(text("""
        ALTER TABLE bridge_team_season_stadium
            DROP CONSTRAINT IF EXISTS bridge_team_season_stadium_canonical_team_id_stadium_id_sea_key
    """))
    conn.execute(text("""
        DROP INDEX IF EXISTS bridge_team_season_stadium_canonical_team_id_stadium_id_sea_key
    """))
    conn.execute(text("""
        CREATE UNIQUE INDEX IF NOT EXISTS ux_bridge_team_stadium_season_ctx
            ON bridge_team_season_stadium (
                canonical_team_id, stadium_id, season_start, season_end, usage_context
            )
    """))


def replace_bridge(
    conn: Connection,
    entries: Iterable[BridgeEntry],
    *,
    dry_run: bool = False,
) -> int:
    """Reemplaza filas bridge para los equipos tocados."""
    entries = list(entries)
    team_ids = sorted({e.team_id for e in entries})
    if not team_ids:
        return 0

    if not dry_run:
        ensure_bridge_schema(conn)

    if dry_run:
        n = conn.execute(
            text(
                "SELECT COUNT(*) FROM bridge_team_season_stadium "
                "WHERE canonical_team_id = ANY(:ids)"
            ),
            {"ids": team_ids},
        ).scalar()
        log.info("[DRY] borraría %d filas bridge (%d equipos)", n, len(team_ids))
    else:
        conn.execute(
            text(
                "DELETE FROM bridge_team_season_stadium "
                "WHERE canonical_team_id = ANY(:ids)"
            ),
            {"ids": team_ids},
        )

    inserted = 0
    for e in entries:
        if e.master_id < 0:
            continue
        if dry_run:
            log.info(
                "[DRY] bridge team=%s master=%s %s–%s ctx=%s",
                e.team_id,
                e.master_id,
                e.season_start,
                e.season_end,
                e.usage_context,
            )
        else:
            conn.execute(
                text("""
                    INSERT INTO bridge_team_season_stadium (
                        canonical_team_id, stadium_id,
                        season_start, season_end, is_home, usage_context
                    ) VALUES (
                        :tid, :sid, :ss, :se, TRUE, :ctx
                    )
                """),
                {
                    "tid": e.team_id,
                    "sid": e.master_id,
                    "ss": e.season_start,
                    "se": e.season_end,
                    "ctx": e.usage_context,
                },
            )
        inserted += 1
    return inserted


def manual_team_ids() -> list[int]:
    """Equipos con bridge manual (no sobrescribir en rebuild automático)."""
    from scripts.apply_mercanza_stadium_validation import TEAM_IDS_FOR_BRIDGE
    from scripts.apply_osen_stadium_batch import OSEN_TEAM_IDS
    from scripts.apply_osen_stadium_batch2 import OSEN2_TEAM_IDS
    from scripts.apply_osen_stadium_batch3 import OSEN3_TEAM_IDS
    from scripts.apply_fail36_stadium_batch import COLLATERAL_TEAM_IDS, FAIL36_TEAM_IDS
    from scripts.apply_verified_stadium_batch import VERIFIED_TEAM_IDS

    return sorted(
        set(TEAM_IDS_FOR_BRIDGE)
        | set(OSEN_TEAM_IDS)
        | set(OSEN2_TEAM_IDS)
        | set(OSEN3_TEAM_IDS)
        | set(VERIFIED_TEAM_IDS)
        | set(FAIL36_TEAM_IDS)
        | set(COLLATERAL_TEAM_IDS)
    )
