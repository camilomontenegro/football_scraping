"""Sincroniza dim_team.home_stadium_master_id desde bridge_team_season_stadium."""
from __future__ import annotations

import logging

from sqlalchemy import text
from sqlalchemy.engine import Connection

log = logging.getLogger(__name__)

_LATEST_HOME_SQL = """
    SELECT DISTINCT ON (b.canonical_team_id)
        b.canonical_team_id AS tid,
        b.stadium_id AS mid
    FROM bridge_team_season_stadium b
    WHERE COALESCE(b.is_home, TRUE) = TRUE
      {team_filter}
    ORDER BY b.canonical_team_id,
        CASE COALESCE(b.usage_context, 'primary')
            WHEN 'primary' THEN 0
            WHEN 'domestic' THEN 1
            WHEN 'european' THEN 2
            ELSE 3
        END,
        b.season_end DESC,
        b.season_start DESC
"""


def sync_team_home_stadiums(
    conn: Connection,
    *,
    dry_run: bool = False,
    team_ids: list[int] | None = None,
) -> dict[str, int]:
    """
    Actualiza dim_team.home_stadium_master_id con la última sede del bridge.

    Args:
        conn: conexión SQLAlchemy activa.
        dry_run: solo cuenta cambios, no escribe.
        team_ids: si se indica, solo esos equipos (+ no limpia el resto).

    Returns:
        dict con updated, unchanged, with_home, cleared.
    """
    params: dict = {}
    team_filter = ""
    if team_ids:
        team_filter = "AND b.canonical_team_id = ANY(:tids)"
        params["tids"] = team_ids

    rows = conn.execute(
        text(_LATEST_HOME_SQL.format(team_filter=team_filter)),
        params,
    ).fetchall()

    updated = unchanged = 0
    for row in rows:
        current = conn.execute(
            text(
                "SELECT home_stadium_master_id FROM dim_team WHERE canonical_id = :tid"
            ),
            {"tid": row.tid},
        ).scalar()
        if current == row.mid:
            unchanged += 1
            continue
        updated += 1
        if dry_run:
            log.info(
                "[DRY] team %s: home_stadium_master_id %s → %s",
                row.tid,
                current,
                row.mid,
            )
            continue
        conn.execute(
            text("""
                UPDATE dim_team
                SET home_stadium_master_id = :mid
                WHERE canonical_id = :tid
            """),
            {"tid": row.tid, "mid": row.mid},
        )

    cleared = 0
    if not team_ids and not dry_run:
        cleared = conn.execute(
            text("""
                UPDATE dim_team dt
                SET home_stadium_master_id = NULL
                WHERE home_stadium_master_id IS NOT NULL
                  AND NOT EXISTS (
                      SELECT 1 FROM bridge_team_season_stadium b
                      WHERE b.canonical_team_id = dt.canonical_id
                        AND COALESCE(b.is_home, TRUE) = TRUE
                  )
            """)
        ).rowcount
    elif not team_ids and dry_run:
        cleared = conn.execute(
            text("""
                SELECT COUNT(*) FROM dim_team dt
                WHERE home_stadium_master_id IS NOT NULL
                  AND NOT EXISTS (
                      SELECT 1 FROM bridge_team_season_stadium b
                      WHERE b.canonical_team_id = dt.canonical_id
                        AND COALESCE(b.is_home, TRUE) = TRUE
                  )
            """)
        ).scalar() or 0

    stats = {
        "updated": updated,
        "unchanged": unchanged,
        "with_home": len(rows),
        "cleared": int(cleared or 0),
    }
    log.info(
        "home_stadium sync: %d con sede, %d actualizados, %d sin cambio, %d limpiados",
        stats["with_home"],
        stats["updated"],
        stats["unchanged"],
        stats["cleared"],
    )
    return stats
