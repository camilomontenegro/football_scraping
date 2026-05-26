"""
scripts/compact_dim_stadium.py
==============================
Post-procesado de dim_stadium tras la migración SCD2.

Operaciones:
  1) Backfill data_hash para todas las filas que no lo tienen.
  2) Fusiona filas adyacentes del mismo equipo con el mismo data_hash,
     extendiendo el rango [valid_from_season, valid_to_season] de la fila
     más antigua y borrando las redundantes.

Idempotente: se puede ejecutar varias veces sin efectos secundarios.

Uso:
    python -m scripts.compact_dim_stadium
    python -m scripts.compact_dim_stadium --dry-run
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
from typing import Optional

from sqlalchemy import text

from loaders.common import engine

log = logging.getLogger(__name__)

_DATA_FIELDS = [
    "stadium_name", "capacity",
    "seats_total", "seats_covered", "seats_vip", "vip_boxes", "seats_standing",
    "inaugurated_year", "built_year", "refurbished_year",
    "owner", "operator", "address", "city", "country",
    "construction_cost", "surface", "architect",
]


def _compute_data_hash(row: dict) -> str:
    payload = {k: row.get(k) for k in _DATA_FIELDS}
    s = json.dumps(payload, sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


def backfill_hashes(conn, dry_run: bool = False, force: bool = False) -> int:
    """Calcula data_hash para las filas de dim_stadium.

    Por defecto sólo procesa filas con data_hash IS NULL. Con force=True
    recalcula el hash de TODAS las filas — útil tras un cambio en
    _DATA_FIELDS o cuando el bootstrap quiere asegurar consistencia.
    """
    where = "" if force else "WHERE data_hash IS NULL"
    rows = conn.execute(text(f"""
        SELECT stadium_id, {", ".join(_DATA_FIELDS)}
        FROM dim_stadium
        {where}
    """)).fetchall()

    log.info(
        "backfill_hashes: %d filas %s.",
        len(rows),
        "a recalcular (force=True)" if force else "sin hash",
    )
    if dry_run:
        return len(rows)

    for r in rows:
        d = dict(r._mapping)
        h = _compute_data_hash(d)
        conn.execute(
            text("UPDATE dim_stadium SET data_hash = :h WHERE stadium_id = :id"),
            {"h": h, "id": r.stadium_id},
        )
    return len(rows)


def merge_overlapping_duplicates(conn, dry_run: bool = False) -> int:
    """Fusiona TODAS las filas del mismo equipo con el mismo data_hash.

    A diferencia de merge_adjacent (que recorre pares consecutivos en
    orden de valid_from_season), esta funcion agrupa POR (equipo, hash) y
    colapsa el grupo entero en una sola fila cuyo rango cubre la union
    [min(valid_from), max(valid_to)] de todas las filas del grupo.

    Es la operacion correcta tras la carga inicial cuando hay duplicados
    creados por inserciones en pasadas distintas o por races SELECT/UPDATE
    (es lo que produce los UniqueViolation que vimos en el log del loader).

    Returns: numero de filas borradas (= filas fusionadas).
    """
    groups = conn.execute(text("""
        SELECT id_transfermarkt_team, data_hash, COUNT(*) AS n
        FROM dim_stadium
        WHERE id_transfermarkt_team IS NOT NULL
          AND data_hash IS NOT NULL
        GROUP BY id_transfermarkt_team, data_hash
        HAVING COUNT(*) > 1
    """)).fetchall()

    log.info(
        "merge_overlapping_duplicates: %d grupos (equipo, hash) con >1 fila.",
        len(groups),
    )
    if dry_run:
        return sum((g.n - 1) for g in groups)

    deleted_total = 0
    for g in groups:
        tm_id, h = g.id_transfermarkt_team, g.data_hash
        rows = conn.execute(text("""
            SELECT stadium_id, valid_from_season, valid_to_season
            FROM dim_stadium
            WHERE id_transfermarkt_team = :tid AND data_hash = :h
            ORDER BY stadium_id
        """), {"tid": tm_id, "h": h}).fetchall()

        if len(rows) <= 1:
            continue

        keep_id = rows[0].stadium_id
        new_from = min(r.valid_from_season for r in rows)
        new_to   = max(r.valid_to_season   for r in rows)

        # Borrar primero las redundantes para liberar el unique index
        # (id_transfermarkt_team, valid_from_season) antes de updatear el keeper.
        for r in rows[1:]:
            conn.execute(
                text("DELETE FROM dim_stadium WHERE stadium_id = :id"),
                {"id": r.stadium_id},
            )
            deleted_total += 1

        conn.execute(text("""
            UPDATE dim_stadium
            SET valid_from_season = :nf,
                valid_to_season   = :nt,
                updated_at        = NOW()
            WHERE stadium_id = :id
        """), {"nf": new_from, "nt": new_to, "id": keep_id})

    return deleted_total


def merge_adjacent(conn, dry_run: bool = False) -> int:
    """Fusiona filas adyacentes del mismo equipo con el mismo data_hash."""
    teams = conn.execute(text("""
        SELECT DISTINCT id_transfermarkt_team
        FROM dim_stadium
        WHERE id_transfermarkt_team IS NOT NULL
    """)).fetchall()

    merged = 0
    for (tm_id,) in teams:
        rows = conn.execute(text("""
            SELECT stadium_id, valid_from_season, valid_to_season, data_hash
            FROM dim_stadium
            WHERE id_transfermarkt_team = :tid
            ORDER BY valid_from_season
        """), {"tid": tm_id}).fetchall()

        i = 0
        while i < len(rows) - 1:
            a, b = rows[i], rows[i + 1]
            if a.data_hash and a.data_hash == b.data_hash:
                # Mismo estado: extender 'a' y borrar 'b'.
                new_to = max(a.valid_to_season, b.valid_to_season)
                if not dry_run:
                    conn.execute(text("""
                        UPDATE dim_stadium
                        SET valid_to_season = :nt, updated_at = NOW()
                        WHERE stadium_id = :id
                    """), {"nt": new_to, "id": a.stadium_id})
                    conn.execute(
                        text("DELETE FROM dim_stadium WHERE stadium_id = :id"),
                        {"id": b.stadium_id},
                    )
                rows.pop(i + 1)
                # Mantener 'a' actualizada en memoria
                rows[i] = rows[i]._replace(valid_to_season=new_to) if hasattr(rows[i], "_replace") else rows[i]
                merged += 1
            else:
                i += 1
    return merged


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
        datefmt="%H:%M:%S",
    )
    parser = argparse.ArgumentParser(description="Compacta dim_stadium (SCD2).")
    parser.add_argument("--dry-run", action="store_true",
                        help="No escribe nada, solo informa de cambios.")
    args = parser.parse_args()

    with engine.begin() as conn:
        n_hash = backfill_hashes(conn, dry_run=args.dry_run)
        n_merge = merge_adjacent(conn, dry_run=args.dry_run)

    verb = "(dry-run) " if args.dry_run else ""
    print(f"\n{verb}data_hash calculado: {n_hash}")
    print(f"{verb}filas fusionadas:    {n_merge}")


if __name__ == "__main__":
    main()
