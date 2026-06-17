"""
scripts/referee_audit_and_fill.py
=================================
Audita el estado de los árbitros en la DB y rellena los huecos
directamente desde los JSON de WhoScored (sin pasar por CSVs).

Uso:
    python scripts/referee_audit_and_fill.py              # solo auditar
    python scripts/referee_audit_and_fill.py --fill       # auditar + rellenar
    python scripts/referee_audit_and_fill.py --fill --dry # simular sin escribir
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

# Añadir raíz del proyecto al path
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import pandas as pd
from sqlalchemy import text

from loaders.common import engine
from utils.data_paths import RAW_ROOT

log = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════
# AUDIT
# ═══════════════════════════════════════════════════════════════════

def audit(conn) -> dict:
    """Imprime un informe completo del estado de referee en la DB."""
    print("\n" + "=" * 70)
    print("  AUDITORÍA DE ÁRBITROS EN LA BASE DE DATOS")
    print("=" * 70)

    # 1. dim_referee
    r = conn.execute(text("""
        SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN canonical_name IS NULL OR canonical_name = '' THEN 1 ELSE 0 END) AS name_null,
            SUM(CASE WHEN country IS NULL THEN 1 ELSE 0 END) AS country_null,
            SUM(CASE WHEN id_sofascore IS NULL THEN 1 ELSE 0 END) AS ss_null,
            SUM(CASE WHEN id_whoscored IS NULL THEN 1 ELSE 0 END) AS ws_null
        FROM dim_referee
    """)).fetchone()

    print(f"\n── dim_referee ({r[0]} filas) ──")
    if r[0] > 0:
        print(f"  canonical_name NULL/vacío : {r[1]:>4} / {r[0]} ({r[1]/r[0]*100:5.1f}%)")
        print(f"  country NULL             : {r[2]:>4} / {r[0]} ({r[2]/r[0]*100:5.1f}%)")
        print(f"  id_sofascore NULL        : {r[3]:>4} / {r[0]} ({r[3]/r[0]*100:5.1f}%)")
        print(f"  id_whoscored NULL        : {r[4]:>4} / {r[0]} ({r[4]/r[0]*100:5.1f}%)")
    else:
        print("  (tabla vacía)")

    # 2. dim_match.referee_id
    r2 = conn.execute(text("""
        SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN referee_id IS NOT NULL THEN 1 ELSE 0 END) AS has_ref,
            SUM(CASE WHEN referee_id IS NULL THEN 1 ELSE 0 END) AS no_ref
        FROM dim_match
    """)).fetchone()

    print(f"\n── dim_match.referee_id ({r2[0]} partidos) ──")
    print(f"  CON referee_id  : {r2[1]:>4} / {r2[0]} ({r2[1]/r2[0]*100:5.1f}%)")
    print(f"  SIN referee_id  : {r2[2]:>4} / {r2[0]} ({r2[2]/r2[0]*100:5.1f}%)")

    # 3. Por data_source
    print(f"\n── referee_id por data_source ──")
    rows = conn.execute(text("""
        SELECT data_source,
               COUNT(*) AS total,
               SUM(CASE WHEN referee_id IS NOT NULL THEN 1 ELSE 0 END) AS has_ref
        FROM dim_match
        GROUP BY data_source
        ORDER BY data_source
    """)).fetchall()
    for row in rows:
        src, total, has = row
        pct = has / total * 100 if total else 0
        fill = "✓" if pct > 90 else "△" if pct > 50 else "✗"
        print(f"  {fill} {src or '(null)':>15}: {has:>4} / {total:>4} ({pct:5.1f}%)")

    # 4. Por temporada
    print(f"\n── referee_id por temporada ──")
    rows = conn.execute(text("""
        SELECT season,
               COUNT(*) AS total,
               SUM(CASE WHEN referee_id IS NOT NULL THEN 1 ELSE 0 END) AS has_ref
        FROM dim_match
        WHERE season IS NOT NULL
        GROUP BY season
        ORDER BY season
    """)).fetchall()
    for row in rows:
        season, total, has = row
        pct = has / total * 100 if total else 0
        fill = "✓" if pct > 90 else "△" if pct > 50 else "✗"
        print(f"  {fill} {season}: {has:>4} / {total:>4} ({pct:5.1f}%)")

    # 5. Matches WhoScored sin referee_id
    r3 = conn.execute(text("""
        SELECT COUNT(*)
        FROM dim_match
        WHERE id_whoscored IS NOT NULL AND referee_id IS NULL
    """)).scalar()
    print(f"\n── Partidos WhoScored SIN referee_id: {r3} ──")

    # 6. Matches Sofascore sin referee_id
    r4 = conn.execute(text("""
        SELECT COUNT(*)
        FROM dim_match
        WHERE id_sofascore IS NOT NULL AND referee_id IS NULL
    """)).scalar()
    print(f"── Partidos Sofascore SIN referee_id: {r4} ──")

    print("=" * 70)
    return {
        "referees_total": r[0],
        "matches_total": r2[0],
        "matches_with_ref": r2[1],
        "matches_without_ref": r2[2],
        "ws_without_ref": r3,
    }


# ═══════════════════════════════════════════════════════════════════
# FILL FROM WHOSCORED JSONs
# ═══════════════════════════════════════════════════════════════════

def fill_from_whoscored(conn, dry_run: bool = False) -> dict:
    """Lee los JSON de WhoScored y rellena dim_referee + dim_match.referee_id."""
    raw_root = RAW_ROOT
    json_pattern = "*/*/whoscored/matches/*/match_centre.json"
    files = sorted(raw_root.glob(json_pattern))
    print(f"\n{'[DRY-RUN] ' if dry_run else ''}Procesando {len(files)} JSON de WhoScored...")

    if not files:
        print(f"  No se encontraron JSONs en {raw_root}/{json_pattern}")
        return {"new_referees": 0, "matches_linked": 0}

    # Cache: id_whoscored -> referee_id (en DB)
    existing = {}
    rows = conn.execute(text(
        "SELECT id_whoscored, referee_id FROM dim_referee WHERE id_whoscored IS NOT NULL"
    )).fetchall()
    for r in rows:
        existing[r[0]] = r[1]
    print(f"  Árbitros ya en DB con id_whoscored: {len(existing)}")

    # Cache: id_whoscored match -> match_id
    match_cache = {}
    rows = conn.execute(text(
        "SELECT id_whoscored, match_id FROM dim_match WHERE id_whoscored IS NOT NULL"
    )).fetchall()
    for r in rows:
        match_cache[r[0]] = r[1]
    print(f"  Partidos con id_whoscored en DB: {len(match_cache)}")

    new_referees = 0
    matches_linked = 0
    skipped_no_match = 0
    skipped_no_ref = 0
    already_linked = 0

    for f in files:
        ws_match_id_str = f.parent.name
        try:
            ws_match_id = int(ws_match_id_str)
        except ValueError:
            continue

        # ¿Tenemos este partido en dim_match?
        db_match_id = match_cache.get(ws_match_id)
        if db_match_id is None:
            skipped_no_match += 1
            continue

        with open(f, encoding="utf-8") as fh:
            data = json.load(fh)

        ref = data.get("referee")
        if not ref or not isinstance(ref, dict):
            skipped_no_ref += 1
            continue

        official_id = ref.get("officialId")
        ref_name = ref.get("name", "").strip()
        if not official_id or not ref_name:
            skipped_no_ref += 1
            continue

        # 1. Asegurar que el árbitro existe en dim_referee
        referee_db_id = existing.get(official_id)
        if referee_db_id is None:
            if not dry_run:
                # Intentar insert
                row = conn.execute(text("""
                    INSERT INTO dim_referee (canonical_name, id_whoscored, data_source, updated_at)
                    VALUES (:name, :ws_id, 'whoscored', NOW())
                    ON CONFLICT (id_whoscored) DO UPDATE
                    SET canonical_name = EXCLUDED.canonical_name,
                        updated_at = NOW()
                    RETURNING referee_id
                """), {"name": ref_name, "ws_id": official_id}).fetchone()
                referee_db_id = row[0] if row else None
            else:
                referee_db_id = -1  # placeholder para dry-run

            if referee_db_id:
                existing[official_id] = referee_db_id
                new_referees += 1

        if referee_db_id is None:
            continue

        # 2. Enlazar dim_match.referee_id
        if not dry_run:
            upd = conn.execute(text("""
                UPDATE dim_match
                SET referee_id = :rid
                WHERE match_id = :mid
                  AND (referee_id IS NULL OR referee_id <> :rid)
            """), {"rid": referee_db_id, "mid": db_match_id})
            if upd.rowcount and upd.rowcount > 0:
                matches_linked += 1
            else:
                already_linked += 1
        else:
            # En dry-run, verificar si ya está enlazado
            cur_ref = conn.execute(text(
                "SELECT referee_id FROM dim_match WHERE match_id = :mid"
            ), {"mid": db_match_id}).scalar()
            if cur_ref is None:
                matches_linked += 1
            else:
                already_linked += 1

    print(f"\n  Resultados {'(simulación)' if dry_run else ''}:")
    print(f"    Árbitros nuevos insertados: {new_referees}")
    print(f"    Partidos enlazados:         {matches_linked}")
    print(f"    Ya estaban enlazados:       {already_linked}")
    print(f"    Sin partido en DB:          {skipped_no_match}")
    print(f"    Sin dato de referee en JSON:{skipped_no_ref}")
    return {"new_referees": new_referees, "matches_linked": matches_linked}


# ═══════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════

def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s -- %(message)s",
        datefmt="%H:%M:%S",
    )
    ap = argparse.ArgumentParser(description="Auditoría y relleno de árbitros.")
    ap.add_argument("--fill", action="store_true", help="Rellenar huecos desde WhoScored JSON")
    ap.add_argument("--dry", action="store_true", help="Simular sin escribir en DB")
    args = ap.parse_args()

    with engine.begin() as conn:
        # Auditoría ANTES
        print("\n▶ ESTADO ANTES:")
        before = audit(conn)

        if args.fill:
            result = fill_from_whoscored(conn, dry_run=args.dry)

            # Auditoría DESPUÉS
            if not args.dry:
                print("\n▶ ESTADO DESPUÉS:")
                after = audit(conn)

                # Resumen delta
                delta_ref = after["referees_total"] - before["referees_total"]
                delta_linked = after["matches_with_ref"] - before["matches_with_ref"]
                print(f"\n📊 RESUMEN:")
                print(f"  Árbitros nuevos: +{delta_ref}")
                print(f"  Partidos enlazados: +{delta_linked}")
                pct_before = before["matches_with_ref"] / before["matches_total"] * 100
                pct_after = after["matches_with_ref"] / after["matches_total"] * 100
                print(f"  Cobertura: {pct_before:.1f}% → {pct_after:.1f}%")


if __name__ == "__main__":
    main()
