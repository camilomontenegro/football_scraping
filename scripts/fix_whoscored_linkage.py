"""
scripts/fix_whoscored_linkage.py
================================
Detecta y repara linkages WhoScored invertidos en dim_match.

Problema: el loader enlazó id_whoscored al fixture equivocado (home/away
invertidos) porque events.csv no distingue home/away y el query usaba OR.
Resultado: venue_name (y managers) corresponden al fixture opuesto.

Lógica de reparación:
  1. Lee match_centre.json para obtener home/away real de cada id_whoscored.
  2. Compara con dim_match.home_team_id / away_team_id.
  3. Si están invertidos, busca el fixture correcto y reasigna id_whoscored,
     venue_name, managers.

Uso:
    python -m scripts.fix_whoscored_linkage --dry-run
    python -m scripts.fix_whoscored_linkage
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from sqlalchemy import text
from loaders.common import engine
from utils.data_paths import RAW_ROOT

log = logging.getLogger(__name__)


def _discover_match_centres() -> dict[int, dict]:
    """Lee todos los match_centre.json y devuelve {ws_match_id: {home_ws, away_ws, venue, ...}}."""
    result = {}
    for mc_path in RAW_ROOT.rglob("whoscored/matches/*/match_centre.json"):
        ws_mid = mc_path.parent.name
        try:
            data = json.loads(mc_path.read_text(encoding="utf-8"))
        except Exception:
            continue
        home = data.get("home", {})
        away = data.get("away", {})
        h_id = home.get("teamId")
        a_id = away.get("teamId")
        if h_id and a_id:
            result[int(ws_mid)] = {
                "home_ws_id": int(h_id),
                "away_ws_id": int(a_id),
                "venue_name": data.get("venueName"),
                "manager_home": home.get("managerName"),
                "manager_away": away.get("managerName"),
                "ht_score": data.get("htScore"),
                "ft_score": data.get("ftScore") or data.get("score"),
            }
    return result


def fix_linkage(dry_run: bool = False):
    # 1. Leer todos los match_centre.json
    centres = _discover_match_centres()
    print(f"  match_centre.json encontrados: {len(centres)}")

    # 2. Cargar mapping WhoScored team_id → canonical_id
    with engine.begin() as conn:
        team_rows = conn.execute(text(
            "SELECT canonical_id, id_whoscored FROM dim_team WHERE id_whoscored IS NOT NULL"
        )).fetchall()
        ws_to_canonical = {r[1]: r[0] for r in team_rows}

        # 3. Obtener partidos con id_whoscored
        matches = conn.execute(text("""
            SELECT match_id, id_whoscored, home_team_id, away_team_id,
                   season, competition_id, venue_name
            FROM dim_match
            WHERE id_whoscored IS NOT NULL
        """)).mappings().fetchall()

        inverted = 0
        correct = 0
        unknown = 0
        fixed = 0
        fix_details = []

        for m in matches:
            ws_id = m["id_whoscored"]
            if ws_id not in centres:
                unknown += 1
                continue

            mc = centres[ws_id]
            # Traducir WhoScored team IDs a canonical IDs
            h_canonical = ws_to_canonical.get(mc["home_ws_id"])
            a_canonical = ws_to_canonical.get(mc["away_ws_id"])

            if not h_canonical or not a_canonical:
                unknown += 1
                continue

            # Comprobar si el linkage es correcto
            if m["home_team_id"] == h_canonical and m["away_team_id"] == a_canonical:
                correct += 1
                continue

            if m["home_team_id"] == a_canonical and m["away_team_id"] == h_canonical:
                # ¡INVERTIDO! Este id_whoscored debería ir al fixture opuesto
                inverted += 1

                # Buscar el fixture correcto en dim_match
                correct_match = conn.execute(text("""
                    SELECT match_id FROM dim_match
                    WHERE home_team_id = :hid
                      AND away_team_id = :aid
                      AND season = :season
                      AND competition_id = :comp_id
                      AND id_whoscored IS NULL
                    LIMIT 1
                """), {
                    "hid": h_canonical,
                    "aid": a_canonical,
                    "season": m["season"],
                    "comp_id": m["competition_id"],
                }).fetchone()

                if correct_match and not dry_run:
                    # Paso A: limpiar el partido actual (quitar id_whoscored + datos erróneos)
                    conn.execute(text("""
                        UPDATE dim_match
                        SET id_whoscored = NULL,
                            venue_name = NULL,
                            manager_home = NULL,
                            manager_away = NULL
                        WHERE match_id = :mid
                    """), {"mid": m["match_id"]})

                    # Paso B: asignar al partido correcto
                    conn.execute(text("""
                        UPDATE dim_match
                        SET id_whoscored = :ws_id,
                            venue_name = :venue,
                            manager_home = :mgr_h,
                            manager_away = :mgr_a
                        WHERE match_id = :mid
                          AND id_whoscored IS NULL
                    """), {
                        "ws_id": ws_id,
                        "venue": mc["venue_name"],
                        "mgr_h": mc["manager_home"],
                        "mgr_a": mc["manager_away"],
                        "mid": correct_match[0],
                    })
                    fixed += 1

                if correct_match:
                    fix_details.append({
                        "ws_id": ws_id,
                        "wrong_match": m["match_id"],
                        "correct_match": correct_match[0],
                        "venue": mc["venue_name"],
                    })
            else:
                # Ni correcto ni invertido — equipos diferentes (raro)
                unknown += 1

        # 4. Resumen
        print(f"\n{'='*60}")
        print(f"  {'DRY-RUN — ' if dry_run else ''}Fix WhoScored linkage invertido")
        print(f"{'='*60}")
        print(f"  Partidos con id_whoscored:  {len(matches):,}")
        print(f"  Correctos:                  {correct:,}")
        print(f"  Invertidos detectados:      {inverted:,}")
        print(f"  Reparados:                  {fixed:,}")
        print(f"  Sin match_centre.json:      {unknown:,}")
        print(f"{'='*60}")

        if fix_details and len(fix_details) <= 20:
            print(f"\n  Detalle de reparaciones:")
            for d in fix_details:
                print(f"    WS#{d['ws_id']}: match {d['wrong_match']} → {d['correct_match']}  ({d['venue']})")
        elif fix_details:
            print(f"\n  Primeras 20 reparaciones:")
            for d in fix_details[:20]:
                print(f"    WS#{d['ws_id']}: match {d['wrong_match']} → {d['correct_match']}  ({d['venue']})")

        # 5. Partidos que quedaron huérfanos (el correcto no existe)
        orphan = inverted - fixed
        if orphan > 0:
            print(f"\n  ⚠ {orphan} invertidos sin fixture correcto disponible")
            print(f"    (el fixture opuesto ya tiene otro id_whoscored asignado)")


def main():
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s - %(message)s")
    parser = argparse.ArgumentParser(description="Reparar linkages WhoScored invertidos")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    fix_linkage(dry_run=args.dry_run)


if __name__ == "__main__":
    main()
