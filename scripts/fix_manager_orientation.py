"""
scripts/fix_manager_orientation.py
==================================
Corrige manager_home / manager_away INVERTIDOS en dim_match, en la propia fila.

Contexto
--------
El linker de WhoScored (match_loader_generico / whoscored_stats_loader) enlaza
por id_whoscored sin verificar la orientación local/visitante. En ligas de ida y
vuelta acaba escribiendo los managers del fixture opuesto, de modo que
manager_home termina siendo el técnico del equipo VISITANTE. Eso rompe la
gráfica de managers (un mismo técnico aparece ligado a decenas de equipos, y
su W/D/L sale invertido).

Por qué no basta fix_whoscored_linkage.py
-----------------------------------------
Aquel script REASIGNA el id_whoscored al fixture correcto, pero necesita que el
fixture destino tenga id_whoscored IS NULL. En ida/vuelta ambas piernas suelen
estar ocupadas -> bloqueo mutuo -> los deja como "huérfanos" sin arreglar.

Este script NO mueve linkages: corrige los managers EN SITIO. Como la identidad
del manager es por-equipo (da igual la pierna), si la fila está invertida basta
con intercambiar manager_home <-> manager_away para que cuadren con
home_team_id / away_team_id de esa misma fila. Es libre de bloqueos.

Fuente de verdad
----------------
data/raw/**/whoscored/matches/<ws_id>/match_centre.json
  home.teamId / home.managerName  y  away.teamId / away.managerName

Uso
---
    python -m scripts.fix_manager_orientation            # DRY-RUN (no escribe)
    python -m scripts.fix_manager_orientation --apply    # aplica los cambios

Recomendado: ejecuta primero el dry-run, revisa los totales y un par de
ejemplos, y solo entonces --apply. Es idempotente: re-ejecutarlo no hace daño.
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parent.parent))

from sqlalchemy import text
from loaders.common import engine

try:
    from utils.data_paths import RAW_ROOT, CLEAN_ROOT
except Exception:  # pragma: no cover - fallback si cambia el layout
    RAW_ROOT = Path(__file__).resolve().parent.parent / "data" / "raw"
    CLEAN_ROOT = Path(__file__).resolve().parent.parent / "data" / "clean"

log = logging.getLogger(__name__)

# Mismos roots que fix_whoscored_linkage.py para máxima cobertura en tu equipo.
RAW_ROOTS = [
    RAW_ROOT,
    Path(r"C:\Users\Ivan\Desktop\football_scraping_data\raw"),
    Path(r"C:\Users\Ivan\Desktop\football_scraping_backup\data\raw"),
]

# Respaldo: match_enrichment.csv (mismo origen WhoScored, derivado de los JSON).
CLEAN_ROOTS = [
    CLEAN_ROOT,
    Path(r"C:\Users\Ivan\Desktop\football_scraping_data\clean"),
    Path(r"C:\Users\Ivan\Desktop\football_scraping_backup\data\clean"),
]


def _discover_match_centres() -> dict[int, dict]:
    """{ws_match_id: {home_ws_id, away_ws_id, manager_home, manager_away}}."""
    result: dict[int, dict] = {}
    for raw_root in RAW_ROOTS:
        if not raw_root.is_dir():
            continue
        for mc_path in raw_root.rglob("whoscored/matches/*/match_centre.json"):
            try:
                ws_mid = int(mc_path.parent.name)
            except ValueError:
                continue
            try:
                data = json.loads(mc_path.read_text(encoding="utf-8"))
            except Exception:
                continue
            home = data.get("home", {}) or {}
            away = data.get("away", {}) or {}
            h_id, a_id = home.get("teamId"), away.get("teamId")
            if h_id and a_id:
                result[ws_mid] = {
                    "home_ws_id": int(h_id),
                    "away_ws_id": int(a_id),
                    "manager_home": home.get("managerName"),
                    "manager_away": away.get("managerName"),
                }
    return result


def _discover_enrichment() -> dict[int, dict]:
    """Respaldo desde match_enrichment.csv (cubre filas sin match_centre.json).

    El CSV trae las mismas columnas de verdad: home_team_ws_id / away_team_ws_id
    emparejadas con manager_home / manager_away. De hecho esas 622 filas sin
    JSON sacaron sus managers justo de aquí, así que este es el origen correcto.
    """
    result: dict[int, dict] = {}
    for clean_root in CLEAN_ROOTS:
        if not clean_root.is_dir():
            continue
        for csv_path in clean_root.rglob("whoscored/match_enrichment.csv"):
            try:
                df = pd.read_csv(csv_path, encoding="utf-8-sig")
            except Exception:
                continue
            for _, row in df.iterrows():
                ws = row.get("whoscored_match_id")
                h = row.get("home_team_ws_id")
                a = row.get("away_team_ws_id")
                if pd.isna(ws) or pd.isna(h) or pd.isna(a):
                    continue
                mh = row.get("manager_home")
                ma = row.get("manager_away")
                result[int(ws)] = {
                    "home_ws_id": int(h),
                    "away_ws_id": int(a),
                    "manager_home": None if pd.isna(mh) else str(mh).strip(),
                    "manager_away": None if pd.isna(ma) else str(ma).strip(),
                }
    return result


def fix(apply: bool = False) -> None:
    mc = _discover_match_centres()
    enr = _discover_enrichment()
    # match_centre.json es la fuente más cruda; match_enrichment.csv rellena
    # los huecos (partidos cuyo JSON ya no está en disco).
    centres = dict(enr)
    centres.update(mc)
    print(f"  match_centre.json: {len(mc):,}  |  "
          f"match_enrichment.csv: {len(enr):,}  |  "
          f"verdad combinada: {len(centres):,}")
    if not centres:
        print("  AVISO: no se encontró ninguna fuente (match_centre.json ni")
        print("         match_enrichment.csv) en RAW_ROOTS / CLEAN_ROOTS. Aborta.")
        return

    with engine.begin() as conn:
        ws_to_canon = {
            r[0]: r[1]
            for r in conn.execute(text(
                "SELECT id_whoscored, canonical_id "
                "FROM dim_team WHERE id_whoscored IS NOT NULL"
            )).fetchall()
        }
        rows = conn.execute(text("""
            SELECT match_id, id_whoscored, home_team_id, away_team_id,
                   manager_home, manager_away
            FROM dim_match
            WHERE id_whoscored IS NOT NULL
        """)).mappings().fetchall()

        n = len(rows)
        aligned = inverted = already_ok = mismatch = no_centre = changed = 0
        samples: list[tuple] = []

        for m in rows:
            mc = centres.get(m["id_whoscored"])
            if not mc:
                no_centre += 1
                continue

            h_can = ws_to_canon.get(mc["home_ws_id"])
            a_can = ws_to_canon.get(mc["away_ws_id"])
            if not h_can or not a_can:
                mismatch += 1
                continue

            if m["home_team_id"] == h_can and m["away_team_id"] == a_can:
                want_h, want_a = mc["manager_home"], mc["manager_away"]
                aligned += 1
            elif m["home_team_id"] == a_can and m["away_team_id"] == h_can:
                want_h, want_a = mc["manager_away"], mc["manager_home"]  # invertida
                inverted += 1
            else:
                mismatch += 1
                continue

            # match_centre sin managers -> no tocamos lo que ya hubiera
            if not (want_h or want_a):
                already_ok += 1
                continue

            if want_h == m["manager_home"] and want_a == m["manager_away"]:
                already_ok += 1
                continue

            if len(samples) < 15:
                samples.append(
                    (m["match_id"], m["manager_home"], m["manager_away"], want_h, want_a)
                )

            if apply:
                conn.execute(text(
                    "UPDATE dim_match SET manager_home = :h, manager_away = :a "
                    "WHERE match_id = :mid"
                ), {"h": want_h, "a": want_a, "mid": m["match_id"]})
            changed += 1

        tag = "APLICADO" if apply else "DRY-RUN (no se escribió nada)"
        print(f"\n{'=' * 62}")
        print(f"  {tag} — corrección de orientación de managers")
        print(f"{'=' * 62}")
        print(f"  Filas con id_whoscored:        {n:,}")
        print(f"  Orientación correcta:          {aligned:,}")
        print(f"  Orientación invertida:         {inverted:,}")
        print(f"  Ya correctas (sin cambio):     {already_ok:,}")
        print(f"  Cambios {'aplicados' if apply else 'a aplicar'}:{' ' * (13 if apply else 14)}{changed:,}")
        print(f"  Equipos WS no mapeables:       {mismatch:,}")
        print(f"  Sin fuente (JSON ni CSV):      {no_centre:,}")
        print(f"{'=' * 62}")
        if samples:
            print("  Ejemplos (match_id: antes -> después):")
            for mid, oh, oa, nh, na in samples:
                print(f"    #{mid}: ({oh} / {oa})  ->  ({nh} / {na})")
        if not apply:
            print("\n  Si los totales cuadran, re-ejecuta con --apply para escribir.")


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    ap = argparse.ArgumentParser(
        description="Corrige manager_home/away invertidos en dim_match (en sitio)."
    )
    ap.add_argument("--apply", action="store_true",
                    help="Aplica los cambios (por defecto: dry-run, no escribe).")
    args = ap.parse_args()
    fix(apply=args.apply)


if __name__ == "__main__":
    main()
