"""
scripts/fix_bundesliga_duplicates.py
====================================
Limpia bundesliga/2025_2026/whoscored/matches/ que tiene 694 partidos
mezclados: 306 de Bundesliga real + 380 de Serie A + 8 de PL.

Los duplicados ya existen en sus carpetas correctas (serie_a/, premier_league/).
Este script borra los directorios que no pertenecen a la Bundesliga y
regenera los clean CSVs del extractor.

Uso:
    python -m scripts.fix_bundesliga_duplicates --dry-run
    python -m scripts.fix_bundesliga_duplicates
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))
from utils.data_paths import RAW_ROOT

BL_TEAMS = {
    "Bayern", "RBL", "Mainz", "FC Koln", "St. Pauli", "Borussia Dortmund",
    "FC Heidenheim", "Wolfsburg", "Leverkusen", "Hoffenheim",
    "Borussia M.Gladbach", "Hamburg", "Union Berlin", "Stuttgart",
    "Eintracht Frankfurt", "Werder Bremen", "Freiburg", "Augsburg",
}


def main():
    parser = argparse.ArgumentParser(
        description="Elimina partidos de Serie A y PL del folder bundesliga/2025_2026")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    base = RAW_ROOT / "bundesliga" / "2025_2026" / "whoscored" / "matches"
    if not base.is_dir():
        print(f"No existe: {base}")
        return

    to_remove = []
    kept = 0
    for match_dir in sorted(base.iterdir()):
        if not match_dir.is_dir():
            continue
        mc = match_dir / "match_centre.json"
        if not mc.exists():
            continue
        try:
            data = json.loads(mc.read_text(encoding="utf-8"))
            h = data.get("home", {}).get("name", "?")
            a = data.get("away", {}).get("name", "?")
        except Exception:
            continue

        if h in BL_TEAMS or a in BL_TEAMS:
            kept += 1
        else:
            to_remove.append((match_dir, h, a))

    print(f"Bundesliga reales: {kept}")
    print(f"Duplicados a eliminar: {len(to_remove)}")

    if to_remove and len(to_remove) <= 20:
        for d, h, a in to_remove:
            print(f"  {d.name}: {h} vs {a}")
    elif to_remove:
        for d, h, a in to_remove[:10]:
            print(f"  {d.name}: {h} vs {a}")
        print(f"  ... y {len(to_remove) - 10} más")

    if args.dry_run or not to_remove:
        return

    removed = 0
    for d, h, a in to_remove:
        shutil.rmtree(d)
        removed += 1
    print(f"\n  Eliminados: {removed} directorios")
    print(f"  Restantes: {kept}")
    print("\n  Ahora re-ejecuta el extractor para regenerar los clean CSVs:")
    print('  python -m scrapers.whoscored_stats_extractor -c "Bundesliga" -s "2025/2026"')


if __name__ == "__main__":
    main()
