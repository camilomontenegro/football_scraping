"""Elimina datos de la temporada 2026/2027 (archivos + BD)."""
from __future__ import annotations

import json
import shutil
from pathlib import Path

from sqlalchemy import text

from loaders.common import engine

STADIUMS_ROOT = Path(r"C:\Users\Ivan\Desktop\stadiums")
SEASON = "2026/2027"
SEASON_FOLDER = "2026_2027"


def purge_files() -> int:
    removed = 0
    for base in (STADIUMS_ROOT / "raw", STADIUMS_ROOT / "clean"):
        if not base.exists():
            continue
        for path in base.rglob(SEASON_FOLDER):
            if path.is_dir():
                n = sum(1 for _ in path.rglob("*") if _.is_file())
                shutil.rmtree(path)
                removed += n
                print(f"  borrado {path} ({n} archivos)")
    cache = STADIUMS_ROOT / ".cache" / "transfermarkt_stadiums_last_scraped.json"
    if cache.exists():
        data = json.loads(cache.read_text(encoding="utf-8"))
        before = len(data)
        data = {k: v for k, v in data.items() if not k.endswith(f"|{2026}")}
        if len(data) < before:
            cache.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
            print(f"  caché TM: {before - len(data)} claves 2026 eliminadas")
    return removed


def purge_db() -> None:
    with engine.begin() as conn:
        m = conn.execute(
            text("SELECT COUNT(*) FROM dim_match WHERE season = :s"),
            {"s": SEASON},
        ).scalar()
        if m:
            conn.execute(text("DELETE FROM dim_match WHERE season = :s"), {"s": SEASON})
            print(f"  dim_match: {m} filas eliminadas")

        # Filas SCD2 solo 2026/27
        d1 = conn.execute(text("""
            DELETE FROM dim_stadium
            WHERE valid_from_season = :s AND valid_to_season = :s
            RETURNING stadium_id
        """), {"s": SEASON}).fetchall()
        print(f"  dim_stadium (solo {SEASON}): {len(d1)} filas eliminadas")

        # Acortar rangos que se extendían hasta 2026/27
        d2 = conn.execute(text("""
            UPDATE dim_stadium
            SET valid_to_season = '2025/2026', updated_at = NOW()
            WHERE valid_to_season = :s AND valid_from_season < :s
            RETURNING stadium_id
        """), {"s": SEASON}).fetchall()
        print(f"  dim_stadium (to acortado a 2025/26): {len(d2)} filas")


if __name__ == "__main__":
    print("Archivos en Desktop/stadiums:")
    n = purge_files()
    print(f"  total archivos: {n}")
    print("\nBase de datos:")
    purge_db()
    print("OK")
