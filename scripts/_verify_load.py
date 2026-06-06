"""Verifica carga Bundesliga 2025/2026 vs clean/."""
from __future__ import annotations

import csv
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from sqlalchemy import text, bindparam
from loaders.common import engine

COMP = "Bundesliga"
SEASON_VARIANTS = ["2025/2026", "25/26", "2025/26", "2025", "Bundesliga 25/26"]
CLEAN = PROJECT_ROOT / "data" / "clean" / "bundesliga" / "2025_2026"


def csv_rows(p: Path) -> int:
    if not p.exists():
        return 0
    with p.open(encoding="utf-8", errors="replace") as f:
        return max(0, sum(1 for _ in f) - 1)


def main() -> None:
    print("=" * 70)
    print(f"VERIFICACION CARGA: {COMP} 2025_2026")
    print("=" * 70)

    print("\n[clean/ esperado]")
    for src, files in [
        ("sofascore", ["matches.csv", "shots.csv", "events.csv"]),
        ("understat", ["matches.csv", "shots.csv"]),
        ("whoscored", ["matches.csv", "events.csv"]),
        ("transfermarkt", ["players.csv", "injuries.csv"]),
    ]:
        parts = [f"{fn}={csv_rows(CLEAN / src / fn)}" for fn in files]
        print(f"  {src}: {', '.join(parts)}")

    with engine.connect() as conn:
        comp_id = conn.execute(
            text(
                "SELECT canonical_id FROM dim_competition WHERE id_transfermarkt = :c"
            ),
            {"c": "L1"},
        ).scalar()
        print(f"\n[BD] competition_id (L1) = {comp_id}")

        sql = text("""
            SELECT COUNT(*) AS n,
                   COUNT(id_sofascore) AS ss,
                   COUNT(id_understat) AS us,
                   COUNT(id_whoscored) AS ws,
                   COUNT(match_date) AS with_date,
                   MAX(match_date) AS last_date
            FROM dim_match
            WHERE competition_id = :cid
              AND season IN :variants
        """).bindparams(bindparam("variants", expanding=True))

        row = conn.execute(
            sql, {"cid": comp_id, "variants": SEASON_VARIANTS}
        ).mappings().one()
        print("\n[dim_match] temporada 2025/26 (por competition_id)")
        for k, v in row.items():
            print(f"  {k}: {v}")

        shots = conn.execute(
            text("""
                SELECT COUNT(*) FROM fact_shots f
                JOIN dim_match m ON f.match_id = m.match_id
                WHERE m.competition_id = :cid AND m.season IN :variants
            """).bindparams(bindparam("variants", expanding=True)),
            {"cid": comp_id, "variants": SEASON_VARIANTS},
        ).scalar()

        events = conn.execute(
            text("""
                SELECT COUNT(*) FROM fact_events e
                JOIN dim_match m ON e.match_id = m.match_id
                WHERE m.competition_id = :cid AND m.season IN :variants
            """).bindparams(bindparam("variants", expanding=True)),
            {"cid": comp_id, "variants": SEASON_VARIANTS},
        ).scalar()

        inj = conn.execute(
            text("SELECT COUNT(*) FROM fact_injuries WHERE season = :s"),
            {"s": "25/26"},
        ).scalar()

        print(f"\n[facts] shots={shots:,}  events={events:,}  injuries(25/26)={inj:,}")

        teams = conn.execute(text("SELECT COUNT(*) FROM dim_team")).scalar()
        print(f"\n[dim_team global] {teams:,} (incluye todas las ligas)")

    print("\n[veredicto dimensiones]")
    n = row["n"]
    if n >= 300:
        print(f"  OK: {n} partidos en dim_match (esperado ~306-311)")
    else:
        print(f"  REVISAR: solo {n} partidos")
    if shots == 0 and events == 0:
        print("  PENDIENTE: no hay facts — ejecuta load_facts")
    print("=" * 70)


if __name__ == "__main__":
    main()
