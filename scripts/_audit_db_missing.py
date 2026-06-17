import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from sqlalchemy import text
from loaders.common import engine
from utils.data_paths import slugify_competition
from wizard.competitions import WORKING_COMPETITION_NAMES

backup = Path(r"C:/Users/ivanm/Desktop/football_scraping_backup/data/clean")

with engine.connect() as conn:
    print("=== Competiciones secundarias ===")
    for c in ("Championship", "Segunda", "Segunda División"):
        r = conn.execute(text("""
            SELECT dc.canonical_name, COUNT(DISTINCT m.match_id), COUNT(e.event_id)
            FROM dim_match m JOIN dim_competition dc ON dc.canonical_id=m.competition_id
            LEFT JOIN fact_events e ON e.match_id=m.match_id
            WHERE dc.canonical_name ILIKE :c
            GROUP BY dc.canonical_name
        """), {"c": "%" + c + "%"}).fetchall()
        for row in r:
            print(f"  {row[0]}: matches={row[1]} events={row[2]}")
        if not r:
            print(f"  {c}: (vacío)")

    print("\n=== Understat / StatsBomb en dim_match ===")
    print("  id_understat:", conn.execute(text("SELECT COUNT(*) FROM dim_match WHERE id_understat IS NOT NULL")).scalar())
    print("  id_statsbomb:", conn.execute(text("SELECT COUNT(*) FROM dim_match WHERE id_statsbomb IS NOT NULL")).scalar())

    print("\n=== Tablas auxiliares ===")
    for tbl in ("fact_transfers", "fact_market_value", "fact_player_match_stats"):
        try:
            n = conn.execute(text(f"SELECT COUNT(*) FROM {tbl}")).scalar()
            print(f"  {tbl}: {n}")
        except Exception as e:
            print(f"  {tbl}: no existe o error")

    print("\n=== Injuries por temporada (top) ===")
    rows = conn.execute(text("SELECT season, COUNT(*) FROM fact_injuries GROUP BY season ORDER BY season")).fetchall()
    for s, n in rows:
        print(f"  {s}: {n}")

    print("\n=== Anomalías de tiros (vs media de la liga) ===")
    from scripts._audit_clean_coverage import EXPECTED_MATCHES
    shot_rows = conn.execute(text("""
        SELECT c.canonical_name, m.season, COUNT(fs.shot_id)
        FROM fact_shots fs JOIN dim_match m ON m.match_id=fs.match_id
        JOIN dim_competition c ON c.canonical_id=m.competition_id
        WHERE c.canonical_name = ANY(:n)
        GROUP BY 1,2 ORDER BY 1,2
    """), {"n": list(WORKING_COMPETITION_NAMES)}).fetchall()
    by_comp = {}
    for comp, season, n in shot_rows:
        by_comp.setdefault(comp, []).append(n)
    for comp, season, n in shot_rows:
        avg = sum(by_comp[comp]) / len(by_comp[comp])
        if n < avg * 0.6:
            print(f"  {comp} {season}: {n} tiros (media liga {avg:.0f})")

print("\n=== Understat en backup (no en WORKING understat league=None para muchas) ===")
for comp in sorted(WORKING_COMPETITION_NAMES):
    slug = slugify_competition(comp)
    base = backup / slug
    if not base.is_dir():
        continue
    us = [p.name for p in base.iterdir() if (p / "understat" / "matches.csv").exists()]
    if us:
        print(f"  {comp}: {us}")

print("\n=== Eredivisie TM en backup (todas temporadas) ===")
slug = "eredivisie"
base = backup / slug
for s in sorted(base.iterdir()) if base.is_dir() else []:
    tm = s / "transfermarkt"
    if tm.is_dir():
        pl = (tm / "players.csv").exists()
        inj = (tm / "injuries.csv").exists()
        if not pl or not inj:
            print(f"  {s.name}: players={pl} injuries={inj}")

print("\n=== Repo raw NO cargado (Championship/Segunda) ===")
repo = Path(r"C:/Users/ivanm/Desktop/football_scraping_alvaro/data/raw")
for slug in ("championship", "segunda_division"):
    p = repo / slug
    if p.is_dir():
        seasons = [x.name for x in p.iterdir() if x.is_dir()]
        print(f"  {slug} en repo: {seasons} (sin cargar a BD)")
