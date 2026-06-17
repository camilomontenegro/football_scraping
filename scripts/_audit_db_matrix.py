"""Matrix BD vs backup - que falta."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from sqlalchemy import text
from loaders.common import engine
from scripts._audit_clean_coverage import EXPECTED_MATCHES, sources_for_comp
from utils.data_paths import slugify_competition
from wizard.competitions import WORKING_COMPETITION_NAMES

BACKUP = Path(r"C:/Users/ivanm/Desktop/football_scraping_backup/data/clean")
SEASONS = [f"{y}_{y+1}" for y in range(2020, 2026)]

with engine.connect() as conn:
    rows = conn.execute(text("""
        SELECT c.canonical_name, m.season,
               COUNT(DISTINCT m.match_id), COUNT(e.event_id)
        FROM dim_competition c
        JOIN dim_match m ON m.competition_id = c.canonical_id
        LEFT JOIN fact_events e ON e.match_id = m.match_id
        WHERE c.canonical_name = ANY(:n)
        GROUP BY 1,2 ORDER BY 1,2
    """), {"n": list(WORKING_COMPETITION_NAMES)}).fetchall()
    shot_rows = conn.execute(text("""
        SELECT c.canonical_name, m.season, COUNT(fs.shot_id)
        FROM fact_shots fs JOIN dim_match m ON m.match_id=fs.match_id
        JOIN dim_competition c ON c.canonical_id=m.competition_id
        WHERE c.canonical_name = ANY(:n)
        GROUP BY 1,2
    """), {"n": list(WORKING_COMPETITION_NAMES)}).fetchall()
    inj_by_comp = conn.execute(text("""
        SELECT dc.canonical_name, COUNT(DISTINCT fi.injury_id)
        FROM fact_injuries fi
        JOIN dim_player dp ON dp.canonical_id = fi.player_id
        JOIN dim_match dm ON dm.match_id IN (
            SELECT fe.match_id FROM fact_events fe WHERE fe.player_id = dp.canonical_id LIMIT 1
        )
        JOIN dim_competition dc ON dc.canonical_id = dm.competition_id
        GROUP BY dc.canonical_name
    """)).fetchall() if False else []
    totals = conn.execute(text("""
        SELECT
          (SELECT COUNT(*) FROM fact_injuries),
          (SELECT COUNT(*) FROM fact_transfers),
          (SELECT COUNT(*) FROM fact_market_value),
          (SELECT COUNT(*) FROM player_review WHERE resolved IS NOT TRUE OR resolved IS NULL)
    """)).one()

db = {(a, s): {"m": m, "e": e, "s": 0} for a, s, m, e in rows}
for a, s, n in shot_rows:
    db.setdefault((a, s), {"m": 0, "e": 0, "s": 0})
    db[(a, s)]["s"] = n

print("=== MATRIZ BD ===")
for comp in sorted(WORKING_COMPETITION_NAMES):
    for sl in SEASONS:
        sd = sl.replace("_", "/")
        r = db.get((comp, sd))
        if not r:
            continue
        exp = EXPECTED_MATCHES.get(comp, 300)
        flag = ""
        if r["e"] == 0:
            flag = " SIN_EVENTOS"
        elif r["m"] < exp * 0.85:
            flag = " POCOS_PARTIDOS"
        if r["s"] == 0:
            flag += " SIN_TIROS"
        print(f"{comp:18} {sd:10} m={r['m']:4} ev={r['e']:8} sh={r['s']:6}{flag}")

print("\n=== EN BACKUP PERO NO EN BD ===")
for comp in sorted(WORKING_COMPETITION_NAMES):
    slug = slugify_competition(comp)
    base = BACKUP / slug
    if not base.is_dir():
        print(f"  {comp}: sin backup")
        continue
    for season_dir in sorted(base.iterdir()):
        if not season_dir.is_dir():
            continue
        sd = season_dir.name.replace("_", "/")
        r = db.get((comp, sd))
        if not r:
            print(f"  {comp} {sd}: backup si, BD no")
        elif r["e"] == 0:
            srcs = [s for s in sources_for_comp(comp) if (season_dir / s).is_dir()]
            print(f"  {comp} {sd}: {r['m']} partidos, 0 eventos (fuentes: {srcs})")

print("\n=== TIROS: SofaScore en backup, 0 en BD ===")
for comp in sorted(WORKING_COMPETITION_NAMES):
    slug = slugify_competition(comp)
    for sl in SEASONS:
        sofa = BACKUP / slug / sl / "sofascore"
        if not (sofa / "shots.csv").exists():
            continue
        sd = sl.replace("_", "/")
        r = db.get((comp, sd), {"s": 0})
        if r.get("s", 0) == 0:
            print(f"  {comp} {sd}")

print("\n=== TM injuries clean en backup 25/26 ===")
for comp in sorted(WORKING_COMPETITION_NAMES):
    slug = slugify_competition(comp)
    inj = BACKUP / slug / "2025_2026" / "transfermarkt" / "injuries.csv"
    pl = BACKUP / slug / "2025_2026" / "transfermarkt" / "players.csv"
    has = inj.exists() or pl.exists()
    print(f"  {comp}: players={pl.exists()} injuries={inj.exists()}")

print("\n=== TOTALES ===")
print(f"  injuries={totals[0]} transfers={totals[1]} market_value={totals[2]} review_pend={totals[3]}")
