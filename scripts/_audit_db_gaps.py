"""Auditoría rápida: qué falta en BD vs backup/clean esperado."""
from __future__ import annotations

import sys
from pathlib import Path

from sqlalchemy import text

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from loaders.common import engine
from scripts._audit_clean_coverage import EXPECTED_MATCHES, sources_for_comp
from utils.data_paths import CLEAN_ROOT, slugify_competition
from wizard.competitions import WORKING_COMPETITION_NAMES

BACKUP_CLEAN = Path(r"C:/Users/ivanm/Desktop/football_scraping_backup/data/clean")
SEASONS = [f"{y}_{y+1}" for y in range(2020, 2026)]


def _db_matrix() -> dict[tuple[str, str], dict]:
    sql = text("""
        SELECT c.canonical_name, m.season,
               COUNT(DISTINCT m.match_id) AS matches,
               COUNT(e.event_id) AS events
        FROM dim_competition c
        JOIN dim_match m ON m.competition_id = c.canonical_id
        LEFT JOIN fact_events e ON e.match_id = m.match_id
        WHERE c.canonical_name = ANY(:names)
        GROUP BY c.canonical_name, m.season
    """)
    with engine.connect() as conn:
        rows = conn.execute(sql, {"names": list(WORKING_COMPETITION_NAMES)}).fetchall()
        shot_rows = conn.execute(text("""
            SELECT c.canonical_name, m.season, COUNT(fs.shot_id)
            FROM fact_shots fs
            JOIN dim_match m ON m.match_id = fs.match_id
            JOIN dim_competition c ON c.canonical_id = m.competition_id
            WHERE c.canonical_name = ANY(:names)
            GROUP BY c.canonical_name, m.season
        """), {"names": list(WORKING_COMPETITION_NAMES)}).fetchall()
        totals = conn.execute(text("""
            SELECT
              (SELECT COUNT(*) FROM fact_injuries),
              (SELECT COUNT(*) FROM player_review WHERE resolved IS NOT TRUE OR resolved IS NULL),
              (SELECT COUNT(*) FROM dim_stadium WHERE latitude IS NULL OR longitude IS NULL)
        """)).one()

    out: dict[tuple[str, str], dict] = {}
    for name, season, matches, events in rows:
        out[(name, season)] = {"matches": matches, "events": events, "shots": 0}
    for name, season, shots in shot_rows:
        out.setdefault((name, season), {"matches": 0, "events": 0, "shots": 0})
        out[(name, season)]["shots"] = shots
    return out, totals


def _backup_seasons(comp: str) -> set[str]:
    slug = slugify_competition(comp)
    base = BACKUP_CLEAN / slug
    if not base.is_dir():
        return set()
    return {p.name for p in base.iterdir() if p.is_dir()}


def _has_clean(comp: str, season: str, source: str) -> bool:
    slug = slugify_competition(comp)
    for root in (CLEAN_ROOT / slug / season / source, BACKUP_CLEAN / slug / season / source):
        if not root.is_dir():
            continue
        if source == "whoscored" and (root / "events.csv").exists():
            return True
        if source == "sofascore" and (root / "matches.csv").exists():
            return True
        if source == "transfermarkt" and (root / "players.csv").exists():
            return True
        if source == "understat" and (root / "matches.csv").exists():
            return True
    return False


def main() -> int:
    db, (injuries, review, stadiums_no_coords) = _db_matrix()

    print("=" * 72)
    print("COBERTURA EN BD (comp × temporada)")
    print("=" * 72)
    missing_events: list[str] = []
    low_matches: list[str] = []
    no_shots: list[str] = []

    for comp in sorted(WORKING_COMPETITION_NAMES):
        exp = EXPECTED_MATCHES.get(comp, 300)
        backup_seasons = _backup_seasons(comp)
        for season_label in SEASONS:
            season_db = season_label.replace("_", "/")
            key = (comp, season_db)
            row = db.get(key)
            if not row:
                if season_label in backup_seasons:
                    missing_events.append(f"{comp} {season_db} — datos en backup, 0 en BD")
                continue
            m, e, s = row["matches"], row["events"], row["shots"]
            if e == 0 and season_label in backup_seasons:
                missing_events.append(f"{comp} {season_db} — {m} partidos pero 0 eventos")
            elif m < exp * 0.85:
                low_matches.append(f"{comp} {season_db} — {m} partidos (ref ~{exp})")
            if s == 0 and _has_clean(comp, season_label, "sofascore"):
                no_shots.append(f"{comp} {season_db} — SofaScore en backup pero 0 tiros en BD")

    print("\n--- Temporadas SIN datos en BD (pero con backup) ---")
    if missing_events:
        for x in missing_events:
            print(" ", x)
    else:
        print("  (ninguna)")

    print("\n--- Temporadas con pocos partidos (<85% referencia) ---")
    for x in low_matches[:25]:
        print(" ", x)
    if len(low_matches) > 25:
        print(f"  ... y {len(low_matches)-25} más")

    print("\n--- Tiros faltantes (SofaScore disponible) ---")
    for x in no_shots[:20]:
        print(" ", x)
    if len(no_shots) > 20:
        print(f"  ... y {len(no_shots)-20} más")

    print("\n--- Fuentes clean en backup NO cargadas en BD ---")
    gaps = []
    for comp in sorted(WORKING_COMPETITION_NAMES):
        slug = slugify_competition(comp)
        base = BACKUP_CLEAN / slug
        if not base.is_dir():
            gaps.append(f"{comp} — sin carpeta en backup")
            continue
        for season in sorted(base.iterdir()):
            if not season.is_dir():
                continue
            season_db = season.name.replace("_", "/")
            row = db.get((comp, season_db), {"events": 0})
            for src in sources_for_comp(comp):
                if _has_clean(comp, season.name, src) and row["events"] == 0 and src in ("whoscored", "sofascore"):
                    gaps.append(f"{comp} {season_db}/{src} — clean OK, BD sin eventos")
                if src == "transfermarkt" and _has_clean(comp, season.name, src):
                    pass  # injuries checked separately
    for g in gaps[:30]:
        print(" ", g)
    if len(gaps) > 30:
        print(f"  ... y {len(gaps)-30} más")

    print("\n--- Tablas / dimensiones incompletas ---")
    print(f"  fact_injuries: {injuries:,} filas")
    print(f"  player_review pendientes: {review:,}")
    print(f"  dim_stadium sin coordenadas: {stadiums_no_coords:,}")

    # injuries by comp from TM clean in backup only for 2025_2026
    tm_missing = []
    for comp in sorted(WORKING_COMPETITION_NAMES):
        if not _has_clean(comp, "2025_2026", "transfermarkt"):
            tm_missing.append(comp)
    if tm_missing:
        print("\n--- Sin datos TM clean 25/26 (jugadores/lesiones) ---")
        for c in tm_missing:
            print(" ", c)

    print("\n--- Resumen cargado en BD ---")
    for comp in sorted(WORKING_COMPETITION_NAMES):
        seasons = sorted(k[1] for k in db if k[0] == comp)
        if seasons:
            print(f"  {comp}: {seasons[0]} → {seasons[-1]} ({len(seasons)} temporadas)")
        else:
            print(f"  {comp}: (vacío)")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
