"""
scripts/backfill_stadium_match.py
=================================
Vincula cada partido con su estadio real (stadium_id en dim_match).

Lógica multinivel (por prioridad):
  1. venue.name del JSON crudo de SofaScore (data/raw/attendance/{ss_id}.json)
     → match exacto o fuzzy contra dim_stadium.stadium_name
  2. venue_name de WhoScored (ya en dim_match.venue_name)
     → match exacto o fuzzy contra dim_stadium.stadium_name
  3. Fallback: estadio del home_team_id por temporada (válido para liga regular)

Esto cubre: reformas, finales en sede neutral, equipos que cambian de estadio, etc.

Uso:
    python -m scripts.backfill_stadium_match --dry-run
    python -m scripts.backfill_stadium_match
    python -m scripts.backfill_stadium_match --force   # re-evalúa todos
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
import unicodedata
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from sqlalchemy import text
from loaders.common import engine

log = logging.getLogger(__name__)

RAW_ATTENDANCE_DIR = Path(__file__).resolve().parent.parent / "data" / "raw" / "attendance"

# ── Normalización de nombres ─────────────────────────────────────────────

def _normalize(name: str) -> str:
    """Lowercase, strip accents, remove punctuation, collapse whitespace."""
    if not name:
        return ""
    # Decompose unicode, strip combining marks (accents)
    nfkd = unicodedata.normalize("NFKD", name)
    ascii_str = "".join(c for c in nfkd if not unicodedata.combining(c))
    # Lowercase, remove punctuation except spaces
    cleaned = re.sub(r"[^\w\s]", "", ascii_str.lower())
    return re.sub(r"\s+", " ", cleaned).strip()


def _similarity(a: str, b: str) -> float:
    """Simple token-overlap similarity (Jaccard on words). 0..1."""
    wa = set(a.split())
    wb = set(b.split())
    if not wa or not wb:
        return 0.0
    return len(wa & wb) / len(wa | wb)


# ── Cargar venue names de SofaScore raw JSONs ────────────────────────────

def _load_sofascore_venues() -> dict[str, str]:
    """Retorna {ss_id_str: venue_name} leyendo data/raw/attendance/*.json."""
    venues = {}
    if not RAW_ATTENDANCE_DIR.exists():
        return venues
    for path in RAW_ATTENDANCE_DIR.glob("*.json"):
        ss_id = path.stem
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            venue = data.get("venue") or {}
            name = venue.get("name")
            if name:
                venues[ss_id] = name
        except Exception:
            pass
    return venues


# ── Core ─────────────────────────────────────────────────────────────────

def backfill(dry_run: bool = False, force: bool = False):
    # 1. Cargar venue names de SofaScore
    ss_venues = _load_sofascore_venues()
    print(f"  SofaScore raw venue names cargados: {len(ss_venues)}")

    with engine.begin() as conn:
        # 2. Asegurar columna
        conn.execute(text(
            "ALTER TABLE dim_match ADD COLUMN IF NOT EXISTS stadium_id INTEGER "
            "REFERENCES dim_stadium (stadium_id) ON DELETE SET NULL"
        ))
        conn.execute(text(
            "CREATE INDEX IF NOT EXISTS idx_match_stadium ON dim_match (stadium_id)"
        ))
        print("  ✓ Columna stadium_id asegurada en dim_match")

        before = conn.execute(text(
            "SELECT COUNT(*) FROM dim_match WHERE stadium_id IS NOT NULL"
        )).scalar()

        # 3. Cargar todos los estadios como lookup
        stadiums = conn.execute(text("""
            SELECT stadium_id, stadium_name, canonical_team_id,
                   valid_from_season, valid_to_season
            FROM dim_stadium
            WHERE stadium_name IS NOT NULL
        """)).mappings().fetchall()

        # Índice normalizado: norm_name -> lista de stadium rows
        stadium_by_norm = {}
        for s in stadiums:
            norm = _normalize(s["stadium_name"])
            stadium_by_norm.setdefault(norm, []).append(dict(s))

        # Índice por team+season para fallback
        stadium_by_team = {}
        for s in stadiums:
            tid = s["canonical_team_id"]
            if tid:
                stadium_by_team.setdefault(tid, []).append(dict(s))

        def _find_by_name(venue_name: str):
            """Busca stadium_id por nombre. Exacto → fuzzy (>0.5)."""
            norm = _normalize(venue_name)
            if not norm:
                return None
            # Exact match
            if norm in stadium_by_norm:
                return stadium_by_norm[norm][0]["stadium_id"]
            # Fuzzy
            best_score, best_id = 0.0, None
            for sn, rows in stadium_by_norm.items():
                score = _similarity(norm, sn)
                if score > best_score:
                    best_score = score
                    best_id = rows[0]["stadium_id"]
            if best_score >= 0.5:
                return best_id
            return None

        def _find_by_team(team_id: int, season: str):
            """Fallback: estadio del equipo para esa temporada."""
            candidates = stadium_by_team.get(team_id, [])
            for s in candidates:
                if s["valid_from_season"] <= season <= s["valid_to_season"]:
                    return s["stadium_id"]
            # Si no hay rango válido, devolver el más reciente
            if candidates:
                return max(candidates, key=lambda x: x["valid_to_season"])["stadium_id"]
            return None

        # 4. Obtener partidos a procesar
        where = "TRUE" if force else "m.stadium_id IS NULL"
        matches = conn.execute(text(f"""
            SELECT m.match_id, m.id_sofascore, m.venue_name,
                   m.home_team_id, m.season
            FROM dim_match m
            WHERE {where}
            ORDER BY m.match_id
        """)).mappings().fetchall()

        total_matches = conn.execute(text("SELECT COUNT(*) FROM dim_match")).scalar()
        print(f"  Partidos a evaluar: {len(matches)} / {total_matches}")

        # 5. Resolver stadium_id para cada partido
        updates = []
        stats = {"ss_exact": 0, "ws_exact": 0, "fallback": 0, "none": 0}

        for m in matches:
            sid = None
            source = None

            # Prioridad 1: SofaScore venue name
            ss_key = str(m["id_sofascore"]) if m["id_sofascore"] else None
            if ss_key and ss_key in ss_venues:
                sid = _find_by_name(ss_venues[ss_key])
                if sid:
                    source = "ss_exact"

            # Prioridad 2: WhoScored venue_name
            if sid is None and m["venue_name"]:
                sid = _find_by_name(m["venue_name"])
                if sid:
                    source = "ws_exact"

            # Prioridad 3: Fallback home team
            if sid is None and m["home_team_id"] and m["season"]:
                sid = _find_by_team(m["home_team_id"], m["season"])
                if sid:
                    source = "fallback"

            if sid:
                stats[source] += 1
                updates.append({"match_id": m["match_id"], "stadium_id": sid})
            else:
                stats["none"] += 1

        # 6. Aplicar updates
        if not dry_run and updates:
            for u in updates:
                conn.execute(text(
                    "UPDATE dim_match SET stadium_id = :stadium_id WHERE match_id = :match_id"
                ), u)

        after = conn.execute(text(
            "SELECT COUNT(*) FROM dim_match WHERE stadium_id IS NOT NULL"
        )).scalar()
        without = total_matches - after

        # 7. Resumen
        print(f"\n{'='*60}")
        print(f"  {'DRY-RUN — ' if dry_run else ''}Backfill stadium_id en dim_match")
        print(f"{'='*60}")
        print(f"  Total partidos:           {total_matches:,}")
        print(f"  Ya tenían stadium:        {before:,}")
        print(f"  ── Nuevos enlazados ──")
        print(f"    Via SofaScore venue:    {stats['ss_exact']:,}")
        print(f"    Via WhoScored venue:    {stats['ws_exact']:,}")
        print(f"    Fallback (home team):   {stats['fallback']:,}")
        print(f"    Sin resolver:           {stats['none']:,}")
        print(f"  Con stadium ahora:        {after:,}")
        print(f"  Sin stadium:              {without:,}")
        if total_matches:
            print(f"  Cobertura:                {100*after/total_matches:.1f}%")
        print(f"{'='*60}")

        # 8. Detalle de los que faltan
        if without > 0:
            rows = conn.execute(text("""
                SELECT m.season,
                       ht.canonical_name AS home_team,
                       COUNT(*) AS partidos
                FROM dim_match m
                LEFT JOIN dim_team ht ON ht.canonical_id = m.home_team_id
                WHERE m.stadium_id IS NULL
                GROUP BY m.season, ht.canonical_name
                ORDER BY COUNT(*) DESC
                LIMIT 15
            """)).mappings().fetchall()

            print(f"\n  Top equipos/temporadas SIN stadium:")
            for r in rows:
                print(f"    {r['season']}  {r['home_team'] or '?':<30s}  {r['partidos']} partidos")


def main():
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s - %(message)s")
    parser = argparse.ArgumentParser(
        description="Backfill dim_match.stadium_id (multinivel: SS venue → WS venue → home team)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Solo muestra cuántos se enlazarían, sin modificar.")
    parser.add_argument("--force", action="store_true",
                        help="Re-evalúa todos los partidos, no solo los que tienen stadium_id NULL.")
    args = parser.parse_args()
    backfill(dry_run=args.dry_run, force=args.force)


if __name__ == "__main__":
    main()
