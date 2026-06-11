"""
scripts/fix_stadium_names.py
============================
Corrige dim_stadium.stadium_name usando el venueName de los match_centre.json
de WhoScored como fuente de verdad histórica.

Problema: TM siempre devuelve el nombre ACTUAL del estadio (naming rights 2026),
no el que tenía en cada temporada. WhoScored sí registra el nombre correcto
del partido en su momento.

Lógica:
  1. Lee todos los match_centre.json: extrae home_team_ws_id + venueName + season.
  2. Agrupa por equipo+temporada: el venue más frecuente = nombre real del estadio.
  3. Cruza con dim_stadium via dim_team.id_whoscored → actualiza stadium_name.
  4. Para temporadas sin datos WhoScored, mantiene el nombre actual de TM.

Uso:
    python -m scripts.fix_stadium_names --dry-run
    python -m scripts.fix_stadium_names
    python -m scripts.fix_stadium_names --rescrape --seasons 2020 2021 2022
    python -m scripts.fix_stadium_names --reload-db
"""

from __future__ import annotations

import argparse
import json
import logging
import subprocess
import sys
from collections import Counter, defaultdict
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from sqlalchemy import text

from loaders.common import engine
from loaders.stadium_loader import load_stadiums
from scripts.competitions import COMPETITIONS, get_competition
from utils.data_paths import CLEAN_ROOT, RAW_ROOT, slugify_competition

log = logging.getLogger(__name__)

_SKIP_CLEAN_DIRS = frozenset({"attendance", "market_value", "transfers", "archive"})


def _slug_to_canonical_map() -> dict[str, str]:
    """Mapea slug de carpeta (la_liga) → nombre canónico (La Liga)."""
    mapping: dict[str, str] = {}
    for name in COMPETITIONS:
        mapping[slugify_competition(name)] = name

    try:
        with engine.connect() as conn:
            rows = conn.execute(text(
                "SELECT canonical_name FROM dim_competition WHERE canonical_name IS NOT NULL"
            )).fetchall()
        for (canonical_name,) in rows:
            mapping[slugify_competition(canonical_name)] = canonical_name
    except Exception as e:
        log.warning("No se pudo leer dim_competition para mapeo de slugs: %s", e)

    return mapping


def resolve_competition_name(identifier: str) -> str | None:
    """Resuelve slug o alias al nombre canónico que espera el scraper TM."""
    if not identifier:
        return None
    if get_competition(identifier):
        return identifier

    slug = slugify_competition(identifier.replace("-", " "))
    resolved = _slug_to_canonical_map().get(slug)
    if resolved and get_competition(resolved):
        return resolved
    return None


def _discover_competition_slugs() -> list[str]:
    """Slugs en data/clean/ que parecen competiciones."""
    if not CLEAN_ROOT.exists():
        return []
    return sorted(
        d.name for d in CLEAN_ROOT.iterdir()
        if d.is_dir() and d.name not in _SKIP_CLEAN_DIRS
    )


def rescrape_stadiums(
    seasons: list[int],
    competitions: list[str] | None = None,
    full_refresh: bool = False,
) -> int:
    """Re-scrapea stadiums.csv en Transfermarkt usando nombres canónicos."""
    slugs = competitions or _discover_competition_slugs()
    if not slugs:
        print("  No hay competiciones para re-scrapear.")
        return 1

    failed = 0
    for slug in slugs:
        canonical = resolve_competition_name(slug)
        if not canonical:
            print(f"Error: slug '{slug}' no mapea a ninguna competición conocida.")
            failed += 1
            continue

        if canonical != slug:
            print(f"\n  [{slug}] → \"{canonical}\"")

        cmd = [
            sys.executable, "-m", "scrapers.transfermarkt_stadiums_scraper",
            "--competition", canonical,
            "--seasons", *[str(y) for y in seasons],
        ]
        if full_refresh:
            cmd.append("--full-refresh")

        print(f"\n  $ {' '.join(cmd)}")
        result = subprocess.run(cmd, check=False)
        if result.returncode != 0:
            failed += 1

    print("\n  Re-scrape completado.", end="")
    if failed:
        print(f" {failed} competición(es) con error.")
    else:
        print(" Sin errores.")
    print("  Ejecuta --reload-db para actualizar dim_stadium.")
    return failed


def reload_db():
    """Recarga dim_stadium desde los CSV en data/clean/."""
    print("\n  Recargando dim_stadium desde CSV...")
    with engine.begin() as conn:
        n = load_stadiums(conn)
    print(f"  [OK] stadium_loader completado — {n} filas afectadas")


def _extract_venues_from_match_centres() -> dict[int, dict[str, str]]:
    """
    Lee match_centre.json y retorna:
      {home_team_ws_id: {season_label: most_common_venue_name}}
    """
    # home_ws_id -> season -> list of venue names
    raw: dict[int, dict[str, list[str]]] = defaultdict(lambda: defaultdict(list))

    for mc_path in RAW_ROOT.rglob("whoscored/matches/*/match_centre.json"):
        # Extraer season del path
        season = None
        for part in mc_path.parts:
            if "_" in part and len(part) == 9 and part[:4].isdigit():
                season = part
                break
        if not season:
            continue

        try:
            data = json.loads(mc_path.read_text(encoding="utf-8"))
            home_id = data.get("home", {}).get("teamId")
            venue = (data.get("venueName") or "").strip()
            if home_id and venue:
                raw[int(home_id)][season].append(venue)
        except Exception:
            pass

    # Reducir a nombre más frecuente por equipo+temporada
    result: dict[int, dict[str, str]] = {}
    for ws_id, seasons in raw.items():
        result[ws_id] = {}
        for season, venues in seasons.items():
            most_common = Counter(venues).most_common(1)[0][0]
            result[ws_id][season] = most_common

    return result


def fix_names(dry_run: bool = False):
    # 1. Extraer venues de WhoScored
    print("  Leyendo match_centre.json...")
    ws_venues = _extract_venues_from_match_centres()
    total_entries = sum(len(v) for v in ws_venues.values())
    print(f"  Equipos WS con venue: {len(ws_venues)}, entradas equipo+temporada: {total_entries}")

    with engine.begin() as conn:
        # 2. Cargar mapping WS team ID → canonical_id
        team_rows = conn.execute(text(
            "SELECT canonical_id, id_whoscored FROM dim_team WHERE id_whoscored IS NOT NULL"
        )).fetchall()
        ws_to_canonical = {r[1]: r[0] for r in team_rows}

        # 3. Cargar dim_stadium actual
        stadiums = conn.execute(text("""
            SELECT stadium_id, canonical_team_id, stadium_name,
                   valid_from_season, valid_to_season
            FROM dim_stadium
        """)).mappings().fetchall()

        # Indexar: (canonical_team_id, season) -> stadium row
        # Un equipo puede tener múltiples rangos de temporada
        stadium_lookup: dict[tuple[int, str], dict] = {}
        for s in stadiums:
            tid = s["canonical_team_id"]
            if not tid:
                continue
            # Expandir el rango para indexar por cada temporada cubierta
            stadium_lookup[(tid, s["valid_from_season"])] = dict(s)

        # 4. Generar correcciones
        fixes = []
        for ws_id, seasons in ws_venues.items():
            canonical_id = ws_to_canonical.get(ws_id)
            if not canonical_id:
                continue

            for season_label, ws_venue_name in seasons.items():
                season_db = season_label.replace("_", "/")
                key = (canonical_id, season_db)

                if key not in stadium_lookup:
                    continue

                current = stadium_lookup[key]
                current_name = current["stadium_name"] or ""

                # ¿Necesita corrección?
                if current_name.strip().lower() != ws_venue_name.strip().lower():
                    fixes.append({
                        "stadium_id": current["stadium_id"],
                        "canonical_team_id": canonical_id,
                        "season": season_db,
                        "old_name": current_name,
                        "new_name": ws_venue_name,
                    })

        # 5. Mostrar resumen
        print(f"\n{'='*70}")
        print(f"  {'DRY-RUN — ' if dry_run else ''}Corrección de stadium_name via WhoScored")
        print(f"{'='*70}")
        print(f"  Entradas dim_stadium a corregir: {len(fixes)}")

        if fixes:
            print(f"\n  Muestra de correcciones:")
            for f in fixes[:20]:
                print(f"    {f['season']}  {f['old_name']:<35s} → {f['new_name']}")
            if len(fixes) > 20:
                print(f"    ... y {len(fixes) - 20} más")

        if dry_run or not fixes:
            if not fixes:
                print("  Nada que corregir.")
            return

        # 6. Aplicar
        updated = 0
        for f in fixes:
            result = conn.execute(text("""
                UPDATE dim_stadium
                SET stadium_name = :new_name,
                    updated_at = NOW()
                WHERE stadium_id = :sid
            """), {"new_name": f["new_name"], "sid": f["stadium_id"]})
            updated += result.rowcount

        print(f"\n  ✓ dim_stadium actualizados: {updated}")

        # 7. Estadísticas de lo que queda sin datos WhoScored
        still_tm = conn.execute(text("""
            SELECT COUNT(*) FROM dim_stadium
            WHERE stadium_name IS NOT NULL
        """)).scalar()
        print(f"  Total dim_stadium con nombre: {still_tm}")
        print(f"  (los que no tienen datos WhoScored mantienen el nombre actual de TM)")


def main():
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s - %(message)s")
    parser = argparse.ArgumentParser(
        description="Corrige dim_stadium.stadium_name usando venueName de WhoScored")
    parser.add_argument("--dry-run", action="store_true",
                        help="Solo muestra qué se corregiría, sin aplicar cambios.")
    parser.add_argument("--rescrape", action="store_true",
                        help="Re-scrapea stadiums.csv en Transfermarkt (nombres canónicos).")
    parser.add_argument("--reload-db", action="store_true",
                        help="Recarga dim_stadium desde data/clean/*/transfermarkt/stadiums.csv.")
    parser.add_argument("--seasons", nargs="+", type=int, default=[2020, 2021, 2022],
                        help="Temporadas para --rescrape (año inicio, ej: 2020 2021 2022).")
    parser.add_argument("--competition", action="append", dest="competitions",
                        metavar="SLUG",
                        help="Limita --rescrape a slug(s) concretos (ej: la_liga). Repetible.")
    parser.add_argument("--full-refresh", action="store_true",
                        help="Ignora caché TM de 30 días en --rescrape.")
    args = parser.parse_args()

    if args.rescrape:
        code = rescrape_stadiums(
            seasons=args.seasons,
            competitions=args.competitions,
            full_refresh=args.full_refresh,
        )
        if code and not args.reload_db:
            sys.exit(1)

    if args.reload_db:
        reload_db()

    if not args.rescrape and not args.reload_db:
        fix_names(dry_run=args.dry_run)


if __name__ == "__main__":
    main()
