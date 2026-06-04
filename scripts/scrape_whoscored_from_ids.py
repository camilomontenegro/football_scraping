"""
scripts/scrape_whoscored_from_ids.py
=====================================
Scrape WhoScored /live data para match IDs que ya tenemos pero les falta
el match_centre.json. Salta completamente la fase de discovery (datepicker).

Fuentes de IDs (en orden de prioridad):
  1. data/raw/<comp>/<season>/whoscored/fixtures.json
  2. data/clean/<comp>/<season>/whoscored/matches.csv
  3. Base de datos: dim_match.id_whoscored

Modos de uso:
  # Scrapear solo los que faltan (match_centre.json no existe)
  python -m scripts.scrape_whoscored_from_ids -c "La Liga" -s 2024/2025

  # Re-scrapear TODOS (forzar refresh)
  python -m scripts.scrape_whoscored_from_ids -c "Bundesliga" -s 2023/2024 --force

  # Solo listar cuántos faltan (sin scrapear)
  python -m scripts.scrape_whoscored_from_ids --all --dry-run

  # Todas las competiciones y temporadas disponibles
  python -m scripts.scrape_whoscored_from_ids --all

  # Scrapear IDs específicos (para parches puntuales)
  python -m scripts.scrape_whoscored_from_ids -c "La Liga" -s 2024/2025 --ids 1913882 1913883

  # Después de scrapear, extraer stats automáticamente
  python -m scripts.scrape_whoscored_from_ids -c "La Liga" -s 2024/2025 --extract-stats
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import sys
import time
from pathlib import Path
from typing import Optional

sys.path.append(str(Path(__file__).resolve().parent.parent))

from utils.data_paths import (
    RAW_ROOT,
    CLEAN_ROOT,
    normalize_season,
    slugify_competition,
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger(__name__)


# ── Descubrimiento de IDs ────────────────────────────────────────────

def _collect_ids_from_fixtures(comp_slug: str, season_label: str) -> set[str]:
    """Lee IDs desde fixtures.json de UNA temporada específica."""
    fixtures_path = RAW_ROOT / comp_slug / season_label / "whoscored" / "fixtures.json"
    if not fixtures_path.exists():
        return set()
    try:
        with open(fixtures_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return {str(m["whoscored_match_id"]) for m in data if m.get("whoscored_match_id")}
    except Exception:
        return set()


def _collect_ids_from_clean_csv(comp_slug: str, season_label: str) -> set[str]:
    """Lee IDs desde clean/matches.csv de WhoScored de UNA temporada."""
    csv_path = CLEAN_ROOT / comp_slug / season_label / "whoscored" / "matches.csv"
    if not csv_path.exists():
        return set()
    ids = set()
    try:
        with open(csv_path, "r", encoding="utf-8-sig") as f:
            for row in csv.DictReader(f):
                mid = row.get("whoscored_match_id", "").strip()
                if mid:
                    ids.add(mid)
    except Exception:
        pass
    return ids


def _collect_ids_from_db(comp_slug: str, season_label: str) -> set[str]:
    """Lee IDs desde dim_match en la DB, filtrando por competición Y temporada."""
    try:
        from loaders.common import engine
        from sqlalchemy import text

        # Normalizar season a formato DB (YYYY/YYYY)
        season_db = season_label.replace("_", "/")

        # Buscar el competition_id que corresponde a este comp_slug
        # Probamos varias estrategias de matching
        with engine.connect() as conn:
            # Primero buscar por nombre de competición en dim_competition
            # El comp_slug es la versión slugificada, necesitamos el canonical_name
            comp_row = conn.execute(text("""
                SELECT canonical_id FROM dim_competition
                WHERE LOWER(REPLACE(REPLACE(canonical_name, ' ', '_'), '-', '_')) = :slug
                LIMIT 1
            """), {"slug": comp_slug}).fetchone()

            if comp_row:
                rows = conn.execute(text("""
                    SELECT id_whoscored FROM dim_match
                    WHERE id_whoscored IS NOT NULL
                      AND season = :season
                      AND competition_id = :comp_id
                """), {"season": season_db, "comp_id": comp_row[0]}).fetchall()
            else:
                # Fallback: buscar por nombre de competición en el campo competition
                rows = conn.execute(text("""
                    SELECT id_whoscored FROM dim_match
                    WHERE id_whoscored IS NOT NULL
                      AND season = :season
                      AND LOWER(REPLACE(REPLACE(competition, ' ', '_'), '-', '_')) = :slug
                """), {"season": season_db, "slug": comp_slug}).fetchall()

            return {str(r[0]) for r in rows}
    except Exception as e:
        log.debug("DB no disponible: %s", e)
        return set()


def _has_match_centre(comp_slug: str, season_label: str, match_id: str) -> bool:
    """Comprueba si ya existe match_centre.json con datos.

    Busca primero en la carpeta específica de la temporada, luego en
    CUALQUIER temporada de la misma competición (por si se guardó en otra).
    """
    # Check exacto: comp/season/whoscored/matches/{id}/match_centre.json
    path = RAW_ROOT / comp_slug / season_label / "whoscored" / "matches" / match_id / "match_centre.json"
    if path.exists() and path.stat().st_size > 100:
        return True

    # Fallback: buscar en cualquier temporada de esta competición
    comp_dir = RAW_ROOT / comp_slug
    if comp_dir.is_dir():
        for season_dir in comp_dir.iterdir():
            if not season_dir.is_dir() or season_dir.name == season_label:
                continue
            alt = season_dir / "whoscored" / "matches" / match_id / "match_centre.json"
            if alt.exists() and alt.stat().st_size > 100:
                return True

    return False


def _collect_ids_owned_by_other_seasons(comp_slug: str, season_label: str) -> set[str]:
    """IDs que pertenecen legítimamente a OTRA temporada de esta competición.

    Si un match_centre.json existe en otra carpeta de temporada, el ID
    pertenece a esa temporada y no a la actual. Esto evita contar IDs
    duplicados que aparecen en CSVs multi-season.
    """
    other_ids: set[str] = set()
    comp_dir = RAW_ROOT / comp_slug
    if not comp_dir.is_dir():
        return other_ids

    for season_dir in comp_dir.iterdir():
        if not season_dir.is_dir() or season_dir.name == season_label:
            continue
        # IDs que tienen match_centre.json en esta OTRA temporada
        matches_dir = season_dir / "whoscored" / "matches"
        if matches_dir.is_dir():
            for match_dir in matches_dir.iterdir():
                if match_dir.is_dir():
                    centre = match_dir / "match_centre.json"
                    if centre.exists() and centre.stat().st_size > 100:
                        other_ids.add(match_dir.name)
        # IDs listados en el fixtures.json de esta OTRA temporada
        other_fixtures = season_dir / "whoscored" / "fixtures.json"
        if other_fixtures.exists():
            try:
                with open(other_fixtures, "r", encoding="utf-8") as f:
                    data = json.load(f)
                for m in data:
                    mid = str(m.get("whoscored_match_id", ""))
                    if mid:
                        other_ids.add(mid)
            except Exception:
                pass

    return other_ids


def collect_match_ids(
    comp_slug: str,
    season_label: str,
    force: bool = False,
    specific_ids: Optional[list[str]] = None,
) -> list[str]:
    """Recopila IDs a scrapear para una competición/temporada.

    Returns: lista de match IDs que necesitan scraping.
    """
    if specific_ids:
        all_ids = set(specific_ids)
        log.info("  %s/%s: %d IDs específicos", comp_slug, season_label, len(all_ids))
    else:
        ids_fixtures = _collect_ids_from_fixtures(comp_slug, season_label)
        ids_csv = _collect_ids_from_clean_csv(comp_slug, season_label)
        ids_db = _collect_ids_from_db(comp_slug, season_label)
        all_ids = ids_fixtures | ids_csv | ids_db
        log.info("  %s/%s: IDs encontrados — fixtures=%d, csv=%d, db=%d, union=%d",
                 comp_slug, season_label, len(ids_fixtures), len(ids_csv), len(ids_db), len(all_ids))

    if not all_ids:
        return []

    # Descartar IDs que pertenecen a OTRA temporada de esta misma competición
    # (evita contar IDs duplicados de CSVs multi-season)
    other_season_ids = _collect_ids_owned_by_other_seasons(comp_slug, season_label)
    before = len(all_ids)
    all_ids -= other_season_ids
    if before != len(all_ids):
        log.debug("  %s/%s: descartados %d IDs de otras temporadas",
                  comp_slug, season_label, before - len(all_ids))

    if not all_ids:
        return []

    if force:
        return sorted(all_ids)

    # Filtrar los que ya tienen match_centre.json
    missing = [mid for mid in sorted(all_ids)
               if not _has_match_centre(comp_slug, season_label, mid)]
    return missing


# ── Scraping ─────────────────────────────────────────────────────────

def scrape_match_ids(
    comp_slug: str,
    season_label: str,
    match_ids: list[str],
    dry_run: bool = False,
) -> int:
    """Scrapea los match IDs indicados via WhoScored /live."""
    if not match_ids:
        log.info("  [%s/%s] Nada que scrapear", comp_slug, season_label)
        return 0

    if dry_run:
        log.info("  [DRY-RUN] %s/%s: %d partidos pendientes", comp_slug, season_label, len(match_ids))
        return 0

    # Import scraper functions
    from scrapers.whoscored_scraper import (
        create_driver,
        restart_driver,
        accept_cookies,
        get_match_data,
        extract_events,
        extract_players_from_match,
        extract_teams_from_match,
        _save_match_raw,
        _write_clean_season,
        _collect_whoscored_events_from_raw,
        DRIVER_RESTART_EVERY,
        FAIL_STREAK_LIMIT,
        LONG_PAUSE_SECONDS,
    )

    # Derivar el nombre de competición del slug
    comp_name = comp_slug.replace("_", " ").title()
    # Intentar resolverlo via competitions.py
    try:
        from wizard.competitions import COMPETITIONS
        for name, cfg in COMPETITIONS.items():
            if slugify_competition(name) == comp_slug:
                comp_name = name
                break
    except Exception:
        pass

    season_ws = season_label.replace("_", "/")
    # Formato corto si aplica (25/26 en lugar de 2025/2026)
    if "/" in season_ws:
        parts = season_ws.split("/")
        if len(parts[0]) == 4 and len(parts[1]) == 4:
            season_ws = f"{parts[0]}/{parts[1][2:]}"

    log.info("[SCRAPE] %s/%s — %d partidos", comp_slug, season_label, len(match_ids))

    driver = create_driver()
    scraped = 0
    all_matches = []
    all_events = []
    all_players = []
    all_teams = []
    fail_streak = 0

    try:
        driver.get("https://es.whoscored.com")
        time.sleep(5)
        accept_cookies(driver)

        for i, mid in enumerate(match_ids, 1):
            if i > 1 and (i - 1) % DRIVER_RESTART_EVERY == 0:
                log.info("[ANTI-BOT] Reinicio preventivo del driver")
                driver = restart_driver(driver)

            log.info("  [%d/%d] Partido %s", i, len(match_ids), mid)
            match_data = get_match_data(driver, mid, season_ws)

            if not match_data or "events" not in match_data:
                fail_streak += 1
                log.warning("  Fallo %d/%d", fail_streak, FAIL_STREAK_LIMIT)
                if fail_streak >= FAIL_STREAK_LIMIT:
                    log.warning("[ANTI-BOT] Pausa larga + reinicio...")
                    time.sleep(LONG_PAUSE_SECONDS)
                    driver = restart_driver(driver)
                    fail_streak = 0
                continue

            fail_streak = 0
            _save_match_raw(comp_name, season_ws, str(mid), match_data)

            all_matches.append({
                "whoscored_match_id": mid,
                "season": season_ws,
                "match_date": match_data.get("match_date"),
                "attendance": match_data.get("attendance"),
            })
            all_events.extend(extract_events(match_data))
            all_players.extend(extract_players_from_match(match_data, competition=comp_name))
            all_teams.extend(extract_teams_from_match(match_data, competition=comp_name))
            scraped += 1

            if i % 10 == 0:
                log.info("  -> %d/%d completados", scraped, len(match_ids))

    except Exception as e:
        log.error("Error fatal: %s", e)
    finally:
        try:
            driver.quit()
        except Exception:
            pass

    # Escribir CSVs clean (append-safe: reconstruye desde todos los raw)
    if scraped > 0:
        s_events = _collect_whoscored_events_from_raw(comp_name, season_ws)
        if not s_events:
            s_events = all_events
        _write_clean_season(comp_name, season_ws, all_matches, s_events, all_players, all_teams)
        log.info("[OK] %d/%d partidos scrapeados en %s/%s", scraped, len(match_ids), comp_slug, season_label)

    return scraped


# ── Orquestador ─────────────────────────────────────────────────────

def discover_all_targets() -> list[tuple[str, str]]:
    """Descubre todas las (comp_slug, season) que tienen IDs de WhoScored."""
    targets = set()
    # Desde fixtures.json
    for path in RAW_ROOT.glob("*/*/whoscored/fixtures.json"):
        parts = path.relative_to(RAW_ROOT).parts
        targets.add((parts[0], parts[1]))
    # Desde clean CSVs
    for path in CLEAN_ROOT.glob("*/*/whoscored/matches.csv"):
        parts = path.relative_to(CLEAN_ROOT).parts
        targets.add((parts[0], parts[1]))
    return sorted(targets)


def run(
    competition: Optional[str] = None,
    season: Optional[str] = None,
    all_comps: bool = False,
    force: bool = False,
    dry_run: bool = False,
    specific_ids: Optional[list[str]] = None,
    extract_stats: bool = False,
):
    """Punto de entrada principal."""
    if all_comps:
        targets = discover_all_targets()
    elif competition and season:
        comp_slug = slugify_competition(competition)
        season_label = normalize_season(season) or season.replace("/", "_")
        targets = [(comp_slug, season_label)]
    else:
        log.error("Especifica --competition + --season, o usa --all")
        return

    total_scraped = 0
    for comp_slug, season_label in targets:
        ids = collect_match_ids(comp_slug, season_label, force=force, specific_ids=specific_ids)
        if not ids and not dry_run:
            log.info("[%s/%s] Todo scrapeado, nada pendiente", comp_slug, season_label)
            continue

        scraped = scrape_match_ids(comp_slug, season_label, ids, dry_run=dry_run)
        total_scraped += scraped

    if total_scraped > 0 and extract_stats:
        log.info("\n[EXTRACT] Extrayendo stats de los nuevos match_centre.json...")
        from scrapers.whoscored_stats_extractor import run as extract_run
        extract_run(competition=competition, season=season)

    log.info("\n[DONE] Total scrapeado: %d partidos", total_scraped)


# ── CLI ──────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Scrapea WhoScored /live usando IDs ya conocidos (sin discovery)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ejemplos:
  # Ver qué falta sin scrapear
  python -m scripts.scrape_whoscored_from_ids --all --dry-run

  # Scrapear La Liga 2024/25 (solo los que faltan)
  python -m scripts.scrape_whoscored_from_ids -c "La Liga" -s 2024/2025

  # Forzar re-scrape de todo Bundesliga 2023/24
  python -m scripts.scrape_whoscored_from_ids -c "Bundesliga" -s 2023/2024 --force

  # Scrapear IDs puntuales
  python -m scripts.scrape_whoscored_from_ids -c "La Liga" -s 2024/2025 --ids 1913882 1913883

  # Scrapear + extraer stats automáticamente
  python -m scripts.scrape_whoscored_from_ids -c "La Liga" -s 2024/2025 --extract-stats
        """,
    )
    parser.add_argument("-c", "--competition", default=None,
                        help="Competición (ej. 'La Liga', 'Bundesliga')")
    parser.add_argument("-s", "--season", default=None,
                        help="Temporada (ej. '2024/2025')")
    parser.add_argument("--all", action="store_true",
                        help="Procesar todas las competiciones/temporadas con IDs")
    parser.add_argument("--force", action="store_true",
                        help="Re-scrapear aunque ya exista match_centre.json")
    parser.add_argument("--dry-run", action="store_true",
                        help="Solo mostrar cuántos faltan, no scrapear")
    parser.add_argument("--ids", nargs="+", default=None,
                        help="IDs específicos a scrapear")
    parser.add_argument("--extract-stats", action="store_true",
                        help="Ejecutar whoscored_stats_extractor después del scraping")
    args = parser.parse_args()

    if not args.competition and not args.all:
        parser.error("Usa -c/--competition + -s/--season, o --all")

    print("=" * 55)
    print("  WhoScored /live Scraper (from known IDs)")
    print("=" * 55)

    run(
        competition=args.competition,
        season=args.season,
        all_comps=args.all,
        force=args.force,
        dry_run=args.dry_run,
        specific_ids=args.ids,
        extract_stats=args.extract_stats,
    )


if __name__ == "__main__":
    main()
