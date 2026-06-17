"""
scrape_whoscored_by_ids.py
==========================
Descarga matchCentreData (/live) para una lista concreta de IDs WhoScored.

Guarda en:
    data/raw/<comp_slug>/<season>/whoscored/matches/<ws_id>/match_centre.json
    (+ events.json, lineups.json, match_meta.json)

Después:
    python -m scrapers.whoscored_stats_extractor -c "<comp>" -s "2025/2026"
    python -m loaders.whoscored_stats_loader

Uso:
    python -m scripts.scrape_whoscored_by_ids -c "La Liga" -s "2025/26" \\
        --match-ids 1874832 1874833

    # IDs desde dim_match (competición/temporada en BD):
    python -m scripts.scrape_whoscored_by_ids -c "La Liga" -s "2025/26" --from-db

    python -m scripts.scrape_whoscored_by_ids -c "La Liga" -s "2025/26" \\
        --from-db --limit 20 --skip-existing
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from sqlalchemy import text

from loaders.common import engine
from utils.scraper_lock import acquire_scraper_lock
from scrapers.whoscored_scraper import (
    DRIVER_RESTART_EVERY,
    FAIL_STREAK_LIMIT,
    LONG_PAUSE_SECONDS,
    _folder_season,
    _normalize_season,
    _save_match_raw,
    _season_raw_dir,
    accept_cookies,
    create_driver,
    get_match_data,
    restart_driver,
    whoscored_season_available,
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger(__name__)


def _competition_id(competition: str) -> int | None:
    from scripts.competitions import get_competition

    conf = get_competition(competition)
    if not conf:
        return None
    code = conf.get("sources", {}).get("transfermarkt", {}).get("league_code")
    if not code:
        return None
    with engine.connect() as conn:
        return conn.execute(
            text(
                "SELECT canonical_id FROM dim_competition "
                "WHERE id_transfermarkt = :c"
            ),
            {"c": code},
        ).scalar()


def _ids_from_db(
    competition: str,
    season_ws: str,
    limit: int | None,
    only_gaps: bool = False,
) -> list[str]:
    """Lee id_whoscored de dim_match para la competición/temporada."""
    from utils.data_paths import season_db_format

    comp_id = _competition_id(competition)
    if comp_id is None:
        log.error("No se encontró competition_id en dim_competition para %s", competition)
        return []

    season_db = season_db_format(season_ws)
    gap_filter = """
          AND (m.referee_id IS NULL OR m.manager_home IS NULL OR m.manager_away IS NULL
               OR m.attendance IS NULL OR m.attendance = 0)
    """ if only_gaps else ""
    sql = f"""
        SELECT m.id_whoscored
        FROM dim_match m
        WHERE m.id_whoscored IS NOT NULL
          AND m.competition_id = :comp_id
          AND m.season = :season
          {gap_filter}
        ORDER BY m.match_id
    """
    with engine.connect() as conn:
        rows = conn.execute(
            text(sql), {"comp_id": comp_id, "season": season_db},
        ).fetchall()
    ids = [str(r[0]) for r in rows]
    if limit:
        ids = ids[:limit]
    return ids


def _already_scraped(competition: str, season_ws: str, ws_id: str) -> bool:
    centre = (
        _season_raw_dir(competition, season_ws)
        / "matches"
        / str(ws_id)
        / "match_centre.json"
    )
    return centre.exists() and centre.stat().st_size > 100


def main() -> None:
    acquire_scraper_lock("scrape_whoscored_by_ids")
    parser = argparse.ArgumentParser(
        description="Scrape WhoScored /live para IDs concretos (matchCentreData)",
    )
    parser.add_argument(
        "-c", "--competition", required=True,
        help='Competición (ej. "La Liga")',
    )
    parser.add_argument(
        "-s", "--season", required=True,
        help='Temporada WhoScored (ej. "2025/26" o "2025/2026")',
    )
    parser.add_argument(
        "--match-ids", nargs="+", type=int, default=None,
        help="IDs WhoScored del partido (los de la URL /matches/<id>/live)",
    )
    parser.add_argument(
        "--from-db", action="store_true",
        help="Tomar id_whoscored desde dim_match (competición + temporada)",
    )
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument(
        "--skip-existing", action="store_true",
        help="No volver a descargar si match_centre.json ya existe",
    )
    parser.add_argument(
        "--only-gaps", action="store_true",
        help="Con --from-db, solo partidos sin árbitro/managers/asistencia",
    )
    args = parser.parse_args()

    season_format = "range"
    try:
        from scripts.competitions import get_competition
        comp = get_competition(args.competition) or {}
        season_format = (
            comp.get("sources", {}).get("whoscored", {}).get("season_format", "range")
        )
    except Exception:
        pass

    season_ws = _normalize_season(args.season, season_format)

    if not whoscored_season_available(args.competition, args.season):
        log.warning(
            "Temporada %s no está en WHOSCORED_STAGES para %s; "
            "se guardará igual en data/raw/.../%s/",
            season_ws, args.competition, _folder_season(season_ws),
        )

    if args.from_db:
        match_ids = _ids_from_db(
            args.competition, args.season, args.limit, only_gaps=args.only_gaps,
        )
    elif args.match_ids:
        match_ids = [str(i) for i in args.match_ids]
        if args.limit:
            match_ids = match_ids[: args.limit]
    else:
        parser.error("Indica --match-ids o --from-db")

    if not match_ids:
        print("[!] No hay IDs para procesar.")
        return

    to_scrape = []
    skipped = 0
    for mid in match_ids:
        if args.skip_existing and _already_scraped(args.competition, season_ws, mid):
            skipped += 1
            continue
        to_scrape.append(mid)

    print("=" * 55)
    print(f"  WhoScored by IDs — {args.competition} {season_ws}")
    print(f"  Descargar: {len(to_scrape)} | omitidos (ya en disco): {skipped}")
    print(f"  Destino: {_season_raw_dir(args.competition, season_ws) / 'matches'}")
    print("=" * 55)

    if not to_scrape:
        print("[OK] Todos los partidos ya tienen match_centre.json.")
        return

    driver = create_driver()
    ok = fail = 0
    fail_streak = 0

    try:
        driver.get("https://es.whoscored.com")
        time.sleep(5)
        accept_cookies(driver)

        for i, mid in enumerate(to_scrape, 1):
            if i > 1 and (i - 1) % DRIVER_RESTART_EVERY == 0:
                driver = restart_driver(driver)

            log.info("[%d/%d] Partido %s", i, len(to_scrape), mid)
            data = get_match_data(driver, mid, season_ws)

            if not data or not data.get("home"):
                fail += 1
                fail_streak += 1
                log.warning("  Sin matchCentreData válido")
                if fail_streak >= FAIL_STREAK_LIMIT:
                    log.warning(
                        "[ANTI-BOT] Pausa %ds y reinicio de driver...",
                        LONG_PAUSE_SECONDS,
                    )
                    time.sleep(LONG_PAUSE_SECONDS)
                    driver = restart_driver(driver)
                    fail_streak = 0
                continue

            fail_streak = 0
            path = _save_match_raw(args.competition, season_ws, mid, data)
            ok += 1
            log.info("  -> %s", path / "match_centre.json")

    finally:
        try:
            driver.quit()
        except Exception:
            pass

    print(f"\n[OK] {ok} guardados, {fail} fallidos, {skipped} omitidos en disco")


if __name__ == "__main__":
    main()
