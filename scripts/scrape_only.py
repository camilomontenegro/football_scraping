"""
scrape_only.py
==============
Descargar datos de TODOS los scrapers (sin cargar en BD).

Uso:
    python -m scripts.scrape_only --understat       # Solo Understat
    python -m scripts.scrape_only --sofascore       # Solo SofaScore
    python -m scripts.scrape_only --statsbomb       # Solo StatsBomb
    python -m scripts.scrape_only --transfermarkt   # Solo Transfermarkt
    python -m scripts.scrape_only --all             # Todos los scrapers
    python -m scripts.scrape_only                   # Default = --all

    python -m scripts.scrape_only --update                        # Incremental (auto-detecta fecha desde BD)
    python -m scripts.scrape_only --from-date 2025-03-01 --all   # Incremental (fecha manual)

Salida:
    data/raw/understat/
    data/raw/sofascore/
    data/raw/statsbomb/
    data/raw/transfermarkt/

Tiempo estimado:
    - Understat:     15-20 minutos
    - StatsBomb:     5-10 minutos
    - Transfermarkt: 10-15 minutos
    - SofaScore:     2-3 horas

Después de descargar:
    python -m scripts.load_dimensions --all
"""

import argparse
import asyncio
import inspect
import logging
import sys
from datetime import datetime
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s - %(message)s",
)
log = logging.getLogger(__name__)


async def run_scraper(scraper_name: str, scraper_func):
    """Ejecuta un scraper con manejo de errores."""
    print(f"\n{'=' * 60}")
    print(f"[>] Iniciando {scraper_name.upper()}")
    print(f"{'=' * 60}")

    try:
        if inspect.iscoroutinefunction(scraper_func):
            await scraper_func()
        else:
            scraper_func()
        print(f"[OK] {scraper_name.upper()} completado")
        return True
    except Exception as e:
        log.error("[ERROR] Error en %s: %s", scraper_name, e, exc_info=True)
        return False


async def main():
    parser = argparse.ArgumentParser(
        description="Descargar datos de todos los scrapers",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ejemplos:
  python -m scripts.scrape_only --understat       # Solo Understat (rápido)
  python -m scripts.scrape_only --statsbomb       # Solo StatsBomb (muy rápido)
  python -m scripts.scrape_only --transfermarkt   # Solo Transfermarkt
  python -m scripts.scrape_only --sofascore       # Solo SofaScore (lento)
  python -m scripts.scrape_only --all             # Todos
  python -m scripts.scrape_only --update          # Incremental desde BD
        """,
    )

    parser.add_argument("--understat",    action="store_true", help="Scraper de Understat")
    parser.add_argument("--sofascore",    action="store_true", help="Scraper de SofaScore")
    parser.add_argument("--statsbomb",    action="store_true", help="Scraper de StatsBomb")
    parser.add_argument("--transfermarkt",action="store_true", help="Scraper de Transfermarkt")
    parser.add_argument("--whoscored",    action="store_true", help="Scraper de WhoScored")
    parser.add_argument("--all",          action="store_true", help="Todos los scrapers")
    parser.add_argument(
        "--competition", "-c", type=str, default="La Liga",
        help="Competición (default: 'La Liga')",
    )
    # Calcular temporada actual por defecto
    curr_year = datetime.now().year
    curr_month = datetime.now().month
    # Si estamos antes de Julio, la temporada es la que empezó el año pasado
    def_season_year = curr_year if curr_month >= 7 else curr_year - 1
    def_season = f"{def_season_year}/{def_season_year + 1}"

    parser.add_argument(
        "--season", "-t", type=str, default=def_season,
        help=f"Temporada (default: {def_season})",
    )
    parser.add_argument(
        "--from-date", type=str, default=None,
        help="Fecha mínima para scraping incremental (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--update", action="store_true",
        help="Consulta la BD para obtener la última fecha y descarga solo partidos nuevos",
    )
    parser.add_argument(
        "--full-refresh", action="store_true",
        help="Fuerza la descarga completa ignorando caché local y base de datos",
    )

    args = parser.parse_args()

    # Sin flags de fuente → ejecutar todos
    if not any([args.understat, args.sofascore, args.statsbomb, args.transfermarkt, args.whoscored, args.all, args.update]):
        args.all = True

    # Resolver from_date
    from_date = args.from_date
    if args.update:
        args.all = True
        from scripts.pipeline_runner import get_last_match_date, get_current_season
        last_date = get_last_match_date(args.competition, args.season)
        if last_date:
            from_date = last_date
            print(f"\n[UPDATE] Último partido en BD: {from_date}")
            # Si los datos nuevos pertenecen a una temporada distinta a la
            # especificada, usamos la temporada actual (basada en hoy).
            current_season = get_current_season()
            if current_season != args.season:
                print(f"[UPDATE] Temporada actual detectada: {current_season} (era {args.season})")
                args.season = current_season
            print(f"[UPDATE] Descargando partidos desde {from_date} para temporada {args.season}...")
        else:
            print(f"\n[UPDATE] No hay datos en BD para {args.competition} {args.season}.")
            print("[UPDATE] Se descargará la temporada completa.")

    if from_date:
        print(f"\n[FILTER] from_date = {from_date}")

    # Importar scrapers
    try:
        from scrapers.understat_scraper import main as understat_main
        from scrapers.sofascore_scraper import main as sofascore_main
        from scrapers.statsbomb_scraper import main as statsbomb_main
        from scrapers.transfermarkt_scraper import main as transfermarkt_main
    except ImportError as e:
        log.error("Error importando scrapers: %s", e)
        return 1

    # Los scrapers con from_date necesitan wrappers para pasarles el argumento
    # ya que main() no acepta parámetros directamente

    from scrapers.understat_scraper import scrape_laliga, save_understat_data, SEASONS_DEFAULT, LEAGUE_DEFAULT
    from scrapers.sofascore_scraper import scrape_sofascore, TOURNAMENT_ID, SEASON_NAMES
    from scrapers.statsbomb_scraper import scrape_statsbomb, COMPETITION_ID, SEASON_IDS, SEASON_LABELS
    from scrapers.transfermarkt_scraper import scrape_transfermarkt, LEAGUE_CODE, SEASONS as TM_SEASONS
    from scripts.competitions import get_competition, get_season_start_year

    season_start = get_season_start_year(args.season)
    comp_config = get_competition(args.competition)

    async def run_understat():
        league_code = None
        if comp_config:
            league_code = comp_config["sources"].get("understat", {}).get("league")
        league_code = league_code or LEAGUE_DEFAULT
        df_matches, df_shots = await scrape_laliga(
            [season_start], league=league_code, from_date=from_date
        )
        save_understat_data(df_matches, df_shots)

    def run_sofascore():
        t_id = None
        if comp_config:
            t_id = comp_config["sources"].get("sofascore", {}).get("tournament_id")
        t_id = t_id or TOURNAMENT_ID
        scrape_sofascore(
            season_name=args.season, 
            tournament_id=t_id, 
            from_date=from_date,
            full_refresh=args.full_refresh
        )

    def run_statsbomb():
        comp_id = None
        if comp_config:
            comp_id = comp_config["sources"].get("statsbomb", {}).get("competition_id")
        comp_id = comp_id or COMPETITION_ID
        scrape_statsbomb(competition_id=comp_id, season_id=season_start, from_date=from_date)

    def run_transfermarkt():
        lc = None
        if comp_config:
            lc = comp_config["sources"].get("transfermarkt", {}).get("league_code")
        lc = lc or LEAGUE_CODE
        scrape_transfermarkt(
            league_code=lc, 
            season=season_start, 
            from_date=from_date,
            full_refresh=args.full_refresh,
            season_label=args.season
        )

    def run_whoscored():
        from scrapers.whoscored_scraper import scrape_whoscored
        scrape_whoscored(
            competition=args.competition,
            season=args.season,
            from_date=from_date
        )

    # ── Understat ────────────────────────────────────────────────────
    if args.all or args.understat:
        success = await run_scraper("understat", run_understat)
        if not success and not args.all:
            return 1

    # ── StatsBomb ────────────────────────────────────────────────────
    if args.all or args.statsbomb:
        success = await run_scraper("statsbomb", run_statsbomb)
        if not success and not args.all:
            return 1

    # ── Transfermarkt ────────────────────────────────────────────────
    if args.all or args.transfermarkt:
        success = await run_scraper("transfermarkt", run_transfermarkt)
        if not success and not args.all:
            return 1

    # ── SofaScore (lento) ────────────────────────────────────────────
    if args.all or args.sofascore:
        if args.sofascore and not args.all:
            print("\n[!] SofaScore es LENTO (~2-3 horas). ¿Continuar? (s/n)")
            if input().lower() != "s":
                print("Cancelado.")
                return 0

        success = await run_scraper("sofascore", run_sofascore)
        if not success and not args.all:
            return 1

    # ── WhoScored ────────────────────────────────────────────────────
    if args.all or args.whoscored:
        if args.whoscored and not args.all:
            print("\n[!] WhoScored requiere abrir navegador. ¿Continuar? (s/n)")
            if input().lower() != "s":
                print("Cancelado.")
                return 0

        success = await run_scraper("whoscored", run_whoscored)
        if not success and not args.all:
            return 1

    # ── Resumen ──────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("[OK] DESCARGA COMPLETADA")
    print("=" * 60)

    data_dir = Path("data/raw")
    if data_dir.exists():
        print("\n[DATA] Datos disponibles:")
        for source_dir in data_dir.iterdir():
            if source_dir.is_dir():
                files = list(source_dir.rglob("*"))
                print(f"  [+] {source_dir.name}: {len(files)} archivos")

    print("\nPróximo paso:")
    print("  python -m scripts.load_dimensions --all")
    print("  (para cargar dim_team, dim_player, dim_match)")

    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
