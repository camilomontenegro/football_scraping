"""
loaders/generic_loader.py
==========================
Loader genérico para cargar cualquier competición en la base de datos.
Sustituye a los loaders individuales por competición (la_liga_loader.py,
bundesliga_loader.py, etc.).
 
Usa competitions.py para obtener:
    - folder:       nombre de la carpeta en data/raw/<fuente>/<folder>/
    - data_sources: fuentes disponibles para esa competición
    - league_code:  código de Transfermarkt para obtener competition_id

Navegacion: 
_menu_principal()
    └── _menu_dimensions()       ← llamada cuando se selecciona una competición
            └── _menu_facts()    ← llamada cuando se selecciona opción 4 "Continuar a hechos"

Menú de navegación:
    MENÚ PRINCIPAL → elegir competición
    MENÚ DIMENSIONES → Teams / Players / Matches / Continuar a hechos / Volver
    MENÚ HECHOS → Shots / Events / Injuries / Volver a dimensiones / Volver al principal
 
Uso:
    python -m loaders.generic_loader
"""
 
import logging
from pathlib import Path

from sqlalchemy import text

from loaders.common import engine
from loaders.fact_loader_generico import load_events, load_injuries, load_shots
from loaders.match_loader_generico import load_matches
from loaders.player_loader_generico import load_players
from loaders.team_loader_generico import load_teams
from utils.data_paths import clean_dir
from wizard.competitions import (
    COMPETITIONS,
    WORKING_COMPETITION_NAMES,
    get_competition,
)

log = logging.getLogger(__name__)

# forma el directorio raiz desde la ruta actual
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Temporada por defecto del menu interactivo. Para historico cambiarla aqui.
DEFAULT_SEASON = "2025_2026"


def _competition_sources(competition_name: str) -> set[str]:
    """Devuelve las fuentes configuradas para una competicion.

    Se deriva del campo `sources` del diccionario COMPETITIONS: una fuente
    cuenta como configurada si su entrada existe y tiene al menos un ID util
    (league_code, tournament_id, league, competition_id...).
    """
    comp = get_competition(competition_name) or {}
    sources_cfg = comp.get("sources", {}) or {}
    out: set[str] = set()
    if sources_cfg.get("transfermarkt", {}).get("league_code"):
        out.add("transfermarkt")
    if sources_cfg.get("sofascore", {}).get("tournament_id") is not None:
        out.add("sofascore")
    if sources_cfg.get("understat", {}).get("league"):
        out.add("understat")
    if sources_cfg.get("statsbomb", {}).get("competition_id") is not None:
        out.add("statsbomb")
    if sources_cfg.get("whoscored", {}).get("tournament_id") is not None:
        out.add("whoscored")
    return out
 
def _setup_logging(competition_name: str) -> None:
    """
    Configura el logging para escribir en consola y en archivo.
    Crea una carpeta 'generic_loader' dentro de logs y un archivo por competición.
    Ejemplo: "La Liga" → logs/generic_loader/la_liga.log
    """
    log_filename = competition_name.lower().replace(" ", "_") + ".log"
    log_path = PROJECT_ROOT / "logs" / "generic_loader" / log_filename
    log_path.parent.mkdir(parents=True, exist_ok=True)
    
    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(logging.Formatter("[%(asctime)s] %(levelname)s - %(message)s"))
    logging.getLogger().addHandler(file_handler)
# ── Helpers ───────────────────────────────────────────────────────────────────
 
def _get_paths(competition_name: str, season: str = DEFAULT_SEASON) -> dict[str, Path]:
    """
    Construye las rutas a las carpetas de `data/clean/<comp>/<season>/<source>/`
    para cada fuente configurada.

    Devuelve un diccionario con claves 'ss', 'tm', 'ws', 'us', 'sb'
    apuntando al directorio CLEAN (existan o no en disco).
    """
    return {
        "ss": clean_dir(competition_name, season, "sofascore"),
        "tm": clean_dir(competition_name, season, "transfermarkt"),
        "ws": clean_dir(competition_name, season, "whoscored"),
        "us": clean_dir(competition_name, season, "understat"),
        "sb": clean_dir(competition_name, season, "statsbomb"),
    }


def _get_competition_id(conn, competition_name: str) -> int:
    """
    Obtiene el canonical_id de la competición en dim_competition
    usando el league_code de Transfermarkt definido en competitions.py.
    """
    comp = get_competition(competition_name)
    league_code = comp["sources"]["transfermarkt"]["league_code"]
    return conn.execute(
        text("SELECT canonical_id FROM dim_competition WHERE id_transfermarkt = :code"),
        {"code": league_code},
    ).scalar()


def _has_source(competition_name: str, source: str) -> bool:
    """Comprueba si una fuente está configurada para la competición."""
    return source in _competition_sources(competition_name)
 
 
# ── Menú de hechos ────────────────────────────────────────────────────────────
def _menu_facts(competition_name: str, competition_id: int, paths: dict) -> str:
    """
    Muestra el menú de hechos y ejecuta la opción elegida.
 
    Devuelve:
        'principal' → volver al menú principal
        'dimensiones' → volver al menú de dimensiones
    """
    while True:
        print(f"\n=== {competition_name} — Hechos ===")
        print("1. Shots")
        print("2. Events")
        print("3. Injuries")
        print("4. Volver a dimensiones")
        print("5. Volver al menú principal")
 
        opcion = input("Selecciona (1-5): ").strip()

        # ------- SHOTS --------
        if opcion == "1":
            log.info("Cargando shots...")
            # comprueba si understat es fuente para la competicion y obtiene la ruta en caso afirmativo
            us_path = paths["us"] if _has_source(competition_name, "understat") else None
            try:
                with engine.begin() as conn:
                    load_shots(
                        conn,
                        ss_path=paths["ss"],
                        competition_id=competition_id,
                        us_path=us_path,
                    )
                log.info("Shots completado.")
            except Exception as e:
                log.error("Error cargando shots: %s", e)
            log.info("-" * 50)

        # ------- EVENTS --------
        elif opcion == "2":
            log.info("Cargando events...")
            # comprueba si whoscored es fuente para la competicion y obtiene la ruta en caso afirmativo
            ws_path = paths["ws"] if _has_source(competition_name, "whoscored") else None
            try:
                with engine.begin() as conn:
                    load_events(
                        conn,
                        ss_path=paths["ss"],
                        ws_path=ws_path,
                    )
                log.info("Events completado.")
            except Exception as e:
                log.error("Error cargando events: %s", e)
            log.info("-" * 50)

        # ------- INJURIES --------
        elif opcion == "3":
            log.info("Cargando injuries...")
            try:
                with engine.begin() as conn:
                    load_injuries(conn, tm_path=paths["tm"])
                log.info("Injuries completado.")
            except Exception as e:
                log.error("Error cargando injuries: %s", e)
            log.info("-" * 50)

        # ------- VOLVER A DIMENSIONES --------
        elif opcion == "4":
            return "dimensiones"

        # ------- VOLVER AL MENÚ PRINCIPAL --------
        elif opcion == "5":
            return "principal"
 
# ── Menú de dimensiones ───────────────────────────────────────────────────────
 
def _menu_dimensions(competition_name: str, competition_id: int, paths: dict) -> str:
    """
    Muestra el menú de dimensiones y ejecuta la opción elegida.
 
    Devuelve:
        'principal' → volver al menú principal
    """
    while True:
        print(f"\n=== {competition_name} — Dimensiones ===")
        print("1. Teams")
        print("2. Players")
        print("3. Matches")
        print("4. Continuar a hechos")
        print("5. Volver al menú principal")
 
        opcion = input("Selecciona (1-5): ").strip()

        # ------- TEAMS --------
        if opcion == "1":
            log.info("Cargando teams...")
            # comprueba si  la fuente ( understat, whoscored,etc) es fuente para la competicion
            # y obtiene la ruta en caso afirmativo 
            us_path = paths["us"] if _has_source(competition_name, "understat") else None
            ws_path = paths["ws"] if _has_source(competition_name, "whoscored") else None

            try:
                with engine.begin() as conn:
                    load_teams(
                        conn,
                        ss_path=paths["ss"],
                        tm_path=paths["tm"],
                        ws_path=ws_path,
                        us_path=us_path,
                    )
                log.info("Teams completado.")
            except Exception as e: 
                 log.error("Error cargando teams: %s", e)
            log.info("-" * 50)

        # ------- PLAYERS --------
        elif opcion == "2":
            log.info("Cargando players...")
            us_path = paths["us"] if _has_source(competition_name, "understat") else None
            ws_path = paths["ws"] if _has_source(competition_name, "whoscored") else None
            try: 
                with engine.begin() as conn:
                    load_players(
                        conn,
                        tm_path=paths["tm"],
                        ss_path=paths["ss"],
                        ws_path=ws_path,
                        us_path=us_path,
                    )
                log.info("Players completado.")
            except Exception as e: 
                log.error("Error cargango players: %s",e)
            log.info("-" * 50)
 
        # ------- MATCHES --------
        elif opcion == "3":
            log.info("Cargando matches...")
            us_path = paths["us"] if _has_source(competition_name, "understat") else None
            ws_path = paths["ws"] if _has_source(competition_name, "whoscored") else None
            try: 

                with engine.begin() as conn:
                    load_matches(
                        conn,
                        ss_path=paths["ss"],
                        competition_id=competition_id,
                        ws_path=ws_path,
                        us_path=us_path,
                    )
                log.info("Matches completado.")
            except Exception as e: 
                log.error("Error cargando matches: %s",e)
            log.info("-" * 50)

        # ------- CONTINUAR A HECHOS  --------
        elif opcion == "4":
            resultado = _menu_facts(competition_name, competition_id, paths)
            if resultado == "principal":
                return "principal"
            # si resultado == "dimensiones" vuelve al bucle de dimensiones

        # ------- VOLVER A MENÚ PRINCIPAL  --------
        elif opcion == "5":
            return "principal"
 
 
# ── Menú principal ────────────────────────────────────────────────────────────
 
def _menu_principal() -> None:
    """
    Muestra el menú principal con todas las competiciones disponibles
    y gestiona la navegación.

    Cuando se seleccciona una competicion llama a _menu_dimensions

    """
    # Competiciones que se consideran "soportadas" por el wizard. Filtramos
    # ademas las que tienen al menos una fuente configurada en `sources`.
    competition_names = [
        name for name in WORKING_COMPETITION_NAMES
        if name in COMPETITIONS and _competition_sources(name)
    ]
    competition_names.sort()
 
    while True:
        print("\n" + "=" * 50)
        print("  LOADER GENÉRICO — SELECCIONA COMPETICIÓN")
        print("=" * 50)
        # itera sobre la lista y añade un contador
        for i, name in enumerate(competition_names, 1):
            # formatea el número ocupando 2 caracteres de ancho, para que el menú quede alineado:
            print(f"{i:2}. {name}")
        print(" 0. Salir")

        # utiliza la longitud de la lista para mostrar el número de opciones disponibles 
        opcion = input("\nSelecciona (0-{}): ".format(len(competition_names))).strip()
 
        if opcion == "0":
            print("Saliendo...")
            break
 
        try:
            # Convierte la opción introducida por el usuario a un índice de la lista.
            index = int(opcion) - 1
            if index < 0 or index >= len(competition_names):
                print("Opción no válida.")
                continue

        except ValueError:
            print("Opción no válida.")
            continue
        
        competition_name = competition_names[index]

        # una vez se ha elegido el nombre de la competicion,   se crea el log para la competcion, si no existe  
        _setup_logging(competition_name)
        # obtiene el diccioanrio de fuentes  y rutas para una competicion 
        paths = _get_paths(competition_name)

        # Obtiene la pk de la competición en dim_competition 
        try:
            with engine.begin() as conn:
                competition_id = _get_competition_id(conn, competition_name) 
        except Exception as e:
            log.error("Error obteniendo competition_id para '%s': %s", competition_name, e)
            continue
 
        if not competition_id:
            log.error(
                "No se encontró competition_id para '%s'. "
                "¿Está cargada en dim_competition?",
                competition_name,
            )
            continue
 
        log.info("Competición seleccionada: %s (id=%d)", competition_name, competition_id)
        _menu_dimensions(competition_name, competition_id, paths)
 
 
# ── Punto de entrada ──────────────────────────────────────────────────────────
 
def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] %(levelname)s - %(message)s",
    )
    _menu_principal()
 
 
if __name__ == "__main__":
    main()
 