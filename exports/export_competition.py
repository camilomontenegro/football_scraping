"""
exports/export_competition.py
==============================
Exporta datos de la base de datos a CSV para una competición concreta.
 
Estructura de carpetas de salida:
    exports/
    ├── la_liga/
    │   ├── competition.csv
    │   ├── teams.csv
    │   ├── matches.csv
    │   ├── players.csv
    │   ├── shots.csv
    │   ├── events.csv
    │   └── injuries.csv
    ├── bundesliga/
    │   └── ...
    ├── player_review/
    │   └── player_review.csv
    └── logs/
        ├── la_liga.log
        ├── bundesliga.log
        └── player_review.log
 
Navegación:
    MENÚ PRINCIPAL → elegir competición
    MENÚ EXPORTACIÓN → elegir qué tablas exportar
    MENÚ PLAYER REVIEW → elegir cuántos registros exportar
 
Uso:
    python -m exports.export_competition
"""
 
import logging
from pathlib import Path
 
import pandas as pd
from sqlalchemy import text
 
from loaders.common import engine
from wizard.competitions import COMPETITIONS, get_competition, WORKING_COMPETITION_NAMES

# instancia de la clase Logger 
log = logging.getLogger(__name__)
 
# Raíz del proyecto — sube dos niveles desde exports/export_competition.py
PROJECT_ROOT = Path(__file__).resolve().parent.parent
 
 
# ── Logging ──────────────────────────────────────────────────────────────────
 
def _setup_logging(competition_name: str) -> None:
    """
    Configura el logging para escribir en consola y en archivo.
    Crea una carpeta 'logs' dentro de exports y un archivo por competición.
 
    Ejemplo:
        _setup_logging("La Liga")       → exports/logs/la_liga.log
        _setup_logging("Bundesliga")    → exports/logs/bundesliga.log
        _setup_logging("player_review") → exports/logs/player_review.log
 
    Parámetros:
        competition_name (str): nombre de la competición o 'player_review'
    """
    log_filename = competition_name.lower().replace(" ", "_") + ".log"
    log_path = PROJECT_ROOT / "exports" / "logs" / log_filename
    log_path.parent.mkdir(parents=True, exist_ok=True)
 
    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(logging.Formatter("[%(asctime)s] %(levelname)s - %(message)s"))
    logging.getLogger().addHandler(file_handler)
 
 
# ── Helpers ───────────────────────────────────────────────────────────────────
 
def _get_export_dir(competition_name: str) -> Path:
    """
    Construye y crea la carpeta de exportación para una competición.
    Usa la el nombre de la competición pasado como argumento al elegir una opción en el menú principal
 
    Ejemplo:
        _get_export_dir("La Liga")          → exports/la_liga/
        _get_export_dir("Bundesliga")       → exports/bundesliga/
        _get_export_dir("Champions League") → exports/champions_league/
 
    Parámetros:
        competition_name (str): nombre de la competición
 
    Devuelve:
        Path apuntando a la carpeta de exportación (ya creada)
    """
    folder = competition_name.lower().replace(" ", "_")
    export_dir = PROJECT_ROOT / "exports" / folder
    export_dir.mkdir(parents=True, exist_ok=True)
    return export_dir
 
 
def _get_competition_id(competition_name: str) -> int | None:
    """
    Obtiene el canonical_id de la competición en dim_competition
    usando el league_code de Transfermarkt definido en competitions.py.
 
    Ejemplo:
        _get_competition_id("La Liga")    → 1
        _get_competition_id("Bundesliga") → 3
 
    Parámetros:
        competition_name (str): nombre de la competición
 
    Devuelve:
        int con el canonical_id, o None si no se encuentra
    """
    comp = get_competition(competition_name)
    league_code = comp["sources"]["transfermarkt"]["league_code"]
    with engine.connect() as conn:
        result = conn.execute(
            text("SELECT canonical_id FROM dim_competition WHERE id_transfermarkt = :code"),
            {"code": league_code},
        ).scalar()
    return result
 
 
def _query_df(conn, sql: str, params: dict = None) -> pd.DataFrame:
    """
    Ejecuta una consulta SQL y devuelve el resultado como DataFrame.
    Alternativa a pd.read_sql que evita warnings de compatibilidad con SQLAlchemy 2.x.
 
    Ejemplo:
        df = _query_df(conn, "SELECT * FROM dim_team WHERE canonical_id = :cid", {"cid": 1})
 
    Parámetros:
        conn   (Connection): conexión SQLAlchemy activa
        sql    (str):        consulta SQL
        params (dict):       parámetros de la consulta
 
    Devuelve:
        pd.DataFrame con los resultados
    """
    result = conn.execute(text(sql), params or {})
    return pd.DataFrame(result.fetchall(), columns=list(result.keys()))
 
 
def _save_csv(df: pd.DataFrame, path: Path, table_name: str) -> None:
    """
    Guarda un DataFrame en CSV e imprime el número de filas exportadas.
 
    Ejemplo:
        _save_csv(df_teams, Path("exports/la_liga/teams.csv"), "Teams")
        → Teams: 20 filas exportadas → exports/la_liga/teams.csv
 
    Parámetros:
        df         (DataFrame): datos a exportar
        path       (Path):      ruta del archivo CSV destino
        table_name (str):       nombre de la tabla para el log
    """
    df.to_csv(path, index=False)
    log.info("%s: %d filas exportadas → %s", table_name, len(df), path)
    print(f"  {table_name}: {len(df)} filas → {path.name}")
 
 
# ── Exportaciones por tabla ───────────────────────────────────────────────────
 
def _export_competition(competition_id: int, export_dir: Path) -> None:
    """
    Exporta la fila de dim_competition correspondiente a la competición.
 
    Ejemplo de salida (competition.csv):
        canonical_id, canonical_name, id_sofascore, ...
        1, LaLiga, 8, ...
 
    Parámetros:
        competition_id (int):  canonical_id de la competición en dim_competition
        export_dir     (Path): carpeta de exportación
    """
    with engine.connect() as conn:
        df = _query_df(conn,
            "SELECT * FROM dim_competition WHERE canonical_id = :cid",
            {"cid": competition_id},
        )
    _save_csv(df, export_dir / "competition.csv", "Competition")
 
 
def _export_teams(competition_id: int, export_dir: Path) -> None:
    """
    Exporta los equipos que han jugado en la competición.
    Se obtienen a partir de los partidos de esa competición (home_team_id y away_team_id).
 
    Ejemplo de salida (teams.csv):
        canonical_id, canonical_name, country, id_sofascore, ...
        1, Real Madrid, España, 2829, ...
 
    Parámetros:
        competition_id (int):  canonical_id de la competición
        export_dir     (Path): carpeta de exportación
    """
    with engine.connect() as conn:
        df = _query_df(conn, """
            SELECT DISTINCT dt.*
            FROM dim_team dt
            WHERE dt.canonical_id IN (
                SELECT home_team_id FROM dim_match WHERE competition_id = :cid
                UNION
                SELECT away_team_id FROM dim_match WHERE competition_id = :cid
            )
            ORDER BY dt.canonical_name
        """, {"cid": competition_id})
    _save_csv(df, export_dir / "teams.csv", "Teams")
 
 
def _export_matches(competition_id: int, export_dir: Path) -> None:
    """
    Exporta todos los partidos de la competición.
 
    Ejemplo de salida (matches.csv):
        match_id, match_date, season, home_team_id, away_team_id, home_score, away_score, ...
        1, 2020-09-12, 2020/2021, 1, 2, 2, 0, ...
 
    Parámetros:
        competition_id (int):  canonical_id de la competición
        export_dir     (Path): carpeta de exportación
    """
    with engine.connect() as conn:
        df = _query_df(conn,
            "SELECT * FROM dim_match WHERE competition_id = :cid ORDER BY match_date",
            {"cid": competition_id},
        )
    # Proteccion  para supuestos en los que el tipo de dato es entero 
    # Si hay algún valor null en la columna, Pandas va a convertir el tipo de dato de toda la columna a float64 para poder representar null como NaN
    # Cuando se guarda en csv el valor entero queda como deciaml ( 2.0 en lugar de 2)
    # Excel y PowerBi pueden  malinterpretar  y  representar los valores erroneamente el csv
    # Para evitar todo esto, se convierte la columna a Int64. 
    for col in ["home_score", "away_score"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    _save_csv(df, export_dir / "matches.csv", "Matches")
 
 
def _export_players(competition_id: int, export_dir: Path) -> None:
    """
    Exporta los jugadores que tienen eventos o tiros en la competición.
    No hay relación directa entre jugadores y competiciones en el schema,
    por lo que se obtienen a través de fact_events y fact_shots.
 
    Ejemplo de salida (players.csv):
        canonical_id, canonical_name, nationality, birth_date, position, ...
        583, Lionel Messi, Argentina, 1987-06-24, Delantero, ...
 
    Parámetros:
        competition_id (int):  canonical_id de la competición
        export_dir     (Path): carpeta de exportación
    """
    with engine.connect() as conn:
        df = _query_df(conn, """
            SELECT DISTINCT dp.*
            FROM dim_player dp
            WHERE dp.canonical_id IN (
                SELECT DISTINCT fe.player_id
                FROM fact_events fe
                JOIN dim_match dm ON fe.match_id = dm.match_id
                WHERE dm.competition_id = :cid
                UNION
                SELECT DISTINCT fs.player_id
                FROM fact_shots fs
                JOIN dim_match dm ON fs.match_id = dm.match_id
                WHERE dm.competition_id = :cid
            )
            ORDER BY dp.canonical_name
        """, {"cid": competition_id})
    _save_csv(df, export_dir / "players.csv", "Players")
 
 
def _export_shots(competition_id: int, export_dir: Path) -> None:
    """
    Exporta todos los tiros de la competición.
 
    Ejemplo de salida (shots.csv):
        shot_id, match_id, player_id, team_id, minute, x, y, xg, result, ...
        1, 100, 583, 1, 45, 0.85, 0.35, 0.23, goal, ...
 
    Parámetros:
        competition_id (int):  canonical_id de la competición
        export_dir     (Path): carpeta de exportación
    """
    with engine.connect() as conn:
        df = _query_df(conn, """
            SELECT fs.*
            FROM fact_shots fs
            JOIN dim_match dm ON fs.match_id = dm.match_id
            WHERE dm.competition_id = :cid
        """, {"cid": competition_id})
    _save_csv(df, export_dir / "shots.csv", "Shots")
 
 
def _export_events(competition_id: int, export_dir: Path) -> None:
    """
    Exporta todos los eventos de la competición.
    Puede ser una tabla muy grande (millones de filas para algunas competiciones).
 
    Ejemplo de salida (events.csv):
        event_id, match_id, player_id, team_id, event_type, minute, ...
        1, 100, 583, 1, Pass, 12, ...
 
    Parámetros:
        competition_id (int):  canonical_id de la competición
        export_dir     (Path): carpeta de exportación
    """
    print("  [!] Events puede ser una tabla muy grande. Exportando...")
    with engine.connect() as conn:
        df = _query_df(conn, """
            SELECT fe.*
            FROM fact_events fe
            JOIN dim_match dm ON fe.match_id = dm.match_id
            WHERE dm.competition_id = :cid
        """, {"cid": competition_id})
    _save_csv(df, export_dir / "events.csv", "Events")
 
 
def _export_injuries(competition_id: int, export_dir: Path) -> None:
    """
    Exporta las lesiones de los jugadores que participaron en la competición.
    Se obtienen a través de los jugadores que tienen eventos o tiros en la competición.
 
    Ejemplo de salida (injuries.csv):
        injury_id, player_id, season, injury_type, date_from, date_until, days_absent, ...
        1, 583, 2020/2021, Rotura muscular, 2020-10-01, 2020-11-15, 45, ...
 
    Parámetros:
        competition_id (int):  canonical_id de la competición
        export_dir     (Path): carpeta de exportación
    """
    with engine.connect() as conn:
        df = _query_df(conn, """
            SELECT fi.*
            FROM fact_injuries fi
            WHERE fi.player_id IN (
                SELECT DISTINCT fe.player_id
                FROM fact_events fe
                JOIN dim_match dm ON fe.match_id = dm.match_id
                WHERE dm.competition_id = :cid
                UNION
                SELECT DISTINCT fs.player_id
                FROM fact_shots fs
                JOIN dim_match dm ON fs.match_id = dm.match_id
                WHERE dm.competition_id = :cid
            )
        """, {"cid": competition_id})
    # Proteccion  para supuestos en los que el tipo de dato es entero 
    # Si hay algún valor null en la columna, Pandas va a convertir el tipo de dato de toda la columna a float64 para poder representar null como NaN
    # Cuando se guarda en csv el valor entero queda como deciaml ( 2.0 en lugar de 2)
    # Excel y PowerBi pueden  malinterpretar  y  representar los valores erroneamente el csv
    # Para evitar todo esto, se convierte la columna a Int64. 

    for col in ["days_absent", "matches_missed"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    _save_csv(df, export_dir / "injuries.csv", "Injuries")
 
 
def _export_player_review(limit: int | None) -> None:
    """
    Exporta registros de player_review a una carpeta separada.
 
    Ejemplo de salida (exports/player_review/player_review.csv):
        id, source_name, source_system, source_id, suggested_canonical_id, similarity_score, resolved, ...
        1, Lionel Messi, sofascore, 12345, 583, 95, True, ...
 
    Parámetros:
        limit (int | None): número máximo de registros a exportar.
                            None exporta todos los registros.
    Ejemplo:
        _export_player_review(100)  → exporta los primeros 100 registros
        _export_player_review(None) → exporta todos los registros
    """
    player_review_dir = PROJECT_ROOT / "exports" / "player_review"
    player_review_dir.mkdir(parents=True, exist_ok=True)
 
    limit_clause = f"LIMIT {limit}" if limit else ""
    with engine.connect() as conn:
        df = _query_df(conn, f"SELECT * FROM player_review ORDER BY id {limit_clause}")
    _save_csv(df, player_review_dir / "player_review.csv", "Player Review")
 
 
# ── Menús ─────────────────────────────────────────────────────────────────────
 
def _menu_player_review() -> None:
    """
    Menú para exportar player_review.
    Pregunta cuántos registros exportar — todos o un número concreto.
    """
    _setup_logging("player_review")
    print("\n=== Exportar Player Review ===")
    print("1. Exportar todos los registros")
    print("2. Exportar N registros")
    print("3. Volver")
 
    opcion = input("Selecciona (1-3): ").strip()
 
    if opcion == "1":
        _export_player_review(limit=None)
 
    elif opcion == "2":
        while True:
            try:
                n = int(input("¿Cuántos registros? ").strip())
                if n > 0:
                    break
                print("Introduce un número mayor que 0.")
            except ValueError:
                print("Introduce un número válido.")
        _export_player_review(limit=n)
 
    elif opcion == "3":
        return
 
 
def _menu_export(competition_name: str, competition_id: int, export_dir: Path) -> str:
    """
    Menú de exportación para una competición concreta.
    Permite exportar tablas individuales o todas a la vez.
 
    Devuelve:
        'principal' → volver al menú principal
    """
    while True:
        print(f"\n=== {competition_name} — Exportar ===")
        print(f"  Carpeta: {export_dir}")
        print("1. Competition")
        print("2. Teams")
        print("3. Matches")
        print("4. Players")
        print("5. Shots")
        print("6. Events")
        print("7. Injuries")
        print("8. Exportar TODO. ")
        print("9. Volver al menú principal")
 
        opcion = input("Selecciona (1-9): ").strip()
 
        # ------- COMPETITION --------
        if opcion == "1":
            try:
                _export_competition(competition_id, export_dir)
            except Exception as e:
                log.error("Error exportando competition: %s", e)
 
        # ------- TEAMS --------
        elif opcion == "2":
            try:
                _export_teams(competition_id, export_dir)
            except Exception as e:
                log.error("Error exportando teams: %s", e)
 
        # ------- MATCHES --------
        elif opcion == "3":
            try:
                _export_matches(competition_id, export_dir)
            except Exception as e:
                log.error("Error exportando matches: %s", e)
 
        # ------- PLAYERS --------
        elif opcion == "4":
            try:
                _export_players(competition_id, export_dir)
            except Exception as e:
                log.error("Error exportando players: %s", e)
 
        # ------- SHOTS --------
        elif opcion == "5":
            try:
                _export_shots(competition_id, export_dir)
            except Exception as e:
                log.error("Error exportando shots: %s", e)
 
        # ------- EVENTS --------
        elif opcion == "6":
            try:
                _export_events(competition_id, export_dir)
            except Exception as e:
                log.error("Error exportando events: %s", e)
 
        # ------- INJURIES --------
        elif opcion == "7":
            try:
                _export_injuries(competition_id, export_dir)
            except Exception as e:
                log.error("Error exportando injuries: %s", e)
 
        # ------- EXPORTAR TODO --------
        elif opcion == "8":
            print("\n  Exportando todas las tablas...")
            try:
                _export_competition(competition_id, export_dir)
                _export_teams(competition_id, export_dir)
                _export_matches(competition_id, export_dir)
                _export_players(competition_id, export_dir)
                _export_shots(competition_id, export_dir)
                _export_events(competition_id, export_dir)
                _export_injuries(competition_id, export_dir)
                print("  Exportación completada.")
            except Exception as e:
                log.error("Error en exportación completa: %s", e)
 
        # ------- VOLVER AL MENÚ PRINCIPAL --------
        elif opcion == "9":
            return "principal"
 
 
def _menu_principal() -> None:
    """
    Menú principal — permite elegir la competición a exportar
    o exportar player_review.
    """
    # List comprehension — obtiene solo competiciones con folder y data_sources definidos
    competition_names = [
        name for name, comp in COMPETITIONS.items()
            if comp.get("sources", {}).get("transfermarkt", {}).get("league_code")
            and name in WORKING_COMPETITION_NAMES
        ]
 
    while True:
        print("\n" + "=" * 50)
        print("  EXPORTADOR — SELECCIONA COMPETICIÓN")
        print("=" * 50)
        for i, name in enumerate(competition_names, 1):
            print(f"{i:2}. {name}")
        print(f"{len(competition_names) + 1:2}. Player Review")
        print(" 0. Salir")
 
        opcion = input("\nSelecciona (0-{}): ".format(len(competition_names) + 1)).strip()
 
        if opcion == "0":
            print("Saliendo...")
            break
 
        # ------- PLAYER REVIEW --------
        if opcion == str(len(competition_names) + 1):
            _menu_player_review()
            continue
 
        try:
            index = int(opcion) - 1
            if index < 0 or index >= len(competition_names):
                print("Opción no válida.")
                continue
        except ValueError:
            print("Opción no válida.")
            continue
 
        competition_name = competition_names[index]
 
        # Configura el log para esta competición → exports/logs/la_liga.log
        _setup_logging(competition_name)
 
        # Obtiene la pk de la competición en dim_competition
        try:
            competition_id = _get_competition_id(competition_name)
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
 
        # Construye la carpeta de exportación para la competición
        export_dir = _get_export_dir(competition_name)
        log.info("Exportando %s (id=%d) → %s", competition_name, competition_id, export_dir)
 
        _menu_export(competition_name, competition_id, export_dir)
 
 
# ── Punto de entrada ──────────────────────────────────────────────────────────
 
def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] %(levelname)s - %(message)s",
    )
    _menu_principal()
 
 
if __name__ == "__main__":
    main()
