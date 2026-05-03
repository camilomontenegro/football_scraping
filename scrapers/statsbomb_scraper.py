"""
scrapers/statsbomb_scraper.py
===============================
Scraper unificado de StatsBomb Open Data. Sigue el mismo patrÃ³n que understat_scraper.py:

    Estructura:
        1. CONSTANTS       - configuraciÃ³n del scraper
        2. HELPERS         - credenciales, df_to_records
        3. FETCH           - funciones puras que envuelven statsbombpy
        4. ORCHESTRATOR    - scrape_statsbomb() acumula todo
        5. TRANSFORM       - adapta campos al esquema de la DB
        6. DIM EXTRACTORS  - extract_teams(), extract_players()
        7. MAIN            - scrape -> transform -> guardar en disco
        8. __main__ guard

    Salida (data/raw/statsbomb/):
        competition_<id>/season_<id>/
            matches_clean.csv             <- dim_match (campos DB)
            teams.csv                     <- dim_team (campos DB)
            players.csv                   <- dim_player (campos DB)
            match_<id>/batch_id=<id>/
                events.json               <- eventos crudos
                lineups.json              <- alineaciones crudas
                events_clean.csv          <- fact_events (campos DB)

    StatsBomb Open Data es libre y no requiere API key.
    Los loaders/ son los Ãºnicos que escriben en la DB.
"""

from __future__ import annotations

import json
import logging
import sys
import time
from pathlib import Path
from typing import Optional

# Allow running directly as a script
sys.path.append(str(Path(__file__).resolve().parent.parent))

import pandas as pd
from statsbombpy import sb

log = logging.getLogger(__name__)

# â”€â”€ CONSTANTS â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

COMPETITION_ID = 11    # La Liga en StatsBomb
SEASON_IDS     = [90, 106, 113, 120, 127]    # 2020/21, 2021/22, 2022/23, 2023/24, 2024/25
SEASON_LABELS  = ["2020/21", "2021/22", "2022/23", "2023/24", "2024/25"]
DELAY_SEC      = 0.3   # pausa entre peticiones (Open Data sin rate limit estricto)
PROJECT_ROOT   = Path(__file__).resolve().parent.parent
OUTPUT_DIR     = PROJECT_ROOT / "data" / "raw" / "statsbomb"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# StatsBomb Open Data no requiere credenciales
_CREDS = {"user": "", "passwd": ""}


# â”€â”€ HELPERS â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def _df_to_records(df: pd.DataFrame) -> list[dict]:
    """Convierte un DataFrame en lista de dicts serializable."""
    if df is None or df.empty:
        return []
    return df.to_dict(orient="records")


def _save_json(data, path: Path) -> None:
    """Guarda JSON en disco de forma segura."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2, default=str)


# â”€â”€ FETCH FUNCTIONS â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def list_competitions() -> pd.DataFrame:
    """Devuelve DataFrame con todas las competiciones de StatsBomb Open Data."""
    try:
        return sb.competitions(creds=_CREDS)
    except Exception as exc:
        log.error("Error al obtener competiciones: %s", exc)
        return pd.DataFrame()


def list_matches(competition_id: int, season_id: int) -> pd.DataFrame:
    """Devuelve DataFrame con los partidos de una competiciÃ³n/temporada."""
    try:
        return sb.matches(
            competition_id=competition_id,
            season_id=season_id,
            creds=_CREDS,
        )
    except Exception as exc:
        log.error("Error al obtener partidos (comp=%d, season=%d): %s",
                  competition_id, season_id, exc)
        return pd.DataFrame()


def get_events(match_id: int) -> pd.DataFrame:
    """Devuelve DataFrame con todos los eventos de un partido."""
    try:
        return sb.events(match_id=match_id, creds=_CREDS)
    except Exception as exc:
        log.error("Error al obtener eventos (match=%d): %s", match_id, exc)
        return pd.DataFrame()


def get_lineups(match_id: int) -> dict:
    """Devuelve dict {team_name: [jugadores]} con los lineups de un partido."""
    try:
        return sb.lineups(match_id=match_id, creds=_CREDS)
    except Exception as exc:
        log.error("Error al obtener lineups (match=%d): %s", match_id, exc)
        return {}


# â”€â”€ ORCHESTRATOR â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def scrape_statsbomb(
    competition_id: int = COMPETITION_ID,
    season_id: int      = None,
    sleep_between: float = DELAY_SEC,
    from_date: Optional[str] = None,
) -> tuple[pd.DataFrame, list[dict], list[dict]]:
    """Orquestador principal: descarga partidos, eventos y lineups.

    Args:
        competition_id: ID de la competicion en StatsBomb (p.ej. 11 = La Liga)
        season_id:      ID de la temporada (p.ej. 90 = 2020/21). Si es None, usa la primera.
        sleep_between:  Pausa entre partidos en segundos
        from_date:      Fecha mínima para partidos (formato YYYY-MM-DD)

    Returns:
        (matches_df, all_events, all_lineups) where:
        - matches_df: DataFrame de partidos
        - all_events: lista de dicts con eventos (con _match_id y _competition anadidos)
        - all_lineups: lista de dicts {match_id, lineups}
    """
    if season_id is None:
        season_id = SEASON_IDS[0]
    
    # Parse from_date if provided
    from_date_obj = None
    if from_date:
        from datetime import datetime
        from_date_obj = datetime.strptime(from_date, "%Y-%m-%d").date()
        log.info("Filtrando partidos desde: %s", from_date_obj)
    
    from utils.batch import generate_batch_id
    batch_id = generate_batch_id()

    log.info("STATSBOMB EXTRACT START | comp=%d | season=%d | batch=%s",
             competition_id, season_id, batch_id)

    print("=" * 55)
    print(f"  StatsBomb scraper - comp={competition_id} season={season_id}")
    print("=" * 55)

    matches_df = list_matches(competition_id, season_id)
    if matches_df.empty:
        log.warning("Sin partidos para competition=%d season=%d", competition_id, season_id)
        return pd.DataFrame(), [], []

    # Filter by from_date if provided
    if from_date_obj and "match_date" in matches_df.columns:
        matches_df = matches_df[matches_df["match_date"] >= from_date_obj]
        log.info("Filtrados %d partidos desde %s", len(matches_df), from_date_obj)
    elif from_date_obj:
        log.warning("Columna 'match_date' no encontrada, sin filtrar por fecha")

    print(f"  [OK] {len(matches_df)} partidos encontrados")

    # Directorio base
    comp_dir = OUTPUT_DIR / f"competition_{competition_id}" / f"season_{season_id}"

    all_events:  list[dict] = []
    all_lineups: list[dict] = []

    for _, row in matches_df.iterrows():
        match_id   = int(row["match_id"])
        home_info  = row.get("home_team", {})
        away_info  = row.get("away_team", {})
        home_name  = home_info.get("home_team_name", str(home_info)) if isinstance(home_info, dict) else str(home_info)
        away_name  = away_info.get("away_team_name", str(away_info)) if isinstance(away_info, dict) else str(away_info)

        log.info("Procesando match %d: %s vs %s", match_id, home_name, away_name)
        print(f"  - match {match_id}: {home_name} vs {away_name}")

        match_dir = comp_dir / f"match_{match_id}" / f"batch_id={batch_id}"
        match_dir.mkdir(parents=True, exist_ok=True)

        try:
            # Eventos
            events_df      = get_events(match_id)
            events_records = _df_to_records(events_df)

            # AÃ±adir contexto para los transforms
            for ev in events_records:
                ev["_match_id_sb"]      = match_id
                ev["_competition_id_sb"]= competition_id
                ev["_season_id_sb"]     = season_id

            _save_json(events_records, match_dir / "events.json")
            all_events.extend(events_records)

            # Lineups
            lineups_raw = get_lineups(match_id)
            lineups_ser: dict = {}
            for team, data in lineups_raw.items():
                if isinstance(data, pd.DataFrame):
                    lineups_ser[team] = data.to_dict(orient="records")
                else:
                    lineups_ser[team] = data
            _save_json(lineups_ser, match_dir / "lineups.json")
            all_lineups.append({"match_id": match_id, "lineups": lineups_ser})

            log.info("  - %d eventos guardados", len(events_records))

        except Exception as exc:
            log.error("Error en match %d: %s", match_id, exc)

        time.sleep(sleep_between)

    log.info("STATSBOMB EXTRACT DONE | matches=%d | events=%d",
             len(matches_df), len(all_events))
    return matches_df, all_events, all_lineups


# â”€â”€ TRANSFORM â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def transform_matches(matches_df: pd.DataFrame) -> pd.DataFrame:
    """Adapta el DataFrame de partidos a las columnas de dim_match.

    Columnas generadas (alineadas con create_tables.sql dim_match):
        id_statsbomb, match_date, competition, season,
        home_team_id_sb, away_team_id_sb,
        home_team_name, away_team_name,
        home_score, away_score, data_source
    """
    if matches_df.empty:
        return pd.DataFrame()

    rows = []
    for _, row in matches_df.iterrows():
        home_info = row.get("home_team", {})
        away_info = row.get("away_team", {})

        home_id   = home_info.get("home_team_id")   if isinstance(home_info, dict) else None
        away_id   = away_info.get("away_team_id")   if isinstance(away_info, dict) else None
        home_name = home_info.get("home_team_name") if isinstance(home_info, dict) else str(home_info)
        away_name = away_info.get("away_team_name") if isinstance(away_info, dict) else str(away_info)

        comp_info   = row.get("competition", {})
        season_info = row.get("season", {})

        rows.append({
            "id_statsbomb":    str(row.get("match_id")),
            "match_date":      str(row.get("match_date", "")),
            "competition":     comp_info.get("competition_name") if isinstance(comp_info, dict) else str(comp_info),
            "season":          season_info.get("season_name")    if isinstance(season_info, dict) else str(season_info),
            "home_team_id_sb": home_id,
            "away_team_id_sb": away_id,
            "home_team_name":  home_name,
            "away_team_name":  away_name,
            "home_score":      row.get("home_score"),
            "away_score":      row.get("away_score"),
            "data_source":     "statsbomb",
        })

    return pd.DataFrame(rows)


def transform_events(events_raw: list[dict]) -> pd.DataFrame:
    """Adapta los eventos crudos a las columnas de fact_events.

    Columnas generadas (alineadas con create_tables.sql fact_events):
        match_id_sb, player_id_sb, player_name,
        team_id_sb, team_name, event_type,
        minute, second, x, y, end_x, end_y,
        outcome, data_source
    """
    rows = []
    for ev in events_raw:
        player_info = ev.get("player", {})
        team_info   = ev.get("team",   {})
        location    = ev.get("location") or []
        end_loc     = (ev.get("pass", {}) or {}).get("end_location") or []
        outcome_raw = ev.get("shot", {}) or ev.get("pass", {}) or {}
        outcome_obj = outcome_raw.get("outcome", {})

        rows.append({
            # FKs a resolver por el loader
            "match_id_sb":  ev.get("_match_id_sb"),
            "player_id_sb": player_info.get("id")   if isinstance(player_info, dict) else None,
            "player_name":  player_info.get("name") if isinstance(player_info, dict) else None,
            "team_id_sb":   team_info.get("id")     if isinstance(team_info,   dict) else None,
            "team_name":    team_info.get("name")   if isinstance(team_info,   dict) else None,
            # Campos de fact_events
            "event_type":   ev.get("type", {}).get("name") if isinstance(ev.get("type"), dict) else ev.get("type"),
            "minute":       ev.get("minute"),
            "second":       ev.get("second"),
            "x":            location[0] if isinstance(location, list) and len(location) > 0 else None,
            "y":            location[1] if isinstance(location, list) and len(location) > 1 else None,
            "end_x":        end_loc[0]  if isinstance(end_loc, list) and len(end_loc)  > 0 else None,
            "end_y":        end_loc[1]  if isinstance(end_loc, list) and len(end_loc)  > 1 else None,
            "outcome":      outcome_obj.get("name") if isinstance(outcome_obj, dict) else None,
            "data_source":  "statsbomb",
        })

    df = pd.DataFrame(rows)
    if not df.empty:
        df["minute"] = pd.to_numeric(df["minute"], errors="coerce").astype("Int16")
        df["second"] = pd.to_numeric(df["second"], errors="coerce").astype("Int16")
        for col in ("x", "y", "end_x", "end_y"):
            df[col] = pd.to_numeric(df[col], errors="coerce").round(4)
    return df


# â”€â”€ DIM EXTRACTORS â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def extract_teams(matches_df: pd.DataFrame) -> pd.DataFrame:
    """Extrae equipos unicos de los partidos -> columnas de dim_team.

    Columnas: id_statsbomb, canonical_name
    """
    if matches_df.empty:
        return pd.DataFrame(columns=["id_statsbomb", "canonical_name"])

    teams = {}
    for _, row in matches_df.iterrows():
        for side_key, id_key, name_key in [
            ("home_team", "home_team_id", "home_team_name"),
            ("away_team", "away_team_id", "away_team_name"),
        ]:
            info = row.get(side_key, {})
            if isinstance(info, dict):
                tid  = info.get(id_key)
                name = info.get(name_key)
                if tid and tid not in teams:
                    teams[str(tid)] = name

    if not teams:
        return pd.DataFrame(columns=["id_statsbomb", "canonical_name"])

    return (
        pd.DataFrame([{"id_statsbomb": k, "canonical_name": v} for k, v in teams.items()])
        .sort_values("id_statsbomb")
        .reset_index(drop=True)
    )


def extract_players(events_df: pd.DataFrame) -> pd.DataFrame:
    """Extrae jugadores unicos de los eventos -> columnas de dim_player.

    Columnas: id_statsbomb, canonical_name
    """
    if events_df.empty or "player_id_sb" not in events_df.columns:
        return pd.DataFrame(columns=["id_statsbomb", "canonical_name"])

    df = (
        events_df[["player_id_sb", "player_name"]]
        .rename(columns={"player_id_sb": "id_statsbomb", "player_name": "canonical_name"})
        .drop_duplicates(subset=["id_statsbomb"])
        .dropna(subset=["id_statsbomb"])
    )

    if df.empty:
        return pd.DataFrame(columns=["id_statsbomb", "canonical_name"])

    return (
        df
        .sort_values("id_statsbomb")
        .reset_index(drop=True)
    )


# â”€â”€ MAIN â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def main():
    logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s - %(message)s")

    print("=" * 55)
    print(f"  StatsBomb scraper - competition={COMPETITION_ID} seasons 2020/21 a 2024/25")
    print("=" * 55)

    # Ver competiciones disponibles si se necesita
    # comps = list_competitions()
    # print(comps[["competition_id","competition_name","season_id","season_name"]])

    for season_id, season_label in zip(SEASON_IDS, SEASON_LABELS):
        print(f"\n[SEASON] Descargando temporada {season_label}...")
        
        matches_df, all_events, _ = scrape_statsbomb(COMPETITION_ID, season_id)

        if matches_df.empty:
            print(f"  [!] No se obtuvieron partidos para {season_label}")
            continue

        print(f"  [SEASON] Temporada {season_label}:")
        print(f"    Partidos: {len(matches_df)}")
        print(f"    Eventos:  {len(all_events)}")

        # Transformar
        df_matches = transform_matches(matches_df)
        df_events  = transform_events(all_events)
        df_teams   = extract_teams(matches_df)
        df_players = extract_players(df_events)

        # Guardar CSVs
        season_dir = OUTPUT_DIR / f"competition_{COMPETITION_ID}" / f"season_{season_id}"
        season_dir.mkdir(parents=True, exist_ok=True)

        paths = {
            "matches": season_dir / "matches_clean.csv",
            "events":  season_dir / "events_clean.csv",
            "teams":   season_dir / "teams.csv",
            "players": season_dir / "players.csv",
        }

        df_matches.to_csv(paths["matches"], index=False, encoding="utf-8-sig")
        df_events.to_csv( paths["events"],  index=False, encoding="utf-8-sig")
        df_teams.to_csv(  paths["teams"],   index=False, encoding="utf-8-sig")
        df_players.to_csv(paths["players"], index=False, encoding="utf-8-sig")

        print(f"  [OK] Archivos guardados en {season_dir}")

    print(f"\n[DONE] Descarga de StatsBomb completada")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Scraper de StatsBomb")
    parser.add_argument("--competition-id", "-c", type=int, default=None,
                        help="ID de la competición en StatsBomb (ej: 11 para La Liga)")
    parser.add_argument("--season-id", "-s", type=int, default=None,
                        help="ID de la temporada en StatsBomb (ej: 90 para 2020/21)")
    
    args = parser.parse_args()
    
    # Usar valores por defecto si no se especifican
    competition_id = args.competition_id if args.competition_id else COMPETITION_ID
    season_id = args.season_id if args.season_id else SEASON_IDS[0]
    
    scrape_statsbomb(competition_id=competition_id, season_id=season_id)
