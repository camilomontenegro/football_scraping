"""
scrapers/understat_scraper.py
==============================
Descarga partidos y tiros de Understat para cualquier competición
definida en competitions.py y los guarda como CSV.
 
Soporta tanto ligas domésticas (API JSON) como europeas (scraping HTML).
Soporta scraping incremental a través de 'from_date'.
"""
import re 
import pandas as pd
import os 
import json 
import sys
import argparse
import asyncio
import aiohttp
from datetime import datetime, date
from pathlib import Path
from typing import Optional, List, Tuple
from urllib.parse import quote

# Importar configuración de competiciones
sys.path.append(str(Path(__file__).resolve().parent.parent))
from wizard.competitions import get_competition

# --- Configuración Base ---
BASE_URL = "https://understat.com"

HEADERS_JSON = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
    "X-Requested-With": "XMLHttpRequest",
    "Accept": "application/json, text/javascript, */*; q=0.01",
}
 
HEADERS_HTML = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}

PROJECT_ROOT = Path(__file__).resolve().parent.parent
# OUTPUT_DIR legacy. Las rutas reales se construyen via utils.data_paths.
OUTPUT_DIR = PROJECT_ROOT / "data" / "raw" / "understat"

from utils.data_paths import save_clean_csv, raw_dir  # noqa: E402


def _save_json(data, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2, default=str)


def _parse_understat_date(date_str: str) -> "date | None":
    """Parsea fecha de Understat (formato: '2025-05-25 00:00:00')."""
    if not date_str:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(date_str[:19], fmt).date()
        except ValueError:
            continue
    return None

# --- Helpers de Red ---

async def fetch(
        session: aiohttp.ClientSession,
        url: str,
        referer: str = None,
        html_mode: bool = False,
        quiet_statuses: tuple[int, ...] = (),
 ) -> Optional[str]:
    """Petición GET genérica. Usa headers JSON o HTML según html_mode.

    `quiet_statuses` permite tratar estados esperables, como 404 en
    `/getMatchData/<id>` de partidos aún sin shotmap, como ausencia de datos
    en lugar de error crítico del scraper.
    """
    headers = (HEADERS_HTML if html_mode else HEADERS_JSON).copy()
    if referer:
        headers["Referer"] = referer
    try:
        async with session.get(url, headers=headers) as resp:
            try:
                resp.raise_for_status()
            except aiohttp.ClientResponseError as e:
                if e.status in quiet_statuses:
                    return None
                raise
            # Understat puede devolver JSON como text/javascript
            if not html_mode and "json" not in resp.headers.get("Content-Type", ""):
                # Still return text to parse with json.loads
                return await resp.text()
            return await resp.text()
    except aiohttp.ClientResponseError as e:
        print(f"  [!] Error HTTP {e.status} en {url}: {e.message}")
        return None
    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        print(f"  [!] Error de red en {url}: {e}")
        return None
    except Exception as e:
        print(f"  [!] Error en {url}: {e}")
        return None

# EXTRACCIÓN DE PARTIDOS

def _parse_matches(raw_matches: list, season: int) -> List[dict]:
    """
    Convierte la lista cruda de partidos de Understat al formato
    normalizado.
    """
    result = []
    for m in raw_matches:
        goals = m.get("goals") or {}
        xg    = m.get("xG")    or {}
        result.append({
            "understat_match_id": m.get("id"),
            "season":             season,
            "datetime":           m.get("datetime"),
            "home_team":          m.get("h", {}).get("title"),
            "away_team":          m.get("a", {}).get("title"),
            "home_team_id":       m.get("h", {}).get("id"),
            "away_team_id":       m.get("a", {}).get("id"),
            "home_goals":         goals.get("h"),
            "away_goals":         goals.get("a"),
            "home_xg":            xg.get("h") if isinstance(xg, dict) else None,
            "away_xg":            xg.get("a") if isinstance(xg, dict) else None,
        })
    return result

async def get_matches_via_endpoint(
    session: aiohttp.ClientSession,
    league: str,
    season: int,
) -> List[dict]:
    """Ligas domésticas: endpoint JSON."""
    url     = f"{BASE_URL}/getLeagueData/{quote(league)}/{season}"
    referer = f"{BASE_URL}/league/{league}/{season}"
    raw = await fetch(session, url, referer=referer, html_mode=False)
    if not raw:
        return []
    try:
        data = json.loads(raw)
        matches_raw = data.get("datesData") or data.get("dates") or []
        return _parse_matches(matches_raw, season)
    except Exception as e:
        print(f"  [!] Error parseando JSON para {league}/{season}: {e}")
        return []
 
async def get_matches_via_html(
    session: aiohttp.ClientSession,
    league: str,
    season: int,
) -> List[dict]:
    """Competiciones europeas: scraping HTML."""
    url = f"{BASE_URL}/league/{league}/{season}"
    raw = await fetch(session, url, referer=f"{BASE_URL}/", html_mode=True)
    if not raw:
        return []
    try:
        pattern = r"var\s+datesData\s*=\s*JSON\.parse\('(.+?)'\)"
        match = re.search(pattern, raw, re.DOTALL)
        if not match:
            print(f"  [!] No se encontró 'datesData' en HTML.")
            return []
        
        # Primero quitamos los quotes escapados
        json_str = match.group(1).encode("utf-8").decode("unicode_escape")
        # Por si aún quedan \', aunque unicode_escape debería manejar mucho
        json_str = json_str.replace("\\'", "'")
        matches_raw = json.loads(json_str)
        return _parse_matches(matches_raw, season)
    except Exception as e:
        print(f"  [!] Error parseando HTML para {league}/{season}: {e}")
        return []
 
# EXTRACCIÓN DE TIROS
 
async def get_match_shots(
    session: aiohttp.ClientSession,
    match_id: str,
) -> List[dict]:
    """Tiros de un partido específico."""
    url     = f"{BASE_URL}/getMatchData/{match_id}"
    referer = f"{BASE_URL}/match/{match_id}"
    raw = await fetch(session, url, referer=referer, html_mode=False, quiet_statuses=(404,))
    if not raw:
        print(f"  [i] Understat no tiene shotmap publicado para el partido {match_id}. Se omite.")
        return []
    try:
        data       = json.loads(raw)
        shots_data = data.get("shots", {})
        shots      = []
        for side in ("h", "a"):
            for shot in shots_data.get(side, []):
                shots.append({
                    "understat_shot_id":    shot.get("id"),
                    "understat_match_id":   match_id,
                    "understat_player_id":  shot.get("player_id"),
                    "understat_team":       shot.get("h_team") if side == "h" else shot.get("a_team"),
                    "side":                 side,
                    "player_name":          shot.get("player"),
                    "minute":               shot.get("minute"),
                    "x":                    shot.get("X"),
                    "y":                    shot.get("Y"),
                    "xg":                   shot.get("xG"),
                    "result":               shot.get("result"),
                    "shot_type":            shot.get("shotType"),
                    "situation":            shot.get("situation"),
                    "last_action":          shot.get("lastAction"),
                    "player_assisted":      shot.get("player_assisted"),
                    "season":               shot.get("season"),
                    "source":               "understat",
                })
        return shots
    except Exception as e:
        print(f"  [!] Error parseando tiros del partido {match_id}: {e}")
        return []

# TRANSFORMACIÓN A FORMATO DB

def transform_shots(df_shots: pd.DataFrame, df_matches: pd.DataFrame) -> pd.DataFrame:
    """Prepara el DataFrame de tiros para carga en fact_shots."""
    if df_shots.empty:
        return df_shots

    df = df_shots.copy()
    df["minute"]   = pd.to_numeric(df["minute"],  errors="coerce").astype("Int16")
    df["x"]        = (pd.to_numeric(df["x"], errors="coerce") * 105).round(2)
    df["y"]        = (pd.to_numeric(df["y"], errors="coerce") * 68).round(2)
    df["xg"]       = pd.to_numeric(df["xg"],       errors="coerce").round(4)

    result_map = {
        "Goal":            "Goal",
        "SavedShot":       "Saved",
        "MissedShots":     "Off T",
        "BlockedShot":     "Blocked",
        "ShotOnPost":      "Post",
        "OwnGoal":         "OwnGoal",
    }
    df["result"] = df["result"].map(result_map).fillna(df["result"])

    shottype_map = {
        "RightFoot": "Right Foot",
        "LeftFoot":  "Left Foot",
        "Head":      "Head",
    }
    df["shot_type"] = df["shot_type"].map(shottype_map).fillna(df["shot_type"])

    situation_map = {
        "OpenPlay":        "Open Play",
        "SetPiece":        "Set Piece",
        "FromCorner":      "From Corner",
        "DirectFreekick":  "Direct Freekick",
        "Penalty":         "Penalty",
    }
    df["situation"] = df["situation"].map(situation_map).fillna(df["situation"])

    cols = [
        "understat_match_id", "understat_player_id", "understat_team",
        "player_name", "minute", "x", "y", "xg",
        "result", "shot_type", "situation",
        "side", "last_action", "player_assisted",
        "season", "source"
    ]
    return df[[c for c in cols if c in df.columns]]

def extract_players(
    df_shots: pd.DataFrame,
    *,
    competition: str | None = None,
    season: str | None = None,
) -> pd.DataFrame:
    if df_shots.empty:
        return pd.DataFrame()
    cols = ["understat_player_id", "player_name"]
    if "understat_team" in df_shots.columns:
        cols.append("understat_team")
    df = (
        df_shots[cols]
        .drop_duplicates(subset=["understat_player_id"])
        .dropna(subset=["understat_player_id"])
        .sort_values("understat_player_id")
        .reset_index(drop=True)
    )
    if "understat_team" in df.columns:
        df = df.rename(columns={"understat_team": "team_name"})
    if competition:
        df["competition"] = competition
    if season:
        df["season"] = season
    df["source"] = "understat"
    return df

def extract_teams(df_matches: pd.DataFrame) -> pd.DataFrame:
    if df_matches.empty:
        return pd.DataFrame()
    home = df_matches[["home_team_id", "home_team"]].rename(
        columns={"home_team_id": "understat_team_id", "home_team": "team_name"})
    away = df_matches[["away_team_id", "away_team"]].rename(
        columns={"away_team_id": "understat_team_id", "away_team": "team_name"})
    return (
        pd.concat([home, away])
        .drop_duplicates()
        .sort_values("understat_team_id")
        .reset_index(drop=True)
    )

# ORQUESTADOR

async def scrape_understat(
    competition_name: str, 
    seasons: List[int], 
    update: bool = False, 
    from_date: str = None,
    delay: float = 1.5,
):
    comp_config = get_competition(competition_name)
    if not comp_config:
        print(f"[!] Competición '{competition_name}' no encontrada en competitions.py.")
        return

    understat_cfg = comp_config["sources"].get("understat")
    if not understat_cfg or not understat_cfg.get("league"):
        print(f"[!] '{competition_name}' no tiene configuración o datos en Understat.")
        return

    league = understat_cfg["league"]
    has_endpoint = understat_cfg.get("has_league_endpoint", True)

    if has_endpoint:
        print(f"[INFO] '{competition_name}' usa endpoint JSON (/getLeagueData/)")
        get_matches = get_matches_via_endpoint
    else:
        print(f"[INFO] '{competition_name}' usa scraping HTML (/league/)")
        get_matches = get_matches_via_html

    from_date_obj = None
    if from_date:
        from_date_obj = datetime.strptime(from_date, "%Y-%m-%d").date()
        print(f"\n[FILTER] Descargando solo partidos desde: {from_date}")

    all_matches: List[dict] = []
    all_shots:   List[dict] = []

    connector = aiohttp.TCPConnector(limit=3)
    timeout   = aiohttp.ClientTimeout(total=30)
    
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        for season in seasons:
            print(f"\n[SEASON] {competition_name} — {season}/{season + 1}")
            matches = await get_matches(session, league, season)

            if not matches:
                print(f"  [!] Sin partidos para {competition_name} {season}. Saltando.")
                continue

            # ── Guardar raw JSON de partidos ──
            _folder = f"{season}_{season + 1}"
            _season_raw = raw_dir(competition_name, _folder, "understat")
            _season_raw.mkdir(parents=True, exist_ok=True)
            _save_json(matches, _season_raw / "matches.json")

            # Filtrar por fecha
            if from_date_obj:
                original_count = len(matches)
                matches = [m for m in matches if m.get("datetime") and _parse_understat_date(m["datetime"]) >= from_date_obj]
                print(f"  [+] {len(matches)} partidos después de {from_date} (filtrados {original_count - len(matches)})")

            all_matches.extend(matches)
            
            processed_count = 0
            for i, match in enumerate(matches, 1):
                mid = match["understat_match_id"]
                match_date = _parse_understat_date(match.get("datetime"))
                if match_date and match_date > date.today():
                    continue

                try:
                    shots = await get_match_shots(session, mid)
                    if shots:
                        _match_raw = _season_raw / "matches" / str(mid)
                        _match_raw.mkdir(parents=True, exist_ok=True)
                        _save_json(shots, _match_raw / "shots.json")
                    for s in shots:
                        s["season"] = season
                    all_shots.extend(shots)
                    processed_count += 1

                    if processed_count % 20 == 0:
                        print(f"  -> {processed_count}/{len(matches)} partidos | Tiros acumulados: {len(all_shots)}")

                    await asyncio.sleep(delay)
                except Exception as e:
                    print(f"  [!] Error inesperado en partido {mid}: {e}")
                    await asyncio.sleep(5)

    # --- Guardar CSVs ---
    if not all_matches:
        print(f"\n[!] No se obtuvieron datos para '{competition_name}'. No se guardan ficheros.")
        return

    df_matches = pd.DataFrame(all_matches)
    df_shots   = pd.DataFrame(all_shots)

    season_year   = seasons[0]
    folder_season = f"{season_year}_{season_year + 1}"

    # Aplicar transformaciones
    df_shots_clean = transform_shots(df_shots, df_matches)
    df_players     = extract_players(
        df_shots,
        competition=competition_name,
        season=folder_season,
    )
    df_teams       = extract_teams(df_matches)

    # Layout canónico: data/clean/<comp>/<season>/understat/<table>.csv

    matches_path = save_clean_csv(competition_name, folder_season, "understat",
                                  "matches", df_matches)
    shots_path = None
    if not df_shots_clean.empty:
        shots_path = save_clean_csv(competition_name, folder_season, "understat",
                                    "shots", df_shots_clean)
    if not df_players.empty:
        save_clean_csv(competition_name, folder_season, "understat",
                       "players", df_players)
    if not df_teams.empty:
        save_clean_csv(competition_name, folder_season, "understat",
                       "teams", df_teams)

    print(f"\n[OK] '{competition_name}' — temporada {folder_season} guardada:")
    print(f"     Partidos : {len(df_matches):>5}  →  {matches_path}")
    if shots_path:
        print(f"     Tiros    : {len(df_shots_clean):>5}  →  {shots_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Understat Unified Scraper")
    parser.add_argument("--competition", type=str, default="La Liga",
                        help="Nombre exacto de la competición (ej: 'Bundesliga')")
    parser.add_argument("--seasons", nargs="+", type=int, default=[2024],
                        help="Lista de temporadas (año de inicio). Ej: 2024")
    parser.add_argument("--from-date", type=str, default=None,
                        help="Fecha desde (YYYY-MM-DD) para update incremental")
    parser.add_argument("--delay", type=float, default=1.5,
                        help="Segundos de espera entre peticiones")
    args = parser.parse_args()
    
    asyncio.run(scrape_understat(
        competition_name=args.competition, 
        seasons=args.seasons,
        update=bool(args.from_date),
        from_date=args.from_date,
        delay=args.delay
    ))
