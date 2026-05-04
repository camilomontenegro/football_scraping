"""
understat_generico.py
======================
Descarga partidos y tiros de Understat para cualquier competición
definida en competitions.py y los guarda como CSV.
 
Lógica según competitions.py → sources.understat.has_league_endpoint:
    True  → endpoint JSON  /getLeagueData/<league>/<season>   (ligas domésticas)
    False → scraping HTML  /league/<league>/<season>          (Champions, Europa, Conference)
    None  → competición sin datos en Understat, se aborta
 
Si Understat cambia algo, solo hay que tocar competitions.py.
"""
import re 
import pandas as pd
import os 
import json 
import sys
import argparse
import asyncio
import aiohttp
from pathlib import Path
from typing import Optional, List, Tuple
from urllib.parse import quote

# Importar configuración de competiciones
sys.path.append(str(Path(__file__).resolve().parent.parent))
from scripts.competitions import get_competition

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
OUTPUT_DIR = PROJECT_ROOT / "data" / "raw" / "understat"
os.makedirs(OUTPUT_DIR, exist_ok=True)


# --- Helpers de Red ---

async def fetch(
        session: aiohttp.ClientSession,
        url: str,
        referer: str = None,
        html_mode: bool = False,
 ) -> Optional[str]:
    """Petición GET genérica. Usa headers JSON o HTML según html_mode."""
    headers = (HEADERS_HTML if html_mode else HEADERS_JSON).copy()
    if referer:
        headers["Referer"] = referer
    try:
        async with session.get(url, headers=headers) as resp:
            resp.raise_for_status()
            return await resp.text()
    except Exception as e:
        print(f"  [!] Error en {url}: {e}")
        return None

# EXTRACCIÓN DE PARTIDOS

def _parse_matches(raw_matches: list, season: int) -> List[dict]:
    """
    Convierte la lista cruda de partidos de Understat al formato
    normalizado. Válido tanto para el endpoint JSON como para el HTML.
    """
    result = []
    for m in raw_matches:
        # Understat a veces devuelve partidos sin resultado (futuros)
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
    """
    Ligas domésticas: endpoint JSON /getLeagueData/<league>/<season>.
 
    La respuesta tiene la clave 'datesData' (lista de partidos).
    Se acepta también 'dates' como fallback por si Understat cambia el nombre.
    """
    url     = f"{BASE_URL}/getLeagueData/{quote(league)}/{season}"
    referer = f"{BASE_URL}/league/{league}/{season}"
    raw = await fetch(session, url, referer=referer, html_mode=False)
    if not raw:
        return []
    try:
        data = json.loads(raw)
        # 'datesData' es la clave real; 'dates' como fallback defensivo
        matches_raw = data.get("datesData") or data.get("dates") or []
        if not matches_raw:
            print(f"  [!] Respuesta vacía o clave desconocida. Claves recibidas: {list(data.keys())}")
        return _parse_matches(matches_raw, season)
    except Exception as e:
        print(f"  [!] Error parseando endpoint JSON para {league}/{season}: {e}")
        return []
 
 
async def get_matches_via_html(
    session: aiohttp.ClientSession,
    league: str,
    season: int,
) -> List[dict]:
    """
    Competiciones europeas: scraping HTML de /league/<league>/<season>.
 
    Understat embebe los datos directamente en el HTML como:
        var datesData = JSON.parse('<json_escapado>');
    Se extrae con regex y se desescapa el JSON.
    """
    url = f"{BASE_URL}/league/{league}/{season}"
    raw = await fetch(session, url, referer=f"{BASE_URL}/", html_mode=True)
    if not raw:
        return []
    try:
        # Patrón: var datesData = JSON.parse('<datos_escapados>');
        pattern = r"var\s+datesData\s*=\s*JSON\.parse\('(.+?)'\)"
        match = re.search(pattern, raw, re.DOTALL)
        if not match:
            print(f"  [!] No se encontró 'datesData' en el HTML de {league}/{season}.")
            print(f"      Variables JS disponibles: {re.findall(r'var\\s+(\\w+)\\s*=\\s*JSON\\.parse', raw)}")
            return []
 
        # Understat escapa las comillas simples como \'
        json_str    = match.group(1).replace("\\'", "'")
        matches_raw = json.loads(json_str)
        return _parse_matches(matches_raw, season)
    except Exception as e:
        print(f"  [!] Error parseando HTML para {league}/{season}: {e}")
        return []
 
 
# ═══════════════════════════════════════════════════════════════════════
# EXTRACCIÓN DE TIROS
# ═══════════════════════════════════════════════════════════════════════
 
async def get_match_shots(
    session: aiohttp.ClientSession,
    match_id: str,
) -> List[dict]:
    """
    Tiros de un partido específico vía /getMatchData/<match_id>.
    Igual para ligas domésticas y europeas — el endpoint es universal.
    """
    url     = f"{BASE_URL}/getMatchData/{match_id}"
    referer = f"{BASE_URL}/match/{match_id}"
    raw = await fetch(session, url, referer=referer, html_mode=False)
    if not raw:
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
                    "player_name":          shot.get("player"),
                    "minute":               shot.get("minute"),
                    "x":                    shot.get("X"),
                    "y":                    shot.get("Y"),
                    "xg":                   shot.get("xG"),
                    "result":               shot.get("result"),
                    "shot_type":            shot.get("shotType"),
                    "situation":            shot.get("situation"),
                    "side":                 side,
                    "season":               shot.get("season"),
                })
        return shots
    except Exception as e:
        print(f"  [!] Error parseando tiros del partido {match_id}: {e}")
        return []
 
 
# ═══════════════════════════════════════════════════════════════════════
# ORQUESTADOR
# ═══════════════════════════════════════════════════════════════════════
 
async def scrape_understat(league_key: str, seasons: List[int], delay: float):
    comp_config = get_competition(league_key)
    if not comp_config:
        print(f"[!] Competición '{league_key}' no encontrada en competitions.py.")
        return
 
    understat_cfg = comp_config["sources"]["understat"]
    league        = understat_cfg.get("league")
    has_endpoint  = understat_cfg.get("has_league_endpoint", True)  # True por defecto para ligas domésticas
 
    # None → competición sin datos en Understat
    if league is None or has_endpoint is None:
        print(f"[!] '{league_key}' no tiene datos en Understat. Abortando.")
        return
 
    # Elegir función de extracción de partidos según el flag del competitions.py
    if has_endpoint:
        print(f"[INFO] '{league_key}' usa endpoint JSON (/getLeagueData/)")
        get_matches = get_matches_via_endpoint
    else:
        print(f"[INFO] '{league_key}' usa scraping HTML (/league/)")
        get_matches = get_matches_via_html
 
    all_matches: List[dict] = []
    all_shots:   List[dict] = []
 
    connector = aiohttp.TCPConnector(limit=3)
    async with aiohttp.ClientSession(connector=connector) as session:
        for season in seasons:
            print(f"\n[SEASON] {league_key} — {season}/{season + 1}")
            matches = await get_matches(session, league, season)
 
            if not matches:
                print(f"  [!] Sin partidos para {league_key} {season}. Saltando.")
                continue
 
            all_matches.extend(matches)
            total = len(matches)
 
            for i, match in enumerate(matches, 1):
                mid   = match["understat_match_id"]
                shots = await get_match_shots(session, mid)
                all_shots.extend(shots)
 
                if i % 20 == 0:
                    print(f"  -> {i}/{total} partidos | Tiros acumulados: {len(all_shots)}")
 
                await asyncio.sleep(delay)
 
    # --- Guardar CSVs ---
    if not all_matches:
        print(f"\n[!] No se obtuvieron datos para '{league_key}'. No se guardan ficheros.")
        return
 
    safe_name = league_key.lower().replace(" ", "_")
    df_matches = pd.DataFrame(all_matches)
    df_shots   = pd.DataFrame(all_shots)
 
    path_matches = OUTPUT_DIR / f"understat_matches_{safe_name}.csv"
    path_shots   = OUTPUT_DIR / f"understat_shots_{safe_name}.csv"
 
    df_matches.to_csv(path_matches, index=False)
    df_shots.to_csv(path_shots,   index=False)
 
    print(f"\n[OK] '{league_key}' guardado:")
    print(f"     Partidos : {len(df_matches):>5}  →  {path_matches}")
    print(f"     Tiros    : {len(df_shots):>5}  →  {path_shots}")
 
 
# ═══════════════════════════════════════════════════════════════════════
# ENTRADA DE SCRIPT
# ═══════════════════════════════════════════════════════════════════════
 
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Understat Generic Scraper")
    parser.add_argument(
        "--competition",
        type=str,
        default="La Liga",
        help="Nombre exacto de la competición según competitions.py (ej: 'Champions League')",
    )
    parser.add_argument(
        "--seasons",
        nargs="+",
        type=int,
        default=[2020, 2021, 2022, 2023, 2024],
        help="Lista de temporadas (año de inicio). Ej: 2022 2023 2024",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=1.5,
        help="Segundos de espera entre partidos para no saturar Understat",
    )
 
    args = parser.parse_args()
    asyncio.run(scrape_understat(args.competition, args.seasons, args.delay))
