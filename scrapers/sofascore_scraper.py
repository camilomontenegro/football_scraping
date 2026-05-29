"""
scrapers/sofascore_scraper.py
==============================
Scraper unificado de SofaScore. Sigue el mismo patron que understat_scraper.py:

    Estructura:
        1. CONSTANTS       - configuracion del scraper
        2. HELPERS         - driver Selenium y peticion JSON
        3. FETCH           - funciones puras de obtencion de datos
        4. ORCHESTRATOR    - scrape_sofascore() acumula todo
        5. TRANSFORM       - adapta campos al esquema de la DB
        6. DIM EXTRACTORS  - extract_teams(), extract_players()
        7. MAIN            - scrape -> transform -> guardar en disco
        8. __main__ guard

    Salida (data/raw/sofascore/):
        season=<label>/
            matches_batch_<id>.json          <- lista cruda de partidos
            matches_clean.csv                <- dim_match (campos DB)
            teams.csv                        <- dim_team  (campos DB)
            players.csv                      <- dim_player (campos DB)
            match_<id>/batch_id=<id>/
                shots.json                   <- tiros crudos
                events.json                  <- incidentes crudos
                lineups.json                 <- alineaciones crudas
                shots_clean.csv              <- fact_shots (campos DB)
                events_clean.csv             <- fact_events (campos DB)

    Los loaders/ son los unicos que escriben en la DB.
"""

from __future__ import annotations

import json
import logging
import os
import random
import re
import csv
import sys
import time
import requests
try:
    from curl_cffi import requests as tls_requests
except ImportError:  # dependencia opcional; queda documentada en requirements.txt
    tls_requests = None
from datetime import datetime, date, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Optional, Dict

# Allow running directly as a script
sys.path.append(str(Path(__file__).resolve().parent.parent))

import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

log = logging.getLogger(__name__)


class SofaScoreBlockedError(RuntimeError):
    """Raised when SofaScore returns anti-bot challenge/forbidden JSON."""

#CONSTANTS
TOURNAMENT_ID = 8                          # La Liga en SofaScore (default CLI)


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        return max(0.0, float(raw))
    except ValueError:
        log.warning("Valor inválido en %s=%r; uso por defecto %.1f", name, raw, default)
        return default


# Pausas anti-bloqueo (env: SOFASCORE_DELAY_SEC, SOFASCORE_MATCH_DELAY_SEC, …)
REQUEST_DELAY_SEC = _env_float("SOFASCORE_DELAY_SEC", 2.5)           # entre peticiones HTTP
MATCH_DELAY_SEC = _env_float("SOFASCORE_MATCH_DELAY_SEC", 4.0)       # tras cada partido (3 endpoints)
JITTER_SEC = _env_float("SOFASCORE_JITTER_SEC", 0.8)                 # aleatorio extra por petición
BLOCK_COOLDOWN_SEC = _env_float("SOFASCORE_BLOCK_COOLDOWN_SEC", 45.0)  # tras 403/429
DELAY_SEC = REQUEST_DELAY_SEC  # alias legacy (reintentos internos)
MAX_HTTP_RETRIES = int(_env_float("SOFASCORE_MAX_RETRIES", 3))

# Perfiles curl_cffi (rotación ante 403 — patrón tunjayoff/sofascore_scraper)
CURL_IMPERSONATE_PROFILES = [
    "chrome136",
    "chrome124",
    "chrome120",
    "chrome110",
    "edge101",
    "safari17_0",
]

_last_request_at: float = 0.0


def _throttle(min_sec: float | None = None) -> None:
    """Espera entre peticiones para no disparar rate-limit / bloqueo de IP."""
    global _last_request_at
    target = (min_sec if min_sec is not None else REQUEST_DELAY_SEC) + (
        random.uniform(0.0, JITTER_SEC) if JITTER_SEC > 0 else 0.0
    )
    elapsed = time.monotonic() - _last_request_at
    if elapsed < target:
        time.sleep(target - elapsed)
    _last_request_at = time.monotonic()


def _throttle_between_matches() -> None:
    """Pausa extra al terminar shots+events+lineups de un partido."""
    if MATCH_DELAY_SEC <= 0:
        return
    extra = MATCH_DELAY_SEC + (random.uniform(0.0, JITTER_SEC) if JITTER_SEC > 0 else 0.0)
    log.debug("Pausa entre partidos: %.1fs", extra)
    time.sleep(extra)
    global _last_request_at
    _last_request_at = time.monotonic()


def _throttle_after_block() -> None:
    """Enfriamiento tras 403/429 antes de seguir con el siguiente partido."""
    if BLOCK_COOLDOWN_SEC <= 0:
        return
    wait = BLOCK_COOLDOWN_SEC + random.uniform(0.0, JITTER_SEC * 2 if JITTER_SEC > 0 else 0.0)
    log.warning("Pausa anti-bloqueo %.0fs antes de continuar…", wait)
    time.sleep(wait)
    global _last_request_at
    _last_request_at = time.monotonic()


def _print_throttle_config() -> None:
    print(
        f"  [INFO] Pausas SofaScore: {REQUEST_DELAY_SEC:.1f}s/petición, "
        f"{MATCH_DELAY_SEC:.1f}s/partido, jitter ±{JITTER_SEC:.1f}s, "
        f"cooldown 403: {BLOCK_COOLDOWN_SEC:.0f}s, reintentos: {MAX_HTTP_RETRIES} "
        f"(env: SOFASCORE_DELAY_SEC, SOFASCORE_MATCH_DELAY_SEC, …)"
    )


def _parse_retry_after_seconds(retry_after: str | None, default_wait: float) -> float:
    """Interpreta cabecera Retry-After (segundos o fecha HTTP)."""
    if not retry_after:
        return default_wait
    try:
        return max(1.0, float(retry_after))
    except ValueError:
        try:
            dt = parsedate_to_datetime(retry_after)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return max(1.0, min(120.0, (dt - datetime.now(timezone.utc)).total_seconds()))
        except (TypeError, ValueError):
            return default_wait


def _backoff_seconds(status_code: int, attempt: int, retry_after: str | None = None) -> float:
    """Espera exponencial en 403/429/503 (inspirado en tunjayoff/sofascore_scraper)."""
    if status_code == 403:
        base = min(120.0, 10.0 * (2 ** attempt))
    elif status_code in {429, 503}:
        base = min(60.0, 5.0 * (2 ** attempt))
    else:
        base = DELAY_SEC * (attempt + 2)
    return _parse_retry_after_seconds(retry_after, base)


PROJECT_ROOT  = Path(__file__).resolve().parent.parent
from scrapers.sofascore_seasons import (
    SOFASCORE_SEASON_IDS,
    TOURNAMENT_ID_BY_COMPETITION,
    default_seasons_for_competition,
    get_fallback_season_id,
    season_lookup_keys as _season_lookup_keys,
    sofascore_season_available,
)

# Compat legacy: temporadas por defecto de La Liga si no se pasa --competition/--seasons
SEASON_NAMES = default_seasons_for_competition("La Liga")
SOFASCORE_API = "https://api.sofascore.com/api/v1"
SOFASCORE_MIRROR_API = "https://api.var11.com/api/v1"
SOFASCORE_WEB = "https://www.sofascore.com/es-la"
SOFASCORE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9,es;q=0.8",
    "Origin": "https://www.sofascore.com",
    "Referer": "https://www.sofascore.com/",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-site",
}
OUTPUT_DIR    = PROJECT_ROOT / "data" / "raw" / "sofascore"  # legacy; ya no se usa para escribir
# Note: mkdir() is called inside scrape_sofascore() to avoid side-effects on import

# Helpers centralizados de rutas
from utils.data_paths import (
    raw_dir,
    save_clean_csv,
)


# HELPERS 

def create_driver(headless: bool = True) -> webdriver.Chrome:
    """Crea un Chrome controlado por Selenium.

    Se usa como fallback cuando el cliente HTTP recibe `challenge`. En entornos
    locales suele funcionar mejor iniciar sesión/cookies desde la web pública
    antes de abrir endpoints de API directamente.
    """
    options = Options()
    if headless:
        options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-extensions")
    options.add_argument("--no-sandbox")
    options.add_argument("--window-size=1365,900")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    options.add_argument(f"--user-agent={SOFASCORE_HEADERS['User-Agent']}")

    profile = os.getenv("SOFASCORE_CHROME_PROFILE")
    if profile:
        options.add_argument(f"--user-data-dir={profile}")

    options.page_load_strategy = "eager"

    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=options,
    )
    try:
        driver.execute_cdp_cmd(
            "Page.addScriptToEvaluateOnNewDocument",
            {"source": "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"},
        )
    except Exception:
        pass
    return driver


_PREFERRED_API_BASE: str | None = None


def _remember_working_base(full_url: str) -> None:
    global _PREFERRED_API_BASE
    for base in (SOFASCORE_API, SOFASCORE_MIRROR_API):
        if full_url.startswith(f"{base}/"):
            _PREFERRED_API_BASE = base
            if base != SOFASCORE_API:
                log.info("SofaScore: usando base alternativa %s", base)
            return


def _api_bases() -> list[str]:
    """API bases to try, in order. Mirror is used automatically after 403/challenge."""
    primary = os.getenv("SOFASCORE_API", SOFASCORE_API).rstrip("/")
    mirror = os.getenv("SOFASCORE_MIRROR_API", SOFASCORE_MIRROR_API).rstrip("/")
    disable_mirror = os.getenv("SOFASCORE_DISABLE_MIRROR", "").lower() in ("1", "true", "yes")
    mirror_first = os.getenv("SOFASCORE_MIRROR_FIRST", "").lower() in ("1", "true", "yes")
    try_official = os.getenv("SOFASCORE_TRY_OFFICIAL", "").lower() in ("1", "true", "yes")

    # Si el probe ya marcó el mirror como única base viable, no quemar 3×403 en oficial.
    if (
        _PREFERRED_API_BASE
        and mirror
        and _PREFERRED_API_BASE.rstrip("/") == mirror
        and not try_official
        and not mirror_first
    ):
        return [mirror]

    bases: list[str] = []
    if _PREFERRED_API_BASE:
        bases.append(_PREFERRED_API_BASE.rstrip("/"))
    if mirror_first and mirror and mirror not in bases:
        bases.append(mirror)
    if primary and primary not in bases:
        bases.append(primary)
    if not disable_mirror and mirror and mirror not in bases:
        bases.append(mirror)
    return bases


def _relative_api_path(url: str) -> str:
    for base in _api_bases():
        prefix = f"{base}/"
        if url.startswith(prefix):
            return url[len(prefix):]
    marker = "/api/v1/"
    idx = url.find(marker)
    if idx >= 0:
        return url[idx + len(marker):]
    return url.lstrip("/")


def _alternate_api_urls(url: str) -> list[str]:
    rel = _relative_api_path(url)
    return [f"{base}/{rel}" for base in _api_bases()]


def _sync_driver_cookies_to_session(driver: webdriver.Chrome, session) -> None:
    """Copia cookies del navegador a la sesión HTTP tras superar el challenge."""
    for cookie in driver.get_cookies():
        name = cookie.get("name")
        value = cookie.get("value")
        if not name or value is None:
            continue
        domain = cookie.get("domain") or ".sofascore.com"
        try:
            session.cookies.set(name, value, domain=domain)
        except Exception:
            try:
                session.cookies.set(name, value)
            except Exception:
                pass


def _browser_fetch_json(driver: webdriver.Chrome, url: str, timeout_ms: int = 30000) -> dict:
    """Ejecuta fetch() dentro del navegador (mismas cookies que la web)."""
    script = """
        const url = arguments[0];
        const timeoutMs = arguments[1];
        const done = arguments[arguments.length - 1];
        const timer = setTimeout(() => done(JSON.stringify({
            error: {reason: "browser fetch timeout"}
        })), timeoutMs);
        fetch(url, {
            credentials: "include",
            headers: {Accept: "application/json, text/plain, */*"},
        })
        .then(async (resp) => {
            const text = await resp.text();
            if (!resp.ok) {
                clearTimeout(timer);
                done(JSON.stringify({error: {reason: `HTTP ${resp.status}`, body: text.slice(0, 200)}}));
                return;
            }
            clearTimeout(timer);
            done(text);
        })
        .catch((err) => {
            clearTimeout(timer);
            done(JSON.stringify({error: {reason: String(err)}}));
        });
    """
    body = driver.execute_async_script(script, url, timeout_ms)
    if not body:
        raise RuntimeError(f"Browser fetch vacío para {url}")
    data = json.loads(body)
    return _validate_sofascore_payload(data, url)


def create_http_session(impersonate: str | None = None):
    """Crea una sesión HTTP con cabeceras de navegador para SofaScore.

    Si está instalado `curl_cffi`, se usa con impersonación de Chrome porque
    SofaScore puede bloquear clientes por fingerprint TLS aunque los headers
    sean correctos. Si no está disponible, se usa `requests` estándar.
    """
    if tls_requests is not None:
        profile = impersonate or os.getenv("SOFASCORE_IMPERSONATE") or random.choice(
            CURL_IMPERSONATE_PROFILES
        )
        session = tls_requests.Session(impersonate=profile)
        log.debug("curl_cffi impersonate=%s", profile)
    else:
        session = requests.Session()
    session.headers.update(SOFASCORE_HEADERS)
    proxy = os.getenv("SOFASCORE_PROXY") or os.getenv("HTTPS_PROXY") or os.getenv("HTTP_PROXY")
    if proxy:
        try:
            session.proxies.update({"http": proxy, "https": proxy})
        except AttributeError:
            session.proxies = {"http": proxy, "https": proxy}
    # Calienta cookies de la web pública. Si falla, no aborta: la API puede
    # seguir respondiendo sin cookies en algunos entornos.
    try:
        session.get(SOFASCORE_WEB, timeout=12)
    except Exception as e:
        log.debug("No se pudieron precargar cookies de SofaScore: %s", e)
    return session


def _is_http_session(client) -> bool:
    if isinstance(client, requests.Session):
        return True
    if tls_requests is not None and isinstance(client, tls_requests.Session):
        return True
    return False


def _validate_sofascore_payload(data: dict, url: str) -> dict:
    """Valida errores de API y normaliza el mensaje de bloqueo."""
    error = data.get("error") if isinstance(data, dict) else None
    if error:
        reason = error.get("reason") or error.get("code") or "unknown"
        raise SofaScoreBlockedError(f"SofaScore bloquea {url}: {reason}")
    return data


def _get_json_http(session: requests.Session, url: str) -> dict:
    """Obtiene JSON vía HTTP probando bases alternativas (oficial → mirror)."""
    timeout = int(_env_float("SOFASCORE_REQUEST_TIMEOUT", 25))
    fast_fail = os.getenv("SOFASCORE_FAST_FAILOVER", "1").lower() in ("1", "true", "yes")
    official_base = SOFASCORE_API.rstrip("/")
    attempt_urls = _alternate_api_urls(url)
    last_exc: Exception | None = None

    for base_idx, attempt_url in enumerate(attempt_urls):
        is_official = attempt_url.startswith(official_base)
        has_mirror_left = base_idx < len(attempt_urls) - 1

        for attempt in range(MAX_HTTP_RETRIES):
            try:
                resp = session.get(attempt_url, timeout=timeout)

                if resp.status_code in {403, 401, 429, 503}:
                    wait = _backoff_seconds(
                        resp.status_code, attempt, resp.headers.get("Retry-After")
                    )
                    if is_official and has_mirror_left and fast_fail:
                        log.debug(
                            "HTTP %s en API oficial; failover al mirror sin más reintentos",
                            resp.status_code,
                        )
                        last_exc = SofaScoreBlockedError(
                            f"SofaScore bloquea {attempt_url}: HTTP {resp.status_code}"
                        )
                        break
                    log.warning(
                        "HTTP %s en %s (intento %d/%d); espera %.0fs",
                        resp.status_code,
                        attempt_url,
                        attempt + 1,
                        MAX_HTTP_RETRIES,
                        wait,
                    )
                    time.sleep(wait)
                    if attempt < MAX_HTTP_RETRIES - 1:
                        continue
                    last_exc = SofaScoreBlockedError(
                        f"SofaScore bloquea {attempt_url}: HTTP {resp.status_code}"
                    )
                    break

                if resp.status_code == 404:
                    log.warning("Recurso no encontrado en %s (404)", attempt_url)
                    last_exc = RuntimeError(f"No encontrado: {attempt_url}")
                    break

                if resp.status_code >= 500:
                    if attempt < MAX_HTTP_RETRIES - 1:
                        time.sleep(_backoff_seconds(resp.status_code, attempt))
                        continue
                    last_exc = RuntimeError(
                        f"Error servidor {resp.status_code} en {attempt_url}"
                    )
                    break

                resp.raise_for_status()
                data = resp.json()
                _remember_working_base(attempt_url)
                return _validate_sofascore_payload(data, attempt_url)

            except SofaScoreBlockedError as e:
                last_exc = e
                break
            except (Exception, ValueError) as e:
                last_exc = e
                if attempt < MAX_HTTP_RETRIES - 1:
                    time.sleep(DELAY_SEC * (attempt + 1))
                    continue
                break

        if base_idx < len(attempt_urls) - 1:
            log.debug("Probando siguiente base API tras fallo en %s", attempt_url)
            continue

    if isinstance(last_exc, SofaScoreBlockedError):
        raise last_exc
    raise RuntimeError(f"No se pudo leer JSON de SofaScore en {url}: {last_exc}")


def _get_json_selenium(driver: webdriver.Chrome, url: str, timeout: float = 8) -> dict:
    """Obtiene JSON con Selenium: fetch en contexto web y fallback a navegar la URL."""
    last_exc: Exception | None = None
    for attempt_url in _alternate_api_urls(url):
        try:
            return _browser_fetch_json(driver, attempt_url, timeout_ms=int(timeout * 1000))
        except Exception as e:
            last_exc = e
            log.debug("Browser fetch falló en %s: %s", attempt_url, e)

    driver.get(_alternate_api_urls(url)[0])
    try:
        WebDriverWait(driver, timeout).until(
            lambda d: len(d.find_element("tag name", "body").text.strip()) > 0
        )
    except Exception:
        pass
    time.sleep(DELAY_SEC)
    body = driver.find_element("tag name", "body").text.strip()
    try:
        return _validate_sofascore_payload(json.loads(body), url)
    except json.JSONDecodeError as e:
        raise RuntimeError(f"SofaScore no devolvió JSON válido en {url}: {body[:200]}") from e


def get_json(client, url: str) -> dict:
    """Devuelve JSON desde SofaScore usando requests o Selenium."""
    _throttle()
    if _is_http_session(client):
        return _get_json_http(client, url)
    return _get_json_selenium(client, url)


# â”€â”€ FETCH FUNCTIONS â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def _api_url(*parts: str) -> str:
    """Construye URL canónica (base primaria) para un path relativo de la API."""
    rel = "/".join(str(p).strip("/") for p in parts if p is not None)
    return f"{_api_bases()[0]}/{rel}"


def get_season_id(client, tournament_id: int, season_name: str) -> tuple[Optional[int], Optional[str]]:
    """Devuelve (season_id, season_label) para un nombre de temporada dado.

    Consulta el endpoint de temporadas del torneo y busca la que
    contenga season_name en su nombre.
    """
    fallback_id, fallback_label = get_fallback_season_id(tournament_id, season_name)
    if fallback_id:
        return fallback_id, fallback_label

    data = get_json(
        client,
        _api_url("unique-tournament", tournament_id, "seasons"),
    )
    possible_names = _season_lookup_keys(season_name)

    for s in data.get("seasons", []):
        season_label = str(s.get("name", ""))
        season_year = str(s.get("year", ""))
        haystack = f"{season_label} {season_year}".lower()
        if any(name in haystack for name in possible_names):
            return s["id"], season_label
    return None, None


def get_reference_season_id(
    competition_name: str,
    tournament_id: int,
    season_name: str,
) -> tuple[Optional[int], Optional[str]]:
    """Resolve SofaScore season_id from the local master reference table."""
    ref_path = PROJECT_ROOT / "data" / "reference" / "source_reference_ids.csv"
    if ref_path.exists():
        candidates = _season_lookup_keys(season_name)
        with ref_path.open("r", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                row_season = row.get("season", "")
                if (
                    row.get("source") == "sofascore"
                    and row.get("competition") == competition_name
                    and str(row.get("competition_id")) == str(tournament_id)
                    and row.get("season_id")
                    and (_season_lookup_keys(row_season) & candidates)
                ):
                    return int(row["season_id"]), row_season or season_name
    return get_fallback_season_id(tournament_id, season_name)


def get_matches(client, tournament_id: int, season_id: int) -> list[dict]:
    """Devuelve todos los partidos de una temporada paginando el endpoint.

    El endpoint devuelve hasta ~20 partidos por pÃ¡gina.
    Navega hacia atrÃ¡s hasta agotar las pÃ¡ginas.
    """
    events = []
    page   = 0
    while True:
        url = _api_url(
            "unique-tournament", tournament_id,
            "season", season_id, "events", "last", page,
        )
        data = get_json(client, url)
        batch = data.get("events", [])
        if not batch:
            break
        events.extend(batch)
        if not data.get("hasNextPage"):
            break
        page += 1
    return events


def _get_match_date(match: dict) -> "date | None":
    """Extrae la fecha de un partido de SofaScore.

    SofaScore guarda la fecha como:
      - `startTimestamp` (Unix segundos) → forma canónica en /events/
      - `startDate` (string "YYYY-MM-DD") → en algunos endpoints
      - `timestamp` / `startTime` → variantes legacy
    Prueba todas en orden.
    """
    from datetime import date, datetime, timezone

    # 1) startTimestamp (Unix epoch en segundos)
    ts = match.get("startTimestamp")
    if ts:
        try:
            return datetime.fromtimestamp(int(ts), tz=timezone.utc).date()
        except (ValueError, TypeError, OSError):
            pass

    # 2) startDate string
    start_date = match.get("startDate") or match.get("start_date")
    if start_date:
        try:
            return date.fromisoformat(str(start_date)[:10])
        except (ValueError, TypeError):
            pass

    # 3) Variantes legacy
    for key in ("timestamp", "startTime"):
        ts = match.get(key)
        if ts:
            try:
                return datetime.fromtimestamp(int(ts), tz=timezone.utc).date()
            except (ValueError, TypeError, OSError):
                continue
    return None


def get_match_shots(client, match_id: int) -> dict:
    return get_json(client, _api_url("event", match_id, "shotmap"))


def get_match_events(client, match_id: int) -> dict:
    return get_json(client, _api_url("event", match_id, "incidents"))


def get_match_lineups(client, match_id: int) -> dict:
    return get_json(client, _api_url("event", match_id, "lineups"))


def _collect_shots_from_raw(matches_dir: Path) -> list[dict]:
    """Reconstruye tiros desde data/raw/.../matches/*/shots.json (no pierde pasadas anteriores)."""
    shots: list[dict] = []
    if not matches_dir.is_dir():
        return shots
    for match_dir in matches_dir.iterdir():
        if not match_dir.is_dir() or not match_dir.name.isdigit():
            continue
        path = match_dir / "shots.json"
        if not path.exists() or path.stat().st_size < 50:
            continue
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError) as e:
            log.debug("shots.json omitido %s: %s", match_dir.name, e)
            continue
        mid = int(match_dir.name)
        for row in data.get("shotmap", []):
            item = dict(row)
            item.setdefault("_match_id_ss", mid)
            shots.append(item)
    return shots


def _collect_events_from_raw(matches_dir: Path) -> list[dict]:
    """Reconstruye incidentes desde data/raw/.../matches/*/events.json."""
    events: list[dict] = []
    if not matches_dir.is_dir():
        return events
    for match_dir in matches_dir.iterdir():
        if not match_dir.is_dir() or not match_dir.name.isdigit():
            continue
        path = match_dir / "events.json"
        if not path.exists() or path.stat().st_size < 50:
            continue
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError) as e:
            log.debug("events.json omitido %s: %s", match_dir.name, e)
            continue
        mid = int(match_dir.name)
        for row in data.get("incidents", []):
            item = dict(row)
            item.setdefault("_match_id_ss", mid)
            events.append(item)
    return events


def _is_match_finished(match: dict) -> bool:
    """True si el partido ya terminó (FETCH_ONLY_FINISHED)."""
    status = match.get("status") or {}
    if not isinstance(status, dict):
        return False
    if str(status.get("type", "")).lower() == "finished":
        return True
    return status.get("code") in (100, "100")


def _match_dir_has_events(match_dir: Path) -> bool:
    path = match_dir / "events.json"
    return path.exists() and path.stat().st_size > 200


def _local_match_ids_with_events(matches_dir: Path) -> set[int]:
    ids: set[int] = set()
    if not matches_dir.is_dir():
        return ids
    for d in matches_dir.iterdir():
        if d.is_dir() and d.name.isdigit() and _match_dir_has_events(d):
            ids.add(int(d.name))
    return ids


def _select_matches_to_scrape(
    matches: list[dict],
    base_path: Path,
    scraped_ids: set[int],
    full_refresh: bool,
    from_date_obj: date | None,
) -> tuple[list[dict], dict[str, int]]:
    """Filtra partidos a descargar (BD + raw local + fecha + opcional solo finalizados)."""
    only_finished = os.getenv("SOFASCORE_ONLY_FINISHED", "").lower() in ("1", "true", "yes")
    local_ids = _local_match_ids_with_events(base_path / "matches")
    all_ids = {int(m["id"]) for m in matches if m.get("id") is not None}

    to_fetch: list[dict] = []
    stats = {
        "skipped_db": 0,
        "skipped_local": 0,
        "skipped_date": 0,
        "skipped_not_finished": 0,
    }

    for m in matches:
        match_id = int(m["id"])
        md = _get_match_date(m)
        match_dir = base_path / "matches" / str(match_id)

        if only_finished and not _is_match_finished(m):
            stats["skipped_not_finished"] += 1
            continue
        if from_date_obj and md and md < from_date_obj:
            stats["skipped_date"] += 1
            continue

        if not full_refresh:
            in_db = match_id in scraped_ids
            has_local = _match_dir_has_events(match_dir)
            # Mantenimiento: re-descargar partidos recientes aunque estén en BD.
            refresh_recent = bool(from_date_obj and md and md >= from_date_obj)
            if in_db and has_local and not refresh_recent:
                stats["skipped_db"] += 1
                continue
            if has_local and not in_db and not refresh_recent:
                stats["skipped_local"] += 1
                continue

        to_fetch.append(m)

    print(
        f"  [INFO] Cobertura fixtures={len(all_ids)} | BD+eventos={len(scraped_ids & all_ids)} "
        f"| raw events={len(local_ids)} | pendientes descarga={len(to_fetch)}"
    )
    missing_db = len(all_ids - scraped_ids)
    missing_raw = len(all_ids - local_ids)
    if missing_db or missing_raw:
        print(
            f"  [INFO] Huecos: sin eventos en BD={missing_db} | sin events.json en raw={missing_raw}"
        )
    if stats["skipped_db"] or stats["skipped_local"]:
        print(
            f"  [INFO] Omitidos: BD+raw OK={stats['skipped_db']} | solo raw OK={stats['skipped_local']}"
        )
    return to_fetch, stats


def get_scraped_sofascore_match_ids() -> set[int]:
    """Obtiene los id_sofascore de los partidos que ya tienen eventos en la BBDD."""
    try:
        from sqlalchemy import text
        from loaders.common import engine
        query = """
            SELECT DISTINCT m.id_sofascore
            FROM dim_match m
            JOIN fact_events e ON m.match_id = e.match_id
            WHERE m.id_sofascore IS NOT NULL
        """
        with engine.connect() as conn:
            rows = conn.execute(text(query)).fetchall()
            return {int(r[0]) for r in rows}
    except Exception as e:
        log.warning("No se pudo consultar BBDD para cache de SofaScore: %s", e)
        return set()


# ── ORCHESTRATOR ──────────────────────────────────────────────────────────────

def _auto_select_api_base(session, tournament_id: int, season_id: int) -> None:
    """Si la API oficial devuelve 403, cambia al mirror sin esperar al fallo en bulk."""
    global _PREFERRED_API_BASE
    if _PREFERRED_API_BASE or os.getenv("SOFASCORE_DISABLE_MIRROR", "").lower() in ("1", "true", "yes"):
        return
    probe = f"{SOFASCORE_API.rstrip('/')}/unique-tournament/{tournament_id}/season/{season_id}/events/last/0"
    try:
        resp = session.get(probe, timeout=12)
        if resp.status_code in {403, 401, 429}:
            raise SofaScoreBlockedError(f"probe HTTP {resp.status_code}")
        resp.raise_for_status()
        _validate_sofascore_payload(resp.json(), probe)
        return
    except Exception:
        mirror = os.getenv("SOFASCORE_MIRROR_API", SOFASCORE_MIRROR_API).rstrip("/")
        _PREFERRED_API_BASE = mirror
        print(f"  [INFO] API oficial bloqueada; usando mirror {mirror}")


def _sofascore_blocked_help(detail: Exception) -> str:
    return (
        "SofaScore bloquea la API oficial y el mirror alternativo también falló. "
        "Prueba SOFASCORE_MIRROR_FIRST=1, SOFASCORE_CHROME_PROFILE con tu perfil de Chrome, "
        "o SOFASCORE_PROXY con un proxy residencial. "
        f"Detalle: {detail}"
    )


def _challenge_wait_seconds() -> int:
    raw = os.getenv("SOFASCORE_CHALLENGE_WAIT_SEC", "90")
    try:
        return max(15, int(raw))
    except ValueError:
        return 90


def _wait_for_browser_access(driver: webdriver.Chrome, probe_url: str) -> bool:
    """Espera a que el usuario resuelva captcha/challenge en Chrome visible."""
    wait_sec = _challenge_wait_seconds()
    print(
        f"  [INFO] Chrome abierto. Si ves captcha, resuélvelo en la ventana de SofaScore.\n"
        f"         Esperando hasta {wait_sec}s a que la API responda..."
    )
    deadline = time.time() + wait_sec
    while time.time() < deadline:
        for attempt_url in _alternate_api_urls(probe_url):
            try:
                _browser_fetch_json(driver, attempt_url, timeout_ms=8000)
                print("  [OK] Acceso a la API confirmado desde el navegador.")
                return True
            except Exception:
                pass
        time.sleep(3)
    return False


def _ensure_selenium_client(
    client,
    driver: webdriver.Chrome | None,
    probe_url: str | None = None,
) -> tuple[object, webdriver.Chrome]:
    """Abre Chrome visible, calienta cookies y opcionalmente espera al usuario."""
    if driver is not None:
        return driver, driver
    print("  [INFO] Reintentando con Chrome visible para resolver cookies/challenge...")
    driver = create_driver(headless=False)
    driver.get(SOFASCORE_WEB)
    time.sleep(4)
    if probe_url and not _wait_for_browser_access(driver, probe_url):
        print("  [WARN] Timeout esperando challenge; se intentará igualmente con fetch del navegador.")
    if _is_http_session(client):
        _sync_driver_cookies_to_session(driver, client)
        return client, driver
    return driver, driver


def _get_season_id_with_fallback(
    client,
    driver: webdriver.Chrome | None,
    tournament_id: int,
    season_name: str,
) -> tuple[Optional[int], Optional[str], object, webdriver.Chrome | None]:
    try:
        season_id, season_label = get_season_id(client, tournament_id, season_name)
        return season_id, season_label, client, driver
    except SofaScoreBlockedError as e:
        print(f"  [WARN] Cliente HTTP bloqueado al resolver temporada: {e}")
        driver, client = _ensure_selenium_client(
            client, driver,
            probe_url=_api_url("unique-tournament", tournament_id, "seasons"),
        )
        try:
            season_id, season_label = get_season_id(client, tournament_id, season_name)
            return season_id, season_label, client, driver
        except SofaScoreBlockedError as selenium_error:
            raise SofaScoreBlockedError(_sofascore_blocked_help(selenium_error)) from selenium_error


def _get_matches_with_fallback(
    client,
    driver: webdriver.Chrome | None,
    tournament_id: int,
    season_id: int,
) -> tuple[list[dict], object, webdriver.Chrome | None]:
    try:
        matches = get_matches(client, tournament_id, season_id)
        return matches, client, driver
    except SofaScoreBlockedError as e:
        print(f"  [WARN] Cliente HTTP bloqueado al descargar partidos: {e}")
        driver, client = _ensure_selenium_client(
            client, driver,
            probe_url=_api_url("unique-tournament", tournament_id, "season", season_id, "events", "last", 0),
        )
        try:
            matches = get_matches(client, tournament_id, season_id)
            return matches, client, driver
        except SofaScoreBlockedError as selenium_error:
            raise SofaScoreBlockedError(_sofascore_blocked_help(selenium_error)) from selenium_error


def scrape_sofascore(
    season_name: str = None,
    tournament_id: int = TOURNAMENT_ID,
    competition_name: str = "Bundesliga",
    from_date: str = None,
    full_refresh: bool = False,
) -> tuple[list[dict], list[dict], list[dict], list[dict]]:
    """Orquestador principal."""
    global _PREFERRED_API_BASE
    print(f"  [INFO] Iniciando scrape_sofascore para {competition_name} ({season_name or 'actual'})...")
    
    if season_name is None:
        season_name = SEASON_NAMES[0]
    
    from_date_obj = None
    if from_date:
        from_date_obj = datetime.strptime(from_date, "%Y-%m-%d").date()
        print(f"  [FILTER] Descargando solo partidos desde: {from_date}")
    
    print("  [INFO] Iniciando sesión HTTP con SofaScore...")
    _print_throttle_config()

    from utils.batch import generate_batch_id
    batch_id = generate_batch_id()

    client = create_http_session()
    driver = None
    all_shots:    list[dict] = []
    all_events:   list[dict] = []
    all_lineups:  list[dict] = []

    try:
        season_id, season_label = get_reference_season_id(
            competition_name, tournament_id, season_name,
        )
        if season_id:
            print(f"  [INFO] SofaScore season_id resuelto desde tabla maestra: {season_id}")
        else:
            season_id, season_label, client, driver = _get_season_id_with_fallback(
                client, driver, tournament_id, season_name,
            )
        if season_id is None:
            raise ValueError(f"Temporada '{season_name}' no encontrada en SofaScore")

        _PREFERRED_API_BASE = None
        mirror = os.getenv("SOFASCORE_MIRROR_API", SOFASCORE_MIRROR_API).rstrip("/")
        if os.getenv("SOFASCORE_MIRROR_FIRST", "").lower() in ("1", "true", "yes"):
            _PREFERRED_API_BASE = mirror
            print(f"  [INFO] SOFASCORE_MIRROR_FIRST activo -> {_PREFERRED_API_BASE}")
        elif _is_http_session(client):
            _auto_select_api_base(client, tournament_id, season_id)
            if _PREFERRED_API_BASE and _PREFERRED_API_BASE.rstrip("/") == mirror:
                print(
                    f"  [INFO] API oficial bloqueada; usando solo mirror {_PREFERRED_API_BASE} "
                    f"(SOFASCORE_TRY_OFFICIAL=1 para forzar oficial)"
                )

        print(f"\n[SEASON] Temporada: {season_label}  (id={season_id})")

        # Resolución de competition_name: si llegó vacío intentamos por
        # tournament_id contra wizard.competitions. Cae a "La Liga" por defecto.
        resolved_comp = competition_name
        if not resolved_comp and tournament_id:
            try:
                from wizard.competitions import COMPETITIONS
                for key, config in COMPETITIONS.items():
                    if config.get("sources", {}).get("sofascore", {}).get("tournament_id") == tournament_id:
                        resolved_comp = key
                        break
            except Exception:
                pass
        if not resolved_comp:
            resolved_comp = "La Liga"

        # Etiqueta de temporada para carpetas (YYYY_YYYY).
        from utils.data_paths import normalize_season as _norm
        folder_season = (
            _norm(season_name) or _norm(season_label)
            or (season_name or season_label or "")
              .replace("/", "_").replace(" ", "_")
        )

        # Rutas canónicas:
        #   raw/<comp>/<season>/sofascore/                 ← fixtures + por-partido
        #   clean/<comp>/<season>/sofascore/<table>.csv    ← CSVs DB-ready
        season_raw_dir = raw_dir(resolved_comp, folder_season, "sofascore")
        season_raw_dir.mkdir(parents=True, exist_ok=True)
        base_path = season_raw_dir
        fixtures_path = base_path / "fixtures.json"

        details_only = os.getenv("SOFASCORE_DETAILS_ONLY", "").lower() in ("1", "true", "yes")
        if details_only and fixtures_path.exists():
            print("  [INFO] SOFASCORE_DETAILS_ONLY: reutilizo fixtures.json local")
            with fixtures_path.open(encoding="utf-8") as f:
                payload = json.load(f)
            matches = payload if isinstance(payload, list) else payload.get("events", [])
        else:
            matches, client, driver = _get_matches_with_fallback(
                client, driver, tournament_id, season_id,
            )
            _save_json(matches, fixtures_path)
        print(f"  [+] {len(matches)} partidos en fixtures")

        # Filtrar por fecha si se especifica from_date.
        # Diagnóstico: distinguir entre "sin fecha extraíble" y "anteriores"
        # para detectar problemas en el parseo del JSON de SofaScore.
        if from_date_obj:
            kept, no_date, before = [], 0, 0
            for m in matches:
                md = _get_match_date(m)
                if md is None:
                    no_date += 1
                elif md >= from_date_obj:
                    kept.append(m)
                else:
                    before += 1
            matches = kept
            print(
                f"  [+] {len(matches)} partidos después de {from_date} "
                f"(descartados: {before} anteriores, {no_date} sin fecha)"
            )

        scraped_ids = get_scraped_sofascore_match_ids() if not full_refresh else set()
        to_fetch, _skip_stats = _select_matches_to_scrape(
            matches, base_path, scraped_ids, full_refresh, from_date_obj,
        )

        for i, m in enumerate(to_fetch, 1):
            match_id = m["id"]
            home     = m.get("homeTeam", {}).get("name", "?")
            away     = m.get("awayTeam", {}).get("name", "?")

            print(f"  [{i}/{len(to_fetch)}] Match {match_id}: {home} vs {away}")

            # raw/<comp>/<season>/sofascore/matches/<match_id>/{shots,events,lineups}.json
            match_dir = base_path / "matches" / str(match_id)
            match_dir.mkdir(parents=True, exist_ok=True)

            match_blocked = False

            # Tiros
            try:
                shots_raw = get_match_shots(client, match_id)
                _save_json(shots_raw, match_dir / "shots.json")
                # AÃ±adir contexto al registro crudo
                for s in shots_raw.get("shotmap", []):
                    s["_match_id_ss"]     = match_id
                    s["_season_label"]    = season_label
                    s["_home_team_id_ss"] = m.get("homeTeam", {}).get("id")
                    s["_away_team_id_ss"] = m.get("awayTeam", {}).get("id")
                all_shots.extend(shots_raw.get("shotmap", []))
            except SofaScoreBlockedError as e:
                match_blocked = True
                log.warning("Shots failed match %d: %s", match_id, e)
            except Exception as e:
                log.warning("Shots failed match %d: %s", match_id, e)

            # Eventos
            try:
                events_raw = get_match_events(client, match_id)
                _save_json(events_raw, match_dir / "events.json")
                for ev in events_raw.get("incidents", []):
                    ev["_match_id_ss"]  = match_id
                    ev["_season_label"] = season_label
                all_events.extend(events_raw.get("incidents", []))
            except SofaScoreBlockedError as e:
                match_blocked = True
                log.warning("Events failed match %d: %s", match_id, e)
            except Exception as e:
                log.warning("Events failed match %d: %s", match_id, e)

            # Alineaciones
            try:
                lineups_raw = get_match_lineups(client, match_id)
                _save_json(lineups_raw, match_dir / "lineups.json")
                all_lineups.append({"match_id": match_id, "data": lineups_raw})
            except SofaScoreBlockedError as e:
                match_blocked = True
                log.warning("Lineups failed match %d: %s", match_id, e)
            except Exception as e:
                log.warning("Fallo general en partido %d: %s", match_id, e)

            if match_blocked:
                _throttle_after_block()
            _throttle_between_matches()

    finally:
        if driver is not None:
            driver.quit()

    if matches:
        # CSV de hechos desde todo el raw acumulado (evita pisar con solo esta pasada).
        matches_dir = base_path / "matches"
        all_shots = _collect_shots_from_raw(matches_dir)
        all_events = _collect_events_from_raw(matches_dir)
        ev_matches = len({e.get("_match_id_ss") for e in all_events})
        sh_matches = len({s.get("_match_id_ss") for s in all_shots})
        print(
            f"  [INFO] Hechos desde raw: {len(all_events)} eventos ({ev_matches} partidos), "
            f"{len(all_shots)} tiros ({sh_matches} partidos)"
        )

        df_matches = transform_matches(matches)
        df_shots   = transform_shots(all_shots)
        df_events  = transform_events(all_events)
        df_teams   = extract_teams(matches)
        df_players = extract_players(
            df_shots, df_events, df_teams,
            competition=resolved_comp,
            season=folder_season,
        )

        # Cada CSV se escribe en data/clean/<comp>/<season>/sofascore/ con
        # nombres simples (la fuente ya está en la carpeta). Se omiten DataFrames
        # vacíos para evitar "No columns to parse from file" al leerlos.
        def _maybe_save(name, df):
            if df is None or df.empty:
                print(f"  [skip] DataFrame vacío, no escribo {name}.csv")
                return
            out = save_clean_csv(resolved_comp, folder_season, "sofascore", name, df)
            print(f"  CSV {name}: {out}")

        _maybe_save("matches", df_matches)
        _maybe_save("shots",   df_shots)
        _maybe_save("events",  df_events)
        _maybe_save("teams",   df_teams)
        _maybe_save("players", df_players)

    return matches, all_shots, all_events, all_lineups


# â”€â”€ TRANSFORM â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def transform_matches(matches: list[dict]) -> pd.DataFrame:
    """Adapta la lista cruda de partidos a las columnas de dim_match.

    Columnas generadas:
        id_sofascore, match_date, competition, season,
        home_team_id_ss, away_team_id_ss,
        home_team_name, away_team_name,
        home_score, away_score, data_source
    """
    rows = []
    for m in matches:
        status = m.get("status", {})
        score  = m.get("homeScore", {}), m.get("awayScore", {})

        attendance = m.get("attendance")
        if attendance is None:
            venue = m.get("venue") or {}
            attendance = venue.get("attendance")

        # Referee (puede ser None en muchos partidos sin oficial publicado)
        ref = m.get("referee") or {}
        ref_country = (ref.get("country") or {}).get("name") if isinstance(ref.get("country"), dict) \
                      else ref.get("country")

        rows.append({
            "id_sofascore":     m.get("id"),
            "match_date":       _ss_timestamp_to_date(m.get("startTimestamp")),
            "competition":      m.get("tournament", {}).get("name"),
            "season":           m.get("season", {}).get("name"),
            "home_team_id_ss":  m.get("homeTeam", {}).get("id"),
            "away_team_id_ss":  m.get("awayTeam", {}).get("id"),
            "home_team_name":   m.get("homeTeam", {}).get("name"),
            "away_team_name":   m.get("awayTeam", {}).get("name"),
            "home_score":       m.get("homeScore", {}).get("current"),
            "away_score":       m.get("awayScore", {}).get("current"),
            "attendance":       attendance,
            "referee_id_ss":    ref.get("id"),
            "referee_name":     ref.get("name"),
            "referee_country":  ref_country,
            "data_source":      "sofascore",
        })
    return pd.DataFrame(rows)


def transform_shots(shots_raw: list[dict]) -> pd.DataFrame:
    """Adapta los tiros crudos a las columnas de fact_shots.

    Columnas generadas:
        id_sofascore (match), player_id_ss, team_id_ss,
        minute, x, y, xg, result, shot_type, situation, data_source
    """
    rows = []
    for s in shots_raw:
        player = s.get("player", {})
        rows.append({
            # Referencias a resolver por el loader en la DB
            "match_id_ss":   s.get("_match_id_ss"),
            "player_id_ss":   player.get("id"),
            "player_name":    player.get("name"),
            "team_id_ss":     s.get("teamId"),
            # Campos de fact_shots
            "minute":         s.get("time"),
            "x":              s.get("playerCoordinates", {}).get("x"),
            "y":              s.get("playerCoordinates", {}).get("y"),
            "xg":             s.get("xg"),
            "result":         s.get("shotType"),          # Goal, Miss, Save...
            "shot_type":      s.get("bodyPart"),          # RightFoot, LeftFoot, Head
            "situation":      s.get("situation"),         # OpenPlay, SetPiece...
            "data_source":    "sofascore",
        })
    df = pd.DataFrame(rows)
    if not df.empty:
        df["x"]   = pd.to_numeric(df["x"],   errors="coerce").round(4)
        df["y"]   = pd.to_numeric(df["y"],   errors="coerce").round(4)
        df["xg"]  = pd.to_numeric(df["xg"],  errors="coerce").round(4)
        df["minute"] = pd.to_numeric(df["minute"], errors="coerce").astype("Int16")
    return df


def transform_events(events_raw: list[dict]) -> pd.DataFrame:
    """Adapta los incidentes crudos a las columnas de fact_events.

    Columnas generadas:
        match_id_ss, player_id_ss, player_name, team_id_ss,
        event_type, minute, x, y, outcome, data_source
    """
    rows = []
    for ev in events_raw:
        player = ev.get("player", {})
        point  = ev.get("incidentPoint") or {}
        rows.append({
            "match_id_ss":  ev.get("_match_id_ss"),
            "player_id_ss": player.get("id"),
            "player_name":  player.get("name"),
            "team_id_ss":   ev.get("teamId"),
            "event_type":   ev.get("incidentType"),
            "minute":       ev.get("time"),
            "second":       None,           # SofaScore no expone segundos en incidentes
            "x":            point.get("x"),
            "y":            point.get("y"),
            "end_x":        None,
            "end_y":        None,
            "outcome":      ev.get("incidentClass"),
            "data_source":  "sofascore",
        })
    df = pd.DataFrame(rows)
    if not df.empty:
        df["minute"] = pd.to_numeric(df["minute"], errors="coerce").astype("Int16")
        df["x"] = pd.to_numeric(df["x"], errors="coerce").round(4)
        df["y"] = pd.to_numeric(df["y"], errors="coerce").round(4)
    return df


# â”€â”€ DIM EXTRACTORS â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def extract_teams(matches: list[dict]) -> pd.DataFrame:
    """Extrae equipos Ãºnicos de la lista de partidos -> columnas de dim_team.

    Columnas: id_sofascore, canonical_name
    """
    teams = {}
    for m in matches:
        for side in ("homeTeam", "awayTeam"):
            t = m.get(side, {})
            tid = t.get("id")
            if tid and tid not in teams:
                teams[tid] = t.get("name")
    df = pd.DataFrame(
        [{"id_sofascore": k, "canonical_name": v} for k, v in teams.items()]
    ).sort_values("id_sofascore").reset_index(drop=True)
    return df


def extract_players(
    shots_df: pd.DataFrame,
    events_df: pd.DataFrame,
    teams_df: pd.DataFrame | None = None,
    *,
    competition: str | None = None,
    season: str | None = None,
) -> pd.DataFrame:
    """Extrae jugadores únicos de tiros y eventos -> columnas de dim_player.

    Columnas: id_sofascore, canonical_name, team_id_ss, team_name, competition, season, source
    """
    team_names: dict = {}
    if teams_df is not None and not teams_df.empty:
        id_col = "id_sofascore" if "id_sofascore" in teams_df.columns else None
        name_col = "canonical_name" if "canonical_name" in teams_df.columns else None
        if id_col and name_col:
            for _, t in teams_df.iterrows():
                tid = t.get(id_col)
                if tid is not None and str(tid).strip():
                    team_names[int(tid)] = t.get(name_col)

    frames = []
    for df in (shots_df, events_df):
        if not df.empty and "player_id_ss" in df.columns:
            cols = ["player_id_ss", "player_name"]
            renames = {"player_id_ss": "id_sofascore", "player_name": "canonical_name"}
            if "team_id_ss" in df.columns:
                cols.append("team_id_ss")
            frames.append(df[cols].rename(columns=renames))
    if not frames:
        return pd.DataFrame(columns=[
            "id_sofascore", "canonical_name", "team_id_ss", "team_name",
            "competition", "season", "source",
        ])
    combined = pd.concat(frames)
    combined = combined.drop_duplicates(subset=["id_sofascore"]).dropna(subset=["id_sofascore"])
    if "team_id_ss" in combined.columns:
        combined["team_name"] = combined["team_id_ss"].apply(
            lambda x: team_names.get(int(x)) if pd.notna(x) else None
        )
    if competition:
        combined["competition"] = competition
    if season:
        combined["season"] = season
    combined["source"] = "sofascore"
    return combined.sort_values("id_sofascore").reset_index(drop=True)


# â”€â”€ HELPERS INTERNOS â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def _save_json(data, path: Path) -> None:
    """Guarda JSON en disco de forma segura."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def _ss_timestamp_to_date(ts) -> Optional[str]:
    """Convierte un Unix timestamp de SofaScore a cadena YYYY-MM-DD."""
    if not ts:
        return None
    from datetime import datetime, timezone
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")


# â”€â”€ MAIN â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def main():
    """CLI: `python -m scrapers.sofascore_scraper --competition "La Liga" --seasons 2024 2025`."""
    import argparse
    logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s - %(message)s")

    parser = argparse.ArgumentParser(description="Scraper de SofaScore por comp+temporada")
    parser.add_argument("--competition", default="La Liga",
                        help='Nombre canónico (ej. "La Liga", "Premier League")')
    parser.add_argument("--seasons", nargs="+", default=None,
                        help='Etiquetas de temporada (ej. "2024/2025" "2025/2026"). '
                             'Si se omite, usa SEASON_NAMES.')
    parser.add_argument("--from-date", type=str, help="YYYY-MM-DD (filtra partidos)")
    parser.add_argument("--full-refresh", action="store_true",
                        help="Ignora la caché de partidos ya cargados en BD")
    args = parser.parse_args()

    # Resuelve el tournament_id de la competición elegida.
    tournament_id = TOURNAMENT_ID
    try:
        from wizard.competitions import get_competition
        cfg = get_competition(args.competition) if args.competition else None
        if cfg:
            tournament_id = cfg.get("sources", {}).get("sofascore", {}).get("tournament_id", TOURNAMENT_ID)
    except Exception:
        pass

    seasons = args.seasons or default_seasons_for_competition(args.competition or "La Liga")

    print("=" * 55)
    print(f"  SofaScore scraper — {args.competition} — {len(seasons)} temporada(s)")
    print("=" * 55)

    for season_name in seasons:
        print(f"\n[SEASON] {season_name}")
        try:
            matches, all_shots, all_events, _ = scrape_sofascore(
                competition_name=args.competition,
                season_name=season_name,
                tournament_id=tournament_id,
                from_date=args.from_date,
                full_refresh=args.full_refresh,
            )
        except ValueError as e:
            log.warning("Temporada %s no disponible: %s", season_name, e)
            continue
        if not matches:
            print(f"  [!] No se obtuvieron partidos para {season_name}")
            continue
        print(f"    Partidos: {len(matches)} | Tiros: {len(all_shots)} | Eventos: {len(all_events)}")

    print("\n[OK] Descarga de SofaScore completada")


if __name__ == "__main__":
    main()
