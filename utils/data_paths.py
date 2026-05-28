"""
utils/data_paths.py
===================
Helpers para la convención unificada de rutas de datos del proyecto.

Estructura objetivo (acordada con el equipo):

    data/
    ├── raw/                 ← JSON crudos producidos por los scrapers
    │   └── <comp_slug>/     ← p.ej. la_liga, premier_league
    │       └── <season>/    ← p.ej. 2024_2025
    │           └── <source>/← p.ej. transfermarkt, sofascore, ...
    │               ├── stadiums/<team_slug>.json
    │               ├── players/<team_slug>.json
    │               └── injuries/<player_id>.json  ← historial completo por jugador
    │
    └── clean/               ← CSV DB-ready (los que leen los loaders)
        └── <comp_slug>/<season>/<source>/
            ├── stadiums.csv
            ├── players.csv
            ├── injuries.csv
            └── matches.csv

Notas de naming:
    - `comp_slug` siempre en snake_case sin acentos: 'la_liga', 'segunda_division',
      'champions_league'. Se obtiene con `slugify_competition()`.
    - `season` siempre como 'YYYY_YYYY' (año de inicio + año de fin). Se obtiene
      con `normalize_season()`.
    - El `batch_id` ya NO se usa como carpeta; va embebido en cada JSON.
"""

from __future__ import annotations

import datetime as _dt
import json
import re
import unicodedata
from pathlib import Path
from typing import Any, Iterable, Optional


# ── Raíces ───────────────────────────────────────────────────────────────────

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_ROOT    = PROJECT_ROOT / "data"
RAW_ROOT     = DATA_ROOT / "raw"
CLEAN_ROOT   = DATA_ROOT / "clean"
ARCHIVE_ROOT = DATA_ROOT / "archive"
CACHE_ROOT   = DATA_ROOT / ".cache"
LOGS_ROOT    = DATA_ROOT / "logs"


# ── Slugs ────────────────────────────────────────────────────────────────────

def _strip_accents(text: str) -> str:
    """Quita tildes y diacríticos manteniendo letras base."""
    return "".join(
        ch for ch in unicodedata.normalize("NFD", text)
        if unicodedata.category(ch) != "Mn"
    )


def slugify_competition(name: str) -> str:
    """
    Normaliza un nombre de competición a snake_case sin tildes ni símbolos.

    Ejemplos:
        "La Liga"            → "la_liga"
        "la-liga"            → "la_liga"
        "Segunda División"   → "segunda_division"
        "Premier League"     → "premier_league"
        "UEFA Champions Lg." → "uefa_champions_lg"

    El resultado es estable y reversible vía un mapa en `competitions.py`
    si fuese necesario reconstruir el nombre humano.
    """
    if not name:
        return ""
    s = _strip_accents(name).lower().strip()
    # Reemplazar cualquier separador o símbolo por '_'
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = s.strip("_")
    return s


# ── Seasons ──────────────────────────────────────────────────────────────────

_SEASON_FULL_RE = re.compile(r"^(\d{4})[\-_/](\d{4})$")
_SEASON_SHORT_RE = re.compile(r"^(\d{2})[\-_/](\d{2})$")
_SEASON_YEAR_RE  = re.compile(r"^(\d{4})$")


def normalize_season(value) -> Optional[str]:
    """
    Convierte cualquier variante de temporada al formato canónico 'YYYY_YYYY'.

    Acepta:
        2024              → '2024_2025'
        '2024'            → '2024_2025'
        '2024_2025'       → '2024_2025'
        '2024/2025'       → '2024_2025'
        '2024-2025'       → '2024_2025'
        '24/25'           → '2024_2025'
        '24-25'           → '2024_2025'
        'season=2024_2025'→ '2024_2025'
        'LaLiga 25'       → '2024_2025'   (heurística: '25' = 25/26)
        'LaLiga 2024/2025'→ '2024_2025'

    Devuelve None si no encuentra patrón reconocible.
    """
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None

    # Quita prefijo 'season=' si existe
    s = re.sub(r"^season\s*=\s*", "", s, flags=re.IGNORECASE)

    # 1) Patrones puros
    m = _SEASON_FULL_RE.match(s)
    if m:
        return f"{m.group(1)}_{m.group(2)}"

    m = _SEASON_SHORT_RE.match(s)
    if m:
        a, b = int(m.group(1)), int(m.group(2))
        # asume 20YY
        return f"20{a:02d}_20{b:02d}"

    m = _SEASON_YEAR_RE.match(s)
    if m:
        y = int(m.group(1))
        return f"{y}_{y+1}"

    # 2) Texto libre tipo "LaLiga 25" / "LaLiga 2024/2025"
    m = re.search(r"(\d{4})[\-_/](\d{4})", s)
    if m:
        return f"{m.group(1)}_{m.group(2)}"
    m = re.search(r"(\d{2})[\-_/](\d{2})", s)
    if m:
        a, b = int(m.group(1)), int(m.group(2))
        return f"20{a:02d}_20{b:02d}"
    m = re.search(r"(?<!\d)(\d{4})(?!\d)", s)
    if m:
        y = int(m.group(1))
        return f"{y}_{y+1}"
    m = re.search(r"(?<!\d)(\d{2})(?!\d)", s)
    if m:
        y = int(m.group(1))
        return f"20{y:02d}_20{y+1:02d}"

    return None


# ── Builders de paths ────────────────────────────────────────────────────────

def raw_dir(competition: str, season, source: str, subdir: Optional[str] = None) -> Path:
    """
    Construye la ruta RAW para una (competición, temporada, fuente).

    Ejemplo:
        raw_dir("La Liga", "2024/2025", "transfermarkt", "stadiums")
        → data/raw/la_liga/2024_2025/transfermarkt/stadiums
    """
    comp_slug = slugify_competition(competition)
    season_label = normalize_season(season) or str(season)
    base = RAW_ROOT / comp_slug / season_label / source
    return (base / subdir) if subdir else base


def clean_dir(competition: str, season, source: Optional[str] = None) -> Path:
    """
    Construye la ruta CLEAN (CSV DB-ready) para una (competición, temporada).

    Si se especifica `source`, baja un nivel más para separar por fuente.
    Si no, devuelve el directorio compartido de la temporada (útil para
    archivos unificados tipo 'matches.csv' que combinan fuentes).

    Ejemplo:
        clean_dir("La Liga", "2024/2025", "transfermarkt")
        → data/clean/la_liga/2024_2025/transfermarkt
    """
    comp_slug = slugify_competition(competition)
    season_label = normalize_season(season) or str(season)
    base = CLEAN_ROOT / comp_slug / season_label
    return (base / source) if source else base


def ensure_dirs(*paths: Path) -> None:
    """Crea los directorios si no existen (atajo para múltiples paths)."""
    for p in paths:
        p.mkdir(parents=True, exist_ok=True)


def archive_dir(competition: str, season, source: Optional[str] = None) -> Path:
    """
    Ruta de archivo histórico (datos descontinuados / snapshots viejos).

    Ejemplo:
        archive_dir("La Liga", "2024_2025", "transfermarkt")
        → data/archive/la_liga/2024_2025/transfermarkt
    """
    comp_slug = slugify_competition(competition)
    season_label = normalize_season(season) or str(season)
    base = ARCHIVE_ROOT / comp_slug / season_label
    return (base / source) if source else base


# ── Cachés globales ──────────────────────────────────────────────────────────

def cache_file(name: str) -> Path:
    """
    Devuelve la ruta canónica de un fichero de caché global del proyecto.

    Convención: `data/.cache/{name}_last_scraped.json` (o tal cual si ya
    trae extensión). Crea el directorio padre si no existe.

    Ejemplos:
        cache_file("transfermarkt_stadiums")
        → data/.cache/transfermarkt_stadiums_last_scraped.json

        cache_file("sofascore_matches.json")
        → data/.cache/sofascore_matches.json
    """
    CACHE_ROOT.mkdir(parents=True, exist_ok=True)
    if "." in name:
        return CACHE_ROOT / name
    return CACHE_ROOT / f"{name}_last_scraped.json"


def load_cache(name: str) -> dict:
    """Carga un JSON de caché por nombre. Devuelve {} si no existe o falla."""
    path = cache_file(name)
    if not path.exists():
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def save_cache(name: str, data: dict) -> Path:
    """Guarda un dict como JSON en `data/.cache/{name}_last_scraped.json`."""
    path = cache_file(name)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)
    return path


# ── I/O JSON / CSV unificado ─────────────────────────────────────────────────

def _json_default(obj: Any):
    """Serializa tipos comunes que `json` no soporta de fábrica (date, Path)."""
    if isinstance(obj, (_dt.date, _dt.datetime)):
        return obj.isoformat()
    if isinstance(obj, Path):
        return str(obj)
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


def save_raw_json(
    competition: str,
    season,
    source: str,
    filename: str,
    data: Any,
    subdir: Optional[str] = None,
) -> Path:
    """
    Guarda un JSON en la ubicación RAW canónica.

    Ruta resultante:
        data/raw/<comp_slug>/<season>/<source>/[<subdir>/]<filename>.json

    El sufijo `.json` se añade si no viene en `filename`.
    Crea directorios padre. Reemplaza si ya existe.

    Ejemplo:
        save_raw_json("La Liga", 2024, "transfermarkt",
                      "fc-barcelona", record, subdir="stadiums")
        → data/raw/la_liga/2024_2025/transfermarkt/stadiums/fc-barcelona.json
    """
    target_dir = raw_dir(competition, season, source, subdir)
    target_dir.mkdir(parents=True, exist_ok=True)
    name = filename if filename.endswith(".json") else f"{filename}.json"
    path = target_dir / name
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2, default=_json_default)
    return path


def save_clean_csv(
    competition: str,
    season,
    source: str,
    filename: str,
    df,
    encoding: str = "utf-8-sig",
) -> Path:
    """
    Guarda un DataFrame como CSV en la ubicación CLEAN canónica.

    Ruta:
        data/clean/<comp_slug>/<season>/<source>/<filename>.csv

    El sufijo `.csv` se añade si no viene en `filename`.
    """
    target_dir = clean_dir(competition, season, source)
    target_dir.mkdir(parents=True, exist_ok=True)
    name = filename if filename.endswith(".csv") else f"{filename}.csv"
    path = target_dir / name
    df.to_csv(path, index=False, encoding=encoding)
    return path


def clean_csv_path(
    competition: str,
    season,
    source: str,
    filename: str,
) -> Path:
    """Devuelve la ruta del CSV CLEAN (NO crea ni escribe)."""
    name = filename if filename.endswith(".csv") else f"{filename}.csv"
    return clean_dir(competition, season, source) / name


# ── Búsqueda cross-competición ───────────────────────────────────────────────

def find_recent_raw_json(
    season,
    source: str,
    filename: str,
    subdir: Optional[str] = None,
    max_age_days: int = 30,
) -> Optional[dict]:
    """
    Busca el JSON más reciente con un nombre dado para una temporada+fuente
    en CUALQUIER competición ya scrapeada.

    Útil para evitar refetch cuando el mismo equipo/partido aparece en
    varias competiciones (UCL + liga doméstica, p.ej.).

    Devuelve el dict parseado del JSON más reciente con mtime ≤ `max_age_days`,
    o None si no encuentra.

    Ejemplo:
        find_recent_raw_json(2024, "transfermarkt",
                             "real-madrid", subdir="stadiums")
    """
    season_label = normalize_season(season) or str(season)
    name = filename if filename.endswith(".json") else f"{filename}.json"
    sub = f"{subdir}/" if subdir else ""
    pattern = f"*/{season_label}/{source}/{sub}{name}"

    today = _dt.date.today()
    best: Optional[tuple[float, Path]] = None
    for path in RAW_ROOT.glob(pattern):
        try:
            mtime = path.stat().st_mtime
            age_days = (today - _dt.datetime.fromtimestamp(mtime).date()).days
            if age_days <= max_age_days and (best is None or mtime > best[0]):
                best = (mtime, path)
        except Exception:
            continue
    if best is None:
        return None
    try:
        with open(best[1], "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


# ── Conversión de temporada al formato BD ────────────────────────────────────

def season_db_format(value) -> Optional[str]:
    """
    Convierte cualquier variante de temporada al formato canónico BD: 'YYYY/YYYY'.

    Es el "puente" entre el formato de paths (`YYYY_YYYY`) y el que usan
    `dim_match.season`, loaders y `utils/season_utils`.

    Ejemplos:
        2024            → '2024/2025'
        '2024_2025'     → '2024/2025'
        '24/25'         → '2024/2025'
    """
    label = normalize_season(value)
    if not label:
        return None
    return label.replace("_", "/")


def season_path_format(value) -> Optional[str]:
    """Alias semántico de `normalize_season()` → 'YYYY_YYYY' (formato carpetas)."""
    return normalize_season(value)


# ── Iterador útil para auditorías ────────────────────────────────────────────

def iter_raw_files(
    competition: Optional[str] = None,
    season=None,
    source: Optional[str] = None,
    subdir: Optional[str] = None,
    suffix: str = ".json",
) -> Iterable[Path]:
    """
    Itera ficheros en `data/raw/` filtrando por nivel.

    Cualquier filtro None se reemplaza por `*` en el glob.
    """
    comp = slugify_competition(competition) if competition else "*"
    season_label = (normalize_season(season) or str(season)) if season else "*"
    src = source or "*"
    sub = f"{subdir}/" if subdir else ""
    pattern = f"{comp}/{season_label}/{src}/{sub}*{suffix}"
    yield from RAW_ROOT.glob(pattern)


def iter_clean_csvs(
    competition: Optional[str] = None,
    season=None,
    source: Optional[str] = None,
    filename: Optional[str] = None,
) -> list[Path]:
    """
    Devuelve todos los CSV en `data/clean/` que cumplen el filtro.

    Cualquier nivel `None` se trata como wildcard. `filename` es el stem
    (sin `.csv`); si es None, devuelve todos los CSV del nivel.

    Ejemplos:
        # Todos los players.csv de cualquier comp/season/source:
        iter_clean_csvs(filename="players")

        # Sólo TM en La Liga 2024_2025:
        iter_clean_csvs("La Liga", 2024, "transfermarkt")
    """
    comp = slugify_competition(competition) if competition else "*"
    season_label = (normalize_season(season) or str(season)) if season else "*"
    src = source or "*"
    name = f"{filename}.csv" if filename else "*.csv"
    pattern = f"{comp}/{season_label}/{src}/{name}"
    return list(CLEAN_ROOT.glob(pattern))


def parse_clean_csv_meta(path: Path) -> dict[str, str]:
    """Extrae comp_slug, season y source desde una ruta bajo data/clean/."""
    try:
        rel = path.relative_to(CLEAN_ROOT)
    except ValueError:
        return {}
    parts = rel.parts
    if len(parts) < 3:
        return {}
    comp_slug, season_label, source = parts[0], parts[1], parts[2]
    season_display = season_label.replace("_", "/")
    return {
        "comp_slug": comp_slug,
        "season": season_label,
        "season_display": season_display,
        "source": source,
    }
