"""
scrapers/transfermarkt_stadiums_scraper.py
==========================================
Scraper de estadios de Transfermarkt por liga + temporada.

Sigue exactamente el mismo patrón que `transfermarkt_scraper.py`:

    Estructura:
        1. CONSTANTS       — configuración (delays, output dir)
        2. HELPERS         — reutiliza request_with_retry del scraper general
        3. FETCH           — get_team_stadium (página /stadion/verein/{id})
        4. ORCHESTRATOR    — scrape_transfermarkt_stadiums()
        5. TRANSFORM       — adapta campos a dim_stadium
        6. MAIN            — CLI con --competition --seasons

    URL ejemplo:
        https://www.transfermarkt.es/fc-barcelona/stadion/verein/131/saison_id/2025

    Salida (rutas estándar de `utils.data_paths`):
        data/raw/<comp_slug>/<season>/transfermarkt/stadiums/<team_slug>.json
        data/clean/<comp_slug>/<season>/transfermarkt/stadiums.csv

    Caché global de antigüedad (30 días):
        data/.cache/transfermarkt_stadiums_last_scraped.json
        (clave: "<comp_slug>|<team_id>|<season>")

    Campos extraídos (todos opcionales si TM no los publica):
        stadium_name, capacity, seats_covered, seats_vip, seats_standing,
        inaugurated_year, built_year, refurbished_year,
        owner, address, city, construction_cost,
        team_slug, team_id_tm, season

    El loader correspondiente hará el UPSERT contra `dim_stadium`
    y enlazará `dim_team.home_stadium_id`.
"""

from __future__ import annotations

import json
import logging
import random
import re
import sys
import time
from datetime import date
from pathlib import Path
from typing import Optional

# Permite ejecutar como script suelto
sys.path.append(str(Path(__file__).resolve().parent.parent))

import pandas as pd
from bs4 import BeautifulSoup

# Reutilizamos helpers/constantes del scraper general
from scrapers.transfermarkt_scraper import (
    DELAY_MIN,
    DELAY_MAX,
    MAX_RETRIES,
    request_with_retry,
    get_league_teams,
)
from utils.data_paths import (
    raw_dir,
    slugify_competition,
    save_raw_json,
    save_clean_csv,
    load_cache as _load_cache_file,
    save_cache as _save_cache_file,
    find_recent_raw_json,
)

log = logging.getLogger(__name__)

# ── CONSTANTS ────────────────────────────────────────────────────────────────

CACHE_NAME = "transfermarkt_stadiums"
CHECKPOINT_EVERY = 5

# Formato esperado de cada clave de caché: "<comp_slug>|<team_id>|<season>".
# Versiones antiguas del scraper escribían sólo "<team_id>|<season>", lo que
# impedía desambiguar cuando el mismo equipo aparece en varias competiciones
# (p.ej. Bayern en Bundesliga + Champions). Esas claves se descartan en carga
# para evitar reusar caché cruzada entre competiciones.
_CACHE_KEY_RE = re.compile(r"^[a-z0-9_]+\|\d+\|\d+$")


def _migrate_legacy_cache_keys(cache: dict) -> tuple[dict, int]:
    """
    Filtra claves de caché que no tengan el formato canónico de 3 partes.

    Devuelve (cache_filtrada, n_descartadas). Las claves legacy se descartan
    en silencio: no podemos saber a qué competición pertenecían, así que
    forzamos un re-scrape (la caché es sólo una optimización, no fuente de
    verdad). Es preferible re-scrapear un equipo que reusar datos de la
    competición incorrecta.
    """
    if not cache:
        return cache, 0
    keep, drop = {}, 0
    for k, v in cache.items():
        if isinstance(k, str) and _CACHE_KEY_RE.match(k):
            keep[k] = v
        else:
            drop += 1
    return keep, drop


def load_cache() -> dict:
    """Carga la caché y migra claves legacy in-place."""
    raw = _load_cache_file(CACHE_NAME)
    migrated, dropped = _migrate_legacy_cache_keys(raw)
    if dropped:
        log.warning(
            "transfermarkt_stadiums cache: descartadas %d claves legacy "
            "(formato antiguo sin comp_slug). Esos equipos se re-scrapearán.",
            dropped,
        )
        # Persistimos la migración para que el aviso no se repita.
        _save_cache_file(CACHE_NAME, migrated)
    return migrated


def save_cache(cache: dict) -> None:
    _save_cache_file(CACHE_NAME, cache)


def _checkpoint_stadiums(
    cache: dict,
    all_stadia: list[dict],
    processed: int,
    competition_name: str,
    season_label: str,
    *,
    force: bool = False,
) -> None:
    """Persist cache and clean CSV every CHECKPOINT_EVERY teams."""
    if not force and processed % CHECKPOINT_EVERY != 0:
        return
    save_cache(cache)
    if all_stadia:
        df = transform_stadiums(all_stadia)
        save_clean_csv(
            competition_name, season_label, "transfermarkt",
            "stadiums", df,
        )
    log.info("Checkpoint: %d equipos procesados — cache y CSV guardados", processed)
    print(f"  [CHECKPOINT] {processed} equipos — datos guardados")


# ── HELPERS ──────────────────────────────────────────────────────────────────

_INT_RE  = re.compile(r"\d[\d\.\,\s ]*\d|\d")
_YEAR_RE = re.compile(r"(19|20)\d{2}")


def _to_int(text: Optional[str]) -> Optional[int]:
    """Convierte '105.000', '99 354', '99,354 espectadores', '60\xa0000' → int.

    Acepta separadores típicos de miles de TM: punto, coma, espacio normal
    y espacio no-rompible (U+00A0). Devuelve None si no encuentra dígitos.
    """
    if not text:
        return None
    m = _INT_RE.search(text)
    if not m:
        return None
    raw = re.sub(r"[\.\,\s ]", "", m.group())
    try:
        return int(raw)
    except ValueError:
        return None


def _to_year(text: Optional[str]) -> Optional[int]:
    if not text:
        return None
    m = _YEAR_RE.search(text)
    return int(m.group()) if m else None


# Mapa label-normalizado → clave de salida.
# TM puede usar variantes; normalizamos a minúsculas sin acentos.
# Importante: SOLO se aplican dentro de la ficha de estadio
# (/stadion/verein/...), así que "nombre" se considera nombre del estadio
# sin riesgo de chocar con páginas de jugadores/clubes.
_LABEL_MAP = {
    # ── nombre del estadio (variantes que usa TM en distintos layouts) ──
    "nombre del estadio":      "stadium_name",
    "estadio":                 "stadium_name",
    "nombre":                  "stadium_name",
    "denominacion":            "stadium_name",
    "name des stadions":       "stadium_name",
    "stadium name":            "stadium_name",
    # ── aforo / asientos ──
    "aforo total":             "capacity",
    "capacidad total":         "capacity",
    "capacidad":               "capacity",
    "asientos":                "seats_total",
    "plazas sentadas":         "seats_total",
    "asientos cubiertos":      "seats_covered",
    "plazas cubiertas":        "seats_covered",
    "asientos vip":            "seats_vip",
    "plazas vip":              "seats_vip",
    "vip":                     "seats_vip",
    "palcos":                  "vip_boxes",
    "palcos vip":              "vip_boxes",
    "plazas de pie":           "seats_standing",
    "asientos de pie":         "seats_standing",
    # ── fechas (inauguracion / construccion / reforma) ──
    "inaugurado":              "inaugurated_year",
    "inauguracion":            "inaugurated_year",
    "ano de inauguracion":     "inaugurated_year",
    "apertura":                "inaugurated_year",
    "ano de apertura":         "inaugurated_year",
    "fecha de inauguracion":   "inaugurated_year",
    "construido":              "built_year",
    "construccion":            "built_year",
    "ano de construccion":     "built_year",
    "ano de finalizacion":     "built_year",
    "finalizacion":            "built_year",
    "reformado":               "refurbished_year",
    "reforma":                 "refurbished_year",
    "ultima reforma":          "refurbished_year",
    "renovacion":              "refurbished_year",
    "ultima renovacion":       "refurbished_year",
    # ── propiedad / gestion ──
    "propietario":             "owner",
    "duenos":                  "owner",
    "duenno":                  "owner",
    "operador":                "operator",
    "gestor":                  "operator",
    "gestionado por":          "operator",
    "explotador":              "operator",
    # ── ubicacion ──
    "direccion":               "address",
    "domicilio":               "address",
    "ciudad":                  "city",
    "localidad":               "city",
    "pais":                    "country",
    # ── construccion ──
    "coste de construccion":   "construction_cost",
    "coste construccion":      "construction_cost",
    "coste":                   "construction_cost",
    "presupuesto":             "construction_cost",
    "superficie":              "surface",
    "cesped":                  "surface",
    "tipo de cesped":          "surface",
    "tipo de superficie":      "surface",
    "arquitecto":              "architect",
    "estudio de arquitectura": "architect",
    # ── historial, naming rights y extras (v3 — informe) ──
    "antes":                       "previous_names_raw",
    "anteriormente":               "previous_names_raw",
    "nombres anteriores":          "previous_names_raw",
    "medidas del terreno de juego": "pitch_dimensions",
    "dimensiones del campo":       "pitch_dimensions",
    "medidas del campo":           "pitch_dimensions",
    "derechos del nombre":         "naming_rights",
    "naming rights":               "naming_rights",
    "duracion":                    "naming_rights_until",
    "cesped con calefaccion":      "has_pitch_heating",
    "calefaccion del cesped":      "has_pitch_heating",
    "pista de atletismo":          "has_athletics_track",
    "capacidad internacional":     "capacity_intl",
}

# Labels que SIEMPRE deben sobrescribir un valor previo (no usar setdefault).
# stadium_name necesita esto porque el header h1 contiene el nombre del CLUB,
# no del estadio, y queremos que cualquier valor extraído de la tabla de
# datos del estadio prevalezca.
_OVERRIDE_KEYS = {"stadium_name"}

_INT_FIELDS = {"capacity", "seats_total", "seats_covered", "seats_vip",
               "vip_boxes", "seats_standing"}
_YEAR_FIELDS = {"inaugurated_year", "built_year", "refurbished_year"}
_BOOL_FIELDS = {"has_pitch_heating", "has_athletics_track"}


def _normalize_label(s: str) -> str:
    """Minúsculas, sin acentos, sin signos finales (':')."""
    if not s:
        return ""
    s = s.strip().rstrip(":").lower()
    repl = {"á":"a","é":"e","í":"i","ó":"o","ú":"u","ü":"u","ñ":"n"}
    for k, v in repl.items():
        s = s.replace(k, v)
    return s


def _slug_to_loose(s: str) -> str:
    """Convierte un slug o nombre a su forma comparable (minúsculas, sin
    acentos, sin separadores). Útil para detectar si el nombre extraído
    coincide accidentalmente con el del equipo."""
    if not s:
        return ""
    s = _normalize_label(s)
    return re.sub(r"[^a-z0-9]", "", s)


# ── Parsers v3 ───────────────────────────────────────────────────────────────

_RE_PREV_NAME = re.compile(
    r"^(.+?)\s*\((\d{2}/\d{2}/\d{4})\s*-\s*(\d{2}/\d{2}/\d{4})\)$"
)


def parse_previous_names(raw: str) -> list[dict]:
    """Parsea el campo 'Antes:' de TM en lista de dicts.

    Formato de entrada (multilínea):
        Nuevo Mirandilla (25/06/2021 - 03/03/2026)
        Ramón de Carranza (03/09/1955 - 24/06/2021)

    Devuelve lista de dicts con keys: name, date_from, date_to (DD/MM/YYYY).
    Si una línea no tiene fechas, date_from y date_to serán None.
    """
    if not raw:
        return []
    results = []
    for line in raw.strip().splitlines():
        line = line.strip()
        if not line:
            continue
        m = _RE_PREV_NAME.match(line)
        if m:
            results.append({
                "name": m.group(1).strip(),
                "date_from": m.group(2),
                "date_to": m.group(3),
            })
        else:
            results.append({"name": line, "date_from": None, "date_to": None})
    return results


def _parse_pitch_dimensions(raw: str) -> tuple[Optional[int], Optional[int]]:
    """Parsea '104m x 68m' o '104 x 68' → (104, 68) (largo, ancho)."""
    if not raw:
        return None, None
    m = re.match(r"(\d+)\s*m?\s*[xX×]\s*(\d+)\s*m?", raw.strip())
    if m:
        return int(m.group(1)), int(m.group(2))
    return None, None


def _extract_stadium_id_from_page(soup) -> Optional[int]:
    """Extrae el ID numérico del estadio desde el enlace /stadion/N.

    TM enlaza a '/bayarena/startseite/stadion/4' — extraemos el 4.
    No confundir con /stadion/verein/{id} que es la ficha del club.
    """
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/stadion/" not in href or "/verein/" in href or "/saison_id/" in href:
            continue
        m_id = re.search(r"/stadion/(\d+)", href)
        if m_id:
            return int(m_id.group(1))
    return None


def _extract_stadium_name_from_page(soup, team_slug: str) -> Optional[str]:
    """
    Intenta extraer el nombre del estadio de la página de TM.

    Orden de preferencia:
      1. Enlace a la página estándar del estadio: <a href="/<stadium_slug>/startseite/stadion/...">
         (no confundir con /stadion/verein/ que es la ficha del club).
      2. Subtítulo / 'club-info' del header de página.
      3. Título de la pestaña del navegador (<title>BayArena | …</title>).

    NUNCA usar h1.data-header__headline-wrapper porque contiene el nombre
    del CLUB, no del estadio.

    Si el valor extraído coincide con el nombre del equipo (team_slug),
    devuelve None para que el caller intente otra fuente.
    """
    team_slug_loose = _slug_to_loose(team_slug)

    # 1) Enlace dedicado al estadio (no /verein/, no /saison_id/).
    #    TM enlaza algo como /bayarena/startseite/stadion/4 con el texto "BayArena".
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/stadion/" not in href:
            continue
        if "/verein/" in href or "/saison_id/" in href:
            continue
        txt = " ".join(a.stripped_strings)
        if not txt:
            continue
        # Filtra textos demasiado largos (descripciones) o ruido común
        if len(txt) > 80 or txt.lower().startswith(("estadio", "stadium")):
            continue
        if _slug_to_loose(txt) == team_slug_loose:
            continue
        return txt.strip()

    # 2) Subtítulo del header de página (club-info / data-header__subtext).
    for sel in (
        "p.data-header__club-info",
        "span.data-header__sub",
        "span.data-header__subtitle",
        "h2.data-header__subtitle",
        "div.dataMain h2",
        "h2.content-box-headline",
    ):
        node = soup.select_one(sel)
        if not node:
            continue
        txt = " ".join(node.stripped_strings)
        # El subtitle puede traer "BayArena | Capacidad: 30.210"; cortamos.
        candidate = re.split(r"[|·•]", txt, maxsplit=1)[0].strip()
        candidate = re.sub(r"^\s*estadio[:\s\-]*", "", candidate,
                           flags=re.IGNORECASE).strip()
        if candidate and _slug_to_loose(candidate) != team_slug_loose:
            return candidate

    # 3) <title> de la página: TM lo formatea como "<Stadium> | <Club> | TM".
    title = soup.find("title")
    if title:
        raw = " ".join(title.stripped_strings)
        first = re.split(r"[|·•\-]", raw, maxsplit=1)[0].strip()
        # Limpia prefijos típicos en distintos idiomas.
        first = re.sub(r"^\s*(estadio|stadium|stadion)[:\s\-]*",
                       "", first, flags=re.IGNORECASE).strip()
        if first and _slug_to_loose(first) != team_slug_loose:
            return first

    return None


# ── FETCH ────────────────────────────────────────────────────────────────────

def get_team_stadium(team_slug: str, team_id: int, season: int) -> dict:
    """
    Descarga la ficha de estadio de un equipo en una temporada concreta.

    URL: /{team_slug}/stadion/verein/{team_id}/saison_id/{season}

    Devuelve un dict con todos los campos que TM publique para ese estadio.
    Si la página no se puede obtener, devuelve dict con sólo metadatos del
    equipo para que el orquestador pueda seguir.
    """
    url = (
        f"https://www.transfermarkt.es/{team_slug}/stadion"
        f"/verein/{team_id}/saison_id/{season}"
    )

    record: dict = {
        "team_slug":   team_slug,
        "team_id_tm":  team_id,
        "season":      season,
        "tm_url":      url,
    }

    r = request_with_retry(url)
    if not r:
        log.warning("Estadio: no se pudo obtener %s", url)
        return record

    soup = BeautifulSoup(r.text, "html.parser")

    # 1) Nombre del estadio.
    #    OJO: en la ficha /stadion/verein/{id} el <h1> de página contiene el
    #    nombre del CLUB, no del estadio. Por eso NO usamos h1. Intentamos
    #    primero estrategias dedicadas (enlace al estadio, subtítulo, <title>)
    #    y dejamos que la tabla de datos sobrescriba si trae un valor más fiable.
    name_candidate = _extract_stadium_name_from_page(soup, team_slug)
    if name_candidate:
        record["stadium_name"] = name_candidate

    # 2) Tabla(s) de datos del estadio.
    #    TM usa varias estructuras a lo largo del tiempo:
    #       - <table class="auflistung">  con filas <th>label</th><td>valor</td>
    #       - <div class="info-table">   con <span class="info-table__content--regular"> + sibling
    #       - <table class="profilheader"> con <th>/<td>
    #    Recorremos todas y aplicamos _LABEL_MAP.
    pairs: list[tuple[str, str]] = []

    # 2.a) tablas auflistung / profilheader
    for table in soup.find_all("table"):
        cls = " ".join(table.get("class") or [])
        if not any(c in cls for c in ("auflistung", "profilheader", "items")):
            continue
        for row in table.find_all("tr"):
            th = row.find(["th", "td"])
            cells = row.find_all(["th", "td"])
            if not th or len(cells) < 2:
                continue
            label = th.get_text(" ", strip=True)
            value = cells[-1].get_text(" ", strip=True)
            if label and value:
                pairs.append((label, value))

    # 2.b) info-table con spans (estructura nueva tipo perfil).
    #    Para 'Dirección' TM suele renderizar varios spans hermanos en líneas
    #    (nombre del estadio, calle, ciudad, país). Concatenamos TODOS los
    #    siblings span hasta el próximo label, separados por coma, para no
    #    quedarnos sólo con la primera línea (que históricamente provocaba
    #    que el nombre del estadio acabase como 'address').
    for label_span in soup.select("span.info-table__content--regular"):
        label = label_span.get_text(" ", strip=True)
        if not label:
            continue
        value_parts: list[str] = []
        sib = label_span.find_next_sibling()
        while sib is not None:
            sib_classes = sib.get("class") or []
            # Otro label → cerramos la lista de valores.
            if sib.name == "span" and "info-table__content--regular" in sib_classes:
                break
            if sib.name == "span":
                txt = sib.get_text(" ", strip=True)
                if txt:
                    value_parts.append(txt)
            sib = sib.find_next_sibling()
        value = ", ".join(value_parts).strip(", ").strip()
        if label and value:
            pairs.append((label, value))

    # 3) Volcado al record aplicando el mapa.
    #    - _OVERRIDE_KEYS (p.ej. stadium_name) sobreescriben SIEMPRE el valor
    #      previo: el heurístico del header puede haber metido algo distinto.
    #    - El resto usa setdefault para no pisar valores anteriores.
    for raw_label, raw_value in pairs:
        key = _LABEL_MAP.get(_normalize_label(raw_label))
        if not key:
            continue
        if key in _INT_FIELDS or key == "capacity_intl":
            new_val = _to_int(raw_value)
        elif key in _YEAR_FIELDS:
            new_val = _to_year(raw_value)
        elif key in _BOOL_FIELDS:
            new_val = raw_value.strip().lower() in (
                "sí", "si", "ja", "yes", "✓", "true",
            )
        else:
            new_val = raw_value.strip()
        if new_val in (None, ""):
            continue
        if key in _OVERRIDE_KEYS:
            record[key] = new_val
        else:
            record.setdefault(key, new_val)

    # 3.b) Post-procesado de campos compuestos.
    #      pitch_dimensions → pitch_length_m + pitch_width_m
    if "pitch_dimensions" in record:
        p_len, p_wid = _parse_pitch_dimensions(record.pop("pitch_dimensions"))
        record.setdefault("pitch_length_m", p_len)
        record.setdefault("pitch_width_m", p_wid)

    #      Extraer ID de estadio de TM desde el enlace /stadion/N
    stadium_tm_id = _extract_stadium_id_from_page(soup)
    if stadium_tm_id:
        record["id_transfermarkt_stadium"] = stadium_tm_id

    # 4) Validación: si stadium_name acabó coincidiendo con el team_slug
    #    (caso típico cuando TM no publica datos y solo había header), lo
    #    descartamos para que el caller no lo guarde como estadio válido.
    name = record.get("stadium_name")
    if name and _slug_to_loose(name) == _slug_to_loose(team_slug):
        log.warning(
            "Descartado stadium_name='%s' por coincidir con team_slug='%s' "
            "(probablemente TM no publicó datos para esta temporada).",
            name, team_slug,
        )
        record.pop("stadium_name", None)

    # 5) Ciudad/país a veces vienen embebidas en 'address'.
    if record.get("address") and not record.get("city"):
        # Ej: "C/ d'Arístides Maillol, s/n, 08028 Barcelona, España"
        parts = [p.strip() for p in record["address"].split(",") if p.strip()]
        if len(parts) >= 2:
            record["city"]    = record.get("city")    or parts[-2]
            record["country"] = record.get("country") or parts[-1]

    return record


# ── ORCHESTRATOR ─────────────────────────────────────────────────────────────

def scrape_transfermarkt_stadiums(
    competition_name: str,
    league_code: str,
    season: int,
    teams: Optional[dict[str, int]] = None,
    season_label: Optional[str] = None,
    full_refresh: bool = False,
) -> list[dict]:
    """
    Descarga los estadios de todos los equipos de una liga en una temporada.

    Args:
        competition_name: nombre humano (ej. "La Liga")
        league_code:      código TM (ej. "ES1")
        season:           año de inicio (ej. 2025)
        teams:            dict {slug: id}. Si None, se auto-descubren.
        season_label:     etiqueta para carpeta (ej. "2025_2026"). Si None se genera.
        full_refresh:     ignora caché de 30 días.

    Returns:
        Lista de dicts con datos crudos por equipo.
    """
    if season_label is None:
        season_label = f"{season}_{season + 1}"

    from utils.batch import generate_batch_id
    batch_id = generate_batch_id()

    from scripts.competitions import get_competition_slug_transfermarkt
    tm_slug = get_competition_slug_transfermarkt(competition_name)
    if not tm_slug:
        # Antes había un fallback silencioso a 'laliga' que provocaba
        # peticiones a la página equivocada (p.ej. al procesar Africa Cup
        # of Nations bajo el slug 'laliga'). Mejor abortar limpiamente.
        raise ValueError(
            f"No hay slug de Transfermarkt definido para '{competition_name}'. "
            "Añádelo a TRANSFERMARKT_COMPETITION_SLUGS en wizard/competitions.py "
            "o usa --competition con una liga soportada."
        )

    if not teams:
        teams_list = get_league_teams(season, tm_slug, league_code)
        teams = {t["team_slug"]: t["team_id"] for t in teams_list}
        log.info("Auto-descubiertos %d equipos para %s %d", len(teams), league_code, season)

    print("=" * 55)
    print(f"  Transfermarkt — Estadios — {league_code} {season_label}")
    print("=" * 55)

    # Todo el manejo de rutas vive en `utils.data_paths`.
    # raw  → data/raw/<comp>/<season>/transfermarkt/stadiums/<team>.json
    # clean → data/clean/<comp>/<season>/transfermarkt/stadiums.csv
    cache       = load_cache() if not full_refresh else {}
    today_str   = str(date.today())
    comp_slug   = slugify_competition(competition_name)
    all_stadia: list[dict] = []
    failed:     list[str]  = []
    skipped_teams = 0
    reused_from_other_comp = 0

    def _persist(record: dict) -> Path:
        record["batch_id"] = batch_id
        return save_raw_json(
            competition_name, season_label, "transfermarkt",
            team_slug, record, subdir="stadiums",
        )

    processed = 0
    try:
        for team_slug, team_id in teams.items():
            processed += 1
            # Clave de caché POR COMPETICIÓN para que el mismo equipo (p.ej. Real
            # Madrid) no sea omitido en La Liga porque ya se scrapeó en UCL.
            cache_key = f"{comp_slug}|{team_id}|{season}"
            last      = cache.get(cache_key)
            json_path = raw_dir(competition_name, season_label, "transfermarkt",
                                "stadiums") / f"{team_slug}.json"

            # Estadios cambian poco: caché 30 días, pero sólo vale si el JSON
            # físicamente existe en esta liga (si lo borraron, regeneramos).
            if not full_refresh and last:
                try:
                    from datetime import datetime as _dt
                    days = (date.today() - _dt.strptime(last, "%Y-%m-%d").date()).days
                    if days < 30 and json_path.exists():
                        try:
                            with open(json_path, "r", encoding="utf-8") as f:
                                all_stadia.append(json.load(f))
                            skipped_teams += 1
                            _checkpoint_stadiums(
                                cache, all_stadia, processed,
                                competition_name, season_label,
                            )
                            continue
                        except Exception as e:
                            log.warning("No se pudo leer JSON cacheado %s: %s", json_path, e)
                except Exception:
                    pass

            # Reuso cross-competición: si UCL/UEL ya scrapeó este equipo
            # recientemente, copiamos el JSON sin pegarle otra vez a TM.
            if not full_refresh:
                reused = find_recent_raw_json(
                    season_label, "transfermarkt", team_slug, subdir="stadiums",
                )
                if reused and reused.get("stadium_name"):
                    _persist(reused)
                    all_stadia.append(reused)
                    cache[cache_key] = today_str
                    reused_from_other_comp += 1
                    cap = reused.get("capacity")
                    print(f"\n[INFO] Estadio: {team_slug} (id={team_id}) "
                          f"— reutilizado de otra competición")
                    print(f"  [OK] {reused.get('stadium_name')} — aforo {cap}")
                    _checkpoint_stadiums(
                        cache, all_stadia, processed,
                        competition_name, season_label,
                    )
                    continue

            print(f"\n[INFO] Estadio: {team_slug} (id={team_id})")

            record = None
            for attempt in range(MAX_RETRIES):
                try:
                    record = get_team_stadium(team_slug, team_id, season)
                    if record and record.get("stadium_name"):
                        break
                except Exception as e:
                    log.warning("%s estadio intento %d: %s", team_slug, attempt + 1, e)
                time.sleep(2 * (attempt + 1))

            if not record or not record.get("stadium_name"):
                log.error("%s sin estadio extraído", team_slug)
                failed.append(team_slug)
                if record:
                    _persist(record)
                    all_stadia.append(record)
                time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))
                _checkpoint_stadiums(
                    cache, all_stadia, processed,
                    competition_name, season_label,
                )
                continue

            _persist(record)
            all_stadia.append(record)
            cache[cache_key] = today_str

            cap = record.get("capacity")
            print(f"  [OK] {record.get('stadium_name')} — aforo {cap}")

            time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))
            _checkpoint_stadiums(
                cache, all_stadia, processed,
                competition_name, season_label,
            )
    except KeyboardInterrupt:
        log.warning("Interrupted — saving progress (%d equipos)", len(all_stadia))
        _checkpoint_stadiums(
            cache, all_stadia, processed,
            competition_name, season_label, force=True,
        )
        raise
    finally:
        if processed and processed % CHECKPOINT_EVERY != 0:
            _checkpoint_stadiums(
                cache, all_stadia, processed,
                competition_name, season_label, force=True,
            )

    print(f"\n  Equipos procesados: {len(teams) - len(failed)}/{len(teams)}")
    if not full_refresh:
        print(f"  Omitidos por caché (<30 días): {skipped_teams}")
        if reused_from_other_comp:
            print(f"  Reutilizados de otra competición: {reused_from_other_comp}")
    if failed:
        print(f"  [WARNING] Fallidos: {failed}")

    if all_stadia:
        from utils.data_paths import clean_csv_path
        print(f"\n  CSV listo: {clean_csv_path(competition_name, season_label, 'transfermarkt', 'stadiums')}")

    return all_stadia


# ── TRANSFORM ────────────────────────────────────────────────────────────────

def transform_stadiums(stadiums_raw: list[dict]) -> pd.DataFrame:
    """
    Adapta la lista cruda a las columnas de `dim_stadium`.

    Columnas (propuesta para create_tables.sql):
        id_transfermarkt_team, team_slug, season,
        stadium_name, capacity, seats_covered, seats_vip,
        seats_standing, inaugurated_year, built_year,
        refurbished_year, owner, operator, address,
        city, country, construction_cost, surface,
        architect, tm_url
    """
    cols = [
        "team_id_tm", "team_slug", "season",
        "stadium_name", "capacity",
        "seats_total", "seats_covered", "seats_vip", "vip_boxes", "seats_standing",
        "inaugurated_year", "built_year", "refurbished_year",
        "owner", "operator", "address", "city", "country",
        "construction_cost", "surface", "architect",
        "tm_url",
        # v3 — nuevos campos del informe
        "previous_names_raw", "id_transfermarkt_stadium",
        "pitch_length_m", "pitch_width_m",
        "naming_rights", "naming_rights_until",
        "has_pitch_heating", "has_athletics_track", "capacity_intl",
    ]
    rows = [{c: s.get(c) for c in cols} for s in stadiums_raw]
    df = pd.DataFrame(rows, columns=cols)

    if not df.empty:
        df["team_id_tm"]       = pd.to_numeric(df["team_id_tm"],       errors="coerce").astype("Int64")
        for c in ("capacity", "seats_total", "seats_covered", "seats_vip",
                  "vip_boxes", "seats_standing"):
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")
        for c in ("inaugurated_year", "built_year", "refurbished_year"):
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int16")
        for c in ("pitch_length_m", "pitch_width_m"):
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int16")
        if "id_transfermarkt_stadium" in df.columns:
            df["id_transfermarkt_stadium"] = pd.to_numeric(
                df["id_transfermarkt_stadium"], errors="coerce"
            ).astype("Int64")
        if "capacity_intl" in df.columns:
            df["capacity_intl"] = pd.to_numeric(
                df["capacity_intl"], errors="coerce"
            ).astype("Int64")
        if "naming_rights_until" in df.columns:
            df["naming_rights_until"] = pd.to_numeric(
                df["naming_rights_until"], errors="coerce"
            ).astype("Int16")
        df = df.drop_duplicates(subset=["team_id_tm", "season"])
        df = df.sort_values(["season", "team_id_tm"]).reset_index(drop=True)

    return df


# ── INTEGRACIÓN CON dim_competition ─────────────────────────────────────────

def list_competitions_from_db() -> list[dict]:
    """
    Devuelve todas las competiciones registradas en `dim_competition` que
    tengan `id_transfermarkt` (league_code) informado.

    Cada item:
        {
            "canonical_name":   "La Liga",
            "country":          "Spain",
            "id_transfermarkt": "ES1",
        }

    Si la conexión a la BD falla (p.ej. en un entorno sin .env), cae
    silenciosamente al diccionario estático `COMPETITIONS` para no romper
    el flujo del wizard.
    """
    try:
        from sqlalchemy import text
        from loaders.common import engine
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT canonical_name, country, id_transfermarkt
                FROM dim_competition
                WHERE id_transfermarkt IS NOT NULL
                ORDER BY canonical_name
            """)).fetchall()
        return [
            {"canonical_name": r[0], "country": r[1], "id_transfermarkt": r[2]}
            for r in rows
        ]
    except Exception as e:
        log.warning("No se pudo leer dim_competition (%s). "
                    "Cayendo al diccionario estático.", e)
        try:
            from scripts.competitions import COMPETITIONS
            out = []
            for name, conf in COMPETITIONS.items():
                lc = conf.get("sources", {}).get("transfermarkt", {}).get("league_code")
                if lc:
                    out.append({
                        "canonical_name":   name,
                        "country":          conf.get("country"),
                        "id_transfermarkt": lc,
                    })
            return out
        except Exception:
            return []


def resolve_competition_from_db(name: str) -> Optional[dict]:
    """
    Busca una competición por nombre canónico en `dim_competition`.

    Devuelve dict con `canonical_name`, `country`, `id_transfermarkt`,
    o None si no existe.

    El match es case-insensitive y tolerante a espacios extra.
    """
    if not name:
        return None
    target = name.strip().lower()
    for comp in list_competitions_from_db():
        if comp["canonical_name"].strip().lower() == target:
            return comp
    return None


# ── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    """
    Punto de entrada CLI.

    Flujo:
      1. Si --list-db: muestra las competiciones disponibles en dim_competition.
      2. Si --competition se da, resuelve primero contra dim_competition
         (fuente de verdad) y si no la encuentra cae al dict COMPETITIONS.

    Ejemplos:
        python -m scrapers.transfermarkt_stadiums_scraper --list-db
        python -m scrapers.transfermarkt_stadiums_scraper \
            --competition "La Liga" --seasons 2024 2025
    """
    import argparse
    parser = argparse.ArgumentParser(
        description="Scraper de estadios de Transfermarkt por liga/temporada."
    )
    parser.add_argument("--competition",
                        help='Nombre canónico tal como aparece en dim_competition '
                             '(ej: "La Liga", "Premier League").')
    parser.add_argument("--seasons", nargs="+", type=int, default=[2025],
                        help="Años de inicio de temporada (ej: 2024 2025)")
    parser.add_argument("--full-refresh", action="store_true",
                        help="Ignora la caché de 30 días")
    parser.add_argument("--list-db", action="store_true",
                        help="Lista las competiciones disponibles en dim_competition y sale")
    parser.add_argument("--all-db", action="store_true",
                        help="Procesa EXACTAMENTE las competiciones que ofrece el wizard "
                             "(WORKING_COMPETITIONS en wizard/competitions.py). Las "
                             "selecciones nacionales se omiten automáticamente porque "
                             "no tienen estadios de club.")
    parser.add_argument("--include-non-working", action="store_true",
                        help="(deprecated) Mantenido por compatibilidad — ya no tiene "
                             "efecto, --all-db itera sólo el wizard.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Solo imprime qué competiciones se procesarían, sin scrapear.")

    args = parser.parse_args()
    # 1) Modo listado
    if args.list_db:
        comps = list_competitions_from_db()
        if not comps:
            print("[!] No se encontraron competiciones con id_transfermarkt en dim_competition.")
            return
        print(f"\n  Competiciones disponibles en dim_competition ({len(comps)}):")
        print("  " + "-" * 60)
        for c in comps:
            print(f"    [{c['id_transfermarkt']:>6}]  {c['canonical_name']:<35} "
                  f"({c.get('country') or '?'})")
        return

    # 2) Modo masivo: iterar EXACTAMENTE las competiciones del wizard.
    if args.all_db:
        # Fuente de verdad = wizard. Iteramos WORKING_COMPETITIONS y para cada
        # nombre usamos `league_code` del dict COMPETITIONS (en lugar del que
        # haya en dim_competition, que puede estar obsoleto — caso ECL→UCOL).
        from wizard.competitions import WORKING_COMPETITION_NAMES
        from scripts.competitions import (
            COMPETITIONS,
            get_competition_slug_transfermarkt,
        )

        # Las competiciones de selecciones no tienen estadios de clubes:
        # /stadion/verein/{team_id} es del CLUB, no de la federación. Las
        # saltamos automáticamente aunque estén en WORKING_COMPETITIONS.
        NATIONAL_TEAM_COMPS = {
            "FIFA World Cup",
            "European Championship",
            "Copa America",
            "UEFA Women's EURO",
            "FIFA Women's World Cup",
            "UEFA Nations League A",
            "UEFA Nations League B",
            "UEFA Nations League C",
            "UEFA Nations League D",
            "World Cup Qualification UEFA",
            "World Cup Qualification CONMEBOL",
            "Int. Friendly",
            "Africa Cup of Nations",
            "Asian Cup",
        }

        # Para avisar de desalineaciones entre wizard y dim_competition
        db_codes = {c["canonical_name"]: c["id_transfermarkt"]
                    for c in list_competitions_from_db()}

        # Mantener el orden del wizard (Ligas nacionales -> continentales -> ...)
        seen = set()
        ordered_names = []
        from wizard.competitions import WORKING_COMPETITIONS
        for _bucket, names in WORKING_COMPETITIONS.items():
            for n in names:
                if n not in seen:
                    seen.add(n)
                    ordered_names.append(n)

        to_process, skipped = [], []
        for name in ordered_names:
            reasons = []
            cfg = COMPETITIONS.get(name, {})
            league_code = (cfg.get("sources", {})
                              .get("transfermarkt", {})
                              .get("league_code"))
            if not league_code:
                reasons.append("sin league_code TM en wizard/competitions.py")
            if not get_competition_slug_transfermarkt(name):
                reasons.append("sin slug TM en TRANSFERMARKT_COMPETITION_SLUGS")
            if name in NATIONAL_TEAM_COMPS:
                reasons.append("seleccion nacional (sin estadios de clubes)")

            if reasons:
                skipped.append((name, "; ".join(reasons)))
                continue

            # Aviso si dim_competition tiene un codigo distinto al del wizard
            db_code = db_codes.get(name)
            if db_code and db_code != league_code:
                log.warning(
                    "league_code de %s difiere: wizard='%s' vs dim_competition='%s'. "
                    "Uso el del wizard.",
                    name, league_code, db_code,
                )
            to_process.append({"canonical_name": name, "league_code": league_code})

        # Resumen
        print("\n" + "=" * 70)
        print(f"  Modo --all-db (wizard): {len(to_process)} competicion(es) a procesar, "
              f"{len(skipped)} omitida(s)")
        print("=" * 70)
        if to_process:
            print("\n  Se procesaran:")
            for c in to_process:
                print(f"    [OK]   {c['canonical_name']:<35} [{c['league_code']}]")
        if skipped:
            print("\n  Se omiten:")
            for name, why in skipped:
                print(f"    [SKIP] {name:<35}  ({why})")

        if args.dry_run:
            print("\n[dry-run] No se ejecuta scraping.")
            return

        if not to_process:
            print("\n[!] Nada que procesar.")
            return

        for c in to_process:
            for season_year in args.seasons:
                print(f"\n-> {c['canonical_name']} ({c['league_code']}) -- {season_year}")
                try:
                    scrape_transfermarkt_stadiums(
                        competition_name=c["canonical_name"],
                        league_code=c["league_code"],
                        season=season_year,
                        full_refresh=args.full_refresh,
                    )
                except Exception as e:
                    log.error("Fallo %s %d: %s", c["canonical_name"], season_year, e)
        return

    # 3) Modo competicion concreta
    if not args.competition:
        parser.error("Debes pasar --competition, --list-db o --all-db")

    comp_db = resolve_competition_from_db(args.competition)
    if comp_db:
        league_code = comp_db["id_transfermarkt"]
        comp_name   = comp_db["canonical_name"]
        log.info("Competicion resuelta via dim_competition: %s -> %s",
                 comp_name, league_code)
    else:
        from scripts.competitions import get_competition
        comp_config = get_competition(args.competition)
        if not comp_config:
            print(f"Error: Competicion '{args.competition}' no encontrada "
                  f"ni en dim_competition ni en COMPETITIONS.")
            return
        league_code = comp_config["sources"]["transfermarkt"]["league_code"]
        comp_name   = args.competition
        log.info("Competicion resuelta via COMPETITIONS estatico: %s -> %s",
                 comp_name, league_code)

    for season_year in args.seasons:
        scrape_transfermarkt_stadiums(
            competition_name=comp_name,
            league_code=league_code,
            season=season_year,
            full_refresh=args.full_refresh,
        )


if __name__ == "__main__":
    main()
