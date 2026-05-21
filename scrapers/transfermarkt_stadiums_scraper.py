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
# TM puede usar variantes; normalizamos a minúsculas sin acentos
_LABEL_MAP = {
    "nombre del estadio":      "stadium_name",
    "estadio":                 "stadium_name",
    "aforo total":             "capacity",
    "capacidad total":         "capacity",
    "capacidad":               "capacity",
    "asientos":                "seats_total",
    "asientos cubiertos":      "seats_covered",
    "asientos vip":            "seats_vip",
    "palcos":                  "vip_boxes",
    "plazas de pie":           "seats_standing",
    "inaugurado":              "inaugurated_year",
    "inauguracion":            "inaugurated_year",
    "construido":              "built_year",
    "construccion":            "built_year",
    "reformado":               "refurbished_year",
    "reforma":                 "refurbished_year",
    "propietario":             "owner",
    "operador":                "operator",
    "direccion":               "address",
    "ciudad":                  "city",
    "pais":                    "country",
    "coste de construccion":   "construction_cost",
    "coste construccion":      "construction_cost",
    "superficie":              "surface",
    "cesped":                  "surface",
    "arquitecto":              "architect",
}

_INT_FIELDS = {"capacity", "seats_total", "seats_covered", "seats_vip",
               "vip_boxes", "seats_standing"}
_YEAR_FIELDS = {"inaugurated_year", "built_year", "refurbished_year"}


def _normalize_label(s: str) -> str:
    """Minúsculas, sin acentos, sin signos finales (':')."""
    if not s:
        return ""
    s = s.strip().rstrip(":").lower()
    repl = {"á":"a","é":"e","í":"i","ó":"o","ú":"u","ü":"u","ñ":"n"}
    for k, v in repl.items():
        s = s.replace(k, v)
    return s


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

    # 1) Nombre del estadio: suele estar en el header de página o en h1
    #    Header moderno: <h1 class="data-header__headline-wrapper"> con el nombre dentro.
    #    Fallback: primer h1 visible, o título del bloque.
    name_node = (
        soup.select_one("h1.data-header__headline-wrapper")
        or soup.select_one("div.dataMain h1")
        or soup.select_one("h1")
    )
    if name_node:
        # Limpia spans internos (números/etiquetas) y se queda con el texto del estadio
        name_txt = " ".join(name_node.stripped_strings)
        # Quita prefijos comunes ("Estadio de:", el nombre del club, etc.)
        name_txt = re.sub(r"^\s*estadio[:\s\-]*", "", name_txt, flags=re.IGNORECASE).strip()
        if name_txt:
            record["stadium_name"] = name_txt

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

    # 2.b) info-table con spans (estructura nueva tipo perfil)
    for label_span in soup.select("span.info-table__content--regular"):
        value_span = label_span.find_next_sibling("span")
        if not value_span:
            continue
        label = label_span.get_text(" ", strip=True)
        value = value_span.get_text(" ", strip=True)
        if label and value:
            pairs.append((label, value))

    # 3) Volcado al record aplicando el mapa
    for raw_label, raw_value in pairs:
        key = _LABEL_MAP.get(_normalize_label(raw_label))
        if not key:
            continue
        if key in _INT_FIELDS:
            record.setdefault(key, _to_int(raw_value))
        elif key in _YEAR_FIELDS:
            record.setdefault(key, _to_year(raw_value))
        else:
            record.setdefault(key, raw_value.strip())

    # 4) Ciudad/país a veces vienen embebidas en 'address'
    if "address" in record and "city" not in record:
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
    tm_slug = get_competition_slug_transfermarkt(competition_name) or "laliga"

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

    for team_slug, team_id in teams.items():
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
            continue

        _persist(record)
        all_stadia.append(record)
        cache[cache_key] = today_str

        cap = record.get("capacity")
        print(f"  [OK] {record.get('stadium_name')} — aforo {cap}")

        time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))
        save_cache(cache)

    print(f"\n  Equipos procesados: {len(teams) - len(failed)}/{len(teams)}")
    if not full_refresh:
        print(f"  Omitidos por caché (<30 días): {skipped_teams}")
        if reused_from_other_comp:
            print(f"  Reutilizados de otra competición: {reused_from_other_comp}")
    if failed:
        print(f"  [WARNING] Fallidos: {failed}")

    save_cache(cache)

    if all_stadia:
        df = transform_stadiums(all_stadia)
        out_csv = save_clean_csv(
            competition_name, season_label, "transfermarkt",
            "stadiums", df,
        )
        print(f"\n  CSV listo: {out_csv}")

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
                        help="Procesa TODAS las competiciones de dim_competition con id_transfermarkt")

    args = parser.parse_args()

    # 1) Modo listado
    if args.list_db:
        comps = list_competitions_from_db()
        if not comps:
            print("[!] No se encontraron competiciones con id_transfermarkt en dim_competition.")
            return
        print(f"\n  Competiciones disponibles en dim_competition ({len(comps)}):")
        print("  " + "─" * 60)
        for c in comps:
            print(f"    [{c['id_transfermarkt']:>6}]  {c['canonical_name']:<35} "
                  f"({c.get('country') or '?'})")
        return

    # 2) Modo masivo
    if args.all_db:
        comps = list_competitions_from_db()
        if not comps:
            print("[!] No hay competiciones en dim_competition.")
            return
        for c in comps:
            for season_year in args.seasons:
                print(f"\n→ {c['canonical_name']} ({c['id_transfermarkt']}) — {season_year}")
                try:
                    scrape_transfermarkt_stadiums(
                        competition_name=c["canonical_name"],
                        league_code=c["id_transfermarkt"],
                        season=season_year,
                        full_refresh=args.full_refresh,
                    )
                except Exception as e:
                    log.error("Falló %s %d: %s", c["canonical_name"], season_year, e)
        return

    # 3) Modo competición concreta
    if not args.competition:
        parser.error("Debes pasar --competition, --list-db o --all-db")

    comp_db = resolve_competition_from_db(args.competition)
    if comp_db:
        league_code = comp_db["id_transfermarkt"]
        comp_name   = comp_db["canonical_name"]
        log.info("Competición resuelta vía dim_competition: %s → %s",
                 comp_name, league_code)
    else:
        # Fallback al diccionario estático
        from scripts.competitions import get_competition
        comp_config = get_competition(args.competition)
        if not comp_config:
            print(f"Error: Competición '{args.competition}' no encontrada "
                  f"ni en dim_competition ni en COMPETITIONS.")
            return
        league_code = comp_config["sources"]["transfermarkt"]["league_code"]
        comp_name   = args.competition
        log.info("Competición resuelta vía COMPETITIONS estático: %s → %s",
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
