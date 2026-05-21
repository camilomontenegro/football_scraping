"""
loaders/stadium_loader.py
=========================
Carga `dim_stadium` desde los CSV producidos por
`scrapers/transfermarkt_stadiums_scraper.py`.

Patrón homólogo al resto de loaders:
    - Lee CSV(s) `transfermarkt_stadiums.csv` bajo data/raw/transfermarkt/...
    - Resuelve el equipo canónico (dim_team) usando id_transfermarkt como
      llave principal (es la fuente que conoce TM).
    - UPSERT contra dim_stadium por la clave única (id_transfermarkt_team, season).

Uso programático:
    from loaders.stadium_loader import load_stadiums
    with engine.begin() as conn:
        load_stadiums(conn, tm_path=Path("data/raw/transfermarkt"))

Uso CLI:
    python -m loaders.stadium_loader
    python -m loaders.stadium_loader --path data/raw/transfermarkt
    python -m loaders.stadium_loader --competition la-liga --season 2025_2026
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import Optional

import pandas as pd
from sqlalchemy import text

from loaders.common import engine, safe_read_csv
from utils.data_paths import (
    CLEAN_ROOT,
    slugify_competition,
    normalize_season,
)

log = logging.getLogger(__name__)

# Columnas esperadas en el CSV producido por el scraper
_STADIUM_COLS = [
    "team_id_tm", "team_slug", "season",
    "stadium_name", "capacity",
    "seats_total", "seats_covered", "seats_vip", "vip_boxes", "seats_standing",
    "inaugurated_year", "built_year", "refurbished_year",
    "owner", "operator", "address", "city", "country",
    "construction_cost", "surface", "architect",
    "tm_url",
]


# ── Helpers ──────────────────────────────────────────────────────────────────

def _to_py(value):
    """Convierte NaN/pd.NA → None para que SQLAlchemy lo trate como NULL."""
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    # numpy ints → ints nativos
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            pass
    return value


def _resolve_canonical_team_id(conn, team_id_tm) -> Optional[int]:
    """Busca el canonical_id de dim_team a partir de id_transfermarkt.

    Devuelve None si el equipo no existe en dim_team todavía. En ese caso
    el registro de estadio se inserta igualmente pero con FK nula, para
    que se pueda resolver más adelante (cuando el team_loader corra).
    """
    if team_id_tm is None:
        return None
    try:
        row = conn.execute(
            text("SELECT canonical_id FROM dim_team WHERE id_transfermarkt = :tid LIMIT 1"),
            {"tid": int(team_id_tm)},
        ).fetchone()
        return row[0] if row else None
    except Exception as e:
        log.warning("No se pudo resolver dim_team para tm_id=%s: %s", team_id_tm, e)
        return None


def _normalize_season(season_value) -> Optional[str]:
    """
    Convierte distintos formatos de temporada a la forma canónica '2025/2026'.

    Acepta:
        2025             -> '2025/2026'
        '2025'           -> '2025/2026'
        '2025_2026'      -> '2025/2026'
        '2025/2026'      -> '2025/2026'
        '25/26'          -> '2025/2026'
    """
    if season_value is None:
        return None
    s = str(season_value).strip().replace("_", "/")
    if not s:
        return None
    # Sólo año de inicio: '2025'
    if s.isdigit() and len(s) == 4:
        return f"{s}/{int(s) + 1}"
    # '25/26' → '2025/2026'
    if "/" in s:
        parts = s.split("/")
        if len(parts) == 2 and all(p.isdigit() for p in parts):
            a, b = parts
            if len(a) == 2:
                a = "20" + a
            if len(b) == 2:
                b = "20" + b
            return f"{a}/{b}"
    return s


# ── UPSERT ──────────────────────────────────────────────────────────────────

_UPSERT_SQL = text("""
    INSERT INTO dim_stadium (
        canonical_team_id, id_transfermarkt_team, team_slug, season,
        stadium_name, capacity, seats_total, seats_covered, seats_vip,
        vip_boxes, seats_standing,
        inaugurated_year, built_year, refurbished_year,
        owner, operator, address, city, country,
        construction_cost, surface, architect, tm_url,
        data_source, updated_at
    ) VALUES (
        :canonical_team_id, :id_transfermarkt_team, :team_slug, :season,
        :stadium_name, :capacity, :seats_total, :seats_covered, :seats_vip,
        :vip_boxes, :seats_standing,
        :inaugurated_year, :built_year, :refurbished_year,
        :owner, :operator, :address, :city, :country,
        :construction_cost, :surface, :architect, :tm_url,
        'transfermarkt', NOW()
    )
    ON CONFLICT (id_transfermarkt_team, season) DO UPDATE SET
        canonical_team_id = COALESCE(dim_stadium.canonical_team_id, EXCLUDED.canonical_team_id),
        team_slug         = COALESCE(EXCLUDED.team_slug,         dim_stadium.team_slug),
        stadium_name      = COALESCE(EXCLUDED.stadium_name,      dim_stadium.stadium_name),
        capacity          = COALESCE(EXCLUDED.capacity,          dim_stadium.capacity),
        seats_total       = COALESCE(EXCLUDED.seats_total,       dim_stadium.seats_total),
        seats_covered     = COALESCE(EXCLUDED.seats_covered,     dim_stadium.seats_covered),
        seats_vip         = COALESCE(EXCLUDED.seats_vip,         dim_stadium.seats_vip),
        vip_boxes         = COALESCE(EXCLUDED.vip_boxes,         dim_stadium.vip_boxes),
        seats_standing    = COALESCE(EXCLUDED.seats_standing,    dim_stadium.seats_standing),
        inaugurated_year  = COALESCE(EXCLUDED.inaugurated_year,  dim_stadium.inaugurated_year),
        built_year        = COALESCE(EXCLUDED.built_year,        dim_stadium.built_year),
        refurbished_year  = COALESCE(EXCLUDED.refurbished_year,  dim_stadium.refurbished_year),
        owner             = COALESCE(EXCLUDED.owner,             dim_stadium.owner),
        operator          = COALESCE(EXCLUDED.operator,          dim_stadium.operator),
        address           = COALESCE(EXCLUDED.address,           dim_stadium.address),
        city              = COALESCE(EXCLUDED.city,              dim_stadium.city),
        country           = COALESCE(EXCLUDED.country,           dim_stadium.country),
        construction_cost = COALESCE(EXCLUDED.construction_cost, dim_stadium.construction_cost),
        surface           = COALESCE(EXCLUDED.surface,           dim_stadium.surface),
        architect         = COALESCE(EXCLUDED.architect,         dim_stadium.architect),
        tm_url            = COALESCE(EXCLUDED.tm_url,            dim_stadium.tm_url),
        updated_at        = NOW()
""")


def _upsert_stadium_row(conn, row: dict) -> bool:
    """UPSERT de una fila de estadio. Devuelve True si se insertó/actualizó."""
    tm_id = _to_py(row.get("team_id_tm"))
    if tm_id is None:
        log.debug("Fila sin team_id_tm, ignorada: %s", row.get("team_slug"))
        return False

    season = _normalize_season(row.get("season"))
    if not season:
        log.debug("Fila sin season válido, ignorada: tm_id=%s", tm_id)
        return False

    canonical_team_id = _resolve_canonical_team_id(conn, tm_id)

    params = {
        "canonical_team_id":     canonical_team_id,
        "id_transfermarkt_team": int(tm_id),
        "team_slug":             _to_py(row.get("team_slug")),
        "season":                season,
        "stadium_name":          _to_py(row.get("stadium_name")),
        "capacity":              _to_py(row.get("capacity")),
        "seats_total":           _to_py(row.get("seats_total")),
        "seats_covered":         _to_py(row.get("seats_covered")),
        "seats_vip":             _to_py(row.get("seats_vip")),
        "vip_boxes":             _to_py(row.get("vip_boxes")),
        "seats_standing":        _to_py(row.get("seats_standing")),
        "inaugurated_year":      _to_py(row.get("inaugurated_year")),
        "built_year":            _to_py(row.get("built_year")),
        "refurbished_year":      _to_py(row.get("refurbished_year")),
        "owner":                 _to_py(row.get("owner")),
        "operator":              _to_py(row.get("operator")),
        "address":               _to_py(row.get("address")),
        "city":                  _to_py(row.get("city")),
        "country":               _to_py(row.get("country")),
        "construction_cost":     _to_py(row.get("construction_cost")),
        "surface":               _to_py(row.get("surface")),
        "architect":             _to_py(row.get("architect")),
        "tm_url":                _to_py(row.get("tm_url")),
    }
    conn.execute(_UPSERT_SQL, params)
    return True


# ── Punto de entrada ─────────────────────────────────────────────────────────

def load_stadiums(
    conn,
    tm_path: Optional[Path] = None,
    competition: Optional[str] = None,
    season: Optional[str] = None,
) -> int:
    """
    Carga dim_stadium desde los CSV producidos por el scraper de estadios.

    Lee de la nueva estructura:
        data/clean/<comp_slug>/<season>/transfermarkt/stadiums.csv

    Args:
        conn:         conexión SQLAlchemy (en transacción).
        tm_path:      raíz `data/clean`. Por defecto se infiere desde el árbol
                      del proyecto. Mantiene el nombre `tm_path` por
                      compatibilidad con llamadas existentes.
        competition:  filtra a una sola competición. Acepta tanto el slug
                      ('la_liga') como el nombre humano ('La Liga') —
                      se normaliza con slugify_competition().
        season:       filtra a una sola temporada. Acepta '2025_2026',
                      '2025/2026', '2025' etc.

    Returns:
        Número de filas upserteadas.
    """
    clean_root = tm_path if tm_path is not None else CLEAN_ROOT

    if not clean_root.exists():
        log.warning("stadium_loader: no existe %s", clean_root)
        return 0

    # Normalizar filtros
    comp_slug = slugify_competition(competition) if competition else None
    season_lbl = normalize_season(season) if season else None

    if comp_slug and season_lbl:
        pattern = f"{comp_slug}/{season_lbl}/transfermarkt/stadiums.csv"
    elif comp_slug:
        pattern = f"{comp_slug}/*/transfermarkt/stadiums.csv"
    elif season_lbl:
        pattern = f"*/{season_lbl}/transfermarkt/stadiums.csv"
    else:
        pattern = "*/*/transfermarkt/stadiums.csv"

    files = sorted(clean_root.glob(pattern))

    # Compatibilidad hacia atrás: si no encontramos nada con la nueva
    # estructura, intentamos la antigua (data/raw/transfermarkt/<comp>/
    # season=<label>/transfermarkt_stadiums.csv) para que loaders viejos
    # sigan funcionando hasta migrar.
    if not files:
        legacy_root = clean_root.parent / "raw" / "transfermarkt"
        if legacy_root.exists():
            log.info("stadium_loader: no hay CSVs en la nueva estructura, "
                     "intentando layout legacy en %s", legacy_root)
            if competition and season:
                legacy_pattern = f"{competition}/season={season}/transfermarkt_stadiums.csv"
            elif competition:
                legacy_pattern = f"{competition}/season=*/transfermarkt_stadiums.csv"
            elif season:
                legacy_pattern = f"*/season={season}/transfermarkt_stadiums.csv"
            else:
                legacy_pattern = "**/transfermarkt_stadiums.csv"
            files = sorted(legacy_root.glob(legacy_pattern))

    if not files:
        log.warning("stadium_loader: no se encontraron CSVs (%s) bajo %s",
                    pattern, clean_root)
        return 0

    log.info("[START] Cargando dim_stadium desde %d CSV(s)…", len(files))
    total = 0
    for f in files:
        df = safe_read_csv(f)
        if df is None or df.empty:
            continue

        # Garantizar todas las columnas (las que falten quedan NaN)
        for c in _STADIUM_COLS:
            if c not in df.columns:
                df[c] = None

        for _, row in df.iterrows():
            try:
                if _upsert_stadium_row(conn, row.to_dict()):
                    total += 1
            except Exception as e:
                log.error("Error upserteando estadio (%s, %s): %s",
                          row.get("team_slug"), row.get("season"), e)

        try:
            rel_path = f.relative_to(clean_root)
        except ValueError:
            rel_path = f
        log.info("  ✓ %s — %d filas procesadas", rel_path, len(df))

    log.info("[OK] dim_stadium completado — %d upserts", total)
    return total


# ── CLI ──────────────────────────────────────────────────────────────────────

def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
        datefmt="%H:%M:%S",
    )
    parser = argparse.ArgumentParser(
        description="Loader de dim_stadium desde CSVs de Transfermarkt."
    )
    parser.add_argument("--path", type=Path,
                        help="Raíz alternativa para data/raw/transfermarkt")
    parser.add_argument("--competition", help="Slug de competición (ej: la-liga)")
    parser.add_argument("--season", help="Temporada en formato carpeta (ej: 2025_2026)")
    args = parser.parse_args()

    with engine.begin() as conn:
        total = load_stadiums(
            conn,
            tm_path=args.path,
            competition=args.competition,
            season=args.season,
        )
    print(f"\nFilas upserteadas en dim_stadium: {total}")


if __name__ == "__main__":
    main()
