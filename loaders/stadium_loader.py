"""
loaders/stadium_loader.py
=========================
Carga dim_stadium desde los CSV producidos por
`scrapers/transfermarkt_stadiums_scraper.py`, usando modelo SCD2:
una fila por ESTADO del estadio, no por temporada.

Para cada (team, season, data) entrante:
  1. Se calcula data_hash (SHA1 de los campos comparables).
  2. Si ya existe una fila para ese equipo con el mismo hash, se EXTIENDE
     su rango [valid_from_season, valid_to_season] para cubrir esta temporada.
  3. Si no existe, se inserta una fila nueva con valid_from = valid_to = season.
  4. Si una fila distinta ya cubre esta temporada (conflicto retroactivo),
     se loguea un warning y no se toca nada — preserva la historia consistente.

Uso CLI:
    python -m loaders.stadium_loader
    python -m loaders.stadium_loader --competition la-liga --season 2025_2026
"""

from __future__ import annotations

import argparse
import hashlib
import json
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
from utils.stadium_name_history import resolve_name as _resolve_historical_name

log = logging.getLogger(__name__)

# Columnas que el CSV trae y que también se comparan vía hash
_DATA_FIELDS = [
    "stadium_name", "capacity",
    "seats_total", "seats_covered", "seats_vip", "vip_boxes", "seats_standing",
    "inaugurated_year", "built_year", "refurbished_year",
    "owner", "operator", "address", "city", "country",
    "construction_cost", "surface", "architect",
]

# Columnas leídas del CSV — `previous_names_raw` se usa para reescribir
# `stadium_name` según la temporada antes de calcular el hash SCD2.
_STADIUM_COLS = [
    "team_id_tm", "team_slug", "season",
    *_DATA_FIELDS, "tm_url",
    "previous_names_raw",
]


def _rewrite_stadium_name_for_season(row: dict) -> dict:
    """Reemplaza ``stadium_name`` por el nombre histórico de la temporada.

    Lee ``previous_names_raw`` y ``season`` de la fila y, si hay historial,
    devuelve una copia con ``stadium_name`` ajustado a lo que el estadio
    se llamaba en esa temporada. Si no hay historial o la temporada cae
    fuera de las eras, el nombre actual se mantiene.

    Esto hace que la lógica SCD2 produzca una fila distinta por cada
    nombre histórico (porque el hash cambia con stadium_name).
    """
    raw = _to_py(row.get("previous_names_raw"))
    if not raw:
        return row
    current = _to_py(row.get("stadium_name"))
    season  = row.get("season")
    new_name = _resolve_historical_name(current, raw, season)
    if new_name and new_name != current:
        # Hacemos copia superficial para no mutar el dict original
        out = dict(row)
        out["stadium_name"] = new_name
        return out
    return row


# ── Helpers ──────────────────────────────────────────────────────────────────

def _to_py(value):
    """Convierte NaN/pd.NA → None y numpy.int → int nativo."""
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            pass
    return value


def _compute_data_hash(data: dict) -> str:
    """SHA1 estable de los campos comparables del estadio."""
    payload = {k: _to_py(data.get(k)) for k in _DATA_FIELDS}
    s = json.dumps(payload, sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


def _resolve_canonical_team_id(conn, team_id_tm) -> Optional[int]:
    """Busca el canonical_id de dim_team a partir de id_transfermarkt."""
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
    """Normaliza '2025', '2025_2026', '25/26' → '2025/2026'."""
    if season_value is None:
        return None
    s = str(season_value).strip().replace("_", "/")
    if not s:
        return None
    if s.isdigit() and len(s) == 4:
        return f"{s}/{int(s) + 1}"
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


# ── SCD2 core ────────────────────────────────────────────────────────────────

_INSERT_SQL = text("""
    INSERT INTO dim_stadium (
        canonical_team_id, id_transfermarkt_team, team_slug,
        valid_from_season, valid_to_season,
        stadium_name, capacity,
        seats_total, seats_covered, seats_vip, vip_boxes, seats_standing,
        inaugurated_year, built_year, refurbished_year,
        owner, operator, address, city, country,
        construction_cost, surface, architect,
        tm_url, data_hash, data_source, updated_at
    ) VALUES (
        :canonical_team_id, :id_transfermarkt_team, :team_slug,
        :valid_from_season, :valid_to_season,
        :stadium_name, :capacity,
        :seats_total, :seats_covered, :seats_vip, :vip_boxes, :seats_standing,
        :inaugurated_year, :built_year, :refurbished_year,
        :owner, :operator, :address, :city, :country,
        :construction_cost, :surface, :architect,
        :tm_url, :data_hash, 'transfermarkt', NOW()
    )
""")


def _upsert_stadium_scd2(conn, row: dict) -> str:
    """SCD2 upsert: una fila por estado.

    Devuelve: "noop" | "extended" | "inserted" | "conflict" | "skipped".
    """
    tm_id = _to_py(row.get("team_id_tm"))
    if tm_id is None:
        return "skipped"
    season = _normalize_season(row.get("season"))
    if not season:
        return "skipped"

    tm_id = int(tm_id)
    h = _compute_data_hash(row)

    # 1) ¿Existe ya una fila con el mismo equipo + mismo hash?
    existing = conn.execute(text("""
        SELECT stadium_id, valid_from_season, valid_to_season
        FROM dim_stadium
        WHERE id_transfermarkt_team = :tid AND data_hash = :h
        ORDER BY valid_from_season
        LIMIT 1
    """), {"tid": tm_id, "h": h}).fetchone()

    if existing:
        new_from = min(existing.valid_from_season, season)
        new_to   = max(existing.valid_to_season,   season)
        if (new_from, new_to) == (existing.valid_from_season, existing.valid_to_season):
            return "noop"
        conn.execute(text("""
            UPDATE dim_stadium
            SET valid_from_season = :nf, valid_to_season = :nt, updated_at = NOW()
            WHERE stadium_id = :id
        """), {"nf": new_from, "nt": new_to, "id": existing.stadium_id})
        return "extended"

    # 2) ¿Hay OTRA fila (hash distinto) que cubre esta temporada?
    conflict = conn.execute(text("""
        SELECT stadium_id, valid_from_season, valid_to_season
        FROM dim_stadium
        WHERE id_transfermarkt_team = :tid
          AND valid_from_season <= :s AND valid_to_season >= :s
        LIMIT 1
    """), {"tid": tm_id, "s": season}).fetchone()

    if conflict:
        log.warning(
            "SCD2 conflict tm_id=%s season=%s: ya existe fila %s con "
            "rango [%s, %s] y datos distintos. No se inserta para preservar "
            "la historia. Si los datos nuevos son correctos, ejecuta "
            "scripts/compact_dim_stadium.py --force o repara manualmente.",
            tm_id, season, conflict.stadium_id,
            conflict.valid_from_season, conflict.valid_to_season,
        )
        return "conflict"

    # 3) Insertar nueva fila SCD2 con rango [season, season]
    canonical_team_id = _resolve_canonical_team_id(conn, tm_id)
    params = {
        "canonical_team_id":     canonical_team_id,
        "id_transfermarkt_team": tm_id,
        "team_slug":             _to_py(row.get("team_slug")),
        "valid_from_season":     season,
        "valid_to_season":       season,
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
        "data_hash":             h,
    }
    conn.execute(_INSERT_SQL, params)
    return "inserted"


# ── Entrada principal ───────────────────────────────────────────────────────

def load_stadiums(
    conn,
    tm_path: Optional[Path] = None,
    competition: Optional[str] = None,
    season: Optional[str] = None,
) -> int:
    """Carga dim_stadium (SCD2) desde los CSV del scraper.

    Returns: número total de filas afectadas (inserted + extended).
    """
    clean_root = tm_path if tm_path is not None else CLEAN_ROOT
    if not clean_root.exists():
        log.warning("stadium_loader: no existe %s", clean_root)
        return 0

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
    if not files:
        log.warning("stadium_loader: no se encontraron CSVs (%s) bajo %s",
                    pattern, clean_root)
        return 0

    log.info("[START] Cargando dim_stadium (SCD2) desde %d CSV(s)…", len(files))
    counts = {"inserted": 0, "extended": 0, "noop": 0, "conflict": 0, "skipped": 0}

    for f in files:
        df = safe_read_csv(f)
        if df is None or df.empty:
            continue
        for c in _STADIUM_COLS:
            if c not in df.columns:
                df[c] = None

        for _, row in df.iterrows():
            sp = conn.begin_nested()
            try:
                # Reescribe stadium_name segun la temporada (SCD2 por nombre)
                row_dict = _rewrite_stadium_name_for_season(row.to_dict())
                action = _upsert_stadium_scd2(conn, row_dict)
                sp.commit()
                counts[action] = counts.get(action, 0) + 1
            except Exception as e:
                sp.rollback()
                counts["skipped"] += 1
                log.error("Error upserteando estadio (%s, %s): %s",
                          row.get("team_slug"), row.get("season"), e)

        try:
            rel_path = f.relative_to(clean_root)
        except ValueError:
            rel_path = f
        log.info("  + %s — %d filas procesadas", rel_path, len(df))

    log.info(
        "[OK] dim_stadium SCD2 — insertadas=%d, extendidas=%d, noop=%d, "
        "conflict=%d, skipped=%d",
        counts["inserted"], counts["extended"], counts["noop"],
        counts["conflict"], counts["skipped"],
    )
    return counts["inserted"] + counts["extended"]


# ── CLI ──────────────────────────────────────────────────────────────────────

def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
        datefmt="%H:%M:%S",
    )
    parser = argparse.ArgumentParser(
        description="Loader SCD2 de dim_stadium desde CSVs de Transfermarkt."
    )
    parser.add_argument("--path", type=Path,
                        help="Raíz alternativa para data/clean")
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
    print(f"\nFilas afectadas en dim_stadium: {total}")


if __name__ == "__main__":
    main()
