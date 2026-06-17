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
  4. Si una fila distinta ya cubre esta temporada (hash distinto), se parte el rango:
     cierra la fila antigua, inserta la nueva y conserva cola con datos previos.

Uso CLI:
    python -m loaders.stadium_loader
    python -m loaders.stadium_loader --competition la-liga --season 2025_2026
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import re
import unicodedata
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
    "seats_total", "vip_boxes", "built_year",
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
    original_name = _to_py(row.get("stadium_name"))
    out = dict(row)
    if raw:
        current = original_name
        season = row.get("season")
        new_name = _resolve_historical_name(current, raw, season)
        if new_name and new_name != current:
            out["stadium_name"] = new_name
    return _sanitize_naming_rights_fields(out, original_name)


_SPONSOR_LIKE = re.compile(
    r"^(jp financial|reale\b|spotify|riyadh air|wanda|civitas|allianz|"
    r"emirates|etihad|signal iduna|red bull|prezero|mewa|wwk|hitachi|"
    r"philips|deutsche bank|nou camp)$",
    re.I,
)
_SPONSOR_TOKEN = re.compile(
    r"\b(financial|capital|air\b|spotify|emirates|etihad|mewa|wwk|hitachi|"
    r"yanmar|vitality|allwyn|cetilar|nordic wellness|ontime|abanca|ryadh|jp\b)\b",
    re.I,
)
_VENUE_WORD = re.compile(
    r"\b(stadium|estadio|arena|stadion|stade|stadio|park|ground|metropolitano|"
    r"völlur|vollur|stadionul|campo|field)\b",
    re.I,
)


def _looks_like_sponsor_label(value: str | None) -> bool:
    """True cuando TM mete el patrocinador de naming rights, no una dirección."""
    if not value:
        return False
    s = value.strip()
    if not s or len(s) > 80:
        return False
    if re.search(r"\d{3,}", s):
        return False
    if "," in s or re.search(r"\b(calle|c/|av\.|pl\.|street|road|s/n)\b", s, re.I):
        return False
    if _SPONSOR_LIKE.match(s):
        return True
    if re.search(r"[®™]", s):
        return True
    if len(s.split()) <= 4 and not _VENUE_WORD.search(s) and _SPONSOR_TOKEN.search(s):
        return True
    return False


_POSTAL_CITY_PREFIX = re.compile(
    r"^(?:[A-Z]{1,4}[\s\-]?)?[\d][\d\s\-–]*",
    re.UNICODE,
)


def parse_city_from_address(address: str | None) -> str | None:
    """Extrae ciudad del último segmento útil de una dirección postal."""
    addr = (address or "").strip()
    if not addr or len(addr) < 3:
        return None
    parts = [p.strip() for p in addr.split(",") if p.strip()]
    if not parts:
        return None

    for candidate in reversed(parts):
        c = candidate.strip()
        if not c or re.fullmatch(r"\d+", c):
            continue
        cleaned = _POSTAL_CITY_PREFIX.sub("", c).strip() or c
        if len(cleaned) < 2:
            continue
        if city_looks_invalid(cleaned, None, addr):
            continue
        if re.search(r"\d", cleaned) and not _POSTAL_CITY_PREFIX.search(c):
            continue
        return cleaned
    return None


def city_looks_invalid(city: str | None, stadium_name: str | None, address: str | None) -> bool:
    """Detecta ciudad claramente errónea (nombre del estadio, patrocinador, etc.)."""
    c = (city or "").strip()
    if not c:
        return False
    name = (stadium_name or "").strip()
    addr = (address or "").strip()
    if name and c.lower() == name.lower():
        return True
    if addr and c.lower() == addr.lower() and not re.search(r"\d{3,}", addr):
        return True
    if c.lower().startswith(("stade ", "estadio ", "stadion ")):
        return True
    if _VENUE_WORD.search(c) and not re.search(r"\d", c):
        return True
    if _looks_like_sponsor_label(c):
        return True
    return False


def address_looks_valid(
    address: str | None,
    stadium_name: str | None = None,
    city: str | None = None,
) -> bool:
    """True cuando address parece calle/postal, no nombre del estadio."""
    addr = (address or "").strip()
    if not addr or len(addr) < 6:
        return False
    name = (stadium_name or "").strip()
    c = (city or "").strip()
    if name and addr.lower() == name.lower():
        return False
    if c and addr.lower() == c.lower():
        return False
    if _looks_like_sponsor_label(addr):
        return False
    if city_looks_invalid(addr, name, None):
        return False
    norm = unicodedata.normalize("NFKD", addr)
    norm_ascii = "".join(ch for ch in norm if not unicodedata.combining(ch))
    if _VENUE_WORD.search(norm_ascii) and not re.search(r"\d", addr):
        return False
    if re.search(r"\d", addr) or "," in addr:
        return True
    if re.search(
        r"\b(calle|c/|av\.|avenue|pl\.|plaza|street|road|rue|via|straße|str\.|"
        r"weg|s/n|paseo|boulevard|bvd|lane|drive|way|boulevard)\b",
        norm_ascii,
        re.I,
    ):
        return True
    return len(norm_ascii.split()) >= 4 and not _VENUE_WORD.search(norm_ascii)


def _sanitize_naming_rights_fields(row: dict, original_stadium_name: str | None) -> dict:
    """Limpia address/city cuando TM copia el naming rights del estadio."""
    out = dict(row)
    sponsor = original_stadium_name
    for field in ("address", "city"):
        val = _to_py(out.get(field))
        if not val:
            continue
        if val == sponsor or val == _to_py(out.get("stadium_name")):
            out[field] = None
        elif field == "address" and _looks_like_sponsor_label(val):
            out[field] = None
        elif field == "city" and city_looks_invalid(val, out.get("stadium_name"), out.get("address")):
            out[field] = None
    return out


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


_POLISH_CHARS = str.maketrans({"ł": "l", "Ł": "L"})

# Slugs TM en dim_stadium → slug derivado de dim_team.canonical_name
_STADIUM_TEAM_SLUG_ALIASES: dict[str, str] = {
    "apoel-nikosia": "apoel-nicosia",
    "ac-pisa-1909": "pisa",
    "fc-astana": "astana",
    "fc-pyunik-erewan": "pyunik-yerevan",
    "sk-sigma-olmutz": "sk-sigma-olomouc",
    "sk-dnipro-1": "sc-dnipro-1",
    "spartak-trnava": "fc-spartak-trnava",
    "ki-klaksvik": "klaksvikar-itrottarfelag",
    "hamrun-spartans": "hamrun-spartans",
    "fc-lausanne-sport": "lausanne-sport",
    "fc-nordsjaelland": "fc-nordsjaelland",
    "fc-basel-1893": "basel",
}


def slugify_team_name(name: str) -> str:
    """Normaliza canonical_name → slug comparable con team_slug TM."""
    if not name:
        return ""
    s = name.translate(_POLISH_CHARS)
    nfkd = unicodedata.normalize("NFKD", s)
    ascii_str = "".join(c for c in nfkd if not unicodedata.combining(c))
    return re.sub(r"[^a-z0-9]+", "-", ascii_str.lower()).strip("-")


def _slug_matches(stadium_slug: str, team_slug: str) -> bool:
    if not stadium_slug or not team_slug:
        return False
    if stadium_slug == team_slug:
        return True
    return (
        stadium_slug.startswith(team_slug + "-")
        or team_slug.startswith(stadium_slug + "-")
    )


def _build_team_slug_index(conn) -> dict[str, int]:
    rows = conn.execute(text(
        "SELECT canonical_id, canonical_name FROM dim_team"
    )).fetchall()
    index: dict[str, int] = {}
    for cid, name in rows:
        slug = slugify_team_name(name or "")
        if slug and slug not in index:
            index[slug] = int(cid)
    return index


def resolve_canonical_team_id_by_slug(conn, team_slug: str | None) -> Optional[int]:
    """Resuelve dim_team.canonical_id comparando slug TM con canonical_name."""
    if not team_slug:
        return None
    slug = _STADIUM_TEAM_SLUG_ALIASES.get(team_slug.strip(), team_slug.strip())
    index = _build_team_slug_index(conn)
    if slug in index:
        return index[slug]
    for team_slug_key, cid in index.items():
        if _slug_matches(slug, team_slug_key):
            return cid
    return None


def _resolve_canonical_team_id(conn, team_id_tm, team_slug: str | None = None) -> Optional[int]:
    """Busca el canonical_id de dim_team por id_transfermarkt o team_slug."""
    if team_id_tm is not None:
        try:
            row = conn.execute(
                text("SELECT canonical_id FROM dim_team WHERE id_transfermarkt = :tid LIMIT 1"),
                {"tid": int(team_id_tm)},
            ).fetchone()
            if row:
                return row[0]
        except Exception as e:
            log.warning("No se pudo resolver dim_team para tm_id=%s: %s", team_id_tm, e)
    return resolve_canonical_team_id_by_slug(conn, team_slug)


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


def _prev_season(season: str) -> Optional[str]:
    parts = season.split("/")
    if len(parts) != 2 or not all(p.isdigit() for p in parts):
        return None
    a, b = int(parts[0]), int(parts[1])
    if a <= 1900:
        return None
    return f"{a - 1}/{b - 1}"


def _next_season(season: str) -> Optional[str]:
    parts = season.split("/")
    if len(parts) != 2 or not all(p.isdigit() for p in parts):
        return None
    a, b = int(parts[0]), int(parts[1])
    return f"{a + 1}/{b + 1}"


def _row_insert_params(row: dict, tm_id: int, season_from: str, season_to: str, h: str,
                        conn, canonical_team_id: Optional[int] = None) -> dict:
    return {
        "canonical_team_id":     canonical_team_id if canonical_team_id is not None
                                 else _resolve_canonical_team_id(
                                     conn, tm_id, _to_py(row.get("team_slug")),
                                 ),
        "id_transfermarkt_team": tm_id,
        "team_slug":             _to_py(row.get("team_slug")),
        "valid_from_season":     season_from,
        "valid_to_season":       season_to,
        "stadium_name":          _to_py(row.get("stadium_name")),
        "capacity":              _to_py(row.get("capacity")),
        "seats_total":           _to_py(row.get("seats_total")),
        "vip_boxes":             _to_py(row.get("vip_boxes")),
        "built_year":            _to_py(row.get("built_year")),
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


def _fetch_stadium_row(conn, stadium_id: int) -> dict:
    row = conn.execute(text("""
        SELECT canonical_team_id, id_transfermarkt_team, team_slug,
               stadium_name, capacity, seats_total, vip_boxes, built_year,
               owner, operator, address, city, country, construction_cost,
               surface, architect, tm_url, data_hash
        FROM dim_stadium WHERE stadium_id = :id
    """), {"id": stadium_id}).mappings().one()
    return dict(row)


def _insert_from_db_row(conn, db_row: dict, season_from: str, season_to: str) -> None:
    params = {
        **{k: db_row.get(k) for k in (
            "canonical_team_id", "id_transfermarkt_team", "team_slug",
            "stadium_name", "capacity", "seats_total", "vip_boxes", "built_year",
            "owner", "operator", "address", "city", "country", "construction_cost",
            "surface", "architect", "tm_url", "data_hash",
        )},
        "valid_from_season": season_from,
        "valid_to_season": season_to,
    }
    conn.execute(_INSERT_SQL, params)


def _split_scd2_on_conflict(conn, row: dict, tm_id: int, season: str, h: str,
                            conflict) -> str:
    """Parte una fila existente cuando la temporada entrante tiene hash distinto."""
    f, t = conflict.valid_from_season, conflict.valid_to_season
    ex_id = conflict.stadium_id

    if f == t == season:
        params = _row_insert_params(row, tm_id, season, season, h, conn)
        params["id"] = ex_id
        set_clause = ", ".join(
            f"{k} = :{k}" for k in params if k not in ("id", "valid_from_season", "valid_to_season")
        )
        conn.execute(text(f"""
            UPDATE dim_stadium SET {set_clause}, updated_at = NOW() WHERE stadium_id = :id
        """), params)
        return "updated"

    old = _fetch_stadium_row(conn, ex_id)
    prev_s = _prev_season(season)
    next_s = _next_season(season)

    if season == f:
        if next_s and next_s <= t:
            conn.execute(text("""
                UPDATE dim_stadium
                SET valid_from_season = :nf, updated_at = NOW()
                WHERE stadium_id = :id
            """), {"nf": next_s, "id": ex_id})
        else:
            conn.execute(text("DELETE FROM dim_stadium WHERE stadium_id = :id"), {"id": ex_id})
    elif prev_s and prev_s >= f:
        conn.execute(text("""
            UPDATE dim_stadium
            SET valid_to_season = :nt, updated_at = NOW()
            WHERE stadium_id = :id
        """), {"nt": prev_s, "id": ex_id})
    else:
        conn.execute(text("DELETE FROM dim_stadium WHERE stadium_id = :id"), {"id": ex_id})

    conn.execute(_INSERT_SQL, _row_insert_params(row, tm_id, season, season, h, conn))

    if next_s and next_s <= t and season < t:
        _insert_from_db_row(conn, old, next_s, t)

    log.info(
        "SCD2 split tm_id=%s season=%s: fila %s partida [%s..%s] → nuevo estado en %s",
        tm_id, season, ex_id, f, t, season,
    )
    return "split"


# ── SCD2 core ────────────────────────────────────────────────────────────────

_INSERT_SQL = text("""
    INSERT INTO dim_stadium (
        canonical_team_id, id_transfermarkt_team, team_slug,
        valid_from_season, valid_to_season,
        stadium_name, capacity,
        seats_total, vip_boxes, built_year,
        owner, operator, address, city, country,
        construction_cost, surface, architect,
        tm_url, data_hash, data_source, updated_at
    ) VALUES (
        :canonical_team_id, :id_transfermarkt_team, :team_slug,
        :valid_from_season, :valid_to_season,
        :stadium_name, :capacity,
        :seats_total, :vip_boxes, :built_year,
        :owner, :operator, :address, :city, :country,
        :construction_cost, :surface, :architect,
        :tm_url, :data_hash, 'transfermarkt', NOW()
    )
""")


def _upsert_stadium_scd2(conn, row: dict) -> str:
    """SCD2 upsert: una fila por estado.

    Devuelve: "noop" | "extended" | "inserted" | "updated" | "split" | "skipped".
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
        return _split_scd2_on_conflict(conn, row, tm_id, season, h, conflict)

    # 3) Insertar nueva fila SCD2 con rango [season, season]
    conn.execute(_INSERT_SQL, _row_insert_params(row, tm_id, season, season, h, conn))
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

    def _season_sort_key(p: Path) -> tuple[str, str]:
        season_part = next(
            (x for x in p.parts if re.fullmatch(r"\d{4}_\d{4}", x)), "0000_0000"
        )
        return season_part, str(p)

    files = sorted(clean_root.glob(pattern), key=_season_sort_key)
    if not files:
        log.warning("stadium_loader: no se encontraron CSVs (%s) bajo %s",
                    pattern, clean_root)
        return 0

    log.info("[START] Cargando dim_stadium (SCD2) desde %d CSV(s)…", len(files))
    counts = {"inserted": 0, "extended": 0, "updated": 0, "split": 0, "noop": 0, "skipped": 0}

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
        "[OK] dim_stadium SCD2 — insertadas=%d, extendidas=%d, actualizadas=%d, "
        "partidas=%d, noop=%d, skipped=%d",
        counts["inserted"], counts["extended"], counts.get("updated", 0),
        counts.get("split", 0), counts["noop"], counts["skipped"],
    )
    return (
        counts["inserted"] + counts["extended"]
        + counts.get("updated", 0) + counts.get("split", 0)
    )


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
