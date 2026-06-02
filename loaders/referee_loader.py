"""
loaders/referee_loader.py
=========================
Carga dim_referee + UPDATE dim_match.referee_id desde los CSV de Sofascore.

Lee data/clean/<comp>/<season>/sofascore/matches.csv que (tras el patch del
scraper) ahora trae las columnas:
    referee_id_ss, referee_name, referee_country.

Flujo:
  1. UPSERT en dim_referee por id_sofascore (clave natural).
  2. UPDATE en dim_match.referee_id usando JOIN por id_sofascore (match).

Idempotente. Re-correr no duplica filas.

Uso:
    python -m loaders.referee_loader
    python -m loaders.referee_loader --competition la-liga --season 2024_2025
"""
from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import Optional

import pandas as pd
from sqlalchemy import text

from loaders.common import engine, safe_read_csv
from utils.data_paths import CLEAN_ROOT, slugify_competition, normalize_season

log = logging.getLogger(__name__)


_UPSERT_REF = text("""
    INSERT INTO dim_referee (canonical_name, country, id_sofascore, data_source, updated_at)
    VALUES (:name, :country, :ssid, 'sofascore', NOW())
    ON CONFLICT (id_sofascore) DO UPDATE
    SET canonical_name = EXCLUDED.canonical_name,
        country        = COALESCE(EXCLUDED.country, dim_referee.country),
        updated_at     = NOW()
    RETURNING referee_id
""")


def _to_int(v) -> Optional[int]:
    if v is None:
        return None
    try:
        if pd.isna(v):
            return None
    except (TypeError, ValueError):
        pass
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _to_str(v) -> Optional[str]:
    if v is None:
        return None
    try:
        if pd.isna(v):
            return None
    except (TypeError, ValueError):
        pass
    s = str(v).strip()
    return s or None


def load_referees(
    conn,
    tm_path: Optional[Path] = None,
    competition: Optional[str] = None,
    season: Optional[str] = None,
) -> int:
    """Carga dim_referee y enlaza dim_match. Devuelve nº matches actualizados."""
    clean_root = tm_path or CLEAN_ROOT
    if not clean_root.exists():
        log.warning("referee_loader: no existe %s", clean_root)
        return 0

    comp_slug = slugify_competition(competition) if competition else None
    season_lbl = normalize_season(season) if season else None

    if comp_slug and season_lbl:
        pattern = f"{comp_slug}/{season_lbl}/sofascore/matches.csv"
    elif comp_slug:
        pattern = f"{comp_slug}/*/sofascore/matches.csv"
    elif season_lbl:
        pattern = f"*/{season_lbl}/sofascore/matches.csv"
    else:
        pattern = "*/*/sofascore/matches.csv"

    files = sorted(clean_root.glob(pattern))
    if not files:
        log.warning("referee_loader: sin CSVs (%s) bajo %s", pattern, clean_root)
        return 0

    log.info("[START] Cargando dim_referee desde %d CSV(s)...", len(files))
    n_upserts = n_matches_updated = n_skipped = 0

    for f in files:
        df = safe_read_csv(f)
        if df is None or df.empty:
            continue
        # Cols mínimas
        for c in ("id_sofascore", "referee_id_ss", "referee_name", "referee_country"):
            if c not in df.columns:
                df[c] = None

        for _, row in df.iterrows():
            ssid = _to_int(row.get("referee_id_ss"))
            rname = _to_str(row.get("referee_name"))
            match_id_ss = _to_int(row.get("id_sofascore"))
            if not ssid or not rname or not match_id_ss:
                n_skipped += 1
                continue

            sp = conn.begin_nested()
            try:
                # 1) UPSERT del árbitro
                ref_row = conn.execute(_UPSERT_REF, {
                    "name":    rname,
                    "country": _to_str(row.get("referee_country")),
                    "ssid":    ssid,
                }).fetchone()
                referee_db_id = ref_row[0] if ref_row else None
                if not referee_db_id:
                    sp.rollback()
                    n_skipped += 1
                    continue

                # 2) ENLAZAR partido en dim_match por id_sofascore
                upd = conn.execute(text("""
                    UPDATE dim_match
                    SET referee_id = :rid
                    WHERE id_sofascore = :mid
                      AND (referee_id IS NULL OR referee_id <> :rid)
                """), {"rid": referee_db_id, "mid": match_id_ss})
                n_matches_updated += upd.rowcount or 0
                n_upserts += 1
                sp.commit()
            except Exception as e:
                sp.rollback()
                n_skipped += 1
                log.error("Error referee match=%s ref_ss=%s: %s", match_id_ss, ssid, e)

        try:
            rel = f.relative_to(clean_root)
        except ValueError:
            rel = f
        log.info("  + %s -- %d filas", rel, len(df))

    log.info(
        "[OK] dim_referee -- upserts=%d, matches_actualizados=%d, skipped=%d",
        n_upserts, n_matches_updated, n_skipped,
    )
    return n_matches_updated


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s -- %(message)s",
        datefmt="%H:%M:%S",
    )
    ap = argparse.ArgumentParser(description="Loader de dim_referee (Sofascore).")
    ap.add_argument("--path", type=Path, help="Raíz alternativa para data/clean")
    ap.add_argument("--competition", help="Slug de competición (ej: la-liga)")
    ap.add_argument("--season", help="Temporada (ej: 2024_2025)")
    args = ap.parse_args()

    with engine.begin() as conn:
        total = load_referees(
            conn, tm_path=args.path,
            competition=args.competition, season=args.season,
        )
    print(f"\nMatches con referee actualizados: {total}")


if __name__ == "__main__":
    main()
