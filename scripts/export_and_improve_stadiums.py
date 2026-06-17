"""
Exporta dim_stadium a CSV, mejora filas con datos incompletos o incorrectos,
y vuelve a exportar con un log de cambios.

Fuentes: overrides JSON, Wikipedia URL, WhoScored, Wikidata/Nominatim.

Uso:
    python -m scripts.export_and_improve_stadiums --dry-run
    python -m scripts.export_and_improve_stadiums
    python -m scripts.export_and_improve_stadiums --skip-wikidata
"""

from __future__ import annotations

import argparse
import csv
import logging
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import unquote, urlparse

from sqlalchemy import text

from loaders.common import engine
from scrapers.wikidata_stadium_enricher import (
    STADIUM_OVERRIDES_PATH,
    _entity_label,
    _fetch_entity,
    _infer_country_from_team,
    _load_cache,
    _name_looks_like_club,
    _save_cache,
    enrich_stadium,
)
from scripts.repair_dim_stadium import fix_names_from_wikipedia

log = logging.getLogger(__name__)

REPORTS = Path(__file__).resolve().parents[1] / "reports"
EXPORT_COLS = [
    "stadium_id", "team_slug", "canonical_team_id", "id_transfermarkt_team",
    "team_name", "stadium_name", "valid_from_season", "valid_to_season",
    "capacity", "city", "country", "address", "surface", "built_year",
    "latitude", "longitude", "timezone", "altitude_m",
    "wikidata_qid", "wikipedia_url", "image_url", "tm_url",
    "data_source", "data_hash", "quality_issues",
]

_STADIUM_WORDS = re.compile(
    r"\b(stadium|estadio|arena|park|field|ground|stadion|stade|stadio|"
    r"metropolitano|complexo|stadionul)\b",
    re.I,
)
_AUTO_GEO = re.compile(r"\(auto geocoded\)", re.I)


def _wikipedia_title(url: str | None) -> str | None:
    if not url:
        return None
    path = urlparse(url).path.rstrip("/")
    slug = unquote(path.split("/")[-1]).replace("_", " ").strip()
    return slug if slug and slug.lower() not in ("wiki", "wikipedia") else None


def _needs_wikidata(issues: list[str], row: dict) -> bool:
    """Solo llamar API cuando el dato es crítico o el nombre está mal."""
    critical = {
        "sin_coords", "nombre_auto_geocode", "nombre_parece_club",
        "nombre_igual_equipo", "sin_nombre",
    }
    if critical & set(issues):
        return True
    if "sin_ciudad" in issues and row.get("data_source") == "synthetic-geocode":
        return True
    return False


def _quality_issues(row: dict) -> list[str]:
    issues: list[str] = []
    name = (row.get("stadium_name") or "").strip()
    team = (row.get("team_name") or row.get("team_slug") or "").strip()

    if not name:
        issues.append("sin_nombre")
    elif _AUTO_GEO.search(name):
        issues.append("nombre_auto_geocode")
    elif _name_looks_like_club(name, team):
        issues.append("nombre_parece_club")
    elif not _STADIUM_WORDS.search(name) and name.lower() == team.lower():
        issues.append("nombre_igual_equipo")

    if not row.get("city"):
        issues.append("sin_ciudad")
    if not row.get("country"):
        issues.append("sin_pais")
    if row.get("latitude") is None or row.get("longitude") is None:
        issues.append("sin_coords")
    if not row.get("wikidata_qid"):
        issues.append("sin_wikidata")
    if not row.get("capacity") and row.get("data_source") == "transfermarkt":
        issues.append("sin_aforo_tm")
    if not row.get("image_url"):
        issues.append("sin_imagen")
    return issues


def _fetch_all(conn) -> list[dict]:
    rows = conn.execute(text("""
        SELECT s.*, t.canonical_name AS team_name, t.country AS team_country
        FROM dim_stadium s
        LEFT JOIN dim_team t ON t.canonical_id = s.canonical_team_id
        ORDER BY s.team_slug, s.valid_from_season, s.stadium_id
    """)).mappings().all()
    out = []
    for r in rows:
        d = dict(r)
        d["quality_issues"] = ";".join(_quality_issues(d))
        out.append(d)
    return out


def export_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=EXPORT_COLS, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k) for k in EXPORT_COLS})


def _best_stadium_label(qid: str | None, enrich_data: dict) -> str | None:
    if qid:
        ent = _fetch_entity(qid)
        if ent:
            lbl = _entity_label(ent)
            if lbl:
                return lbl
    return None


def _improve_row(row: dict, cache: dict, use_wikidata: bool, dry_run: bool) -> dict | None:
    """Devuelve dict de columnas a actualizar o None si no hay cambios."""
    team = row.get("team_name") or row.get("team_slug") or ""
    name = (row.get("stadium_name") or "").strip()
    updates: dict = {}
    issues = _quality_issues(row)

    if not issues:
        return None

    # País desde dim_team
    if "sin_pais" in issues and row.get("team_country"):
        updates["country"] = row["team_country"]
    elif "sin_pais" in issues and team:
        inferred = _infer_country_from_team(team, row.get("country") or "")
        if inferred:
            updates["country"] = inferred

    # Nombre desde Wikipedia URL
    if any(x in issues for x in ("nombre_parece_club", "nombre_igual_equipo")):
        wiki_name = _wikipedia_title(row.get("wikipedia_url"))
        if wiki_name and _STADIUM_WORDS.search(wiki_name):
            updates["stadium_name"] = wiki_name

    enrich_data: dict = {}
    if use_wikidata and not dry_run and _needs_wikidata(issues, row):
        enrich_data = enrich_stadium(
            {
                "stadium_name": updates.get("stadium_name", name),
                "team": team,
                "team_slug": row.get("team_slug"),
                "wikidata_qid": row.get("wikidata_qid"),
                "city": row.get("city") or updates.get("city"),
                "country": updates.get("country") or row.get("country"),
                "address": row.get("address"),
            },
            cache=cache,
            require_coords="sin_coords" in issues,
            require_image=False,
            use_wikidata=True,
        )
    elif dry_run and _needs_wikidata(issues, row):
        updates["_would_call_wikidata"] = True

    for col in (
        "latitude", "longitude", "timezone", "altitude_m",
        "wikidata_qid", "wikipedia_url", "image_url", "architect", "operator",
    ):
        val = enrich_data.get(col)
        if val not in (None, "") and row.get(col) in (None, ""):
            updates[col] = val

    # Nombre desde etiqueta Wikidata
    name_issues = {"nombre_auto_geocode", "nombre_parece_club", "nombre_igual_equipo", "sin_nombre"}
    if issues and name_issues & set(issues):
        qid = updates.get("wikidata_qid") or enrich_data.get("wikidata_qid") or row.get("wikidata_qid")
        label = _best_stadium_label(qid, enrich_data)
        if label and not _name_looks_like_club(label, team):
            updates["stadium_name"] = label

    if not updates:
        return None

    if dry_run:
        return updates

    set_clause = ", ".join(f"{k} = :{k}" for k in updates)
    with engine.begin() as conn:
        conn.execute(
            text(f"UPDATE dim_stadium SET {set_clause}, updated_at = NOW() WHERE stadium_id = :id"),
            {**updates, "id": row["stadium_id"]},
        )
    return updates


def cheap_bulk_fixes(dry_run: bool = False) -> int:
    """Rellena país desde dim_team y nombres desde Wikipedia sin API externa."""
    n = 0
    with engine.begin() as conn:
        if not dry_run:
            r = conn.execute(text("""
                UPDATE dim_stadium ds
                SET country = COALESCE(NULLIF(TRIM(ds.country), ''), t.country),
                    updated_at = NOW()
                FROM dim_team t
                WHERE t.canonical_id = ds.canonical_team_id
                  AND (ds.country IS NULL OR TRIM(ds.country) = '')
                  AND t.country IS NOT NULL AND TRIM(t.country) <> ''
            """))
            n += r.rowcount or 0
        n += fix_names_from_wikipedia(conn, dry_run=dry_run)
    return n


def count_wikidata_candidates(rows: list[dict]) -> int:
    return sum(
        1 for r in rows
        if r.get("quality_issues") and _needs_wikidata(_quality_issues(r), r)
    )


def improve_all(use_wikidata: bool = True, dry_run: bool = False) -> list[dict]:
    changes: list[dict] = []
    cache = _load_cache()

    with engine.connect() as conn:
        rows = _fetch_all(conn)

    flagged = [r for r in rows if r.get("quality_issues")]
    log.info("Filas con incidencias: %d / %d", len(flagged), len(rows))

    for i, row in enumerate(flagged, 1):
        if i % 25 == 0:
            _save_cache(cache)
            log.info("Progreso %d/%d", i, len(flagged))

        try:
            upd = _improve_row(row, cache, use_wikidata, dry_run)
        except Exception as exc:
            log.warning("stadium_id=%s error: %s", row["stadium_id"], exc)
            continue

        if upd:
            changes.append({
                "stadium_id": row["stadium_id"],
                "team_slug": row["team_slug"],
                "old_name": row.get("stadium_name"),
                "issues": row.get("quality_issues"),
                **{f"new_{k}": v for k, v in upd.items()},
            })
            time.sleep(0.05)

    _save_cache(cache)
    return changes


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s — %(message)s",
        datefmt="%H:%M:%S",
    )
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--skip-wikidata", action="store_true")
    ap.add_argument("--skip-preflight", action="store_true",
                    help="No ejecutar repair/override antes de mejorar.")
    args = ap.parse_args()

    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    before_path = REPORTS / f"dim_stadium_before_{ts}.csv"
    after_path = REPORTS / f"dim_stadium_after_{ts}.csv"
    changes_path = REPORTS / f"dim_stadium_changes_{ts}.csv"
    latest_before = REPORTS / "dim_stadium_latest.csv"

    with engine.connect() as conn:
        before_rows = _fetch_all(conn)
    export_csv(before_path, before_rows)
    export_csv(latest_before, before_rows)
    flagged = sum(1 for r in before_rows if r.get("quality_issues"))
    print(f"Exportado: {before_path} ({len(before_rows)} filas, {flagged} con incidencias)")

    if not args.skip_preflight and not args.dry_run:
        import subprocess
        root = str(REPORTS.parent)
        print("\n── Preflight: overrides + WhoScored ──")
        subprocess.run([sys.executable, "-m", "scripts.apply_stadium_overrides"], check=False, cwd=root)
        subprocess.run([sys.executable, "-m", "scripts.fix_stadium_names"], check=False, cwd=root)

    print("\n── Correcciones locales (país, Wikipedia) ──")
    n_cheap = cheap_bulk_fixes(dry_run=args.dry_run)
    print(f"  Filas tocadas (bulk): {n_cheap}")

    with engine.connect() as conn:
        mid_rows = _fetch_all(conn)
    wikidata_n = count_wikidata_candidates(mid_rows)
    print(f"\n── Mejora fila a fila ({wikidata_n} consultas Wikidata estimadas) ──")
    changes = improve_all(use_wikidata=not args.skip_wikidata, dry_run=args.dry_run)

    with engine.connect() as conn:
        after_rows = _fetch_all(conn)
    export_csv(after_path, after_rows)
    export_csv(REPORTS / "dim_stadium_latest.csv", after_rows)

    if changes:
        with changes_path.open("w", newline="", encoding="utf-8") as f:
            keys = sorted({k for c in changes for k in c})
            w = csv.DictWriter(f, fieldnames=keys, extrasaction="ignore")
            w.writeheader()
            w.writerows(changes)
        print(f"Cambios: {changes_path} ({len(changes)} filas tocadas)")
    else:
        print("Sin cambios automáticos.")

    after_flagged = sum(1 for r in after_rows if r.get("quality_issues"))
    print(f"\nExportado: {after_path}")
    print(f"Incidencias: {flagged} → {after_flagged}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
