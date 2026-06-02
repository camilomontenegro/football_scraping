"""
scripts/import_understat_teams_zip.py
=====================================
Importa un ZIP con mapeos Understat team_id -> team_name y lo escribe al layout
canónico del repo en:

  data/clean/<competition_slug>/2025_2026/understat/teams.csv

Esperado en el ZIP (nombres típicos):
  understat_teams_laliga.csv
  understat_teams_premier_league.csv
  understat_teams_bundesliga.csv
  understat_teams_ligue_1.csv
  understat_teams_serie_a.csv

Cada CSV del ZIP puede venir como:
  team_id,team_name

El script normaliza a:
  understat_team_id,team_name

Uso:
  python -m scripts.import_understat_teams_zip --zip "C:\\Users\\Ivan\\Desktop\\understat_teams.zip"
  python -m scripts.import_understat_teams_zip --zip "..." --season 2025_2026
  python -m scripts.import_understat_teams_zip --zip "..." --dry-run
"""

from __future__ import annotations

import argparse
import csv
import sys
import zipfile
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from utils.data_paths import clean_csv_path  # noqa: E402


SEASON_DEFAULT = "2025_2026"

ZIP_TO_COMP = {
    "laliga": "la_liga",
    "premier_league": "premier_league",
    "bundesliga": "bundesliga",
    "ligue_1": "ligue_1",
    "serie_a": "serie_a",
}


def _parse_comp_from_filename(name: str) -> str | None:
    stem = Path(name).stem.lower()
    for key in ZIP_TO_COMP:
        if stem.endswith(key) or f"_{key}_" in stem or stem.startswith(f"understat_teams_{key}"):
            return ZIP_TO_COMP[key]
    return None


def _read_zip_csv(zf: zipfile.ZipFile, member: str) -> list[dict]:
    with zf.open(member) as fh:
        # zipfile da bytes -> decode
        text = fh.read().decode("utf-8-sig", errors="replace").splitlines()
    reader = csv.DictReader(text)
    return list(reader)


def _normalize_rows(rows: list[dict]) -> list[dict]:
    out: list[dict] = []
    for r in rows:
        us_id = r.get("understat_team_id") or r.get("team_id") or r.get("id") or r.get("teamId")
        name = r.get("team_name") or r.get("name") or r.get("team")
        if us_id is None or name is None:
            continue
        try:
            us_id_int = int(str(us_id).strip())
        except Exception:
            continue
        name = str(name).strip()
        if not name:
            continue
        out.append({"understat_team_id": us_id_int, "team_name": name})
    # dedup por id (keep last)
    dedup: dict[int, dict] = {}
    for r in out:
        dedup[int(r["understat_team_id"])] = r
    return list(dedup.values())


def _merge_existing(path: Path, rows_new: list[dict]) -> list[dict]:
    if not path.exists():
        return rows_new
    try:
        existing = path.read_text(encoding="utf-8").splitlines()
        reader = csv.DictReader(existing)
        rows_old = _normalize_rows(list(reader))
    except Exception:
        rows_old = []

    merged: dict[int, dict] = {int(r["understat_team_id"]): r for r in rows_old}
    for r in rows_new:
        merged[int(r["understat_team_id"])] = r
    return list(merged.values())


def _write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    rows_sorted = sorted(rows, key=lambda r: int(r["understat_team_id"]))
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=["understat_team_id", "team_name"])
        w.writeheader()
        w.writerows(rows_sorted)


def main() -> int:
    p = argparse.ArgumentParser(description="Import understat team mappings from zip into data/clean/*/understat/teams.csv")
    p.add_argument("--zip", dest="zip_path", required=True, type=Path)
    p.add_argument("--season", default=SEASON_DEFAULT)
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()

    if not args.zip_path.exists():
        raise SystemExit(f"ZIP not found: {args.zip_path}")

    written = 0
    with zipfile.ZipFile(args.zip_path, "r") as zf:
        members = [m for m in zf.namelist() if m.lower().endswith(".csv")]
        for m in sorted(members):
            comp = _parse_comp_from_filename(m)
            if not comp:
                continue
            rows = _normalize_rows(_read_zip_csv(zf, m))
            if not rows:
                continue
            out_path = clean_csv_path(comp, args.season, "understat", "teams.csv")
            merged = _merge_existing(out_path, rows)
            if args.dry_run:
                print(f"[dry-run] {comp}: {len(rows)} new rows -> {out_path} (merged={len(merged)})")
                continue
            _write_csv(out_path, merged)
            written += 1
            print(f"[OK] {comp}: wrote {len(merged)} rows -> {out_path}")

    if written == 0:
        print("No league team files found in zip.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

