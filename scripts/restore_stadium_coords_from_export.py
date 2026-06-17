"""Restore lat/lon/wikidata from pre-reload CSV export."""
from __future__ import annotations

import csv
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy import text

from loaders.common import engine

CSV_PATH = Path(__file__).resolve().parents[1] / "reports" / "dim_stadium_latest.csv"

GEO_COLS = (
    "latitude", "longitude", "timezone", "altitude_m",
    "wikidata_qid", "wikipedia_url", "image_url", "city", "country",
)


def _norm(s: str | None) -> str:
    return (s or "").strip().lower()


def load_csv_lookup() -> dict[tuple[str, str], dict]:
    lookup: dict[tuple[str, str], dict] = {}
    with CSV_PATH.open(encoding="utf-8", newline="") as fh:
        for row in csv.DictReader(fh):
            lat = row.get("latitude") or ""
            if not lat.strip():
                continue
            key = (_norm(row.get("team_slug")), _norm(row.get("stadium_name")))
            lookup[key] = {c: row.get(c) for c in GEO_COLS}
    return lookup


def restore(dry_run: bool = False) -> int:
    lookup = load_csv_lookup()
    updated = 0
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT stadium_id, team_slug, stadium_name
            FROM dim_stadium
            WHERE data_source = 'transfermarkt'
              AND (latitude IS NULL OR longitude IS NULL)
        """)).mappings().fetchall()

        for row in rows:
            key = (_norm(row["team_slug"]), _norm(row["stadium_name"]))
            data = lookup.get(key)
            if not data:
                # fallback: any stadium for this team with coords in export
                prefix = _norm(row["team_slug"]) + "\0"
                candidates = [v for k, v in lookup.items() if k[0] == _norm(row["team_slug"])]
                data = candidates[0] if len(candidates) == 1 else None
            if not data or not data.get("latitude"):
                continue
            updated += 1
            if dry_run:
                continue
            sets = ", ".join(f"{c} = :{c}" for c in GEO_COLS)
            with engine.begin() as c2:
                c2.execute(text(f"""
                    UPDATE dim_stadium SET {sets}, updated_at = NOW()
                    WHERE stadium_id = :stadium_id
                """), {**data, "stadium_id": row["stadium_id"]})

    return updated


if __name__ == "__main__":
    n = restore(dry_run="--dry-run" in sys.argv)
    print(f"Restored coords for {n} rows" + (" (dry-run)" if "--dry-run" in sys.argv else ""))
