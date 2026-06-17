"""
Exporta equipos con estadio sintético sin coords para rellenar overrides.

Uso:
    python -m scripts.export_stadium_override_template
    python -m scripts.export_stadium_override_template --with-wikidata-probe
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy import text

from loaders.common import engine

OUT = Path(__file__).resolve().parents[1] / "data" / "stadium_overrides.json"

SQL = text("""
    SELECT DISTINCT ON (s.canonical_team_id)
           t.canonical_name AS team,
           s.stadium_name,
           s.team_slug,
           s.country,
           s.wikidata_qid,
           s.latitude,
           s.longitude
    FROM dim_stadium s
    JOIN dim_team t ON t.canonical_id = s.canonical_team_id
    WHERE s.data_source = 'synthetic-geocode'
      AND (s.latitude IS NULL OR s.longitude IS NULL)
    ORDER BY s.canonical_team_id
""")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--with-wikidata-probe",
        action="store_true",
        help="Intenta rellenar image_url/wikidata_qid desde Wikidata (lento, rate limit).",
    )
    args = parser.parse_args()

    with engine.connect() as conn:
        rows = conn.execute(SQL).mappings().fetchall()

    entries: list[dict] = []
    for row in rows:
        entry = {
            "match": row["team_slug"] or row["team"],
            "team": row["team"],
            "stadium_name": row["stadium_name"],
            "country": row["country"],
            "latitude": row["latitude"],
            "longitude": row["longitude"],
            "wikidata_qid": row["wikidata_qid"],
            "image_url": None,
            "notes": "Rellena lat/lon. image_url: URL directa o deja null y usa wikidata_qid.",
        }
        if args.with_wikidata_probe and row["team"]:
            from scrapers.wikidata_stadium_enricher import query_wikidata_by_club

            try:
                wd = query_wikidata_by_club(row["team"])
                if wd.get("wikidata_qid"):
                    entry["wikidata_qid"] = wd.get("wikidata_qid")
                if wd.get("image_url"):
                    entry["image_url"] = wd.get("image_url")
                if entry["latitude"] is None and wd.get("latitude") is not None:
                    entry["latitude"] = wd.get("latitude")
                    entry["longitude"] = wd.get("longitude")
            except Exception as exc:
                entry["notes"] = f"wikidata probe failed: {exc}"
        entries.append(entry)

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(
        json.dumps({"overrides": entries}, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    print(f"Wrote {len(entries)} entries -> {OUT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
