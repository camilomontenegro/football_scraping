"""Auditoría rápida de nulos en dim_stadium."""
from sqlalchemy import text
from loaders.common import engine

COLS = [
    "stadium_name", "capacity", "seats_total", "built_year", "city", "country",
    "previous_names_raw", "pitch_length_m", "pitch_width_m", "naming_rights",
    "architect", "operator", "latitude", "longitude", "wikidata_qid", "image_url",
    "surface", "owner", "address", "capacity_intl", "has_pitch_heating",
]

with engine.connect() as conn:
    total = conn.execute(text("SELECT COUNT(*) FROM dim_stadium")).scalar()
    current = conn.execute(text(
        "SELECT COUNT(*) FROM dim_stadium WHERE COALESCE(is_current, TRUE)"
    )).scalar()
    print(f"Total filas: {total} | Vigentes: {current}\n")
    print(f"{'Campo':<22} {'% all':>8} {'% cur':>8} {'vacios':>8}")
    print("-" * 50)
    for col in COLS:
        row = conn.execute(text(f"""
            SELECT
              ROUND(100.0 * COUNT(*) FILTER (
                WHERE {col} IS NOT NULL AND CAST({col} AS TEXT) <> ''
              ) / NULLIF(COUNT(*), 0), 1),
              ROUND(100.0 * COUNT(*) FILTER (
                WHERE COALESCE(is_current, TRUE)
                  AND {col} IS NOT NULL AND CAST({col} AS TEXT) <> ''
              ) / NULLIF(COUNT(*) FILTER (WHERE COALESCE(is_current, TRUE)), 0), 1),
              COUNT(*) FILTER (WHERE {col} IS NULL OR CAST({col} AS TEXT) = '')
            FROM dim_stadium
        """)).one()
        print(f"{col:<22} {row[0] or 0:>7}% {row[1] or 0:>7}% {row[2]:>8}")
