"""Reset invalid stadium coordinates (e.g. wrong-hemisphere geocodes)."""
import sys
from pathlib import Path

from sqlalchemy import text

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from loaders.common import engine

# European leagues: lat should be roughly 35–72 N. Clear obvious bad rows.
RESET_SQL = text("""
    UPDATE dim_stadium
    SET latitude = NULL,
        longitude = NULL,
        timezone = NULL,
        updated_at = NOW()
    WHERE latitude IS NOT NULL
      AND (latitude < 20 OR latitude > 72 OR ABS(longitude) > 30)
""")

if __name__ == "__main__":
    with engine.begin() as conn:
        n = conn.execute(RESET_SQL).rowcount
    print(f"Reset {n} stadium row(s) with out-of-range coordinates.")
