"""Aplica db/migrations/add_event_qualifiers.sql."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from sqlalchemy import text

from loaders.common import engine

sql_path = Path(__file__).resolve().parent.parent / "db" / "migrations" / "add_event_qualifiers.sql"
statements = [
    s.strip() for s in sql_path.read_text(encoding="utf-8").split(";")
    if s.strip() and not all(line.strip().startswith("--") or not line.strip()
                             for line in s.strip().splitlines())
]

with engine.begin() as conn:
    for stmt in statements:
        conn.execute(text(stmt))

with engine.connect() as conn:
    rows = conn.execute(text("""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = 'fact_events'
          AND column_name IN ('qualifiers', 'whoscored_event_id')
        ORDER BY column_name
    """)).fetchall()
    print("Migration OK:", rows)
