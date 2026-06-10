"""
Drop the configured PostgreSQL database completely.

This is intentionally separate from setup_db.py. Use it only when you want to
delete every table and every row in the configured DB_NAME database.

Run:
    python -m db.drop_database --yes
"""

from __future__ import annotations

import argparse
import os

from dotenv import load_dotenv
from sqlalchemy import create_engine, text


load_dotenv()

DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "football_db")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD")


def main() -> int:
    parser = argparse.ArgumentParser(description="Drop the configured PostgreSQL database.")
    parser.add_argument("--yes", action="store_true", help="Confirm the destructive drop.")
    args = parser.parse_args()

    if not DB_PASSWORD:
        print("ERROR: DB_PASSWORD must be set in .env file")
        return 1

    if DB_NAME in {"postgres", "template0", "template1"}:
        print(f"ERROR: refusing to drop protected database {DB_NAME!r}")
        return 1

    if not args.yes:
        print(f"Refusing to drop {DB_NAME!r} without --yes.")
        print("Run: python -m db.drop_database --yes")
        return 1

    url_postgres = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/postgres"
    engine = create_engine(url_postgres, isolation_level="AUTOCOMMIT")

    with engine.connect() as conn:
        exists = conn.execute(
            text("SELECT 1 FROM pg_database WHERE datname = :name"),
            {"name": DB_NAME},
        ).fetchone()
        if not exists:
            print(f"Database {DB_NAME!r} does not exist.")
            return 0

        conn.execute(text("""
            SELECT pg_terminate_backend(pid)
            FROM pg_stat_activity
            WHERE datname = :name
              AND pid <> pg_backend_pid()
        """), {"name": DB_NAME})
        conn.execute(text(f'DROP DATABASE "{DB_NAME}"'))

    print(f"Database {DB_NAME!r} dropped.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
