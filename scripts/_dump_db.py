"""Dump completo de PostgreSQL vía pg_dump."""
from __future__ import annotations

import os
import subprocess
import sys
from datetime import date
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")

PG_DUMP = Path(r"C:\Program Files\PostgreSQL\18\bin\pg_dump.exe")
OUT_DIR = ROOT / "data" / "dumps"
OUT_FILE = OUT_DIR / f"football_db_full_{date.today().isoformat()}.dump"


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    env = os.environ.copy()
    env["PGPASSWORD"] = os.getenv("DB_PASSWORD", "")

    cmd = [
        str(PG_DUMP),
        "-h", os.getenv("DB_HOST", "127.0.0.1"),
        "-p", os.getenv("DB_PORT", "5432"),
        "-U", os.getenv("DB_USER", "postgres"),
        "-d", os.getenv("DB_NAME", "football_db"),
        "-F", "c",
        "-f", str(OUT_FILE),
    ]
    print("Dumping to", OUT_FILE)
    subprocess.run(cmd, check=True, env=env)
    size_gb = OUT_FILE.stat().st_size / (1024 ** 3)
    print(f"OK {OUT_FILE} ({size_gb:.2f} GB)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
