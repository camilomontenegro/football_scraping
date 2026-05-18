#!/usr/bin/env python3
"""
scripts/migrate_player_names.py
================================
One-time migration: normalize existing dim_player.canonical_name rows
by stripping accents and lowercasing, so they match the output of
mdm_engine.normalize() used in all player lookups.

Safe to re-run — rows already normalized are left unchanged.

Run from football_scraping/:
    python -m scripts.migrate_player_names
"""

import logging
import sys
from pathlib import Path

from sqlalchemy import text

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from loaders.common import engine
from utils.mdm_engine import normalize

log = logging.getLogger(__name__)


def normalize_player_names() -> int:
    with engine.begin() as conn:
        rows = conn.execute(
            text("SELECT canonical_id, canonical_name FROM dim_player")
        ).fetchall()

        updated = 0
        for cid, cname in rows:
            normed = normalize(cname)
            if normed and normed != cname:
                conn.execute(
                    text("UPDATE dim_player SET canonical_name = :n WHERE canonical_id = :cid"),
                    {"n": normed, "cid": cid},
                )
                updated += 1

    log.info("Normalized %d / %d dim_player rows", updated, len(rows))
    return updated


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s - %(message)s")
    normalize_player_names()
