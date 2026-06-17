"""
Reapunta match_stadium_id a un dim_stadium con coords cuando el venue actual no las tiene.
"""
from __future__ import annotations

import logging

from sqlalchemy import text

from loaders.common import engine
from loaders.match_stadium_resolver import name_similarity, normalize_name

log = logging.getLogger(__name__)

MIN_SCORE = 0.45


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    with engine.connect() as conn:
        bad = conn.execute(text("""
            SELECT DISTINCT m.match_stadium_id, s.stadium_name
            FROM dim_match m
            JOIN dim_stadium s ON s.stadium_id = m.match_stadium_id
            WHERE s.latitude IS NULL OR s.longitude IS NULL
        """)).mappings().all()
        sources = conn.execute(text("""
            SELECT stadium_id, stadium_name, latitude, longitude
            FROM dim_stadium
            WHERE latitude IS NOT NULL AND longitude IS NOT NULL
        """)).mappings().all()

    norm_sources = [(normalize_name(s["stadium_name"]), s) for s in sources]
    remapped: dict[int, int] = {}
    for row in bad:
        sid = int(row["match_stadium_id"])
        if sid in remapped:
            continue
        tn = normalize_name(row["stadium_name"] or "")
        best_score, best = 0.0, None
        for sn, s in norm_sources:
            if int(s["stadium_id"]) == sid:
                continue
            sc = name_similarity(tn, sn)
            if sc > best_score:
                best_score, best = sc, s
        if best_score >= MIN_SCORE and best:
            remapped[sid] = int(best["stadium_id"])
            log.info(
                "remap stadium_id %s %r -> %s %r (%.2f)",
                sid, row["stadium_name"], best["stadium_id"], best["stadium_name"], best_score,
            )

    if not remapped:
        print("remapped: 0")
        return 0

    updated = 0
    with engine.begin() as conn:
        for old_id, new_id in remapped.items():
            n = conn.execute(text("""
                UPDATE dim_match SET match_stadium_id = :new
                WHERE match_stadium_id = :old
            """), {"old": old_id, "new": new_id}).rowcount
            updated += n
    print(f"remapped venues: {len(remapped)}  matches updated: {updated}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
