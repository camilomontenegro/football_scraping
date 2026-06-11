"""
Backfill columnas enriquecidas en fact_events desde qualifiers JSONB.

Uso:
    python -m scripts.backfill_event_enriched_columns
    python -m scripts.backfill_event_enriched_columns --batch-size 250000
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path

from sqlalchemy import text

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from loaders.common import engine

log = logging.getLogger(__name__)

UPDATE_SQL = text("""
UPDATE fact_events e
SET
    body_part = COALESCE(e.body_part, CASE
        WHEN e.qualifiers ? 'RightFoot'     THEN 'RightFoot'
        WHEN e.qualifiers ? 'LeftFoot'      THEN 'LeftFoot'
        WHEN e.qualifiers ? 'Head'          THEN 'Head'
        WHEN e.qualifiers ? 'OtherBodyPart' THEN 'OtherBodyPart'
    END),
    goal_mouth_y = COALESCE(
        e.goal_mouth_y,
        NULLIF(TRIM(e.qualifiers->>'GoalMouthY'), '')::numeric
    ),
    goal_mouth_z = COALESCE(
        e.goal_mouth_z,
        NULLIF(TRIM(e.qualifiers->>'GoalMouthZ'), '')::numeric
    ),
    angle = COALESCE(
        e.angle,
        NULLIF(TRIM(e.qualifiers->>'Angle'), '')::numeric
    ),
    length = COALESCE(
        e.length,
        NULLIF(TRIM(e.qualifiers->>'Length'), '')::numeric
    ),
    pass_end_x = COALESCE(
        e.pass_end_x,
        NULLIF(TRIM(e.qualifiers->>'PassEndX'), '')::numeric
    ),
    pass_end_y = COALESCE(
        e.pass_end_y,
        NULLIF(TRIM(e.qualifiers->>'PassEndY'), '')::numeric
    ),
    blocked_x = COALESCE(
        e.blocked_x,
        NULLIF(TRIM(e.qualifiers->>'BlockedX'), '')::numeric
    ),
    blocked_y = COALESCE(
        e.blocked_y,
        NULLIF(TRIM(e.qualifiers->>'BlockedY'), '')::numeric
    ),
    is_assisted = COALESCE(e.is_assisted,
        CASE WHEN e.qualifiers ? 'Assisted' THEN TRUE END),
    is_individual_play = COALESCE(e.is_individual_play,
        CASE WHEN e.qualifiers ? 'IndividualPlay' THEN TRUE END),
    is_big_chance = COALESCE(e.is_big_chance,
        CASE WHEN e.qualifiers ? 'BigChance' THEN TRUE END),
    is_key_pass = COALESCE(e.is_key_pass,
        CASE WHEN e.qualifiers ? 'KeyPass' THEN TRUE END),
    is_fast_break = COALESCE(e.is_fast_break,
        CASE WHEN e.qualifiers ? 'FastBreak' THEN TRUE END),
    shot_zone = COALESCE(e.shot_zone, CASE
        WHEN e.qualifiers ? 'BoxCentre'      THEN 'BoxCentre'
        WHEN e.qualifiers ? 'BoxLeft'        THEN 'BoxLeft'
        WHEN e.qualifiers ? 'BoxRight'       THEN 'BoxRight'
        WHEN e.qualifiers ? 'OutOfBoxCentre' THEN 'OutOfBoxCentre'
        WHEN e.qualifiers ? 'OutOfBoxLeft'   THEN 'OutOfBoxLeft'
        WHEN e.qualifiers ? 'SmallBoxCentre' THEN 'SmallBoxCentre'
        WHEN e.qualifiers ? 'SmallBoxRight'  THEN 'SmallBoxRight'
        WHEN e.qualifiers ? 'SmallBoxLeft'   THEN 'SmallBoxLeft'
    END),
    shot_placement = COALESCE(e.shot_placement, CASE
        WHEN e.qualifiers ? 'LowLeft'    THEN 'LowLeft'
        WHEN e.qualifiers ? 'LowCentre'  THEN 'LowCentre'
        WHEN e.qualifiers ? 'LowRight'   THEN 'LowRight'
        WHEN e.qualifiers ? 'HighLeft'   THEN 'HighLeft'
        WHEN e.qualifiers ? 'HighCentre' THEN 'HighCentre'
        WHEN e.qualifiers ? 'HighRight'  THEN 'HighRight'
    END),
    situation_detail = COALESCE(e.situation_detail, CASE
        WHEN e.qualifiers ? 'RegularPlay'    THEN 'RegularPlay'
        WHEN e.qualifiers ? 'FromCorner'     THEN 'FromCorner'
        WHEN e.qualifiers ? 'FastBreak'      THEN 'FastBreak'
        WHEN e.qualifiers ? 'DirectFreekick' THEN 'DirectFreekick'
        WHEN e.qualifiers ? 'SetPiece'       THEN 'SetPiece'
        WHEN e.qualifiers ? 'Penalty'        THEN 'Penalty'
    END)
WHERE e.data_source = 'whoscored'
  AND e.qualifiers IS NOT NULL
  AND e.event_id BETWEEN :lo AND :hi
""")


def _stats(conn) -> dict:
    row = conn.execute(text("""
        SELECT
            COUNT(*) FILTER (WHERE data_source = 'whoscored') AS ws_total,
            COUNT(*) FILTER (WHERE data_source = 'whoscored' AND qualifiers IS NOT NULL) AS ws_q,
            COUNT(*) FILTER (WHERE data_source = 'whoscored' AND length IS NOT NULL) AS ws_len,
            COUNT(*) FILTER (WHERE data_source = 'whoscored' AND pass_end_x IS NOT NULL) AS ws_pex,
            COUNT(*) FILTER (WHERE data_source = 'whoscored' AND body_part IS NOT NULL) AS ws_bp,
            COUNT(*) FILTER (WHERE data_source = 'whoscored' AND is_key_pass IS TRUE) AS ws_kp
        FROM fact_events
    """)).one()
    return dict(row._mapping)


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-size", type=int, default=500_000)
    args = parser.parse_args()

    with engine.connect() as conn:
        bounds = conn.execute(text("""
            SELECT MIN(event_id), MAX(event_id)
            FROM fact_events
            WHERE data_source = 'whoscored' AND qualifiers IS NOT NULL
        """)).one()
        before = _stats(conn)

    lo_id, hi_id = bounds[0], bounds[1]
    if lo_id is None:
        log.error("No hay filas WhoScored con qualifiers")
        return 1

    log.info("Antes: %s", before)
    log.info("Backfill event_id %d → %d (lotes de %d)", lo_id, hi_id, args.batch_size)

    t0 = time.time()
    updated_batches = 0
    cur = lo_id
    while cur <= hi_id:
        batch_hi = min(cur + args.batch_size - 1, hi_id)
        with engine.begin() as conn:
            result = conn.execute(UPDATE_SQL, {"lo": cur, "hi": batch_hi})
            n = result.rowcount
        updated_batches += 1
        log.info("  lote %d: event_id %d–%d → %d filas", updated_batches, cur, batch_hi, n)
        cur = batch_hi + 1

    with engine.connect() as conn:
        after = _stats(conn)

    elapsed = time.time() - t0
    log.info("Después: %s", after)
    log.info("Completado en %.1f min", elapsed / 60)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
