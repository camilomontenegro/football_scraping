#!/usr/bin/env python3
"""
scripts/smart_batch_review.py
==============================
One-time batch processing of the 439 unresolved player_review entries.

Recalculates similarity scores from scratch (the cached 50 may be stale
after 305+ new players were added to dim_player). Then:

  - Auto-link:   fresh score >= 90 AND last name matches → link to existing
  - Auto-new:    fresh score <  40 → create new player
  - Skip:        everything else → leave for interactive review

Usage:
  python scripts/smart_batch_review.py              # dry-run (default)
  python scripts/smart_batch_review.py --apply       # actually apply changes
  python scripts/smart_batch_review.py --source sofascore --apply
"""
import sys
import argparse
import os
from pathlib import Path

# Fix Windows console encoding
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import text
from loaders.common import engine
from utils.mdm_engine import (
    normalize, _similarity_score, _get_player_cache,
)
from utils.mdm_config import SOURCE_ID_FIELDS


def _last_name(norm_name: str) -> str | None:
    parts = norm_name.split() if norm_name else []
    return parts[-1] if parts else None


def smart_batch(apply: bool = False, source_filter: str | None = None):
    where_src = "AND source_system = :src" if source_filter else ""
    params = {"src": source_filter} if source_filter else {}

    with engine.connect() as conn:
        cache = _get_player_cache(conn)
        print(f"  Player cache: {len(cache):,} players")

        rows = conn.execute(text(f"""
            SELECT id, source_name, source_system, source_id,
                   similarity_score, suggested_canonical_id
            FROM player_review
            WHERE resolved = FALSE {where_src}
            ORDER BY id
        """), params).fetchall()

    total = len(rows)
    print(f"  Unresolved reviews: {total}\n")

    will_link = []
    will_create = []
    will_skip = []

    for idx, row in enumerate(rows, 1):
        rev_id = row.id
        source_name = row.source_name
        source_system = row.source_system
        source_id = row.source_id
        old_score = row.similarity_score or 0
        old_suggested = row.suggested_canonical_id

        norm = normalize(source_name)
        if not norm:
            will_skip.append((rev_id, source_name, 0, None, None, "invalid name"))
            continue

        # Recalculate fresh score against full cache
        best_score = 0
        best_id = None
        best_name = None
        for p in cache:
            score = _similarity_score(norm, p["norm"])
            if score > best_score:
                best_score = score
                best_id = p["id"]
                best_name = p["name"]

        # Determine action
        source_last = _last_name(norm)
        candidate_last = _last_name(normalize(best_name)) if best_name else None
        last_match = source_last and candidate_last and source_last == candidate_last

        if best_id and best_score >= 90 and last_match:
            will_link.append((rev_id, source_name, best_score, best_id, best_name, source_system, source_id))
        elif best_id and best_score >= 85 and last_match and len(norm.split()) >= 2:
            will_link.append((rev_id, source_name, best_score, best_id, best_name, source_system, source_id))
        elif best_score < 40:
            will_create.append((rev_id, source_name, best_score, best_id, best_name, source_system, source_id))
        else:
            reason = f"score={best_score} last_match={last_match}"
            will_skip.append((rev_id, source_name, best_score, best_id, best_name, reason))

        if idx % 50 == 0:
            print(f"  [{idx}/{total}] link={len(will_link)} new={len(will_create)} skip={len(will_skip)}")

    # Summary
    print(f"\n{'='*74}")
    print(f"  RESULTS (dry_run={not apply})")
    print(f"{'='*74}")
    print(f"  Will LINK to existing:  {len(will_link):,}")
    print(f"  Will CREATE new:        {len(will_create):,}")
    print(f"  Will SKIP (ambiguous):  {len(will_skip):,}")
    print()

    if will_link:
        print("  --- LINK candidates (top 20) ---")
        for rev_id, sname, score, cid, cname, ssys, sid in will_link[:20]:
            print(f"    [{ssys}] {sname!r} -> {cname!r} (score={score})")

    if will_create:
        print(f"\n  --- CREATE candidates (top 20) ---")
        for rev_id, sname, score, cid, cname, ssys, sid in will_create[:20]:
            suggested = f" (best match: {cname!r} @ {score})" if cname else ""
            print(f"    [{ssys}] {sname!r}{suggested}")

    if will_skip:
        print(f"\n  --- SKIP candidates (top 20) ---")
        for rev_id, sname, score, cid, cname, reason in will_skip[:20]:
            cname_str = f" vs {cname!r}" if cname else ""
            print(f"    {sname!r}{cname_str} ({reason})")

    if not apply:
        print(f"\n  Re-run with --apply to execute changes.")
        return

    # Apply changes
    print(f"\n  Applying changes...")

    linked = 0
    created = 0
    errors = 0

    # Link existing
    for rev_id, source_name, best_score, best_id, best_name, source_system, source_id in will_link:
        id_col = SOURCE_ID_FIELDS.get(source_system, {}).get("player")
        if not id_col:
            errors += 1
            continue
        try:
            with engine.begin() as conn:
                # Update dim_player with source ID
                conn.execute(text(f"""
                    UPDATE dim_player SET {id_col} = :sid
                    WHERE canonical_id = :cid AND {id_col} IS NULL
                """), {"sid": source_id, "cid": best_id})

                # Mark review as resolved
                conn.execute(text("""
                    UPDATE player_review
                    SET resolved = TRUE, canonical_id_assigned = :cid,
                        similarity_score = :score, reviewed_at = NOW()
                    WHERE id = :rid
                """), {"cid": best_id, "score": best_score, "rid": rev_id})
            linked += 1
        except Exception as e:
            errors += 1
            print(f"    ERROR linking {source_name}: {e}")

    # Create new players
    for rev_id, source_name, best_score, _, _, source_system, source_id in will_create:
        id_col = SOURCE_ID_FIELDS.get(source_system, {}).get("player")
        if not id_col:
            errors += 1
            continue
        try:
            with engine.begin() as conn:
                # Check if source_id already exists (conflict)
                existing = conn.execute(text(f"""
                    SELECT canonical_id FROM dim_player WHERE {id_col} = :sid LIMIT 1
                """), {"sid": source_id}).fetchone()

                if existing:
                    # Already linked somehow — just resolve the review
                    conn.execute(text("""
                        UPDATE player_review
                        SET resolved = TRUE, canonical_id_assigned = :cid,
                            similarity_score = :score, reviewed_at = NOW()
                        WHERE id = :rid
                    """), {"cid": existing.canonical_id, "score": best_score, "rid": rev_id})
                    linked += 1
                    continue

                # Insert new player
                new_id = conn.execute(text(f"""
                    INSERT INTO dim_player (canonical_name, {id_col})
                    VALUES (:name, :sid)
                    RETURNING canonical_id
                """), {"name": source_name, "sid": source_id}).scalar()

                conn.execute(text("""
                    UPDATE player_review
                    SET resolved = TRUE, canonical_id_assigned = :cid,
                        similarity_score = :score, reviewed_at = NOW()
                    WHERE id = :rid
                """), {"cid": new_id, "score": best_score, "rid": rev_id})
            created += 1
        except Exception as e:
            errors += 1
            print(f"    ERROR creating {source_name}: {e}")

    # Update scores for skipped reviews (so they have fresh scores)
    updated_scores = 0
    for rev_id, source_name, best_score, best_id, best_name, reason in will_skip:
        if best_id:
            try:
                with engine.begin() as conn:
                    conn.execute(text("""
                        UPDATE player_review
                        SET suggested_canonical_id = :cid, similarity_score = :score
                        WHERE id = :rid
                    """), {"cid": best_id, "score": best_score, "rid": rev_id})
                updated_scores += 1
            except Exception:
                pass

    print(f"\n{'='*74}")
    print(f"  APPLIED:")
    print(f"    Linked:         {linked}")
    print(f"    Created:        {created}")
    print(f"    Errors:         {errors}")
    print(f"    Scores updated: {updated_scores}")
    print(f"    Still pending:  {len(will_skip)}")
    print(f"{'='*74}")

    # Final DB count
    with engine.connect() as conn:
        pending = conn.execute(text("SELECT COUNT(*) FROM player_review WHERE resolved = FALSE")).scalar()
        total_players = conn.execute(text("SELECT COUNT(*) FROM dim_player")).scalar()
        print(f"\n  DB state: {pending} pending reviews, {total_players} total players")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true", help="Apply changes (default is dry-run)")
    parser.add_argument("--source", type=str, default=None, help="Filter by source system")
    args = parser.parse_args()

    try:
        smart_batch(apply=args.apply, source_filter=args.source)
    except KeyboardInterrupt:
        print("\nCancelled.")
    except Exception as e:
        print(f"\nFatal: {e}")
        import traceback
        traceback.print_exc()
