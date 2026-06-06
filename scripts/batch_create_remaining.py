#!/usr/bin/env python3
"""
scripts/batch_create_remaining.py
===================================
Creates new dim_player entries for all remaining unresolved player_review
entries that have no source_id conflict with existing players.

Handles cross-source duplicates by creating the first occurrence and
linking subsequent same-name entries to the same canonical_id.

Usage:
  python scripts/batch_create_remaining.py              # dry-run
  python scripts/batch_create_remaining.py --apply      # apply changes
"""
import sys
import os
from pathlib import Path

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

sys.path.insert(0, str(Path(__file__).parent.parent))

import argparse
from sqlalchemy import text
from loaders.common import engine
from utils.mdm_engine import normalize
from utils.mdm_config import SOURCE_ID_FIELDS


def batch_create(apply: bool = False):
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT id, source_name, source_system, source_id, similarity_score
            FROM player_review
            WHERE resolved = FALSE
            ORDER BY source_name, id
        """)).fetchall()

    total = len(rows)
    print(f"\n  Unresolved reviews: {total}")
    if total == 0:
        print("  Nothing to do.")
        return

    # First pass: check for source_id conflicts
    conflicts = 0
    safe = []
    for row in rows:
        id_col = SOURCE_ID_FIELDS.get(row.source_system, {}).get("player")
        if not id_col:
            continue
        with engine.connect() as conn:
            existing = conn.execute(text(f"""
                SELECT canonical_id FROM dim_player WHERE {id_col} = :sid LIMIT 1
            """), {"sid": row.source_id}).fetchone()
        if existing:
            conflicts += 1
        else:
            safe.append(row)

    print(f"  Conflicts (source_id already exists): {conflicts}")
    print(f"  Safe to create: {len(safe)}")

    # Group by normalized name to detect cross-source duplicates
    name_groups: dict[str, list] = {}
    for row in safe:
        norm = normalize(row.source_name) or row.source_name.lower()
        name_groups.setdefault(norm, []).append(row)

    duplicates = sum(1 for g in name_groups.values() if len(g) > 1)
    unique_players = len(name_groups)
    print(f"  Unique player names: {unique_players}")
    print(f"  Cross-source duplicate groups: {duplicates}")

    if duplicates > 0:
        print(f"\n  --- Duplicate groups ---")
        for norm, group in sorted(name_groups.items()):
            if len(group) > 1:
                sources = [f"{r.source_system}:{r.source_id}" for r in group]
                print(f"    {group[0].source_name}: {', '.join(sources)}")

    if not apply:
        print(f"\n  Re-run with --apply to create {unique_players} new players "
              f"and resolve {len(safe)} reviews.")
        return

    # Apply: create players, handling cross-source duplicates
    print(f"\n  Creating {unique_players} new players...")
    created = 0
    linked = 0
    errors = 0

    for norm, group in name_groups.items():
        # First entry: create new player
        first = group[0]
        id_col = SOURCE_ID_FIELDS.get(first.source_system, {}).get("player")
        if not id_col:
            errors += len(group)
            continue

        try:
            with engine.begin() as conn:
                new_id = conn.execute(text(f"""
                    INSERT INTO dim_player (canonical_name, {id_col})
                    VALUES (:name, :sid)
                    RETURNING canonical_id
                """), {"name": first.source_name, "sid": first.source_id}).scalar()

                conn.execute(text("""
                    UPDATE player_review
                    SET resolved = TRUE, canonical_id_assigned = :cid,
                        similarity_score = :score, reviewed_at = NOW()
                    WHERE id = :rid
                """), {"cid": new_id, "score": first.similarity_score or 0, "rid": first.id})
            created += 1
        except Exception as e:
            errors += 1
            print(f"    ERROR creating {first.source_name}: {e}")
            continue

        # Remaining entries in group: link to the same canonical_id
        for extra in group[1:]:
            extra_id_col = SOURCE_ID_FIELDS.get(extra.source_system, {}).get("player")
            if not extra_id_col:
                errors += 1
                continue
            try:
                with engine.begin() as conn:
                    # Add source ID to the same player
                    conn.execute(text(f"""
                        UPDATE dim_player SET {extra_id_col} = :sid
                        WHERE canonical_id = :cid AND {extra_id_col} IS NULL
                    """), {"sid": extra.source_id, "cid": new_id})

                    conn.execute(text("""
                        UPDATE player_review
                        SET resolved = TRUE, canonical_id_assigned = :cid,
                            similarity_score = :score, reviewed_at = NOW()
                        WHERE id = :rid
                    """), {"cid": new_id, "score": extra.similarity_score or 0, "rid": extra.id})
                linked += 1
            except Exception as e:
                errors += 1
                print(f"    ERROR linking {extra.source_name} ({extra.source_system}): {e}")

    print(f"\n{'='*74}")
    print(f"  RESULTS:")
    print(f"    New players created:   {created}")
    print(f"    Cross-source linked:   {linked}")
    print(f"    Errors:                {errors}")
    print(f"    Total resolved:        {created + linked}")
    print(f"{'='*74}")

    with engine.connect() as conn:
        pending = conn.execute(text(
            "SELECT COUNT(*) FROM player_review WHERE resolved = FALSE"
        )).scalar()
        total_players = conn.execute(text("SELECT COUNT(*) FROM dim_player")).scalar()
        print(f"\n  DB state: {pending} pending reviews, {total_players} total players")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    try:
        batch_create(apply=args.apply)
    except KeyboardInterrupt:
        print("\nCancelled.")
    except Exception as e:
        print(f"\nFatal: {e}")
        import traceback
        traceback.print_exc()
