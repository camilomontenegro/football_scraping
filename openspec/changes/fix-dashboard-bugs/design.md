## Context

The dashboard is a read-only Streamlit app backed by a PostgreSQL database populated by Transfermarkt, Understat, SofaScore, StatsBomb, and WhoScored scrapers. Four bugs share a common root: the query layer diverged from the schema — `fact_injuries` has no `team_id` column, `analytics.py` points at a legacy VARCHAR column for competition filtering, and the name-resolver stores accented canonical names while comparing against accent-stripped lookup keys.

All fixes are **query-layer only** — no schema migrations, no scraper re-runs, no backfill scripts required.

## Goals / Non-Goals

**Goals:**
- Injuries tab team filter works without schema changes, using a player-based join through `fact_events`.
- `get_season_summary()` injuries metric scopes to the selected team.
- `get_injury_type_breakdown()` respects the `team` parameter.
- Goalkeepers (and any player) with accented canonical names resolve correctly in `mdm_engine.py`.
- Shot Intelligence heatmap and player finishing chart filter by competition through the `dim_competition` FK join.

**Non-Goals:**
- Any schema migration or `ALTER TABLE`.
- Re-scraping any source or running backfill scripts.
- Adding competition-level filtering to `fact_injuries` (table has no competition link by design).
- Fixing analytics queries beyond `get_heatmap_data` and `get_player_finishing`.
- Changing the Streamlit UI layout.

## Decisions

### D1 — Filter injuries by team via `fact_events` subquery, no schema change

**Decision:** Instead of adding `team_id` to `fact_injuries`, scope injury queries to a team by filtering on which players appeared for that team using `fact_events`:

```sql
-- injuries for a specific team in a specific season
SELECT ... FROM fact_injuries fi
WHERE fi.season = :short_season
  AND fi.player_id IN (
      SELECT fe.player_id
      FROM fact_events fe
      JOIN dim_match m ON m.match_id = fe.match_id
      WHERE fe.team_id = :tid
        AND m.season  = :season   -- full format "2020/2021"
  )
```

When season is not selected (all-seasons view), the `AND m.season = :season` clause is dropped — the filter becomes "players who ever appeared for this team in any event."

`fact_events` is preferred over `fact_shots` because it captures all players (substitutions, cards, lineup events) not just those who took a shot — which matters for defenders and goalkeepers who rarely appear in `fact_shots`.

**Alternatives considered:**
- *Add `team_id` column to `fact_injuries` + backfill*: no schema change needed with the subquery approach; schema changes carry operational risk (migration scripts, re-runs if data is reloaded).
- *Subquery through `fact_shots`*: narrower coverage — injured players who never took a shot (e.g. a backup keeper injured before playing) would be invisible to the filter.
- *Player→team mapping table*: over-engineered.

**Trade-offs:**
- Players who appear in `fact_injuries` but have zero events in `fact_events` for that team/season won't show up under the team filter. Acceptable for analytics — these are fringe cases (long-term injured players with no appearances at all).
- Mid-season transfers: a player who moved teams mid-season will appear under both teams' filters (they have events for each). This is arguably correct behaviour — the injury happened during their time at the club.

---

### D2 — Normalize canonical names at insert time in `player_loader.py` Phase 1

**Decision:** Apply `normalize()` to `canonical_name` before the `INSERT INTO dim_player` in `_load_phase1_transfermarkt()`. All stored canonical names are accent-free from the start, so the existing SQL lookup `WHERE LOWER(canonical_name) = :n` (where `:n` is already normalized by `mdm_engine.normalize()`) produces correct matches.

**Alternatives considered:**
- *Use PostgreSQL `unaccent()` extension*: requires the extension installed and enabled — an additional DB setup dependency.
- *Apply `unaccent()` only in `mdm_engine.py` lookups*: fixes new lookups but leaves existing `dim_player` rows inconsistent.

**Why this approach:** Self-contained in Python, zero DB dependencies. The one-time backfill of existing rows (a Python loop issuing `UPDATE` statements) is the only operational step required, and it's idempotent.

**Display name trade-off:** Stored names lose their accents (e.g., "Ángel Correa" → "angel correa"). Acceptable for an internal analytics tool; can be revisited by adding a `display_name` column if needed later.

---

### D3 — Fix competition filter in `analytics.py` using FK join pattern from `explore.py`

**Decision:** Replace the two instances of `AND m.competition = :competition` in `analytics.py` with the same `JOIN dim_competition dc ON dc.canonical_id = m.competition_id AND dc.canonical_name = :competition` pattern already used throughout `explore.py`. The competition filter is built up front using f-string injection (same as `_comp_clause()` in `explore.py`), not appended after the `WHERE` clause.

**Alternatives considered:**
- *Populate `dim_match.competition` VARCHAR column consistently*: standardizing it would require touching every loader. Using the FK is the correct long-term path.

## Risks / Trade-offs

- **[Risk] Accented display names** → After D2, stored names like "Ángel Correa" become "angel correa". Mitigation: add `display_name VARCHAR` to `dim_player` if the product ever needs accented display; for now acceptable.
- **[Risk] Existing `dim_player` rows have accented names** → D2 normalizes new inserts but not existing rows. Mitigation: run `scripts/migrate_player_names.py` once to normalize existing rows (idempotent, pure Python, no DDL).
- **[Risk] Players with no events invisible to team injury filter** → Covered under D1 trade-offs above. Acceptable for this use case.

## Migration Plan

1. Run `python -m scripts.migrate_player_names` once to normalize existing `dim_player.canonical_name` rows.
2. Deploy updated `explore.py`, `analytics.py`, `player_loader.py`.
3. Verify: Injuries tab → select team → filtered results, no error.
4. Verify: Shot Intelligence → switch competition → heatmap updates.

**Rollback:** All changes are query-layer only. Reverting the Python files is sufficient — no schema rollback needed.

## Open Questions

- Should `display_name` (with accents) be added to `dim_player` for future UI polish? Not required for this fix.
- Are there other analytics queries beyond `get_heatmap_data` and `get_player_finishing` that use `m.competition`? (Confirmed none — `get_shot_type_breakdown`, `get_situation_breakdown`, `get_period_breakdown`, `get_setpiece_goals` do not filter by competition.)
