## Why

Four bugs in the Streamlit dashboard produce incorrect data or runtime errors when users filter by team: the injuries metric never scopes to a selected team, the injuries tab crashes on team selection because `fact_injuries` lacks a `team_id` column, goalkeepers with accented names are silently dropped from GK stats due to an accent-stripping mismatch in the player resolver, and the Shot Intelligence heatmap ignores the competition filter and fails to render because it queries a legacy text column instead of the proper FK.

## What Changes

- **`fact_injuries` schema** — add `team_id INTEGER REFERENCES dim_team` column via migration; backfill from `fact_shots`/`fact_events` (SQL-only, no re-scraping).
- **`explore.py` — `get_season_summary()`** — fix injuries count to filter by `tid` when a team is selected (bug: both branches use the same unconditioned query).
- **`explore.py` — `get_injuries_standalone()` and `get_injury_type_breakdown()`** — fix team filter to use the new `fi.team_id` column; `get_injury_type_breakdown()` currently ignores its `team` parameter entirely.
- **`mdm_engine.py` — `normalize()`** — stop stripping accents before the DB lookup; instead, change the SQL match to use `unaccent(LOWER(canonical_name))` **or** apply `normalize()` when inserting canonical names in `player_loader.py` Phase 1 so the stored names are already accent-free.
- **`analytics.py` — `get_heatmap_data()` and `get_player_finishing()`** — replace `AND m.competition = :competition` (legacy VARCHAR column) with a proper `JOIN dim_competition dc ON dc.canonical_id = m.competition_id WHERE dc.canonical_name = :competition`, matching the pattern already used in `explore.py`.

## Capabilities

### New Capabilities

- `injuries-team-filter`: Team-scoped injury data — `fact_injuries` gains a `team_id` FK, the injuries tab filters correctly by team, and the Exploration summary card shows team-scoped injury counts.
- `player-name-normalization`: Consistent accent-free name storage in `dim_player` so name resolution in `mdm_engine.py` matches regardless of diacritics, fixing missing goalkeepers and other players with accented names.
- `shot-intelligence-competition-filter`: Shot Intelligence analytics queries (`get_heatmap_data`, `get_player_finishing`) filter by competition using the canonical FK join, so the heatmap and finishing chart update correctly when competition changes.

### Modified Capabilities

## Impact

- **Database schema** — `fact_injuries` gains one nullable FK column; a migration script (or inline `ALTER TABLE`) is required before the app can run without errors when a team is selected on the Injuries tab.
- **`dashboard/explore.py`** — `get_season_summary`, `get_injuries_standalone`, `get_injury_type_breakdown`.
- **`dashboard/analytics.py`** — `get_heatmap_data`, `get_player_finishing`.
- **`utils/mdm_engine.py`** — `normalize()` and the SQL lookup inside `resolve_player()`.
- **`loaders/player_loader.py`** — Phase 1 insert of `dim_player.canonical_name`.
- **No scraper changes required** — all data needed for the backfill is already on disk or in the DB.
