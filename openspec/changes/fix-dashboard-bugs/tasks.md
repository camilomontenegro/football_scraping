## 1. Player Name Normalization — dim_player Backfill

- [x] 1.1 Create `scripts/migrate_player_names.py` — normalizes existing `dim_player.canonical_name` rows by calling `mdm_engine.normalize()` per row and issuing UPDATE statements (idempotent, no DDL)
- [x] 1.2 In `player_loader.py` `_load_phase1_transfermarkt()`, apply `normalize()` to `name` before the `INSERT INTO dim_player` so all future inserts are accent-free from the start

## 2. Fix Player Name Resolver

- [x] 2.1 Verify `mdm_engine.py` `resolve_player()` exact-match step (`WHERE LOWER(canonical_name) = :n`) works correctly after the dim_player backfill — normalized stored names match normalized lookup key, no code change needed
- [x] 2.2 Verify `mdm_engine.py` fuzzy step calls `normalize(cand_name)` on each candidate — confirmed, consistent with normalized stored names

## 3. Fix Injuries Queries in explore.py (join via fact_events — no schema change)

- [x] 3.1 In `get_season_summary()`, replace `AND team_id = :tid` with a `fact_events` subquery: filter `fi.player_id IN (SELECT fe.player_id FROM fact_events fe JOIN dim_match m ... WHERE fe.team_id = :tid AND m.season = :season)`
- [x] 3.2 In `get_injuries_standalone()`, add team filter via the same `fact_events` subquery; when season is selected, also constrain the subquery to that season
- [x] 3.3 In `get_injury_type_breakdown()`, add the missing team filter via the same `fact_events` subquery pattern (previously the `team` parameter was ignored entirely)

## 4. Fix Analytics Competition Filter in analytics.py

- [x] 4.1 In `get_heatmap_data()`, replace `AND m.competition = :competition` with `JOIN dim_competition dc ON dc.canonical_id = m.competition_id` + `AND dc.canonical_name = :competition` built using f-string injection before the WHERE clause
- [x] 4.2 In `get_player_finishing()`, apply the same competition FK join fix as 4.1

## 5. Verification

- [ ] 5.1 Run `python -m scripts.migrate_player_names` to normalize existing dim_player rows
- [ ] 5.2 Open the Exploration tab, select a team — confirm the Injuries metric card changes per team
- [ ] 5.3 Open the Injuries tab, select a team — confirm no error and table/chart are filtered to that team
- [ ] 5.4 Open the Goalkeepers tab — confirm goalkeepers with accented names appear for their team
- [ ] 5.5 Open Shot Intelligence, switch competition — confirm the heatmap grid updates
- [ ] 5.6 Open Shot Intelligence with a competition/season that has Understat data — confirm the mplsoccer pitch renders
