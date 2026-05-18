## ADDED Requirements

### Requirement: fact_injuries stores team_id
The `fact_injuries` table SHALL have a nullable `team_id INTEGER` foreign key referencing `dim_team(canonical_id)`, populated for each injury record where the player's team can be determined.

#### Scenario: Column exists after migration
- **WHEN** the migration script runs
- **THEN** `fact_injuries` has a `team_id` column and all rows where the player appears in `fact_shots` or the players CSV have a non-NULL `team_id`

#### Scenario: Backfill from shots data
- **WHEN** a player has shot records in `fact_shots` for a given season
- **THEN** their `fact_injuries.team_id` for that season is set to the team_id with the most shot appearances in that season

#### Scenario: CSV fallback for players with no shots
- **WHEN** a player has no `fact_shots` records for an injury season but appears in `transfermarkt_champions_players.csv` for that team and season
- **THEN** their `fact_injuries.team_id` is set from the CSV-derived team lookup

#### Scenario: NULL for unresolvable team
- **WHEN** a player's team cannot be determined from shots data or the players CSV
- **THEN** `fact_injuries.team_id` remains NULL and no error is raised

---

### Requirement: Exploration tab injuries metric scopes to selected team
The `get_season_summary()` function in `explore.py` SHALL filter the injuries count by `team_id` when a team is selected.

#### Scenario: All teams selected — season-wide count
- **WHEN** no team is selected (team = None)
- **THEN** the Injuries metric card shows the total injury count for the whole season

#### Scenario: Specific team selected — team-scoped count
- **WHEN** a team is selected
- **THEN** the Injuries metric card shows only injury records where `fact_injuries.team_id` matches that team, not the season total

#### Scenario: Team with no injuries
- **WHEN** a team is selected and has no injury records for the season
- **THEN** the Injuries metric card shows 0

---

### Requirement: Injuries tab team filter works without error
The `get_injuries_standalone()` function SHALL filter injury records by team when a team is selected, using `fi.team_id`.

#### Scenario: Team selected — filtered table
- **WHEN** a user selects a team on the Injuries tab
- **THEN** only that team's injury records are shown in the table (no PostgreSQL error)

#### Scenario: All teams selected — full table
- **WHEN** no team is selected
- **THEN** all injury records for the season are shown

---

### Requirement: Injury type breakdown respects team filter
The `get_injury_type_breakdown()` function SHALL filter by team when a team is selected. Currently the `team` parameter is ignored.

#### Scenario: Team selected — filtered breakdown chart
- **WHEN** a user selects a team on the Injuries tab
- **THEN** the "Top injury types" bar chart shows only injury types for that team

#### Scenario: All teams — full breakdown
- **WHEN** no team is selected
- **THEN** the bar chart aggregates across all teams
