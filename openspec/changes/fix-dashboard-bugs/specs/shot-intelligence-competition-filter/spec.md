## ADDED Requirements

### Requirement: Heatmap data filters by competition using FK join
The `get_heatmap_data()` function in `analytics.py` SHALL filter by competition using `JOIN dim_competition dc ON dc.canonical_id = m.competition_id WHERE dc.canonical_name = :competition`, not the legacy `m.competition` VARCHAR column.

#### Scenario: Competition selected — filtered heatmap
- **WHEN** a user selects a competition on the Shot Intelligence tab
- **THEN** `get_heatmap_data()` returns only shots from matches belonging to that competition, using the `competition_id` FK

#### Scenario: Competition changed — heatmap updates
- **WHEN** a user switches from one competition to another
- **THEN** the heatmap grid updates to reflect the new competition's shot data

#### Scenario: No competition selected — all competitions included
- **WHEN** competition is None
- **THEN** `get_heatmap_data()` returns shots across all competitions for the season

---

### Requirement: Player finishing chart filters by competition using FK join
The `get_player_finishing()` function in `analytics.py` SHALL filter by competition using the same `dim_competition` FK join pattern.

#### Scenario: Competition selected — filtered finishing chart
- **WHEN** a user selects a competition on the Shot Intelligence tab
- **THEN** `get_player_finishing()` returns only players with shots from that competition

#### Scenario: Competition changed — finishing chart updates
- **WHEN** a user switches competition
- **THEN** the "Player Finishing Quality" chart reflects the new competition's data

---

### Requirement: Heatmap pitch renders when Understat data exists
The Shot Intelligence pitch SHALL render the mplsoccer field graphic whenever `get_heatmap_data()` returns at least one zone with 10+ shots.

#### Scenario: Understat data exists — pitch renders
- **WHEN** Understat shots with coordinates are present for the selected season and competition
- **THEN** the mplsoccer pitch is drawn with the heatmap overlay visible

#### Scenario: No data for selection — info message shown
- **WHEN** no Understat shots exist for the selected season/competition/team
- **THEN** an info message is shown (no pitch drawn, no error)
