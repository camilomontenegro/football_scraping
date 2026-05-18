## ADDED Requirements

### Requirement: dim_player canonical names are stored accent-free
The `player_loader.py` Phase 1 loader SHALL apply `normalize()` to `canonical_name` before inserting into `dim_player`, so all stored names are lowercase and accent-free (matching the output of `mdm_engine.normalize()`).

#### Scenario: Accented name normalized on insert
- **WHEN** Transfermarkt provides a player name with diacritics (e.g. "Ángel Correa")
- **THEN** `dim_player.canonical_name` is stored as "angel correa"

#### Scenario: Existing rows backfilled
- **WHEN** the migration script runs
- **THEN** all existing `dim_player.canonical_name` values are updated to their normalized (accent-free, lowercase) form

---

### Requirement: Player name resolution matches accent-free canonical names
The `resolve_player()` function in `mdm_engine.py` SHALL produce matches for players whose canonical name contains accents, because the stored name and the lookup key are both normalized.

#### Scenario: Goalkeeper with accented name resolves to canonical ID
- **WHEN** WhoScored or any source provides a goalkeeper name with accents (e.g. "Unai Simón")
- **THEN** `resolve_player()` returns the correct `canonical_id` instead of queuing the player in `player_review`

#### Scenario: Player without accents still resolves
- **WHEN** a player name has no accents (e.g. "Jan Oblak")
- **THEN** `resolve_player()` continues to return the correct `canonical_id` (no regression)

#### Scenario: Goalkeeper appears in GK stats tab
- **WHEN** a goalkeeper's name is resolved via the accent-free lookup
- **THEN** the goalkeeper appears in `get_goalkeeper_stats()` results for their team and season

---

### Requirement: Fuzzy match candidate search uses normalized comparison
The fuzzy-match step in `resolve_player()` SHALL compare the normalized input name against normalized candidate names, so accent differences do not reduce similarity scores.

#### Scenario: High-similarity fuzzy match for accented name
- **WHEN** the source name normalizes to a string with ≥ 85% Jaccard similarity to a stored canonical name
- **THEN** the player resolves directly instead of being queued in `player_review`
