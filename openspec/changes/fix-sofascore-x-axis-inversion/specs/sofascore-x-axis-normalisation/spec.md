## ADDED Requirements

### Requirement: SofaScore x-coordinates are mirrored before banding
The heatmap SQL CTE SHALL apply `(100 - x) * 1.05` to SofaScore x-coordinates so that shots near the opponent's goal map to high x values (right side of the pitch), consistent with Understat's attacking-direction convention.

#### Scenario: SofaScore shot near attacking goal appears on right side
- **WHEN** a SofaScore shot has `x = 5` (close to the attacking goal in SofaScore convention)
- **THEN** the normalised x_m = (100 - 5) * 1.05 = 99.75 m, placing the shot in x_band = 90 (right side of pitch)

#### Scenario: SofaScore shot in midfield appears in centre
- **WHEN** a SofaScore shot has `x = 50`
- **THEN** the normalised x_m = (100 - 50) * 1.05 = 52.5 m, placing the shot in x_band = 50 (centre)

#### Scenario: Understat shots are unaffected
- **WHEN** the database contains Understat shots with x already in 0-105 m scale
- **THEN** those shots continue to use the existing normalisation (`x * 105` for raw 0-1, or `x` as-is for metre scale) and cluster on the right side unchanged

#### Scenario: Combined heatmap clusters on right for both sources
- **WHEN** the heatmap renders with both Understat and SofaScore shots selected
- **THEN** attacking shots from both sources appear on the right half of the pitch

### Requirement: SofaScore scraper stores x-coordinates in attacking-right convention
The scraper's `transform_shots()` and `transform_events()` functions SHALL store x as `(100 - raw_x) * 1.05` so that newly scraped data is saved with the correct orientation.

#### Scenario: Freshly scraped SofaScore shot stored with correct orientation
- **WHEN** the scraper receives a SofaScore shot with raw `playerCoordinates.x = 10`
- **THEN** the stored x value in `fact_shots` is (100 - 10) * 1.05 = 94.5 m

#### Scenario: Scraper and analytics agree on orientation
- **WHEN** a freshly scraped SofaScore row is read by `get_heatmap_data()`
- **THEN** the analytics CTE does not double-invert the value (existing stored rows with old orientation are still corrected inline by the CTE)
