## Why

SofaScore encodes shot x-coordinates starting from the attacking goal (x=0 near goal, x=100 at own end), while Understat encodes them starting from the own goal (x=0 at own end, x=105 near goal). Because the heatmap treats all x values the same way, SofaScore shots cluster on the left side of the pitch and Understat shots cluster on the right — the opposite of each other — making the combined heatmap misleading.

## What Changes

- The SofaScore x normalisation in the heatmap SQL CTE is changed from `x * 1.05` to `(100 - x) * 1.05`, mirroring the axis so shots near the attacking goal map to high x values (right side), consistent with Understat.
- The same inversion is applied in `sofascore_scraper.py` so coordinates stored from future scrapes are already in the correct orientation.

## Capabilities

### New Capabilities

- `sofascore-x-axis-normalisation`: Correct x-axis orientation for SofaScore shot coordinates so they are consistent with Understat's attacking-direction convention (high x = near opponent's goal).

### Modified Capabilities

## Impact

- `dashboard/analytics.py` — `get_heatmap_data()` CTE `_norm`: SofaScore x expression
- `scrapers/sofascore_scraper.py` — `transform_shots()` and `transform_events()`: x coordinate line
- No schema changes, no data migrations needed for the analytics fix (handled inline in SQL)
- Existing SofaScore data already in the database will be displayed correctly after the analytics fix; re-scraping is needed only to fix stored coordinates
