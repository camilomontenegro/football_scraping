## 1. Analytics Query Fix

- [x] 1.1 In `dashboard/analytics.py`, inside `get_heatmap_data()` CTE `_norm`, change SofaScore x from `fs.x * 1.05` to `(100 - fs.x) * 1.05`
- [x] 1.2 Verify the SofaScore y expression (`fs.y * 0.68`) is unchanged

## 2. Scraper Fix

- [x] 2.1 In `scrapers/sofascore_scraper.py`, `transform_shots()`: change x normalisation from `pd.to_numeric(df["x"], ...) * 1.05` to `(100 - pd.to_numeric(df["x"], ...)) * 1.05`
- [x] 2.2 In `scrapers/sofascore_scraper.py`, `transform_events()`: apply the same inversion to the event x coordinate line

## 3. Verification

- [ ] 3.1 Run the dashboard and confirm SofaScore (Champions League) heatmap clusters on the right side of the pitch
- [ ] 3.2 Confirm Understat (La Liga) heatmap still clusters on the right side unchanged
- [ ] 3.3 Confirm combined view (both sources) shows a single consistent cluster on the right
