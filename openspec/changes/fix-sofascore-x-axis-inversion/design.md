## Context

The dashboard heatmap normalises shot coordinates from multiple sources to a common 0-105 m × 0-68 m metre scale. Understat encodes x from the own goal outward (x≈0 = own goal, x≈105 = opponent's goal), so attacking shots cluster at high x (right side of the pitch). SofaScore encodes x from the attacking goal outward (x≈0 = opponent's goal, x≈100 = own goal), so the same attacking shots have low x values — the opposite orientation. After the previous normalisation fix (`x * 1.05`), SofaScore shots were correctly scaled to metres but remained mirrored, appearing on the left while Understat shots appeared on the right.

## Goals / Non-Goals

**Goals:**
- Heatmap shows all sources with attacking shots clustered on the right (high x), consistent with Understat convention.
- Scraper stores future SofaScore coordinates in the correct orientation.
- No schema migrations or database backfills required — the analytics fix is query-layer only.

**Non-Goals:**
- Correcting SofaScore y-axis orientation (not observed to be a problem).
- Backfilling existing SofaScore rows in `fact_shots` (the inline SQL inversion handles them at query time).
- Fixing any other data source (StatsBomb, WhoScored not yet included in the heatmap).

## Decisions

### D1 — Invert x in the SQL CTE, not in a backfill script

**Decision:** Apply `(100 - x) * 1.05` for SofaScore inside the `_norm` CTE in `get_heatmap_data()`. Existing SofaScore rows in the database are corrected at query time with no data migration.

**Alternatives considered:**
- *Backfill script (`UPDATE fact_shots SET x = (100 - x) * 1.05 WHERE data_source = 'sofascore'`)* — destructive and irreversible; if the formula is wrong, data cannot be recovered without a re-scrape.
- *Dual-column storage (`x_raw`, `x_norm`)* — over-engineered for a two-line formula.

**Why inline:** Zero operational risk. Wrong formula → fix the query and redeploy. No data lost.

### D2 — Also invert in the scraper for forward consistency

**Decision:** Update `sofascore_scraper.py` so newly scraped coordinates are stored already inverted (`(100 - x) * 1.05`). This makes the stored values consistent with Understat and simplifies future consumers that read raw DB values.

**Trade-off:** Scraper and analytics query now both apply the inversion. If `fact_shots` already contains SofaScore rows and the scraper re-runs without a prior backfill, the analytics CTE will double-invert those new rows (because it still applies `(100 - x) * 1.05` to values that are already inverted). Mitigation: after updating the scraper, truncate existing SofaScore shot rows and re-scrape, OR update the analytics CTE to detect already-inverted values. For now the simpler path is acceptable given the data volume.

## Risks / Trade-offs

- **[Risk] Double-inversion if scraper re-runs before backfill** → Mitigation: document that existing SofaScore `fact_shots` rows should be deleted before re-scraping after the scraper change. The analytics query handles un-inverted data (old rows) correctly in the meantime.
- **[Risk] SofaScore y-axis may also need adjustment** → If y is also oriented differently, this fix alone will not fully correct the heatmap. Mitigation: verify visually after deployment; y fix is a trivial follow-on change.
- **[Risk] Formula assumes SofaScore x is always 0-100** → Validated by DB query showing `MAX(x) ≈ 100` for sofascore rows. Edge values slightly above 100 (out-of-bounds positions) will produce slightly negative metre values; HAVING COUNT(*) >= 10 prevents these fringe cells from appearing.
