# Design Decisions & Implementation Notes

## Architecture Overview

The project implements a classic ETL pipeline with a star schema at its core:

```
5 scrapers → player_matcher → dim_player (canonical)
                                    ↓
                     fact_shots / fact_events / fact_injuries
```

All fact rows are keyed on `dim_player.canonical_id`. No fact row is inserted without first resolving a `canonical_id` through the player matching engine.

---

## Key Decisions

### 1. SQLAlchemy ORM over raw SQL

Using SQLAlchemy's declarative ORM means the same model code works for both SQLite (local dev) and MySQL (production) by switching the `DATABASE_URL` environment variable. The `db/create_tables.sql` file is kept as a human-readable DDL reference for DBA review, but `Base.metadata.create_all()` is authoritative.

### 2. StatsBomb-first load order

StatsBomb Open Data is the canonical seed for `dim_player` because it provides:
- Full player names (first + last)
- Birth dates in ISO format
- UUIDs as stable identifiers

Every subsequent source runs `resolve_player()` against existing `dim_player` rows rather than against each other. This avoids combinatorial ambiguity and ensures the highest-quality data is the ground truth.

### 3. Multi-signal entity resolution (not name-only fuzzy matching)

Name-only matching produces false positives (common names like "David García", transliteration variants of non-Latin names). Adding birth date (35 pts) as the second signal drastically reduces ambiguity. Nationality (15 pts) and position (10 pts) provide tiebreakers.

**Thresholds after empirical testing:**
- ≥ 85 → auto-match (high confidence)
- 60–84 → manual review queue (ambiguous)
- < 60 → new player insert (no plausible candidate)

### 4. Coordinate normalisation at load time

Storing raw coordinates from each source would require every downstream query to know the per-source coordinate system. Instead, all scrapers call `normalize_coords(x, y, source)` before inserting, and the DB columns always hold metres (105×68 pitch). Raw files in `data/raw/` preserve original values for re-processing.

### 5. Raw-file-first pattern

Every scraper writes the raw API response / extracted JSON to `data/raw/<source>/` before any transformation. This enables re-processing without re-scraping — important given rate limits (Transfermarkt, WhoScored) and unofficial APIs that may change (SofaScore).

### 6. Selenium only for WhoScored

WhoScored requires JavaScript rendering to serve data and employs anti-bot measures. Selenium with headless Chrome is the minimal viable approach. All other sources are accessible via `requests`, which keeps the operational footprint small (no ChromeDriver dependency for 4 of 5 scrapers).

---

## Issues Encountered

### `player_review` growth

Sources without birth dates (Understat, WhoScored) produce more 60–84 score matches because the 35-point birth-date signal is zero. Common names (e.g. "Diego García") generate multiple candidates in the review band. Manual resolution is documented in `docs/player_review_notes.md`.

### Understat JS encoding

Understat embeds shot data as a JSON string inside a JavaScript `JSON.parse('...')` call, with the inner string hex-escaped using Python-style `\x` sequences. Decoding requires `.encode('utf-8').decode('unicode_escape')` before `json.loads`.

### SofaScore API stability

The SofaScore API at `api.sofascore.com/api/v1/` is unofficial and undocumented. Endpoints and response shapes may change without notice. The raw-file-first pattern mitigates re-scraping cost when this occurs.

### WhoScored anti-bot

WhoScored actively blocks scrapers. The mitigation — headless Chrome, realistic user-agent, randomised delays — is best-effort. If blocked, rotating the user-agent string or adding a residential proxy may be necessary. Graceful failure (log warning, continue to next match) ensures other sources are unaffected.

### dim_match deduplication

The same fixture may appear in both StatsBomb and Understat. The current design inserts one row per source per match (discriminated by the `source` column), avoiding a second entity-resolution problem. This means cross-source shot analysis requires a join on `home_team + away_team + date + source`.

---

## Trade-offs Accepted

| Trade-off | Reason |
|---|---|
| No cross-source match deduplication | Avoids a second fuzzy-matching problem; `source` column provides filtering |
| Coordinate normalisation is irreversible | Downstream simplicity outweighs raw storage; originals preserved in `data/raw/` |
| WhoScored scraper is best-effort | Anti-bot countermeasures make 100% reliability impossible without a paid proxy |
| `player_review` requires manual resolution | ML classifier would be over-engineering; review queue is the right fallback |
