# Football Data Pipeline

Multi-source ETL system that collects, integrates, and stores football statistics from five data providers into a unified PostgreSQL database.

**Scope:** La Liga — seasons 2020/21 to 2024/25.

---

## Table of Contents

1. [Architecture](#architecture)
2. [Database Schema](#database-schema)
3. [Setup](#setup)
4. [Running the Pipeline](#running-the-pipeline)
5. [Scrapers Reference](#scrapers-reference)
6. [Scripts Reference](#scripts-reference)
7. [Player Review Queue](#player-review-queue)
8. [Troubleshooting](#troubleshooting)

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                                  │
│                                                                       │
│  Transfermarkt   SofaScore   Understat   StatsBomb   WhoScored       │
│  (players,       (matches,   (shots,     (events,    (events,        │
│   injuries)       events,    xG)          lineups)    coords)         │
│                   shots)                                              │
└───────┬──────────────┬────────────┬───────────┬───────────┬─────────┘
        │              │            │           │           │
        ▼              ▼            ▼           ▼           ▼
┌─────────────────────────────────────────────────────────────────────┐
│                     RAW LAYER  (data/raw/)                           │
│         JSON / CSV files — never modified after download             │
└───────┬──────────────┬────────────┬───────────┬───────────┬─────────┘
        │              │            │           │           │
        ▼              ▼            ▼           ▼           ▼
┌─────────────────────────────────────────────────────────────────────┐
│                   STAGING LAYER  (PostgreSQL)                        │
│  stg_transfermarkt_*   stg_sofascore_*   stg_understat_shots        │
│  stg_statsbomb_events  stg_whoscored_events                          │
└─────────────────────────────┬───────────────────────────────────────┘
                              │
                    ┌─────────▼─────────┐
                    │   MDM ENGINE       │
                    │  name resolution   │
                    │  alias matching    │
                    │  player_review     │
                    └─────────┬─────────┘
                              │
        ┌─────────────────────┼──────────────────────┐
        ▼                     ▼                      ▼
┌──────────────┐    ┌──────────────────┐    ┌───────────────┐
│ DIMENSIONS   │    │ FACTS            │    │ ALIGNMENT     │
│              │    │                  │    │               │
│ dim_player   │    │ fact_shots       │    │ external_ids  │
│ dim_team     │    │ fact_events      │    │ player_alias  │
│ dim_match    │    │ fact_injuries    │    │ team_alias    │
│ dim_season   │    │                  │    │ match_align   │
│ dim_injury_  │    │                  │    │               │
│   type       │    │                  │    │               │
└──────────────┘    └──────────────────┘    └───────────────┘
```

---

## Database Schema

### Dimensions

#### `dim_player`
Canonical player registry. One row per real-world player.

| Column | Type | Description |
|---|---|---|
| player_id | SERIAL PK | Internal ID |
| name_canonical | VARCHAR(150) | Canonical full name |
| nationality | VARCHAR(80) | Country of nationality |
| birth_date | DATE | Date of birth |
| player_position | VARCHAR(50) | Position (e.g. Delantero, Portero) |
| created_at | TIMESTAMP | Row creation time |

#### `dim_team`
| Column | Type | Description |
|---|---|---|
| team_id | SERIAL PK | Internal ID |
| name_canonical | VARCHAR(150) | Canonical team name |
| country | VARCHAR(80) | Country |
| created_at | TIMESTAMP | |

#### `dim_match`
| Column | Type | Description |
|---|---|---|
| match_id | SERIAL PK | Internal ID |
| match_date | DATE | Date of the match |
| season_id | INTEGER FK | → dim_season |
| home_team_id | INTEGER FK | → dim_team |
| away_team_id | INTEGER FK | → dim_team |
| home_score | SMALLINT | Final home goals |
| away_score | SMALLINT | Final away goals |
| data_source | VARCHAR(30) | Source that created this row |
| created_at | TIMESTAMP | |

#### `dim_season`
| Column | Type | Description |
|---|---|---|
| season_id | SERIAL PK | |
| label | VARCHAR(20) | e.g. `2020/2021` |
| year_start | SMALLINT | e.g. 2020 |
| year_end | SMALLINT | e.g. 2021 |

#### `dim_injury_type`
| Column | Type | Description |
|---|---|---|
| injury_type_id | SERIAL PK | |
| name | VARCHAR(200) | Injury label (e.g. "Muscle Injury") |
| category | VARCHAR(80) | Broad category |

---

### Facts

#### `fact_shots`
One row per shot attempt. Sources: Understat (primary), SofaScore, StatsBomb.

| Column | Type | Description |
|---|---|---|
| shot_id | SERIAL PK | |
| match_id | INTEGER FK | → dim_match (nullable) |
| player_id | INTEGER FK | → dim_player (nullable) |
| team_id | INTEGER FK | → dim_team |
| minute | SMALLINT | Minute of the shot |
| x | DECIMAL(7,4) | Pitch X coordinate |
| y | DECIMAL(7,4) | Pitch Y coordinate |
| xg | DECIMAL(7,4) | Expected goals value |
| result | VARCHAR(30) | Goal / SavedShot / MissedShots / BlockedShot |
| shot_type | VARCHAR(30) | RightFoot / LeftFoot / Head |
| situation | VARCHAR(50) | OpenPlay / SetPiece / Corner / Penalty |
| data_source | VARCHAR(30) | understat / sofascore / statsbomb |
| external_id | TEXT | ID in source system |

#### `fact_events`
One row per on-pitch event. Sources: WhoScored (with coords), SofaScore (incidents), StatsBomb (detailed).

| Column | Type | Description |
|---|---|---|
| event_id | SERIAL PK | |
| match_id | INTEGER FK | → dim_match (nullable) |
| player_id | INTEGER FK | → dim_player |
| team_id | INTEGER FK | → dim_team |
| event_type | VARCHAR(50) | Pass / Shot / Tackle / Card / Substitution… |
| minute | SMALLINT | |
| second | SMALLINT | (StatsBomb only) |
| x | DECIMAL(7,4) | Start X (WhoScored / StatsBomb only) |
| y | DECIMAL(7,4) | Start Y |
| end_x | DECIMAL(7,4) | End X (StatsBomb only) |
| end_y | DECIMAL(7,4) | End Y (StatsBomb only) |
| outcome | VARCHAR(50) | Result of the event |
| data_source | VARCHAR(30) | whoscored / sofascore / statsbomb |
| external_id | TEXT | |

> **Note:** SofaScore events (substitutions, cards, VAR decisions) have NULL coordinates by design — they are incident markers, not spatial events.

#### `fact_injuries`
One row per injury spell per player.

| Column | Type | Description |
|---|---|---|
| injury_id | SERIAL PK | |
| player_id | INTEGER FK | → dim_player |
| team_id | INTEGER FK | → dim_team |
| injury_type_id | INTEGER FK | → dim_injury_type |
| season_id | INTEGER FK | → dim_season |
| date_from | DATE | Start of absence |
| date_until | DATE | Return date |
| days_absent | INTEGER | |
| matches_missed | SMALLINT | |

---

### MDM / Resolution Tables

| Table | Purpose |
|---|---|
| `player_name_alias` | Maps variant spellings to a canonical player |
| `team_name_alias` | Maps variant team names to a canonical team |
| `player_external_ids` | Source IDs per player (transfermarkt, sofascore…) |
| `team_external_ids` | Source IDs per team |
| `match_external_ids` | Source IDs per match |
| `transfermarkt_player_mapping` | Transfermarkt ID → canonical player_id |
| `player_resolution_log` | Audit trail of every MDM resolution decision |
| `match_alignment` | Cross-source match linking records |
| `player_alignment` | Cross-source player linking records |
| `player_review` | Manual review queue for ambiguous player matches (score 60–84) |

---

### Staging Tables

Temporary landing zone — idempotent, reloaded on each pipeline run.

| Table | Source |
|---|---|
| `stg_transfermarkt_players` | Transfermarkt player roster |
| `stg_transfermarkt_injuries` | Transfermarkt injury records |
| `stg_sofascore_shots` | SofaScore shot data |
| `stg_sofascore_events` | SofaScore incident data |
| `stg_understat_shots` | Understat shot + xG data |
| `stg_statsbomb_events` | StatsBomb event data |
| `stg_whoscored_events` | WhoScored event data with coordinates |

---

## Setup

### Requirements

- Python 3.10+
- PostgreSQL 14+
- Google Chrome (for SofaScore and WhoScored scrapers)

### Installation

```bash
# 1. Clone the repository
git clone https://github.com/camilomontenegro/football_scraping.git
cd football_scrapping

# 2. Create virtual environment
python -m venv venv
source venv/bin/activate        # Windows: venv\Scripts\activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Configure database credentials
cp .env.example .env
# Edit .env and fill in DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD

# 5. Create database tables
python db/setup_db.py

# 6. Verify everything is ready
python -m scripts.health_check
```

### `.env` file

```
DB_HOST=localhost
DB_PORT=5432
DB_NAME=football_db
DB_USER=postgres
DB_PASSWORD=your_password
```

---

## Running the Pipeline

### Full pipeline (recommended order)

```bash
# Step 1 — Download raw data (30 min – 3 hours depending on sources)
python -m scripts.scrape_only --transfermarkt
python -m scripts.scrape_only --understat
python -m scripts.scrape_only --statsbomb
python -m scripts.scrape_only --sofascore     # slowest (~2-3h)
# WhoScored: run scrapers/whoscored_scraper.py manually (see below)

# Step 2 — Load dimensions
python -m scripts.load_dimensions --teams
python -m scripts.load_dimensions --players
python -m scripts.load_dimensions --matches

# Step 3 — Load facts
python -m scripts.load_facts --injuries
python -m scripts.load_facts --shots
python -m scripts.load_facts --events

# Step 4 — Review unresolved players (optional)
python -m scripts.review_players --stats
```

### Using the full orchestrator

```bash
# Everything in one command (extract + load)
python pipeline_runner.py

# Skip re-downloading if raw data already exists
python pipeline_runner.py --skip-extract

# Dry run — shows what would happen without writing to DB
python pipeline_runner.py --dry-run

# Only specific sources
python pipeline_runner.py --sources transfermarkt understat
```

---

## Scrapers Reference

### Transfermarkt
**Method:** HTTP requests + BeautifulSoup  
**Anti-bot:** None (respectful delays 2–4s between requests)  
**Output:** `data/raw/transfermarkt/`  
**Seasons:** 2020 → 2024  
**Data collected:** Squad rosters, player profiles (name, position, nationality, DOB), injury history  
**Estimated time:** 10–15 minutes

```bash
python -m scripts.scrape_only --transfermarkt
# or directly:
python scrapers/transfermarkt_scraper.py
```

**Configuration** (in `scrapers/transfermarkt_scraper.py`):
```python
SEASONS = [2020, 2021, 2022, 2023, 2024]   # year = season start
```

---

### Understat
**Method:** Async HTTP (aiohttp)  
**Anti-bot:** None (with realistic headers + 1.5s delay)  
**Output:** `data/raw/understat/`  
**Seasons:** 2020 → 2024  
**Data collected:** Shot-level data with xG values, match results  
**Estimated time:** 15–20 minutes  
**Note:** Does not cover Champions League.

```bash
python -m scripts.scrape_only --understat
# or directly:
python scrapers/understat_scraper.py
```

---

### StatsBomb
**Method:** `statsbombpy` library (Open Data — no API key needed)  
**Anti-bot:** N/A (free public dataset)  
**Output:** `data/raw/statsbomb/`  
**Seasons:** 2020/21 → 2024/25 (competition ID 11 = La Liga)  
**Data collected:** Full event sequences with coordinates, lineups, detailed shot data  
**Estimated time:** 5–10 minutes  
**Note:** Open Data is limited to specific competitions. Coverage may be partial.

```bash
python -m scripts.scrape_only --statsbomb
# or directly:
python scrapers/statsbomb_scraper.py
```

---

### SofaScore
**Method:** Selenium + headless Chrome (unofficial API)  
**Anti-bot:** Mild — Chrome is used to bypass JS rendering  
**Output:** `data/raw/sofascore/`  
**Seasons:** Configurable via `SOFASCORE_SEASON_NAME`  
**Data collected:** Matches, shots, incidents (cards, substitutions, VAR decisions)  
**Estimated time:** 2–3 hours  
**Parallel workers:** 4 (configurable via `PARALLEL_WORKERS`)

```bash
python -m scripts.scrape_only --sofascore
# or directly:
python scrapers/sofascore_scraper.py
```

**Configuration** (in `scrapers/sofascore_scraper.py`):
```python
HEADLESS = True            # Set False to see the browser
PARALLEL_WORKERS = 4       # Simultaneous match requests
```

> **Important:** SofaScore events do not contain pitch coordinates. They are incident-type events only (substitutions, cards, VAR). Do not expect x/y values in `fact_events` rows with `data_source = 'sofascore'`.

---

### WhoScored
**Method:** Selenium + headless Chrome  
**Anti-bot:** Strong — WhoScored actively blocks automated access  
**Output:** `data/raw/whoscored/`  
**Seasons:** 2020/21 → 2025/26 (season URLs hardcoded)  
**Data collected:** Full event sequences with X/Y pitch coordinates  
**Estimated time:** Variable — may require manual intervention if blocked

```bash
python scrapers/whoscored_scraper.py
```

**Configuration** (in `scrapers/whoscored_scraper.py`):
```python
HEADLESS = False    # Recommended: False so you can intervene if blocked
DELAY_MIN = 3.0     # Seconds between requests
DELAY_MAX = 6.0
OUTPUT_DIR = 'data/raw/whoscored'   # Update this path if needed
```

**If blocked:** Set `HEADLESS = False`, complete any CAPTCHA manually in the opened browser window, then let the scraper continue.

**Season URLs** are hardcoded in `SEASON_URLS` dict — update if WhoScored changes their URL structure.

---

## Scripts Reference

All scripts are in the `scripts/` directory and run as Python modules from the project root.

| Script | Purpose | Key flags |
|---|---|---|
| `scrape_only.py` | Download raw data without touching DB | `--transfermarkt` `--understat` `--statsbomb` `--sofascore` `--all` |
| `load_dimensions.py` | Load dim_team, dim_player, dim_match | `--teams` `--players` `--matches` `--all` |
| `load_facts.py` | Load fact_shots, fact_events, fact_injuries | `--shots` `--events` `--injuries` `--all` |
| `review_players.py` | Inspect and manage the player_review queue | `--stats` `--unresolved` `--candidates` `--export` |
| `health_check.py` | Verify DB connection, schema, directories | `--verbose` `--fix` |
| `check_teams.py` | Inspect team name resolution issues | — |
| `query_players.py` | Search players in the DB | — |
| `resolve_players.py` | Batch-resolve player_review entries | — |
| `pipeline_runner.py` | Full orchestrator (extract + load) | `--skip-extract` `--dry-run` `--sources` |

---

## Player Review Queue

When the MDM engine processes a player name, it scores similarity against existing canonical players (0–100):

```
Score ≥ 85  →  Automatic match    →  linked to existing dim_player
Score 60–84 →  Manual review      →  inserted into player_review
Score < 60  →  New player         →  inserted as new dim_player row
```

### Checking the queue

```bash
# Summary statistics
python -m scripts.review_players --stats

# List unresolved cases
python -m scripts.review_players --unresolved

# Export to CSV for batch review
python -m scripts.review_players --export
```

### Resolving a case manually (SQL)

```sql
-- 1. Confirm a suggested match
UPDATE player_review
SET resolved = TRUE,
    canonical_id_assigned = <confirmed_player_id>
WHERE id = <review_id>;

-- 2. If the player is genuinely new, insert and resolve
INSERT INTO dim_player (name_canonical, nationality, birth_date, player_position)
VALUES ('Player Name', 'Spain', '1995-03-15', 'Delantero');

UPDATE player_review
SET resolved = TRUE,
    canonical_id_assigned = <new_player_id>
WHERE id = <review_id>;
```

**Scoring weights:**
- Name similarity (fuzzy): 40 pts
- Birth date exact match: 35 pts / ±1 year: 15 pts
- Nationality match: 15 pts
- Position match: 10 pts

---

## Troubleshooting

### `DB_PASSWORD environment variable not set`
Copy `.env.example` to `.env` and fill in your PostgreSQL credentials.

### `psycopg2` encoding errors with Spanish characters
PostgreSQL locale may be set to `Spanish_Spain.1252`. The connection in `loaders/common.py` already sets `client_encoding=utf8`. If errors persist, run in PostgreSQL:
```sql
ALTER SYSTEM SET lc_messages = 'en_US.UTF-8';
SELECT pg_reload_conf();
```

### `DECIMAL` overflow on coordinates
The schema uses `DECIMAL(7,4)` for x/y columns. If you see overflow errors on an older schema version, run:
```sql
ALTER TABLE fact_shots  ALTER COLUMN x TYPE DECIMAL(7,4),
                        ALTER COLUMN y TYPE DECIMAL(7,4),
                        ALTER COLUMN xg TYPE DECIMAL(7,4);
ALTER TABLE fact_events ALTER COLUMN x TYPE DECIMAL(7,4),
                        ALTER COLUMN y TYPE DECIMAL(7,4),
                        ALTER COLUMN end_x TYPE DECIMAL(7,4),
                        ALTER COLUMN end_y TYPE DECIMAL(7,4);
```

### SofaScore shots have NULL `team_id`
The SofaScore scraper does not always capture `team_id` in shot records. This is a known data gap — shots from Understat are the primary source for xG analysis.

### WhoScored blocks the scraper
Set `HEADLESS = False` in `scrapers/whoscored_scraper.py`. If a CAPTCHA appears, solve it manually. Increase `DELAY_MIN` / `DELAY_MAX` if blocks persist.

### `dim_season` not found when loading dim_match
Run `load_dimensions --teams` and `load_dimensions --players` before `--matches`. The season row must exist before matches can be linked to it.

---

## Project Structure

```
football_scrapping/
│
├── scrapers/               Raw data collection (no DB writes)
│   ├── transfermarkt_scraper.py
│   ├── sofascore_scraper.py
│   ├── understat_scraper.py
│   ├── statsbomb_scraper.py
│   └── whoscored_scraper.py
│
├── staging/                Load raw files into staging tables
│   ├── load_transfermarkt.py
│   ├── load_sofascore.py
│   ├── load_understat.py
│   ├── load_statsbomb.py
│   └── load_whoscored.py
│
├── loaders/                Load staging → dimensions + facts
│   ├── common.py           DB engine / connection
│   ├── player_loader.py
│   ├── team_loader.py
│   ├── match_loader.py
│   └── fact_loader.py
│
├── transform/              Alternative transform layer (pipeline_runner.py path)
│   ├── dim_players.py
│   ├── dim_teams.py
│   ├── dim_match.py
│   ├── fact_shots.py
│   ├── fact_events.py
│   └── fact_injuries.py
│
├── utils/                  Shared utilities
│   ├── mdm_engine.py       Name resolution engine
│   ├── mdm_config.py       Entity configuration
│   ├── canonical_teams.py  Team name normalization map
│   ├── player_matcher.py   Fuzzy scoring for player MDM
│   └── db.py               SQLAlchemy engine (pipeline_runner path)
│
├── scripts/                Runnable entry points
│   ├── scrape_only.py
│   ├── load_dimensions.py
│   ├── load_facts.py
│   ├── review_players.py
│   ├── health_check.py
│   └── pipeline_runner.py
│
├── db/
│   ├── create_tables.sql   Full DDL reference
│   └── setup_db.py         Creates DB schema via SQLAlchemy
│
├── data/
│   └── raw/                Raw scraped files (gitignored)
│       ├── transfermarkt/
│       ├── sofascore/
│       ├── understat/
│       ├── statsbomb/
│       └── whoscored/
│
├── pipeline_runner.py      Root-level orchestrator
├── requirements.txt
└── .env.example
```

---

## Data Source Summary

| Source | Method | Speed | Coordinates | Seasons | Champions League |
|---|---|---|---|---|---|
| Transfermarkt | HTTP + BS4 | Fast | No | 2020–2024 | Partial |
| Understat | Async HTTP | Fast | Yes (xG) | 2020–2024 | No |
| StatsBomb | Library | Fast | Yes (full) | 2020–2024 | Limited |
| SofaScore | Selenium | Slow | Incidents only | Configurable | Yes |
| WhoScored | Selenium | Variable | Yes (full) | 2020–2025 | Partial |
