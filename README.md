# Football Scraping ETL

A Python ETL pipeline that extracts football data from five public sources, resolves player identities across sources, normalises coordinates, and loads everything into a star-schema relational database.

## Tech Stack

| Layer | Technology |
|---|---|
| Language | Python 3.10+ |
| Database | MySQL (prod) / SQLite (local dev) |
| ORM | SQLAlchemy 2.x |
| Scraping | requests, BeautifulSoup4, Selenium |
| Data processing | pandas |
| Name matching | thefuzz |

## Project Structure

```
football_scrapping/
├── data/raw/              # Raw downloaded JSON (gitignored)
├── db/
│   ├── models.py          # SQLAlchemy ORM models + engine/session helpers
│   └── create_tables.sql  # MySQL DDL reference (informational)
├── scrapers/
│   ├── statsbomb_loader.py
│   ├── understat_scraper.py
│   ├── sofascore_scraper.py
│   ├── transfermarkt_scraper.py
│   └── whoscored_scraper.py
├── utils/
│   ├── helpers.py         # normalize_coords, parse_date
│   └── player_matcher.py  # calculate_match_score, resolve_player
└── tests/
```

## Setup

```bash
# 1. Clone and enter the project
git clone <repo-url>
cd football_scrapping

# 2. Create virtual environment
python -m venv venv
source venv/bin/activate       # Windows: venv\Scripts\activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Configure environment
cp .env.example .env
# Edit .env — set DATABASE_URL to MySQL or SQLite connection string

# 5. Create database tables
python -c "from db.models import get_engine, Base; Base.metadata.create_all(get_engine()); print('Done')"

# 6. Verify connection
python -c "from db.models import get_engine; get_engine().connect(); print('OK')"
```

## Running the Scrapers

Run scrapers **in order** — StatsBomb must run first to seed `dim_player`.

### 1. StatsBomb (seeds dim_player, fact_shots, fact_events)

```bash
# Load La Liga 2020/21 (competition 11, season 90)
python -m scrapers.statsbomb_loader --competition-id 11 --season-id 90

# List available competitions
python -c "from statsbombpy import sb; print(sb.competitions()[['competition_id','season_id','competition_name','season_name']])"
```

### 2. Understat (shots with xG)

```bash
# Load specific matches
python -m scrapers.understat_scraper --match-ids 14090 14091 14092

# Load a full league/season (fetches match list first)
python -m scrapers.understat_scraper --league EPL --season 2021
```

### 3. SofaScore (match events)

```bash
python -m scrapers.sofascore_scraper --match-ids 10000000 10000001
```

### 4. Transfermarkt (injury history)

```bash
# Transfermarkt player IDs (found in player profile URLs)
python -m scrapers.transfermarkt_scraper --player-ids 28003 8198 25557
```

### 5. WhoScored (match events via Selenium)

Requires Chrome and ChromeDriver installed.

```bash
python -m scrapers.whoscored_scraper --match-ids 1657100 1657101
```

## Running Tests

```bash
pytest tests/ -v
```

## Handling player_review

Players that score 60–84 against `dim_player` candidates are placed in `player_review` with `resolved=FALSE`. To resolve them manually:

```sql
-- Inspect ambiguous matches
SELECT pr.id, pr.source_name, pr.source_system, pr.similarity_score,
       dp.canonical_name, dp.birth_date
FROM player_review pr
LEFT JOIN dim_player dp ON pr.suggested_canonical_id = dp.canonical_id
WHERE pr.resolved = FALSE
ORDER BY pr.similarity_score DESC;

-- Confirm a match
UPDATE player_review SET resolved = TRUE, canonical_id_assigned = <canonical_id> WHERE id = <pr_id>;
-- Then update dim_player manually if the source ID column is still NULL
UPDATE dim_player SET id_understat = <source_id> WHERE canonical_id = <canonical_id>;
```

## Environment Variables

| Variable | Description |
|---|---|
| `DATABASE_URL` | SQLAlchemy connection URL. MySQL: `mysql+pymysql://user:pass@host/db`; SQLite: `sqlite:///data/football.db` |

## Coordinate Systems

All coordinates are stored in metres on a 105×68 pitch:

| Source | Raw system | Conversion |
|---|---|---|
| StatsBomb | 0–120 / 0–80 | × (105/120) / × (68/80) |
| Understat | 0–1 normalised | × 105 / × 68 |
| SofaScore | 0–100 % | × (105/100) / × (68/100) |
| WhoScored | 0–100 % | × (105/100) / × (68/100) |
