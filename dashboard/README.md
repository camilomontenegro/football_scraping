# Dashboard

Streamlit dashboard for exploring and monitoring football data. All tabs are
read-only **except for the Wizard tab**, which launches the scraping pipeline
and therefore writes to the database.

## Run

    cd football_scraping
    streamlit run dashboard/app.py

Opens at http://localhost:8501.

## Tabs

### Exploration
Select competition + season + team (or "All teams") to browse:
- Results — match outcomes, W/D/L record for selected team
- Player stats — goals, xG, shots per player (all sources combined)
- Shots by source — shot counts and xG per data source, with bar chart
- Injuries — injury records with days absent and matches missed
- Events — event type breakdown per source

### Pipeline monitoring
- Live DB counts (players / matches / shots / injuries)
- Season scanner — detects missing seasons from StatsBomb, Understat, SofaScore
- Coverage per source for any season, with progress bars
- Player review queue stats and table
- Recent matches

### Wizard
Stepwise selectors (operation → competition → season → source → match filter →
confirm) that launch the scraping pipeline by calling
`wizard.pipeline_runner.run_pipeline`. The pipeline's log streams into the tab
and persists to `data/logs/wizard_latest_log.txt`. Selecting a team filter
produces a CSV download after the run completes. This is the only tab that
writes to the database; the others stay read-only.

## Notes

- This dashboard is **read-only**. To load data, run `python pipeline_runner.py` from
  `football_scraping/`. The dashboard never triggers scrapers or staging loaders.
- SofaScore events have NULL coordinates by design (incident-only source).
- Source IDs (StatsBomb, Understat, SofaScore) come from constants in
  `pipeline_runner.py`. Adding a new competition requires extending those constants
  and `dashboard/explore.get_competitions()`.
- Transfermarkt and WhoScored are surfaced informationally only; their season
  scanners always return `[]`.

## Troubleshooting

- **DB connection error** — check `.env` at `football_scraping/.env`
- **Empty tables / "No data found"** — run `python pipeline_runner.py` to load data first
- **ImportError** — launch from the `football_scraping/` directory, not from inside `dashboard/`
- **Scanner timeout** — SofaScore may be rate-limiting; retry in a few minutes
