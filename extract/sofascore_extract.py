from pathlib import Path
from utils.batch import generate_batch_id
from extract.base_extractor import save_json
from scrapers.sofascore import *


def run_sofascore_extract(season_name: str, tournament_id: int = 8) -> str:

    driver = create_driver()
    batch_id = generate_batch_id()

    try:
        season_id, season_label = get_season_id(driver, tournament_id, season_name)
        if not season_id:
            raise ValueError(f"Season {season_name} not found")

        matches = get_matches(driver, tournament_id, season_id)

        base_path = Path(f"data/raw/sofascore/season={season_label}")
        base_path.mkdir(parents=True, exist_ok=True)

        # GUARDAR MATCHES (CRÍTICO)
        save_json(matches, base_path / f"matches_batch_{batch_id}.json")

        processed = 0

        for m in matches:

            match_id = m["id"]

            match_dir = base_path / f"match_{match_id}" / f"batch_id={batch_id}"
            match_dir.mkdir(parents=True, exist_ok=True)

            print(f"\nMatch {match_id} → {m['homeTeam']['name']} vs {m['awayTeam']['name']}")

            # ─────────────────────────────
            # SHOTS
            # ─────────────────────────────
            try:
                shots = get_match_shots(driver, match_id)
                save_json(shots, match_dir / "shots.json")
            except Exception as e:
                print(f"Shots failed: {e}")

            # ─────────────────────────────
            # EVENTS
            # ─────────────────────────────
            try:
                events = get_match_events(driver, match_id)
                save_json(events, match_dir / "events.json")
            except Exception as e:
                print(f"Events failed: {e}")

            # ─────────────────────────────
            # LINEUPS
            # ─────────────────────────────
            try:
                lineups = get_match_lineups(driver, match_id)
                save_json(lineups, match_dir / "lineups.json")
            except Exception as e:
                print(f"Lineups failed: {e}")

            processed += 1

        print(f"\nExtraction completed → batch={batch_id} | matches={processed}")
        return batch_id

    finally:
        driver.quit()