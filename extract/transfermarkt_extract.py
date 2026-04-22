import os
import json
import time
import logging
from datetime import datetime, date
from typing import Dict, List, Optional
from scrapers.transfermarkt import get_squad, get_player_injuries
from utils.batch import generate_batch_id

RAW_PATH = "data/raw/transfermarkt"

MAX_RETRIES = 3
RETRY_DELAY = 2

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def json_serializer(obj):
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")


def save_json(path: str, data: List[Dict]) -> bool:
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2, default=json_serializer)
        return True
    except Exception as e:
        logger.error(f"Error guardando {path}: {e}")
        return False


def scrape_team_with_retry(team_slug: str, team_id: int, season: int) -> Optional[List[Dict]]:
    for attempt in range(MAX_RETRIES):
        try:
            players = get_squad(team_slug, team_id, season)
            if players:
                return players
        except Exception as e:
            logger.warning(f"{team_slug} intento {attempt+1}: {e}")

        time.sleep(RETRY_DELAY * (attempt + 1))

    return None


def extract_transfermarkt(league_code: str = "ES1", season: int = 2020, TEAMS: Optional[Dict[str, int]] = None) -> Dict:
    batch_id = generate_batch_id()
    logger.info(f"EXTRACT START | league={league_code} | season={season} | batch_id={batch_id}")

    if not TEAMS:
        from scrapers.transfermarkt import get_league_teams
        TEAMS = get_league_teams(league_code, str(season))
        logger.info(f"Auto-descubiertos {len(TEAMS)} equipos para {league_code} {season}.")

    stats = {
        "batch_id": batch_id,
        "league": league_code,
        "season": season,
        "teams_processed": 0,
        "teams_failed": [],
        "total_players": 0,
        "total_injuries": 0,
        "start_time": datetime.now()
    }

    for team_slug, team_id in TEAMS.items():
        logger.info(f"Procesando {team_slug}")

        players = scrape_team_with_retry(team_slug, team_id, season)

        if not players:
            logger.error(f"{team_slug} sin datos")
            stats["teams_failed"].append(team_slug)
            continue

        injuries_all = []

        for p in players:
            p.update({
                "team_name": team_slug,
                "season": season,
                "batch_id": batch_id
            })

        for p in players:
            try:
                injuries = get_player_injuries(p["player_slug"], p["player_id"])

                for i in injuries:
                    i.update({
                        "team_name": team_slug,
                        "player_name": p["player_name"],
                        "player_id_tm": p["player_id"],
                        "season": season,
                        "batch_id": batch_id
                    })

                injuries_all.extend(injuries)
                time.sleep(0.5)

            except Exception as e:
                logger.warning(f"{p['player_name']} error: {e}")

        base_path = f"{RAW_PATH}/season={season}/{team_slug}/batch_id={batch_id}"

        ok_players = save_json(f"{base_path}/players.json", players)
        ok_injuries = save_json(f"{base_path}/injuries.json", injuries_all)

        if ok_players and ok_injuries:
            stats["teams_processed"] += 1
            stats["total_players"] += len(players)
            stats["total_injuries"] += len(injuries_all)
            logger.info(f"{team_slug}: {len(players)} players | {len(injuries_all)} injuries")
        else:
            stats["teams_failed"].append(team_slug)

    stats["end_time"] = datetime.now()
    stats["duration_seconds"] = (stats["end_time"] - stats["start_time"]).total_seconds()

    logger.info("=" * 60)
    logger.info("EXTRACT FINISHED")
    logger.info(f"League: {league_code} Season: {season}")
    logger.info(f"Batch: {batch_id}")
    logger.info(f"Teams OK: {stats['teams_processed']}")
    logger.info(f"Failed: {stats['teams_failed']}")
    logger.info(f"Players: {stats['total_players']}")
    logger.info(f"Injuries: {stats['total_injuries']}")
    logger.info(f"Duration: {stats['duration_seconds']:.1f}s")
    logger.info("=" * 60)

    return stats


def validate_extraction(batch_id: str, season: int = 2020) -> bool:
    base_path = f"{RAW_PATH}/season={season}"

    if not os.path.exists(base_path):
        logger.error(f"No existe {base_path}")
        return False

    errors = []
    ok = 0

    for team in os.listdir(base_path):
        path = os.path.join(base_path, team, f"batch_id={batch_id}")

        if not os.path.exists(path):
            continue

        players = os.path.exists(f"{path}/players.json")
        injuries = os.path.exists(f"{path}/injuries.json")

        if players and injuries:
            ok += 1
        else:
            errors.append(team)

    logger.info(f"Validacion: {ok} equipos OK | errores: {errors}")
    return len(errors) == 0


if __name__ == "__main__":
    TEAMS = {
        "real-madrid": 418,
        "fc-barcelona": 131,
    }

    stats = extract_transfermarkt("ES1", 2020, TEAMS)

    if stats["teams_processed"] > 0:
        validate_extraction(stats["batch_id"], 2020)