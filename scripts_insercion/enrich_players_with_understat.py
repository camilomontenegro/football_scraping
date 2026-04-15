# populate_understat_player_ids.py (corregido)
import json
import logging
import sys
from pathlib import Path
from thefuzz import process, fuzz

ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT))

from db.connection import get_connection

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

TEAMS_OF_INTEREST = frozenset({'Real Madrid', 'Barcelona'})
THRESHOLD = 85


class PlayerMatcher:
    """Matching de jugadores entre Understat y SofaScore."""

    def __init__(self, sofa_players: dict, threshold: int = THRESHOLD):
        self.threshold = threshold
        self.sofa_players = sofa_players
        self._sofa_names = list(sofa_players.keys())

    @staticmethod
    def _normalize(name: str) -> str:
        if not name:
            return ""
        for src, dst in [
            ('á','a'),('é','e'),('í','i'),('ó','o'),('ú','u'),('ñ','n'),
            (' Jr.',''),(' II',''),
        ]:
            name = name.replace(src, dst)
        return name.strip().lower()

    def find_best_match(self, understat_name: str) -> tuple[str | None, int]:
        understat_norm = self._normalize(understat_name)

        for sofa_name in self._sofa_names:
            if self._normalize(sofa_name) == understat_norm:
                return sofa_name, 100

        best_match, score = process.extractOne(
            understat_name,
            self._sofa_names,
            scorer=fuzz.token_sort_ratio
        )
        if score >= self.threshold:
            return best_match, score

        return None, 0


def load_sofascore_players() -> dict:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT player_id, name_canonical, id_sofascore
                FROM dim_player
                WHERE id_sofascore IS NOT NULL
            """)
            players = {
                name: {'player_id': pid, 'id_sofascore': sofa_id}
                for pid, name, sofa_id in cur.fetchall()
            }
    logger.info("Jugadores SofaScore cargados: %d", len(players))
    return players


def extract_understat_players(season: int = 2020) -> dict:
    """Extrae SOLO jugadores de Real Madrid y Barcelona desde Understat"""
    understat_file = ROOT / f"data/raw/understat/shots_La_Liga_{season}.json"

    if not understat_file.exists():
        logger.error("Archivo Understat no encontrado: %s", understat_file)
        return {}

    with open(understat_file, encoding='utf-8') as f:
        shots = json.load(f)

    players: dict = {}
    for shot in shots:
        home = shot.get('home_team', '')
        away = shot.get('away_team', '')
        player_name = shot.get('player')
        player_id = shot.get('player_id')
        h_a = shot.get('h_a', '')  # 'h' = local, 'a' = visitante

        if not player_name or not player_id:
            continue

        # Solo jugadores que pertenecen a Madrid o Barcelona
        es_interes = False
        if h_a == 'h' and home in TEAMS_OF_INTEREST:
            es_interes = True
        elif h_a == 'a' and away in TEAMS_OF_INTEREST:
            es_interes = True

        if not es_interes:
            continue

        if player_id not in players:
            players[player_id] = player_name

    logger.info("Jugadores Understat (solo %s): %d", 
                ', '.join(TEAMS_OF_INTEREST), len(players))
    return players


def batch_update_understat_ids(matches: list[dict]) -> int:
    updated = 0
    with get_connection() as conn:
        with conn.cursor() as cur:
            for match in matches:
                cur.execute("""
                    UPDATE dim_player
                    SET id_understat = %s
                    WHERE player_id = %s
                      AND (id_understat IS NULL OR id_understat != %s)
                """, (match['understat_id'], match['player_id'], match['understat_id']))
                if cur.rowcount > 0:
                    updated += 1
                    logger.info("Actualizado: '%s' -> '%s' (score=%s, understat_id=%s)",
                                match['understat_name'], match['sofa_name'],
                                match['score'], match['understat_id'])
            conn.commit()
            cur.execute("SELECT COUNT(*), COUNT(id_understat) FROM dim_player")
            total, con_understat = cur.fetchone()

    logger.info("=" * 50)
    logger.info("RESUMEN FINAL")
    logger.info("=" * 50)
    logger.info("Total jugadores en dim_player : %d", total)
    logger.info("Con id_understat              : %d", con_understat)
    logger.info("Actualizados en esta ejecución: %d", updated)
    return updated


def populate_understat_ids(season: int = 2020) -> None:
    understat_players = extract_understat_players(season=season)
    if not understat_players:
        return

    sofa_players = load_sofascore_players()
    matcher = PlayerMatcher(sofa_players, threshold=THRESHOLD)

    matches = []
    for understat_id, understat_name in understat_players.items():
        best_match, score = matcher.find_best_match(understat_name)
        if best_match:
            player_data = sofa_players[best_match]
            matches.append({
                'understat_id': understat_id,
                'understat_name': understat_name,
                'player_id': player_data['player_id'],
                'sofa_name': best_match,
                'score': score,
            })

    logger.info("Matches encontrados: %d de %d posibles", len(matches), len(understat_players))
    batch_update_understat_ids(matches)


if __name__ == "__main__":
    populate_understat_ids(season=2020)