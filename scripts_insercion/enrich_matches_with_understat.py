# enrich_matches_with_team_normalizer.py
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

# Equipos de interés — ampliar aquí sin tocar lógica
TEAMS_OF_INTEREST = frozenset({'Real Madrid', 'Barcelona'})


class TeamMatcher:
    """Normalizador de nombres de equipos con mapeo manual + fuzzy matching."""

    TEAM_MAPPING = {
        'FC Barcelona': 'Barcelona',
        'Barcelona': 'Barcelona',
        'Real Madrid': 'Real Madrid',
        'Atlético Madrid': 'Atletico Madrid',
        'Athletic Club': 'Athletic Club',
        'Athletic Bilbao': 'Athletic Club',
        'Real Betis': 'Real Betis',
        'Betis': 'Real Betis',
        'Sevilla': 'Sevilla',
        'Valencia': 'Valencia',
        'Villarreal': 'Villarreal',
        'Real Sociedad': 'Real Sociedad',
        'Getafe': 'Getafe',
        'Granada': 'Granada',
        'Celta Vigo': 'Celta Vigo',
        'Celta': 'Celta Vigo',
        'Levante UD': 'Levante',
        'Levante': 'Levante',
        'Osasuna': 'Osasuna',
        'Deportivo Alavés': 'Alaves',
        'Alavés': 'Alaves',
        'Alaves': 'Alaves',
        'Huesca': 'Huesca',
        'SD Huesca': 'Huesca',
        'Elche': 'Elche',
        'Cádiz': 'Cadiz',
        'Cadiz': 'Cadiz',
        'Real Valladolid': 'Real Valladolid',
        'Valladolid': 'Real Valladolid',
        'Eibar': 'Eibar',
    }

    def __init__(self, threshold: int = 85):
        self.threshold = threshold
        self.canonical_names = list(set(self.TEAM_MAPPING.values()))

    def normalize(self, name: str) -> str | None:
        """Normaliza un nombre de equipo a su forma canónica."""
        if not name:
            return None

        name_clean = name.strip()

        # 1. Mapeo directo exacto
        if name_clean in self.TEAM_MAPPING:
            return self.TEAM_MAPPING[name_clean]

        # 2. Fuzzy matching contra nombres canónicos
        best_match, score = process.extractOne(
            name_clean,
            self.canonical_names,
            scorer=fuzz.token_sort_ratio
        )

        if score >= self.threshold:
            return best_match

        # 3. Sin match: devolver el nombre original para no perder datos
        logger.warning("Sin normalización para '%s' (mejor score fuzzy: %d)", name_clean, score)
        return name_clean


def load_understat_matches(team_matcher: TeamMatcher, season: int = 2020) -> dict:
    """
    Carga partidos desde el JSON de Understat y normaliza nombres de equipos.

    Retorna un dict: (home_norm, away_norm) -> {id_understat, scores, ...}
    Solo incluye partidos donde al menos un equipo esté en TEAMS_OF_INTEREST.
    """
    understat_file = ROOT / f"data/raw/understat/shots_La_Liga_{season}.json"

    if not understat_file.exists():
        logger.error("Archivo Understat no encontrado: %s", understat_file)
        return {}

    with open(understat_file, encoding='utf-8') as f:
        shots = json.load(f)

    matches: dict = {}

    for shot in shots:
        match_id = shot.get('match_id')
        if not match_id:
            continue

        home_raw = shot.get('home_team')
        away_raw = shot.get('away_team')

        home_norm = team_matcher.normalize(home_raw)
        away_norm = team_matcher.normalize(away_raw)

        # Filtrar solo partidos que involucren a los equipos de interés
        if not TEAMS_OF_INTEREST & {home_norm, away_norm}:
            continue

        key = (home_norm, away_norm)
        if key not in matches:
            home_score = shot.get('h_goals')
            away_score = shot.get('a_goals')
            matches[key] = {
                'id_understat': int(match_id),
                'home_team_raw': home_raw,
                'away_team_raw': away_raw,
                'home_score': int(home_score) if home_score is not None else None,
                'away_score': int(away_score) if away_score is not None else None,
                'match_date': shot.get('date'),
            }

    logger.info("Partidos Understat normalizados: %d", len(matches))
    return matches


def _find_understat_data(
    home_norm: str,
    away_norm: str,
    understat_matches: dict,
) -> dict | None:
    """
    Busca el partido en el dict de Understat probando ambos órdenes.
    Siempre devuelve una COPIA para evitar mutar el dict original.
    """
    # Orden directo
    if (home_norm, away_norm) in understat_matches:
        return dict(understat_matches[(home_norm, away_norm)])

    # Orden inverso: intercambiar scores en la copia
    if (away_norm, home_norm) in understat_matches:
        data = dict(understat_matches[(away_norm, home_norm)])
        data['home_score'], data['away_score'] = data['away_score'], data['home_score']
        return data

    return None


def enrich_matches(season: int = 2020) -> None:
    """Enriquece dim_match con id_understat usando normalización de nombres."""

    team_matcher = TeamMatcher(threshold=85)
    understat_matches = load_understat_matches(team_matcher, season=season)

    if not understat_matches:
        logger.warning("No hay datos de Understat. Abortando.")
        return

    with get_connection() as conn:
        with conn.cursor() as cur:

            cur.execute("""
                SELECT match_id, home_team, away_team, home_score, away_score
                FROM dim_match
            """)
            db_matches = cur.fetchall()
            logger.info("Partidos en dim_match: %d", len(db_matches))

            updated_understat = 0
            updated_scores = 0

            for match_id, home_db, away_db, current_home_score, current_away_score in db_matches:

                home_norm = team_matcher.normalize(home_db)
                away_norm = team_matcher.normalize(away_db)

                understat_data = _find_understat_data(home_norm, away_norm, understat_matches)

                if not understat_data:
                    logger.debug(
                        "No encontrado: %s vs %s (norm: %s vs %s)",
                        home_db, away_db, home_norm, away_norm
                    )
                    continue

                understat_id = understat_data['id_understat']

                # Comprobar que este id_understat no esté ya asignado a otro partido
                cur.execute("""
                    SELECT match_id FROM dim_match
                    WHERE id_understat = %s AND match_id <> %s
                """, (understat_id, match_id))

                conflict = cur.fetchone()
                if conflict:
                    logger.warning(
                        "Conflicto: id_understat=%s ya está asignado al match_id=%s. "
                        "Saltando %s vs %s (match_id=%s).",
                        understat_id, conflict[0], home_db, away_db, match_id
                    )
                    continue

                # Actualizar id_understat si está vacío
                cur.execute("""
                    UPDATE dim_match
                    SET id_understat = %s
                    WHERE match_id = %s AND id_understat IS NULL
                """, (understat_id, match_id))

                if cur.rowcount > 0:
                    updated_understat += 1
                    logger.info(
                        "Actualizado id_understat: %s vs %s -> %s",
                        home_db, away_db, understat_id
                    )

                # Completar resultados nulos
                missing_scores = current_home_score is None or current_away_score is None
                has_understat_scores = (
                    understat_data['home_score'] is not None
                    and understat_data['away_score'] is not None
                )

                if missing_scores and has_understat_scores:
                    cur.execute("""
                        UPDATE dim_match
                        SET home_score = %s, away_score = %s
                        WHERE match_id = %s
                          AND (home_score IS NULL OR away_score IS NULL)
                    """, (understat_data['home_score'], understat_data['away_score'], match_id))

                    if cur.rowcount > 0:
                        updated_scores += 1
                        logger.info(
                            "Completado resultado: %s %s - %s %s",
                            home_db,
                            understat_data['home_score'],
                            understat_data['away_score'],
                            away_db,
                        )

            conn.commit()

            # Resumen final
            cur.execute("""
                SELECT
                    COUNT(*)                                                        AS total,
                    COUNT(id_understat)                                             AS con_understat,
                    COUNT(CASE WHEN home_score IS NULL OR away_score IS NULL THEN 1 END) AS con_nulos
                FROM dim_match
            """)
            total, con_understat, con_nulos = cur.fetchone()

            logger.info("=" * 50)
            logger.info("RESUMEN")
            logger.info("=" * 50)
            logger.info("Partidos con id_understat actualizado : %d", updated_understat)
            logger.info("Partidos con resultado completado     : %d", updated_scores)
            logger.info("Estado final — total: %d | con understat: %d | nulos: %d",
                        total, con_understat, con_nulos)


if __name__ == "__main__":
    enrich_matches(season=2020)