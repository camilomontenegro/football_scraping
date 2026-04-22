from utils.field_precedence_config import FIELD_PRECEDENCE


def get_source_rank(entity: str, field: str, source: str):
    precedence = FIELD_PRECEDENCE.get(entity, {}).get(field, [])
    try:
        return precedence.index(source)
    except ValueError:
        return 999


def should_update(entity: str, field: str, current_source: str, new_source: str):

    current_rank = get_source_rank(entity, field, current_source)
    new_rank = get_source_rank(entity, field, new_source)

    return new_rank < current_rank


def pick_best_value(entity: str, field: str, candidates: list):

    if not candidates:
        return None

    precedence = FIELD_PRECEDENCE.get(entity, {}).get(field, [])

    def rank(c):
        try:
            return precedence.index(c["source"])
        except ValueError:
            return 999

    sorted_candidates = sorted(candidates, key=rank)
    return sorted_candidates[0]["value"]