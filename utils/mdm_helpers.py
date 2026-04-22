def get_entity_id(result):
    if isinstance(result, dict):
        return result.get("id")
    return result


def get_confidence(result):
    if isinstance(result, dict):
        return result.get("confidence", 100)
    return 100


def get_match_type(result):
    if isinstance(result, dict):
        return result.get("match_type", "unknown")
    return "unknown"