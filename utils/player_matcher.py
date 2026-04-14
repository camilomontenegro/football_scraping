from thefuzz import fuzz

def calculate_match_score(a, b):
    """
    Puntúa de 0 a 100 la probabilidad de que dos registros sean el mismo jugador.
    - Nombre:      40 puntos
    - Nacimiento:  35 puntos (exacto) / 15 puntos (±1 año)
    - Nacionalidad: 15 puntos
    - Posición:    10 puntos
    """
    score = 0

    # Nombre (40 puntos)
    name_score = fuzz.token_sort_ratio(
        a["canonical_name"].lower(),
        b["canonical_name"].lower()
    )
    score += int((name_score / 100) * 40)

    # Fecha de nacimiento (35 puntos)
    if a.get("birth_date") and b.get("birth_date"):
        diff = abs((a["birth_date"] - b["birth_date"]).days)
        if diff == 0:
            score += 35
        elif diff <= 365:
            score += 15

    # Nacionalidad (15 puntos)
    if a.get("nationality") and b.get("nationality"):
        if a["nationality"] == b["nationality"]:
            score += 15

    # Posición (10 puntos)
    if a.get("position") and b.get("position"):
        if a["position"] == b["position"]:
            score += 10

    return score

from db.models import DimPlayer, PlayerReview

def resolve_player(incoming, session, id_field, auto_threshold=85, review_threshold=60):
    """
    Busca en dim_player el jugador que mejor encaja con 'incoming'.
    - Score >= 85: match automático → actualiza el ID de la fuente
    - 60 <= Score < 85: revisión manual → inserta en PlayerReview
    - Score < 60 o sin candidatos: jugador nuevo → inserta en DimPlayer
    """
    candidates = session.query(DimPlayer).all()
    
    best_score = 0
    best_player = None
    
    for candidate in candidates:
        a = {
            "canonical_name": incoming["name"],
            "birth_date": incoming.get("birth_date"),
            "nationality": incoming.get("nationality"),
            "position": incoming.get("position"),
        }
        b = {
            "canonical_name": candidate.canonical_name,
            "birth_date": candidate.birth_date,
            "nationality": candidate.nationality,
            "position": candidate.position,
        }
        score = calculate_match_score(a, b)
        if score > best_score:
            best_score = score
            best_player = candidate

    # Match automático
    if best_player and best_score >= auto_threshold:
        setattr(best_player, id_field, incoming["source_id"])
        return best_player.canonical_id

    # Revisión manual
    if best_player and best_score >= review_threshold:
        review = PlayerReview(
            source_name=incoming["name"],
            source_system=incoming["source_system"],
            source_id=str(incoming["source_id"]),
            suggested_canonical_id=best_player.canonical_id,
            similarity_score=best_score,
            resolved=False,
        )
        session.add(review)
        return None

    # Jugador nuevo
    new_player = DimPlayer(canonical_name=incoming["name"])
    setattr(new_player, id_field, incoming["source_id"])
    if incoming.get("birth_date"):
        new_player.birth_date = incoming["birth_date"]
    if incoming.get("nationality"):
        new_player.nationality = incoming["nationality"]
    if incoming.get("position"):
        new_player.position = incoming["position"]
    session.add(new_player)
    session.flush()
    return new_player.canonical_id

