import pytest
from datetime import date

from utils.player_matcher import calculate_match_score


PLAYER_FULL = {
    "canonical_name": "Lionel Messi",
    "birth_date": date(1987, 6, 24),
    "nationality": "Argentina",
    "position": "Forward",
}


def test_perfect_match():
    score = calculate_match_score(PLAYER_FULL, PLAYER_FULL.copy())
    assert score == 100


def test_name_only_no_birth_date():
    a = {"canonical_name": "Lionel Messi", "birth_date": None, "nationality": "Argentina", "position": "Forward"}
    b = {"canonical_name": "Lionel Messi", "birth_date": None, "nationality": "Argentina", "position": "Forward"}
    score = calculate_match_score(a, b)
    # name=40, birth=0, nationality=15, position=10 → 65
    assert score == 65


def test_name_match_birth_date_absent_one_side():
    a = {"canonical_name": "Lionel Messi", "birth_date": date(1987, 6, 24), "nationality": "Argentina", "position": "Forward"}
    b = {"canonical_name": "Lionel Messi", "birth_date": None, "nationality": "Argentina", "position": "Forward"}
    score = calculate_match_score(a, b)
    # name=40, birth=0 (one side missing), nationality=15, position=10 → 65
    assert score == 65


def test_complete_mismatch():
    a = {"canonical_name": "John Smith", "birth_date": date(1990, 1, 1), "nationality": "England", "position": "Goalkeeper"}
    b = {"canonical_name": "Carlos Ruiz", "birth_date": date(1975, 8, 15), "nationality": "Guatemala", "position": "Forward"}
    score = calculate_match_score(a, b)
    assert score < 30


def test_birth_date_within_one_year():
    a = {**PLAYER_FULL, "birth_date": date(1987, 6, 24)}
    b = {**PLAYER_FULL, "birth_date": date(1988, 3, 10)}
    score = calculate_match_score(a, b)
    # name=40, birth=15 (±1yr), nationality=15, position=10 → 80
    assert score == 80


def test_birth_date_more_than_one_year_apart():
    a = {**PLAYER_FULL, "birth_date": date(1987, 6, 24)}
    b = {**PLAYER_FULL, "birth_date": date(1989, 7, 1)}
    score = calculate_match_score(a, b)
    # name=40, birth=0 (>1yr), nationality=15, position=10 → 65
    assert score == 65


def test_similar_name_scores_proportionally():
    a = {"canonical_name": "Cristiano Ronaldo", "birth_date": None, "nationality": None, "position": None}
    b = {"canonical_name": "Ronaldo Cristiano", "birth_date": None, "nationality": None, "position": None}
    score = calculate_match_score(a, b)
    # token_sort_ratio("cristiano ronaldo", "cristiano ronaldo") = 100 → name=40
    assert score == 40
