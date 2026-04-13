"""Integration tests for resolve_player — uses an in-memory SQLite database."""
import os
import pytest
from datetime import date

os.environ.setdefault("DATABASE_URL", "sqlite:///:memory:")

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from db.models import Base, DimPlayer, PlayerReview
from utils.player_matcher import resolve_player


@pytest.fixture()
def session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    s = Session()
    yield s
    s.close()


@pytest.fixture()
def seeded_session(session):
    """Seed one StatsBomb-like player into dim_player."""
    player = DimPlayer(
        canonical_name="Lionel Messi",
        birth_date=date(1987, 6, 24),
        nationality="Argentina",
        position="Forward",
        id_statsbomb="dummy-uuid-messi",
    )
    session.add(player)
    session.commit()
    return session


# --- 5.1: callable from scraper context ---
def test_resolve_player_callable(seeded_session):
    result = resolve_player(
        {"name": "Lionel Messi", "birth_date": date(1987, 6, 24), "nationality": "Argentina", "position": "Forward", "source_id": 999, "source_system": "understat"},
        seeded_session,
        "id_understat",
    )
    assert result is not None


# --- 5.2: auto-match returns existing canonical_id and updates source ID ---
def test_auto_match_high_score(seeded_session):
    canonical_id = resolve_player(
        {"name": "Lionel Messi", "birth_date": date(1987, 6, 24), "nationality": "Argentina", "position": "Forward", "source_id": 42, "source_system": "understat"},
        seeded_session,
        "id_understat",
    )
    seeded_session.commit()

    player = seeded_session.query(DimPlayer).filter_by(canonical_id=canonical_id).first()
    assert player is not None
    assert player.id_understat == 42
    assert canonical_id == player.canonical_id


# --- 5.3: review-queue path returns None and inserts player_review row ---
def test_review_queue_mid_score(seeded_session):
    # Exact same name (40) + birth within 1yr (15) + same nationality (15) + no position (0) = 70 → review
    result = resolve_player(
        {"name": "Lionel Messi", "birth_date": date(1988, 6, 1), "nationality": "Argentina", "position": None, "source_id": 77, "source_system": "sofascore"},
        seeded_session,
        "id_sofascore",
    )
    seeded_session.commit()

    assert result is None
    review = seeded_session.query(PlayerReview).first()
    assert review is not None
    assert review.resolved is False
    assert review.similarity_score is not None


# --- new player insert (<60) ---
def test_new_player_insert(session):
    """Empty dim_player → any player resolves as new insert."""
    result = resolve_player(
        {"name": "Unknown Player", "birth_date": None, "nationality": None, "position": None, "source_id": 1, "source_system": "understat"},
        session,
        "id_understat",
    )
    session.commit()
    assert result is not None
    player = session.query(DimPlayer).filter_by(canonical_id=result).first()
    assert player.canonical_name == "Unknown Player"
    assert player.id_understat == 1
