import os
from contextlib import contextmanager

from dotenv import load_dotenv
from sqlalchemy import (
    Boolean,
    Column,
    Date,
    DateTime,
    ForeignKey,
    Integer,
    Numeric,
    SmallInteger,
    String,
    create_engine,
    func,
)
from sqlalchemy.orm import DeclarativeBase, sessionmaker

load_dotenv()


def get_engine():
    url = os.environ.get("DATABASE_URL")
    if not url:
        raise EnvironmentError(
            "DATABASE_URL is not set. Copy .env.example to .env and fill in your credentials."
        )
    return create_engine(url)


def get_session():
    engine = get_engine()
    Session = sessionmaker(bind=engine)
    return Session()


@contextmanager
def session_scope():
    session = get_session()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


class Base(DeclarativeBase):
    pass


class DimPlayer(Base):
    __tablename__ = "dim_player"

    player_id = Column(Integer, primary_key=True, autoincrement=True)
    name_canonical = Column(String(150), nullable=False)
    nationality = Column(String(80), nullable=True)
    birth_date = Column(Date, nullable=True)
    player_position = Column(String(50), nullable=True)
    id_sofascore = Column(Integer, nullable=True)
    id_understat = Column(Integer, nullable=True)
    id_transfermarkt = Column(Integer, nullable=True)
    id_statsbomb = Column(String(50), nullable=True)
    id_whoscored = Column(Integer, nullable=True)
    created_at = Column(DateTime, server_default=func.now())


class DimTeam(Base):
    __tablename__ = "dim_team"

    team_id = Column(Integer, primary_key=True, autoincrement=True)
    name_canonical = Column(String(150), nullable=False)
    country = Column(String(80), nullable=True)
    id_sofascore = Column(Integer, nullable=True)
    id_statsbomb = Column(Integer, nullable=True)
    id_understat = Column(Integer, nullable=True)
    id_whoscored = Column(Integer, nullable=True)
    id_transfermarkt = Column(Integer, nullable=True)
    created_at = Column(DateTime, server_default=func.now())


class DimMatch(Base):
    __tablename__ = "dim_match"

    match_id = Column(Integer, primary_key=True, autoincrement=True)
    match_date = Column(Date, nullable=True)
    competition = Column(String(100), nullable=True)
    season = Column(String(20), nullable=True)
    home_team = Column(String(100), nullable=True)
    away_team = Column(String(100), nullable=True)
    home_score = Column(SmallInteger, nullable=True)
    away_score = Column(SmallInteger, nullable=True)
    data_source = Column(String(50), nullable=True)
    id_sofascore = Column(Integer, nullable=True)
    id_understat = Column(Integer, nullable=True)
    id_statsbomb = Column(Integer, nullable=True)
    id_whoscored = Column(Integer, nullable=True)


class FactShots(Base):
    __tablename__ = "fact_shots"

    shot_id = Column(Integer, primary_key=True, autoincrement=True)
    match_id = Column(Integer, ForeignKey("dim_match.match_id"), nullable=True)
    player_id = Column(Integer, ForeignKey("dim_player.player_id"), nullable=True)
    team_id = Column(Integer, ForeignKey("dim_team.team_id"), nullable=True)
    minute = Column(SmallInteger, nullable=True)
    x = Column(Numeric(6, 4), nullable=True)
    y = Column(Numeric(6, 4), nullable=True)
    xg = Column(Numeric(6, 4), nullable=True)
    result = Column(String(30), nullable=True)
    shot_type = Column(String(30), nullable=True)
    situation = Column(String(50), nullable=True)
    data_source = Column(String(30), nullable=True)


class FactEvents(Base):
    __tablename__ = "fact_events"

    event_id = Column(Integer, primary_key=True, autoincrement=True)
    match_id = Column(Integer, ForeignKey("dim_match.match_id"), nullable=True)
    player_id = Column(Integer, ForeignKey("dim_player.player_id"), nullable=True)
    team_id = Column(Integer, ForeignKey("dim_team.team_id"), nullable=True)
    event_type = Column(String(50), nullable=True)
    minute = Column(SmallInteger, nullable=True)
    second = Column(SmallInteger, nullable=True)
    x = Column(Numeric(6, 4), nullable=True)
    y = Column(Numeric(6, 4), nullable=True)
    end_x = Column(Numeric(6, 4), nullable=True)
    end_y = Column(Numeric(6, 4), nullable=True)
    outcome = Column(String(50), nullable=True)
    data_source = Column(String(30), nullable=True)


class FactInjuries(Base):
    __tablename__ = "fact_injuries"

    injury_id = Column(Integer, primary_key=True, autoincrement=True)
    player_id = Column(Integer, ForeignKey("dim_player.player_id"), nullable=True)
    season = Column(String(20), nullable=True)
    injury_type = Column(String(200), nullable=True)
    date_from = Column(Date, nullable=True)
    date_until = Column(Date, nullable=True)
    days_absent = Column(Integer, nullable=True)
    matches_missed = Column(SmallInteger, nullable=True)


class PlayerReview(Base):
    __tablename__ = "player_review"

    id = Column(Integer, primary_key=True, autoincrement=True)
    source_name = Column(String(150), nullable=True)
    source_system = Column(String(50), nullable=True)
    source_id = Column(String(50), nullable=True)
    suggested_player_id = Column(Integer, ForeignKey("dim_player.player_id"), nullable=True)
    similarity_score = Column(SmallInteger, nullable=True)
    resolved = Column(Boolean, default=False)
    player_id_assigned = Column(Integer, ForeignKey("dim_player.player_id"), nullable=True)
