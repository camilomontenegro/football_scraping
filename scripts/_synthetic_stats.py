import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from sqlalchemy import text
from loaders.common import engine

with engine.connect() as c:
    auto = c.execute(text(
        "SELECT COUNT(*) FROM dim_stadium WHERE data_source='synthetic-geocode' "
        "AND stadium_name ILIKE '%(auto geocoded)%'"
    )).scalar()
    syn = c.execute(text(
        "SELECT COUNT(*) FROM dim_stadium WHERE data_source='synthetic-geocode'"
    )).scalar()
    clubish = c.execute(text("""
        SELECT COUNT(*) FROM dim_stadium s
        JOIN dim_team t ON t.canonical_id = s.canonical_team_id
        WHERE s.data_source='synthetic-geocode'
          AND LOWER(TRIM(s.stadium_name)) = LOWER(TRIM(t.canonical_name))
    """)).scalar()
    print("synthetic total", syn)
    print("placeholder (auto geocoded)", auto)
    print("stadium_name = team name", clubish)
    print("\n--- placeholder samples ---")
    for r in c.execute(text(
        "SELECT team_slug, stadium_name FROM dim_stadium "
        "WHERE data_source='synthetic-geocode' AND stadium_name ILIKE '%(auto geocoded)%' LIMIT 10"
    )):
        print(r)
    print("\n--- club-as-stadium samples ---")
    for r in c.execute(text("""
        SELECT s.team_slug, s.stadium_name
        FROM dim_stadium s JOIN dim_team t ON t.canonical_id=s.canonical_team_id
        WHERE s.data_source='synthetic-geocode'
          AND LOWER(TRIM(s.stadium_name)) = LOWER(TRIM(t.canonical_name))
        LIMIT 10
    """)):
        print(r)
