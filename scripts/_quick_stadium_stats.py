"""Quick dim_stadium stats."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy import text
from loaders.common import engine

with engine.connect() as c:
    q = lambda sql: c.execute(text(sql)).scalar()
    print("total rows", q("SELECT COUNT(*) FROM dim_stadium"))
    print("TM rows", q("SELECT COUNT(*) FROM dim_stadium WHERE data_source = 'transfermarkt'"))
    print("TM with coords", q(
        "SELECT COUNT(*) FROM dim_stadium WHERE data_source = 'transfermarkt' AND latitude IS NOT NULL"
    ))
    print("TM without coords", q(
        "SELECT COUNT(*) FROM dim_stadium WHERE data_source = 'transfermarkt' AND latitude IS NULL"
    ))
    print("teams multi scd2", q(
        "SELECT COUNT(*) FROM ("
        "SELECT team_slug FROM dim_stadium WHERE data_source = 'transfermarkt' "
        "GROUP BY team_slug HAVING COUNT(*) > 1) x"
    ))
    print("match coverage", q(
        "SELECT COUNT(*) FROM dim_match WHERE stadium_id IS NOT NULL"
    ), "/", q("SELECT COUNT(*) FROM dim_match"))
