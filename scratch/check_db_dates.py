from loaders.common import engine
from sqlalchemy import text
import logging

logging.basicConfig(level=logging.ERROR)

def check_dates():
    with engine.connect() as conn:
        res = conn.execute(text('SELECT competition, season, MAX(match_date) FROM dim_match GROUP BY competition, season')).fetchall()
        for row in res:
            print(f"Comp: {row[0]} | Season: {row[1]} | Last Date: {row[2]}")

if __name__ == "__main__":
    check_dates()
