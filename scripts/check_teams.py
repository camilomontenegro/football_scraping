from loaders.common import engine
from sqlalchemy import text

with engine.connect() as conn:
    print("TEMPORADAS EN dim_match:")
    res = conn.execute(text("SELECT DISTINCT season FROM dim_match"))
    for r in res:
        print(f"Season: {r[0]}")
