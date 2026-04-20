from sqlalchemy import text

def load_dim_injury_types(conn):
    conn.execute(text("""
        INSERT INTO dim_injury_type (name)
        SELECT DISTINCT injury_type
        FROM stg_transfermarkt_injuries
        WHERE injury_type IS NOT NULL
        AND NOT EXISTS (
            SELECT 1
            FROM dim_injury_type d
            WHERE d.name = injury_type
        )
    """))