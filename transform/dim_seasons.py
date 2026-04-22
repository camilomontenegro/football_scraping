from sqlalchemy import text

def load_dim_season(conn, label, year_start, year_end):
    result = conn.execute(text("""
        INSERT INTO dim_season (label, year_start, year_end)
        VALUES (:label, :year_start, :year_end)
        ON CONFLICT (label) DO NOTHING
    """), {
        "label": label,
        "year_start": year_start,
        "year_end": year_end
    })

    # devolver métrica real (más fiable que rowcount en Postgres)
    return conn.execute(text("""
        SELECT COUNT(*) 
        FROM dim_season
        WHERE label = :label
    """), {"label": label}).scalar()