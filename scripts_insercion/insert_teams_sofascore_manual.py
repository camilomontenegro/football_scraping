import sys
sys.path.append('../')

from db.connection import get_connection

EQUIPOS = [
    {'name_canonical': 'Real Madrid',  'id_sofascore': 2829, 'country': 'Spain'},
    {'name_canonical': 'FC Barcelona', 'id_sofascore': 2817, 'country': 'Spain'},
]

with get_connection() as conn:
    with conn.cursor() as cur:

        for equipo in EQUIPOS:

            cur.execute("""
                INSERT INTO dim_team (
                    name_canonical,
                    country,
                    id_sofascore
                )
                VALUES (%s, %s, %s)
                ON CONFLICT (id_sofascore)
                DO NOTHING
            """, (
                equipo['name_canonical'],
                equipo['country'],
                equipo['id_sofascore']
            ))

        conn.commit()

print("dim_team listo")