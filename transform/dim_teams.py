from sqlalchemy import text
from utils.mdm_engine import resolve
from utils.mdm_helpers import get_entity_id


def load_dim_teams(conn):
    # Primero cargamos de SofaScore (donde los nombres son bonitos)
    sofa_rows = conn.execute(text("""
        SELECT DISTINCT home_team_name as team FROM stg_sofascore_matches
        UNION
        SELECT DISTINCT away_team_name as team FROM stg_sofascore_matches
    """)).fetchall()
    
    for (team_name,) in sofa_rows:
        resolve(conn, "team", team_name, "sofascore")

    # Luego procesamos Transfermarkt para rellenar países y resolver aliases
    rows = conn.execute(text("""
        SELECT DISTINCT team_name, MAX(team_country) as team_country
        FROM stg_transfermarkt_players
        GROUP BY team_name
    """)).fetchall()

    inserted = 0

    for team_name, team_country in rows:
        result = resolve(conn, "team", team_name, "transfermarkt")
        team_id = get_entity_id(result)

        if team_id:
            if team_country:
                conn.execute(text("""
                    UPDATE dim_team
                    SET country = COALESCE(country, :ctry)
                    WHERE team_id = :tid
                """), {"ctry": team_country, "tid": team_id})
            inserted += 1

    return inserted