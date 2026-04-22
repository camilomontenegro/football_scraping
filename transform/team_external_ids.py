from sqlalchemy import text
from utils.mdm_engine import resolve
from utils.mdm_helpers import get_entity_id


def load_team_external_ids(conn):
    rows = conn.execute(text("""
        SELECT DISTINCT team_name
        FROM stg_transfermarkt_players
    """)).fetchall()

    inserted = 0

    for (team_name,) in rows:

        result = resolve(conn, "team", team_name, "transfermarkt")
        team_id = get_entity_id(result)

        if team_id:
            conn.execute(text("""
                INSERT INTO team_external_ids (team_id, source, external_id)
                VALUES (:team_id, 'transfermarkt', :external_id)
                ON CONFLICT (team_id, source) DO NOTHING
            """), {
                "team_id": team_id,
                "external_id": team_name
            })

            inserted += 1

    return inserted