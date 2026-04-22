from sqlalchemy import text
from utils.mdm_engine import resolve
from utils.mdm_helpers import get_entity_id


def load_player_mapping(conn):

    rows = conn.execute(text("""
        SELECT DISTINCT id_transfermarkt, player_name
        FROM stg_transfermarkt_players
    """)).fetchall()

    inserted = 0

    for tm_id, name in rows:

        if not name:
            continue

        result = resolve(conn, "player", name, "transfermarkt")
        player_id = get_entity_id(result)

        if not player_id:
            continue

        conn.execute(text("""
            INSERT INTO transfermarkt_player_mapping (player_id_tm, player_id)
            VALUES (:tm_id, :player_id)
            ON CONFLICT DO NOTHING
        """), {
            "tm_id": tm_id,
            "player_id": player_id
        })

        conn.execute(text("""
            INSERT INTO player_external_ids (player_id, source, external_id)
            VALUES (:player_id, 'transfermarkt', :tm_id)
            ON CONFLICT DO NOTHING
        """), {
            "player_id": player_id,
            "tm_id": tm_id
        })

        inserted += 1

    return inserted