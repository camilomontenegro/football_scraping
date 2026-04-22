from sqlalchemy import text
from utils.mdm_engine import resolve
from utils.mdm_helpers import get_entity_id


def load_dim_players(conn):

    rows = conn.execute(text("""
        SELECT DISTINCT ON (player_name)
            player_name,
            nationality,
            birth_date,
            position
        FROM stg_transfermarkt_players
        ORDER BY player_name, loaded_at DESC
    """)).fetchall()

    inserted = 0

    for name, nat, dob, pos in rows:

        if not name:
            continue

        result = resolve(conn, "player", name, "transfermarkt")
        player_id = get_entity_id(result)

        if not player_id:
            continue

        #  casteo seguro
        if dob:
            dob = str(dob)

        conn.execute(text("""
            UPDATE dim_player
            SET
                nationality = COALESCE(nationality, :nat),
                birth_date  = COALESCE(birth_date, :dob),
                player_position = COALESCE(player_position, :pos)
            WHERE player_id = :id
        """), {
            "id": player_id,
            "nat": nat,
            "dob": dob,
            "pos": pos
        })

        inserted += 1

    return inserted