from sqlalchemy import text


# ─────────────────────────────────────────────
# TRANSFERMARKT
# ─────────────────────────────────────────────

def load_player_external_ids_transfermarkt(conn):

    return conn.execute(text("""
        INSERT INTO player_external_ids (player_id, source, external_id)
        SELECT DISTINCT
            player_id,
            'transfermarkt',
            player_id_tm
        FROM transfermarkt_player_mapping
        WHERE player_id_tm IS NOT NULL
        ON CONFLICT DO NOTHING
    """)).rowcount


# ─────────────────────────────────────────────
# TEAMS SOFASCORE (incluye shots + events)
# ─────────────────────────────────────────────

def load_team_external_ids_sofascore(conn):

    return conn.execute(text("""
        INSERT INTO team_external_ids (team_id, source, external_id)
        SELECT DISTINCT
            dt.team_id,
            'sofascore',
            st.team_name
        FROM stg_sofascore_events st
        JOIN dim_team dt
            ON LOWER(dt.name_canonical) = LOWER(st.team_name)
        WHERE st.team_name IS NOT NULL

        UNION

        SELECT DISTINCT
            dt.team_id,
            'sofascore',
            sp.team_name
        FROM stg_sofascore_shots sp
        JOIN dim_team dt
            ON LOWER(dt.name_canonical) = LOWER(sp.team_name)
        WHERE sp.team_name IS NOT NULL

        ON CONFLICT DO NOTHING
    """)).rowcount


# ─────────────────────────────────────────────
# WRAPPER
# ─────────────────────────────────────────────

def run_external_ids(conn):

    return {
        "player_tm": load_player_external_ids_transfermarkt(conn),
        "team_sf": load_team_external_ids_sofascore(conn)
    }