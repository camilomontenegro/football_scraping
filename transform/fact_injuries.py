from sqlalchemy import text

def load_fact_injuries(conn):
    result = conn.execute(text("""
        INSERT INTO fact_injuries (
            player_id,
            team_id,
            injury_type_id,
            season_id,
            date_from,
            date_until,
            days_absent,
            matches_missed
        )
        SELECT
            pm.player_id,
            dt.team_id,
            dit.injury_type_id,
            ds.season_id,
            i.date_from,
            i.date_until,
            i.days_absent,
            i.matches_missed

        FROM stg_transfermarkt_injuries i

        JOIN transfermarkt_player_mapping pm
            ON pm.player_id_tm = i.player_id_tm

        JOIN stg_transfermarkt_players tm
            ON tm.id_transfermarkt = i.player_id_tm

        JOIN team_name_alias tna
            ON LOWER(TRIM(tna.alias_name)) = LOWER(REPLACE(TRIM(tm.team_name), '-', ' '))
            OR LOWER(TRIM(tna.alias_name)) = LOWER(TRIM(tm.team_name))
        
        JOIN dim_team dt
            ON dt.team_id = tna.team_id

        JOIN dim_injury_type dit
            ON LOWER(TRIM(dit.name)) = LOWER(TRIM(i.injury_type))

        JOIN dim_season ds
            ON ds.year_end = CAST(i.season AS INTEGER)

        ON CONFLICT (player_id, injury_type_id, date_from) DO NOTHING
    """))

    return result.rowcount