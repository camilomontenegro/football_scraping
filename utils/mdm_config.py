ENTITY_CONFIG = {
    "player": {
        "dim_table": "dim_player",
        "id_field": "player_id",
        "name_field": "name_canonical",
        "alias_table": "player_name_alias",
        "alias_id_field": "player_id"
    },
    "team": {
        "dim_table": "dim_team",
        "id_field": "team_id",
        "name_field": "name_canonical",
        "alias_table": "team_name_alias",
        "alias_id_field": "team_id"
    }
}