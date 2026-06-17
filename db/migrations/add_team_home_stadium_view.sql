-- Vista de sede habitual por equipo (denormaliza dim_team.home_stadium_master_id).

CREATE OR REPLACE VIEW vw_team_home_stadium AS
SELECT
    dt.canonical_id,
    dt.canonical_name,
    dt.country,
    dt.home_stadium_master_id,
    sm.canonical_name AS stadium_name,
    sm.wikidata_qid,
    sm.city,
    sm.country AS stadium_country,
    sm.capacity,
    sm.latitude,
    sm.longitude
FROM dim_team dt
LEFT JOIN dim_stadium_master sm ON sm.stadium_id = dt.home_stadium_master_id;
