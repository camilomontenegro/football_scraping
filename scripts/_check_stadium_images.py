import sys
sys.path.insert(0, ".")
from sqlalchemy import text
from loaders.common import engine

with engine.connect() as c:
    total = c.execute(text("SELECT COUNT(*) FROM dim_stadium")).scalar()
    with_img = c.execute(text("SELECT COUNT(*) FROM dim_stadium WHERE image_url IS NOT NULL AND TRIM(image_url) <> ''")).scalar()
    with_qid = c.execute(text("SELECT COUNT(*) FROM dim_stadium WHERE wikidata_qid IS NOT NULL")).scalar()
    qid_no_img = c.execute(text("""
        SELECT COUNT(*) FROM dim_stadium
        WHERE wikidata_qid IS NOT NULL
          AND (image_url IS NULL OR TRIM(image_url) = '')
    """)).scalar()
    syn = c.execute(text("""
        SELECT COUNT(*) FILTER (WHERE image_url IS NOT NULL AND TRIM(image_url) <> '') AS img,
               COUNT(*) AS total
        FROM dim_stadium WHERE data_source = 'synthetic-geocode'
    """)).one()
    print("total_stadiums", total)
    print("with_image_url", with_img)
    print("with_wikidata_qid", with_qid)
    print("qid_but_no_image", qid_no_img)
    print("synthetic", dict(syn._mapping))
    rows = c.execute(text("""
        SELECT t.canonical_name, s.wikidata_qid, s.image_url
        FROM dim_stadium s
        JOIN dim_team t ON t.canonical_id = s.canonical_team_id
        WHERE s.data_source = 'synthetic-geocode'
        ORDER BY s.image_url NULLS FIRST
        LIMIT 15
    """)).fetchall()
    for r in rows:
        img = "YES" if r.image_url else "NO"
        print(img, r.canonical_name, r.wikidata_qid)
