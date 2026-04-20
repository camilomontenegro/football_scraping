import json
import logging
from pathlib import Path
from sqlalchemy import text

log = logging.getLogger(__name__)

RAW_BASE = Path("data/raw/transfermarkt")

def load_stg_transfermarkt_players(conn, players_json, batch_id):
    inserted = 0
    for p in players_json:
        try:
            conn.execute(text("""
                INSERT INTO stg_transfermarkt_players (
                    id_transfermarkt,
                    player_name,
                    team_name,
                    team_country,
                    nationality,
                    birth_date,
                    position,
                    raw_json,
                    batch_id
                )
                VALUES (
                    :id, :name, :team, :tcountry, :nat, :dob, :pos, :raw, :batch
                )
                ON CONFLICT (id_transfermarkt, batch_id) DO NOTHING
            """), {
                "id": str(p.get("player_id", "")),
                "name": p.get("player_name"),
                "team": p.get("team_name"),
                "tcountry": p.get("team_country"),
                "nat": p.get("nationality"),
                "dob": p.get("birth_date"),
                "pos": p.get("position"),
                "raw": json.dumps(p, ensure_ascii=False),
                "batch": batch_id
            })
            inserted += 1
        except Exception as e:
            log.warning("Error insertando transfermarkt player %s: %s", p.get("player_id"), e)
    return inserted

def load_stg_transfermarkt_injuries(conn, injuries_json, batch_id):
    inserted = 0
    for i in injuries_json:
        try:
            conn.execute(text("""
                INSERT INTO stg_transfermarkt_injuries (
                    player_id_tm, season, injury_type,
                    date_from, date_until, days_absent, matches_missed,
                    raw_json, batch_id
                )
                VALUES (
                    :pid, :season, :itype,
                    :dfrom, :duntil, :days, :matches,
                    :raw, :batch
                )
                ON CONFLICT (player_id_tm, injury_type, date_from, batch_id) DO NOTHING
            """), {
                "pid": str(i.get("player_id_tm", "")),
                "season": str(i.get("season", "")),
                "itype": i.get("injury_type"),
                "dfrom": i.get("date_from"),
                "duntil": i.get("date_until"),
                "days": int(i.get("days_absent")) if i.get("days_absent") else None,
                "matches": int(i.get("matches_missed")) if i.get("matches_missed") else None,
                "raw": json.dumps(i, ensure_ascii=False),
                "batch": batch_id
            })
            inserted += 1
        except Exception as e:
            log.warning("Error insertando transfermarkt injury %s: %s", i.get("player_id_tm"), e)
    return inserted


def run_transfermarkt_loader(conn, base_dir: str | Path = RAW_BASE, batch_id: str | None = None) -> dict:
    """Carga los JSONs guardados por el extractor en staging."""
    base_dir = Path(base_dir)
    res = {"players": 0, "injuries": 0}

    player_files = list(base_dir.glob("**/players.json"))
    for pf in player_files:
        eff_batch = batch_id or pf.parent.name.replace("batch_id=", "")
        try:
            with open(pf, encoding="utf-8") as f:
                data = json.load(f)
            n = load_stg_transfermarkt_players(conn, data, eff_batch)
            res["players"] += n
        except Exception as e:
            log.error("Error importando transfermarkt players %s: %s", pf, e)

    injury_files = list(base_dir.glob("**/injuries.json"))
    for inf in injury_files:
        eff_batch = batch_id or inf.parent.name.replace("batch_id=", "")
        try:
            with open(inf, encoding="utf-8") as f:
                data = json.load(f)
            n = load_stg_transfermarkt_injuries(conn, data, eff_batch)
            res["injuries"] += n
        except Exception as e:
            log.error("Error importando transfermarkt injuries %s: %s", inf, e)

    log.info("TOTAL Transfermarkt staging: %d players, %d injuries", res["players"], res["injuries"])
    return res