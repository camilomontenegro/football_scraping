from pathlib import Path
import json
from sqlalchemy import text


# ─────────────────────────────
# HELPERS
# ─────────────────────────────

def load_json(path):
    if not path.exists():
        return None
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def find_batch_folder(match_dir: Path):
    for p in match_dir.iterdir():
        if p.is_dir() and p.name.startswith("batch_id"):
            return p
    return None


# ─────────────────────────────
# SHOTS
# shots.json → {"shotmap": [...]}
# Coordenadas en playerCoordinates: {"x": ..., "y": ...}
# xG dentro de draw: {"xg": ...}
# ─────────────────────────────

def load_sofascore_shots(conn, match_id, shots_raw, batch_id):

    if isinstance(shots_raw, dict):
        shots = shots_raw.get("shotmap", [])
    else:
        shots = shots_raw

    for s in shots:

        player = s.get("player") or {}
        coords = s.get("playerCoordinates") or {}
        draw   = s.get("draw") or {}
        xg     = s.get("xg") or draw.get("xg")

        conn.execute(text("""
            INSERT INTO stg_sofascore_shots (
                match_id_ext, player_id_ext, player_name, team_name,
                minute, x, y, xg, result, shot_type, raw_json, batch_id
            )
            VALUES (
                :match_id, :player_id, :player, :team,
                :minute, :x, :y, :xg, :result, :shot_type, :raw, :batch
            )
        """), {
            "match_id":  match_id,
            "player_id": player.get("id"),
            "player":    player.get("name"),
            "team":      "home" if s.get("isHome") else "away",
            "minute":    s.get("time"),
            "x":         coords.get("x"),
            "y":         coords.get("y"),
            "xg":        xg,
            "result":    s.get("shotType"),
            "shot_type": s.get("situation"),
            "raw":       json.dumps(s, ensure_ascii=False),
            "batch":     batch_id
        })


# ─────────────────────────────
# EVENTS
# events.json → {"incidents": [...]}
# ─────────────────────────────

def load_sofascore_events(conn, match_id, events_json, batch_id):

    if isinstance(events_json, dict):
        incidents = events_json.get("incidents", [])
    else:
        incidents = events_json

    for e in incidents:

        player = e.get("player") or {}
        team   = e.get("team") or {}

        minute = e.get("time")
        if isinstance(minute, str):
            try:
                minute = int(minute)
            except Exception:
                minute = None

        conn.execute(text("""
            INSERT INTO stg_sofascore_events (
                match_id_ext, player_id_ext, incident_type,
                minute, player_name, team_name, raw_json, batch_id
            )
            VALUES (
                :match_id, :player_id, :type,
                :minute, :player, :team, :raw, :batch
            )
        """), {
            "match_id":  match_id,
            "player_id": player.get("id"),
            "type":      e.get("incidentType"),
            "minute":    minute,
            "player":    player.get("name"),
            "team":      team.get("name"),
            "raw":       json.dumps(e, ensure_ascii=False),
            "batch":     batch_id
        })


# ─────────────────────────────
# MAIN STAGING ORCHESTRATOR
# ─────────────────────────────

def run_sofascore_loader(conn, base_dir, batch_id=None):

    base_dir = Path(base_dir)
    matches  = [d for d in base_dir.rglob("match_*") if d.is_dir()]

    print(f"Matches found: {len(matches)}")

    total = 0

    for match_dir in matches:

        match_id  = int(match_dir.name.replace("match_", ""))
        batch_dir = find_batch_folder(match_dir)

        if not batch_dir:
            print(f"[SKIP] No batch folder: {match_dir}")
            continue

        shots  = load_json(batch_dir / "shots.json")
        events = load_json(batch_dir / "events.json")

        if shots:
            load_sofascore_shots(conn, match_id, shots, batch_id)

        if events:
            load_sofascore_events(conn, match_id, events, batch_id)

        total += 1
        print(f"[OK] match {match_id}")

    print(f"\nTOTAL LOADED: {total}")
    return total


# ─────────────────────────────
# DIRECT STAGING (MATCH LEVEL)
# ─────────────────────────────

def run_sofascore_staging(conn, match_id, shots, events, batch_id):
    load_sofascore_shots(conn, match_id, shots, batch_id)
    load_sofascore_events(conn, match_id, events, batch_id)
    return True