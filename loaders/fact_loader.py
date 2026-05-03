"""
loaders/fact_loader.py
=======================
Carga las tablas de hechos desde los CSV producidos por los scrapers.

Funciones:
    load_shots(conn)    → fact_shots   (SofaScore + Understat)
    load_events(conn)   → fact_events  (SofaScore + StatsBomb + WhoScored)
    load_injuries(conn) → fact_injuries (Transfermarkt)

Resolución de FKs:
    - match_id:  via dim_match.id_sofascore / id_understat / id_statsbomb
    - player_id: via dim_player.id_sofascore / id_understat / id_transfermarkt / id_statsbomb / id_whoscored
    - team_id:   via dim_team.id_sofascore / id_understat / id_statsbomb

Schema destino:
    fact_shots:    shot_id, match_id, player_id, team_id, minute, x, y, xg,
                   result, shot_type, situation, data_source
    fact_events:   event_id, match_id, player_id, team_id, event_type,
                   minute, second, x, y, end_x, end_y, outcome, data_source
    fact_injuries: injury_id, player_id, season, injury_type,
                   date_from, date_until, days_absent, matches_missed
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

import pandas as pd
from sqlalchemy import text

from loaders.common import engine
from utils.canonical_teams import normalize_team_name

log = logging.getLogger(__name__)

RAW_SS = Path("data/raw/sofascore")
RAW_TM = Path("data/raw/transfermarkt")
RAW_US = Path("data/raw/understat")
RAW_SB = Path("data/raw/statsbomb")
RAW_WS = Path("data/raw/whoscored")


def _ensure_date(val) -> Optional[str]:
    """Asegura formato YYYY-MM-DD y maneja epochs numéricos."""
    if val is None or str(val).strip().lower() in ("nan", "none", ""):
        return None
    if isinstance(val, (int, float)):
        try:
            from datetime import datetime
            return datetime.fromtimestamp(val / 1000.0).date().isoformat()
        except Exception:
            return None
    return str(val)[:10]



# ── Helpers de resolución de FKs ───────────────────────────────────────────

def _match_id_by_source(conn, source: str, ext_id) -> Optional[int]:
    """Devuelve dim_match.match_id dado el ID externo de una fuente."""
    if ext_id is None:
        return None
    col_map = {
        "sofascore":     "id_sofascore",
        "understat":     "id_understat",
        "statsbomb":     "id_statsbomb",
        "whoscored":     "id_whoscored",
        "transfermarkt": "id_transfermarkt",
    }
    col = col_map.get(source)
    if not col:
        return None
    row = conn.execute(
        text(f"SELECT match_id FROM dim_match WHERE {col} = :eid LIMIT 1"),
        {"eid": ext_id},
    ).fetchone()
    return row[0] if row else None


def _player_id_by_source(conn, source: str, ext_id) -> Optional[int]:
    """Devuelve dim_player.canonical_id dado el ID externo de una fuente."""
    if ext_id is None:
        return None
    col_map = {
        "sofascore":     "id_sofascore",
        "understat":     "id_understat",
        "statsbomb":     "id_statsbomb",
        "whoscored":     "id_whoscored",
        "transfermarkt": "id_transfermarkt",
    }
    col = col_map.get(source)
    if not col:
        return None
    row = conn.execute(
        text(f"SELECT canonical_id FROM dim_player WHERE {col} = :eid LIMIT 1"),
        {"eid": ext_id},
    ).fetchone()
    return row[0] if row else None


def _team_id_by_source(conn, source: str, ext_id) -> Optional[int]:
    """Devuelve dim_team.canonical_id dado el ID externo de una fuente."""
    if ext_id is None:
        return None
    col_map = {
        "sofascore":     "id_sofascore",
        "understat":     "id_understat",
        "statsbomb":     "id_statsbomb",
        "whoscored":     "id_whoscored",
        "transfermarkt": "id_transfermarkt",
    }
    col = col_map.get(source)
    if not col:
        return None
    row = conn.execute(
        text(f"SELECT canonical_id FROM dim_team WHERE {col} = :eid LIMIT 1"),
        {"eid": ext_id},
    ).fetchone()
    return row[0] if row else None


def _safe_int(val) -> Optional[int]:
    try:
        return int(val) if val is not None and str(val).strip() not in ("", "nan") else None
    except (ValueError, TypeError):
        return None


def _safe_float(val) -> Optional[float]:
    try:
        return float(val) if val is not None and str(val).strip() not in ("", "nan") else None
    except (ValueError, TypeError):
        return None


# ── FACT_SHOTS ────────────────────────────────────────────────────────────────

def _load_shots_sofascore(conn) -> int:
    files = list(RAW_SS.glob("**/*shots*.csv"))
    if not files:
        log.info("fact_shots: no hay archivos de tiros de SofaScore")
        return 0

    all_rows: list[dict] = []
    for f in files:
        try:
            df = pd.read_csv(f)
            all_rows.extend(df.to_dict("records"))
        except Exception as e:
            log.error("Error reading file %s: %s", f, e)
            continue

    count = skipped = 0
    for row in all_rows:
        try:
            mid = _match_id_by_source(conn, "sofascore", _safe_int(row.get("match_id_ss")))
            pid = _player_id_by_source(conn, "sofascore", _safe_int(row.get("player_id_ss")))
            tid = _team_id_by_source(conn, "sofascore",   _safe_int(row.get("team_id_ss")))

            if not mid or not pid or not tid:
                skipped += 1
                continue

            conn.execute(text("""
                INSERT INTO fact_shots
                    (match_id, player_id, team_id, minute, x, y, xg,
                     result, shot_type, situation, data_source)
                VALUES
                    (:mid, :pid, :tid, :min, :x, :y, :xg,
                     :result, :stype, :sit, 'sofascore')
                ON CONFLICT (match_id, player_id, minute, x, y, data_source) DO NOTHING
            """), {
                "mid":    mid,
                "pid":    pid,
                "tid":    tid,
                "min":    _safe_int(row.get("minute")),
                "x":      _safe_float(row.get("x")),
                "y":      _safe_float(row.get("y")),
                "xg":     _safe_float(row.get("xg")),
                "result": row.get("result") or None,
                "stype":  row.get("shot_type") or None,
                "sit":    row.get("situation") or None,
            })
            count += 1
        except Exception as e:
            log.warning("Error inserting shot record: %s", e)
            skipped += 1
            continue

    log.info("fact_shots ← SofaScore: %d insertados | %d sin FKs resueltas", count, skipped)
    return count


def _load_shots_understat(conn) -> int:
    files = list(RAW_US.glob("**/*shots*.csv"))
    if not files:
        log.info("fact_shots: no hay archivos de tiros de understat")
        return 0

    try:
        dfs = [pd.read_csv(f) for f in files]
        df = pd.concat(dfs, ignore_index=True)
    except Exception as e:
        log.error("Error reading understat shots: %s", e)
        return 0

    count = skipped = 0
    for _, row in df.iterrows():
        try:
            mid = _match_id_by_source(conn, "understat", _safe_int(row.get("understat_match_id")))
            pid = _player_id_by_source(conn, "understat", _safe_int(row.get("understat_player_id")))

            # team_id via nombre del equipo normalizado → busca por canonical_name
            team_name = row.get("understat_team")
            tid = None
            if team_name:
                canonical = normalize_team_name(team_name)  # resuelve aliases y acentos
                t_row = conn.execute(
                    text("SELECT canonical_id FROM dim_team WHERE LOWER(canonical_name) = :n LIMIT 1"),
                    {"n": canonical.lower()},
                ).fetchone()
                if t_row:
                    tid = t_row[0]

            if not mid or not pid or not tid:
                skipped += 1
                continue

            conn.execute(text("""
                INSERT INTO fact_shots
                    (match_id, player_id, team_id, minute, x, y, xg,
                     result, shot_type, situation, data_source)
                VALUES
                    (:mid, :pid, :tid, :min, :x, :y, :xg,
                     :result, :stype, :sit, 'understat')
                ON CONFLICT (match_id, player_id, minute, x, y, data_source) DO NOTHING
            """), {
                "mid":    mid,
                "pid":    pid,
                "tid":    tid,
                "min":    _safe_int(row.get("minute")),
                "x":      _safe_float(row.get("x")),
                "y":      _safe_float(row.get("y")),
                "xg":     _safe_float(row.get("xg")),
                "result": row.get("result") or None,
                "stype":  row.get("shot_type") or None,
                "sit":    row.get("situation") or None,
            })
            count += 1
        except Exception as e:
            log.warning("Error inserting understat shot: %s", e)
            skipped += 1
            continue

    log.info("fact_shots ← Understat: %d insertados | %d sin FKs resueltas", count, skipped)
    return count


def load_shots(conn) -> int:
    """Carga fact_shots desde SofaScore y Understat."""
    log.info("[START] Cargando fact_shots...")
    total = _load_shots_sofascore(conn) + _load_shots_understat(conn)
    log.info("[OK] fact_shots completado — %d tiros insertados", total)
    return total


# ── FACT_EVENTS ───────────────────────────────────────────────────────────────

def _load_events_source(conn, source: str, file_pattern: str, files_dir: Path) -> int:
    """Carga eventos de una fuente genérica."""
    files = list(files_dir.glob(file_pattern))
    if not files:
        log.info("fact_events: no hay archivos %s en %s", file_pattern, files_dir)
        return 0

    all_rows: list[dict] = []
    for f in files:
        try:
            df = pd.read_csv(f)
            all_rows.extend(df.to_dict("records"))
        except Exception as e:
            log.warning("Error leyendo %s: %s", f, e)

    # Columnas de ID de fuente difieren según el scraper
    mid_col = {
        "sofascore": "match_id_ss",
        "statsbomb": "match_id_sb",
        "whoscored": "whoscored_match_id",
    }.get(source, "match_id_ss")

    pid_col = {
        "sofascore": "player_id_ss",
        "statsbomb": "player_id_sb",
        "whoscored": "whoscored_player_id",
    }.get(source, "player_id_ss")

    tid_col = {
        "sofascore": "team_id_ss",
        "statsbomb": "team_id_sb",
        "whoscored": "whoscored_team_id",          # WhoScored no tiene team_id en events
    }.get(source)

    count = skipped = 0
    for row in all_rows:
        mid = _match_id_by_source(conn, source, _safe_int(row.get(mid_col)))
        pid = _player_id_by_source(conn, source, _safe_int(row.get(pid_col)))
        tid = _team_id_by_source(conn, source, _safe_int(row.get(tid_col))) if tid_col else None

        if not mid or not pid:
            skipped += 1
            continue

        # Si no hay team_id, intentar derivarlo del partido (home o away)
        if not tid:
            m_row = conn.execute(
                text("SELECT home_team_id FROM dim_match WHERE match_id = :mid LIMIT 1"),
                {"mid": mid},
            ).fetchone()
            tid = m_row[0] if m_row else None

        if not tid:
            skipped += 1
            continue

        conn.execute(text("""
            INSERT INTO fact_events
                (match_id, player_id, team_id, event_type,
                 minute, second, x, y, end_x, end_y,
                 outcome, data_source)
            VALUES
                (:mid, :pid, :tid, :etype,
                 :min, :sec, :x, :y, :ex, :ey,
                 :out, :src)
            ON CONFLICT (match_id, player_id, event_type, minute, second, x, y, data_source)
            DO NOTHING
        """), {
            "mid":   mid,
            "pid":   pid,
            "tid":   tid,
            "etype": row.get("event_type") or None,
            "min":   _safe_int(row.get("minute")),
            "sec":   _safe_int(row.get("second")),
            "x":     _safe_float(row.get("x")),
            "y":     _safe_float(row.get("y")),
            "ex":    _safe_float(row.get("end_x")),
            "ey":    _safe_float(row.get("end_y")),
            "out":   row.get("outcome") or None,
            "src":   source,
        })
        count += 1

    log.info("fact_events ← %s: %d insertados | %d sin FKs", source, count, skipped)
    return count


def load_events(conn) -> int:
    """Carga fact_events desde SofaScore, StatsBomb y WhoScored."""
    log.info("[START] Cargando fact_events...")
    total = 0
    total += _load_events_source(conn, "sofascore", "**/*events*.csv", RAW_SS)
    total += _load_events_source(conn, "statsbomb", "**/*events*.csv", RAW_SB)
    total += _load_events_source(conn, "whoscored", "**/*events*.csv", RAW_WS)
    log.info("[OK] fact_events completado — %d eventos insertados", total)
    return total


# ── FACT_INJURIES ─────────────────────────────────────────────────────────────

def load_injuries(conn) -> int:
    """Carga fact_injuries desde injuries_clean.json de Transfermarkt."""
    log.info("[START] Cargando fact_injuries...")
    files = list(RAW_TM.glob("**/*injuries*.csv"))

    if not files:
        log.warning("fact_injuries: no hay archivos de injuries en %s", RAW_TM)
        return 0

    all_rows: list[dict] = []
    for f in files:
        try:
            df = pd.read_csv(f)
            all_rows.extend(df.to_dict("records"))
        except Exception as e:
            log.warning("Error leyendo %s: %s", f, e)

    count = skipped = 0
    for row in all_rows:
        # cambio player_id_tm por player_id
        # Usar player_id_tm como parte del nombre del savepoint para depuración
        sp_name = f"injury_{_safe_int(row.get('player_id'))}_{count}"
        conn.execute(text(f"SAVEPOINT {sp_name}"))
        
        try:
            # cambio player_id_tm por player_id
            pid = _player_id_by_source(conn, "transfermarkt", _safe_int(row.get("player_id")))

            if not pid:
                conn.execute(text(f"RELEASE SAVEPOINT {sp_name}"))
                skipped += 1
                continue

            date_from  = _ensure_date(row.get("date_from"))
            date_until = _ensure_date(row.get("date_until"))

            conn.execute(text("""
                INSERT INTO fact_injuries
                    (player_id, season, injury_type, date_from,
                     date_until, days_absent, matches_missed)
                VALUES
                    (:pid, :season, :itype, :dfrom,
                     :duntil, :days, :mm)
                ON CONFLICT (player_id, season, injury_type, date_from)
                DO NOTHING
            """), {
                "pid":    pid,
                "season": row.get("season") or None,
                "itype":  row.get("injury_type") or None,
                "dfrom":  date_from,
                "duntil": date_until,
                "days":   _safe_int(row.get("days_absent")),
                "mm":     _safe_int(row.get("matches_missed")),
            })
            conn.execute(text(f"RELEASE SAVEPOINT {sp_name}"))
            count += 1
        except Exception as e:
            conn.execute(text(f"ROLLBACK TO SAVEPOINT {sp_name}"))
            log.warning("Error inserting injury record: %s", e)
            skipped += 1
            continue

    log.info("fact_injuries ← Transfermarkt: %d insertadas | %d sin jugador resuelto", count, skipped)
    return count


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s - %(message)s")
    with engine.begin() as conn:
        load_shots(conn)
        load_events(conn)
        load_injuries(conn)
