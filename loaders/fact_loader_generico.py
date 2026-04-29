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


def _load_shots_sofascore(conn, ss_path: Path, competition_id:int ) -> int:
    """Carga tiros de SofaScore desde shots_clean.csv."""
    files = list(ss_path.glob("**/shots_clean.csv"))
    if not files:
        log.info("fact_shots: no hay shots_clean.csv de SofaScore")
        return 0

    all_rows: list[dict] = []
    for f in files:
        try:
            
            df = pd.read_csv(f)
            # convierte  el dataframe en una lista de diccionarios y añade a la lista all_rows 
            all_rows.extend(df.to_dict("records"))
        except Exception as e:
            log.error("Error reading file %s: %s", f, e)
            continue

    count = skipped = 0

    # Carga los  datos de los partidos necesarios para poder  determianr el equipo del jugador que realizo el tiro
    #diccionario  que tendra como clave el match_id  y como valor una tupla con home_team_id y  away_team_id
    matches_cache = {}
    # fetchall devuelve lista de tuplas  en la que cada tupla  contiene match_id,  home_team_id y  away_team_id
    rows = conn.execute(text("""
                            SELECT match_id, home_team_id,away_team_id
                            FROM   dim_match 
                            WHERE competition_id = :comp_id 
                        """), {"comp_id": competition_id}).fetchall()
    
    #recorre la lista de tuplas y añade key:value pairs  al diccionario
    for row in rows: 
        matches_cache[row[0]] = (row[1],row[2])

    # recorre la lista de diccionarios donde cada row es un diccionario 
    for row in all_rows:
        try:
            mid = _match_id_by_source(conn, "sofascore", _safe_int(row.get("match_id_ss")))
            pid = _player_id_by_source(conn, "sofascore", _safe_int(row.get("player_id_ss")))
            #tid = _team_id_by_source(conn, "sofascore",   _safe_int(row.get("team_id_ss")))

            #boolean indica si el partido se jugo en casa o fuera. 
            #Se usa para extraer el id del equipo del jugador que realizó el tiro
            is_home = row.get("is_home")
            
            if is_home  is not None  and mid:
                match_teams= matches_cache.get(mid)
                if(match_teams):
                    tid= match_teams[0] if is_home else match_teams[1]

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



def _load_shots_understat(conn, us_path: Path) -> int:
    """Carga tiros de Understat desde understat_shots_laliga.csv."""
    f = us_path  / "understat_shots_laliga.csv"
    if not f.exists():
        log.info("fact_shots: no hay understat_shots_laliga.csv")
        return 0

    try:
        df = pd.read_csv(f)
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


def load_shots(conn, ss_path: Path, competition_id: int, us_path: Optional[Path] = None) -> int:
    """Carga fact_shots desde SofaScore y Understat.

    Parámetros:
        conn:    conexión a la base de datos
        ss_path: ruta a la carpeta de SofaScore de la competición (obligatorio)
        us_path: ruta a la carpeta de Understat (opcional)
    """

    log.info("[START] Cargando fact_shots...")
    total = _load_shots_sofascore(conn, ss_path,competition_id)
    if us_path:
        total += _load_shots_understat(conn, us_path)
    log.info("[OK] fact_shots completado — %d tiros insertados", total)
    return total



# ── FACT_EVENTS ───────────────────────────────────────────────────────────────

def _load_events_source(conn, source: str, file_pattern: str, files_dir: Path) -> int:
    """Carga eventos de una fuente genérica desde events_clean.json."""
    files = list(files_dir.glob(file_pattern.replace(".json", ".csv")))

    if not files:
        log.info("fact_events: no hay %s en %s", file_pattern.replace(".json", ".csv"), files_dir)
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


def load_events(
    conn,
    ss_path: Optional[Path] = None,
    sb_path: Optional[Path] = None,
    ws_path: Optional[Path] = None,
) -> int:
    """Carga fact_events desde SofaScore, StatsBomb y WhoScored.

    Parámetros:
        conn:    conexión a la base de datos
        ss_path: ruta a SofaScore (opcional)
        sb_path: ruta a StatsBomb (opcional)
        ws_path: ruta a WhoScored (opcional)
    """
    log.info("[START] Cargando fact_events...")
    total = 0

    # **/events_clean*.csv busca  cualquier archivo qeu contenga events_clean en el nombre
    if ss_path:
        total += _load_events_source(conn, "sofascore", "**/events_clean.csv", ss_path)
    if sb_path:
        total += _load_events_source(conn, "statsbomb", "**/events_clean.csv", sb_path)
    if ws_path:
        total += _load_events_source(conn, "whoscored", "**/events_clean.csv", ws_path)
    log.info("[OK] fact_events completado — %d eventos insertados", total)
    return total




# ── FACT_INJURIES ─────────────────────────────────────────────────────────────

def load_injuries(conn,  tm_path: Path) -> int:
    """Carga fact_injuries desde injuries_clean.json de Transfermarkt."""

    log.info("[START] Cargando fact_injuries...")
    #
    files = list(tm_path.glob("**/injuries_clean.csv"))
    
    if not files:
        log.warning("fact_injuries: no hay injuries_clean.csv en %s", tm_path)
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

