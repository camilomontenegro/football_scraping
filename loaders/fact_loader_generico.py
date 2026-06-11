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
                   minute, second, x, y, end_x, end_y, outcome, data_source,
                   qualifiers (JSONB, WhoScored), whoscored_event_id
    fact_injuries: injury_id, player_id, season, injury_type,
                   date_from, date_until, days_absent, matches_missed
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Optional

import pandas as pd
from sqlalchemy import text

from loaders.common import engine
from utils.canonical_teams import normalize_team_name
from utils.coordinate_normalization import _normalize_coordinates

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
    """
    Devuelve dim_match.match_id dado el ID externo de una fuente.
    
    ext_id: id del partido en la fuente 
    """
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
    
    # en dim_match id_statsbomb es VARCHAR. Se pasa como String
    eid = _safe_str(ext_id) if source == "statsbomb" else ext_id
    if eid is None: 
        return None

    row = conn.execute(
        text(f"SELECT match_id FROM dim_match WHERE {col} = :eid LIMIT 1"),
        {"eid": eid},
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
    
    # en dim_team id_statsbomb es VARCHAR. Se pasa como String
    eid = _safe_str(ext_id) if source == "statsbomb" else ext_id
    if eid is None:
        return None
    
    row = conn.execute(
        text(f"SELECT canonical_id FROM dim_player WHERE {col} = :eid LIMIT 1"),
        {"eid": eid},
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
    
    # en dim_team id_statsbomb es VARCHAR. Se pasa como String
    eid = _safe_str(ext_id) if source == "statsbomb" else ext_id
    if eid is None:
        return None

    row = conn.execute(
        text(f"SELECT canonical_id FROM dim_team WHERE {col} = :eid LIMIT 1"),
        {"eid": eid},
    ).fetchone()
    return row[0] if row else None


def _safe_int(val) -> Optional[int]:
    """
    Convierte un valor a entero de forma segura.
    
    Pandas representa los valores vacíos de un CSV como float('nan').
    Sin este método, int(float('nan')) lanzaría un ValueError.
    
    Devuelve None si el valor es None, cadena vacía o 'nan' — 
    que es lo que la BD espera para campos opcionales.
    
    Ejemplos:
        _safe_int(45)    → 45
        _safe_int("45")  → 45
        _safe_int(None)  → None
        _safe_int("nan") → None
        _safe_int("")    → None
    """
    try:
        return int(val) if val is not None and str(val).strip() not in ("", "nan") else None
    except (ValueError, TypeError):
        return None


def _safe_float(val) -> Optional[float]:
    """
    Convierte un valor a float de forma segura.
    
    Mismo propósito que _safe_int pero para valores decimales
    como coordenadas x/y o xG.
    
    Devuelve None si el valor es None, cadena vacía o 'nan'.
    
    Ejemplos:
        _safe_float(6.4)    → 6.4
        _safe_float("6.4")  → 6.4
        _safe_float(None)   → None
        _safe_float("nan")  → None
        _safe_float("")     → None
    """
    try:
        return float(val) if val is not None and str(val).strip() not in ("", "nan") else None
    except (ValueError, TypeError):
        return None

def _to_bool(val) -> Optional[bool]:
    """Parsea un valor a bool con tolerancia a strings y NaN.

    bool("False") en Python es True (cualquier string no vacio es truthy),
    asi que NO se puede usar bool() sobre lo que pandas devuelve. Aqui
    parseamos explicitamente las representaciones habituales.

    Devuelve None si el valor no es interpretable.
    """
    if val is None:
        return None
    if isinstance(val, bool):
        return val
    try:
        if pd.isna(val):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(val, (int, float)):
        return bool(int(val))
    s = str(val).strip().lower()
    if s in ("true", "t", "1", "yes", "y", "home"):
        return True
    if s in ("false", "f", "0", "no", "n", "away"):
        return False
    return None


def _safe_str(val) -> Optional[str]:
    """
    Convierte un valor a string de forma segura.
    
    Útil para campos como id_statsbomb en dim_match que son VARCHAR
    pero vienen como enteros en los CSV.
    
    Ejemplos:
        _safe_str(3773386)  → "3773386"
        _safe_str("abc")    → "abc"
        _safe_str(None)     → None
        _safe_str("nan")    → None
        _safe_str("")       → None
    """
    try:
        return str(val) if val is not None and str(val).strip() not in ("", "nan") else None
    except (ValueError, TypeError):
        return None



# ── FACT_SHOTS ────────────────────────────────────────────────────────────────


def _load_shots_sofascore(conn, ss_path: Path, competition_id:int ) -> int:
    """Carga tiros de SofaScore desde shots.csv."""
    files = list(ss_path.glob("**/shots.csv"))
    if not files:
        log.info("fact_shots: no hay shots.csv de SofaScore")
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

    # Carga los  datos de los partidos necesarios para poder  determinar el equipo del jugador que realizo el tiro
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
            # intenta obtener el id del equipo 
            tid = _team_id_by_source(conn, "sofascore",   _safe_int(row.get("team_id_ss")))

            
            if not tid:
                # boolean indica si el jugador es del equipo local; se usa
                # para sacar el team_id del partido (home o away).
                # Pandas puede leer la columna como string "True"/"False":
                # _to_bool() lo normaliza correctamente (bool("False") seria True).
                is_home = _to_bool(row.get("is_home"))

                if is_home is not None and mid:
                    match_teams = matches_cache.get(mid)
                    if match_teams:
                        tid = match_teams[0] if is_home else match_teams[1]

            if not mid or not pid or not tid:
                skipped += 1
                continue

                        
            # normaliza coordenadas antes de insertar
            x_norm, y_norm = _normalize_coordinates(
                _safe_float(row.get("x")),
                _safe_float(row.get("y"))
            )

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
                "x":      x_norm,
                "y":      y_norm,
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
    
    files = list(us_path.glob("**/*shots*.csv"))
    if not files:
        log.info("fact_shots: no hay archivos de shots de Understat en %s", us_path)
        return 0

    try:
        dfs = [pd.read_csv(f) for f in files]
        df = pd.concat(dfs, ignore_index=True)
    except Exception as e:
        log.error("Error reading understat shots: %s", e)
        return 0
    
    # carga los partidos con id_understat no nulo
    matches_cache = {}
    match_rows = conn.execute(text("""
        SELECT match_id, home_team_id, away_team_id
        FROM dim_match
        WHERE id_understat IS NOT NULL
    """)).fetchall()
    # guarda en el diccionario el match id como clave y una tupla con home_team_id y away_team_id como valor
    for match_row in match_rows:
        matches_cache[match_row[0]] = (match_row[1], match_row[2])
        

    count = skipped = 0
    for _, row in df.iterrows():
        try:
            mid = _match_id_by_source(conn, "understat", _safe_int(row.get("understat_match_id")))
            pid = _player_id_by_source(conn, "understat", _safe_int(row.get("understat_player_id")))

            # no todos los csv de understat vienen  con el id del equipo. El de la Liga sí. El resto, no
            #  Intentar por nombre de equipo  ( la liga) 
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
            
            # Si no hay team_name, intentar por side (Premier, Bundesliga, etc.)
            if not tid:
                side = row.get("side")
                if side and mid:
                    # obtiene la tupla (home_team_id, away_team_id) para ese partido
                    match_teams = matches_cache.get(mid)
                    if match_teams:
                        # si side es "h" (home) → home_team_id (posición 0)
                        # si side es "a" (away) → away_team_id (posición 1)
                        tid = match_teams[0] if side == "h" else match_teams[1]

            if not mid or not pid or not tid:
                skipped += 1
                continue

            # Asegura normalización  de coordenadas antes de insertar
            # De la api ya vienen  en formato 0-1 se supone, pero se incluye por si acaso
            x_norm, y_norm = _normalize_coordinates(
                _safe_float(row.get("x")),
                _safe_float(row.get("y"))
)

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
                "x":      x_norm,
                "y":      y_norm,
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

def _qualifiers_json(val) -> Optional[str]:
    """Normaliza qualifiers del CSV WhoScored a JSON string para JSONB."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    if isinstance(val, dict):
        return json.dumps(val, ensure_ascii=False)
    if isinstance(val, str):
        s = val.strip()
        if not s:
            return None
        try:
            return json.dumps(json.loads(s), ensure_ascii=False)
        except json.JSONDecodeError:
            return None
    return None


def _build_qualifiers_from_columns(row: dict) -> Optional[str]:
    """Reconstruye JSONB qualifiers desde columnas individuales q_* del CSV enriched.

    El enriched_events extractor genera columnas individuales (q_right_foot,
    q_goal_mouth_y, etc.) en vez de un blob JSON. Esta función las reagrupa
    en un dict JSON para almacenar en la columna JSONB de fact_events.
    """
    quals = {}

    # Columnas de valor (float/string)
    _val_cols = {
        "q_zone": "Zone", "q_length": "Length", "q_angle": "Angle",
        "q_pass_end_x": "PassEndX", "q_pass_end_y": "PassEndY",
        "q_goal_mouth_y": "GoalMouthY", "q_goal_mouth_z": "GoalMouthZ",
        "q_blocked_x": "BlockedX", "q_blocked_y": "BlockedY",
        "q_jersey_number": "JerseyNumber", "q_player_pos": "PlayerPosition",
        "q_related_event_id": "RelatedEventId",
        "q_opposite_related_event": "OppositeRelatedEvent",
    }
    for csv_col, qname in _val_cols.items():
        v = row.get(csv_col)
        if v is not None and str(v).strip() not in ("", "nan"):
            try:
                quals[qname] = float(v)
            except (ValueError, TypeError):
                quals[qname] = str(v)

    # Columnas booleanas (1/0)
    _flag_cols = {
        "q_right_foot": "RightFoot", "q_left_foot": "LeftFoot",
        "q_head": "Head", "q_head_pass": "HeadPass",
        "q_cross": "Cross", "q_chipped": "Chipped", "q_longball": "Longball",
        "q_offensive": "Offensive", "q_defensive": "Defensive",
        "q_freekick": "FreekickTaken", "q_indirect_freekick": "IndirectFreekickTaken",
        "q_direct_freekick": "DirectFreekick", "q_corner": "CornerTaken",
        "q_throw_in": "ThrowIn", "q_goal_kick": "GoalKick", "q_foul": "Foul",
        "q_assisted": "Assisted", "q_intent_assist": "IntentionalAssist",
        "q_key_pass": "KeyPass", "q_shot_assist": "ShotAssist",
        "q_big_chance": "BigChance", "q_big_chance_created": "BigChanceCreated",
        "q_fast_break": "FastBreak", "q_regular_play": "RegularPlay",
        "q_individual_play": "IndividualPlay", "q_first_touch": "FirstTouch",
        "q_layoff": "LayOff", "q_throughball": "Throughball", "q_volley": "Volley",
        "q_standing_save": "StandingSave", "q_diving_save": "DivingSave",
        "q_blocked": "Blocked", "q_outfielder_block": "OutfielderBlock",
        "q_from_corner": "FromCorner", "q_leading_to_goal": "LeadingToGoal",
        "q_yellow": "Yellow", "q_aerial_foul": "AerialFoul",
        "q_keeper_save_inbox": "KeeperSaveInTheBox",
        "q_keeper_save_obox": "KeeperSaveObox",
        "q_parried_safe": "ParriedSafe", "q_parried_danger": "ParriedDanger",
        "q_collected": "Collected", "q_blocked_cross": "BlockedCross",
        "q_offside": "PlayerCaughtOffside",
        # Shot zone flags
        "q_shot_low_left": "LowLeft", "q_shot_low_centre": "LowCentre",
        "q_shot_low_right": "LowRight", "q_shot_high_left": "HighLeft",
        "q_shot_high_centre": "HighCentre", "q_shot_high_right": "HighRight",
        "q_miss_left": "MissLeft", "q_miss_right": "MissRight",
        "q_miss_high": "MissHigh", "q_small_box_right": "SmallBoxRight",
        "q_small_box_left": "SmallBoxLeft", "q_box_centre": "BoxCentre",
        "q_box_left": "BoxLeft", "q_box_right": "BoxRight",
        "q_oob_centre": "OutOfBoxCentre", "q_oob_left": "OutOfBoxLeft",
        "q_oob_deep_left": "OutOfBoxDeepLeft", "q_deep_box_left": "DeepBoxLeft",
    }
    for csv_col, qname in _flag_cols.items():
        v = row.get(csv_col)
        if v is not None and _safe_int(v) == 1:
            quals[qname] = True

    return json.dumps(quals, ensure_ascii=False) if quals else None


def _derive_body_part(row: dict) -> Optional[str]:
    """Deriva body_part de las flags q_right_foot, q_left_foot, q_head."""
    if _safe_int(row.get("q_right_foot")) == 1:
        return "RightFoot"
    if _safe_int(row.get("q_left_foot")) == 1:
        return "LeftFoot"
    if _safe_int(row.get("q_head")) == 1:
        return "Head"
    # OtherBodyPart no tiene flag propia en el extractor actual
    return None


def _derive_shot_zone(row: dict) -> Optional[str]:
    """Deriva la zona del campo desde donde se tira."""
    _zones = [
        ("q_box_centre", "BoxCentre"), ("q_box_left", "BoxLeft"),
        ("q_box_right", "BoxRight"), ("q_oob_centre", "OutOfBoxCentre"),
        ("q_oob_left", "OutOfBoxLeft"), ("q_oob_deep_left", "OutOfBoxDeepLeft"),
        ("q_deep_box_left", "DeepBoxLeft"),
        ("q_small_box_right", "SmallBoxRight"), ("q_small_box_left", "SmallBoxLeft"),
    ]
    for col, name in _zones:
        if _safe_int(row.get(col)) == 1:
            return name
    return None


def _derive_shot_placement(row: dict) -> Optional[str]:
    """Deriva hacia dónde va el tiro en la portería."""
    _placements = [
        ("q_shot_low_left", "LowLeft"), ("q_shot_low_centre", "LowCentre"),
        ("q_shot_low_right", "LowRight"), ("q_shot_high_left", "HighLeft"),
        ("q_shot_high_centre", "HighCentre"), ("q_shot_high_right", "HighRight"),
    ]
    for col, name in _placements:
        if _safe_int(row.get(col)) == 1:
            return name
    return None


def _derive_situation_detail(row: dict) -> Optional[str]:
    """Deriva la situación de juego del tiro/gol."""
    _situations = [
        ("q_regular_play", "RegularPlay"), ("q_from_corner", "FromCorner"),
        ("q_fast_break", "FastBreak"), ("q_direct_freekick", "DirectFreekick"),
    ]
    # Penalty se detecta mejor desde el event_type o situation, pero lo incluimos
    for col, name in _situations:
        if _safe_int(row.get(col)) == 1:
            return name
    return None


def _load_events_source(conn, source: str, file_pattern: str, files_dir: Path) -> int:
    """Carga eventos de una fuente genérica desde CSV.

    Para WhoScored enriched_events: extrae columnas individuales q_* y las
    mapea tanto a columnas dedicadas (body_part, goal_mouth_y, etc.) como
    al blob JSONB qualifiers.
    """
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
        "whoscored": "whoscored_team_id",
    }.get(source)

    is_ws = source == "whoscored"

    count = skipped = 0
    for row in all_rows:
        mid = _match_id_by_source(conn, source, _safe_int(row.get(mid_col)))
        pid = _player_id_by_source(conn, source, _safe_int(row.get(pid_col)))
        tid = _team_id_by_source(conn, source, _safe_int(row.get(tid_col))) if tid_col else None

        if not mid or not pid or not tid:
            skipped += 1
            continue

        # Asegura normalización de coordenadas en formato 0-1 antes de insertar
        x_norm, y_norm = _normalize_coordinates(
            _safe_float(row.get("x")),
            _safe_float(row.get("y"))
        )
        ex_norm, ey_norm = _normalize_coordinates(
            _safe_float(row.get("end_x")),
            _safe_float(row.get("end_y"))
        )

        # ── Qualifiers (solo WhoScored) ──────────────────────────
        if is_ws:
            # Dos formatos posibles de CSV:
            #   a) events.csv antiguo: columna "qualifiers" con JSON string
            #   b) enriched_events.csv: columnas individuales q_*
            quals_raw = row.get("qualifiers")
            quals = _qualifiers_json(quals_raw)

            # Si no hay columna qualifiers, reconstruir desde q_*
            if quals is None and row.get("q_right_foot") is not None:
                quals = _build_qualifiers_from_columns(row)

            # Parsear el JSON para extraer campos individuales
            # (funciona tanto si el JSON viene del CSV como si fue reconstruido)
            qdict: dict = {}
            if quals:
                try:
                    qdict = json.loads(quals)
                except (json.JSONDecodeError, TypeError):
                    qdict = {}

            ws_eid = _safe_int(row.get("event_id")) or _safe_int(row.get("whoscored_event_id"))

            # Body part: primero de q_* columns, luego de JSON
            body_part = _derive_body_part(row)
            if body_part is None:
                if qdict.get("RightFoot"):
                    body_part = "RightFoot"
                elif qdict.get("LeftFoot"):
                    body_part = "LeftFoot"
                elif qdict.get("Head"):
                    body_part = "Head"
                elif qdict.get("OtherBodyPart"):
                    body_part = "OtherBodyPart"

            # Numeric values: q_* columns fallback a JSON
            gm_y = _safe_float(row.get("q_goal_mouth_y")) or _safe_float(qdict.get("GoalMouthY"))
            gm_z = _safe_float(row.get("q_goal_mouth_z")) or _safe_float(qdict.get("GoalMouthZ"))
            angle = _safe_float(row.get("q_angle")) or _safe_float(qdict.get("Angle"))
            length = _safe_float(row.get("q_length")) or _safe_float(qdict.get("Length"))
            pe_x = _safe_float(row.get("q_pass_end_x")) or _safe_float(qdict.get("PassEndX"))
            pe_y = _safe_float(row.get("q_pass_end_y")) or _safe_float(qdict.get("PassEndY"))

            # Boolean flags: q_* columns fallback a JSON
            is_assisted = True if (_safe_int(row.get("q_assisted")) == 1 or qdict.get("Assisted")) else None
            is_indiv = True if (_safe_int(row.get("q_individual_play")) == 1 or qdict.get("IndividualPlay")) else None
            is_big = True if (_safe_int(row.get("q_big_chance")) == 1 or qdict.get("BigChance")) else None
            is_kp = True if (_safe_int(row.get("q_key_pass")) == 1 or qdict.get("KeyPass")) else None
            is_fb = True if (_safe_int(row.get("q_fast_break")) == 1 or qdict.get("FastBreak")) else None

            # Shot zone: q_* columns fallback a JSON
            shot_zone = _derive_shot_zone(row)
            if shot_zone is None:
                for zn in ("BoxCentre", "BoxLeft", "BoxRight", "OutOfBoxCentre",
                           "OutOfBoxLeft", "SmallBoxCentre", "SmallBoxRight", "SmallBoxLeft"):
                    if qdict.get(zn):
                        shot_zone = zn
                        break

            # Shot placement
            shot_placement = _derive_shot_placement(row)
            if shot_placement is None:
                for sp in ("LowLeft", "LowCentre", "LowRight",
                           "HighLeft", "HighCentre", "HighRight"):
                    if qdict.get(sp):
                        shot_placement = sp
                        break

            # Situation detail
            sit_detail = _derive_situation_detail(row)
            if sit_detail is None:
                for sd in ("RegularPlay", "FromCorner", "FastBreak",
                           "DirectFreekick", "SetPiece", "Penalty"):
                    if qdict.get(sd):
                        sit_detail = sd
                        break
            blk_x = _safe_float(row.get("q_blocked_x"))
            blk_y = _safe_float(row.get("q_blocked_y"))
        else:
            quals = None
            ws_eid = None
            body_part = gm_y = gm_z = angle = length = None
            pe_x = pe_y = None
            is_assisted = is_indiv = is_big = is_kp = is_fb = None
            shot_zone = shot_placement = sit_detail = None
            blk_x = blk_y = None

        conn.execute(text("""
            INSERT INTO fact_events
                (match_id, player_id, team_id, event_type,
                 minute, second, x, y, end_x, end_y,
                 outcome, data_source, qualifiers, whoscored_event_id,
                 body_part, goal_mouth_y, goal_mouth_z,
                 angle, length, pass_end_x, pass_end_y,
                 is_assisted, is_individual_play, is_big_chance,
                 is_key_pass, is_fast_break,
                 shot_zone, shot_placement, situation_detail,
                 blocked_x, blocked_y)
            VALUES
                (:mid, :pid, :tid, :etype,
                 :min, :sec, :x, :y, :ex, :ey,
                 :out, :src, CAST(:quals AS jsonb), :ws_eid,
                 :body_part, :gm_y, :gm_z,
                 :angle, :length, :pe_x, :pe_y,
                 :is_assisted, :is_indiv, :is_big,
                 :is_kp, :is_fb,
                 :shot_zone, :shot_placement, :sit_detail,
                 :blk_x, :blk_y)
            ON CONFLICT (
                match_id, player_id, event_type, minute,
                COALESCE(second, -1),
                COALESCE(x, -1.0),
                COALESCE(y, -1.0),
                data_source)
            DO UPDATE SET
                qualifiers = COALESCE(EXCLUDED.qualifiers, fact_events.qualifiers),
                whoscored_event_id = COALESCE(
                    EXCLUDED.whoscored_event_id, fact_events.whoscored_event_id),
                end_x = COALESCE(EXCLUDED.end_x, fact_events.end_x),
                end_y = COALESCE(EXCLUDED.end_y, fact_events.end_y),
                body_part = COALESCE(EXCLUDED.body_part, fact_events.body_part),
                goal_mouth_y = COALESCE(EXCLUDED.goal_mouth_y, fact_events.goal_mouth_y),
                goal_mouth_z = COALESCE(EXCLUDED.goal_mouth_z, fact_events.goal_mouth_z),
                angle = COALESCE(EXCLUDED.angle, fact_events.angle),
                length = COALESCE(EXCLUDED.length, fact_events.length),
                pass_end_x = COALESCE(EXCLUDED.pass_end_x, fact_events.pass_end_x),
                pass_end_y = COALESCE(EXCLUDED.pass_end_y, fact_events.pass_end_y),
                is_assisted = COALESCE(EXCLUDED.is_assisted, fact_events.is_assisted),
                is_individual_play = COALESCE(EXCLUDED.is_individual_play, fact_events.is_individual_play),
                is_big_chance = COALESCE(EXCLUDED.is_big_chance, fact_events.is_big_chance),
                is_key_pass = COALESCE(EXCLUDED.is_key_pass, fact_events.is_key_pass),
                is_fast_break = COALESCE(EXCLUDED.is_fast_break, fact_events.is_fast_break),
                shot_zone = COALESCE(EXCLUDED.shot_zone, fact_events.shot_zone),
                shot_placement = COALESCE(EXCLUDED.shot_placement, fact_events.shot_placement),
                situation_detail = COALESCE(EXCLUDED.situation_detail, fact_events.situation_detail),
                blocked_x = COALESCE(EXCLUDED.blocked_x, fact_events.blocked_x),
                blocked_y = COALESCE(EXCLUDED.blocked_y, fact_events.blocked_y)
        """), {
            "mid":   mid,
            "pid":   pid,
            "tid":   tid,
            "etype": row.get("event_type") or None,
            "min":   _safe_int(row.get("minute")),
            "sec":   _safe_int(row.get("second")),
            "x":     x_norm,
            "y":     y_norm,
            "ex":    ex_norm,
            "ey":    ey_norm,
            "out":   row.get("outcome") or row.get("outcome_type") or None,
            "src":   source,
            "quals": quals,
            "ws_eid": ws_eid,
            "body_part": body_part,
            "gm_y": gm_y,
            "gm_z": gm_z,
            "angle": angle,
            "length": length,
            "pe_x": pe_x,
            "pe_y": pe_y,
            "is_assisted": is_assisted,
            "is_indiv": is_indiv,
            "is_big": is_big,
            "is_kp": is_kp,
            "is_fb": is_fb,
            "shot_zone": shot_zone,
            "shot_placement": shot_placement,
            "sit_detail": sit_detail,
            "blk_x": blk_x,
            "blk_y": blk_y,
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
        total += _load_events_source(conn, "sofascore", "**/events.csv", ss_path)
    if sb_path:
        total += _load_events_source(conn, "statsbomb", "**/events.csv", sb_path)
    if ws_path:
        total += _load_events_source(conn, "whoscored", "**/*events*.csv", ws_path)
    log.info("[OK] fact_events completado — %d eventos insertados", total)
    return total




# ── FACT_INJURIES ─────────────────────────────────────────────────────────────

def load_injuries(conn,  tm_path: Path) -> int:
    """Carga fact_injuries desde injuries_clean.json de Transfermarkt."""

    log.info("[START] Cargando fact_injuries...")
    #
    files = list(tm_path.glob("**/injuries.csv"))

    if not files:
        log.warning("fact_injuries: no hay injuries.csv en %s", tm_path)
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
        # El CSV del scraper TM produce `player_id_tm`. Mantenemos fallbacks
        # a `id_transfermarkt`/`player_id` por si en el futuro se renombra.
        tm_pid = (
            _safe_int(row.get("player_id_tm"))
            or _safe_int(row.get("id_transfermarkt"))
            or _safe_int(row.get("player_id"))
        )
        sp_name = f"injury_{tm_pid}_{count}"
        conn.execute(text(f"SAVEPOINT {sp_name}"))

        try:
            pid = _player_id_by_source(conn, "transfermarkt", tm_pid)

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

