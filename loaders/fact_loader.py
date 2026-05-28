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

from loaders.common import engine, safe_read_csv as _safe_read_csv
from utils.canonical_teams import normalize_team_name
from utils.data_paths import iter_clean_csvs

log = logging.getLogger(__name__)


# Filtro de competición activo (lo setea cada load_* de hechos).
_active_comp_filter: list = [None]


def _iter_clean(source: str, filename: str) -> list[Path]:
    """Lista los CSV canónicos `data/clean/[<comp>/]*/<source>/<filename>.csv`."""
    return iter_clean_csvs(
        competition=_active_comp_filter[0],
        source=source, filename=filename,
    )


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

_SOURCE_COL_MAP = {
    "sofascore":     "id_sofascore",
    "understat":     "id_understat",
    "statsbomb":     "id_statsbomb",
    "whoscored":     "id_whoscored",
    "transfermarkt": "id_transfermarkt",
}


def _build_id_cache(conn, table: str, pk: str, source: str) -> dict:
    """Construye un dict {ext_id: canonical_id/match_id} con UN solo SELECT.

    Reemplaza llamadas N×_match_id_by_source / _player_id_by_source /
    _team_id_by_source por un acceso O(1) en memoria. Es lo que evita
    que la carga de fact_events se cuelgue con cientos de miles de filas.
    """
    col = _SOURCE_COL_MAP.get(source)
    if not col:
        return {}
    rows = conn.execute(
        text(f"SELECT {col}, {pk} FROM {table} WHERE {col} IS NOT NULL")
    ).fetchall()
    return {ext: int(canon) for ext, canon in rows if ext is not None}


def _match_id_by_source(conn, source: str, ext_id) -> Optional[int]:
    """Devuelve dim_match.match_id dado el ID externo de una fuente."""
    if ext_id is None:
        return None
    col = _SOURCE_COL_MAP.get(source)
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


# `_safe_read_csv` se importa de loaders.common (ver alias en el import).


# ── FACT_SHOTS ────────────────────────────────────────────────────────────────

def _load_shots_sofascore(conn) -> int:
    """Carga tiros de SofaScore desde data/clean/<comp>/<season>/sofascore/shots.csv."""
    files = _iter_clean("sofascore", "shots")
    if not files:
        log.info("fact_shots: no hay shots.csv de SofaScore")
        return 0

    all_rows: list[dict] = []
    for f in files:
        df = _safe_read_csv(f)
        if df is None or df.empty:
            continue
        all_rows.extend(df.to_dict("records"))

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
    """Carga tiros desde data/clean/<comp>/<season>/understat/shots.csv."""
    files = _iter_clean("understat", "shots")
    if not files:
        log.info("fact_shots: no hay shots.csv de Understat")
        return 0

    dfs = []
    for f in files:
        df = _safe_read_csv(f)
        if df is None or df.empty:
            continue
        dfs.append(df)
        log.info("  · %s", f.name)
    if not dfs:
        return 0
    df = pd.concat(dfs, ignore_index=True)

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


def load_shots(conn, comp_name: str | None = None) -> int:
    """Carga fact_shots desde SofaScore y Understat."""
    log.info("[START] Cargando fact_shots... (comp=%s)", comp_name or "todas")
    _active_comp_filter[0] = comp_name
    try:
        total = _load_shots_sofascore(conn) + _load_shots_understat(conn)
    finally:
        _active_comp_filter[0] = None
    log.info("[OK] fact_shots completado — %d tiros insertados", total)
    return total


# ── FACT_EVENTS ───────────────────────────────────────────────────────────────

def _load_events_source(conn, source: str) -> int:
    """Carga eventos de una fuente desde data/clean/<comp>/<season>/<source>/events.csv."""
    files = _iter_clean(source, "events")
    if not files:
        log.info("fact_events: no hay events.csv de %s", source)
        return 0

    all_rows: list[dict] = []
    for f in files:
        df = _safe_read_csv(f)
        if df is None or df.empty:
            continue
        all_rows.extend(df.to_dict("records"))

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

    # Pre-cachear FKs en memoria. Sin esto, con cientos de miles de filas
    # se ejecutarían millones de SELECTs y el proceso se cuelga.
    log.info("  precargando cachés de FKs (%s)...", source)
    match_cache  = _build_id_cache(conn, "dim_match",  "match_id",     source)
    player_cache = _build_id_cache(conn, "dim_player", "canonical_id", source)
    team_cache   = _build_id_cache(conn, "dim_team",   "canonical_id", source)
    home_team_by_match: dict[int, int] = {}
    rows = conn.execute(
        text("SELECT match_id, home_team_id FROM dim_match WHERE home_team_id IS NOT NULL")
    ).fetchall()
    for mid, hid in rows:
        home_team_by_match[int(mid)] = int(hid)
    log.info(
        "  cachés: matches=%d  players=%d  teams=%d",
        len(match_cache), len(player_cache), len(team_cache),
    )

    count = skipped = 0
    batch: list[dict] = []
    BATCH_SIZE = 5000
    insert_sql = text("""
        INSERT INTO fact_events
            (match_id, player_id, team_id, event_type,
             minute, second, x, y, end_x, end_y,
             outcome, data_source)
        VALUES
            (:mid, :pid, :tid, :etype,
             :min, :sec, :x, :y, :ex, :ey,
             :out, :src)
        ON CONFLICT (match_id, player_id, event_type, minute,
                     COALESCE(second, -1),
                     COALESCE(x, -1.0),
                     COALESCE(y, -1.0),
                     data_source)
        DO NOTHING
    """)

    def _flush():
        nonlocal batch
        if batch:
            conn.execute(insert_sql, batch)
            batch = []

    for row in all_rows:
        mid_ext = _safe_int(row.get(mid_col))
        pid_ext = _safe_int(row.get(pid_col))
        tid_ext = _safe_int(row.get(tid_col)) if tid_col else None

        mid = match_cache.get(mid_ext) if mid_ext is not None else None
        pid = player_cache.get(pid_ext) if pid_ext is not None else None
        tid = team_cache.get(tid_ext) if tid_ext is not None else None

        if not mid or not pid:
            skipped += 1
            continue
        if not tid:
            tid = home_team_by_match.get(mid)
        if not tid:
            skipped += 1
            continue

        batch.append({
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
        if len(batch) >= BATCH_SIZE:
            _flush()
            log.info("  · %d eventos insertados...", count)

    _flush()

    log.info("fact_events ← %s: %d insertados | %d sin FKs", source, count, skipped)
    return count


def load_events(conn, comp_name: str | None = None) -> int:
    """Carga fact_events desde SofaScore, StatsBomb y WhoScored."""
    log.info("[START] Cargando fact_events... (comp=%s)", comp_name or "todas")
    _active_comp_filter[0] = comp_name
    try:
        total = 0
        total += _load_events_source(conn, "sofascore")
        total += _load_events_source(conn, "statsbomb")
        total += _load_events_source(conn, "whoscored")
    finally:
        _active_comp_filter[0] = None
    log.info("[OK] fact_events completado — %d eventos insertados", total)
    return total


# ── FACT_INJURIES ─────────────────────────────────────────────────────────────

def load_injuries(conn, comp_name: str | None = None) -> int:
    """Carga fact_injuries desde `data/clean/<comp>/<season>/transfermarkt/injuries.csv`.

    El CSV puede usar 'player_id' o 'player_id_tm' como ID del jugador.
    """
    log.info("[START] Cargando fact_injuries... (comp=%s)", comp_name or "todas")
    _active_comp_filter[0] = comp_name
    files = _iter_clean("transfermarkt", "injuries")
    if not files:
        _active_comp_filter[0] = None
        log.warning("fact_injuries: no hay injuries.csv canónicos de Transfermarkt")
        log.warning("  → ejecuta el scraper de Transfermarkt para generar lesiones")
        return 0

    all_rows: list[dict] = []
    for f in files:
        df = _safe_read_csv(f)
        if df is None or df.empty:
            continue
        all_rows.extend(df.to_dict("records"))
        log.info("  · %s (%d filas)", f.name, len(df))

    count = skipped = 0
    for row in all_rows:
        # El ID puede venir como 'player_id' (Osen) o 'player_id_tm' (legado)
        tm_id = _safe_int(row.get("player_id") or row.get("player_id_tm"))
        sp_name = f"injury_{tm_id}_{count}"
        conn.execute(text(f"SAVEPOINT {sp_name}"))

        try:
            pid = _player_id_by_source(conn, "transfermarkt", tm_id)

            if not pid:
                conn.execute(text(f"RELEASE SAVEPOINT {sp_name}"))
                skipped += 1
                continue

            date_from  = _ensure_date(row.get("date_from"))
            date_until = _ensure_date(row.get("date_until"))

            conn.execute(text("""
                INSERT INTO fact_injuries
                    (player_id, season, injury_type, date_from,
                     date_until, days_absent, matches_missed,
                     club_name, club_id_tm, club_slug)
                VALUES
                    (:pid, :season, :itype, :dfrom,
                     :duntil, :days, :mm,
                     :club_name, :club_id_tm, :club_slug)
                ON CONFLICT (player_id, season, injury_type, date_from)
                DO UPDATE SET
                    club_name  = COALESCE(EXCLUDED.club_name,  fact_injuries.club_name),
                    club_id_tm = COALESCE(EXCLUDED.club_id_tm, fact_injuries.club_id_tm),
                    club_slug  = COALESCE(EXCLUDED.club_slug,  fact_injuries.club_slug)
            """), {
                "pid":        pid,
                "season":     row.get("season") or None,
                "itype":      row.get("injury_type") or None,
                "dfrom":      date_from,
                "duntil":     date_until,
                "days":       _safe_int(row.get("days_absent")),
                "mm":         _safe_int(row.get("matches_missed")),
                "club_name":  row.get("club_name") or None,
                "club_id_tm": _safe_int(row.get("club_id_tm")),
                "club_slug":  row.get("club_slug") or None,
            })
            conn.execute(text(f"RELEASE SAVEPOINT {sp_name}"))
            count += 1
        except Exception as e:
            conn.execute(text(f"ROLLBACK TO SAVEPOINT {sp_name}"))
            log.warning("Error inserting injury record: %s", e)
            skipped += 1
            continue

    _active_comp_filter[0] = None
    log.info("fact_injuries ← Transfermarkt: %d insertadas | %d sin jugador resuelto", count, skipped)
    return count


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s - %(message)s")
    with engine.begin() as conn:
        load_shots(conn)
        load_events(conn)
        load_injuries(conn)
