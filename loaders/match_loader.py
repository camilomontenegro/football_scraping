"""
loaders/match_loader.py
========================
Carga dim_match desde los CSVs canónicos en
`data/clean/<comp>/<season>/<source>/<table>.csv`.

FUENTES (en orden):
    1. SofaScore  matches.csv → MASTER (id_sofascore)
    2. Understat  matches.csv → enlaza/inserta + id_understat
    3. StatsBomb  matches.csv → enlaza id_statsbomb por nombre
    4. WhoScored  events.csv  → enlaza/inserta partidos por id_whoscored

Convenciones:
    - season SIEMPRE en formato canónico 'YYYY/YYYY' (utils.season_utils.normalize_season)
    - dim_team debe estar cargado antes (necesitamos canonical_id por id_*).
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd
from sqlalchemy import text

from loaders.common import engine, safe_read_csv
from utils.season_utils import normalize_season
from utils.data_paths import iter_clean_csvs

log = logging.getLogger(__name__)


# Filtro de competición activo (lo setea load_matches para encadenar a sub-pasos).
_active_comp_filter: list = [None]


def _iter_clean(source: str, filename: str) -> list:
    """Lista todos los CSV canónicos `data/clean/[<comp>/]*/<source>/<filename>.csv`."""
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
            return datetime.fromtimestamp(val / 1000.0).date().isoformat()
        except Exception:
            return None
    return str(val)[:10]


def _safe_int(val) -> Optional[int]:
    """Convierte valores numericos opcionales; NaN/cadenas vacias -> None."""
    if val is None or pd.isna(val):
        return None
    text_val = str(val).strip()
    if text_val.lower() in ("", "nan", "none"):
        return None
    try:
        return int(float(text_val))
    except (TypeError, ValueError):
        return None


# ── Helpers ──────────────────────────────────────────────────────────────────

def _build_competition_alias_map() -> dict[str, str]:
    """alias-en-minúsculas → canonical_name esperado en dim_competition.

    Construido en tiempo de ejecución desde `wizard.competitions.COMPETITIONS`:
        • La key del dict (el nombre canónico que usa el wizard).
        • `name` (cómo se llama en los CSV de algunos scrapers, p.ej. "LaLiga").
        • `sources.<src>.name` de cada fuente.
    """
    try:
        from wizard.competitions import COMPETITIONS
    except Exception:
        return {}
    out: dict[str, str] = {}
    for canonical, conf in COMPETITIONS.items():
        out[canonical.lower()] = canonical
        nm = conf.get("name")
        if isinstance(nm, str) and nm:
            out.setdefault(nm.lower(), canonical)
        for src_conf in (conf.get("sources") or {}).values():
            src_nm = (src_conf or {}).get("name")
            if isinstance(src_nm, str) and src_nm:
                out.setdefault(src_nm.lower(), canonical)
    return out


_COMPETITION_ALIAS_MAP: dict[str, str] = _build_competition_alias_map()


def _competition_id_resolver(conn):
    """Devuelve `resolve(raw_name) → canonical_id | None`, cacheado por loader.

    Estrategia:
        1. Match exacto contra `dim_competition.canonical_name`.
        2. Match contra el alias-map construido desde COMPETITIONS.
        3. Si `raw_name` es "X, Y" (p.ej. "UEFA Champions League, Group A"),
           se prueba sólo con la parte antes de la coma.
        4. ILIKE substring contra `canonical_name` como último recurso.
    El valor `None` (no encontrado) también se cachea para no repetir queries.
    """
    name_to_id: dict[str, int] = {}
    for r in conn.execute(
        text("SELECT canonical_id, canonical_name FROM dim_competition")
    ).fetchall():
        name_to_id[(r[1] or "").lower()] = r[0]

    cache: dict[str, Optional[int]] = {}

    def _lookup(raw: Optional[str]) -> Optional[int]:
        if not raw:
            return None
        key = raw.strip().lower()
        if key in cache:
            return cache[key]

        cid = name_to_id.get(key)
        if cid is None:
            mapped = _COMPETITION_ALIAS_MAP.get(key)
            if mapped:
                cid = name_to_id.get(mapped.lower())

        if cid is None and "," in raw:
            head = raw.split(",", 1)[0].strip().lower()
            cid = name_to_id.get(head)
            if cid is None:
                mapped = _COMPETITION_ALIAS_MAP.get(head)
                if mapped:
                    cid = name_to_id.get(mapped.lower())

        if cid is None:
            row = conn.execute(
                text(
                    "SELECT canonical_id FROM dim_competition "
                    "WHERE LOWER(canonical_name) = :n "
                    "   OR LOWER(canonical_name) LIKE :like_n "
                    "ORDER BY LENGTH(canonical_name) ASC LIMIT 1"
                ),
                {"n": key, "like_n": f"%{key}%"},
            ).fetchone()
            cid = row[0] if row else None

        cache[key] = cid
        if cid is None:
            log.warning(
                "match_loader: no se pudo mapear competition=%r a dim_competition",
                raw,
            )
        return cid

    return _lookup


def _resolve_team_by_ss_id(conn, ss_id) -> Optional[int]:
    if ss_id is None:
        return None
    row = conn.execute(
        text("SELECT canonical_id FROM dim_team WHERE id_sofascore = :sid LIMIT 1"),
        {"sid": int(ss_id)},
    ).fetchone()
    return row[0] if row else None


def _resolve_team_by_understat_id(conn, us_id) -> Optional[int]:
    if us_id is None:
        return None
    row = conn.execute(
        text("SELECT canonical_id FROM dim_team WHERE id_understat = :uid LIMIT 1"),
        {"uid": int(us_id)},
    ).fetchone()
    return row[0] if row else None


def _resolve_team_by_sb_id(conn, sb_id) -> Optional[int]:
    if not sb_id:
        return None
    row = conn.execute(
        text("SELECT canonical_id FROM dim_team WHERE id_statsbomb = :sid LIMIT 1"),
        {"sid": str(sb_id)},
    ).fetchone()
    return row[0] if row else None


# ── SofaScore ─────────────────────────────────────────────────────────────────

def _load_from_sofascore(conn) -> int:
    files = _iter_clean("sofascore", "matches")
    if not files:
        log.warning("match_loader: no se encontraron matches.csv de SofaScore")
        return 0

    all_rows: list[dict] = []
    for f in files:
        df = safe_read_csv(f)
        if df is None or df.empty:
            continue
        all_rows.extend(df.to_dict("records"))

    seen: dict[int, dict] = {}
    for row in all_rows:
        sid = row.get("id_sofascore")
        if sid is None:
            continue
        try:
            sid = int(sid)
        except (ValueError, TypeError):
            continue
        if sid not in seen:
            seen[sid] = row

    resolve_comp_id = _competition_id_resolver(conn)

    inserted = skipped = 0
    for sid, row in seen.items():
        sp_name = f"match_{sid}"
        conn.execute(text(f"SAVEPOINT {sp_name}"))
        try:
            h_ss_id = row.get("home_team_id_ss")
            a_ss_id = row.get("away_team_id_ss")
            h_canonical = _resolve_team_by_ss_id(conn, h_ss_id) if h_ss_id else None
            a_canonical = _resolve_team_by_ss_id(conn, a_ss_id) if a_ss_id else None

            if not h_canonical or not a_canonical:
                conn.execute(text(f"RELEASE SAVEPOINT {sp_name}"))
                skipped += 1
                continue

            match_date  = _ensure_date(row.get("match_date"))
            competition = row.get("competition") or "La Liga"
            season      = normalize_season(row.get("season"))
            home_score  = row.get("home_score") if pd.notna(row.get("home_score")) else None
            away_score  = row.get("away_score") if pd.notna(row.get("away_score")) else None
            comp_id     = resolve_comp_id(competition)

            attendance_raw = row.get("attendance")
            attendance_val = _safe_int(attendance_raw) if pd.notna(attendance_raw) else None

            conn.execute(text("""
                INSERT INTO dim_match
                    (match_date, competition, season,
                     home_team_id, away_team_id,
                     competition_id,
                     home_score, away_score,
                     attendance,
                     data_source, id_sofascore)
                VALUES
                    (:date, :comp, :season, :hid, :aid, :cid,
                     :hsc, :asc, :att, 'sofascore', :sid)
                ON CONFLICT (id_sofascore) WHERE id_sofascore IS NOT NULL
                DO UPDATE SET
                    match_date     = EXCLUDED.match_date,
                    home_score     = EXCLUDED.home_score,
                    away_score     = EXCLUDED.away_score,
                    competition    = EXCLUDED.competition,
                    season         = EXCLUDED.season,
                    competition_id = COALESCE(EXCLUDED.competition_id, dim_match.competition_id),
                    attendance     = COALESCE(EXCLUDED.attendance, dim_match.attendance)
            """), {
                "date": match_date,
                "comp": competition,
                "season": season,
                "hid":  h_canonical,
                "aid":  a_canonical,
                "cid":  comp_id,
                "hsc":  _safe_int(home_score),
                "asc":  _safe_int(away_score),
                "att":  attendance_val,
                "sid":  sid,
            })
            conn.execute(text(f"RELEASE SAVEPOINT {sp_name}"))
            inserted += 1
        except Exception as e:
            conn.execute(text(f"ROLLBACK TO SAVEPOINT {sp_name}"))
            log.error("Error inserting match %d: %s", sid, e)
            skipped += 1

    log.info("dim_match ← SofaScore: %d insertados | %d sin equipos resueltos", inserted, skipped)
    return inserted


# ── Understat ─────────────────────────────────────────────────────────────────

def _load_from_understat(conn) -> int:
    """Lee TODOS los matches.csv de Understat → enlaza/inserta partidos.

    Matching:
      1. Exacto por (match_date, home_id, away_id).
      2. Fallback laxo por (home_id, away_id, season) — útil cuando los partidos
         vinieron de WhoScored sin match_date. Aprovecha para rellenar la fecha.
    """
    files = _iter_clean("understat", "matches")
    if not files:
        log.info("match_loader: no hay matches.csv de Understat")
        return 0

    dfs = []
    for f in files:
        d = safe_read_csv(f)
        if d is None or d.empty:
            continue
        try:
            # Inferir competition desde la carpeta padre si no viene en el CSV:
            # data/clean/<comp_slug>/<season>/understat/matches.csv → <comp_slug>
            if "competition" not in d.columns:
                comp_slug = f.parents[2].name  # 0=file dir is .../understat, 1=season, 2=comp
                d["competition"] = comp_slug.replace("_", " ").title() if comp_slug else "Unknown"
            dfs.append(d)
            log.info("  · %s", f)
        except Exception as e:
            log.warning("Error leyendo %s: %s", f, e)
    if not dfs:
        return 0
    df = pd.concat(dfs, ignore_index=True)

    resolve_comp_id = _competition_id_resolver(conn)

    linked = 0
    for _, row in df.iterrows():
        us_mid     = row.get("understat_match_id")
        us_home_id = row.get("home_team_id")
        us_away_id = row.get("away_team_id")
        date_str   = row.get("datetime", "")

        if not us_mid:
            continue

        match_date = None
        if date_str:
            try:
                match_date = str(date_str)[:10]
            except Exception:
                pass

        h_canonical = _resolve_team_by_understat_id(conn, us_home_id)
        a_canonical = _resolve_team_by_understat_id(conn, us_away_id)
        if not h_canonical or not a_canonical:
            continue

        norm_season = normalize_season(row.get("season"))
        raw_comp    = str(row.get("competition") or "Unknown")
        comp_id     = resolve_comp_id(raw_comp)

        # 1) Match exacto por fecha + equipos
        existing = None
        if match_date:
            existing = conn.execute(text("""
                SELECT match_id, match_date FROM dim_match
                WHERE match_date = :date
                  AND home_team_id = :hid
                  AND away_team_id = :aid
                LIMIT 1
            """), {"date": match_date, "hid": h_canonical, "aid": a_canonical}).fetchone()

        # 2) Fallback laxo por equipos + season
        if not existing and norm_season:
            existing = conn.execute(text("""
                SELECT match_id, match_date FROM dim_match
                WHERE home_team_id = :hid
                  AND away_team_id = :aid
                  AND season = :season
                LIMIT 1
            """), {"hid": h_canonical, "aid": a_canonical, "season": norm_season}).fetchone()

        if existing:
            ex_id, ex_date = existing
            conn.execute(text("""
                UPDATE dim_match
                SET id_understat   = COALESCE(id_understat, :uid),
                    competition_id = COALESCE(competition_id, :cid)
                WHERE match_id = :mid
            """), {"uid": int(us_mid), "cid": comp_id, "mid": ex_id})
            if match_date and not ex_date:
                conn.execute(text("""
                    UPDATE dim_match SET match_date = :d
                    WHERE match_id = :mid AND match_date IS NULL
                """), {"d": match_date, "mid": ex_id})
            linked += 1
        else:
            hsc = row.get("home_goals")
            asc = row.get("away_goals")
            conn.execute(text("""
                INSERT INTO dim_match
                    (match_date, competition, season,
                     home_team_id, away_team_id,
                     competition_id,
                     home_score, away_score,
                     data_source, id_understat)
                VALUES
                    (:date, :comp, :season, :hid, :aid, :cid,
                     :hsc, :asc, 'understat', :uid)
                ON CONFLICT (id_understat) WHERE id_understat IS NOT NULL DO NOTHING
            """), {
                "date":   match_date,
                "comp":   raw_comp,
                "season": norm_season,
                "hid":    h_canonical,
                "aid":    a_canonical,
                "cid":    comp_id,
                "hsc":    _safe_int(hsc),
                "asc":    _safe_int(asc),
                "uid":    int(us_mid),
            })
            linked += 1

    log.info("dim_match ← Understat: %d enlazados/insertados", linked)
    return linked


# ── StatsBomb ────────────────────────────────────────────────────────────────

def _load_from_statsbomb(conn) -> int:
    files = _iter_clean("statsbomb", "matches")
    if not files:
        return 0

    resolve_comp_id = _competition_id_resolver(conn)

    linked = 0
    for f in files:
        df = safe_read_csv(f)
        if df is None or df.empty:
            continue

        for _, row in df.iterrows():
            sb_mid    = row.get("id_statsbomb")
            data_date = _ensure_date(row.get("match_date"))
            norm_season = normalize_season(row.get("season")) or str(row.get("season", ""))
            raw_comp = row.get("competition")

            h_name = row.get("home_team_name")
            a_name = row.get("away_team_name")
            if not sb_mid or not data_date or not h_name or not a_name:
                continue

            from utils.canonical_teams import normalize_team_name
            h_norm = normalize_team_name(h_name).lower()
            a_norm = normalize_team_name(a_name).lower()

            h_row = conn.execute(text(
                "SELECT canonical_id FROM dim_team WHERE LOWER(canonical_name) = :n"
            ), {"n": h_norm}).fetchone()
            a_row = conn.execute(text(
                "SELECT canonical_id FROM dim_team WHERE LOWER(canonical_name) = :n"
            ), {"n": a_norm}).fetchone()
            if not h_row or not a_row:
                continue

            existing = conn.execute(text("""
                SELECT match_id FROM dim_match
                WHERE match_date = :date
                  AND home_team_id = :hid
                  AND away_team_id = :aid
                  AND season = :season
                LIMIT 1
            """), {
                "date":   data_date,
                "hid":    h_row[0],
                "aid":    a_row[0],
                "season": norm_season,
            }).fetchone()

            if existing:
                comp_id = resolve_comp_id(raw_comp) if raw_comp else None
                att_raw = row.get("attendance")
                att_val = _safe_int(att_raw) if pd.notna(att_raw) else None
                conn.execute(text("""
                    UPDATE dim_match
                    SET id_statsbomb   = COALESCE(id_statsbomb, :sid),
                        competition_id = COALESCE(competition_id, :cid),
                        attendance     = COALESCE(dim_match.attendance, :att)
                    WHERE match_id = :mid
                """), {"sid": str(sb_mid), "cid": comp_id, "att": att_val, "mid": existing[0]})
                linked += 1

    log.info("dim_match ← StatsBomb: %d partidos enlazados", linked)
    return linked


# ── WhoScored ────────────────────────────────────────────────────────────────

def _load_from_whoscored(conn) -> int:
    """Lee TODOS los events.csv de WhoScored → enlaza/inserta partidos.

    Saca equipos de los eventos y la fecha del matches.csv de cada (comp, season).
    """
    files = _iter_clean("whoscored", "events")
    if not files:
        log.info("match_loader: no hay events.csv de WhoScored")
        return 0

    log.info("Analizando eventos de WhoScored para vincular partidos (%d archivos)...", len(files))

    # Cache whoscored_match_id -> {match_date, attendance} desde matches.csv
    match_dates: dict[str, str] = {}
    match_attendance: dict[str, int] = {}
    for f in _iter_clean("whoscored", "matches"):
        mdf = safe_read_csv(f)
        if mdf is None or mdf.empty:
            continue
        if "whoscored_match_id" not in mdf.columns:
            continue
        for _, mr in mdf.iterrows():
            mid_v = mr.get("whoscored_match_id")
            if not pd.notna(mid_v):
                continue
            mid_str = str(mid_v)
            mdate = mr.get("match_date")
            if pd.notna(mdate) and str(mdate).strip():
                match_dates[mid_str] = str(mdate)[:10]
            att = mr.get("attendance")
            if pd.notna(att):
                att_int = _safe_int(att)
                if att_int:
                    match_attendance[mid_str] = att_int
    if match_dates:
        log.info("  fechas cargadas: %d partidos con match_date conocida", len(match_dates))
    if match_attendance:
        log.info("  attendance cargado: %d partidos con asistencia", len(match_attendance))

    match_map: dict[str, dict] = {}
    for f in files:
        # Inferir competition desde el path: data/clean/<comp>/<season>/whoscored/events.csv
        comp_from_file = f.parents[2].name.replace("_", " ").title()
        df = safe_read_csv(f)
        if df is None or df.empty:
            continue
        for mid, group in df.groupby("whoscored_match_id"):
            starts = group[group["event_type"] == "Start"]
            unique_teams = starts["whoscored_team_id"].unique().tolist()
            if len(unique_teams) < 2:
                unique_teams = group["whoscored_team_id"].dropna().unique().tolist()
            if len(unique_teams) < 2:
                continue

            ws_season = str(group["season"].iloc[0])
            comp_value = (
                str(group["competition"].iloc[0])
                if "competition" in group.columns and pd.notna(group["competition"].iloc[0])
                else comp_from_file
            )
            norm_season = normalize_season(ws_season) or ws_season

            match_map[str(mid)] = {
                "home_ws_id":  int(unique_teams[0]),
                "away_ws_id":  int(unique_teams[1]),
                "season":      norm_season,
                "competition": comp_value,
            }

    # Pre-cache id_whoscored -> canonical_id
    team_cache: dict[int, int] = {}
    rows = conn.execute(
        text("SELECT id_whoscored, canonical_id FROM dim_team WHERE id_whoscored IS NOT NULL")
    ).fetchall()
    for ws_id, can_id in rows:
        team_cache[int(ws_id)] = can_id

    resolve_comp_id = _competition_id_resolver(conn)

    linked = inserted = updated_dates = skipped_no_team = skipped_duplicate = 0
    for ws_mid, info in match_map.items():
        hid = team_cache.get(info["home_ws_id"])
        aid = team_cache.get(info["away_ws_id"])
        if not hid or not aid:
            skipped_no_team += 1
            continue

        m_date = match_dates.get(str(ws_mid))
        m_att  = match_attendance.get(str(ws_mid))
        ws_mid_int = int(ws_mid)
        comp_id = resolve_comp_id(info.get("competition"))

        assigned = conn.execute(text("""
            SELECT match_id, match_date FROM dim_match
            WHERE id_whoscored = :sid
            LIMIT 1
        """), {"sid": ws_mid_int}).fetchone()
        if assigned:
            assigned_id, assigned_date = assigned
            if m_date and not assigned_date:
                conn.execute(text("""
                    UPDATE dim_match SET match_date = :d
                    WHERE match_id = :mid AND match_date IS NULL
                """), {"d": m_date, "mid": assigned_id})
                updated_dates += 1
            conn.execute(text("""
                UPDATE dim_match
                SET competition_id = COALESCE(competition_id, :cid),
                    attendance     = COALESCE(dim_match.attendance, :att)
                WHERE match_id = :mid
            """), {"cid": comp_id, "att": m_att, "mid": assigned_id})
            linked += 1
            continue

        existing = conn.execute(text("""
            SELECT match_id, match_date, id_whoscored FROM dim_match
            WHERE home_team_id = :hid
              AND away_team_id = :aid
              AND season = :season
            LIMIT 1
        """), {"hid": hid, "aid": aid, "season": info["season"]}).fetchone()

        if existing:
            ex_id, ex_date, ex_ws_id = existing
            if ex_ws_id is not None:
                skipped_duplicate += 1
                continue
            conn.execute(text("""
                UPDATE dim_match
                SET id_whoscored   = COALESCE(id_whoscored, :sid),
                    competition_id = COALESCE(competition_id, :cid),
                    attendance     = COALESCE(dim_match.attendance, :att)
                WHERE match_id = :mid
            """), {"sid": ws_mid_int, "cid": comp_id, "att": m_att, "mid": ex_id})
            linked += 1
            if m_date and not ex_date:
                conn.execute(text("""
                    UPDATE dim_match SET match_date = :d
                    WHERE match_id = :mid AND match_date IS NULL
                """), {"d": m_date, "mid": ex_id})
                updated_dates += 1
        else:
            conn.execute(text("""
                INSERT INTO dim_match
                    (match_date, competition, season,
                     home_team_id, away_team_id,
                     competition_id, attendance,
                     data_source, id_whoscored)
                VALUES
                    (:date, :comp, :season, :hid, :aid, :cid,
                     :att, 'whoscored', :sid)
                ON CONFLICT (id_whoscored) WHERE id_whoscored IS NOT NULL DO NOTHING
            """), {
                "date":   m_date,
                "comp":   info["competition"],
                "season": info["season"],
                "hid":    hid,
                "aid":    aid,
                "cid":    comp_id,
                "att":    m_att,
                "sid":    ws_mid_int,
            })
            inserted += 1

    log.info(
        "dim_match ← WhoScored: %d enlazados | %d insertados | %d fechas añadidas | %d sin equipos",
        linked, inserted, updated_dates, skipped_no_team,
    )
    return linked + inserted


# ── Transfermarkt attendance backfill ─────────────────────────────────────────

def _backfill_attendance_from_transfermarkt(conn) -> int:
    """Backfill dim_match.attendance from Transfermarkt attendance CSVs.

    Matches by (home_team_name, away_team_name, season) using fuzzy team name
    resolution. Only fills attendance where it's currently NULL.
    """
    files = _iter_clean("transfermarkt", "attendance")
    if not files:
        return 0

    from utils.canonical_teams import normalize_team_name

    updated = 0
    for f in files:
        df = safe_read_csv(f)
        if df is None or df.empty:
            continue

        for _, row in df.iterrows():
            att_raw = row.get("attendance")
            if not pd.notna(att_raw):
                continue
            att_val = _safe_int(att_raw)
            if not att_val or att_val < 100:
                continue

            h_name = row.get("home_team")
            a_name = row.get("away_team")
            if not h_name or not a_name:
                continue

            h_norm = normalize_team_name(str(h_name)).lower()
            a_norm = normalize_team_name(str(a_name)).lower()

            h_row = conn.execute(text(
                "SELECT canonical_id FROM dim_team WHERE LOWER(canonical_name) = :n"
            ), {"n": h_norm}).fetchone()
            a_row = conn.execute(text(
                "SELECT canonical_id FROM dim_team WHERE LOWER(canonical_name) = :n"
            ), {"n": a_norm}).fetchone()
            if not h_row or not a_row:
                continue

            match_date = _ensure_date(row.get("match_date"))
            norm_season = normalize_season(row.get("season"))

            # Match by date + teams (most precise)
            existing = None
            if match_date:
                existing = conn.execute(text("""
                    SELECT match_id FROM dim_match
                    WHERE match_date = :date
                      AND home_team_id = :hid
                      AND away_team_id = :aid
                      AND attendance IS NULL
                    LIMIT 1
                """), {"date": match_date, "hid": h_row[0], "aid": a_row[0]}).fetchone()

            # Fallback by teams + season
            if not existing and norm_season:
                existing = conn.execute(text("""
                    SELECT match_id FROM dim_match
                    WHERE home_team_id = :hid
                      AND away_team_id = :aid
                      AND season = :season
                      AND attendance IS NULL
                    LIMIT 1
                """), {"hid": h_row[0], "aid": a_row[0], "season": norm_season}).fetchone()

            if existing:
                conn.execute(text("""
                    UPDATE dim_match SET attendance = :att
                    WHERE match_id = :mid AND attendance IS NULL
                """), {"att": att_val, "mid": existing[0]})
                updated += 1

    if updated:
        log.info("dim_match ← Transfermarkt attendance: %d partidos actualizados", updated)
    return updated


# ── Punto de entrada ──────────────────────────────────────────────────────────

def backfill_competition_id(conn) -> int:
    """Rellena `dim_match.competition_id` en las filas que lo tienen NULL.

    Usa el mismo resolutor que los loaders, de modo que aplica el mismo
    mapeo de alias ("LaLiga" → "La Liga", "UEFA Champions League, Group A"
    → "Champions League", …). Idempotente: vuelve a ejecutarlo cuando
    quieras; sólo toca filas con NULL.
    """
    resolve_comp_id = _competition_id_resolver(conn)
    rows = conn.execute(text("""
        SELECT DISTINCT competition
        FROM dim_match
        WHERE competition_id IS NULL AND competition IS NOT NULL
    """)).fetchall()

    total_updated = 0
    for (raw_comp,) in rows:
        cid = resolve_comp_id(raw_comp)
        if cid is None:
            continue
        result = conn.execute(text("""
            UPDATE dim_match
            SET competition_id = :cid
            WHERE competition_id IS NULL
              AND competition = :raw
        """), {"cid": cid, "raw": raw_comp})
        updated = result.rowcount or 0
        if updated:
            log.info(
                "backfill competition_id: %s → id=%d (%d filas)",
                raw_comp, cid, updated,
            )
        total_updated += updated

    remaining = conn.execute(text(
        "SELECT COUNT(*) FROM dim_match WHERE competition_id IS NULL"
    )).scalar() or 0
    log.info(
        "[OK] backfill competition_id: %d filas actualizadas, %d siguen NULL",
        total_updated, remaining,
    )
    return total_updated


def load_matches(conn, comp_name: str | None = None) -> int:
    """Carga dim_match desde todas las fuentes.

    Args:
        comp_name: si se especifica restringe a `data/clean/<comp_slug>/...`.
    """
    log.info("[START] Cargando dim_match... (comp=%s)", comp_name or "todas")
    _active_comp_filter[0] = comp_name
    try:
        _load_from_sofascore(conn)
        _load_from_understat(conn)
        _load_from_statsbomb(conn)
        _load_from_whoscored(conn)
        _backfill_attendance_from_transfermarkt(conn)
        backfill_competition_id(conn)
    finally:
        _active_comp_filter[0] = None

    total = conn.execute(text("SELECT COUNT(*) FROM dim_match")).scalar()
    log.info("[OK] dim_match completado — %d partidos", total)
    return total


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s - %(message)s")
    with engine.begin() as conn:
        load_matches(conn)
