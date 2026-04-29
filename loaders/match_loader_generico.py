"""
loaders/match_loader.py
========================

Loader genérico de partidos. No contiene rutas hardcodeadas —
las recibe como parámetros para poder reutilizarse en cualquier competición.
Se refactorizan los metodos para que reciban como paramentros la ruta y el competition_id

Uso:
    from loaders.match_loader import load_matches
    load_matches(conn, ss_path=Path("data/raw/sofascore"), competition_id=1)
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

import pandas as pd
from sqlalchemy import text

from loaders.common import engine
from utils.season_utils import normalize_season

log = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent


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


# ── Helpers de resolución de FKs ─────────────────────────────────────────────

def _resolve_team_by_ss_id(conn, ss_id: int) -> Optional[int]:
    """Devuelve canonical_id de dim_team dado un id_sofascore."""
    if ss_id is None:
        return None
    row = conn.execute(
        text("SELECT canonical_id FROM dim_team WHERE id_sofascore = :sid LIMIT 1"),
        {"sid": int(ss_id)},
    ).fetchone()
    return row[0] if row else None


def _resolve_team_by_understat_id(conn, us_id: int) -> Optional[int]:
    """Devuelve canonical_id de dim_team dado un id_understat."""
    if us_id is None:
        return None
    row = conn.execute(
        text("SELECT canonical_id FROM dim_team WHERE id_understat = :uid LIMIT 1"),
        {"uid": int(us_id)},
    ).fetchone()
    return row[0] if row else None


def _resolve_team_by_sb_id(conn, sb_id: str) -> Optional[int]:
    """Devuelve canonical_id de dim_team dado un id_statsbomb."""
    if not sb_id:
        return None
    row = conn.execute(
        text("SELECT canonical_id FROM dim_team WHERE id_statsbomb = :sid LIMIT 1"),
        {"sid": str(sb_id)},
    ).fetchone()
    return row[0] if row else None


# ── Carga desde SofaScore ─────────────────────────────────────────────────────

def _load_from_sofascore(conn, ss_path: Path, competition_id: int) -> int:
    """
    Lee matches_clean.csv de SofaScore → upsert en dim_match.

    Parámetros:
        conn:           conexión a la base de datos
        ss_path:        ruta a la carpeta de SofaScore de la competición
        competition_id: canonical_id de dim_competition para enlazar el partido
    """
    files = list(ss_path.glob("**/matches_clean.csv"))
    if not files:
        log.warning("match_loader: no se encontraron matches_clean.csv en %s", ss_path)
        return 0

    all_rows: list[dict] = []
    for f in files:
        try:
            df = pd.read_csv(f)
            all_rows.extend(df.to_dict("records"))
        except Exception as e:
            log.error("Error reading matches file %s: %s", f, e)
            continue

    # deduplicar por id_sofascore
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
                log.debug("match %d: equipos no resueltos (h=%s, a=%s) — skip", sid, h_ss_id, a_ss_id)
                conn.execute(text(f"RELEASE SAVEPOINT {sp_name}"))
                skipped += 1
                continue

            match_date = _ensure_date(row.get("match_date"))
            competition = row.get("competition") or None
            season      = normalize_season(row.get("season"))
            home_score  = row.get("home_score")  if pd.notna(row.get("home_score")) else None
            away_score  = row.get("away_score")  if pd.notna(row.get("away_score")) else None

            conn.execute(text("""
                INSERT INTO dim_match
                    (match_date, competition, season,
                     home_team_id, away_team_id,
                     home_score, away_score,
                     data_source, id_sofascore, competition_id)
                VALUES
                    (:date, :comp, :season,
                     :hid, :aid,
                     :hsc, :asc,
                     'sofascore', :sid, :competition_id)
                ON CONFLICT (id_sofascore) WHERE id_sofascore IS NOT NULL
                DO UPDATE SET
                    match_date     = EXCLUDED.match_date,
                    home_score     = EXCLUDED.home_score,
                    away_score     = EXCLUDED.away_score,
                    competition    = EXCLUDED.competition,
                    season         = EXCLUDED.season,
                    competition_id = EXCLUDED.competition_id
            """), {
                "date":           match_date,
                "comp":           competition,
                "season":         season,
                "hid":            h_canonical,
                "aid":            a_canonical,
                "hsc":            int(home_score) if home_score is not None else None,
                "asc":            int(away_score) if away_score is not None else None,
                "sid":            sid,
                "competition_id": competition_id,
            })
            conn.execute(text(f"RELEASE SAVEPOINT {sp_name}"))
            inserted += 1
        except Exception as e:
            conn.execute(text(f"ROLLBACK TO SAVEPOINT {sp_name}"))
            log.error("Error inserting match %d: %s", sid, e)
            skipped += 1
            continue

    log.info("dim_match ← SofaScore: %d insertados | %d sin equipos resueltos", inserted, skipped)
    return inserted


# ── Carga desde Understat ─────────────────────────────────────────────────────

def _load_from_understat(conn, us_path: Path, competition_id: int) -> int:
    """
    Lee understat_matches_laliga.csv → añade id_understat a partidos ya cargados.

    Parámetros:
        conn:           conexión a la base de datos
        us_path:        ruta a la carpeta de Understat de la competición
        competition_id: canonical_id de dim_competition para filtrar la búsqueda
    """
    f = us_path / "understat_matches_laliga.csv"
    if not f.exists():
        log.info("match_loader: no hay understat_matches_laliga.csv en %s", us_path)
        return 0

    try:
        df = pd.read_csv(f)
    except Exception as e:
        log.warning("Error leyendo %s: %s", f, e)
        return 0

    linked = 0
    for _, row in df.iterrows():
        us_mid     = row.get("understat_match_id")
        us_home_id = row.get("home_team_id")
        us_away_id = row.get("away_team_id")
        date_str   = row.get("datetime", "")

        if not us_mid:
            continue

        match_date = str(date_str)[:10] if date_str else None

        h_canonical = _resolve_team_by_understat_id(conn, us_home_id)
        a_canonical = _resolve_team_by_understat_id(conn, us_away_id)

        if not h_canonical or not a_canonical or not match_date:
            continue

        existing = conn.execute(text("""
            SELECT match_id FROM dim_match
            WHERE match_date    = :date
              AND home_team_id  = :hid
              AND away_team_id  = :aid
              AND competition_id = :comp_id
            LIMIT 1
        """), {"date": match_date, "hid": h_canonical, "aid": a_canonical, "comp_id": competition_id}).fetchone()

        if existing:
            conn.execute(text("""
                UPDATE dim_match
                SET id_understat = :uid
                WHERE match_id = :mid AND id_understat IS NULL
            """), {"uid": int(us_mid), "mid": existing[0]})
            linked += 1
        else:
            hsc = row.get("home_goals")
            asc = row.get("away_goals")
            conn.execute(text("""
                INSERT INTO dim_match
                    (match_date, season, home_team_id, away_team_id,
                     home_score, away_score, data_source,
                     id_understat, competition_id)
                VALUES
                    (:date, :season, :hid, :aid,
                     :hsc, :asc, 'understat',
                     :uid, :comp_id)
                ON CONFLICT (id_understat) WHERE id_understat IS NOT NULL DO NOTHING
            """), {
                "date":     match_date,
                "season":   str(row.get("season", "")),
                "hid":      h_canonical,
                "aid":      a_canonical,
                "hsc":      int(hsc) if hsc is not None else None,
                "asc":      int(asc) if asc is not None else None,
                "uid":      int(us_mid),
                "comp_id":  competition_id,
            })
            linked += 1

    log.info("dim_match ← Understat: %d partidos enlazados/insertados", linked)
    return linked


# ── Carga desde StatsBomb ─────────────────────────────────────────────────────

def _load_from_statsbomb(conn, sb_path: Path, competition_id: int) -> int:
    """
    Lee matches_clean.csv de StatsBomb → añade id_statsbomb a partidos existentes.

    Parámetros:
        conn:           conexión a la base de datos
        sb_path:        ruta a la carpeta de StatsBomb de la competición
        competition_id: canonical_id de dim_competition para filtrar la búsqueda
    """
    files = list(sb_path.glob("**/matches_clean.csv"))
    if not files:
        return 0

    linked = 0
    for f in files:
        try:
            df = pd.read_csv(f)
        except Exception as e:
            log.warning("Error leyendo %s: %s", f, e)
            continue

        for _, row in df.iterrows():
            sb_mid    = row.get("id_statsbomb")
            data_date = _ensure_date(row.get("match_date"))
            h_name    = row.get("home_team_name")
            a_name    = row.get("away_team_name")

            if not sb_mid or not data_date or not h_name or not a_name:
                continue

            from utils.canonical_teams import normalize_team_name
            h_norm = normalize_team_name(h_name).lower()
            a_norm = normalize_team_name(a_name).lower()

            h_row = conn.execute(text("SELECT canonical_id FROM dim_team WHERE LOWER(canonical_name) = :n"), {"n": h_norm}).fetchone()
            a_row = conn.execute(text("SELECT canonical_id FROM dim_team WHERE LOWER(canonical_name) = :n"), {"n": a_norm}).fetchone()

            if not h_row or not a_row:
                continue

            existing = conn.execute(text("""
                SELECT match_id FROM dim_match
                WHERE match_date    = :date
                  AND home_team_id  = :hid
                  AND away_team_id  = :aid
                  AND competition_id = :comp_id
                LIMIT 1
            """), {
                "date":    data_date,
                "hid":     h_row[0],
                "aid":     a_row[0],
                "comp_id": competition_id,
            }).fetchone()

            if existing:
                conn.execute(text("""
                    UPDATE dim_match
                    SET id_statsbomb = :sid
                    WHERE match_id = :mid AND id_statsbomb IS NULL
                """), {"sid": str(sb_mid), "mid": existing[0]})
                linked += 1

    log.info("dim_match ← StatsBomb: %d partidos enlazados", linked)
    return linked


# ── Carga desde WhoScored ─────────────────────────────────────────────────────

def _load_from_whoscored(conn, ws_path: Path, competition_id: int) -> int:
    """
    Lee el CSV de eventos de WhoScored → añade id_whoscored a partidos existentes.
    Deriva los equipos de cada partido a partir de los eventos.

    Parámetros:
        conn:           conexión a la base de datos
        ws_path:        ruta a la carpeta de WhoScored de la competición
        competition_id: canonical_id de dim_competition para filtrar la búsqueda
    """
    # busca el CSV de eventos — puede llamarse de distinta forma según la competición
    event_files = list(ws_path.glob("*events*.csv"))
    if not event_files:
        log.info("match_loader: no hay CSV de eventos de WhoScored en %s", ws_path)
        return 0

    f = event_files[0]
    log.info("Analizando eventos de WhoScored: %s", f.name)

    try:
        df = pd.read_csv(f)
    except Exception as e:
        log.error("Error leyendo %s: %s", f, e)
        return 0

    # mapear match_id → (home_ws_id, away_ws_id, season)
    match_map: dict[str, dict] = {}
    for mid, group in df.groupby("whoscored_match_id"):
        starts       = group[group["event_type"] == "Start"]
        unique_teams = starts["whoscored_team_id"].unique().tolist()
        if len(unique_teams) < 2:
            unique_teams = group["whoscored_team_id"].dropna().unique().tolist()
        if len(unique_teams) >= 2:
            ws_season = normalize_season(str(group["season"].iloc[0]))
            match_map[str(mid)] = {
                "home_ws_id": int(unique_teams[0]),
                "away_ws_id": int(unique_teams[1]),
                "season":     ws_season,
            }

    linked = 0
    for ws_mid, info in match_map.items():
        h_row = conn.execute(text("SELECT canonical_id FROM dim_team WHERE id_whoscored = :sid"), {"sid": info["home_ws_id"]}).fetchone()
        a_row = conn.execute(text("SELECT canonical_id FROM dim_team WHERE id_whoscored = :sid"), {"sid": info["away_ws_id"]}).fetchone()

        if not h_row or not a_row:
            continue

        existing = conn.execute(text("""
            SELECT match_id FROM dim_match
            WHERE home_team_id   = :hid
              AND away_team_id   = :aid
              AND season         = :season
              AND competition_id = :comp_id
            LIMIT 1
        """), {
            "hid":     h_row[0],
            "aid":     a_row[0],
            "season":  info["season"],
            "comp_id": competition_id,
        }).fetchone()

        if existing:
            conn.execute(text("""
                UPDATE dim_match
                SET id_whoscored = :sid
                WHERE match_id = :mid AND id_whoscored IS NULL
            """), {"sid": int(ws_mid), "mid": existing[0]})
            linked += 1

    log.info("dim_match ← WhoScored: %d partidos enlazados", linked)
    return linked



# ── Punto de entrada genérico ─────────────────────────────────────────────────

def load_matches(
    conn,
    ss_path:        Path,
    competition_id: int,
    us_path:        Optional[Path] = None,
    sb_path:        Optional[Path] = None,
    ws_path:        Optional[Path] = None,
) -> int:
    """
    Carga dim_match para una competición concreta.
    SofaScore es la fuente master — ss_path y competition_id son obligatorios.
    Las rutas de Understat, StatsBomb y WhoScored son opcionales.

    Parámetros:
        conn:           conexión a la base de datos
        ss_path:        ruta a la carpeta de SofaScore de la competición
        competition_id: canonical_id de dim_competition
        us_path:        ruta a Understat (opcional)
        sb_path:        ruta a StatsBomb (opcional)
        ws_path:        ruta a WhoScored (opcional)
    """
    log.info("[START] Cargando dim_match — competition_id=%s", competition_id)

    _load_from_sofascore(conn, ss_path, competition_id)

    if us_path:
        _load_from_understat(conn, us_path, competition_id)

    if sb_path:
        _load_from_statsbomb(conn, sb_path, competition_id)

    if ws_path:
        _load_from_whoscored(conn, ws_path, competition_id)

    total = conn.execute(text("SELECT COUNT(*) FROM dim_match")).scalar()
    log.info("[OK] dim_match completado — %d partidos", total)
    return total