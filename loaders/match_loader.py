"""
loaders/match_loader.py
========================
Carga dim_match desde los archivos CSV producidos por los scrapers.

FUENTES (en orden de prioridad):
    1. SofaScore matches_clean.csv  → MASTER (establece id_sofascore)
    2. Understat matches CSV        → añade id_understat a partidos ya existentes

Dependencias:
    - dim_team debe estar cargado antes (necesitamos canonical_id por id_sofascore)

    data_source, id_sofascore, id_understat, id_statsbomb, id_whoscored
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd
from sqlalchemy import text

from loaders.common import engine

log = logging.getLogger(__name__)

# Usar ruta absoluta basada en la carpeta del proyecto
PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_SS = PROJECT_ROOT / "data" / "raw" / "sofascore"
RAW_US = PROJECT_ROOT / "data" / "raw" / "understat"
RAW_SB = PROJECT_ROOT / "data" / "raw" / "statsbomb"
RAW_WS = PROJECT_ROOT / "data" / "raw" / "whoscored"


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



# ── Helpers ──────────────────────────────────────────────────────────────────

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

def _load_from_sofascore(conn) -> int:
    """Lee matches_clean.csv de SofaScore → upsert en dim_match.

    SofaScore es la fuente master de partidos:
        - canonical_name de equipos via id_sofascore
        - match_date, competition, season, scores
    """
    files = list(RAW_SS.glob("**/matches_clean.csv"))
    if not files:
        log.warning("match_loader: no se encontraron matches_clean.csv en %s", RAW_SS)
        return 0

    all_rows: list[dict] = []
    for f in files:
        try:
            df = pd.read_csv(f)
            all_rows.extend(df.to_dict("records"))
        except Exception as e:
            log.error("Error reading matches file %s: %s", f, e)
            continue

    # Deduplicar por id_sofascore
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

            match_date  = _ensure_date(row.get("match_date"))
            competition = row.get("competition") or "La Liga"
            season      = row.get("season")      or None
            home_score  = row.get("home_score")  if pd.notna(row.get("home_score")) else None
            away_score  = row.get("away_score")  if pd.notna(row.get("away_score")) else None

            conn.execute(text("""
                INSERT INTO dim_match
                    (match_date, competition, season,
                     home_team_id, away_team_id,
                     home_score, away_score,
                     data_source, id_sofascore)
                VALUES
                    (:date, :comp, :season,
                     :hid, :aid,
                     :hsc, :asc,
                     'sofascore', :sid)
                ON CONFLICT (id_sofascore) WHERE id_sofascore IS NOT NULL
                DO UPDATE SET
                    match_date  = EXCLUDED.match_date,
                    home_score  = EXCLUDED.home_score,
                    away_score  = EXCLUDED.away_score,
                    competition = EXCLUDED.competition,
                    season      = EXCLUDED.season
            """), {
                "date": match_date,
                "comp": competition,
                "season": season,
                "hid":  h_canonical,
                "aid":  a_canonical,
                "hsc":  int(home_score) if home_score is not None else None,
                "asc":  int(away_score) if away_score is not None else None,
                "sid":  sid,
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

def _load_from_understat(conn) -> int:
    """Lee understat_matches_laliga.csv → añade id_understat a partidos SS ya cargados.

    Estrategia de matching:
        Buscar dim_match por (match_date, home_team_id, away_team_id) donde
        equipos se resuelven via id_understat en dim_team.
    """
    files = list(RAW_US.glob("**/*matches*.csv"))
    if not files:
        log.info("match_loader: no hay archivos de matches de understat")
        return 0

    try:
        dfs = [pd.read_csv(f) for f in files]
        df = pd.concat(dfs, ignore_index=True)
    except Exception as e:
        log.warning("Error leyendo archivos de understat: %s", e)
        return 0

    linked = 0
    for _, row in df.iterrows():
        us_mid     = row.get("understat_match_id")
        us_home_id = row.get("home_team_id")
        us_away_id = row.get("away_team_id")
        date_str   = row.get("datetime", "")

        if not us_mid:
            continue

        # Convertir fecha
        match_date = None
        if date_str:
            try:
                match_date = str(date_str)[:10]  # YYYY-MM-DD
            except Exception:
                pass

        # Resolver equipos por id_understat
        h_canonical = _resolve_team_by_understat_id(conn, us_home_id)
        a_canonical = _resolve_team_by_understat_id(conn, us_away_id)

        if not h_canonical or not a_canonical or not match_date:
            continue

        # Buscar el partido en dim_match por fecha y equipos
        existing = conn.execute(text("""
            SELECT match_id FROM dim_match
            WHERE match_date = :date
              AND home_team_id = :hid
              AND away_team_id = :aid
            LIMIT 1
        """), {"date": match_date, "hid": h_canonical, "aid": a_canonical}).fetchone()

        if existing:
            conn.execute(text("""
                UPDATE dim_match
                SET id_understat = :uid
                WHERE match_id = :mid AND id_understat IS NULL
            """), {"uid": int(us_mid), "mid": existing[0]})
            linked += 1
        else:
            # Partido no cargado desde SS → insertar desde Understat
            hsc = row.get("home_goals")
            asc = row.get("away_goals")
            conn.execute(text("""
                INSERT INTO dim_match
                    (match_date, competition, season,
                     home_team_id, away_team_id,
                     home_score, away_score,
                     data_source, id_understat)
                VALUES
                    (:date, 'La Liga', :season,
                     :hid, :aid,
                     :hsc, :asc,
                     'understat', :uid)
                ON CONFLICT (id_understat) WHERE id_understat IS NOT NULL DO NOTHING
            """), {
                "date": match_date,
                "season": str(row.get("season", "")),
                "hid": h_canonical, "aid": a_canonical,
                "hsc": int(hsc) if hsc is not None else None,
                "asc": int(asc) if asc is not None else None,
                "uid": int(us_mid),
            })
            linked += 1

    log.info("dim_match ← Understat: %d partidos enlazados/insertados", linked)
    return linked


def _load_from_statsbomb(conn) -> int:
    """Lee matches_clean.csv de StatsBomb → añade id_statsbomb a partidos existentes."""
    files = list(RAW_SB.glob("**/matches_clean.csv"))
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
            sb_mid = row.get("id_statsbomb")
            data_date = _ensure_date(row.get("match_date"))
            
            # Normalizar season: "2020/2021" -> "LaLiga 20/21"
            sb_season = str(row.get("season", ""))
            if len(sb_season) >= 9: # ejemplo 2020/2021
                s_part = f"{sb_season[2:4]}/{sb_season[7:9]}"
                norm_season = f"LaLiga {s_part}"
            else:
                norm_season = sb_season

            h_name = row.get("home_team_name")
            a_name = row.get("away_team_name")
            
            if not sb_mid or not data_date or not h_name or not a_name:
                continue

            # Buscar equipos canónicos en la BD por nombre
            from utils.canonical_teams import normalize_team_name
            h_norm = normalize_team_name(h_name).lower()
            a_norm = normalize_team_name(a_name).lower()

            h_row = conn.execute(text("SELECT canonical_id FROM dim_team WHERE LOWER(canonical_name) = :n"), {"n": h_norm}).fetchone()
            a_row = conn.execute(text("SELECT canonical_id FROM dim_team WHERE LOWER(canonical_name) = :n"), {"n": a_norm}).fetchone()

            if not h_row or not a_row:
                continue

            hid, aid = h_row[0], a_row[0]

            # Buscar partido en dim_match
            existing = conn.execute(text("""
                SELECT match_id FROM dim_match
                WHERE match_date = :date
                  AND home_team_id = :hid
                  AND away_team_id = :aid
                  AND (season = :season OR season LIKE :s_like)
                LIMIT 1
            """), {
                "date": data_date, 
                "hid": hid, 
                "aid": aid, 
                "season": norm_season,
                "s_like": f"%{norm_season.replace('LaLiga ', '')}%"
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


def _load_from_whoscored(conn) -> int:
    """Lee whoscored_events_laliga.csv → añade id_whoscored a partidos existentes.
    
    Como el CSV de matches de WS no tiene equipos, los extraemos de los eventos.
    """
    files = list(RAW_WS.glob("**/*events*.csv"))
    if not files:
        return 0

    log.info("Analizando eventos de WhoScored para vincular partidos...")
    
    match_map: dict[str, dict] = {}
    
    try:
        dfs = [pd.read_csv(f) for f in files]
        df = pd.concat(dfs, ignore_index=True)
        
        for mid, group in df.groupby("whoscored_match_id"):
            # Obtener IDs de equipos únicos en el orden en que aparecen
            # Nos fijamos especialmente en los eventos de tipo 'Start'
            starts = group[group["event_type"] == "Start"]
            unique_teams = starts["whoscored_team_id"].unique().tolist()
            
            if len(unique_teams) < 2:
                # Si no hay 'Start', probamos con cualquier evento
                unique_teams = group["whoscored_team_id"].dropna().unique().tolist()
            
            if len(unique_teams) >= 2:
                # Normalizar season: "2020/21" -> "LaLiga 20/21"
                ws_season = group["season"].iloc[0]
                if "/" in ws_season and not ws_season.startswith("LaLiga"):
                    norm_season = f"LaLiga {ws_season}" # Ya viene como 20/21 o 2020/21?
                    # Si viene como 2020/21 -> queremos 20/21
                    if len(ws_season) > 5:
                        parts = ws_season.split("/")
                        norm_season = f"LaLiga {parts[0][-2:]}/{parts[1][-2:]}"
                else:
                    norm_season = ws_season

                match_map[str(mid)] = {
                    "home_ws_id": int(unique_teams[0]),
                    "away_ws_id": int(unique_teams[1]),
                    "season": norm_season
                }
    except Exception as e:
        log.error("Error analizando eventos de WhoScored: %s", e)
        return 0

    linked = 0
    for ws_mid, info in match_map.items():
        # Resolver IDs canónicas
        h_row = conn.execute(text("SELECT canonical_id FROM dim_team WHERE id_whoscored = :sid"), {"sid": info["home_ws_id"]}).fetchone()
        a_row = conn.execute(text("SELECT canonical_id FROM dim_team WHERE id_whoscored = :sid"), {"sid": info["away_ws_id"]}).fetchone()
        
        if not h_row or not a_row:
            continue
            
        hid, aid = h_row[0], a_row[0]
        
        # Buscar en dim_match (usamos season para filtrar)
        # Nota: La season en WS viene como "2020/21" y en dim_match puede variar, 
        # pero SofaScore usa el mismo formato.
        existing = conn.execute(text("""
            SELECT match_id FROM dim_match
            WHERE home_team_id = :hid
              AND away_team_id = :aid
              AND season = :season
            LIMIT 1
        """), {"hid": hid, "aid": aid, "season": info["season"]}).fetchone()
        
        if existing:
            conn.execute(text("""
                UPDATE dim_match
                SET id_whoscored = :sid
                WHERE match_id = :mid AND id_whoscored IS NULL
            """), {"sid": int(ws_mid), "mid": existing[0]})
            linked += 1

    log.info("dim_match ← WhoScored: %d partidos enlazados", linked)
    return linked


# ── Punto de entrada ──────────────────────────────────────────────────────────

def load_matches(conn) -> int:
    """Carga dim_match desde SofaScore (master) y Understat (complementario).

    Returns:
        Número total de partidos en dim_match.
    """
    log.info("[START] Cargando dim_match...")
    _load_from_sofascore(conn)
    _load_from_understat(conn)
    _load_from_statsbomb(conn)
    _load_from_whoscored(conn)

    total = conn.execute(text("SELECT COUNT(*) FROM dim_match")).scalar()
    log.info("[OK] dim_match completado — %d partidos", total)
    return total


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s - %(message)s")
    with engine.begin() as conn:
        load_matches(conn)
