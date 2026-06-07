"""
scrapers/whoscored_stats_extractor.py
======================================
Extrae datos avanzados de los match_centre.json existentes en data/raw/
y genera CSVs clean listos para cargar en PostgreSQL.

NO hace scraping — solo lee los JSON que whoscored_scraper.py ya guardó.

Genera por competición/temporada:
    data/clean/<comp>/<season>/whoscored/
        player_match_stats.csv   → fact_player_match_stats
        formations.csv           → fact_formations
        referees.csv             → dim_referee (upsert)
        match_enrichment.csv     → UPDATE dim_match (venue, managers, scores)

Uso:
    python -m scrapers.whoscored_stats_extractor
    python -m scrapers.whoscored_stats_extractor --competition "La Liga" --season 2025/2026
    python -m scrapers.whoscored_stats_extractor --dry-run
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from collections import defaultdict
from pathlib import Path
from typing import Optional

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parent.parent))

from utils.data_paths import (
    RAW_ROOT,
    normalize_season,
    save_clean_csv,
    slugify_competition,
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger(__name__)


# ── Helpers ──────────────────────────────────────────────────────────

def _last_value(stat_dict: dict) -> Optional[float]:
    """Extrae el último valor de un dict {minute_str: value}.

    WhoScored almacena stats como {'0': 6.0, '1': 6.01, ...}.
    El último minuto tiene el acumulado final.
    """
    if not stat_dict or not isinstance(stat_dict, dict):
        return None
    try:
        last_key = max(stat_dict.keys(), key=lambda k: int(k))
        return float(stat_dict[last_key])
    except (ValueError, TypeError):
        return None


def _total_value(stat_dict: dict) -> Optional[int]:
    """Suma todos los valores (para stats acumulativas como pases, tackles)."""
    if not stat_dict or not isinstance(stat_dict, dict):
        return None
    try:
        return int(sum(float(v) for v in stat_dict.values()))
    except (ValueError, TypeError):
        return None


def _safe_int(val) -> Optional[int]:
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


# ── Extractores por entidad ──────────────────────────────────────────

def extract_player_match_stats(data: dict, match_id: str) -> list[dict]:
    """Extrae stats de cada jugador del matchCentreData."""
    rows = []
    for side in ("home", "away"):
        team = data.get(side, {})
        team_id_ws = team.get("teamId")
        if not team_id_ws:
            continue
        for p in team.get("players", []):
            stats = p.get("stats", {})
            if not stats:
                continue

            # Rating final
            rating = _last_value(stats.get("ratings"))

            # Sustitución
            subbed_in = _safe_int(p.get("subbedInExpandedMinute"))
            subbed_out = _safe_int(p.get("subbedOutExpandedMinute"))

            row = {
                "whoscored_match_id": match_id,
                "whoscored_player_id": p.get("playerId"),
                "whoscored_team_id": team_id_ws,
                "player_name": p.get("name"),
                "side": side,
                "is_starter": p.get("isFirstEleven", False),
                "position": p.get("position"),
                "shirt_no": _safe_int(p.get("shirtNo")),
                "age": _safe_int(p.get("age")),
                "height_cm": _safe_int(p.get("height")),
                "weight_kg": _safe_int(p.get("weight")),
                "is_man_of_the_match": p.get("isManOfTheMatch", False),
                "subbed_in_minute": subbed_in,
                "subbed_out_minute": subbed_out,
                "rating": round(rating, 2) if rating is not None else None,

                # Pases
                "passes_total": _total_value(stats.get("passesTotal")),
                "passes_accurate": _total_value(stats.get("passesAccurate")),
                "passes_key": _total_value(stats.get("passesKey")),
                "pass_success_pct": _last_value(stats.get("passSuccess")),

                # Tiros
                "shots_total": _total_value(stats.get("shotsTotal")),
                "shots_on_target": _total_value(stats.get("shotsOnTarget")),
                "shots_off_target": _total_value(stats.get("shotsOffTarget")),
                "shots_blocked": _total_value(stats.get("shotsBlocked")),

                # Regates
                "dribbles_attempted": _total_value(stats.get("dribblesAttempted")),
                "dribbles_won": _total_value(stats.get("dribblesWon")),
                "dribbles_lost": _total_value(stats.get("dribblesLost")),

                # Defensa
                "tackles_total": _total_value(stats.get("tacklesTotal")),
                "tackles_successful": _total_value(stats.get("tackleSuccessful")),
                "interceptions": _total_value(stats.get("interceptions")),
                "clearances": _total_value(stats.get("clearances")),

                # Aéreo
                "aerials_total": _total_value(stats.get("aerialsTotal")),
                "aerials_won": _total_value(stats.get("aerialsWon")),

                # Disciplina y otros
                "fouls_committed": _total_value(stats.get("foulsCommited")),
                "was_dribbled_past": _total_value(stats.get("dribbledPast")),
                "dispossessed": _total_value(stats.get("dispossessed")),
                "touches": _total_value(stats.get("touches")),
                "offsides_caught": _total_value(stats.get("offsidesCaught")),

                # Corners y saques de banda
                "corners_total": _total_value(stats.get("cornersTotal")),
                "corners_accurate": _total_value(stats.get("cornersAccurate")),
                "throw_ins_total": _total_value(stats.get("throwInsTotal")),
                "throw_ins_accurate": _total_value(stats.get("throwInsAccurate")),

                # Portero
                "saves_total": _total_value(stats.get("totalSaves")),
                "saves_parried_safe": _total_value(stats.get("parriedSafe")),
                "saves_parried_danger": _total_value(stats.get("parriedDanger")),
                "claims_high": _total_value(stats.get("claimsHigh")),
                "collected": _total_value(stats.get("collected")),

                # Posesión
                "possession_pct": _last_value(stats.get("possession")),

                "data_source": "whoscored",
            }
            rows.append(row)
    return rows


def extract_formations(data: dict, match_id: str) -> list[dict]:
    """Extrae formaciones con rango temporal."""
    rows = []
    for side in ("home", "away"):
        team = data.get(side, {})
        team_id_ws = team.get("teamId")
        if not team_id_ws:
            continue
        for fm in team.get("formations", []):
            rows.append({
                "whoscored_match_id": match_id,
                "whoscored_team_id": team_id_ws,
                "side": side,
                "formation_name": fm.get("formationName", ""),
                "captain_player_id_ws": fm.get("captainPlayerId"),
                "start_minute": _safe_int(fm.get("startMinuteExpanded", 0)),
                "end_minute": _safe_int(fm.get("endMinuteExpanded")),
                "data_source": "whoscored",
            })
    return rows


def extract_referee(data: dict) -> Optional[dict]:
    """Extrae datos del árbitro."""
    ref = data.get("referee")
    if not ref or not isinstance(ref, dict):
        return None
    official_id = ref.get("officialId")
    if not official_id:
        return None
    first = (ref.get("firstName") or "").strip()
    last = (ref.get("lastName") or "").strip()
    name = f"{first} {last}".strip() if first or last else ref.get("name", "")
    return {
        "id_whoscored": official_id,
        "canonical_name": name,
        "data_source": "whoscored",
    }


def extract_match_enrichment(data: dict, match_id: str) -> dict:
    """Extrae datos de enriquecimiento para dim_match."""
    home = data.get("home", {})
    away = data.get("away", {})
    ref = data.get("referee", {})
    return {
        "whoscored_match_id": match_id,
        "home_team_ws_id": home.get("teamId"),
        "away_team_ws_id": away.get("teamId"),
        "venue_name": data.get("venueName"),
        "manager_home": home.get("managerName"),
        "manager_away": away.get("managerName"),
        "ht_score": data.get("htScore"),
        "ft_score": data.get("ftScore") or data.get("score"),
        "attendance": data.get("attendance"),
        "referee_id_whoscored": ref.get("officialId") if ref else None,
        "referee_name": ref.get("name") if ref else None,
        "start_time": data.get("startTime"),
    }


# ── Orquestador ─────────────────────────────────────────────────────

def discover_match_centres(
    competition: Optional[str] = None,
    season: Optional[str] = None,
) -> dict[tuple[str, str], list[Path]]:
    """Descubre todos los match_centre.json agrupados por (comp_slug, season)."""
    result: dict[tuple[str, str], list[Path]] = defaultdict(list)

    comp_filter = slugify_competition(competition) if competition else None
    season_filter = normalize_season(season) if season else None

    for comp_dir in sorted(RAW_ROOT.iterdir()):
        if not comp_dir.is_dir():
            continue
        comp_slug = comp_dir.name
        if comp_filter and comp_slug != comp_filter:
            continue

        for season_dir in sorted(comp_dir.iterdir()):
            if not season_dir.is_dir():
                continue
            season_label = season_dir.name
            if season_filter and season_label != season_filter:
                continue

            ws_dir = season_dir / "whoscored" / "matches"
            if not ws_dir.is_dir():
                continue

            for match_dir in ws_dir.iterdir():
                if not match_dir.is_dir():
                    continue
                centre = match_dir / "match_centre.json"
                if centre.exists() and centre.stat().st_size > 100:
                    result[(comp_slug, season_label)].append(centre)

    return result


def process_competition_season(
    comp_slug: str,
    season_label: str,
    match_files: list[Path],
    dry_run: bool = False,
) -> dict:
    """Procesa todos los match_centre.json de una competición/temporada."""
    all_stats = []
    all_formations = []
    all_referees = {}  # id_whoscored → dict (dedup)
    all_enrichments = []

    for fp in match_files:
        match_id = fp.parent.name
        try:
            data = json.loads(fp.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError) as e:
            log.debug("Omitido %s: %s", fp, e)
            continue

        all_stats.extend(extract_player_match_stats(data, match_id))
        all_formations.extend(extract_formations(data, match_id))

        ref = extract_referee(data)
        if ref:
            all_referees[ref["id_whoscored"]] = ref

        all_enrichments.append(extract_match_enrichment(data, match_id))

    summary = {
        "comp_slug": comp_slug,
        "season": season_label,
        "matches_processed": len(match_files),
        "player_stats_rows": len(all_stats),
        "formations_rows": len(all_formations),
        "referees_unique": len(all_referees),
        "enrichment_rows": len(all_enrichments),
    }

    if dry_run:
        log.info(
            "  [DRY-RUN] %s/%s: %d matches → %d stats, %d formations, %d referees",
            comp_slug, season_label, len(match_files),
            len(all_stats), len(all_formations), len(all_referees),
        )
        return summary

    # Convertir comp_slug de vuelta a un nombre que save_clean_csv entienda
    # El slug ya es el formato correcto para las rutas
    season_db = season_label  # ya está en formato YYYY_YYYY

    if all_stats:
        df = pd.DataFrame(all_stats)
        path = save_clean_csv(comp_slug, season_db, "whoscored", "player_match_stats", df)
        log.info("  · player_match_stats: %d filas → %s", len(df), path)

    if all_formations:
        df = pd.DataFrame(all_formations)
        path = save_clean_csv(comp_slug, season_db, "whoscored", "formations", df)
        log.info("  · formations: %d filas → %s", len(df), path)

    if all_referees:
        df = pd.DataFrame(list(all_referees.values()))
        path = save_clean_csv(comp_slug, season_db, "whoscored", "referees", df)
        log.info("  · referees: %d filas → %s", len(df), path)

    if all_enrichments:
        df = pd.DataFrame(all_enrichments)
        path = save_clean_csv(comp_slug, season_db, "whoscored", "match_enrichment", df)
        log.info("  · match_enrichment: %d filas → %s", len(df), path)

    return summary


def run(
    competition: Optional[str] = None,
    season: Optional[str] = None,
    dry_run: bool = False,
) -> list[dict]:
    """Punto de entrada principal."""
    discovered = discover_match_centres(competition, season)

    if not discovered:
        log.warning("No se encontraron match_centre.json%s%s",
                     f" para {competition}" if competition else "",
                     f" temporada {season}" if season else "")
        return []

    total_matches = sum(len(v) for v in discovered.values())
    log.info(
        "Descubiertos %d match_centre.json en %d competición/temporada(s)",
        total_matches, len(discovered),
    )

    summaries = []
    for (comp_slug, season_label), files in sorted(discovered.items()):
        log.info("\n[%s/%s] %d partidos", comp_slug, season_label, len(files))
        s = process_competition_season(comp_slug, season_label, files, dry_run=dry_run)
        summaries.append(s)

    # Resumen global
    total_stats = sum(s["player_stats_rows"] for s in summaries)
    total_form = sum(s["formations_rows"] for s in summaries)
    total_ref = sum(s["referees_unique"] for s in summaries)
    log.info(
        "\n[TOTAL] %d matches → %d player_stats, %d formations, %d referees",
        total_matches, total_stats, total_form, total_ref,
    )
    return summaries


# ── CLI ──────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Extrae stats avanzadas de match_centre.json (WhoScored)"
    )
    parser.add_argument("--competition", "-c", default=None,
                        help="Filtrar por competición (slug o nombre)")
    parser.add_argument("--season", "-s", default=None,
                        help="Filtrar por temporada (ej. 2025/2026)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Solo contar, no escribir CSVs")
    args = parser.parse_args()

    print("=" * 55)
    print("  WhoScored Stats Extractor")
    print("=" * 55)
    summaries = run(
        competition=args.competition,
        season=args.season,
        dry_run=args.dry_run,
    )
    if not summaries:
        print("\n[!] Sin datos para procesar.")
        return

    print("\n[OK] Extracción completada:")
    for s in summaries:
        print(f"  {s['comp_slug']}/{s['season']}: "
              f"{s['player_stats_rows']} stats, "
              f"{s['formations_rows']} formations, "
              f"{s['referees_unique']} referees")


if __name__ == "__main__":
    main()
