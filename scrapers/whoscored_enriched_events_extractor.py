"""
scrapers/whoscored_enriched_events_extractor.py
=================================================
Extrae eventos ENRIQUECIDOS de match_centre.json con TODOS los campos
que pide el informe de diferencias del PM:

  - Campos base: event_id, minute, second, period, team_id, player_id, etc.
  - Coordenadas: x, y (0-100), x_pct/y_pct (0-1), x_m/y_m (metros 105x68)
  - isTouch: directamente del JSON
  - Qualifiers expandidos: q_length, q_angle, q_zone, q_goal_mouth_y/z,
    q_blocked_x/y, q_jersey_number, q_pass_end_x/y, q_player_pos, etc.
  - timestamp_ms: derivado de minute+second
  - Flags one-hot de cualificadores booleanos (q_right_foot, q_left_foot,
    q_head, q_cross, q_freekick, etc.)

NO hace scraping — lee los match_centre.json existentes en data/raw/.
Genera un CSV por competición/temporada en data/clean/.

Uso:
    python -m scrapers.whoscored_enriched_events_extractor
    python -m scrapers.whoscored_enriched_events_extractor -c "La Liga" -s 2025/2026
    python -m scrapers.whoscored_enriched_events_extractor --dry-run
"""

from __future__ import annotations

import argparse
import json
import logging
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

# Pitch dimensions for coordinate conversion (FIFA standard)
PITCH_LENGTH_M = 105.0
PITCH_WIDTH_M = 68.0

# Qualifier flags to extract as boolean columns (1/0)
# Mapped: qualifier_displayName → column_name
QUALIFIER_FLAGS = {
    "RightFoot": "q_right_foot",
    "LeftFoot": "q_left_foot",
    "Head": "q_head",
    "HeadPass": "q_head_pass",
    "Cross": "q_cross",
    "Chipped": "q_chipped",
    "Longball": "q_longball",
    "Offensive": "q_offensive",
    "Defensive": "q_defensive",
    "FreekickTaken": "q_freekick",
    "IndirectFreekickTaken": "q_indirect_freekick",
    "DirectFreekick": "q_direct_freekick",
    "CornerTaken": "q_corner",
    "ThrowIn": "q_throw_in",
    "GoalKick": "q_goal_kick",
    "Foul": "q_foul",
    "Assisted": "q_assisted",
    "IntentionalAssist": "q_intent_assist",
    "KeyPass": "q_key_pass",
    "ShotAssist": "q_shot_assist",
    "BigChance": "q_big_chance",
    "BigChanceCreated": "q_big_chance_created",
    "FastBreak": "q_fast_break",
    "RegularPlay": "q_regular_play",
    "IndividualPlay": "q_individual_play",
    "FirstTouch": "q_first_touch",
    "LayOff": "q_layoff",
    "Throughball": "q_throughball",
    "Volley": "q_volley",
    "StandingSave": "q_standing_save",
    "DivingSave": "q_diving_save",
    "Blocked": "q_blocked",
    "OutfielderBlock": "q_outfielder_block",
    "FromCorner": "q_from_corner",
    "LeadingToGoal": "q_leading_to_goal",
    "Yellow": "q_yellow",
    "AerialFoul": "q_aerial_foul",
    "KeeperSaveInTheBox": "q_keeper_save_inbox",
    "KeeperSaveObox": "q_keeper_save_obox",
    "ParriedSafe": "q_parried_safe",
    "ParriedDanger": "q_parried_danger",
    "Collected": "q_collected",
    "BlockedCross": "q_blocked_cross",
    "PlayerCaughtOffside": "q_offside",
    "OtherBodyPart": "q_other_body_part",
    "OwnGoal": "q_own_goal",
    "OneOnOne": "q_one_on_one",
    "SetPiece": "q_set_piece",
    "Penalty": "q_penalty",
    "IntentionalGoalAssist": "q_intent_goal_assist",
    "Tackle": "q_tackle",
    "Interception": "q_interception",
    "ErrorLeadsToGoal": "q_error_leads_to_goal",
    "ErrorLeadsToShot": "q_error_leads_to_shot",
    "SecondYellow": "q_second_yellow",
    "RedCard": "q_red_card",
}

# Qualifier values to extract as numeric/string columns
QUALIFIER_VALUES = {
    "Zone": "q_zone",
    "Length": "q_length",
    "Angle": "q_angle",
    "PassEndX": "q_pass_end_x",
    "PassEndY": "q_pass_end_y",
    "GoalMouthY": "q_goal_mouth_y",
    "GoalMouthZ": "q_goal_mouth_z",
    "BlockedX": "q_blocked_x",
    "BlockedY": "q_blocked_y",
    "JerseyNumber": "q_jersey_number",
    "PlayerPosition": "q_player_pos",
    "RelatedEventId": "q_related_event_id",
    "OppositeRelatedEvent": "q_opposite_related_event",
}

# Shot zone qualifiers (for goal mouth positioning)
SHOT_ZONE_FLAGS = {
    "LowLeft": "q_shot_low_left",
    "LowCentre": "q_shot_low_centre",
    "LowRight": "q_shot_low_right",
    "HighLeft": "q_shot_high_left",
    "HighCentre": "q_shot_high_centre",
    "HighRight": "q_shot_high_right",
    "MissLeft": "q_miss_left",
    "MissRight": "q_miss_right",
    "MissHigh": "q_miss_high",
    "SmallBoxRight": "q_small_box_right",
    "SmallBoxLeft": "q_small_box_left",
    "SmallBoxCentre": "q_small_box_centre",
    "BoxCentre": "q_box_centre",
    "BoxLeft": "q_box_left",
    "BoxRight": "q_box_right",
    "OutOfBoxCentre": "q_oob_centre",
    "OutOfBoxLeft": "q_oob_left",
    "OutOfBoxDeepLeft": "q_oob_deep_left",
    "DeepBoxLeft": "q_deep_box_left",
}


def _safe_float(val) -> Optional[float]:
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def extract_enriched_events(data: dict, match_id: str) -> list[dict]:
    """Extrae eventos enriquecidos de un matchCentreData."""
    events = data.get("events", [])
    if not events:
        return []

    # Build player jersey/position lookup from team data
    player_info: dict[int, dict] = {}
    for side in ("home", "away"):
        team = data.get(side, {})
        for p in team.get("players", []):
            pid = p.get("playerId")
            if pid:
                player_info[pid] = {
                    "shirt_no": p.get("shirtNo"),
                    "position": p.get("position"),
                    "player_name": p.get("name"),
                }

    # Match metadata for the "partido" field
    home = data.get("home", {})
    away = data.get("away", {})
    home_name = home.get("name", "")
    away_name = away.get("name", "")
    home_team_id = home.get("teamId")
    away_team_id = away.get("teamId")
    ft_score = data.get("ftScore") or data.get("score") or ""

    rows = []
    for e in events:
        # Base fields
        etype = e.get("type", {})
        event_type = etype.get("displayName") if isinstance(etype, dict) else etype
        outcome_obj = e.get("outcomeType", {})
        outcome = outcome_obj.get("displayName") if isinstance(outcome_obj, dict) else outcome_obj
        period_obj = e.get("period", {})
        period = period_obj.get("displayName") if isinstance(period_obj, dict) else period_obj

        minute = e.get("minute")
        second = e.get("second")
        x_raw = _safe_float(e.get("x"))
        y_raw = _safe_float(e.get("y"))
        end_x_raw = _safe_float(e.get("endX"))
        end_y_raw = _safe_float(e.get("endY"))

        # Coordinate conversions
        # WhoScored: x,y are 0-100 (percentage of pitch)
        x_pct = round(x_raw / 100.0, 4) if x_raw is not None else None
        y_pct = round(y_raw / 100.0, 4) if y_raw is not None else None
        x_m = round(x_raw * PITCH_LENGTH_M / 100.0, 2) if x_raw is not None else None
        y_m = round(y_raw * PITCH_WIDTH_M / 100.0, 2) if y_raw is not None else None
        end_x_pct = round(end_x_raw / 100.0, 4) if end_x_raw is not None else None
        end_y_pct = round(end_y_raw / 100.0, 4) if end_y_raw is not None else None

        # timestamp_ms from minute + second
        timestamp_ms = None
        if minute is not None and second is not None:
            timestamp_ms = (int(minute) * 60 + int(second)) * 1000

        # Determine team name and side
        team_id = e.get("teamId")
        if team_id == home_team_id:
            team_name = home_name
            side = "home"
        elif team_id == away_team_id:
            team_name = away_name
            side = "away"
        else:
            team_name = ""
            side = ""

        # Player info fallback
        player_id = e.get("playerId")
        player_name = e.get("playerName") or ""
        pinfo = player_info.get(player_id, {})
        if not player_name and pinfo:
            player_name = pinfo.get("player_name", "")

        # Construct base row
        row = {
            "whoscored_match_id": match_id,
            "partido": f"{home_name} {ft_score} {away_name}".strip(),
            "event_id": e.get("eventId"),
            "minute": minute,
            "second": second,
            "timestamp_ms": timestamp_ms,
            "period": period,
            "is_touch": 1 if e.get("isTouch") else 0,
            "whoscored_team_id": team_id,
            "team_name": team_name,
            "side": side,
            "whoscored_player_id": player_id,
            "player_name": player_name,
            "event_type": event_type,
            "outcome_type": outcome,
            # Coordinates: original (0-100)
            "x": x_raw,
            "y": y_raw,
            # Coordinates: decimal (0-1) — for DB compatibility
            "x_pct": x_pct,
            "y_pct": y_pct,
            # Coordinates: meters (105x68)
            "x_m": x_m,
            "y_m": y_m,
            "end_x": end_x_raw,
            "end_y": end_y_raw,
            "end_x_pct": end_x_pct,
            "end_y_pct": end_y_pct,
            # Player metadata
            "q_jersey_number": pinfo.get("shirt_no"),
            "q_player_pos": pinfo.get("position"),
        }

        # Parse qualifiers
        quals: dict[str, Optional[str]] = {}
        for q in e.get("qualifiers", []):
            qt = q.get("type", {})
            qname = qt.get("displayName") if isinstance(qt, dict) else qt
            qval = q.get("value")
            quals[qname] = qval

        # Value qualifiers (overwrite player-level defaults if event has them)
        for qname, col in QUALIFIER_VALUES.items():
            if qname in quals:
                val = quals[qname]
                # Convert numeric values
                if col in ("q_length", "q_angle", "q_pass_end_x", "q_pass_end_y",
                           "q_goal_mouth_y", "q_goal_mouth_z", "q_blocked_x", "q_blocked_y"):
                    row[col] = _safe_float(val)
                elif col == "q_jersey_number" and val is not None:
                    row[col] = val
                elif col == "q_player_pos" and val is not None:
                    row[col] = val
                else:
                    row[col] = val

        # Duplicate pass end coords as q_pass_end_x/y (from qualifiers or endX/endY)
        if row.get("q_pass_end_x") is None and end_x_raw is not None:
            row["q_pass_end_x"] = end_x_raw
        if row.get("q_pass_end_y") is None and end_y_raw is not None:
            row["q_pass_end_y"] = end_y_raw

        # Boolean flag qualifiers
        for qname, col in QUALIFIER_FLAGS.items():
            row[col] = 1 if qname in quals else 0

        # Shot zone flags
        for qname, col in SHOT_ZONE_FLAGS.items():
            row[col] = 1 if qname in quals else 0

        row["data_source"] = "whoscored"
        rows.append(row)

    return rows


# ── Orquestador ─────────────────────────────────────────────────────

def discover_and_extract(
    competition: Optional[str] = None,
    season: Optional[str] = None,
    dry_run: bool = False,
) -> list[dict]:
    """Descubre y extrae eventos enriquecidos de todos los match_centre.json."""
    comp_filter = slugify_competition(competition) if competition else None
    season_filter = normalize_season(season) if season else None

    # Group by (comp_slug, season)
    groups: dict[tuple[str, str], list[Path]] = defaultdict(list)

    for comp_dir in sorted(RAW_ROOT.iterdir()):
        if not comp_dir.is_dir():
            continue
        if comp_filter and comp_dir.name != comp_filter:
            continue

        for season_dir in sorted(comp_dir.iterdir()):
            if not season_dir.is_dir():
                continue
            if season_filter and season_dir.name != season_filter:
                continue

            matches_dir = season_dir / "whoscored" / "matches"
            if not matches_dir.is_dir():
                continue

            for match_dir in matches_dir.iterdir():
                if not match_dir.is_dir():
                    continue
                centre = match_dir / "match_centre.json"
                if centre.exists() and centre.stat().st_size > 100:
                    groups[(comp_dir.name, season_dir.name)].append(centre)

    if not groups:
        log.warning("No se encontraron match_centre.json")
        return []

    total = sum(len(v) for v in groups.values())
    log.info("Descubiertos %d partidos en %d comp/season(s)", total, len(groups))

    summaries = []
    for (comp_slug, season_label), files in sorted(groups.items()):
        log.info("\n[%s/%s] %d partidos", comp_slug, season_label, len(files))

        if dry_run:
            summaries.append({"comp": comp_slug, "season": season_label,
                              "matches": len(files), "events": "?"})
            continue

        all_events = []
        for fp in files:
            match_id = fp.parent.name
            try:
                data = json.loads(fp.read_text(encoding="utf-8"))
            except Exception as e:
                log.debug("Omitido %s: %s", fp, e)
                continue
            all_events.extend(extract_enriched_events(data, match_id))

        if all_events:
            df = pd.DataFrame(all_events)
            path = save_clean_csv(comp_slug, season_label, "whoscored",
                                  "enriched_events", df)
            log.info("  · enriched_events: %d filas → %s", len(df), path)
            summaries.append({"comp": comp_slug, "season": season_label,
                              "matches": len(files), "events": len(df)})
        else:
            log.warning("  Sin eventos para %s/%s", comp_slug, season_label)

    return summaries


def main():
    parser = argparse.ArgumentParser(
        description="Extrae eventos enriquecidos de WhoScored match_centre.json"
    )
    parser.add_argument("-c", "--competition", default=None)
    parser.add_argument("-s", "--season", default=None)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    print("=" * 60)
    print("  WhoScored Enriched Events Extractor")
    print("=" * 60)
    summaries = discover_and_extract(
        competition=args.competition,
        season=args.season,
        dry_run=args.dry_run,
    )
    if summaries:
        total_events = sum(s.get("events", 0) for s in summaries if isinstance(s.get("events"), int))
        print(f"\n[OK] {total_events:,} eventos enriquecidos extraídos")


if __name__ == "__main__":
    main()
