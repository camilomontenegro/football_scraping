"""
scripts/crosslink_player_ids.py
================================
Completa IDs faltantes en dim_player (id_sofascore, id_understat, id_whoscored)
cruzando los CSVs de cada fuente por nombre normalizado + equipo.

NO requiere conexión a la DB — trabaja directamente sobre exports/players.csv
y los CSVs de data/clean/. Genera un SQL de UPDATE para aplicar en PostgreSQL.

Estrategia de matching:
  1. Exact match: nombre normalizado idéntico + mismo equipo (via team name)
  2. Fuzzy match: similaridad ≥ 85% + mismo equipo

Uso:
    python -m scripts.crosslink_player_ids
    python -m scripts.crosslink_player_ids --dry-run
    python -m scripts.crosslink_player_ids --output updates.sql
"""

from __future__ import annotations

import argparse
import csv
import logging
import os
import re
import sys
import unicodedata
from collections import defaultdict
from pathlib import Path
from typing import Optional

sys.path.append(str(Path(__file__).resolve().parent.parent))

from utils.data_paths import CLEAN_ROOT

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent


# ── Normalización de nombres ─────────────────────────────────────────

def normalize_name(name: str) -> str:
    """Normaliza un nombre para comparación: quita acentos, lowercase, trim."""
    if not name:
        return ""
    # NFD decomposition → quitar diacríticos
    s = unicodedata.normalize("NFD", name)
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")
    s = s.lower().strip()
    # Quitar caracteres no alfanuméricos excepto espacios y guiones
    s = re.sub(r"[^a-z0-9\s\-]", "", s)
    # Colapsar espacios
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _similarity(a: str, b: str) -> float:
    """Similaridad entre dos strings (0-1) usando SequenceMatcher."""
    from difflib import SequenceMatcher
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, a, b).ratio()


# ── Carga de datos por fuente ────────────────────────────────────────

def _load_source_players(source: str, id_col: str, name_col: str, team_col: str) -> dict[str, list[dict]]:
    """Carga jugadores de una fuente, agrupados por nombre normalizado.

    Returns: {normalized_name: [{id, name, team_name, team_id}, ...]}
    """
    result: dict[str, list[dict]] = defaultdict(list)
    seen_ids: set[str] = set()

    for csv_path in CLEAN_ROOT.glob(f"**/{source}/players.csv"):
        try:
            with open(csv_path, "r", encoding="utf-8-sig") as f:
                for row in csv.DictReader(f):
                    pid = row.get(id_col, "").strip()
                    pname = row.get(name_col, "").strip()
                    team = row.get(team_col, "").strip()
                    if not pid or not pname or pid in seen_ids:
                        continue
                    seen_ids.add(pid)
                    norm = normalize_name(pname)
                    result[norm].append({
                        "id": pid,
                        "name": pname,
                        "team_name": team,
                    })
        except Exception as e:
            log.debug("Error reading %s: %s", csv_path, e)

    return result


def _load_dim_players() -> list[dict]:
    """Carga dim_player desde el export CSV."""
    export_path = PROJECT_ROOT / "exports" / "players.csv"
    if not export_path.exists():
        log.error("exports/players.csv no encontrado. Ejecuta export_all.py primero.")
        return []
    with open(export_path, "r", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def _load_team_id_mappings() -> dict:
    """Carga mappings de team IDs entre fuentes desde exports/teams.csv."""
    export_path = PROJECT_ROOT / "exports" / "teams.csv"
    if not export_path.exists():
        return {}
    # ws_team_id → {ss_team_id, us_team_id, tm_team_id}
    teams = {}
    with open(export_path, "r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            cid = row.get("canonical_id", "").strip()
            teams[cid] = {
                "name": row.get("canonical_name", ""),
                "id_sofascore": row.get("id_sofascore", "").strip().replace(".0", ""),
                "id_understat": row.get("id_understat", "").strip().replace(".0", ""),
                "id_whoscored": row.get("id_whoscored", "").strip().replace(".0", ""),
                "id_transfermarkt": row.get("id_transfermarkt", "").strip().replace(".0", ""),
            }
    return teams


# ── Motor de cross-linking ───────────────────────────────────────────

def crosslink(dry_run: bool = False) -> dict:
    """Ejecuta el cross-linking entre fuentes."""

    dim_players = _load_dim_players()
    if not dim_players:
        return {}

    # Cargar jugadores por fuente
    log.info("Cargando jugadores por fuente...")
    ws_players = _load_source_players("whoscored", "whoscored_player_id", "player_name", "team_name")
    ss_players = _load_source_players("sofascore", "id_sofascore", "canonical_name", "team_name")
    us_players = _load_source_players("understat", "understat_player_id", "player_name", "")

    log.info("  WhoScored: %d nombres únicos", len(ws_players))
    log.info("  SofaScore: %d nombres únicos", len(ss_players))
    log.info("  Understat: %d nombres únicos", len(us_players))

    # Team mappings para validar equipo
    team_map = _load_team_id_mappings()

    # Construir reverse maps: source_team_id → canonical_team_ids
    ws_team_to_canonical: dict[str, str] = {}
    ss_team_to_canonical: dict[str, str] = {}
    us_team_to_canonical: dict[str, str] = {}
    for cid, tinfo in team_map.items():
        if tinfo["id_whoscored"]:
            ws_team_to_canonical[tinfo["id_whoscored"]] = cid
        if tinfo["id_sofascore"]:
            ss_team_to_canonical[tinfo["id_sofascore"]] = cid
        if tinfo["id_understat"]:
            us_team_to_canonical[tinfo["id_understat"]] = cid

    updates: list[dict] = []
    stats = {"ws_exact": 0, "ws_fuzzy": 0, "ss_exact": 0, "ss_fuzzy": 0,
             "us_exact": 0, "us_fuzzy": 0}

    # Fase 1: exact match (rápido)
    for p in dim_players:
        canonical_id = p["canonical_id"]
        norm_name = normalize_name(p["canonical_name"])
        if not norm_name:
            continue

        for field, source, label in [
            ("id_whoscored", ws_players, "ws"),
            ("id_sofascore", ss_players, "ss"),
            ("id_understat", us_players, "us"),
        ]:
            if p.get(field, "").strip():
                continue
            if norm_name in source:
                candidates = source[norm_name]
                if len(candidates) == 1:
                    updates.append({
                        "canonical_id": canonical_id,
                        "field": field,
                        "value": candidates[0]["id"],
                        "matched_name": candidates[0]["name"],
                        "method": "exact",
                    })
                    stats[f"{label}_exact"] += 1

    # Fase 2: fuzzy match (solo para jugadores que no se resolvieron en fase 1)
    # Usar prefix index para limitar comparaciones
    resolved = {(u["canonical_id"], u["field"]) for u in updates}

    for field, source, label in [
        ("id_whoscored", ws_players, "ws"),
        ("id_sofascore", ss_players, "ss"),
        ("id_understat", us_players, "us"),
    ]:
        prefix_idx = _build_prefix_index(source)
        for p in dim_players:
            canonical_id = p["canonical_id"]
            if p.get(field, "").strip():
                continue
            if (canonical_id, field) in resolved:
                continue
            norm_name = normalize_name(p["canonical_name"])
            if not norm_name or len(norm_name) < 4:
                continue

            # Solo comparar contra candidatos con prefijo similar
            candidates_to_check: set[str] = set()
            prefix = norm_name[:4]
            if prefix in prefix_idx:
                candidates_to_check.update(prefix_idx[prefix])
            parts = norm_name.split()
            if len(parts) > 1:
                last_prefix = parts[-1][:4]
                if len(last_prefix) >= 4 and last_prefix in prefix_idx:
                    candidates_to_check.update(prefix_idx[last_prefix])

            best_score = 0.0
            best_match = None
            for source_norm in candidates_to_check:
                sim = _similarity(norm_name, source_norm)
                if sim >= 0.88 and sim > best_score:
                    best_score = sim
                    best_match = source[source_norm][0]

            if best_match:
                updates.append({
                    "canonical_id": canonical_id,
                    "field": field,
                    "value": best_match["id"],
                    "matched_name": best_match["name"],
                    "method": "fuzzy",
                })
                stats[f"{label}_fuzzy"] += 1

    log.info("\n=== Cross-linking results ===")
    log.info("  WhoScored: %d exact + %d fuzzy = %d",
             stats["ws_exact"], stats["ws_fuzzy"], stats["ws_exact"] + stats["ws_fuzzy"])
    log.info("  SofaScore: %d exact + %d fuzzy = %d",
             stats["ss_exact"], stats["ss_fuzzy"], stats["ss_exact"] + stats["ss_fuzzy"])
    log.info("  Understat: %d exact + %d fuzzy = %d",
             stats["us_exact"], stats["us_fuzzy"], stats["us_exact"] + stats["us_fuzzy"])
    log.info("  Total updates: %d", len(updates))

    if dry_run:
        log.info("[DRY-RUN] No se escribió nada.")
        return stats

    # Generar SQL
    output_path = PROJECT_ROOT / "db" / "crosslink_updates.sql"
    _write_sql(updates, output_path)
    log.info("SQL escrito en %s", output_path)

    return stats


def _build_prefix_index(source_players: dict[str, list[dict]], prefix_len: int = 4) -> dict[str, list[str]]:
    """Crea un índice de prefijos para búsqueda rápida."""
    idx: dict[str, list[str]] = defaultdict(list)
    for norm in source_players:
        if len(norm) >= prefix_len:
            idx[norm[:prefix_len]].append(norm)
        # También indexar por apellido (última palabra)
        parts = norm.split()
        if len(parts) > 1:
            last = parts[-1]
            if len(last) >= prefix_len:
                idx[last[:prefix_len]].append(norm)
    return idx


def _find_match(
    norm_name: str,
    source_players: dict[str, list[dict]],
    threshold: float = 0.85,
    prefix_index: Optional[dict] = None,
) -> Optional[dict]:
    """Busca un match exacto o fuzzy en los jugadores de una fuente."""
    # 1. Exact match
    if norm_name in source_players:
        candidates = source_players[norm_name]
        if len(candidates) == 1:
            return {"id": candidates[0]["id"], "name": candidates[0]["name"], "method": "exact"}
        return {"id": candidates[0]["id"], "name": candidates[0]["name"], "method": "exact"}

    # 2. Fuzzy match — solo contra candidatos con prefijo similar
    if not prefix_index or len(norm_name) < 4:
        return None

    candidates_to_check: set[str] = set()
    # Prefijo del nombre completo
    prefix = norm_name[:4]
    if prefix in prefix_index:
        candidates_to_check.update(prefix_index[prefix])
    # Prefijo del apellido
    parts = norm_name.split()
    if len(parts) > 1:
        last_prefix = parts[-1][:4]
        if last_prefix in prefix_index:
            candidates_to_check.update(prefix_index[last_prefix])

    best_score = 0.0
    best_match = None
    for source_norm in candidates_to_check:
        sim = _similarity(norm_name, source_norm)
        if sim >= threshold and sim > best_score:
            best_score = sim
            best_match = source_players[source_norm][0]

    if best_match:
        return {"id": best_match["id"], "name": best_match["name"], "method": "fuzzy"}

    return None


def _write_sql(updates: list[dict], output_path: Path) -> None:
    """Genera archivo SQL con UPDATEs para dim_player."""
    field_to_column = {
        "id_whoscored": "id_whoscored",
        "id_sofascore": "id_sofascore",
        "id_understat": "id_understat",
    }
    # Agrupar por field para generar UPDATEs más compactos
    by_field: dict[str, list[dict]] = defaultdict(list)
    for u in updates:
        by_field[u["field"]].append(u)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write("-- Auto-generated by crosslink_player_ids.py\n")
        f.write("-- Aplica con: psql -U postgres -d football_db -f db/crosslink_updates.sql\n\n")
        f.write("BEGIN;\n\n")

        for field, field_updates in by_field.items():
            col = field_to_column[field]
            # Determinar tipo (int para ws/us, int para ss)
            f.write(f"-- {col}: {len(field_updates)} updates\n")
            for u in field_updates:
                val = u["value"]
                f.write(
                    f"UPDATE dim_player SET {col} = {val} "
                    f"WHERE canonical_id = {u['canonical_id']} "
                    f"AND {col} IS NULL; "
                    f"-- matched: {u['matched_name']} ({u['method']})\n"
                )
            f.write("\n")

        f.write("COMMIT;\n")


# ── CLI ──────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Cross-link player IDs entre fuentes")
    parser.add_argument("--dry-run", action="store_true", help="Solo contar, no escribir SQL")
    parser.add_argument("--output", default=None, help="Ruta del SQL de salida")
    args = parser.parse_args()

    print("=" * 55)
    print("  Player ID Cross-Linker")
    print("=" * 55)
    stats = crosslink(dry_run=args.dry_run)
    if stats:
        total = sum(stats.values())
        print(f"\n[OK] {total} links encontrados")


if __name__ == "__main__":
    main()
