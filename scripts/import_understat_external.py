"""
scripts/import_understat_external.py
======================================
Importa CSVs de Understat preparados por un colaborador externo al layout
canónico ``data/clean/<comp>/<season>/understat/``.

Fuente: carpeta ``understat/`` en la raíz del repo, organizada por liga:
  understat/bundesliga/understat_{matches,players,shots}_bundesliga.csv
  understat/ligue_1/understat_{matches,players,shots}_ligue_1.csv
  understat/seria_a/understat_{matches,players,shots}_serie_a.csv      (sic)

Destino:
  data/clean/bundesliga/2025_2026/understat/{matches,players,shots,teams}.csv
  data/clean/ligue_1/2025_2026/understat/{matches,players,shots,teams}.csv
  data/clean/serie_a/2025_2026/understat/{matches,players,shots,teams}.csv

Los CSVs externos vienen en formato RAW (pre-transform). Reusamos las
funciones ``transform_shots`` y ``extract_teams`` del scraper Understat para
aplicar la misma normalización que en La Liga / Premier (mapas de
result/shot_type/situation, coords ×105/×68, redondeos), garantizando que
los CSVs importados sean indistinguibles de los scrapeados localmente.

Uso:
  python scripts/import_understat_external.py                    # las 3 ligas
  python scripts/import_understat_external.py --league Bundesliga
  python scripts/import_understat_external.py --dry-run          # validar
"""
import argparse
import sys
from pathlib import Path

import pandas as pd

# Permitir imports desde la raíz del proyecto
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from scrapers.understat_scraper import transform_shots, extract_teams  # noqa: E402
from utils.data_paths import save_clean_csv  # noqa: E402


# Cada entrada describe una liga importable:
#   - external_dir: carpeta dentro de understat/ (puede contener typos)
#   - file_suffix:  sufijo de los ficheros (sin extensión)
#   - competition:  nombre canónico (resuelto por wizard.competitions)
EXTERNAL_LEAGUES = [
    {
        "external_dir": "bundesliga",
        "file_suffix":  "bundesliga",
        "competition":  "Bundesliga",
    },
    {
        "external_dir": "ligue_1",
        "file_suffix":  "ligue_1",
        "competition":  "Ligue 1",
    },
    {
        "external_dir": "seria_a",      # typo en la carpeta de origen
        "file_suffix":  "serie_a",
        "competition":  "Serie A",
    },
]

SEASON = "2025_2026"
SOURCE = "understat"


def _team_lookup(df_matches: pd.DataFrame) -> dict:
    """``match_id -> {'h': home_team, 'a': away_team}``."""
    return {
        int(row.understat_match_id): {"h": row.home_team, "a": row.away_team}
        for row in df_matches.itertuples()
    }


def _reconstruct_shots_raw(
    df_shots_external: pd.DataFrame,
    df_matches: pd.DataFrame,
) -> pd.DataFrame:
    """
    Da al DataFrame de shots externo el shape esperado por ``transform_shots``:

      understat_shot_id, understat_match_id, understat_player_id,
      understat_team, side, player_name, minute, x, y, xg, result,
      shot_type, situation, last_action, player_assisted, season, source

    - ``understat_team`` se reconstruye desde ``match_id`` + ``side`` usando
      df_matches.
    - ``last_action`` y ``player_assisted`` no vienen en la fuente externa →
      se rellenan a NaN (consistente con cómo quedarían si Understat no
      hubiera publicado esos campos).
    - ``source`` se setea a "understat".
    """
    df = df_shots_external.copy()

    lookup = _team_lookup(df_matches)
    df["understat_team"] = df.apply(
        lambda row: (lookup.get(int(row.understat_match_id), {}) or {}).get(row.side),
        axis=1,
    )

    for missing in ("last_action", "player_assisted"):
        if missing not in df.columns:
            df[missing] = pd.NA

    df["source"] = "understat"
    return df


def import_league(spec: dict, *, dry_run: bool = False) -> bool:
    ext_dir = PROJECT_ROOT / "understat" / spec["external_dir"]
    if not ext_dir.is_dir():
        print(f"[!] Falta carpeta de origen: {ext_dir}")
        return False

    suffix = spec["file_suffix"]
    matches_csv = ext_dir / f"understat_matches_{suffix}.csv"
    players_csv = ext_dir / f"understat_players_{suffix}.csv"
    shots_csv   = ext_dir / f"understat_shots_{suffix}.csv"

    for path in (matches_csv, players_csv, shots_csv):
        if not path.exists():
            print(f"[!] Falta {path.name} para {spec['competition']}")
            return False

    df_matches   = pd.read_csv(matches_csv)
    df_players   = pd.read_csv(players_csv)
    df_shots_ext = pd.read_csv(shots_csv)

    print(f"\n[{spec['competition']}]")
    print(f"  Leído  : matches={len(df_matches)}  players={len(df_players)}  shots={len(df_shots_ext)}")

    df_shots_full  = _reconstruct_shots_raw(df_shots_ext, df_matches)
    df_shots_clean = transform_shots(df_shots_full, df_matches)
    df_teams       = extract_teams(df_matches)

    # Sanity: ningún shot debe quedarse sin understat_team
    null_team = df_shots_clean["understat_team"].isna().sum()
    if null_team:
        print(f"  [!] AVISO: {null_team} tiros sin understat_team (match_id ausente en matches.csv)")

    print(f"  Transf : shots={len(df_shots_clean)}  teams={len(df_teams)}")

    if dry_run:
        print("  (dry-run: nada escrito)")
        return True

    save_clean_csv(spec["competition"], SEASON, SOURCE, "matches", df_matches)
    save_clean_csv(spec["competition"], SEASON, SOURCE, "players", df_players)
    save_clean_csv(spec["competition"], SEASON, SOURCE, "shots",   df_shots_clean)
    save_clean_csv(spec["competition"], SEASON, SOURCE, "teams",   df_teams)
    print(f"  OK  -> data/clean/<slug>/{SEASON}/{SOURCE}/")
    return True


def main():
    parser = argparse.ArgumentParser(
        description="Importa CSVs externos de Understat al layout canónico.",
    )
    parser.add_argument(
        "--league",
        choices=[s["competition"] for s in EXTERNAL_LEAGUES] + ["all"],
        default="all",
        help="Liga a importar (default: all)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="No escribe ficheros; solo informa lo que se haría.",
    )
    args = parser.parse_args()

    if args.league == "all":
        specs = EXTERNAL_LEAGUES
    else:
        specs = [s for s in EXTERNAL_LEAGUES if s["competition"] == args.league]

    results = [import_league(s, dry_run=args.dry_run) for s in specs]

    print(f"\n=== {sum(results)}/{len(results)} ligas procesadas ===")


if __name__ == "__main__":
    main()
