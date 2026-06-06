"""
scripts/import_teammate_bundle.py
====================================
Importa el bundle ``2025-2026-*.zip`` (Transfermarkt + SofaScore) al layout
``data/clean/<comp>/2025_2026/<source>/``.

Estructura esperada dentro del zip:
  2025-2026/tranfermarkt/<slug>/players_clean.csv
  2025-2026/tranfermarkt/<slug>/injuries_clean.csv
  2025-2026/sofascore/<slug>/sofascore/season=*/matches_clean.csv
  ...

Uso:
  python scripts/import_teammate_bundle.py --zip "C:\\Users\\...\\2025-2026.zip"
  python scripts/import_teammate_bundle.py --extracted "C:\\...\\2025-2026"
"""
from __future__ import annotations

import argparse
import shutil
import sys
import zipfile
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from utils.data_paths import clean_csv_path, save_clean_csv, slugify_competition  # noqa: E402

SEASON = "2025_2026"

# slug en zip → nombre canónico wizard
FOLDER_TO_COMPETITION: dict[str, str] = {
    "la_liga": "La Liga",
    "premier_league": "Premier League",
    "bundesliga": "Bundesliga",
    "serie_a": "Serie A",
    "ligue_1": "Ligue 1",
    "primeira_liga": "Primeira Liga",
    "eredivisie": "Eredivisie",
    "champions": "Champions League",
    "champions_league": "Champions League",
    "europa_league": "Europa League",
    "uefa_europa_league": "Europa League",
}

SOFA_RENAME = {
    "matches_clean.csv": "matches.csv",
    "shots_clean.csv": "shots.csv",
    "events_clean.csv": "events.csv",
    "players.csv": "players.csv",
    "teams.csv": "teams.csv",
}


def _merge_df(path: Path, df_new: pd.DataFrame, key) -> pd.DataFrame:
    if not path.exists():
        return df_new
    df_old = pd.read_csv(path)
    if df_old.empty:
        return df_new
    keys = [key] if isinstance(key, str) else list(key)
    keys = [k for k in keys if k in df_old.columns and k in df_new.columns]
    if not keys:
        return pd.concat([df_old, df_new], ignore_index=True)
    return (
        pd.concat([df_old, df_new], ignore_index=True)
        .drop_duplicates(subset=keys, keep="last")
    )


def _transform_tm_players(df: pd.DataFrame, competition: str) -> pd.DataFrame:
    if df.empty:
        return df
    out = pd.DataFrame({
        "id_transfermarkt": pd.to_numeric(df.get("player_id"), errors="coerce"),
        "canonical_name": df.get("player_name"),
        "nationality": df.get("nationality"),
        "birth_date": df.get("birth_date"),
        "position": df.get("position"),
        "competition": competition,
        "team_name": df.get("team_name", df.get("team")),
        "team_slug": df.get("team_slug", df.get("team")),
        "team_id_tm": pd.to_numeric(df.get("team_id_tm"), errors="coerce"),
        "season": df.get("season"),
        "source": "transfermarkt",
    })
    out = out.dropna(subset=["id_transfermarkt"])
    out["id_transfermarkt"] = out["id_transfermarkt"].astype("Int64")
    return out.drop_duplicates(subset=["id_transfermarkt"]).reset_index(drop=True)


def _transform_tm_injuries(df: pd.DataFrame, players: pd.DataFrame | None) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    if "player_id_tm" not in out.columns and "player_id" in out.columns:
        out["player_id_tm"] = out["player_id"]
    out["player_id_tm"] = pd.to_numeric(out["player_id_tm"], errors="coerce")
    if "player_name" not in out.columns and players is not None and not players.empty:
        names = players.set_index("id_transfermarkt")["canonical_name"]
        out["player_name"] = out["player_id_tm"].map(names)
    for col in (
        "player_name", "season", "injury_type", "date_from", "date_until",
        "days_absent", "matches_missed", "club_name", "club_id_tm", "club_slug",
        "squad_team_slug",
    ):
        if col not in out.columns:
            out[col] = pd.NA
    return out


def _bundle_root(extracted: Path) -> Path:
    """Acepta raíz con o sin carpeta intermedia 2025-2026."""
    if (extracted / "2025-2026").is_dir():
        return extracted / "2025-2026"
    if (extracted / "tranfermarkt").is_dir() or (extracted / "sofascore").is_dir():
        return extracted
    raise FileNotFoundError(f"No se reconoce estructura en {extracted}")


def import_transfermarkt(root: Path, *, merge: bool, dry_run: bool) -> int:
    tm_root = root / "tranfermarkt"
    if not tm_root.is_dir():
        print("[!] Sin carpeta tranfermarkt/ en el bundle")
        return 0
    count = 0
    for slug_dir in sorted(tm_root.iterdir()):
        if not slug_dir.is_dir():
            continue
        slug = slug_dir.name
        competition = FOLDER_TO_COMPETITION.get(slug)
        if not competition:
            print(f"  [skip] TM slug desconocido: {slug}")
            continue
        players_path = slug_dir / "players_clean.csv"
        injuries_path = slug_dir / "injuries_clean.csv"
        if not players_path.exists() and not injuries_path.exists():
            continue
        print(f"\n[TM] {competition} ({slug})")
        if players_path.exists():
            df_p = _transform_tm_players(pd.read_csv(players_path), competition)
            dest = clean_csv_path(competition, SEASON, "transfermarkt", "players")
            if merge:
                df_p = _merge_df(dest, df_p, "id_transfermarkt")
            print(f"  players: {len(df_p)} filas")
            if not dry_run:
                save_clean_csv(competition, SEASON, "transfermarkt", "players", df_p)
        if injuries_path.exists():
            df_players = (
                _transform_tm_players(pd.read_csv(players_path), competition)
                if players_path.exists()
                else None
            )
            df_i = _transform_tm_injuries(pd.read_csv(injuries_path), df_players)
            dest = clean_csv_path(competition, SEASON, "transfermarkt", "injuries")
            subset = ["player_id_tm", "date_from", "injury_type"]
            subset = [c for c in subset if c in df_i.columns]
            if merge and subset:
                df_i = _merge_df(dest, df_i, subset)
            print(f"  injuries: {len(df_i)} filas")
            if not dry_run:
                save_clean_csv(competition, SEASON, "transfermarkt", "injuries", df_i)
        count += 1
    return count


def import_sofascore(root: Path, *, merge: bool, dry_run: bool) -> int:
    sofa_root = root / "sofascore"
    if not sofa_root.is_dir():
        print("[!] Sin carpeta sofascore/ en el bundle")
        return 0
    count = 0
    for slug_dir in sorted(sofa_root.iterdir()):
        if not slug_dir.is_dir():
            continue
        slug = slug_dir.name
        competition = FOLDER_TO_COMPETITION.get(slug)
        if not competition:
            print(f"  [skip] SofaScore slug desconocido: {slug}")
            continue
        season_dirs = [
            d for d in slug_dir.glob("sofascore/season=*")
            if (d / "matches_clean.csv").exists() or list(d.glob("*.csv"))
        ]
        if not season_dirs:
            continue
        # El glob puede partir en "season=Serie A 25" vs "25_26"; quedarse con datos.
        season_dir = max(
            season_dirs,
            key=lambda d: len(list(d.glob("*.csv"))),
        )
        print(f"\n[SofaScore] {competition} ({slug})")
        for src_name, dest_name in SOFA_RENAME.items():
            src = season_dir / src_name
            if not src.exists():
                continue
            df = pd.read_csv(src)
            stem = dest_name.replace(".csv", "")
            dest = clean_csv_path(competition, SEASON, "sofascore", stem)
            if merge:
                if stem == "matches":
                    key = "id_sofascore"
                elif stem == "shots":
                    key = ["match_id_ss", "minute", "player_id_ss"]
                    key = [k for k in key if k in df.columns]
                    if not key:
                        key = "id_sofascore"
                elif stem == "events":
                    key = ["match_id_ss", "minute", "event_type"]
                    key = [k for k in key if k in df.columns] or "match_id_ss"
                elif stem == "players":
                    key = "id_sofascore"
                elif stem == "teams":
                    key = "id_sofascore"
                else:
                    key = df.columns[0]
                df = _merge_df(dest, df, key)
            print(f"  {stem}: {len(df)} filas")
            if not dry_run:
                save_clean_csv(competition, SEASON, "sofascore", stem, df)
        count += 1
    return count


def main() -> None:
    parser = argparse.ArgumentParser(description="Importa bundle 2025-2026 (TM + SofaScore)")
    parser.add_argument("--zip", type=Path, help="Ruta al .zip del bundle")
    parser.add_argument("--extracted", type=Path, help="Carpeta ya descomprimida")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--no-merge", action="store_true")
    args = parser.parse_args()

    if not args.zip and not args.extracted:
        parser.error("Indica --zip o --extracted")

    merge = not args.no_merge
    tmp_dir: Path | None = None
    try:
        if args.extracted:
            root = _bundle_root(args.extracted.resolve())
        else:
            tmp_dir = PROJECT_ROOT / "data" / ".import_staging" / "teammate_bundle"
            if tmp_dir.exists():
                shutil.rmtree(tmp_dir)
            tmp_dir.mkdir(parents=True)
            print(f"Extrayendo {args.zip} ...")
            with zipfile.ZipFile(args.zip, "r") as zf:
                zf.extractall(tmp_dir)
            root = _bundle_root(tmp_dir)

        print(f"Bundle root: {root}")
        n_tm = import_transfermarkt(root, merge=merge, dry_run=args.dry_run)
        n_ss = import_sofascore(root, merge=merge, dry_run=args.dry_run)
        print(f"\n=== OK: {n_tm} competiciones TM, {n_ss} SofaScore ===")
    finally:
        if tmp_dir and tmp_dir.exists() and not args.extracted:
            shutil.rmtree(tmp_dir, ignore_errors=True)


if __name__ == "__main__":
    main()
