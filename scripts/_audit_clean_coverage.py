"""Auditoría rápida de cobertura en data/clean/<comp>/<season>/."""
from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from wizard.competitions import WORKING_COMPETITION_NAMES, get_competition
from utils.data_paths import clean_dir, slugify_competition
from wizard.pipeline_runner import _is_international

SEASON = "2025_2026"

EXPECTED = {
    "sofascore": ["teams.csv", "players.csv", "matches.csv", "shots.csv", "events.csv"],
    "transfermarkt": ["players.csv", "injuries.csv"],
    "understat": ["teams.csv", "players.csv", "matches.csv", "shots.csv"],
    "whoscored": ["teams.csv", "players.csv", "matches.csv", "events.csv"],
    "statsbomb": ["teams.csv", "players.csv", "matches.csv", "events.csv"],
}

# Partidos en temporada completa (referencia SofaScore matches.csv)
EXPECTED_MATCHES = {
    "La Liga": 380,
    "Premier League": 380,
    "Bundesliga": 306,
    "Serie A": 380,
    "Ligue 1": 306,
    "Primeira Liga": 306,
    "Eredivisie": 306,
    "Champions League": 189,
    "Europa League": 189,
    "Europa Conference League": 189,
    "European Championship": 51,
    "Copa America": 32,
    "FIFA World Cup": 104,
}


def count_csv_rows(path: Path) -> int:
    if not path.exists() or path.stat().st_size == 0:
        return 0
    with path.open(encoding="utf-8", errors="replace") as f:
        return max(0, sum(1 for _ in f) - 1)


def sources_for_comp(name: str) -> list[str]:
    conf = get_competition(name) or {}
    srcs = conf.get("sources", {})
    out: list[str] = []
    if srcs.get("transfermarkt", {}).get("league_code"):
        out.append("transfermarkt")
    if srcs.get("sofascore", {}).get("tournament_id") is not None:
        out.append("sofascore")
    if srcs.get("understat", {}).get("league") and not _is_international(conf):
        out.append("understat")
    if srcs.get("statsbomb", {}).get("competition_id") is not None:
        out.append("statsbomb")
    ws = srcs.get("whoscored", {})
    if ws.get("tournament_id") is not None or ws.get("tournament"):
        out.append("whoscored")
    return out


def main() -> int:
    print("=" * 100)
    print(f"AUDITORIA data/clean/  temporada {SEASON}")
    print("=" * 100)

    issues: list[tuple[str, str, str]] = []
    ok_count = 0

    for comp in sorted(WORKING_COMPETITION_NAMES):
        slug = slugify_competition(comp)
        base = PROJECT_ROOT / "data" / "clean" / slug / SEASON
        if not base.exists():
            issues.append((comp, "MISSING", f"sin carpeta {base.relative_to(PROJECT_ROOT)}"))
            print(f"\n## {comp}: SIN DATOS")
            continue

        exp_srcs = sources_for_comp(comp)
        exp_matches = EXPECTED_MATCHES.get(comp)
        header = f"\n## {comp}"
        if exp_matches:
            header += f"  (ref. ~{exp_matches} partidos SS)"
        print(header)

        comp_ok = True

        for src in exp_srcs:
            d = clean_dir(comp, SEASON, src)
            missing = [f for f in EXPECTED[src] if not (d / f).exists()]
            if missing:
                comp_ok = False
                issues.append((comp, src, f"faltan archivos: {missing}"))
                print(f"  [{src}] FALTA: {missing}")
                continue

            sizes = {f: count_csv_rows(d / f) for f in EXPECTED[src]}
            print(f"  [{src}] " + ", ".join(f"{k}={v}" for k, v in sizes.items()))

            if src == "sofascore" and exp_matches:
                m = sizes.get("matches.csv", 0)
                if m == 0:
                    comp_ok = False
                    issues.append((comp, src, "matches.csv vacio"))
                else:
                    pct = 100 * m / exp_matches
                    if m < exp_matches * 0.5:
                        comp_ok = False
                        issues.append((comp, src, f"solo {m}/{exp_matches} partidos ({pct:.0f}%)"))
                        print(f"         WARN: {m}/{exp_matches} partidos ({pct:.0f}%)")
                    elif m < exp_matches * 0.85:
                        print(f"         nota: {m}/{exp_matches} ({pct:.0f}%) — temporada en curso")

            if src == "whoscored":
                e = sizes.get("events.csv", 0)
                m = sizes.get("matches.csv", 0)
                if e == 0:
                    comp_ok = False
                    issues.append((comp, src, "events.csv vacio o sin filas"))
                elif exp_matches and m > 0 and m < exp_matches * 0.5:
                    comp_ok = False
                    issues.append((comp, src, f"solo {m} partidos WS ({100*m/exp_matches:.0f}%)"))

            if src == "understat" and sizes.get("matches.csv", 0) == 0:
                comp_ok = False
                issues.append((comp, src, "matches.csv vacio"))

            if src == "statsbomb" and sizes.get("matches.csv", 0) == 0:
                comp_ok = False
                issues.append((comp, src, "sin datos statsbomb"))

        for sub in sorted(base.iterdir()):
            if sub.is_dir() and sub.name not in exp_srcs:
                print(f"  [extra] carpeta {sub.name}/ (no requerida por config)")

        if comp_ok:
            ok_count += 1

    print("\n" + "=" * 100)
    print(f"OK sin gaps criticos: {ok_count}/{len(WORKING_COMPETITION_NAMES)}")
    print(f"Problemas detectados: {len(issues)}")
    for c, s, msg in issues:
        print(f"  - {c} / {s}: {msg}")
    print("=" * 100)
    return 1 if issues else 0


if __name__ == "__main__":
    raise SystemExit(main())
