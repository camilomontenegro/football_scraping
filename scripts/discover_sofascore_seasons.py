"""
Discover SofaScore season IDs and optionally regenerate scrapers/sofascore_seasons.py.

Uses the public api.var11.com mirror when the official SofaScore API is blocked.

Usage:
    python scripts/discover_sofascore_seasons.py
    python scripts/discover_sofascore_seasons.py --write
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from scrapers.sofascore_seasons import SOFASCORE_SEASON_IDS, sofascore_season_available
from wizard.competitions import COMPETITIONS, WORKING_COMPETITION_NAMES


def main() -> None:
    parser = argparse.ArgumentParser(description="List or regenerate SofaScore season IDs")
    parser.add_argument(
        "--write",
        action="store_true",
        help="Regenerate scrapers/sofascore_seasons.py from the mirror API",
    )
    args = parser.parse_args()

    if args.write:
        subprocess.run([sys.executable, str(ROOT / "scripts" / "_gen_sofascore_seasons.py")], check=True)
        print("[OK] scrapers/sofascore_seasons.py regenerated")
        return

    report = {}
    for comp in sorted(WORKING_COMPETITION_NAMES):
        tid = COMPETITIONS[comp]["sources"]["sofascore"]["tournament_id"]
        seasons = SOFASCORE_SEASON_IDS.get(tid, {})
        report[comp] = {
            "tournament_id": tid,
            "seasons": seasons,
            "sample_checks": {
                "2024/2025": sofascore_season_available(comp, "2024/2025"),
                "2025/2026": sofascore_season_available(comp, "2025/2026"),
            },
        }
        print(f"{comp} (tid={tid}): {len(seasons)} seasons")

    out = ROOT / "data" / "logs" / "sofascore_seasons_discovered.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(f"[OK] snapshot -> {out}")


if __name__ == "__main__":
    main()
