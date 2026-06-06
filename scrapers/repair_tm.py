"""
scrapers/repair_tm.py
======================
DEPRECATED — utilidad legacy de reconstrucción de CSVs de Transfermarkt cuando
los CSVs se perdían pero quedaban los JSON crudos.

Con el nuevo layout canónico (`data/raw/<comp>/<season>/transfermarkt/players/`
y `…/injuries/`), la regeneración correcta es ejecutar el scraper normal:

    python -m scrapers.transfermarkt_scraper --competition "La Liga" --seasons 2024

Si necesitas el comportamiento antiguo (consolidar JSON viejos en CSV), recupera
este archivo desde el historial de git.
"""

from __future__ import annotations

import sys


def main() -> None:
    print(
        "[repair_tm] DEPRECATED. Usa `python -m scrapers.transfermarkt_scraper "
        "--competition <Comp> --seasons <YYYY>` para regenerar players.csv y "
        "injuries.csv en el layout canónico.",
        file=sys.stderr,
    )
    sys.exit(2)


if __name__ == "__main__":
    main()
