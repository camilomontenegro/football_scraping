"""
scripts/organize_data.py
========================
DEPRECATED — usar `scripts.reorganize_data` en su lugar.

Este script implementaba un layout antiguo que ya NO es el estándar:

    data/raw/<source>/<comp_slug>/season=YYYY_YYYY/<archivo>   ← obsoleto

El layout canónico actual (definido en `utils/data_paths.py`) es:

    data/raw/<comp_slug>/<season>/<source>/<files>
    data/clean/<comp_slug>/<season>/<source>/<files>.csv

Para migrar usa:

    python -m scripts.reorganize_data --dry-run
    python -m scripts.reorganize_data --apply

Se mantiene este módulo como puro redirector para no romper docs/aliases.
"""
from __future__ import annotations

import sys


_REDIRECT_MSG = (
    "[DEPRECATED] `scripts.organize_data` ha sido reemplazado por "
    "`scripts.reorganize_data`.\n"
    "    Estructura objetivo: data/raw/<comp>/<season>/<source>/...\n"
    "    Ejecuta:\n"
    "        python -m scripts.reorganize_data --dry-run\n"
    "        python -m scripts.reorganize_data --apply\n"
)


def main() -> None:
    print(_REDIRECT_MSG, file=sys.stderr)
    try:
        from scripts.reorganize_data import main as _new_main
    except Exception as exc:
        print(f"No se pudo cargar reorganize_data: {exc}", file=sys.stderr)
        sys.exit(2)
    _new_main()


if __name__ == "__main__":
    main()
