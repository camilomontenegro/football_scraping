"""
Wrapper de compatibilidad para scripts.pipeline_runner.
Redirige la ejecución al nuevo módulo en la carpeta wizard/.
"""
import sys
from pathlib import Path

# Asegurar que la raíz esté en el path
root = Path(__file__).resolve().parent.parent
if str(root) not in sys.path:
    sys.path.append(str(root))

from wizard.pipeline_runner import *
from wizard.pipeline_runner import _season_variants

if __name__ == "__main__":
    from wizard.pipeline_runner import main
    main()
