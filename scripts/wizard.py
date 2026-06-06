"""
Wrapper de compatibilidad para scripts.wizard.
Redirige la ejecución al nuevo módulo en la carpeta wizard/.
"""
import sys
from pathlib import Path

# Asegurar que la raíz esté en el path
root = Path(__file__).resolve().parent.parent
if str(root) not in sys.path:
    sys.path.append(str(root))

from wizard.wizard import *

if __name__ == "__main__":
    from wizard.wizard import main
    main()
