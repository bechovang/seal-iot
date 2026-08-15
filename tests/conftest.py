import os
import sys
from pathlib import Path

# Ensure the flat module dirs (adapters, bus, history, perceive) and root config
# are importable regardless of where pytest is invoked from.
_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))