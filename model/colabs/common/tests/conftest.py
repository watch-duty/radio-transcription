"""pytest configuration for the common/ test suite.

Module-scope only — these are pure unit tests over in-memory data; no Docker,
no database, no env-var injection is needed (unlike the backend conftest).

Also puts ``model/colabs/`` on ``sys.path`` so the suite's ``from common...``
imports resolve when pytest is invoked directly (e.g. a plain ``pytest`` run
from ``model/``), without first ``pip install -e``-ing the ``common`` package.
Without this, ``colabs/__init__.py`` makes pytest's prepend-import mode expose
the library only as ``colabs.common``.
"""

import sys
from pathlib import Path

# conftest.py lives at model/colabs/common/tests/ — parents[2] is model/colabs/.
_COLABS_DIR = str(Path(__file__).resolve().parents[2])
if _COLABS_DIR not in sys.path:
    sys.path.insert(0, _COLABS_DIR)
