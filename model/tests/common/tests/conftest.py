"""pytest configuration for the common/ test suite.

Module-scope only — these are pure unit tests over in-memory data; no Docker,
no database, no env-var injection is needed (unlike the backend conftest).

Also puts ``model/src/`` on ``sys.path`` so the suite's ``from common...``
imports resolve when pytest is invoked directly without first installing the
``radio-transcription-model`` package.
"""

import sys
from pathlib import Path

# conftest.py lives at model/tests/common/tests/ — parents[3] is model/.
_SRC_DIR = str(Path(__file__).resolve().parents[3] / "src")
if _SRC_DIR not in sys.path:
    sys.path.insert(0, _SRC_DIR)

_TESTS_DIR = str(Path(__file__).resolve().parents[2])
if _TESTS_DIR not in sys.path:
    sys.path.append(_TESTS_DIR)
