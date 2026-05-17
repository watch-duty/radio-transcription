"""pytest configuration for the common/ test suite.

Module-scope only — these are pure unit tests over in-memory data; no Docker,
no database, no env-var injection is needed (unlike the backend conftest).
"""

from pathlib import Path

import pytest

_TESTS_DIR = Path(__file__).resolve().parent


@pytest.fixture(scope="session")
def golden_data_dir() -> Path:
    """Path to tests/golden_data/ alongside this conftest (created on demand)."""
    return _TESTS_DIR / "golden_data"
