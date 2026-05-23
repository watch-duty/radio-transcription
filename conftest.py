from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

HEAVY_TEST_OPTION = "--include-heavy-tests"
HEAVY_TEST_OPTION_DEST = "include_heavy_tests"


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        HEAVY_TEST_OPTION,
        dest=HEAVY_TEST_OPTION_DEST,
        action="store_true",
        default=False,
        help=(
            "Include tests that start Docker/testcontainers containers or "
            "require externally managed services."
        ),
    )


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line(
        "markers",
        "heavy: starts Docker/testcontainers containers or requires external services",
    )

    include_heavy = bool(config.getoption(HEAVY_TEST_OPTION_DEST))
    if not include_heavy and _explicit_heavy_target_selected(config):
        message = (
            f"Heavy tests require {HEAVY_TEST_OPTION}. Use an existing mise task "
            "such as `mise run test:component`, `mise run test:api`, or "
            "`mise run test:e2e:local`."
        )
        raise pytest.UsageError(message)

    if include_heavy and _xdist_enabled(config):
        message = (
            "Heavy tests must run without pytest-xdist because each worker can "
            "start its own Docker services. Re-run with `-n 0` after "
            f"{HEAVY_TEST_OPTION}."
        )
        raise pytest.UsageError(message)


def pytest_ignore_collect(
    collection_path: Path,
    config: pytest.Config,
) -> bool | None:
    if config.getoption(HEAVY_TEST_OPTION_DEST):
        return None

    if _is_heavy_path(collection_path, config.rootpath):
        return True

    return None


def _explicit_heavy_target_selected(config: pytest.Config) -> bool:
    for raw_arg in config.args:
        arg = raw_arg.split("::", 1)[0]
        if arg.startswith("-"):
            continue
        if _is_heavy_path(Path(arg), config.rootpath):
            return True
    return False


def _is_heavy_path(path: Path, rootpath: Path) -> bool:
    candidate = path if path.is_absolute() else rootpath / path
    try:
        rel_path = candidate.resolve().relative_to(rootpath.resolve())
    except ValueError:
        return False

    if rel_path.parts and rel_path.parts[0] == "integration_tests":
        return True

    return candidate.is_file() and rel_path.name.endswith("_integration.py")


def _xdist_enabled(config: pytest.Config) -> bool:
    numprocesses: Any = getattr(config.option, "numprocesses", None)
    return numprocesses not in (None, 0, "0")
