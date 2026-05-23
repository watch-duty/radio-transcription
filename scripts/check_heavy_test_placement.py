from __future__ import annotations

import ast
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

HEAVY_IMPORT_ROOTS = {"docker", "testcontainers"}
HEAVY_STRING_MARKERS = ("alloydbomni",)
SKIP_DIRS = {
    ".git",
    ".mypy_cache",
    ".pytest_cache",
    ".ruff_cache",
    ".venv",
    "__pycache__",
    "node_modules",
    "radio_transcription.egg-info",
}


def main() -> int:
    offenders: list[Path] = []
    for path in _python_files():
        if not _is_test_like(path):
            continue
        if _is_allowed_heavy_test_path(path):
            continue
        if _uses_heavy_test_helpers(path):
            offenders.append(path.relative_to(ROOT))

    if not offenders:
        sys.stdout.write("Heavy test placement check passed\n")
        return 0

    sys.stderr.write(
        "Heavy test helpers are only allowed under integration_tests/ or in "
        "*_integration.py files:\n"
    )
    for offender in offenders:
        sys.stderr.write(f"  - {offender}\n")
    return 1


def _python_files() -> list[Path]:
    files: list[Path] = []
    for dirpath, dirnames, filenames in os.walk(ROOT):
        dirnames[:] = [
            dirname for dirname in dirnames if dirname not in SKIP_DIRS
        ]
        files.extend(
            Path(dirpath) / filename
            for filename in filenames
            if filename.endswith(".py")
        )
    return files


def _is_test_like(path: Path) -> bool:
    rel_parts = path.relative_to(ROOT).parts
    return (
        path.name == "conftest.py"
        or path.name.startswith("test_")
        or path.name.endswith("_test.py")
        or "tests" in rel_parts
    )


def _is_allowed_heavy_test_path(path: Path) -> bool:
    rel_path = path.relative_to(ROOT)
    return rel_path.parts[0] == "integration_tests" or path.name.endswith(
        "_integration.py"
    )


def _uses_heavy_test_helpers(path: Path) -> bool:
    try:
        tree = ast.parse(path.read_text(), filename=str(path))
    except SyntaxError as exc:
        sys.stderr.write(f"Could not parse {path.relative_to(ROOT)}: {exc}\n")
        return True

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            if any(_is_heavy_module(alias.name) for alias in node.names):
                return True
        elif isinstance(node, ast.ImportFrom):
            if node.module and _is_heavy_module(node.module):
                return True
        elif isinstance(node, ast.Constant) and isinstance(node.value, str):
            if any(marker in node.value for marker in HEAVY_STRING_MARKERS):
                return True

    return False


def _is_heavy_module(module: str) -> bool:
    return module.split(".", 1)[0] in HEAVY_IMPORT_ROOTS


if __name__ == "__main__":
    raise SystemExit(main())
