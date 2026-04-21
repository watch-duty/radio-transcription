#!/usr/bin/env python3
"""Dump the contract of required env vars per backend pipeline service.

AST-walks ``backend/pipeline/<service>/`` for calls to ``_require_env("LITERAL")``
and emits a JSON object mapping service name to the sorted list of env var
names it requires at runtime. Service = immediate child directory of
``backend/pipeline/``; services with zero required vars are omitted.

Used by the deployment repo's CI (see ``verify_required_env.py``) to assert that
every required env var has a value set in terraform's ``container_env``.

Fails fast if ``_require_env(ARG)`` is called with anything other than a string
literal — a non-literal would hide the contract from static analysis.
"""

from __future__ import annotations

import argparse
import ast
import json
import sys
from pathlib import Path

_REQUIRE_ENV_NAME = "_require_env"


def extract_required_env(tree: ast.AST, source_path: Path) -> list[str]:
    """Return all literal env var names passed to ``_require_env(...)`` calls."""
    found: list[str] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        is_require_env = (
            isinstance(func, ast.Name) and func.id == _REQUIRE_ENV_NAME
        ) or (
            isinstance(func, ast.Attribute) and func.attr == _REQUIRE_ENV_NAME
        )
        if not is_require_env:
            continue
        if not node.args:
            msg = f"{source_path}:{node.lineno}: _require_env() called with no arguments"
            raise ValueError(msg)
        first = node.args[0]
        if not (isinstance(first, ast.Constant) and isinstance(first.value, str)):
            msg = (
                f"{source_path}:{node.lineno}: _require_env() first argument "
                f"must be a string literal (got {ast.dump(first)}). "
                "Non-literals hide the contract from static analysis."
            )
            raise TypeError(msg)
        found.append(first.value)
    return found


def scan_service_dir(service_dir: Path) -> list[str]:
    """Return the sorted, de-duplicated set of required env vars in ``service_dir``."""
    required: set[str] = set()
    for py_file in sorted(service_dir.rglob("*.py")):
        try:
            source = py_file.read_text(encoding="utf-8")
        except OSError as exc:
            msg = f"cannot read {py_file}: {exc}"
            raise OSError(msg) from exc
        try:
            tree = ast.parse(source, filename=str(py_file))
        except SyntaxError as exc:
            msg = f"{py_file}: syntax error: {exc}"
            raise SyntaxError(msg) from exc
        required.update(extract_required_env(tree, py_file))
    return sorted(required)


def dump_pipeline_required_env(pipeline_root: Path) -> dict[str, list[str]]:
    """Return ``{service: [env_var, ...]}`` for every pipeline service with required vars."""
    if not pipeline_root.is_dir():
        msg = f"pipeline root does not exist or is not a directory: {pipeline_root}"
        raise FileNotFoundError(msg)

    result: dict[str, list[str]] = {}
    for child in sorted(pipeline_root.iterdir()):
        if not child.is_dir():
            continue
        if child.name.startswith((".", "_")):
            continue
        required = scan_service_dir(child)
        if required:
            result[child.name] = required
    return result


def _default_pipeline_root() -> Path:
    return Path(__file__).resolve().parent.parent / "backend" / "pipeline"


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--pipeline-root",
        type=Path,
        default=_default_pipeline_root(),
        help="Path to backend/pipeline directory (default: resolved from script location).",
    )
    args = parser.parse_args()

    try:
        result = dump_pipeline_required_env(args.pipeline_root)
    except (TypeError, ValueError, SyntaxError, FileNotFoundError, OSError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    json.dump(result, sys.stdout, indent=2, sort_keys=True)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
