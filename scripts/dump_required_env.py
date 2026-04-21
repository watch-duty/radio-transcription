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

Note for maintainers: adding a new service under ``backend/pipeline/<name>/``
means this script will automatically pick it up, but Gate 1a in the
deployment repo won't actually check it until ``<name>`` is added to
``SERVICE_TO_RESOURCE_ADDRESS`` in
``radio-transcription-deployment/scripts/verify_required_env.py`` (the
mapping from service → ``google_compute_instance_template`` resource
address). Until then, the verifier emits a ``[WARN]`` and skips the service.
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
        # Accept either positional (_require_env("X")) or keyword
        # (_require_env(name="X")) call forms. The signature is
        # `_require_env(name: str)`, so kwarg usage is valid Python and
        # callers do write it occasionally.
        if node.args:
            name_node: ast.AST = node.args[0]
        else:
            name_kw = next((k for k in node.keywords if k.arg == "name"), None)
            if name_kw is None:
                msg = (
                    f"{source_path}:{node.lineno}: _require_env() called with "
                    "no positional argument and no `name=...` keyword"
                )
                raise ValueError(msg)
            name_node = name_kw.value
        if not (
            isinstance(name_node, ast.Constant)
            and isinstance(name_node.value, str)
        ):
            msg = (
                f"{source_path}:{node.lineno}: _require_env() name argument "
                f"must be a string literal (got {ast.dump(name_node)}). "
                "Non-literals hide the contract from static analysis."
            )
            raise TypeError(msg)
        found.append(name_node.value)
    return found


def _is_test_file(py_file: Path, service_dir: Path) -> bool:
    """Return True if ``py_file`` looks like a test file.

    A test that mocks ``_require_env("FAKE_VAR")`` would otherwise contaminate
    the runtime contract with the mock var name.
    """
    rel_parts = py_file.relative_to(service_dir).parts
    if any(part in {"tests", "test"} for part in rel_parts):
        return True
    name = py_file.name
    return name.startswith("test_") or name.endswith("_test.py")


def scan_service_dir(service_dir: Path) -> list[str]:
    """Return the sorted, de-duplicated set of required env vars in ``service_dir``."""
    required: set[str] = set()
    for py_file in sorted(service_dir.rglob("*.py")):
        if _is_test_file(py_file, service_dir):
            continue
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
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
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
    except (
        TypeError,
        ValueError,
        SyntaxError,
        FileNotFoundError,
        OSError,
    ) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    json.dump(result, sys.stdout, indent=2, sort_keys=True)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
