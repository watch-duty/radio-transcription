#!/usr/bin/env python3
"""Fail closed when project ty diagnostics exceed the reviewed baseline."""

from __future__ import annotations

import collections
import json
import pathlib
import subprocess
import sys
import typing

BASELINE_COMMIT = "55df18dd"
TY_VERSION = "0.0.42"
DIAGNOSTIC_COUNT = 332
REPOSITORY_ROOT = pathlib.Path(__file__).resolve().parents[1]
BASELINE_PATH = pathlib.Path(__file__).with_name("ty_baseline.json")

type DiagnosticIdentity = tuple[str, str, str]
type DiagnosticCounter = collections.Counter[DiagnosticIdentity]


class BaselineCheckError(RuntimeError):
    """The reviewed baseline or the current ty result is not trustworthy."""


def _require_string(value: object, field: str) -> str:
    if not isinstance(value, str) or not value:
        message = f"{field} must be a nonempty string"
        raise BaselineCheckError(message)
    return value


def _normalize_path(value: object, *, field: str) -> str:
    raw_path = _require_string(value, field).replace("\\", "/")
    path = pathlib.PurePosixPath(raw_path)
    if path.is_absolute() or ".." in path.parts or raw_path in {"", "."}:
        message = f"{field} must be repository-relative"
        raise BaselineCheckError(message)
    return path.as_posix()


def _identity_from_baseline(
    value: object, index: int
) -> tuple[
    DiagnosticIdentity,
    int,
]:
    field = f"diagnostics[{index}]"
    if not isinstance(value, dict):
        message = f"{field} must be an object"
        raise BaselineCheckError(message)
    record = typing.cast("dict[str, object]", value)
    expected_keys = {"path", "check_name", "description", "count"}
    if set(record) != expected_keys:
        message = f"{field} must contain exactly {sorted(expected_keys)}"
        raise BaselineCheckError(message)
    path = _normalize_path(record["path"], field=f"{field}.path")
    if path != record["path"]:
        message = f"{field}.path must already be normalized"
        raise BaselineCheckError(message)
    check_name = _require_string(
        record["check_name"],
        f"{field}.check_name",
    )
    description = _require_string(
        record["description"],
        f"{field}.description",
    )
    count = record["count"]
    if isinstance(count, bool) or not isinstance(count, int) or count <= 0:
        message = f"{field}.count must be a positive integer"
        raise BaselineCheckError(message)
    return (path, check_name, description), count


def _load_baseline(path: pathlib.Path) -> DiagnosticCounter:
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        message = f"cannot read a valid baseline from {path}"
        raise BaselineCheckError(message) from exc
    if not isinstance(document, dict):
        message = "baseline root must be an object"
        raise BaselineCheckError(message)
    baseline_document = typing.cast("dict[str, object]", document)
    expected_keys = {
        "baseline_commit",
        "ty_version",
        "diagnostic_count",
        "diagnostics",
    }
    if set(baseline_document) != expected_keys:
        message = f"baseline must contain exactly {sorted(expected_keys)}"
        raise BaselineCheckError(message)
    if baseline_document["baseline_commit"] != BASELINE_COMMIT:
        message = f"baseline_commit must be exactly {BASELINE_COMMIT}"
        raise BaselineCheckError(message)
    if baseline_document["ty_version"] != TY_VERSION:
        message = f"ty_version must be exactly {TY_VERSION}"
        raise BaselineCheckError(message)
    diagnostic_count = baseline_document["diagnostic_count"]
    if (
        isinstance(diagnostic_count, bool)
        or not isinstance(diagnostic_count, int)
        or diagnostic_count != DIAGNOSTIC_COUNT
    ):
        message = f"diagnostic_count must be exactly {DIAGNOSTIC_COUNT}"
        raise BaselineCheckError(message)
    diagnostics = baseline_document["diagnostics"]
    if not isinstance(diagnostics, list) or not diagnostics:
        message = "diagnostics must be a nonempty list"
        raise BaselineCheckError(message)

    baseline: DiagnosticCounter = collections.Counter()
    previous: DiagnosticIdentity | None = None
    for index, value in enumerate(diagnostics):
        identity, count = _identity_from_baseline(value, index)
        if previous is not None and identity <= previous:
            message = "baseline diagnostics must be strictly sorted and unique"
            raise BaselineCheckError(message)
        baseline[identity] = count
        previous = identity
    if baseline.total() != DIAGNOSTIC_COUNT:
        message = (
            "baseline diagnostic multiplicity totals "
            f"{baseline.total()}, expected {DIAGNOSTIC_COUNT}"
        )
        raise BaselineCheckError(message)
    return baseline


def _run(command: tuple[str, ...]) -> subprocess.CompletedProcess[str]:
    try:
        result = subprocess.run(
            command,
            cwd=REPOSITORY_ROOT,
            check=False,
            capture_output=True,
            text=True,
        )
    except OSError as exc:
        message = f"could not execute {' '.join(command)}"
        raise BaselineCheckError(message) from exc
    if result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip() or "no output"
        message = f"{' '.join(command)} exited {result.returncode}: {detail}"
        raise BaselineCheckError(message)
    return result


def _check_ty_version() -> None:
    result = _run(("uv", "run", "ty", "--version"))
    if result.stdout.strip() != f"ty {TY_VERSION}":
        message = (
            f"running ty version must be exactly {TY_VERSION}; "
            f"received {result.stdout.strip()!r}"
        )
        raise BaselineCheckError(message)


def _current_diagnostics() -> DiagnosticCounter:
    result = _run(
        (
            "uv",
            "run",
            "ty",
            "check",
            "--output-format",
            "gitlab",
            "--exit-zero",
        )
    )
    try:
        diagnostics = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        message = "ty GitLab output is not valid JSON"
        raise BaselineCheckError(message) from exc
    if not isinstance(diagnostics, list):
        message = "ty GitLab output must be a JSON list"
        raise BaselineCheckError(message)

    current: DiagnosticCounter = collections.Counter()
    for index, diagnostic in enumerate(diagnostics):
        field = f"current diagnostics[{index}]"
        if not isinstance(diagnostic, dict):
            message = f"{field} must be an object"
            raise BaselineCheckError(message)
        diagnostic_record = typing.cast("dict[str, object]", diagnostic)
        location = diagnostic_record.get("location")
        if not isinstance(location, dict):
            message = f"{field}.location must be an object"
            raise BaselineCheckError(message)
        location_record = typing.cast("dict[str, object]", location)
        identity = (
            _normalize_path(
                location_record.get("path"),
                field=f"{field}.path",
            ),
            _require_string(
                diagnostic_record.get("check_name"),
                f"{field}.check_name",
            ),
            _require_string(
                diagnostic_record.get("description"),
                f"{field}.description",
            ),
        )
        current[identity] += 1
    return current


def check_ty_baseline(
    baseline_path: pathlib.Path = BASELINE_PATH,
) -> tuple[int, int]:
    """Validate the reviewed baseline and reject diagnostic regressions."""
    baseline = _load_baseline(baseline_path)
    _check_ty_version()
    current = _current_diagnostics()
    regressions = current - baseline
    if regressions:
        lines = ["new or increased ty diagnostics:"]
        for (path, check_name, description), count in sorted(
            regressions.items()
        ):
            lines.append(f"  {count} x {path}: {check_name}: {description}")
        raise BaselineCheckError("\n".join(lines))
    return current.total(), baseline.total()


def main() -> int:
    try:
        current_count, baseline_count = check_ty_baseline()
    except BaselineCheckError as exc:
        sys.stderr.write(f"ty baseline check failed: {exc}\n")
        return 1
    sys.stdout.write(
        "ty baseline check passed: "
        f"{current_count} current diagnostics <= {baseline_count} reviewed\n"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
