"""Gate ingestion MIG rollouts using Cloud Logging settle telemetry."""

from __future__ import annotations

import argparse
import datetime
import json
import subprocess
import sys
import time
from collections.abc import Mapping, Sequence
from pathlib import Path

_EXPECTED_WORKERS = (1, 2)
_SNIPPET_MAX_CHARS = 500
_ADMISSION_PREDICATE = 'jsonPayload.event_type="lease_admission_cycle"'
_CRITICAL_PREDICATE = "severity>=CRITICAL"


def _as_int(value: object) -> int | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def is_worker_settled(payload: Mapping[str, object]) -> bool:
    if payload.get("error") not in (None, ""):
        return False
    if payload.get("memory_paused") is not False:
        return False

    slack = _as_int(payload.get("slack"))
    admission_budget = _as_int(payload.get("admission_budget"))
    primary_acquired = _as_int(payload.get("primary_acquired"))
    recovery_acquired = _as_int(payload.get("recovery_acquired", 0))

    numeric_fields = (
        slack,
        admission_budget,
        primary_acquired,
        recovery_acquired,
    )
    if any(value is None for value in numeric_fields):
        return False

    acquired = int(primary_acquired) + int(recovery_acquired)
    return int(slack) <= int(admission_budget) or acquired < int(
        admission_budget
    )


def build_log_filter(
    *,
    instance_id: str,
    since: str,
    predicate: str,
) -> str:
    return "\n".join(
        [
            'resource.type="gce_instance"',
            f'resource.labels.instance_id="{instance_id}"',
            f'timestamp >= "{since}"',
            predicate,
        ]
    )


def _mapping_or_empty(value: object) -> Mapping[str, object]:
    if isinstance(value, Mapping):
        return value
    return {}


def _payload_summary(entry: Mapping[str, object]) -> str:
    text_payload = entry.get("textPayload")
    if text_payload:
        return str(text_payload)[:_SNIPPET_MAX_CHARS]

    json_payload = _mapping_or_empty(entry.get("jsonPayload"))
    json_message = json_payload.get("message")
    if json_message:
        return str(json_message)[:_SNIPPET_MAX_CHARS]

    proto_payload = _mapping_or_empty(entry.get("protoPayload"))
    status = _mapping_or_empty(proto_payload.get("status"))
    proto_message = status.get("message")
    if proto_message:
        return str(proto_message)[:_SNIPPET_MAX_CHARS]

    return ""


def normalize_log_entry(entry: Mapping[str, object]) -> dict[str, object]:
    json_payload = _mapping_or_empty(entry.get("jsonPayload"))
    normalized = {
        "timestamp": str(entry.get("timestamp") or ""),
        "severity": str(entry.get("severity") or ""),
        "log_name": str(entry.get("logName") or ""),
        "payload": dict(json_payload),
    }
    summary = _payload_summary(entry)
    if summary:
        normalized["summary"] = summary
    return normalized


def _critical_log_snippet(entry: Mapping[str, object]) -> dict[str, object]:
    normalized = normalize_log_entry(entry)
    return {
        "timestamp": normalized["timestamp"],
        "severity": normalized["severity"],
        "log_name": normalized["log_name"],
        "summary": str(normalized.get("summary") or ""),
    }


def _latest_admission_by_worker(
    entries: Sequence[Mapping[str, object]],
) -> dict[int, dict[str, object]]:
    latest: dict[int, dict[str, object]] = {}
    for entry in entries:
        normalized = normalize_log_entry(entry)
        payload = normalized["payload"]
        if not isinstance(payload, dict):
            continue
        if payload.get("event_type") != "lease_admission_cycle":
            continue
        worker_index = _as_int(payload.get("worker_index"))
        if worker_index is None:
            continue
        telemetry = dict(payload)
        telemetry["timestamp"] = normalized["timestamp"]
        current = latest.get(worker_index)
        if current is None or str(telemetry["timestamp"]) >= str(
            current.get("timestamp", "")
        ):
            latest[worker_index] = telemetry
    return latest


def evaluate_gate(
    *,
    admission_entries: Sequence[Mapping[str, object]],
    critical_entries: Sequence[Mapping[str, object]],
    expected_workers: Sequence[int] = _EXPECTED_WORKERS,
    now: str | None = None,
) -> dict[str, object]:
    latest = _latest_admission_by_worker(admission_entries)
    workers: dict[str, dict[str, object]] = {}
    missing_workers: list[int] = []
    unsettled_workers: list[int] = []

    for worker_index in expected_workers:
        telemetry = latest.get(worker_index)
        seen = telemetry is not None
        settled = seen and is_worker_settled(telemetry)
        if not seen:
            missing_workers.append(worker_index)
        elif not settled:
            unsettled_workers.append(worker_index)
        workers[str(worker_index)] = {
            "seen": seen,
            "settled": settled,
            "latest": telemetry or {},
        }

    critical_logs = [_critical_log_snippet(entry) for entry in critical_entries]
    status = "passed"
    reason = "settled"
    if critical_logs:
        status = "failed"
        reason = "critical_log"
    elif missing_workers or unsettled_workers:
        status = "failed"
        reason = "timeout"

    return {
        "status": status,
        "reason": reason,
        "evaluated_at": now or _utc_now(),
        "expected_workers": list(expected_workers),
        "workers": workers,
        "missing_workers": missing_workers,
        "unsettled_workers": unsettled_workers,
        "critical_logs": critical_logs,
    }


def _utc_now() -> str:
    return (
        datetime.datetime.now(datetime.UTC).isoformat().replace("+00:00", "Z")
    )


def _validate_instance_id(instance_id: str) -> None:
    if instance_id and instance_id.isdigit():
        return
    msg = "--instance-id must be a non-empty numeric GCE instance id"
    raise ValueError(msg)


def _validate_since(since: str) -> None:
    if not since.endswith("Z"):
        msg = "--since must be a UTC RFC3339 timestamp ending in Z"
        raise ValueError(msg)
    try:
        datetime.datetime.fromisoformat(f"{since[:-1]}+00:00")
    except ValueError as exc:
        msg = "--since must be parseable as an RFC3339 timestamp"
        raise ValueError(msg) from exc


def _read_logs(
    *,
    project: str,
    filter_text: str,
    limit: int,
) -> list[dict[str, object]]:
    result = subprocess.run(
        [
            "gcloud",
            "logging",
            "read",
            filter_text,
            "--project",
            project,
            "--limit",
            str(limit),
            "--format",
            "json",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        stderr = result.stderr.strip()
        msg = f"gcloud logging read failed with exit {result.returncode}: {stderr}"
        raise RuntimeError(msg)

    if not result.stdout.strip():
        return []
    decoded = json.loads(result.stdout)
    if not isinstance(decoded, list):
        msg = "gcloud logging read returned JSON that was not a list"
        raise TypeError(msg)
    return [item for item in decoded if isinstance(item, dict)]


def _with_context(
    result: Mapping[str, object],
    *,
    project: str,
    instance_id: str,
    since: str,
    elapsed_sec: float,
) -> dict[str, object]:
    enriched = dict(result)
    enriched["project"] = project
    enriched["instance_id"] = instance_id
    enriched["since"] = since
    enriched["elapsed_sec"] = round(elapsed_sec, 3)
    return enriched


def _write_json_result(path: str | None, result: Mapping[str, object]) -> None:
    if not path:
        return
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(result, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _print_human_summary(result: Mapping[str, object]) -> None:
    status = str(result.get("status", "unknown")).upper()
    reason = result.get("reason", "unknown")
    elapsed = result.get("elapsed_sec", 0.0)
    sys.stdout.write(
        f"Ingestion rollout gate {status}: {reason} ({elapsed}s)\n"
    )

    critical_logs = result.get("critical_logs")
    if isinstance(critical_logs, list) and critical_logs:
        sys.stdout.write("Critical logs:\n")
        for entry in critical_logs[:5]:
            if not isinstance(entry, Mapping):
                continue
            timestamp = entry.get("timestamp", "")
            severity = entry.get("severity", "")
            summary = entry.get("summary", "")
            sys.stdout.write(f"- {timestamp} {severity}: {summary}\n")

    workers = result.get("workers")
    if isinstance(workers, Mapping):
        sys.stdout.write("Workers:\n")
        for worker_index, state in sorted(workers.items()):
            if not isinstance(state, Mapping):
                continue
            seen = state.get("seen")
            settled = state.get("settled")
            latest = state.get("latest")
            sys.stdout.write(
                f"- worker {worker_index}: seen={seen} settled={settled} "
                f"latest={latest}\n"
            )


def _parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Gate ingestion MIG rollout progress on worker settle logs.",
    )
    parser.add_argument("--project", required=True)
    parser.add_argument("--instance-id", required=True)
    parser.add_argument("--since", required=True)
    parser.add_argument("--timeout-sec", type=float, default=900.0)
    parser.add_argument("--poll-interval-sec", type=float, default=30.0)
    parser.add_argument("--output-json")
    parser.add_argument("--limit", type=int, default=200)
    return parser.parse_args(argv)


def _failure_result(
    *,
    reason: str,
    project: str,
    instance_id: str,
    since: str,
    elapsed_sec: float,
    message: str,
) -> dict[str, object]:
    return {
        "status": "failed",
        "reason": reason,
        "evaluated_at": _utc_now(),
        "project": project,
        "instance_id": instance_id,
        "since": since,
        "elapsed_sec": round(elapsed_sec, 3),
        "expected_workers": list(_EXPECTED_WORKERS),
        "workers": {
            str(worker_index): {"seen": False, "settled": False, "latest": {}}
            for worker_index in _EXPECTED_WORKERS
        },
        "missing_workers": list(_EXPECTED_WORKERS),
        "unsettled_workers": [],
        "critical_logs": [],
        "message": message,
    }


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    started = time.monotonic()

    try:
        _validate_instance_id(args.instance_id)
        _validate_since(args.since)
    except ValueError as exc:
        result = _failure_result(
            reason="invalid_arguments",
            project=args.project,
            instance_id=args.instance_id,
            since=args.since,
            elapsed_sec=0.0,
            message=str(exc),
        )
        _write_json_result(args.output_json, result)
        sys.stderr.write(f"{exc}\n")
        return 2

    critical_filter = build_log_filter(
        instance_id=args.instance_id,
        since=args.since,
        predicate=_CRITICAL_PREDICATE,
    )
    admission_filter = build_log_filter(
        instance_id=args.instance_id,
        since=args.since,
        predicate=_ADMISSION_PREDICATE,
    )

    while True:
        elapsed_sec = time.monotonic() - started
        try:
            critical_entries = _read_logs(
                project=args.project,
                filter_text=critical_filter,
                limit=args.limit,
            )
            if critical_entries:
                result = evaluate_gate(
                    admission_entries=[],
                    critical_entries=critical_entries,
                )
                result = _with_context(
                    result,
                    project=args.project,
                    instance_id=args.instance_id,
                    since=args.since,
                    elapsed_sec=elapsed_sec,
                )
                _write_json_result(args.output_json, result)
                _print_human_summary(result)
                return 1

            admission_entries = _read_logs(
                project=args.project,
                filter_text=admission_filter,
                limit=args.limit,
            )
        except (RuntimeError, TypeError, json.JSONDecodeError) as exc:
            result = _failure_result(
                reason="log_query_error",
                project=args.project,
                instance_id=args.instance_id,
                since=args.since,
                elapsed_sec=elapsed_sec,
                message=str(exc),
            )
            _write_json_result(args.output_json, result)
            sys.stderr.write(f"{exc}\n")
            return 1

        result = evaluate_gate(
            admission_entries=admission_entries,
            critical_entries=[],
        )
        result = _with_context(
            result,
            project=args.project,
            instance_id=args.instance_id,
            since=args.since,
            elapsed_sec=elapsed_sec,
        )
        if result["status"] == "passed":
            _write_json_result(args.output_json, result)
            _print_human_summary(result)
            return 0

        if elapsed_sec >= args.timeout_sec:
            _write_json_result(args.output_json, result)
            _print_human_summary(result)
            return 1

        remaining_sec = args.timeout_sec - elapsed_sec
        sleep_sec = min(args.poll_interval_sec, remaining_sec)
        if sleep_sec > 0:
            time.sleep(sleep_sec)


if __name__ == "__main__":
    sys.exit(main())
