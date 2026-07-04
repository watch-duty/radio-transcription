from __future__ import annotations

import json
import subprocess

from scripts import ingestion_rollout_gate as gate


def _admission_entry(
    *,
    timestamp: str,
    worker_index: int,
    slack: int,
    admission_budget: int,
    primary_acquired: int,
    recovery_acquired: int = 0,
    memory_paused: bool = False,
    error: str | None = None,
    event_type: str = "lease_admission_cycle",
) -> dict[str, object]:
    return {
        "timestamp": timestamp,
        "severity": "INFO",
        "jsonPayload": {
            "event_type": event_type,
            "worker_index": worker_index,
            "slack": slack,
            "admission_budget": admission_budget,
            "primary_acquired": primary_acquired,
            "recovery_acquired": recovery_acquired,
            "memory_paused": memory_paused,
            "error": error,
            "hostname": "container-hostname-telemetry-only",
        },
    }


def _critical_entry(
    timestamp: str = "2026-07-04T20:00:30Z",
) -> dict[str, object]:
    return {
        "timestamp": timestamp,
        "severity": "CRITICAL",
        "logName": "projects/prod/logs/python",
        "textPayload": "Event loop stall -- heartbeat did not complete",
    }


def test_worker_is_settled_when_slack_is_within_admission_budget() -> None:
    payload = {
        "error": None,
        "memory_paused": False,
        "slack": 10,
        "admission_budget": 20,
        "primary_acquired": 20,
        "recovery_acquired": 0,
    }

    assert gate.is_worker_settled(payload)


def test_worker_is_settled_when_underfilled() -> None:
    payload = {
        "error": "",
        "memory_paused": False,
        "slack": 40,
        "admission_budget": 20,
        "primary_acquired": 8,
        "recovery_acquired": 3,
    }

    assert gate.is_worker_settled(payload)


def test_worker_is_unsettled_for_errors_pauses_pressure_or_missing_fields() -> (
    None
):
    base = {
        "error": None,
        "memory_paused": False,
        "slack": 21,
        "admission_budget": 20,
        "primary_acquired": 20,
        "recovery_acquired": 0,
    }

    assert not gate.is_worker_settled({**base, "error": "db timeout"})
    assert not gate.is_worker_settled({**base, "memory_paused": True})
    assert not gate.is_worker_settled(base)
    assert not gate.is_worker_settled(
        {k: v for k, v in base.items() if k != "slack"}
    )


def test_evaluate_gate_passes_using_latest_cycle_per_worker() -> None:
    entries = [
        _admission_entry(
            timestamp="2026-07-04T20:00:00Z",
            worker_index=1,
            slack=50,
            admission_budget=20,
            primary_acquired=20,
        ),
        _admission_entry(
            timestamp="2026-07-04T20:00:10Z",
            worker_index=1,
            slack=5,
            admission_budget=20,
            primary_acquired=20,
        ),
        _admission_entry(
            timestamp="2026-07-04T20:00:05Z",
            worker_index=2,
            slack=30,
            admission_budget=20,
            primary_acquired=5,
            recovery_acquired=4,
        ),
    ]

    result = gate.evaluate_gate(admission_entries=entries, critical_entries=[])

    assert result["status"] == "passed"
    assert result["reason"] == "settled"
    assert result["workers"]["1"]["settled"] is True
    assert result["workers"]["1"]["latest"]["slack"] == 5
    assert result["workers"]["2"]["settled"] is True


def test_evaluate_gate_fails_fast_when_critical_entries_exist() -> None:
    entries = [
        _admission_entry(
            timestamp="2026-07-04T20:00:10Z",
            worker_index=1,
            slack=5,
            admission_budget=20,
            primary_acquired=20,
        ),
        _admission_entry(
            timestamp="2026-07-04T20:00:12Z",
            worker_index=2,
            slack=5,
            admission_budget=20,
            primary_acquired=20,
        ),
    ]

    result = gate.evaluate_gate(
        admission_entries=entries,
        critical_entries=[_critical_entry()],
    )

    assert result["status"] == "failed"
    assert result["reason"] == "critical_log"
    assert result["critical_logs"][0]["severity"] == "CRITICAL"
    assert "Event loop stall" in result["critical_logs"][0]["summary"]


def test_evaluate_gate_reports_missing_and_unsettled_workers() -> None:
    entries = [
        _admission_entry(
            timestamp="2026-07-04T20:00:10Z",
            worker_index=1,
            slack=50,
            admission_budget=20,
            primary_acquired=20,
        ),
    ]

    result = gate.evaluate_gate(admission_entries=entries, critical_entries=[])

    assert result["status"] == "failed"
    assert result["reason"] == "timeout"
    assert result["workers"]["1"]["seen"] is True
    assert result["workers"]["1"]["settled"] is False
    assert result["workers"]["2"]["seen"] is False
    assert result["unsettled_workers"] == [1]
    assert result["missing_workers"] == [2]


def test_evaluate_gate_ignores_unrelated_event_types() -> None:
    entries = [
        _admission_entry(
            timestamp="2026-07-04T20:00:10Z",
            worker_index=1,
            slack=5,
            admission_budget=20,
            primary_acquired=20,
            event_type="other_event",
        ),
        _admission_entry(
            timestamp="2026-07-04T20:00:12Z",
            worker_index=2,
            slack=5,
            admission_budget=20,
            primary_acquired=20,
        ),
    ]

    result = gate.evaluate_gate(admission_entries=entries, critical_entries=[])

    assert result["status"] == "failed"
    assert result["workers"]["1"]["seen"] is False
    assert result["workers"]["2"]["settled"] is True


def test_build_log_filter_scopes_to_fresh_instance_and_timestamp() -> None:
    filter_text = gate.build_log_filter(
        instance_id="1234567890",
        since="2026-07-04T20:00:00Z",
        predicate='jsonPayload.event_type="lease_admission_cycle"',
    )

    assert 'resource.type="gce_instance"' in filter_text
    assert 'resource.labels.instance_id="1234567890"' in filter_text
    assert 'timestamp >= "2026-07-04T20:00:00Z"' in filter_text
    assert 'jsonPayload.event_type="lease_admission_cycle"' in filter_text
    assert "hostname" not in filter_text


def test_critical_filter_contract_uses_general_severity() -> None:
    filter_text = gate.build_log_filter(
        instance_id="1234567890",
        since="2026-07-04T20:00:00Z",
        predicate="severity>=CRITICAL",
    )

    assert "severity>=CRITICAL" in filter_text
    assert "severity >= CRITICAL" not in filter_text


def test_read_logs_uses_gcloud_logging_read_as_json(monkeypatch) -> None:
    calls = []

    def fake_run(args, *, capture_output, text, check):
        calls.append(
            {
                "args": args,
                "capture_output": capture_output,
                "text": text,
                "check": check,
            }
        )
        return subprocess.CompletedProcess(
            args=args,
            returncode=0,
            stdout=json.dumps([_critical_entry()]),
            stderr="",
        )

    monkeypatch.setattr(gate.subprocess, "run", fake_run)

    entries = gate._read_logs(
        project="prod-project",
        filter_text="severity>=CRITICAL",
        limit=7,
    )

    assert entries[0]["severity"] == "CRITICAL"
    assert calls == [
        {
            "args": [
                "gcloud",
                "logging",
                "read",
                "severity>=CRITICAL",
                "--project",
                "prod-project",
                "--limit",
                "7",
                "--format",
                "json",
            ],
            "capture_output": True,
            "text": True,
            "check": False,
        }
    ]


def test_main_writes_json_artifact_when_workers_settle(
    tmp_path, monkeypatch
) -> None:
    def fake_read_logs(*, project, filter_text, limit):
        assert project == "prod-project"
        assert limit == 200
        if "severity>=CRITICAL" in filter_text:
            return []
        return [
            _admission_entry(
                timestamp="2026-07-04T20:00:10Z",
                worker_index=1,
                slack=1,
                admission_budget=20,
                primary_acquired=20,
            ),
            _admission_entry(
                timestamp="2026-07-04T20:00:12Z",
                worker_index=2,
                slack=30,
                admission_budget=20,
                primary_acquired=4,
            ),
        ]

    output_json = tmp_path / "gate" / "result.json"
    monkeypatch.setattr(gate, "_read_logs", fake_read_logs)

    exit_code = gate.main(
        [
            "--project",
            "prod-project",
            "--instance-id",
            "1234567890",
            "--since",
            "2026-07-04T20:00:00Z",
            "--timeout-sec",
            "0",
            "--poll-interval-sec",
            "0",
            "--output-json",
            str(output_json),
        ]
    )

    result = json.loads(output_json.read_text())
    assert exit_code == 0
    assert result["status"] == "passed"
    assert result["reason"] == "settled"
    assert result["project"] == "prod-project"
    assert result["instance_id"] == "1234567890"
    assert result["since"] == "2026-07-04T20:00:00Z"
    assert result["expected_workers"] == [1, 2]
