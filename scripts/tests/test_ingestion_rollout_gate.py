from __future__ import annotations

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


def _critical_entry(timestamp: str = "2026-07-04T20:00:30Z") -> dict[str, object]:
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


def test_worker_is_unsettled_for_errors_pauses_pressure_or_missing_fields() -> None:
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
    assert not gate.is_worker_settled({k: v for k, v in base.items() if k != "slack"})


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
