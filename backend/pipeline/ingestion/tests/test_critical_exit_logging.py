from __future__ import annotations

from pathlib import Path

_SOURCE = Path("backend/pipeline/ingestion/collector_runtime.py").read_text(
    encoding="utf-8"
)


def _assert_logger_critical_before_exit(phrase: str) -> None:
    phrase_pos = _SOURCE.index(phrase)
    critical_pos = _SOURCE.rfind("logger.critical", 0, phrase_pos)
    exit_pos = _SOURCE.find("os._exit(1)", phrase_pos)

    assert critical_pos != -1, phrase
    assert exit_pos != -1, phrase
    assert critical_pos < phrase_pos < exit_pos, phrase


def test_event_loop_stall_self_exit_logs_critical() -> None:
    _assert_logger_critical_before_exit(
        "Event loop stall -- heartbeat did not complete"
    )


def test_bookmark_fence_self_exit_logs_critical() -> None:
    _assert_logger_critical_before_exit("Fence violation on bookmark")


def test_source_observation_fence_self_exit_logs_critical() -> None:
    _assert_logger_critical_before_exit("Fence violation on source observation")


def test_heartbeat_fence_self_exit_logs_critical() -> None:
    _assert_logger_critical_before_exit("Heartbeat fence violation")


def test_hard_exit_paths_remain_queryable_by_critical_severity() -> None:
    assert "logger.critical" in _SOURCE
    assert "os._exit(1)" in _SOURCE
