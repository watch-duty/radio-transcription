"""Tests for status-reason-detail storage boundaries."""

from __future__ import annotations

from backend.pipeline.ingestion import status_reason_detail


def test_cap_status_reason_detail_for_storage_adds_marker() -> None:
    result = status_reason_detail.cap_status_reason_detail_for_storage(
        "x" * (status_reason_detail.MAX_STATUS_REASON_DETAIL_LENGTH + 1)
    )

    assert len(result) == status_reason_detail.MAX_STATUS_REASON_DETAIL_LENGTH
    assert result.endswith("[truncated]")


def test_exception_text_includes_class_and_collapses_message() -> None:
    result = status_reason_detail.exception_text(
        ValueError("line one\n line two")
    )

    assert result == "ValueError: line one line two"


def test_exception_text_uses_class_name_when_message_is_empty() -> None:
    result = status_reason_detail.exception_text(TimeoutError())

    assert result == "TimeoutError"
