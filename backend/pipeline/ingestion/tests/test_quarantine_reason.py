"""Tests for quarantine-reason storage boundaries."""

from __future__ import annotations

from backend.pipeline.ingestion import quarantine_reason


def test_quarantine_reason_module_does_not_sanitize_or_redact() -> None:
    assert not hasattr(quarantine_reason, "sanitize_quarantine_reason")


def test_cap_quarantine_reason_for_storage_adds_marker() -> None:
    result = quarantine_reason.cap_quarantine_reason_for_storage(
        "x" * (quarantine_reason.MAX_QUARANTINE_REASON_LENGTH + 1)
    )

    assert len(result) == quarantine_reason.MAX_QUARANTINE_REASON_LENGTH
    assert result.endswith("[truncated]")


def test_exception_text_includes_class_and_collapses_message() -> None:
    result = quarantine_reason.exception_text(ValueError("line one\n line two"))

    assert result == "ValueError: line one line two"


def test_exception_text_uses_class_name_when_message_is_empty() -> None:
    result = quarantine_reason.exception_text(TimeoutError())

    assert result == "TimeoutError"
