"""Tests for shared feed lifecycle storage helpers."""

from __future__ import annotations

import typing

import pytest

from backend.pipeline.storage import feed_lifecycle, quarantine_reason
from backend.pipeline.storage.feed_store import FeedStatus, FeedStatusReason


def test_default_failure_budget_constants_match_existing_behavior() -> None:
    assert feed_lifecycle.DEFAULT_FAILURE_THRESHOLD == 5
    assert feed_lifecycle.DEFAULT_BACKOFF_BASE_SEC == 15
    assert feed_lifecycle.DEFAULT_BACKOFF_MAX_SEC == 600


def test_status_reason_storage_value_accepts_enum_or_none() -> None:
    assert feed_lifecycle.status_reason_storage_value(None) is None
    assert (
        feed_lifecycle.status_reason_storage_value(
            FeedStatusReason.SYSTEM_PIPELINE_ERROR,
        )
        == "system_pipeline_error"
    )


def test_status_reason_storage_value_rejects_raw_string() -> None:
    with pytest.raises(TypeError):
        feed_lifecycle.status_reason_storage_value(
            typing.cast("typing.Any", "system_pipeline_error"),
        )


def test_status_reason_storage_value_rejects_other_enums() -> None:
    with pytest.raises(TypeError):
        feed_lifecycle.status_reason_storage_value(
            typing.cast("typing.Any", FeedStatus.ACTIVE),
        )


def test_status_reason_detail_storage_value_caps_reason() -> None:
    long_reason = "x" * (quarantine_reason.MAX_QUARANTINE_REASON_LENGTH + 1)

    result = feed_lifecycle.status_reason_detail_storage_value(long_reason)

    assert result is not None
    assert len(result) == quarantine_reason.MAX_QUARANTINE_REASON_LENGTH
    assert result.endswith("[truncated]")
