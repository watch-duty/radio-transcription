"""Tests for shared collector failure classification helpers."""

from __future__ import annotations

import unittest

from backend.pipeline.ingestion.collectors.failure_classification import (
    FailureClassification,
    ItemBatchOutcome,
    ItemFailure,
    collector_failure,
    missing_source_feed_id_failure,
)
from backend.pipeline.ingestion.models import FeedFailure
from backend.pipeline.storage.feed_store import FeedStatusReason


def _require_item_failure(value: ItemFailure | None) -> ItemFailure:
    """Return a typed item failure for tests that intentionally expect one."""
    if value is None:
        msg = "Expected ItemFailure, got None"
        raise AssertionError(msg)
    return value


class TestItemBatchOutcome(unittest.TestCase):
    """Shared item-failure promotion rules."""

    def test_failure_classification_preserves_fields(self) -> None:
        classification = FailureClassification(
            FeedStatusReason.SOURCE_UNREACHABLE,
            "download_failed",
        )

        self.assertIs(
            classification.status_reason,
            FeedStatusReason.SOURCE_UNREACHABLE,
        )
        self.assertEqual(classification.reason, "download_failed")

    def test_item_failure_from_classification_copies_fields(self) -> None:
        classification = FailureClassification(
            FeedStatusReason.SOURCE_UNREACHABLE,
            "download_failed",
        )

        failure = ItemFailure.from_classification(classification)

        self.assertIs(
            failure.status_reason,
            FeedStatusReason.SOURCE_UNREACHABLE,
        )
        self.assertEqual(failure.reason, "download_failed")

    def test_item_failure_preserves_status_reason_and_reason(self) -> None:
        failure = ItemFailure(
            FeedStatusReason.SOURCE_UNREACHABLE,
            "download_failed",
        )

        self.assertIs(
            failure.status_reason,
            FeedStatusReason.SOURCE_UNREACHABLE,
        )
        self.assertEqual(failure.reason, "download_failed")

    def test_no_attempted_items_returns_none(self) -> None:
        outcome = ItemBatchOutcome()

        self.assertIsNone(outcome.promoted_failure())

    def test_any_success_returns_none(self) -> None:
        outcome = ItemBatchOutcome()
        failure = ItemFailure(
            FeedStatusReason.SOURCE_UNREACHABLE,
            "download_failed",
        )
        outcome.record_attempt()
        outcome.record_failure(failure)
        outcome.record_chunk_produced()

        self.assertIsNone(outcome.promoted_failure())

    def test_missing_classified_failure_returns_none(self) -> None:
        outcome = ItemBatchOutcome()
        failure = ItemFailure(
            FeedStatusReason.SOURCE_UNREACHABLE,
            "download_failed",
        )
        outcome.record_attempt()
        outcome.record_failure(failure)
        outcome.record_attempt()

        self.assertIsNone(outcome.promoted_failure())

    def test_same_reason_all_failed_returns_that_failure(self) -> None:
        outcome = ItemBatchOutcome()
        failures = [
            ItemFailure(
                FeedStatusReason.SOURCE_UNREACHABLE,
                "download_failed",
            ),
            ItemFailure(
                FeedStatusReason.SOURCE_UNREACHABLE,
                "download_failed",
            ),
        ]
        for failure in failures:
            outcome.record_attempt()
            outcome.record_failure(failure)

        result = outcome.promoted_failure()

        result = _require_item_failure(result)
        self.assertIs(
            result.status_reason,
            FeedStatusReason.SOURCE_UNREACHABLE,
        )
        self.assertEqual(result.reason, "download_failed")

    def test_mixed_reason_all_failed_returns_collector_error(self) -> None:
        outcome = ItemBatchOutcome()
        failures = [
            ItemFailure(
                FeedStatusReason.SOURCE_UNREACHABLE,
                "download_failed",
            ),
            ItemFailure(
                FeedStatusReason.SYSTEM_CONFIGURATION_INVALID,
                "bad_url",
            ),
        ]
        for failure in failures:
            outcome.record_attempt()
            outcome.record_failure(failure)

        result = outcome.promoted_failure()

        result = _require_item_failure(result)
        self.assertIs(
            result.status_reason,
            FeedStatusReason.SYSTEM_COLLECTOR_ERROR,
        )
        self.assertEqual(result.reason, "mixed_item_failures")

    def test_collector_failure_helper_returns_typed_exception(self) -> None:
        result = collector_failure(
            FeedStatusReason.SOURCE_UNREACHABLE,
            "source_unreachable",
        )

        self.assertIsInstance(result, FeedFailure)
        self.assertIs(
            result.status_reason,
            FeedStatusReason.SOURCE_UNREACHABLE,
        )
        self.assertEqual(str(result), "source_unreachable")

    def test_missing_source_feed_id_failure(self) -> None:
        result = missing_source_feed_id_failure()

        self.assertIsInstance(result, FeedFailure)
        self.assertIs(
            result.status_reason,
            FeedStatusReason.SYSTEM_CONFIGURATION_INVALID,
        )
        self.assertEqual(str(result), "missing_source_feed_id")


if __name__ == "__main__":
    unittest.main()
