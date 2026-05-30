"""Tests for shared collector failure classification helpers."""

from __future__ import annotations

import unittest

from backend.pipeline.ingestion.collectors.failure_classification import (
    ItemFailure,
    aggregate_item_failures,
    collector_failure,
    missing_source_feed_id_failure,
)
from backend.pipeline.ingestion.models import CollectorFailure
from backend.pipeline.storage.feed_store import FeedStatusReason


class TestItemFailureAggregation(unittest.TestCase):
    """Shared item-failure promotion rules."""

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
        self.assertIsNone(
            aggregate_item_failures(
                [],
                attempted_count=0,
                succeeded_count=0,
            ),
        )

    def test_any_success_returns_none(self) -> None:
        failure = ItemFailure(
            FeedStatusReason.SOURCE_UNREACHABLE,
            "download_failed",
        )

        self.assertIsNone(
            aggregate_item_failures(
                [failure],
                attempted_count=1,
                succeeded_count=1,
            ),
        )

    def test_missing_classified_failure_returns_none(self) -> None:
        failure = ItemFailure(
            FeedStatusReason.SOURCE_UNREACHABLE,
            "download_failed",
        )

        self.assertIsNone(
            aggregate_item_failures(
                [failure],
                attempted_count=2,
                succeeded_count=0,
            ),
        )

    def test_same_reason_all_failed_returns_that_failure(self) -> None:
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

        result = aggregate_item_failures(
            failures,
            attempted_count=2,
            succeeded_count=0,
        )

        self.assertIsNotNone(result)
        self.assertIs(
            result.status_reason,
            FeedStatusReason.SOURCE_UNREACHABLE,
        )
        self.assertEqual(result.reason, "download_failed")

    def test_mixed_reason_all_failed_returns_collector_error(self) -> None:
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

        result = aggregate_item_failures(
            failures,
            attempted_count=2,
            succeeded_count=0,
        )

        self.assertIsNotNone(result)
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

        self.assertIsInstance(result, CollectorFailure)
        self.assertIs(
            result.status_reason,
            FeedStatusReason.SOURCE_UNREACHABLE,
        )
        self.assertEqual(str(result), "source_unreachable")

    def test_missing_source_feed_id_failure(self) -> None:
        result = missing_source_feed_id_failure()

        self.assertIsInstance(result, CollectorFailure)
        self.assertIs(
            result.status_reason,
            FeedStatusReason.SYSTEM_CONFIGURATION_INVALID,
        )
        self.assertEqual(str(result), "missing_source_feed_id")


if __name__ == "__main__":
    unittest.main()
