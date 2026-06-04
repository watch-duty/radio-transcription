"""Tests for shared collector failure classification helpers."""

from __future__ import annotations

import unittest

from backend.pipeline.ingestion.collectors.failure_classification import (
    ItemBatchOutcome,
    ItemDownloadResult,
    ItemFailure,
    collector_failure,
    item_download_http_failure,
    missing_source_feed_id_failure,
    raise_item_failure,
    standardize_item_download_result,
)
from backend.pipeline.ingestion.models import CollectorFailure
from backend.pipeline.storage.feed_store import FeedStatusReason


def _require_item_failure(value: ItemFailure | None) -> ItemFailure:
    """Return a typed item failure for tests that intentionally expect one."""
    if value is None:
        msg = "Expected ItemFailure, got None"
        raise AssertionError(msg)
    return value


class TestItemBatchOutcome(unittest.TestCase):
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


class TestItemDownloadResult(unittest.TestCase):
    def test_accepts_success_result_with_content_type(self) -> None:
        result = ItemDownloadResult(
            audio_bytes=b"audio",
            content_type="audio/mpeg; charset=binary",
        )

        self.assertEqual(result.audio_bytes, b"audio")
        self.assertIsNone(result.failure)
        self.assertEqual(result.content_type, "audio/mpeg; charset=binary")

    def test_accepts_failure_result(self) -> None:
        failure = ItemFailure(
            FeedStatusReason.SOURCE_UNREACHABLE,
            "item_download_failed",
        )

        result = ItemDownloadResult(failure=failure)

        self.assertIsNone(result.audio_bytes)
        self.assertIs(result.failure, failure)
        self.assertIsNone(result.content_type)

    def test_accepts_empty_result_for_shutdown_compatibility(self) -> None:
        result = ItemDownloadResult()

        self.assertIsNone(result.audio_bytes)
        self.assertIsNone(result.failure)
        self.assertIsNone(result.content_type)

    def test_rejects_success_and_failure_together(self) -> None:
        failure = ItemFailure(
            FeedStatusReason.SOURCE_UNREACHABLE,
            "item_download_failed",
        )

        with self.assertRaises(ValueError) as ctx:
            ItemDownloadResult(audio_bytes=b"audio", failure=failure)

        self.assertEqual(
            str(ctx.exception),
            "ItemDownloadResult cannot contain both audio_bytes and failure",
        )

    def test_standardize_item_download_result_preserves_typed_result(
        self,
    ) -> None:
        result = ItemDownloadResult(audio_bytes=b"audio")

        self.assertIs(standardize_item_download_result(result), result)

    def test_standardize_item_download_result_wraps_bytes(self) -> None:
        result = standardize_item_download_result(b"audio")

        self.assertEqual(result.audio_bytes, b"audio")
        self.assertIsNone(result.failure)

    def test_standardize_item_download_result_wraps_none(self) -> None:
        result = standardize_item_download_result(None)

        self.assertIsNone(result.audio_bytes)
        self.assertIsNone(result.failure)


class TestItemDownloadHttpFailure(unittest.TestCase):
    def test_auth_statuses_are_system_authentication_failed(self) -> None:
        for status in (401, 403):
            with self.subTest(status=status):
                failure = item_download_http_failure(status)

                self.assertIs(
                    failure.status_reason,
                    FeedStatusReason.SYSTEM_AUTHENTICATION_FAILED,
                )
                self.assertEqual(failure.reason, f"item_http_{status}")

    def test_rate_limit_status_is_source_rate_limited(self) -> None:
        failure = item_download_http_failure(429)

        self.assertIs(
            failure.status_reason,
            FeedStatusReason.SOURCE_RATE_LIMITED,
        )
        self.assertEqual(failure.reason, "item_http_429")

    def test_other_statuses_are_source_unreachable_with_exact_status(
        self,
    ) -> None:
        for status in (404, 410, 500, 503):
            with self.subTest(status=status):
                failure = item_download_http_failure(status)

                self.assertIs(
                    failure.status_reason,
                    FeedStatusReason.SOURCE_UNREACHABLE,
                )
                self.assertEqual(failure.reason, f"item_http_{status}")

    def test_custom_reason_prefix_is_supported_for_compatibility(self) -> None:
        failure = item_download_http_failure(404, reason_prefix="custom_http")

        self.assertIs(
            failure.status_reason,
            FeedStatusReason.SOURCE_UNREACHABLE,
        )
        self.assertEqual(failure.reason, "custom_http_404")

    def test_raise_item_failure_preserves_status_and_reason(self) -> None:
        failure = ItemFailure(
            FeedStatusReason.SOURCE_UNREACHABLE,
            "item_http_503",
        )

        with self.assertRaises(CollectorFailure) as ctx:
            raise_item_failure(failure)

        self.assertIs(
            ctx.exception.status_reason,
            FeedStatusReason.SOURCE_UNREACHABLE,
        )
        self.assertEqual(str(ctx.exception), "item_http_503")


if __name__ == "__main__":
    unittest.main()
