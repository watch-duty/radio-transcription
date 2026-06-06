"""Tests for shared HTTP failure classification."""

from __future__ import annotations

import unittest

from backend.pipeline.ingestion.collectors.failure_classifiers import (
    http_status,
)
from backend.pipeline.storage import feed_store


def _require_classification(
    value,
):
    """Return a classification for tests that intentionally expect one."""
    if value is None:
        msg = "Expected FailureClassification, got None"
        raise AssertionError(msg)
    return value


class TestHTTPStatusClassifier(unittest.TestCase):
    """Shared HTTP status policy behavior."""

    def test_success_status_returns_none(self) -> None:
        self.assertIsNone(
            http_status.classify_http_status(200, reason_prefix="item_http")
        )
        self.assertIsNone(
            http_status.classify_http_status(302, reason_prefix="item_http")
        )

    def test_auth_statuses_map_to_authentication_failed(self) -> None:
        for status in (401, 403):
            with self.subTest(status=status):
                classification = _require_classification(
                    http_status.classify_http_status(
                        status,
                        reason_prefix="item_http",
                    )
                )

                self.assertIs(
                    classification.status_reason,
                    feed_store.FeedStatusReason.SYSTEM_AUTHENTICATION_FAILED,
                )
                self.assertEqual(
                    classification.reason,
                    f"item_http_{status}",
                )

    def test_rate_limit_maps_to_rate_limited(self) -> None:
        classification = _require_classification(
            http_status.classify_http_status(
                429,
                reason_prefix="item_http",
            )
        )

        self.assertIs(
            classification.status_reason,
            feed_store.FeedStatusReason.SOURCE_RATE_LIMITED,
        )
        self.assertEqual(classification.reason, "item_http_429")

    def test_default_transient_statuses_map_to_source_unreachable(
        self,
    ) -> None:
        for status in (408, 500, 503):
            with self.subTest(status=status):
                classification = _require_classification(
                    http_status.classify_http_status(
                        status,
                        reason_prefix="item_http",
                    )
                )

                self.assertIs(
                    classification.status_reason,
                    feed_store.FeedStatusReason.SOURCE_UNREACHABLE,
                )
                self.assertEqual(
                    classification.reason,
                    f"item_http_{status}",
                )

    def test_default_ambiguous_statuses_map_to_collector_error(
        self,
    ) -> None:
        for status in (400, 404, 409, 410, 799):
            with self.subTest(status=status):
                classification = _require_classification(
                    http_status.classify_http_status(
                        status,
                        reason_prefix="item_http",
                    )
                )

                self.assertIs(
                    classification.status_reason,
                    feed_store.FeedStatusReason.SYSTEM_COLLECTOR_ERROR,
                )
                self.assertEqual(
                    classification.reason,
                    f"item_http_{status}",
                )

    def test_exact_override_beats_family_default(self) -> None:
        policy = http_status.HTTPStatusPolicy(
            exact={404: feed_store.FeedStatusReason.SOURCE_OFFLINE},
            default_4xx=feed_store.FeedStatusReason.SOURCE_UNREACHABLE,
            default_5xx=feed_store.FeedStatusReason.SOURCE_UNREACHABLE,
            default_other_failure=(
                feed_store.FeedStatusReason.SOURCE_UNREACHABLE
            ),
        )

        classification = _require_classification(
            http_status.classify_http_status(
                404,
                reason_prefix="stream_http",
                policy=policy,
            )
        )

        self.assertIs(
            classification.status_reason,
            feed_store.FeedStatusReason.SOURCE_OFFLINE,
        )
        self.assertEqual(classification.reason, "stream_http_404")

    def test_fire_notifications_poll_policy_maps_4xx_to_configuration_invalid(
        self,
    ) -> None:
        policy = http_status.HTTPStatusPolicy(
            exact={
                401: (feed_store.FeedStatusReason.SYSTEM_AUTHENTICATION_FAILED),
                403: (feed_store.FeedStatusReason.SYSTEM_AUTHENTICATION_FAILED),
                429: feed_store.FeedStatusReason.SOURCE_RATE_LIMITED,
            },
            default_4xx=(
                feed_store.FeedStatusReason.SYSTEM_CONFIGURATION_INVALID
            ),
            default_5xx=feed_store.FeedStatusReason.SOURCE_UNREACHABLE,
            default_other_failure=(
                feed_store.FeedStatusReason.SOURCE_UNREACHABLE
            ),
        )

        for status in (400, 404):
            with self.subTest(status=status):
                classification = _require_classification(
                    http_status.classify_http_status(
                        status,
                        reason_prefix="fn_api_http",
                        policy=policy,
                    )
                )
                self.assertIs(
                    classification.status_reason,
                    feed_store.FeedStatusReason.SYSTEM_CONFIGURATION_INVALID,
                )

        for status, reason in (
            (
                401,
                feed_store.FeedStatusReason.SYSTEM_AUTHENTICATION_FAILED,
            ),
            (
                403,
                feed_store.FeedStatusReason.SYSTEM_AUTHENTICATION_FAILED,
            ),
            (429, feed_store.FeedStatusReason.SOURCE_RATE_LIMITED),
        ):
            with self.subTest(status=status):
                classification = _require_classification(
                    http_status.classify_http_status(
                        status,
                        reason_prefix="fn_api_http",
                        policy=policy,
                    )
                )
                self.assertIs(classification.status_reason, reason)


if __name__ == "__main__":
    unittest.main()
