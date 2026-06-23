"""Tests for shared HTTP failure classification."""

from __future__ import annotations

import unittest

from backend.pipeline.ingestion.failure_classifiers import (
    http_status,
)
from backend.pipeline.storage import feed_store


def _require_status_reason(value: feed_store.FeedStatusReason | None):
    """Return a status reason for tests that intentionally expect one."""
    if value is None:
        msg = "Expected FeedStatusReason, got None"
        raise AssertionError(msg)
    return value


class TestHTTPStatusClassifier(unittest.TestCase):
    """Shared HTTP status policy behavior."""

    def test_success_status_returns_none(self) -> None:
        self.assertIsNone(http_status.classify_http_status(200))
        self.assertIsNone(http_status.classify_http_status(302))

    def test_auth_statuses_map_to_authentication_failed(self) -> None:
        for status in (401, 403):
            with self.subTest(status=status):
                status_reason = _require_status_reason(
                    http_status.classify_http_status(status)
                )

                self.assertIs(
                    status_reason,
                    feed_store.FeedStatusReason.SYSTEM_AUTHENTICATION_FAILED,
                )

    def test_rate_limit_maps_to_rate_limited(self) -> None:
        status_reason = _require_status_reason(
            http_status.classify_http_status(429)
        )

        self.assertIs(
            status_reason,
            feed_store.FeedStatusReason.SOURCE_RATE_LIMITED,
        )

    def test_default_transient_statuses_map_to_source_unreachable(
        self,
    ) -> None:
        for status in (408, 500, 502, 503, 504, 599):
            with self.subTest(status=status):
                status_reason = _require_status_reason(
                    http_status.classify_http_status(status)
                )

                self.assertIs(
                    status_reason,
                    feed_store.FeedStatusReason.SOURCE_UNREACHABLE,
                )

    def test_retryable_statuses_are_shared_policy(self) -> None:
        for status in (408, 429, 500, 502, 503, 504, 599):
            with self.subTest(status=status):
                self.assertTrue(http_status.is_retryable_http_status(status))

        for status in (200, 302, 400, 401, 403, 404, 600):
            with self.subTest(status=status):
                self.assertFalse(http_status.is_retryable_http_status(status))

    def test_ambiguous_and_nonstandard_statuses_return_none(self) -> None:
        for status in (400, 404, 409, 410, 423, 425, 426, 600, 799, 999):
            with self.subTest(status=status):
                self.assertIsNone(http_status.classify_http_status(status))

    def test_exact_override_beats_family_default(self) -> None:
        policy = http_status.HTTPStatusPolicy(
            exact={404: feed_store.FeedStatusReason.SOURCE_OFFLINE},
            default_4xx=feed_store.FeedStatusReason.SOURCE_UNREACHABLE,
            default_5xx=feed_store.FeedStatusReason.SOURCE_UNREACHABLE,
            default_other_failure=(
                feed_store.FeedStatusReason.SOURCE_UNREACHABLE
            ),
        )

        status_reason = _require_status_reason(
            http_status.classify_http_status(
                404,
                policy=policy,
            )
        )

        self.assertIs(
            status_reason,
            feed_store.FeedStatusReason.SOURCE_OFFLINE,
        )

    def test_fire_notifications_poll_policy_maps_4xx_to_source_configuration_invalid(
        self,
    ) -> None:
        policy = http_status.HTTPStatusPolicy(
            exact={
                401: (feed_store.FeedStatusReason.SYSTEM_AUTHENTICATION_FAILED),
                403: (feed_store.FeedStatusReason.SYSTEM_AUTHENTICATION_FAILED),
                429: feed_store.FeedStatusReason.SOURCE_RATE_LIMITED,
            },
            default_4xx=(
                feed_store.FeedStatusReason.SYSTEM_SOURCE_CONFIGURATION_INVALID
            ),
            default_5xx=feed_store.FeedStatusReason.SOURCE_UNREACHABLE,
            default_other_failure=(
                feed_store.FeedStatusReason.SOURCE_UNREACHABLE
            ),
        )

        for status in (400, 404):
            with self.subTest(status=status):
                status_reason = _require_status_reason(
                    http_status.classify_http_status(
                        status,
                        policy=policy,
                    )
                )
                self.assertIs(
                    status_reason,
                    feed_store.FeedStatusReason.SYSTEM_SOURCE_CONFIGURATION_INVALID,
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
                status_reason = _require_status_reason(
                    http_status.classify_http_status(
                        status,
                        policy=policy,
                    )
                )
                self.assertIs(status_reason, reason)


if __name__ == "__main__":
    unittest.main()
