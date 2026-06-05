"""Tests for shared ffmpeg failure classification."""

from __future__ import annotations

import unittest

from backend.pipeline.ingestion.collectors.failure_classifiers import (
    ffmpeg,
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


class TestFfmpegClassifier(unittest.TestCase):
    """Shared ffmpeg terminal evidence behavior."""

    def test_extracts_stable_http_status_patterns(self) -> None:
        cases = {
            "HTTP error 404 Not Found": 404,
            "Server returned 503 Service Unavailable": 503,
            "HTTP/1.1 429 Too Many Requests": 429,
        }
        for stderr, status in cases.items():
            with self.subTest(stderr=stderr):
                self.assertEqual(
                    ffmpeg.extract_http_status_from_ffmpeg_stderr(stderr),
                    status,
                )

    def test_unmatched_stderr_returns_none(self) -> None:
        self.assertIsNone(
            ffmpeg.extract_http_status_from_ffmpeg_stderr("Input/output error")
        )

    def test_stderr_http_status_maps_first(self) -> None:
        classification = _require_classification(
            ffmpeg.classify_ffmpeg_failure(
                exit_code=1,
                stderr_text="HTTP error 404 Not Found",
                probe_http_status=200,
            )
        )

        self.assertIs(
            classification.status_reason,
            feed_store.FeedStatusReason.SOURCE_UNREACHABLE,
        )
        self.assertEqual(classification.reason, "stream_http_404")

    def test_stderr_skips_success_status_before_terminal_status(self) -> None:
        classification = _require_classification(
            ffmpeg.classify_ffmpeg_failure(
                exit_code=1,
                stderr_text=(
                    "HTTP/1.1 200 OK\n"
                    "retrying stream\n"
                    "Server returned 503 Service Unavailable"
                ),
                probe_http_status=200,
            )
        )

        self.assertIs(
            classification.status_reason,
            feed_store.FeedStatusReason.SOURCE_UNREACHABLE,
        )
        self.assertEqual(classification.reason, "stream_http_503")

    def test_probe_status_maps_without_stderr_status(self) -> None:
        policy = http_status.HTTPStatusPolicy(
            exact={404: feed_store.FeedStatusReason.SOURCE_OFFLINE},
        )

        classification = _require_classification(
            ffmpeg.classify_ffmpeg_failure(
                exit_code=1,
                stderr_text="Input/output error",
                probe_http_status=404,
                http_policy=policy,
            )
        )

        self.assertIs(
            classification.status_reason,
            feed_store.FeedStatusReason.SOURCE_OFFLINE,
        )
        self.assertEqual(classification.reason, "stream_http_404")

    def test_timeout_maps_to_capture_timeout(self) -> None:
        classification = _require_classification(
            ffmpeg.classify_ffmpeg_failure(exit_code=None, timed_out=True)
        )

        self.assertIs(
            classification.status_reason,
            feed_store.FeedStatusReason.SYSTEM_COLLECTOR_ERROR,
        )
        self.assertEqual(classification.reason, "capture_timeout")

    def test_positive_exit_maps_to_ffmpeg_exit_reason(self) -> None:
        classification = _require_classification(
            ffmpeg.classify_ffmpeg_failure(exit_code=8)
        )

        self.assertIs(
            classification.status_reason,
            feed_store.FeedStatusReason.SYSTEM_COLLECTOR_ERROR,
        )
        self.assertEqual(classification.reason, "ffmpeg_exit_8")

    def test_signal_exit_maps_to_ffmpeg_signal_reason(self) -> None:
        classification = _require_classification(
            ffmpeg.classify_ffmpeg_failure(exit_code=-9)
        )

        self.assertIs(
            classification.status_reason,
            feed_store.FeedStatusReason.SYSTEM_COLLECTOR_ERROR,
        )
        self.assertEqual(classification.reason, "ffmpeg_signal_9")

    def test_zero_exit_without_timeout_returns_none(self) -> None:
        self.assertIsNone(ffmpeg.classify_ffmpeg_failure(exit_code=0))
        self.assertIsNone(ffmpeg.classify_ffmpeg_failure(exit_code=None))


if __name__ == "__main__":
    unittest.main()
