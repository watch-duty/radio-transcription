"""Tests for shared ffmpeg failure classification."""

from __future__ import annotations

import subprocess
import unittest

from backend.pipeline.ingestion.failure_classifiers import (
    ffmpeg,
    http_status,
)
from backend.pipeline.storage import feed_store


def _require_classification(
    value,
):
    """Return ffmpeg info for tests that intentionally expect one."""
    if value is None:
        msg = "Expected FfmpegFailureInfo, got None"
        raise AssertionError(msg)
    return value


def _require_ffprobe_classification(
    value,
):
    """Return ffprobe info for tests that intentionally expect one."""
    if value is None:
        msg = "Expected ffprobe failure info, got None"
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
        policy = http_status.HTTPStatusPolicy(
            exact={404: feed_store.FeedStatusReason.SOURCE_OFFLINE},
        )
        classification = _require_classification(
            ffmpeg.classify_ffmpeg_failure(
                exit_code=1,
                stderr_text="HTTP error 404 Not Found",
                probe_http_status=200,
                http_policy=policy,
            )
        )

        self.assertIs(
            classification.status_reason,
            feed_store.FeedStatusReason.SOURCE_OFFLINE,
        )
        self.assertIs(
            classification.kind,
            ffmpeg.FfmpegFailureKind.HTTP_STATUS,
        )
        self.assertIs(
            classification.source,
            ffmpeg.FfmpegEvidenceSource.STDERR,
        )
        self.assertEqual(classification.http_status, 404)

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
        self.assertIs(
            classification.kind,
            ffmpeg.FfmpegFailureKind.HTTP_STATUS,
        )
        self.assertEqual(classification.http_status, 503)

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
        self.assertIs(
            classification.kind,
            ffmpeg.FfmpegFailureKind.HTTP_STATUS,
        )
        self.assertIs(
            classification.source,
            ffmpeg.FfmpegEvidenceSource.PROBE,
        )
        self.assertEqual(classification.http_status, 404)

    def test_timeout_maps_to_timeout_info(self) -> None:
        classification = _require_classification(
            ffmpeg.classify_ffmpeg_failure(exit_code=None, timed_out=True)
        )

        self.assertIs(
            classification.status_reason,
            feed_store.FeedStatusReason.SYSTEM_COLLECTOR_ERROR,
        )
        self.assertIs(classification.kind, ffmpeg.FfmpegFailureKind.TIMEOUT)

    def test_positive_exit_maps_to_process_exit_info(self) -> None:
        classification = _require_classification(
            ffmpeg.classify_ffmpeg_failure(exit_code=8)
        )

        self.assertIs(
            classification.status_reason,
            feed_store.FeedStatusReason.SYSTEM_COLLECTOR_ERROR,
        )
        self.assertIs(
            classification.kind,
            ffmpeg.FfmpegFailureKind.PROCESS_EXIT,
        )
        self.assertEqual(classification.exit_code, 8)

    def test_signal_exit_maps_to_process_signal_info(self) -> None:
        classification = _require_classification(
            ffmpeg.classify_ffmpeg_failure(exit_code=-9)
        )

        self.assertIs(
            classification.status_reason,
            feed_store.FeedStatusReason.SYSTEM_COLLECTOR_ERROR,
        )
        self.assertIs(
            classification.kind,
            ffmpeg.FfmpegFailureKind.PROCESS_SIGNAL,
        )
        self.assertEqual(classification.signal_number, 9)

    def test_zero_exit_without_timeout_returns_none(self) -> None:
        self.assertIsNone(ffmpeg.classify_ffmpeg_failure(exit_code=0))
        self.assertIsNone(ffmpeg.classify_ffmpeg_failure(exit_code=None))

    def test_renders_http_status_with_useful_stderr_text(self) -> None:
        info = ffmpeg.FfmpegFailureInfo(
            status_reason=feed_store.FeedStatusReason.SOURCE_UNREACHABLE,
            kind=ffmpeg.FfmpegFailureKind.HTTP_STATUS,
            source=ffmpeg.FfmpegEvidenceSource.STDERR,
            http_status=503,
            exit_code=1,
        )

        self.assertEqual(
            ffmpeg.render_ffmpeg_diagnostic(
                info,
                "Server returned 503 Service Unavailable",
            ),
            "Server returned 503 Service Unavailable",
        )

    def test_renders_process_exit_with_stderr_tail(self) -> None:
        info = ffmpeg.FfmpegFailureInfo(
            status_reason=feed_store.FeedStatusReason.SYSTEM_COLLECTOR_ERROR,
            kind=ffmpeg.FfmpegFailureKind.PROCESS_EXIT,
            source=ffmpeg.FfmpegEvidenceSource.PROCESS,
            exit_code=8,
        )

        self.assertEqual(
            ffmpeg.render_ffmpeg_diagnostic(info), "ffmpeg exited with code 8"
        )
        self.assertEqual(
            ffmpeg.render_ffmpeg_diagnostic(info, "Input/output error"),
            "ffmpeg exited with code 8; Input/output error",
        )

    def test_renders_process_signal_and_timeout(self) -> None:
        signal_info = ffmpeg.FfmpegFailureInfo(
            status_reason=feed_store.FeedStatusReason.SYSTEM_COLLECTOR_ERROR,
            kind=ffmpeg.FfmpegFailureKind.PROCESS_SIGNAL,
            source=ffmpeg.FfmpegEvidenceSource.PROCESS,
            exit_code=-9,
            signal_number=9,
        )
        timeout_info = ffmpeg.FfmpegFailureInfo(
            status_reason=feed_store.FeedStatusReason.SYSTEM_COLLECTOR_ERROR,
            kind=ffmpeg.FfmpegFailureKind.TIMEOUT,
            source=ffmpeg.FfmpegEvidenceSource.PROCESS,
        )

        self.assertEqual(
            ffmpeg.render_ffmpeg_diagnostic(signal_info, "Killed"),
            "ffmpeg terminated by signal 9; Killed",
        )
        self.assertEqual(
            ffmpeg.render_ffmpeg_diagnostic(timeout_info),
            "Stream capture timed out",
        )

    def test_classifies_ffprobe_process_exit_exception(self) -> None:
        exc = subprocess.CalledProcessError(
            1,
            ["ffprobe"],
            stderr=b"first line\nInvalid data found when processing input\n",
        )

        info = _require_ffprobe_classification(
            ffmpeg.classify_ffprobe_exception(exc)
        )

        self.assertIs(
            info.status_reason,
            feed_store.FeedStatusReason.SYSTEM_COLLECTOR_ERROR,
        )
        self.assertIs(info.kind, ffmpeg.FfmpegFailureKind.PROCESS_EXIT)
        self.assertEqual(info.exit_code, 1)
        self.assertEqual(
            ffmpeg.render_ffprobe_diagnostic(
                info,
                ffmpeg.ffprobe_exception_diagnostic_text(exc),
            ),
            (
                "ffprobe exited with code 1; "
                "first line\nInvalid data found when processing input"
            ),
        )

    def test_ffprobe_process_exit_without_stderr_omits_sentinel(self) -> None:
        exc = subprocess.CalledProcessError(1, ["ffprobe"], stderr=b"")
        info = _require_ffprobe_classification(
            ffmpeg.classify_ffprobe_exception(exc)
        )

        self.assertIsNone(ffmpeg.ffprobe_exception_diagnostic_text(exc))
        self.assertEqual(
            ffmpeg.render_ffprobe_diagnostic(
                info,
                ffmpeg.ffprobe_exception_diagnostic_text(exc),
            ),
            "ffprobe exited with code 1",
        )

    def test_ffprobe_signal_exception_renders_signal(self) -> None:
        exc = subprocess.CalledProcessError(
            -9,
            ["ffprobe"],
            stderr=b"Killed\n",
        )
        info = _require_ffprobe_classification(
            ffmpeg.classify_ffprobe_exception(exc)
        )

        self.assertIs(info.kind, ffmpeg.FfmpegFailureKind.PROCESS_SIGNAL)
        self.assertEqual(info.signal_number, 9)
        self.assertEqual(
            ffmpeg.render_ffprobe_diagnostic(
                info,
                ffmpeg.ffprobe_exception_diagnostic_text(exc),
            ),
            "ffprobe terminated by signal 9; Killed",
        )

    def test_ffprobe_timeout_exception_renders_timeout(self) -> None:
        exc = subprocess.TimeoutExpired(
            ["ffprobe"],
            timeout=10,
            stderr=b"probe still running\n",
        )
        info = _require_ffprobe_classification(
            ffmpeg.classify_ffprobe_exception(exc)
        )

        self.assertIs(info.kind, ffmpeg.FfmpegFailureKind.TIMEOUT)
        self.assertEqual(
            ffmpeg.render_ffprobe_diagnostic(
                info,
                ffmpeg.ffprobe_exception_diagnostic_text(exc),
            ),
            "ffprobe timed out after 10s; probe still running",
        )

    def test_ffprobe_non_subprocess_exception_is_not_classified(self) -> None:
        self.assertIsNone(
            ffmpeg.classify_ffprobe_exception(ValueError("bad duration"))
        )

    def test_ffprobe_exception_failure_reason_falls_back_for_generic_exception(
        self,
    ) -> None:
        self.assertEqual(
            ffmpeg.ffprobe_exception_failure_reason(ValueError("bad duration")),
            "ValueError: bad duration",
        )

    def test_ffprobe_exception_failure_reason_renders_subprocess_evidence(
        self,
    ) -> None:
        exc = subprocess.CalledProcessError(
            1,
            ["ffprobe"],
            stderr=b"Invalid data found when processing input\n",
        )

        self.assertEqual(
            ffmpeg.ffprobe_exception_failure_reason(exc),
            (
                "ffprobe exited with code 1; "
                "Invalid data found when processing input"
            ),
        )

    def test_ffprobe_stderr_diagnostic_uses_last_thirty_lines(self) -> None:
        stderr = "".join(f"line {index}\n" for index in range(35)).encode()
        exc = subprocess.CalledProcessError(1, ["ffprobe"], stderr=stderr)

        diagnostic = ffmpeg.ffprobe_exception_diagnostic_text(exc)
        if diagnostic is None:
            self.fail("Expected ffprobe stderr diagnostic text")

        self.assertNotIn("line 4", diagnostic)
        self.assertIn("line 5", diagnostic)
        self.assertIn("line 34", diagnostic)


if __name__ == "__main__":
    unittest.main()
