"""Cross-collector regression locks for item failure semantics."""

from __future__ import annotations

import asyncio
import logging
import pathlib
import unittest
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock, patch

from backend.pipeline.ingestion.collectors import (
    item_downloads,
    payloads,
    telemetry,
)
from backend.pipeline.ingestion.collectors.bcfy_calls import (
    bcfy_calls_collector,
)
from backend.pipeline.ingestion.collectors.failure_classification import (
    ItemBatchOutcome,
    ItemFailure,
)
from backend.pipeline.ingestion.collectors.fire_notifications.client import (
    FireNotificationsRestClient,
)
from backend.pipeline.ingestion.collectors.openmhz import (
    collector as openmhz_collector,
)
from backend.pipeline.ingestion.models import FeedFailure
from backend.pipeline.storage.feed_store import FeedStatusReason


class _CaptureHandler(logging.Handler):
    def __init__(self) -> None:
        super().__init__()
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


class TestCrossCollectorShutdownSemantics(unittest.IsolatedAsyncioTestCase):
    @patch(
        "backend.pipeline.ingestion.collectors.bcfy_calls"
        ".bcfy_calls_collector.control_flow.sleep_or_cancel",
        new_callable=AsyncMock,
    )
    async def test_bcfy_calls_retry_shutdown_raises_cancelled_error(
        self,
        mock_sleep: AsyncMock,
    ) -> None:
        mock_sleep.side_effect = asyncio.CancelledError
        response = AsyncMock(status=503)
        context_manager = MagicMock()
        context_manager.__aenter__ = AsyncMock(return_value=response)
        context_manager.__aexit__ = AsyncMock(return_value=False)
        session = MagicMock()
        session.get.return_value = context_manager

        with self.assertRaises(asyncio.CancelledError):
            await bcfy_calls_collector._download_audio(
                session,
                "https://audio.example/test.mp3",
                asyncio.Event(),
            )

        session.get.assert_called_once()
        mock_sleep.assert_awaited_once()

    @patch(
        "backend.pipeline.ingestion.collectors.fire_notifications.client.control_flow.sleep_or_cancel",
        new_callable=AsyncMock,
    )
    async def test_fire_notifications_retry_shutdown_raises_cancelled_error(
        self,
        mock_sleep: AsyncMock,
    ) -> None:
        mock_sleep.side_effect = asyncio.CancelledError
        session = MagicMock()
        session.get = AsyncMock(return_value=MagicMock(status_code=503))
        client = FireNotificationsRestClient(
            session=session,
            url_base="http://mock-api",
            s3_base_url="http://mock-s3-bucket",
            user="test-user",
            password="test-password",
        )

        with self.assertRaises(asyncio.CancelledError):
            await client.download_audio(
                "test.mp3",
                asyncio.Event(),
            )

        session.get.assert_awaited_once()
        mock_sleep.assert_awaited_once()

    @patch(
        "backend.pipeline.ingestion.collectors.openmhz"
        ".collector.control_flow.sleep_or_cancel",
        new_callable=AsyncMock,
    )
    async def test_openmhz_retry_shutdown_raises_cancelled_error(
        self,
        mock_sleep: AsyncMock,
    ) -> None:
        mock_sleep.side_effect = asyncio.CancelledError
        session = MagicMock()
        session.get = AsyncMock(return_value=MagicMock(status_code=503))

        with self.assertRaises(asyncio.CancelledError):
            await openmhz_collector._download_m4a(
                session,
                "https://media2.openmhz.com/test.m4a",
                asyncio.Event(),
            )

        session.get.assert_awaited_once()
        mock_sleep.assert_awaited_once()


class TestCrossCollectorItemFailureSemantics(unittest.TestCase):
    def test_terminal_item_http_statuses_use_shared_item_scope(self) -> None:
        cases = {
            404: (
                FeedStatusReason.SYSTEM_COLLECTOR_ERROR,
                "item_http_404",
            ),
            429: (FeedStatusReason.SOURCE_RATE_LIMITED, "item_http_429"),
            503: (FeedStatusReason.SOURCE_UNREACHABLE, "item_http_503"),
        }

        for status, (status_reason, reason) in cases.items():
            with self.subTest(status=status):
                failure = item_downloads.item_http_failure(status)

                self.assertIs(failure.status_reason, status_reason)
                self.assertEqual(failure.reason, reason)

    def test_retry_exhaustion_without_http_evidence_is_shared_fallback(
        self,
    ) -> None:
        failure = item_downloads.item_download_failed()

        self.assertIs(
            failure.status_reason,
            FeedStatusReason.SOURCE_UNREACHABLE,
        )
        self.assertEqual(failure.reason, "item_download_failed")

    def test_retry_exhaustion_preserves_exception_text(self) -> None:
        failure = item_downloads.item_download_failed(
            TimeoutError("socket down")
        )

        self.assertIs(
            failure.status_reason,
            FeedStatusReason.SOURCE_UNREACHABLE,
        )
        self.assertEqual(
            failure.reason,
            "item_download_failed: TimeoutError: socket down",
        )

    def test_partial_success_suppresses_item_batch_promotion(self) -> None:
        outcome = ItemBatchOutcome()
        outcome.record_attempt()
        outcome.record_failure(
            ItemFailure(
                FeedStatusReason.SOURCE_UNREACHABLE,
                "item_http_503",
            )
        )
        outcome.record_chunk_produced()

        self.assertIsNone(outcome.promoted_failure())

    def test_all_failures_with_same_reason_promote_first_failure(
        self,
    ) -> None:
        outcome = ItemBatchOutcome()
        failure = ItemFailure(
            FeedStatusReason.SOURCE_UNREACHABLE,
            "item_http_503",
        )
        for _ in range(2):
            outcome.record_attempt()
            outcome.record_failure(failure)

        promoted = outcome.promoted_failure()

        self.assertEqual(promoted, failure)

    def test_mixed_item_failures_promote_collector_error(self) -> None:
        outcome = ItemBatchOutcome()
        failures = [
            ItemFailure(
                FeedStatusReason.SYSTEM_AUTHENTICATION_FAILED,
                "item_http_401",
            ),
            ItemFailure(
                FeedStatusReason.SOURCE_UNREACHABLE,
                "item_http_503",
            ),
        ]
        for failure in failures:
            outcome.record_attempt()
            outcome.record_failure(failure)

        promoted = outcome.promoted_failure()

        self.assertIsNotNone(promoted)
        assert promoted is not None
        self.assertIs(
            promoted.status_reason,
            FeedStatusReason.SYSTEM_COLLECTOR_ERROR,
        )
        self.assertEqual(promoted.reason, "mixed_item_failures")


class TestPollingPayloadRegression(unittest.TestCase):
    def test_bcfy_missing_calls_is_empty_response(self) -> None:
        self.assertEqual(
            bcfy_calls_collector._extract_calls_from_response({}), []
        )

    def test_bcfy_empty_calls_list_is_empty_response(self) -> None:
        self.assertEqual(
            bcfy_calls_collector._extract_calls_from_response({"calls": []}),
            [],
        )

    def test_bcfy_non_list_calls_is_malformed_payload(self) -> None:
        with self.assertRaises(FeedFailure) as cm:
            bcfy_calls_collector._extract_calls_from_response({"calls": {}})

        self.assertIs(
            cm.exception.status_reason,
            FeedStatusReason.SYSTEM_COLLECTOR_ERROR,
        )
        self.assertEqual(cm.exception.reason, "calls_api_payload_malformed")

    def test_missing_files_is_empty_fire_notifications_response(self) -> None:
        self.assertEqual(
            payloads.extract_optional_item_list(
                {},
                "files",
                malformed_reason="fn_api_payload_malformed",
            ),
            [],
        )

    def test_empty_files_list_is_empty_fire_notifications_response(
        self,
    ) -> None:
        self.assertEqual(
            payloads.extract_optional_item_list(
                {"files": []},
                "files",
                malformed_reason="fn_api_payload_malformed",
            ),
            [],
        )

    def test_non_list_files_is_malformed_payload(self) -> None:
        with self.assertRaises(FeedFailure) as cm:
            payloads.extract_optional_item_list(
                {"files": {}},
                "files",
                malformed_reason="fn_api_payload_malformed",
            )

        self.assertIs(
            cm.exception.status_reason,
            FeedStatusReason.SYSTEM_COLLECTOR_ERROR,
        )
        self.assertEqual(cm.exception.reason, "fn_api_payload_malformed")


class TestCallDownloadFailedRegression(unittest.TestCase):
    def test_call_download_failed_slo_marker_lives_only_in_helper(
        self,
    ) -> None:
        marker = "# SLO: " + "call_download_failed emit"
        ingestion_dir = pathlib.Path(__file__).resolve().parents[2]

        found_files: list[str] = []
        for py_file in ingestion_dir.rglob("*.py"):
            if "tests" in py_file.parts:
                continue
            if marker in py_file.read_text(encoding="utf-8"):
                found_files.append(
                    py_file.relative_to(ingestion_dir).as_posix()
                )

        self.assertEqual(found_files, ["collectors/telemetry.py"])

    def test_call_download_failed_emit_uses_bounded_payload_fields(
        self,
    ) -> None:
        logger = logging.getLogger(
            "backend.pipeline.ingestion.collectors.tests."
            "failure_semantics_regression"
        )
        logger.setLevel(logging.WARNING)
        logger.propagate = False
        handler = _CaptureHandler()
        logger.addHandler(handler)
        try:
            telemetry.emit_call_download_failed(
                logger,
                feed_id="feed-123",
                source_type="openmhz",
            )
        finally:
            logger.removeHandler(handler)
            logger.propagate = True

        self.assertEqual(len(handler.records), 1)
        fields = cast(
            "dict[str, Any]",
            handler.records[0].__dict__["json_fields"],
        )
        self.assertEqual(
            fields,
            {
                "event_type": "call_download_failed",
                "feed_id": "feed-123",
                "source_type": "openmhz",
            },
        )


if __name__ == "__main__":
    unittest.main()
