"""Tests for shared collector semantics helpers."""

from __future__ import annotations

import asyncio
import logging
import unittest
from typing import Any, cast

from backend.pipeline.ingestion import slo_contract
from backend.pipeline.ingestion.collectors import (
    control_flow,
    item_downloads,
    payloads,
    telemetry,
)
from backend.pipeline.ingestion.models import FeedFailure
from backend.pipeline.storage import feed_store


class _CaptureHandler(logging.Handler):
    def __init__(self) -> None:
        super().__init__()
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


class TestSleepOrCancel(unittest.IsolatedAsyncioTestCase):
    async def test_returns_after_timeout_when_shutdown_unset(self) -> None:
        shutdown = asyncio.Event()

        await control_flow.sleep_or_cancel(shutdown, 0.001)

    async def test_raises_cancelled_error_when_shutdown_already_set(
        self,
    ) -> None:
        shutdown = asyncio.Event()
        shutdown.set()

        with self.assertRaises(asyncio.CancelledError):
            await control_flow.sleep_or_cancel(shutdown, 1)

    async def test_raises_cancelled_error_when_shutdown_set_during_wait(
        self,
    ) -> None:
        shutdown = asyncio.Event()

        async def _set_shutdown() -> None:
            await asyncio.sleep(0)
            shutdown.set()

        task = asyncio.create_task(_set_shutdown())
        with self.assertRaises(asyncio.CancelledError):
            await control_flow.sleep_or_cancel(shutdown, 1)
        await task


class TestAwaitOrCancel(unittest.IsolatedAsyncioTestCase):
    async def test_operation_completes_successfully(self) -> None:
        shutdown = asyncio.Event()

        async def op() -> str:
            return "success_val"

        result = await control_flow.await_or_cancel(op(), shutdown)

        self.assertEqual(result, "success_val")

    async def test_shutdown_already_set_prevents_operation(self) -> None:
        shutdown = asyncio.Event()
        shutdown.set()
        op_called = False

        async def op() -> str:
            nonlocal op_called
            op_called = True
            return "ok"

        with self.assertRaises(asyncio.CancelledError):
            await control_flow.await_or_cancel(op(), shutdown)

        self.assertFalse(op_called)

    async def test_shutdown_triggered_during_operation(self) -> None:
        shutdown = asyncio.Event()
        op_started = asyncio.Event()
        op_cancelled = asyncio.Event()

        async def op() -> str:
            op_started.set()
            try:
                await asyncio.Future()
            except asyncio.CancelledError:
                op_cancelled.set()
                raise
            msg = "unreachable"
            raise AssertionError(msg)

        async def trigger_shutdown() -> None:
            await op_started.wait()
            shutdown.set()

        trigger_task = asyncio.create_task(trigger_shutdown())

        with self.assertRaises(asyncio.CancelledError):
            await control_flow.await_or_cancel(op(), shutdown)

        await trigger_task
        await asyncio.wait_for(op_cancelled.wait(), timeout=1.0)

    async def test_operation_fails_propagates_exception(self) -> None:
        shutdown = asyncio.Event()

        async def op() -> str:
            msg = "op failed"
            raise ValueError(msg)

        with self.assertRaises(ValueError) as cm:
            await control_flow.await_or_cancel(op(), shutdown)

        self.assertEqual(str(cm.exception), "op failed")

    async def test_operation_wins_on_simultaneous_completion(self) -> None:
        shutdown = asyncio.Event()

        async def op() -> str:
            shutdown.set()
            await shutdown.wait()
            return "success_val"

        result = await control_flow.await_or_cancel(op(), shutdown)

        self.assertEqual(result, "success_val")


class TestItemDownloadHelpers(unittest.TestCase):
    def test_classifies_terminal_item_http_status(self) -> None:
        failure = item_downloads.item_http_failure(401)

        self.assertIs(
            failure.status_reason,
            feed_store.FeedStatusReason.SYSTEM_AUTHENTICATION_FAILED,
        )
        self.assertEqual(failure.reason, "item_http_401")

    def test_unmapped_item_http_status_falls_back_to_collector_error(
        self,
    ) -> None:
        failure = item_downloads.item_http_failure(404)

        self.assertIs(
            failure.status_reason,
            feed_store.FeedStatusReason.SYSTEM_COLLECTOR_ERROR,
        )
        self.assertEqual(failure.reason, "item_http_404")

    def test_retry_exhaustion_without_http_evidence_is_unreachable(
        self,
    ) -> None:
        failure = item_downloads.item_download_failed()

        self.assertIs(
            failure.status_reason,
            feed_store.FeedStatusReason.SOURCE_UNREACHABLE,
        )
        self.assertEqual(failure.reason, "item_download_failed")


class TestPayloadHelpers(unittest.TestCase):
    def test_missing_field_returns_empty_list(self) -> None:
        self.assertEqual(
            payloads.extract_optional_item_list(
                {},
                "files",
                malformed_reason="payload_malformed",
            ),
            [],
        )

    def test_list_field_returns_list_unchanged(self) -> None:
        files: list[Any] = [{"id": "a"}]

        self.assertIs(
            payloads.extract_optional_item_list(
                {"files": files},
                "files",
                malformed_reason="payload_malformed",
            ),
            files,
        )

    def test_present_non_list_field_raises_feed_failure(self) -> None:
        with self.assertRaises(FeedFailure) as cm:
            payloads.extract_optional_item_list(
                {"files": {}},
                "files",
                malformed_reason="payload_malformed",
            )

        self.assertIs(
            cm.exception.status_reason,
            feed_store.FeedStatusReason.SYSTEM_SOURCE_PAYLOAD_INVALID,
        )
        self.assertEqual(cm.exception.reason, "payload_malformed")


class TestTelemetryHelpers(unittest.TestCase):
    def test_call_download_failed_event_has_bounded_payload_fields(
        self,
    ) -> None:
        logger = logging.getLogger(
            "backend.pipeline.ingestion.collectors.tests.telemetry"
        )
        logger.setLevel(logging.WARNING)
        handler = _CaptureHandler()
        logger.addHandler(handler)
        logger.propagate = False
        try:
            telemetry.emit_call_download_failed(
                logger,
                feed_id=123,
                source_type="openmhz",
            )
        finally:
            logger.removeHandler(handler)
            logger.propagate = True

        self.assertEqual(len(handler.records), 1)
        self.assertEqual(
            handler.records[0].getMessage(), "Call download failed"
        )
        self.assertEqual(
            cast(
                "dict[str, Any]",
                handler.records[0].__dict__["json_fields"],
            ),
            {
                "event_type": (slo_contract.EVENT_TYPE_CALL_DOWNLOAD_FAILED),
                "feed_id": "123",
                "source_type": "openmhz",
            },
        )


if __name__ == "__main__":
    unittest.main()
