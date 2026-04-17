"""Tests for the shared collector retry utilities."""

from __future__ import annotations

import asyncio
import unittest
from unittest.mock import AsyncMock, patch

from backend.pipeline.ingestion.collectors._retry import (
    _ServerError,
    _ShutdownInterrupt,
    retry_http_op,
)
from backend.pipeline.ingestion.collectors._utils import _sleep_or_shutdown

_UTILS_SLEEP = "backend.pipeline.ingestion.collectors._utils._sleep_or_shutdown"


class TestSleepOrShutdown(unittest.IsolatedAsyncioTestCase):
    async def test_returns_false_on_timeout(self) -> None:
        shutdown = asyncio.Event()
        result = await _sleep_or_shutdown(shutdown, 0.001)
        self.assertFalse(result)

    async def test_returns_true_when_shutdown_already_set(self) -> None:
        shutdown = asyncio.Event()
        shutdown.set()
        result = await _sleep_or_shutdown(shutdown, 10.0)
        self.assertTrue(result)


class TestRetryHttpOpSuccess(unittest.IsolatedAsyncioTestCase):
    async def test_returns_result_on_first_attempt(self) -> None:
        fn = AsyncMock(return_value=b"data")
        result = await retry_http_op(fn, asyncio.Event())
        self.assertEqual(result, b"data")
        fn.assert_awaited_once()

    async def test_returns_none_when_fn_returns_none(self) -> None:
        fn = AsyncMock(return_value=None)
        result = await retry_http_op(fn, asyncio.Event())
        self.assertIsNone(result)
        fn.assert_awaited_once()


class TestRetryHttpOpRetry(unittest.IsolatedAsyncioTestCase):
    @patch(_UTILS_SLEEP, new_callable=AsyncMock)
    async def test_retries_on_server_error_then_succeeds(
        self, mock_sleep: AsyncMock
    ) -> None:
        mock_sleep.return_value = False
        fn = AsyncMock(side_effect=[_ServerError("5XX"), b"ok"])
        result = await retry_http_op(fn, asyncio.Event(), max_retries=3)
        self.assertEqual(result, b"ok")
        self.assertEqual(fn.await_count, 2)
        mock_sleep.assert_called_once()

    @patch(_UTILS_SLEEP, new_callable=AsyncMock)
    async def test_returns_none_on_retry_exhaustion(
        self, mock_sleep: AsyncMock
    ) -> None:
        mock_sleep.return_value = False
        fn = AsyncMock(side_effect=_ServerError("5XX"))
        result = await retry_http_op(fn, asyncio.Event(), max_retries=3)
        self.assertIsNone(result)
        self.assertEqual(fn.await_count, 4)  # 1 initial + 3 retries

    @patch(_UTILS_SLEEP, new_callable=AsyncMock)
    async def test_retry_count_matches_max_retries(
        self, mock_sleep: AsyncMock
    ) -> None:
        mock_sleep.return_value = False
        fn = AsyncMock(side_effect=_ServerError("5XX"))
        await retry_http_op(fn, asyncio.Event(), max_retries=2)
        self.assertEqual(fn.await_count, 3)  # 1 initial + 2 retries


class TestRetryHttpOpNonRetryable(unittest.IsolatedAsyncioTestCase):
    async def test_non_server_error_propagates_immediately(self) -> None:
        fn = AsyncMock(side_effect=RuntimeError("fatal"))
        with self.assertRaisesRegex(RuntimeError, "fatal"):
            await retry_http_op(fn, asyncio.Event(), max_retries=3)
        fn.assert_awaited_once()

    async def test_value_error_propagates_without_retry(self) -> None:
        fn = AsyncMock(side_effect=ValueError("bad input"))
        with self.assertRaises(ValueError):
            await retry_http_op(fn, asyncio.Event(), max_retries=3)
        fn.assert_awaited_once()


class TestRetryHttpOpShutdown(unittest.IsolatedAsyncioTestCase):
    @patch(_UTILS_SLEEP, new_callable=AsyncMock)
    async def test_returns_none_on_shutdown_during_backoff(
        self, mock_sleep: AsyncMock
    ) -> None:
        mock_sleep.return_value = True  # shutdown fires
        fn = AsyncMock(side_effect=_ServerError("5XX"))
        result = await retry_http_op(fn, asyncio.Event(), max_retries=3)
        self.assertIsNone(result)
        fn.assert_awaited_once()
        mock_sleep.assert_called_once()

    @patch(_UTILS_SLEEP, new_callable=AsyncMock)
    async def test_shutdown_interrupt_is_base_exception(
        self, mock_sleep: AsyncMock
    ) -> None:
        """_ShutdownInterrupt is BaseException and is caught by retry_http_op."""
        self.assertTrue(issubclass(_ShutdownInterrupt, BaseException))
        mock_sleep.return_value = True
        fn = AsyncMock(side_effect=_ServerError("5XX"))
        result = await retry_http_op(fn, asyncio.Event(), max_retries=3)
        self.assertIsNone(result)


class TestRetryHttpOpJitter(unittest.IsolatedAsyncioTestCase):
    @patch(_UTILS_SLEEP, new_callable=AsyncMock)
    async def test_jitter_zero_uses_pure_exponential(
        self, mock_sleep: AsyncMock
    ) -> None:
        mock_sleep.return_value = False
        fn = AsyncMock(side_effect=[_ServerError("5XX"), b"ok"])
        result = await retry_http_op(
            fn, asyncio.Event(), max_retries=2, jitter=0.0
        )
        self.assertEqual(result, b"ok")
        mock_sleep.assert_called_once()

    @patch(_UTILS_SLEEP, new_callable=AsyncMock)
    async def test_jitter_nonzero_still_calls_sleep(
        self, mock_sleep: AsyncMock
    ) -> None:
        mock_sleep.return_value = False
        fn = AsyncMock(side_effect=[_ServerError("5XX"), b"ok"])
        result = await retry_http_op(
            fn, asyncio.Event(), max_retries=2, jitter=1.0
        )
        self.assertEqual(result, b"ok")
        mock_sleep.assert_called_once()
