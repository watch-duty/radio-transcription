from __future__ import annotations

import asyncio
import typing
import unittest
from unittest import mock

from backend.pipeline.ingestion.retry import (
    LeaseExpiredError,
    retry_with_lease_check,
    settle_issued_operation,
)


class TestIssuedOperationSettlement(unittest.IsolatedAsyncioTestCase):
    """Issued work settles to one definitive outcome."""

    async def test_returns_successful_result(self) -> None:
        async def succeed() -> str:
            return "settled"

        settlement = await settle_issued_operation(succeed())

        self.assertEqual(settlement.result, "settled")
        self.assertIsNone(settlement.failure)
        self.assertIsNone(settlement.cancellation)

    async def test_returns_operation_failure(self) -> None:
        failure = RuntimeError("operation failed")

        async def fail() -> None:
            raise failure

        settlement = await settle_issued_operation(fail())

        self.assertIsNone(settlement.result)
        self.assertIs(settlement.failure, failure)
        self.assertIsNone(settlement.cancellation)

    async def test_inner_cancellation_is_operation_failure(self) -> None:
        failure = asyncio.CancelledError("inner cancellation")

        async def cancel_operation() -> None:
            raise failure

        settlement = await settle_issued_operation(cancel_operation())

        self.assertIsNone(settlement.result)
        self.assertIs(settlement.failure, failure)
        self.assertIsNone(settlement.cancellation)

    async def test_repeated_cancellation_does_not_starve_child(self) -> None:
        started = asyncio.Event()
        release = asyncio.Event()
        progress = 0

        async def settle_after_progress() -> str:
            nonlocal progress
            started.set()
            for _ in range(3):
                await asyncio.sleep(0)
                progress += 1
            await release.wait()
            return "settled"

        task = asyncio.create_task(
            settle_issued_operation(settle_after_progress())
        )
        await started.wait()
        first_marker = object()
        task.cancel(first_marker)
        await asyncio.sleep(0)

        for _ in range(5):
            task.cancel(object())
            await asyncio.sleep(0)

        self.assertEqual(progress, 3)
        self.assertFalse(task.done())

        release.set()
        settlement = await task

        self.assertEqual(settlement.result, "settled")
        self.assertIsNone(settlement.failure)
        self.assertIsNotNone(settlement.cancellation)
        assert settlement.cancellation is not None
        self.assertIs(settlement.cancellation.args[0], first_marker)

    async def test_inner_and_caller_cancellation_remain_distinct(self) -> None:
        started = asyncio.Event()
        release = asyncio.Event()
        failure = asyncio.CancelledError("inner cancellation")

        async def cancel_operation() -> None:
            started.set()
            await release.wait()
            raise failure

        task = asyncio.create_task(settle_issued_operation(cancel_operation()))
        await started.wait()
        caller_marker = object()
        task.cancel(caller_marker)
        await asyncio.sleep(0)
        release.set()

        settlement = await task

        self.assertIsNone(settlement.result)
        self.assertIs(settlement.failure, failure)
        self.assertIsNotNone(settlement.cancellation)
        assert settlement.cancellation is not None
        self.assertIs(settlement.cancellation.args[0], caller_marker)


class TestRetrySuccessFirstAttempt(unittest.IsolatedAsyncioTestCase):
    """Tests for retry_with_lease_check when fn succeeds immediately."""

    async def test_returns_result_on_first_attempt(self) -> None:
        fn = mock.AsyncMock(return_value="ok")
        result = await retry_with_lease_check(
            fn,
            lease_lost=asyncio.Event(),
            shutdown=asyncio.Event(),
        )
        self.assertEqual(result, "ok")
        fn.assert_awaited_once()

    async def test_passes_args_to_fn(self) -> None:
        fn = mock.AsyncMock(return_value=42)
        await retry_with_lease_check(
            fn,
            "a",
            "b",
            lease_lost=asyncio.Event(),
            shutdown=asyncio.Event(),
        )
        fn.assert_awaited_once_with("a", "b")


class TestRetryOnRetryable(unittest.IsolatedAsyncioTestCase):
    """Tests for retry on retryable exceptions."""

    async def test_retries_on_retryable_then_succeeds(self) -> None:
        fn = mock.AsyncMock(side_effect=[OSError("fail"), "ok"])
        result = await retry_with_lease_check(
            fn,
            lease_lost=asyncio.Event(),
            shutdown=asyncio.Event(),
            max_retries=2,
            base_delay_sec=0.0,
            retryable=(OSError,),
        )
        self.assertEqual(result, "ok")
        self.assertEqual(fn.await_count, 2)

    async def test_exhausts_max_retries_and_raises(self) -> None:
        fn = mock.AsyncMock(side_effect=OSError("fail"))
        with self.assertRaises(OSError):
            await retry_with_lease_check(
                fn,
                lease_lost=asyncio.Event(),
                shutdown=asyncio.Event(),
                max_retries=2,
                base_delay_sec=0.0,
                retryable=(OSError,),
            )
        # 1 initial + 2 retries = 3 total
        self.assertEqual(fn.await_count, 3)


class TestRetryNonRetryable(unittest.IsolatedAsyncioTestCase):
    """Tests for non-retryable exceptions."""

    async def test_non_retryable_raises_immediately(self) -> None:
        fn = mock.AsyncMock(side_effect=ValueError("bad"))
        with self.assertRaises(ValueError):
            await retry_with_lease_check(
                fn,
                lease_lost=asyncio.Event(),
                shutdown=asyncio.Event(),
                max_retries=3,
                retryable=(OSError,),
            )
        fn.assert_awaited_once()


class TestRetryLeaseLost(unittest.IsolatedAsyncioTestCase):
    """Tests for lease-lost abort behavior."""

    async def test_aborts_before_first_attempt_if_lease_lost(self) -> None:
        fn = mock.AsyncMock(return_value="ok")
        lease_lost = asyncio.Event()
        lease_lost.set()
        with self.assertRaises(LeaseExpiredError):
            await retry_with_lease_check(
                fn,
                lease_lost=lease_lost,
                shutdown=asyncio.Event(),
            )
        fn.assert_not_awaited()

    async def test_aborts_during_backoff_if_lease_lost(self) -> None:
        fn = mock.AsyncMock(side_effect=OSError("fail"))
        lease_lost = asyncio.Event()

        async def _set_lease_lost_soon() -> None:
            await asyncio.sleep(0.01)
            lease_lost.set()

        task = asyncio.create_task(_set_lease_lost_soon())
        with self.assertRaises(LeaseExpiredError):
            await retry_with_lease_check(
                fn,
                lease_lost=lease_lost,
                shutdown=asyncio.Event(),
                max_retries=5,
                base_delay_sec=10.0,
                retryable=(OSError,),
            )
        await task


class TestRetryShutdown(unittest.IsolatedAsyncioTestCase):
    """Tests for shutdown respect."""

    async def test_aborts_before_first_attempt_if_shutdown(self) -> None:
        """CancelledError raised if shutdown is set before first attempt."""
        fn = mock.AsyncMock(return_value="ok")
        shutdown = asyncio.Event()
        shutdown.set()

        with self.assertRaises(asyncio.CancelledError):
            await retry_with_lease_check(
                fn,
                lease_lost=asyncio.Event(),
                shutdown=shutdown,
            )
        fn.assert_not_awaited()

    async def test_raises_cancelled_on_shutdown_after_failed_attempt(
        self,
    ) -> None:
        """On shutdown after a failed attempt, raise CancelledError."""
        shutdown = asyncio.Event()

        async def _fail_then_signal(*args: object) -> str:
            shutdown.set()
            msg = "transient"
            raise OSError(msg)

        fn = mock.AsyncMock(side_effect=_fail_then_signal)

        with self.assertRaises(asyncio.CancelledError):
            await retry_with_lease_check(
                fn,
                lease_lost=asyncio.Event(),
                shutdown=shutdown,
                max_retries=3,
                base_delay_sec=0.0,
                retryable=(OSError,),
            )
        # First attempt fails and sets shutdown, then loop-top check raises CancelledError
        self.assertEqual(fn.await_count, 1)

    async def test_raises_cancelled_during_backoff_on_shutdown(self) -> None:
        fn = mock.AsyncMock(side_effect=OSError("transient"))
        shutdown = asyncio.Event()

        async def _set_shutdown_soon() -> None:
            await asyncio.sleep(0.01)
            shutdown.set()

        task = asyncio.create_task(_set_shutdown_soon())
        with self.assertRaises(asyncio.CancelledError):
            await retry_with_lease_check(
                fn,
                lease_lost=asyncio.Event(),
                shutdown=shutdown,
                max_retries=5,
                base_delay_sec=10.0,
                retryable=(OSError,),
            )
        await task

    async def test_caller_cancellation_during_backoff_cleanup_propagates(
        self,
    ) -> None:
        marker = object()
        attempts = 0
        retry_task: asyncio.Task[str] | None = None

        class CancelParentWhenCleanedUp(asyncio.Event):
            async def wait(self) -> typing.Literal[True]:
                try:
                    return await super().wait()
                except asyncio.CancelledError:
                    assert retry_task is not None
                    retry_task.cancel(marker)
                    raise

        async def fail_once() -> str:
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                message = "transient"
                raise OSError(message)
            return "unexpected retry"

        with mock.patch(
            "backend.pipeline.ingestion.retry.random.uniform",
            return_value=0.01,
        ):
            retry_task = asyncio.create_task(
                retry_with_lease_check(
                    fail_once,
                    lease_lost=CancelParentWhenCleanedUp(),
                    shutdown=asyncio.Event(),
                    max_retries=1,
                    base_delay_sec=0.01,
                    max_delay_sec=0.01,
                    retryable=(OSError,),
                )
            )

            with self.assertRaises(asyncio.CancelledError) as raised:
                await retry_task

        self.assertIsInstance(raised.exception, asyncio.CancelledError)
        self.assertEqual(attempts, 1)


class TestRetryJitterBounds(unittest.IsolatedAsyncioTestCase):
    """Tests for jitter bounds."""

    async def test_jitter_respects_max_delay(self) -> None:
        """Backoff delay never exceeds max_delay_sec."""
        delays: list[float] = []
        original_wait = asyncio.wait

        async def _capture_wait(fs, **kwargs):
            if "timeout" in kwargs and kwargs["timeout"] is not None:
                delays.append(kwargs["timeout"])
            return await original_wait(fs, **kwargs)

        fn = mock.AsyncMock(side_effect=[OSError("fail")] * 3 + ["ok"])

        with mock.patch("asyncio.wait", side_effect=_capture_wait):
            await retry_with_lease_check(
                fn,
                lease_lost=asyncio.Event(),
                shutdown=asyncio.Event(),
                max_retries=3,
                base_delay_sec=1.0,
                max_delay_sec=2.0,
                retryable=(OSError,),
            )

        for d in delays:
            self.assertLessEqual(d, 2.0)
            self.assertGreaterEqual(d, 0.0)


if __name__ == "__main__":
    unittest.main()
