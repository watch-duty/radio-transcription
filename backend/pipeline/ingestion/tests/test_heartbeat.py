from __future__ import annotations

import asyncio
import unittest
import uuid
from unittest import mock

from backend.pipeline.ingestion.heartbeat import HeartbeatMonitor

_WORKER_ID = uuid.UUID("11111111-2222-3333-4444-555555555555")
_FEED_A = uuid.UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
_FEED_B = uuid.UUID("bbbbbbbb-cccc-dddd-eeee-ffffffffffff")


def _make_pool(renewed_ids: set[uuid.UUID] | None = None) -> mock.AsyncMock:
    """Build a mock asyncpg.Pool whose FeedStore batch renewal returns *renewed_ids*."""
    pool = mock.AsyncMock()
    if renewed_ids is not None:
        pool.fetch.return_value = [{"id": fid} for fid in renewed_ids]
    else:
        pool.fetch.return_value = []
    return pool


class TestHeartbeatMonitorRenewal(unittest.IsolatedAsyncioTestCase):
    """Tests for the heartbeat renewal loop."""

    async def test_renew_heartbeat_keeps_task_alive(self) -> None:
        """When batch renew returns the feed ID the task is NOT cancelled."""
        pool = _make_pool({_FEED_A})
        monitor = HeartbeatMonitor(pool, _WORKER_ID, interval_sec=0.01)

        task = asyncio.create_task(asyncio.sleep(10))
        monitor.register(_FEED_A, task)
        monitor.start()

        await asyncio.sleep(0.05)
        await monitor.stop()

        self.assertFalse(task.cancelled())
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task

    async def test_fence_violation_cancels_task(self) -> None:
        """When batch renew omits the feed ID the task is cancelled and removed."""
        pool = _make_pool(set())
        monitor = HeartbeatMonitor(pool, _WORKER_ID, interval_sec=0.01)

        task = asyncio.create_task(asyncio.sleep(10))
        monitor.register(_FEED_A, task)
        monitor.start()

        await asyncio.sleep(0.05)
        await monitor.stop()

        self.assertTrue(task.cancelled())
        self.assertNotIn(_FEED_A, monitor._feeds)  # noqa: SLF001

    async def test_fence_violation_preserves_reregistered_task(self) -> None:
        """
        Verify re-registered task survives fence violation cleanup.

        Simulates the race: while the async batch renewal runs, the
        orchestrator unregisters the fenced feed and re-registers a fresh
        task for the same feed_id. The identity check must prevent eviction
        of the new task.
        """
        pool = _make_pool(set())
        monitor = HeartbeatMonitor(pool, _WORKER_ID, interval_sec=100)

        old_task = asyncio.create_task(asyncio.sleep(10))
        new_task = asyncio.create_task(asyncio.sleep(10))
        monitor.register(_FEED_A, old_task)

        original_batch = monitor._store.renew_heartbeats_batch  # noqa: SLF001

        async def _intercept(*args, **kwargs) -> set[uuid.UUID]:  # noqa: ANN002, ANN003
            result = await original_batch(*args, **kwargs)
            # Inject re-registration between the async call returning and
            # the monitor loop processing the results.
            monitor.register(_FEED_A, new_task)
            return result

        monitor._store.renew_heartbeats_batch = _intercept  # type: ignore[assignment]  # noqa: SLF001
        monitor.start()
        await asyncio.sleep(0.05)
        await monitor.stop()

        # Old task must be cancelled (fence violation).
        self.assertTrue(old_task.cancelled())
        # New task must NOT be evicted from the registry.
        self.assertIs(monitor._feeds.get(_FEED_A), new_task)  # noqa: SLF001
        self.assertFalse(new_task.cancelled())

        new_task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await new_task

    async def test_exception_does_not_cancel_task(self) -> None:
        """A transient DB error is logged but does NOT cancel the feed task."""
        pool = mock.AsyncMock()
        pool.fetch.side_effect = RuntimeError("transient db error")
        monitor = HeartbeatMonitor(pool, _WORKER_ID, interval_sec=0.01)

        task = asyncio.create_task(asyncio.sleep(10))
        monitor.register(_FEED_A, task)
        monitor.start()

        await asyncio.sleep(0.05)
        await monitor.stop()

        self.assertFalse(task.cancelled())
        # Feed should remain in the registry.
        self.assertIn(_FEED_A, monitor._feeds)  # noqa: SLF001
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task

    async def test_mixed_results_only_cancels_fenced_feed(self) -> None:
        """One feed renewed, one missing — only the missing task is cancelled."""
        pool = _make_pool({_FEED_A})
        monitor = HeartbeatMonitor(pool, _WORKER_ID, interval_sec=0.01)

        task_a = asyncio.create_task(asyncio.sleep(10))
        task_b = asyncio.create_task(asyncio.sleep(10))
        monitor.register(_FEED_A, task_a)
        monitor.register(_FEED_B, task_b)
        monitor.start()

        await asyncio.sleep(0.05)
        await monitor.stop()

        self.assertFalse(task_a.cancelled())
        self.assertTrue(task_b.cancelled())
        self.assertIn(_FEED_A, monitor._feeds)  # noqa: SLF001
        self.assertNotIn(_FEED_B, monitor._feeds)  # noqa: SLF001

        task_a.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task_a


class TestHeartbeatMonitorCleanup(unittest.IsolatedAsyncioTestCase):
    """Tests for completed-task cleanup."""

    async def test_done_task_is_removed_silently(self) -> None:
        """A task that completed before the heartbeat cycle is removed."""
        pool = _make_pool(set())
        monitor = HeartbeatMonitor(pool, _WORKER_ID, interval_sec=0.01)

        # Create a task that finishes immediately.
        task = asyncio.create_task(asyncio.sleep(0))
        await task
        monitor.register(_FEED_A, task)
        monitor.start()

        await asyncio.sleep(0.05)
        await monitor.stop()

        self.assertNotIn(_FEED_A, monitor._feeds)  # noqa: SLF001

    async def test_done_task_with_exception_is_cleaned_up_and_logged(self) -> None:
        """A task that raised is removed and its exception is logged."""

        async def _failing_coro() -> None:
            msg = "boom"
            raise RuntimeError(msg)

        task = asyncio.create_task(_failing_coro())
        # Let the task finish with an exception.
        await asyncio.sleep(0)

        pool = _make_pool(set())
        monitor = HeartbeatMonitor(pool, _WORKER_ID, interval_sec=0.01)
        monitor.register(_FEED_A, task)

        with mock.patch("backend.pipeline.ingestion.heartbeat.logger") as mock_logger:
            monitor.start()
            await asyncio.sleep(0.05)
            await monitor.stop()

        self.assertNotIn(_FEED_A, monitor._feeds)  # noqa: SLF001
        # The crash should have been logged at ERROR level with the feed_id.
        mock_logger.error.assert_called()
        log_args = mock_logger.error.call_args
        self.assertEqual(log_args[0][1], _FEED_A)


class TestHeartbeatMonitorLifecycle(unittest.IsolatedAsyncioTestCase):
    """Tests for start / stop / register / unregister."""

    async def test_stop_swallows_cancelled_error(self) -> None:
        """stop() awaits the background task and swallows CancelledError."""
        pool = _make_pool(set())
        monitor = HeartbeatMonitor(pool, _WORKER_ID, interval_sec=10)

        monitor.start()
        # stop() should return cleanly, not raise CancelledError.
        await monitor.stop()

    async def test_stop_on_unstarted_monitor(self) -> None:
        """stop() on a monitor that was never started does not raise."""
        pool = mock.AsyncMock()
        monitor = HeartbeatMonitor(pool, _WORKER_ID)
        await monitor.stop()

    async def test_unregister_removes_feed(self) -> None:
        """Unregister removes a feed from the registry."""
        pool = mock.AsyncMock()
        monitor = HeartbeatMonitor(pool, _WORKER_ID)
        task = asyncio.create_task(asyncio.sleep(10))
        monitor.register(_FEED_A, task)

        monitor.unregister(_FEED_A)

        self.assertNotIn(_FEED_A, monitor._feeds)  # noqa: SLF001
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task

    async def test_unregister_missing_feed_is_noop(self) -> None:
        """Unregister on a non-existent feed does not raise."""
        pool = mock.AsyncMock()
        monitor = HeartbeatMonitor(pool, _WORKER_ID)
        monitor.unregister(_FEED_A)  # Should not raise.

    async def test_double_start_raises(self) -> None:
        """Calling start() twice raises RuntimeError."""
        pool = _make_pool(set())
        monitor = HeartbeatMonitor(pool, _WORKER_ID, interval_sec=10)

        monitor.start()
        with self.assertRaises(RuntimeError):
            monitor.start()
        await monitor.stop()

    async def test_start_after_stop_succeeds(self) -> None:
        """A stopped monitor can be restarted."""
        pool = _make_pool(set())
        monitor = HeartbeatMonitor(pool, _WORKER_ID, interval_sec=10)

        monitor.start()
        await monitor.stop()
        # Should not raise — _task was cleared by stop().
        monitor.start()
        await monitor.stop()

    async def test_no_db_call_when_no_feeds_registered(self) -> None:
        """No DB call is made when the feed registry is empty."""
        pool = mock.AsyncMock()
        monitor = HeartbeatMonitor(pool, _WORKER_ID, interval_sec=0.01)

        monitor.start()
        await asyncio.sleep(0.05)
        await monitor.stop()

        pool.fetch.assert_not_called()


class TestHeartbeatMonitorSnapshotSafety(unittest.IsolatedAsyncioTestCase):
    """Tests for concurrent register/unregister safety."""

    async def test_unregister_during_renewal_does_not_raise(self) -> None:
        """Unregistering a feed while a renewal is in-flight is safe."""
        pool = _make_pool({_FEED_A, _FEED_B})
        monitor = HeartbeatMonitor(pool, _WORKER_ID, interval_sec=0.01)

        task_a = asyncio.create_task(asyncio.sleep(10))
        task_b = asyncio.create_task(asyncio.sleep(10))
        monitor.register(_FEED_A, task_a)
        monitor.register(_FEED_B, task_b)
        monitor.start()

        # Unregister while the loop may be mid-iteration.
        await asyncio.sleep(0.02)
        monitor.unregister(_FEED_A)

        await asyncio.sleep(0.03)
        await monitor.stop()

        # The loop should not have raised.  Feed B should still be tracked.
        self.assertNotIn(_FEED_A, monitor._feeds)  # noqa: SLF001

        task_a.cancel()
        task_b.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task_a
        with self.assertRaises(asyncio.CancelledError):
            await task_b


if __name__ == "__main__":
    unittest.main()
