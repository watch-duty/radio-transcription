from __future__ import annotations

import asyncio
import unittest
import uuid
from unittest import mock

import aiohttp
import asyncpg

from backend.pipeline.ingestion.normalizer_runtime import NormalizerRuntime
from backend.pipeline.storage.feed_store import HeartbeatResult, LeasedFeed

_WORKER_ID = uuid.UUID("11111111-2222-3333-4444-555555555555")
_FEED_ID = uuid.UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")

_FEED = LeasedFeed(
    id=_FEED_ID,
    name="Test Feed",
    source_type="bcfy_feeds",
    last_processed_filename=None,
    stream_url="http://stream.example.com/feed",
)


def _mock_pubsub_publish(message_id: str = "test-message-id") -> mock._patch:
    """Patch publish_audio_chunk to return a fixed message id (at call site)."""
    return mock.patch(
        "backend.pipeline.ingestion.normalizer_runtime.publish_audio_chunk",
        new_callable=mock.AsyncMock,
        return_value=message_id,
    )


def _mock_upload_audio(gcs_path: str = "gs://b/p") -> mock._patch:
    """Patch upload_audio to return a deterministic GCS path."""
    return mock.patch(
        "backend.pipeline.ingestion.normalizer_runtime.upload_audio",
        new_callable=mock.AsyncMock,
        return_value=gcs_path,
    )


def _make_settings(**overrides) -> mock.MagicMock:
    """Build a mock NormalizerSettings with sensible defaults."""
    defaults = {
        "worker_id": _WORKER_ID,
        "max_feeds_per_worker": 250,
        "lease_poll_interval_sec": 5.0,
        "heartbeat_interval_sec": 15.0,
        "heartbeat_stall_timeout_sec": 45.0,
        "graceful_shutdown_timeout_sec": 10.0,
        "audio_staging_bucket": "test-bucket",
        "pubsub_topic_path": "projects/p/topics/t",
        "db_pool_min_size": 2,
        "db_pool_max_size": 5,
        "db_host": "10.0.0.1",
        "db_port": 6432,
        "db_user": "user",
        "db_name": "db",
        "db_password": "pass",
        "db_command_timeout_sec": 30.0,
        "db_connect_timeout_sec": 10.0,
        "feed_failure_threshold": 3,
        "abandonment_window_sec": 60.0,
        # Retry settings — must be real numbers so min()/random.uniform()
        # don't blow up on MagicMock auto-created attributes.
        "gcs_upload_max_retries": 3,
        "gcs_upload_retry_base_delay_sec": 0.5,
        "gcs_upload_retry_max_delay_sec": 8.0,
        "bookmark_max_retries": 2,
        "bookmark_retry_base_delay_sec": 0.5,
        "bookmark_retry_max_delay_sec": 4.0,
    }
    defaults.update(overrides)
    return mock.MagicMock(**defaults)


def _make_runtime(**settings_overrides) -> NormalizerRuntime:
    """Build a runtime with a mock capture_fn and settings."""

    async def _dummy_capture(feed, shutdown):
        yield b"chunk"

    settings = _make_settings(**settings_overrides)
    rt = NormalizerRuntime(capture_fn=_dummy_capture, settings=settings)
    # Pre-initialize _lease_lost so tests don't need _main().
    rt._lease_lost = asyncio.Event()
    return rt


class TestSleepOrShutdown(unittest.IsolatedAsyncioTestCase):
    """Tests for _sleep_or_shutdown."""

    async def test_returns_false_on_timeout(self) -> None:
        """Returns False when the sleep elapses normally."""
        rt = _make_runtime()
        rt._shutdown = asyncio.Event()
        result = await rt._sleep_or_shutdown(0.01)
        self.assertFalse(result)

    async def test_returns_true_on_shutdown(self) -> None:
        """Returns True when shutdown is signalled before timeout."""
        rt = _make_runtime()
        rt._shutdown = asyncio.Event()
        rt._shutdown.set()
        result = await rt._sleep_or_shutdown(10.0)
        self.assertTrue(result)


class TestReapCompletedTasks(unittest.IsolatedAsyncioTestCase):
    """Tests for _reap_completed_tasks."""

    async def test_removes_done_tasks(self) -> None:
        """Completed tasks are removed from _feed_tasks."""
        rt = _make_runtime()
        task = asyncio.create_task(asyncio.sleep(0))
        await task
        rt._feed_tasks[_FEED_ID] = task

        rt._reap_completed_tasks()

        self.assertNotIn(_FEED_ID, rt._feed_tasks)

    async def test_handles_cancelled_task(self) -> None:
        """Cancelled tasks are removed without raising."""
        rt = _make_runtime()
        task = asyncio.create_task(asyncio.sleep(100))
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        rt._feed_tasks[_FEED_ID] = task

        rt._reap_completed_tasks()

        self.assertNotIn(_FEED_ID, rt._feed_tasks)

    async def test_logs_exception(self) -> None:
        """Tasks that raised are cleaned up and logged."""

        async def _boom() -> None:
            msg = "boom"
            raise RuntimeError(msg)

        rt = _make_runtime()
        task = asyncio.create_task(_boom())
        await asyncio.sleep(0)  # let task finish
        rt._feed_tasks[_FEED_ID] = task

        with mock.patch(
            "backend.pipeline.ingestion.normalizer_runtime.logger",
        ) as mock_logger:
            rt._reap_completed_tasks()

        mock_logger.error.assert_called()
        self.assertNotIn(_FEED_ID, rt._feed_tasks)


class TestProcessFeedFenceViolation(unittest.IsolatedAsyncioTestCase):
    """Tests for _process_feed fence violation."""

    async def test_bookmark_fence_failure_exits_process(self) -> None:
        """When bookmark fence fails, os._exit is called."""

        async def _one_chunk(feed, shutdown):
            yield b"audio"

        rt = NormalizerRuntime(capture_fn=_one_chunk, settings=_make_settings())
        rt._shutdown = asyncio.Event()
        rt._lease_lost = asyncio.Event()
        rt._store = mock.AsyncMock()
        rt._store.update_feed_progress.return_value = False
        rt._releasing_feeds = set()

        with (
            _mock_upload_audio(),
            _mock_pubsub_publish(),
            mock.patch(
                "backend.pipeline.ingestion.normalizer_runtime.os._exit",
            ) as mock_exit,
            mock.patch("logging.shutdown"),
        ):
            await rt._process_feed(_FEED)
            mock_exit.assert_called_once_with(1)


class TestProcessFeedShutdown(unittest.IsolatedAsyncioTestCase):
    """Tests for _process_feed shutdown behavior."""

    async def test_shutdown_skips_individual_release(self) -> None:
        """When shutdown is set, task returns without calling release_feed."""

        async def _one_chunk(feed, shutdown):
            yield b"audio"

        rt = NormalizerRuntime(capture_fn=_one_chunk, settings=_make_settings())
        rt._shutdown = asyncio.Event()
        rt._shutdown.set()
        rt._lease_lost = asyncio.Event()
        rt._store = mock.AsyncMock()
        rt._store.update_feed_progress.return_value = True
        rt._releasing_feeds = set()

        with _mock_upload_audio(), _mock_pubsub_publish():
            await rt._process_feed(_FEED)

        rt._store.release_feed.assert_not_called()


class TestProcessFeedNormalCompletion(unittest.IsolatedAsyncioTestCase):
    """Tests for _process_feed normal completion."""

    async def test_normal_completion_releases_feed(self) -> None:
        """When generator exhausts, release_feed is called."""

        async def _one_chunk(feed, shutdown):
            yield b"audio"

        rt = NormalizerRuntime(capture_fn=_one_chunk, settings=_make_settings())
        rt._shutdown = asyncio.Event()
        rt._lease_lost = asyncio.Event()
        rt._store = mock.AsyncMock()
        rt._store.update_feed_progress.return_value = True
        rt._releasing_feeds = set()

        with _mock_upload_audio(), _mock_pubsub_publish():
            await rt._process_feed(_FEED)

        rt._store.release_feed.assert_awaited_once()

    async def test_releasing_feeds_cleaned_up_after_release(self) -> None:
        """_releasing_feeds is empty after release completes."""

        async def _one_chunk(feed, shutdown):
            yield b"audio"

        rt = NormalizerRuntime(capture_fn=_one_chunk, settings=_make_settings())
        rt._shutdown = asyncio.Event()
        rt._lease_lost = asyncio.Event()
        rt._store = mock.AsyncMock()
        rt._store.update_feed_progress.return_value = True
        rt._releasing_feeds = set()

        with _mock_upload_audio(), _mock_pubsub_publish():
            await rt._process_feed(_FEED)

        self.assertEqual(rt._releasing_feeds, set())


class TestHeartbeatCycle(unittest.IsolatedAsyncioTestCase):
    """Tests for _heartbeat_cycle."""

    @staticmethod
    def _diag(
        feed_id: uuid.UUID,
        *,
        worker: uuid.UUID = _WORKER_ID,
        status: str = "active",
        renewed: bool = True,
    ) -> HeartbeatResult:
        return HeartbeatResult(
            id=feed_id,
            current_worker=worker,
            current_status=status,
            renewed=renewed,
        )

    async def test_all_renewed_no_action(self) -> None:
        """When all feeds are renewed, no action is taken."""
        rt = _make_runtime()
        task = asyncio.create_task(asyncio.sleep(100))
        rt._feed_tasks[_FEED_ID] = task
        rt._releasing_feeds = set()
        rt._heartbeat_store = mock.AsyncMock()
        rt._heartbeat_store.renew_heartbeats_batch_diagnostic.return_value = [
            self._diag(_FEED_ID, renewed=True),
        ]

        await rt._heartbeat_cycle()

        self.assertFalse(task.cancelled())
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task

    async def test_lost_feeds_trigger_exit(self) -> None:
        """When any feed is lost from heartbeat renewal, os._exit is called."""
        other_worker = uuid.UUID("99999999-8888-7777-6666-555555555555")
        rt = _make_runtime()
        task = asyncio.create_task(asyncio.sleep(100))
        rt._feed_tasks[_FEED_ID] = task
        rt._releasing_feeds = set()
        rt._heartbeat_store = mock.AsyncMock()
        rt._heartbeat_store.renew_heartbeats_batch_diagnostic.return_value = [
            self._diag(_FEED_ID, worker=other_worker, renewed=False),
        ]

        with (
            mock.patch(
                "backend.pipeline.ingestion.normalizer_runtime.os._exit",
            ) as mock_exit,
            mock.patch("logging.shutdown"),
        ):
            await rt._heartbeat_cycle()
            mock_exit.assert_called_once_with(1)

        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task

    async def test_releasing_feeds_excluded_from_lost(self) -> None:
        """Feeds in _releasing_feeds are not flagged as lost."""
        other_worker = uuid.UUID("99999999-8888-7777-6666-555555555555")
        rt = _make_runtime()
        task = asyncio.create_task(asyncio.sleep(100))
        rt._feed_tasks[_FEED_ID] = task
        rt._releasing_feeds = {_FEED_ID}
        rt._heartbeat_store = mock.AsyncMock()
        rt._heartbeat_store.renew_heartbeats_batch_diagnostic.return_value = [
            self._diag(_FEED_ID, worker=other_worker, renewed=False),
        ]

        with mock.patch(
            "backend.pipeline.ingestion.normalizer_runtime.os._exit",
        ) as mock_exit:
            await rt._heartbeat_cycle()
            mock_exit.assert_not_called()

        self.assertFalse(task.cancelled())
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task

    async def test_done_tasks_excluded_from_lost(self) -> None:
        """Tasks that completed between snapshot and DB response are excluded."""
        other_worker = uuid.UUID("99999999-8888-7777-6666-555555555555")
        rt = _make_runtime()
        task = asyncio.create_task(asyncio.sleep(0))
        await task  # let it complete
        rt._feed_tasks[_FEED_ID] = task
        rt._releasing_feeds = set()
        rt._heartbeat_store = mock.AsyncMock()
        rt._heartbeat_store.renew_heartbeats_batch_diagnostic.return_value = [
            self._diag(_FEED_ID, worker=other_worker, renewed=False),
        ]

        with mock.patch(
            "backend.pipeline.ingestion.normalizer_runtime.os._exit",
        ) as mock_exit:
            await rt._heartbeat_cycle()
            mock_exit.assert_not_called()

    async def test_diagnostic_info_logged_on_fence_violation(self) -> None:
        """Per-feed diagnostic details are logged before termination."""
        other_worker = uuid.UUID("99999999-8888-7777-6666-555555555555")
        rt = _make_runtime()
        task = asyncio.create_task(asyncio.sleep(100))
        rt._feed_tasks[_FEED_ID] = task
        rt._releasing_feeds = set()
        rt._heartbeat_store = mock.AsyncMock()
        rt._heartbeat_store.renew_heartbeats_batch_diagnostic.return_value = [
            self._diag(
                _FEED_ID,
                worker=other_worker,
                status="active",
                renewed=False,
            ),
        ]

        with (
            mock.patch(
                "backend.pipeline.ingestion.normalizer_runtime.os._exit",
            ),
            mock.patch("logging.shutdown"),
            mock.patch(
                "backend.pipeline.ingestion.normalizer_runtime.logger",
            ) as mock_logger,
        ):
            await rt._heartbeat_cycle()

        # Should have logged per-feed diagnostic info
        critical_calls = [
            c for c in mock_logger.critical.call_args_list if "current_worker" in str(c)
        ]
        self.assertEqual(len(critical_calls), 1)
        self.assertIn(str(other_worker), str(critical_calls[0]))

        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task

    async def test_deleted_feed_logs_no_db_row(self) -> None:
        """A feed missing from DB results logs 'no DB row returned'."""
        rt = _make_runtime()
        task = asyncio.create_task(asyncio.sleep(100))
        rt._feed_tasks[_FEED_ID] = task
        rt._releasing_feeds = set()
        rt._heartbeat_store = mock.AsyncMock()
        # DB returns empty list — feed row was deleted
        rt._heartbeat_store.renew_heartbeats_batch_diagnostic.return_value = []

        with (
            mock.patch(
                "backend.pipeline.ingestion.normalizer_runtime.os._exit",
            ),
            mock.patch("logging.shutdown"),
            mock.patch(
                "backend.pipeline.ingestion.normalizer_runtime.logger",
            ) as mock_logger,
        ):
            await rt._heartbeat_cycle()

        critical_calls = [
            c
            for c in mock_logger.critical.call_args_list
            if "no DB row returned" in str(c)
        ]
        self.assertEqual(len(critical_calls), 1)
        self.assertIn(str(_FEED_ID), str(critical_calls[0]))

        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task


class TestMainPoolCreation(unittest.IsolatedAsyncioTestCase):
    """Tests for pool creation in _main."""

    @mock.patch(
        "backend.pipeline.ingestion.normalizer_runtime.FeedStore",
    )
    @mock.patch(
        "backend.pipeline.ingestion.normalizer_runtime.create_pool",
        new_callable=mock.AsyncMock,
    )
    async def test_heartbeat_pool_uses_create_pool_helper(
        self,
        mock_create_pool: mock.AsyncMock,
        mock_feed_store: mock.MagicMock,
    ) -> None:
        """Heartbeat pool must use create_pool helper with min/max_size=1."""
        rt = _make_runtime()

        with (
            mock.patch.object(rt, "_leasing_loop", new_callable=mock.AsyncMock),
            mock.patch.object(rt, "_shutdown_sequence", new_callable=mock.AsyncMock),
            mock.patch("threading.Thread"),
        ):
            await rt._main()

        self.assertEqual(mock_create_pool.call_count, 2)
        heartbeat_call = mock_create_pool.call_args_list[1]
        self.assertEqual(heartbeat_call.kwargs["min_size"], 1)
        self.assertEqual(heartbeat_call.kwargs["max_size"], 1)


class TestShutdownSequence(unittest.IsolatedAsyncioTestCase):
    """Tests for _shutdown_sequence."""

    async def test_cancels_all_tasks(self) -> None:
        """All feed tasks are cancelled during shutdown."""
        rt = _make_runtime()
        rt._shutdown = asyncio.Event()
        rt._thread_stop = mock.MagicMock()
        rt._heartbeat_thread = None
        rt._store = mock.AsyncMock()
        rt._data_pool = mock.AsyncMock()
        rt._heartbeat_pool = mock.AsyncMock()
        rt._pubsub_client = mock.AsyncMock()
        rt._gcs_client = mock.AsyncMock()

        task = asyncio.create_task(asyncio.sleep(1000))
        rt._feed_tasks[_FEED_ID] = task

        await rt._shutdown_sequence()

        self.assertTrue(task.cancelled())
        rt._pubsub_client.close.assert_awaited_once()
        rt._gcs_client.close.assert_awaited_once()

    async def test_closes_pools(self) -> None:
        """Both pools are closed during shutdown."""
        rt = _make_runtime()
        rt._shutdown = asyncio.Event()
        rt._thread_stop = mock.MagicMock()
        rt._heartbeat_thread = None
        rt._store = mock.AsyncMock()
        rt._data_pool = mock.AsyncMock()
        rt._heartbeat_pool = mock.AsyncMock()
        rt._pubsub_client = mock.AsyncMock()
        rt._gcs_client = mock.AsyncMock()

        with mock.patch(
            "backend.pipeline.ingestion.normalizer_runtime.close_pool",
            new_callable=mock.AsyncMock,
        ) as mock_close_pool:
            await rt._shutdown_sequence()

        rt._heartbeat_pool.close.assert_awaited_once()
        mock_close_pool.assert_awaited_once_with(rt._data_pool)
        rt._pubsub_client.close.assert_awaited_once()
        rt._gcs_client.close.assert_awaited_once()


class TestHeartbeatLoopSetsLeaseLost(unittest.IsolatedAsyncioTestCase):
    """Tests for _heartbeat_loop setting _lease_lost on exception."""

    async def test_heartbeat_exception_sets_lease_lost(self) -> None:
        """Transient heartbeat error sets _lease_lost via call_soon_threadsafe."""
        rt = _make_runtime()
        rt._loop = asyncio.get_running_loop()
        rt._thread_stop = mock.MagicMock()
        # Simulate: first wait returns False (tick), second returns True (stop).
        rt._thread_stop.is_set.side_effect = [False, True]
        rt._thread_stop.wait.return_value = False

        with (
            mock.patch.object(rt, "_heartbeat_cycle", return_value=None),
            mock.patch(
                "asyncio.run_coroutine_threadsafe",
            ) as mock_run,
        ):
            future = mock.MagicMock()
            future.result.side_effect = RuntimeError("DB gone")
            mock_run.return_value = future
            rt._heartbeat_loop()

        # _lease_lost should have been set via call_soon_threadsafe.
        # Since we're already on the event loop, we can check directly.
        # The call_soon_threadsafe was scheduled but we need to yield.
        await asyncio.sleep(0)
        self.assertTrue(rt._lease_lost.is_set())

    async def test_fence_violation_sets_lease_lost(self) -> None:
        """Fence violation in _heartbeat_cycle sets _lease_lost before exit."""
        other_worker = uuid.UUID("99999999-8888-7777-6666-555555555555")
        rt = _make_runtime()
        task = asyncio.create_task(asyncio.sleep(100))
        rt._feed_tasks[_FEED_ID] = task
        rt._releasing_feeds = set()
        rt._heartbeat_store = mock.AsyncMock()
        rt._heartbeat_store.renew_heartbeats_batch_diagnostic.return_value = [
            HeartbeatResult(
                id=_FEED_ID,
                current_worker=other_worker,
                current_status="active",
                renewed=False,
            ),
        ]

        with (
            mock.patch(
                "backend.pipeline.ingestion.normalizer_runtime.os._exit",
            ),
            mock.patch("logging.shutdown"),
        ):
            await rt._heartbeat_cycle()

        self.assertTrue(rt._lease_lost.is_set())

        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task


class TestProcessFeedRetry(unittest.IsolatedAsyncioTestCase):
    """Tests for _process_feed with transient upload failures triggering retry."""

    async def test_transient_upload_failure_retries_and_succeeds(self) -> None:
        """GCS upload fails once then succeeds — pipeline continues."""

        async def _one_chunk(feed, shutdown):
            yield b"audio"

        rt = NormalizerRuntime(capture_fn=_one_chunk, settings=_make_settings())
        rt._shutdown = asyncio.Event()
        rt._lease_lost = asyncio.Event()
        rt._store = mock.AsyncMock()
        rt._store.update_feed_progress.return_value = True
        rt._releasing_feeds = set()

        upload_mock = mock.AsyncMock(
            side_effect=[aiohttp.ClientError("transient"), "gs://b/p"],
        )

        with (
            mock.patch(
                "backend.pipeline.ingestion.normalizer_runtime.upload_audio",
                upload_mock,
            ),
            _mock_pubsub_publish(),
        ):
            await rt._process_feed(_FEED)

        self.assertEqual(upload_mock.await_count, 2)
        rt._store.release_feed.assert_awaited_once()

    async def test_lease_lost_during_upload_aborts_without_db_write(self) -> None:
        """LeaseExpiredError aborts cleanly — no report_feed_failure call."""

        async def _one_chunk(feed, shutdown):
            yield b"audio"

        rt = NormalizerRuntime(capture_fn=_one_chunk, settings=_make_settings())
        rt._shutdown = asyncio.Event()
        rt._lease_lost = asyncio.Event()
        rt._lease_lost.set()
        rt._store = mock.AsyncMock()
        rt._releasing_feeds = set()

        with (
            mock.patch(
                "backend.pipeline.ingestion.normalizer_runtime.upload_audio",
                mock.AsyncMock(return_value="gs://b/p"),
            ),
            _mock_pubsub_publish(),
        ):
            await rt._process_feed(_FEED)

        # LeaseExpiredError caught by dedicated handler — no DB write attempted
        rt._store.report_feed_failure.assert_not_awaited()
        rt._store.release_feed.assert_not_awaited()

    async def test_lease_lost_during_bookmark_backoff_aborts(self) -> None:
        """Lease loss during bookmark retry aborts without DB write."""

        async def _one_chunk(feed, shutdown):
            yield b"audio"

        rt = NormalizerRuntime(capture_fn=_one_chunk, settings=_make_settings())
        rt._shutdown = asyncio.Event()
        rt._lease_lost = asyncio.Event()
        rt._store = mock.AsyncMock()
        # Bookmark fails with a retryable error, then lease is lost
        rt._store.update_feed_progress.side_effect = asyncpg.InterfaceError(
            "connection lost"
        )
        rt._releasing_feeds = set()

        async def _set_lease_lost_soon() -> None:
            await asyncio.sleep(0.01)
            rt._lease_lost.set()

        with (
            mock.patch(
                "backend.pipeline.ingestion.normalizer_runtime.upload_audio",
                mock.AsyncMock(return_value="gs://b/p"),
            ),
            _mock_pubsub_publish(),
        ):
            task = asyncio.create_task(_set_lease_lost_soon())
            await rt._process_feed(_FEED)
            await task

        # LeaseExpiredError caught by dedicated handler — no DB write attempted
        rt._store.report_feed_failure.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
