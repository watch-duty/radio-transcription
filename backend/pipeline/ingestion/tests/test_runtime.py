from __future__ import annotations

import asyncio
import unittest
import uuid
from unittest import mock

from backend.pipeline.ingestion.normalizer_runtime import NormalizerRuntime
from backend.pipeline.schema_types.raw_audio_chunk_pb2 import AudioChunk
from backend.pipeline.storage.feed_store import LeasedFeed

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
    """Patch publisher.publish to return a future with a fixed message id."""
    publish_future = mock.MagicMock()
    publish_future.result.return_value = message_id
    return mock.patch(
        "backend.pipeline.ingestion.normalizer_runtime.publisher.publish",
        return_value=publish_future,
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
        "final_staging_bucket": "test-bucket",
        "pubsub_topic_path": "projects/p/topics/t",
        "db_pool_min_size": 2,
        "db_pool_max_size": 5,
        "db_host": "10.0.0.1",
        "db_port": 5432,
        "db_user": "user",
        "db_name": "db",
        "db_password": "pass",
        "db_command_timeout_sec": 30.0,
        "db_connect_timeout_sec": 10.0,
        "feed_failure_threshold": 3,
        "abandonment_window_sec": 60.0,
    }
    defaults.update(overrides)
    m = mock.MagicMock()
    m.configure_mock(**defaults)
    return m


def _make_runtime(**settings_overrides) -> NormalizerRuntime:
    """Build a runtime with a mock capture_fn and settings."""

    async def _dummy_capture(feed, shutdown):
        yield b"chunk"

    settings = _make_settings(**settings_overrides)
    return NormalizerRuntime(capture_fn=_dummy_capture, settings=settings)


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
        rt._store = mock.AsyncMock()
        rt._store.update_feed_progress.return_value = True
        rt._releasing_feeds = set()

        with _mock_upload_audio(), _mock_pubsub_publish():
            await rt._process_feed(_FEED)

        self.assertEqual(rt._releasing_feeds, set())


class TestProcessFeedTimestamps(unittest.IsolatedAsyncioTestCase):
    """Tests for _process_feed timestamp population."""

    async def test_sets_start_timestamp_on_audio_chunk(self) -> None:
        """The start_timestamp field must be populated before publishing."""

        async def _one_chunk(feed, shutdown):
            yield b"audio"

        rt = NormalizerRuntime(capture_fn=_one_chunk, settings=_make_settings())
        rt._shutdown = asyncio.Event()
        rt._store = mock.AsyncMock()
        rt._store.update_feed_progress.return_value = True
        rt._releasing_feeds = set()

        with (
            _mock_upload_audio(),
            _mock_pubsub_publish() as mock_publish,
        ):
            await rt._process_feed(_FEED)

            # 1. Get the published data
            mock_publish.assert_called_once()
            _, args, _ = mock_publish.mock_calls[0]
            published_bytes = args[1]

            # 2. Parse back and verify
            chunk = AudioChunk()
            chunk.ParseFromString(published_bytes)

            self.assertTrue(chunk.HasField("start_timestamp"))
            # Just ensure it's not the default (epoch 0)
            self.assertGreater(chunk.start_timestamp.seconds, 1700000000)


class TestHeartbeatCycle(unittest.IsolatedAsyncioTestCase):
    """Tests for _heartbeat_cycle."""

    async def test_all_renewed_no_action(self) -> None:
        """When all feeds are renewed, no action is taken."""
        rt = _make_runtime()
        task = asyncio.create_task(asyncio.sleep(100))
        rt._feed_tasks[_FEED_ID] = task
        rt._releasing_feeds = set()
        rt._heartbeat_store = mock.AsyncMock()
        rt._heartbeat_store.renew_heartbeats_batch.return_value = {_FEED_ID}

        await rt._heartbeat_cycle()

        self.assertFalse(task.cancelled())
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task

    async def test_lost_feeds_trigger_exit(self) -> None:
        """When any feed is lost from heartbeat renewal, os._exit is called."""
        rt = _make_runtime()
        task = asyncio.create_task(asyncio.sleep(100))
        rt._feed_tasks[_FEED_ID] = task
        rt._releasing_feeds = set()
        rt._heartbeat_store = mock.AsyncMock()
        rt._heartbeat_store.renew_heartbeats_batch.return_value = set()

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
        rt = _make_runtime()
        task = asyncio.create_task(asyncio.sleep(100))
        rt._feed_tasks[_FEED_ID] = task
        rt._releasing_feeds = {_FEED_ID}
        rt._heartbeat_store = mock.AsyncMock()
        rt._heartbeat_store.renew_heartbeats_batch.return_value = set()

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
        rt = _make_runtime()
        task = asyncio.create_task(asyncio.sleep(0))
        await task  # let it complete
        rt._feed_tasks[_FEED_ID] = task
        rt._releasing_feeds = set()
        rt._heartbeat_store = mock.AsyncMock()
        rt._heartbeat_store.renew_heartbeats_batch.return_value = set()

        with mock.patch(
            "backend.pipeline.ingestion.normalizer_runtime.os._exit",
        ) as mock_exit:
            await rt._heartbeat_cycle()
            mock_exit.assert_not_called()


class TestMainPoolCreation(unittest.IsolatedAsyncioTestCase):
    """Tests for pool creation in _main."""

    @mock.patch(
        "backend.pipeline.ingestion.normalizer_runtime.FeedStore",
    )
    @mock.patch(
        "backend.pipeline.ingestion.normalizer_runtime.asyncpg.create_pool",
        new_callable=mock.AsyncMock,
    )
    @mock.patch(
        "backend.pipeline.ingestion.normalizer_runtime.create_pool",
        new_callable=mock.AsyncMock,
    )
    async def test_heartbeat_pool_disables_statement_cache(
        self,
        mock_create_pool: mock.AsyncMock,
        mock_asyncpg_create_pool: mock.AsyncMock,
        mock_feed_store: mock.MagicMock,
    ) -> None:
        """Heartbeat pool must use statement_cache_size=0 for PgBouncer compat."""
        rt = _make_runtime()

        with (
            mock.patch.object(rt, "_leasing_loop", new_callable=mock.AsyncMock),
            mock.patch.object(rt, "_shutdown_sequence", new_callable=mock.AsyncMock),
            mock.patch("threading.Thread"),
        ):
            await rt._main()

        mock_asyncpg_create_pool.assert_called_once()
        call_kwargs = mock_asyncpg_create_pool.call_args.kwargs
        self.assertEqual(call_kwargs["statement_cache_size"], 0)


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

        task = asyncio.create_task(asyncio.sleep(1000))
        rt._feed_tasks[_FEED_ID] = task

        with mock.patch(
            "backend.pipeline.ingestion.normalizer_runtime.close_client",
            new_callable=mock.AsyncMock,
        ):
            await rt._shutdown_sequence()

        self.assertTrue(task.cancelled())

    async def test_closes_pools(self) -> None:
        """Both pools are closed during shutdown."""
        rt = _make_runtime()
        rt._shutdown = asyncio.Event()
        rt._thread_stop = mock.MagicMock()
        rt._heartbeat_thread = None
        rt._store = mock.AsyncMock()
        rt._data_pool = mock.AsyncMock()
        rt._heartbeat_pool = mock.AsyncMock()

        with (
            mock.patch(
                "backend.pipeline.ingestion.normalizer_runtime.close_client",
                new_callable=mock.AsyncMock,
            ),
            mock.patch(
                "backend.pipeline.ingestion.normalizer_runtime.close_pool",
                new_callable=mock.AsyncMock,
            ) as mock_close_pool,
        ):
            await rt._shutdown_sequence()

        rt._heartbeat_pool.close.assert_awaited_once()
        mock_close_pool.assert_awaited_once_with(rt._data_pool)


if __name__ == "__main__":
    unittest.main()
