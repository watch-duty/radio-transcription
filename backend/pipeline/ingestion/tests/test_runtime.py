from __future__ import annotations

import asyncio
import datetime
import unittest
import uuid
from unittest import mock

import aiohttp
import asyncpg

from backend.pipeline.common.constants import CHUNK_DURATION_SECONDS
from backend.pipeline.ingestion.normalizer_runtime import (
    CapturedChunk,
    NormalizerRuntime,
)
from backend.pipeline.storage.feed_store import (
    HeartbeatResult,
    LeasedFeed,
    SourceType,
)
from backend.pipeline.storage.settings import AlloyDBSettings

_WORKER_ID = uuid.UUID("11111111-2222-3333-4444-555555555555")
_FEED_ID = uuid.UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")


def _make_captured_chunk(audio_bytes: bytes) -> CapturedChunk:
    """Build a CapturedChunk with a current timestamp and a 15-second window."""
    now = datetime.datetime.now(datetime.UTC)
    return CapturedChunk(
        audio_bytes=audio_bytes,
        chunk_start_time=now,
        chunk_end_time=now + datetime.timedelta(seconds=CHUNK_DURATION_SECONDS),
    )


_FEED = LeasedFeed(
    id=_FEED_ID,
    name="Test Feed",
    external_id="ext-id",
    source_type=SourceType.BCFY_FEEDS,
    last_processed_filename=None,
    last_bookmark_time=None,
    fencing_token=1,
    source_feed_id="123",
)


def _mock_pubsub_publish(message_id: str = "test-message-id") -> mock._patch:
    """Patch publish_audio_chunk to return a fixed message id (at call site)."""
    return mock.patch(
        "backend.pipeline.ingestion.normalizer_runtime.gcp_helper.publish_audio_chunk",
        new_callable=mock.AsyncMock,
        return_value=message_id,
    )


def _mock_upload_audio(gcs_path: str = "gs://b/p") -> mock._patch:
    """Patch upload_staged_audio to return a deterministic GCS path.

    _process_feed calls gcp_helper.upload_staged_audio (the entry point
    the production pipeline uses), not the inner gcp_helper.upload_audio.
    Patch the entry point directly so tests assert behavior at the same
    boundary the code actually invokes.
    """
    return mock.patch(
        "backend.pipeline.ingestion.normalizer_runtime.gcp_helper.upload_staged_audio",
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
        "continuous_pubsub_topic_path": "projects/p/topics/t",
        "db": AlloyDBSettings(
            host="10.0.0.1",
            port=6432,
            user="user",
            db_name="db",
            password="pass",
            pool_min_size=2,
            pool_max_size=5,
            command_timeout_sec=30.0,
            connect_timeout_sec=10.0,
        ),
        "google_cloud_project": None,
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
        # Real values so health_server doesn't try to bind the MagicMock-default
        # port 1 when a test exercises _main().
        "health_check_port": 8080,
        "health_check_startup_grace_sec": 120.0,
        # Per-type claim caps + ramp + SIGTERM release settings — must be
        # real ints/floats so min()/random.uniform()/arithmetic don't blow
        # up on MagicMock auto-created attributes.
        "cap_bcfy_feeds": 240,
        "cap_bcfy_calls": 600,
        "cap_openmhz": 900,
        "claim_ramp_pct": 100,
        "sigterm_release_batch_size": 50,
        "sigterm_release_jitter_max_sec": 2.0,
    }
    defaults.update(overrides)
    m = mock.MagicMock()
    m.configure_mock(**defaults)
    return m


def _make_runtime(**settings_overrides) -> NormalizerRuntime:
    """Build a runtime with a mock capture_fn and settings."""

    async def _dummy_capture(feed, shutdown):
        yield _make_captured_chunk(b"chunk")

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


class TestLeasingLoopOrphanedTask(unittest.IsolatedAsyncioTestCase):
    """Tests for orphaned task cancellation during re-lease."""

    async def test_released_feed_cancels_orphaned_task(self) -> None:
        """Re-leasing a feed cancels the still-running old task."""
        rt = _make_runtime()
        rt._shutdown = asyncio.Event()
        rt._store = mock.AsyncMock()
        rt._releasing_feeds = set()

        # Simulate an existing running task for the same feed
        old_task = asyncio.create_task(asyncio.sleep(1000))
        rt._feed_tasks[_FEED_ID] = old_task

        # Simulate acquire_feeds_batch returning the same feed (re-leased)
        rt._store.acquire_feeds_batch.return_value = [_FEED]

        # Patch _process_feed to avoid running the real pipeline
        with mock.patch.object(
            rt, "_process_feed", new_callable=mock.AsyncMock
        ):
            # Run one iteration: reap, acquire, sleep → shutdown
            rt._store.acquire_feeds_batch.side_effect = [
                [_FEED],  # first call returns re-leased feed
                asyncio.CancelledError,  # stop the loop
            ]
            rt._shutdown.set()  # stop after first iteration
            await rt._leasing_loop()

        # Yield to let the event loop process the cancellation
        # (Python 3.12+ makes task cancellation strictly cooperative)
        await asyncio.sleep(0)
        # Old task must have been cancelled
        self.assertTrue(old_task.cancelled())
        # New task must be in _feed_tasks (not the old one)
        self.assertIn(_FEED_ID, rt._feed_tasks)
        self.assertIsNot(rt._feed_tasks[_FEED_ID], old_task)

    async def test_total_slack_bounded_across_branches(self) -> None:
        """Sum of per-branch LIMITs must not exceed total_slack (cold start).

        Regression: without the round-robin apportion, three branches each
        get `min(cap, total_slack)`, so at max_feeds_per_worker=250 the
        query could legitimately return 250 + 250 + 250 = 740 feeds and
        blow past the worker budget. The apportion must guarantee
        sum(limits) <= total_slack.
        """
        rt = _make_runtime(
            max_feeds_per_worker=250,
            cap_bcfy_feeds=240,
            cap_bcfy_calls=600,
            cap_openmhz=900,
        )
        rt._shutdown = asyncio.Event()
        rt._store = mock.AsyncMock()
        rt._releasing_feeds = set()
        rt._store.acquire_feeds_batch.side_effect = [
            [],  # empty result so no tasks spawn
            asyncio.CancelledError,
        ]
        rt._store.acquire_feeds_recovery.return_value = []

        rt._shutdown.set()
        await rt._leasing_loop()

        # Inspect the call made to acquire_feeds_batch; args[2:5] are the
        # three per-type LIMITs.
        call = rt._store.acquire_feeds_batch.await_args_list[0]
        limits = call[0][2:5]
        self.assertEqual(sum(limits), 250)  # exactly total_slack
        self.assertTrue(
            all(limit >= 0 for limit in limits),
            "no branch should receive a negative LIMIT",
        )

    async def test_already_done_orphan_rerelease_does_not_leak(self) -> None:
        """Done task at re-lease time must be cleaned up, not silently dropped.

        Regression: if the old task finished between `_reap_completed_tasks`
        and the next `acquire_feeds_batch` return, `existing.done()` is True
        and the cancellation branch used to short-circuit — which skipped
        the _held_by_type decrement and left the old task unreachable from
        the reaper (overwritten in _feed_tasks). That leaked 1 slot of
        that source_type per occurrence.
        """
        rt = _make_runtime()
        rt._shutdown = asyncio.Event()
        rt._store = mock.AsyncMock()
        rt._releasing_feeds = set()

        # Pre-existing DONE task for _FEED. Use a task that has already
        # completed (no exception).
        done_task: asyncio.Task[None] = asyncio.create_task(asyncio.sleep(0))
        await done_task
        assert done_task.done()
        rt._feed_tasks[_FEED_ID] = done_task
        rt._task_source_types[_FEED_ID] = SourceType.BCFY_FEEDS
        rt._held_by_type[SourceType.BCFY_FEEDS] = 1

        rt._store.acquire_feeds_batch.side_effect = [
            [_FEED],
            asyncio.CancelledError,
        ]
        rt._store.acquire_feeds_recovery.return_value = []

        with mock.patch.object(
            rt, "_process_feed", new_callable=mock.AsyncMock
        ):
            rt._shutdown.set()
            await rt._leasing_loop()

        # One feed held → count is 1, not 2 (would be 2 before the fix
        # because the done-task path skipped the decrement).
        self.assertEqual(rt._held_by_type[SourceType.BCFY_FEEDS], 1)

    async def test_orphan_rerelease_does_not_leak_held_by_type(self) -> None:
        """Re-leasing a held feed must not double-count _held_by_type.

        Regression: the old task becomes unreachable from _reap_completed_tasks
        once _feed_tasks[lease["id"]] is overwritten, so its decrement never
        fires while the new task's +=1 does. Without the inline decrement at
        cancel time, _held_by_type would leak one slot per re-lease and
        structurally starve the per-type cap over time.
        """
        rt = _make_runtime()
        rt._shutdown = asyncio.Event()
        rt._store = mock.AsyncMock()
        rt._releasing_feeds = set()

        # Pre-populate state to look like the worker already holds _FEED
        # (source_type=BCFY_FEEDS) with a running task.
        old_task = asyncio.create_task(asyncio.sleep(1000))
        rt._feed_tasks[_FEED_ID] = old_task
        rt._task_source_types[_FEED_ID] = SourceType.BCFY_FEEDS
        rt._held_by_type[SourceType.BCFY_FEEDS] = 1

        # Single leasing cycle: returns the same feed (re-lease). Recovery
        # path returns empty so it doesn't interfere.
        rt._store.acquire_feeds_batch.side_effect = [
            [_FEED],
            asyncio.CancelledError,
        ]
        rt._store.acquire_feeds_recovery.return_value = []

        with mock.patch.object(
            rt, "_process_feed", new_callable=mock.AsyncMock
        ):
            rt._shutdown.set()
            await rt._leasing_loop()

        # Let the cancellation propagate.
        await asyncio.sleep(0)

        # The invariant that matters: one feed held → count is 1, not 2.
        # Before the fix, this would be 2 because the old task's decrement
        # was routed through the reaper, which never sees the old task
        # after _feed_tasks[_FEED_ID] got overwritten.
        self.assertEqual(rt._held_by_type[SourceType.BCFY_FEEDS], 1)
        # And _task_source_types still has exactly the new task's entry.
        self.assertEqual(rt._task_source_types[_FEED_ID], SourceType.BCFY_FEEDS)
        self.assertTrue(old_task.cancelled())


class TestProcessFeedFenceViolation(unittest.IsolatedAsyncioTestCase):
    """Tests for _process_feed fence violation."""

    async def test_bookmark_fence_failure_exits_process(self) -> None:
        """When bookmark fence fails, os._exit is called."""

        async def _one_chunk(feed, shutdown):
            yield _make_captured_chunk(b"audio")

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
            yield _make_captured_chunk(b"audio")

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
            yield _make_captured_chunk(b"audio")

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
            yield _make_captured_chunk(b"audio")

        rt = NormalizerRuntime(capture_fn=_one_chunk, settings=_make_settings())
        rt._shutdown = asyncio.Event()
        rt._lease_lost = asyncio.Event()
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
            yield _make_captured_chunk(b"audio")

        rt = NormalizerRuntime(capture_fn=_one_chunk, settings=_make_settings())
        rt._shutdown = asyncio.Event()
        rt._lease_lost = asyncio.Event()
        rt._store = mock.AsyncMock()
        rt._store.update_feed_progress.return_value = True
        rt._releasing_feeds = set()

        with (
            _mock_upload_audio(),
            _mock_pubsub_publish() as mock_publish,
        ):
            await rt._process_feed(_FEED)

            mock_publish.assert_called_once()
            _, args, kwargs = mock_publish.mock_calls[0]

            self.assertEqual(len(args), 6)
            self.assertEqual(
                args[1], rt._normalizer_settings.continuous_pubsub_topic_path
            )
            self.assertEqual(args[2], str(_FEED["id"]))
            self.assertEqual(args[3], "Test Feed")
            self.assertEqual(args[4], "ext-id")
            self.assertTrue(args[5].startswith("gs://"))

            self.assertIn("start_timestamp", kwargs)
            self.assertIsNotNone(kwargs["start_timestamp"])
            self.assertIsInstance(kwargs["start_timestamp"], datetime.datetime)
            self.assertGreater(
                kwargs["start_timestamp"].timestamp(), 1700000000
            )


class TestProcessFeedSessionId(unittest.IsolatedAsyncioTestCase):
    """Tests for _process_feed session ID population."""

    async def test_session_id_populated_and_identical_across_chunks(
        self,
    ) -> None:
        """The session_id field must be populated and identical for all chunks in a session."""

        async def _two_chunks(feed, shutdown):
            yield _make_captured_chunk(b"audio1")
            yield _make_captured_chunk(b"audio2")

        rt = NormalizerRuntime(
            capture_fn=_two_chunks, settings=_make_settings()
        )
        rt._shutdown = asyncio.Event()
        rt._lease_lost = asyncio.Event()
        rt._store = mock.AsyncMock()
        rt._store.update_feed_progress.return_value = True
        rt._releasing_feeds = set()

        with (
            _mock_upload_audio(),
            _mock_pubsub_publish() as mock_publish,
        ):
            await rt._process_feed(_FEED)

            self.assertEqual(mock_publish.call_count, 2)

            _, _, kwargs1 = mock_publish.mock_calls[0]
            _, _, kwargs2 = mock_publish.mock_calls[1]

            self.assertIn("session_id", kwargs1)
            self.assertIn("session_id", kwargs2)
            self.assertTrue(len(kwargs1["session_id"]) > 0)
            self.assertEqual(kwargs1["session_id"], kwargs2["session_id"])


class TestProcessFeedTopicRouting(unittest.IsolatedAsyncioTestCase):
    """Tests for _process_feed topic routing based on SourceType."""

    async def test_routes_continuous_feed_to_default_topic(self) -> None:
        """Continuous feeds (BCFY_FEEDS) go to continuous_pubsub_topic_path."""

        async def _one_chunk(feed, shutdown):
            yield _make_captured_chunk(b"audio")

        rt = _make_runtime(
            continuous_pubsub_topic_path="projects/p/topics/continuous"
        )
        rt._shutdown = asyncio.Event()
        rt._store = mock.AsyncMock()
        rt._store.update_feed_progress.return_value = True
        rt._releasing_feeds = set()

        with _mock_upload_audio(), _mock_pubsub_publish() as mock_publish:
            await rt._process_feed(_FEED)  # _FEED is BCFY_FEEDS

            mock_publish.assert_called_once()
            _, args, _ = mock_publish.mock_calls[0]
            self.assertEqual(args[1], "projects/p/topics/continuous")

    async def test_routes_segmented_feed_to_segmented_topic(self) -> None:
        """Segmented feeds (not BCFY_FEEDS) go to segmented_pubsub_topic_path."""

        async def _one_chunk(feed, shutdown):
            yield _make_captured_chunk(b"audio")

        rt = _make_runtime(
            continuous_pubsub_topic_path="projects/p/topics/continuous",
            segmented_pubsub_topic_path="projects/p/topics/segmented",
        )
        rt._shutdown = asyncio.Event()
        rt._store = mock.AsyncMock()
        rt._store.update_feed_progress.return_value = True
        rt._releasing_feeds = set()

        segmented_feed = LeasedFeed(
            id=_FEED_ID,
            name="Test Feed",
            external_id="ext-id",
            source_type=SourceType.OPENMHZ,  # Not BCFY_FEEDS
            last_processed_filename=None,
            last_bookmark_time=None,
            fencing_token=1,
            source_feed_id="123",
        )

        with _mock_upload_audio(), _mock_pubsub_publish() as mock_publish:
            await rt._process_feed(segmented_feed)

            mock_publish.assert_called_once()
            _, args, _ = mock_publish.mock_calls[0]
            self.assertEqual(args[1], "projects/p/topics/segmented")

    async def test_raises_if_segmented_topic_missing(self) -> None:
        """Raises ValueError if segmented feed processed but segmented topic missing."""

        async def _one_chunk(feed, shutdown):
            yield _make_captured_chunk(b"audio")

        rt = _make_runtime(
            continuous_pubsub_topic_path="projects/p/topics/continuous",
            segmented_pubsub_topic_path=None,  # Missing
        )
        rt._shutdown = asyncio.Event()
        rt._store = mock.AsyncMock()
        rt._releasing_feeds = set()

        segmented_feed = LeasedFeed(
            id=_FEED_ID,
            name="Test Feed",
            external_id="ext-id",
            source_type=SourceType.OPENMHZ,
            last_processed_filename=None,
            last_bookmark_time=None,
            fencing_token=1,
            source_feed_id="123",
        )

        with _mock_upload_audio(), _mock_pubsub_publish():
            with self.assertRaises(ValueError) as context:
                await rt._process_feed(segmented_feed)
            self.assertIn(
                "Segmented Pub/Sub topic path not configured",
                str(context.exception),
            )


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

    async def test_skip_if_recent_is_not_a_fence_violation(self) -> None:
        """renewed=False + current_worker=self (skip-if-recent) must not trigger os._exit."""
        rt = _make_runtime()
        task = asyncio.create_task(asyncio.sleep(100))
        rt._feed_tasks[_FEED_ID] = task
        rt._releasing_feeds = set()
        rt._heartbeat_store = mock.AsyncMock()
        rt._heartbeat_store.renew_heartbeats_batch_diagnostic.return_value = [
            # Skip-if-recent short-circuits the UPDATE inside the SQL — we
            # still own the feed (current_worker=_WORKER_ID) but renewed is
            # False because last_heartbeat was <15 s fresh.
            self._diag(_FEED_ID, worker=_WORKER_ID, renewed=False),
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
            c
            for c in mock_logger.critical.call_args_list
            if "current_worker" in str(c)
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

    async def test_stamp_updated_on_empty_feeds(self) -> None:
        """last_heartbeat_tick is stamped when the cycle dispatches with no feeds."""
        rt = _make_runtime()
        rt._feed_tasks = {}
        rt._heartbeat_store = mock.AsyncMock()

        self.assertIsNone(rt._health_state.last_heartbeat_tick)
        await rt._heartbeat_cycle()

        self.assertIsNotNone(rt._health_state.last_heartbeat_tick)
        # DB wasn't called because no active feeds.
        rt._heartbeat_store.renew_heartbeats_batch_diagnostic.assert_not_called()

    async def test_stamp_updated_even_when_db_raises(self) -> None:
        """
        Critical: last_heartbeat_tick is stamped at cycle dispatch (before
        the DB call), so a transient AlloyDB outage doesn't age the stamp
        and trigger fleet-wide autohealer kills (thundering herd). Regression
        guard for the original design where the stamp was only set on
        successful DB renewal.
        """
        rt = _make_runtime()
        task = asyncio.create_task(asyncio.sleep(100))
        rt._feed_tasks[_FEED_ID] = task
        rt._releasing_feeds = set()
        rt._heartbeat_store = mock.AsyncMock()
        # Simulate AlloyDB outage: DB call raises.
        rt._heartbeat_store.renew_heartbeats_batch_diagnostic.side_effect = (
            asyncpg.exceptions.CannotConnectNowError("AlloyDB unavailable")
        )

        self.assertIsNone(rt._health_state.last_heartbeat_tick)

        with self.assertRaises(asyncpg.exceptions.CannotConnectNowError):
            await rt._heartbeat_cycle()

        # Despite the DB raising, the stamp was set before the await —
        # /healthz will still return 200 and the worker rides out the outage.
        self.assertIsNotNone(rt._health_state.last_heartbeat_tick)

        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task


class TestMainPoolCreation(unittest.IsolatedAsyncioTestCase):
    """Tests for pool creation in _main."""

    @mock.patch(
        "backend.pipeline.ingestion.normalizer_runtime.FeedStore",
    )
    @mock.patch(
        "backend.pipeline.ingestion.normalizer_runtime.create_pool_with_retry",
        new_callable=mock.AsyncMock,
    )
    async def test_heartbeat_pool_uses_create_pool_helper(
        self,
        mock_create_pool_with_retry: mock.AsyncMock,
        mock_feed_store: mock.MagicMock,
    ) -> None:
        """Heartbeat pool must use create_pool_with_retry helper with min/max_size=1."""
        rt = _make_runtime()

        with (
            mock.patch.object(rt, "_leasing_loop", new_callable=mock.AsyncMock),
            mock.patch.object(
                rt, "_shutdown_sequence", new_callable=mock.AsyncMock
            ),
            mock.patch("threading.Thread"),
            mock.patch(
                "backend.pipeline.ingestion.normalizer_runtime.health_server.start",
                new_callable=mock.AsyncMock,
            ),
        ):
            await rt._main()

        self.assertEqual(mock_create_pool_with_retry.call_count, 2)
        heartbeat_call = mock_create_pool_with_retry.call_args_list[1]
        hb_settings = heartbeat_call.args[0]
        self.assertEqual(hb_settings.pool_min_size, 1)
        self.assertEqual(hb_settings.pool_max_size, 1)


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

    async def test_health_runner_cleanup_runs_before_heartbeat_stop(
        self,
    ) -> None:
        """
        Ordering invariant: /healthz server must be stopped before the heartbeat
        thread is signaled to stop. Probes get a clean connection-refused during
        the shutdown window rather than hanging on a socket whose event loop is
        about to drain.
        """
        rt = _make_runtime()
        rt._shutdown = asyncio.Event()
        rt._thread_stop = mock.MagicMock()
        rt._heartbeat_thread = None
        rt._store = mock.AsyncMock()
        rt._data_pool = mock.AsyncMock()
        rt._heartbeat_pool = mock.AsyncMock()
        rt._pubsub_client = mock.AsyncMock()
        rt._gcs_client = mock.AsyncMock()
        rt._health_runner = mock.AsyncMock()

        call_order: list[str] = []
        rt._health_runner.cleanup.side_effect = lambda: call_order.append(
            "cleanup"
        )
        rt._thread_stop.set.side_effect = lambda: call_order.append("stop")

        await rt._shutdown_sequence()

        rt._health_runner.cleanup.assert_awaited_once()
        self.assertEqual(call_order, ["cleanup", "stop"])

    async def test_health_runner_cleanup_failure_does_not_skip_heartbeat_stop(
        self,
    ) -> None:
        """
        If runner.cleanup() raises, the rest of shutdown must still run — we
        need to signal the heartbeat thread and release leases even if the
        /healthz server couldn't be torn down cleanly.
        """
        rt = _make_runtime()
        rt._shutdown = asyncio.Event()
        rt._thread_stop = mock.MagicMock()
        rt._heartbeat_thread = None
        rt._store = mock.AsyncMock()
        rt._data_pool = mock.AsyncMock()
        rt._heartbeat_pool = mock.AsyncMock()
        rt._pubsub_client = mock.AsyncMock()
        rt._gcs_client = mock.AsyncMock()
        rt._health_runner = mock.AsyncMock()
        rt._health_runner.cleanup.side_effect = RuntimeError("boom")

        await rt._shutdown_sequence()

        rt._thread_stop.set.assert_called_once()
        # With an empty _feed_tasks the release path iterates 0 times and
        # never calls release_feeds_batch_by_ids. The invariant we care
        # about is that cleanup continues past the health_runner boom.
        self.assertEqual(len(rt._feed_tasks), 0)


class TestCalculateBranchLimits(unittest.TestCase):
    """Water-filling apportion: sum(limits) <= total_slack, no starvation."""

    CAPS = {
        SourceType.BCFY_FEEDS: 240,
        SourceType.BCFY_CALLS: 600,
        SourceType.OPENMHZ: 900,
    }

    def test_cold_start_bounds_sum_at_total_slack(self) -> None:
        # max_feeds_per_worker=250, all held=0 → sum must be exactly 250.
        held = dict.fromkeys(self.CAPS, 0)
        limits = NormalizerRuntime._calculate_branch_limits(250, self.CAPS, held)
        self.assertEqual(sum(limits.values()), 250)
        self.assertTrue(all(v >= 0 for v in limits.values()))

    def test_plan_target_800_bounds_sum(self) -> None:
        # max_feeds_per_worker=800 (scaling-plan target), all held=0.
        held = dict.fromkeys(self.CAPS, 0)
        limits = NormalizerRuntime._calculate_branch_limits(800, self.CAPS, held)
        self.assertEqual(sum(limits.values()), 800)

    def test_slack_exceeds_cap_sum_clamps_at_caps(self) -> None:
        # total_slack=2000 > sum(caps)=1740 → each branch gets its cap,
        # leftover slack is unassigned.
        held = dict.fromkeys(self.CAPS, 0)
        limits = NormalizerRuntime._calculate_branch_limits(2000, self.CAPS, held)
        self.assertEqual(limits[SourceType.BCFY_FEEDS], 240)
        self.assertEqual(limits[SourceType.BCFY_CALLS], 600)
        self.assertEqual(limits[SourceType.OPENMHZ], 900)
        self.assertEqual(sum(limits.values()), 1740)

    def test_type_at_cap_yields_zero_for_that_branch(self) -> None:
        held = {
            SourceType.BCFY_FEEDS: 240,
            SourceType.BCFY_CALLS: 0,
            SourceType.OPENMHZ: 0,
        }
        limits = NormalizerRuntime._calculate_branch_limits(250, self.CAPS, held)
        self.assertEqual(limits[SourceType.BCFY_FEEDS], 0)
        self.assertEqual(sum(limits.values()), 250)

    def test_small_headroom_branch_redistributes_to_larger(self) -> None:
        # bcfy_feeds has only 10 headroom; slack=300 must go mostly to
        # bcfy_calls + openmhz.
        held = {
            SourceType.BCFY_FEEDS: 230,
            SourceType.BCFY_CALLS: 0,
            SourceType.OPENMHZ: 0,
        }
        limits = NormalizerRuntime._calculate_branch_limits(300, self.CAPS, held)
        self.assertLessEqual(limits[SourceType.BCFY_FEEDS], 10)
        self.assertEqual(sum(limits.values()), 300)

    def test_zero_slack_returns_all_zeros(self) -> None:
        held = dict.fromkeys(self.CAPS, 0)
        limits = NormalizerRuntime._calculate_branch_limits(0, self.CAPS, held)
        self.assertEqual(limits, dict.fromkeys(self.CAPS, 0))

    def test_negative_held_defensive_does_not_overrun_cap(self) -> None:
        # Corrupted state: decrement bug made held negative. max() clamp
        # must prevent the "cap - held > cap" hazard.
        held = {
            SourceType.BCFY_FEEDS: -5,
            SourceType.BCFY_CALLS: 0,
            SourceType.OPENMHZ: 0,
        }
        limits = NormalizerRuntime._calculate_branch_limits(1000, self.CAPS, held)
        # Each branch still bounded at its cap even with corrupted held.
        self.assertLessEqual(limits[SourceType.BCFY_FEEDS], 240)
        self.assertLessEqual(limits[SourceType.BCFY_CALLS], 600)
        self.assertLessEqual(limits[SourceType.OPENMHZ], 900)


class TestBatchedSigtermRelease(unittest.IsolatedAsyncioTestCase):
    """Tests for the batched+jittered SIGTERM release path."""

    async def test_release_fires_in_batches(self) -> None:
        """120 leases + batch=50 → 3 calls with sizes [50, 50, 20]."""
        rt = _make_runtime(sigterm_release_batch_size=50)
        rt._shutdown = asyncio.Event()
        rt._thread_stop = mock.MagicMock()
        rt._heartbeat_thread = None
        rt._store = mock.AsyncMock()
        rt._store.release_feeds_batch_by_ids.return_value = 50
        rt._data_pool = mock.AsyncMock()
        rt._heartbeat_pool = mock.AsyncMock()
        rt._pubsub_client = mock.AsyncMock()
        rt._gcs_client = mock.AsyncMock()
        rt._health_runner = mock.AsyncMock()

        # Populate 120 feed tasks + matching source_type entries. Use
        # async-no-op tasks that we cancel ourselves so shutdown sees them
        # as completed.
        feed_ids = [uuid.uuid4() for _ in range(120)]
        for fid in feed_ids:
            t = asyncio.create_task(asyncio.sleep(0))
            rt._feed_tasks[fid] = t
            rt._task_source_types[fid] = (
                rt._task_source_types.get(fid)
                or list(rt._held_by_type.keys())[0]
            )
        await asyncio.gather(*rt._feed_tasks.values(), return_exceptions=True)

        # Replace asyncio.sleep in the runtime module so jitter is
        # deterministic and we can count calls.
        sleep_calls: list[float] = []

        async def _fake_sleep(secs: float) -> None:
            sleep_calls.append(secs)

        with mock.patch(
            "backend.pipeline.ingestion.normalizer_runtime.asyncio.sleep",
            new=_fake_sleep,
        ):
            await rt._shutdown_sequence()

        # Three batches with sizes [50, 50, 20].
        calls = rt._store.release_feeds_batch_by_ids.await_args_list
        self.assertEqual(len(calls), 3)
        self.assertEqual(len(calls[0][0][1]), 50)
        self.assertEqual(len(calls[1][0][1]), 50)
        self.assertEqual(len(calls[2][0][1]), 20)

        # One jitter sleep (pre-drain, before the first batch), not per
        # batch. 120 feeds / batch=50 = 3 batches, but we only want a
        # single fleet-wide-stagger sleep regardless of batch count so
        # shutdown duration stays deterministic.
        jitter_sleeps = [
            s for s in sleep_calls
            if 0 <= s <= rt._normalizer_settings.sigterm_release_jitter_max_sec
        ]
        self.assertEqual(
            len(jitter_sleeps), 1,
            f"expected exactly 1 pre-drain jitter sleep; saw {sleep_calls}",
        )

        # Parallel state scrubbed.
        self.assertEqual(rt._task_source_types, {})


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
            mock.patch.object(rt, "_heartbeat_cycle"),
            mock.patch(
                "asyncio.run_coroutine_threadsafe",
            ) as mock_run,
        ):
            future = mock.MagicMock()
            future.result.side_effect = RuntimeError("DB gone")
            mock_run.return_value = future
            rt._heartbeat_loop()

            # Prevent "coroutine was never awaited" warning
            coro = mock_run.call_args[0][0]
            coro.close()

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
            yield _make_captured_chunk(b"audio")

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
                "backend.pipeline.ingestion.normalizer_runtime.gcp_helper.upload_staged_audio",
                upload_mock,
            ),
            _mock_pubsub_publish(),
        ):
            await rt._process_feed(_FEED)

        self.assertEqual(upload_mock.await_count, 2)
        rt._store.release_feed.assert_awaited_once()

    async def test_lease_lost_during_upload_aborts_without_db_write(
        self,
    ) -> None:
        """LeaseExpiredError aborts cleanly — no report_feed_failure call."""

        async def _one_chunk(feed, shutdown):
            yield _make_captured_chunk(b"audio")

        rt = NormalizerRuntime(capture_fn=_one_chunk, settings=_make_settings())
        rt._shutdown = asyncio.Event()
        rt._lease_lost = asyncio.Event()
        rt._lease_lost.set()
        rt._store = mock.AsyncMock()
        rt._releasing_feeds = set()

        with (
            mock.patch(
                "backend.pipeline.ingestion.normalizer_runtime.gcp_helper.upload_staged_audio",
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
            yield _make_captured_chunk(b"audio")

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
                "backend.pipeline.ingestion.normalizer_runtime.gcp_helper.upload_staged_audio",
                mock.AsyncMock(return_value="gs://b/p"),
            ),
            _mock_pubsub_publish(),
        ):
            task = asyncio.create_task(_set_lease_lost_soon())
            await rt._process_feed(_FEED)
            await task

        # LeaseExpiredError caught by dedicated handler — no DB write attempted
        rt._store.report_feed_failure.assert_not_awaited()


class TestProcessFeedQuarantine(unittest.IsolatedAsyncioTestCase):
    """Tests for _process_feed quarantine telemetry emission."""

    async def test_quarantine_emits_telemetry(self) -> None:
        """When report_feed_failure returns 'quarantined', telemetry fires."""

        async def _failing_capture(feed, shutdown):
            yield _make_captured_chunk(b"audio")
            msg = "capture failed"
            raise RuntimeError(msg)

        rt = NormalizerRuntime(
            capture_fn=_failing_capture, settings=_make_settings()
        )
        rt._shutdown = asyncio.Event()
        rt._lease_lost = asyncio.Event()
        rt._store = mock.AsyncMock()
        rt._store.update_feed_progress.return_value = True
        rt._store.report_feed_failure.return_value = "quarantined"
        rt._releasing_feeds = set()

        with (
            _mock_upload_audio(),
            _mock_pubsub_publish(),
            mock.patch(
                "backend.pipeline.ingestion.normalizer_runtime.quarantine_telemetry"
            ) as mock_telemetry,
        ):
            mock_telemetry.emit_quarantine_event = mock.AsyncMock()
            await rt._process_feed(_FEED)

        mock_telemetry.emit_quarantine_event.assert_awaited_once_with(
            feed_id=str(_FEED_ID),
            feed_name="Test Feed",
            source_type="bcfy_feeds",
        )
        # _releasing_feeds cleaned up
        self.assertEqual(rt._releasing_feeds, set())

    async def test_failing_status_does_not_emit_telemetry(self) -> None:
        """When report_feed_failure returns 'failing', no telemetry fires."""

        async def _failing_capture(feed, shutdown):
            yield _make_captured_chunk(b"audio")
            msg = "capture failed"
            raise RuntimeError(msg)

        rt = NormalizerRuntime(
            capture_fn=_failing_capture, settings=_make_settings()
        )
        rt._shutdown = asyncio.Event()
        rt._lease_lost = asyncio.Event()
        rt._store = mock.AsyncMock()
        rt._store.update_feed_progress.return_value = True
        rt._store.report_feed_failure.return_value = "failing"
        rt._releasing_feeds = set()

        with (
            _mock_upload_audio(),
            _mock_pubsub_publish(),
            mock.patch(
                "backend.pipeline.ingestion.normalizer_runtime.quarantine_telemetry"
            ) as mock_telemetry,
        ):
            mock_telemetry.emit_quarantine_event = mock.AsyncMock()
            await rt._process_feed(_FEED)

        mock_telemetry.emit_quarantine_event.assert_not_awaited()


class TestProcessFeedPublishAttributes(unittest.IsolatedAsyncioTestCase):
    """Contract tests: publish_audio_chunk must receive session_id and source_type."""

    async def test_uses_chunk_session_id(self) -> None:
        """Runtime publishes with the session_id from CapturedChunk."""
        chunk_session_id = "chunk-supplied-session-id"

        async def _one_chunk(feed, shutdown):
            now = datetime.datetime.now(datetime.UTC)
            yield CapturedChunk(
                audio_bytes=b"audio",
                chunk_start_time=now,
                chunk_end_time=now + datetime.timedelta(seconds=15),
                session_id=chunk_session_id,
            )

        rt = NormalizerRuntime(capture_fn=_one_chunk, settings=_make_settings())
        rt._shutdown = asyncio.Event()
        rt._lease_lost = asyncio.Event()
        rt._store = mock.AsyncMock()
        rt._store.update_feed_progress.return_value = True
        rt._releasing_feeds = set()

        with _mock_upload_audio(), _mock_pubsub_publish() as mock_publish:
            await rt._process_feed(_FEED)

        mock_publish.assert_called_once()
        _, _, kwargs = mock_publish.mock_calls[0]
        self.assertEqual(kwargs["session_id"], chunk_session_id)

    async def test_distinct_session_ids_passed_through_per_chunk(self) -> None:
        """Each chunk's session_id is passed through independently."""
        sid_a = "session-a"
        sid_b = "session-b"

        async def _two_chunks(feed, shutdown):
            now = datetime.datetime.now(datetime.UTC)
            yield CapturedChunk(
                audio_bytes=b"audio1",
                chunk_start_time=now,
                chunk_end_time=now + datetime.timedelta(seconds=15),
                session_id=sid_a,
            )
            yield CapturedChunk(
                audio_bytes=b"audio2",
                chunk_start_time=now + datetime.timedelta(seconds=15),
                chunk_end_time=now + datetime.timedelta(seconds=30),
                session_id=sid_b,
            )

        rt = NormalizerRuntime(
            capture_fn=_two_chunks, settings=_make_settings()
        )
        rt._shutdown = asyncio.Event()
        rt._lease_lost = asyncio.Event()
        rt._store = mock.AsyncMock()
        rt._store.update_feed_progress.return_value = True
        rt._releasing_feeds = set()

        with _mock_upload_audio(), _mock_pubsub_publish() as mock_publish:
            await rt._process_feed(_FEED)

        self.assertEqual(mock_publish.call_count, 2)
        _, _, kw1 = mock_publish.mock_calls[0]
        _, _, kw2 = mock_publish.mock_calls[1]
        self.assertEqual(kw1["session_id"], sid_a)
        self.assertEqual(kw2["session_id"], sid_b)

    async def test_fallback_session_id_when_none(self) -> None:
        """Runtime generates a fallback UUID and warns when session_id is None."""

        async def _one_chunk(feed, shutdown):
            yield _make_captured_chunk(b"audio")  # session_id=None

        rt = NormalizerRuntime(capture_fn=_one_chunk, settings=_make_settings())
        rt._shutdown = asyncio.Event()
        rt._lease_lost = asyncio.Event()
        rt._store = mock.AsyncMock()
        rt._store.update_feed_progress.return_value = True
        rt._releasing_feeds = set()

        with (
            _mock_upload_audio(),
            _mock_pubsub_publish() as mock_publish,
            self.assertLogs(
                "backend.pipeline.ingestion.normalizer_runtime",
                level="WARNING",
            ) as log_cm,
        ):
            await rt._process_feed(_FEED)

        mock_publish.assert_called_once()
        _, _, kwargs = mock_publish.mock_calls[0]
        self.assertIsNotNone(kwargs["session_id"])
        self.assertTrue(len(kwargs["session_id"]) > 0)
        self.assertTrue(
            any("fallback" in msg for msg in log_cm.output),
        )

    async def test_source_type_passed(self) -> None:
        """publish_audio_chunk receives source_type matching the feed."""

        async def _one_chunk(feed, shutdown):
            yield _make_captured_chunk(b"audio")

        rt = NormalizerRuntime(capture_fn=_one_chunk, settings=_make_settings())
        rt._shutdown = asyncio.Event()
        rt._lease_lost = asyncio.Event()
        rt._store = mock.AsyncMock()
        rt._store.update_feed_progress.return_value = True
        rt._releasing_feeds = set()

        with _mock_upload_audio(), _mock_pubsub_publish() as mock_publish:
            await rt._process_feed(_FEED)

        mock_publish.assert_called_once()
        _, _, kwargs = mock_publish.mock_calls[0]
        self.assertEqual(kwargs["source_type"], _FEED["source_type"])


if __name__ == "__main__":
    unittest.main()
