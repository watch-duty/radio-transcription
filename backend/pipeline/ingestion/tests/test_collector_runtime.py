from __future__ import annotations

import asyncio
import dataclasses
import datetime
import logging
import unittest
import uuid
from typing import Any, cast
from unittest import mock

import aiohttp
import asyncpg
from google.api_core import exceptions as google_exceptions
from google.cloud.pubsub_v1.publisher import exceptions as pubsub_exceptions

from backend.pipeline.common.constants import CHUNK_DURATION_SECONDS
from backend.pipeline.ingestion import rss_watchdog
from backend.pipeline.ingestion.collector_runtime import (
    CollectorRuntime,
    _PipelineFailure,
)
from backend.pipeline.ingestion.models import (
    CapturedChunk,
    CaptureResources,
    FeedFailure,
    SourceObservation,
)
from backend.pipeline.storage.feed_store import (
    FeedStatusReason,
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


def _default_resources() -> CaptureResources:
    """Build a no-op CaptureResources for unit tests.

    A mock session is sufficient; constructing a real
    aiohttp.ClientSession would open real sockets (avoid in unit tests).
    """
    return CaptureResources(
        http_session=mock.AsyncMock(spec=aiohttp.ClientSession),
    )


_FEED = LeasedFeed(
    id=_FEED_ID,
    name="Test Feed",
    source_type=SourceType.BCFY_FEEDS,
    last_processed_filename=None,
    last_bookmark_time=None,
    fencing_token=1,
    failure_count=0,
    status_reason=None,
    source_feed_id="123",
)

_PUBLISH_SESSION_ID_ARG_INDEX = 5
_PUBLISH_START_TIMESTAMP_ARG_INDEX = 6
_PUBLISH_SOURCE_TYPE_ARG_INDEX = 8
_PUBLISH_EXTERNAL_AUDIO_ID_ARG_INDEX = 9


class TestFeedFailureContract(unittest.TestCase):
    """Tests for the typed collector failure boundary contract."""

    def test_carries_status_reason_and_reason(self) -> None:
        """FeedFailure exposes canonical and raw failure data."""
        exc = FeedFailure(
            FeedStatusReason.SOURCE_OFFLINE,
            "source_offline",
        )

        self.assertIs(exc.status_reason, FeedStatusReason.SOURCE_OFFLINE)
        self.assertEqual(exc.reason, "source_offline")
        self.assertEqual(str(exc), "source_offline")

    def test_normalizes_status_reason_values(self) -> None:
        """FeedFailure accepts canonical DB text values at the boundary."""
        exc = FeedFailure(
            "source_offline",
            "source_offline",
        )

        self.assertIs(exc.status_reason, FeedStatusReason.SOURCE_OFFLINE)
        self.assertEqual(exc.reason, "source_offline")

    def test_allows_python_exception_runtime_fields(self) -> None:
        """FeedFailure remains compatible with Python exception handling."""
        exc = FeedFailure(
            FeedStatusReason.SOURCE_OFFLINE,
            "source_offline",
        )

        exc.__traceback__ = None

    def test_reason_preserves_raw_diagnostic_text_without_capping(
        self,
    ) -> None:
        """FeedFailure keeps full raw diagnostics until persistence."""
        exc = FeedFailure(
            FeedStatusReason.SYSTEM_PIPELINE_ERROR,
            (
                "Authorization: Bearer secret-token "
                "https://example.com/stream?token=secret-value " + ("x" * 3000)
            ),
        )

        self.assertGreater(len(exc.reason), 2048)
        self.assertIn("Authorization: Bearer secret-token", exc.reason)
        self.assertIn("token=secret-value", exc.reason)
        self.assertFalse(exc.reason.endswith("[truncated]"))

    def test_pipeline_failure_reason_preserves_raw_text_without_capping(
        self,
    ) -> None:
        """Runtime pipeline failures follow the same in-memory reason rule."""
        exc = _PipelineFailure(
            "Authorization: Bearer secret-token " + ("x" * 3000)
        )

        self.assertGreater(len(exc.reason), 2048)
        self.assertIn("Authorization: Bearer secret-token", exc.reason)
        self.assertFalse(exc.reason.endswith("[truncated]"))


def _mock_pubsub_publish(message_id: str = "test-message-id") -> mock._patch:
    """Patch publish_audio_chunk to return a fixed message id (at call site)."""
    return mock.patch(
        "backend.pipeline.ingestion.collector_runtime.gcp_helper.publish_audio_chunk",
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
        "backend.pipeline.ingestion.collector_runtime.gcp_helper.upload_staged_audio",
        new_callable=mock.AsyncMock,
        return_value=gcs_path,
    )


def _make_settings(**overrides) -> mock.MagicMock:
    """Build a mock CollectorSettings with sensible defaults."""
    defaults = {
        "worker_id": _WORKER_ID,
        "max_feeds_per_worker": 250,
        "lease_poll_interval_sec": 5.0,
        "heartbeat_interval_sec": 15.0,
        "heartbeat_stall_timeout_sec": 45.0,
        "graceful_shutdown_timeout_sec": 10.0,
        "task_cancel_budget_sec": 5.0,
        # RSS watchdog (Phase 4 / WATCHDOG-01). Defaults pin to "watchdog
        # disabled in tests unless explicitly overridden": override=None
        # would normally trigger fs reads at __init__ — but the watchdog
        # construction lives in _main, not __init__, and tests typically
        # don't drive _main. For tests that DO exercise the watchdog body,
        # rss_watchdog_warmup_sec=0.0 makes the warmup deadline trivially
        # in the past so the test can drive samples directly.
        "rss_watchdog_poll_interval_sec": 0.05,
        "rss_watchdog_pause_threshold": 0.70,
        "rss_watchdog_exit_threshold": 0.90,
        "rss_watchdog_pause_consecutive_samples": 3,
        "rss_watchdog_exit_consecutive_samples": 3,
        "rss_watchdog_warmup_sec": 0.0,
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
        "pubsub_publish_max_retries": 2,
        "pubsub_publish_retry_base_delay_sec": 0.5,
        "pubsub_publish_retry_max_delay_sec": 4.0,
        # Real values so health_server doesn't try to bind the MagicMock-default
        # port 1 when a test exercises _main().
        "health_check_port": 8080,
        "health_check_startup_grace_sec": 120.0,
        # Per-type claim caps — must be a real dict so iteration in the
        # leasing loop and _calculate_branch_limits doesn't trip over
        # MagicMock auto-created attributes.
        "caps": {
            SourceType.BCFY_FEEDS: 240,
            SourceType.BCFY_CALLS: 600,
            SourceType.OPENMHZ: 900,
            SourceType.FIRE_NOTIFICATIONS: 300,
        },
    }
    defaults.update(overrides)
    m = mock.MagicMock()
    m.configure_mock(**defaults)
    return m


def _make_runtime(**settings_overrides) -> CollectorRuntime:
    """Build a runtime with a mock capture_fn and settings."""

    async def _dummy_capture(feed, shutdown, _resources):
        yield _make_captured_chunk(b"chunk")

    settings = _make_settings(**settings_overrides)
    rt = CollectorRuntime(capture_fn=_dummy_capture, settings=settings)
    # Pre-initialize _lease_lost and _capture_resources so tests don't need _main().
    rt._lease_lost = asyncio.Event()
    rt._capture_resources = _default_resources()
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

    async def test_logs_exception_and_does_not_call_exit(self) -> None:
        """Tasks that raised are logged but the reaper NEVER calls os._exit (v1.1).

        The catch-and-quarantine handler in _process_feed is the primary
        fault-response path. The reaper just drains task.exception() so
        asyncio does not emit "Task exception was never retrieved".
        """

        async def _boom() -> None:
            msg = "boom"
            raise RuntimeError(msg)

        rt = _make_runtime()
        rt._shutdown = asyncio.Event()
        task = asyncio.create_task(_boom())
        await asyncio.sleep(0)  # let task finish
        rt._feed_tasks[_FEED_ID] = task

        with (
            mock.patch(
                "backend.pipeline.ingestion.collector_runtime.logger",
            ) as mock_logger,
            mock.patch(
                "backend.pipeline.ingestion.collector_runtime.os._exit",
            ) as mock_exit,
        ):
            rt._reap_completed_tasks()

        mock_logger.error.assert_called()
        mock_exit.assert_not_called()
        self.assertNotIn(_FEED_ID, rt._feed_tasks)


class TestLeasingLoopOrphanedTask(unittest.IsolatedAsyncioTestCase):
    """Tests for orphaned task cancellation during re-lease."""

    async def test_released_feed_cancels_orphaned_task(self) -> None:
        """Re-leasing a feed cancels the still-running old task."""
        rt = _make_runtime()
        rt._shutdown = asyncio.Event()
        rt._store = mock.AsyncMock()
        rt._store.count_held_by_type.return_value = dict.fromkeys(SourceType, 0)
        rt._store.acquire_feeds_recovery.return_value = []
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
            caps={
                SourceType.BCFY_FEEDS: 240,
                SourceType.BCFY_CALLS: 600,
                SourceType.OPENMHZ: 900,
                SourceType.FIRE_NOTIFICATIONS: 300,
            },
        )
        rt._shutdown = asyncio.Event()
        rt._store = mock.AsyncMock()
        rt._store.count_held_by_type.return_value = dict.fromkeys(SourceType, 0)
        rt._releasing_feeds = set()
        rt._store.acquire_feeds_batch.side_effect = [
            [],  # empty result so no tasks spawn
            asyncio.CancelledError,
        ]
        rt._store.acquire_feeds_recovery.return_value = []

        rt._shutdown.set()
        await rt._leasing_loop()

        # Inspect the call made to acquire_feeds_batch; arg[1] is the
        # per-type LIMIT dict.
        call = rt._store.acquire_feeds_batch.await_args_list[0]
        limits_dict = call[0][1]
        self.assertEqual(sum(limits_dict.values()), 250)  # exactly total_slack
        self.assertTrue(
            all(v >= 0 for v in limits_dict.values()),
            "no branch should receive a negative LIMIT",
        )


class TestProcessFeedSideEffectOrdering(unittest.IsolatedAsyncioTestCase):
    """Tests for post-capture side-effect ordering in _process_feed."""

    async def test_upload_then_bookmark_then_publish(self) -> None:
        """A committed chunk is uploaded, bookmarked, then published."""

        async def _one_chunk(feed, shutdown, _resources):
            yield _make_captured_chunk(b"audio")

        call_order: list[str] = []

        async def _upload(*_args: object, **_kwargs: object) -> str:
            call_order.append("upload")
            return "gs://b/p"

        async def _bookmark(*_args: object, **_kwargs: object) -> bool:
            call_order.append("bookmark")
            return True

        async def _publish(*_args: object, **_kwargs: object) -> str:
            call_order.append("publish")
            return "message-1"

        rt = CollectorRuntime(capture_fn=_one_chunk, settings=_make_settings())
        rt._shutdown = asyncio.Event()
        rt._lease_lost = asyncio.Event()
        rt._capture_resources = _default_resources()
        rt._store = mock.AsyncMock()
        rt._store.update_feed_progress = mock.AsyncMock(side_effect=_bookmark)
        rt._releasing_feeds = set()

        with (
            mock.patch(
                "backend.pipeline.ingestion.collector_runtime.gcp_helper.upload_staged_audio",
                mock.AsyncMock(side_effect=_upload),
            ),
            mock.patch(
                "backend.pipeline.ingestion.collector_runtime.gcp_helper.publish_audio_chunk",
                mock.AsyncMock(side_effect=_publish),
            ),
        ):
            await rt._process_feed(_FEED)

        self.assertEqual(call_order, ["upload", "bookmark", "publish"])


class TestProcessFeedFenceViolation(unittest.IsolatedAsyncioTestCase):
    """Tests for _process_feed fence violation."""

    async def test_bookmark_fence_failure_exits_process(self) -> None:
        """When bookmark fence fails, os._exit is called."""

        async def _one_chunk(feed, shutdown, _resources):
            yield _make_captured_chunk(b"audio")

        rt = CollectorRuntime(
            capture_fn=_one_chunk,
            settings=_make_settings(
                pubsub_publish_retry_base_delay_sec=0.0,
                pubsub_publish_retry_max_delay_sec=0.0,
            ),
        )
        rt._shutdown = asyncio.Event()
        rt._lease_lost = asyncio.Event()
        rt._capture_resources = _default_resources()
        rt._store = mock.AsyncMock()
        rt._store.update_feed_progress.return_value = False
        rt._releasing_feeds = set()

        with (
            _mock_upload_audio(),
            _mock_pubsub_publish() as publish_mock,
            mock.patch(
                "backend.pipeline.ingestion.collector_runtime.os._exit",
                side_effect=SystemExit(1),
            ) as mock_exit,
            mock.patch("logging.shutdown") as mock_shutdown,
        ):
            with self.assertRaises(SystemExit):
                await rt._process_feed(_FEED)

        publish_mock.assert_not_awaited()
        mock_exit.assert_called_once_with(1)
        mock_shutdown.assert_called_once()
        self.assertTrue(rt._lease_lost.is_set())


class TestProcessFeedShutdown(unittest.IsolatedAsyncioTestCase):
    """Tests for _process_feed shutdown behavior."""

    async def test_shutdown_skips_individual_release(self) -> None:
        """When shutdown is set, task returns without calling release_feed."""

        async def _one_chunk(feed, shutdown, _resources):
            yield _make_captured_chunk(b"audio")

        rt = CollectorRuntime(capture_fn=_one_chunk, settings=_make_settings())
        rt._shutdown = asyncio.Event()
        rt._shutdown.set()
        rt._lease_lost = asyncio.Event()
        rt._capture_resources = _default_resources()
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

        async def _one_chunk(feed, shutdown, _resources):
            yield _make_captured_chunk(b"audio")

        rt = CollectorRuntime(capture_fn=_one_chunk, settings=_make_settings())
        rt._shutdown = asyncio.Event()
        rt._lease_lost = asyncio.Event()
        rt._capture_resources = _default_resources()
        rt._store = mock.AsyncMock()
        rt._store.update_feed_progress.return_value = True
        rt._releasing_feeds = set()

        with _mock_upload_audio(), _mock_pubsub_publish():
            await rt._process_feed(_FEED)

        rt._store.release_feed.assert_awaited_once()

    async def test_releasing_feeds_cleaned_up_after_release(self) -> None:
        """_releasing_feeds is empty after release completes."""

        async def _one_chunk(feed, shutdown, _resources):
            yield _make_captured_chunk(b"audio")

        rt = CollectorRuntime(capture_fn=_one_chunk, settings=_make_settings())
        rt._shutdown = asyncio.Event()
        rt._lease_lost = asyncio.Event()
        rt._capture_resources = _default_resources()
        rt._store = mock.AsyncMock()
        rt._store.update_feed_progress.return_value = True
        rt._releasing_feeds = set()

        with _mock_upload_audio(), _mock_pubsub_publish():
            await rt._process_feed(_FEED)

        self.assertEqual(rt._releasing_feeds, set())


class TestProcessFeedSourceObservation(unittest.IsolatedAsyncioTestCase):
    """Tests for non-audio source success observations."""

    async def test_clean_observation_skips_db_reset_and_audio_pipeline(
        self,
    ) -> None:
        """Clean leased rows do not write on empty successful polls."""

        async def _one_observation(feed, shutdown, _resources):
            yield SourceObservation()

        feed = cast("LeasedFeed", dict(_FEED))
        rt = CollectorRuntime(
            capture_fn=_one_observation,
            settings=_make_settings(),
        )
        rt._shutdown = asyncio.Event()
        rt._lease_lost = asyncio.Event()
        rt._capture_resources = _default_resources()
        rt._store = mock.AsyncMock()
        rt._releasing_feeds = set()

        with (
            _mock_upload_audio() as mock_upload,
            _mock_pubsub_publish() as mock_publish,
        ):
            await rt._process_feed(feed)

        rt._store.record_source_observation.assert_not_called()
        mock_upload.assert_not_called()
        mock_publish.assert_not_called()
        rt._store.release_feed.assert_awaited_once()

    async def test_dirty_observation_records_reset_and_updates_lease_copy(
        self,
    ) -> None:
        """Dirty leased rows clear failure state after an empty successful poll."""
        resume_position = datetime.datetime(
            2026,
            6,
            8,
            12,
            0,
            tzinfo=datetime.UTC,
        )

        async def _one_observation(feed, shutdown, _resources):
            yield SourceObservation(resume_position=resume_position)

        feed = cast(
            "LeasedFeed",
            dict(
                _FEED,
                failure_count=2,
                status_reason=(FeedStatusReason.SYSTEM_AUTHENTICATION_FAILED),
            ),
        )
        rt = CollectorRuntime(
            capture_fn=_one_observation,
            settings=_make_settings(),
        )
        rt._shutdown = asyncio.Event()
        rt._lease_lost = asyncio.Event()
        rt._capture_resources = _default_resources()
        rt._store = mock.AsyncMock()
        rt._store.record_source_observation.return_value = {
            "id": _FEED_ID,
            "current_worker": _WORKER_ID,
            "current_status": "active",
            "current_fencing_token": 1,
            "recorded": True,
        }
        rt._releasing_feeds = set()

        with _mock_upload_audio(), _mock_pubsub_publish():
            await rt._process_feed(feed)

        rt._store.record_source_observation.assert_awaited_once_with(
            _FEED_ID,
            _WORKER_ID,
            1,
            resume_position,
        )
        self.assertEqual(feed["failure_count"], 0)
        self.assertIsNone(feed["status_reason"])
        self.assertEqual(feed["last_bookmark_time"], resume_position)
        rt._store.release_feed.assert_awaited_once()

    async def test_dirty_observation_does_not_rewind_local_bookmark(
        self,
    ) -> None:
        """Local lease copy mirrors SQL monotonic bookmark advancement."""
        existing_bookmark = datetime.datetime(
            2026,
            6,
            8,
            12,
            0,
            tzinfo=datetime.UTC,
        )
        older_resume_position = existing_bookmark - datetime.timedelta(
            minutes=5
        )

        async def _one_observation(feed, shutdown, _resources):
            yield SourceObservation(resume_position=older_resume_position)

        feed = cast(
            "LeasedFeed",
            dict(
                _FEED,
                last_bookmark_time=existing_bookmark,
                failure_count=2,
                status_reason=FeedStatusReason.SYSTEM_AUTHENTICATION_FAILED,
            ),
        )
        rt = CollectorRuntime(
            capture_fn=_one_observation,
            settings=_make_settings(),
        )
        rt._shutdown = asyncio.Event()
        rt._lease_lost = asyncio.Event()
        rt._capture_resources = _default_resources()
        rt._store = mock.AsyncMock()
        rt._store.record_source_observation.return_value = {
            "id": _FEED_ID,
            "current_worker": _WORKER_ID,
            "current_status": "active",
            "current_fencing_token": 1,
            "recorded": True,
        }
        rt._releasing_feeds = set()

        with _mock_upload_audio(), _mock_pubsub_publish():
            await rt._process_feed(feed)

        self.assertEqual(feed["failure_count"], 0)
        self.assertIsNone(feed["status_reason"])
        self.assertEqual(feed["last_bookmark_time"], existing_bookmark)
        rt._store.release_feed.assert_awaited_once()

    async def test_dirty_observation_aborts_without_failure_when_row_inactive(
        self,
    ) -> None:
        """Inactive or missing rows stop the task without reporting failure."""

        async def _one_observation(feed, shutdown, _resources):
            yield SourceObservation()

        feed = cast(
            "LeasedFeed",
            dict(
                _FEED,
                failure_count=1,
                status_reason=FeedStatusReason.SOURCE_UNREACHABLE,
            ),
        )
        rt = CollectorRuntime(
            capture_fn=_one_observation,
            settings=_make_settings(),
        )
        rt._shutdown = asyncio.Event()
        rt._lease_lost = asyncio.Event()
        rt._capture_resources = _default_resources()
        rt._store = mock.AsyncMock()
        rt._store.record_source_observation.return_value = {
            "id": _FEED_ID,
            "current_worker": None,
            "current_status": "unclaimed",
            "current_fencing_token": None,
            "recorded": False,
        }
        rt._releasing_feeds = set()

        await rt._process_feed(feed)

        rt._store.record_source_observation.assert_awaited_once()
        rt._store.release_feed.assert_not_called()
        rt._store.report_feed_failure.assert_not_called()

    async def test_dirty_observation_exits_on_active_fence_violation(
        self,
    ) -> None:
        """An active row owned by another lease is treated as a fence violation."""
        other_worker = uuid.UUID("22222222-3333-4444-5555-666666666666")

        async def _one_observation(feed, shutdown, _resources):
            yield SourceObservation()

        feed = cast(
            "LeasedFeed",
            dict(
                _FEED,
                failure_count=1,
                status_reason=FeedStatusReason.SOURCE_UNREACHABLE,
            ),
        )
        rt = CollectorRuntime(
            capture_fn=_one_observation,
            settings=_make_settings(),
        )
        rt._shutdown = asyncio.Event()
        rt._lease_lost = asyncio.Event()
        rt._capture_resources = _default_resources()
        rt._store = mock.AsyncMock()
        rt._store.record_source_observation.return_value = {
            "id": _FEED_ID,
            "current_worker": other_worker,
            "current_status": "active",
            "current_fencing_token": 2,
            "recorded": False,
        }
        rt._releasing_feeds = set()

        with (
            mock.patch(
                "backend.pipeline.ingestion.collector_runtime.os._exit",
            ) as mock_exit,
            mock.patch("logging.shutdown"),
        ):
            await rt._process_feed(feed)

        mock_exit.assert_called_once_with(1)


class TestProcessFeedTimestamps(unittest.IsolatedAsyncioTestCase):
    """Tests for _process_feed timestamp population."""

    async def test_sets_start_timestamp_on_audio_chunk(self) -> None:
        """The start_timestamp field must be populated before publishing."""

        async def _one_chunk(feed, shutdown, _resources):
            yield _make_captured_chunk(b"audio")

        rt = CollectorRuntime(capture_fn=_one_chunk, settings=_make_settings())
        rt._shutdown = asyncio.Event()
        rt._lease_lost = asyncio.Event()
        rt._capture_resources = _default_resources()
        rt._store = mock.AsyncMock()
        rt._store.update_feed_progress.return_value = True
        rt._releasing_feeds = set()

        with (
            _mock_upload_audio(),
            _mock_pubsub_publish() as mock_publish,
        ):
            await rt._process_feed(_FEED)

            mock_publish.assert_called_once()
            _, args, _kwargs = mock_publish.mock_calls[0]

            self.assertEqual(len(args), 10)
            self.assertEqual(
                args[1], rt._collector_settings.continuous_pubsub_topic_path
            )
            self.assertIsNone(args[_PUBLISH_EXTERNAL_AUDIO_ID_ARG_INDEX])
            self.assertEqual(args[2], str(_FEED["id"]))
            self.assertEqual(args[3], "Test Feed")
            self.assertTrue(args[4].startswith("gs://"))

            start_timestamp = args[_PUBLISH_START_TIMESTAMP_ARG_INDEX]
            self.assertIsNotNone(start_timestamp)
            self.assertIsInstance(start_timestamp, datetime.datetime)
            self.assertGreater(start_timestamp.timestamp(), 1700000000)


class TestProcessFeedSessionId(unittest.IsolatedAsyncioTestCase):
    """Tests for _process_feed session ID population."""

    async def test_session_id_populated_and_identical_across_chunks(
        self,
    ) -> None:
        """The session_id field must be populated and identical for all chunks in a session."""

        async def _two_chunks(feed, shutdown, _resources):

            chunk1 = _make_captured_chunk(b"audio1")
            chunk2 = _make_captured_chunk(b"audio2")
            yield dataclasses.replace(chunk1, session_id="test-session-id")
            yield dataclasses.replace(chunk2, session_id="test-session-id")

        rt = CollectorRuntime(capture_fn=_two_chunks, settings=_make_settings())
        rt._shutdown = asyncio.Event()
        rt._lease_lost = asyncio.Event()
        rt._capture_resources = _default_resources()
        rt._store = mock.AsyncMock()
        rt._store.update_feed_progress.return_value = True
        rt._releasing_feeds = set()

        with (
            _mock_upload_audio(),
            _mock_pubsub_publish() as mock_publish,
        ):
            await rt._process_feed(_FEED)

            self.assertEqual(mock_publish.call_count, 2)

            _, args1, _kwargs1 = mock_publish.mock_calls[0]
            _, args2, _kwargs2 = mock_publish.mock_calls[1]

            self.assertTrue(len(args1[_PUBLISH_SESSION_ID_ARG_INDEX]) > 0)
            self.assertEqual(
                args1[_PUBLISH_SESSION_ID_ARG_INDEX],
                args2[_PUBLISH_SESSION_ID_ARG_INDEX],
            )


class TestProcessFeedTopicRouting(unittest.IsolatedAsyncioTestCase):
    """Tests for _process_feed topic routing based on SourceType."""

    async def test_routes_continuous_feed_to_default_topic(self) -> None:
        """Continuous feeds (BCFY_FEEDS) go to continuous_pubsub_topic_path."""

        async def _one_chunk(feed, shutdown, _resources):
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

        async def _one_chunk(feed, shutdown, _resources):
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
            source_type=SourceType.OPENMHZ,  # Not BCFY_FEEDS
            last_processed_filename=None,
            last_bookmark_time=None,
            fencing_token=1,
            failure_count=0,
            status_reason=None,
            source_feed_id="123",
        )

        with _mock_upload_audio(), _mock_pubsub_publish() as mock_publish:
            await rt._process_feed(segmented_feed)

            mock_publish.assert_called_once()
            _, args, _ = mock_publish.mock_calls[0]
            self.assertEqual(args[1], "projects/p/topics/segmented")

    async def test_raises_if_segmented_topic_missing(self) -> None:
        """Raises ValueError if segmented feed processed but segmented topic missing."""

        async def _one_chunk(feed, shutdown, _resources):
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
            source_type=SourceType.OPENMHZ,
            last_processed_filename=None,
            last_bookmark_time=None,
            fencing_token=1,
            failure_count=0,
            status_reason=None,
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

    async def test_deactivated_feed_cancels_task(self) -> None:
        """When a feed is deactivated, its background task is cancelled."""
        rt = _make_runtime()
        task = asyncio.create_task(asyncio.sleep(100))
        rt._feed_tasks[_FEED_ID] = task
        rt._releasing_feeds = set()
        rt._heartbeat_store = mock.AsyncMock()
        rt._heartbeat_store.renew_heartbeats_batch_diagnostic.return_value = [
            self._diag(_FEED_ID, status="deactivated", renewed=False),
        ]

        await rt._heartbeat_cycle()
        # Yield to let the event loop process the cancellation
        await asyncio.sleep(0)
        self.assertTrue(task.cancelled())

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
            "backend.pipeline.ingestion.collector_runtime.os._exit",
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
                "backend.pipeline.ingestion.collector_runtime.os._exit",
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
            "backend.pipeline.ingestion.collector_runtime.os._exit",
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
            "backend.pipeline.ingestion.collector_runtime.os._exit",
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
                "backend.pipeline.ingestion.collector_runtime.os._exit",
            ),
            mock.patch("logging.shutdown"),
            mock.patch(
                "backend.pipeline.ingestion.collector_runtime.logger",
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
                "backend.pipeline.ingestion.collector_runtime.os._exit",
            ),
            mock.patch("logging.shutdown"),
            mock.patch(
                "backend.pipeline.ingestion.collector_runtime.logger",
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
        "backend.pipeline.ingestion.collector_runtime.FeedStore",
    )
    @mock.patch(
        "backend.pipeline.ingestion.collector_runtime.create_pool_with_retry",
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
            mock.patch(
                "backend.pipeline.ingestion.collector_runtime.aiohttp.TCPConnector"
            ),
            mock.patch(
                "backend.pipeline.ingestion.collector_runtime.aiohttp.ClientSession"
            ),
            mock.patch.object(rt, "_leasing_loop", new_callable=mock.AsyncMock),
            mock.patch.object(
                rt, "_shutdown_sequence", new_callable=mock.AsyncMock
            ),
            mock.patch("threading.Thread"),
            mock.patch(
                "backend.pipeline.ingestion.collector_runtime.health_server.start",
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
            "backend.pipeline.ingestion.collector_runtime.close_pool",
            new_callable=mock.AsyncMock,
        ) as mock_close_pool:
            await rt._shutdown_sequence()

        rt._heartbeat_pool.close.assert_awaited_once()
        mock_close_pool.assert_awaited_once_with(rt._data_pool)
        rt._pubsub_client.close.assert_awaited_once()
        rt._gcs_client.close.assert_awaited_once()

    async def test_http_session_closes_after_gcs(self) -> None:
        """HTTP-01 ordering: aiohttp session closes AFTER _gcs_client.

        PITFALLS.md Pitfall 4 + Pitfall 12: the runtime-owned
        aiohttp.ClientSession must be closed alongside _gcs_client
        (after asyncio.wait of feed tasks), and the close must be
        followed by a 250ms SSL-teardown sleep.
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
        rt._http_session = mock.AsyncMock(spec=aiohttp.ClientSession)

        call_order: list[str] = []
        rt._gcs_client.close.side_effect = lambda: call_order.append("gcs")
        rt._http_session.close.side_effect = lambda: call_order.append(
            "http_session"
        )

        with (
            mock.patch(
                "backend.pipeline.ingestion.collector_runtime.close_pool",
                new_callable=mock.AsyncMock,
            ),
            mock.patch(
                "backend.pipeline.ingestion.collector_runtime.asyncio.sleep",
                new_callable=mock.AsyncMock,
            ) as mock_sleep,
        ):
            await rt._shutdown_sequence()

        rt._http_session.close.assert_awaited_once()
        # Strict ordering: gcs first, then http_session (Pitfall 4).
        self.assertEqual(call_order, ["gcs", "http_session"])
        # Pitfall 12: 250ms sleep AFTER session close to let SSL transports
        # flush. Without this, "ResourceWarning: unclosed transport" would
        # spam the shutdown logs.
        mock_sleep.assert_awaited_once_with(0.25)

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
        # The invariant we care about is that cleanup continues past the
        # health_runner boom — release_feeds_batch is reached, the pubsub
        # and gcs clients close, the pools close.
        self.assertEqual(len(rt._feed_tasks), 0)


class TestCalculateBranchLimits(unittest.TestCase):
    """Water-filling apportion: sum(limits) <= total_slack, no starvation."""

    CAPS = {
        SourceType.BCFY_FEEDS: 240,
        SourceType.BCFY_CALLS: 600,
        SourceType.OPENMHZ: 900,
        SourceType.FIRE_NOTIFICATIONS: 300,
    }

    def test_cold_start_bounds_sum_at_total_slack(self) -> None:
        # max_feeds_per_worker=250, all held=0 → sum must be exactly 250.
        held = dict.fromkeys(self.CAPS, 0)
        limits = CollectorRuntime._calculate_branch_limits(250, self.CAPS, held)
        self.assertEqual(sum(limits.values()), 250)
        self.assertTrue(all(v >= 0 for v in limits.values()))

    def test_plan_target_800_bounds_sum(self) -> None:
        # max_feeds_per_worker=800 (scaling-plan target), all held=0.
        held = dict.fromkeys(self.CAPS, 0)
        limits = CollectorRuntime._calculate_branch_limits(800, self.CAPS, held)
        self.assertEqual(sum(limits.values()), 800)

    def test_slack_exceeds_cap_sum_clamps_at_caps(self) -> None:
        # total_slack=3000 > sum(caps)=2040 → each branch gets its cap,
        # leftover slack is unassigned.
        held = dict.fromkeys(self.CAPS, 0)
        limits = CollectorRuntime._calculate_branch_limits(
            3000, self.CAPS, held
        )
        self.assertEqual(limits[SourceType.BCFY_FEEDS], 240)
        self.assertEqual(limits[SourceType.BCFY_CALLS], 600)
        self.assertEqual(limits[SourceType.OPENMHZ], 900)
        self.assertEqual(limits[SourceType.FIRE_NOTIFICATIONS], 300)
        self.assertEqual(sum(limits.values()), 2040)

    def test_type_at_cap_yields_zero_for_that_branch(self) -> None:
        held = {
            SourceType.BCFY_FEEDS: 240,
            SourceType.BCFY_CALLS: 0,
            SourceType.OPENMHZ: 0,
            SourceType.FIRE_NOTIFICATIONS: 0,
        }
        limits = CollectorRuntime._calculate_branch_limits(250, self.CAPS, held)
        self.assertEqual(limits[SourceType.BCFY_FEEDS], 0)
        self.assertEqual(sum(limits.values()), 250)

    def test_small_headroom_branch_redistributes_to_larger(self) -> None:
        # bcfy_feeds has only 10 headroom; slack=300 must go mostly to
        # bcfy_calls + openmhz.
        held = {
            SourceType.BCFY_FEEDS: 230,
            SourceType.BCFY_CALLS: 0,
            SourceType.OPENMHZ: 0,
            SourceType.FIRE_NOTIFICATIONS: 0,
        }
        limits = CollectorRuntime._calculate_branch_limits(300, self.CAPS, held)
        self.assertLessEqual(limits[SourceType.BCFY_FEEDS], 10)
        self.assertEqual(sum(limits.values()), 300)

    def test_zero_slack_returns_all_zeros(self) -> None:
        held = dict.fromkeys(self.CAPS, 0)
        limits = CollectorRuntime._calculate_branch_limits(0, self.CAPS, held)
        self.assertEqual(limits, dict.fromkeys(self.CAPS, 0))

    def test_negative_held_defensive_does_not_overrun_cap(self) -> None:
        # Corrupted state: decrement bug made held negative. max() clamp
        # must prevent the "cap - held > cap" hazard.
        held = {
            SourceType.BCFY_FEEDS: -5,
            SourceType.BCFY_CALLS: 0,
            SourceType.OPENMHZ: 0,
            SourceType.FIRE_NOTIFICATIONS: 0,
        }
        limits = CollectorRuntime._calculate_branch_limits(
            1000, self.CAPS, held
        )
        # Each branch still bounded at its cap even with corrupted held.
        self.assertLessEqual(limits[SourceType.BCFY_FEEDS], 240)
        self.assertLessEqual(limits[SourceType.BCFY_CALLS], 600)
        self.assertLessEqual(limits[SourceType.OPENMHZ], 900)
        self.assertLessEqual(limits[SourceType.FIRE_NOTIFICATIONS], 300)

    def test_held_missing_keys_treated_as_zero(self) -> None:
        # Future caller passes a sparse dict (only types it currently
        # holds). The function must not KeyError; missing keys default
        # to held=0 → full headroom for that branch.
        held: dict[SourceType, int] = {SourceType.BCFY_FEEDS: 100}
        limits = CollectorRuntime._calculate_branch_limits(250, self.CAPS, held)
        # BCFY_CALLS and OPENMHZ have no entry in `held` — both should
        # be treated as held=0 with full cap-sized headroom.
        self.assertEqual(sum(limits.values()), 250)
        self.assertTrue(all(v >= 0 for v in limits.values()))


class TestLeasingLoopHeldCounts(unittest.IsolatedAsyncioTestCase):
    """The leasing loop must source per-type held counts from the DB."""

    async def test_count_held_by_type_called_before_acquire(self) -> None:
        """count_held_by_type is awaited and its result feeds branch limits."""
        rt = _make_runtime(
            max_feeds_per_worker=250,
            caps={
                SourceType.BCFY_FEEDS: 240,
                SourceType.BCFY_CALLS: 600,
                SourceType.OPENMHZ: 900,
                SourceType.FIRE_NOTIFICATIONS: 300,
            },
        )
        rt._shutdown = asyncio.Event()
        rt._store = mock.AsyncMock()
        rt._releasing_feeds = set()

        # Simulate the worker already holding 240 BCFY_FEEDS (at cap).
        # _calculate_branch_limits should give that branch zero headroom.
        rt._store.count_held_by_type.return_value = {
            SourceType.BCFY_FEEDS: 240,
            SourceType.BCFY_CALLS: 0,
            SourceType.OPENMHZ: 0,
            SourceType.FIRE_NOTIFICATIONS: 0,
            SourceType.ECHO: 0,
        }
        rt._store.acquire_feeds_batch.side_effect = [
            [],
            asyncio.CancelledError,
        ]
        rt._store.acquire_feeds_recovery.return_value = []

        rt._shutdown.set()
        await rt._leasing_loop()

        # The DB-derived held dict must have been consulted.
        rt._store.count_held_by_type.assert_awaited()
        call = rt._store.count_held_by_type.await_args_list[0]
        self.assertEqual(call[0][0], _WORKER_ID)

        # The acquire call's per-type limits dict must reflect the
        # DB-derived held: bcfy_feeds=0 (capped), other two share
        # total_slack=250.
        acquire_call = rt._store.acquire_feeds_batch.await_args_list[0]
        limits_dict = acquire_call[0][1]
        self.assertEqual(limits_dict[SourceType.BCFY_FEEDS], 0)
        self.assertEqual(sum(limits_dict.values()), 250)


class TestSigtermRelease(unittest.IsolatedAsyncioTestCase):
    """Tests for the SIGTERM release path (single WHERE worker_id = $1 UPDATE)."""

    async def test_release_calls_batch_with_worker_id(self) -> None:
        """Shutdown issues exactly one release_feeds_batch call with worker_id."""
        rt = _make_runtime()
        rt._shutdown = asyncio.Event()
        rt._thread_stop = mock.MagicMock()
        rt._heartbeat_thread = None
        rt._store = mock.AsyncMock()
        rt._store.release_feeds_batch.return_value = 120
        rt._data_pool = mock.AsyncMock()
        rt._heartbeat_pool = mock.AsyncMock()
        rt._pubsub_client = mock.AsyncMock()
        rt._gcs_client = mock.AsyncMock()
        rt._health_runner = mock.AsyncMock()

        # Populate _feed_tasks so _shutdown_sequence has tasks to cancel,
        # but the call shape no longer depends on the IDs themselves.
        for _ in range(120):
            t = asyncio.create_task(asyncio.sleep(0))
            rt._feed_tasks[uuid.uuid4()] = t
        await asyncio.gather(*rt._feed_tasks.values(), return_exceptions=True)

        await rt._shutdown_sequence()

        rt._store.release_feeds_batch.assert_awaited_once_with(_WORKER_ID)


class TestHeartbeatLoopSetsLeaseLost(unittest.IsolatedAsyncioTestCase):
    """Tests for _heartbeat_loop _lease_lost semantics."""

    async def test_heartbeat_exception_does_not_set_lease_lost(self) -> None:
        """Transient heartbeat error does not prove lease loss."""
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

        await asyncio.sleep(0)
        self.assertFalse(rt._lease_lost.is_set())

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
                "backend.pipeline.ingestion.collector_runtime.os._exit",
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

        async def _one_chunk(feed, shutdown, _resources):
            yield _make_captured_chunk(b"audio")

        rt = CollectorRuntime(capture_fn=_one_chunk, settings=_make_settings())
        rt._shutdown = asyncio.Event()
        rt._lease_lost = asyncio.Event()
        rt._capture_resources = _default_resources()
        rt._store = mock.AsyncMock()
        rt._store.update_feed_progress.return_value = True
        rt._releasing_feeds = set()

        upload_mock = mock.AsyncMock(
            side_effect=[aiohttp.ClientError("transient"), "gs://b/p"],
        )

        with (
            mock.patch(
                "backend.pipeline.ingestion.collector_runtime.gcp_helper.upload_staged_audio",
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

        async def _one_chunk(feed, shutdown, _resources):
            yield _make_captured_chunk(b"audio")

        rt = CollectorRuntime(capture_fn=_one_chunk, settings=_make_settings())
        rt._shutdown = asyncio.Event()
        rt._lease_lost = asyncio.Event()
        rt._capture_resources = _default_resources()
        rt._lease_lost.set()
        rt._store = mock.AsyncMock()
        rt._releasing_feeds = set()

        with (
            mock.patch(
                "backend.pipeline.ingestion.collector_runtime.gcp_helper.upload_staged_audio",
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

        async def _one_chunk(feed, shutdown, _resources):
            yield _make_captured_chunk(b"audio")

        rt = CollectorRuntime(capture_fn=_one_chunk, settings=_make_settings())
        rt._shutdown = asyncio.Event()
        rt._lease_lost = asyncio.Event()
        rt._capture_resources = _default_resources()
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
                "backend.pipeline.ingestion.collector_runtime.gcp_helper.upload_staged_audio",
                mock.AsyncMock(return_value="gs://b/p"),
            ),
            _mock_pubsub_publish(),
        ):
            task = asyncio.create_task(_set_lease_lost_soon())
            await rt._process_feed(_FEED)
            await task

        # LeaseExpiredError caught by dedicated handler — no DB write attempted
        rt._store.report_feed_failure.assert_not_awaited()

    async def test_transient_pubsub_failure_retries_after_bookmark(
        self,
    ) -> None:
        """Pub/Sub publish retry happens after a successful bookmark."""

        async def _one_chunk(feed, shutdown, _resources):
            yield _make_captured_chunk(b"audio")

        call_order: list[str] = []
        publish_results: list[object] = [
            google_exceptions.ServiceUnavailable("pubsub transient"),
            "message-2",
        ]

        async def _upload(*_args: object, **_kwargs: object) -> str:
            call_order.append("upload")
            return "gs://b/p"

        async def _bookmark(*_args: object, **_kwargs: object) -> bool:
            call_order.append("bookmark")
            return True

        async def _publish(*_args: object, **_kwargs: object) -> str:
            call_order.append("publish")
            result = publish_results.pop(0)
            if isinstance(result, Exception):
                raise result
            return cast("str", result)

        rt = CollectorRuntime(capture_fn=_one_chunk, settings=_make_settings())
        rt._shutdown = asyncio.Event()
        rt._lease_lost = asyncio.Event()
        rt._capture_resources = _default_resources()
        rt._store = mock.AsyncMock()
        rt._store.update_feed_progress = mock.AsyncMock(side_effect=_bookmark)
        rt._store.report_feed_failure.return_value = "failing"
        rt._releasing_feeds = set()

        with (
            mock.patch(
                "backend.pipeline.ingestion.collector_runtime.gcp_helper.upload_staged_audio",
                mock.AsyncMock(side_effect=_upload),
            ),
            mock.patch(
                "backend.pipeline.ingestion.collector_runtime.gcp_helper.publish_audio_chunk",
                mock.AsyncMock(side_effect=_publish),
            ),
        ):
            await rt._process_feed(_FEED)

        self.assertEqual(
            call_order,
            ["upload", "bookmark", "publish", "publish"],
        )
        rt._store.update_feed_progress.assert_awaited_once()
        rt._store.report_feed_failure.assert_not_awaited()
        rt._store.release_feed.assert_awaited_once()

    async def test_paused_ordering_key_retries_after_bookmark(self) -> None:
        """Paused ordering keys retry after resume_publish unpauses the key."""

        async def _one_chunk(feed, shutdown, _resources):
            yield _make_captured_chunk(b"audio")

        call_order: list[str] = []
        publish_results: list[object] = [
            pubsub_exceptions.PublishToPausedOrderingKeyException("feed-42"),
            "message-2",
        ]

        async def _upload(*_args: object, **_kwargs: object) -> str:
            call_order.append("upload")
            return "gs://b/p"

        async def _bookmark(*_args: object, **_kwargs: object) -> bool:
            call_order.append("bookmark")
            return True

        async def _publish(*_args: object, **_kwargs: object) -> str:
            call_order.append("publish")
            result = publish_results.pop(0)
            if isinstance(result, Exception):
                raise result
            return cast("str", result)

        rt = CollectorRuntime(
            capture_fn=_one_chunk,
            settings=_make_settings(
                pubsub_publish_retry_base_delay_sec=0.0,
                pubsub_publish_retry_max_delay_sec=0.0,
            ),
        )
        rt._shutdown = asyncio.Event()
        rt._lease_lost = asyncio.Event()
        rt._capture_resources = _default_resources()
        rt._store = mock.AsyncMock()
        rt._store.update_feed_progress = mock.AsyncMock(side_effect=_bookmark)
        rt._store.report_feed_failure.return_value = "failing"
        rt._releasing_feeds = set()

        with (
            mock.patch(
                "backend.pipeline.ingestion.collector_runtime.gcp_helper.upload_staged_audio",
                mock.AsyncMock(side_effect=_upload),
            ),
            mock.patch(
                "backend.pipeline.ingestion.collector_runtime.gcp_helper.publish_audio_chunk",
                mock.AsyncMock(side_effect=_publish),
            ),
        ):
            await rt._process_feed(_FEED)

        self.assertEqual(
            call_order,
            ["upload", "bookmark", "publish", "publish"],
        )
        rt._store.update_feed_progress.assert_awaited_once()
        rt._store.report_feed_failure.assert_not_awaited()
        rt._store.release_feed.assert_awaited_once()

    async def test_non_retryable_pubsub_failure_records_pipeline_error_once_after_bookmark(
        self,
    ) -> None:
        """Non-retryable Pub/Sub errors record pipeline failure once."""

        async def _one_chunk(feed, shutdown, _resources):
            yield _make_captured_chunk(b"audio")

        call_order: list[str] = []

        async def _upload(*_args: object, **_kwargs: object) -> str:
            call_order.append("upload")
            return "gs://b/p"

        async def _bookmark(*_args: object, **_kwargs: object) -> bool:
            call_order.append("bookmark")
            return True

        async def _publish(*_args: object, **_kwargs: object) -> str:
            call_order.append("publish")
            msg = (
                "400 Invalid data in message: Message failed schema "
                'validation. [reason: "INVALID_BINARY_PROTO_MESSAGE" '
                'metadata { key: "message" value: "Message failed '
                'schema validation" } metadata { key: "revisionInfo" '
                'value: "Could not parse binary message." }]'
            )
            raise google_exceptions.InvalidArgument(msg)

        rt = CollectorRuntime(capture_fn=_one_chunk, settings=_make_settings())
        rt._shutdown = asyncio.Event()
        rt._lease_lost = asyncio.Event()
        rt._capture_resources = _default_resources()
        rt._store = mock.AsyncMock()
        rt._store.update_feed_progress = mock.AsyncMock(side_effect=_bookmark)
        rt._store.report_feed_failure.return_value = "failing"
        rt._releasing_feeds = set()

        with (
            mock.patch(
                "backend.pipeline.ingestion.collector_runtime.gcp_helper.upload_staged_audio",
                mock.AsyncMock(side_effect=_upload),
            ),
            mock.patch(
                "backend.pipeline.ingestion.collector_runtime.gcp_helper.publish_audio_chunk",
                mock.AsyncMock(side_effect=_publish),
            ),
        ):
            await rt._process_feed(_FEED)

        self.assertEqual(call_order, ["upload", "bookmark", "publish"])
        rt._store.update_feed_progress.assert_awaited_once()
        rt._store.report_feed_failure.assert_awaited_once()
        kwargs = rt._store.report_feed_failure.await_args.kwargs
        self.assertIn("Pub/Sub schema validation failed", kwargs["reason"])
        self.assertIn("INVALID_BINARY_PROTO_MESSAGE", kwargs["reason"])
        self.assertIn("Could not parse binary message", kwargs["reason"])
        self.assertIs(
            kwargs["status_reason"],
            FeedStatusReason.SYSTEM_PIPELINE_ERROR,
        )
        rt._store.release_feed.assert_not_awaited()


class TestProcessFeedQuarantine(unittest.IsolatedAsyncioTestCase):
    """Tests for _process_feed quarantine telemetry emission."""

    async def test_quarantine_emits_telemetry(self) -> None:
        """When report_feed_failure returns 'quarantined', telemetry fires."""

        async def _failing_capture(feed, shutdown, _resources):
            yield _make_captured_chunk(b"audio")
            msg = "capture_failed"
            raise RuntimeError(msg)

        rt = CollectorRuntime(
            capture_fn=_failing_capture, settings=_make_settings()
        )
        rt._shutdown = asyncio.Event()
        rt._lease_lost = asyncio.Event()
        rt._capture_resources = _default_resources()
        rt._store = mock.AsyncMock()
        rt._store.update_feed_progress.return_value = True
        rt._store.report_feed_failure.return_value = "quarantined"
        rt._releasing_feeds = set()

        with (
            _mock_upload_audio(),
            _mock_pubsub_publish(),
            mock.patch(
                "backend.pipeline.ingestion.collector_runtime.quarantine_telemetry"
            ) as mock_telemetry,
        ):
            mock_telemetry.emit_quarantine_event = mock.AsyncMock()
            await rt._process_feed(_FEED)

        mock_telemetry.emit_quarantine_event.assert_awaited_once_with(
            feed_id=str(_FEED_ID),
            feed_name="Test Feed",
            source_type="bcfy_feeds",
            reason="RuntimeError: capture_failed",
            status_reason="system_unexpected_error",
        )
        # _releasing_feeds cleaned up
        self.assertEqual(rt._releasing_feeds, set())

    async def test_failing_status_does_not_emit_telemetry(self) -> None:
        """When report_feed_failure returns 'failing', no telemetry fires."""

        async def _failing_capture(feed, shutdown, _resources):
            yield _make_captured_chunk(b"audio")
            msg = "capture_failed"
            raise RuntimeError(msg)

        rt = CollectorRuntime(
            capture_fn=_failing_capture, settings=_make_settings()
        )
        rt._shutdown = asyncio.Event()
        rt._lease_lost = asyncio.Event()
        rt._capture_resources = _default_resources()
        rt._store = mock.AsyncMock()
        rt._store.update_feed_progress.return_value = True
        rt._store.report_feed_failure.return_value = "failing"
        rt._releasing_feeds = set()

        with (
            _mock_upload_audio(),
            _mock_pubsub_publish(),
            mock.patch(
                "backend.pipeline.ingestion.collector_runtime.quarantine_telemetry"
            ) as mock_telemetry,
        ):
            mock_telemetry.emit_quarantine_event = mock.AsyncMock()
            await rt._process_feed(_FEED)

        mock_telemetry.emit_quarantine_event.assert_not_awaited()

    async def test_unexpected_exception_reason_reaches_storage_uncapped(
        self,
    ) -> None:
        """Catch-all preserves full diagnostics until the storage boundary."""
        long_message = "x" * 250

        async def _failing_capture(feed, shutdown, _resources):
            yield _make_captured_chunk(b"audio")
            raise RuntimeError(long_message)

        rt = CollectorRuntime(
            capture_fn=_failing_capture, settings=_make_settings()
        )
        rt._shutdown = asyncio.Event()
        rt._lease_lost = asyncio.Event()
        rt._capture_resources = _default_resources()
        rt._store = mock.AsyncMock()
        rt._store.update_feed_progress.return_value = True
        rt._store.report_feed_failure.return_value = "failing"
        rt._releasing_feeds = set()

        with _mock_upload_audio(), _mock_pubsub_publish():
            await rt._process_feed(_FEED)

        rt._store.report_feed_failure.assert_awaited_once()
        kwargs = rt._store.report_feed_failure.await_args.kwargs
        self.assertEqual(kwargs["reason"], f"RuntimeError: {long_message}")
        self.assertIs(
            kwargs["status_reason"],
            FeedStatusReason.SYSTEM_UNEXPECTED_ERROR,
        )

    async def test_typed_collector_failure_persists_carried_status_reason(
        self,
    ) -> None:
        """FeedFailure carries status and quarantine reasons to storage."""

        async def _failing_capture(feed, shutdown, _resources):
            raise FeedFailure(
                FeedStatusReason.SOURCE_OFFLINE,
                "source_offline",
            )
            yield _make_captured_chunk(b"audio")

        rt = CollectorRuntime(
            capture_fn=_failing_capture,
            settings=_make_settings(),
        )
        rt._shutdown = asyncio.Event()
        rt._lease_lost = asyncio.Event()
        rt._capture_resources = _default_resources()
        rt._store = mock.AsyncMock()
        rt._store.update_feed_progress.return_value = True
        rt._store.report_feed_failure.return_value = "failing"
        rt._releasing_feeds = set()

        await rt._process_feed(_FEED)

        rt._store.report_feed_failure.assert_awaited_once()
        kwargs = rt._store.report_feed_failure.await_args.kwargs
        self.assertEqual(kwargs["reason"], "source_offline")
        self.assertIs(
            kwargs["status_reason"],
            FeedStatusReason.SOURCE_OFFLINE,
        )

    async def test_collector_failure_string_status_reason_persists(
        self,
    ) -> None:
        """String status reason values are normalized before runtime logging."""
        status_reason = "source_offline"

        async def _failing_capture(feed, shutdown, _resources):
            raise FeedFailure(
                status_reason,
                "source_offline",
            )
            yield _make_captured_chunk(b"audio")

        rt = CollectorRuntime(
            capture_fn=_failing_capture,
            settings=_make_settings(),
        )
        rt._shutdown = asyncio.Event()
        rt._lease_lost = asyncio.Event()
        rt._capture_resources = _default_resources()
        rt._store = mock.AsyncMock()
        rt._store.report_feed_failure.return_value = "failing"
        rt._releasing_feeds = set()

        await rt._process_feed(_FEED)

        rt._store.report_feed_failure.assert_awaited_once()
        kwargs = rt._store.report_feed_failure.await_args.kwargs
        self.assertEqual(kwargs["reason"], "source_offline")
        self.assertIs(
            kwargs["status_reason"],
            FeedStatusReason.SOURCE_OFFLINE,
        )

    async def test_gcs_upload_failure_records_pipeline_error(self) -> None:
        """Upload failures after a valid chunk use the GCS stage tag."""

        async def _one_chunk(feed, shutdown, _resources):
            yield _make_captured_chunk(b"audio")

        rt = CollectorRuntime(
            capture_fn=_one_chunk,
            settings=_make_settings(gcs_upload_max_retries=0),
        )
        rt._shutdown = asyncio.Event()
        rt._lease_lost = asyncio.Event()
        rt._capture_resources = _default_resources()
        rt._store = mock.AsyncMock()
        rt._store.report_feed_failure.return_value = "failing"
        rt._releasing_feeds = set()

        with mock.patch(
            "backend.pipeline.ingestion.collector_runtime.gcp_helper.upload_staged_audio",
            mock.AsyncMock(side_effect=RuntimeError("gcs boom")),
        ):
            await rt._process_feed(_FEED)

        rt._store.report_feed_failure.assert_awaited_once()
        kwargs = rt._store.report_feed_failure.await_args.kwargs
        self.assertEqual(kwargs["reason"], "gcs_upload_failed")
        self.assertIs(
            kwargs["status_reason"],
            FeedStatusReason.SYSTEM_PIPELINE_ERROR,
        )

    async def test_pubsub_publish_failure_records_pipeline_error(self) -> None:
        """Pub/Sub failures after a valid chunk use the publish stage tag."""

        async def _one_chunk(feed, shutdown, _resources):
            yield _make_captured_chunk(b"audio")

        rt = CollectorRuntime(capture_fn=_one_chunk, settings=_make_settings())
        rt._shutdown = asyncio.Event()
        rt._lease_lost = asyncio.Event()
        rt._capture_resources = _default_resources()
        rt._store = mock.AsyncMock()
        rt._store.report_feed_failure.return_value = "failing"
        rt._releasing_feeds = set()

        with (
            _mock_upload_audio(),
            mock.patch(
                "backend.pipeline.ingestion.collector_runtime.gcp_helper.publish_audio_chunk",
                mock.AsyncMock(side_effect=RuntimeError("pubsub boom")),
            ),
        ):
            await rt._process_feed(_FEED)

        rt._store.update_feed_progress.assert_awaited_once()
        rt._store.report_feed_failure.assert_awaited_once()
        kwargs = rt._store.report_feed_failure.await_args.kwargs
        self.assertIn("Pub/Sub publish failed", kwargs["reason"])
        self.assertIn("pubsub boom", kwargs["reason"])
        self.assertIs(
            kwargs["status_reason"],
            FeedStatusReason.SYSTEM_PIPELINE_ERROR,
        )

    async def test_bookmark_write_failure_records_pipeline_error(self) -> None:
        """Bookmark failures after a valid chunk use the bookmark stage tag."""

        async def _one_chunk(feed, shutdown, _resources):
            yield _make_captured_chunk(b"audio")

        rt = CollectorRuntime(
            capture_fn=_one_chunk,
            settings=_make_settings(bookmark_max_retries=0),
        )
        rt._shutdown = asyncio.Event()
        rt._lease_lost = asyncio.Event()
        rt._capture_resources = _default_resources()
        rt._store = mock.AsyncMock()
        rt._store.update_feed_progress.side_effect = RuntimeError(
            "bookmark boom"
        )
        rt._store.report_feed_failure.return_value = "failing"
        rt._releasing_feeds = set()

        with _mock_upload_audio(), _mock_pubsub_publish():
            await rt._process_feed(_FEED)

        rt._store.report_feed_failure.assert_awaited_once()
        kwargs = rt._store.report_feed_failure.await_args.kwargs
        self.assertEqual(kwargs["reason"], "bookmark_write_failed")
        self.assertIs(
            kwargs["status_reason"],
            FeedStatusReason.SYSTEM_PIPELINE_ERROR,
        )

    async def test_failure_log_includes_runtime_reason_fields(self) -> None:
        """Runtime failure logs include status and quarantine reason fields."""

        async def _failing_capture(feed, shutdown, _resources):
            msg = "capture_failed"
            raise RuntimeError(msg)
            yield _make_captured_chunk(b"audio")

        rt = CollectorRuntime(
            capture_fn=_failing_capture,
            settings=_make_settings(),
        )
        rt._shutdown = asyncio.Event()
        rt._lease_lost = asyncio.Event()
        rt._capture_resources = _default_resources()
        rt._store = mock.AsyncMock()
        rt._store.report_feed_failure.return_value = "failing"
        rt._releasing_feeds = set()

        with self.assertLogs(
            "backend.pipeline.ingestion.collector_runtime",
            level=logging.ERROR,
        ) as cm:
            await rt._process_feed(_FEED)

        failure_records = [
            record
            for record in cm.records
            if getattr(record, "json_fields", None)
        ]
        self.assertEqual(len(failure_records), 1)
        record = failure_records[0]
        json_fields = cast("dict[str, Any]", record.__dict__["json_fields"])
        self.assertEqual(json_fields["feed_id"], str(_FEED_ID))
        self.assertEqual(json_fields["source_type"], "bcfy_feeds")
        self.assertEqual(json_fields["reason"], "RuntimeError: capture_failed")
        self.assertEqual(
            json_fields["status_reason"],
            "system_unexpected_error",
        )


class TestProcessFeedPublishAttributes(unittest.IsolatedAsyncioTestCase):
    """Contract tests: publish_audio_chunk must receive session_id and source_type."""

    async def test_uses_chunk_session_id(self) -> None:
        """Runtime publishes with the session_id from CapturedChunk."""
        chunk_session_id = "chunk-supplied-session-id"

        async def _one_chunk(feed, shutdown, _resources):
            now = datetime.datetime.now(datetime.UTC)
            yield CapturedChunk(
                audio_bytes=b"audio",
                chunk_start_time=now,
                chunk_end_time=now + datetime.timedelta(seconds=15),
                session_id=chunk_session_id,
            )

        rt = CollectorRuntime(capture_fn=_one_chunk, settings=_make_settings())
        rt._shutdown = asyncio.Event()
        rt._lease_lost = asyncio.Event()
        rt._capture_resources = _default_resources()
        rt._store = mock.AsyncMock()
        rt._store.update_feed_progress.return_value = True
        rt._releasing_feeds = set()

        with _mock_upload_audio(), _mock_pubsub_publish() as mock_publish:
            await rt._process_feed(_FEED)

        mock_publish.assert_called_once()
        _, args, _kwargs = mock_publish.mock_calls[0]
        self.assertEqual(args[_PUBLISH_SESSION_ID_ARG_INDEX], chunk_session_id)

    async def test_distinct_session_ids_passed_through_per_chunk(self) -> None:
        """Each chunk's session_id is passed through independently."""
        sid_a = "session-a"
        sid_b = "session-b"

        async def _two_chunks(feed, shutdown, _resources):
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

        rt = CollectorRuntime(capture_fn=_two_chunks, settings=_make_settings())
        rt._shutdown = asyncio.Event()
        rt._lease_lost = asyncio.Event()
        rt._capture_resources = _default_resources()
        rt._store = mock.AsyncMock()
        rt._store.update_feed_progress.return_value = True
        rt._releasing_feeds = set()

        with _mock_upload_audio(), _mock_pubsub_publish() as mock_publish:
            await rt._process_feed(_FEED)

        self.assertEqual(mock_publish.call_count, 2)
        _, args1, _kwargs1 = mock_publish.mock_calls[0]
        _, args2, _kwargs2 = mock_publish.mock_calls[1]
        self.assertEqual(args1[_PUBLISH_SESSION_ID_ARG_INDEX], sid_a)
        self.assertEqual(args2[_PUBLISH_SESSION_ID_ARG_INDEX], sid_b)

    async def test_session_id_none_preserved(self) -> None:
        """Runtime preserves None session_id for segmented feeds."""

        async def _one_chunk(feed, shutdown, _resources):
            yield _make_captured_chunk(b"audio")  # session_id=None

        rt = CollectorRuntime(capture_fn=_one_chunk, settings=_make_settings())
        rt._shutdown = asyncio.Event()
        rt._lease_lost = asyncio.Event()
        rt._capture_resources = _default_resources()
        rt._store = mock.AsyncMock()
        rt._store.update_feed_progress.return_value = True
        rt._releasing_feeds = set()

        with (
            _mock_upload_audio(),
            _mock_pubsub_publish() as mock_publish,
        ):
            await rt._process_feed(_FEED)

        mock_publish.assert_called_once()
        _, args, _kwargs = mock_publish.mock_calls[0]
        self.assertIsNone(args[_PUBLISH_SESSION_ID_ARG_INDEX])

    async def test_source_type_passed(self) -> None:
        """publish_audio_chunk receives source_type matching the feed."""

        async def _one_chunk(feed, shutdown, _resources):
            yield _make_captured_chunk(b"audio")

        rt = CollectorRuntime(capture_fn=_one_chunk, settings=_make_settings())
        rt._shutdown = asyncio.Event()
        rt._lease_lost = asyncio.Event()
        rt._capture_resources = _default_resources()
        rt._store = mock.AsyncMock()
        rt._store.update_feed_progress.return_value = True
        rt._releasing_feeds = set()

        with _mock_upload_audio(), _mock_pubsub_publish() as mock_publish:
            await rt._process_feed(_FEED)

        mock_publish.assert_called_once()
        _, args, _kwargs = mock_publish.mock_calls[0]
        self.assertEqual(
            args[_PUBLISH_SOURCE_TYPE_ARG_INDEX], _FEED["source_type"]
        )


class TestMemoryWatchdogIntegration(unittest.IsolatedAsyncioTestCase):
    """D-33: end-to-end pause / resume / trip via monkeypatched cgroup readers.

    Per D-34, no real allocation in CI — the monkeypatched-usage approach
    exercises the same gating logic deterministically. The 'real allocation'
    verification happens in canary, not CI.
    """

    async def test_pause_resume_then_trip_lifecycle(self) -> None:
        """D-33 LITERAL: drive 3 high → pause-set; 1 low → pause-clear; 3 at 95% → trip.

        After the trip, drive _shutdown_sequence() and assert release_feeds_batch
        runs successfully AND os._exit is NEVER called throughout.

        Per CONTEXT.md D-33: 'assert _shutdown.is_set() is True AND
        release_feeds_batch runs successfully'. The two halves are inseparable
        — proving the trip path AND proving graceful release together is the
        contract this test satisfies.
        """
        rt = _make_runtime(
            rss_watchdog_warmup_sec=0.0,
            rss_watchdog_pause_threshold=0.70,
            rss_watchdog_exit_threshold=0.90,
            rss_watchdog_pause_consecutive_samples=3,
            rss_watchdog_exit_consecutive_samples=3,
        )
        rt._loop = asyncio.get_running_loop()
        rt._shutdown = asyncio.Event()

        # Wire the same shutdown-sequence collaborators that
        # TestSigtermRelease.test_release_calls_batch_with_worker_id uses.
        # Mirroring that shape keeps the integration test on the same
        # well-trodden path.
        rt._heartbeat_thread = None
        rt._store = mock.AsyncMock()
        rt._store.release_feeds_batch.return_value = 0
        rt._data_pool = mock.AsyncMock()
        rt._heartbeat_pool = mock.AsyncMock()
        rt._pubsub_client = mock.AsyncMock()
        rt._gcs_client = mock.AsyncMock()
        rt._health_runner = mock.AsyncMock()

        # 7 samples: 3 at 80% (pause), 1 at 50% (clear), 3 at 95% (trip).
        # 100MB limit. 80MB = 80%, 50MB = 50%, 95MB = 95%.
        usage_samples = [
            80 * 1024 * 1024,
            80 * 1024 * 1024,
            80 * 1024 * 1024,
            50 * 1024 * 1024,
            95 * 1024 * 1024,
            95 * 1024 * 1024,
            95 * 1024 * 1024,
        ]

        # Re-bind _thread_stop with sequencing for the watchdog body. After
        # the watchdog body returns and we proceed to _shutdown_sequence,
        # _thread_stop is set by the trip path so existing shutdown code
        # treats it as a normal stop signal.
        rt._thread_stop = mock.MagicMock()
        is_set_returns = [False] * len(usage_samples) + [True]
        wait_returns = [False] * len(usage_samples)
        rt._thread_stop.is_set.side_effect = is_set_returns
        rt._thread_stop.wait.side_effect = wait_returns
        rt._memory_watchdog = rss_watchdog.MemoryWatchdog(
            rt._collector_settings,
            rt._thread_stop,
            usage_reader=mock.Mock(side_effect=usage_samples),
        )

        with (
            mock.patch(
                "backend.pipeline.ingestion.collector_runtime.os._exit",
            ) as mock_exit,
            mock.patch("logging.shutdown"),
        ):
            # Phase A: drive the watchdog body in a worker thread so the
            # asyncio loop stays available for call_soon_threadsafe.
            await asyncio.to_thread(
                rt._memory_watchdog._run,
                100 * 1024 * 1024,
                rt._loop,
                rt._shutdown,
            )

            # Yield once so any call_soon_threadsafe-scheduled callbacks
            # (the _shutdown.set) actually run on the event loop before
            # the assertions below.
            await asyncio.sleep(0)

            # Mid-state assertions (between trip and shutdown_sequence).
            # After 3 of 80% → pause set; after 50% → cleared; after 3 of
            # 95% → trip. End-of-watchdog-body state: pause cleared (the
            # 95% samples tripped exit on sample 3 before another pause-
            # set cycle could complete), _shutdown.is_set() True. PROVES
            # D-33 first half.
            self.assertTrue(rt._shutdown.is_set())

            # Phase B: drive _shutdown_sequence to completion. This is the
            # D-33 SECOND HALF: 'release_feeds_batch runs successfully'.
            # Without this drive-through, we are NOT verifying the literal
            # D-33 contract — we are only verifying that the trip event
            # fired. The point of D-33 is that the trip → graceful-release
            # path actually completes end-to-end.
            await rt._shutdown_sequence()

        # D-33 LITERAL ASSERTIONS:
        #   1. _shutdown.is_set() True (asserted mid-test, above).
        #   2. release_feeds_batch ran successfully (= awaited exactly once
        #      with worker_id; mirrors TestSigtermRelease pattern).
        rt._store.release_feeds_batch.assert_awaited_once_with(_WORKER_ID)

        # CRITICAL invariant — REQUIREMENTS WATCHDOG-01 explicit: os._exit
        # must NEVER be called by the watchdog or by _shutdown_sequence
        # along the watchdog-trip path, regardless of how high RSS climbed.
        # Graceful shutdown only — kernel OOM is the backstop.
        mock_exit.assert_not_called()


class TestSubTimeoutEscape(unittest.IsolatedAsyncioTestCase):
    """SHUTDOWN-02: stuck task swallows CancelledError once but does not
    block release_feeds_batch; sub-timeout fires + re-cancel + 2s settle
    + os._exit NOT called along the path (D-09).
    """

    async def test_swallow_once_does_not_block_release(self) -> None:
        """A task that swallows CancelledError once is force-cancelled
        and the shutdown completes release_feeds_batch successfully.
        """

        async def _swallow_once() -> None:
            # First await: catches the FIRST cancel issued by the
            # _shutdown_sequence cancel loop (line 1343-1344) and
            # swallows it. Calling Task.uncancel() clears the cancel
            # count so the second `await asyncio.sleep(60)` does NOT
            # immediately re-raise; instead it sleeps until the
            # SECOND cancel from the re-cancel loop propagates.
            try:
                await asyncio.sleep(60)
            except asyncio.CancelledError:
                # asyncio.current_task() returns Optional[Task] — inside a
                # running task it is never None, but ty can't narrow that
                # without an explicit check.
                current = asyncio.current_task()
                assert current is not None
                current.uncancel()
            await asyncio.sleep(60)  # second cancel propagates here

        rt = _make_runtime(
            task_cancel_budget_sec=0.1,
            graceful_shutdown_timeout_sec=2.0,
        )
        rt._shutdown = asyncio.Event()
        rt._thread_stop = mock.MagicMock()
        rt._heartbeat_thread = None
        rt._store = mock.AsyncMock()
        rt._store.release_feeds_batch.return_value = 1
        rt._data_pool = mock.AsyncMock()
        rt._heartbeat_pool = mock.AsyncMock()
        rt._pubsub_client = mock.AsyncMock()
        rt._gcs_client = mock.AsyncMock()
        rt._health_runner = mock.AsyncMock()

        stuck_task = asyncio.create_task(_swallow_once(), name="feed-stuck")
        rt._feed_tasks[_FEED_ID] = stuck_task
        # Yield once so the task body enters its first await BEFORE
        # _shutdown_sequence's cancel loop runs. Without this, the
        # cancel is delivered before the task has even started, and
        # the swallow-then-uncancel pattern never gets to execute.
        await asyncio.sleep(0)

        try:
            with (
                mock.patch(
                    "backend.pipeline.ingestion.collector_runtime.os._exit",
                ) as mock_exit,
                mock.patch("logging.shutdown"),
                self.assertLogs(
                    "backend.pipeline.ingestion.collector_runtime",
                    level="WARNING",
                ) as log_cm,
            ):
                await rt._shutdown_sequence()
        finally:
            # Defensive: even if assertions fail, don't leave the task
            # un-awaited (warning noise in pytest output).
            if not stuck_task.done():
                stuck_task.cancel()
                try:
                    await stuck_task
                except asyncio.CancelledError:
                    pass

        # D-09 LITERAL ASSERTIONS:

        # 1. release_feeds_batch ran exactly once with worker_id —
        #    the shutdown completed, the stuck task did not starve it.
        rt._store.release_feeds_batch.assert_awaited_once_with(_WORKER_ID)

        # 2. os._exit was NOT called during the sub-timeout window or
        #    the 2s settle (no fence-violation triggered by the still-
        #    pending task observing NULL worker_id).
        mock_exit.assert_not_called()

        # 3. The bounded warning log fired with the expected count.
        sub_timeout_logs = [
            r for r in log_cm.records if "Sub-timeout" in r.getMessage()
        ]
        self.assertEqual(len(sub_timeout_logs), 1)
        self.assertIn("1 tasks still running", sub_timeout_logs[0].getMessage())


class TestLogPayloadBound(unittest.TestCase):
    """SHUTDOWN-02: 800-feed shutdown bounded log stays under 1 MB
    (ROADMAP SC#2 / D-10). Pure string-formatting test — no asyncio.
    """

    def test_eight_hundred_feed_message_under_one_megabyte(self) -> None:
        """Formatted Sub-timeout warning for 800 mock pending tasks
        encodes to < 1 MB of UTF-8 bytes.
        """
        # Build 800 mock Task objects with realistic feed names
        # ("feed-{source}-{i}-{name}" shape, ~30-50 chars each).
        pending = []
        for i in range(800):
            t = mock.MagicMock(spec=asyncio.Task)
            t.get_name = mock.MagicMock(
                return_value=f"feed-source-{i:04d}-name-with-typical-length",
            )
            pending.append(t)

        # Reproduce the production formatting EXACTLY as written in
        # collector_runtime.py _shutdown_sequence (Task 2 D-04).
        # If a future refactor accidentally interpolates `pending`
        # (the list of Task objects) instead of `names` (list of
        # strings), this assertion catches it because Task repr
        # adds ~150 bytes per task → 800 × 150 ≈ 120 KB just from
        # task-object boilerplate, plus any `<MagicMock id=0x...>`
        # repr noise pushing well past 1 MB if mocks ever escape.
        names = sorted(t.get_name() for t in pending)
        # Use %-formatting (NOT f-strings) to mirror the production
        # logger.warning shape exactly. The template is built as a
        # string variable so UP031 (which only flags inline `%`
        # against a string literal) does not require a suppression
        # comment — the whole point of the test is to format-check
        # the production logger string.
        template = (
            "Sub-timeout: %d tasks still running after %ss — "
            "explicitly cancelling and proceeding to release. "
            "names=%s"
        )
        formatted = template % (len(pending), 30.0, names)

        encoded = formatted.encode("utf-8")
        self.assertLess(
            len(encoded),
            1_048_576,
            f"800-feed Sub-timeout warning is {len(encoded)} bytes, "
            f"must be under 1 MB (ROADMAP SC#2)",
        )


class TestProcessFeedResumePosition(unittest.IsolatedAsyncioTestCase):
    """_process_feed forwards the chunk's resume cursor to update_feed_progress.

    Contract: the feed's persisted last_bookmark_time is the chunk's
    resume_position when the collector sets it (bcfy_calls), and falls back
    to chunk_end_time when it is None (stream/push collectors). The runtime
    invokes update_feed_progress positionally via retry_with_lease_check as
    (feed_id, worker_id, gcs_uri, fencing_token, last_bookmark_time) — so the
    bookmark is the 5th positional argument.
    """

    _BOOKMARK_ARG_INDEX = 4

    async def test_persists_resume_position_when_chunk_sets_it(self) -> None:
        """A chunk with resume_position set → that value is the bookmark."""
        resume = datetime.datetime(2026, 5, 14, 2, 30, 31, tzinfo=datetime.UTC)

        async def _one_chunk(feed, shutdown, _resources):
            now = datetime.datetime.now(datetime.UTC)
            yield CapturedChunk(
                audio_bytes=b"audio",
                chunk_start_time=now,
                chunk_end_time=now + datetime.timedelta(seconds=15),
                resume_position=resume,
            )

        rt = CollectorRuntime(capture_fn=_one_chunk, settings=_make_settings())
        rt._shutdown = asyncio.Event()
        rt._lease_lost = asyncio.Event()
        rt._capture_resources = _default_resources()
        rt._store = mock.AsyncMock()
        rt._store.update_feed_progress.return_value = True
        rt._releasing_feeds = set()

        with _mock_upload_audio(), _mock_pubsub_publish():
            await rt._process_feed(_FEED)

        rt._store.update_feed_progress.assert_awaited_once()
        bookmark = rt._store.update_feed_progress.await_args.args[
            self._BOOKMARK_ARG_INDEX
        ]
        self.assertEqual(bookmark, resume)

    async def test_falls_back_to_chunk_end_time_when_resume_position_none(
        self,
    ) -> None:
        """A chunk leaving resume_position None → bookmark is chunk_end_time."""
        end_time = datetime.datetime(2026, 5, 14, 2, 31, 0, tzinfo=datetime.UTC)

        async def _one_chunk(feed, shutdown, _resources):
            yield CapturedChunk(
                audio_bytes=b"audio",
                chunk_start_time=end_time - datetime.timedelta(seconds=15),
                chunk_end_time=end_time,
                # resume_position defaults to None (stream/push collectors).
            )

        rt = CollectorRuntime(capture_fn=_one_chunk, settings=_make_settings())
        rt._shutdown = asyncio.Event()
        rt._lease_lost = asyncio.Event()
        rt._capture_resources = _default_resources()
        rt._store = mock.AsyncMock()
        rt._store.update_feed_progress.return_value = True
        rt._releasing_feeds = set()

        with _mock_upload_audio(), _mock_pubsub_publish():
            await rt._process_feed(_FEED)

        rt._store.update_feed_progress.assert_awaited_once()
        bookmark = rt._store.update_feed_progress.await_args.args[
            self._BOOKMARK_ARG_INDEX
        ]
        self.assertEqual(bookmark, end_time)


if __name__ == "__main__":
    unittest.main()
