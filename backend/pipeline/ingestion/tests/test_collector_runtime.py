"""Focused tests for generic CollectorRuntime composition and lifecycle."""

from __future__ import annotations

import asyncio
import datetime
import unittest
import uuid
from unittest import mock

import aiohttp

from backend.pipeline.ingestion import (
    collector_runtime,
    failure_policy,
    grant_control,
    grant_supervisor,
    worker_profiles,
)
from backend.pipeline.ingestion import (
    settings as ingestion_settings,
)
from backend.pipeline.ingestion.models import (
    CapturedChunk,
    CaptureResources,
    FeedFailure,
    SourceObservation,
)
from backend.pipeline.storage import feed_store
from backend.pipeline.storage.settings import AlloyDBSettings

_WORKER_ID = uuid.UUID("11111111-2222-3333-4444-555555555555")
_FEED_ID = uuid.UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
_ACTOR_ID = "service_account:gcp:test"


def _settings(
    mode: worker_profiles.BcfyCallsAuthorityMode = (
        worker_profiles.BcfyCallsAuthorityMode.SID_LEASE
    ),
) -> ingestion_settings.CollectorSettings:
    """Build complete, deterministic settings for runtime unit tests."""
    caps = {
        feed_store.SourceType.BCFY_FEEDS: 240,
        feed_store.SourceType.BCFY_CALLS: 600,
        feed_store.SourceType.OPENMHZ: 900,
        feed_store.SourceType.FIRE_NOTIFICATIONS: 600,
    }
    return ingestion_settings.CollectorSettings(
        worker_id=_WORKER_ID,
        bcfy_calls_authority_mode=mode,
        caps=caps,
        max_feeds_per_worker=800,
        max_sids_per_worker=32,
        lease_admission_cycle_budget=20,
        sid_lease_admission_cycle_budget=2,
        bcfy_calls_work_concurrency=16,
        lease_poll_interval_sec=5.0,
        lease_poll_jitter_max_sec=0.0,
        startup_stagger_max_sec=0.0,
        startup_jitter_max_sec=0.0,
        heartbeat_interval_sec=15.0,
        heartbeat_stall_timeout_sec=45.0,
        graceful_shutdown_timeout_sec=90.0,
        task_cancel_budget_sec=30.0,
        rss_watchdog_poll_interval_sec=2.0,
        rss_watchdog_pause_threshold=0.70,
        rss_watchdog_exit_threshold=0.90,
        rss_watchdog_pause_consecutive_samples=3,
        rss_watchdog_exit_consecutive_samples=3,
        rss_watchdog_warmup_sec=60.0,
        abandonment_window_sec=60.0,
        feed_failure_threshold=5,
        audio_staging_bucket="test-bucket",
        continuous_pubsub_topic_path="projects/p/topics/continuous",
        segmented_pubsub_topic_path="projects/p/topics/segmented",
        google_cloud_project=None,
        segment_temp_dir=None,
        health_check_port=8080,
        health_check_startup_grace_sec=120.0,
        gcs_upload_max_retries=3,
        gcs_upload_retry_base_delay_sec=0.5,
        gcs_upload_retry_max_delay_sec=8.0,
        bookmark_max_retries=2,
        bookmark_retry_base_delay_sec=0.5,
        bookmark_retry_max_delay_sec=4.0,
        pubsub_publish_max_retries=2,
        pubsub_publish_retry_base_delay_sec=0.5,
        pubsub_publish_retry_max_delay_sec=4.0,
        db=AlloyDBSettings(
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
    )


async def _empty_capture(
    _feed: feed_store.LeasedFeed,
    _stop: asyncio.Event,
    _resources: CaptureResources,
):
    if False:
        yield SourceObservation()


def _runtime(
    mode: worker_profiles.BcfyCallsAuthorityMode = (
        worker_profiles.BcfyCallsAuthorityMode.SID_LEASE
    ),
) -> collector_runtime.CollectorRuntime:
    runtime = collector_runtime.CollectorRuntime(
        _empty_capture,
        _settings(mode),
        runtime_actor_id=_ACTOR_ID,
    )
    runtime._shutdown = asyncio.Event()
    runtime._capture_resources = CaptureResources(
        http_session=mock.AsyncMock(spec=aiohttp.ClientSession),
    )
    return runtime


def _feed() -> feed_store.LeasedFeed:
    return feed_store.LeasedFeed(
        id=_FEED_ID,
        name="Test Feed",
        source_type=feed_store.SourceType.BCFY_FEEDS,
        last_processed_filename=None,
        last_bookmark_time=None,
        fencing_token=7,
        failure_count=0,
        status_reason=None,
        source_feed_id="123",
        tags=None,
    )


def _grant() -> feed_store.FeedGrant:
    return feed_store.FeedGrant(
        feed_id=_FEED_ID,
        owner_worker_id=_WORKER_ID,
        fencing_token=7,
    )


def _context() -> grant_control.RunContext:
    return grant_control.RunContext(
        stop_requested=asyncio.Event(),
        grant_lost=asyncio.Event(),
    )


class TestPacingHelpers(unittest.TestCase):
    """Small deterministic scheduling helpers retain their old contracts."""

    def test_startup_delay_combines_stagger_and_jitter(self) -> None:
        with mock.patch.object(
            collector_runtime.random,
            "uniform",
            return_value=1.25,
        ):
            deterministic, jitter, total = (
                collector_runtime._startup_pacing_delay(
                    _WORKER_ID,
                    60.0,
                    2.0,
                )
            )

        self.assertGreaterEqual(deterministic, 0.0)
        self.assertLess(deterministic, 60.0)
        self.assertEqual(jitter, 1.25)
        self.assertEqual(total, deterministic + jitter)

    def test_heartbeat_tick_resets_large_drift(self) -> None:
        self.assertEqual(
            collector_runtime._advance_heartbeat_tick(
                next_tick=10.0,
                interval=5.0,
                now=25.1,
            ),
            30.1,
        )


class TestSupervisorComposition(unittest.IsolatedAsyncioTestCase):
    """Runtime selects one common supervisor for Feed and SID grants."""

    async def _compose(
        self,
        mode: worker_profiles.BcfyCallsAuthorityMode,
    ) -> tuple[
        collector_runtime.CollectorRuntime,
        mock.AsyncMock,
        mock.MagicMock,
    ]:
        runtime = _runtime(mode)
        runtime._data_pool = mock.MagicMock()
        runtime._heartbeat_pool = mock.MagicMock()
        runtime._http_session = mock.MagicMock()

        pool = mock.AsyncMock()
        supervisor = mock.MagicMock()
        with (
            mock.patch.object(
                collector_runtime.bcfy_calls_work_pool,
                "BcfyCallsWorkPool",
                return_value=pool,
            ) as pool_constructor,
            mock.patch.object(
                collector_runtime.grant_supervisor,
                "GrantSupervisor",
                return_value=supervisor,
            ) as supervisor_constructor,
            mock.patch.object(
                collector_runtime.bcfy_calls_provider,
                "CallsProviderClient",
                return_value=mock.MagicMock(),
            ),
            mock.patch.object(
                collector_runtime.bcfy_calls_pipeline,
                "BcfyCallsFeedBatchExecutor",
                return_value=mock.MagicMock(),
            ),
            mock.patch.object(
                collector_runtime.bcfy_calls_sid_runner,
                "BcfyCallsSidRunner",
                return_value=mock.MagicMock(),
            ),
        ):
            await runtime._compose_supervisor()

        pool.start.assert_awaited_once_with()
        pool_constructor.assert_called_once()
        return runtime, pool, supervisor_constructor

    async def test_composes_feed_and_sid_into_one_supervisor(self) -> None:
        runtime, pool, supervisor_constructor = await self._compose(
            worker_profiles.BcfyCallsAuthorityMode.SID_LEASE
        )

        _profile, registrations = supervisor_constructor.call_args.args
        self.assertEqual(
            [registration.domain_id for registration in registrations],
            [
                grant_control.DomainId.FEED,
                grant_control.DomainId.SID,
            ],
        )
        self.assertIs(runtime._supervisor, supervisor_constructor.return_value)
        self.assertIs(runtime._work_pool, pool)

    async def test_failed_work_pool_start_is_not_published_for_shutdown(
        self,
    ) -> None:
        runtime = _runtime()
        runtime._data_pool = mock.MagicMock()
        runtime._heartbeat_pool = mock.MagicMock()
        runtime._http_session = mock.MagicMock()
        pool = mock.AsyncMock()
        pool.start.side_effect = RuntimeError("pool start failed")

        with (
            mock.patch.object(
                collector_runtime.bcfy_calls_work_pool,
                "BcfyCallsWorkPool",
                return_value=pool,
            ),
            mock.patch.object(
                collector_runtime.bcfy_calls_provider,
                "CallsProviderClient",
                return_value=mock.MagicMock(),
            ),
            mock.patch.object(
                collector_runtime.bcfy_calls_pipeline,
                "BcfyCallsFeedBatchExecutor",
                return_value=mock.MagicMock(),
            ),
        ):
            with self.assertRaisesRegex(RuntimeError, "pool start failed"):
                await runtime._compose_supervisor()

        self.assertIsNone(runtime._work_pool)

    async def test_sid_mode_excludes_calls_from_feed_claim_store(
        self,
    ) -> None:
        runtime = _runtime(worker_profiles.BcfyCallsAuthorityMode.SID_LEASE)
        runtime._data_pool = mock.MagicMock()
        runtime._heartbeat_pool = mock.MagicMock()
        runtime._http_session = mock.MagicMock()

        with (
            mock.patch.object(
                collector_runtime,
                "FeedStore",
                side_effect=(mock.MagicMock(), mock.MagicMock()),
            ) as feed_store_constructor,
            mock.patch.object(
                collector_runtime.bcfy_calls_work_pool,
                "BcfyCallsWorkPool",
                return_value=mock.AsyncMock(),
            ),
            mock.patch.object(
                collector_runtime.grant_supervisor,
                "GrantSupervisor",
                return_value=mock.MagicMock(),
            ),
            mock.patch.object(
                collector_runtime.bcfy_calls_provider,
                "CallsProviderClient",
                return_value=mock.MagicMock(),
            ),
            mock.patch.object(
                collector_runtime.bcfy_calls_pipeline,
                "BcfyCallsFeedBatchExecutor",
                return_value=mock.MagicMock(),
            ),
            mock.patch.object(
                collector_runtime.bcfy_calls_sid_runner,
                "BcfyCallsSidRunner",
                return_value=mock.MagicMock(),
            ),
        ):
            await runtime._compose_supervisor()

        for call in feed_store_constructor.call_args_list:
            claim_types = call.kwargs["claim_types"]
            self.assertNotIn(
                feed_store.SourceType.BCFY_CALLS,
                claim_types,
            )
            self.assertIn(feed_store.SourceType.BCFY_FEEDS, claim_types)

    async def test_legacy_mode_keeps_calls_in_feed_claim_store(
        self,
    ) -> None:
        runtime = _runtime(worker_profiles.BcfyCallsAuthorityMode.LEGACY_FEED)
        runtime._data_pool = mock.MagicMock()
        runtime._heartbeat_pool = mock.MagicMock()
        runtime._http_session = mock.MagicMock()

        with (
            mock.patch.object(
                collector_runtime,
                "FeedStore",
                side_effect=(mock.MagicMock(), mock.MagicMock()),
            ) as feed_store_constructor,
            mock.patch.object(
                collector_runtime.bcfy_calls_work_pool,
                "BcfyCallsWorkPool",
                return_value=mock.AsyncMock(),
            ),
            mock.patch.object(
                collector_runtime.grant_supervisor,
                "GrantSupervisor",
                return_value=mock.MagicMock(),
            ),
            mock.patch.object(
                collector_runtime.bcfy_calls_provider,
                "CallsProviderClient",
                return_value=mock.MagicMock(),
            ),
            mock.patch.object(
                collector_runtime.bcfy_calls_pipeline,
                "BcfyCallsFeedBatchExecutor",
                return_value=mock.MagicMock(),
            ),
            mock.patch.object(
                collector_runtime.bcfy_calls_sid_runner,
                "BcfyCallsSidRunner",
                return_value=mock.MagicMock(),
            ),
        ):
            await runtime._compose_supervisor()

        for call in feed_store_constructor.call_args_list:
            self.assertIn(
                feed_store.SourceType.BCFY_CALLS,
                call.kwargs["claim_types"],
            )


class TestAdmissionAndHeartbeat(unittest.IsolatedAsyncioTestCase):
    """Admission and heartbeat both delegate to the sole supervisor."""

    async def test_admission_cycle_delegates_once(self) -> None:
        runtime = _runtime()
        supervisor = mock.MagicMock()
        supervisor.admit_cycle = mock.AsyncMock()
        runtime._supervisor = supervisor
        runtime._memory_watchdog = mock.MagicMock()
        runtime._memory_watchdog.is_paused.return_value = False
        sleep = mock.AsyncMock(return_value=True)

        with mock.patch.object(runtime, "_sleep_or_shutdown", sleep):
            await runtime._leasing_loop()

        supervisor.admit_cycle.assert_awaited_once_with(_WORKER_ID)

    async def test_memory_pause_skips_admission(self) -> None:
        runtime = _runtime()
        supervisor = mock.MagicMock()
        supervisor.admit_cycle = mock.AsyncMock()
        runtime._supervisor = supervisor
        runtime._memory_watchdog = mock.MagicMock()
        runtime._memory_watchdog.is_paused.return_value = True
        sleep = mock.AsyncMock(return_value=True)

        with mock.patch.object(runtime, "_sleep_or_shutdown", sleep):
            await runtime._leasing_loop()

        supervisor.admit_cycle.assert_not_awaited()

    async def test_heartbeat_delegates_and_stamps_health(self) -> None:
        runtime = _runtime()
        supervisor = mock.MagicMock()

        async def heartbeat(on_dispatch) -> None:
            on_dispatch()

        supervisor.heartbeat_cycle = mock.AsyncMock(side_effect=heartbeat)
        runtime._supervisor = supervisor

        await runtime._heartbeat_cycle()

        supervisor.heartbeat_cycle.assert_awaited_once()
        self.assertIsNotNone(runtime._health_state.last_heartbeat_tick)

    async def test_integrity_event_raises_exact_failure(self) -> None:
        runtime = _runtime()
        failure = grant_control.GrantControlIntegrityError("uncertain")
        supervisor = mock.MagicMock()
        supervisor.integrity_failure_event = asyncio.Event()
        supervisor.integrity_failure_event.set()
        supervisor.integrity_failure = failure
        runtime._supervisor = supervisor

        with self.assertRaisesRegex(
            grant_control.GrantControlIntegrityError,
            "uncertain",
        ):
            await runtime._sleep_or_shutdown(1.0)


class TestFeedRunner(unittest.IsolatedAsyncioTestCase):
    """Feed work returns closed outcomes without owning finalization."""

    async def test_preexisting_loss_returns_run_lost(self) -> None:
        runtime = _runtime()
        context = _context()
        context.grant_lost.set()

        result = await runtime._process_feed(
            _grant(),
            _feed(),
            context,
        )

        self.assertIsInstance(result, grant_control.RunLost)

    async def test_stop_request_returns_normal_completion(self) -> None:
        runtime = _runtime()
        context = _context()
        context.stop_requested.set()

        result = await runtime._process_feed(
            _grant(),
            _feed(),
            context,
        )

        self.assertIsInstance(result, grant_control.RunCompleted)

    async def test_collector_failure_returns_failure_evidence(self) -> None:
        async def failing_capture(_feed, _stop, _resources):
            if False:
                yield SourceObservation()
            raise FeedFailure(
                feed_store.FeedStatusReason.SOURCE_OFFLINE,
                "offline",
            )

        runtime = collector_runtime.CollectorRuntime(
            failing_capture,
            _settings(),
            runtime_actor_id=_ACTOR_ID,
        )
        runtime._capture_resources = CaptureResources(
            http_session=mock.AsyncMock(spec=aiohttp.ClientSession),
        )

        result = await runtime._process_feed(
            _grant(),
            _feed(),
            _context(),
        )

        self.assertEqual(
            result,
            grant_control.RunFailed(
                feed_store.FeedStatusReason.SOURCE_OFFLINE,
                "offline",
            ),
        )

    async def test_bookmark_fence_rejection_returns_run_lost(self) -> None:
        now = datetime.datetime.now(datetime.UTC)

        async def one_chunk(_feed, _stop, _resources):
            yield CapturedChunk(
                audio_bytes=b"audio",
                chunk_start_time=now,
                chunk_end_time=now + datetime.timedelta(seconds=15),
            )

        runtime = collector_runtime.CollectorRuntime(
            one_chunk,
            _settings(),
            runtime_actor_id=_ACTOR_ID,
        )
        runtime._capture_resources = CaptureResources(
            http_session=mock.AsyncMock(spec=aiohttp.ClientSession),
        )
        runtime._store = mock.AsyncMock()
        runtime._store.update_feed_progress.return_value = False

        with mock.patch.object(
            collector_runtime.gcp_helper,
            "upload_staged_audio",
            new_callable=mock.AsyncMock,
            return_value="gs://bucket/item.flac",
        ):
            result = await runtime._process_feed(
                _grant(),
                _feed(),
                _context(),
            )

        self.assertIsInstance(result, grant_control.RunLost)

    async def test_publish_settles_after_stop_follows_bookmark(self) -> None:
        runtime = _runtime()
        runtime._store = mock.AsyncMock()
        context = _context()

        async def update_progress(*_args, **_kwargs) -> bool:
            context.stop_requested.set()
            return True

        runtime._store.update_feed_progress.side_effect = update_progress
        now = datetime.datetime.now(datetime.UTC)
        chunk = CapturedChunk(
            audio_bytes=b"audio",
            chunk_start_time=now,
            chunk_end_time=now + datetime.timedelta(seconds=15),
        )
        with (
            mock.patch.object(
                collector_runtime.gcp_helper,
                "upload_staged_audio",
                new_callable=mock.AsyncMock,
                return_value="gs://bucket/item.flac",
            ),
            mock.patch.object(
                collector_runtime.gcp_helper,
                "publish_audio_chunk",
                new_callable=mock.AsyncMock,
                return_value="message-id",
            ) as publish,
        ):
            await runtime._process_captured_chunk(
                _feed(),
                chunk,
                0,
                _grant(),
                "projects/p/topics/continuous",
                "flac",
                "audio/flac",
                context,
            )

        publish.assert_awaited_once()

    async def test_admitted_chunk_settles_after_runner_cancellation(
        self,
    ) -> None:
        now = datetime.datetime.now(datetime.UTC)

        async def one_chunk(_feed, _stop, _resources):
            yield CapturedChunk(
                audio_bytes=b"audio",
                chunk_start_time=now,
                chunk_end_time=now + datetime.timedelta(seconds=15),
            )

        runtime = collector_runtime.CollectorRuntime(
            one_chunk,
            _settings(),
            runtime_actor_id=_ACTOR_ID,
        )
        runtime._capture_resources = CaptureResources(
            http_session=mock.AsyncMock(spec=aiohttp.ClientSession),
        )
        runtime._store = mock.AsyncMock()
        runtime._store.update_feed_progress.return_value = True
        upload_started = asyncio.Event()
        finish_upload = asyncio.Event()

        async def upload(*_args, **_kwargs) -> str:
            upload_started.set()
            await finish_upload.wait()
            return "gs://bucket/item.flac"

        with (
            mock.patch.object(
                collector_runtime.gcp_helper,
                "upload_staged_audio",
                side_effect=upload,
            ),
            mock.patch.object(
                collector_runtime.gcp_helper,
                "publish_audio_chunk",
                new_callable=mock.AsyncMock,
                return_value="message-id",
            ) as publish,
        ):
            processing = asyncio.create_task(
                runtime._process_feed(
                    _grant(),
                    _feed(),
                    _context(),
                )
            )
            await upload_started.wait()
            processing.cancel()
            await asyncio.sleep(0)
            self.assertFalse(processing.done())

            finish_upload.set()
            with self.assertRaises(asyncio.CancelledError):
                await processing

        runtime._store.update_feed_progress.assert_awaited_once()
        publish.assert_awaited_once()

    async def test_rejected_observation_sets_grant_lost(self) -> None:
        runtime = _runtime()
        runtime._store = mock.AsyncMock()
        runtime._store.record_source_observation.return_value = (
            feed_store.SourceObservationResult(
                id=_FEED_ID,
                current_status="deactivated",
                current_worker=None,
                current_fencing_token=7,
                recorded=False,
            )
        )
        context = _context()

        with self.assertRaises(collector_runtime.LeaseExpiredError):
            await runtime._process_source_observation(
                _feed(),
                SourceObservation(
                    resume_position=datetime.datetime.now(datetime.UTC)
                ),
                _grant(),
                context,
            )

        self.assertTrue(context.grant_lost.is_set())


class TestFailurePolicy(unittest.TestCase):
    """Feed and SID failures use the same budget classification."""

    def test_only_configuration_failure_consumes_budget(self) -> None:
        runtime = _runtime()

        budgeted = runtime._plan_failure(
            feed_store.FeedStatusReason.SYSTEM_CONFIGURATION_INVALID,
            "bad membership",
        )
        non_budgeted = runtime._plan_failure(
            feed_store.FeedStatusReason.SYSTEM_PIPELINE_ERROR,
            "storage outage",
        )

        self.assertIsInstance(
            budgeted.treatment,
            failure_policy.ConsumeFailureBudget,
        )
        self.assertIsInstance(
            non_budgeted.treatment,
            failure_policy.RetryWithoutBudget,
        )


class TestShutdown(unittest.IsolatedAsyncioTestCase):
    """Shutdown drains supervisor before queue and shared resources."""

    async def test_shutdown_order_is_supervisor_pool_clients_stores(
        self,
    ) -> None:
        runtime = _runtime()
        order: list[str] = []
        runtime._health_runner = mock.AsyncMock()
        runtime._memory_watchdog = mock.MagicMock()
        runtime._memory_watchdog.join = mock.AsyncMock()
        runtime._work_pool = mock.AsyncMock()
        runtime._work_pool.close.side_effect = lambda: order.append("pool")
        runtime._pubsub_client = mock.AsyncMock()
        runtime._pubsub_client.close.side_effect = lambda: order.append(
            "pubsub"
        )
        runtime._gcs_client = mock.AsyncMock()
        runtime._gcs_client.close.side_effect = lambda: order.append("gcs")
        runtime._http_session = mock.AsyncMock()
        runtime._http_session.close.side_effect = lambda: order.append("http")
        runtime._heartbeat_pool = mock.MagicMock()
        runtime._data_pool = mock.MagicMock()

        supervisor = mock.MagicMock()

        async def shutdown(**kwargs) -> None:
            order.append("supervisor")
            await kwargs["stop_heartbeat_supervision"]()

        supervisor.shutdown = mock.AsyncMock(side_effect=shutdown)
        runtime._supervisor = supervisor

        async def close_test_pool(pool) -> None:
            order.append(
                "heartbeat_store"
                if pool is runtime._heartbeat_pool
                else "data_store"
            )

        with (
            mock.patch.object(
                collector_runtime,
                "close_pool",
                side_effect=close_test_pool,
            ),
            mock.patch.object(
                collector_runtime.asyncio,
                "sleep",
                new_callable=mock.AsyncMock,
            ),
        ):
            await runtime._shutdown_sequence()

        self.assertEqual(
            order,
            [
                "supervisor",
                "pool",
                "pubsub",
                "gcs",
                "http",
                "heartbeat_store",
                "data_store",
            ],
        )

    async def test_undrained_supervisor_preserves_resources(self) -> None:
        runtime = _runtime()
        runtime._memory_watchdog = mock.MagicMock()
        runtime._memory_watchdog.join = mock.AsyncMock()
        runtime._supervisor = mock.MagicMock()
        runtime._supervisor.shutdown = mock.AsyncMock(
            side_effect=grant_supervisor.SupervisorNotDrainedError()
        )
        runtime._work_pool = mock.AsyncMock()
        runtime._pubsub_client = mock.AsyncMock()
        runtime._gcs_client = mock.AsyncMock()

        with self.assertRaises(grant_supervisor.SupervisorNotDrainedError):
            await runtime._shutdown_sequence()

        runtime._work_pool.close.assert_not_awaited()
        runtime._pubsub_client.close.assert_not_awaited()
        runtime._gcs_client.close.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
