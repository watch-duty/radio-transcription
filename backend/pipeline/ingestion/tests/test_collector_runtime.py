from __future__ import annotations

import asyncio
import dataclasses
import datetime
import logging
import socket
import unittest
import uuid
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast
from unittest import mock

import aiohttp
import asyncpg
from google.api_core import exceptions as google_exceptions
from google.cloud.pubsub_v1.publisher import exceptions as pubsub_exceptions

from backend.pipeline.common.constants import CHUNK_DURATION_SECONDS
from backend.pipeline.ingestion import (
    collector_runtime,
    failure_policy,
    feed_grant_control,
    feed_work_scheduler,
    grant_control,
    grant_supervisor,
    source_runtime_specs,
    worker_profiles,
)
from backend.pipeline.ingestion.collector_runtime import (
    CollectorRuntime,
    _PipelineFailure,
)
from backend.pipeline.ingestion.collectors.bcfy_calls import sid_runner
from backend.pipeline.ingestion.failure_classifiers import pubsub
from backend.pipeline.ingestion.models import (
    CapturedChunk,
    CaptureResources,
    FeedFailure,
    SourceObservation,
)
from backend.pipeline.storage import ingestion_lease_store
from backend.pipeline.storage.feed_store import (
    FeedGrant,
    FeedStatus,
    FeedStatusReason,
    LeasedFeed,
    SourceType,
)
from backend.pipeline.storage.settings import AlloyDBSettings

if TYPE_CHECKING:
    import signal

_WORKER_ID = uuid.UUID("11111111-2222-3333-4444-555555555555")
_FEED_ID = uuid.UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
_RUNTIME_ACTOR_ID = "service_account:gcp:123456789012345678901"


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


def _make_leased_feed(
    feed_id: uuid.UUID,
    source_type: SourceType = SourceType.BCFY_FEEDS,
) -> LeasedFeed:
    """Build a leased feed with a stable id and source type."""
    return LeasedFeed(
        id=feed_id,
        name=f"Test Feed {feed_id}",
        source_type=source_type,
        last_processed_filename=None,
        last_bookmark_time=None,
        fencing_token=1,
        failure_count=0,
        status_reason=None,
        source_feed_id=str(feed_id),
    )


def _lease_snapshot(
    *,
    status: FeedStatus = FeedStatus.ACTIVE,
) -> ingestion_lease_store.LeaseSnapshot:
    now = datetime.datetime(2026, 7, 11, 12, 0, tzinfo=datetime.UTC)
    return ingestion_lease_store.LeaseSnapshot(
        status=status,
        last_heartbeat=now,
        failure_count=0,
        retry_after=None,
        status_reason=None,
        status_reason_detail=None,
        status_reason_updated_at=None,
        audit_revision=1,
        membership_revision=1,
        updated_at=now,
    )


_PUBLISH_SESSION_ID_ARG_INDEX = 5
_PUBLISH_START_TIMESTAMP_ARG_INDEX = 6
_PUBLISH_SOURCE_TYPE_ARG_INDEX = 8
_PUBLISH_EXTERNAL_AUDIO_ID_ARG_INDEX = 9
_LEASE_ADMISSION_EVENT_TYPE = "lease_admission_cycle"
_LEASE_ADMISSION_FIELDS = {
    "event_type",
    "worker_id",
    "worker_index",
    "hostname",
    "active_feeds",
    "max_feeds",
    "slack",
    "admission_budget",
    "primary_acquired",
    "recovery_acquired",
    "total_acquired",
    "memory_paused",
    "error",
}
_LEASE_ADMISSION_STATE_FIELDS = {
    "state",
    "status",
    "admission_state",
}


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

    def test_preserves_diagnostic_reason_until_storage(self) -> None:
        """FeedFailure preserves diagnostics for the storage boundary cap."""
        reason = "ffmpeg_exit_1 " + ("x" * 3000)
        exc = FeedFailure(
            FeedStatusReason.SYSTEM_COLLECTOR_ERROR,
            reason,
        )

        self.assertEqual(exc.reason, reason)
        self.assertEqual(str(exc), reason)

    def test_pipeline_failure_reason_preserves_diagnostic_text(self) -> None:
        """Runtime pipeline failures keep raw diagnostics until storage."""
        reason = "Authorization: Bearer secret-token " + ("x" * 3000)
        exc = _PipelineFailure(
            reason,
            status_reason=FeedStatusReason.SYSTEM_PIPELINE_ERROR,
        )

        self.assertEqual(exc.reason, reason)
        self.assertEqual(str(exc), reason)

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
        "lease_admission_cycle_budget": 20,
        "lease_poll_interval_sec": 5.0,
        "startup_stagger_max_sec": 60.0,
        "startup_jitter_max_sec": 2.0,
        "lease_poll_jitter_max_sec": 1.0,
        "worker_index": None,
        "heartbeat_interval_sec": 15.0,
        "heartbeat_stall_timeout_sec": 45.0,
        "graceful_shutdown_timeout_sec": 10.0,
        "task_cancel_budget_sec": 5.0,
        # Memory watchdog (Phase 4 / WATCHDOG-01). Defaults pin to "watchdog
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
        "segment_temp_dir": None,
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
    if "worker_profile" not in overrides:
        defaults["worker_profile"] = worker_profiles.resolve_worker_profile(
            None,
            feed_owned_cap=defaults["max_feeds_per_worker"],
            feed_claims_per_cycle=defaults["lease_admission_cycle_budget"],
        )
    m = mock.MagicMock()
    m.configure_mock(**defaults)
    return m


def _make_runtime(**settings_overrides) -> CollectorRuntime:
    """Build a runtime with a mock capture_fn and settings."""

    async def _dummy_capture(feed, shutdown, _resources):
        yield _make_captured_chunk(b"chunk")

    settings = _make_settings(**settings_overrides)
    rt = CollectorRuntime(capture_fn=_dummy_capture, settings=settings)
    # Pre-initialize per-process resources so unit tests don't need _main().
    rt._shutdown = asyncio.Event()
    rt._capture_resources = _default_resources()
    return rt


class _ControlledProcessScheduler:
    """Small process-scheduler fake for runtime lifecycle ordering."""

    def __init__(
        self,
        *,
        close_result: feed_work_scheduler.Undrained | None = None,
        start_failure: BaseException | None = None,
        order: list[str] | None = None,
    ) -> None:
        self.integrity_failure_event = asyncio.Event()
        self.close_result = close_result
        self.start_failure = start_failure
        self.order = order if order is not None else []
        self.failure: BaseException | None = None
        self.start_calls = 0
        self.close_calls = 0
        self.open_lane_calls = 0

    async def start(self) -> None:
        self.start_calls += 1
        self.order.append("scheduler_start")
        if self.start_failure is not None:
            raise self.start_failure

    def open_lane(self, _grant: object) -> object:
        self.open_lane_calls += 1
        msg = "controlled process scheduler opened an unexpected lane"
        raise AssertionError(msg)

    async def close(self) -> feed_work_scheduler.Undrained | None:
        self.close_calls += 1
        self.order.append("scheduler_close")
        return self.close_result

    def fail(self, failure: BaseException) -> None:
        self.failure = failure
        self.integrity_failure_event.set()

    def raise_if_failed(self) -> None:
        if self.failure is not None:
            message = "controlled scheduler failed"
            raise feed_work_scheduler.SchedulerIntegrityError(
                message
            ) from self.failure


def _make_feed_grant(
    feed: LeasedFeed = _FEED,
    *,
    owner_worker_id: uuid.UUID = _WORKER_ID,
) -> FeedGrant:
    """Build exact immutable authority for a leased Feed payload."""
    return FeedGrant(
        feed_id=feed["id"],
        owner_worker_id=owner_worker_id,
        fencing_token=feed["fencing_token"],
    )


def _make_run_context(
    *,
    stopped: bool = False,
    lost: bool = False,
    retry_transitions: list[bool] | None = None,
) -> grant_control.RunContext:
    """Build exact-generation runner signals for a unit test."""
    stop_requested = asyncio.Event()
    grant_lost = asyncio.Event()
    if stopped:
        stop_requested.set()
    if lost:
        grant_lost.set()
    transitions = retry_transitions if retry_transitions is not None else []
    return grant_control.RunContext(
        stop_requested=stop_requested,
        grant_lost=grant_lost,
        set_retrying=transitions.append,
    )


async def _run_feed(
    runtime: CollectorRuntime,
    feed: LeasedFeed = _FEED,
    *,
    grant: FeedGrant | None = None,
    context: grant_control.RunContext | None = None,
) -> grant_control.RunOutcome:
    """Run the typed Feed shell used by the production registration."""
    runner = collector_runtime._FeedRunner(runtime)
    return await runner.run(
        grant or _make_feed_grant(feed),
        feed,
        context or _make_run_context(),
    )


def _lease_admission_payloads(
    records: list[logging.LogRecord],
) -> list[dict[str, Any]]:
    """Return structured lease-admission payloads from captured logs."""
    return [
        cast("dict[str, Any]", record.__dict__["json_fields"])
        for record in records
        if getattr(record, "json_fields", {}).get("event_type")
        == _LEASE_ADMISSION_EVENT_TYPE
    ]


def _assert_lease_admission_schema(
    test_case: unittest.TestCase,
    payload: dict[str, Any],
) -> None:
    """Assert the raw lease-admission telemetry schema is exact."""
    test_case.assertEqual(set(payload), _LEASE_ADMISSION_FIELDS)
    test_case.assertFalse(_LEASE_ADMISSION_STATE_FIELDS & set(payload))


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

    async def test_idle_scheduler_integrity_interrupts_without_a_lane(
        self,
    ) -> None:
        """An idle selected-SID scheduler cannot lose a worker silently."""
        rt = _make_runtime(
            worker_profile=worker_profiles.SID_DORMANT_PROFILE,
            caps={},
        )
        scheduler = _ControlledProcessScheduler()
        rt._feed_work_scheduler = scheduler
        failure = RuntimeError("idle fixed worker failed")

        waiting = asyncio.create_task(rt._sleep_or_shutdown(60.0))
        scheduler.fail(failure)

        with self.assertRaises(
            feed_work_scheduler.SchedulerIntegrityError
        ) as raised:
            await asyncio.wait_for(waiting, timeout=1)
        self.assertIs(raised.exception.__cause__, failure)
        self.assertEqual(scheduler.open_lane_calls, 0)

    async def test_scheduler_integrity_wins_a_simultaneous_runtime_race(
        self,
    ) -> None:
        rt = _make_runtime(
            worker_profile=worker_profiles.SID_DORMANT_PROFILE,
            caps={},
        )
        scheduler = _ControlledProcessScheduler()
        rt._feed_work_scheduler = scheduler
        scheduler_failure = RuntimeError("scheduler worker failed first")
        supervisor_failure = grant_control.GrantControlIntegrityError(
            "supervisor also failed"
        )
        supervisor = mock.MagicMock()
        supervisor.integrity_failure_event = asyncio.Event()
        supervisor.integrity_failure = supervisor_failure
        rt._supervisor = supervisor

        waiting = asyncio.create_task(rt._sleep_or_shutdown(60.0))
        scheduler.fail(scheduler_failure)
        supervisor.integrity_failure_event.set()

        with self.assertRaises(
            feed_work_scheduler.SchedulerIntegrityError
        ) as raised:
            await asyncio.wait_for(waiting, timeout=1)
        self.assertIs(raised.exception.__cause__, scheduler_failure)


class TestStartupPacingHelpers(unittest.TestCase):
    """Tests for startup pacing and poll jitter helper functions."""

    def test_bounded_jitter_skips_zero_max(self) -> None:
        """Zero jitter max avoids calling random.uniform."""
        with mock.patch(
            "backend.pipeline.ingestion.collector_runtime.random.uniform",
        ) as mock_uniform:
            delay = collector_runtime._bounded_jitter(0.0)

        mock_uniform.assert_not_called()
        self.assertEqual(delay, 0.0)

    def test_bounded_jitter_returns_random_delay(self) -> None:
        """Positive jitter max returns bounded non-cryptographic jitter."""
        with mock.patch(
            "backend.pipeline.ingestion.collector_runtime.random.uniform",
            return_value=1.25,
        ) as mock_uniform:
            delay = collector_runtime._bounded_jitter(2.0)

        mock_uniform.assert_called_once_with(0.0, 2.0)
        self.assertEqual(delay, 1.25)

    def test_deterministic_startup_stagger_zero_max(self) -> None:
        """Zero max stagger disables deterministic startup delay."""
        delay = collector_runtime._deterministic_startup_stagger(
            _WORKER_ID,
            0.0,
        )

        self.assertEqual(delay, 0.0)

    def test_deterministic_startup_stagger_stable_and_bounded(self) -> None:
        """Worker UUID maps to a stable delay inside [0, max_sec)."""
        worker_id = uuid.UUID(int=1 << 127)

        first = collector_runtime._deterministic_startup_stagger(
            worker_id,
            60.0,
        )
        second = collector_runtime._deterministic_startup_stagger(
            worker_id,
            60.0,
        )

        self.assertEqual(first, second)
        self.assertGreaterEqual(first, 0.0)
        self.assertLess(first, 60.0)
        self.assertEqual(first, 30.0)

    def test_startup_pacing_delay_adds_random_jitter(self) -> None:
        """Startup pacing reports deterministic, random, and total delay."""
        with mock.patch(
            "backend.pipeline.ingestion.collector_runtime.random.uniform",
            return_value=1.25,
        ) as mock_uniform:
            deterministic, random_delay, total = (
                collector_runtime._startup_pacing_delay(
                    _WORKER_ID,
                    60.0,
                    2.0,
                )
            )

        mock_uniform.assert_called_once_with(0.0, 2.0)
        self.assertEqual(random_delay, 1.25)
        self.assertEqual(total, deterministic + random_delay)

    def test_startup_pacing_delay_skips_zero_jitter(self) -> None:
        """Zero jitter max avoids calling random.uniform."""
        with mock.patch(
            "backend.pipeline.ingestion.collector_runtime.random.uniform",
        ) as mock_uniform:
            _deterministic, random_delay, _total = (
                collector_runtime._startup_pacing_delay(
                    _WORKER_ID,
                    60.0,
                    0.0,
                )
            )

        mock_uniform.assert_not_called()
        self.assertEqual(random_delay, 0.0)

    def test_lease_poll_sleep_seconds_adds_jitter(self) -> None:
        """Lease poll sleep includes bounded non-cryptographic jitter."""
        with mock.patch(
            "backend.pipeline.ingestion.collector_runtime.random.uniform",
            return_value=0.75,
        ) as mock_uniform:
            sleep_seconds = collector_runtime._lease_poll_sleep_seconds(
                5.0,
                1.0,
            )

        mock_uniform.assert_called_once_with(0.0, 1.0)
        self.assertEqual(sleep_seconds, 5.75)

    def test_lease_poll_sleep_seconds_skips_zero_jitter(self) -> None:
        """Zero poll jitter max keeps the fixed poll interval."""
        with mock.patch(
            "backend.pipeline.ingestion.collector_runtime.random.uniform",
        ) as mock_uniform:
            sleep_seconds = collector_runtime._lease_poll_sleep_seconds(
                5.0,
                0.0,
            )

        mock_uniform.assert_not_called()
        self.assertEqual(sleep_seconds, 5.0)

    def test_advance_heartbeat_tick_keeps_moderate_catchup(self) -> None:
        """Small drift still allows one immediate catch-up heartbeat."""
        next_tick = collector_runtime._advance_heartbeat_tick(
            next_tick=100.0,
            interval=15.0,
            now=116.0,
        )

        self.assertEqual(next_tick, 115.0)

    def test_advance_heartbeat_tick_resets_large_drift(self) -> None:
        """Large drift resets the ticker instead of spinning catch-up loops."""
        next_tick = collector_runtime._advance_heartbeat_tick(
            next_tick=100.0,
            interval=15.0,
            now=131.0,
        )

        self.assertEqual(next_tick, 146.0)


class TestSupervisorAdmissionDelegation(unittest.IsolatedAsyncioTestCase):
    """CollectorRuntime delegates admission to its one supervisor."""

    async def test_leasing_loop_delegates_one_cycle(self) -> None:
        rt = _make_runtime(
            lease_poll_interval_sec=5.0,
            lease_poll_jitter_max_sec=0.0,
        )
        rt._supervisor = mock.AsyncMock()
        rt._memory_watchdog.is_paused = mock.MagicMock(return_value=False)

        with mock.patch.object(
            rt,
            "_sleep_or_shutdown",
            new=mock.AsyncMock(return_value=True),
        ) as sleep_or_shutdown:
            await rt._leasing_loop()

        rt._supervisor.admit_cycle.assert_awaited_once_with(_WORKER_ID)
        sleep_or_shutdown.assert_awaited_once_with(5.0)

    async def test_memory_pause_skips_admission(self) -> None:
        rt = _make_runtime(rss_watchdog_poll_interval_sec=0.25)
        rt._supervisor = mock.AsyncMock()
        rt._memory_watchdog.is_paused = mock.MagicMock(return_value=True)

        with mock.patch.object(
            rt,
            "_sleep_or_shutdown",
            new=mock.AsyncMock(return_value=True),
        ) as sleep_or_shutdown:
            await rt._leasing_loop()

        rt._supervisor.admit_cycle.assert_not_awaited()
        sleep_or_shutdown.assert_awaited_once_with(0.25)

    async def test_integrity_failure_interrupts_leasing_poll(self) -> None:
        rt = _make_runtime(
            lease_poll_interval_sec=60.0,
            lease_poll_jitter_max_sec=0.0,
        )
        failure = grant_control.GrantControlIntegrityError(
            "heartbeat correlation failed"
        )
        supervisor = mock.MagicMock()
        supervisor.admit_cycle = mock.AsyncMock()
        supervisor.integrity_failure_event = asyncio.Event()
        supervisor.integrity_failure = failure
        rt._supervisor = supervisor
        rt._memory_watchdog.is_paused = mock.MagicMock(return_value=False)

        leasing = asyncio.create_task(rt._leasing_loop())
        await asyncio.sleep(0)
        supervisor.integrity_failure_event.set()

        with self.assertRaisesRegex(
            grant_control.GrantControlIntegrityError,
            "heartbeat correlation failed",
        ):
            await asyncio.wait_for(leasing, timeout=1)

        supervisor.admit_cycle.assert_awaited_once_with(_WORKER_ID)


class TestLeasedFeedPayloadValidator(unittest.TestCase):
    """The registered Feed payload boundary validates concrete mappings."""

    def test_accepts_valid_mapping(self) -> None:
        self.assertTrue(collector_runtime._is_leased_feed_payload(_FEED))

    def test_rejects_missing_wrong_and_malformed_fields(self) -> None:
        missing = dict(_FEED)
        del missing["name"]
        wrong_type = {**_FEED, "source_type": "bcfy_feeds"}
        malformed_tags = {**_FEED, "tags": [{"name": 1}]}

        for payload in (missing, wrong_type, malformed_tags):
            with self.subTest(payload=payload):
                self.assertFalse(
                    collector_runtime._is_leased_feed_payload(payload)
                )

    def test_typed_dict_is_not_used_as_runtime_class(self) -> None:
        source = Path(collector_runtime.__file__).read_text()
        self.assertNotIn("isinstance(value, LeasedFeed)", source)


class TestLeasedFeedPayloadSupervisorBoundary(unittest.IsolatedAsyncioTestCase):
    """Malformed Feed claims fail before runner or registry admission."""

    @staticmethod
    def _build_supervisor(
        payload: object,
    ) -> tuple[
        grant_supervisor.GrantSupervisor,
        mock.AsyncMock,
        mock.AsyncMock,
    ]:
        settings = _make_settings(
            max_feeds_per_worker=1,
            lease_admission_cycle_budget=1,
        )
        allocation = worker_profiles.allocation_for_domain(
            settings.worker_profile,
            grant_control.DomainId.FEED,
        )
        assert allocation is not None
        grant = _make_feed_grant()
        claimed = False
        control = mock.AsyncMock()

        async def claim(mode, owner_worker_id, limit):
            nonlocal claimed
            if mode is grant_control.ClaimMode.RECOVERY or claimed:
                return ()
            claimed = True
            return (
                grant_control.ClaimedGrant(
                    grant=grant,
                    payload=payload,
                    lifecycle=grant_control.LifecycleEvidence(
                        durable_failing=False
                    ),
                ),
            )

        async def finalize(finalized_grant, finalized_payload, terminal):
            assert finalized_payload is payload
            return grant_control.FinalizeResult(
                finalized_grant,
                grant_control.FinalizeDisposition.APPLIED,
                None,
            )

        control.claim.side_effect = claim
        control.finalize.side_effect = finalize
        runner = mock.AsyncMock()
        runner.run.return_value = grant_control.RunCompleted()
        registration = grant_supervisor.RegisteredDomain(
            domain_id=grant_control.DomainId.FEED,
            authority_kind=worker_profiles.AuthorityKind.FEED,
            grant_type=FeedGrant,
            payload_validator=collector_runtime._is_leased_feed_payload,
            authority_of=collector_runtime._feed_authority,
            owner_of=collector_runtime._feed_owner,
            fencing_token_of=collector_runtime._feed_fencing_token,
            control=control,
            runner=runner,
            allocation=allocation,
            terminal_decision_for=lambda outcome: grant_control.NeutralRelease(
                grant_control.TerminalCause.NORMAL
            ),
        )
        supervisor = grant_supervisor.GrantSupervisor(
            settings.worker_profile,
            (registration,),
            finalize_concurrency=1,
        )
        return supervisor, runner, control

    async def test_valid_payload_reaches_runner_unchanged(self) -> None:
        supervisor, runner, control = self._build_supervisor(_FEED)

        await supervisor.admit_cycle(_WORKER_ID)
        for _ in range(3):
            await asyncio.sleep(0)

        runner.run.assert_awaited_once()
        self.assertIs(runner.run.await_args.args[1], _FEED)
        control.finalize.assert_awaited_once()

    async def test_malformed_payloads_never_reach_runner(self) -> None:
        missing = dict(_FEED)
        del missing["name"]
        payloads = (
            missing,
            {**_FEED, "source_type": "bcfy_feeds"},
            {**_FEED, "tags": [{"name": 1}]},
        )

        for payload in payloads:
            with self.subTest(payload=payload):
                supervisor, runner, control = self._build_supervisor(payload)
                await supervisor.admit_cycle(_WORKER_ID)

                self.assertTrue(supervisor.integrity_failure_event.is_set())
                self.assertEqual(
                    supervisor.snapshot()
                    .counts_by_domain[grant_control.DomainId.FEED]
                    .active,
                    0,
                )
                runner.run.assert_not_awaited()
                control.finalize.assert_not_awaited()


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

        rt = CollectorRuntime(
            capture_fn=_one_chunk,
            settings=_make_settings(),
            runtime_actor_id=_RUNTIME_ACTOR_ID,
        )
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
            await _run_feed(rt, _FEED)

        self.assertEqual(call_order, ["upload", "bookmark", "publish"])

    async def test_stop_after_bookmark_still_settles_publish(self) -> None:
        """A committed bookmark makes publication a required final step."""

        async def _one_chunk(feed, shutdown, _resources):
            yield _make_captured_chunk(b"audio")

        context = _make_run_context()
        call_order: list[str] = []

        async def _upload(*_args: object, **_kwargs: object) -> str:
            call_order.append("upload")
            return "gs://b/p"

        async def _bookmark(*_args: object, **_kwargs: object) -> bool:
            call_order.append("bookmark")
            context.stop_requested.set()
            return True

        async def _publish(*_args: object, **_kwargs: object) -> str:
            call_order.append("publish")
            return "message-1"

        rt = CollectorRuntime(
            capture_fn=_one_chunk,
            settings=_make_settings(),
            runtime_actor_id=_RUNTIME_ACTOR_ID,
        )
        rt._capture_resources = _default_resources()
        rt._store = mock.AsyncMock()
        rt._store.update_feed_progress = mock.AsyncMock(side_effect=_bookmark)

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
            outcome = await _run_feed(rt, _FEED, context=context)

        self.assertEqual(call_order, ["upload", "bookmark", "publish"])
        self.assertEqual(
            outcome,
            grant_control.RunStopped(grant_control.TerminalCause.SHUTDOWN),
        )

    async def test_hard_cancel_during_publish_is_undrained_and_not_released(
        self,
    ) -> None:
        async def _one_chunk(feed, shutdown, _resources):
            yield _make_captured_chunk(b"audio")

        publish_started = asyncio.Event()
        release_publish = asyncio.Event()

        async def _publish(*_args: object, **_kwargs: object) -> str:
            publish_started.set()
            await release_publish.wait()
            return "message-1"

        rt = CollectorRuntime(
            capture_fn=_one_chunk,
            settings=_make_settings(
                max_feeds_per_worker=1,
                lease_admission_cycle_budget=1,
            ),
        )
        rt._capture_resources = _default_resources()
        rt._store = mock.AsyncMock()
        rt._store.update_feed_progress.return_value = True
        allocation = worker_profiles.allocation_for_domain(
            rt._collector_settings.worker_profile,
            grant_control.DomainId.FEED,
        )
        assert allocation is not None
        control = mock.AsyncMock()
        claimed = False

        async def claim(mode, owner_worker_id, limit):
            nonlocal claimed
            if mode is grant_control.ClaimMode.RECOVERY or claimed:
                return ()
            claimed = True
            return (
                grant_control.ClaimedGrant(
                    grant=_make_feed_grant(),
                    payload=_FEED,
                    lifecycle=grant_control.LifecycleEvidence(
                        durable_failing=False
                    ),
                ),
            )

        control.claim.side_effect = claim
        supervisor = grant_supervisor.GrantSupervisor(
            rt._collector_settings.worker_profile,
            (
                grant_supervisor.RegisteredDomain(
                    domain_id=grant_control.DomainId.FEED,
                    authority_kind=worker_profiles.AuthorityKind.FEED,
                    grant_type=FeedGrant,
                    payload_validator=collector_runtime._is_leased_feed_payload,
                    authority_of=collector_runtime._feed_authority,
                    owner_of=collector_runtime._feed_owner,
                    fencing_token_of=collector_runtime._feed_fencing_token,
                    control=control,
                    runner=collector_runtime._FeedRunner(rt),
                    allocation=allocation,
                    terminal_decision_for=rt._feed_terminal_decision,
                ),
            ),
            finalize_concurrency=1,
        )

        with (
            _mock_upload_audio(),
            mock.patch(
                "backend.pipeline.ingestion.collector_runtime.gcp_helper.publish_audio_chunk",
                mock.AsyncMock(side_effect=_publish),
            ),
        ):
            await supervisor.admit_cycle(_WORKER_ID)
            await asyncio.wait_for(publish_started.wait(), timeout=1)

            async def stop_heartbeat() -> None:
                return None

            result = await supervisor.shutdown(
                cooperative_grace_sec=0,
                external_stop_deadline_sec=0,
                stop_heartbeat_supervision=stop_heartbeat,
            )

            self.assertEqual(
                result,
                grant_supervisor.ShutdownResult(0, 1, 1),
            )
            control.finalize.assert_not_awaited()
            release_publish.set()
            managed = next(iter(supervisor._registry.values()))
            root_task = managed.root_task
            self.assertIsNotNone(root_task)
            assert root_task is not None
            await root_task

        control.finalize.assert_not_awaited()
        self.assertTrue(managed.runner_closed.is_set())

    async def test_chunk_ingested_slo_includes_stream_interval_lag(
        self,
    ) -> None:
        """stream_interval_lag_sec, when set by the collector, reaches the SLO log."""
        now = datetime.datetime.now(datetime.UTC)
        chunk = CapturedChunk(
            audio_bytes=b"audio",
            chunk_start_time=now,
            chunk_end_time=now
            + datetime.timedelta(seconds=CHUNK_DURATION_SECONDS),
            stream_interval_lag_sec=5.5,
        )

        async def _one_chunk(feed, shutdown, _resources):
            yield chunk

        rt = CollectorRuntime(
            capture_fn=_one_chunk,
            settings=_make_settings(),
            runtime_actor_id=_RUNTIME_ACTOR_ID,
        )
        rt._shutdown = asyncio.Event()
        rt._lease_lost = asyncio.Event()
        rt._capture_resources = _default_resources()
        rt._store = mock.AsyncMock()
        rt._store.update_feed_progress = mock.AsyncMock(return_value=True)
        rt._releasing_feeds = set()

        with (
            mock.patch(
                "backend.pipeline.ingestion.collector_runtime.gcp_helper.upload_staged_audio",
                mock.AsyncMock(return_value="gs://b/p"),
            ),
            mock.patch(
                "backend.pipeline.ingestion.collector_runtime.gcp_helper.publish_audio_chunk",
                mock.AsyncMock(return_value="message-1"),
            ),
            mock.patch(
                "backend.pipeline.ingestion.collector_runtime.logger"
            ) as mock_logger,
        ):
            await _run_feed(rt, _FEED)

        ingested_calls = [
            call
            for call in mock_logger.info.call_args_list
            if call.args and call.args[0] == "Chunk ingested"
        ]
        self.assertEqual(len(ingested_calls), 1)
        payload = ingested_calls[0].kwargs["extra"]["json_fields"]
        self.assertEqual(payload["stream_interval_lag_sec"], 5.5)


class TestProcessFeedFenceViolation(unittest.IsolatedAsyncioTestCase):
    """Exact bookmark loss is localized to one supervised generation."""

    async def test_bookmark_fence_failure_returns_lost(self) -> None:
        async def _one_chunk(feed, shutdown, _resources):
            yield _make_captured_chunk(b"audio")

        rt = CollectorRuntime(capture_fn=_one_chunk, settings=_make_settings())
        rt._capture_resources = _default_resources()
        rt._store = mock.AsyncMock()
        rt._store.update_feed_progress.return_value = False
        context = _make_run_context()

        with _mock_upload_audio(), _mock_pubsub_publish() as publish_mock:
            outcome = await _run_feed(rt, _FEED, context=context)

        self.assertIsInstance(outcome, grant_control.RunLost)
        self.assertTrue(context.grant_lost.is_set())
        publish_mock.assert_not_awaited()
        rt._store.release_feed.assert_not_awaited()
        rt._store.report_feed_failure.assert_not_awaited()


class TestProcessFeedShutdown(unittest.IsolatedAsyncioTestCase):
    """Tests for _process_feed shutdown behavior."""

    async def test_pre_requested_shutdown_has_no_pipeline_side_effects(
        self,
    ) -> None:

        async def _one_chunk(feed, shutdown, _resources):
            yield _make_captured_chunk(b"audio")

        rt = CollectorRuntime(
            capture_fn=_one_chunk,
            settings=_make_settings(),
            runtime_actor_id=_RUNTIME_ACTOR_ID,
        )
        rt._capture_resources = _default_resources()
        rt._store = mock.AsyncMock()
        rt._store.update_feed_progress.return_value = True

        with (
            _mock_upload_audio() as upload,
            _mock_pubsub_publish() as publish,
        ):
            outcome = await _run_feed(
                rt,
                _FEED,
                context=_make_run_context(stopped=True),
            )

        self.assertEqual(
            outcome,
            grant_control.RunStopped(grant_control.TerminalCause.SHUTDOWN),
        )
        upload.assert_not_awaited()
        publish.assert_not_awaited()
        rt._store.update_feed_progress.assert_not_awaited()


class TestProcessFeedIteratorCleanup(unittest.IsolatedAsyncioTestCase):
    """The runner acknowledges closure only after capture cleanup."""

    async def test_early_stop_awaits_capture_aclose(self) -> None:
        cleanup_started = asyncio.Event()
        release_cleanup = asyncio.Event()
        cleanup_done = asyncio.Event()

        async def _one_observation(feed, shutdown, _resources):
            try:
                shutdown.set()
                yield SourceObservation()
            finally:
                cleanup_started.set()
                await release_cleanup.wait()
                cleanup_done.set()

        rt = CollectorRuntime(
            capture_fn=_one_observation,
            settings=_make_settings(),
        )
        rt._capture_resources = _default_resources()
        rt._store = mock.AsyncMock()
        context = _make_run_context()

        running = asyncio.create_task(_run_feed(rt, _FEED, context=context))
        await asyncio.wait_for(cleanup_started.wait(), timeout=1)

        self.assertFalse(running.done())
        self.assertFalse(cleanup_done.is_set())
        release_cleanup.set()
        outcome = await running

        self.assertTrue(cleanup_done.is_set())
        self.assertEqual(
            outcome,
            grant_control.RunStopped(grant_control.TerminalCause.SHUTDOWN),
        )


class TestProcessFeedNormalCompletion(unittest.IsolatedAsyncioTestCase):
    """Normal runner completion returns evidence without finalizing storage."""

    async def test_normal_completion_is_closed_and_store_neutral(self) -> None:
        async def _one_chunk(feed, shutdown, _resources):
            yield _make_captured_chunk(b"audio")

        rt = CollectorRuntime(
            capture_fn=_one_chunk,
            settings=_make_settings(),
            runtime_actor_id=_RUNTIME_ACTOR_ID,
        )
        rt._capture_resources = _default_resources()
        rt._store = mock.AsyncMock()
        rt._store.update_feed_progress.return_value = True

        with _mock_upload_audio(), _mock_pubsub_publish():
            outcome = await _run_feed(rt, _FEED)

        self.assertIsInstance(outcome, grant_control.RunCompleted)
        rt._store.release_feed.assert_not_awaited()
        rt._store.report_feed_failure.assert_not_awaited()
        rt._store.release_non_budgeted_failure.assert_not_awaited()


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
            runtime_actor_id=_RUNTIME_ACTOR_ID,
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
            await _run_feed(rt, feed)

        rt._store.record_source_observation.assert_not_called()
        mock_upload.assert_not_called()
        mock_publish.assert_not_called()
        rt._store.release_feed.assert_not_awaited()

    async def test_clean_observation_with_resume_position_records_cursor(
        self,
    ) -> None:
        """Clean leased rows still persist observation resume positions."""
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

        feed = cast("LeasedFeed", dict(_FEED))
        rt = CollectorRuntime(
            capture_fn=_one_observation,
            settings=_make_settings(),
            runtime_actor_id=_RUNTIME_ACTOR_ID,
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

        with (
            _mock_upload_audio(),
            _mock_pubsub_publish(),
        ):
            await _run_feed(rt, feed)

        rt._store.record_source_observation.assert_awaited_once_with(
            _FEED_ID,
            _WORKER_ID,
            1,
            resume_position,
            actor_id=_RUNTIME_ACTOR_ID,
        )
        self.assertEqual(feed["failure_count"], 0)
        self.assertIsNone(feed["status_reason"])
        self.assertEqual(feed["last_bookmark_time"], resume_position)
        rt._store.release_feed.assert_not_awaited()

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
            runtime_actor_id=_RUNTIME_ACTOR_ID,
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

        with (
            _mock_upload_audio(),
            _mock_pubsub_publish(),
        ):
            await _run_feed(rt, feed)

        rt._store.record_source_observation.assert_awaited_once_with(
            _FEED_ID,
            _WORKER_ID,
            1,
            resume_position,
            actor_id=_RUNTIME_ACTOR_ID,
        )
        self.assertEqual(feed["failure_count"], 0)
        self.assertIsNone(feed["status_reason"])
        self.assertEqual(feed["last_bookmark_time"], resume_position)
        rt._store.release_feed.assert_not_awaited()

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
            await _run_feed(rt, feed)

        self.assertEqual(feed["failure_count"], 0)
        self.assertIsNone(feed["status_reason"])
        self.assertEqual(feed["last_bookmark_time"], existing_bookmark)
        rt._store.release_feed.assert_not_awaited()

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

        await _run_feed(rt, feed)

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
        run_context = _make_run_context()

        with mock.patch(
            "backend.pipeline.ingestion.collector_runtime.os._exit",
        ) as mock_exit:
            outcome = await _run_feed(
                rt,
                feed,
                context=run_context,
            )

        self.assertIsInstance(outcome, grant_control.RunLost)
        self.assertTrue(run_context.grant_lost.is_set())
        mock_exit.assert_not_called()


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
            await _run_feed(rt, _FEED)

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
            await _run_feed(rt, _FEED)

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
            await _run_feed(rt, _FEED)  # _FEED is BCFY_FEEDS

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
            await _run_feed(rt, segmented_feed)

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
            outcome = await _run_feed(rt, segmented_feed)

        self.assertIsInstance(outcome, grant_control.RunFailed)
        assert isinstance(outcome, grant_control.RunFailed)
        self.assertIs(
            outcome.status_reason,
            FeedStatusReason.SYSTEM_RUNTIME_CONFIGURATION_INVALID,
        )
        self.assertIn(
            "Segmented Pub/Sub topic path not configured",
            outcome.reason or "",
        )


class TestHeartbeatCycle(unittest.IsolatedAsyncioTestCase):
    """The runtime heartbeat thread dispatches only the supervisor cycle."""

    async def test_dispatch_updates_health_stamp(self) -> None:
        rt = _make_runtime()
        rt._supervisor = mock.AsyncMock()

        async def heartbeat_cycle(on_dispatch) -> None:
            on_dispatch()

        rt._supervisor.heartbeat_cycle.side_effect = heartbeat_cycle

        self.assertIsNone(rt._health_state.last_heartbeat_tick)
        await rt._heartbeat_cycle()

        self.assertIsNotNone(rt._health_state.last_heartbeat_tick)
        rt._supervisor.heartbeat_cycle.assert_awaited_once()

    async def test_dispatch_stamp_precedes_control_error(self) -> None:
        rt = _make_runtime()
        rt._supervisor = mock.AsyncMock()

        async def heartbeat_cycle(on_dispatch) -> None:
            on_dispatch()
            msg = "database unavailable"
            raise RuntimeError(msg)

        rt._supervisor.heartbeat_cycle.side_effect = heartbeat_cycle

        with self.assertRaisesRegex(RuntimeError, "database unavailable"):
            await rt._heartbeat_cycle()

        self.assertIsNotNone(rt._health_state.last_heartbeat_tick)


class TestMainPoolCreation(unittest.IsolatedAsyncioTestCase):
    """Tests for pool creation in _main."""

    @mock.patch(
        "backend.pipeline.ingestion.collector_runtime.FeedStore",
    )
    @mock.patch(
        "backend.pipeline.ingestion.collector_runtime.create_pool_with_retry",
        new_callable=mock.AsyncMock,
    )
    async def test_shutdown_time_integrity_failure_exits_after_cleanup(
        self,
        mock_create_pool_with_retry: mock.AsyncMock,
        _mock_feed_store: mock.MagicMock,
    ) -> None:
        rt = _make_runtime(startup_stagger_max_sec=0.0)
        failure = grant_control.GrantControlIntegrityError(
            "finalization outcome unavailable"
        )
        order: list[str] = []

        async def leasing_loop() -> None:
            order.append("leasing")
            rt._supervisor._surface_integrity_failure(failure)

        async def shutdown_sequence() -> None:
            order.append("shutdown")

        mock_create_pool_with_retry.side_effect = (
            mock.AsyncMock(),
            mock.AsyncMock(),
        )
        loop = asyncio.get_running_loop()
        with (
            mock.patch.object(loop, "add_signal_handler"),
            mock.patch(
                "backend.pipeline.ingestion.collector_runtime.aiohttp.TCPConnector"
            ),
            mock.patch(
                "backend.pipeline.ingestion.collector_runtime.aiohttp.ClientSession"
            ),
            mock.patch.object(rt, "_leasing_loop", side_effect=leasing_loop),
            mock.patch.object(
                rt,
                "_shutdown_sequence",
                side_effect=shutdown_sequence,
            ),
            mock.patch("threading.Thread"),
            mock.patch.object(rt._memory_watchdog, "start"),
            mock.patch(
                "backend.pipeline.ingestion.collector_runtime.health_server.start",
                new_callable=mock.AsyncMock,
            ),
            mock.patch(
                "backend.pipeline.ingestion.collector_runtime.quarantine_telemetry.configure"
            ),
        ):
            with self.assertRaisesRegex(
                grant_control.GrantControlIntegrityError,
                "finalization outcome unavailable",
            ):
                await rt._main()

        self.assertEqual(order, ["leasing", "shutdown"])

    @mock.patch(
        "backend.pipeline.ingestion.collector_runtime.FeedStore",
    )
    @mock.patch(
        "backend.pipeline.ingestion.collector_runtime.create_pool_with_retry",
        new_callable=mock.AsyncMock,
    )
    async def test_startup_pacing_runs_before_pool_creation(
        self,
        mock_create_pool_with_retry: mock.AsyncMock,
        _mock_feed_store: mock.MagicMock,
    ) -> None:
        """Startup pacing occurs after signal setup and before DB pools."""
        with mock.patch.object(
            socket,
            "gethostname",
            return_value="worker-host",
        ):
            rt = _make_runtime(
                startup_stagger_max_sec=0.0,
                lease_admission_cycle_budget=20,
                max_feeds_per_worker=250,
                worker_index=1,
            )
        call_order: list[str] = []
        loop = asyncio.get_running_loop()

        async def _sleep_or_shutdown(seconds: float) -> bool:
            self.assertIsNotNone(rt._shutdown)
            self.assertEqual(seconds, 0.5)
            call_order.append("startup_sleep")
            return False

        async def _create_pool(_settings: AlloyDBSettings) -> mock.Mock:
            call_order.append("pool")
            return mock.Mock()

        def _add_signal_handler(
            _sig: signal.Signals,
            _callback: Any,
            *_args: Any,
        ) -> None:
            call_order.append("signal")

        with (
            mock.patch.object(
                loop,
                "add_signal_handler",
                side_effect=_add_signal_handler,
            ),
            mock.patch(
                "backend.pipeline.ingestion.collector_runtime.random.uniform",
                return_value=0.5,
            ),
            mock.patch(
                "backend.pipeline.ingestion.collector_runtime.aiohttp.TCPConnector"
            ),
            mock.patch(
                "backend.pipeline.ingestion.collector_runtime.aiohttp.ClientSession"
            ),
            mock.patch.object(
                rt,
                "_sleep_or_shutdown",
                side_effect=_sleep_or_shutdown,
            ),
            mock.patch.object(rt, "_leasing_loop", new_callable=mock.AsyncMock),
            mock.patch.object(
                rt, "_shutdown_sequence", new_callable=mock.AsyncMock
            ),
            mock.patch("threading.Thread"),
            mock.patch.object(rt._memory_watchdog, "start"),
            mock.patch(
                "backend.pipeline.ingestion.collector_runtime.health_server.start",
                new_callable=mock.AsyncMock,
            ),
            mock.patch(
                "backend.pipeline.ingestion.collector_runtime.quarantine_telemetry.configure"
            ),
            mock.patch(
                "backend.pipeline.ingestion.collector_runtime.logger",
            ) as mock_logger,
        ):
            mock_create_pool_with_retry.side_effect = _create_pool

            await rt._main()

        self.assertEqual(call_order[:3], ["signal", "signal", "startup_sleep"])
        self.assertIn("pool", call_order)
        startup_payload = mock_logger.info.call_args_list[0].kwargs["extra"][
            "json_fields"
        ]
        self.assertEqual(startup_payload["event_type"], "startup_pacing")
        self.assertEqual(startup_payload["worker_id"], str(_WORKER_ID))
        self.assertEqual(startup_payload["worker_index"], 1)
        self.assertEqual(startup_payload["hostname"], "worker-host")
        self.assertEqual(startup_payload["deterministic_delay_sec"], 0.0)
        self.assertEqual(startup_payload["random_delay_sec"], 0.5)
        self.assertEqual(startup_payload["total_delay_sec"], 0.5)
        self.assertEqual(startup_payload["startup_stagger_max_sec"], 0.0)
        self.assertEqual(startup_payload["startup_jitter_max_sec"], 2.0)
        self.assertEqual(startup_payload["lease_admission_cycle_budget"], 20)
        self.assertEqual(startup_payload["max_feeds_per_worker"], 250)
        self.assertEqual(startup_payload["profile"], "legacy")
        self.assertEqual(
            startup_payload["profile_digest"],
            worker_profiles.profile_digest(
                rt._collector_settings.worker_profile
            ),
        )
        self.assertEqual(startup_payload["selected_domains"], ["feed"])
        self.assertIsInstance(startup_payload["process_id"], int)

    @mock.patch(
        "backend.pipeline.ingestion.collector_runtime.FeedStore",
    )
    @mock.patch(
        "backend.pipeline.ingestion.collector_runtime.create_pool_with_retry",
        new_callable=mock.AsyncMock,
    )
    async def test_startup_pacing_shutdown_opens_no_pools(
        self,
        mock_create_pool_with_retry: mock.AsyncMock,
        mock_feed_store: mock.MagicMock,
    ) -> None:
        """Shutdown during startup pacing returns before pool creation."""
        rt = _make_runtime(startup_stagger_max_sec=0.0)

        with (
            mock.patch.object(
                rt,
                "_sleep_or_shutdown",
                new_callable=mock.AsyncMock,
                return_value=True,
            ) as mock_sleep,
            mock.patch.object(
                rt, "_shutdown_sequence", new_callable=mock.AsyncMock
            ) as mock_shutdown_sequence,
            mock.patch(
                "backend.pipeline.ingestion.collector_runtime.random.uniform",
                return_value=0.25,
            ),
            mock.patch(
                "backend.pipeline.ingestion.collector_runtime.logger",
            ) as mock_logger,
        ):
            await rt._main()

        mock_sleep.assert_awaited_once_with(0.25)
        mock_create_pool_with_retry.assert_not_awaited()
        mock_feed_store.assert_not_called()
        mock_shutdown_sequence.assert_not_awaited()
        event_types = [
            call.kwargs["extra"]["json_fields"]["event_type"]
            for call in mock_logger.info.call_args_list
            if "extra" in call.kwargs
        ]
        self.assertIn("startup_pacing_interrupted", event_types)

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

    @mock.patch(
        "backend.pipeline.ingestion.collector_runtime.create_pool_with_retry",
        new_callable=mock.AsyncMock,
    )
    async def test_partial_scheduler_start_failure_closes_before_clients(
        self,
        mock_create_pool_with_retry: mock.AsyncMock,
    ) -> None:
        """A stored partial start is closed before shared resources."""
        order: list[str] = []
        failure = RuntimeError("scheduler start failed")
        scheduler = _ControlledProcessScheduler(
            start_failure=failure,
            order=order,
        )
        data_pool = mock.AsyncMock()
        heartbeat_pool = mock.AsyncMock()
        mock_create_pool_with_retry.side_effect = (
            data_pool,
            heartbeat_pool,
        )
        http_session = mock.AsyncMock()

        def scheduler_factory():
            order.append("scheduler_factory")
            self.assertIs(rt._data_pool, data_pool)
            self.assertIs(rt._heartbeat_pool, heartbeat_pool)
            self.assertIs(rt._http_session, http_session)
            self.assertIs(
                rt._capture_resources.http_session,
                http_session,
            )
            self.assertIsNotNone(rt._gcs_client)
            self.assertIsNotNone(rt._pubsub_client)
            return scheduler

        rt = CollectorRuntime(
            capture_fn=mock.AsyncMock(),
            settings=_make_settings(
                worker_profile=worker_profiles.SID_DORMANT_PROFILE,
                caps={},
                startup_stagger_max_sec=0.0,
                startup_jitter_max_sec=0.0,
            ),
            scheduler_factory=scheduler_factory,
        )
        rt._pubsub_client = mock.AsyncMock()
        rt._pubsub_client.close.side_effect = lambda: order.append(
            "pubsub_close"
        )
        rt._gcs_client = mock.AsyncMock()
        rt._gcs_client.close.side_effect = lambda: order.append("gcs_close")
        rt._memory_watchdog.join = mock.AsyncMock()
        heartbeat_pool.close.side_effect = lambda: order.append(
            "heartbeat_pool_close"
        )
        http_session.close.side_effect = lambda: order.append("http_close")
        loop = asyncio.get_running_loop()

        with (
            self.assertRaisesRegex(RuntimeError, "scheduler start failed"),
            mock.patch.object(loop, "add_signal_handler"),
            mock.patch(
                "backend.pipeline.ingestion.collector_runtime.aiohttp.TCPConnector"
            ),
            mock.patch(
                "backend.pipeline.ingestion.collector_runtime.aiohttp.ClientSession",
                return_value=http_session,
            ),
            mock.patch.object(rt._memory_watchdog, "start"),
            mock.patch.object(rt, "_compose_supervisor") as compose,
            mock.patch(
                "backend.pipeline.ingestion.collector_runtime.quarantine_telemetry.configure"
            ),
            mock.patch(
                "backend.pipeline.ingestion.collector_runtime.close_pool",
                new=mock.AsyncMock(
                    side_effect=lambda _pool: order.append("data_pool_close")
                ),
            ),
            mock.patch(
                "backend.pipeline.ingestion.collector_runtime.asyncio.sleep",
                new=mock.AsyncMock(),
            ),
        ):
            await rt._main()

        compose.assert_not_called()
        self.assertIs(rt._feed_work_scheduler, scheduler)
        self.assertEqual(scheduler.start_calls, 1)
        self.assertEqual(scheduler.close_calls, 1)
        self.assertLess(
            order.index("scheduler_factory"),
            order.index("scheduler_start"),
        )
        self.assertLess(
            order.index("scheduler_close"),
            order.index("pubsub_close"),
        )
        self.assertLess(order.index("pubsub_close"), order.index("gcs_close"))
        self.assertLess(order.index("gcs_close"), order.index("http_close"))
        self.assertLess(
            order.index("http_close"),
            order.index("heartbeat_pool_close"),
        )
        self.assertLess(
            order.index("heartbeat_pool_close"),
            order.index("data_pool_close"),
        )


class TestSelectedDomainComposition(unittest.IsolatedAsyncioTestCase):
    """One runtime composes only profile-selected typed domains."""

    @staticmethod
    def _runtime(
        profile: worker_profiles.WorkerProfile,
        *,
        sid_store_factory=None,
        sid_runner_factory=None,
        scheduler_factory=None,
    ) -> CollectorRuntime:
        rt = CollectorRuntime(
            capture_fn=mock.AsyncMock(),
            settings=_make_settings(
                worker_profile=profile,
                caps=(
                    source_runtime_specs.default_caps()
                    if worker_profiles.allocation_for_domain(
                        profile,
                        grant_control.DomainId.FEED,
                    )
                    is not None
                    else {}
                ),
            ),
            sid_store_factory=sid_store_factory,
            sid_runner_factory=sid_runner_factory,
            scheduler_factory=scheduler_factory,
        )
        rt._shutdown = asyncio.Event()
        rt._data_pool = mock.AsyncMock()
        rt._heartbeat_pool = mock.AsyncMock()
        rt._capture_resources = _default_resources()
        return rt

    async def test_feed_only_constructs_no_sid_dependencies(self) -> None:
        sid_store_factory = mock.Mock()
        sid_runner_factory = mock.Mock()
        scheduler_factory = mock.Mock()
        rt = self._runtime(
            worker_profiles.LEGACY_PROFILE,
            sid_store_factory=sid_store_factory,
            sid_runner_factory=sid_runner_factory,
            scheduler_factory=scheduler_factory,
        )

        await rt._start_feed_work_scheduler()
        rt._compose_supervisor()

        sid_store_factory.assert_not_called()
        sid_runner_factory.assert_not_called()
        scheduler_factory.assert_not_called()
        self.assertIsNone(rt._feed_work_scheduler)
        self.assertEqual(
            set(rt._supervisor.snapshot().counts_by_domain),
            {grant_control.DomainId.FEED},
        )
        self.assertIn(SourceType.BCFY_CALLS, rt._store._claim_types)

    async def test_sid_only_dormant_constructs_without_claims(self) -> None:
        data_store = mock.AsyncMock(
            spec=ingestion_lease_store.IngestionLeaseStore
        )
        heartbeat_store = mock.AsyncMock(
            spec=ingestion_lease_store.IngestionLeaseStore
        )
        scheduler = _ControlledProcessScheduler()
        scheduler_factory = mock.Mock(return_value=scheduler)
        rt = self._runtime(
            worker_profiles.SID_DORMANT_PROFILE,
            scheduler_factory=scheduler_factory,
        )
        runner = mock.AsyncMock()

        with (
            mock.patch.object(
                collector_runtime.ingestion_lease_store,
                "IngestionLeaseStore",
                side_effect=(data_store, heartbeat_store),
            ) as store_factory,
            mock.patch.object(
                collector_runtime.bcfy_calls_sid_runner,
                "BcfyCallsSidRunner",
                return_value=runner,
            ) as runner_constructor,
        ):
            await rt._start_feed_work_scheduler()
            rt._compose_supervisor()
        await rt._supervisor.admit_cycle(_WORKER_ID)

        scheduler_factory.assert_called_once_with()
        self.assertEqual(scheduler.start_calls, 1)
        self.assertIs(rt._feed_work_scheduler, scheduler)
        runner_constructor.assert_called_once_with(scheduler)
        self.assertEqual(
            store_factory.call_args_list,
            [mock.call(rt._data_pool), mock.call(rt._heartbeat_pool)],
        )
        data_store.claim_unclaimed.assert_not_awaited()
        data_store.claim_recoverable.assert_not_awaited()
        self.assertEqual(scheduler.open_lane_calls, 0)
        self.assertEqual(
            set(rt._supervisor.snapshot().counts_by_domain),
            {grant_control.DomainId.SID},
        )

    async def test_mixed_dormant_uses_one_supervisor_and_resource_set(
        self,
    ) -> None:
        store_factory = mock.Mock(
            side_effect=(mock.AsyncMock(), mock.AsyncMock())
        )
        scheduler = _ControlledProcessScheduler()
        scheduler_factory = mock.Mock(return_value=scheduler)
        runner_factory = mock.Mock(return_value=mock.AsyncMock())
        rt = self._runtime(
            worker_profiles.MIXED_DORMANT_PROFILE,
            sid_store_factory=store_factory,
            sid_runner_factory=runner_factory,
            scheduler_factory=scheduler_factory,
        )

        await rt._start_feed_work_scheduler()
        rt._compose_supervisor()

        scheduler_factory.assert_called_once_with()
        self.assertEqual(scheduler.start_calls, 1)
        runner_factory.assert_called_once()
        resources = runner_factory.call_args.args[0]
        self.assertIs(resources.feed_work_scheduler, scheduler)
        self.assertEqual(
            set(rt._supervisor.snapshot().counts_by_domain),
            {grant_control.DomainId.FEED, grant_control.DomainId.SID},
        )
        self.assertIn(SourceType.BCFY_CALLS, rt._store._claim_types)
        self.assertIs(
            rt._capture_resources.http_session,
            rt._capture_resources.http_session,
        )

    async def test_claims_enabled_sid_requires_both_factories(self) -> None:
        allocation = dataclasses.replace(
            worker_profiles.SID_DORMANT_PROFILE.allocations[0],
            claims_enabled=True,
        )
        profile = dataclasses.replace(
            worker_profiles.SID_DORMANT_PROFILE,
            name="sid-controlled-test",
            allocations=(allocation,),
        )
        scheduler = _ControlledProcessScheduler()
        rt = self._runtime(
            profile,
            scheduler_factory=mock.Mock(return_value=scheduler),
        )
        await rt._start_feed_work_scheduler()

        with self.assertRaisesRegex(
            ValueError,
            "requires controlled store and runner factories",
        ):
            rt._compose_supervisor()

    async def test_invalid_scheduler_is_rejected_before_sid_stores(
        self,
    ) -> None:
        store_factory = mock.Mock()
        scheduler_factory = mock.Mock(return_value=object())
        rt = self._runtime(
            worker_profiles.SID_DORMANT_PROFILE,
            sid_store_factory=store_factory,
            scheduler_factory=scheduler_factory,
        )

        with self.assertRaisesRegex(
            TypeError,
            "scheduler factory returned an invalid scheduler",
        ):
            await rt._start_feed_work_scheduler()

        scheduler_factory.assert_called_once_with()
        store_factory.assert_not_called()
        self.assertIsNone(rt._feed_work_scheduler)

    async def test_default_scheduler_adapters_are_explicitly_unconnected(
        self,
    ) -> None:
        constructed = object()
        with mock.patch.object(
            collector_runtime.feed_work_scheduler,
            "FeedWorkScheduler",
            return_value=constructed,
        ) as scheduler_constructor:
            result = collector_runtime._create_unconnected_feed_work_scheduler()

        self.assertIs(result, constructed)
        scheduler_constructor.assert_called_once()
        executor = scheduler_constructor.call_args.args[0]
        committer = scheduler_constructor.call_args.kwargs["boundary_committer"]
        self.assertIsInstance(
            executor, collector_runtime._UnconnectedCallExecutor
        )
        self.assertIsInstance(
            committer,
            collector_runtime._UnconnectedBoundaryCommitter,
        )
        grant = ingestion_lease_store.LeaseGrant(
            SourceType.BCFY_CALLS,
            "123",
            _WORKER_ID,
            1,
        )

        with self.assertRaisesRegex(
            feed_work_scheduler.SchedulerIntegrityError,
            "call executor is unconnected",
        ):
            await executor.execute(object())
        with self.assertRaisesRegex(
            feed_work_scheduler.SchedulerIntegrityError,
            "boundary committer is unconnected",
        ):
            await committer.commit(grant, (), final_logical=True)

    async def test_quarantine_observer_adds_static_runtime_context(
        self,
    ) -> None:
        rt = self._runtime(worker_profiles.LEGACY_PROFILE)
        decision = grant_control.BudgetedFailureDecision(
            failure_threshold=3,
            backoff_base_sec=15,
            backoff_max_sec=600,
            status_reason=FeedStatusReason.SYSTEM_CONFIGURATION_INVALID,
            actor_id=_RUNTIME_ACTOR_ID,
            reason="invalid configuration",
        )

        with mock.patch(
            "backend.pipeline.ingestion.collector_runtime.quarantine_telemetry.emit_quarantine_event",
            new=mock.AsyncMock(),
        ) as emit:
            await rt._emit_feed_quarantine(
                _make_feed_grant(),
                _FEED,
                decision,
            )

        emit.assert_awaited_once_with(
            feed_id=str(_FEED_ID),
            feed_name=_FEED["name"],
            source_type=SourceType.BCFY_FEEDS.value,
            reason="invalid configuration",
            status_reason=(FeedStatusReason.SYSTEM_CONFIGURATION_INVALID.value),
            profile="legacy",
            profile_digest=worker_profiles.profile_digest(
                worker_profiles.LEGACY_PROFILE
            ),
            domain_id="feed",
            authority_kind="feed",
        )

    async def test_controlled_sid_full_runtime_lifecycle(  # noqa: PLR0915
        self,
    ) -> None:
        allocation = dataclasses.replace(
            worker_profiles.SID_DORMANT_PROFILE.allocations[0],
            claims_enabled=True,
        )
        profile = dataclasses.replace(
            worker_profiles.SID_DORMANT_PROFILE,
            name="sid-controlled-test",
            allocations=(allocation,),
        )
        grant = ingestion_lease_store.LeaseGrant(
            SourceType.BCFY_CALLS,
            "123",
            _WORKER_ID,
            1,
        )
        active_snapshot = _lease_snapshot()
        data_store = mock.AsyncMock(
            spec=ingestion_lease_store.IngestionLeaseStore
        )
        heartbeat_store = mock.AsyncMock(
            spec=ingestion_lease_store.IngestionLeaseStore
        )
        data_store.claim_unclaimed.return_value = (
            ingestion_lease_store.LeaseClaim(grant, active_snapshot),
        )
        data_store.claim_recoverable.return_value = ()
        heartbeat_store.renew_heartbeats.return_value = (
            ingestion_lease_store.LeaseHeartbeatResult(
                grant,
                ingestion_lease_store.LeaseOperationDisposition.APPLIED,
                active_snapshot,
            ),
        )
        order: list[str] = []
        lane_opened = asyncio.Event()
        lane_closed = asyncio.Event()
        received_resources: list[object] = []
        store_factory = mock.Mock(side_effect=(data_store, heartbeat_store))

        def runner_factory(resources):
            received_resources.append(resources)
            return sid_runner.BcfyCallsSidRunner(resources.feed_work_scheduler)

        rt = self._runtime(
            profile,
            sid_store_factory=store_factory,
            sid_runner_factory=runner_factory,
        )
        await rt._start_feed_work_scheduler()
        scheduler = rt._feed_work_scheduler
        self.assertIsInstance(
            scheduler,
            feed_work_scheduler.FeedWorkScheduler,
        )
        assert scheduler is not None
        original_open_lane = scheduler.open_lane
        original_scheduler_close = scheduler.close

        def recording_open_lane(run_grant):
            lane = original_open_lane(run_grant)
            original_lane_close = lane.close

            async def recording_lane_close(reason):
                result = await original_lane_close(reason)
                order.append("lane_closed")
                lane_closed.set()
                return result

            lane.close = recording_lane_close
            lane_opened.set()
            return lane

        async def recording_scheduler_close():
            order.append("scheduler_close")
            return await original_scheduler_close()

        scheduler.open_lane = recording_open_lane
        scheduler.close = recording_scheduler_close
        rt._compose_supervisor()

        await rt._supervisor.admit_cycle(_WORKER_ID)
        await asyncio.wait_for(lane_opened.wait(), timeout=1)
        await rt._heartbeat_cycle()

        scheduler_snapshot = await scheduler._snapshot()
        self.assertEqual(scheduler_snapshot.lane_count, 1)
        snapshot = rt._health_state.snapshot_provider()
        self.assertEqual(
            snapshot.counts_by_domain[grant_control.DomainId.SID].active,
            1,
        )
        heartbeat_store.renew_heartbeats.assert_awaited_once_with((grant,))
        self.assertEqual(len(received_resources), 1)
        resources = received_resources[0]
        self.assertIs(resources.data_store, data_store)
        self.assertIs(resources.heartbeat_store, heartbeat_store)
        self.assertIs(resources.capture_resources, rt._capture_resources)
        self.assertIs(resources.gcs_client, rt._gcs_client)
        self.assertIs(resources.pubsub_client, rt._pubsub_client)
        self.assertIs(resources.health_state, rt._health_state)
        self.assertIs(resources.shutdown, rt._shutdown)
        self.assertIs(resources.feed_work_scheduler, scheduler)

        async def release(*_args, **_kwargs):
            self.assertTrue(lane_closed.is_set())
            self.assertTrue(rt._thread_stop.is_set())
            lane_snapshot = await scheduler._lanes[grant]._snapshot()
            self.assertTrue(lane_snapshot.closed)
            managed = next(iter(rt._supervisor._registry.values()))
            self.assertTrue(managed.runner_closed.is_set())
            self.assertIsInstance(
                managed.closed_outcome,
                grant_control.RunStopped,
            )
            assert isinstance(
                managed.closed_outcome,
                grant_control.RunStopped,
            )
            self.assertIs(
                managed.closed_outcome.cause,
                grant_control.TerminalCause.PLANNED_DRAIN,
            )
            order.append("release")
            return ingestion_lease_store.LeaseOperationResult(
                ingestion_lease_store.LeaseOperationDisposition.APPLIED,
                _lease_snapshot(status=FeedStatus.UNCLAIMED),
            )

        data_store.release.side_effect = release
        rt._heartbeat_thread = None
        rt._memory_watchdog.join = mock.AsyncMock()
        rt._pubsub_client = mock.AsyncMock()
        rt._pubsub_client.close.side_effect = lambda: order.append(
            "pubsub_close"
        )
        rt._gcs_client = mock.AsyncMock()
        rt._http_session = mock.AsyncMock()

        with (
            mock.patch(
                "backend.pipeline.ingestion.collector_runtime.close_pool",
                new=mock.AsyncMock(),
            ),
            mock.patch(
                "backend.pipeline.ingestion.collector_runtime.asyncio.sleep",
                new=mock.AsyncMock(),
            ),
        ):
            await rt._shutdown_sequence()

        data_store.release.assert_awaited_once_with(
            grant,
            cause=ingestion_lease_store.LeaseReleaseCause.SHUTDOWN,
        )
        self.assertLess(order.index("lane_closed"), order.index("release"))
        self.assertLess(order.index("release"), order.index("scheduler_close"))
        self.assertLess(
            order.index("scheduler_close"),
            order.index("pubsub_close"),
        )
        self.assertLess(order.index("release"), order.index("pubsub_close"))


class TestShutdownSequence(unittest.IsolatedAsyncioTestCase):
    """Runtime shutdown delegates exact grant drain before resource close."""

    @staticmethod
    def _configure_shutdown_runtime(
        result: grant_supervisor.ShutdownResult,
    ) -> CollectorRuntime:
        rt = _make_runtime()
        rt._supervisor = mock.AsyncMock()
        rt._supervisor.shutdown.return_value = result
        rt._heartbeat_thread = None
        rt._memory_watchdog.join = mock.AsyncMock()
        rt._store = mock.AsyncMock()
        rt._pubsub_client = mock.AsyncMock()
        rt._gcs_client = mock.AsyncMock()
        rt._http_session = mock.AsyncMock()
        rt._heartbeat_pool = mock.AsyncMock()
        rt._data_pool = mock.AsyncMock()
        return rt

    async def test_drained_shutdown_closes_shared_resources(self) -> None:
        order: list[str] = []
        result = grant_supervisor.ShutdownResult(
            finalized=1,
            abandoned=0,
            undrained=0,
        )
        rt = self._configure_shutdown_runtime(result)
        scheduler = _ControlledProcessScheduler(order=order)
        rt._feed_work_scheduler = scheduler

        async def shutdown(**_kwargs):
            order.append("supervisor_shutdown")
            return result

        rt._supervisor.shutdown.side_effect = shutdown
        rt._pubsub_client.close.side_effect = lambda: order.append(
            "pubsub_close"
        )

        with (
            mock.patch(
                "backend.pipeline.ingestion.collector_runtime.close_pool",
                new=mock.AsyncMock(),
            ) as close_data_pool,
            mock.patch(
                "backend.pipeline.ingestion.collector_runtime.asyncio.sleep",
                new=mock.AsyncMock(),
            ),
        ):
            await rt._shutdown_sequence()

        rt._supervisor.shutdown.assert_awaited_once()
        rt._pubsub_client.close.assert_awaited_once()
        rt._gcs_client.close.assert_awaited_once()
        rt._http_session.close.assert_awaited_once()
        rt._heartbeat_pool.close.assert_awaited_once()
        close_data_pool.assert_awaited_once_with(rt._data_pool)
        rt._store.release_feeds_batch.assert_not_called()
        self.assertEqual(scheduler.close_calls, 1)
        self.assertLess(
            order.index("supervisor_shutdown"),
            order.index("scheduler_close"),
        )
        self.assertLess(
            order.index("scheduler_close"),
            order.index("pubsub_close"),
        )

    async def test_undrained_shutdown_logs_bounded_count(
        self,
    ) -> None:
        rt = self._configure_shutdown_runtime(
            grant_supervisor.ShutdownResult(
                finalized=0,
                abandoned=800,
                undrained=800,
            )
        )
        scheduler = _ControlledProcessScheduler()
        rt._feed_work_scheduler = scheduler

        with (
            mock.patch(
                "backend.pipeline.ingestion.collector_runtime.close_pool",
                new=mock.AsyncMock(),
            ) as close_data_pool,
            self.assertLogs(
                "backend.pipeline.ingestion.collector_runtime",
                level=logging.CRITICAL,
            ) as logs,
        ):
            await rt._shutdown_sequence()

        self.assertEqual(len(logs.records), 1)
        self.assertEqual(logs.records[0].args, (800,))
        self.assertLess(len(logs.records[0].getMessage().encode("utf-8")), 1024)
        rt._pubsub_client.close.assert_not_awaited()
        rt._gcs_client.close.assert_not_awaited()
        rt._http_session.close.assert_not_awaited()
        rt._heartbeat_pool.close.assert_not_awaited()
        close_data_pool.assert_not_awaited()
        self.assertEqual(scheduler.close_calls, 0)

    async def test_scheduler_undrained_preserves_shared_resources(
        self,
    ) -> None:
        rt = self._configure_shutdown_runtime(
            grant_supervisor.ShutdownResult(
                finalized=1,
                abandoned=0,
                undrained=0,
            )
        )
        scheduler = _ControlledProcessScheduler(
            close_result=feed_work_scheduler.Undrained(
                None,
                feed_work_scheduler.LaneCloseReason.SCHEDULER_SHUTDOWN,
            )
        )
        rt._feed_work_scheduler = scheduler

        with (
            self.assertRaisesRegex(
                grant_control.GrantControlIntegrityError,
                "process scheduler did not drain",
            ),
            mock.patch(
                "backend.pipeline.ingestion.collector_runtime.close_pool",
                new=mock.AsyncMock(),
            ) as close_data_pool,
        ):
            await rt._shutdown_sequence()

        self.assertEqual(scheduler.close_calls, 1)
        rt._pubsub_client.close.assert_not_awaited()
        rt._gcs_client.close.assert_not_awaited()
        rt._http_session.close.assert_not_awaited()
        rt._heartbeat_pool.close.assert_not_awaited()
        close_data_pool.assert_not_awaited()

    async def test_live_heartbeat_thread_fails_closed_before_resource_close(
        self,
    ) -> None:
        rt = self._configure_shutdown_runtime(
            grant_supervisor.ShutdownResult(
                finalized=0,
                abandoned=0,
                undrained=0,
            )
        )
        heartbeat_thread = mock.Mock()
        heartbeat_thread.is_alive.return_value = True
        rt._heartbeat_thread = heartbeat_thread

        async def shutdown(**kwargs: object) -> None:
            stop_heartbeat = cast(
                "Any",
                kwargs["stop_heartbeat_supervision"],
            )
            await stop_heartbeat()

        rt._supervisor.shutdown.side_effect = shutdown
        with (
            self.assertRaisesRegex(
                RuntimeError,
                "heartbeat thread did not stop",
            ),
            mock.patch(
                "backend.pipeline.ingestion.collector_runtime.close_pool",
                new=mock.AsyncMock(),
            ) as close_data_pool,
        ):
            await rt._shutdown_sequence()

        heartbeat_thread.join.assert_called_once_with(timeout=5)
        rt._pubsub_client.close.assert_not_awaited()
        rt._gcs_client.close.assert_not_awaited()
        rt._http_session.close.assert_not_awaited()
        rt._heartbeat_pool.close.assert_not_awaited()
        close_data_pool.assert_not_awaited()

    async def test_health_cleanup_failure_does_not_skip_supervisor(
        self,
    ) -> None:
        rt = self._configure_shutdown_runtime(
            grant_supervisor.ShutdownResult(
                finalized=0,
                abandoned=0,
                undrained=0,
            )
        )
        rt._health_runner = mock.AsyncMock()
        rt._health_runner.cleanup.side_effect = RuntimeError("boom")

        with (
            mock.patch(
                "backend.pipeline.ingestion.collector_runtime.close_pool",
                new=mock.AsyncMock(),
            ),
            mock.patch(
                "backend.pipeline.ingestion.collector_runtime.asyncio.sleep",
                new=mock.AsyncMock(),
            ),
        ):
            await rt._shutdown_sequence()

        rt._supervisor.shutdown.assert_awaited_once()
        rt._pubsub_client.close.assert_awaited_once()


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
        limits = feed_grant_control._calculate_branch_limits(
            250, self.CAPS, held
        )
        self.assertEqual(sum(limits.values()), 250)
        self.assertTrue(all(v >= 0 for v in limits.values()))

    def test_plan_target_800_bounds_sum(self) -> None:
        # max_feeds_per_worker=800 (scaling-plan target), all held=0.
        held = dict.fromkeys(self.CAPS, 0)
        limits = feed_grant_control._calculate_branch_limits(
            800, self.CAPS, held
        )
        self.assertEqual(sum(limits.values()), 800)

    def test_slack_exceeds_cap_sum_clamps_at_caps(self) -> None:
        # total_slack=3000 > sum(caps)=2040 → each branch gets its cap,
        # leftover slack is unassigned.
        held = dict.fromkeys(self.CAPS, 0)
        limits = feed_grant_control._calculate_branch_limits(
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
        limits = feed_grant_control._calculate_branch_limits(
            250, self.CAPS, held
        )
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
        limits = feed_grant_control._calculate_branch_limits(
            300, self.CAPS, held
        )
        self.assertLessEqual(limits[SourceType.BCFY_FEEDS], 10)
        self.assertEqual(sum(limits.values()), 300)

    def test_zero_slack_returns_all_zeros(self) -> None:
        held = dict.fromkeys(self.CAPS, 0)
        limits = feed_grant_control._calculate_branch_limits(0, self.CAPS, held)
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
        limits = feed_grant_control._calculate_branch_limits(
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
        limits = feed_grant_control._calculate_branch_limits(
            250, self.CAPS, held
        )
        # BCFY_CALLS and OPENMHZ have no entry in `held` — both should
        # be treated as held=0 with full cap-sized headroom.
        self.assertEqual(sum(limits.values()), 250)
        self.assertTrue(all(v >= 0 for v in limits.values()))


class TestFeedProfileComposition(unittest.TestCase):
    """Runtime settings keep non-default Feed capacity in the profile."""

    def test_non_default_capacity_and_claim_budget(self) -> None:
        settings = _make_settings(
            max_feeds_per_worker=123,
            lease_admission_cycle_budget=7,
        )
        allocation = worker_profiles.allocation_for_domain(
            settings.worker_profile,
            grant_control.DomainId.FEED,
        )

        self.assertIsNotNone(allocation)
        assert allocation is not None
        self.assertEqual(allocation.owned_cap, 123)
        self.assertEqual(allocation.claims_per_cycle, 7)
        self.assertIn(SourceType.BCFY_CALLS, settings.caps)


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
        retry_transitions: list[bool] = []
        run_context = _make_run_context(
            retry_transitions=retry_transitions,
        )

        with (
            mock.patch(
                "backend.pipeline.ingestion.collector_runtime.gcp_helper.upload_staged_audio",
                upload_mock,
            ),
            _mock_pubsub_publish(),
        ):
            await _run_feed(rt, _FEED, context=run_context)

        self.assertEqual(upload_mock.await_count, 2)
        self.assertEqual(retry_transitions, [True, False])
        rt._store.release_feed.assert_not_awaited()

    async def test_preconfirmed_grant_loss_has_no_pipeline_side_effects(
        self,
    ) -> None:
        async def _one_chunk(feed, shutdown, _resources):
            yield _make_captured_chunk(b"audio")

        rt = CollectorRuntime(capture_fn=_one_chunk, settings=_make_settings())
        rt._capture_resources = _default_resources()
        rt._store = mock.AsyncMock()

        with (
            mock.patch(
                "backend.pipeline.ingestion.collector_runtime.gcp_helper.upload_staged_audio",
                mock.AsyncMock(return_value="gs://b/p"),
            ) as upload,
            _mock_pubsub_publish() as publish,
        ):
            outcome = await _run_feed(
                rt,
                _FEED,
                context=_make_run_context(lost=True),
            )

        self.assertIsInstance(outcome, grant_control.RunLost)
        upload.assert_not_awaited()
        publish.assert_not_awaited()
        rt._store.report_feed_failure.assert_not_awaited()
        rt._store.release_feed.assert_not_awaited()

    async def test_lease_lost_during_bookmark_backoff_aborts(self) -> None:
        """Lease loss during bookmark retry aborts without DB write."""

        async def _one_chunk(feed, shutdown, _resources):
            yield _make_captured_chunk(b"audio")

        rt = CollectorRuntime(capture_fn=_one_chunk, settings=_make_settings())
        rt._capture_resources = _default_resources()
        rt._store = mock.AsyncMock()
        rt._store.update_feed_progress.side_effect = asyncpg.InterfaceError(
            "connection lost"
        )
        stop_requested = asyncio.Event()
        grant_lost = asyncio.Event()

        def retry_state_changed(retrying: bool) -> None:  # noqa: FBT001
            if retrying:
                grant_lost.set()

        context = grant_control.RunContext(
            stop_requested=stop_requested,
            grant_lost=grant_lost,
            set_retrying=retry_state_changed,
        )

        with (
            mock.patch(
                "backend.pipeline.ingestion.collector_runtime.gcp_helper.upload_staged_audio",
                mock.AsyncMock(return_value="gs://b/p"),
            ),
            _mock_pubsub_publish(),
        ):
            outcome = await _run_feed(rt, _FEED, context=context)

        self.assertIsInstance(outcome, grant_control.RunLost)
        self.assertEqual(rt._store.update_feed_progress.await_count, 1)
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
            await _run_feed(rt, _FEED)

        self.assertEqual(
            call_order,
            ["upload", "bookmark", "publish", "publish"],
        )
        rt._store.update_feed_progress.assert_awaited_once()
        rt._store.report_feed_failure.assert_not_awaited()
        rt._store.release_feed.assert_not_awaited()

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
            await _run_feed(rt, _FEED)

        self.assertEqual(
            call_order,
            ["upload", "bookmark", "publish", "publish"],
        )
        rt._store.update_feed_progress.assert_awaited_once()
        rt._store.report_feed_failure.assert_not_awaited()
        rt._store.release_feed.assert_not_awaited()

    async def test_non_retryable_pubsub_failure_is_finalized_once(self) -> None:
        """Publish-gap evidence stays closed until exact control finalization."""

        async def _one_chunk(feed, shutdown, _resources):
            yield _make_captured_chunk(b"audio")

        publish_error = google_exceptions.InvalidArgument(
            "Message failed schema validation"
        )
        expected_reason = pubsub.publish_failure_reason(publish_error)
        rt = CollectorRuntime(capture_fn=_one_chunk, settings=_make_settings())
        rt._capture_resources = _default_resources()
        rt._store = mock.AsyncMock()
        rt._store.update_feed_progress.return_value = True
        rt._store.release_non_budgeted_failure.return_value = "failing"
        retry_after = datetime.datetime(2026, 6, 15, tzinfo=datetime.UTC)

        with (
            _mock_upload_audio(),
            mock.patch(
                "backend.pipeline.ingestion.collector_runtime.gcp_helper.publish_audio_chunk",
                mock.AsyncMock(side_effect=publish_error),
            ),
        ):
            outcome = await _run_feed(rt, _FEED)

        self.assertIsInstance(outcome, grant_control.RunFailed)
        assert isinstance(outcome, grant_control.RunFailed)
        self.assertIs(
            outcome.status_reason,
            FeedStatusReason.PIPELINE_PUBLISH_AFTER_BOOKMARK_FAILED,
        )
        self.assertEqual(outcome.reason, expected_reason)
        rt._store.release_non_budgeted_failure.assert_not_awaited()

        with (
            mock.patch.object(
                CollectorRuntime,
                "_non_budgeted_retry_after",
                return_value=retry_after,
            ) as retry_factory,
            mock.patch(
                "backend.pipeline.ingestion.collector_runtime.failure_policy.classify_failure_policy",
                wraps=failure_policy.classify_failure_policy,
            ) as classify,
            self.assertLogs(
                "backend.pipeline.ingestion.collector_runtime",
                level=logging.INFO,
            ) as cm,
        ):
            decision = rt._feed_terminal_decision(outcome)

        classify.assert_called_once_with(outcome.status_reason)
        retry_factory.assert_called_once_with()
        self.assertIsInstance(
            decision,
            grant_control.NonBudgetedFailureDecision,
        )
        control = feed_grant_control.FeedGrantControl(
            rt._store,
            mock.AsyncMock(),
            rt._collector_settings.caps,
            datetime.timedelta(seconds=60),
        )
        result = await control.finalize(_make_feed_grant(), _FEED, decision)

        self.assertIs(
            result.disposition,
            grant_control.FinalizeDisposition.APPLIED,
        )
        rt._store.release_non_budgeted_failure.assert_awaited_once()
        fields = [
            cast("dict[str, Any]", record.__dict__["json_fields"])
            for record in cm.records
            if getattr(record, "json_fields", {}).get("event_type")
        ]
        self.assertEqual(
            {field["event_type"] for field in fields},
            {
                "feed_failure_policy_decision",
                "post_bookmark_publish_failure",
            },
        )


class TestFeedTerminalOwnership(unittest.IsolatedAsyncioTestCase):
    """Runner evidence and supervisor-selected storage stay separate."""

    async def _failed_outcome(
        self,
        status_reason: FeedStatusReason,
        reason: str,
    ) -> tuple[CollectorRuntime, grant_control.RunFailed]:
        async def _failing_capture(feed, shutdown, _resources):
            raise FeedFailure(status_reason, reason)
            yield _make_captured_chunk(b"audio")

        rt = CollectorRuntime(
            capture_fn=_failing_capture,
            settings=_make_settings(),
            runtime_actor_id=_RUNTIME_ACTOR_ID,
        )
        rt._capture_resources = _default_resources()
        rt._store = mock.AsyncMock()
        outcome = await _run_feed(rt, _FEED)
        self.assertIsInstance(outcome, grant_control.RunFailed)
        assert isinstance(outcome, grant_control.RunFailed)
        rt._store.report_feed_failure.assert_not_awaited()
        rt._store.release_non_budgeted_failure.assert_not_awaited()
        return rt, outcome

    async def test_budgeted_failure_is_selected_and_finalized_once(
        self,
    ) -> None:
        rt, outcome = await self._failed_outcome(
            FeedStatusReason.SYSTEM_CONFIGURATION_INVALID,
            "configuration_invalid",
        )
        rt._store.report_feed_failure.return_value = "quarantined"

        with mock.patch(
            "backend.pipeline.ingestion.collector_runtime.failure_policy.classify_failure_policy",
            wraps=failure_policy.classify_failure_policy,
        ) as classify:
            decision = rt._feed_terminal_decision(outcome)

        classify.assert_called_once_with(outcome.status_reason)
        self.assertIsInstance(decision, grant_control.BudgetedFailureDecision)
        control = feed_grant_control.FeedGrantControl(
            rt._store,
            mock.AsyncMock(),
            rt._collector_settings.caps,
            datetime.timedelta(seconds=60),
        )
        result = await control.finalize(_make_feed_grant(), _FEED, decision)

        self.assertIs(
            result.disposition,
            grant_control.FinalizeDisposition.APPLIED,
        )
        rt._store.report_feed_failure.assert_awaited_once()
        rt._store.release_non_budgeted_failure.assert_not_awaited()

    async def test_non_budgeted_failure_uses_one_retry_timestamp(self) -> None:
        rt, outcome = await self._failed_outcome(
            FeedStatusReason.SOURCE_RATE_LIMITED,
            "source_rate_limited",
        )
        retry_after = datetime.datetime(2026, 6, 15, tzinfo=datetime.UTC)
        rt._store.release_non_budgeted_failure.return_value = "failing"

        with (
            mock.patch(
                "backend.pipeline.ingestion.collector_runtime.failure_policy.classify_failure_policy",
                wraps=failure_policy.classify_failure_policy,
            ) as classify,
            mock.patch.object(
                CollectorRuntime,
                "_non_budgeted_retry_after",
                return_value=retry_after,
            ) as retry_factory,
        ):
            decision = rt._feed_terminal_decision(outcome)

        classify.assert_called_once_with(outcome.status_reason)
        retry_factory.assert_called_once_with()
        self.assertIsInstance(
            decision,
            grant_control.NonBudgetedFailureDecision,
        )
        assert isinstance(
            decision,
            grant_control.NonBudgetedFailureDecision,
        )
        self.assertEqual(decision.retry_after, retry_after)
        control = feed_grant_control.FeedGrantControl(
            rt._store,
            mock.AsyncMock(),
            rt._collector_settings.caps,
            datetime.timedelta(seconds=60),
        )
        await control.finalize(_make_feed_grant(), _FEED, decision)

        rt._store.report_feed_failure.assert_not_awaited()
        rt._store.release_non_budgeted_failure.assert_awaited_once()

    async def test_untyped_collector_error_returns_canonical_failure(
        self,
    ) -> None:
        async def _failing_capture(feed, shutdown, _resources):
            msg = "capture_failed"
            raise RuntimeError(msg)
            yield _make_captured_chunk(b"audio")

        rt = CollectorRuntime(
            capture_fn=_failing_capture, settings=_make_settings()
        )
        rt._capture_resources = _default_resources()
        rt._store = mock.AsyncMock()

        outcome = await _run_feed(rt, _FEED)

        self.assertIsInstance(outcome, grant_control.RunFailed)
        assert isinstance(outcome, grant_control.RunFailed)
        self.assertIs(
            outcome.status_reason,
            FeedStatusReason.SYSTEM_UNEXPECTED_ERROR,
        )
        self.assertIn("capture_failed", outcome.reason or "")
        rt._store.report_feed_failure.assert_not_awaited()

    async def test_pre_stopped_generation_is_neutral(self) -> None:
        rt = _make_runtime()
        rt._store = mock.AsyncMock()

        with mock.patch(
            "backend.pipeline.ingestion.collector_runtime.failure_policy.classify_failure_policy"
        ) as classify:
            outcome = await _run_feed(
                rt,
                _FEED,
                context=_make_run_context(stopped=True),
            )
            decision = rt._feed_terminal_decision(outcome)

        self.assertIsInstance(outcome, grant_control.RunStopped)
        self.assertEqual(
            decision,
            grant_control.NeutralRelease(grant_control.TerminalCause.SHUTDOWN),
        )
        classify.assert_not_called()
        rt._store.report_feed_failure.assert_not_awaited()


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
            await _run_feed(rt, _FEED)

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
            await _run_feed(rt, _FEED)

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
            await _run_feed(rt, _FEED)

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
            await _run_feed(rt, _FEED)

        mock_publish.assert_called_once()
        _, args, _kwargs = mock_publish.mock_calls[0]
        self.assertEqual(
            args[_PUBLISH_SOURCE_TYPE_ARG_INDEX], _FEED["source_type"]
        )


class TestWatchdogLifecycle(unittest.IsolatedAsyncioTestCase):
    """The sole runtime shutdown owner also joins the memory watchdog."""

    async def test_shutdown_joins_watchdog(self) -> None:
        rt = TestShutdownSequence._configure_shutdown_runtime(
            grant_supervisor.ShutdownResult(
                finalized=0,
                abandoned=0,
                undrained=0,
            )
        )

        with (
            mock.patch(
                "backend.pipeline.ingestion.collector_runtime.close_pool",
                new=mock.AsyncMock(),
            ),
            mock.patch(
                "backend.pipeline.ingestion.collector_runtime.asyncio.sleep",
                new=mock.AsyncMock(),
            ),
        ):
            await rt._shutdown_sequence()

        rt._memory_watchdog.join.assert_awaited_once_with(timeout_sec=3)


class TestProcessFeedResumePosition(unittest.IsolatedAsyncioTestCase):
    """_process_feed forwards the chunk's resume cursor to update_feed_progress.

    Contract: the feed's persisted last_bookmark_time is the chunk's
    resume_position when the collector sets it (bcfy_calls), and falls back
    to chunk_end_time when it is None (stream/push collectors). Runtime passes
    the bookmark and audit causal inputs as named storage arguments.
    """

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

        rt = CollectorRuntime(
            capture_fn=_one_chunk,
            settings=_make_settings(),
            runtime_actor_id=_RUNTIME_ACTOR_ID,
        )
        rt._shutdown = asyncio.Event()
        rt._lease_lost = asyncio.Event()
        rt._capture_resources = _default_resources()
        rt._store = mock.AsyncMock()
        rt._store.update_feed_progress.return_value = True
        rt._releasing_feeds = set()
        feed = cast(
            "LeasedFeed",
            dict(
                _FEED,
                failure_count=2,
                status_reason=FeedStatusReason.SOURCE_UNREACHABLE,
            ),
        )

        with (
            _mock_upload_audio(),
            _mock_pubsub_publish(),
        ):
            await _run_feed(rt, feed)

        rt._store.update_feed_progress.assert_awaited_once()
        kwargs = rt._store.update_feed_progress.await_args.kwargs
        self.assertEqual(kwargs["last_bookmark_time"], resume)
        self.assertEqual(kwargs["actor_id"], _RUNTIME_ACTOR_ID)

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

        rt = CollectorRuntime(
            capture_fn=_one_chunk,
            settings=_make_settings(),
            runtime_actor_id=_RUNTIME_ACTOR_ID,
        )
        rt._shutdown = asyncio.Event()
        rt._lease_lost = asyncio.Event()
        rt._capture_resources = _default_resources()
        rt._store = mock.AsyncMock()
        rt._store.update_feed_progress.return_value = True
        rt._releasing_feeds = set()

        with (
            _mock_upload_audio(),
            _mock_pubsub_publish(),
        ):
            await _run_feed(rt, _FEED)

        rt._store.update_feed_progress.assert_awaited_once()
        kwargs = rt._store.update_feed_progress.await_args.kwargs
        self.assertEqual(kwargs["last_bookmark_time"], end_time)
        self.assertEqual(kwargs["actor_id"], _RUNTIME_ACTOR_ID)


if __name__ == "__main__":
    unittest.main()
