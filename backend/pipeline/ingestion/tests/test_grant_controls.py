"""Contract tests for the closed Feed and SID grant controls."""

from __future__ import annotations

import asyncio
import dataclasses
import datetime
import inspect
import pathlib
import unittest
import uuid
from unittest import mock

from backend.pipeline.ingestion import (
    failure_policy,
    feed_grant_control,
    grant_control,
    retry,
    sid_grant_control,
    source_runtime_specs,
)
from backend.pipeline.storage import feed_store, ingestion_lease_store

_OWNER_ID = uuid.UUID("11111111-2222-3333-4444-555555555555")
_OTHER_OWNER_ID = uuid.UUID("22222222-3333-4444-5555-666666666666")
_NOW = datetime.datetime(2026, 7, 11, 12, 0, tzinfo=datetime.UTC)
_ABANDONMENT = datetime.timedelta(seconds=60)
_ACTOR_ID = "service_account:gcp:grant-control-tests"


def _leased_feed(
    feed_id: uuid.UUID,
    *,
    source_type: feed_store.SourceType = feed_store.SourceType.BCFY_CALLS,
    fencing_token: int = 7,
    failure_count: int = 0,
    status_reason: feed_store.FeedStatusReason | None = None,
) -> feed_store.LeasedFeed:
    return feed_store.LeasedFeed(
        id=feed_id,
        name=f"Feed {feed_id}",
        source_type=source_type,
        last_processed_filename=None,
        last_bookmark_time=None,
        fencing_token=fencing_token,
        failure_count=failure_count,
        status_reason=status_reason,
        source_feed_id="123-456",
        tags=None,
    )


def _feed_grant(
    feed_id: uuid.UUID | None = None,
    *,
    owner_worker_id: uuid.UUID = _OWNER_ID,
    fencing_token: int = 7,
) -> feed_store.FeedGrant:
    return feed_store.FeedGrant(
        feed_id or uuid.uuid4(),
        owner_worker_id,
        fencing_token,
    )


def _feed_snapshot(
    *,
    status: feed_store.FeedStatus = feed_store.FeedStatus.ACTIVE,
    failure_count: int = 0,
    status_reason: feed_store.FeedStatusReason | None = None,
) -> feed_store.FeedHeartbeatSnapshot:
    return feed_store.FeedHeartbeatSnapshot(
        status=status,
        last_heartbeat=_NOW,
        failure_count=failure_count,
        status_reason=status_reason,
    )


def _lease_grant(
    lease_key: str = "123",
    *,
    owner_worker_id: uuid.UUID = _OWNER_ID,
    fencing_token: int = 7,
) -> ingestion_lease_store.LeaseGrant:
    return ingestion_lease_store.LeaseGrant(
        source_type=feed_store.SourceType.BCFY_CALLS,
        lease_key=lease_key,
        owner_worker_id=owner_worker_id,
        fencing_token=fencing_token,
    )


def _lease_snapshot(
    *,
    status: feed_store.FeedStatus = feed_store.FeedStatus.ACTIVE,
    failure_count: int = 0,
    status_reason: feed_store.FeedStatusReason | None = None,
) -> ingestion_lease_store.LeaseSnapshot:
    return ingestion_lease_store.LeaseSnapshot(
        status=status,
        last_heartbeat=_NOW,
        failure_count=failure_count,
        retry_after=None,
        status_reason=status_reason,
        status_reason_detail=None,
        status_reason_updated_at=None,
        audit_revision=4,
        membership_revision=3,
        updated_at=_NOW,
    )


def _lease_claim(
    lease_key: str,
    *,
    failure_count: int = 0,
    status_reason: feed_store.FeedStatusReason | None = None,
) -> ingestion_lease_store.LeaseClaim:
    return ingestion_lease_store.LeaseClaim(
        grant=_lease_grant(lease_key),
        snapshot=_lease_snapshot(
            failure_count=failure_count,
            status_reason=status_reason,
        ),
    )


def _payload_for_grant(
    grant: feed_store.FeedGrant | ingestion_lease_store.LeaseGrant,
) -> feed_store.LeasedFeed | ingestion_lease_store.LeaseSnapshot:
    if isinstance(grant, feed_store.FeedGrant):
        return _leased_feed(
            grant.feed_id,
            fencing_token=grant.fencing_token,
        )
    return _lease_snapshot()


def _budgeted_decision() -> grant_control.BudgetedFailureDecision:
    return grant_control.BudgetedFailureDecision(
        failure_threshold=5,
        backoff_base_sec=15,
        backoff_max_sec=600,
        status_reason=feed_store.FeedStatusReason.SYSTEM_CONFIGURATION_INVALID,
        actor_id=_ACTOR_ID,
        reason="invalid configuration",
    )


def _non_budgeted_decision() -> grant_control.NonBudgetedFailureDecision:
    return grant_control.NonBudgetedFailureDecision(
        retry_after=_NOW + datetime.timedelta(minutes=8),
        status_reason=feed_store.FeedStatusReason.SOURCE_UNREACHABLE,
        actor_id=_ACTOR_ID,
        reason="provider unavailable",
    )


class TestGrantControlVocabulary(unittest.TestCase):
    """Static contract for the deliberately small generic vocabulary."""

    def test_closed_enums_have_only_the_planned_values(self) -> None:
        expected = {
            grant_control.DomainId: {"feed", "sid"},
            grant_control.ClaimMode: {"primary", "recovery"},
            grant_control.HeartbeatDisposition: {
                "retained",
                "administrative_stop",
                "lost",
                "unavailable",
            },
            grant_control.FinalizeDisposition: {
                "applied",
                "accepted_noop",
                "lost",
                "unavailable",
            },
            grant_control.TerminalCause: {
                "normal",
                "shutdown",
                "planned_drain",
                "cancellation",
            },
        }

        for enum_type, values in expected.items():
            with self.subTest(enum_type=enum_type.__name__):
                self.assertEqual({item.value for item in enum_type}, values)

    def test_lifecycle_evidence_has_only_one_boolean_projection(self) -> None:
        evidence = grant_control.LifecycleEvidence(durable_failing=True)

        self.assertEqual(
            tuple(field.name for field in dataclasses.fields(evidence)),
            ("durable_failing",),
        )
        self.assertFalse(hasattr(evidence, "__dict__"))

    def test_protocol_signatures_are_only_claim_heartbeat_finalize_run(
        self,
    ) -> None:
        control_methods = {
            name
            for name, value in vars(grant_control.GrantControl).items()
            if inspect.isfunction(value) and not name.startswith("_")
        }
        runner_methods = {
            name
            for name, value in vars(grant_control.GrantRunner).items()
            if inspect.isfunction(value) and not name.startswith("_")
        }

        self.assertEqual(control_methods, {"claim", "heartbeat", "finalize"})
        self.assertEqual(runner_methods, {"run"})
        self.assertEqual(
            tuple(
                inspect.signature(grant_control.GrantControl.claim).parameters
            ),
            ("self", "mode", "owner_worker_id", "limit"),
        )
        self.assertEqual(
            tuple(
                inspect.signature(
                    grant_control.GrantControl.finalize
                ).parameters
            ),
            ("self", "grant", "payload", "terminal"),
        )

    def test_run_context_owns_only_closed_signals_and_callback(self) -> None:
        context = grant_control.RunContext(
            stop_requested=asyncio.Event(),
            grant_lost=asyncio.Event(),
            set_retrying=mock.Mock(),
        )

        self.assertEqual(
            tuple(field.name for field in dataclasses.fields(context)),
            ("stop_requested", "grant_lost", "set_retrying"),
        )

    def test_contract_has_no_open_metadata_or_health_surface(self) -> None:
        source = pathlib.Path(grant_control.__file__).read_text()

        for forbidden in (
            "typing.Any",
            "dict[str, object]",
            "def health",
            "metadata:",
            "repository",
            "retry_with_lease_check",
        ):
            with self.subTest(forbidden=forbidden):
                self.assertNotIn(forbidden, source)


class TestFeedGrantControl(unittest.IsolatedAsyncioTestCase):
    """Feed adapter mapping, ordering, and one-shot terminal tests."""

    def setUp(self) -> None:
        self.data_store = mock.AsyncMock(spec=feed_store.FeedStore)
        self.heartbeat_store = mock.AsyncMock(spec=feed_store.FeedStore)
        self.caps = source_runtime_specs.default_caps()
        self.control = feed_grant_control.FeedGrantControl(
            self.data_store,
            self.heartbeat_store,
            self.caps,
            _ABANDONMENT,
        )

    async def test_primary_claim_preserves_water_fill_payload_and_calls(
        self,
    ) -> None:
        first = _leased_feed(
            uuid.UUID(int=2),
            source_type=feed_store.SourceType.BCFY_CALLS,
        )
        second = _leased_feed(
            uuid.UUID(int=1),
            source_type=feed_store.SourceType.OPENMHZ,
            failure_count=2,
            status_reason=feed_store.FeedStatusReason.SOURCE_UNREACHABLE,
        )
        self.data_store.count_held_by_type.return_value = dict.fromkeys(
            feed_store.SourceType,
            0,
        )
        self.data_store.acquire_feeds_batch.return_value = [first, second]

        result = await self.control.claim(
            grant_control.ClaimMode.PRIMARY,
            _OWNER_ID,
            8,
        )

        self.data_store.count_held_by_type.assert_awaited_once_with(_OWNER_ID)
        self.data_store.acquire_feeds_batch.assert_awaited_once()
        owner, limits = self.data_store.acquire_feeds_batch.await_args.args
        self.assertEqual(owner, _OWNER_ID)
        self.assertEqual(sum(limits.values()), 8)
        self.assertIn(feed_store.SourceType.BCFY_CALLS, limits)
        self.assertEqual(tuple(limits), tuple(self.caps))
        self.assertEqual(
            [item.grant.feed_id for item in result], [first["id"], second["id"]]
        )
        self.assertIs(result[0].payload, first)
        self.assertIs(result[1].payload, second)
        self.assertFalse(result[0].lifecycle.durable_failing)
        self.assertTrue(result[1].lifecycle.durable_failing)
        self.data_store.acquire_feeds_recovery.assert_not_awaited()

    async def test_recovery_refreshes_counts_and_uses_remaining_limits(
        self,
    ) -> None:
        self.data_store.count_held_by_type.return_value = {
            source_type: self.caps[source_type] if index == 0 else 0
            for index, source_type in enumerate(self.caps)
        }
        self.data_store.acquire_feeds_recovery.return_value = []

        result = await self.control.claim(
            grant_control.ClaimMode.RECOVERY,
            _OWNER_ID,
            10,
        )

        self.assertEqual(result, ())
        self.data_store.count_held_by_type.assert_awaited_once_with(_OWNER_ID)
        self.data_store.acquire_feeds_recovery.assert_awaited_once()
        owner, seconds, limits = (
            self.data_store.acquire_feeds_recovery.await_args.args
        )
        self.assertEqual(owner, _OWNER_ID)
        self.assertEqual(seconds, 60.0)
        self.assertEqual(sum(limits.values()), 10)
        self.assertEqual(next(iter(limits.values())), 0)
        self.data_store.acquire_feeds_batch.assert_not_awaited()

    async def test_zero_claim_limit_touches_no_store(self) -> None:
        result = await self.control.claim(
            grant_control.ClaimMode.PRIMARY,
            _OWNER_ID,
            0,
        )

        self.assertEqual(result, ())
        self.data_store.count_held_by_type.assert_not_awaited()
        self.data_store.acquire_feeds_batch.assert_not_awaited()

    async def test_heartbeat_maps_every_disposition_in_caller_order(
        self,
    ) -> None:
        dispositions = (
            feed_store.FeedGrantOperationDisposition.APPLIED,
            feed_store.FeedGrantOperationDisposition.ACCEPTED_NOOP,
            feed_store.FeedGrantOperationDisposition.STATUS_INELIGIBLE,
            feed_store.FeedGrantOperationDisposition.MISSING,
            feed_store.FeedGrantOperationDisposition.OWNER_MISMATCH,
            feed_store.FeedGrantOperationDisposition.FENCE_MISMATCH,
        )
        grants = tuple(_feed_grant() for _ in dispositions)
        results = []
        for index, disposition in enumerate(dispositions):
            snapshot = None
            if (
                disposition
                is not feed_store.FeedGrantOperationDisposition.MISSING
            ):
                snapshot = _feed_snapshot(
                    failure_count=1 if index == 1 else 0,
                )
            results.append(
                feed_store.FeedGrantHeartbeatResult(
                    grants[index],
                    disposition,
                    snapshot,
                )
            )
        self.heartbeat_store.renew_grant_heartbeats.return_value = tuple(
            results
        )

        translated = await self.control.heartbeat(grants)

        self.heartbeat_store.renew_grant_heartbeats.assert_awaited_once_with(
            grants
        )
        self.assertEqual([item.grant for item in translated], list(grants))
        self.assertEqual(
            [item.disposition for item in translated],
            [
                grant_control.HeartbeatDisposition.RETAINED,
                grant_control.HeartbeatDisposition.RETAINED,
                grant_control.HeartbeatDisposition.ADMINISTRATIVE_STOP,
                grant_control.HeartbeatDisposition.LOST,
                grant_control.HeartbeatDisposition.LOST,
                grant_control.HeartbeatDisposition.LOST,
            ],
        )
        first_lifecycle = translated[0].lifecycle
        second_lifecycle = translated[1].lifecycle
        self.assertIsNotNone(first_lifecycle)
        self.assertIsNotNone(second_lifecycle)
        assert first_lifecycle is not None
        assert second_lifecycle is not None
        self.assertFalse(first_lifecycle.durable_failing)
        self.assertTrue(second_lifecycle.durable_failing)
        self.assertTrue(all(item.lifecycle is None for item in translated[2:]))

    async def test_heartbeat_rejects_every_malformed_correlation(self) -> None:
        first = _feed_grant()
        second = _feed_grant()
        first_result = feed_store.FeedGrantHeartbeatResult(
            first,
            feed_store.FeedGrantOperationDisposition.ACCEPTED_NOOP,
            _feed_snapshot(),
        )
        second_result = feed_store.FeedGrantHeartbeatResult(
            second,
            feed_store.FeedGrantOperationDisposition.ACCEPTED_NOOP,
            _feed_snapshot(),
        )
        unknown_result = feed_store.FeedGrantHeartbeatResult(
            _feed_grant(),
            feed_store.FeedGrantOperationDisposition.ACCEPTED_NOOP,
            _feed_snapshot(),
        )
        cases = (
            (first_result,),
            (first_result, second_result, unknown_result),
            (first_result, first_result),
            (second_result, first_result),
            (unknown_result, second_result),
            (mock.Mock(), second_result),
        )

        for results in cases:
            with self.subTest(results=results):
                self.heartbeat_store.reset_mock()
                self.heartbeat_store.renew_grant_heartbeats.return_value = (
                    results
                )
                with self.assertRaises(
                    grant_control.GrantControlIntegrityError
                ):
                    await self.control.heartbeat((first, second))

    async def test_heartbeat_store_exception_propagates_once(self) -> None:
        grant = _feed_grant()
        self.heartbeat_store.renew_grant_heartbeats.side_effect = RuntimeError(
            "heartbeat unavailable"
        )

        with self.assertRaisesRegex(RuntimeError, "heartbeat unavailable"):
            await self.control.heartbeat((grant,))

        self.heartbeat_store.renew_grant_heartbeats.assert_awaited_once()

    async def test_every_neutral_cause_uses_exact_feed_release(self) -> None:
        grant = _feed_grant()

        for cause in grant_control.TerminalCause:
            with self.subTest(cause=cause.value):
                self.data_store.reset_mock()
                self.data_store.release_feed.return_value = True

                result = await self.control.finalize(
                    grant,
                    _payload_for_grant(grant),
                    grant_control.NeutralRelease(cause),
                )

                self.assertIs(
                    result.disposition,
                    grant_control.FinalizeDisposition.APPLIED,
                )
                self.data_store.release_feed.assert_awaited_once_with(
                    grant.feed_id,
                    grant.owner_worker_id,
                    grant.fencing_token,
                )

    async def test_every_terminal_decision_calls_one_exact_feed_method(
        self,
    ) -> None:
        grant = _feed_grant()
        self.data_store.release_feed.return_value = True
        self.data_store.report_feed_failure.return_value = "quarantined"
        self.data_store.release_non_budgeted_failure.return_value = "failing"
        policy_mock = mock.Mock(side_effect=AssertionError("policy called"))
        retry_mock = mock.AsyncMock(side_effect=AssertionError("retry called"))

        with (
            mock.patch.object(
                failure_policy,
                "classify_failure_policy",
                policy_mock,
            ),
            mock.patch.object(retry, "retry_with_lease_check", retry_mock),
        ):
            released = await self.control.finalize(
                grant,
                _payload_for_grant(grant),
                grant_control.NeutralRelease(
                    grant_control.TerminalCause.SHUTDOWN
                ),
            )
            budgeted = await self.control.finalize(
                grant,
                _payload_for_grant(grant),
                _budgeted_decision(),
            )
            non_budgeted = await self.control.finalize(
                grant,
                _payload_for_grant(grant),
                _non_budgeted_decision(),
            )

        self.assertIs(
            released.disposition,
            grant_control.FinalizeDisposition.APPLIED,
        )
        budgeted_lifecycle = budgeted.lifecycle
        non_budgeted_lifecycle = non_budgeted.lifecycle
        self.assertIsNotNone(budgeted_lifecycle)
        self.assertIsNotNone(non_budgeted_lifecycle)
        assert budgeted_lifecycle is not None
        assert non_budgeted_lifecycle is not None
        self.assertTrue(budgeted_lifecycle.durable_failing)
        self.assertTrue(non_budgeted_lifecycle.durable_failing)
        self.data_store.release_feed.assert_awaited_once_with(
            grant.feed_id,
            grant.owner_worker_id,
            grant.fencing_token,
        )
        decision = _budgeted_decision()
        self.data_store.report_feed_failure.assert_awaited_once_with(
            grant.feed_id,
            grant.owner_worker_id,
            grant.fencing_token,
            decision.failure_threshold,
            decision.backoff_base_sec,
            decision.backoff_max_sec,
            actor_id=decision.actor_id,
            reason=decision.reason,
            status_reason=decision.status_reason,
        )
        non_budgeted_decision = _non_budgeted_decision()
        self.data_store.release_non_budgeted_failure.assert_awaited_once_with(
            grant.feed_id,
            grant.owner_worker_id,
            grant.fencing_token,
            retry_after=non_budgeted_decision.retry_after,
            status_reason=non_budgeted_decision.status_reason,
            actor_id=non_budgeted_decision.actor_id,
            reason=non_budgeted_decision.reason,
        )
        self.data_store.release_feeds_batch.assert_not_awaited()
        policy_mock.assert_not_called()
        retry_mock.assert_not_awaited()

    async def test_lost_and_ambiguous_feed_finalization_fail_closed(
        self,
    ) -> None:
        grant = _feed_grant()
        self.data_store.release_feed.return_value = False

        released = await self.control.finalize(
            grant,
            _payload_for_grant(grant),
            grant_control.NeutralRelease(grant_control.TerminalCause.NORMAL),
        )

        self.assertIs(
            released.disposition,
            grant_control.FinalizeDisposition.LOST,
        )
        self.data_store.report_feed_failure.side_effect = RuntimeError(
            "outcome unknown"
        )
        with self.assertRaisesRegex(RuntimeError, "outcome unknown"):
            await self.control.finalize(
                grant,
                _payload_for_grant(grant),
                _budgeted_decision(),
            )
        self.data_store.report_feed_failure.assert_awaited_once()

    async def test_quarantine_observer_runs_after_exact_commit(self) -> None:
        async def observe(*_args: object) -> None:
            self.data_store.report_feed_failure.assert_awaited_once()

        observer = mock.AsyncMock(side_effect=observe)
        control = feed_grant_control.FeedGrantControl(
            self.data_store,
            self.heartbeat_store,
            self.caps,
            _ABANDONMENT,
            on_quarantined=observer,
        )
        grant = _feed_grant()
        payload = _payload_for_grant(grant)
        assert isinstance(payload, dict)
        decision = _budgeted_decision()
        self.data_store.report_feed_failure.return_value = "quarantined"

        result = await control.finalize(grant, payload, decision)

        self.assertIs(
            result.disposition,
            grant_control.FinalizeDisposition.APPLIED,
        )
        observer.assert_awaited_once_with(grant, payload, decision)

    async def test_quarantine_observer_failure_is_non_authoritative(
        self,
    ) -> None:
        msg = "observer unavailable"
        observer = mock.AsyncMock(side_effect=RuntimeError(msg))
        control = feed_grant_control.FeedGrantControl(
            self.data_store,
            self.heartbeat_store,
            self.caps,
            _ABANDONMENT,
            on_quarantined=observer,
        )
        grant = _feed_grant()
        payload = _payload_for_grant(grant)
        assert isinstance(payload, dict)
        self.data_store.report_feed_failure.return_value = "quarantined"

        result = await control.finalize(
            grant,
            payload,
            _budgeted_decision(),
        )

        self.assertIs(
            result.disposition,
            grant_control.FinalizeDisposition.APPLIED,
        )
        self.data_store.report_feed_failure.assert_awaited_once()

    async def test_quarantine_observer_cancellation_is_non_authoritative(
        self,
    ) -> None:
        observer = mock.AsyncMock(side_effect=asyncio.CancelledError)
        control = feed_grant_control.FeedGrantControl(
            self.data_store,
            self.heartbeat_store,
            self.caps,
            _ABANDONMENT,
            on_quarantined=observer,
        )
        grant = _feed_grant()
        payload = _payload_for_grant(grant)
        assert isinstance(payload, dict)
        self.data_store.report_feed_failure.return_value = "quarantined"

        result = await control.finalize(
            grant,
            payload,
            _budgeted_decision(),
        )

        self.assertIs(
            result.disposition,
            grant_control.FinalizeDisposition.APPLIED,
        )
        observer.assert_awaited_once()

    async def test_quarantine_observer_timeout_is_non_authoritative(
        self,
    ) -> None:
        observer_started = asyncio.Event()
        observer_cancelled = asyncio.Event()

        async def observe(*_args: object) -> None:
            observer_started.set()
            try:
                await asyncio.Event().wait()
            finally:
                observer_cancelled.set()

        observer = mock.AsyncMock(side_effect=observe)
        control = feed_grant_control.FeedGrantControl(
            self.data_store,
            self.heartbeat_store,
            self.caps,
            _ABANDONMENT,
            on_quarantined=observer,
        )
        grant = _feed_grant()
        payload = _payload_for_grant(grant)
        assert isinstance(payload, dict)
        decision = _budgeted_decision()
        self.data_store.report_feed_failure.return_value = "quarantined"

        with mock.patch.object(
            feed_grant_control,
            "_QUARANTINE_OBSERVER_TIMEOUT_SEC",
            0.01,
        ):
            result = await asyncio.wait_for(
                control.finalize(grant, payload, decision),
                timeout=1,
            )

        self.assertTrue(observer_started.is_set())
        self.assertTrue(observer_cancelled.is_set())
        self.assertIs(
            result.disposition,
            grant_control.FinalizeDisposition.APPLIED,
        )
        self.data_store.report_feed_failure.assert_awaited_once()
        observer.assert_awaited_once_with(grant, payload, decision)

    async def test_quarantine_observer_ignores_durable_failing(self) -> None:
        observer = mock.AsyncMock()
        control = feed_grant_control.FeedGrantControl(
            self.data_store,
            self.heartbeat_store,
            self.caps,
            _ABANDONMENT,
            on_quarantined=observer,
        )
        grant = _feed_grant()
        payload = _payload_for_grant(grant)
        assert isinstance(payload, dict)
        self.data_store.report_feed_failure.return_value = "failing"

        await control.finalize(grant, payload, _budgeted_decision())

        observer.assert_not_awaited()

    async def test_non_budgeted_quarantine_is_rejected(self) -> None:
        observer = mock.AsyncMock()
        control = feed_grant_control.FeedGrantControl(
            self.data_store,
            self.heartbeat_store,
            self.caps,
            _ABANDONMENT,
            on_quarantined=observer,
        )
        grant = _feed_grant()
        payload = _payload_for_grant(grant)
        assert isinstance(payload, dict)
        self.data_store.release_non_budgeted_failure.return_value = (
            "quarantined"
        )

        with self.assertRaises(grant_control.GrantControlIntegrityError):
            await control.finalize(
                grant,
                payload,
                _non_budgeted_decision(),
            )

        observer.assert_not_awaited()


class TestSidGrantControl(unittest.IsolatedAsyncioTestCase):
    """SID adapter mapping, ordering, and one-shot terminal tests."""

    def setUp(self) -> None:
        self.data_store = mock.AsyncMock(
            spec=ingestion_lease_store.IngestionLeaseStore
        )
        self.heartbeat_store = mock.AsyncMock(
            spec=ingestion_lease_store.IngestionLeaseStore
        )
        self.control = sid_grant_control.SidGrantControl(
            self.data_store,
            self.heartbeat_store,
            feed_store.SourceType.BCFY_CALLS,
            _ABANDONMENT,
        )

    async def test_primary_and_recovery_preserve_claim_order_and_snapshots(
        self,
    ) -> None:
        first = _lease_claim("200")
        second = _lease_claim(
            "100",
            failure_count=2,
            status_reason=feed_store.FeedStatusReason.SOURCE_UNREACHABLE,
        )
        self.data_store.claim_unclaimed.return_value = (first, second)
        self.data_store.claim_recoverable.return_value = (second, first)

        primary = await self.control.claim(
            grant_control.ClaimMode.PRIMARY,
            _OWNER_ID,
            2,
        )
        recovery = await self.control.claim(
            grant_control.ClaimMode.RECOVERY,
            _OWNER_ID,
            2,
        )

        self.data_store.claim_unclaimed.assert_awaited_once_with(
            feed_store.SourceType.BCFY_CALLS,
            _OWNER_ID,
            2,
        )
        self.data_store.claim_recoverable.assert_awaited_once_with(
            feed_store.SourceType.BCFY_CALLS,
            _OWNER_ID,
            2,
            _ABANDONMENT,
        )
        self.assertEqual(
            [item.grant for item in primary],
            [first.grant, second.grant],
        )
        self.assertIs(primary[0].payload, first.snapshot)
        self.assertIs(primary[1].payload, second.snapshot)
        self.assertFalse(primary[0].lifecycle.durable_failing)
        self.assertTrue(primary[1].lifecycle.durable_failing)
        self.assertEqual(
            [item.grant for item in recovery],
            [second.grant, first.grant],
        )
        self.data_store.load_membership.assert_not_awaited()
        self.data_store.commit_child_mutations.assert_not_awaited()

    async def test_heartbeat_maps_every_disposition_in_caller_order(
        self,
    ) -> None:
        dispositions = (
            ingestion_lease_store.LeaseOperationDisposition.APPLIED,
            ingestion_lease_store.LeaseOperationDisposition.ACCEPTED_NOOP,
            ingestion_lease_store.LeaseOperationDisposition.STATUS_INELIGIBLE,
            ingestion_lease_store.LeaseOperationDisposition.MISSING,
            ingestion_lease_store.LeaseOperationDisposition.OWNER_MISMATCH,
            ingestion_lease_store.LeaseOperationDisposition.FENCE_MISMATCH,
        )
        grants = tuple(_lease_grant(str(index + 1)) for index in range(6))
        results = []
        for index, disposition in enumerate(dispositions):
            snapshot = None
            if (
                disposition
                is not ingestion_lease_store.LeaseOperationDisposition.MISSING
            ):
                snapshot = _lease_snapshot(
                    failure_count=1 if index == 1 else 0,
                )
            results.append(
                ingestion_lease_store.LeaseHeartbeatResult(
                    grants[index],
                    disposition,
                    snapshot,
                )
            )
        self.heartbeat_store.renew_heartbeats.return_value = tuple(results)

        translated = await self.control.heartbeat(grants)

        self.heartbeat_store.renew_heartbeats.assert_awaited_once_with(grants)
        self.assertEqual([item.grant for item in translated], list(grants))
        self.assertEqual(
            [item.disposition for item in translated],
            [
                grant_control.HeartbeatDisposition.RETAINED,
                grant_control.HeartbeatDisposition.RETAINED,
                grant_control.HeartbeatDisposition.ADMINISTRATIVE_STOP,
                grant_control.HeartbeatDisposition.LOST,
                grant_control.HeartbeatDisposition.LOST,
                grant_control.HeartbeatDisposition.LOST,
            ],
        )
        first_lifecycle = translated[0].lifecycle
        second_lifecycle = translated[1].lifecycle
        self.assertIsNotNone(first_lifecycle)
        self.assertIsNotNone(second_lifecycle)
        assert first_lifecycle is not None
        assert second_lifecycle is not None
        self.assertFalse(first_lifecycle.durable_failing)
        self.assertTrue(second_lifecycle.durable_failing)
        self.assertTrue(all(item.lifecycle is None for item in translated[2:]))

    async def test_heartbeat_rejects_every_malformed_correlation(self) -> None:
        first = _lease_grant("100")
        second = _lease_grant("200")
        first_result = ingestion_lease_store.LeaseHeartbeatResult(
            first,
            ingestion_lease_store.LeaseOperationDisposition.ACCEPTED_NOOP,
            _lease_snapshot(),
        )
        second_result = ingestion_lease_store.LeaseHeartbeatResult(
            second,
            ingestion_lease_store.LeaseOperationDisposition.ACCEPTED_NOOP,
            _lease_snapshot(),
        )
        unknown_result = ingestion_lease_store.LeaseHeartbeatResult(
            _lease_grant("300"),
            ingestion_lease_store.LeaseOperationDisposition.ACCEPTED_NOOP,
            _lease_snapshot(),
        )
        cases = (
            (first_result,),
            (first_result, second_result, unknown_result),
            (first_result, first_result),
            (second_result, first_result),
            (unknown_result, second_result),
            (mock.Mock(), second_result),
        )

        for results in cases:
            with self.subTest(results=results):
                self.heartbeat_store.reset_mock()
                self.heartbeat_store.renew_heartbeats.return_value = results
                with self.assertRaises(
                    grant_control.GrantControlIntegrityError
                ):
                    await self.control.heartbeat((first, second))

    async def test_heartbeat_store_exception_propagates_once(self) -> None:
        grant = _lease_grant()
        self.heartbeat_store.renew_heartbeats.side_effect = RuntimeError(
            "heartbeat unavailable"
        )

        with self.assertRaisesRegex(RuntimeError, "heartbeat unavailable"):
            await self.control.heartbeat((grant,))

        self.heartbeat_store.renew_heartbeats.assert_awaited_once()

    async def test_every_neutral_cause_maps_to_exact_lease_release(
        self,
    ) -> None:
        grant = _lease_grant()
        expected = {
            grant_control.TerminalCause.NORMAL: (
                ingestion_lease_store.LeaseReleaseCause.NORMAL
            ),
            grant_control.TerminalCause.SHUTDOWN: (
                ingestion_lease_store.LeaseReleaseCause.SHUTDOWN
            ),
            grant_control.TerminalCause.PLANNED_DRAIN: (
                ingestion_lease_store.LeaseReleaseCause.REBALANCE
            ),
            grant_control.TerminalCause.CANCELLATION: (
                ingestion_lease_store.LeaseReleaseCause.CANCELLATION
            ),
        }

        for cause, store_cause in expected.items():
            with self.subTest(cause=cause.value):
                self.data_store.reset_mock()
                self.data_store.release.return_value = (
                    ingestion_lease_store.LeaseOperationResult(
                        ingestion_lease_store.LeaseOperationDisposition.APPLIED,
                        _lease_snapshot(status=feed_store.FeedStatus.UNCLAIMED),
                    )
                )

                result = await self.control.finalize(
                    grant,
                    _payload_for_grant(grant),
                    grant_control.NeutralRelease(cause),
                )

                self.assertIs(
                    result.disposition,
                    grant_control.FinalizeDisposition.APPLIED,
                )
                self.data_store.release.assert_awaited_once_with(
                    grant,
                    cause=store_cause,
                )
                self.data_store.load_membership.assert_not_awaited()
                self.data_store.commit_child_mutations.assert_not_awaited()

    async def test_failure_decisions_make_one_exact_finalize_call(self) -> None:
        grant = _lease_grant()
        before = _lease_snapshot()
        after = _lease_snapshot(
            status=feed_store.FeedStatus.FAILING,
            failure_count=1,
            status_reason=(
                feed_store.FeedStatusReason.SYSTEM_CONFIGURATION_INVALID
            ),
        )
        self.data_store.finalize_failure.return_value = (
            ingestion_lease_store.LeaseFailureResult(
                ingestion_lease_store.LeaseOperationDisposition.APPLIED,
                ingestion_lease_store.LeaseFailureEffect.FAILURE_RECORDED,
                before,
                after,
            )
        )
        policy_mock = mock.Mock(side_effect=AssertionError("policy called"))
        retry_mock = mock.AsyncMock(side_effect=AssertionError("retry called"))

        with (
            mock.patch.object(
                failure_policy,
                "classify_failure_policy",
                policy_mock,
            ),
            mock.patch.object(retry, "retry_with_lease_check", retry_mock),
        ):
            budgeted = await self.control.finalize(
                grant,
                _payload_for_grant(grant),
                _budgeted_decision(),
            )
            budgeted_call = self.data_store.finalize_failure.await_args
            self.data_store.finalize_failure.reset_mock()
            non_budgeted = await self.control.finalize(
                grant,
                _payload_for_grant(grant),
                _non_budgeted_decision(),
            )
            non_budgeted_call = self.data_store.finalize_failure.await_args

        budgeted_action = budgeted_call.args[1]
        self.assertIsInstance(
            budgeted_action,
            ingestion_lease_store.BudgetedFailure,
        )
        self.assertEqual(budgeted_action.failure_threshold, 5)
        self.assertEqual(budgeted_call.args[0], grant)
        self.assertEqual(
            budgeted_call.args[2],
            _budgeted_decision().status_reason,
        )
        self.assertEqual(budgeted_call.kwargs["actor_id"], _ACTOR_ID)
        self.assertIsInstance(
            non_budgeted_call.args[1],
            ingestion_lease_store.NonBudgetedFailure,
        )
        self.assertEqual(
            non_budgeted_call.args[1].retry_after,
            _non_budgeted_decision().retry_after,
        )
        self.assertIs(
            budgeted.disposition,
            grant_control.FinalizeDisposition.APPLIED,
        )
        budgeted_lifecycle = budgeted.lifecycle
        non_budgeted_lifecycle = non_budgeted.lifecycle
        self.assertIsNotNone(budgeted_lifecycle)
        self.assertIsNotNone(non_budgeted_lifecycle)
        assert budgeted_lifecycle is not None
        assert non_budgeted_lifecycle is not None
        self.assertTrue(budgeted_lifecycle.durable_failing)
        self.assertTrue(non_budgeted_lifecycle.durable_failing)
        policy_mock.assert_not_called()
        retry_mock.assert_not_awaited()
        self.data_store.load_membership.assert_not_awaited()
        self.data_store.commit_child_mutations.assert_not_awaited()

    async def test_non_budgeted_quarantine_is_rejected(self) -> None:
        grant = _lease_grant()
        before = _lease_snapshot()
        quarantined = _lease_snapshot(
            status=feed_store.FeedStatus.QUARANTINED,
            failure_count=5,
            status_reason=(
                feed_store.FeedStatusReason.SYSTEM_CONFIGURATION_INVALID
            ),
        )
        for effect in (
            ingestion_lease_store.LeaseFailureEffect.QUARANTINED,
            ingestion_lease_store.LeaseFailureEffect.FAILURE_RECORDED,
        ):
            with self.subTest(effect=effect):
                self.data_store.finalize_failure.return_value = (
                    ingestion_lease_store.LeaseFailureResult(
                        ingestion_lease_store.LeaseOperationDisposition.APPLIED,
                        effect,
                        before,
                        quarantined,
                    )
                )

                with self.assertRaises(
                    grant_control.GrantControlIntegrityError
                ):
                    await self.control.finalize(
                        grant,
                        _payload_for_grant(grant),
                        _non_budgeted_decision(),
                    )

    async def test_finalize_maps_noop_and_loss_without_batch_release(
        self,
    ) -> None:
        grant = _lease_grant()
        snapshot = _lease_snapshot()
        cases = (
            (
                ingestion_lease_store.LeaseOperationDisposition.ACCEPTED_NOOP,
                grant_control.FinalizeDisposition.ACCEPTED_NOOP,
                snapshot,
            ),
            (
                ingestion_lease_store.LeaseOperationDisposition.MISSING,
                grant_control.FinalizeDisposition.LOST,
                None,
            ),
            (
                ingestion_lease_store.LeaseOperationDisposition.OWNER_MISMATCH,
                grant_control.FinalizeDisposition.LOST,
                snapshot,
            ),
            (
                ingestion_lease_store.LeaseOperationDisposition.FENCE_MISMATCH,
                grant_control.FinalizeDisposition.LOST,
                snapshot,
            ),
            (
                ingestion_lease_store.LeaseOperationDisposition.STATUS_INELIGIBLE,
                grant_control.FinalizeDisposition.LOST,
                snapshot,
            ),
        )

        for disposition, expected, current in cases:
            with self.subTest(disposition=disposition.value):
                self.data_store.reset_mock()
                self.data_store.release.return_value = (
                    ingestion_lease_store.LeaseOperationResult(
                        disposition,
                        current,
                    )
                )

                result = await self.control.finalize(
                    grant,
                    _payload_for_grant(grant),
                    grant_control.NeutralRelease(
                        grant_control.TerminalCause.NORMAL
                    ),
                )

                self.assertIs(result.disposition, expected)
                self.assertFalse(hasattr(self.data_store, "release_batch"))

    async def test_retained_release_requires_snapshot_evidence(self) -> None:
        grant = _lease_grant()
        self.data_store.release.return_value = (
            ingestion_lease_store.LeaseOperationResult(
                ingestion_lease_store.LeaseOperationDisposition.ACCEPTED_NOOP,
                None,
            )
        )

        with self.assertRaises(grant_control.GrantControlIntegrityError):
            await self.control.finalize(
                grant,
                _payload_for_grant(grant),
                grant_control.NeutralRelease(
                    grant_control.TerminalCause.NORMAL
                ),
            )

    async def test_store_exception_propagates_without_retry(self) -> None:
        grant = _lease_grant()
        self.data_store.finalize_failure.side_effect = RuntimeError(
            "outcome unknown"
        )

        with self.assertRaisesRegex(RuntimeError, "outcome unknown"):
            await self.control.finalize(
                grant,
                _payload_for_grant(grant),
                _budgeted_decision(),
            )

        self.data_store.finalize_failure.assert_awaited_once()


class TestGrantControlStructuralBoundaries(unittest.TestCase):
    """Static guards against policy, retry, and later-phase coupling."""

    def test_adapters_have_only_claim_heartbeat_finalize_public_methods(
        self,
    ) -> None:
        for adapter in (
            feed_grant_control.FeedGrantControl,
            sid_grant_control.SidGrantControl,
        ):
            with self.subTest(adapter=adapter.__name__):
                methods = {
                    name
                    for name, value in vars(adapter).items()
                    if inspect.isfunction(value) and not name.startswith("_")
                }
                self.assertEqual(methods, {"claim", "heartbeat", "finalize"})

    def test_adapters_do_not_call_policy_retry_or_later_phase_surfaces(
        self,
    ) -> None:
        feed_source = pathlib.Path(feed_grant_control.__file__).read_text()
        sid_source = pathlib.Path(sid_grant_control.__file__).read_text()

        for source in (feed_source, sid_source):
            self.assertNotIn("classify_failure_policy", source)
            self.assertNotIn("retry_with_lease_check", source)
        self.assertNotIn("release_feeds_batch", feed_source)
        for forbidden in (
            "load_membership(",
            "commit_child_mutations(",
            "scheduler",
            "poller",
            "cursor",
            "cohort",
        ):
            with self.subTest(forbidden=forbidden):
                self.assertNotIn(forbidden, sid_source)
