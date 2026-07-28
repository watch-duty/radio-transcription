"""Contract tests for the closed Feed and SID grant controls."""

from __future__ import annotations

import asyncio
import dataclasses
import datetime
import inspect
import pathlib
import typing
import unittest
import uuid
from unittest import mock

import asyncpg

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
_TRANSIENT_POSTGRES_ERRORS = (
    asyncpg.TooManyConnectionsError,
    asyncpg.AdminShutdownError,
    asyncpg.CrashShutdownError,
    asyncpg.CannotConnectNowError,
    asyncpg.QueryCanceledError,
)


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


def _lease_grant(
    lease_key: str = "123",
    *,
    source_type: feed_store.SourceType = feed_store.SourceType.BCFY_CALLS,
    owner_worker_id: uuid.UUID = _OWNER_ID,
    fencing_token: int = 7,
) -> ingestion_lease_store.LeaseGrant:
    return ingestion_lease_store.LeaseGrant(
        source_type=source_type,
        lease_key=lease_key,
        owner_worker_id=owner_worker_id,
        fencing_token=fencing_token,
    )


def _lease_claim(
    lease_key: str,
) -> ingestion_lease_store.LeaseClaim:
    return ingestion_lease_store.LeaseClaim(grant=_lease_grant(lease_key))


def _feed_payload_for_grant(
    grant: feed_store.FeedGrant,
) -> feed_store.LeasedFeed:
    return _leased_feed(
        grant.feed_id,
        fencing_token=grant.fencing_token,
    )


def _budgeted_plan() -> failure_policy.FailurePersistencePlan:
    return failure_policy.FailurePersistencePlan(
        status_reason=feed_store.FeedStatusReason.SYSTEM_CONFIGURATION_INVALID,
        reason="invalid configuration",
        treatment=failure_policy.ConsumeFailureBudget(
            failure_threshold=5,
            backoff_base_sec=15,
            backoff_max_sec=600,
        ),
    )


def _non_budgeted_plan() -> failure_policy.FailurePersistencePlan:
    return failure_policy.FailurePersistencePlan(
        status_reason=feed_store.FeedStatusReason.SOURCE_UNREACHABLE,
        reason="provider unavailable",
        treatment=failure_policy.RetryWithoutBudget(
            _NOW + datetime.timedelta(minutes=8)
        ),
    )


class TestGrantControlVocabulary(unittest.TestCase):
    """Static contract for the deliberately small generic vocabulary."""

    def test_closed_enums_have_only_the_planned_values(self) -> None:
        expected = {
            grant_control.DomainId: {"feed", "sid"},
            grant_control.ClaimMode: {"primary", "recovery"},
            grant_control.HeartbeatDisposition: {
                "retained",
                "ineligible",
                "lost",
            },
            grant_control.FinalizeDisposition: {
                "applied",
                "lost",
            },
        }

        for enum_type, values in expected.items():
            with self.subTest(enum_type=enum_type.__name__):
                self.assertEqual({item.value for item in enum_type}, values)

    def test_results_expose_only_functional_control_state(self) -> None:
        expected_fields = {
            grant_control.ClaimedGrant: ("grant", "payload"),
            grant_control.GrantHeartbeat: ("grant", "disposition"),
            grant_control.FinalizeResult: ("grant", "disposition"),
            grant_control.NeutralRelease: (),
        }

        for result_type, expected in expected_fields.items():
            with self.subTest(result_type=result_type.__name__):
                self.assertEqual(
                    tuple(
                        field.name for field in dataclasses.fields(result_type)
                    ),
                    expected,
                )

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

    def test_run_context_owns_only_closed_signals(self) -> None:
        context = grant_control.RunContext(
            stop_requested=asyncio.Event(),
            grant_lost=asyncio.Event(),
        )

        self.assertEqual(
            tuple(field.name for field in dataclasses.fields(context)),
            ("stop_requested", "grant_lost"),
        )
        self.assertEqual(
            set(typing.get_args(grant_control.RunOutcome.__value__)),
            {
                grant_control.RunCompleted,
                grant_control.RunLost,
                grant_control.RunFailed,
            },
        )

    def test_exact_grants_expose_permanent_unit_keys(self) -> None:
        feed_grant = _feed_grant()
        sid_grant = _lease_grant()

        self.assertEqual(feed_grant.unit_key, feed_grant.feed_id)
        self.assertEqual(
            sid_grant.unit_key,
            (sid_grant.source_type, sid_grant.lease_key),
        )

    def test_dataclass_annotations_resolve_at_runtime(self) -> None:
        failed_hints = typing.get_type_hints(grant_control.RunFailed)
        context_hints = typing.get_type_hints(grant_control.RunContext)

        self.assertIs(
            failed_hints["status_reason"],
            feed_store.FeedStatusReason,
        )
        self.assertIs(context_hints["stop_requested"], asyncio.Event)
        self.assertIs(context_hints["grant_lost"], asyncio.Event)

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
            actor_id=_ACTOR_ID,
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

    async def test_claim_rejects_excess_and_duplicate_authority(self) -> None:
        first = _leased_feed(uuid.UUID(int=1))
        second = _leased_feed(uuid.UUID(int=2))
        cases = (
            ("excess", [first, second], 1),
            ("duplicate", [first, first], 2),
        )

        for case_name, payloads, limit in cases:
            with self.subTest(case_name=case_name, limit=limit):
                self.data_store.reset_mock()
                self.data_store.count_held_by_type.return_value = dict.fromkeys(
                    feed_store.SourceType,
                    0,
                )
                self.data_store.acquire_feeds_batch.return_value = payloads

                with self.assertRaises(
                    grant_control.GrantControlIntegrityError
                ):
                    await self.control.claim(
                        grant_control.ClaimMode.PRIMARY,
                        _OWNER_ID,
                        limit,
                    )

    async def test_heartbeat_maps_every_disposition_in_caller_order(
        self,
    ) -> None:
        dispositions = (
            feed_store.FeedGrantOperationDisposition.APPLIED,
            feed_store.FeedGrantOperationDisposition.STATUS_INELIGIBLE,
            feed_store.FeedGrantOperationDisposition.MISSING,
            feed_store.FeedGrantOperationDisposition.OWNER_MISMATCH,
            feed_store.FeedGrantOperationDisposition.FENCE_MISMATCH,
        )
        grants = tuple(_feed_grant() for _ in dispositions)
        results = tuple(
            feed_store.FeedGrantHeartbeatResult(grant, disposition)
            for grant, disposition in zip(grants, dispositions, strict=True)
        )
        self.heartbeat_store.renew_grant_heartbeats.return_value = results

        translated = await self.control.heartbeat(grants)

        self.heartbeat_store.renew_grant_heartbeats.assert_awaited_once_with(
            grants
        )
        self.assertEqual([item.grant for item in translated], list(grants))
        self.assertEqual(
            [item.disposition for item in translated],
            [
                grant_control.HeartbeatDisposition.RETAINED,
                grant_control.HeartbeatDisposition.INELIGIBLE,
                grant_control.HeartbeatDisposition.LOST,
                grant_control.HeartbeatDisposition.LOST,
                grant_control.HeartbeatDisposition.LOST,
            ],
        )

    async def test_heartbeat_rejects_every_malformed_correlation(self) -> None:
        first = _feed_grant()
        second = _feed_grant()
        first_result = feed_store.FeedGrantHeartbeatResult(
            first,
            feed_store.FeedGrantOperationDisposition.APPLIED,
        )
        second_result = feed_store.FeedGrantHeartbeatResult(
            second,
            feed_store.FeedGrantOperationDisposition.APPLIED,
        )
        unknown_result = feed_store.FeedGrantHeartbeatResult(
            _feed_grant(),
            feed_store.FeedGrantOperationDisposition.APPLIED,
        )
        cases = (
            (first_result,),
            (first_result, second_result, unknown_result),
            (first_result, first_result),
            (second_result, first_result),
            (unknown_result, second_result),
        )

        for case_index, results in enumerate(cases):
            with self.subTest(case_index=case_index):
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

    async def test_transient_store_io_is_classified_for_supervision(
        self,
    ) -> None:
        grant = _feed_grant()
        payload = _feed_payload_for_grant(grant)
        operations = (
            (
                self.data_store.count_held_by_type,
                self.control.claim(
                    grant_control.ClaimMode.PRIMARY,
                    _OWNER_ID,
                    1,
                ),
            ),
            (
                self.heartbeat_store.renew_grant_heartbeats,
                self.control.heartbeat((grant,)),
            ),
            (
                self.data_store.release_feed,
                self.control.finalize(
                    grant,
                    payload,
                    grant_control.NeutralRelease(),
                ),
            ),
        )

        for store_call, operation in operations:
            with self.subTest(store_call=store_call._mock_name):
                failure = OSError("connection reset")
                store_call.side_effect = failure
                with self.assertRaises(
                    grant_control.GrantControlBackendUnavailable
                ) as raised:
                    await operation
                self.assertIs(raised.exception.__cause__, failure)
                store_call.side_effect = None

    async def test_transient_postgres_errors_are_classified_for_supervision(
        self,
    ) -> None:
        grant = _feed_grant()

        for failure_type in _TRANSIENT_POSTGRES_ERRORS:
            with self.subTest(failure_type=failure_type.__name__):
                failure = failure_type("backend temporarily unavailable")
                self.heartbeat_store.renew_grant_heartbeats.side_effect = (
                    failure
                )
                with self.assertRaises(
                    grant_control.GrantControlBackendUnavailable
                ) as raised:
                    await self.control.heartbeat((grant,))
                self.assertIs(raised.exception.__cause__, failure)
                self.heartbeat_store.renew_grant_heartbeats.side_effect = None

    async def test_neutral_release_uses_exact_feed_release(self) -> None:
        grant = _feed_grant()
        self.data_store.release_feed.return_value = True

        result = await self.control.finalize(
            grant,
            _feed_payload_for_grant(grant),
            grant_control.NeutralRelease(),
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
        budgeted_plan = _budgeted_plan()
        non_budgeted_plan = _non_budgeted_plan()
        policy_mock = mock.Mock(side_effect=AssertionError("policy called"))
        retry_mock = mock.AsyncMock(side_effect=AssertionError("retry called"))

        with (
            mock.patch.object(
                failure_policy,
                "consumes_failure_budget",
                policy_mock,
            ),
            mock.patch.object(retry, "retry_with_lease_check", retry_mock),
        ):
            released = await self.control.finalize(
                grant,
                _feed_payload_for_grant(grant),
                grant_control.NeutralRelease(),
            )
            budgeted = await self.control.finalize(
                grant,
                _feed_payload_for_grant(grant),
                budgeted_plan,
            )
            non_budgeted = await self.control.finalize(
                grant,
                _feed_payload_for_grant(grant),
                non_budgeted_plan,
            )

        self.assertIs(
            released.disposition,
            grant_control.FinalizeDisposition.APPLIED,
        )
        self.assertIs(
            budgeted.disposition,
            grant_control.FinalizeDisposition.APPLIED,
        )
        self.assertIs(
            non_budgeted.disposition,
            grant_control.FinalizeDisposition.APPLIED,
        )
        self.data_store.release_feed.assert_awaited_once_with(
            grant.feed_id,
            grant.owner_worker_id,
            grant.fencing_token,
        )
        treatment = typing.cast(
            "failure_policy.ConsumeFailureBudget",
            budgeted_plan.treatment,
        )
        self.data_store.report_feed_failure.assert_awaited_once_with(
            grant.feed_id,
            grant.owner_worker_id,
            grant.fencing_token,
            treatment.failure_threshold,
            treatment.backoff_base_sec,
            treatment.backoff_max_sec,
            actor_id=_ACTOR_ID,
            reason=budgeted_plan.reason,
            status_reason=budgeted_plan.status_reason,
        )
        retry_treatment = typing.cast(
            "failure_policy.RetryWithoutBudget",
            non_budgeted_plan.treatment,
        )
        self.data_store.release_non_budgeted_failure.assert_awaited_once_with(
            grant.feed_id,
            grant.owner_worker_id,
            grant.fencing_token,
            retry_after=retry_treatment.retry_after,
            status_reason=non_budgeted_plan.status_reason,
            actor_id=_ACTOR_ID,
            reason=non_budgeted_plan.reason,
        )
        policy_mock.assert_not_called()
        retry_mock.assert_not_awaited()

    async def test_lost_and_ambiguous_feed_finalization_fail_closed(
        self,
    ) -> None:
        grant = _feed_grant()
        self.data_store.release_feed.return_value = False

        released = await self.control.finalize(
            grant,
            _feed_payload_for_grant(grant),
            grant_control.NeutralRelease(),
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
                _feed_payload_for_grant(grant),
                _budgeted_plan(),
            )
        self.data_store.report_feed_failure.assert_awaited_once()

    async def test_failure_finalization_emits_legacy_policy_event(self) -> None:
        grant = _feed_grant()
        payload = _feed_payload_for_grant(grant)
        plan = _budgeted_plan()
        self.data_store.report_feed_failure.return_value = "failing"

        with self.assertLogs(feed_grant_control.logger, level="INFO") as logs:
            result = await self.control.finalize(grant, payload, plan)

        self.assertIs(
            result.disposition,
            grant_control.FinalizeDisposition.APPLIED,
        )
        records = [
            typing.cast("dict[str, object]", record.__dict__["json_fields"])
            for record in logs.records
        ]
        self.assertEqual(len(records), 1)
        self.assertEqual(
            records[0],
            {
                "event_type": "feed_failure_policy_decision",
                "feed_id": str(grant.feed_id),
                "source_type": feed_store.SourceType.BCFY_CALLS.value,
                "reason": plan.reason,
                "status_reason": plan.status_reason.value,
                "replay_missing": False,
                "data_gap_known": False,
                "executed_action": "increment_feed_failure_budget",
            },
        )

    async def test_publish_gap_emits_policy_and_gap_events(self) -> None:
        grant = _feed_grant()
        payload = _feed_payload_for_grant(grant)
        retry_after = _NOW + datetime.timedelta(minutes=8)
        plan = failure_policy.FailurePersistencePlan(
            status_reason=(
                feed_store.FeedStatusReason.PIPELINE_PUBLISH_AFTER_BOOKMARK_FAILED
            ),
            reason="publish failed",
            treatment=failure_policy.RetryWithoutBudget(retry_after),
        )
        self.data_store.release_non_budgeted_failure.return_value = "failing"

        with self.assertLogs(feed_grant_control.logger, level="INFO") as logs:
            result = await self.control.finalize(grant, payload, plan)

        self.assertIs(
            result.disposition,
            grant_control.FinalizeDisposition.APPLIED,
        )
        records = [
            typing.cast("dict[str, object]", record.__dict__["json_fields"])
            for record in logs.records
        ]
        self.assertEqual(
            [record["event_type"] for record in records],
            [
                "feed_failure_policy_decision",
                "post_bookmark_publish_failure",
            ],
        )
        for record in records:
            self.assertEqual(record["feed_id"], str(grant.feed_id))
            self.assertEqual(record["status_reason"], plan.status_reason.value)
            self.assertTrue(record["replay_missing"])
            self.assertTrue(record["data_gap_known"])
            self.assertEqual(
                record["executed_action"],
                "record_post_bookmark_publish_gap",
            )
        self.assertEqual(records[0]["retry_after"], retry_after.isoformat())
        self.assertNotIn("retry_after", records[1])

    async def test_quarantine_observer_runs_after_exact_commit(self) -> None:
        async def observe(*_args: object) -> None:
            self.data_store.report_feed_failure.assert_awaited_once()

        observer = mock.AsyncMock(side_effect=observe)
        control = feed_grant_control.FeedGrantControl(
            self.data_store,
            self.heartbeat_store,
            self.caps,
            _ABANDONMENT,
            actor_id=_ACTOR_ID,
            on_quarantined=observer,
        )
        grant = _feed_grant()
        payload = _feed_payload_for_grant(grant)
        plan = _budgeted_plan()
        self.data_store.report_feed_failure.return_value = "quarantined"

        result = await control.finalize(grant, payload, plan)

        self.assertIs(
            result.disposition,
            grant_control.FinalizeDisposition.APPLIED,
        )
        observer.assert_awaited_once_with(grant, payload, plan)

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
            actor_id=_ACTOR_ID,
            on_quarantined=observer,
        )
        grant = _feed_grant()
        payload = _feed_payload_for_grant(grant)
        self.data_store.report_feed_failure.return_value = "quarantined"

        result = await control.finalize(
            grant,
            payload,
            _budgeted_plan(),
        )

        self.assertIs(
            result.disposition,
            grant_control.FinalizeDisposition.APPLIED,
        )
        self.data_store.report_feed_failure.assert_awaited_once()

    async def test_quarantine_observer_cancellation_is_non_authoritative(
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
            actor_id=_ACTOR_ID,
            on_quarantined=observer,
        )
        grant = _feed_grant()
        payload = _feed_payload_for_grant(grant)
        self.data_store.report_feed_failure.return_value = "quarantined"

        finalization = asyncio.create_task(
            control.finalize(
                grant,
                payload,
                _budgeted_plan(),
            )
        )
        await asyncio.wait_for(observer_started.wait(), timeout=1)
        finalization.cancel()
        result = await finalization

        self.assertTrue(observer_cancelled.is_set())
        self.assertIs(
            result.disposition,
            grant_control.FinalizeDisposition.APPLIED,
        )
        self.data_store.report_feed_failure.assert_awaited_once()
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
            actor_id=_ACTOR_ID,
            on_quarantined=observer,
        )
        grant = _feed_grant()
        payload = _feed_payload_for_grant(grant)
        plan = _budgeted_plan()
        self.data_store.report_feed_failure.return_value = "quarantined"

        with mock.patch.object(
            feed_grant_control,
            "_QUARANTINE_OBSERVER_TIMEOUT_SEC",
            0.01,
        ):
            result = await asyncio.wait_for(
                control.finalize(grant, payload, plan),
                timeout=1,
            )

        self.assertTrue(observer_started.is_set())
        self.assertTrue(observer_cancelled.is_set())
        self.assertIs(
            result.disposition,
            grant_control.FinalizeDisposition.APPLIED,
        )
        self.data_store.report_feed_failure.assert_awaited_once()
        observer.assert_awaited_once_with(grant, payload, plan)

    async def test_quarantine_observer_ignores_non_quarantined_failure(
        self,
    ) -> None:
        observer = mock.AsyncMock()
        control = feed_grant_control.FeedGrantControl(
            self.data_store,
            self.heartbeat_store,
            self.caps,
            _ABANDONMENT,
            actor_id=_ACTOR_ID,
            on_quarantined=observer,
        )
        grant = _feed_grant()
        payload = _feed_payload_for_grant(grant)
        self.data_store.report_feed_failure.return_value = "failing"

        await control.finalize(grant, payload, _budgeted_plan())

        observer.assert_not_awaited()

    async def test_non_budgeted_quarantine_is_rejected(self) -> None:
        observer = mock.AsyncMock()
        control = feed_grant_control.FeedGrantControl(
            self.data_store,
            self.heartbeat_store,
            self.caps,
            _ABANDONMENT,
            actor_id=_ACTOR_ID,
            on_quarantined=observer,
        )
        grant = _feed_grant()
        payload = _feed_payload_for_grant(grant)
        self.data_store.release_non_budgeted_failure.return_value = (
            "quarantined"
        )

        with self.assertRaises(grant_control.GrantControlIntegrityError):
            await control.finalize(
                grant,
                payload,
                _non_budgeted_plan(),
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
            actor_id=_ACTOR_ID,
        )

    async def test_primary_and_recovery_preserve_order_and_mode(
        self,
    ) -> None:
        first = _lease_claim("200")
        second = _lease_claim("100")
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
        self.assertIs(
            primary[0].payload,
            grant_control.ClaimMode.PRIMARY,
        )
        self.assertIs(
            recovery[0].payload,
            grant_control.ClaimMode.RECOVERY,
        )
        self.assertEqual(
            [item.grant for item in recovery],
            [second.grant, first.grant],
        )
        self.data_store.load_membership.assert_not_awaited()
        self.data_store.commit_child_mutations.assert_not_awaited()

    async def test_claim_mode_is_returned_for_every_claim(
        self,
    ) -> None:
        stale_active = _lease_claim("stale-active")
        retained_failure = _lease_claim("retained-failure")
        primary_failed = _lease_claim("primary-failed")
        self.data_store.claim_recoverable.return_value = (
            stale_active,
            retained_failure,
        )
        self.data_store.claim_unclaimed.return_value = (primary_failed,)

        recovered = await self.control.claim(
            grant_control.ClaimMode.RECOVERY,
            _OWNER_ID,
            2,
        )
        primary = await self.control.claim(
            grant_control.ClaimMode.PRIMARY,
            _OWNER_ID,
            1,
        )

        self.assertTrue(
            all(
                claim.payload is grant_control.ClaimMode.RECOVERY
                for claim in recovered
            )
        )
        self.assertIs(
            primary[0].payload,
            grant_control.ClaimMode.PRIMARY,
        )

    async def test_zero_claim_limit_touches_no_store(self) -> None:
        for mode in grant_control.ClaimMode:
            with self.subTest(mode=mode.value):
                result = await self.control.claim(mode, _OWNER_ID, 0)

                self.assertEqual(result, ())

        self.data_store.claim_unclaimed.assert_not_awaited()
        self.data_store.claim_recoverable.assert_not_awaited()

    async def test_claim_rejects_every_malformed_authority(self) -> None:
        first = _lease_claim("100")
        second = _lease_claim("200")
        wrong_source = ingestion_lease_store.LeaseClaim(
            _lease_grant(
                "wrong-source",
                source_type=feed_store.SourceType.BCFY_FEEDS,
            )
        )
        wrong_owner = ingestion_lease_store.LeaseClaim(
            _lease_grant(
                "wrong-owner",
                owner_worker_id=_OTHER_OWNER_ID,
            )
        )
        cases = (
            ("excess", (first, second), 1),
            ("duplicate", (first, first), 2),
            ("wrong_source", (wrong_source,), 1),
            ("wrong_owner", (wrong_owner,), 1),
        )

        for case_name, claims, limit in cases:
            with self.subTest(case_name=case_name, limit=limit):
                self.data_store.reset_mock()
                self.data_store.claim_unclaimed.return_value = claims

                with self.assertRaises(
                    grant_control.GrantControlIntegrityError
                ):
                    await self.control.claim(
                        grant_control.ClaimMode.PRIMARY,
                        _OWNER_ID,
                        limit,
                    )

    async def test_heartbeat_maps_every_disposition_in_caller_order(
        self,
    ) -> None:
        dispositions = (
            ingestion_lease_store.LeaseOperationDisposition.APPLIED,
            ingestion_lease_store.LeaseOperationDisposition.STATUS_INELIGIBLE,
            ingestion_lease_store.LeaseOperationDisposition.MISSING,
            ingestion_lease_store.LeaseOperationDisposition.OWNER_MISMATCH,
            ingestion_lease_store.LeaseOperationDisposition.FENCE_MISMATCH,
        )
        grants = tuple(
            _lease_grant(str(index + 1)) for index in range(len(dispositions))
        )
        results = tuple(
            ingestion_lease_store.LeaseHeartbeatResult(grant, disposition)
            for grant, disposition in zip(grants, dispositions, strict=True)
        )
        self.heartbeat_store.renew_heartbeats.return_value = results

        translated = await self.control.heartbeat(grants)

        self.heartbeat_store.renew_heartbeats.assert_awaited_once_with(grants)
        self.assertEqual([item.grant for item in translated], list(grants))
        self.assertEqual(
            [item.disposition for item in translated],
            [
                grant_control.HeartbeatDisposition.RETAINED,
                grant_control.HeartbeatDisposition.INELIGIBLE,
                grant_control.HeartbeatDisposition.LOST,
                grant_control.HeartbeatDisposition.LOST,
                grant_control.HeartbeatDisposition.LOST,
            ],
        )

    async def test_heartbeat_rejects_every_malformed_correlation(self) -> None:
        first = _lease_grant("100")
        second = _lease_grant("200")
        first_result = ingestion_lease_store.LeaseHeartbeatResult(
            first,
            ingestion_lease_store.LeaseOperationDisposition.APPLIED,
        )
        second_result = ingestion_lease_store.LeaseHeartbeatResult(
            second,
            ingestion_lease_store.LeaseOperationDisposition.APPLIED,
        )
        unknown_result = ingestion_lease_store.LeaseHeartbeatResult(
            _lease_grant("300"),
            ingestion_lease_store.LeaseOperationDisposition.APPLIED,
        )
        cases = (
            (first_result,),
            (first_result, second_result, unknown_result),
            (first_result, first_result),
            (second_result, first_result),
            (unknown_result, second_result),
        )

        for case_index, results in enumerate(cases):
            with self.subTest(case_index=case_index):
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

    async def test_transient_store_io_is_classified_for_supervision(
        self,
    ) -> None:
        grant = _lease_grant()
        operations = (
            (
                self.data_store.claim_unclaimed,
                self.control.claim(
                    grant_control.ClaimMode.PRIMARY,
                    _OWNER_ID,
                    1,
                ),
            ),
            (
                self.heartbeat_store.renew_heartbeats,
                self.control.heartbeat((grant,)),
            ),
            (
                self.data_store.release,
                self.control.finalize(
                    grant,
                    grant_control.ClaimMode.PRIMARY,
                    grant_control.NeutralRelease(),
                ),
            ),
        )

        for store_call, operation in operations:
            with self.subTest(store_call=store_call._mock_name):
                failure = OSError("connection reset")
                store_call.side_effect = failure
                with self.assertRaises(
                    grant_control.GrantControlBackendUnavailable
                ) as raised:
                    await operation
                self.assertIs(raised.exception.__cause__, failure)
                store_call.side_effect = None

    async def test_transient_postgres_errors_are_classified_for_supervision(
        self,
    ) -> None:
        grant = _lease_grant()

        for failure_type in _TRANSIENT_POSTGRES_ERRORS:
            with self.subTest(failure_type=failure_type.__name__):
                failure = failure_type("backend temporarily unavailable")
                self.heartbeat_store.renew_heartbeats.side_effect = failure
                with self.assertRaises(
                    grant_control.GrantControlBackendUnavailable
                ) as raised:
                    await self.control.heartbeat((grant,))
                self.assertIs(raised.exception.__cause__, failure)
                self.heartbeat_store.renew_heartbeats.side_effect = None

    async def test_neutral_release_maps_to_exact_lease_release(
        self,
    ) -> None:
        grant = _lease_grant()
        self.data_store.release.return_value = (
            ingestion_lease_store.LeaseOperationResult(
                ingestion_lease_store.LeaseOperationDisposition.APPLIED,
            )
        )

        result = await self.control.finalize(
            grant,
            grant_control.ClaimMode.PRIMARY,
            grant_control.NeutralRelease(),
        )

        self.assertIs(
            result.disposition,
            grant_control.FinalizeDisposition.APPLIED,
        )
        self.data_store.release.assert_awaited_once_with(grant)
        self.data_store.load_membership.assert_not_awaited()
        self.data_store.commit_child_mutations.assert_not_awaited()

    async def test_failure_plans_make_one_exact_finalize_call(self) -> None:
        grant = _lease_grant()
        self.data_store.finalize_failure.return_value = (
            ingestion_lease_store.LeaseFailureResult(
                ingestion_lease_store.LeaseOperationDisposition.APPLIED,
                feed_store.FeedStatus.FAILING,
            )
        )
        budgeted_plan = _budgeted_plan()
        non_budgeted_plan = _non_budgeted_plan()
        policy_mock = mock.Mock(side_effect=AssertionError("policy called"))
        retry_mock = mock.AsyncMock(side_effect=AssertionError("retry called"))

        with (
            mock.patch.object(
                failure_policy,
                "consumes_failure_budget",
                policy_mock,
            ),
            mock.patch.object(retry, "retry_with_lease_check", retry_mock),
        ):
            budgeted = await self.control.finalize(
                grant,
                grant_control.ClaimMode.PRIMARY,
                budgeted_plan,
            )
            budgeted_call = self.data_store.finalize_failure.await_args
            self.data_store.finalize_failure.reset_mock()
            non_budgeted = await self.control.finalize(
                grant,
                grant_control.ClaimMode.PRIMARY,
                non_budgeted_plan,
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
            budgeted_plan.status_reason,
        )
        self.assertEqual(budgeted_call.kwargs["actor_id"], _ACTOR_ID)
        self.assertIsInstance(
            non_budgeted_call.args[1],
            ingestion_lease_store.NonBudgetedFailure,
        )
        self.assertEqual(
            non_budgeted_call.args[1].retry_after,
            typing.cast(
                "failure_policy.RetryWithoutBudget",
                non_budgeted_plan.treatment,
            ).retry_after,
        )
        self.assertIs(
            budgeted.disposition,
            grant_control.FinalizeDisposition.APPLIED,
        )
        self.assertIs(
            non_budgeted.disposition,
            grant_control.FinalizeDisposition.APPLIED,
        )
        policy_mock.assert_not_called()
        retry_mock.assert_not_awaited()
        self.data_store.load_membership.assert_not_awaited()
        self.data_store.commit_child_mutations.assert_not_awaited()

    async def test_non_budgeted_quarantine_is_rejected(self) -> None:
        grant = _lease_grant()
        self.data_store.finalize_failure.return_value = (
            ingestion_lease_store.LeaseFailureResult(
                ingestion_lease_store.LeaseOperationDisposition.APPLIED,
                feed_store.FeedStatus.QUARANTINED,
            )
        )

        with self.assertRaises(grant_control.GrantControlIntegrityError):
            await self.control.finalize(
                grant,
                grant_control.ClaimMode.PRIMARY,
                _non_budgeted_plan(),
            )

    async def test_finalize_maps_loss_without_batch_release(
        self,
    ) -> None:
        grant = _lease_grant()
        dispositions = (
            ingestion_lease_store.LeaseOperationDisposition.MISSING,
            ingestion_lease_store.LeaseOperationDisposition.OWNER_MISMATCH,
            ingestion_lease_store.LeaseOperationDisposition.FENCE_MISMATCH,
            ingestion_lease_store.LeaseOperationDisposition.STATUS_INELIGIBLE,
        )

        for disposition in dispositions:
            with self.subTest(disposition=disposition.value):
                self.data_store.reset_mock()
                self.data_store.release.return_value = (
                    ingestion_lease_store.LeaseOperationResult(
                        disposition,
                    )
                )

                result = await self.control.finalize(
                    grant,
                    grant_control.ClaimMode.PRIMARY,
                    grant_control.NeutralRelease(),
                )

                self.assertIs(
                    result.disposition,
                    grant_control.FinalizeDisposition.LOST,
                )
                self.assertFalse(hasattr(self.data_store, "release_batch"))

    async def test_store_exception_propagates_without_retry(self) -> None:
        grant = _lease_grant()
        self.data_store.finalize_failure.side_effect = RuntimeError(
            "outcome unknown"
        )

        with self.assertRaisesRegex(RuntimeError, "outcome unknown"):
            await self.control.finalize(
                grant,
                grant_control.ClaimMode.PRIMARY,
                _budgeted_plan(),
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
            self.assertNotIn("consumes_failure_budget", source)
            self.assertNotIn("retry_with_lease_check", source)
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
