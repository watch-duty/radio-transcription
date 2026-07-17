"""Deterministic contracts for the exact-generation grant supervisor."""

from __future__ import annotations

import ast
import asyncio
import datetime
import inspect
import pathlib
import typing
import unittest
import uuid
from unittest import mock

from backend.pipeline.ingestion import (
    failure_policy,
    grant_control,
    grant_supervisor,
    sid_grant_control,
    worker_profiles,
)
from backend.pipeline.storage import feed_store, ingestion_lease_store

_OWNER_ID = uuid.UUID("11111111-2222-3333-4444-555555555555")
_OTHER_OWNER_ID = uuid.UUID("22222222-3333-4444-5555-666666666666")
_NOW = datetime.datetime(2026, 7, 11, 12, 0, tzinfo=datetime.UTC)
_SID_FAILURE_CASES = (
    (
        "provider-exhaustion",
        feed_store.FeedStatusReason.SYSTEM_SOURCE_PAYLOAD_INVALID,
        "invalid provider envelope",
    ),
    (
        "membership-invariant",
        feed_store.FeedStatusReason.SYSTEM_SOURCE_CONFIGURATION_INVALID,
        "bcfy_calls_sid_membership_invalid",
    ),
    (
        "logical-failure-threshold",
        feed_store.FeedStatusReason.SOURCE_RATE_LIMITED,
        "provider rate limit",
    ),
    (
        "promoted-page",
        feed_store.FeedStatusReason.SYSTEM_SOURCE_PAYLOAD_INVALID,
        "terminal item skip",
    ),
)


def _leased_feed(
    feed_id: uuid.UUID,
    *,
    fencing_token: int = 1,
) -> feed_store.LeasedFeed:
    return feed_store.LeasedFeed(
        id=feed_id,
        name=f"Feed {feed_id}",
        source_type=feed_store.SourceType.BCFY_CALLS,
        last_processed_filename=None,
        last_bookmark_time=None,
        fencing_token=fencing_token,
        failure_count=0,
        status_reason=None,
        source_feed_id="123-456",
        tags=None,
    )


def _sid_payload(
    mode: grant_control.ClaimMode = grant_control.ClaimMode.PRIMARY,
) -> sid_grant_control.SidClaimPayload:
    return sid_grant_control.SidClaimPayload(mode)


_FAILURE_BUDGET = failure_policy.ConsumeFailureBudget(5, 15, 600)


def _plan_failure(
    outcome: grant_control.RunFailed,
) -> failure_policy.FailurePersistencePlan:
    return failure_policy.plan_failure(
        outcome.status_reason,
        outcome.reason,
        budgeted=_FAILURE_BUDGET,
        non_budgeted=lambda: failure_policy.RetryWithoutBudget(_NOW),
    )


class _ControlledControl[GrantT, PayloadT]:
    def __init__(self) -> None:
        self.domain_id = grant_control.DomainId.FEED
        self.results: dict[
            grant_control.ClaimMode,
            tuple[grant_control.ClaimedGrant[GrantT, PayloadT], ...],
        ] = {
            grant_control.ClaimMode.PRIMARY: (),
            grant_control.ClaimMode.RECOVERY: (),
        }
        self.claim_calls: list[tuple[grant_control.ClaimMode, int]] = []
        self.claim_error: BaseException | None = None
        self.call_order: list[tuple[str, grant_control.ClaimMode]] | None = None
        self.call_label = ""
        self.blocked_modes: set[grant_control.ClaimMode] = set()
        self.claim_entered = {
            mode: asyncio.Event() for mode in grant_control.ClaimMode
        }
        self.release_claim = asyncio.Event()
        self.finalize_calls: list[
            tuple[GrantT, grant_control.TerminalDecision]
        ] = []
        self.finalize_payloads: list[PayloadT] = []
        self.heartbeat_calls: list[tuple[GrantT, ...]] = []
        self.heartbeat_results: (
            tuple[grant_control.GrantHeartbeat[GrantT], ...] | None
        ) = None
        self.heartbeat_error: Exception | None = None
        self.block_heartbeat = False
        self.heartbeat_entered = asyncio.Event()
        self.release_heartbeat = asyncio.Event()
        self.finalize_result: grant_control.FinalizeResult[GrantT] | None = None
        self.finalize_error: Exception | None = None
        self.block_finalize = False
        self.swallow_finalize_cancellation = False
        self.finalize_entered = asyncio.Event()
        self.two_finalizers_entered = asyncio.Event()
        self.release_finalize = asyncio.Event()
        self.finalize_active = 0
        self.max_finalize_active = 0

    async def claim(
        self,
        mode: grant_control.ClaimMode,
        owner_worker_id: uuid.UUID,
        limit: int,
    ) -> tuple[grant_control.ClaimedGrant[GrantT, PayloadT], ...]:
        del owner_worker_id
        self.claim_calls.append((mode, limit))
        if self.call_order is not None:
            self.call_order.append((self.call_label, mode))
        if mode in self.blocked_modes:
            self.claim_entered[mode].set()
            await self.release_claim.wait()
        if self.claim_error is not None:
            raise self.claim_error
        return self.results[mode]

    async def heartbeat(
        self,
        grants: typing.Sequence[GrantT],
    ) -> tuple[grant_control.GrantHeartbeat[GrantT], ...]:
        grants = tuple(grants)
        self.heartbeat_calls.append(grants)
        self.heartbeat_entered.set()
        if self.block_heartbeat:
            await self.release_heartbeat.wait()
        if self.heartbeat_error is not None:
            raise self.heartbeat_error
        if self.heartbeat_results is not None:
            return self.heartbeat_results
        return tuple(
            grant_control.GrantHeartbeat(
                grant,
                grant_control.HeartbeatDisposition.RETAINED,
            )
            for grant in grants
        )

    async def finalize(
        self,
        grant: GrantT,
        payload: PayloadT,
        terminal: grant_control.TerminalDecision,
    ) -> grant_control.FinalizeResult[GrantT]:
        self.finalize_calls.append((grant, terminal))
        self.finalize_payloads.append(payload)
        self.finalize_entered.set()
        self.finalize_active += 1
        self.max_finalize_active = max(
            self.max_finalize_active,
            self.finalize_active,
        )
        if self.finalize_active >= 2:
            self.two_finalizers_entered.set()
        try:
            if self.block_finalize:
                try:
                    await self.release_finalize.wait()
                except asyncio.CancelledError:
                    if not self.swallow_finalize_cancellation:
                        raise
                    current = asyncio.current_task()
                    assert current is not None
                    current.uncancel()
                    await self.release_finalize.wait()
            if self.finalize_error is not None:
                raise self.finalize_error
            if self.finalize_result is not None:
                return self.finalize_result
            return grant_control.FinalizeResult(
                grant,
                grant_control.FinalizeDisposition.APPLIED,
            )
        finally:
            self.finalize_active -= 1


class _ControlledRunner[GrantT, PayloadT]:
    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.finish = asyncio.Event()
        self.calls: list[tuple[GrantT, PayloadT, grant_control.RunContext]] = []
        self.outcome: grant_control.RunOutcome = grant_control.RunCompleted()
        self.wait_for_signal: str | None = None
        self.signal_observed = asyncio.Event()
        self.child_started = asyncio.Event()
        self.release_child = asyncio.Event()
        self.block_child_cleanup = False
        self.swallow_cancellation = False

    async def run(
        self,
        grant: GrantT,
        payload: PayloadT,
        context: grant_control.RunContext,
    ) -> grant_control.RunOutcome:
        self.calls.append((grant, payload, context))
        self.started.set()
        if self.wait_for_signal == "stop":
            await context.stop_requested.wait()
            self.signal_observed.set()
        elif self.wait_for_signal == "loss":
            await context.grant_lost.wait()
            self.signal_observed.set()
        else:
            await self.finish.wait()
        if self.block_child_cleanup:
            self.child_started.set()
            try:
                await self.release_child.wait()
            except asyncio.CancelledError:
                if not self.swallow_cancellation:
                    raise
                current = asyncio.current_task()
                assert current is not None
                current.uncancel()
                await self.release_child.wait()
        return self.outcome


def _profile(
    *,
    process_cap: int = 4,
    feed_cap: int = 2,
    sid_cap: int = 2,
    feed_budget: int = 2,
    sid_budget: int = 2,
    domains: tuple[grant_control.DomainId, ...] = (
        grant_control.DomainId.FEED,
        grant_control.DomainId.SID,
    ),
) -> worker_profiles.WorkerProfile:
    allocations = []
    for domain_id in domains:
        if domain_id is grant_control.DomainId.FEED:
            cap = feed_cap
            budget = feed_budget
        else:
            cap = sid_cap
            budget = sid_budget
        allocations.append(
            worker_profiles.DomainAllocation(
                domain_id=domain_id,
                owned_cap=cap,
                claims_per_cycle=budget,
                claims_enabled=True,
            )
        )
    return worker_profiles.validate_worker_profile(
        worker_profiles.WorkerProfile(
            name="test",
            version=1,
            resource_class=worker_profiles.ResourceClass.SHARED,
            process_owned_cap=process_cap,
            allocations=tuple(allocations),
        )
    )


def _feed_registration(
    control: _ControlledControl[
        feed_store.FeedGrant,
        feed_store.LeasedFeed,
    ],
    runner: _ControlledRunner[
        feed_store.FeedGrant,
        feed_store.LeasedFeed,
    ],
) -> grant_supervisor.RegisteredDomain[
    feed_store.FeedGrant,
    feed_store.LeasedFeed,
]:
    control.domain_id = grant_control.DomainId.FEED
    return grant_supervisor.RegisteredDomain(
        domain_id=grant_control.DomainId.FEED,
        control=control,
        runner=runner,
    )


def _sid_registration(
    control: _ControlledControl[
        ingestion_lease_store.LeaseGrant,
        sid_grant_control.SidClaimPayload,
    ],
    runner: _ControlledRunner[
        ingestion_lease_store.LeaseGrant,
        sid_grant_control.SidClaimPayload,
    ],
) -> grant_supervisor.RegisteredDomain[
    ingestion_lease_store.LeaseGrant,
    sid_grant_control.SidClaimPayload,
]:
    control.domain_id = grant_control.DomainId.SID
    return grant_supervisor.RegisteredDomain(
        domain_id=grant_control.DomainId.SID,
        control=control,
        runner=runner,
    )


def _claim[GrantT, PayloadT](
    grant: GrantT,
    payload: PayloadT,
) -> grant_control.ClaimedGrant[GrantT, PayloadT]:
    return grant_control.ClaimedGrant(
        grant=grant,
        payload=payload,
    )


async def _settle_terminal_tasks(
    supervisor: grant_supervisor.GrantSupervisor,
) -> None:
    while supervisor._terminal_tasks:
        tasks = tuple(supervisor._terminal_tasks)
        await asyncio.gather(*tasks, return_exceptions=True)
        supervisor._terminal_tasks.difference_update(tasks)


class TestGrantSupervisor(unittest.IsolatedAsyncioTestCase):
    """Controlled reservation, admission, and exact-generation proofs."""

    def test_registration_rejects_cross_wired_control_domain(self) -> None:
        control: _ControlledControl[
            feed_store.FeedGrant,
            feed_store.LeasedFeed,
        ] = _ControlledControl()
        runner: _ControlledRunner[
            feed_store.FeedGrant,
            feed_store.LeasedFeed,
        ] = _ControlledRunner()

        with self.assertRaisesRegex(
            ValueError,
            "does not match control domain",
        ):
            grant_supervisor.RegisteredDomain(
                domain_id=grant_control.DomainId.SID,
                control=control,
                runner=runner,
            )

    def test_public_annotations_resolve_at_runtime(self) -> None:
        hints = typing.get_type_hints(
            grant_supervisor.GrantSupervisor.admit_cycle
        )

        self.assertIs(hints["owner_worker_id"], uuid.UUID)

    def _mixed(
        self,
        profile: worker_profiles.WorkerProfile,
        *,
        failure_planner: typing.Callable[
            [grant_control.RunFailed],
            failure_policy.FailurePersistencePlan,
        ] = _plan_failure,
        failure_plan_observer: typing.Callable[
            [grant_control.DomainId, failure_policy.FailurePersistencePlan],
            None,
        ]
        | None = None,
    ) -> tuple[
        grant_supervisor.GrantSupervisor,
        _ControlledControl[feed_store.FeedGrant, feed_store.LeasedFeed],
        _ControlledRunner[feed_store.FeedGrant, feed_store.LeasedFeed],
        _ControlledControl[
            ingestion_lease_store.LeaseGrant,
            sid_grant_control.SidClaimPayload,
        ],
        _ControlledRunner[
            ingestion_lease_store.LeaseGrant,
            sid_grant_control.SidClaimPayload,
        ],
    ]:
        feed_control = _ControlledControl[
            feed_store.FeedGrant,
            feed_store.LeasedFeed,
        ]()
        feed_runner = _ControlledRunner[
            feed_store.FeedGrant,
            feed_store.LeasedFeed,
        ]()
        sid_control = _ControlledControl[
            ingestion_lease_store.LeaseGrant,
            sid_grant_control.SidClaimPayload,
        ]()
        sid_runner = _ControlledRunner[
            ingestion_lease_store.LeaseGrant,
            sid_grant_control.SidClaimPayload,
        ]()
        allocations = {
            allocation.domain_id: allocation
            for allocation in profile.allocations
        }
        registrations: list[object] = []
        if grant_control.DomainId.FEED in allocations:
            registrations.append(
                _feed_registration(
                    feed_control,
                    feed_runner,
                )
            )
        if grant_control.DomainId.SID in allocations:
            registrations.append(
                _sid_registration(
                    sid_control,
                    sid_runner,
                )
            )
        supervisor = grant_supervisor.GrantSupervisor(
            profile,
            registrations,
            finalize_concurrency=2,
            failure_planner=failure_planner,
            failure_plan_observer=failure_plan_observer,
        )
        return (
            supervisor,
            feed_control,
            feed_runner,
            sid_control,
            sid_runner,
        )

    async def test_failure_is_planned_once_after_exact_revalidation(
        self,
    ) -> None:
        profile = _profile(
            process_cap=1,
            feed_cap=1,
            feed_budget=1,
            domains=(grant_control.DomainId.FEED,),
        )
        planner = mock.Mock(side_effect=_plan_failure)
        observer = mock.Mock()
        (
            supervisor,
            feed_control,
            feed_runner,
            _sid_control,
            _sid_runner,
        ) = self._mixed(
            profile,
            failure_planner=planner,
            failure_plan_observer=observer,
        )
        feed_id = uuid.UUID(int=9001)
        grant = feed_store.FeedGrant(feed_id, _OWNER_ID, 1)
        outcome = grant_control.RunFailed(
            feed_store.FeedStatusReason.SYSTEM_CONFIGURATION_INVALID,
            "invalid configuration",
        )
        feed_control.results[grant_control.ClaimMode.PRIMARY] = (
            _claim(grant, _leased_feed(feed_id)),
        )
        feed_runner.outcome = outcome

        await supervisor.admit_cycle(_OWNER_ID)
        await asyncio.wait_for(feed_runner.started.wait(), timeout=1)
        feed_runner.finish.set()
        managed = next(iter(supervisor._registry.values()))
        assert managed.root_task is not None
        await asyncio.wait_for(managed.root_task, timeout=1)
        await _settle_terminal_tasks(supervisor)

        planner.assert_called_once_with(outcome)
        self.assertEqual(len(feed_control.finalize_calls), 1)
        plan = feed_control.finalize_calls[0][1]
        observer.assert_called_once_with(grant_control.DomainId.FEED, plan)
        self.assertEqual(feed_control.finalize_calls, [(grant, plan)])

    async def test_stale_failure_never_materializes_a_plan(self) -> None:
        profile = _profile(
            process_cap=1,
            feed_cap=1,
            feed_budget=1,
            domains=(grant_control.DomainId.FEED,),
        )
        planner = mock.Mock(side_effect=_plan_failure)
        (
            supervisor,
            feed_control,
            feed_runner,
            _sid_control,
            _sid_runner,
        ) = self._mixed(profile, failure_planner=planner)
        feed_id = uuid.UUID(int=9002)
        grant = feed_store.FeedGrant(feed_id, _OWNER_ID, 1)
        feed_control.results[grant_control.ClaimMode.PRIMARY] = (
            _claim(grant, _leased_feed(feed_id)),
        )
        feed_runner.outcome = grant_control.RunFailed(
            feed_store.FeedStatusReason.SOURCE_UNREACHABLE,
            "provider unavailable",
        )

        await supervisor._finalize_semaphore.acquire()
        await supervisor._finalize_semaphore.acquire()
        try:
            await supervisor.admit_cycle(_OWNER_ID)
            await asyncio.wait_for(feed_runner.started.wait(), timeout=1)
            managed = next(iter(supervisor._registry.values()))
            feed_runner.finish.set()
            assert managed.root_task is not None
            await asyncio.wait_for(managed.root_task, timeout=1)
            self.assertTrue(supervisor._discard_current(managed))
            planner.assert_not_called()
        finally:
            supervisor._finalize_semaphore.release()
            supervisor._finalize_semaphore.release()
        await _settle_terminal_tasks(supervisor)

        planner.assert_not_called()
        self.assertEqual(feed_control.finalize_calls, [])

    async def _close(
        self,
        supervisor: grant_supervisor.GrantSupervisor,
        *runners: _ControlledRunner[object, object],
    ) -> None:
        for runner in runners:
            runner.finish.set()
            runner.release_child.set()
        for managed in tuple(supervisor._registry.values()):
            managed.stop_requested.set()
            managed.grant_lost.set()
            task = managed.root_task
            if task is not None:
                await asyncio.wait_for(task, timeout=1)
        await _settle_terminal_tasks(supervisor)

    async def test_feed_and_sid_share_admission_and_generation_contract(
        self,
    ) -> None:
        profile = _profile()
        (
            supervisor,
            feed_control,
            feed_runner,
            sid_control,
            sid_runner,
        ) = self._mixed(profile)
        feed_id = uuid.UUID(int=1)
        feed_grant = feed_store.FeedGrant(feed_id, _OWNER_ID, 7)
        sid_grant = ingestion_lease_store.LeaseGrant(
            feed_store.SourceType.BCFY_CALLS,
            "123",
            _OWNER_ID,
            8,
        )
        feed_control.results[grant_control.ClaimMode.PRIMARY] = (
            _claim(feed_grant, _leased_feed(feed_id, fencing_token=7)),
        )
        sid_control.results[grant_control.ClaimMode.PRIMARY] = (
            _claim(sid_grant, _sid_payload()),
        )

        try:
            await supervisor.admit_cycle(_OWNER_ID)
            await asyncio.wait_for(feed_runner.started.wait(), timeout=1)
            await asyncio.wait_for(sid_runner.started.wait(), timeout=1)

            self.assertEqual(len(supervisor._registry), 2)
            self.assertEqual(supervisor._process_owned, 2)
            self.assertEqual(supervisor._process_reserved, 0)
            slots = set(supervisor._registry)
            self.assertIn(
                grant_supervisor._UnitSlot(
                    grant_control.DomainId.FEED,
                    feed_id,
                ),
                slots,
            )
            self.assertIn(
                grant_supervisor._UnitSlot(
                    grant_control.DomainId.SID,
                    (feed_store.SourceType.BCFY_CALLS, "123"),
                ),
                slots,
            )
            self.assertIs(feed_runner.calls[0][0], feed_grant)
            self.assertIs(sid_runner.calls[0][0], sid_grant)
        finally:
            await self._close(
                supervisor,
                typing.cast("_ControlledRunner[object, object]", feed_runner),
                typing.cast("_ControlledRunner[object, object]", sid_runner),
            )

    async def test_sid_opaque_payload_identity_survives_run_and_finalize(
        self,
    ) -> None:
        profile = _profile(
            process_cap=1,
            sid_cap=1,
            sid_budget=1,
            domains=(grant_control.DomainId.SID,),
        )
        (
            supervisor,
            _feed_control,
            _feed_runner,
            sid_control,
            sid_runner,
        ) = self._mixed(profile)
        grant = ingestion_lease_store.LeaseGrant(
            feed_store.SourceType.BCFY_CALLS,
            "opaque-payload",
            _OWNER_ID,
            1,
        )
        payload = _sid_payload(grant_control.ClaimMode.RECOVERY)
        sid_control.results[grant_control.ClaimMode.PRIMARY] = (
            _claim(grant, payload),
        )

        await supervisor.admit_cycle(_OWNER_ID)
        await asyncio.wait_for(sid_runner.started.wait(), timeout=1)
        self.assertIs(sid_runner.calls[0][1], payload)
        sid_runner.finish.set()
        managed = next(iter(supervisor._registry.values()))
        assert managed.root_task is not None
        await managed.root_task
        await _settle_terminal_tasks(supervisor)

        self.assertEqual(len(sid_control.finalize_payloads), 1)
        self.assertIs(sid_control.finalize_payloads[0], payload)
        self.assertIs(
            payload.claim_mode,
            grant_control.ClaimMode.RECOVERY,
        )

    async def test_all_sid_failure_sources_have_one_generic_parent_writer(
        self,
    ) -> None:
        for index, (case_id, status_reason, reason) in enumerate(
            _SID_FAILURE_CASES,
            start=1,
        ):
            with self.subTest(case=case_id):
                profile = _profile(
                    process_cap=1,
                    sid_cap=1,
                    sid_budget=1,
                    domains=(grant_control.DomainId.SID,),
                )
                (
                    supervisor,
                    feed_control,
                    _feed_runner,
                    sid_control,
                    sid_runner,
                ) = self._mixed(profile)
                grant = ingestion_lease_store.LeaseGrant(
                    feed_store.SourceType.BCFY_CALLS,
                    f"failure-{index}",
                    _OWNER_ID,
                    index,
                )
                payload = _sid_payload()
                outcome = grant_control.RunFailed(status_reason, reason)
                sid_control.results[grant_control.ClaimMode.PRIMARY] = (
                    _claim(grant, payload),
                )
                sid_runner.outcome = outcome

                await supervisor.admit_cycle(_OWNER_ID)
                await asyncio.wait_for(sid_runner.started.wait(), timeout=1)
                managed = next(iter(supervisor._registry.values()))
                root_task = managed.root_task
                self.assertIsNotNone(root_task)
                assert root_task is not None
                sid_runner.finish.set()
                await asyncio.wait_for(root_task, timeout=1)
                await _settle_terminal_tasks(supervisor)

                self.assertEqual(
                    sid_control.finalize_calls,
                    [(grant, _plan_failure(outcome))],
                )
                self.assertEqual(sid_control.finalize_payloads, [payload])
                self.assertEqual(feed_control.finalize_calls, [])
                self.assertEqual(supervisor._registry, {})
                self.assertEqual(supervisor._process_owned, 0)

    async def test_reservation_precedes_both_control_awaits_and_bounds_capacity(
        self,
    ) -> None:
        profile = _profile(
            process_cap=3,
            feed_cap=2,
            sid_cap=1,
            sid_budget=1,
        )
        (
            supervisor,
            feed_control,
            feed_runner,
            sid_control,
            sid_runner,
        ) = self._mixed(profile)
        feed_control.blocked_modes.add(grant_control.ClaimMode.PRIMARY)
        sid_control.blocked_modes.add(grant_control.ClaimMode.PRIMARY)
        admission = asyncio.create_task(supervisor.admit_cycle(_OWNER_ID))

        try:
            await asyncio.wait_for(
                feed_control.claim_entered[
                    grant_control.ClaimMode.PRIMARY
                ].wait(),
                timeout=1,
            )
            await asyncio.wait_for(
                sid_control.claim_entered[
                    grant_control.ClaimMode.PRIMARY
                ].wait(),
                timeout=1,
            )
            self.assertEqual(supervisor._process_reserved, 3)
            self.assertEqual(
                supervisor._reserved_by_domain[grant_control.DomainId.FEED],
                2,
            )
            self.assertEqual(
                supervisor._reserved_by_domain[grant_control.DomainId.SID],
                1,
            )
            self.assertLessEqual(
                supervisor._process_owned + supervisor._process_reserved,
                profile.process_owned_cap,
            )
            for allocation in profile.allocations:
                self.assertLessEqual(
                    supervisor._owned_by_domain[allocation.domain_id]
                    + supervisor._reserved_by_domain[allocation.domain_id],
                    allocation.owned_cap,
                )
            feed_control.release_claim.set()
            sid_control.release_claim.set()
            await admission
            self.assertEqual(supervisor._process_reserved, 0)
            self.assertEqual(sum(supervisor._reserved_by_domain.values()), 0)
        finally:
            if not admission.done():
                admission.cancel()
                await admission
            await self._close(
                supervisor,
                typing.cast("_ControlledRunner[object, object]", feed_runner),
                typing.cast("_ControlledRunner[object, object]", sid_runner),
            )

    async def test_primary_precedes_recovery_and_sid_total_budget_is_two(
        self,
    ) -> None:
        profile = _profile(process_cap=4, feed_cap=2, sid_cap=2)
        (
            supervisor,
            feed_control,
            feed_runner,
            sid_control,
            sid_runner,
        ) = self._mixed(profile)
        order: list[tuple[str, grant_control.ClaimMode]] = []
        feed_control.call_order = order
        feed_control.call_label = "feed"
        sid_control.call_order = order
        sid_control.call_label = "sid"
        feed_id = uuid.UUID(int=2)
        feed_control.results[grant_control.ClaimMode.PRIMARY] = (
            _claim(
                feed_store.FeedGrant(feed_id, _OWNER_ID, 1),
                _leased_feed(feed_id),
            ),
        )
        sid_control.results[grant_control.ClaimMode.PRIMARY] = (
            _claim(
                ingestion_lease_store.LeaseGrant(
                    feed_store.SourceType.BCFY_CALLS,
                    "456",
                    _OWNER_ID,
                    1,
                ),
                _sid_payload(),
            ),
        )
        try:
            await supervisor.admit_cycle(_OWNER_ID)
            feed_control.results[grant_control.ClaimMode.PRIMARY] = ()
            sid_control.results[grant_control.ClaimMode.PRIMARY] = ()
            feed_control.claim_calls.clear()
            sid_control.claim_calls.clear()
            order.clear()

            await supervisor.admit_cycle(_OWNER_ID)

            sid_total_ask = sum(
                limit for _mode, limit in sid_control.claim_calls
            )
            self.assertLessEqual(sid_total_ask, 2)
            self.assertEqual(
                [mode for _label, mode in order],
                [
                    grant_control.ClaimMode.PRIMARY,
                    grant_control.ClaimMode.PRIMARY,
                    grant_control.ClaimMode.RECOVERY,
                    grant_control.ClaimMode.RECOVERY,
                ],
            )
        finally:
            await self._close(
                supervisor,
                typing.cast("_ControlledRunner[object, object]", feed_runner),
                typing.cast("_ControlledRunner[object, object]", sid_runner),
            )

    async def test_primary_underfill_restores_budget_for_recovery(self) -> None:
        profile = _profile(
            process_cap=3,
            feed_cap=3,
            feed_budget=3,
            domains=(grant_control.DomainId.FEED,),
        )
        (
            supervisor,
            feed_control,
            feed_runner,
            _sid_control,
            _sid_runner,
        ) = self._mixed(profile)
        primary_id = uuid.UUID(int=40)
        recovery_id = uuid.UUID(int=41)
        feed_control.results[grant_control.ClaimMode.PRIMARY] = (
            _claim(
                feed_store.FeedGrant(primary_id, _OWNER_ID, 1),
                _leased_feed(primary_id),
            ),
        )
        feed_control.results[grant_control.ClaimMode.RECOVERY] = (
            _claim(
                feed_store.FeedGrant(recovery_id, _OWNER_ID, 1),
                _leased_feed(recovery_id),
            ),
        )

        try:
            await supervisor.admit_cycle(_OWNER_ID)

            self.assertEqual(
                feed_control.claim_calls,
                [
                    (grant_control.ClaimMode.PRIMARY, 3),
                    (grant_control.ClaimMode.RECOVERY, 2),
                ],
            )
            self.assertEqual(supervisor._process_owned, 2)
            self.assertEqual(supervisor._process_reserved, 0)
        finally:
            await self._close(
                supervisor,
                typing.cast("_ControlledRunner[object, object]", feed_runner),
            )

    async def test_shutdown_reconciles_claim_returning_after_admission_stop(
        self,
    ) -> None:
        profile = _profile(
            process_cap=1,
            feed_cap=1,
            feed_budget=1,
            domains=(grant_control.DomainId.FEED,),
        )
        (
            supervisor,
            feed_control,
            feed_runner,
            _sid_control,
            _sid_runner,
        ) = self._mixed(profile)
        feed_id = uuid.UUID(int=4)
        feed_control.results[grant_control.ClaimMode.PRIMARY] = (
            _claim(
                feed_store.FeedGrant(feed_id, _OWNER_ID, 1),
                _leased_feed(feed_id),
            ),
        )
        feed_control.blocked_modes.add(grant_control.ClaimMode.PRIMARY)
        admission = asyncio.create_task(supervisor.admit_cycle(_OWNER_ID))
        await asyncio.wait_for(
            feed_control.claim_entered[grant_control.ClaimMode.PRIMARY].wait(),
            timeout=1,
        )

        async def stop_heartbeat() -> None:
            return None

        claim_wait_entered = asyncio.Event()
        wait_for_claims = supervisor._claim_shutdown_blocker

        async def observed_claim_wait(
            wait_timeout_sec: float,
        ) -> bool:
            claim_wait_entered.set()
            return await wait_for_claims(wait_timeout_sec)

        with mock.patch.object(
            supervisor,
            "_claim_shutdown_blocker",
            side_effect=observed_claim_wait,
        ):
            shutdown = asyncio.create_task(
                supervisor.shutdown(
                    cooperative_grace_sec=0,
                    external_stop_deadline_sec=1,
                    stop_heartbeat_supervision=stop_heartbeat,
                )
            )
            await asyncio.wait_for(claim_wait_entered.wait(), timeout=1)
            self.assertFalse(shutdown.done())
            feed_control.release_claim.set()
            await admission
            result = await shutdown

        self.assertIsNone(result)
        self.assertEqual(supervisor._registry, {})
        self.assertEqual(supervisor._process_reserved, 0)
        self.assertEqual(feed_runner.calls, [])
        self.assertEqual(len(feed_control.finalize_calls), 1)
        self.assertEqual(feed_control.finalize_calls[0][0].feed_id, feed_id)
        self.assertEqual(
            feed_control.finalize_calls[0][1],
            grant_control.NeutralRelease(grant_control.TerminalCause.SHUTDOWN),
        )

    async def test_shutdown_rejects_unsettled_claim_mutation(self) -> None:
        profile = _profile(
            process_cap=1,
            feed_cap=1,
            feed_budget=1,
            domains=(grant_control.DomainId.FEED,),
        )
        (
            supervisor,
            feed_control,
            _feed_runner,
            _sid_control,
            _sid_runner,
        ) = self._mixed(profile)
        feed_control.blocked_modes.add(grant_control.ClaimMode.PRIMARY)
        admission = asyncio.create_task(supervisor.admit_cycle(_OWNER_ID))
        await asyncio.wait_for(
            feed_control.claim_entered[grant_control.ClaimMode.PRIMARY].wait(),
            timeout=1,
        )
        heartbeat_stopped = asyncio.Event()

        async def stop_heartbeat() -> None:
            heartbeat_stopped.set()

        with self.assertRaises(grant_supervisor.SupervisorNotDrainedError):
            await supervisor.shutdown(
                cooperative_grace_sec=0,
                external_stop_deadline_sec=0,
                stop_heartbeat_supervision=stop_heartbeat,
            )

        self.assertFalse(heartbeat_stopped.is_set())
        feed_control.release_claim.set()
        await admission
        self.assertEqual(supervisor._process_reserved, 0)

    async def test_claim_exception_is_unknown_and_disables_admission(
        self,
    ) -> None:
        profile = _profile(
            process_cap=1,
            feed_cap=1,
            feed_budget=1,
            domains=(grant_control.DomainId.FEED,),
        )
        (
            supervisor,
            feed_control,
            feed_runner,
            _sid_control,
            _sid_runner,
        ) = self._mixed(profile)
        feed_control.claim_error = RuntimeError("commit outcome unknown")

        await supervisor.admit_cycle(_OWNER_ID)

        self.assertFalse(supervisor.admission_enabled)
        self.assertTrue(supervisor.integrity_failure_event.is_set())
        failure = supervisor.integrity_failure
        self.assertIsInstance(
            failure,
            grant_control.GrantControlIntegrityError,
        )
        assert failure is not None
        self.assertIs(
            failure.__cause__,
            feed_control.claim_error,
        )
        self.assertEqual(supervisor._process_reserved, 0)
        self.assertEqual(supervisor._registry, {})
        self.assertEqual(feed_runner.calls, [])

        await supervisor.admit_cycle(_OWNER_ID)

        self.assertEqual(
            feed_control.claim_calls,
            [(grant_control.ClaimMode.PRIMARY, 1)],
        )

    async def test_constrained_empty_cycles_rotate_first_domain(self) -> None:
        profile = _profile(
            process_cap=2,
            feed_cap=1,
            sid_cap=1,
            feed_budget=1,
            sid_budget=1,
        )
        (
            supervisor,
            feed_control,
            feed_runner,
            sid_control,
            sid_runner,
        ) = self._mixed(profile)
        order: list[tuple[str, grant_control.ClaimMode]] = []
        feed_control.call_order = order
        feed_control.call_label = "feed"
        sid_control.call_order = order
        sid_control.call_label = "sid"

        await supervisor.admit_cycle(_OWNER_ID)
        await supervisor.admit_cycle(_OWNER_ID)

        self.assertEqual(
            feed_control.claim_calls,
            [
                (grant_control.ClaimMode.PRIMARY, 1),
                (grant_control.ClaimMode.RECOVERY, 1),
                (grant_control.ClaimMode.PRIMARY, 1),
                (grant_control.ClaimMode.RECOVERY, 1),
            ],
        )
        self.assertEqual(
            sid_control.claim_calls,
            [
                (grant_control.ClaimMode.PRIMARY, 1),
                (grant_control.ClaimMode.RECOVERY, 1),
                (grant_control.ClaimMode.PRIMARY, 1),
                (grant_control.ClaimMode.RECOVERY, 1),
            ],
        )
        primary_order = [
            label
            for label, mode in order
            if mode is grant_control.ClaimMode.PRIMARY
        ]
        self.assertEqual(primary_order, ["feed", "sid", "sid", "feed"])
        await self._close(
            supervisor,
            typing.cast("_ControlledRunner[object, object]", feed_runner),
            typing.cast("_ControlledRunner[object, object]", sid_runner),
        )

    async def test_overreturn_wrong_owner_and_duplicate_stop_admission(
        self,
    ) -> None:
        cases = ("overreturn", "wrong-owner", "duplicate")
        for case in cases:
            with self.subTest(case=case):
                profile = _profile(
                    process_cap=2,
                    feed_cap=2,
                    feed_budget=1 if case == "overreturn" else 2,
                    domains=(grant_control.DomainId.FEED,),
                )
                (
                    supervisor,
                    feed_control,
                    feed_runner,
                    _sid_control,
                    _sid_runner,
                ) = self._mixed(profile)
                first_id = uuid.UUID(int=10)
                first = _claim(
                    feed_store.FeedGrant(
                        first_id,
                        _OTHER_OWNER_ID if case == "wrong-owner" else _OWNER_ID,
                        1,
                    ),
                    _leased_feed(first_id),
                )
                if case == "overreturn":
                    second_id = uuid.UUID(int=11)
                    claims = (
                        first,
                        _claim(
                            feed_store.FeedGrant(
                                second_id,
                                _OWNER_ID,
                                1,
                            ),
                            _leased_feed(second_id),
                        ),
                    )
                elif case == "duplicate":
                    claims = (
                        first,
                        _claim(
                            feed_store.FeedGrant(
                                first_id,
                                _OWNER_ID,
                                2,
                            ),
                            _leased_feed(first_id, fencing_token=2),
                        ),
                    )
                else:
                    claims = (first,)
                feed_control.results[grant_control.ClaimMode.PRIMARY] = claims

                await supervisor.admit_cycle(_OWNER_ID)

                self.assertFalse(supervisor.admission_enabled)
                self.assertEqual(supervisor._registry, {})
                self.assertEqual(feed_runner.calls, [])
                self.assertEqual(supervisor._process_reserved, 0)

    async def test_old_generation_callback_cannot_mutate_successor(
        self,
    ) -> None:
        profile = _profile(
            process_cap=1,
            feed_cap=1,
            feed_budget=1,
            domains=(grant_control.DomainId.FEED,),
        )
        (
            supervisor,
            feed_control,
            feed_runner,
            _sid_control,
            _sid_runner,
        ) = self._mixed(profile)
        feed_id = uuid.UUID(int=20)
        old_grant = feed_store.FeedGrant(feed_id, _OWNER_ID, 1)
        feed_control.results[grant_control.ClaimMode.PRIMARY] = (
            _claim(old_grant, _leased_feed(feed_id)),
        )
        await supervisor.admit_cycle(_OWNER_ID)
        old_slot = next(iter(supervisor._registry))
        old_managed = supervisor._registry[old_slot]
        self.assertTrue(
            supervisor._reserve_terminal(
                old_managed,
                grant_supervisor._ConfirmedLoss(),
            )
        )
        feed_runner.finish.set()
        await asyncio.wait_for(old_managed.runner_closed.wait(), timeout=1)
        old_task = old_managed.root_task
        self.assertIsNotNone(old_task)
        assert old_task is not None
        await old_task
        self.assertNotIn(old_slot, supervisor._registry)

        successor = feed_store.FeedGrant(feed_id, _OWNER_ID, 2)
        feed_control.results[grant_control.ClaimMode.PRIMARY] = (
            _claim(successor, _leased_feed(feed_id, fencing_token=2)),
        )
        feed_runner.finish.clear()
        await supervisor.admit_cycle(_OWNER_ID)
        successor_slot = next(iter(supervisor._registry))

        supervisor._root_done(old_managed, old_task)

        self.assertIn(successor_slot, supervisor._registry)
        self.assertEqual(
            supervisor._registry[successor_slot].grant.fencing_token,
            2,
        )
        self.assertEqual(supervisor._process_owned, 1)
        self.assertEqual(feed_control.finalize_calls, [])
        await self._close(
            supervisor,
            typing.cast("_ControlledRunner[object, object]", feed_runner),
        )

    async def test_heartbeat_dispatches_before_io_and_rejects_bad_correlation(  # noqa: PLR0915
        self,
    ) -> None:
        cases = (
            "missing",
            "extra",
            "scrambled",
            "duplicate",
            "malformed",
            "unavailable",
            "exception",
        )
        for case in cases:
            with self.subTest(case=case):
                profile = _profile(
                    process_cap=2,
                    feed_cap=2,
                    feed_budget=2,
                    domains=(grant_control.DomainId.FEED,),
                )
                (
                    supervisor,
                    feed_control,
                    feed_runner,
                    _sid_control,
                    _sid_runner,
                ) = self._mixed(profile)
                first_id = uuid.UUID(int=100)
                second_id = uuid.UUID(int=101)
                first = feed_store.FeedGrant(first_id, _OWNER_ID, 1)
                second = feed_store.FeedGrant(second_id, _OWNER_ID, 1)
                feed_control.results[grant_control.ClaimMode.PRIMARY] = (
                    _claim(first, _leased_feed(first_id)),
                    _claim(second, _leased_feed(second_id)),
                )
                await supervisor.admit_cycle(_OWNER_ID)
                first_result = grant_control.GrantHeartbeat(
                    first,
                    grant_control.HeartbeatDisposition.RETAINED,
                )
                second_result = grant_control.GrantHeartbeat(
                    second,
                    grant_control.HeartbeatDisposition.RETAINED,
                )
                if case == "missing":
                    feed_control.heartbeat_results = (first_result,)
                elif case == "extra":
                    feed_control.heartbeat_results = (
                        first_result,
                        second_result,
                        first_result,
                    )
                elif case == "scrambled":
                    feed_control.heartbeat_results = (
                        second_result,
                        first_result,
                    )
                elif case == "duplicate":
                    feed_control.heartbeat_results = (
                        first_result,
                        first_result,
                    )
                elif case == "malformed":
                    feed_control.heartbeat_results = typing.cast(
                        "tuple[grant_control.GrantHeartbeat[feed_store.FeedGrant], ...]",
                        (object(), object()),
                    )
                elif case == "unavailable":
                    feed_control.heartbeat_results = (
                        grant_control.GrantHeartbeat(
                            first,
                            grant_control.HeartbeatDisposition.UNAVAILABLE,
                        ),
                        grant_control.GrantHeartbeat(
                            second,
                            grant_control.HeartbeatDisposition.UNAVAILABLE,
                        ),
                    )
                else:
                    feed_control.heartbeat_error = RuntimeError(
                        "control unavailable"
                    )
                feed_control.block_heartbeat = True
                dispatched = asyncio.Event()
                cycle = asyncio.create_task(
                    supervisor.heartbeat_cycle(dispatched.set)
                )

                try:
                    await asyncio.wait_for(
                        feed_control.heartbeat_entered.wait(),
                        timeout=1,
                    )
                    self.assertTrue(dispatched.is_set())
                    self.assertTrue(
                        all(
                            managed.terminal_state
                            is grant_supervisor.TerminalState.OPEN
                            for managed in supervisor._registry.values()
                        )
                    )
                    feed_control.release_heartbeat.set()
                    await cycle

                    self.assertFalse(supervisor.admission_enabled)
                    self.assertTrue(supervisor.integrity_failure_event.is_set())
                    self.assertEqual(len(supervisor._registry), 2)
                    self.assertTrue(
                        all(
                            managed.terminal_state
                            is grant_supervisor.TerminalState.ABANDONED
                            for managed in supervisor._registry.values()
                        )
                    )
                    self.assertTrue(
                        all(
                            managed.stop_requested.is_set()
                            and managed.grant_lost.is_set()
                            for managed in supervisor._registry.values()
                        )
                    )
                    calls = len(feed_control.heartbeat_calls)
                    await supervisor.heartbeat_cycle(lambda: None)
                    self.assertEqual(len(feed_control.heartbeat_calls), calls)
                    self.assertEqual(feed_control.finalize_calls, [])
                    self.assertEqual(supervisor._process_owned, 2)
                finally:
                    if not cycle.done():
                        cycle.cancel()
                        await cycle
                    await self._close(
                        supervisor,
                        typing.cast(
                            "_ControlledRunner[object, object]", feed_runner
                        ),
                    )

    async def test_shutdown_reservation_yields_to_heartbeat_uncertainty(
        self,
    ) -> None:
        for runner_closes_first in (False, True):
            with self.subTest(runner_closes_first=runner_closes_first):
                profile = _profile(
                    process_cap=1,
                    feed_cap=1,
                    feed_budget=1,
                    domains=(grant_control.DomainId.FEED,),
                )
                (
                    supervisor,
                    feed_control,
                    feed_runner,
                    _sid_control,
                    _sid_runner,
                ) = self._mixed(profile)
                feed_id = uuid.UUID(int=109 + runner_closes_first)
                grant = feed_store.FeedGrant(feed_id, _OWNER_ID, 1)
                feed_control.results[grant_control.ClaimMode.PRIMARY] = (
                    _claim(grant, _leased_feed(feed_id)),
                )
                feed_control.block_heartbeat = True
                feed_control.heartbeat_error = RuntimeError(
                    "authority unavailable"
                )
                feed_runner.wait_for_signal = "stop"
                feed_runner.block_child_cleanup = True
                await supervisor.admit_cycle(_OWNER_ID)

                heartbeat = asyncio.create_task(
                    supervisor.heartbeat_cycle(lambda: None)
                )
                await asyncio.wait_for(
                    feed_control.heartbeat_entered.wait(),
                    timeout=1,
                )

                async def stop_heartbeat() -> None:
                    return None

                shutdown = asyncio.create_task(
                    supervisor.shutdown(
                        cooperative_grace_sec=30,
                        external_stop_deadline_sec=30,
                        stop_heartbeat_supervision=stop_heartbeat,
                    )
                )
                await asyncio.wait_for(
                    feed_runner.child_started.wait(),
                    timeout=1,
                )
                managed = next(iter(supervisor._registry.values()))
                self.assertIs(
                    managed.terminal_kind,
                    grant_supervisor._ReservationKind.SHUTDOWN,
                )
                if runner_closes_first:
                    feed_runner.release_child.set()
                    await asyncio.wait_for(
                        managed.runner_closed.wait(),
                        timeout=1,
                    )

                feed_control.release_heartbeat.set()
                await heartbeat
                state_after_heartbeat = managed.terminal_state
                kind_after_heartbeat = managed.terminal_kind
                feed_runner.release_child.set()
                result = await shutdown

                self.assertIs(
                    state_after_heartbeat,
                    grant_supervisor.TerminalState.ABANDONED,
                )
                self.assertIs(
                    kind_after_heartbeat,
                    grant_supervisor._ReservationKind.UNCERTAIN,
                )
                self.assertIsNone(result)
                self.assertEqual(feed_control.finalize_calls, [])
                self.assertEqual(supervisor._process_owned, 1)
                self.assertTrue(supervisor.integrity_failure_event.is_set())

    async def test_shutdown_failure_refinement_waits_for_heartbeat(
        self,
    ) -> None:
        profile = _profile(
            process_cap=1,
            feed_cap=1,
            feed_budget=1,
            domains=(grant_control.DomainId.FEED,),
        )
        (
            supervisor,
            feed_control,
            feed_runner,
            _sid_control,
            _sid_runner,
        ) = self._mixed(profile)
        feed_id = uuid.UUID(int=111)
        grant = feed_store.FeedGrant(feed_id, _OWNER_ID, 1)
        feed_control.results[grant_control.ClaimMode.PRIMARY] = (
            _claim(grant, _leased_feed(feed_id)),
        )
        feed_control.block_heartbeat = True
        feed_control.heartbeat_error = RuntimeError("authority unavailable")
        feed_runner.wait_for_signal = "stop"
        feed_runner.outcome = grant_control.RunFailed(
            feed_store.FeedStatusReason.SYSTEM_CONFIGURATION_INVALID,
            "runner failed during shutdown",
        )
        await supervisor.admit_cycle(_OWNER_ID)

        heartbeat = asyncio.create_task(
            supervisor.heartbeat_cycle(lambda: None)
        )
        await asyncio.wait_for(feed_control.heartbeat_entered.wait(), timeout=1)
        heartbeat_stop_entered = asyncio.Event()

        async def stop_heartbeat() -> None:
            heartbeat_stop_entered.set()
            await heartbeat

        shutdown = asyncio.create_task(
            supervisor.shutdown(
                cooperative_grace_sec=30,
                external_stop_deadline_sec=30,
                stop_heartbeat_supervision=stop_heartbeat,
            )
        )
        await asyncio.wait_for(heartbeat_stop_entered.wait(), timeout=1)
        managed = next(iter(supervisor._registry.values()))
        feed_control.release_heartbeat.set()
        result = await shutdown

        self.assertIs(
            managed.terminal_state,
            grant_supervisor.TerminalState.ABANDONED,
        )
        self.assertIs(
            managed.terminal_kind,
            grant_supervisor._ReservationKind.UNCERTAIN,
        )
        self.assertIsNone(result)
        self.assertEqual(feed_control.finalize_calls, [])
        self.assertEqual(supervisor._process_owned, 1)
        self.assertTrue(supervisor.integrity_failure_event.is_set())

    async def test_administrative_sid_stop_is_reserved_and_localized(
        self,
    ) -> None:
        profile = _profile()
        (
            supervisor,
            feed_control,
            feed_runner,
            sid_control,
            sid_runner,
        ) = self._mixed(profile)
        feed_id = uuid.UUID(int=110)
        feed_grant = feed_store.FeedGrant(feed_id, _OWNER_ID, 1)
        sid_grant = ingestion_lease_store.LeaseGrant(
            feed_store.SourceType.BCFY_CALLS,
            "110",
            _OWNER_ID,
            1,
        )
        feed_control.results[grant_control.ClaimMode.PRIMARY] = (
            _claim(feed_grant, _leased_feed(feed_id)),
        )
        sid_control.results[grant_control.ClaimMode.PRIMARY] = (
            _claim(sid_grant, _sid_payload()),
        )
        sid_runner.wait_for_signal = "stop"
        sid_runner.block_child_cleanup = True
        await supervisor.admit_cycle(_OWNER_ID)
        sid_control.heartbeat_results = (
            grant_control.GrantHeartbeat(
                sid_grant,
                grant_control.HeartbeatDisposition.ADMINISTRATIVE_STOP,
            ),
        )

        try:
            with mock.patch.object(
                grant_supervisor.logger,
                "info",
            ) as log_info:
                await supervisor.heartbeat_cycle(lambda: None)
            await asyncio.wait_for(sid_runner.signal_observed.wait(), timeout=1)
            sid_managed = next(
                managed
                for managed in supervisor._registry.values()
                if managed.slot.domain_id is grant_control.DomainId.SID
            )
            self.assertIs(
                sid_managed.terminal_kind,
                grant_supervisor._ReservationKind.ADMINISTRATIVE,
            )
            self.assertFalse(feed_runner.calls[0][2].stop_requested.is_set())
            self.assertFalse(feed_runner.calls[0][2].grant_lost.is_set())
            self.assertEqual(feed_control.finalize_calls, [])
            self.assertEqual(sid_control.finalize_calls, [])
            stop_record = next(
                call.kwargs["extra"]["json_fields"]
                for call in log_info.call_args_list
                if call.kwargs["extra"]["json_fields"]["event_type"]
                == "administrative_stop"
            )
            self.assertEqual(
                (
                    stop_record["profile"],
                    stop_record["profile_digest"],
                    stop_record["domain_id"],
                    stop_record["authority_kind"],
                ),
                (
                    profile.name,
                    worker_profiles.profile_digest(profile),
                    grant_control.DomainId.SID.value,
                    worker_profiles.AuthorityKind.SID_LEASE.value,
                ),
            )
            sid_runner.release_child.set()
            await asyncio.wait_for(sid_managed.runner_closed.wait(), timeout=1)
            self.assertTrue(
                all(
                    key.domain_id is grant_control.DomainId.FEED
                    for key in supervisor._registry
                )
            )
        finally:
            await self._close(
                supervisor,
                typing.cast("_ControlledRunner[object, object]", feed_runner),
                typing.cast("_ControlledRunner[object, object]", sid_runner),
            )

    async def test_exact_loss_is_reserved_before_runner_signal(self) -> None:
        profile = _profile(
            process_cap=1,
            feed_cap=1,
            feed_budget=1,
            domains=(grant_control.DomainId.FEED,),
        )
        (
            supervisor,
            feed_control,
            feed_runner,
            _sid_control,
            _sid_runner,
        ) = self._mixed(profile)
        feed_id = uuid.UUID(int=120)
        grant = feed_store.FeedGrant(feed_id, _OWNER_ID, 1)
        feed_control.results[grant_control.ClaimMode.PRIMARY] = (
            _claim(grant, _leased_feed(feed_id)),
        )
        feed_runner.wait_for_signal = "loss"
        feed_runner.block_child_cleanup = True
        await supervisor.admit_cycle(_OWNER_ID)
        feed_control.heartbeat_results = (
            grant_control.GrantHeartbeat(
                grant,
                grant_control.HeartbeatDisposition.LOST,
            ),
        )

        with mock.patch.object(
            grant_supervisor.logger,
            "info",
        ) as log_info:
            await supervisor.heartbeat_cycle(lambda: None)
        await asyncio.wait_for(feed_runner.signal_observed.wait(), timeout=1)
        managed = next(iter(supervisor._registry.values()))
        self.assertIs(
            managed.terminal_kind,
            grant_supervisor._ReservationKind.CONFIRMED_LOSS,
        )
        self.assertIs(
            managed.terminal_state,
            grant_supervisor.TerminalState.ABANDONED,
        )
        self.assertEqual(feed_control.finalize_calls, [])
        loss_record = next(
            call.kwargs["extra"]["json_fields"]
            for call in log_info.call_args_list
            if call.kwargs["extra"]["json_fields"]["event_type"] == "loss"
        )
        self.assertEqual(
            (
                loss_record["profile"],
                loss_record["profile_digest"],
                loss_record["domain_id"],
                loss_record["authority_kind"],
            ),
            (
                profile.name,
                worker_profiles.profile_digest(profile),
                grant_control.DomainId.FEED.value,
                worker_profiles.AuthorityKind.FEED.value,
            ),
        )
        feed_runner.release_child.set()
        await asyncio.wait_for(managed.runner_closed.wait(), timeout=1)
        self.assertEqual(supervisor._registry, {})

    async def test_active_runner_remains_owned_across_heartbeat(self) -> None:
        profile = _profile(
            process_cap=1,
            feed_cap=1,
            feed_budget=1,
            domains=(grant_control.DomainId.FEED,),
        )
        (
            supervisor,
            feed_control,
            feed_runner,
            _sid_control,
            _sid_runner,
        ) = self._mixed(profile)
        feed_id = uuid.UUID(int=130)
        grant = feed_store.FeedGrant(feed_id, _OWNER_ID, 1)
        feed_control.results[grant_control.ClaimMode.PRIMARY] = (
            _claim(grant, _leased_feed(feed_id)),
        )
        try:
            await supervisor.admit_cycle(_OWNER_ID)
            await asyncio.wait_for(feed_runner.started.wait(), timeout=1)
            self.assertEqual(
                supervisor.active_count(grant_control.DomainId.FEED),
                1,
            )
            await supervisor.heartbeat_cycle(lambda: None)
            self.assertEqual(len(feed_control.heartbeat_calls), 1)
            self.assertEqual(feed_control.finalize_calls, [])
        finally:
            await self._close(
                supervisor,
                typing.cast("_ControlledRunner[object, object]", feed_runner),
            )

    async def test_failure_wins_shutdown_race_with_one_terminal_write(
        self,
    ) -> None:
        profile = _profile(
            process_cap=1,
            feed_cap=1,
            feed_budget=1,
            domains=(grant_control.DomainId.FEED,),
        )
        (
            supervisor,
            feed_control,
            feed_runner,
            _sid_control,
            _sid_runner,
        ) = self._mixed(profile)
        feed_id = uuid.UUID(int=140)
        grant = feed_store.FeedGrant(feed_id, _OWNER_ID, 1)
        payload = _leased_feed(feed_id)
        feed_control.results[grant_control.ClaimMode.PRIMARY] = (
            _claim(grant, payload),
        )
        feed_runner.outcome = grant_control.RunFailed(
            feed_store.FeedStatusReason.SYSTEM_CONFIGURATION_INVALID,
            "bounded failure",
        )
        feed_control.block_finalize = True
        await supervisor.admit_cycle(_OWNER_ID)
        feed_runner.finish.set()
        await asyncio.wait_for(feed_control.finalize_entered.wait(), timeout=1)

        heartbeat_stopped = asyncio.Event()

        async def stop_heartbeat() -> None:
            heartbeat_stopped.set()

        shutdown = asyncio.create_task(
            supervisor.shutdown(
                cooperative_grace_sec=0,
                external_stop_deadline_sec=0,
                stop_heartbeat_supervision=stop_heartbeat,
            )
        )
        await asyncio.wait_for(heartbeat_stopped.wait(), timeout=1)
        self.assertEqual(len(feed_control.finalize_calls), 1)
        self.assertEqual(feed_control.finalize_payloads, [payload])
        terminal = feed_control.finalize_calls[0][1]
        self.assertIsInstance(
            terminal,
            failure_policy.FailurePersistencePlan,
        )
        assert isinstance(terminal, failure_policy.FailurePersistencePlan)
        self.assertIsInstance(
            terminal.treatment,
            failure_policy.ConsumeFailureBudget,
        )
        feed_control.release_finalize.set()
        await shutdown

        self.assertEqual(len(feed_control.finalize_calls), 1)
        self.assertFalse(
            any(
                isinstance(decision, grant_control.NeutralRelease)
                for _grant, decision in feed_control.finalize_calls
            )
        )

    async def test_concurrent_shutdowns_share_one_terminal_task(
        self,
    ) -> None:
        profile = _profile(
            process_cap=1,
            feed_cap=1,
            feed_budget=1,
            domains=(grant_control.DomainId.FEED,),
        )
        (
            supervisor,
            feed_control,
            feed_runner,
            _sid_control,
            _sid_runner,
        ) = self._mixed(profile)
        feed_id = uuid.UUID(int=141)
        grant = feed_store.FeedGrant(feed_id, _OWNER_ID, 1)
        feed_control.results[grant_control.ClaimMode.PRIMARY] = (
            _claim(grant, _leased_feed(feed_id)),
        )
        feed_runner.wait_for_signal = "stop"
        feed_control.block_finalize = True
        await supervisor.admit_cycle(_OWNER_ID)
        terminal_tasks: list[
            asyncio.Task[grant_supervisor._FinalizeEffect]
        ] = []
        both_started = asyncio.Event()
        start_terminal = supervisor._start_terminal_task

        def observed_start(
            managed: grant_supervisor._ManagedGrant,
        ) -> asyncio.Task[grant_supervisor._FinalizeEffect]:
            task = start_terminal(managed)
            terminal_tasks.append(task)
            if len(terminal_tasks) == 2:
                both_started.set()
            return task

        async def stop_heartbeat() -> None:
            return None

        with mock.patch.object(
            supervisor,
            "_start_terminal_task",
            side_effect=observed_start,
        ):
            shutdowns = tuple(
                asyncio.create_task(
                    supervisor.shutdown(
                        cooperative_grace_sec=30,
                        external_stop_deadline_sec=30,
                        stop_heartbeat_supervision=stop_heartbeat,
                    )
                )
                for _ in range(2)
            )
            await asyncio.wait_for(both_started.wait(), timeout=1)
            await asyncio.wait_for(
                feed_control.finalize_entered.wait(),
                timeout=1,
            )
            self.assertIs(terminal_tasks[0], terminal_tasks[1])
            self.assertEqual(len(feed_control.finalize_calls), 1)
            feed_control.release_finalize.set()
            results = await asyncio.gather(*shutdowns)

        self.assertEqual(len(feed_control.finalize_calls), 1)
        self.assertEqual(results, [None, None])
        repeated = await supervisor.shutdown(
            cooperative_grace_sec=0,
            external_stop_deadline_sec=0,
            stop_heartbeat_supervision=stop_heartbeat,
        )
        self.assertIsNone(repeated)
        self.assertEqual(len(feed_control.finalize_calls), 1)

    async def test_cancelled_terminal_write_is_uncertain_and_never_retried(
        self,
    ) -> None:
        profile = _profile(
            process_cap=1,
            feed_cap=1,
            feed_budget=1,
            domains=(grant_control.DomainId.FEED,),
        )
        (
            supervisor,
            feed_control,
            feed_runner,
            _sid_control,
            _sid_runner,
        ) = self._mixed(profile)
        feed_id = uuid.UUID(int=142)
        grant = feed_store.FeedGrant(feed_id, _OWNER_ID, 1)
        feed_control.results[grant_control.ClaimMode.PRIMARY] = (
            _claim(grant, _leased_feed(feed_id)),
        )
        feed_control.block_finalize = True
        await supervisor.admit_cycle(_OWNER_ID)
        feed_runner.finish.set()
        await asyncio.wait_for(feed_control.finalize_entered.wait(), timeout=1)
        managed = next(iter(supervisor._registry.values()))
        terminal_task = managed.terminal_task
        self.assertIsNotNone(terminal_task)
        assert terminal_task is not None
        terminal_task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await terminal_task
        self.assertIs(
            managed.terminal_state,
            grant_supervisor.TerminalState.ABANDONED,
        )
        self.assertIs(
            managed.terminal_kind,
            grant_supervisor._ReservationKind.UNCERTAIN,
        )
        self.assertEqual(len(feed_control.finalize_calls), 1)
        self.assertFalse(supervisor.admission_enabled)
        self.assertTrue(supervisor.integrity_failure_event.is_set())
        self.assertIsInstance(
            supervisor.integrity_failure,
            grant_control.GrantControlIntegrityError,
        )
        self.assertNotIsInstance(
            supervisor.integrity_failure,
            asyncio.CancelledError,
        )

        async def stop_heartbeat() -> None:
            return None

        result = await supervisor.shutdown(
            cooperative_grace_sec=0,
            external_stop_deadline_sec=0,
            stop_heartbeat_supervision=stop_heartbeat,
        )

        self.assertIsNone(result)
        self.assertEqual(len(feed_control.finalize_calls), 1)
        self.assertEqual(supervisor._process_owned, 1)

    async def test_shutdown_is_bounded_when_finalizer_resists_cancellation(
        self,
    ) -> None:
        profile = _profile(
            process_cap=1,
            feed_cap=1,
            feed_budget=1,
            domains=(grant_control.DomainId.FEED,),
        )
        (
            supervisor,
            feed_control,
            feed_runner,
            _sid_control,
            _sid_runner,
        ) = self._mixed(profile)
        feed_id = uuid.UUID(int=143)
        grant = feed_store.FeedGrant(feed_id, _OWNER_ID, 1)
        feed_control.results[grant_control.ClaimMode.PRIMARY] = (
            _claim(grant, _leased_feed(feed_id)),
        )
        feed_control.block_finalize = True
        feed_control.swallow_finalize_cancellation = True
        await supervisor.admit_cycle(_OWNER_ID)
        feed_runner.finish.set()
        await asyncio.wait_for(feed_control.finalize_entered.wait(), timeout=1)

        async def stop_heartbeat() -> None:
            return None

        with self.assertRaises(grant_supervisor.SupervisorNotDrainedError):
            await asyncio.wait_for(
                supervisor.shutdown(
                    cooperative_grace_sec=0,
                    external_stop_deadline_sec=0,
                    stop_heartbeat_supervision=stop_heartbeat,
                ),
                timeout=1,
            )

        self.assertEqual(len(supervisor._terminal_tasks), 1)
        feed_control.release_finalize.set()
        await _settle_terminal_tasks(supervisor)
        self.assertEqual(supervisor._registry, {})

    async def test_shutdown_waits_for_child_and_heartbeat_stop_before_release(
        self,
    ) -> None:
        profile = _profile(
            process_cap=1,
            sid_cap=1,
            sid_budget=1,
            domains=(grant_control.DomainId.SID,),
        )
        (
            supervisor,
            _feed_control,
            _feed_runner,
            sid_control,
            sid_runner,
        ) = self._mixed(profile)
        grant = ingestion_lease_store.LeaseGrant(
            feed_store.SourceType.BCFY_CALLS,
            "150",
            _OWNER_ID,
            1,
        )
        sid_control.results[grant_control.ClaimMode.PRIMARY] = (
            _claim(grant, _sid_payload()),
        )
        sid_runner.wait_for_signal = "stop"
        sid_runner.block_child_cleanup = True
        await supervisor.admit_cycle(_OWNER_ID)
        stop_entered = asyncio.Event()
        allow_stop = asyncio.Event()

        async def stop_heartbeat() -> None:
            stop_entered.set()
            await allow_stop.wait()

        shutdown = asyncio.create_task(
            supervisor.shutdown(
                cooperative_grace_sec=30,
                external_stop_deadline_sec=30,
                stop_heartbeat_supervision=stop_heartbeat,
            )
        )
        await asyncio.wait_for(sid_runner.signal_observed.wait(), timeout=1)
        await asyncio.wait_for(sid_runner.child_started.wait(), timeout=1)
        managed = next(iter(supervisor._registry.values()))
        self.assertIs(
            managed.terminal_kind,
            grant_supervisor._ReservationKind.SHUTDOWN,
        )
        self.assertEqual(sid_control.finalize_calls, [])
        await supervisor.heartbeat_cycle(lambda: None)
        self.assertEqual(len(sid_control.heartbeat_calls), 1)
        sid_runner.release_child.set()
        await asyncio.wait_for(stop_entered.wait(), timeout=1)
        self.assertEqual(sid_control.finalize_calls, [])
        allow_stop.set()
        result = await shutdown

        self.assertIsNone(result)
        self.assertEqual(len(sid_control.finalize_calls), 1)
        self.assertIsInstance(
            sid_control.finalize_calls[0][1],
            grant_control.NeutralRelease,
        )

    async def test_heartbeat_stop_failure_prevents_exact_finalization(
        self,
    ) -> None:
        profile = _profile(
            process_cap=1,
            feed_cap=1,
            feed_budget=1,
            domains=(grant_control.DomainId.FEED,),
        )
        (
            supervisor,
            feed_control,
            feed_runner,
            _sid_control,
            _sid_runner,
        ) = self._mixed(profile)
        feed_id = uuid.UUID(int=151)
        grant = feed_store.FeedGrant(feed_id, _OWNER_ID, 1)
        feed_control.results[grant_control.ClaimMode.PRIMARY] = (
            _claim(grant, _leased_feed(feed_id)),
        )
        feed_runner.wait_for_signal = "stop"
        await supervisor.admit_cycle(_OWNER_ID)

        async def stop_heartbeat() -> None:
            msg = "heartbeat thread did not stop"
            raise RuntimeError(msg)

        with self.assertRaisesRegex(
            RuntimeError,
            "heartbeat thread did not stop",
        ):
            await supervisor.shutdown(
                cooperative_grace_sec=30,
                external_stop_deadline_sec=30,
                stop_heartbeat_supervision=stop_heartbeat,
            )

        self.assertEqual(feed_control.finalize_calls, [])
        self.assertEqual(supervisor._process_owned, 1)
        self.assertFalse(supervisor._heartbeat_stopped.is_set())

    async def test_never_closed_child_is_undrained_and_never_released(
        self,
    ) -> None:
        profile = _profile(
            process_cap=1,
            sid_cap=1,
            sid_budget=1,
            domains=(grant_control.DomainId.SID,),
        )
        (
            supervisor,
            _feed_control,
            _feed_runner,
            sid_control,
            sid_runner,
        ) = self._mixed(profile)
        grant = ingestion_lease_store.LeaseGrant(
            feed_store.SourceType.BCFY_CALLS,
            "160",
            _OWNER_ID,
            1,
        )
        sid_control.results[grant_control.ClaimMode.PRIMARY] = (
            _claim(grant, _sid_payload()),
        )
        sid_runner.wait_for_signal = "stop"
        sid_runner.block_child_cleanup = True
        sid_runner.swallow_cancellation = True
        await supervisor.admit_cycle(_OWNER_ID)

        async def stop_heartbeat() -> None:
            return None

        with self.assertRaises(grant_supervisor.SupervisorNotDrainedError):
            await supervisor.shutdown(
                cooperative_grace_sec=0,
                external_stop_deadline_sec=0,
                stop_heartbeat_supervision=stop_heartbeat,
            )

        self.assertEqual(sid_control.finalize_calls, [])
        self.assertEqual(supervisor._process_owned, 1)
        managed = next(iter(supervisor._registry.values()))
        self.assertIs(
            managed.terminal_state,
            grant_supervisor.TerminalState.ABANDONED,
        )
        await supervisor.heartbeat_cycle(lambda: None)
        self.assertEqual(sid_control.heartbeat_calls, [])
        sid_runner.release_child.set()
        task = managed.root_task
        self.assertIsNotNone(task)
        assert task is not None
        await task
        self.assertEqual(sid_control.finalize_calls, [])
        self.assertEqual(supervisor._process_owned, 1)

    async def test_raw_runner_cancellation_is_unclosed_and_never_released(
        self,
    ) -> None:
        profile = _profile(
            process_cap=1,
            feed_cap=1,
            feed_budget=1,
            domains=(grant_control.DomainId.FEED,),
        )
        (
            supervisor,
            feed_control,
            _feed_runner,
            _sid_control,
            _sid_runner,
        ) = self._mixed(profile)
        feed_id = uuid.UUID(int=161)
        feed_control.results[grant_control.ClaimMode.PRIMARY] = (
            _claim(
                feed_store.FeedGrant(feed_id, _OWNER_ID, 1),
                _leased_feed(feed_id),
            ),
        )
        await supervisor.admit_cycle(_OWNER_ID)
        managed = next(iter(supervisor._registry.values()))
        root_task = managed.root_task
        self.assertIsNotNone(root_task)
        assert root_task is not None

        root_task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await root_task

        self.assertFalse(managed.runner_closed.is_set())

        async def stop_heartbeat() -> None:
            return None

        with self.assertRaises(grant_supervisor.SupervisorNotDrainedError):
            await supervisor.shutdown(
                cooperative_grace_sec=0,
                external_stop_deadline_sec=0,
                stop_heartbeat_supervision=stop_heartbeat,
            )

        self.assertEqual(feed_control.finalize_calls, [])
        self.assertEqual(supervisor._process_owned, 1)
        managed = next(iter(supervisor._registry.values()))
        self.assertIs(
            managed.terminal_state,
            grant_supervisor.TerminalState.ABANDONED,
        )

    async def test_shutdown_finalization_respects_bounded_concurrency(
        self,
    ) -> None:
        profile = _profile(
            process_cap=3,
            feed_cap=3,
            feed_budget=3,
            domains=(grant_control.DomainId.FEED,),
        )
        (
            supervisor,
            feed_control,
            feed_runner,
            _sid_control,
            _sid_runner,
        ) = self._mixed(profile)
        feed_control.results[grant_control.ClaimMode.PRIMARY] = tuple(
            _claim(
                feed_store.FeedGrant(uuid.UUID(int=index), _OWNER_ID, 1),
                _leased_feed(uuid.UUID(int=index)),
            )
            for index in range(201, 204)
        )
        feed_runner.wait_for_signal = "stop"
        feed_control.block_finalize = True
        await supervisor.admit_cycle(_OWNER_ID)

        async def stop_heartbeat() -> None:
            return None

        shutdown = asyncio.create_task(
            supervisor.shutdown(
                cooperative_grace_sec=30,
                external_stop_deadline_sec=30,
                stop_heartbeat_supervision=stop_heartbeat,
            )
        )
        await asyncio.wait_for(
            feed_control.two_finalizers_entered.wait(),
            timeout=1,
        )
        self.assertEqual(feed_control.finalize_active, 2)
        self.assertEqual(len(feed_control.finalize_calls), 2)
        feed_control.release_finalize.set()
        result = await shutdown

        self.assertIsNone(result)
        self.assertEqual(feed_control.max_finalize_active, 2)
        self.assertEqual(len(feed_control.finalize_calls), 3)

    async def test_stale_heartbeat_failure_cannot_poison_successor(
        self,
    ) -> None:
        profile = _profile(
            process_cap=2,
            feed_cap=2,
            feed_budget=1,
            domains=(grant_control.DomainId.FEED,),
        )
        (
            supervisor,
            feed_control,
            feed_runner,
            _sid_control,
            _sid_runner,
        ) = self._mixed(profile)
        feed_id = uuid.UUID(int=170)
        old_grant = feed_store.FeedGrant(feed_id, _OWNER_ID, 1)
        feed_control.results[grant_control.ClaimMode.PRIMARY] = (
            _claim(old_grant, _leased_feed(feed_id)),
        )
        await supervisor.admit_cycle(_OWNER_ID)
        feed_control.block_heartbeat = True
        feed_control.heartbeat_error = RuntimeError("stale heartbeat failed")
        cycle = asyncio.create_task(supervisor.heartbeat_cycle(lambda: None))
        await asyncio.wait_for(feed_control.heartbeat_entered.wait(), timeout=1)

        successor = feed_store.FeedGrant(feed_id, _OWNER_ID, 2)
        feed_control.results[grant_control.ClaimMode.PRIMARY] = (
            _claim(successor, _leased_feed(feed_id, fencing_token=2)),
        )
        await supervisor.admit_cycle(_OWNER_ID)
        successor_managed = next(iter(supervisor._registry.values()))
        feed_control.release_heartbeat.set()
        await cycle

        self.assertEqual(successor_managed.grant.fencing_token, 2)
        self.assertTrue(supervisor.admission_enabled)
        self.assertIsNone(supervisor.integrity_failure)
        self.assertIs(
            successor_managed.terminal_state,
            grant_supervisor.TerminalState.OPEN,
        )
        await self._close(
            supervisor,
            typing.cast("_ControlledRunner[object, object]", feed_runner),
        )

    async def test_ambiguous_terminal_write_is_abandoned_without_retry(
        self,
    ) -> None:
        profile = _profile(
            process_cap=1,
            feed_cap=1,
            feed_budget=1,
            domains=(grant_control.DomainId.FEED,),
        )
        (
            supervisor,
            feed_control,
            feed_runner,
            _sid_control,
            _sid_runner,
        ) = self._mixed(profile)
        feed_id = uuid.UUID(int=180)
        grant = feed_store.FeedGrant(feed_id, _OWNER_ID, 1)
        feed_control.results[grant_control.ClaimMode.PRIMARY] = (
            _claim(grant, _leased_feed(feed_id)),
        )
        feed_control.finalize_error = RuntimeError("outcome unknown")
        feed_runner.finish.set()

        await supervisor.admit_cycle(_OWNER_ID)
        await asyncio.wait_for(feed_control.finalize_entered.wait(), timeout=1)
        await asyncio.wait_for(
            supervisor.integrity_failure_event.wait(),
            timeout=1,
        )

        managed = next(iter(supervisor._registry.values()))
        self.assertIs(
            managed.terminal_state,
            grant_supervisor.TerminalState.ABANDONED,
        )
        self.assertEqual(len(feed_control.finalize_calls), 1)
        self.assertEqual(supervisor._process_owned, 1)
        await supervisor.heartbeat_cycle(lambda: None)
        self.assertEqual(feed_control.heartbeat_calls, [])
        self.assertEqual(len(feed_control.finalize_calls), 1)

    async def test_shutdown_reservation_refines_to_failed_runner_decision(
        self,
    ) -> None:
        profile = _profile(
            process_cap=1,
            feed_cap=1,
            feed_budget=1,
            domains=(grant_control.DomainId.FEED,),
        )
        (
            supervisor,
            feed_control,
            feed_runner,
            _sid_control,
            _sid_runner,
        ) = self._mixed(profile)
        feed_id = uuid.UUID(int=190)
        grant = feed_store.FeedGrant(feed_id, _OWNER_ID, 1)
        feed_control.results[grant_control.ClaimMode.PRIMARY] = (
            _claim(grant, _leased_feed(feed_id)),
        )
        feed_runner.wait_for_signal = "stop"
        feed_runner.block_child_cleanup = True
        feed_runner.outcome = grant_control.RunFailed(
            feed_store.FeedStatusReason.SYSTEM_CONFIGURATION_INVALID,
            "late failure",
        )
        await supervisor.admit_cycle(_OWNER_ID)

        async def stop_heartbeat() -> None:
            return None

        shutdown = asyncio.create_task(
            supervisor.shutdown(
                cooperative_grace_sec=30,
                external_stop_deadline_sec=30,
                stop_heartbeat_supervision=stop_heartbeat,
            )
        )
        await asyncio.wait_for(feed_runner.signal_observed.wait(), timeout=1)
        managed = next(iter(supervisor._registry.values()))
        self.assertIs(
            managed.terminal_kind,
            grant_supervisor._ReservationKind.SHUTDOWN,
        )
        feed_runner.release_child.set()
        result = await shutdown

        self.assertIsNone(result)
        self.assertEqual(len(feed_control.finalize_calls), 1)
        self.assertIsInstance(
            feed_control.finalize_calls[0][1],
            failure_policy.FailurePersistencePlan,
        )

    async def test_lifecycle_records_are_low_cardinality(
        self,
    ) -> None:
        profile = _profile()
        (
            supervisor,
            feed_control,
            feed_runner,
            sid_control,
            sid_runner,
        ) = self._mixed(profile)
        feed_id = uuid.UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
        feed_grant = feed_store.FeedGrant(feed_id, _OWNER_ID, 1)
        sid_grant = ingestion_lease_store.LeaseGrant(
            feed_store.SourceType.BCFY_CALLS,
            "sensitive-sid-key",
            _OWNER_ID,
            1,
        )
        feed_control.results[grant_control.ClaimMode.PRIMARY] = (
            _claim(feed_grant, _leased_feed(feed_id)),
        )
        sid_control.results[grant_control.ClaimMode.PRIMARY] = (
            grant_control.ClaimedGrant(
                grant=sid_grant,
                payload=_sid_payload(),
            ),
        )
        feed_runner.wait_for_signal = "stop"
        sid_runner.wait_for_signal = "stop"

        with mock.patch.object(grant_supervisor.logger, "info") as log_info:
            await supervisor.admit_cycle(_OWNER_ID)
            await asyncio.wait_for(feed_runner.started.wait(), timeout=1)
            await asyncio.wait_for(sid_runner.started.wait(), timeout=1)
            self.assertEqual(
                supervisor.active_count(grant_control.DomainId.FEED),
                1,
            )
            self.assertEqual(
                supervisor.active_count(grant_control.DomainId.SID),
                1,
            )
            await supervisor.heartbeat_cycle(lambda: None)

            async def stop_heartbeat() -> None:
                return None

            result = await supervisor.shutdown(
                cooperative_grace_sec=30,
                external_stop_deadline_sec=30,
                stop_heartbeat_supervision=stop_heartbeat,
            )

        self.assertIsNone(result)

        records = [
            call.kwargs["extra"]["json_fields"]
            for call in log_info.call_args_list
        ]
        self.assertTrue(records)
        self.assertTrue(
            {
                "admission",
                "heartbeat",
                "shutdown",
                "finalization",
            }.issubset({record["event_type"] for record in records})
        )
        expected_contexts = {
            (
                grant_control.DomainId.FEED.value,
                worker_profiles.AuthorityKind.FEED.value,
            ),
            (
                grant_control.DomainId.SID.value,
                worker_profiles.AuthorityKind.SID_LEASE.value,
            ),
        }
        self.assertEqual(
            {
                (record["domain_id"], record["authority_kind"])
                for record in records
            },
            expected_contexts,
        )
        for record in records:
            self.assertEqual(record["profile"], profile.name)
            self.assertEqual(
                record["profile_digest"],
                worker_profiles.profile_digest(profile),
            )
            rendered = str(record).lower()
            for forbidden in (
                str(feed_id),
                "sensitive-sid-key",
                str(_OWNER_ID),
                "authorization",
                "signed_url",
                "credential",
                "bounded failure",
                "late failure",
            ):
                self.assertNotIn(forbidden, rendered)


class TestGrantSupervisorStructure(unittest.TestCase):
    """Static domain-agnostic and private-erasure contracts."""

    def test_required_values_are_frozen_and_terminal_state_is_exact(
        self,
    ) -> None:
        for value_type in (
            grant_supervisor.RegisteredDomain,
            grant_supervisor._UnitSlot,
            grant_supervisor._ErasedRegisteredDomain,
        ):
            with self.subTest(value_type=value_type.__name__):
                self.assertTrue(value_type.__dataclass_params__.frozen)
        self.assertTrue(
            issubclass(
                grant_supervisor.SupervisorNotDrainedError,
                RuntimeError,
            )
        )
        self.assertEqual(
            {state.value for state in grant_supervisor.TerminalState},
            {"open", "reserved", "finalized", "abandoned"},
        )

    def test_lifecycle_methods_have_no_feed_or_sid_dispatch_branch(
        self,
    ) -> None:
        tree = ast.parse(inspect.getsource(grant_supervisor.GrantSupervisor))
        forbidden_attributes = {
            "FEED",
            "SID",
            "FeedGrantControl",
            "SidGrantControl",
        }
        lifecycle_methods = {
            "admit_cycle",
            "_reserve_admission",
            "_claim_reserved",
            "_validate_claim_batch",
            "_register_claim",
            "_run_managed",
            "_root_done",
            "_discard_current",
            "heartbeat_cycle",
            "_validate_heartbeat_results",
            "_fail_heartbeat_domain",
            "_reserve_terminal",
            "_finalize_reserved",
            "active_count",
            "shutdown",
        }
        class_node = typing.cast("ast.ClassDef", tree.body[0])
        for node in class_node.body:
            if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            if node.name not in lifecycle_methods:
                continue
            attributes = {
                child.attr
                for child in ast.walk(node)
                if isinstance(child, ast.Attribute)
            }
            with self.subTest(method=node.name):
                self.assertTrue(attributes.isdisjoint(forbidden_attributes))

    def test_erasure_is_private_and_imports_no_data_plane_or_store(
        self,
    ) -> None:
        source = pathlib.Path(grant_supervisor.__file__).read_text()

        self.assertNotIn("typing.Any", source)
        self.assertNotIn("LeasedFeed", source)
        for forbidden in (
            "feed_grant_control",
            "sid_grant_control",
            "feed_store",
            "ingestion_lease_store",
            "collector_runtime",
            "health_server",
            "router",
            "scheduler",
            "poller",
            "membership",
            "gcs",
            "pubsub",
            "aiohttp",
            "asyncpg",
        ):
            with self.subTest(forbidden=forbidden):
                self.assertNotIn(f"import {forbidden}", source)

    def test_future_domain_requires_catalog_profile_and_registration(
        self,
    ) -> None:
        self.assertEqual(
            set(grant_control.DomainId),
            {grant_control.DomainId.FEED, grant_control.DomainId.SID},
        )
        self.assertEqual(
            set(worker_profiles.DOMAIN_CATALOG),
            set(grant_control.DomainId),
        )
        self.assertEqual(
            tuple(
                inspect.signature(grant_supervisor.RegisteredDomain).parameters
            ),
            ("domain_id", "control", "runner"),
        )

    def test_supervisor_has_no_client_pool_or_sleep_owned_api(self) -> None:
        public_methods = {
            name
            for name, value in vars(grant_supervisor.GrantSupervisor).items()
            if callable(value) and not name.startswith("_")
        }
        self.assertEqual(
            public_methods,
            {"active_count", "admit_cycle", "heartbeat_cycle", "shutdown"},
        )
        source = pathlib.Path(grant_supervisor.__file__).read_text()
        test_source = pathlib.Path(__file__).read_text()
        for forbidden in (
            "ClientSession",
            "asyncpg.Pool",
            "gcs_client",
            "pubsub_client",
            "asyncio" + ".sleep",
            "time" + ".sleep",
        ):
            with self.subTest(forbidden=forbidden):
                self.assertNotIn(forbidden, source)
        tree = ast.parse(test_source)
        sleep_calls = {
            (child.func.value.id, child.func.attr)
            for child in ast.walk(tree)
            if isinstance(child, ast.Call)
            and isinstance(child.func, ast.Attribute)
            and isinstance(child.func.value, ast.Name)
            and child.func.attr == "sleep"
        }
        self.assertEqual(sleep_calls, set())


if __name__ == "__main__":
    unittest.main()
