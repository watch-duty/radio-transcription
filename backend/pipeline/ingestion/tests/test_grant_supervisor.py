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
    grant_control,
    grant_supervisor,
    worker_profiles,
)
from backend.pipeline.storage import feed_store, ingestion_lease_store

_OWNER_ID = uuid.UUID("11111111-2222-3333-4444-555555555555")
_OTHER_OWNER_ID = uuid.UUID("22222222-3333-4444-5555-666666666666")
_NOW = datetime.datetime(2026, 7, 11, 12, 0, tzinfo=datetime.UTC)


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


def _lease_snapshot() -> ingestion_lease_store.LeaseSnapshot:
    return ingestion_lease_store.LeaseSnapshot(
        status=feed_store.FeedStatus.ACTIVE,
        last_heartbeat=_NOW,
        failure_count=0,
        retry_after=None,
        status_reason=None,
        status_reason_detail=None,
        status_reason_updated_at=None,
        audit_revision=1,
        membership_revision=1,
        updated_at=_NOW,
    )


def _valid_leased_feed(
    value: object,
) -> typing.TypeGuard[feed_store.LeasedFeed]:
    if not isinstance(value, dict):
        return False
    mapping = typing.cast("dict[str, object]", value)
    required = {
        "id",
        "name",
        "source_type",
        "last_processed_filename",
        "last_bookmark_time",
        "fencing_token",
        "failure_count",
        "status_reason",
        "source_feed_id",
    }
    if not required.issubset(mapping):
        return False
    return (
        isinstance(mapping["id"], uuid.UUID)
        and isinstance(mapping["name"], str)
        and isinstance(mapping["source_type"], feed_store.SourceType)
        and (
            mapping["last_processed_filename"] is None
            or isinstance(mapping["last_processed_filename"], str)
        )
        and (
            mapping["last_bookmark_time"] is None
            or isinstance(mapping["last_bookmark_time"], datetime.datetime)
        )
        and not isinstance(mapping["fencing_token"], bool)
        and isinstance(mapping["fencing_token"], int)
        and not isinstance(mapping["failure_count"], bool)
        and isinstance(mapping["failure_count"], int)
        and (
            mapping["status_reason"] is None
            or isinstance(mapping["status_reason"], feed_store.FeedStatusReason)
        )
        and (
            mapping["source_feed_id"] is None
            or isinstance(mapping["source_feed_id"], str)
        )
    )


def _valid_lease_snapshot(
    value: object,
) -> typing.TypeGuard[ingestion_lease_store.LeaseSnapshot]:
    return isinstance(value, ingestion_lease_store.LeaseSnapshot)


def _terminal_for(
    outcome: grant_control.RunOutcome,
) -> grant_control.TerminalDecision:
    if isinstance(outcome, grant_control.RunFailed):
        return grant_control.BudgetedFailureDecision(
            failure_threshold=5,
            backoff_base_sec=15,
            backoff_max_sec=600,
            status_reason=outcome.status_reason,
            actor_id="service_account:gcp:grant-supervisor-tests",
            reason=outcome.reason,
        )
    return grant_control.NeutralRelease(grant_control.TerminalCause.NORMAL)


class _ControlledControl[GrantT, PayloadT]:
    def __init__(self) -> None:
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
                grant_control.LifecycleEvidence(durable_failing=False),
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
                await self.release_finalize.wait()
            if self.finalize_error is not None:
                raise self.finalize_error
            if self.finalize_result is not None:
                return self.finalize_result
            return grant_control.FinalizeResult(
                grant,
                grant_control.FinalizeDisposition.APPLIED,
                None,
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
        self.set_retrying = False

    async def run(
        self,
        grant: GrantT,
        payload: PayloadT,
        context: grant_control.RunContext,
    ) -> grant_control.RunOutcome:
        self.calls.append((grant, payload, context))
        self.started.set()
        if self.set_retrying:
            context.set_retrying(True)
        try:
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
        finally:
            if self.set_retrying:
                context.set_retrying(False)
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
    allocation: worker_profiles.DomainAllocation,
    *,
    validator: typing.Callable[
        [object],
        typing.TypeGuard[feed_store.LeasedFeed],
    ] = _valid_leased_feed,
) -> grant_supervisor.RegisteredDomain[
    feed_store.FeedGrant,
    feed_store.LeasedFeed,
]:
    return grant_supervisor.RegisteredDomain(
        domain_id=grant_control.DomainId.FEED,
        authority_kind=worker_profiles.AuthorityKind.FEED,
        grant_type=feed_store.FeedGrant,
        payload_validator=validator,
        authority_of=lambda grant: grant_supervisor.FeedAuthority(
            grant.feed_id
        ),
        owner_of=lambda grant: grant.owner_worker_id,
        fencing_token_of=lambda grant: grant.fencing_token,
        control=control,
        runner=runner,
        allocation=allocation,
        terminal_decision_for=_terminal_for,
    )


def _sid_registration(
    control: _ControlledControl[
        ingestion_lease_store.LeaseGrant,
        ingestion_lease_store.LeaseSnapshot,
    ],
    runner: _ControlledRunner[
        ingestion_lease_store.LeaseGrant,
        ingestion_lease_store.LeaseSnapshot,
    ],
    allocation: worker_profiles.DomainAllocation,
) -> grant_supervisor.RegisteredDomain[
    ingestion_lease_store.LeaseGrant,
    ingestion_lease_store.LeaseSnapshot,
]:
    return grant_supervisor.RegisteredDomain(
        domain_id=grant_control.DomainId.SID,
        authority_kind=worker_profiles.AuthorityKind.SID_LEASE,
        grant_type=ingestion_lease_store.LeaseGrant,
        payload_validator=_valid_lease_snapshot,
        authority_of=lambda grant: grant_supervisor.SidAuthority(
            grant.source_type.value,
            grant.lease_key,
        ),
        owner_of=lambda grant: grant.owner_worker_id,
        fencing_token_of=lambda grant: grant.fencing_token,
        control=control,
        runner=runner,
        allocation=allocation,
        terminal_decision_for=_terminal_for,
    )


def _claim[GrantT, PayloadT](
    grant: GrantT,
    payload: PayloadT,
) -> grant_control.ClaimedGrant[GrantT, PayloadT]:
    return grant_control.ClaimedGrant(
        grant=grant,
        payload=payload,
        lifecycle=grant_control.LifecycleEvidence(durable_failing=False),
    )


class TestGrantSupervisor(unittest.IsolatedAsyncioTestCase):
    """Controlled reservation, admission, and exact-generation proofs."""

    def _mixed(
        self,
        profile: worker_profiles.WorkerProfile,
    ) -> tuple[
        grant_supervisor.GrantSupervisor,
        _ControlledControl[feed_store.FeedGrant, feed_store.LeasedFeed],
        _ControlledRunner[feed_store.FeedGrant, feed_store.LeasedFeed],
        _ControlledControl[
            ingestion_lease_store.LeaseGrant,
            ingestion_lease_store.LeaseSnapshot,
        ],
        _ControlledRunner[
            ingestion_lease_store.LeaseGrant,
            ingestion_lease_store.LeaseSnapshot,
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
            ingestion_lease_store.LeaseSnapshot,
        ]()
        sid_runner = _ControlledRunner[
            ingestion_lease_store.LeaseGrant,
            ingestion_lease_store.LeaseSnapshot,
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
                    allocations[grant_control.DomainId.FEED],
                )
            )
        if grant_control.DomainId.SID in allocations:
            registrations.append(
                _sid_registration(
                    sid_control,
                    sid_runner,
                    allocations[grant_control.DomainId.SID],
                )
            )
        supervisor = grant_supervisor.GrantSupervisor(
            profile,
            registrations,
            finalize_concurrency=2,
        )
        return (
            supervisor,
            feed_control,
            feed_runner,
            sid_control,
            sid_runner,
        )

    async def _close(
        self,
        supervisor: grant_supervisor.GrantSupervisor,
        *runners: _ControlledRunner[object, object],
    ) -> None:
        for runner in runners:
            runner.finish.set()
            runner.release_child.set()
        for managed in tuple(supervisor._registry.values()):
            task = managed.root_task
            if task is not None and not task.done():
                task.cancel()
            if task is not None:
                await task
        await supervisor._settle_terminal_tasks()

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
            _claim(sid_grant, _lease_snapshot()),
        )

        try:
            await supervisor.admit_cycle(_OWNER_ID)
            await asyncio.wait_for(feed_runner.started.wait(), timeout=1)
            await asyncio.wait_for(sid_runner.started.wait(), timeout=1)

            self.assertEqual(len(supervisor._registry), 2)
            self.assertEqual(supervisor._process_owned, 2)
            self.assertEqual(supervisor._process_reserved, 0)
            authorities = {key.authority for key in supervisor._registry}
            self.assertIn(grant_supervisor.FeedAuthority(feed_id), authorities)
            self.assertIn(
                grant_supervisor.SidAuthority("bcfy_calls", "123"),
                authorities,
            )
            self.assertIs(feed_runner.calls[0][0], feed_grant)
            self.assertIs(sid_runner.calls[0][0], sid_grant)
        finally:
            await self._close(
                supervisor,
                typing.cast("_ControlledRunner[object, object]", feed_runner),
                typing.cast("_ControlledRunner[object, object]", sid_runner),
            )

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
                _lease_snapshot(),
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
        ) -> grant_supervisor.ShutdownResult | None:
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

        self.assertEqual(result, grant_supervisor.ShutdownResult(1, 0, 0))
        self.assertEqual(supervisor._registry, {})
        self.assertEqual(supervisor._process_reserved, 0)
        self.assertEqual(feed_runner.calls, [])
        self.assertEqual(len(feed_control.finalize_calls), 1)
        self.assertEqual(feed_control.finalize_calls[0][0].feed_id, feed_id)
        self.assertEqual(
            feed_control.finalize_calls[0][1],
            grant_control.NeutralRelease(grant_control.TerminalCause.SHUTDOWN),
        )

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
        self.assertIsInstance(
            supervisor.integrity_failure,
            grant_control.GrantControlIntegrityError,
        )
        self.assertIs(
            supervisor.integrity_failure.__cause__,
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

    async def test_malformed_feed_payloads_fail_before_registration_or_run(
        self,
    ) -> None:
        profile = _profile(
            process_cap=1,
            feed_cap=1,
            feed_budget=1,
            domains=(grant_control.DomainId.FEED,),
        )
        malformed = (
            {"id": uuid.UUID(int=1)},
            {**_leased_feed(uuid.UUID(int=2)), "name": 7},
        )
        for payload in malformed:
            with self.subTest(payload=payload):
                (
                    supervisor,
                    feed_control,
                    feed_runner,
                    _sid_control,
                    _sid_runner,
                ) = self._mixed(profile)
                feed_id = typing.cast(
                    "uuid.UUID",
                    typing.cast("dict[str, object]", payload)["id"],
                )
                grant = feed_store.FeedGrant(
                    feed_id,
                    _OWNER_ID,
                    1,
                )
                feed_control.results[grant_control.ClaimMode.PRIMARY] = (
                    typing.cast(
                        "grant_control.ClaimedGrant[feed_store.FeedGrant, feed_store.LeasedFeed]",
                        _claim(grant, payload),
                    ),
                )

                await supervisor.admit_cycle(_OWNER_ID)

                self.assertFalse(supervisor.admission_enabled)
                self.assertTrue(supervisor.integrity_failure_event.is_set())
                self.assertEqual(supervisor._registry, {})
                self.assertEqual(feed_runner.calls, [])
                self.assertEqual(supervisor._process_reserved, 0)

    async def test_validator_exception_is_a_visible_integrity_failure(
        self,
    ) -> None:
        profile = _profile(
            process_cap=1,
            feed_cap=1,
            feed_budget=1,
            domains=(grant_control.DomainId.FEED,),
        )
        feed_control = _ControlledControl[
            feed_store.FeedGrant,
            feed_store.LeasedFeed,
        ]()
        feed_runner = _ControlledRunner[
            feed_store.FeedGrant,
            feed_store.LeasedFeed,
        ]()

        def raising_validator(
            _value: object,
        ) -> typing.TypeGuard[feed_store.LeasedFeed]:
            msg = "validator bug"
            raise RuntimeError(msg)

        registration = _feed_registration(
            feed_control,
            feed_runner,
            profile.allocations[0],
            validator=raising_validator,
        )
        supervisor = grant_supervisor.GrantSupervisor(
            profile,
            (registration,),
            finalize_concurrency=1,
        )
        feed_id = uuid.UUID(int=3)
        feed_control.results[grant_control.ClaimMode.PRIMARY] = (
            _claim(
                feed_store.FeedGrant(feed_id, _OWNER_ID, 1),
                _leased_feed(feed_id),
            ),
        )

        await supervisor.admit_cycle(_OWNER_ID)

        self.assertFalse(supervisor.admission_enabled)
        self.assertIsInstance(
            supervisor.integrity_failure,
            grant_control.GrantControlIntegrityError,
        )
        self.assertEqual(supervisor._registry, {})
        self.assertEqual(feed_runner.calls, [])

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
                    claims = (first, first)
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
        old_key = next(iter(supervisor._registry))
        old_managed = supervisor._registry[old_key]
        self.assertTrue(
            supervisor._reserve_terminal(
                old_key,
                grant_supervisor._ConfirmedLoss(),
            )
        )
        feed_runner.finish.set()
        await asyncio.wait_for(old_managed.runner_closed.wait(), timeout=1)
        old_task = old_managed.root_task
        self.assertIsNotNone(old_task)
        assert old_task is not None
        await old_task
        self.assertNotIn(old_key, supervisor._registry)

        successor = feed_store.FeedGrant(feed_id, _OWNER_ID, 2)
        feed_control.results[grant_control.ClaimMode.PRIMARY] = (
            _claim(successor, _leased_feed(feed_id, fencing_token=2)),
        )
        feed_runner.finish.clear()
        await supervisor.admit_cycle(_OWNER_ID)
        successor_key = next(iter(supervisor._registry))

        supervisor._root_done(old_key, old_task)

        self.assertIn(successor_key, supervisor._registry)
        self.assertEqual(successor_key.fencing_token, 2)
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
                retained = grant_control.LifecycleEvidence(
                    durable_failing=False
                )
                first_result = grant_control.GrantHeartbeat(
                    first,
                    grant_control.HeartbeatDisposition.RETAINED,
                    retained,
                )
                second_result = grant_control.GrantHeartbeat(
                    second,
                    grant_control.HeartbeatDisposition.RETAINED,
                    retained,
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
                            None,
                        ),
                        grant_control.GrantHeartbeat(
                            second,
                            grant_control.HeartbeatDisposition.UNAVAILABLE,
                            None,
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
            _claim(sid_grant, _lease_snapshot()),
        )
        sid_runner.wait_for_signal = "stop"
        sid_runner.block_child_cleanup = True
        await supervisor.admit_cycle(_OWNER_ID)
        sid_control.heartbeat_results = (
            grant_control.GrantHeartbeat(
                sid_grant,
                grant_control.HeartbeatDisposition.ADMINISTRATIVE_STOP,
                None,
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
                if managed.key.domain_id is grant_control.DomainId.SID
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
                None,
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

    async def test_local_retry_remains_active_across_heartbeat(self) -> None:
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
        feed_runner.set_retrying = True

        try:
            await supervisor.admit_cycle(_OWNER_ID)
            await asyncio.wait_for(feed_runner.started.wait(), timeout=1)
            snapshot = supervisor.snapshot()
            self.assertEqual(
                snapshot.counts_by_domain[grant_control.DomainId.FEED],
                grant_supervisor.GrantCount(1, 1, 0),
            )
            await supervisor.heartbeat_cycle(lambda: None)
            self.assertEqual(len(feed_control.heartbeat_calls), 1)
            self.assertEqual(feed_control.finalize_calls, [])
            self.assertTrue(next(iter(supervisor._registry.values())).retrying)
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
        self.assertIsInstance(
            feed_control.finalize_calls[0][1],
            grant_control.BudgetedFailureDecision,
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
            key: grant_supervisor._GenerationKey,
        ) -> asyncio.Task[grant_supervisor._FinalizeEffect]:
            task = start_terminal(key)
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
        self.assertEqual(
            results,
            [
                grant_supervisor.ShutdownResult(1, 0, 0),
                grant_supervisor.ShutdownResult(1, 0, 0),
            ],
        )
        repeated = await supervisor.shutdown(
            cooperative_grace_sec=0,
            external_stop_deadline_sec=0,
            stop_heartbeat_supervision=stop_heartbeat,
        )
        self.assertEqual(
            repeated,
            grant_supervisor.ShutdownResult(0, 0, 0),
        )
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

        self.assertEqual(result, grant_supervisor.ShutdownResult(0, 1, 0))
        self.assertEqual(len(feed_control.finalize_calls), 1)
        self.assertEqual(supervisor._process_owned, 1)

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
            _claim(grant, _lease_snapshot()),
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

        self.assertEqual(result, grant_supervisor.ShutdownResult(1, 0, 0))
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
            _claim(grant, _lease_snapshot()),
        )
        sid_runner.wait_for_signal = "stop"
        sid_runner.block_child_cleanup = True
        sid_runner.swallow_cancellation = True
        await supervisor.admit_cycle(_OWNER_ID)

        async def stop_heartbeat() -> None:
            return None

        result = await supervisor.shutdown(
            cooperative_grace_sec=0,
            external_stop_deadline_sec=0,
            stop_heartbeat_supervision=stop_heartbeat,
        )

        self.assertEqual(result, grant_supervisor.ShutdownResult(0, 1, 1))
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

        self.assertEqual(result.finalized, 3)
        self.assertEqual(feed_control.max_finalize_active, 2)
        self.assertEqual(len(feed_control.finalize_calls), 3)

    async def test_old_generation_heartbeat_result_cannot_touch_successor(
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
        feed_id = uuid.UUID(int=170)
        old_grant = feed_store.FeedGrant(feed_id, _OWNER_ID, 1)
        feed_control.results[grant_control.ClaimMode.PRIMARY] = (
            _claim(old_grant, _leased_feed(feed_id)),
        )
        await supervisor.admit_cycle(_OWNER_ID)
        old_key = next(iter(supervisor._registry))
        feed_control.heartbeat_results = (
            grant_control.GrantHeartbeat(
                old_grant,
                grant_control.HeartbeatDisposition.RETAINED,
                grant_control.LifecycleEvidence(durable_failing=True),
            ),
        )
        feed_control.block_heartbeat = True
        cycle = asyncio.create_task(supervisor.heartbeat_cycle(lambda: None))
        await asyncio.wait_for(feed_control.heartbeat_entered.wait(), timeout=1)
        self.assertTrue(
            supervisor._reserve_terminal(
                old_key,
                grant_supervisor._ConfirmedLoss(),
            )
        )
        feed_runner.finish.set()
        old_managed = supervisor._registry[old_key]
        await asyncio.wait_for(old_managed.runner_closed.wait(), timeout=1)
        self.assertNotIn(old_key, supervisor._registry)
        feed_runner.finish.clear()

        successor = feed_store.FeedGrant(feed_id, _OWNER_ID, 2)
        feed_control.results[grant_control.ClaimMode.PRIMARY] = (
            _claim(successor, _leased_feed(feed_id, fencing_token=2)),
        )
        await supervisor.admit_cycle(_OWNER_ID)
        successor_managed = next(iter(supervisor._registry.values()))
        feed_control.release_heartbeat.set()
        await cycle

        self.assertEqual(successor_managed.key.fencing_token, 2)
        self.assertFalse(successor_managed.lifecycle.durable_failing)
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

    async def test_shutdown_reservation_wins_before_failed_runner_observes_stop(
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

        self.assertEqual(result.finalized, 1)
        self.assertEqual(len(feed_control.finalize_calls), 1)
        self.assertIsInstance(
            feed_control.finalize_calls[0][1],
            grant_control.NeutralRelease,
        )

    async def test_snapshot_and_lifecycle_records_are_low_cardinality(
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
                payload=_lease_snapshot(),
                lifecycle=grant_control.LifecycleEvidence(durable_failing=True),
            ),
        )
        feed_runner.wait_for_signal = "stop"
        sid_runner.wait_for_signal = "stop"

        with mock.patch.object(grant_supervisor.logger, "info") as log_info:
            await supervisor.admit_cycle(_OWNER_ID)
            await asyncio.wait_for(feed_runner.started.wait(), timeout=1)
            await asyncio.wait_for(sid_runner.started.wait(), timeout=1)
            feed_runner.calls[0][2].set_retrying(True)
            sid_runner.calls[0][2].set_retrying(True)
            snapshot = supervisor.snapshot()
            await supervisor.heartbeat_cycle(lambda: None)
            feed_runner.calls[0][2].set_retrying(False)
            sid_runner.calls[0][2].set_retrying(False)

            async def stop_heartbeat() -> None:
                return None

            result = await supervisor.shutdown(
                cooperative_grace_sec=30,
                external_stop_deadline_sec=30,
                stop_heartbeat_supervision=stop_heartbeat,
            )

        self.assertEqual(result.finalized, 2)
        self.assertEqual(snapshot.profile, profile.name)
        self.assertEqual(
            snapshot.profile_digest,
            worker_profiles.profile_digest(profile),
        )
        self.assertEqual(
            snapshot.counts_by_domain[grant_control.DomainId.FEED],
            grant_supervisor.GrantCount(1, 1, 0),
        )
        self.assertEqual(
            snapshot.counts_by_domain[grant_control.DomainId.SID],
            grant_supervisor.GrantCount(1, 1, 1),
        )
        with self.assertRaises(TypeError):
            typing.cast(
                "dict[grant_control.DomainId, grant_supervisor.GrantCount]",
                snapshot.counts_by_domain,
            )[grant_control.DomainId.FEED] = grant_supervisor.GrantCount(
                0, 0, 0
            )

        records = [
            call.kwargs["extra"]["json_fields"]
            for call in log_info.call_args_list
        ]
        self.assertTrue(records)
        self.assertTrue(
            {
                "admission",
                "heartbeat",
                "retry_state",
                "count_snapshot",
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
            grant_supervisor.FeedAuthority,
            grant_supervisor.SidAuthority,
            grant_supervisor._AuthoritySlot,
            grant_supervisor._GenerationKey,
            grant_supervisor._ErasedRegisteredDomain,
            grant_supervisor.GrantCount,
            grant_supervisor.SupervisorSnapshot,
            grant_supervisor.ShutdownResult,
        ):
            with self.subTest(value_type=value_type.__name__):
                self.assertTrue(value_type.__dataclass_params__.frozen)
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
            "_discard_exact_generation",
            "heartbeat_cycle",
            "_validate_heartbeat_results",
            "_fail_heartbeat_domain",
            "_reserve_terminal",
            "_finalize_reserved",
            "snapshot",
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
        self.assertNotIn("typing.cast", source)
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

    def test_feed_validator_checks_mapping_shape_not_typed_dict_runtime(
        self,
    ) -> None:
        source = inspect.getsource(_valid_leased_feed)

        self.assertNotIn("isinstance(value, feed_store.LeasedFeed)", source)
        self.assertTrue(_valid_leased_feed(_leased_feed(uuid.UUID(int=30))))
        self.assertFalse(_valid_leased_feed({"id": uuid.UUID(int=30)}))
        malformed = _leased_feed(uuid.UUID(int=31))
        malformed["fencing_token"] = typing.cast("int", "wrong")
        self.assertFalse(_valid_leased_feed(malformed))

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
            (
                "domain_id",
                "authority_kind",
                "grant_type",
                "payload_validator",
                "authority_of",
                "owner_of",
                "fencing_token_of",
                "control",
                "runner",
                "allocation",
                "terminal_decision_for",
            ),
        )

    def test_supervisor_has_no_client_pool_or_sleep_owned_api(self) -> None:
        public_methods = {
            name
            for name, value in vars(grant_supervisor.GrantSupervisor).items()
            if callable(value) and not name.startswith("_")
        }
        self.assertEqual(
            public_methods,
            {"admit_cycle", "heartbeat_cycle", "snapshot", "shutdown"},
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
