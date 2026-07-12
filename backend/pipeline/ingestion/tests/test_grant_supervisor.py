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
    _outcome: grant_control.RunOutcome,
) -> grant_control.TerminalDecision:
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
        return self.results[mode]

    async def heartbeat(
        self,
        grants: typing.Sequence[GrantT],
    ) -> tuple[grant_control.GrantHeartbeat[GrantT], ...]:
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
        terminal: grant_control.TerminalDecision,
    ) -> grant_control.FinalizeResult[GrantT]:
        self.finalize_calls.append((grant, terminal))
        return grant_control.FinalizeResult(
            grant,
            grant_control.FinalizeDisposition.APPLIED,
            None,
        )


class _ControlledRunner[GrantT, PayloadT]:
    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.finish = asyncio.Event()
        self.calls: list[tuple[GrantT, PayloadT, grant_control.RunContext]] = []
        self.outcome: grant_control.RunOutcome = grant_control.RunCompleted()

    async def run(
        self,
        grant: GrantT,
        payload: PayloadT,
        context: grant_control.RunContext,
    ) -> grant_control.RunOutcome:
        self.calls.append((grant, payload, context))
        self.started.set()
        await self.finish.wait()
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
        for managed in tuple(supervisor._registry.values()):
            task = managed.root_task
            if task is not None and not task.done():
                task.cancel()
            if task is not None:
                await task

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
                (grant_control.ClaimMode.PRIMARY, 1),
            ],
        )
        self.assertEqual(
            sid_control.claim_calls,
            [
                (grant_control.ClaimMode.PRIMARY, 1),
                (grant_control.ClaimMode.PRIMARY, 1),
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
        feed_runner.finish.set()
        await supervisor.admit_cycle(_OWNER_ID)
        old_key = next(iter(supervisor._registry))
        old_managed = supervisor._registry[old_key]
        await asyncio.wait_for(old_managed.runner_closed.wait(), timeout=1)
        old_task = old_managed.root_task
        self.assertIsNotNone(old_task)
        assert old_task is not None
        await old_task
        self.assertTrue(supervisor._discard_exact_generation(old_key))

        successor = feed_store.FeedGrant(feed_id, _OWNER_ID, 2)
        feed_control.results[grant_control.ClaimMode.PRIMARY] = (
            _claim(successor, _leased_feed(feed_id, fencing_token=2)),
        )
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


if __name__ == "__main__":
    unittest.main()
