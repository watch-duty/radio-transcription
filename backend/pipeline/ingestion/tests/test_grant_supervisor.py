"""Tests for the shared exact-generation grant supervisor."""

from __future__ import annotations

import ast
import asyncio
import datetime
import inspect
import textwrap
import typing
import unittest
import uuid

from backend.pipeline.ingestion import (
    failure_policy,
    grant_control,
    grant_supervisor,
    worker_profiles,
)
from backend.pipeline.storage import feed_store, ingestion_lease_store

_OWNER = uuid.UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
_NOW = datetime.datetime(2026, 7, 16, 12, 0, tzinfo=datetime.UTC)


def _feed_grant(
    feed_id: uuid.UUID,
    fencing_token: int,
) -> feed_store.FeedGrant:
    return feed_store.FeedGrant(feed_id, _OWNER, fencing_token)


def _sid_grant(
    lease_key: str,
    fencing_token: int,
) -> ingestion_lease_store.LeaseGrant:
    return ingestion_lease_store.LeaseGrant(
        source_type=feed_store.SourceType.BCFY_CALLS,
        lease_key=lease_key,
        owner_worker_id=_OWNER,
        fencing_token=fencing_token,
    )


def _feed_payload(
    grant: feed_store.FeedGrant,
) -> feed_store.LeasedFeed:
    return {
        "id": grant.feed_id,
        "name": f"Feed {grant.feed_id}",
        "source_type": feed_store.SourceType.BCFY_CALLS,
        "last_processed_filename": None,
        "last_bookmark_time": None,
        "fencing_token": grant.fencing_token,
        "failure_count": 0,
        "status_reason": None,
        "source_feed_id": "123-456",
    }


def _failure_plan(
    failed: grant_control.RunFailed,
) -> failure_policy.FailurePersistencePlan:
    return failure_policy.FailurePersistencePlan(
        status_reason=failed.status_reason,
        reason=failed.reason,
        treatment=failure_policy.RetryWithoutBudget(
            _NOW + datetime.timedelta(seconds=30)
        ),
    )


def _profile(
    *,
    feed_cap: int = 2,
    feed_budget: int = 2,
    sid_cap: int = 2,
    sid_budget: int = 2,
) -> worker_profiles.WorkerProfile:
    allocations = (
        worker_profiles.DomainAllocation(
            domain_id=grant_control.DomainId.FEED,
            owned_cap=feed_cap,
            claims_per_cycle=feed_budget,
            claims_enabled=True,
        ),
        worker_profiles.DomainAllocation(
            domain_id=grant_control.DomainId.SID,
            owned_cap=sid_cap,
            claims_per_cycle=sid_budget,
            claims_enabled=True,
        ),
    )
    return worker_profiles.validate_worker_profile(
        worker_profiles.WorkerProfile(
            name="test-mixed",
            allocations=allocations,
        )
    )


def _sid_profile() -> worker_profiles.WorkerProfile:
    return worker_profiles.validate_worker_profile(
        worker_profiles.WorkerProfile(
            name="test-sid",
            allocations=(
                worker_profiles.DomainAllocation(
                    domain_id=grant_control.DomainId.SID,
                    owned_cap=2,
                    claims_per_cycle=2,
                    claims_enabled=True,
                ),
            ),
        )
    )


class _Control[GrantT, PayloadT]:
    """Deterministic typed control with explicit I/O barriers."""

    def __init__(self) -> None:
        self.domain_id = grant_control.DomainId.FEED
        self.claim_batches: dict[
            grant_control.ClaimMode,
            list[
                tuple[
                    grant_control.ClaimedGrant[GrantT, PayloadT],
                    ...,
                ]
            ],
        ] = {
            grant_control.ClaimMode.PRIMARY: [],
            grant_control.ClaimMode.RECOVERY: [],
        }
        self.claim_calls: list[
            tuple[grant_control.ClaimMode, uuid.UUID, int]
        ] = []
        self.claim_started = {
            mode: asyncio.Event() for mode in grant_control.ClaimMode
        }
        self.claim_gate: asyncio.Event | None = None
        self.claim_error: BaseException | None = None
        self.heartbeat_dispositions: dict[
            GrantT,
            grant_control.HeartbeatDisposition,
        ] = {}
        self.heartbeat_calls: list[tuple[GrantT, ...]] = []
        self.heartbeat_started = asyncio.Event()
        self.heartbeat_gate: asyncio.Event | None = None
        self.heartbeat_error: BaseException | None = None
        self.finalize_disposition = grant_control.FinalizeDisposition.APPLIED
        self.finalize_calls: list[
            tuple[
                GrantT,
                PayloadT,
                grant_control.TerminalDecision,
            ]
        ] = []
        self.finalize_started = asyncio.Event()
        self.finalize_gate: asyncio.Event | None = None
        self.resist_finalize_cancellation = False
        self.before_finalize: typing.Callable[[], None] | None = None
        self.finalize_error: BaseException | None = None

    def queue_claims(
        self,
        mode: grant_control.ClaimMode,
        *claims: grant_control.ClaimedGrant[GrantT, PayloadT],
    ) -> None:
        self.claim_batches[mode].append(tuple(claims))

    async def claim(
        self,
        mode: grant_control.ClaimMode,
        owner_worker_id: uuid.UUID,
        limit: int,
    ) -> tuple[grant_control.ClaimedGrant[GrantT, PayloadT], ...]:
        self.claim_calls.append((mode, owner_worker_id, limit))
        self.claim_started[mode].set()
        if self.claim_gate is not None:
            await self.claim_gate.wait()
        if self.claim_error is not None:
            raise self.claim_error
        batches = self.claim_batches[mode]
        return batches.pop(0) if batches else ()

    async def heartbeat(
        self,
        grants: typing.Sequence[GrantT],
    ) -> tuple[grant_control.GrantHeartbeat[GrantT], ...]:
        grants = tuple(grants)
        self.heartbeat_calls.append(grants)
        self.heartbeat_started.set()
        if self.heartbeat_gate is not None:
            await self.heartbeat_gate.wait()
        if self.heartbeat_error is not None:
            raise self.heartbeat_error
        return tuple(
            grant_control.GrantHeartbeat(
                grant,
                self.heartbeat_dispositions.get(
                    grant,
                    grant_control.HeartbeatDisposition.RETAINED,
                ),
            )
            for grant in grants
        )

    async def finalize(
        self,
        grant: GrantT,
        payload: PayloadT,
        terminal: grant_control.TerminalDecision,
    ) -> grant_control.FinalizeResult[GrantT]:
        self.finalize_calls.append((grant, payload, terminal))
        self.finalize_started.set()
        if self.before_finalize is not None:
            self.before_finalize()
        if self.finalize_gate is not None:
            try:
                await self.finalize_gate.wait()
            except asyncio.CancelledError:
                if not self.resist_finalize_cancellation:
                    raise
                current = asyncio.current_task()
                assert current is not None
                current.uncancel()
                await self.finalize_gate.wait()
        if self.finalize_error is not None:
            raise self.finalize_error
        return grant_control.FinalizeResult(
            grant,
            self.finalize_disposition,
        )


class _Runner[GrantT, PayloadT]:
    """Controlled runner that acknowledges closure in ``finally``."""

    def __init__(self) -> None:
        self.contexts: dict[GrantT, grant_control.RunContext] = {}
        self.payloads: dict[GrantT, PayloadT] = {}
        self.outcomes: dict[GrantT, grant_control.RunOutcome] = {}
        self.started: dict[GrantT, asyncio.Event] = {}
        self.finished: dict[GrantT, asyncio.Event] = {}
        self.release = asyncio.Event()

    def started_event(self, grant: GrantT) -> asyncio.Event:
        return self.started.setdefault(grant, asyncio.Event())

    def finished_event(self, grant: GrantT) -> asyncio.Event:
        return self.finished.setdefault(grant, asyncio.Event())

    async def run(
        self,
        grant: GrantT,
        payload: PayloadT,
        context: grant_control.RunContext,
    ) -> grant_control.RunOutcome:
        self.contexts[grant] = context
        self.payloads[grant] = payload
        self.started_event(grant).set()
        try:
            await self.release.wait()
            return self.outcomes.get(grant, grant_control.RunCompleted())
        finally:
            self.finished_event(grant).set()


class _CancellationResistantRunner[GrantT, PayloadT](_Runner[GrantT, PayloadT]):
    """Controlled runner that acknowledges cancellation without closing."""

    def __init__(self) -> None:
        super().__init__()
        self.cancellation_seen = asyncio.Event()

    async def run(
        self,
        grant: GrantT,
        payload: PayloadT,
        context: grant_control.RunContext,
    ) -> grant_control.RunOutcome:
        self.contexts[grant] = context
        self.payloads[grant] = payload
        self.started_event(grant).set()
        try:
            while True:
                try:
                    await self.release.wait()
                    return self.outcomes.get(
                        grant,
                        grant_control.RunCompleted(),
                    )
                except asyncio.CancelledError:
                    self.cancellation_seen.set()
        finally:
            self.finished_event(grant).set()


def _feed_registration(
    control: _Control[feed_store.FeedGrant, feed_store.LeasedFeed],
    runner: _Runner[feed_store.FeedGrant, feed_store.LeasedFeed],
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
    control: _Control[
        ingestion_lease_store.LeaseGrant,
        grant_control.ClaimMode,
    ],
    runner: _Runner[
        ingestion_lease_store.LeaseGrant,
        grant_control.ClaimMode,
    ],
) -> grant_supervisor.RegisteredDomain[
    ingestion_lease_store.LeaseGrant,
    grant_control.ClaimMode,
]:
    control.domain_id = grant_control.DomainId.SID
    return grant_supervisor.RegisteredDomain(
        domain_id=grant_control.DomainId.SID,
        control=control,
        runner=runner,
    )


async def _wait(event: asyncio.Event) -> None:
    await asyncio.wait_for(event.wait(), timeout=1)


class TestGrantSupervisor(unittest.IsolatedAsyncioTestCase):
    """Tests for admission, exact lifecycle, and ordered shutdown."""

    def setUp(self) -> None:
        self.supervisors: list[grant_supervisor.GrantSupervisor] = []

    async def asyncTearDown(self) -> None:
        async def stop_heartbeats() -> None:
            return

        for supervisor in self.supervisors:
            await supervisor.shutdown(
                cooperative_grace_sec=0,
                external_stop_deadline_sec=0.1,
                stop_heartbeat_supervision=stop_heartbeats,
            )

    def _supervisor(
        self,
        profile: worker_profiles.WorkerProfile,
        *registrations: object,
    ) -> grant_supervisor.GrantSupervisor:
        supervisor = grant_supervisor.GrantSupervisor(
            profile,
            registrations,
            finalize_concurrency=2,
            failure_planner=_failure_plan,
        )
        self.supervisors.append(supervisor)
        return supervisor

    def test_constructor_requires_exact_profile_domain_registration(
        self,
    ) -> None:
        feed_control: _Control[
            feed_store.FeedGrant,
            feed_store.LeasedFeed,
        ] = _Control()
        feed_runner: _Runner[
            feed_store.FeedGrant,
            feed_store.LeasedFeed,
        ] = _Runner()
        feed = _feed_registration(feed_control, feed_runner)

        with self.assertRaisesRegex(ValueError, "not registered: sid"):
            grant_supervisor.GrantSupervisor(
                _profile(),
                (feed,),
                finalize_concurrency=1,
                failure_planner=_failure_plan,
            )
        with self.assertRaisesRegex(ValueError, "duplicate registered"):
            grant_supervisor.GrantSupervisor(
                worker_profiles.LEGACY_PROFILE,
                (feed, feed),
                finalize_concurrency=1,
                failure_planner=_failure_plan,
            )

    def test_registration_rejects_cross_wired_control_domain(self) -> None:
        feed_control: _Control[
            feed_store.FeedGrant,
            feed_store.LeasedFeed,
        ] = _Control()
        feed_runner: _Runner[
            feed_store.FeedGrant,
            feed_store.LeasedFeed,
        ] = _Runner()

        with self.assertRaisesRegex(
            ValueError,
            "does not match control domain",
        ):
            grant_supervisor.RegisteredDomain(
                domain_id=grant_control.DomainId.SID,
                control=feed_control,
                runner=feed_runner,
            )

    def test_public_annotations_resolve_at_runtime(self) -> None:
        hints = typing.get_type_hints(
            grant_supervisor.GrantSupervisor.admit_cycle
        )

        self.assertIs(hints["owner_worker_id"], uuid.UUID)

    async def test_primary_then_recovery_share_each_domain_budget(
        self,
    ) -> None:
        feed_control: _Control[
            feed_store.FeedGrant,
            feed_store.LeasedFeed,
        ] = _Control()
        feed_runner: _Runner[
            feed_store.FeedGrant,
            feed_store.LeasedFeed,
        ] = _Runner()
        sid_control: _Control[
            ingestion_lease_store.LeaseGrant,
            grant_control.ClaimMode,
        ] = _Control()
        sid_runner: _Runner[
            ingestion_lease_store.LeaseGrant,
            grant_control.ClaimMode,
        ] = _Runner()
        feed_grant = _feed_grant(uuid.uuid4(), 1)
        sid_grant = _sid_grant("123", 1)
        feed_control.queue_claims(
            grant_control.ClaimMode.PRIMARY,
            grant_control.ClaimedGrant(
                feed_grant,
                _feed_payload(feed_grant),
            ),
        )
        sid_control.queue_claims(grant_control.ClaimMode.PRIMARY)
        sid_control.queue_claims(
            grant_control.ClaimMode.RECOVERY,
            grant_control.ClaimedGrant(
                sid_grant,
                grant_control.ClaimMode.RECOVERY,
            ),
        )
        supervisor = self._supervisor(
            _profile(),
            _feed_registration(feed_control, feed_runner),
            _sid_registration(sid_control, sid_runner),
        )

        await supervisor.admit_cycle(_OWNER)
        await _wait(feed_runner.started_event(feed_grant))
        await _wait(sid_runner.started_event(sid_grant))

        self.assertEqual(
            [call[0] for call in feed_control.claim_calls],
            [
                grant_control.ClaimMode.PRIMARY,
                grant_control.ClaimMode.RECOVERY,
            ],
        )
        self.assertEqual(
            [call[2] for call in feed_control.claim_calls],
            [2, 1],
        )
        self.assertEqual(
            [call[2] for call in sid_control.claim_calls],
            [2, 2],
        )
        self.assertEqual(
            supervisor.active_count(grant_control.DomainId.FEED),
            1,
        )
        self.assertEqual(
            supervisor.active_count(grant_control.DomainId.SID),
            1,
        )

    async def test_in_flight_reservations_prevent_concurrent_overclaim(
        self,
    ) -> None:
        feed_control: _Control[
            feed_store.FeedGrant,
            feed_store.LeasedFeed,
        ] = _Control()
        feed_runner: _Runner[
            feed_store.FeedGrant,
            feed_store.LeasedFeed,
        ] = _Runner()
        feed_control.claim_gate = asyncio.Event()
        profile = worker_profiles.WorkerProfile(
            name="feed-only-small",
            allocations=(
                worker_profiles.DomainAllocation(
                    domain_id=grant_control.DomainId.FEED,
                    owned_cap=1,
                    claims_per_cycle=1,
                    claims_enabled=True,
                ),
            ),
        )
        supervisor = self._supervisor(
            worker_profiles.validate_worker_profile(profile),
            _feed_registration(feed_control, feed_runner),
        )

        first = asyncio.create_task(supervisor.admit_cycle(_OWNER))
        await _wait(feed_control.claim_started[grant_control.ClaimMode.PRIMARY])
        await supervisor.admit_cycle(_OWNER)

        self.assertEqual(len(feed_control.claim_calls), 1)
        self.assertEqual(feed_control.claim_calls[0][2], 1)
        feed_control.claim_gate.set()
        await first

    async def test_heartbeat_ineligible_stops_only_one_runner_without_write(
        self,
    ) -> None:
        control: _Control[
            feed_store.FeedGrant,
            feed_store.LeasedFeed,
        ] = _Control()
        runner: _Runner[
            feed_store.FeedGrant,
            feed_store.LeasedFeed,
        ] = _Runner()
        first = _feed_grant(uuid.uuid4(), 1)
        second = _feed_grant(uuid.uuid4(), 1)
        control.queue_claims(
            grant_control.ClaimMode.PRIMARY,
            grant_control.ClaimedGrant(first, _feed_payload(first)),
            grant_control.ClaimedGrant(second, _feed_payload(second)),
        )
        control.heartbeat_dispositions[first] = (
            grant_control.HeartbeatDisposition.INELIGIBLE
        )
        supervisor = self._supervisor(
            worker_profiles.LEGACY_PROFILE,
            _feed_registration(control, runner),
        )
        await supervisor.admit_cycle(_OWNER)
        await _wait(runner.started_event(first))
        await _wait(runner.started_event(second))

        dispatches = 0

        def dispatched() -> None:
            nonlocal dispatches
            dispatches += 1

        await supervisor.heartbeat_cycle(dispatched)

        self.assertEqual(dispatches, 1)
        self.assertTrue(runner.contexts[first].stop_requested.is_set())
        self.assertFalse(runner.contexts[first].grant_lost.is_set())
        self.assertFalse(runner.contexts[second].stop_requested.is_set())
        runner.release.set()
        await _wait(runner.finished_event(first))
        await _wait(runner.finished_event(second))
        await _wait(control.finalize_started)
        self.assertEqual(
            [call[0] for call in control.finalize_calls],
            [second],
        )

    async def test_heartbeat_loss_signals_exact_grant_without_finalize(
        self,
    ) -> None:
        control: _Control[
            ingestion_lease_store.LeaseGrant,
            grant_control.ClaimMode,
        ] = _Control()
        runner: _Runner[
            ingestion_lease_store.LeaseGrant,
            grant_control.ClaimMode,
        ] = _Runner()
        grant = _sid_grant("123", 1)
        control.queue_claims(
            grant_control.ClaimMode.PRIMARY,
            grant_control.ClaimedGrant(
                grant,
                grant_control.ClaimMode.PRIMARY,
            ),
        )
        control.heartbeat_dispositions[grant] = (
            grant_control.HeartbeatDisposition.LOST
        )
        supervisor = self._supervisor(
            _sid_profile(),
            _sid_registration(control, runner),
        )
        await supervisor.admit_cycle(_OWNER)
        await _wait(runner.started_event(grant))

        await supervisor.heartbeat_cycle(lambda: None)

        self.assertTrue(runner.contexts[grant].stop_requested.is_set())
        self.assertTrue(runner.contexts[grant].grant_lost.is_set())
        runner.release.set()
        await _wait(runner.finished_event(grant))
        self.assertEqual(control.finalize_calls, [])

    async def test_heartbeat_uncertainty_fails_closed_without_finalize(
        self,
    ) -> None:
        control: _Control[
            ingestion_lease_store.LeaseGrant,
            grant_control.ClaimMode,
        ] = _Control()
        runner: _Runner[
            ingestion_lease_store.LeaseGrant,
            grant_control.ClaimMode,
        ] = _Runner()
        grant = _sid_grant("123", 1)
        control.queue_claims(
            grant_control.ClaimMode.PRIMARY,
            grant_control.ClaimedGrant(
                grant,
                grant_control.ClaimMode.PRIMARY,
            ),
        )
        control.heartbeat_error = RuntimeError("database unavailable")
        supervisor = self._supervisor(
            _sid_profile(),
            _sid_registration(control, runner),
        )
        await supervisor.admit_cycle(_OWNER)
        await _wait(runner.started_event(grant))

        await supervisor.heartbeat_cycle(lambda: None)

        self.assertFalse(supervisor.admission_enabled)
        self.assertTrue(runner.contexts[grant].stop_requested.is_set())
        self.assertTrue(runner.contexts[grant].grant_lost.is_set())
        runner.release.set()
        await _wait(runner.finished_event(grant))
        self.assertEqual(control.finalize_calls, [])

    async def test_stale_heartbeat_failure_cannot_poison_successor(
        self,
    ) -> None:
        control: _Control[
            feed_store.FeedGrant,
            feed_store.LeasedFeed,
        ] = _Control()
        runner: _CancellationResistantRunner[
            feed_store.FeedGrant,
            feed_store.LeasedFeed,
        ] = _CancellationResistantRunner()
        feed_id = uuid.uuid4()
        first = _feed_grant(feed_id, 1)
        second = _feed_grant(feed_id, 2)
        control.queue_claims(
            grant_control.ClaimMode.PRIMARY,
            grant_control.ClaimedGrant(first, _feed_payload(first)),
        )
        supervisor = self._supervisor(
            worker_profiles.LEGACY_PROFILE,
            _feed_registration(control, runner),
        )
        await supervisor.admit_cycle(_OWNER)
        await _wait(runner.started_event(first))

        control.heartbeat_gate = asyncio.Event()
        control.heartbeat_error = RuntimeError("stale heartbeat failed")
        heartbeat = asyncio.create_task(
            supervisor.heartbeat_cycle(lambda: None)
        )
        await _wait(control.heartbeat_started)

        control.queue_claims(
            grant_control.ClaimMode.PRIMARY,
            grant_control.ClaimedGrant(second, _feed_payload(second)),
        )
        await supervisor.admit_cycle(_OWNER)
        await _wait(runner.started_event(second))
        control.heartbeat_gate.set()
        await heartbeat

        self.assertTrue(supervisor.admission_enabled)
        self.assertIsNone(supervisor.integrity_failure)
        self.assertFalse(runner.contexts[second].stop_requested.is_set())
        self.assertEqual(
            next(iter(supervisor._registry.values())).claim.grant,
            second,
        )
        runner.release.set()
        await _wait(runner.finished_event(first))
        await _wait(runner.finished_event(second))

    async def test_completed_runner_finalizes_exactly_once(
        self,
    ) -> None:
        control: _Control[
            feed_store.FeedGrant,
            feed_store.LeasedFeed,
        ] = _Control()
        runner: _Runner[
            feed_store.FeedGrant,
            feed_store.LeasedFeed,
        ] = _Runner()
        grant = _feed_grant(uuid.uuid4(), 1)
        control.queue_claims(
            grant_control.ClaimMode.PRIMARY,
            grant_control.ClaimedGrant(grant, _feed_payload(grant)),
        )
        control.finalize_gate = asyncio.Event()
        supervisor = self._supervisor(
            worker_profiles.LEGACY_PROFILE,
            _feed_registration(control, runner),
        )
        await supervisor.admit_cycle(_OWNER)
        await _wait(runner.started_event(grant))
        runner.release.set()
        await _wait(control.finalize_started)

        await supervisor.heartbeat_cycle(lambda: None)
        self.assertEqual(len(control.finalize_calls), 1)
        self.assertIsInstance(
            control.finalize_calls[0][2],
            grant_control.NeutralRelease,
        )
        self.assertEqual(control.heartbeat_calls, [])
        control.finalize_gate.set()

    async def test_failed_runner_uses_one_preclassified_plan(
        self,
    ) -> None:
        control: _Control[
            feed_store.FeedGrant,
            feed_store.LeasedFeed,
        ] = _Control()
        runner: _Runner[
            feed_store.FeedGrant,
            feed_store.LeasedFeed,
        ] = _Runner()
        grant = _feed_grant(uuid.uuid4(), 1)
        control.queue_claims(
            grant_control.ClaimMode.PRIMARY,
            grant_control.ClaimedGrant(grant, _feed_payload(grant)),
        )
        runner.outcomes[grant] = grant_control.RunFailed(
            feed_store.FeedStatusReason.SYSTEM_COLLECTOR_ERROR,
            "download exhausted",
        )
        supervisor = self._supervisor(
            worker_profiles.LEGACY_PROFILE,
            _feed_registration(control, runner),
        )
        await supervisor.admit_cycle(_OWNER)
        await _wait(runner.started_event(grant))
        runner.release.set()
        await _wait(control.finalize_started)

        terminal = control.finalize_calls[0][2]
        self.assertIsInstance(
            terminal,
            failure_policy.FailurePersistencePlan,
        )
        assert isinstance(terminal, failure_policy.FailurePersistencePlan)
        self.assertEqual(
            terminal.status_reason,
            feed_store.FeedStatusReason.SYSTEM_COLLECTOR_ERROR,
        )
        self.assertIsInstance(
            terminal.treatment,
            failure_policy.RetryWithoutBudget,
        )

    async def test_finalize_loss_discards_generation_and_signals_loss(
        self,
    ) -> None:
        control: _Control[
            ingestion_lease_store.LeaseGrant,
            grant_control.ClaimMode,
        ] = _Control()
        runner: _Runner[
            ingestion_lease_store.LeaseGrant,
            grant_control.ClaimMode,
        ] = _Runner()
        grant = _sid_grant("123", 1)
        control.queue_claims(
            grant_control.ClaimMode.PRIMARY,
            grant_control.ClaimedGrant(
                grant,
                grant_control.ClaimMode.PRIMARY,
            ),
        )
        control.finalize_disposition = grant_control.FinalizeDisposition.LOST
        supervisor = self._supervisor(
            _sid_profile(),
            _sid_registration(control, runner),
        )
        await supervisor.admit_cycle(_OWNER)
        await _wait(runner.started_event(grant))
        runner.release.set()
        await _wait(control.finalize_started)
        await _wait(runner.finished_event(grant))

        self.assertTrue(runner.contexts[grant].grant_lost.is_set())

    async def test_finalize_uncertainty_is_not_retried(
        self,
    ) -> None:
        control: _Control[
            ingestion_lease_store.LeaseGrant,
            grant_control.ClaimMode,
        ] = _Control()
        runner: _Runner[
            ingestion_lease_store.LeaseGrant,
            grant_control.ClaimMode,
        ] = _Runner()
        grant = _sid_grant("123", 1)
        control.queue_claims(
            grant_control.ClaimMode.PRIMARY,
            grant_control.ClaimedGrant(
                grant,
                grant_control.ClaimMode.PRIMARY,
            ),
        )
        control.finalize_error = RuntimeError("connection lost")
        supervisor = self._supervisor(
            _sid_profile(),
            _sid_registration(control, runner),
        )
        await supervisor.admit_cycle(_OWNER)
        await _wait(runner.started_event(grant))
        runner.release.set()
        await _wait(control.finalize_started)
        await _wait(runner.finished_event(grant))

        self.assertEqual(len(control.finalize_calls), 1)
        self.assertFalse(supervisor.admission_enabled)
        self.assertTrue(supervisor.integrity_failure_event.is_set())

    async def test_shutdown_stops_heartbeats_before_exact_release(
        self,
    ) -> None:
        control: _Control[
            feed_store.FeedGrant,
            feed_store.LeasedFeed,
        ] = _Control()
        runner: _Runner[
            feed_store.FeedGrant,
            feed_store.LeasedFeed,
        ] = _Runner()
        grant = _feed_grant(uuid.uuid4(), 1)
        control.queue_claims(
            grant_control.ClaimMode.PRIMARY,
            grant_control.ClaimedGrant(grant, _feed_payload(grant)),
        )
        supervisor = self._supervisor(
            worker_profiles.LEGACY_PROFILE,
            _feed_registration(control, runner),
        )
        await supervisor.admit_cycle(_OWNER)
        await _wait(runner.started_event(grant))
        heartbeat_stopped = asyncio.Event()

        def require_stopped() -> None:
            self.assertTrue(heartbeat_stopped.is_set())

        control.before_finalize = require_stopped

        async def stop_heartbeats() -> None:
            heartbeat_stopped.set()

        shutdown_task = asyncio.create_task(
            supervisor.shutdown(
                cooperative_grace_sec=1,
                external_stop_deadline_sec=1,
                stop_heartbeat_supervision=stop_heartbeats,
            )
        )
        await _wait(runner.contexts[grant].stop_requested)
        runner.release.set()
        result = await shutdown_task
        self.supervisors.remove(supervisor)

        self.assertIsNone(result)
        self.assertEqual(len(control.finalize_calls), 1)

    async def test_shutdown_waits_for_already_in_flight_finalization(
        self,
    ) -> None:
        control: _Control[
            feed_store.FeedGrant,
            feed_store.LeasedFeed,
        ] = _Control()
        runner: _Runner[
            feed_store.FeedGrant,
            feed_store.LeasedFeed,
        ] = _Runner()
        grant = _feed_grant(uuid.uuid4(), 1)
        control.queue_claims(
            grant_control.ClaimMode.PRIMARY,
            grant_control.ClaimedGrant(grant, _feed_payload(grant)),
        )
        control.finalize_gate = asyncio.Event()
        supervisor = self._supervisor(
            worker_profiles.LEGACY_PROFILE,
            _feed_registration(control, runner),
        )
        await supervisor.admit_cycle(_OWNER)
        await _wait(runner.started_event(grant))
        runner.release.set()
        await _wait(control.finalize_started)
        heartbeat_stopped = asyncio.Event()

        async def stop_heartbeats() -> None:
            heartbeat_stopped.set()

        shutdown_task = asyncio.create_task(
            supervisor.shutdown(
                cooperative_grace_sec=1,
                external_stop_deadline_sec=1,
                stop_heartbeat_supervision=stop_heartbeats,
            )
        )
        await _wait(heartbeat_stopped)
        self.assertFalse(shutdown_task.done())

        control.finalize_gate.set()
        result = await shutdown_task
        self.supervisors.remove(supervisor)

        self.assertIsNone(result)

    async def test_shutdown_is_bounded_when_finalizer_resists_cancellation(
        self,
    ) -> None:
        control: _Control[
            feed_store.FeedGrant,
            feed_store.LeasedFeed,
        ] = _Control()
        runner: _Runner[
            feed_store.FeedGrant,
            feed_store.LeasedFeed,
        ] = _Runner()
        grant = _feed_grant(uuid.uuid4(), 1)
        control.queue_claims(
            grant_control.ClaimMode.PRIMARY,
            grant_control.ClaimedGrant(grant, _feed_payload(grant)),
        )
        control.finalize_gate = asyncio.Event()
        control.resist_finalize_cancellation = True
        supervisor = self._supervisor(
            worker_profiles.LEGACY_PROFILE,
            _feed_registration(control, runner),
        )
        await supervisor.admit_cycle(_OWNER)
        await _wait(runner.started_event(grant))
        runner.release.set()
        await _wait(control.finalize_started)

        async def stop_heartbeats() -> None:
            return

        with self.assertRaises(grant_supervisor.SupervisorNotDrainedError):
            await asyncio.wait_for(
                supervisor.shutdown(
                    cooperative_grace_sec=0,
                    external_stop_deadline_sec=0,
                    stop_heartbeat_supervision=stop_heartbeats,
                ),
                timeout=1,
            )

        finalization_tasks = tuple(supervisor._finalization_tasks)
        self.assertEqual(len(finalization_tasks), 1)
        control.finalize_gate.set()
        await asyncio.gather(*finalization_tasks)
        self.assertEqual(supervisor._registry, {})

    async def test_shutdown_tracks_superseded_in_flight_finalizer(
        self,
    ) -> None:
        control: _Control[
            feed_store.FeedGrant,
            feed_store.LeasedFeed,
        ] = _Control()
        runner: _Runner[
            feed_store.FeedGrant,
            feed_store.LeasedFeed,
        ] = _Runner()
        feed_id = uuid.uuid4()
        first = _feed_grant(feed_id, 1)
        second = _feed_grant(feed_id, 2)
        control.queue_claims(
            grant_control.ClaimMode.PRIMARY,
            grant_control.ClaimedGrant(first, _feed_payload(first)),
        )
        control.finalize_gate = asyncio.Event()
        control.resist_finalize_cancellation = True
        supervisor = self._supervisor(
            worker_profiles.LEGACY_PROFILE,
            _feed_registration(control, runner),
        )
        await supervisor.admit_cycle(_OWNER)
        await _wait(runner.started_event(first))
        runner.release.set()
        await _wait(control.finalize_started)

        runner.release.clear()
        runner.outcomes[second] = grant_control.RunLost()
        control.queue_claims(
            grant_control.ClaimMode.PRIMARY,
            grant_control.ClaimedGrant(second, _feed_payload(second)),
        )
        await supervisor.admit_cycle(_OWNER)
        await _wait(runner.started_event(second))
        runner.release.set()
        await _wait(runner.finished_event(second))
        self.assertEqual(supervisor._registry, {})

        async def stop_heartbeats() -> None:
            return

        with self.assertRaises(grant_supervisor.SupervisorNotDrainedError):
            await asyncio.wait_for(
                supervisor.shutdown(
                    cooperative_grace_sec=0,
                    external_stop_deadline_sec=0,
                    stop_heartbeat_supervision=stop_heartbeats,
                ),
                timeout=1,
            )

        finalization_tasks = tuple(supervisor._finalization_tasks)
        self.assertEqual(len(finalization_tasks), 1)
        control.finalize_gate.set()
        await asyncio.gather(*finalization_tasks)

    async def test_shutdown_rejects_unsettled_claim_mutation(self) -> None:
        control: _Control[
            feed_store.FeedGrant,
            feed_store.LeasedFeed,
        ] = _Control()
        runner: _Runner[
            feed_store.FeedGrant,
            feed_store.LeasedFeed,
        ] = _Runner()
        control.claim_gate = asyncio.Event()
        supervisor = self._supervisor(
            worker_profiles.LEGACY_PROFILE,
            _feed_registration(control, runner),
        )
        admission = asyncio.create_task(supervisor.admit_cycle(_OWNER))
        await _wait(control.claim_started[grant_control.ClaimMode.PRIMARY])
        heartbeat_stopped = asyncio.Event()

        async def stop_heartbeats() -> None:
            heartbeat_stopped.set()

        with self.assertRaises(grant_supervisor.SupervisorNotDrainedError):
            await supervisor.shutdown(
                cooperative_grace_sec=0,
                external_stop_deadline_sec=0,
                stop_heartbeat_supervision=stop_heartbeats,
            )

        self.assertFalse(heartbeat_stopped.is_set())
        control.claim_gate.set()
        await admission

    async def test_shutdown_rejects_cancellation_resistant_runner(
        self,
    ) -> None:
        control: _Control[
            feed_store.FeedGrant,
            feed_store.LeasedFeed,
        ] = _Control()
        runner: _CancellationResistantRunner[
            feed_store.FeedGrant,
            feed_store.LeasedFeed,
        ] = _CancellationResistantRunner()
        grant = _feed_grant(uuid.uuid4(), 1)
        control.queue_claims(
            grant_control.ClaimMode.PRIMARY,
            grant_control.ClaimedGrant(grant, _feed_payload(grant)),
        )
        supervisor = self._supervisor(
            worker_profiles.LEGACY_PROFILE,
            _feed_registration(control, runner),
        )
        await supervisor.admit_cycle(_OWNER)
        await _wait(runner.started_event(grant))
        heartbeat_stopped = asyncio.Event()

        async def stop_heartbeats() -> None:
            heartbeat_stopped.set()

        with self.assertRaises(grant_supervisor.SupervisorNotDrainedError):
            await supervisor.shutdown(
                cooperative_grace_sec=0,
                external_stop_deadline_sec=0,
                stop_heartbeat_supervision=stop_heartbeats,
            )

        self.assertTrue(heartbeat_stopped.is_set())
        self.assertEqual(control.finalize_calls, [])
        await _wait(runner.cancellation_seen)
        runner.release.set()
        await _wait(runner.finished_event(grant))
        self.assertEqual(supervisor._registry, {})

    async def test_higher_fence_successor_uses_one_ownership_slot(
        self,
    ) -> None:
        control: _Control[
            feed_store.FeedGrant,
            feed_store.LeasedFeed,
        ] = _Control()
        runner: _CancellationResistantRunner[
            feed_store.FeedGrant,
            feed_store.LeasedFeed,
        ] = _CancellationResistantRunner()
        feed_id = uuid.uuid4()
        first = _feed_grant(feed_id, 1)
        second = _feed_grant(feed_id, 2)
        control.queue_claims(
            grant_control.ClaimMode.PRIMARY,
            grant_control.ClaimedGrant(first, _feed_payload(first)),
        )
        control.finalize_disposition = grant_control.FinalizeDisposition.LOST
        supervisor = self._supervisor(
            worker_profiles.LEGACY_PROFILE,
            _feed_registration(control, runner),
        )
        await supervisor.admit_cycle(_OWNER)
        await _wait(runner.started_event(first))

        control.queue_claims(
            grant_control.ClaimMode.PRIMARY,
            grant_control.ClaimedGrant(second, _feed_payload(second)),
        )
        await supervisor.admit_cycle(_OWNER)
        await _wait(runner.started_event(second))
        await _wait(runner.cancellation_seen)

        self.assertTrue(runner.contexts[first].stop_requested.is_set())
        self.assertTrue(runner.contexts[first].grant_lost.is_set())
        self.assertEqual(
            supervisor.active_count(grant_control.DomainId.FEED),
            1,
        )
        self.assertEqual(
            supervisor._owned_by_domain[grant_control.DomainId.FEED],
            1,
        )
        self.assertFalse(runner.contexts[second].stop_requested.is_set())
        self.assertEqual(
            next(iter(supervisor._registry.values())).claim.grant,
            second,
        )

        runner.release.set()
        await _wait(runner.finished_event(first))
        await _wait(runner.finished_event(second))
        await _wait(control.finalize_started)
        self.assertEqual([call[0] for call in control.finalize_calls], [second])

    async def test_equal_fence_collision_fails_closed(self) -> None:
        control: _Control[
            feed_store.FeedGrant,
            feed_store.LeasedFeed,
        ] = _Control()
        runner: _Runner[
            feed_store.FeedGrant,
            feed_store.LeasedFeed,
        ] = _Runner()
        feed_id = uuid.uuid4()
        first = _feed_grant(feed_id, 1)
        duplicate = _feed_grant(feed_id, 1)
        control.queue_claims(
            grant_control.ClaimMode.PRIMARY,
            grant_control.ClaimedGrant(first, _feed_payload(first)),
        )
        supervisor = self._supervisor(
            worker_profiles.LEGACY_PROFILE,
            _feed_registration(control, runner),
        )
        await supervisor.admit_cycle(_OWNER)
        await _wait(runner.started_event(first))
        control.queue_claims(
            grant_control.ClaimMode.PRIMARY,
            grant_control.ClaimedGrant(
                duplicate,
                _feed_payload(duplicate),
            ),
        )

        with self.assertRaises(grant_control.GrantControlIntegrityError):
            await supervisor.admit_cycle(_OWNER)

        self.assertFalse(supervisor.admission_enabled)
        self.assertEqual(
            next(iter(supervisor._registry.values())).claim.grant,
            first,
        )

    async def test_same_unit_generations_in_one_batch_fail_atomically(
        self,
    ) -> None:
        control: _Control[
            feed_store.FeedGrant,
            feed_store.LeasedFeed,
        ] = _Control()
        runner: _Runner[
            feed_store.FeedGrant,
            feed_store.LeasedFeed,
        ] = _Runner()
        feed_id = uuid.uuid4()
        first = _feed_grant(feed_id, 1)
        second = _feed_grant(feed_id, 2)
        control.queue_claims(
            grant_control.ClaimMode.PRIMARY,
            grant_control.ClaimedGrant(first, _feed_payload(first)),
            grant_control.ClaimedGrant(second, _feed_payload(second)),
        )
        supervisor = self._supervisor(
            worker_profiles.LEGACY_PROFILE,
            _feed_registration(control, runner),
        )

        with self.assertRaises(grant_control.GrantControlIntegrityError):
            await supervisor.admit_cycle(_OWNER)

        self.assertFalse(supervisor.admission_enabled)
        self.assertEqual(supervisor._registry, {})
        self.assertEqual(runner.contexts, {})
        self.assertTrue(
            all(count == 0 for count in supervisor._owned_by_domain.values())
        )
        self.assertTrue(
            all(count == 0 for count in supervisor._reserved_by_domain.values())
        )

    async def test_superseded_finalization_cannot_remove_successor(
        self,
    ) -> None:
        control: _Control[
            feed_store.FeedGrant,
            feed_store.LeasedFeed,
        ] = _Control()
        runner: _Runner[
            feed_store.FeedGrant,
            feed_store.LeasedFeed,
        ] = _Runner()
        feed_id = uuid.uuid4()
        first = _feed_grant(feed_id, 1)
        second = _feed_grant(feed_id, 2)
        control.finalize_gate = asyncio.Event()
        control.queue_claims(
            grant_control.ClaimMode.PRIMARY,
            grant_control.ClaimedGrant(first, _feed_payload(first)),
        )
        supervisor = self._supervisor(
            worker_profiles.LEGACY_PROFILE,
            _feed_registration(control, runner),
        )
        await supervisor.admit_cycle(_OWNER)
        await _wait(runner.started_event(first))
        first_managed = next(iter(supervisor._registry.values()))
        runner.release.set()
        await _wait(control.finalize_started)
        first_finalization = first_managed.finalization_task
        self.assertIsNotNone(first_finalization)

        runner.release = asyncio.Event()
        control.queue_claims(
            grant_control.ClaimMode.PRIMARY,
            grant_control.ClaimedGrant(second, _feed_payload(second)),
        )
        await supervisor.admit_cycle(_OWNER)
        await _wait(runner.started_event(second))
        control.finalize_gate.set()
        assert first_finalization is not None
        await first_finalization

        self.assertTrue(supervisor.admission_enabled)
        self.assertEqual(
            next(iter(supervisor._registry.values())).claim.grant,
            second,
        )
        self.assertFalse(runner.contexts[second].stop_requested.is_set())

    async def test_active_count_is_store_free_and_domain_scoped(self) -> None:
        control: _Control[
            ingestion_lease_store.LeaseGrant,
            grant_control.ClaimMode,
        ] = _Control()
        runner: _Runner[
            ingestion_lease_store.LeaseGrant,
            grant_control.ClaimMode,
        ] = _Runner()
        grant = _sid_grant("123", 1)
        control.queue_claims(
            grant_control.ClaimMode.PRIMARY,
            grant_control.ClaimedGrant(
                grant,
                grant_control.ClaimMode.PRIMARY,
            ),
        )
        supervisor = self._supervisor(
            _sid_profile(),
            _sid_registration(control, runner),
        )
        await supervisor.admit_cycle(_OWNER)
        await _wait(runner.started_event(grant))

        self.assertEqual(
            supervisor.active_count(grant_control.DomainId.SID),
            1,
        )
        self.assertEqual(
            supervisor.active_count(grant_control.DomainId.FEED),
            0,
        )

    def test_lifecycle_algorithm_has_no_feed_or_sid_dispatch(self) -> None:
        for method_name in (
            "admit_cycle",
            "_run_managed",
            "_finalize_exact",
            "heartbeat_cycle",
            "shutdown",
        ):
            tree = ast.parse(
                textwrap.dedent(
                    inspect.getsource(
                        getattr(
                            grant_supervisor.GrantSupervisor,
                            method_name,
                        )
                    )
                )
            )
            domain_members = {
                node.attr
                for node in ast.walk(tree)
                if isinstance(node, ast.Attribute)
                and node.attr in {"FEED", "SID"}
            }
            self.assertEqual(domain_members, set(), method_name)
