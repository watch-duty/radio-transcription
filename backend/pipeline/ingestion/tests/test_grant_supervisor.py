"""Tests for the shared exact-generation grant supervisor."""

from __future__ import annotations

import ast
import asyncio
import datetime
import inspect
import textwrap
import types
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


def _is_feed_payload(value: object) -> typing.TypeGuard[feed_store.LeasedFeed]:
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
    if set(mapping) < required:
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
            or isinstance(
                mapping["status_reason"],
                feed_store.FeedStatusReason,
            )
        )
        and (
            mapping["source_feed_id"] is None
            or isinstance(mapping["source_feed_id"], str)
        )
    )


def _is_claim_mode(
    value: object,
) -> typing.TypeGuard[grant_control.ClaimMode]:
    return isinstance(value, grant_control.ClaimMode)


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
    process_cap: int | None = None,
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
            process_owned_cap=(
                feed_cap + sid_cap if process_cap is None else process_cap
            ),
            allocations=allocations,
        )
    )


def _sid_profile() -> worker_profiles.WorkerProfile:
    return worker_profiles.validate_worker_profile(
        worker_profiles.WorkerProfile(
            name="test-sid",
            process_owned_cap=2,
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


def _feed_registration(
    control: _Control[feed_store.FeedGrant, feed_store.LeasedFeed],
    runner: _Runner[feed_store.FeedGrant, feed_store.LeasedFeed],
) -> grant_supervisor.RegisteredDomain[
    feed_store.FeedGrant,
    feed_store.LeasedFeed,
]:
    return grant_supervisor.RegisteredDomain(
        domain_id=grant_control.DomainId.FEED,
        grant_type=feed_store.FeedGrant,
        payload_validator=_is_feed_payload,
        authority_of=lambda grant: grant_supervisor.FeedAuthority(
            grant.feed_id
        ),
        owner_of=lambda grant: grant.owner_worker_id,
        fencing_token_of=lambda grant: grant.fencing_token,
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
    return grant_supervisor.RegisteredDomain(
        domain_id=grant_control.DomainId.SID,
        grant_type=ingestion_lease_store.LeaseGrant,
        payload_validator=_is_claim_mode,
        authority_of=lambda grant: grant_supervisor.SidAuthority(
            grant.source_type.value,
            grant.lease_key,
        ),
        owner_of=lambda grant: grant.owner_worker_id,
        fencing_token_of=lambda grant: grant.fencing_token,
        control=control,
        runner=runner,
    )


async def _wait(event: asyncio.Event) -> None:
    await asyncio.wait_for(event.wait(), timeout=1)


def _assign_snapshot_count(
    mapping: typing.Mapping[
        grant_control.DomainId,
        grant_supervisor.GrantCount,
    ],
) -> None:
    writable = typing.cast(
        "dict[grant_control.DomainId, grant_supervisor.GrantCount]",
        mapping,
    )
    writable[grant_control.DomainId.SID] = grant_supervisor.GrantCount(active=0)


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
            supervisor.snapshot()
            .counts_by_domain[grant_control.DomainId.FEED]
            .active,
            1,
        )
        self.assertEqual(
            supervisor.snapshot()
            .counts_by_domain[grant_control.DomainId.SID]
            .active,
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
            process_owned_cap=1,
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

    async def test_invalid_erased_payload_fails_closed_before_runner(
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
        grant = _feed_grant(uuid.uuid4(), 1)
        invalid = typing.cast("feed_store.LeasedFeed", {"id": grant.feed_id})
        feed_control.queue_claims(
            grant_control.ClaimMode.PRIMARY,
            grant_control.ClaimedGrant(grant, invalid),
        )
        supervisor = self._supervisor(
            worker_profiles.LEGACY_PROFILE,
            _feed_registration(feed_control, feed_runner),
        )

        with self.assertRaisesRegex(
            grant_control.GrantControlIntegrityError,
            "invalid runner payload",
        ):
            await supervisor.admit_cycle(_OWNER)

        self.assertFalse(supervisor.admission_enabled)
        self.assertTrue(supervisor.integrity_failure_event.is_set())
        self.assertNotIn(grant, feed_runner.contexts)
        self.assertEqual(
            supervisor.snapshot()
            .counts_by_domain[grant_control.DomainId.FEED]
            .active,
            0,
        )

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

        self.assertEqual(result.finalized, 1)
        self.assertEqual(result.abandoned, 0)
        self.assertEqual(result.undrained, 0)
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

        self.assertEqual(result.finalized, 1)
        self.assertEqual(result.abandoned, 0)
        self.assertEqual(result.undrained, 0)

    async def test_stale_generation_key_cannot_remove_successor(
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
        control.finalize_disposition = grant_control.FinalizeDisposition.LOST
        supervisor = self._supervisor(
            worker_profiles.LEGACY_PROFILE,
            _feed_registration(control, runner),
        )
        await supervisor.admit_cycle(_OWNER)
        await _wait(runner.started_event(first))
        old_key = next(iter(supervisor._registry))
        runner.release.set()
        await _wait(control.finalize_started)
        await _wait(runner.finished_event(first))

        runner.release = asyncio.Event()
        control.finalize_started = asyncio.Event()
        control.finalize_disposition = grant_control.FinalizeDisposition.APPLIED
        control.queue_claims(
            grant_control.ClaimMode.PRIMARY,
            grant_control.ClaimedGrant(second, _feed_payload(second)),
        )
        await supervisor.admit_cycle(_OWNER)
        await _wait(runner.started_event(second))

        supervisor._handle_runner_closed(old_key)

        self.assertEqual(
            supervisor.snapshot()
            .counts_by_domain[grant_control.DomainId.FEED]
            .active,
            1,
        )
        self.assertFalse(runner.contexts[second].stop_requested.is_set())

    async def test_snapshot_is_immutable_and_store_free(self) -> None:
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

        snapshot = supervisor.snapshot()

        self.assertEqual(snapshot.profile, "test-sid")
        self.assertIsInstance(snapshot.counts_by_domain, types.MappingProxyType)
        self.assertEqual(
            snapshot.counts_by_domain[grant_control.DomainId.SID],
            grant_supervisor.GrantCount(active=1),
        )
        with self.assertRaises(TypeError):
            _assign_snapshot_count(snapshot.counts_by_domain)

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
