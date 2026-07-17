"""Exact-generation supervision shared by ingestion grant domains."""

from __future__ import annotations

import asyncio
import dataclasses
import enum
import logging
import typing
import uuid  # noqa: TC003

from backend.pipeline.ingestion import (
    failure_policy,
    grant_control,
    worker_profiles,
)

logger = logging.getLogger(__name__)


type FailurePlanner = typing.Callable[
    [grant_control.RunFailed],
    failure_policy.FailurePersistencePlan,
]


class SupervisorNotDrainedError(RuntimeError):
    """Supervisor-owned work may still use shared runtime resources."""


@dataclasses.dataclass(frozen=True, slots=True)
class RegisteredDomain[
    GrantT: grant_control.ExactGrant[typing.Hashable],
    PayloadT,
]:
    """Typed control and runner for one selected ownership domain.

    The registration is the sole type-erasure boundary. The supervisor owns
    the lifecycle algorithm; controls own storage translation and runners own
    source work.

    Attributes:
        domain_id: Static ownership domain selected by the worker profile.
        control: Authoritative claim, heartbeat, and finalization adapter.
        runner: Source-specific work under one exact grant.
    """

    domain_id: grant_control.DomainId
    control: grant_control.GrantControl[GrantT, PayloadT]
    runner: grant_control.GrantRunner[GrantT, PayloadT]

    def __post_init__(self) -> None:
        if self.control.domain_id is not self.domain_id:
            msg = "registered domain does not match control domain"
            raise ValueError(msg)


type _AnyRegisteredDomain = RegisteredDomain[
    grant_control.ExactGrant[typing.Hashable],
    object,
]


@dataclasses.dataclass(frozen=True, slots=True)
class _UnitSlot:
    domain_id: grant_control.DomainId
    unit_key: typing.Hashable


@dataclasses.dataclass(frozen=True, slots=True)
class _ErasedClaim:
    grant: grant_control.ExactGrant[typing.Hashable]
    payload: object
    run: typing.Callable[
        [grant_control.RunContext],
        typing.Awaitable[grant_control.RunOutcome],
    ]


@dataclasses.dataclass(frozen=True, slots=True)
class _ErasedHeartbeat:
    grant: object
    disposition: grant_control.HeartbeatDisposition


@dataclasses.dataclass(frozen=True, slots=True)
class _ErasedRegisteredDomain:
    domain_id: grant_control.DomainId
    allocation: worker_profiles.DomainAllocation
    claim: typing.Callable[
        [grant_control.ClaimMode, uuid.UUID, int],
        typing.Awaitable[tuple[_ErasedClaim, ...]],
    ]
    heartbeat: typing.Callable[
        [typing.Sequence[object]],
        typing.Awaitable[tuple[_ErasedHeartbeat, ...]],
    ]
    finalize: typing.Callable[
        [object, object, grant_control.TerminalDecision],
        typing.Awaitable[grant_control.FinalizeDisposition],
    ]


@dataclasses.dataclass(slots=True)
class _ManagedGrant:
    domain: _ErasedRegisteredDomain
    claim: _ErasedClaim
    slot: _UnitSlot
    context: grant_control.RunContext
    runner_closed: asyncio.Event
    root_task: asyncio.Task[None] | None = None
    terminal_task: asyncio.Task[_FinalizeEffect] | None = None
    outcome: grant_control.RunOutcome | None = None
    administrative_stop: bool = False
    lost: bool = False
    uncertain: bool = False
    cancelled: bool = False


class _FinalizeEffect(enum.StrEnum):
    APPLIED = "applied"
    LOST = "lost"
    ABANDONED = "abandoned"


def _erase_registered_domain[
    GrantT: grant_control.ExactGrant[typing.Hashable],
    PayloadT,
](
    registered: RegisteredDomain[GrantT, PayloadT],
    allocation: worker_profiles.DomainAllocation,
) -> _ErasedRegisteredDomain:
    """Build the one statically checked heterogeneous boundary."""

    def erase_claim(
        candidate: grant_control.ClaimedGrant[GrantT, PayloadT],
    ) -> _ErasedClaim:
        grant = candidate.grant
        payload = candidate.payload

        async def run(
            context: grant_control.RunContext,
        ) -> grant_control.RunOutcome:
            return await registered.runner.run(grant, payload, context)

        return _ErasedClaim(
            grant=grant,
            payload=payload,
            run=run,
        )

    async def claim(
        mode: grant_control.ClaimMode,
        owner_worker_id: uuid.UUID,
        limit: int,
    ) -> tuple[_ErasedClaim, ...]:
        candidates = await registered.control.claim(
            mode,
            owner_worker_id,
            limit,
        )
        return tuple(erase_claim(candidate) for candidate in candidates)

    async def heartbeat(
        grants: typing.Sequence[object],
    ) -> tuple[_ErasedHeartbeat, ...]:
        typed_grants = typing.cast("typing.Sequence[GrantT]", grants)
        results = await registered.control.heartbeat(typed_grants)
        if len(results) != len(typed_grants):
            msg = "heartbeat result cardinality mismatch"
            raise grant_control.GrantControlIntegrityError(msg)
        erased: list[_ErasedHeartbeat] = []
        for index, result in enumerate(results):
            if result.grant != typed_grants[index]:
                msg = "heartbeat result identity or order mismatch"
                raise grant_control.GrantControlIntegrityError(msg)
            erased.append(
                _ErasedHeartbeat(
                    grant=result.grant,
                    disposition=result.disposition,
                )
            )
        return tuple(erased)

    async def finalize(
        grant_value: object,
        payload_value: object,
        terminal: grant_control.TerminalDecision,
    ) -> grant_control.FinalizeDisposition:
        grant = typing.cast("GrantT", grant_value)
        payload = typing.cast("PayloadT", payload_value)
        result = await registered.control.finalize(
            grant,
            payload,
            terminal,
        )
        if result.grant != grant:
            msg = "finalization result grant mismatch"
            raise grant_control.GrantControlIntegrityError(msg)
        return result.disposition

    return _ErasedRegisteredDomain(
        domain_id=registered.domain_id,
        allocation=allocation,
        claim=claim,
        heartbeat=heartbeat,
        finalize=finalize,
    )


def _require_run_outcome(
    value: object,
) -> grant_control.RunOutcome:
    if not isinstance(
        value,
        (
            grant_control.RunCompleted,
            grant_control.RunLost,
            grant_control.RunFailed,
        ),
    ):
        msg = "runner returned an invalid outcome"
        raise grant_control.GrantControlIntegrityError(msg)
    return value


class GrantSupervisor:
    """Own one lifecycle algorithm for every registered grant domain."""

    def __init__(
        self,
        profile: worker_profiles.WorkerProfile,
        registered_domains: typing.Iterable[object],
        *,
        finalize_concurrency: int,
        failure_planner: FailurePlanner,
    ) -> None:
        validated_profile = worker_profiles.validate_worker_profile(profile)
        if isinstance(finalize_concurrency, bool):
            msg = "finalize_concurrency must be an integer"
            raise TypeError(msg)
        if finalize_concurrency <= 0:
            msg = "finalize_concurrency must be positive"
            raise ValueError(msg)

        registrations = tuple(registered_domains)
        selected_allocations = {
            allocation.domain_id: allocation
            for allocation in validated_profile.allocations
        }
        registrations_by_id: dict[grant_control.DomainId, object] = {}
        for candidate in registrations:
            if not isinstance(candidate, RegisteredDomain):
                msg = "registered domains must be RegisteredDomain values"
                raise TypeError(msg)
            registered = candidate
            if registered.domain_id in registrations_by_id:
                msg = (
                    f"duplicate registered domain: {registered.domain_id.value}"
                )
                raise ValueError(msg)
            if registered.domain_id not in selected_allocations:
                msg = (
                    "registered domain is not selected by profile: "
                    f"{registered.domain_id.value}"
                )
                raise ValueError(msg)
            registrations_by_id[registered.domain_id] = registered
        missing = set(selected_allocations) - set(registrations_by_id)
        if missing:
            missing_names = ", ".join(
                sorted(domain_id.value for domain_id in missing)
            )
            msg = f"profile domains are not registered: {missing_names}"
            raise ValueError(msg)

        domains = tuple(
            _erase_registered_domain(
                typing.cast(
                    "_AnyRegisteredDomain",
                    registrations_by_id[allocation.domain_id],
                ),
                allocation,
            )
            for allocation in validated_profile.allocations
        )
        self._profile = validated_profile
        self._domains = domains
        self._domains_by_id = {domain.domain_id: domain for domain in domains}
        self._failure_planner = failure_planner
        self._finalize_semaphore = asyncio.Semaphore(finalize_concurrency)
        self._registry: dict[_UnitSlot, _ManagedGrant] = {}
        self._owned_by_domain = dict.fromkeys(self._domains_by_id, 0)
        self._reserved_by_domain = dict.fromkeys(self._domains_by_id, 0)
        self._process_owned = 0
        self._process_reserved = 0
        self._domain_start_cursor = 0
        self._admission_enabled = True
        self._shutting_down = False
        self._integrity_failure: BaseException | None = None
        self._integrity_failure_event = asyncio.Event()
        self._claim_tasks: set[asyncio.Task[int]] = set()
        self._runner_tasks: set[asyncio.Task[None]] = set()
        self._terminal_tasks: set[asyncio.Task[_FinalizeEffect]] = set()

    @property
    def admission_enabled(self) -> bool:
        """Whether another admission cycle may begin."""
        return self._admission_enabled

    @property
    def integrity_failure_event(self) -> asyncio.Event:
        """Monotonic signal for the first fail-closed integrity outcome."""
        return self._integrity_failure_event

    @property
    def integrity_failure(self) -> BaseException | None:
        """Return the first surfaced integrity outcome."""
        return self._integrity_failure

    async def admit_cycle(  # noqa: PLR0912
        self,
        owner_worker_id: uuid.UUID,
    ) -> None:
        """Run one capacity-safe primary-then-recovery admission cycle."""
        if not self._admission_enabled:
            return
        enabled = tuple(
            domain
            for domain in self._domains
            if domain.allocation.claims_enabled
        )
        if not enabled:
            return
        start = self._domain_start_cursor % len(enabled)
        ordered = enabled[start:] + enabled[:start]
        self._domain_start_cursor = (start + 1) % len(enabled)
        remaining = {
            domain.domain_id: domain.allocation.claims_per_cycle
            for domain in enabled
        }

        for mode in (
            grant_control.ClaimMode.PRIMARY,
            grant_control.ClaimMode.RECOVERY,
        ):
            if not self._admission_enabled:
                return
            reservations: list[tuple[_ErasedRegisteredDomain, int]] = []
            for domain in ordered:
                ask = self._reserve_admission(
                    domain,
                    remaining[domain.domain_id],
                )
                if ask:
                    remaining[domain.domain_id] -= ask
                    reservations.append((domain, ask))
            if not reservations:
                continue

            tasks = tuple(
                asyncio.create_task(
                    self._claim_reserved(
                        domain,
                        mode,
                        owner_worker_id,
                        ask,
                    )
                )
                for domain, ask in reservations
            )
            self._claim_tasks.update(tasks)
            for task in tasks:
                task.add_done_callback(self._claim_task_done)
            results = await asyncio.gather(*tasks, return_exceptions=True)

            first_failure: BaseException | None = None
            for (domain, ask), result in zip(
                reservations,
                results,
                strict=True,
            ):
                if isinstance(result, BaseException):
                    if first_failure is None:
                        first_failure = result
                else:
                    remaining[domain.domain_id] += ask - result
            if first_failure is not None:
                raise first_failure

    def _reserve_admission(
        self,
        domain: _ErasedRegisteredDomain,
        remaining_cycle_budget: int,
    ) -> int:
        domain_id = domain.domain_id
        domain_headroom = (
            domain.allocation.owned_cap
            - self._owned_by_domain[domain_id]
            - self._reserved_by_domain[domain_id]
        )
        process_headroom = (
            self._profile.process_owned_cap
            - self._process_owned
            - self._process_reserved
        )
        ask = min(domain_headroom, remaining_cycle_budget, process_headroom)
        if ask <= 0:
            return 0
        self._reserved_by_domain[domain_id] += ask
        self._process_reserved += ask
        return ask

    async def _claim_reserved(
        self,
        domain: _ErasedRegisteredDomain,
        mode: grant_control.ClaimMode,
        owner_worker_id: uuid.UUID,
        reservation: int,
    ) -> int:
        remaining_reservation = reservation
        registered_count = 0
        try:
            claims = await domain.claim(mode, owner_worker_id, reservation)
            self._require_claim_count(claims, reservation)
            self._validate_claim_batch(domain, owner_worker_id, claims)
            for claim in claims:
                self._consume_reservation(domain.domain_id)
                remaining_reservation -= 1
                self._register_claim(domain, claim)
                registered_count += 1
            return registered_count  # noqa: TRY300
        except asyncio.CancelledError as exc:
            failure = grant_control.GrantControlIntegrityError(
                "claim outcome is unknown after cancellation"
            )
            failure.__cause__ = exc
            self._surface_integrity_failure(failure)
            raise
        except Exception as exc:
            failure = (
                exc
                if isinstance(
                    exc,
                    grant_control.GrantControlIntegrityError,
                )
                else grant_control.GrantControlIntegrityError(
                    "claim outcome is unknown"
                )
            )
            if failure is not exc:
                failure.__cause__ = exc
            self._surface_integrity_failure(failure)
            raise failure
        finally:
            if remaining_reservation:
                self._release_reservation(
                    domain.domain_id,
                    remaining_reservation,
                )

    def _require_claim_count(
        self,
        claims: typing.Sequence[_ErasedClaim],
        reservation: int,
    ) -> None:
        if len(claims) > reservation:
            msg = "control returned more claims than reserved"
            raise grant_control.GrantControlIntegrityError(msg)

    def _claim_task_done(self, task: asyncio.Task[int]) -> None:
        self._claim_tasks.discard(task)
        if not task.cancelled():
            task.exception()

    def _validate_claim_batch(
        self,
        domain: _ErasedRegisteredDomain,
        owner_worker_id: uuid.UUID,
        claims: tuple[_ErasedClaim, ...],
    ) -> None:
        seen_slots: set[_UnitSlot] = set()
        for claim in claims:
            grant = claim.grant
            if grant.owner_worker_id != owner_worker_id:
                msg = "claim owner does not match admission owner"
                raise grant_control.GrantControlIntegrityError(msg)
            slot = _UnitSlot(domain.domain_id, grant.unit_key)
            if slot in seen_slots:
                msg = "claim batch contains a duplicate unit"
                raise grant_control.GrantControlIntegrityError(msg)
            current = self._registry.get(slot)
            if current is not None and not self._is_valid_successor(
                current,
                grant,
                owner_worker_id,
            ):
                msg = "claim collides with a current unit generation"
                raise grant_control.GrantControlIntegrityError(msg)
            seen_slots.add(slot)

    def _is_valid_successor(
        self,
        current: _ManagedGrant,
        successor: grant_control.ExactGrant[typing.Hashable],
        owner_worker_id: uuid.UUID,
    ) -> bool:
        current_grant = current.claim.grant
        return (
            current_grant.owner_worker_id == owner_worker_id
            and successor.owner_worker_id == owner_worker_id
            and successor.fencing_token > current_grant.fencing_token
        )

    def _consume_reservation(
        self,
        domain_id: grant_control.DomainId,
    ) -> None:
        self._reserved_by_domain[domain_id] -= 1
        self._process_reserved -= 1

    def _release_reservation(
        self,
        domain_id: grant_control.DomainId,
        count: int,
    ) -> None:
        self._reserved_by_domain[domain_id] -= count
        self._process_reserved -= count

    def _register_claim(
        self,
        domain: _ErasedRegisteredDomain,
        claim: _ErasedClaim,
    ) -> None:
        slot = _UnitSlot(domain.domain_id, claim.grant.unit_key)
        superseded = self._registry.get(slot)
        context = grant_control.RunContext(
            stop_requested=asyncio.Event(),
            grant_lost=asyncio.Event(),
        )
        managed = _ManagedGrant(
            domain=domain,
            claim=claim,
            slot=slot,
            context=context,
            runner_closed=asyncio.Event(),
        )
        self._registry[slot] = managed
        if superseded is None:
            self._owned_by_domain[domain.domain_id] += 1
            self._process_owned += 1
        else:
            superseded.lost = True
            superseded.context.grant_lost.set()
            superseded.context.stop_requested.set()
            if (
                superseded.root_task is not None
                and not superseded.root_task.done()
            ):
                superseded.root_task.cancel()
        task = asyncio.create_task(self._run_managed(managed))
        managed.root_task = task
        self._runner_tasks.add(task)
        task.add_done_callback(self._root_task_done)

    async def _run_managed(self, managed: _ManagedGrant) -> None:
        try:
            outcome = _require_run_outcome(
                await managed.claim.run(managed.context)
            )
            if self._is_current(managed):
                managed.outcome = outcome
            if self._is_current(managed) and isinstance(
                outcome,
                grant_control.RunLost,
            ):
                managed.lost = True
                managed.context.grant_lost.set()
        except asyncio.CancelledError:
            managed.cancelled = True
            if self._is_current(managed) and not self._shutting_down:
                managed.uncertain = True
                failure = grant_control.GrantControlIntegrityError(
                    "runner was cancelled outside ordered shutdown"
                )
                self._surface_integrity_failure(failure)
            raise
        except Exception as exc:
            if self._is_current(managed):
                managed.uncertain = True
                failure = (
                    exc
                    if isinstance(
                        exc,
                        grant_control.GrantControlIntegrityError,
                    )
                    else grant_control.GrantControlIntegrityError(
                        "runner exited without a closed outcome"
                    )
                )
                if failure is not exc:
                    failure.__cause__ = exc
                self._surface_integrity_failure(failure)
        finally:
            managed.runner_closed.set()
            self._handle_runner_closed(managed)

    def _root_task_done(self, task: asyncio.Task[None]) -> None:
        self._runner_tasks.discard(task)
        if task.cancelled():
            return
        try:
            task.result()
        except Exception:
            logger.exception("Managed grant task escaped its isolation point")

    def _handle_runner_closed(self, managed: _ManagedGrant) -> None:
        if not self._is_current(managed) or not managed.runner_closed.is_set():
            return
        if managed.administrative_stop or managed.lost or managed.uncertain:
            self._discard_current(managed)
            return
        if self._shutting_down:
            return
        self._start_terminal_task(managed)

    def _start_terminal_task(
        self,
        managed: _ManagedGrant,
    ) -> asyncio.Task[_FinalizeEffect] | None:
        if (
            not self._is_current(managed)
            or not managed.runner_closed.is_set()
            or managed.terminal_task is not None
            or managed.administrative_stop
            or managed.lost
            or managed.uncertain
        ):
            return None
        try:
            terminal = self._terminal_decision(managed)
        except Exception as exc:
            managed.uncertain = True
            failure = grant_control.GrantControlIntegrityError(
                "failure planner did not produce a terminal decision"
            )
            failure.__cause__ = exc
            self._surface_integrity_failure(failure)
            self._discard_current(managed)
            return None
        task = asyncio.create_task(self._finalize_exact(managed, terminal))
        managed.terminal_task = task
        self._terminal_tasks.add(task)
        task.add_done_callback(self._terminal_task_done)
        return task

    def _terminal_decision(
        self,
        managed: _ManagedGrant,
    ) -> grant_control.TerminalDecision:
        outcome = managed.outcome
        if isinstance(outcome, grant_control.RunFailed):
            return self._failure_planner(outcome)
        return grant_control.NeutralRelease()

    async def _finalize_exact(  # noqa: PLR0911
        self,
        managed: _ManagedGrant,
        terminal: grant_control.TerminalDecision,
    ) -> _FinalizeEffect:
        if not self._is_current(managed):
            return _FinalizeEffect.LOST
        try:
            async with self._finalize_semaphore:
                if not self._is_current(managed):
                    return _FinalizeEffect.LOST
                disposition = await managed.domain.finalize(
                    managed.claim.grant,
                    managed.claim.payload,
                    terminal,
                )
        except asyncio.CancelledError as exc:
            if self._is_current(managed):
                managed.uncertain = True
                failure = grant_control.GrantControlIntegrityError(
                    "finalization outcome is unknown after cancellation"
                )
                failure.__cause__ = exc
                self._surface_integrity_failure(failure)
                self._discard_current(managed)
            raise
        except Exception as exc:
            if not self._is_current(managed):
                return _FinalizeEffect.LOST
            managed.uncertain = True
            failure = (
                exc
                if isinstance(
                    exc,
                    grant_control.GrantControlIntegrityError,
                )
                else grant_control.GrantControlIntegrityError(
                    "finalization outcome is unknown"
                )
            )
            if failure is not exc:
                failure.__cause__ = exc
            self._surface_integrity_failure(failure)
            self._discard_current(managed)
            return _FinalizeEffect.ABANDONED

        if not self._is_current(managed):
            return _FinalizeEffect.LOST
        if disposition is grant_control.FinalizeDisposition.APPLIED:
            self._discard_current(managed)
            return _FinalizeEffect.APPLIED
        if disposition is grant_control.FinalizeDisposition.LOST:
            managed.context.grant_lost.set()
            self._discard_current(managed)
            return _FinalizeEffect.LOST
        msg = "finalization returned an unknown disposition"
        failure = grant_control.GrantControlIntegrityError(msg)
        self._surface_integrity_failure(failure)
        managed.uncertain = True
        self._discard_current(managed)
        return _FinalizeEffect.ABANDONED

    def _terminal_task_done(
        self,
        task: asyncio.Task[_FinalizeEffect],
    ) -> None:
        self._terminal_tasks.discard(task)
        if task.cancelled():
            return
        try:
            task.result()
        except Exception:
            logger.exception("Grant finalization escaped its isolation point")

    async def heartbeat_cycle(
        self,
        on_dispatch: typing.Callable[[], None],
    ) -> None:
        """Heartbeat exact current generations grouped by domain."""
        on_dispatch()
        for domain in self._domains:
            expected = tuple(
                managed
                for managed in self._registry.values()
                if managed.domain is domain
                and managed.terminal_task is None
                and not managed.administrative_stop
                and not managed.lost
                and not managed.uncertain
            )
            if not expected:
                continue
            grants = tuple(managed.claim.grant for managed in expected)
            try:
                results = await domain.heartbeat(grants)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._fail_heartbeat_domain(expected, exc)
                continue
            if len(results) != len(expected):
                failure = grant_control.GrantControlIntegrityError(
                    "heartbeat result cardinality mismatch"
                )
                self._fail_heartbeat_domain(expected, failure)
                continue

            for managed, result in zip(expected, results, strict=True):
                if (
                    not self._is_current(managed)
                    or managed.terminal_task is not None
                ):
                    continue
                if (
                    result.disposition
                    is grant_control.HeartbeatDisposition.RETAINED
                ):
                    continue
                if (
                    result.disposition
                    is grant_control.HeartbeatDisposition.INELIGIBLE
                ):
                    managed.administrative_stop = True
                    managed.context.stop_requested.set()
                elif (
                    result.disposition
                    is grant_control.HeartbeatDisposition.LOST
                ):
                    managed.lost = True
                    managed.context.grant_lost.set()
                    managed.context.stop_requested.set()
                else:
                    failure = grant_control.GrantControlIntegrityError(
                        "heartbeat returned an unknown disposition"
                    )
                    self._fail_heartbeat_domain((managed,), failure)
                    continue
                if managed.runner_closed.is_set():
                    self._handle_runner_closed(managed)

    def _fail_heartbeat_domain(
        self,
        expected: typing.Sequence[_ManagedGrant],
        failure: BaseException,
    ) -> None:
        current = tuple(
            managed for managed in expected if self._is_current(managed)
        )
        if not current:
            return
        self._surface_integrity_failure(failure)
        for managed in current:
            managed.uncertain = True
            managed.context.grant_lost.set()
            managed.context.stop_requested.set()
            if managed.runner_closed.is_set():
                self._handle_runner_closed(managed)

    def active_count(self, domain_id: grant_control.DomainId) -> int:
        """Return the local count of running grants in one domain.

        Args:
            domain_id: Ownership domain to count.

        Returns:
            Current grants whose runners have not closed.
        """
        domain = self._domains_by_id.get(domain_id)
        if domain is None:
            return 0
        return sum(
            managed.domain is domain and not managed.runner_closed.is_set()
            for managed in self._registry.values()
        )

    async def _claim_shutdown_blocker(
        self,
        wait_timeout_sec: float,
    ) -> bool:
        """Wait for known claim mutations and report whether any remain."""
        claim_tasks = tuple(self._claim_tasks)
        if not claim_tasks:
            return False
        _done, pending_claims = await asyncio.wait(
            claim_tasks,
            timeout=wait_timeout_sec,
        )
        if not pending_claims:
            return False
        for managed in tuple(self._registry.values()):
            managed.context.stop_requested.set()
        return True

    async def shutdown(
        self,
        *,
        cooperative_grace_sec: float,
        external_stop_deadline_sec: float,
        stop_heartbeat_supervision: typing.Callable[
            [],
            typing.Awaitable[None],
        ],
    ) -> None:
        """Drain runners, stop heartbeats, then finalize closed grants."""
        self._validate_timeout(
            cooperative_grace_sec,
            "cooperative_grace_sec",
        )
        self._validate_timeout(
            external_stop_deadline_sec,
            "external_stop_deadline_sec",
        )
        self._admission_enabled = False
        self._shutting_down = True
        if await self._claim_shutdown_blocker(external_stop_deadline_sec):
            raise SupervisorNotDrainedError

        initial = tuple(self._registry.values())
        for managed in initial:
            managed.context.stop_requested.set()

        root_tasks = tuple(self._runner_tasks)
        remaining = await self._wait_tasks(
            root_tasks,
            cooperative_grace_sec,
        )
        for task in remaining:
            task.cancel()
        if remaining:
            remaining = await self._wait_tasks(
                remaining,
                external_stop_deadline_sec,
            )

        for managed in initial:
            if self._is_current(managed) and not managed.runner_closed.is_set():
                managed.uncertain = True
                managed.context.grant_lost.set()
        undrained = len(remaining)

        await stop_heartbeat_supervision()

        for managed in tuple(self._registry.values()):
            if managed.runner_closed.is_set():
                if managed.terminal_task is None and (
                    managed.administrative_stop
                    or managed.lost
                    or managed.uncertain
                ):
                    self._handle_runner_closed(managed)
                elif managed.terminal_task is None:
                    self._start_terminal_task(managed)

        undrained += await self._finalize_closed_grants(
            external_stop_deadline_sec
        )
        if undrained:
            raise SupervisorNotDrainedError

    async def _finalize_closed_grants(
        self,
        wait_timeout_sec: float,
    ) -> int:
        """Bound cleanup for current and superseded finalizer tasks."""
        pending = await self._wait_tasks(
            self._terminal_tasks,
            wait_timeout_sec,
        )
        for task in pending:
            task.cancel()
        if pending:
            _done, pending = await asyncio.wait(pending, timeout=0)
        return len(pending)

    def _validate_timeout(self, value: float, field_name: str) -> None:
        if isinstance(value, bool):
            msg = f"{field_name} must be a number"
            raise TypeError(msg)
        if value < 0:
            msg = f"{field_name} must be nonnegative"
            raise ValueError(msg)

    async def _wait_tasks[ResultT](
        self,
        tasks: typing.Iterable[asyncio.Task[ResultT]],
        timeout_sec: float,
    ) -> set[asyncio.Task[ResultT]]:
        pending_input = {task for task in tasks if not task.done()}
        if not pending_input:
            return set()
        _, pending = await asyncio.wait(
            pending_input,
            timeout=timeout_sec,
        )
        return pending

    def _is_current(self, managed: _ManagedGrant) -> bool:
        return self._registry.get(managed.slot) is managed

    def _discard_current(self, managed: _ManagedGrant) -> bool:
        if not self._is_current(managed):
            return False
        del self._registry[managed.slot]
        self._owned_by_domain[managed.slot.domain_id] -= 1
        self._process_owned -= 1
        return True

    def _surface_integrity_failure(self, failure: BaseException) -> None:
        self._admission_enabled = False
        if self._integrity_failure is not None:
            return
        self._integrity_failure = failure
        self._integrity_failure_event.set()
        logger.error(
            "Grant supervisor failed closed",
            exc_info=(type(failure), failure, failure.__traceback__),
        )
