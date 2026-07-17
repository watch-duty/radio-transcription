"""Exact-generation supervision for heterogeneous ingestion grants."""

from __future__ import annotations

import asyncio
import dataclasses
import enum
import logging
import types
import typing
import uuid

from backend.pipeline.ingestion import (
    failure_policy,
    grant_control,
    worker_profiles,
)

logger = logging.getLogger(__name__)


class SupervisorNotDrainedError(RuntimeError):
    """Supervisor-owned work may still use shared runtime resources."""


@dataclasses.dataclass(frozen=True, slots=True)
class RegisteredDomain[
    GrantT: grant_control.ExactGrant[typing.Hashable],
    PayloadT,
]:
    """One typed grant-control and runner registration.

    Attributes:
        domain_id: Closed ownership domain.
        control: Typed authoritative claim/heartbeat/finalize adapter.
        runner: Typed source-work runner.
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


class TerminalState(enum.StrEnum):
    """One exact generation's terminal linearization state."""

    OPEN = "open"
    RESERVED = "reserved"
    FINALIZED = "finalized"
    ABANDONED = "abandoned"


class _ReservationKind(enum.StrEnum):
    STORAGE = "storage"
    ADMINISTRATIVE = "administrative"
    CONFIRMED_LOSS = "confirmed_loss"
    UNCERTAIN = "uncertain"
    SHUTDOWN = "shutdown"


class _LifecycleEvent(enum.StrEnum):
    ADMISSION = "admission"
    HEARTBEAT = "heartbeat"
    ADMINISTRATIVE_STOP = "administrative_stop"
    LOSS = "loss"
    UNAVAILABLE = "unavailable"
    FINALIZATION = "finalization"
    SHUTDOWN = "shutdown"


class _FinalizeEffect(enum.StrEnum):
    FINALIZED = "finalized"
    ABANDONED = "abandoned"
    LOST = "lost"
    SKIPPED = "skipped"


@dataclasses.dataclass(frozen=True, slots=True)
class _AdministrativeNoWrite:
    pass


@dataclasses.dataclass(frozen=True, slots=True)
class _ConfirmedLoss:
    pass


@dataclasses.dataclass(frozen=True, slots=True)
class _UncertainAbandonment:
    pass


type _TerminalReservation = (
    grant_control.TerminalDecision
    | grant_control.RunFailed
    | _AdministrativeNoWrite
    | _ConfirmedLoss
    | _UncertainAbandonment
)

_STORAGE_RESERVATION_TYPES = (
    grant_control.NeutralRelease,
    failure_policy.FailurePersistencePlan,
    grant_control.RunFailed,
)


@dataclasses.dataclass(frozen=True, slots=True)
class _LifecycleRecord:
    event_type: str
    profile: str
    profile_digest: str
    domain_id: str
    authority_kind: str
    outcome: str


@dataclasses.dataclass(frozen=True, slots=True)
class _UnitSlot:
    domain_id: grant_control.DomainId
    unit_key: typing.Hashable


@dataclasses.dataclass(frozen=True, slots=True)
class _ErasedClaim:
    """One statically checked claim at the heterogeneous boundary."""

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
    """The single checked heterogeneous boundary used by the supervisor."""

    domain_id: grant_control.DomainId
    authority_kind: worker_profiles.AuthorityKind
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
        typing.Awaitable[grant_control.FinalizeResult[object]],
    ]


@dataclasses.dataclass(slots=True)
class _ManagedGrant:
    domain: _ErasedRegisteredDomain
    grant: grant_control.ExactGrant[typing.Hashable]
    payload: object
    slot: _UnitSlot
    root_task: asyncio.Task[None] | None
    stop_requested: asyncio.Event
    grant_lost: asyncio.Event
    runner_closed: asyncio.Event
    closed_outcome: grant_control.RunOutcome | None = None
    terminal_state: TerminalState = TerminalState.OPEN
    terminal_kind: _ReservationKind | None = None
    terminal_decision: _TerminalReservation | None = None
    terminal_task: asyncio.Task[_FinalizeEffect] | None = None


def _erase_registered_domain[
    GrantT: grant_control.ExactGrant[typing.Hashable],
    PayloadT,
](
    registered: RegisteredDomain[GrantT, PayloadT],
    allocation: worker_profiles.DomainAllocation,
    authority_kind: worker_profiles.AuthorityKind,
) -> _ErasedRegisteredDomain:
    """Build the sole statically checked heterogeneous conversion."""

    def bind_run(
        grant: GrantT,
        payload: PayloadT,
    ) -> typing.Callable[
        [grant_control.RunContext],
        typing.Awaitable[grant_control.RunOutcome],
    ]:
        async def run(
            context: grant_control.RunContext,
        ) -> grant_control.RunOutcome:
            return await registered.runner.run(grant, payload, context)

        return run

    def erase_claim(
        candidate: grant_control.ClaimedGrant[GrantT, PayloadT],
    ) -> _ErasedClaim:
        grant = candidate.grant
        payload = candidate.payload
        return _ErasedClaim(
            grant=grant,
            payload=payload,
            run=bind_run(grant, payload),
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
        return tuple(
            _ErasedHeartbeat(
                grant=result.grant,
                disposition=result.disposition,
            )
            for result in results
        )

    async def finalize(
        grant: object,
        payload: object,
        terminal: grant_control.TerminalDecision,
    ) -> grant_control.FinalizeResult[object]:
        typed_grant = typing.cast("GrantT", grant)
        typed_payload = typing.cast("PayloadT", payload)
        result = await registered.control.finalize(
            typed_grant,
            typed_payload,
            terminal,
        )
        return grant_control.FinalizeResult(
            grant=result.grant,
            disposition=result.disposition,
        )

    return _ErasedRegisteredDomain(
        domain_id=registered.domain_id,
        authority_kind=authority_kind,
        allocation=allocation,
        claim=claim,
        heartbeat=heartbeat,
        finalize=finalize,
    )


class GrantSupervisor:
    """Own one capacity-safe, exact-generation grant registry."""

    def __init__(  # noqa: PLR0915
        self,
        profile: worker_profiles.WorkerProfile,
        registered_domains: typing.Iterable[object],
        *,
        finalize_concurrency: int,
        failure_planner: typing.Callable[
            [grant_control.RunFailed],
            failure_policy.FailurePersistencePlan,
        ],
        failure_plan_observer: typing.Callable[
            [grant_control.DomainId, failure_policy.FailurePersistencePlan],
            None,
        ]
        | None = None,
    ) -> None:
        """Validate immutable composition before any admission side effect."""
        self._profile = worker_profiles.validate_worker_profile(profile)
        if isinstance(finalize_concurrency, bool):
            msg = "finalize_concurrency must be an integer"
            raise TypeError(msg)
        if finalize_concurrency <= 0:
            msg = "finalize_concurrency must be positive"
            raise ValueError(msg)
        if not callable(failure_planner):
            msg = "failure_planner must be callable"
            raise TypeError(msg)
        if failure_plan_observer is not None and not callable(
            failure_plan_observer
        ):
            msg = "failure_plan_observer must be callable or None"
            raise TypeError(msg)

        supplied = tuple(registered_domains)
        by_domain: dict[
            grant_control.DomainId,
            _ErasedRegisteredDomain,
        ] = {}
        logical_authorities: set[str] = set()
        for registered in supplied:
            if not isinstance(registered, RegisteredDomain):
                msg = "registered_domains must contain RegisteredDomain values"
                raise TypeError(msg)
            if registered.domain_id in by_domain:
                msg = f"duplicate registered domain: {registered.domain_id}"
                raise ValueError(msg)
            allocation = worker_profiles.allocation_for_domain(
                self._profile,
                registered.domain_id,
            )
            if allocation is None:
                msg = "registered domain is absent from the selected profile"
                raise ValueError(msg)
            catalog_entry = worker_profiles.DOMAIN_CATALOG.get(
                registered.domain_id
            )
            if catalog_entry is None:
                msg = "registered domain is absent from the static catalog"
                raise ValueError(msg)
            if catalog_entry.logical_authority in logical_authorities:
                msg = "registered domains duplicate a logical authority"
                raise ValueError(msg)
            logical_authorities.add(catalog_entry.logical_authority)
            by_domain[registered.domain_id] = _erase_registered_domain(
                typing.cast("_AnyRegisteredDomain", registered),
                allocation,
                catalog_entry.authority_kind,
            )

        selected_ids = {
            allocation.domain_id for allocation in self._profile.allocations
        }
        if set(by_domain) != selected_ids:
            msg = "registered domains must exactly cover the selected profile"
            raise ValueError(msg)

        ordered = tuple(
            by_domain[allocation.domain_id]
            for allocation in self._profile.allocations
        )
        self._domains = ordered
        self._domains_by_id = types.MappingProxyType(
            {domain.domain_id: domain for domain in ordered}
        )
        self._profile_digest = worker_profiles.profile_digest(self._profile)
        self._finalize_concurrency = finalize_concurrency
        self._finalize_semaphore = asyncio.Semaphore(finalize_concurrency)
        self._failure_planner = failure_planner
        self._failure_plan_observer = failure_plan_observer
        self._registry: dict[_UnitSlot, _ManagedGrant] = {}
        self._owned_by_domain = dict.fromkeys(self._domains_by_id, 0)
        self._reserved_by_domain = dict.fromkeys(self._domains_by_id, 0)
        self._process_owned = 0
        self._process_reserved = 0
        self._domain_start_cursor = 0
        self._admission_enabled = True
        self._integrity_failure: BaseException | None = None
        self._integrity_failure_event = asyncio.Event()
        self._heartbeat_stopped = asyncio.Event()
        self._claim_tasks: set[asyncio.Task[int]] = set()
        self._runner_tasks: set[asyncio.Task[None]] = set()
        self._terminal_tasks: set[asyncio.Task[_FinalizeEffect]] = set()

    @property
    def admission_enabled(self) -> bool:
        """Whether this supervisor may begin another admission cycle."""
        return self._admission_enabled

    @property
    def integrity_failure_event(self) -> asyncio.Event:
        """Monotonic common signal for a surfaced control-plane failure."""
        return self._integrity_failure_event

    @property
    def integrity_failure(self) -> BaseException | None:
        """Return the first surfaced control-plane integrity failure."""
        return self._integrity_failure

    async def admit_cycle(  # noqa: PLR0912
        self,
        owner_worker_id: uuid.UUID,
    ) -> None:
        """Run one reservation-first primary then recovery admission cycle.

        Args:
            owner_worker_id: Exact runtime-epoch worker UUID for all claims.

        Raises:
            TypeError: If ``owner_worker_id`` is not a UUID.
            Exception: A non-integrity claim failure after all reservations
                in that pass have reconciled.
        """
        if not isinstance(owner_worker_id, uuid.UUID):
            msg = "owner_worker_id must be a UUID"
            raise TypeError(msg)
        if not self._admission_enabled:
            return
        enabled = tuple(
            domain
            for domain in self._domains
            if domain.allocation.claims_enabled
        )
        if not enabled:
            for domain in self._domains:
                self._emit_lifecycle(
                    _LifecycleEvent.ADMISSION,
                    domain,
                    outcome="disabled",
                )
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
                if ask > 0:
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
            for (_domain, ask), result in zip(
                reservations,
                results,
                strict=True,
            ):
                if isinstance(result, BaseException):
                    self._emit_lifecycle(
                        _LifecycleEvent.ADMISSION,
                        _domain,
                        outcome="control_error",
                    )
                    if first_failure is None:
                        first_failure = result
                else:
                    remaining[_domain.domain_id] += ask - result
            if first_failure is not None:
                raise first_failure
        for domain in self._domains:
            self._emit_lifecycle(
                _LifecycleEvent.ADMISSION,
                domain,
                outcome=(
                    "complete"
                    if domain.allocation.claims_enabled
                    else "disabled"
                ),
            )

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
            if len(claims) > reservation:
                msg = "control returned more claims than reserved"
                raise grant_control.GrantControlIntegrityError(msg)  # noqa: TRY301
            self._validate_claim_batch(domain, owner_worker_id, claims)
            stopped_in_flight = not self._admission_enabled
            for claim in claims:
                self._consume_reservation(domain.domain_id)
                remaining_reservation -= 1
                self._register_claim(
                    domain,
                    claim,
                    start_runner=not stopped_in_flight,
                )
                registered_count += 1
            if stopped_in_flight:
                self._emit_lifecycle(
                    _LifecycleEvent.ADMISSION,
                    domain,
                    outcome="stopped_in_flight",
                )
            return registered_count  # noqa: TRY300
        except asyncio.CancelledError as exc:
            failure = grant_control.GrantControlIntegrityError(
                "claim outcome is unknown after cancellation"
            )
            failure.__cause__ = exc
            self._surface_integrity_failure(failure)
            self._emit_lifecycle(
                _LifecycleEvent.ADMISSION,
                domain,
                outcome="cancelled_unknown",
            )
            raise
        except grant_control.GrantControlIntegrityError as exc:
            self._surface_integrity_failure(exc)
            self._emit_lifecycle(
                _LifecycleEvent.ADMISSION,
                domain,
                outcome="integrity_failure",
            )
            return 0
        except Exception as exc:
            failure = grant_control.GrantControlIntegrityError(
                "claim outcome is unknown"
            )
            failure.__cause__ = exc
            self._surface_integrity_failure(failure)
            self._emit_lifecycle(
                _LifecycleEvent.ADMISSION,
                domain,
                outcome="unavailable_unknown",
            )
            return 0
        finally:
            if remaining_reservation:
                self._release_reservation(
                    domain.domain_id,
                    remaining_reservation,
                )

    def _claim_task_done(self, task: asyncio.Task[int]) -> None:
        self._claim_tasks.discard(task)

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
                msg = "claim owner does not match the admission owner"
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
        current_grant = current.grant
        return (
            current_grant.owner_worker_id == owner_worker_id
            and successor.owner_worker_id == owner_worker_id
            and successor.fencing_token > current_grant.fencing_token
        )

    def _consume_reservation(self, domain_id: grant_control.DomainId) -> None:
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
        *,
        start_runner: bool = True,
    ) -> None:
        slot = _UnitSlot(domain.domain_id, claim.grant.unit_key)
        superseded = self._registry.get(slot)
        managed = _ManagedGrant(
            domain=domain,
            grant=claim.grant,
            payload=claim.payload,
            slot=slot,
            root_task=None,
            stop_requested=asyncio.Event(),
            grant_lost=asyncio.Event(),
            runner_closed=asyncio.Event(),
        )
        self._registry[slot] = managed
        if superseded is None:
            self._owned_by_domain[domain.domain_id] += 1
            self._process_owned += 1
        else:
            superseded.grant_lost.set()
            superseded.stop_requested.set()
            if (
                superseded.root_task is not None
                and not superseded.root_task.done()
            ):
                superseded.root_task.cancel()
        if not start_runner:
            managed.closed_outcome = grant_control.RunStopped(
                grant_control.TerminalCause.SHUTDOWN
            )
            managed.runner_closed.set()
            self._reserve_terminal(
                managed,
                grant_control.NeutralRelease(
                    grant_control.TerminalCause.SHUTDOWN
                ),
            )
            self._emit_lifecycle(
                _LifecycleEvent.ADMISSION,
                domain,
                outcome="registered_shutdown_only",
            )
            return
        root_task = asyncio.create_task(
            self._run_managed(managed, claim.run),
            name=(
                f"grant-{domain.domain_id.value}-{claim.grant.fencing_token}"
            ),
        )
        managed.root_task = root_task
        self._runner_tasks.add(root_task)
        self._emit_lifecycle(
            _LifecycleEvent.ADMISSION,
            domain,
            outcome="registered",
        )
        root_task.add_done_callback(
            lambda task, entry=managed: self._root_done(entry, task)
        )

    async def _run_managed(
        self,
        managed: _ManagedGrant,
        run: typing.Callable[
            [grant_control.RunContext],
            typing.Awaitable[grant_control.RunOutcome],
        ],
    ) -> None:
        context = grant_control.RunContext(
            stop_requested=managed.stop_requested,
            grant_lost=managed.grant_lost,
        )
        acknowledged_closed_outcome = False
        try:
            outcome = await run(context)
            if not isinstance(
                outcome,
                (
                    grant_control.RunCompleted,
                    grant_control.RunStopped,
                    grant_control.RunLost,
                    grant_control.RunFailed,
                ),
            ):
                msg = "runner returned an invalid closed outcome"
                raise grant_control.GrantControlIntegrityError(msg)
            acknowledged_closed_outcome = True
            if self._is_current(managed):
                managed.closed_outcome = outcome
        finally:
            if acknowledged_closed_outcome:
                managed.runner_closed.set()
                self._handle_runner_closed(managed)

    def _root_done(
        self,
        managed: _ManagedGrant,
        task: asyncio.Task[None],
    ) -> None:
        self._runner_tasks.discard(task)
        try:
            exception = task.exception()
        except asyncio.CancelledError:
            return
        if exception is None:
            return
        if self._is_current(managed):
            self._surface_integrity_failure(exception)

    def _handle_runner_closed(
        self,
        managed: _ManagedGrant,
    ) -> None:
        if not self._is_current(managed):
            return
        if managed.terminal_kind is _ReservationKind.ADMINISTRATIVE:
            self._emit_lifecycle(
                _LifecycleEvent.ADMINISTRATIVE_STOP,
                managed.domain,
                outcome="closed",
            )
            self._discard_current(managed)
            return
        if managed.terminal_kind is _ReservationKind.CONFIRMED_LOSS:
            self._emit_lifecycle(
                _LifecycleEvent.LOSS,
                managed.domain,
                outcome="closed",
            )
            self._discard_current(managed)
            return
        if managed.terminal_kind in (
            _ReservationKind.UNCERTAIN,
            _ReservationKind.SHUTDOWN,
            _ReservationKind.STORAGE,
        ):
            return

        outcome = managed.closed_outcome
        if outcome is None:
            if self._reserve_terminal(managed, _UncertainAbandonment()):
                failure = grant_control.GrantControlIntegrityError(
                    "runner closed without a valid outcome"
                )
                self._surface_integrity_failure(failure)
            return
        if isinstance(outcome, grant_control.RunLost):
            if self._reserve_terminal(managed, _ConfirmedLoss()):
                self._discard_current(managed)
            return
        if isinstance(outcome, grant_control.RunFailed):
            decision: _TerminalReservation = outcome
        elif isinstance(outcome, grant_control.RunStopped):
            decision = grant_control.NeutralRelease(outcome.cause)
        else:
            decision = grant_control.NeutralRelease(
                grant_control.TerminalCause.NORMAL
            )
        if self._reserve_terminal(managed, decision):
            self._start_terminal_task(managed)

    def _reserve_terminal(
        self,
        managed: _ManagedGrant,
        decision: _TerminalReservation,
    ) -> bool:
        """Synchronously reserve the first terminal owner for one grant."""
        if (
            not self._is_current(managed)
            or managed.terminal_state is not TerminalState.OPEN
        ):
            return False
        if isinstance(decision, _AdministrativeNoWrite):
            kind = _ReservationKind.ADMINISTRATIVE
            state = TerminalState.RESERVED
        elif isinstance(decision, _ConfirmedLoss):
            kind = _ReservationKind.CONFIRMED_LOSS
            state = TerminalState.ABANDONED
        elif isinstance(decision, _UncertainAbandonment):
            kind = _ReservationKind.UNCERTAIN
            state = TerminalState.ABANDONED
        elif (
            isinstance(decision, grant_control.NeutralRelease)
            and decision.cause is grant_control.TerminalCause.SHUTDOWN
        ):
            kind = _ReservationKind.SHUTDOWN
            state = TerminalState.RESERVED
        elif isinstance(decision, _STORAGE_RESERVATION_TYPES):
            kind = _ReservationKind.STORAGE
            state = TerminalState.RESERVED
        else:
            msg = "decision must be a closed terminal reservation"
            raise TypeError(msg)
        managed.terminal_kind = kind
        managed.terminal_decision = decision
        managed.terminal_state = state
        return True

    def _start_terminal_task(
        self,
        managed: _ManagedGrant,
    ) -> asyncio.Task[_FinalizeEffect]:
        if not self._is_current(managed):
            msg = "cannot start finalization for a non-current generation"
            raise RuntimeError(msg)
        if managed.terminal_task is not None:
            return managed.terminal_task
        task = asyncio.create_task(
            self._finalize_reserved(managed),
            name=(
                f"grant-finalize-{managed.slot.domain_id.value}-"
                f"{managed.grant.fencing_token}"
            ),
        )
        managed.terminal_task = task
        self._terminal_tasks.add(task)
        task.add_done_callback(self._terminal_task_done)
        return task

    def _terminal_task_done(
        self,
        task: asyncio.Task[_FinalizeEffect],
    ) -> None:
        self._terminal_tasks.discard(task)
        try:
            task.exception()
        except asyncio.CancelledError:
            pass

    async def _finalize_reserved(  # noqa: PLR0911, PLR0912, PLR0915
        self,
        managed: _ManagedGrant,
    ) -> _FinalizeEffect:
        if (
            not self._is_current(managed)
            or managed.terminal_state is not TerminalState.RESERVED
            or managed.terminal_kind
            not in (_ReservationKind.STORAGE, _ReservationKind.SHUTDOWN)
            or not managed.runner_closed.is_set()
        ):
            return _FinalizeEffect.SKIPPED
        if (
            managed.terminal_kind is _ReservationKind.SHUTDOWN
            and not self._heartbeat_stopped.is_set()
        ):
            return _FinalizeEffect.SKIPPED
        reservation = managed.terminal_decision
        if not isinstance(reservation, _STORAGE_RESERVATION_TYPES):
            return _FinalizeEffect.SKIPPED
        try:
            async with self._finalize_semaphore:
                if (
                    not self._is_current(managed)
                    or managed.terminal_state is not TerminalState.RESERVED
                    or managed.terminal_decision is not reservation
                ):
                    return _FinalizeEffect.SKIPPED
                terminal: grant_control.TerminalDecision
                if isinstance(reservation, grant_control.RunFailed):
                    terminal = self._materialize_failure_plan(reservation)
                    self._observe_failure_plan(managed.domain, terminal)
                else:
                    terminal = reservation
                result = await managed.domain.finalize(
                    managed.grant,
                    managed.payload,
                    terminal,
                )
        except asyncio.CancelledError as exc:
            if self._is_current(managed):
                managed.terminal_state = TerminalState.ABANDONED
                managed.terminal_kind = _ReservationKind.UNCERTAIN
                managed.terminal_decision = _UncertainAbandonment()
                self._emit_lifecycle(
                    _LifecycleEvent.FINALIZATION,
                    managed.domain,
                    outcome="cancelled_unknown",
                )
                failure = grant_control.GrantControlIntegrityError(
                    "finalization outcome is unknown after cancellation"
                )
                failure.__cause__ = exc
                self._surface_integrity_failure(failure)
            raise
        except Exception as exc:
            if not self._is_current(managed):
                return _FinalizeEffect.SKIPPED
            managed.terminal_state = TerminalState.ABANDONED
            managed.terminal_kind = _ReservationKind.UNCERTAIN
            managed.terminal_decision = _UncertainAbandonment()
            self._emit_lifecycle(
                _LifecycleEvent.FINALIZATION,
                managed.domain,
                outcome="unavailable",
            )
            self._surface_integrity_failure(exc)
            return _FinalizeEffect.ABANDONED

        if (
            not self._is_current(managed)
            or managed.terminal_state is not TerminalState.RESERVED
            or managed.terminal_decision is not reservation
        ):
            return _FinalizeEffect.SKIPPED
        if result.grant != managed.grant:
            failure = grant_control.GrantControlIntegrityError(
                "finalize result identity mismatch"
            )
            managed.terminal_state = TerminalState.ABANDONED
            managed.terminal_kind = _ReservationKind.UNCERTAIN
            managed.terminal_decision = _UncertainAbandonment()
            self._emit_lifecycle(
                _LifecycleEvent.FINALIZATION,
                managed.domain,
                outcome="identity_mismatch",
            )
            self._surface_integrity_failure(failure)
            return _FinalizeEffect.ABANDONED
        if not isinstance(
            result.disposition,
            grant_control.FinalizeDisposition,
        ):
            failure = grant_control.GrantControlIntegrityError(
                "finalize result disposition is malformed"
            )
            managed.terminal_state = TerminalState.ABANDONED
            managed.terminal_kind = _ReservationKind.UNCERTAIN
            managed.terminal_decision = _UncertainAbandonment()
            self._emit_lifecycle(
                _LifecycleEvent.FINALIZATION,
                managed.domain,
                outcome="malformed",
            )
            self._surface_integrity_failure(failure)
            return _FinalizeEffect.ABANDONED
        if result.disposition in (
            grant_control.FinalizeDisposition.APPLIED,
            grant_control.FinalizeDisposition.ACCEPTED_NOOP,
        ):
            managed.terminal_state = TerminalState.FINALIZED
            self._emit_lifecycle(
                _LifecycleEvent.FINALIZATION,
                managed.domain,
                outcome="finalized",
            )
            self._discard_current(managed)
            return _FinalizeEffect.FINALIZED
        if result.disposition is grant_control.FinalizeDisposition.LOST:
            managed.terminal_state = TerminalState.ABANDONED
            managed.terminal_kind = _ReservationKind.CONFIRMED_LOSS
            managed.terminal_decision = _ConfirmedLoss()
            self._emit_lifecycle(
                _LifecycleEvent.FINALIZATION,
                managed.domain,
                outcome="lost",
            )
            self._discard_current(managed)
            return _FinalizeEffect.LOST
        managed.terminal_state = TerminalState.ABANDONED
        managed.terminal_kind = _ReservationKind.UNCERTAIN
        managed.terminal_decision = _UncertainAbandonment()
        failure = grant_control.GrantControlIntegrityError(
            "finalize returned unavailable or unknown authority"
        )
        self._emit_lifecycle(
            _LifecycleEvent.FINALIZATION,
            managed.domain,
            outcome="unavailable",
        )
        self._surface_integrity_failure(failure)
        return _FinalizeEffect.ABANDONED

    async def heartbeat_cycle(  # noqa: PLR0912
        self,
        on_dispatch: typing.Callable[[], None],
    ) -> None:
        """Heartbeat exact current generations grouped by registered domain.

        Args:
            on_dispatch: Non-awaiting local-liveness stamp invoked before any
                control I/O.

        Raises:
            TypeError: If ``on_dispatch`` is not callable.
        """
        if not callable(on_dispatch):
            msg = "on_dispatch must be callable"
            raise TypeError(msg)
        on_dispatch()
        for domain in self._domains:
            expected = tuple(
                (
                    managed,
                    managed.terminal_state,
                    managed.terminal_kind,
                )
                for managed in self._registry.values()
                if managed.domain is domain
                and self._heartbeat_eligible(managed)
            )
            if not expected:
                self._emit_lifecycle(
                    _LifecycleEvent.HEARTBEAT,
                    domain,
                    outcome="empty",
                )
                continue
            grants = tuple(managed.grant for managed, _state, _kind in expected)
            try:
                results = await domain.heartbeat(grants)
                self._validate_heartbeat_results(expected, results)
                if any(
                    result.disposition
                    is grant_control.HeartbeatDisposition.UNAVAILABLE
                    for result in results
                ):
                    msg = "heartbeat control reported unavailable authority"
                    raise grant_control.GrantControlIntegrityError(msg)  # noqa: TRY301
            except Exception as exc:
                self._fail_heartbeat_domain(domain, expected, exc)
                continue

            for expected_entry, result in zip(
                expected,
                results,
                strict=True,
            ):
                managed, state, kind = expected_entry
                if (
                    not self._is_current(managed)
                    or managed.terminal_state is not state
                    or managed.terminal_kind is not kind
                    or not self._heartbeat_eligible(managed)
                ):
                    continue
                if (
                    result.disposition
                    is grant_control.HeartbeatDisposition.RETAINED
                ):
                    self._emit_lifecycle(
                        _LifecycleEvent.HEARTBEAT,
                        domain,
                        outcome="retained",
                    )
                elif (
                    result.disposition
                    is grant_control.HeartbeatDisposition.ADMINISTRATIVE_STOP
                ):
                    if self._reserve_terminal(
                        managed,
                        _AdministrativeNoWrite(),
                    ):
                        self._emit_lifecycle(
                            _LifecycleEvent.ADMINISTRATIVE_STOP,
                            domain,
                            outcome="reserved",
                        )
                        managed.stop_requested.set()
                        if managed.runner_closed.is_set():
                            self._handle_runner_closed(managed)
                elif (
                    result.disposition
                    is grant_control.HeartbeatDisposition.LOST
                ):
                    if self._reserve_terminal(managed, _ConfirmedLoss()):
                        self._emit_lifecycle(
                            _LifecycleEvent.LOSS,
                            domain,
                            outcome="reserved",
                        )
                        managed.grant_lost.set()
                        if managed.runner_closed.is_set():
                            self._handle_runner_closed(managed)

    def _validate_heartbeat_results(
        self,
        expected: tuple[
            tuple[
                _ManagedGrant,
                TerminalState,
                _ReservationKind | None,
            ],
            ...,
        ],
        results: tuple[_ErasedHeartbeat, ...],
    ) -> None:
        if len(results) != len(expected):
            msg = "heartbeat result cardinality mismatch"
            raise grant_control.GrantControlIntegrityError(msg)
        seen: list[object] = []
        for index, result in enumerate(results):
            managed = expected[index][0]
            if result.grant != managed.grant or result.grant in seen:
                msg = "heartbeat result identity or order mismatch"
                raise grant_control.GrantControlIntegrityError(msg)
            seen.append(result.grant)
            if not isinstance(
                result.disposition,
                grant_control.HeartbeatDisposition,
            ):
                msg = "heartbeat returned an invalid disposition"
                raise grant_control.GrantControlIntegrityError(msg)

    def _heartbeat_eligible(self, managed: _ManagedGrant) -> bool:
        if managed.terminal_state is TerminalState.OPEN:
            return True
        return (
            managed.terminal_state is TerminalState.RESERVED
            and managed.terminal_kind is _ReservationKind.SHUTDOWN
            and not managed.runner_closed.is_set()
            and not self._heartbeat_stopped.is_set()
        )

    def _abandon_heartbeat_uncertainty(
        self,
        managed: _ManagedGrant,
        expected_state: TerminalState,
        expected_kind: _ReservationKind | None,
    ) -> bool:
        """Fail closed one generation from an in-flight heartbeat batch.

        Shutdown reservations are provisional and perform no storage write
        until heartbeat supervision has stopped. An uncertainty returned for
        a batch dispatched before shutdown must therefore supersede that
        reservation, even if the runner closed while the request was in
        flight. Other terminal reservations own their outcome and cannot be
        replaced here.
        """
        if not self._is_current(managed):
            return False
        unchanged_and_eligible = (
            managed.terminal_state is expected_state
            and managed.terminal_kind is expected_kind
            and self._heartbeat_eligible(managed)
        )
        provisional_shutdown = (
            not self._heartbeat_stopped.is_set()
            and managed.terminal_state is TerminalState.RESERVED
            and managed.terminal_kind is _ReservationKind.SHUTDOWN
            and (
                expected_state is TerminalState.OPEN
                or (
                    expected_state is TerminalState.RESERVED
                    and expected_kind is _ReservationKind.SHUTDOWN
                )
            )
        )
        if not unchanged_and_eligible and not provisional_shutdown:
            return False
        if managed.terminal_state is TerminalState.OPEN:
            if not self._reserve_terminal(
                managed,
                _UncertainAbandonment(),
            ):
                return False
        else:
            managed.terminal_state = TerminalState.ABANDONED
            managed.terminal_kind = _ReservationKind.UNCERTAIN
            managed.terminal_decision = _UncertainAbandonment()
        managed.grant_lost.set()
        managed.stop_requested.set()
        return True

    def _fail_heartbeat_domain(
        self,
        domain: _ErasedRegisteredDomain,
        expected: tuple[
            tuple[
                _ManagedGrant,
                TerminalState,
                _ReservationKind | None,
            ],
            ...,
        ],
        failure: BaseException,
    ) -> None:
        current = tuple(
            entry for entry in expected if self._is_current(entry[0])
        )
        if not current:
            return
        self._admission_enabled = False
        for managed, state, kind in current:
            self._abandon_heartbeat_uncertainty(managed, state, kind)
        self._emit_lifecycle(
            _LifecycleEvent.UNAVAILABLE,
            domain,
            outcome="fail_closed",
        )
        self._surface_integrity_failure(failure)

    def active_count(self, domain_id: grant_control.DomainId) -> int:
        """Return the number of current runners active in one domain.

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
            managed.stop_requested.set()
        return True

    def _refine_shutdown_failure(self, managed: _ManagedGrant) -> None:
        """Resolve one provisional shutdown reservation to real failure."""
        if (
            not self._is_current(managed)
            or managed.terminal_state is not TerminalState.RESERVED
            or managed.terminal_kind is not _ReservationKind.SHUTDOWN
            or not isinstance(managed.closed_outcome, grant_control.RunFailed)
        ):
            return
        managed.terminal_kind = _ReservationKind.STORAGE
        managed.terminal_decision = managed.closed_outcome
        self._emit_lifecycle(
            _LifecycleEvent.SHUTDOWN,
            managed.domain,
            outcome="refined_failure",
        )

    def _refine_shutdown_failures(self) -> None:
        """Resolve every closed provisional shutdown failure once."""
        for managed in tuple(self._registry.values()):
            self._refine_shutdown_failure(managed)

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
        """Drain runners, stop heartbeats, then exact-finalize closed grants."""
        self._validate_shutdown_timeout(
            cooperative_grace_sec,
            "cooperative_grace_sec",
        )
        self._validate_shutdown_timeout(
            external_stop_deadline_sec,
            "external_stop_deadline_sec",
        )
        if not callable(stop_heartbeat_supervision):
            msg = "stop_heartbeat_supervision must be callable"
            raise TypeError(msg)

        self._admission_enabled = False
        if await self._claim_shutdown_blocker(external_stop_deadline_sec):
            raise SupervisorNotDrainedError
        initial = tuple(self._registry.values())
        for managed in initial:
            self._reserve_terminal(
                managed,
                grant_control.NeutralRelease(
                    grant_control.TerminalCause.SHUTDOWN
                ),
            )
        for domain in self._domains:
            self._emit_lifecycle(
                _LifecycleEvent.SHUTDOWN,
                domain,
                outcome="reserved",
            )
        for managed in initial:
            managed.stop_requested.set()

        await self._await_runner_closure(
            initial,
            cooperative_grace_sec,
        )
        remaining = tuple(
            managed
            for managed in initial
            if self._is_current(managed) and not managed.runner_closed.is_set()
        )
        live_runner_tasks = tuple(
            task for task in self._runner_tasks if not task.done()
        )
        for task in live_runner_tasks:
            task.cancel()
        task_wait = asyncio.create_task(
            self._wait_runner_tasks(
                live_runner_tasks,
                external_stop_deadline_sec,
            )
        )
        try:
            await self._await_runner_closure(
                remaining,
                external_stop_deadline_sec,
            )
        finally:
            pending_runner_tasks = await task_wait

        undrained = 0
        for managed in remaining:
            if self._is_current(managed) and not managed.runner_closed.is_set():
                undrained += 1
                managed.terminal_state = TerminalState.ABANDONED
                managed.terminal_kind = _ReservationKind.UNCERTAIN
                managed.terminal_decision = _UncertainAbandonment()
                managed.grant_lost.set()
        undrained += len(pending_runner_tasks)

        await stop_heartbeat_supervision()
        self._heartbeat_stopped.set()

        self._refine_shutdown_failures()

        undrained += await self._finalize_closed_grants(
            external_stop_deadline_sec
        )

        for domain in self._domains:
            self._emit_lifecycle(
                _LifecycleEvent.SHUTDOWN,
                domain,
                outcome="not_drained" if undrained else "complete",
            )
        if undrained:
            raise SupervisorNotDrainedError

    async def _finalize_closed_grants(
        self,
        wait_timeout_sec: float,
    ) -> int:
        """Start current finalizers and bound all generation cleanup."""
        for managed in tuple(self._registry.values()):
            if (
                managed.runner_closed.is_set()
                and managed.terminal_state is TerminalState.RESERVED
                and managed.terminal_kind
                in (_ReservationKind.SHUTDOWN, _ReservationKind.STORAGE)
            ):
                self._start_terminal_task(managed)

        pending = await self._wait_terminal_tasks(wait_timeout_sec)
        for task in pending:
            task.cancel()
        if pending:
            _done, pending = await asyncio.wait(pending, timeout=0)
        return len(pending)

    def _validate_shutdown_timeout(self, value: float, name: str) -> None:
        if isinstance(value, bool):
            msg = f"{name} must be a number"
            raise TypeError(msg)
        if value < 0:
            msg = f"{name} must be nonnegative"
            raise ValueError(msg)

    async def _await_runner_closure(
        self,
        entries: typing.Sequence[_ManagedGrant],
        wait_timeout_sec: float,
    ) -> None:
        waits = tuple(
            asyncio.create_task(managed.runner_closed.wait())
            for managed in entries
            if not managed.runner_closed.is_set()
        )
        if not waits:
            return
        _done, pending = await asyncio.wait(
            waits,
            timeout=wait_timeout_sec,
        )
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)

    async def _wait_runner_tasks(
        self,
        tasks: typing.Sequence[asyncio.Task[None]],
        wait_timeout_sec: float,
    ) -> set[asyncio.Task[None]]:
        pending_input = {task for task in tasks if not task.done()}
        if not pending_input:
            return set()
        _done, pending = await asyncio.wait(
            pending_input,
            timeout=wait_timeout_sec,
        )
        return pending

    async def _wait_terminal_tasks(
        self,
        wait_timeout_sec: float,
    ) -> set[asyncio.Task[_FinalizeEffect]]:
        pending_input = {
            task for task in self._terminal_tasks if not task.done()
        }
        if not pending_input:
            return set()
        _done, pending = await asyncio.wait(
            pending_input,
            timeout=wait_timeout_sec,
        )
        return pending

    def _emit_lifecycle(
        self,
        event_type: _LifecycleEvent,
        domain: _ErasedRegisteredDomain,
        *,
        outcome: str,
    ) -> None:
        record = _LifecycleRecord(
            event_type=event_type.value,
            profile=self._profile.name,
            profile_digest=self._profile_digest,
            domain_id=domain.domain_id.value,
            authority_kind=domain.authority_kind.value,
            outcome=outcome,
        )
        logger.info(
            "Grant supervisor lifecycle",
            extra={"json_fields": dataclasses.asdict(record)},
        )

    def _observe_failure_plan(
        self,
        domain: _ErasedRegisteredDomain,
        plan: failure_policy.FailurePersistencePlan,
    ) -> None:
        """Run optional telemetry without allowing it to affect lifecycle."""
        observer = self._failure_plan_observer
        if observer is None:
            return
        try:
            observer(domain.domain_id, plan)
        except Exception:
            logger.exception("Failure-plan observer raised")

    def _materialize_failure_plan(
        self,
        failure: grant_control.RunFailed,
    ) -> failure_policy.FailurePersistencePlan:
        """Validate the sole shared planner result before storage I/O."""
        plan = self._failure_planner(failure)
        if not isinstance(plan, failure_policy.FailurePersistencePlan):
            msg = "failure_planner returned an invalid plan"
            raise grant_control.GrantControlIntegrityError(msg)
        return plan

    def _is_current(self, managed: _ManagedGrant) -> bool:
        return self._registry.get(managed.slot) is managed

    def _discard_current(self, managed: _ManagedGrant) -> bool:
        """Remove one closed current grant, never its successor."""
        if not self._is_current(managed) or not managed.runner_closed.is_set():
            return False
        del self._registry[managed.slot]
        self._owned_by_domain[managed.slot.domain_id] -= 1
        self._process_owned -= 1
        return True

    def _surface_integrity_failure(self, failure: BaseException) -> None:
        self._admission_enabled = False
        if self._integrity_failure is None:
            self._integrity_failure = failure
            self._integrity_failure_event.set()
