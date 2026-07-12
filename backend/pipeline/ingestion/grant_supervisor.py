"""Exact-generation supervision for heterogeneous ingestion grants."""

from __future__ import annotations

import asyncio
import dataclasses
import enum
import logging
import types
import typing
import uuid

from backend.pipeline.ingestion import grant_control, worker_profiles

logger = logging.getLogger(__name__)


@dataclasses.dataclass(frozen=True, slots=True)
class FeedAuthority:
    """Typed durable authority for one Feed."""

    feed_id: uuid.UUID

    def __post_init__(self) -> None:
        if not isinstance(self.feed_id, uuid.UUID):
            msg = "feed_id must be a UUID"
            raise TypeError(msg)


@dataclasses.dataclass(frozen=True, slots=True)
class SidAuthority:
    """Typed durable authority for one source-defined SID Lease."""

    source_type: str
    lease_key: str

    def __post_init__(self) -> None:
        if not isinstance(self.source_type, str):
            msg = "source_type must be a string"
            raise TypeError(msg)
        if not self.source_type.strip():
            msg = "source_type must not be empty"
            raise ValueError(msg)
        if not isinstance(self.lease_key, str):
            msg = "lease_key must be a string"
            raise TypeError(msg)
        if not self.lease_key.strip():
            msg = "lease_key must not be empty"
            raise ValueError(msg)


type Authority = FeedAuthority | SidAuthority


@dataclasses.dataclass(frozen=True, slots=True)
class GrantCount:
    """Low-cardinality local counts for one registered domain."""

    active: int
    retrying: int
    durable_failing: int


@dataclasses.dataclass(frozen=True, slots=True)
class SupervisorSnapshot:
    """Immutable local-only health projection for the selected profile."""

    profile: str
    profile_digest: str
    counts_by_domain: typing.Mapping[grant_control.DomainId, GrantCount]


@dataclasses.dataclass(frozen=True, slots=True)
class ShutdownResult:
    """Bounded result counts for one completed supervisor shutdown."""

    finalized: int
    abandoned: int
    undrained: int


@dataclasses.dataclass(frozen=True, slots=True)
class RegisteredDomain[GrantT, PayloadT]:
    """One typed grant-control and runner registration.

    Attributes:
        domain_id: Closed durable-authority domain.
        authority_kind: Stable low-cardinality authority classification.
        grant_type: Concrete immutable runtime grant class.
        payload_validator: Explicit runtime validator for the runner payload.
        authority_of: Pure typed durable-authority extractor.
        owner_of: Pure typed owner UUID extractor.
        fencing_token_of: Pure typed fencing-token extractor.
        control: Typed authoritative claim/heartbeat/finalize adapter.
        runner: Typed source-work runner.
        allocation: Validated process-local capacity declaration.
        terminal_decision_for: Pure selector for a closed runner outcome.
    """

    domain_id: grant_control.DomainId
    authority_kind: worker_profiles.AuthorityKind
    grant_type: type[GrantT]
    payload_validator: typing.Callable[[object], typing.TypeGuard[PayloadT]]
    authority_of: typing.Callable[[GrantT], Authority]
    owner_of: typing.Callable[[GrantT], uuid.UUID]
    fencing_token_of: typing.Callable[[GrantT], int]
    control: grant_control.GrantControl[GrantT, PayloadT]
    runner: grant_control.GrantRunner[GrantT, PayloadT]
    allocation: worker_profiles.DomainAllocation
    terminal_decision_for: typing.Callable[
        [grant_control.RunOutcome],
        grant_control.TerminalDecision,
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
    RETRY_STATE = "retry_state"
    FINALIZATION = "finalization"
    COUNT_SNAPSHOT = "count_snapshot"
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
    | _AdministrativeNoWrite
    | _ConfirmedLoss
    | _UncertainAbandonment
)

_STORAGE_DECISION_TYPES = (
    grant_control.NeutralRelease,
    grant_control.BudgetedFailureDecision,
    grant_control.NonBudgetedFailureDecision,
)


@dataclasses.dataclass(frozen=True, slots=True)
class _LifecycleRecord:
    event_type: str
    profile: str
    profile_digest: str
    domain_id: str
    authority_kind: str
    outcome: str
    active: int | None = None
    retrying: int | None = None
    durable_failing: int | None = None


@dataclasses.dataclass(frozen=True, slots=True)
class _AuthoritySlot:
    domain_id: grant_control.DomainId
    authority: Authority


@dataclasses.dataclass(frozen=True, slots=True)
class _GenerationKey:
    domain_id: grant_control.DomainId
    authority: Authority
    owner_worker_id: uuid.UUID
    fencing_token: int


@dataclasses.dataclass(frozen=True, slots=True)
class _ErasedClaim:
    """A claim erased only after the registered runtime checks pass."""

    grant: object
    payload: object
    lifecycle: grant_control.LifecycleEvidence
    authority: Authority
    owner_worker_id: uuid.UUID
    fencing_token: int
    run: typing.Callable[
        [grant_control.RunContext],
        typing.Awaitable[grant_control.RunOutcome],
    ]


@dataclasses.dataclass(frozen=True, slots=True)
class _ErasedHeartbeat:
    grant: object
    disposition: grant_control.HeartbeatDisposition
    lifecycle: grant_control.LifecycleEvidence | None


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
    terminal_decision_for: typing.Callable[
        [grant_control.RunOutcome],
        grant_control.TerminalDecision,
    ]


@dataclasses.dataclass(slots=True)
class _ManagedGrant:
    domain: _ErasedRegisteredDomain
    grant: object
    payload: object
    lifecycle: grant_control.LifecycleEvidence
    key: _GenerationKey
    root_task: asyncio.Task[None] | None
    stop_requested: asyncio.Event
    grant_lost: asyncio.Event
    runner_closed: asyncio.Event
    retrying: bool = False
    closed_outcome: grant_control.RunOutcome | None = None
    terminal_state: TerminalState = TerminalState.OPEN
    terminal_kind: _ReservationKind | None = None
    terminal_decision: _TerminalReservation | None = None
    terminal_task: asyncio.Task[_FinalizeEffect] | None = None


_AUTHORITY_TYPES: typing.Mapping[
    worker_profiles.AuthorityKind,
    type[object],
] = types.MappingProxyType(
    {
        worker_profiles.AuthorityKind.FEED: FeedAuthority,
        worker_profiles.AuthorityKind.SID_LEASE: SidAuthority,
    }
)


def _erase_registered_domain[GrantT, PayloadT](  # noqa: PLR0915
    registered: RegisteredDomain[GrantT, PayloadT],
) -> _ErasedRegisteredDomain:
    """Build the sole checked conversion from one typed registration."""

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

    def check_claim(candidate: object) -> _ErasedClaim:
        if not isinstance(candidate, grant_control.ClaimedGrant):
            msg = "claim returned an invalid result type"
            raise grant_control.GrantControlIntegrityError(msg)
        grant = candidate.grant
        payload = candidate.payload
        if not isinstance(grant, registered.grant_type):
            msg = "claim returned a grant for the wrong domain"
            raise grant_control.GrantControlIntegrityError(msg)
        try:
            payload_valid = registered.payload_validator(payload)
        except Exception as exc:
            msg = "claim payload validator raised"
            raise grant_control.GrantControlIntegrityError(msg) from exc
        if not payload_valid:
            msg = "claim returned a malformed runner payload"
            raise grant_control.GrantControlIntegrityError(msg)
        if not isinstance(candidate.lifecycle, grant_control.LifecycleEvidence):
            msg = "claim returned invalid lifecycle evidence"
            raise grant_control.GrantControlIntegrityError(msg)
        try:
            authority = registered.authority_of(grant)
            owner_worker_id = registered.owner_of(grant)
            fencing_token = registered.fencing_token_of(grant)
        except Exception as exc:
            msg = "claim grant extractor raised"
            raise grant_control.GrantControlIntegrityError(msg) from exc
        expected_authority_type = _AUTHORITY_TYPES[registered.authority_kind]
        if not isinstance(authority, expected_authority_type):
            msg = "claim returned an authority for the wrong domain"
            raise grant_control.GrantControlIntegrityError(msg)
        if not isinstance(owner_worker_id, uuid.UUID):
            msg = "claim returned an invalid owner UUID"
            raise grant_control.GrantControlIntegrityError(msg)
        if isinstance(fencing_token, bool) or not isinstance(
            fencing_token,
            int,
        ):
            msg = "claim returned a non-integer fencing token"
            raise grant_control.GrantControlIntegrityError(msg)
        if fencing_token < 0:
            msg = "claim returned a negative fencing token"
            raise grant_control.GrantControlIntegrityError(msg)
        return _ErasedClaim(
            grant=grant,
            payload=payload,
            lifecycle=candidate.lifecycle,
            authority=authority,
            owner_worker_id=owner_worker_id,
            fencing_token=fencing_token,
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
        if not isinstance(candidates, tuple):
            msg = "claim results must be an immutable tuple"
            raise grant_control.GrantControlIntegrityError(msg)
        return tuple(check_claim(candidate) for candidate in candidates)

    async def heartbeat(
        grants: typing.Sequence[object],
    ) -> tuple[_ErasedHeartbeat, ...]:
        typed_grants: list[GrantT] = []
        for grant in grants:
            if not isinstance(grant, registered.grant_type):
                msg = "managed heartbeat grant failed its registered type"
                raise grant_control.GrantControlIntegrityError(msg)
            typed_grants.append(grant)
        results = await registered.control.heartbeat(tuple(typed_grants))
        if not isinstance(results, tuple):
            msg = "heartbeat results must be an immutable tuple"
            raise grant_control.GrantControlIntegrityError(msg)
        erased: list[_ErasedHeartbeat] = []
        for result in results:
            if not isinstance(result, grant_control.GrantHeartbeat):
                msg = "heartbeat returned an invalid result type"
                raise grant_control.GrantControlIntegrityError(msg)
            erased.append(
                _ErasedHeartbeat(
                    grant=result.grant,
                    disposition=result.disposition,
                    lifecycle=result.lifecycle,
                )
            )
        return tuple(erased)

    async def finalize(
        grant: object,
        payload: object,
        terminal: grant_control.TerminalDecision,
    ) -> grant_control.FinalizeResult[object]:
        if not isinstance(grant, registered.grant_type):
            msg = "managed finalization grant failed its registered type"
            raise grant_control.GrantControlIntegrityError(msg)
        if not registered.payload_validator(payload):
            msg = "managed finalization payload failed validation"
            raise grant_control.GrantControlIntegrityError(msg)
        result = await registered.control.finalize(grant, payload, terminal)
        if not isinstance(result, grant_control.FinalizeResult):
            msg = "finalize returned an invalid result type"
            raise grant_control.GrantControlIntegrityError(msg)
        return grant_control.FinalizeResult(
            grant=result.grant,
            disposition=result.disposition,
            lifecycle=result.lifecycle,
        )

    return _ErasedRegisteredDomain(
        domain_id=registered.domain_id,
        authority_kind=registered.authority_kind,
        allocation=registered.allocation,
        claim=claim,
        heartbeat=heartbeat,
        finalize=finalize,
        terminal_decision_for=registered.terminal_decision_for,
    )


class GrantSupervisor:
    """Own one capacity-safe, exact-generation grant registry."""

    def __init__(  # noqa: PLR0912, PLR0915
        self,
        profile: worker_profiles.WorkerProfile,
        registered_domains: typing.Iterable[object],
        *,
        finalize_concurrency: int,
    ) -> None:
        """Validate immutable composition before any admission side effect."""
        self._profile = worker_profiles.validate_worker_profile(profile)
        if isinstance(finalize_concurrency, bool) or not isinstance(
            finalize_concurrency,
            int,
        ):
            msg = "finalize_concurrency must be an integer"
            raise TypeError(msg)
        if finalize_concurrency <= 0:
            msg = "finalize_concurrency must be positive"
            raise ValueError(msg)

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
            if allocation is None or registered.allocation != allocation:
                msg = "registered domain allocation does not match profile"
                raise ValueError(msg)
            catalog_entry = worker_profiles.DOMAIN_CATALOG.get(
                registered.domain_id
            )
            if catalog_entry is None:
                msg = "registered domain is absent from the static catalog"
                raise ValueError(msg)
            if registered.authority_kind is not catalog_entry.authority_kind:
                msg = "registered authority kind does not match the catalog"
                raise ValueError(msg)
            if catalog_entry.logical_authority in logical_authorities:
                msg = "registered domains duplicate a logical authority"
                raise ValueError(msg)
            logical_authorities.add(catalog_entry.logical_authority)
            if not isinstance(registered.grant_type, type):
                msg = "registered grant_type must be a concrete runtime class"
                raise TypeError(msg)
            for callback in (
                registered.payload_validator,
                registered.authority_of,
                registered.owner_of,
                registered.fencing_token_of,
                registered.terminal_decision_for,
            ):
                if not callable(callback):
                    msg = "registered domain callbacks must be callable"
                    raise TypeError(msg)
            by_domain[registered.domain_id] = _erase_registered_domain(
                registered
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
        self._registry: dict[_GenerationKey, _ManagedGrant] = {}
        self._current_by_slot: dict[_AuthoritySlot, _GenerationKey] = {}
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
        seen_slots: set[_AuthoritySlot] = set()
        seen_keys: set[_GenerationKey] = set()
        for claim in claims:
            if claim.owner_worker_id != owner_worker_id:
                msg = "claim owner does not match the admission owner"
                raise grant_control.GrantControlIntegrityError(msg)
            slot = _AuthoritySlot(domain.domain_id, claim.authority)
            key = _GenerationKey(
                domain_id=domain.domain_id,
                authority=claim.authority,
                owner_worker_id=claim.owner_worker_id,
                fencing_token=claim.fencing_token,
            )
            if slot in seen_slots or key in seen_keys:
                msg = "claim batch contains a duplicate authority or generation"
                raise grant_control.GrantControlIntegrityError(msg)
            if slot in self._current_by_slot or key in self._registry:
                msg = "claim collides with a currently managed authority"
                raise grant_control.GrantControlIntegrityError(msg)
            seen_slots.add(slot)
            seen_keys.add(key)

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
        key = _GenerationKey(
            domain_id=domain.domain_id,
            authority=claim.authority,
            owner_worker_id=claim.owner_worker_id,
            fencing_token=claim.fencing_token,
        )
        slot = _AuthoritySlot(domain.domain_id, claim.authority)
        managed = _ManagedGrant(
            domain=domain,
            grant=claim.grant,
            payload=claim.payload,
            lifecycle=claim.lifecycle,
            key=key,
            root_task=None,
            stop_requested=asyncio.Event(),
            grant_lost=asyncio.Event(),
            runner_closed=asyncio.Event(),
        )
        self._registry[key] = managed
        self._current_by_slot[slot] = key
        self._owned_by_domain[domain.domain_id] += 1
        self._process_owned += 1
        if not start_runner:
            managed.closed_outcome = grant_control.RunStopped(
                grant_control.TerminalCause.SHUTDOWN
            )
            managed.runner_closed.set()
            self._reserve_terminal(
                key,
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
            self._run_managed(key, claim.run),
            name=(f"grant-{domain.domain_id.value}-{claim.fencing_token}"),
        )
        managed.root_task = root_task
        self._emit_lifecycle(
            _LifecycleEvent.ADMISSION,
            domain,
            outcome="registered",
        )
        root_task.add_done_callback(
            lambda task, generation=key: self._root_done(generation, task)
        )

    async def _run_managed(
        self,
        key: _GenerationKey,
        run: typing.Callable[
            [grant_control.RunContext],
            typing.Awaitable[grant_control.RunOutcome],
        ],
    ) -> None:
        managed = self._registry.get(key)
        if managed is None:
            return
        context = grant_control.RunContext(
            stop_requested=managed.stop_requested,
            grant_lost=managed.grant_lost,
            set_retrying=lambda retrying: self._set_retrying(key, retrying),
        )
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
            current = self._registry.get(key)
            if current is managed:
                managed.closed_outcome = outcome
        except asyncio.CancelledError:
            current = self._registry.get(key)
            if current is managed:
                managed.closed_outcome = grant_control.RunStopped(
                    grant_control.TerminalCause.CANCELLATION
                )
        finally:
            if managed.retrying:
                self._set_retrying(key, False)  # noqa: FBT003
            managed.runner_closed.set()
            self._handle_runner_closed(key)

    def _set_retrying(
        self,
        key: _GenerationKey,
        retrying: bool,  # noqa: FBT001
    ) -> None:
        if not isinstance(retrying, bool):
            msg = "retrying must be a bool"
            raise TypeError(msg)
        managed = self._registry.get(key)
        slot = _AuthoritySlot(key.domain_id, key.authority)
        if (
            managed is not None
            and self._current_by_slot.get(slot) == key
            and managed.key == key
            and (managed.terminal_state is TerminalState.OPEN or not retrying)
        ):
            if managed.retrying == retrying:
                return
            managed.retrying = retrying
            self._emit_lifecycle(
                _LifecycleEvent.RETRY_STATE,
                managed.domain,
                outcome="retrying" if retrying else "active",
            )

    def _root_done(
        self,
        key: _GenerationKey,
        task: asyncio.Task[None],
    ) -> None:
        try:
            exception = task.exception()
        except asyncio.CancelledError:
            return
        if exception is None:
            return
        slot = _AuthoritySlot(key.domain_id, key.authority)
        if (
            self._registry.get(key) is not None
            and self._current_by_slot.get(slot) == key
        ):
            self._surface_integrity_failure(exception)

    def _handle_runner_closed(  # noqa: PLR0911, PLR0912
        self,
        key: _GenerationKey,
    ) -> None:
        managed = self._current_exact(key)
        if managed is None:
            return
        if managed.terminal_kind is _ReservationKind.ADMINISTRATIVE:
            self._emit_lifecycle(
                _LifecycleEvent.ADMINISTRATIVE_STOP,
                managed.domain,
                outcome="closed",
            )
            self._discard_exact_generation(key)
            return
        if managed.terminal_kind is _ReservationKind.CONFIRMED_LOSS:
            self._emit_lifecycle(
                _LifecycleEvent.LOSS,
                managed.domain,
                outcome="closed",
            )
            self._discard_exact_generation(key)
            return
        if managed.terminal_kind in (
            _ReservationKind.UNCERTAIN,
            _ReservationKind.SHUTDOWN,
            _ReservationKind.STORAGE,
        ):
            return

        outcome = managed.closed_outcome
        if outcome is None:
            if self._reserve_terminal(key, _UncertainAbandonment()):
                failure = grant_control.GrantControlIntegrityError(
                    "runner closed without a valid outcome"
                )
                self._surface_integrity_failure(failure)
            return
        if isinstance(outcome, grant_control.RunLost):
            if self._reserve_terminal(key, _ConfirmedLoss()):
                self._discard_exact_generation(key)
            return
        if isinstance(outcome, grant_control.RunFailed):
            try:
                decision = managed.domain.terminal_decision_for(outcome)
            except Exception as exc:
                self._reserve_terminal(key, _UncertainAbandonment())
                self._surface_integrity_failure(exc)
                return
            if not isinstance(decision, _STORAGE_DECISION_TYPES):
                failure = grant_control.GrantControlIntegrityError(
                    "terminal selector returned an invalid decision"
                )
                self._reserve_terminal(key, _UncertainAbandonment())
                self._surface_integrity_failure(failure)
                return
        elif isinstance(outcome, grant_control.RunStopped):
            decision = grant_control.NeutralRelease(outcome.cause)
        else:
            decision = grant_control.NeutralRelease(
                grant_control.TerminalCause.NORMAL
            )
        if self._reserve_terminal(key, decision):
            self._start_terminal_task(key)

    def _reserve_terminal(
        self,
        key: _GenerationKey,
        decision: _TerminalReservation,
    ) -> bool:
        """Synchronously reserve the first terminal owner for ``key``."""
        managed = self._current_exact(key)
        if managed is None or managed.terminal_state is not TerminalState.OPEN:
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
        elif isinstance(decision, _STORAGE_DECISION_TYPES):
            kind = _ReservationKind.STORAGE
            state = TerminalState.RESERVED
        else:
            msg = "decision must be a closed terminal reservation"
            raise TypeError(msg)
        managed.terminal_kind = kind
        managed.terminal_decision = decision
        managed.terminal_state = state
        return True

    def _current_exact(self, key: _GenerationKey) -> _ManagedGrant | None:
        managed = self._registry.get(key)
        slot = _AuthoritySlot(key.domain_id, key.authority)
        if (
            managed is None
            or managed.key != key
            or self._current_by_slot.get(slot) != key
        ):
            return None
        return managed

    def _start_terminal_task(
        self,
        key: _GenerationKey,
    ) -> asyncio.Task[_FinalizeEffect]:
        managed = self._current_exact(key)
        if managed is None:
            msg = "cannot start finalization for a non-current generation"
            raise RuntimeError(msg)
        if managed.terminal_task is not None:
            return managed.terminal_task
        task = asyncio.create_task(
            self._finalize_reserved(key),
            name=f"grant-finalize-{key.domain_id.value}-{key.fencing_token}",
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
        key: _GenerationKey,
    ) -> _FinalizeEffect:
        managed = self._current_exact(key)
        if (
            managed is None
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
        decision = managed.terminal_decision
        if not isinstance(decision, _STORAGE_DECISION_TYPES):
            return _FinalizeEffect.SKIPPED
        try:
            async with self._finalize_semaphore:
                current = self._current_exact(key)
                if (
                    current is not managed
                    or managed.terminal_state is not TerminalState.RESERVED
                    or managed.terminal_decision is not decision
                ):
                    return _FinalizeEffect.SKIPPED
                result = await managed.domain.finalize(
                    managed.grant,
                    managed.payload,
                    decision,
                )
        except asyncio.CancelledError as exc:
            current = self._current_exact(key)
            if current is managed:
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
            current = self._current_exact(key)
            if current is managed:
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

        current = self._current_exact(key)
        if (
            current is not managed
            or managed.terminal_state is not TerminalState.RESERVED
            or managed.terminal_decision is not decision
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
        ) or (
            result.lifecycle is not None
            and not isinstance(
                result.lifecycle,
                grant_control.LifecycleEvidence,
            )
        ):
            failure = grant_control.GrantControlIntegrityError(
                "finalize result lifecycle or disposition is malformed"
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
            if result.lifecycle is not None:
                managed.lifecycle = result.lifecycle
            managed.terminal_state = TerminalState.FINALIZED
            self._emit_lifecycle(
                _LifecycleEvent.FINALIZATION,
                managed.domain,
                outcome="finalized",
            )
            self._discard_exact_generation(key)
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
            self._discard_exact_generation(key)
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
                and self._current_exact(managed.key) is managed
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
                current = self._current_exact(managed.key)
                if (
                    current is not managed
                    or managed.terminal_state is not state
                    or managed.terminal_kind is not kind
                    or not self._heartbeat_eligible(managed)
                ):
                    continue
                if (
                    result.disposition
                    is grant_control.HeartbeatDisposition.RETAINED
                ):
                    lifecycle = result.lifecycle
                    if lifecycle is None:
                        continue
                    managed.lifecycle = lifecycle
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
                        managed.key,
                        _AdministrativeNoWrite(),
                    ):
                        self._emit_lifecycle(
                            _LifecycleEvent.ADMINISTRATIVE_STOP,
                            domain,
                            outcome="reserved",
                        )
                        managed.stop_requested.set()
                        if managed.runner_closed.is_set():
                            self._handle_runner_closed(managed.key)
                elif (
                    result.disposition
                    is grant_control.HeartbeatDisposition.LOST
                ):
                    if self._reserve_terminal(managed.key, _ConfirmedLoss()):
                        self._emit_lifecycle(
                            _LifecycleEvent.LOSS,
                            domain,
                            outcome="reserved",
                        )
                        managed.grant_lost.set()
                        if managed.runner_closed.is_set():
                            self._handle_runner_closed(managed.key)

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
            retained = (
                result.disposition
                is grant_control.HeartbeatDisposition.RETAINED
            )
            if retained != isinstance(
                result.lifecycle,
                grant_control.LifecycleEvidence,
            ):
                msg = "heartbeat lifecycle evidence is malformed"
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
        self._admission_enabled = False
        for managed, state, kind in expected:
            if (
                self._current_exact(managed.key) is not managed
                or managed.terminal_state is not state
                or managed.terminal_kind is not kind
                or not self._heartbeat_eligible(managed)
            ):
                continue
            if self._reserve_terminal(
                managed.key,
                _UncertainAbandonment(),
            ):
                managed.grant_lost.set()
                managed.stop_requested.set()
        self._emit_lifecycle(
            _LifecycleEvent.UNAVAILABLE,
            domain,
            outcome="fail_closed",
        )
        self._surface_integrity_failure(failure)

    def snapshot(self) -> SupervisorSnapshot:
        """Return a store-free immutable local lifecycle count snapshot."""
        counts = {
            domain.domain_id: self._count_for_domain(domain)
            for domain in self._domains
        }
        for domain in self._domains:
            count = counts[domain.domain_id]
            self._emit_lifecycle(
                _LifecycleEvent.COUNT_SNAPSHOT,
                domain,
                outcome="observed",
                count=count,
            )
        return SupervisorSnapshot(
            profile=self._profile.name,
            profile_digest=self._profile_digest,
            counts_by_domain=types.MappingProxyType(counts),
        )

    def _count_for_domain(
        self,
        domain: _ErasedRegisteredDomain,
    ) -> GrantCount:
        entries = tuple(
            managed
            for managed in self._registry.values()
            if managed.domain is domain
            and self._current_exact(managed.key) is managed
        )
        active_entries = tuple(
            managed
            for managed in entries
            if managed.terminal_state is TerminalState.OPEN
            or (
                managed.terminal_state is TerminalState.RESERVED
                and not managed.runner_closed.is_set()
            )
        )
        return GrantCount(
            active=len(active_entries),
            retrying=sum(managed.retrying for managed in active_entries),
            durable_failing=sum(
                managed.lifecycle.durable_failing for managed in entries
            ),
        )

    async def _claim_shutdown_blocker(
        self,
        wait_timeout_sec: float,
    ) -> ShutdownResult | None:
        """Wait for known claim mutations or report a fail-closed drain."""
        claim_tasks = tuple(self._claim_tasks)
        if not claim_tasks:
            return None
        _done, pending_claims = await asyncio.wait(
            claim_tasks,
            timeout=wait_timeout_sec,
        )
        if not pending_claims:
            return None
        current = tuple(
            managed
            for managed in self._registry.values()
            if self._current_exact(managed.key) is managed
        )
        for managed in current:
            managed.stop_requested.set()
        return ShutdownResult(
            finalized=0,
            abandoned=sum(
                managed.terminal_state is TerminalState.ABANDONED
                for managed in current
            ),
            undrained=len(pending_claims)
            + sum(not managed.runner_closed.is_set() for managed in current),
        )

    async def shutdown(
        self,
        *,
        cooperative_grace_sec: float,
        external_stop_deadline_sec: float,
        stop_heartbeat_supervision: typing.Callable[
            [],
            typing.Awaitable[None],
        ],
    ) -> ShutdownResult:
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
        blocked = await self._claim_shutdown_blocker(external_stop_deadline_sec)
        if blocked is not None:
            return blocked
        initial = tuple(self._registry.values())
        for managed in initial:
            if self._current_exact(managed.key) is managed:
                self._reserve_terminal(
                    managed.key,
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
            if self._current_exact(managed.key) is managed:
                managed.stop_requested.set()

        await self._await_runner_closure(
            initial,
            cooperative_grace_sec,
        )
        remaining = tuple(
            managed
            for managed in initial
            if self._current_exact(managed.key) is managed
            and not managed.runner_closed.is_set()
        )
        for managed in remaining:
            task = managed.root_task
            if task is not None and not task.done():
                task.cancel()
        await self._await_runner_closure(
            remaining,
            external_stop_deadline_sec,
        )

        undrained = 0
        for managed in remaining:
            if (
                self._current_exact(managed.key) is managed
                and not managed.runner_closed.is_set()
            ):
                undrained += 1
                managed.terminal_state = TerminalState.ABANDONED
                managed.terminal_kind = _ReservationKind.UNCERTAIN
                managed.terminal_decision = _UncertainAbandonment()
                managed.grant_lost.set()

        await stop_heartbeat_supervision()
        self._heartbeat_stopped.set()

        shutdown_tasks = tuple(
            self._start_terminal_task(managed.key)
            for managed in tuple(self._registry.values())
            if self._current_exact(managed.key) is managed
            and managed.runner_closed.is_set()
            and managed.terminal_state is TerminalState.RESERVED
            and managed.terminal_kind is _ReservationKind.SHUTDOWN
        )
        effects = (
            await asyncio.gather(*shutdown_tasks) if shutdown_tasks else ()
        )
        await self._settle_terminal_tasks()
        finalized = sum(
            effect is _FinalizeEffect.FINALIZED for effect in effects
        )
        abandoned = sum(
            managed.terminal_state is TerminalState.ABANDONED
            for managed in self._registry.values()
        )
        for domain in self._domains:
            self._emit_lifecycle(
                _LifecycleEvent.SHUTDOWN,
                domain,
                outcome="complete",
            )
        return ShutdownResult(
            finalized=finalized,
            abandoned=abandoned,
            undrained=undrained,
        )

    def _validate_shutdown_timeout(self, value: float, name: str) -> None:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
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

    async def _settle_terminal_tasks(self) -> None:
        while self._terminal_tasks:
            tasks = tuple(self._terminal_tasks)
            await asyncio.gather(*tasks, return_exceptions=True)
            self._terminal_tasks.difference_update(tasks)

    def _emit_lifecycle(
        self,
        event_type: _LifecycleEvent,
        domain: _ErasedRegisteredDomain,
        *,
        outcome: str,
        count: GrantCount | None = None,
    ) -> None:
        record = _LifecycleRecord(
            event_type=event_type.value,
            profile=self._profile.name,
            profile_digest=self._profile_digest,
            domain_id=domain.domain_id.value,
            authority_kind=domain.authority_kind.value,
            outcome=outcome,
            active=count.active if count is not None else None,
            retrying=count.retrying if count is not None else None,
            durable_failing=(
                count.durable_failing if count is not None else None
            ),
        )
        logger.info(
            "Grant supervisor lifecycle",
            extra={"json_fields": dataclasses.asdict(record)},
        )

    def _discard_exact_generation(self, key: _GenerationKey) -> bool:
        """Remove one closed exact generation, never its successor."""
        managed = self._registry.get(key)
        slot = _AuthoritySlot(key.domain_id, key.authority)
        if (
            managed is None
            or not managed.runner_closed.is_set()
            or self._current_by_slot.get(slot) != key
        ):
            return False
        del self._registry[key]
        del self._current_by_slot[slot]
        self._owned_by_domain[key.domain_id] -= 1
        self._process_owned -= 1
        return True

    def _surface_integrity_failure(self, failure: BaseException) -> None:
        self._admission_enabled = False
        if self._integrity_failure is None:
            self._integrity_failure = failure
            self._integrity_failure_event.set()
