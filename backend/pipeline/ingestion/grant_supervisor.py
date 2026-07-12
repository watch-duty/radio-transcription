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
        [object, grant_control.TerminalDecision],
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
    terminal_decision: grant_control.TerminalDecision | None = None


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
        terminal: grant_control.TerminalDecision,
    ) -> grant_control.FinalizeResult[object]:
        if not isinstance(grant, registered.grant_type):
            msg = "managed finalization grant failed its registered type"
            raise grant_control.GrantControlIntegrityError(msg)
        result = await registered.control.finalize(grant, terminal)
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

    async def admit_cycle(self, owner_worker_id: uuid.UUID) -> None:
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
            results = await asyncio.gather(*tasks, return_exceptions=True)
            first_failure: BaseException | None = None
            for (_domain, _ask), result in zip(
                reservations,
                results,
                strict=True,
            ):
                if isinstance(result, BaseException):
                    if first_failure is None:
                        first_failure = result
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
            if len(claims) > reservation:
                msg = "control returned more claims than reserved"
                raise grant_control.GrantControlIntegrityError(msg)  # noqa: TRY301
            self._validate_claim_batch(domain, owner_worker_id, claims)
            for claim in claims:
                self._consume_reservation(domain.domain_id)
                remaining_reservation -= 1
                self._register_claim(domain, claim)
                registered_count += 1
            return registered_count  # noqa: TRY300
        except grant_control.GrantControlIntegrityError as exc:
            self._surface_integrity_failure(exc)
            return 0
        finally:
            if remaining_reservation:
                self._release_reservation(
                    domain.domain_id,
                    remaining_reservation,
                )

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
        root_task = asyncio.create_task(
            self._run_managed(key, claim.run),
            name=(f"grant-{domain.domain_id.value}-{claim.fencing_token}"),
        )
        managed.root_task = root_task
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
            managed.retrying = False
            managed.runner_closed.set()

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
        ):
            managed.retrying = retrying

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
