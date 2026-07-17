"""Exact-generation supervision shared by ingestion grant domains."""

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


@dataclasses.dataclass(frozen=True, slots=True)
class FeedAuthority:
    """Permanent authority identity for one Feed."""

    feed_id: uuid.UUID


@dataclasses.dataclass(frozen=True, slots=True)
class SidAuthority:
    """Permanent authority identity for one source-defined SID."""

    source_type: str
    lease_key: str

    def __post_init__(self) -> None:
        if not self.source_type.strip():
            msg = "source_type must not be empty"
            raise ValueError(msg)
        if not self.lease_key.strip():
            msg = "lease_key must not be empty"
            raise ValueError(msg)


type Authority = FeedAuthority | SidAuthority
type FailurePlanner = typing.Callable[
    [grant_control.RunFailed],
    failure_policy.FailurePersistencePlan,
]


@dataclasses.dataclass(frozen=True, slots=True)
class GrantCount:
    """Local count for one selected authority domain."""

    active: int


@dataclasses.dataclass(frozen=True, slots=True)
class SupervisorSnapshot:
    """Store-free local supervisor state."""

    profile: str
    counts_by_domain: typing.Mapping[grant_control.DomainId, GrantCount]


@dataclasses.dataclass(frozen=True, slots=True)
class ShutdownResult:
    """Bounded outcome of an ordered supervisor shutdown."""

    finalized: int
    abandoned: int
    undrained: int


@dataclasses.dataclass(frozen=True, slots=True)
class RegisteredDomain[GrantT, PayloadT]:
    """Typed control and runner for one selected authority domain.

    The registration is the sole type-erasure boundary. The supervisor owns
    the lifecycle algorithm; controls own storage translation and runners own
    source work.

    Attributes:
        domain_id: Static authority domain selected by the worker profile.
        grant_type: Concrete immutable grant class.
        payload_validator: Runtime narrowing for the erased runner payload.
        authority_of: Extracts the permanent typed authority identity.
        owner_of: Extracts the runtime-epoch worker identity.
        fencing_token_of: Extracts the exact ownership generation.
        control: Authoritative claim, heartbeat, and finalization adapter.
        runner: Source-specific work under one exact grant.
    """

    domain_id: grant_control.DomainId
    grant_type: type[GrantT]
    payload_validator: typing.Callable[[object], typing.TypeGuard[PayloadT]]
    authority_of: typing.Callable[[GrantT], Authority]
    owner_of: typing.Callable[[GrantT], uuid.UUID]
    fencing_token_of: typing.Callable[[GrantT], int]
    control: grant_control.GrantControl[GrantT, PayloadT]
    runner: grant_control.GrantRunner[GrantT, PayloadT]


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
    grant: object
    payload: object
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
    key: _GenerationKey
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


_AUTHORITY_TYPES: typing.Mapping[
    grant_control.DomainId,
    type[Authority],
] = types.MappingProxyType(
    {
        grant_control.DomainId.FEED: FeedAuthority,
        grant_control.DomainId.SID: SidAuthority,
    }
)


def _erase_registered_domain[GrantT, PayloadT](  # noqa: PLR0915
    registered: RegisteredDomain[GrantT, PayloadT],
    allocation: worker_profiles.DomainAllocation,
) -> _ErasedRegisteredDomain:
    """Build the one checked heterogeneous boundary."""
    expected_authority_type = _AUTHORITY_TYPES[registered.domain_id]

    def erase_claim(
        candidate: grant_control.ClaimedGrant[GrantT, PayloadT],
    ) -> _ErasedClaim:
        grant_value: object = candidate.grant
        if not isinstance(grant_value, registered.grant_type):
            msg = "claim returned the wrong grant type"
            raise grant_control.GrantControlIntegrityError(msg)
        grant = grant_value

        payload_value: object = candidate.payload
        try:
            payload_valid = registered.payload_validator(payload_value)
        except Exception as exc:
            msg = "claim payload validator failed"
            raise grant_control.GrantControlIntegrityError(msg) from exc
        if not payload_valid:
            msg = "claim returned an invalid runner payload"
            raise grant_control.GrantControlIntegrityError(msg)
        payload = payload_value

        try:
            authority = registered.authority_of(grant)
            owner_worker_id = registered.owner_of(grant)
            fencing_token = registered.fencing_token_of(grant)
        except Exception as exc:
            msg = "claim grant extraction failed"
            raise grant_control.GrantControlIntegrityError(msg) from exc
        if not isinstance(authority, expected_authority_type):
            msg = "claim returned the wrong authority type"
            raise grant_control.GrantControlIntegrityError(msg)
        if not isinstance(owner_worker_id, uuid.UUID):
            msg = "claim returned an invalid owner"
            raise grant_control.GrantControlIntegrityError(msg)
        if isinstance(fencing_token, bool) or not isinstance(
            fencing_token,
            int,
        ):
            msg = "claim returned an invalid fencing token"
            raise grant_control.GrantControlIntegrityError(msg)
        if fencing_token < 0:
            msg = "claim returned a negative fencing token"
            raise grant_control.GrantControlIntegrityError(msg)

        async def run(
            context: grant_control.RunContext,
        ) -> grant_control.RunOutcome:
            return await registered.runner.run(grant, payload, context)

        return _ErasedClaim(
            grant=grant,
            payload=payload,
            authority=authority,
            owner_worker_id=owner_worker_id,
            fencing_token=fencing_token,
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
        typed_grants: list[GrantT] = []
        for grant_value in grants:
            if not isinstance(grant_value, registered.grant_type):
                msg = "heartbeat received the wrong grant type"
                raise grant_control.GrantControlIntegrityError(msg)
            typed_grants.append(grant_value)
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
        if not isinstance(grant_value, registered.grant_type):
            msg = "finalization received the wrong grant type"
            raise grant_control.GrantControlIntegrityError(msg)
        if not registered.payload_validator(payload_value):
            msg = "finalization received an invalid runner payload"
            raise grant_control.GrantControlIntegrityError(msg)
        result = await registered.control.finalize(
            grant_value,
            payload_value,
            terminal,
        )
        if result.grant != grant_value:
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
                    "RegisteredDomain[object, object]",
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
        self._registry: dict[_GenerationKey, _ManagedGrant] = {}
        self._current_by_slot: dict[_AuthoritySlot, _GenerationKey] = {}
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
        self._terminal_tasks: set[asyncio.Task[_FinalizeEffect]] = set()
        self._abandoned_total = 0

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
        seen_slots: set[_AuthoritySlot] = set()
        seen_keys: set[_GenerationKey] = set()
        for claim in claims:
            if claim.owner_worker_id != owner_worker_id:
                msg = "claim owner does not match admission owner"
                raise grant_control.GrantControlIntegrityError(msg)
            slot = _AuthoritySlot(domain.domain_id, claim.authority)
            key = _GenerationKey(
                domain.domain_id,
                claim.authority,
                claim.owner_worker_id,
                claim.fencing_token,
            )
            if (
                slot in seen_slots
                or slot in self._current_by_slot
                or key in seen_keys
                or key in self._registry
            ):
                msg = "claim collides with an owned authority generation"
                raise grant_control.GrantControlIntegrityError(msg)
            seen_slots.add(slot)
            seen_keys.add(key)

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
        key = _GenerationKey(
            domain.domain_id,
            claim.authority,
            claim.owner_worker_id,
            claim.fencing_token,
        )
        slot = _AuthoritySlot(domain.domain_id, claim.authority)
        context = grant_control.RunContext(
            stop_requested=asyncio.Event(),
            grant_lost=asyncio.Event(),
        )
        managed = _ManagedGrant(
            domain=domain,
            claim=claim,
            key=key,
            context=context,
            runner_closed=asyncio.Event(),
        )
        self._registry[key] = managed
        self._current_by_slot[slot] = key
        self._owned_by_domain[domain.domain_id] += 1
        self._process_owned += 1
        task = asyncio.create_task(self._run_managed(key))
        managed.root_task = task
        task.add_done_callback(self._root_task_done)

    async def _run_managed(self, key: _GenerationKey) -> None:
        managed = self._current_exact(key)
        if managed is None:
            return
        try:
            outcome = _require_run_outcome(
                await managed.claim.run(managed.context)
            )
            managed.outcome = outcome
            if isinstance(outcome, grant_control.RunLost):
                managed.lost = True
                managed.context.grant_lost.set()
        except asyncio.CancelledError:
            managed.cancelled = True
            if not self._shutting_down:
                managed.uncertain = True
                failure = grant_control.GrantControlIntegrityError(
                    "runner was cancelled outside ordered shutdown"
                )
                self._surface_integrity_failure(failure)
            raise
        except Exception as exc:
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
            self._handle_runner_closed(key)

    def _root_task_done(self, task: asyncio.Task[None]) -> None:
        if task.cancelled():
            return
        try:
            task.result()
        except Exception:
            logger.exception("Managed grant task escaped its isolation point")

    def _handle_runner_closed(self, key: _GenerationKey) -> None:
        managed = self._current_exact(key)
        if managed is None or not managed.runner_closed.is_set():
            return
        if managed.administrative_stop or managed.lost or managed.uncertain:
            if managed.uncertain:
                self._abandoned_total += 1
            self._discard_exact_generation(key)
            return
        if self._shutting_down:
            return
        self._start_terminal_task(managed)

    def _start_terminal_task(
        self,
        managed: _ManagedGrant,
    ) -> asyncio.Task[_FinalizeEffect] | None:
        if (
            self._current_exact(managed.key) is not managed
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
            self._abandoned_total += 1
            failure = grant_control.GrantControlIntegrityError(
                "failure planner did not produce a terminal decision"
            )
            failure.__cause__ = exc
            self._surface_integrity_failure(failure)
            self._discard_exact_generation(managed.key)
            return None
        task = asyncio.create_task(self._finalize_exact(managed.key, terminal))
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

    async def _finalize_exact(
        self,
        key: _GenerationKey,
        terminal: grant_control.TerminalDecision,
    ) -> _FinalizeEffect:
        managed = self._current_exact(key)
        if managed is None:
            return _FinalizeEffect.LOST
        try:
            async with self._finalize_semaphore:
                if self._current_exact(key) is not managed:
                    return _FinalizeEffect.LOST
                disposition = await managed.domain.finalize(
                    managed.claim.grant,
                    managed.claim.payload,
                    terminal,
                )
        except asyncio.CancelledError as exc:
            managed.uncertain = True
            self._abandoned_total += 1
            failure = grant_control.GrantControlIntegrityError(
                "finalization outcome is unknown after cancellation"
            )
            failure.__cause__ = exc
            self._surface_integrity_failure(failure)
            self._discard_exact_generation(key)
            raise
        except Exception as exc:
            managed.uncertain = True
            self._abandoned_total += 1
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
            self._discard_exact_generation(key)
            return _FinalizeEffect.ABANDONED

        if disposition is grant_control.FinalizeDisposition.APPLIED:
            self._discard_exact_generation(key)
            return _FinalizeEffect.APPLIED
        if disposition is grant_control.FinalizeDisposition.LOST:
            managed.context.grant_lost.set()
            self._discard_exact_generation(key)
            return _FinalizeEffect.LOST
        msg = "finalization returned an unknown disposition"
        failure = grant_control.GrantControlIntegrityError(msg)
        self._surface_integrity_failure(failure)
        managed.uncertain = True
        self._abandoned_total += 1
        self._discard_exact_generation(key)
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
                and self._current_exact(managed.key) is managed
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
                    self._current_exact(managed.key) is not managed
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
                    self._handle_runner_closed(managed.key)

    def _fail_heartbeat_domain(
        self,
        expected: typing.Sequence[_ManagedGrant],
        failure: BaseException,
    ) -> None:
        self._surface_integrity_failure(failure)
        for managed in expected:
            if self._current_exact(managed.key) is not managed:
                continue
            managed.uncertain = True
            managed.context.grant_lost.set()
            managed.context.stop_requested.set()
            if managed.runner_closed.is_set():
                self._handle_runner_closed(managed.key)

    def snapshot(self) -> SupervisorSnapshot:
        """Return immutable local counts without storage access."""
        counts = {
            domain.domain_id: GrantCount(
                active=sum(
                    managed.domain is domain
                    and self._current_exact(managed.key) is managed
                    for managed in self._registry.values()
                )
            )
            for domain in self._domains
        }
        return SupervisorSnapshot(
            profile=self._profile.name,
            counts_by_domain=types.MappingProxyType(counts),
        )

    async def shutdown(  # noqa: PLR0912
        self,
        *,
        cooperative_grace_sec: float,
        external_stop_deadline_sec: float,
        stop_heartbeat_supervision: typing.Callable[
            [],
            typing.Awaitable[None],
        ],
    ) -> ShutdownResult:
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
        abandoned_before = self._abandoned_total

        claim_tasks = tuple(self._claim_tasks)
        pending_claims = await self._wait_tasks(
            claim_tasks,
            external_stop_deadline_sec,
        )
        for task in pending_claims:
            task.cancel()
        if pending_claims:
            pending_claims = await self._wait_tasks(
                pending_claims,
                external_stop_deadline_sec,
            )
        if pending_claims:
            return ShutdownResult(
                finalized=0,
                abandoned=0,
                undrained=len(pending_claims),
            )

        initial = tuple(self._registry.values())
        for managed in initial:
            if self._current_exact(managed.key) is managed:
                managed.context.stop_requested.set()

        root_tasks = tuple(
            managed.root_task
            for managed in initial
            if managed.root_task is not None and not managed.root_task.done()
        )
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

        undrained = 0
        for managed in initial:
            if (
                self._current_exact(managed.key) is managed
                and not managed.runner_closed.is_set()
            ):
                managed.uncertain = True
                managed.context.grant_lost.set()
                undrained += 1
                self._abandoned_total += 1

        await stop_heartbeat_supervision()

        terminal_tasks: list[asyncio.Task[_FinalizeEffect]] = []
        for managed in tuple(self._registry.values()):
            if (
                self._current_exact(managed.key) is managed
                and managed.runner_closed.is_set()
            ):
                if managed.terminal_task is not None:
                    terminal_tasks.append(managed.terminal_task)
                elif (
                    managed.administrative_stop
                    or managed.lost
                    or managed.uncertain
                ):
                    self._handle_runner_closed(managed.key)
                else:
                    task = self._start_terminal_task(managed)
                    if task is not None:
                        terminal_tasks.append(task)

        pending_terminals = await self._wait_tasks(
            terminal_tasks,
            external_stop_deadline_sec,
        )
        for task in pending_terminals:
            task.cancel()
        if pending_terminals:
            await asyncio.gather(
                *pending_terminals,
                return_exceptions=True,
            )
        effects = tuple(
            task.result()
            for task in terminal_tasks
            if task.done() and not task.cancelled()
        )
        return ShutdownResult(
            finalized=sum(
                effect is _FinalizeEffect.APPLIED for effect in effects
            ),
            abandoned=self._abandoned_total - abandoned_before,
            undrained=undrained,
        )

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

    def _current_exact(
        self,
        key: _GenerationKey,
    ) -> _ManagedGrant | None:
        managed = self._registry.get(key)
        if managed is None:
            return None
        slot = _AuthoritySlot(key.domain_id, key.authority)
        if self._current_by_slot.get(slot) != key:
            return None
        return managed

    def _discard_exact_generation(self, key: _GenerationKey) -> bool:
        managed = self._current_exact(key)
        if managed is None:
            return False
        slot = _AuthoritySlot(key.domain_id, key.authority)
        del self._registry[key]
        del self._current_by_slot[slot]
        self._owned_by_domain[key.domain_id] -= 1
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
