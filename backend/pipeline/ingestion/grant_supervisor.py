"""Exact-generation supervision shared by ingestion grant domains."""

from __future__ import annotations

import asyncio
import dataclasses
import logging
import typing
import uuid  # noqa: TC003

from backend.pipeline.ingestion import (
    failure_policy,
    grant_control,
    worker_profiles,
)

logger = logging.getLogger(__name__)

_LEASE_ADMISSION_EVENT_TYPE = "lease_admission_cycle"


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


@dataclasses.dataclass(slots=True, eq=False)
class _ManagedGrant:
    domain: _ErasedRegisteredDomain
    claim: _ErasedClaim
    slot: _UnitSlot
    context: grant_control.RunContext
    runner_closed: bool = False
    runner_task: asyncio.Task[None] | None = None
    finalization_task: asyncio.Task[None] | None = None
    outcome: grant_control.RunOutcome | None = None
    discard_without_finalize: bool = False


def _erase_registered_domain[
    GrantT: grant_control.ExactGrant[typing.Hashable],
    PayloadT,
](
    registered: RegisteredDomain[GrantT, PayloadT],
    allocation: worker_profiles.DomainAllocation,
) -> _ErasedRegisteredDomain:
    """Build the one statically checked heterogeneous boundary.

    Args:
        registered: Typed control and runner for one ownership domain.
        allocation: Validated admission capacity for that domain.

    Returns:
        Erased callbacks that preserve the registered grant-payload pairing.
    """

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


class GrantSupervisor:
    """Own one lifecycle algorithm for every registered grant domain.

    Attributes:
        admission_enabled: Whether another admission cycle may begin.
        integrity_failure_event: Monotonic first-integrity-failure signal.
        integrity_failure: First fail-closed integrity outcome, if any.
    """

    def __init__(
        self,
        profile: worker_profiles.WorkerProfile,
        registered_domains: typing.Iterable[object],
        *,
        finalize_concurrency: int,
        failure_planner: FailurePlanner,
    ) -> None:
        """Initialize one closed supervisor composition.

        Args:
            profile: Validated domain topology and admission capacities.
            registered_domains: One typed registration for every profile domain.
            finalize_concurrency: Maximum concurrent storage finalizations.
            failure_planner: Pure mapping from runner failure to persistence.

        Returns:
            None.

        Raises:
            TypeError: A registration or finalization limit has an invalid type.
            ValueError: Registrations do not exactly match the profile, or the
                finalization limit is not positive.
        """
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
        self._domains = domains
        self._failure_planner = failure_planner
        self._finalize_semaphore = asyncio.Semaphore(finalize_concurrency)
        self._registry: dict[_UnitSlot, _ManagedGrant] = {}
        self._owned_by_domain = {
            domain.domain_id: 0 for domain in self._domains
        }
        self._reserved_by_domain = {
            domain.domain_id: 0 for domain in self._domains
        }
        self._admission_enabled = True
        self._shutting_down = False
        self._integrity_failure: BaseException | None = None
        self._integrity_failure_event = asyncio.Event()
        self._claim_tasks: set[asyncio.Task[int]] = set()
        self._runner_tasks: set[asyncio.Task[None]] = set()
        self._finalization_tasks: set[asyncio.Task[None]] = set()

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
        *,
        memory_paused: bool = False,
    ) -> None:
        """Run one capacity-safe primary-then-recovery admission cycle.

        Args:
            owner_worker_id: Worker that must own every returned exact grant.
            memory_paused: Whether memory pressure suppresses claims this cycle.

        Returns:
            None after all reserved claim calls settle.

        Raises:
            GrantControlIntegrityError: A claim outcome is uncertain or violates
                the exact-grant contract.
            asyncio.CancelledError: The admission cycle is cancelled.
        """
        acquired = {
            (domain.domain_id, mode): 0
            for domain in self._domains
            for mode in grant_control.ClaimMode
        }
        if not self._admission_enabled or memory_paused:
            self._log_admission_cycle(
                owner_worker_id,
                acquired,
                memory_paused=memory_paused,
                error=None,
            )
            return
        enabled = tuple(
            domain
            for domain in self._domains
            if domain.allocation.claims_enabled
        )
        if not enabled:
            self._log_admission_cycle(
                owner_worker_id,
                acquired,
                memory_paused=False,
                error=None,
            )
            return
        remaining = {
            domain.domain_id: domain.allocation.claims_per_cycle
            for domain in enabled
        }

        for mode in (
            grant_control.ClaimMode.PRIMARY,
            grant_control.ClaimMode.RECOVERY,
        ):
            if not self._admission_enabled:
                break
            reservations: list[tuple[_ErasedRegisteredDomain, int]] = []
            for domain in enabled:
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
                    acquired[(domain.domain_id, mode)] += result
                    remaining[domain.domain_id] += ask - result
            if first_failure is not None:
                self._log_admission_cycle(
                    owner_worker_id,
                    acquired,
                    memory_paused=False,
                    error=first_failure,
                )
                raise first_failure

        self._log_admission_cycle(
            owner_worker_id,
            acquired,
            memory_paused=False,
            error=None,
        )

    def _log_admission_cycle(
        self,
        owner_worker_id: uuid.UUID,
        acquired: typing.Mapping[
            tuple[grant_control.DomainId, grant_control.ClaimMode],
            int,
        ],
        *,
        memory_paused: bool,
        error: BaseException | None,
    ) -> None:
        """Emit one domain-aware operational summary for this cadence."""
        for domain in self._domains:
            allocation = domain.allocation
            active = self._owned_by_domain[domain.domain_id]
            slack = allocation.owned_cap - active
            admission_budget = (
                min(max(0, slack), allocation.claims_per_cycle)
                if (
                    self._admission_enabled
                    and allocation.claims_enabled
                    and not memory_paused
                )
                else 0
            )
            primary = acquired[
                (domain.domain_id, grant_control.ClaimMode.PRIMARY)
            ]
            recovery = acquired[
                (domain.domain_id, grant_control.ClaimMode.RECOVERY)
            ]
            logger.info(
                "Grant admission cycle",
                extra={
                    "json_fields": {
                        "event_type": _LEASE_ADMISSION_EVENT_TYPE,
                        "worker_id": str(owner_worker_id),
                        "domain_id": domain.domain_id.value,
                        "active_units": active,
                        "max_units": allocation.owned_cap,
                        "slack": slack,
                        "admission_budget": admission_budget,
                        "primary_acquired": primary,
                        "recovery_acquired": recovery,
                        "total_acquired": primary + recovery,
                        "claims_enabled": allocation.claims_enabled,
                        "admission_enabled": self._admission_enabled,
                        "memory_paused": memory_paused,
                        "error": (
                            type(error).__name__ if error is not None else None
                        ),
                    }
                },
            )

    def _reserve_admission(
        self,
        domain: _ErasedRegisteredDomain,
        remaining_cycle_budget: int,
    ) -> int:
        """Reserve currently available domain capacity for one claim call.

        Args:
            domain: Domain whose owned cap constrains the reservation.
            remaining_cycle_budget: Unspent claim budget for this cycle.

        Returns:
            Reserved claim count, or zero when no capacity remains.
        """
        domain_id = domain.domain_id
        domain_headroom = (
            domain.allocation.owned_cap
            - self._owned_by_domain[domain_id]
            - self._reserved_by_domain[domain_id]
        )
        ask = min(domain_headroom, remaining_cycle_budget)
        if ask <= 0:
            return 0
        self._reserved_by_domain[domain_id] += ask
        return ask

    async def _claim_reserved(
        self,
        domain: _ErasedRegisteredDomain,
        mode: grant_control.ClaimMode,
        owner_worker_id: uuid.UUID,
        reservation: int,
    ) -> int:
        """Execute one reserved claim mutation and register its exact grants.

        Args:
            domain: Erased domain control receiving the claim call.
            mode: Primary or recovery admission mode.
            owner_worker_id: Required owner of every returned exact grant.
            reservation: Maximum grants the call may return.

        Returns:
            Number of returned grants registered with the supervisor.

        Raises:
            GrantControlIntegrityError: The claim is uncertain or malformed.
            asyncio.CancelledError: The claim task is cancelled after failing
                the supervisor closed.
            Exception: The claim backend failed before returning a result.
        """
        remaining_reservation = reservation
        registered_count = 0
        try:
            try:
                claims = await domain.claim(mode, owner_worker_id, reservation)
            except asyncio.CancelledError as exc:
                failure = grant_control.GrantControlIntegrityError(
                    "claim outcome is unknown after cancellation"
                )
                failure.__cause__ = exc
                self._surface_integrity_failure(failure)
                raise
            except grant_control.GrantControlIntegrityError as failure:
                self._surface_integrity_failure(failure)
                raise
            except grant_control.GrantControlBackendUnavailable:
                raise
            except Exception as exc:
                failure = grant_control.GrantControlIntegrityError(
                    "claim failed outside the typed backend boundary"
                )
                failure.__cause__ = exc
                self._surface_integrity_failure(failure)
                raise failure

            try:
                self._require_claim_count(claims, reservation)
                self._validate_claim_batch(domain, owner_worker_id, claims)
                for claim in claims:
                    self._consume_reservation(domain.domain_id)
                    remaining_reservation -= 1
                    self._register_claim(domain, claim)
                    registered_count += 1
                return registered_count  # noqa: TRY300
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
        """Validate one claim batch atomically before starting any runners.

        Args:
            domain: Domain that returned the claims.
            owner_worker_id: Owner requested by the admission cycle.
            claims: Complete erased claim batch to validate.

        Returns:
            None when every claim is safe to register.

        Raises:
            GrantControlIntegrityError: Ownership, unit uniqueness, or
                generation succession is invalid.
        """
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

    def _release_reservation(
        self,
        domain_id: grant_control.DomainId,
        count: int,
    ) -> None:
        self._reserved_by_domain[domain_id] -= count

    def _register_claim(
        self,
        domain: _ErasedRegisteredDomain,
        claim: _ErasedClaim,
    ) -> None:
        """Install one exact generation and start or close its runner.

        A strictly newer same-worker generation replaces the current registry
        entry without increasing the domain ownership count. The superseded
        runner remains task-tracked until it closes. Claims that settle during
        shutdown are installed as closed for later neutral finalization without
        invoking their runner.

        Args:
            domain: Domain that produced the validated claim.
            claim: Exact grant and bound runner payload to install.

        Returns:
            None.
        """
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
        )
        self._registry[slot] = managed
        if superseded is None:
            self._owned_by_domain[domain.domain_id] += 1
        else:
            superseded.discard_without_finalize = True
            superseded.context.grant_lost.set()
            superseded.context.stop_requested.set()
            if (
                superseded.runner_task is not None
                and not superseded.runner_task.done()
            ):
                superseded.runner_task.cancel()
        if self._shutting_down:
            managed.context.stop_requested.set()
            managed.runner_closed = True
            return
        task = asyncio.create_task(self._run_managed(managed))
        managed.runner_task = task
        self._runner_tasks.add(task)
        task.add_done_callback(self._runner_task_done)

    async def _run_managed(self, managed: _ManagedGrant) -> None:
        """Run one exact generation and linearize its terminal outcome.

        Args:
            managed: Current or superseded generation to execute.

        Returns:
            None after runner closure has been acknowledged.

        Raises:
            asyncio.CancelledError: The runner task is cancelled. Cancellation
                outside ordered shutdown fails a current generation closed.
        """
        try:
            outcome = await managed.claim.run(managed.context)
            if self._is_current(managed):
                managed.outcome = outcome
            if self._is_current(managed) and isinstance(
                outcome,
                grant_control.RunLost,
            ):
                managed.discard_without_finalize = True
                managed.context.grant_lost.set()
        except asyncio.CancelledError:
            if self._is_current(managed) and not self._shutting_down:
                managed.discard_without_finalize = True
                failure = grant_control.GrantControlIntegrityError(
                    "runner was cancelled outside ordered shutdown"
                )
                self._surface_integrity_failure(failure)
            raise
        except Exception as exc:
            if self._is_current(managed):
                managed.discard_without_finalize = True
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
            managed.runner_closed = True
            self._handle_runner_closed(managed)

    def _runner_task_done(self, task: asyncio.Task[None]) -> None:
        self._runner_tasks.discard(task)
        if task.cancelled():
            return
        try:
            task.result()
        except Exception as exc:
            logger.exception("Managed grant task escaped its isolation point")
            self._surface_integrity_failure(exc)

    def _handle_runner_closed(self, managed: _ManagedGrant) -> None:
        if not self._is_current(managed) or not managed.runner_closed:
            return
        if managed.discard_without_finalize:
            self._discard_current(managed)
            return
        if self._shutting_down:
            return
        self._start_finalization_task(managed)

    def _start_finalization_task(
        self,
        managed: _ManagedGrant,
    ) -> asyncio.Task[None] | None:
        """Start finalization once for one closed current generation.

        Args:
            managed: Closed generation eligible for terminal persistence.

        Returns:
            The tracked finalization task, or ``None`` when ineligible or when
            failure planning itself fails closed.
        """
        if (
            not self._is_current(managed)
            or not managed.runner_closed
            or managed.finalization_task is not None
            or managed.discard_without_finalize
        ):
            return None
        try:
            terminal = self._terminal_decision(managed)
        except Exception as exc:
            managed.discard_without_finalize = True
            failure = grant_control.GrantControlIntegrityError(
                "failure planner did not produce a terminal decision"
            )
            failure.__cause__ = exc
            self._surface_integrity_failure(failure)
            self._discard_current(managed)
            return None
        task = asyncio.create_task(self._finalize_exact(managed, terminal))
        managed.finalization_task = task
        self._finalization_tasks.add(task)
        task.add_done_callback(self._finalization_task_done)
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
    ) -> None:
        """Finalize one exact current generation under bounded concurrency.

        Args:
            managed: Closed generation whose authority must remain current.
            terminal: Preselected neutral release or failure persistence plan.

        Returns:
            None after applying, losing, abandoning, or becoming stale.

        Raises:
            asyncio.CancelledError: Finalization cancellation is surfaced after
                the current generation fails closed.
        """
        if not self._is_current(managed):
            return
        try:
            async with self._finalize_semaphore:
                if not self._is_current(managed):
                    return
                disposition = await managed.domain.finalize(
                    managed.claim.grant,
                    managed.claim.payload,
                    terminal,
                )
        except asyncio.CancelledError as exc:
            if self._is_current(managed):
                managed.discard_without_finalize = True
                failure = grant_control.GrantControlIntegrityError(
                    "finalization outcome is unknown after cancellation"
                )
                failure.__cause__ = exc
                self._surface_integrity_failure(failure)
                self._discard_current(managed)
            raise
        except Exception as exc:
            if not self._is_current(managed):
                return
            managed.discard_without_finalize = True
            if isinstance(exc, grant_control.GrantControlIntegrityError):
                self._surface_integrity_failure(exc)
            elif isinstance(
                exc,
                grant_control.GrantControlBackendUnavailable,
            ):
                # A closed runner has no remaining side effects to protect.
                # Whether this fenced finalization committed or not, dropping
                # the local generation is safe: storage already released it,
                # or the abandonment sweep will make it recoverable.
                logger.exception(
                    "Grant finalization backend failed; "
                    "abandonment safety net will recover",
                    extra={
                        "json_fields": {
                            "domain_id": managed.domain.domain_id.value,
                            "unit_key": str(managed.claim.grant.unit_key),
                        }
                    },
                )
            else:
                failure = grant_control.GrantControlIntegrityError(
                    "finalization failed outside the typed backend boundary"
                )
                failure.__cause__ = exc
                self._surface_integrity_failure(failure)
            self._discard_current(managed)
            return

        if not self._is_current(managed):
            return
        if disposition is grant_control.FinalizeDisposition.APPLIED:
            self._discard_current(managed)
            return
        if disposition is grant_control.FinalizeDisposition.LOST:
            managed.context.grant_lost.set()
            self._discard_current(managed)
            return
        msg = "finalization returned an unknown disposition"
        failure = grant_control.GrantControlIntegrityError(msg)
        self._surface_integrity_failure(failure)
        managed.discard_without_finalize = True
        self._discard_current(managed)

    def _finalization_task_done(
        self,
        task: asyncio.Task[None],
    ) -> None:
        self._finalization_tasks.discard(task)
        if task.cancelled():
            return
        try:
            task.result()
        except Exception as exc:
            logger.exception("Grant finalization escaped its isolation point")
            self._surface_integrity_failure(exc)

    async def heartbeat_cycle(
        self,
        on_dispatch: typing.Callable[[], None],
    ) -> None:
        """Heartbeat exact current generations grouped by domain.

        Args:
            on_dispatch: Synchronous health callback invoked before storage I/O.

        Returns:
            None after every selected domain heartbeat settles.

        Raises:
            asyncio.CancelledError: The cycle is cancelled. Outside ordered
                shutdown, the affected current domain first fails closed.
        """
        on_dispatch()
        for domain in self._domains:
            expected = tuple(
                managed
                for managed in self._registry.values()
                if managed.domain is domain
                and managed.finalization_task is None
                and not managed.discard_without_finalize
            )
            if not expected:
                continue
            grants = tuple(managed.claim.grant for managed in expected)
            try:
                results = await domain.heartbeat(grants)
            except asyncio.CancelledError as exc:
                if not self._shutting_down:
                    failure = grant_control.GrantControlIntegrityError(
                        "heartbeat outcome is unknown after cancellation"
                    )
                    failure.__cause__ = exc
                    self._fail_heartbeat_domain(expected, failure)
                raise
            except Exception as error:
                self._handle_heartbeat_error(domain, expected, error)
                continue

            for managed, result in zip(expected, results, strict=True):
                if (
                    not self._is_current(managed)
                    or managed.finalization_task is not None
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
                    managed.discard_without_finalize = True
                    managed.context.stop_requested.set()
                elif (
                    result.disposition
                    is grant_control.HeartbeatDisposition.LOST
                ):
                    managed.discard_without_finalize = True
                    managed.context.grant_lost.set()
                    managed.context.stop_requested.set()
                else:
                    failure = grant_control.GrantControlIntegrityError(
                        "heartbeat returned an unknown disposition"
                    )
                    self._fail_heartbeat_domain(expected, failure)
                    continue
                if managed.runner_closed:
                    self._handle_runner_closed(managed)

    def _handle_heartbeat_error(
        self,
        domain: _ErasedRegisteredDomain,
        expected: typing.Sequence[_ManagedGrant],
        error: Exception,
    ) -> None:
        """Classify a heartbeat error as integrity or retryable backend I/O.

        Args:
            domain: Domain whose heartbeat backend failed.
            expected: Exact generations included in the request.
            error: Backend or integrity failure raised by the domain.

        Returns:
            None after fatal evidence is surfaced or retry evidence is logged.
        """
        if isinstance(error, grant_control.GrantControlIntegrityError):
            self._fail_heartbeat_domain(expected, error)
            return

        if not isinstance(
            error,
            grant_control.GrantControlBackendUnavailable,
        ):
            failure = grant_control.GrantControlIntegrityError(
                "heartbeat failed outside the typed backend boundary"
            )
            failure.__cause__ = error
            self._fail_heartbeat_domain(expected, failure)
            return

        # Heartbeat renewal is retry-safe: it can only extend the current exact
        # generation, never create or transfer authority. Keep this domain's
        # runners fenced and let the next heartbeat retry, while still renewing
        # other domains.
        logger.exception(
            "Grant heartbeat backend failed; retrying next cycle",
            extra={
                "json_fields": {
                    "domain_id": domain.domain_id.value,
                }
            },
        )

    def _fail_heartbeat_domain(
        self,
        expected: typing.Sequence[_ManagedGrant],
        failure: BaseException,
    ) -> None:
        """Fail the still-current subset of one heartbeat request closed.

        Args:
            expected: Generations submitted in the failed heartbeat request.
            failure: Uncertain or malformed heartbeat outcome to surface.

        Returns:
            None after current runners receive stop and grant-loss signals.
        """
        current = tuple(
            managed for managed in expected if self._is_current(managed)
        )
        if not current:
            return
        self._surface_integrity_failure(failure)
        for managed in current:
            managed.discard_without_finalize = True
            managed.context.grant_lost.set()
            managed.context.stop_requested.set()
            if managed.runner_closed:
                self._handle_runner_closed(managed)

    def active_count(self, domain_id: grant_control.DomainId) -> int:
        """Return the local count of running grants in one domain.

        Args:
            domain_id: Ownership domain to count.

        Returns:
            Current grants whose runners have not closed.
        """
        if domain_id not in self._owned_by_domain:
            return 0
        return sum(
            managed.slot.domain_id is domain_id and not managed.runner_closed
            for managed in self._registry.values()
        )

    async def _claim_shutdown_blocker(
        self,
        wait_timeout_sec: float,
    ) -> bool:
        """Wait for known claim mutations and report whether any remain.

        Args:
            wait_timeout_sec: Maximum remaining shutdown time to wait.

        Returns:
            Whether a known claim mutation remains unsettled.
        """
        claim_tasks = tuple(self._claim_tasks)
        if not claim_tasks:
            return False
        _done, pending_claims = await asyncio.wait(
            claim_tasks,
            timeout=wait_timeout_sec,
        )
        if not pending_claims:
            return False
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
        """Drain runners, stop heartbeats, then finalize closed grants.

        ``external_stop_deadline_sec`` is one absolute budget beginning when
        this method starts. Claim settlement, cooperative and forced runner
        drain, heartbeat supervision stop, and finalization all consume it.

        Args:
            cooperative_grace_sec: Preferred runner drain time, clamped to the
                remaining external deadline.
            external_stop_deadline_sec: Total shutdown deadline in seconds.
            stop_heartbeat_supervision: Callback that stops dispatch and joins
                any in-flight heartbeat cycle.

        Returns:
            None after all supervisor-owned work is proven quiescent.

        Raises:
            TypeError: A timeout is a boolean rather than a number.
            ValueError: A timeout is negative.
            SupervisorNotDrainedError: Claims, runners, heartbeat supervision,
                or finalizers do not settle within the shared deadline.
            asyncio.CancelledError: The enclosing shutdown is cancelled.
        """
        self._validate_timeout(
            cooperative_grace_sec,
            "cooperative_grace_sec",
        )
        self._validate_timeout(
            external_stop_deadline_sec,
            "external_stop_deadline_sec",
        )
        loop = asyncio.get_running_loop()
        external_stop_deadline = loop.time() + external_stop_deadline_sec

        def remaining_external_time() -> float:
            return max(0.0, external_stop_deadline - loop.time())

        self._admission_enabled = False
        self._shutting_down = True

        initial = tuple(self._registry.values())
        for managed in initial:
            managed.context.stop_requested.set()

        if await self._claim_shutdown_blocker(remaining_external_time()):
            raise SupervisorNotDrainedError

        runner_tasks = tuple(self._runner_tasks)
        remaining = await self._wait_tasks(
            runner_tasks,
            min(cooperative_grace_sec, remaining_external_time()),
        )
        for task in remaining:
            task.cancel()
        if remaining:
            remaining = await self._wait_tasks(
                remaining,
                remaining_external_time(),
            )

        for managed in initial:
            if self._is_current(managed) and not managed.runner_closed:
                managed.discard_without_finalize = True
                managed.context.grant_lost.set()
        undrained = len(remaining)

        await self._stop_heartbeat_supervision(
            stop_heartbeat_supervision,
            remaining_external_time(),
        )

        for managed in tuple(self._registry.values()):
            if managed.runner_closed:
                if (
                    managed.finalization_task is None
                    and managed.discard_without_finalize
                ):
                    self._handle_runner_closed(managed)
                elif managed.finalization_task is None:
                    self._start_finalization_task(managed)

        undrained += await self._finalize_closed_grants(
            remaining_external_time()
        )
        if undrained:
            raise SupervisorNotDrainedError

    async def _stop_heartbeat_supervision(
        self,
        stop: typing.Callable[[], typing.Awaitable[None]],
        wait_timeout_sec: float,
    ) -> None:
        """Stop heartbeat dispatch without exceeding the shutdown deadline.

        Args:
            stop: Callback that stops and joins heartbeat supervision.
            wait_timeout_sec: Maximum remaining shutdown time to wait.

        Returns:
            None after heartbeat supervision has stopped.

        Raises:
            SupervisorNotDrainedError: The callback times out, is cancelled, or
                raises before proving heartbeat supervision stopped.
            asyncio.CancelledError: The enclosing shutdown is cancelled.
        """

        async def stop_task() -> None:
            await stop()

        task = asyncio.create_task(stop_task())
        try:
            pending = await self._wait_tasks((task,), wait_timeout_sec)
        except asyncio.CancelledError:
            task.cancel()
            task.add_done_callback(self._heartbeat_stop_task_done)
            raise
        if pending:
            task.cancel()
            task.add_done_callback(self._heartbeat_stop_task_done)
            msg = "heartbeat supervision did not stop before the deadline"
            raise SupervisorNotDrainedError(msg)
        try:
            task.result()
        except asyncio.CancelledError as exc:
            msg = "heartbeat supervision stop was cancelled"
            raise SupervisorNotDrainedError(msg) from exc
        except Exception as exc:
            msg = "heartbeat supervision failed to stop"
            raise SupervisorNotDrainedError(msg) from exc

    def _heartbeat_stop_task_done(self, task: asyncio.Task[None]) -> None:
        """Consume a late heartbeat-stop result after bounded shutdown."""
        if not task.cancelled():
            task.exception()

    async def _finalize_closed_grants(
        self,
        wait_timeout_sec: float,
    ) -> int:
        """Bound cleanup for current and superseded finalizer tasks.

        Args:
            wait_timeout_sec: Maximum remaining shutdown time to wait.

        Returns:
            Number of finalizer tasks still running after cancellation.
        """
        pending = await self._wait_tasks(
            self._finalization_tasks,
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
