"""Immutable worker-profile and authority-domain configuration values."""

from __future__ import annotations

import dataclasses
import enum
import types
import typing

from backend.pipeline.ingestion import grant_control


class BcfyCallsAuthorityMode(enum.StrEnum):
    """Startup-only authority selection for Broadcastify Calls."""

    LEGACY_FEED = "legacy_feed"
    SID_LEASE = "sid_lease"


@dataclasses.dataclass(frozen=True, slots=True)
class DomainAllocation:
    """One immutable domain admission allocation.

    Attributes:
        domain_id: Durable-authority domain selected for this process.
        owned_cap: Maximum grants the process may hold for the domain.
        claims_per_cycle: Maximum new grants requested per admission cycle.
        claims_enabled: Effective startup decision to admit new grants.
    """

    domain_id: grant_control.DomainId
    owned_cap: int
    claims_per_cycle: int
    claims_enabled: bool


@dataclasses.dataclass(frozen=True, slots=True)
class WorkerProfile:
    """One immutable process topology and capacity selection.

    Attributes:
        name: Stable deployment-facing profile selector.
        allocations: Immutable per-domain admission allocations.
    """

    name: str
    allocations: tuple[DomainAllocation, ...]


LEGACY_PROFILE = WorkerProfile(
    name="legacy",
    allocations=(
        DomainAllocation(
            domain_id=grant_control.DomainId.FEED,
            owned_cap=800,
            claims_per_cycle=20,
            claims_enabled=True,
        ),
    ),
)

MIXED_DORMANT_PROFILE = WorkerProfile(
    name="mixed-dormant",
    allocations=(
        LEGACY_PROFILE.allocations[0],
        DomainAllocation(
            domain_id=grant_control.DomainId.SID,
            owned_cap=32,
            claims_per_cycle=2,
            claims_enabled=False,
        ),
    ),
)

SID_DORMANT_PROFILE = WorkerProfile(
    name="sid-dormant",
    allocations=(MIXED_DORMANT_PROFILE.allocations[1],),
)

WORKER_PROFILE_PRESETS: typing.Mapping[str, WorkerProfile] = (
    types.MappingProxyType(
        {
            LEGACY_PROFILE.name: LEGACY_PROFILE,
            MIXED_DORMANT_PROFILE.name: MIXED_DORMANT_PROFILE,
            SID_DORMANT_PROFILE.name: SID_DORMANT_PROFILE,
        }
    )
)


def _positive_int(value: int, field_name: str) -> int:
    if isinstance(value, bool) or value <= 0:
        msg = f"{field_name} must be a positive integer"
        raise ValueError(msg)
    return value


def _validate_profile_shape(profile: WorkerProfile) -> None:
    if not profile.name.strip():
        msg = "Worker profile name must not be empty"
        raise ValueError(msg)
    if not profile.allocations:
        msg = "Worker profile must select at least one domain"
        raise ValueError(msg)


def _validate_allocation(
    allocation: DomainAllocation,
) -> DomainAllocation:
    owned_cap = _positive_int(
        allocation.owned_cap,
        f"Domain {allocation.domain_id.value} owned_cap",
    )
    claims_per_cycle = _positive_int(
        allocation.claims_per_cycle,
        f"Domain {allocation.domain_id.value} claims_per_cycle",
    )
    if claims_per_cycle > owned_cap:
        msg = (
            f"Domain {allocation.domain_id.value} claims_per_cycle "
            "must not exceed owned_cap"
        )
        raise ValueError(msg)
    return allocation


def validate_worker_profile(profile: WorkerProfile) -> WorkerProfile:
    """Validate and return one closed immutable worker profile.

    Args:
        profile: Candidate profile value.

    Returns:
        The validated immutable profile.

    Raises:
        ValueError: If profile structure or allocation is invalid.
    """
    _validate_profile_shape(profile)

    seen_domains: set[grant_control.DomainId] = set()
    for candidate in profile.allocations:
        allocation = _validate_allocation(candidate)
        if allocation.domain_id in seen_domains:
            msg = (
                f"Duplicate worker profile domain: {allocation.domain_id.value}"
            )
            raise ValueError(msg)
        seen_domains.add(allocation.domain_id)
    return profile


def allocation_for_domain(
    profile: WorkerProfile,
    domain_id: grant_control.DomainId,
) -> DomainAllocation | None:
    """Return the selected allocation for ``domain_id``, if any.

    Args:
        profile: Worker topology to inspect.
        domain_id: Durable-authority domain to locate.

    Returns:
        The matching immutable allocation, or ``None`` when absent.
    """
    for allocation in profile.allocations:
        if allocation.domain_id is domain_id:
            return allocation
    return None


def derive_bcfy_calls_authority(
    profile: WorkerProfile,
    mode: BcfyCallsAuthorityMode,
) -> WorkerProfile:
    """Derive both Calls claim authorities from one closed startup mode.

    ``DomainAllocation.claims_enabled`` is an effective runtime output. The
    input profile contributes only selected topology and admission capacity;
    its claim flags cannot independently select Broadcastify Calls ownership.

    Args:
        profile: Immutable topology and capacity description.
        mode: Sole process-wide Broadcastify Calls authority selection.

    Returns:
        A validated frozen profile with both claim authorities derived.

    Raises:
        ValueError: If the topology cannot host the selected authority.
    """
    validated = validate_worker_profile(profile)

    feed_allocation = allocation_for_domain(
        validated,
        grant_control.DomainId.FEED,
    )
    sid_allocation = allocation_for_domain(
        validated,
        grant_control.DomainId.SID,
    )
    if mode is BcfyCallsAuthorityMode.LEGACY_FEED and feed_allocation is None:
        msg = "legacy_feed authority requires a selected Feed domain"
        raise ValueError(msg)
    if mode is BcfyCallsAuthorityMode.SID_LEASE and sid_allocation is None:
        msg = "sid_lease authority requires a selected SID domain"
        raise ValueError(msg)

    allocations: list[DomainAllocation] = []
    for allocation in validated.allocations:
        if allocation.domain_id is grant_control.DomainId.FEED:
            claims_enabled = True
        elif allocation.domain_id is grant_control.DomainId.SID:
            claims_enabled = mode is BcfyCallsAuthorityMode.SID_LEASE
        else:
            claims_enabled = allocation.claims_enabled
        allocations.append(
            dataclasses.replace(
                allocation,
                claims_enabled=claims_enabled,
            )
        )
    return validate_worker_profile(
        dataclasses.replace(validated, allocations=tuple(allocations))
    )


def resolve_worker_profile(
    selector: str | None,
    *,
    feed_owned_cap: int = 800,
    feed_claims_per_cycle: int = 20,
    sid_owned_cap: int = 32,
    sid_claims_per_cycle: int = 2,
) -> WorkerProfile:
    """Resolve a closed preset with explicit immutable domain capacities.

    Args:
        selector: Exact preset name, or ``None`` for the legacy profile.
        feed_owned_cap: Feed-domain ownership ceiling.
        feed_claims_per_cycle: Feed-domain admission-cycle budget.
        sid_owned_cap: SID-domain ownership ceiling.
        sid_claims_per_cycle: SID-domain admission-cycle budget.

    Returns:
        A validated immutable profile containing the selected domains only.

    Raises:
        ValueError: ``selector`` is blank or unknown, or a selected capacity
            is invalid.
        TypeError: A selected profile value has an invalid type.
    """
    if selector is None:
        preset = LEGACY_PROFILE
    elif not selector.strip():
        msg = "WORKER_PROFILE must not be blank"
        raise ValueError(msg)
    else:
        preset = WORKER_PROFILE_PRESETS.get(selector)
        if preset is None:
            msg = f"Unknown WORKER_PROFILE: {selector}"
            raise ValueError(msg)

    allocations: list[DomainAllocation] = []
    for allocation in preset.allocations:
        if allocation.domain_id is grant_control.DomainId.FEED:
            allocations.append(
                dataclasses.replace(
                    allocation,
                    owned_cap=feed_owned_cap,
                    claims_per_cycle=feed_claims_per_cycle,
                )
            )
        elif allocation.domain_id is grant_control.DomainId.SID:
            allocations.append(
                dataclasses.replace(
                    allocation,
                    owned_cap=sid_owned_cap,
                    claims_per_cycle=sid_claims_per_cycle,
                )
            )
        else:
            allocations.append(allocation)

    profile = dataclasses.replace(
        preset,
        allocations=tuple(allocations),
    )
    return validate_worker_profile(profile)


for _profile in WORKER_PROFILE_PRESETS.values():
    validate_worker_profile(_profile)
