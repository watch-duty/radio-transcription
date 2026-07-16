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
        process_owned_cap: Maximum enabled grants across the process.
        allocations: Immutable per-domain admission allocations.
    """

    name: str
    process_owned_cap: int
    allocations: tuple[DomainAllocation, ...]


LEGACY_PROFILE = WorkerProfile(
    name="legacy",
    process_owned_cap=800,
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
    process_owned_cap=832,
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
    process_owned_cap=32,
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


def _positive_int(value: object, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        msg = f"{field_name} must be a positive integer"
        raise ValueError(msg)
    return value


def _validate_profile_shape(profile: WorkerProfile) -> None:
    if not isinstance(profile.name, str) or not profile.name.strip():
        msg = "Worker profile name must not be empty"
        raise ValueError(msg)
    _positive_int(profile.process_owned_cap, "Worker profile process_owned_cap")
    if not isinstance(profile.allocations, tuple):
        msg = "Worker profile allocations must be an immutable tuple"
        raise TypeError(msg)
    if not profile.allocations:
        msg = "Worker profile must select at least one domain"
        raise ValueError(msg)


def _validate_allocation(
    allocation: object,
) -> tuple[DomainAllocation, int]:
    if not isinstance(allocation, DomainAllocation):
        msg = "Worker profile allocation is invalid"
        raise TypeError(msg)
    if not isinstance(allocation.domain_id, grant_control.DomainId):
        msg = "Worker profile contains an empty or unknown domain"
        raise TypeError(msg)

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
    if not isinstance(allocation.claims_enabled, bool):
        msg = (
            f"Domain {allocation.domain_id.value} claims_enabled must be a bool"
        )
        raise TypeError(msg)
    return allocation, owned_cap


def validate_worker_profile(profile: object) -> WorkerProfile:
    """Validate and return one closed immutable worker profile.

    Args:
        profile: Candidate profile value.

    Returns:
        The validated immutable profile.

    Raises:
        TypeError: If ``profile`` is not a ``WorkerProfile``.
        ValueError: If profile structure or allocation is invalid.
    """
    if not isinstance(profile, WorkerProfile):
        msg = "profile must be a WorkerProfile"
        raise TypeError(msg)
    _validate_profile_shape(profile)

    seen_domains: set[grant_control.DomainId] = set()
    enabled_owned_cap = 0
    for candidate in profile.allocations:
        allocation, owned_cap = _validate_allocation(candidate)
        if allocation.domain_id in seen_domains:
            msg = (
                f"Duplicate worker profile domain: {allocation.domain_id.value}"
            )
            raise ValueError(msg)
        seen_domains.add(allocation.domain_id)
        if allocation.claims_enabled:
            enabled_owned_cap += owned_cap

    if enabled_owned_cap > profile.process_owned_cap:
        msg = (
            "Enabled domain owned caps exceed the worker profile process "
            "envelope"
        )
        raise ValueError(msg)
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
        TypeError: If ``mode`` is not a ``BcfyCallsAuthorityMode``.
        ValueError: If the topology cannot host the selected authority or the
            derived enabled capacity exceeds the process envelope.
    """
    validated = validate_worker_profile(profile)
    if not isinstance(mode, BcfyCallsAuthorityMode):
        msg = "mode must be a BcfyCallsAuthorityMode"
        raise TypeError(msg)

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

    allocations = tuple(
        dataclasses.replace(
            allocation,
            claims_enabled=(
                allocation.domain_id is grant_control.DomainId.FEED
                or mode is BcfyCallsAuthorityMode.SID_LEASE
            ),
        )
        for allocation in validated.allocations
    )
    return validate_worker_profile(
        dataclasses.replace(validated, allocations=allocations)
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
    elif not isinstance(selector, str) or not selector.strip():
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
        else:
            allocations.append(
                dataclasses.replace(
                    allocation,
                    owned_cap=sid_owned_cap,
                    claims_per_cycle=sid_claims_per_cycle,
                )
            )

    process_owned_cap = sum(allocation.owned_cap for allocation in allocations)
    profile = dataclasses.replace(
        preset,
        process_owned_cap=process_owned_cap,
        allocations=tuple(allocations),
    )
    return validate_worker_profile(profile)


for _profile in WORKER_PROFILE_PRESETS.values():
    validate_worker_profile(_profile)
