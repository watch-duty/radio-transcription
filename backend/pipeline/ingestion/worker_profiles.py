"""Immutable worker-profile and authority-domain configuration values."""

from __future__ import annotations

import dataclasses

from backend.pipeline.ingestion import grant_control


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

MIXED_PROFILE = WorkerProfile(
    name="mixed",
    allocations=(
        LEGACY_PROFILE.allocations[0],
        DomainAllocation(
            domain_id=grant_control.DomainId.SID,
            owned_cap=32,
            claims_per_cycle=2,
            claims_enabled=True,
        ),
    ),
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


def build_mixed_worker_profile(
    *,
    feed_owned_cap: int = 800,
    feed_claims_per_cycle: int = 20,
    sid_owned_cap: int = 32,
    sid_claims_per_cycle: int = 2,
) -> WorkerProfile:
    """Build the fixed Feed-and-SID profile with explicit capacities.

    Args:
        feed_owned_cap: Feed-domain ownership ceiling.
        feed_claims_per_cycle: Feed-domain admission-cycle budget.
        sid_owned_cap: SID-domain ownership ceiling.
        sid_claims_per_cycle: SID-domain admission-cycle budget.

    Returns:
        A validated immutable mixed-domain profile.

    Raises:
        ValueError: A selected capacity is invalid.
    """
    feed, sid = MIXED_PROFILE.allocations
    return validate_worker_profile(
        dataclasses.replace(
            MIXED_PROFILE,
            allocations=(
                dataclasses.replace(
                    feed,
                    owned_cap=feed_owned_cap,
                    claims_per_cycle=feed_claims_per_cycle,
                ),
                dataclasses.replace(
                    sid,
                    owned_cap=sid_owned_cap,
                    claims_per_cycle=sid_claims_per_cycle,
                ),
            ),
        )
    )


for _profile in (LEGACY_PROFILE, MIXED_PROFILE):
    validate_worker_profile(_profile)
