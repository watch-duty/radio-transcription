"""Immutable worker-profile and authority-domain configuration values."""

from __future__ import annotations

import dataclasses
import enum
import hashlib
import json
import types
import typing

from backend.pipeline.ingestion import grant_control


class AuthorityKind(enum.StrEnum):
    """Closed durable authority kinds exposed to runtime observability."""

    FEED = "feed"
    SID_LEASE = "sid_lease"


class BcfyCallsAuthorityMode(enum.StrEnum):
    """Startup-only authority selection for Broadcastify Calls."""

    LEGACY_FEED = "legacy_feed"
    SID_LEASE = "sid_lease"


class ResourceClass(enum.StrEnum):
    """Stable deployment resource classes, never storage authority."""

    SHARED = "shared"
    CONTINUOUS = "continuous"
    DISCRETE = "discrete"


@dataclasses.dataclass(frozen=True, slots=True)
class DomainCatalogEntry:
    """Static data-only metadata for one durable authority domain.

    Attributes:
        domain_id: Canonical runtime domain identity.
        authority_kind: Stable low-cardinality authority classification.
        logical_authority: Unique data-only storage authority identifier.
        compatible_resource_classes: Deployment classes allowed to select it.
        required_config_group: Static selected-domain configuration group.
    """

    domain_id: grant_control.DomainId
    authority_kind: AuthorityKind
    logical_authority: str
    compatible_resource_classes: frozenset[ResourceClass]
    required_config_group: str


@dataclasses.dataclass(frozen=True, slots=True)
class DomainAllocation:
    """One immutable domain admission allocation."""

    domain_id: grant_control.DomainId
    owned_cap: int
    claims_per_cycle: int
    claims_enabled: bool


@dataclasses.dataclass(frozen=True, slots=True)
class WorkerProfile:
    """One immutable process topology and capacity selection."""

    name: str
    version: int
    resource_class: ResourceClass
    process_owned_cap: int
    allocations: tuple[DomainAllocation, ...]


DOMAIN_CATALOG: typing.Mapping[
    grant_control.DomainId,
    DomainCatalogEntry,
] = types.MappingProxyType(
    {
        grant_control.DomainId.FEED: DomainCatalogEntry(
            domain_id=grant_control.DomainId.FEED,
            authority_kind=AuthorityKind.FEED,
            logical_authority="feed",
            compatible_resource_classes=frozenset(
                (ResourceClass.SHARED, ResourceClass.CONTINUOUS),
            ),
            required_config_group="feed",
        ),
        grant_control.DomainId.SID: DomainCatalogEntry(
            domain_id=grant_control.DomainId.SID,
            authority_kind=AuthorityKind.SID_LEASE,
            logical_authority="ingestion_lease",
            compatible_resource_classes=frozenset(
                (ResourceClass.SHARED, ResourceClass.DISCRETE),
            ),
            required_config_group="sid",
        ),
    }
)


LEGACY_PROFILE = WorkerProfile(
    name="legacy",
    version=1,
    resource_class=ResourceClass.SHARED,
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
    version=1,
    resource_class=ResourceClass.SHARED,
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
    version=1,
    resource_class=ResourceClass.DISCRETE,
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


def _validated_catalog_entry(
    domain_id: grant_control.DomainId,
    catalog: typing.Mapping[
        grant_control.DomainId,
        DomainCatalogEntry,
    ],
) -> DomainCatalogEntry:
    entry = catalog.get(domain_id)
    if not isinstance(entry, DomainCatalogEntry):
        msg = f"Unknown worker profile domain: {domain_id.value}"
        raise TypeError(msg)
    if entry.domain_id is not domain_id:
        msg = f"Catalog entry key does not match domain: {domain_id.value}"
        raise ValueError(msg)
    if not isinstance(entry.authority_kind, AuthorityKind):
        msg = f"Domain {domain_id.value} has an invalid authority kind"
        raise TypeError(msg)
    if not isinstance(entry.logical_authority, str):
        msg = f"Domain {domain_id.value} logical authority must be a string"
        raise TypeError(msg)
    if not entry.logical_authority.strip():
        msg = f"Domain {domain_id.value} has no logical authority"
        raise ValueError(msg)
    if (
        not isinstance(entry.compatible_resource_classes, frozenset)
        or not entry.compatible_resource_classes
        or any(
            not isinstance(resource_class, ResourceClass)
            for resource_class in entry.compatible_resource_classes
        )
    ):
        msg = f"Domain {domain_id.value} has invalid resource classes"
        raise ValueError(msg)
    if not isinstance(entry.required_config_group, str):
        msg = f"Domain {domain_id.value} config group must be a string"
        raise TypeError(msg)
    if not entry.required_config_group.strip():
        msg = f"Domain {domain_id.value} has no required config group"
        raise ValueError(msg)
    return entry


def _validate_profile_shape(profile: WorkerProfile) -> None:
    if not isinstance(profile.name, str) or not profile.name.strip():
        msg = "Worker profile name must not be empty"
        raise ValueError(msg)
    _positive_int(profile.version, "Worker profile version")
    if not isinstance(profile.resource_class, ResourceClass):
        msg = "Worker profile resource_class is invalid"
        raise TypeError(msg)
    _positive_int(profile.process_owned_cap, "Worker profile process_owned_cap")
    if not isinstance(profile.allocations, tuple):
        msg = "Worker profile allocations must be an immutable tuple"
        raise TypeError(msg)
    if not profile.allocations:
        msg = "Worker profile must select at least one domain"
        raise ValueError(msg)


def _validate_allocation(
    profile: WorkerProfile,
    allocation: object,
    catalog: typing.Mapping[
        grant_control.DomainId,
        DomainCatalogEntry,
    ],
) -> tuple[DomainAllocation, DomainCatalogEntry, int]:
    if not isinstance(allocation, DomainAllocation):
        msg = "Worker profile allocation is invalid"
        raise TypeError(msg)
    if not isinstance(allocation.domain_id, grant_control.DomainId):
        msg = "Worker profile contains an empty or unknown domain"
        raise TypeError(msg)

    entry = _validated_catalog_entry(allocation.domain_id, catalog)
    if profile.resource_class not in entry.compatible_resource_classes:
        msg = (
            f"Domain {allocation.domain_id.value} is incompatible with "
            f"resource class {profile.resource_class.value}"
        )
        raise ValueError(msg)
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
    return allocation, entry, owned_cap


def validate_worker_profile(
    profile: object,
    catalog: typing.Mapping[
        grant_control.DomainId,
        DomainCatalogEntry,
    ] = DOMAIN_CATALOG,
) -> WorkerProfile:
    """Validate and return one closed immutable worker profile.

    Args:
        profile: Candidate profile value.
        catalog: Closed static domain metadata used for validation.

    Returns:
        The validated immutable profile.

    Raises:
        TypeError: If ``profile`` is not a ``WorkerProfile``.
        ValueError: If profile structure, allocation, or catalog metadata is
            invalid.
    """
    if not isinstance(profile, WorkerProfile):
        msg = "profile must be a WorkerProfile"
        raise TypeError(msg)
    _validate_profile_shape(profile)

    seen_domains: set[grant_control.DomainId] = set()
    seen_authorities: set[str] = set()
    enabled_owned_cap = 0
    for candidate in profile.allocations:
        allocation, entry, owned_cap = _validate_allocation(
            profile,
            candidate,
            catalog,
        )
        if allocation.domain_id in seen_domains:
            msg = (
                f"Duplicate worker profile domain: {allocation.domain_id.value}"
            )
            raise ValueError(msg)
        seen_domains.add(allocation.domain_id)
        if entry.logical_authority in seen_authorities:
            msg = (
                "Duplicate worker profile logical authority: "
                f"{entry.logical_authority}"
            )
            raise ValueError(msg)
        seen_authorities.add(entry.logical_authority)
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
    """Return the selected allocation for ``domain_id``, if any."""
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
    """Resolve a closed preset with explicit immutable domain capacities."""
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


def profile_digest(profile: WorkerProfile) -> str:
    """Return a stable non-authoritative SHA-256 profile identity."""
    validated = validate_worker_profile(profile)
    ordered_allocations = sorted(
        validated.allocations,
        key=lambda allocation: allocation.domain_id.value,
    )
    canonical_document = {
        "name": validated.name,
        "version": validated.version,
        "resource_class": validated.resource_class.value,
        "process_owned_cap": validated.process_owned_cap,
        "allocations": [
            {
                "domain_id": allocation.domain_id.value,
                "owned_cap": allocation.owned_cap,
                "claims_per_cycle": allocation.claims_per_cycle,
                "claims_enabled": allocation.claims_enabled,
            }
            for allocation in ordered_allocations
        ],
    }
    encoded = json.dumps(
        canonical_document,
        ensure_ascii=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


for _profile in WORKER_PROFILE_PRESETS.values():
    validate_worker_profile(_profile)
