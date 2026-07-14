from __future__ import annotations

from backend.pipeline.common import log_helper, tracing_utils
from backend.pipeline.ingestion import (
    collector_runtime,
    grant_control,
    router,
    settings,
    source_runtime_specs,
    worker_profiles,
)
from backend.pipeline.storage import feed_store


def _validate_feed_domain_configuration(
    collector_settings: settings.CollectorSettings,
) -> None:
    source_types = router.supported_source_types()

    # Verify topic paths for all supported source types at startup.
    for source_type in source_types:
        try:
            topic_path = router.resolve_topic_path(
                feed_store.SourceType(source_type),
                collector_settings,
            )
        except ValueError as exc:
            msg = f"Startup check failed for source type {source_type}: {exc}"
            raise RuntimeError(msg) from exc
        if not topic_path:
            msg = (
                f"Startup check failed for source type {source_type}: "
                "Pub/Sub topic path not configured"
            )
            raise RuntimeError(msg)

    # Cross-registry invariant: VM-claimable SourceRuntimeSpec entries must
    # match registered collectors and caps. Drift in either direction is silent
    # in production: workers either claim feeds they cannot process, or ship
    # code paths that never claim their feeds.
    collector_types = {
        feed_store.SourceType(source_type) for source_type in source_types
    }
    cap_types = set(collector_settings.caps)
    if collector_types != cap_types:
        msg = (
            "Startup invariant violated: collector registry "
            f"{sorted(source_type.value for source_type in collector_types)} "
            "differs from "
            "caps registry "
            f"{sorted(source_type.value for source_type in cap_types)}. "
            "Both _COLLECTORS (router.py) and SourceRuntimeSpec "
            "must be updated together when adding or removing a VM source."
        )
        raise RuntimeError(msg)


def _validate_sid_domain_configuration(
    allocation: worker_profiles.DomainAllocation,
    collector_settings: settings.CollectorSettings,
) -> None:
    catalog_entry = worker_profiles.DOMAIN_CATALOG.get(
        grant_control.DomainId.SID
    )
    if (
        catalog_entry is None
        or allocation.domain_id is not grant_control.DomainId.SID
        or catalog_entry.domain_id is not grant_control.DomainId.SID
        or catalog_entry.authority_kind
        is not worker_profiles.AuthorityKind.SID_LEASE
        or catalog_entry.required_config_group != "sid"
    ):
        msg = "Static SID domain catalog configuration is invalid"
        raise RuntimeError(msg)
    calls_spec = source_runtime_specs.source_spec(
        feed_store.SourceType.BCFY_CALLS
    )
    if (
        not calls_spec.claimable
        or calls_spec.default_cap is None
        or calls_spec.topic_kind is not source_runtime_specs.TopicKind.SEGMENTED
        or calls_spec.source_type is not feed_store.SourceType.BCFY_CALLS
    ):
        msg = "Static SID Calls source configuration is invalid"
        raise RuntimeError(msg)
    if not source_runtime_specs.url_base_for(feed_store.SourceType.BCFY_CALLS):
        msg = "SID Calls URL base is not configured"
        raise RuntimeError(msg)
    try:
        topic_path = router.resolve_topic_path(
            feed_store.SourceType.BCFY_CALLS,
            collector_settings,
        )
    except ValueError as exc:
        msg = f"SID Calls segmented topic configuration is invalid: {exc}"
        raise RuntimeError(msg) from exc
    if not topic_path:
        msg = "SID Calls segmented topic is not configured"
        raise RuntimeError(msg)


def _validate_calls_authority_configuration(
    profile: worker_profiles.WorkerProfile,
    collector_settings: settings.CollectorSettings,
) -> None:
    """Prove one and only one effective Calls claim authority."""
    mode = collector_settings.bcfy_calls_authority_mode
    if not isinstance(mode, worker_profiles.BcfyCallsAuthorityMode):
        msg = "CollectorSettings has an invalid Calls authority mode"
        raise TypeError(msg)
    feed_allocation = worker_profiles.allocation_for_domain(
        profile,
        grant_control.DomainId.FEED,
    )
    sid_allocation = worker_profiles.allocation_for_domain(
        profile,
        grant_control.DomainId.SID,
    )
    expected_claim_caps = (
        dict(collector_settings.caps) if feed_allocation is not None else {}
    )
    if mode is worker_profiles.BcfyCallsAuthorityMode.SID_LEASE:
        expected_claim_caps.pop(feed_store.SourceType.BCFY_CALLS, None)
    if dict(collector_settings.feed_claim_caps) != expected_claim_caps:
        msg = "Effective Feed claim caps do not match Calls authority mode"
        raise RuntimeError(msg)

    calls_feed_enabled = (
        feed_allocation is not None
        and feed_allocation.claims_enabled
        and feed_store.SourceType.BCFY_CALLS
        in collector_settings.feed_claim_caps
    )
    sid_enabled = sid_allocation is not None and sid_allocation.claims_enabled
    if calls_feed_enabled is sid_enabled:
        msg = "Exactly one Calls Feed or SID Lease authority must be enabled"
        raise RuntimeError(msg)
    if mode is worker_profiles.BcfyCallsAuthorityMode.LEGACY_FEED:
        if not calls_feed_enabled or sid_enabled:
            msg = "legacy_feed mode does not match derived Calls authority"
            raise RuntimeError(msg)
        return
    if calls_feed_enabled or not sid_enabled:
        msg = "sid_lease mode does not match derived Calls authority"
        raise RuntimeError(msg)


def _validate_selected_domain_configuration(
    profile: worker_profiles.WorkerProfile,
    collector_settings: settings.CollectorSettings,
) -> None:
    if collector_settings.worker_profile is not profile:
        msg = "CollectorSettings must retain the resolved worker profile"
        raise RuntimeError(msg)

    for allocation in profile.allocations:
        if allocation.domain_id is grant_control.DomainId.FEED:
            _validate_feed_domain_configuration(collector_settings)
        elif allocation.domain_id is grant_control.DomainId.SID:
            _validate_sid_domain_configuration(allocation, collector_settings)
    _validate_calls_authority_configuration(profile, collector_settings)


def main() -> None:
    """
    Entry point for capture.

    Initializes CollectorRuntime with the correct capture function and
    blocks until graceful shutdown completes.
    """
    collector_settings = settings.CollectorSettings()
    profile = collector_settings.worker_profile
    _validate_selected_domain_configuration(profile, collector_settings)
    log_helper.setup_logging()
    tracing_utils.setup_tracing(
        service_name="ingestion-service",
        is_ingestion=True,
    )
    runtime = collector_runtime.CollectorRuntime(
        router.route_capturer,
        collector_settings,
    )
    runtime.run()


if __name__ == "__main__":
    main()
