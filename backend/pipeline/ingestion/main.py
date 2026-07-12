from __future__ import annotations

from backend.pipeline.common import log_helper, tracing_utils
from backend.pipeline.ingestion import (
    collector_runtime,
    grant_control,
    router,
    settings,
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
) -> None:
    catalog_entry = worker_profiles.DOMAIN_CATALOG.get(
        grant_control.DomainId.SID
    )
    if (
        catalog_entry is None
        or catalog_entry.domain_id is not grant_control.DomainId.SID
        or catalog_entry.authority_kind
        is not worker_profiles.AuthorityKind.SID_LEASE
        or catalog_entry.required_config_group != "sid"
    ):
        msg = "Static SID domain catalog configuration is invalid"
        raise RuntimeError(msg)
    if allocation.claims_enabled:
        msg = "Phase 3 SID claims must remain disabled"
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
            _validate_sid_domain_configuration(allocation)


def main() -> None:
    """
    Entry point for capture.

    Initializes CollectorRuntime with the correct capture function and
    blocks until graceful shutdown completes.
    """
    profile = settings.load_worker_profile_from_env()
    collector_settings = settings.CollectorSettings(worker_profile=profile)
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
