from __future__ import annotations

from backend.pipeline.common.log_helper import setup_logging
from backend.pipeline.common.tracing_utils import setup_tracing
from backend.pipeline.ingestion import source_runtime_specs
from backend.pipeline.ingestion.collector_runtime import CollectorRuntime
from backend.pipeline.ingestion.router import (
    resolve_topic_path,
    route_capturer,
    supported_source_types,
)
from backend.pipeline.ingestion.settings import CollectorSettings
from backend.pipeline.storage.feed_store import SourceType


def main() -> None:
    """
    Entry point for capture.

    Initializes CollectorRuntime with the correct capture function and
    blocks until graceful shutdown completes.
    """
    setup_logging()
    setup_tracing(service_name="ingestion-service", is_ingestion=True)
    settings = CollectorSettings()

    # Verify topic paths for all supported source types at startup
    for st in supported_source_types():
        try:
            resolve_topic_path(SourceType(st), settings)
        except ValueError as e:
            msg = f"Startup check failed for source type {st}: {e}"
            raise RuntimeError(msg) from e

    # Configured Feed caps must cover exactly the Feed-authority sources.
    feed_claimable_types = set(
        source_runtime_specs.feed_claimable_source_specs()
    )
    cap_types = set(settings.feed_claim_caps)
    if feed_claimable_types != cap_types:
        msg = (
            "Startup invariant violated: Feed-claimable source registry "
            f"{sorted(t.value for t in feed_claimable_types)} differs from "
            "Feed-claim caps registry "
            f"{sorted(t.value for t in cap_types)}."
        )
        raise RuntimeError(msg)

    # Registered collectors must cover every Feed-authority source plus the
    # legacy Calls route retained pending separate collector-code removal.
    collector_types = {SourceType(st) for st in supported_source_types()}
    expected_collector_types = feed_claimable_types | {SourceType.BCFY_CALLS}
    if collector_types != expected_collector_types:
        msg = (
            "Startup invariant violated: collector registry "
            f"{sorted(t.value for t in collector_types)} differs from "
            "current authority sources "
            f"{sorted(t.value for t in expected_collector_types)}."
        )
        raise RuntimeError(msg)

    runtime = CollectorRuntime(route_capturer, settings)
    runtime.run()


if __name__ == "__main__":
    main()
