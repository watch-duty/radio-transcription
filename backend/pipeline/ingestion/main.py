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

    # Cross-registry invariant: Feed-claimable SourceRuntimeSpec entries must
    # match configured Feed caps. Drift in either direction is silent in
    # production: workers either claim feeds they cannot process or omit an
    # intended Feed-authority source.
    expected_cap_types = set(source_runtime_specs.feed_claimable_source_specs())
    cap_types = set(settings.feed_claim_caps)
    if expected_cap_types != cap_types:
        msg = (
            "Startup invariant violated: Feed-claimable source registry "
            f"{sorted(t.value for t in expected_cap_types)} differs from "
            "Feed-claim caps registry "
            f"{sorted(t.value for t in cap_types)}."
        )
        raise RuntimeError(msg)

    runtime = CollectorRuntime(route_capturer, settings)
    runtime.run()


if __name__ == "__main__":
    main()
