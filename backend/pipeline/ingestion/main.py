from __future__ import annotations

from backend.pipeline.common.log_helper import setup_logging
from backend.pipeline.common.tracing_utils import setup_tracing
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

    # Cross-registry invariant: the set of types that have a registered
    # collector (router._COLLECTORS) must equal the set of types with
    # per-worker caps (settings._DEFAULT_CAPS). Drift in either direction
    # is silent in production:
    #   - Type in caps but not collectors → worker claims feeds it
    #     can't process; route_capturer raises at first chunk.
    #   - Type in collectors but not caps → worker has the code path but
    #     never claims that type (no CTE branch generated, no recovery
    #     filter match) — feeds back up indefinitely.
    collector_types = {SourceType(st) for st in supported_source_types()}
    cap_types = set(settings.caps.keys())
    if collector_types != cap_types:
        msg = (
            "Startup invariant violated: collector registry "
            f"{sorted(t.value for t in collector_types)} differs from "
            f"caps registry {sorted(t.value for t in cap_types)}. "
            "Both _COLLECTORS (router.py) and _DEFAULT_CAPS (settings.py) "
            "must be updated together when adding or removing a SourceType."
        )
        raise RuntimeError(msg)

    runtime = CollectorRuntime(route_capturer, settings)
    runtime.run()


if __name__ == "__main__":
    main()
