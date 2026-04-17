from __future__ import annotations

from backend.pipeline.common.logging import setup_logging
from backend.pipeline.ingestion.normalizer_runtime import NormalizerRuntime
from backend.pipeline.ingestion.router import (
    resolve_topic_path,
    route_capturer,
    supported_source_types,
)
from backend.pipeline.ingestion.settings import NormalizerSettings
from backend.pipeline.storage.feed_store import SourceType


def main() -> None:
    """
    Entry point for capture.

    Initializes NormalizerRuntime with the correct capture function and
    blocks until graceful shutdown completes.
    """
    setup_logging()
    settings = NormalizerSettings(source_types=supported_source_types())

    # Verify topic paths for all supported source types at startup
    for st in supported_source_types():
        try:
            resolve_topic_path(SourceType(st), settings)
        except ValueError as e:
            msg = f"Startup check failed for source type {st}: {e}"
            raise RuntimeError(msg) from e

    runtime = NormalizerRuntime(route_capturer, settings)
    runtime.run()


if __name__ == "__main__":
    main()
