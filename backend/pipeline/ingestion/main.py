from __future__ import annotations

from backend.pipeline.common.logging import setup_logging
from backend.pipeline.ingestion.normalizer_runtime import NormalizerRuntime
from backend.pipeline.ingestion.router import route_capturer
from backend.pipeline.ingestion.settings import NormalizerSettings


def main() -> None:
    """
    Entry point for capture.

    Initializes NormalizerRuntime with the correct capture function and
    blocks until graceful shutdown completes.
    """
    setup_logging()
    settings = NormalizerSettings()
    runtime = NormalizerRuntime(route_capturer, settings)
    runtime.run()


if __name__ == "__main__":
    main()
