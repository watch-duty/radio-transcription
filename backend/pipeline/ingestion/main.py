from __future__ import annotations

from typing import TYPE_CHECKING

from collectors.icecast_collector import capture_icecast_stream

from backend.pipeline.common.logging import setup_logging
from backend.pipeline.ingestion.normalizer_runtime import NormalizerRuntime
from backend.pipeline.ingestion.settings import NormalizerSettings

if TYPE_CHECKING:
    import asyncio
    import datetime
    from collections.abc import AsyncIterator

    from backend.pipeline.storage.feed_store import LeasedFeed


def _route_capturer(feed: LeasedFeed, shutdown_event: asyncio.Event) -> AsyncIterator[tuple[bytes, datetime.datetime]]:
    match feed["source_type"]:
        case "bcfy_feeds":
            return capture_icecast_stream(feed, shutdown_event)
        case _:
            msg = f"Unsupported source_type: {feed['source_type']}"
            raise ValueError(msg)


def main() -> None:
    """
    Entry point for capture.

    Initializes NormalizerRuntime with the correct capture function and
    blocks until graceful shutdown completes.
    """
    setup_logging()
    settings = NormalizerSettings()
    runtime = NormalizerRuntime(_route_capturer, settings)
    runtime.run()


if __name__ == "__main__":
    main()
