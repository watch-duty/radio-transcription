from __future__ import annotations

import importlib
from typing import TYPE_CHECKING

from backend.pipeline.storage.feed_store import SourceType

if TYPE_CHECKING:
    import asyncio
    import datetime
    from collections.abc import AsyncIterator

    from backend.pipeline.storage.feed_store import LeasedFeed

# Maps source_type -> (module_path, function_name, url_base).
# To add a new collector, add a single entry here.
_COLLECTOR_REGISTRY: dict[SourceType, tuple[str, str, str]] = {
    SourceType.BCFY_FEEDS: (
        "backend.pipeline.ingestion.collectors.icecast_collector",
        "capture_icecast_stream",
        "https://partner.broadcastify.com/",
    ),
}


def route_capturer(
    feed: LeasedFeed, shutdown_event: asyncio.Event
) -> AsyncIterator[tuple[bytes, datetime.datetime]]:
    """Routes the feed to the appropriate capture function.

    Looks up the collector in ``_COLLECTOR_REGISTRY`` by source_type,
    lazily imports it, and calls it with the feed and shutdown event.
    """
    source_type = feed["source_type"]
    entry = _COLLECTOR_REGISTRY.get(source_type)
    if entry is None:
        msg = f"Unsupported source_type: {source_type}"
        raise ValueError(msg)

    module_path, func_name, url_base = entry
    module = importlib.import_module(module_path)
    capture_fn = getattr(module, func_name)
    return capture_fn(feed, shutdown_event, url_base=url_base)
