from __future__ import annotations

import importlib
from typing import TYPE_CHECKING

from backend.pipeline.ingestion.models import CollectorEntry
from backend.pipeline.storage.feed_store import SourceType

if TYPE_CHECKING:
    import asyncio
    from collections.abc import AsyncIterator

    from backend.pipeline.ingestion.models import CapturedChunk
    from backend.pipeline.storage.feed_store import LeasedFeed

BCFY_FEEDS_URL_BASE = "https://partner.broadcastify.com/"
OPENMHZ_URL_BASE = "https://api.openmhz.com/"


# Maps source_type -> CollectorEntry.
# To add a new collector, add a single entry here.
_COLLECTOR_REGISTRY: dict[SourceType, CollectorEntry] = {
    SourceType.BCFY_FEEDS: CollectorEntry(
        module_path="backend.pipeline.ingestion.collectors.icecast_collector",
        func_name="capture_icecast_stream",
        url_base=BCFY_FEEDS_URL_BASE,
    ),
    SourceType.OPENMHZ: CollectorEntry(
        module_path="backend.pipeline.ingestion.collectors.openmhz.collector",
        func_name="openmhz_collector",
        url_base=OPENMHZ_URL_BASE,
    ),
}


def supported_source_types() -> list[str]:
    """Return source-type slugs that have registered collectors."""
    return [st.value for st in _COLLECTOR_REGISTRY]


def route_capturer(
    feed: LeasedFeed, shutdown_event: asyncio.Event
) -> AsyncIterator[CapturedChunk]:
    """Routes the feed to the appropriate capture function.

    Looks up the collector in ``_COLLECTOR_REGISTRY`` by source_type,
    lazily imports it, and calls it with the feed and shutdown event.
    """
    source_type = feed["source_type"]
    entry = _COLLECTOR_REGISTRY.get(source_type)
    if entry is None:
        msg = f"Unsupported source_type: {source_type}"
        raise ValueError(msg)

    module = importlib.import_module(entry.module_path)
    capture_fn = getattr(module, entry.func_name)
    return capture_fn(feed, shutdown_event, url_base=entry.url_base)
