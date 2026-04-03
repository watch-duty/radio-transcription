from __future__ import annotations

import importlib
from typing import TYPE_CHECKING, NamedTuple

from backend.pipeline.storage.feed_store import SourceType

if TYPE_CHECKING:
    import asyncio
    import datetime
    from collections.abc import AsyncIterator

    from backend.pipeline.storage.feed_store import LeasedFeed

BCFY_FEEDS_URL_BASE = "https://partner.broadcastify.com/"


class CollectorEntry(NamedTuple):
    """Registry entry describing how to locate and invoke a collector.

    Attributes:
        module_path: Fully-qualified Python module path of the collector.
        func_name: Name of the capture function within that module.
        url_base: Base URL passed to the capture function.
    """

    module_path: str
    func_name: str
    url_base: str


# Maps source_type -> CollectorEntry.
# To add a new collector, add a single entry here.
_COLLECTOR_REGISTRY: dict[SourceType, CollectorEntry] = {
    SourceType.BCFY_FEEDS: CollectorEntry(
        module_path="backend.pipeline.ingestion.collectors.icecast_collector",
        func_name="capture_icecast_stream",
        url_base=BCFY_FEEDS_URL_BASE,
    ),
}


def supported_source_types() -> list[str]:
    """Return source-type slugs that have registered collectors."""
    return [st.value for st in _COLLECTOR_REGISTRY]


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

    module = importlib.import_module(entry.module_path)
    capture_fn = getattr(module, entry.func_name)
    return capture_fn(feed, shutdown_event, url_base=entry.url_base)
