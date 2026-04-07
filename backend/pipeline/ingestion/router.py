from __future__ import annotations

from typing import TYPE_CHECKING

from backend.pipeline.ingestion.collectors.icecast_collector import (
    capture_icecast_stream,
)
from backend.pipeline.ingestion.collectors.openmhz.collector import (
    openmhz_collector,
)
from backend.pipeline.ingestion.models import CollectorFn
from backend.pipeline.storage.feed_store import SourceType

if TYPE_CHECKING:
    import asyncio
    from collections.abc import AsyncIterator

    from backend.pipeline.ingestion.models import CapturedChunk
    from backend.pipeline.storage.feed_store import LeasedFeed

BCFY_FEEDS_URL_BASE = "https://partner.broadcastify.com/"
OPENMHZ_URL_BASE = "https://api.openmhz.com/"

# Typed registry: ty/mypy checks each value matches CollectorFn.
# Adding a new collector = 1 import + 1 dict entry.
_COLLECTORS: dict[SourceType, tuple[CollectorFn, str]] = {
    SourceType.BCFY_FEEDS: (capture_icecast_stream, BCFY_FEEDS_URL_BASE),
    SourceType.OPENMHZ: (openmhz_collector, OPENMHZ_URL_BASE),
}


def supported_source_types() -> list[str]:
    """Return source-type slugs that have registered collectors."""
    return [st.value for st in _COLLECTORS]


def route_capturer(
    feed: LeasedFeed, shutdown_event: asyncio.Event
) -> AsyncIterator[CapturedChunk]:
    """Routes the feed to the appropriate capture function."""
    source_type = feed["source_type"]
    entry = _COLLECTORS.get(source_type)
    if entry is None:
        msg = f"Unsupported source_type: {source_type}"
        raise ValueError(msg)

    capture_fn, url_base = entry
    return capture_fn(feed, shutdown_event, url_base)
