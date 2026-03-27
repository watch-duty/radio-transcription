from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import asyncio
    import datetime
    from collections.abc import AsyncIterator

    from backend.pipeline.storage.feed_store import LeasedFeed


def route_capturer(
    feed: LeasedFeed, shutdown_event: asyncio.Event
) -> AsyncIterator[tuple[bytes, datetime.datetime]]:
    """
    Routes the feed to the appropriate capture function based on its source_type.
    """
    match feed["source_type"]:
        case "bcfy_feeds":
            from backend.pipeline.ingestion.collectors.icecast_collector import (  # noqa: PLC0415
                capture_icecast_stream,
            )
            return capture_icecast_stream(feed, shutdown_event)
        case _:
            msg = f"Unsupported source_type: {feed['source_type']}"
            raise ValueError(msg)
