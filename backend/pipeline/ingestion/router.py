from __future__ import annotations

from typing import TYPE_CHECKING

from backend.pipeline.ingestion import source_runtime_specs
from backend.pipeline.ingestion.collectors.bcfy_calls import (
    bcfy_calls_collector,
)
from backend.pipeline.ingestion.collectors.fire_notifications import (
    collector as fn_collector_module,
)
from backend.pipeline.ingestion.collectors.icecast import icecast_collector
from backend.pipeline.ingestion.collectors.openmhz import (
    collector as openmhz_collector_module,
)
from backend.pipeline.storage.feed_store import SourceType

if TYPE_CHECKING:
    import asyncio
    from collections.abc import AsyncIterator

    from backend.pipeline.ingestion.models import (
        CaptureEvent,
        CaptureResources,
        CollectorFn,
    )
    from backend.pipeline.ingestion.settings import CollectorSettings
    from backend.pipeline.storage.feed_store import LeasedFeed

# Typed registry: ty/mypy checks each value matches CollectorFn.
# Adding a new VM collector is deliberately not just this dict: update
# SourceType, source_type seed data, SourceRuntimeSpec, and tests. main.py
# enforces _COLLECTORS == claimable SourceRuntimeSpec types at startup so a new
# type does not silently remain unclaimed or get claimed without a route.
_COLLECTORS: dict[SourceType, CollectorFn] = {
    SourceType.BCFY_FEEDS: icecast_collector.capture_icecast_stream,
    SourceType.BCFY_CALLS: bcfy_calls_collector.capture_bcfy_calls,
    SourceType.OPENMHZ: openmhz_collector_module.openmhz_collector,
    SourceType.FIRE_NOTIFICATIONS: (
        fn_collector_module.fire_notifications_collector
    ),
}


def supported_source_types() -> list[str]:
    """Return source-type slugs that have registered collectors."""
    return [st.value for st in _COLLECTORS]


def resolve_topic_path(
    source_type: SourceType, settings: CollectorSettings
) -> str:
    """Determines the Pub/Sub topic path based on the source type."""
    spec = source_runtime_specs.source_spec(source_type)
    if spec.topic_kind is source_runtime_specs.TopicKind.CONTINUOUS:
        return settings.continuous_pubsub_topic_path

    topic_path = settings.segmented_pubsub_topic_path
    if not topic_path:
        msg = f"Segmented Pub/Sub topic path not configured for source type {source_type}"
        raise ValueError(msg)
    return topic_path


def route_capturer(
    feed: LeasedFeed,
    shutdown_event: asyncio.Event,
    resources: CaptureResources,
) -> AsyncIterator[CaptureEvent]:
    """Routes the feed to the appropriate capture function."""
    source_type = feed["source_type"]
    entry = _COLLECTORS.get(source_type)
    if entry is None:
        msg = f"Unsupported source_type: {source_type}"
        raise ValueError(msg)

    capture_fn = entry
    url_base = source_runtime_specs.url_base_for(source_type)
    return capture_fn(feed, shutdown_event, url_base, resources)
