"""Runtime metadata for supported ingestion source types."""

from __future__ import annotations

import dataclasses
import enum
import os
from types import MappingProxyType

from backend.pipeline.storage import feed_store


class TopicKind(enum.StrEnum):
    """Pub/Sub topic family for captured chunks from a source type."""

    CONTINUOUS = "continuous"
    SEGMENTED = "segmented"


@dataclasses.dataclass(frozen=True)
class SourceRuntimeSpec:
    """Data-only runtime metadata for one source type.

    Attributes:
        source_type: Source type slug used by feed rows and source-type seeds.
        topic_kind: Pub/Sub topic family used after capture.
        claimable: Whether VM ingestion workers should lease this source type.
        default_cap: Default per-worker lease cap for VM-claimable types.
        url_base_env: Optional env var that overrides the collector URL base.
        url_base_default: Default collector URL base when no env override exists.
    """

    source_type: feed_store.SourceType
    topic_kind: TopicKind
    claimable: bool
    default_cap: int | None = None
    url_base_env: str | None = None
    url_base_default: str = ""


SOURCE_RUNTIME_SPECS = MappingProxyType(
    {
        feed_store.SourceType.BCFY_FEEDS: SourceRuntimeSpec(
            source_type=feed_store.SourceType.BCFY_FEEDS,
            topic_kind=TopicKind.CONTINUOUS,
            claimable=True,
            default_cap=240,
            url_base_env="BCFY_FEEDS_URL_BASE",
            url_base_default="https://partner.broadcastify.com/",
        ),
        feed_store.SourceType.BCFY_CALLS: SourceRuntimeSpec(
            source_type=feed_store.SourceType.BCFY_CALLS,
            topic_kind=TopicKind.SEGMENTED,
            claimable=True,
            default_cap=600,
            url_base_env="BCFY_CALLS_URL_BASE",
            url_base_default="https://api.bcfy.io/calls/v1/live/",
        ),
        feed_store.SourceType.ECHO: SourceRuntimeSpec(
            source_type=feed_store.SourceType.ECHO,
            topic_kind=TopicKind.SEGMENTED,
            claimable=False,
        ),
        feed_store.SourceType.OPENMHZ: SourceRuntimeSpec(
            source_type=feed_store.SourceType.OPENMHZ,
            topic_kind=TopicKind.SEGMENTED,
            claimable=True,
            default_cap=900,
            url_base_default="https://api.openmhz.com/",
        ),
        feed_store.SourceType.FIRE_NOTIFICATIONS: SourceRuntimeSpec(
            source_type=feed_store.SourceType.FIRE_NOTIFICATIONS,
            topic_kind=TopicKind.SEGMENTED,
            claimable=True,
            default_cap=300,
            url_base_env="FIRE_NOTIFICATIONS_URL_BASE",
        ),
    }
)


def source_spec(
    source_type: feed_store.SourceType,
) -> SourceRuntimeSpec:
    """Return runtime metadata for ``source_type``."""
    return SOURCE_RUNTIME_SPECS[source_type]


def claimable_source_specs() -> dict[
    feed_store.SourceType,
    SourceRuntimeSpec,
]:
    """Return VM-claimable source specs keyed by source type."""
    return {
        source_type: spec
        for source_type, spec in SOURCE_RUNTIME_SPECS.items()
        if spec.claimable
    }


def default_caps() -> dict[feed_store.SourceType, int]:
    """Return default VM lease caps keyed by source type."""
    caps: dict[feed_store.SourceType, int] = {}
    for source_type, spec in claimable_source_specs().items():
        if spec.default_cap is None:
            msg = f"Claimable source type {source_type.value} has no cap"
            raise ValueError(msg)
        caps[source_type] = spec.default_cap
    return caps


def url_base_for(source_type: feed_store.SourceType) -> str:
    """Return the configured collector URL base for ``source_type``."""
    spec = source_spec(source_type)
    if spec.url_base_env is None:
        return spec.url_base_default
    return os.environ.get(spec.url_base_env, spec.url_base_default)
