"""Runtime metadata for supported ingestion source types."""

from __future__ import annotations

import dataclasses
import enum
import os
from types import MappingProxyType

from backend.pipeline.ingestion import constants
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
        feed_claimable: Whether Feed grants own this source type.
        default_feed_cap: Default per-worker Feed lease cap.
        url_base_env: Optional env var that overrides the collector URL base.
        url_base_default: Default collector URL base when no env override
            exists.
    """

    source_type: feed_store.SourceType
    topic_kind: TopicKind
    feed_claimable: bool
    default_feed_cap: int | None = None
    url_base_env: str | None = None
    url_base_default: str = ""


SOURCE_RUNTIME_SPECS = MappingProxyType(
    {
        feed_store.SourceType.BCFY_FEEDS: SourceRuntimeSpec(
            source_type=feed_store.SourceType.BCFY_FEEDS,
            topic_kind=TopicKind.CONTINUOUS,
            feed_claimable=True,
            default_feed_cap=240,
            url_base_env="BCFY_FEEDS_URL_BASE",
            url_base_default=constants.BCFY_FEEDS_PARTNER_URL_BASE,
        ),
        feed_store.SourceType.GENERIC_ICECAST: SourceRuntimeSpec(
            source_type=feed_store.SourceType.GENERIC_ICECAST,
            topic_kind=TopicKind.CONTINUOUS,
            feed_claimable=True,
            # Same collector and same continuous-stream resource profile as
            # bcfy_feeds, so it inherits that cap rather than introducing an
            # unmeasured one.
            default_feed_cap=240,
            # No URL base: source_feed_id already holds the full stream URL,
            # so there is nothing to prepend.
        ),
        feed_store.SourceType.BCFY_CALLS: SourceRuntimeSpec(
            source_type=feed_store.SourceType.BCFY_CALLS,
            topic_kind=TopicKind.SEGMENTED,
            feed_claimable=False,
            url_base_env="BCFY_CALLS_URL_BASE",
            url_base_default=constants.BCFY_CALLS_URL_BASE,
        ),
        feed_store.SourceType.ECHO: SourceRuntimeSpec(
            source_type=feed_store.SourceType.ECHO,
            topic_kind=TopicKind.SEGMENTED,
            feed_claimable=False,
        ),
        feed_store.SourceType.OPENMHZ: SourceRuntimeSpec(
            source_type=feed_store.SourceType.OPENMHZ,
            topic_kind=TopicKind.SEGMENTED,
            feed_claimable=True,
            default_feed_cap=900,
            url_base_default=constants.OPENMHZ_URL_BASE,
        ),
        feed_store.SourceType.FIRE_NOTIFICATIONS: SourceRuntimeSpec(
            source_type=feed_store.SourceType.FIRE_NOTIFICATIONS,
            topic_kind=TopicKind.SEGMENTED,
            feed_claimable=True,
            # Fire has no controlled mono-source benchmark yet. This is the
            # existing bcfy_calls cap used as a provisional proxy; under the
            # default 800-task ceiling it also prevents Fire from consuming
            # the final 200 worker slots. See "Worker Cap Calibration" in
            # collectors/README.md for the evidence, limitations, and update
            # procedure.
            default_feed_cap=600,
            url_base_env="FIRE_NOTIFICATIONS_URL_BASE",
        ),
    }
)


def source_spec(
    source_type: feed_store.SourceType,
) -> SourceRuntimeSpec:
    """Return runtime metadata for ``source_type``."""
    return SOURCE_RUNTIME_SPECS[source_type]


def feed_claimable_source_specs() -> dict[
    feed_store.SourceType,
    SourceRuntimeSpec,
]:
    """Return Feed-authority source specs keyed by source type."""
    return {
        source_type: spec
        for source_type, spec in SOURCE_RUNTIME_SPECS.items()
        if spec.feed_claimable
    }


def default_feed_claim_caps() -> dict[feed_store.SourceType, int]:
    """Return default Feed lease caps keyed by source type."""
    caps: dict[feed_store.SourceType, int] = {}
    for source_type, spec in feed_claimable_source_specs().items():
        if spec.default_feed_cap is None:
            msg = f"Feed-claimable source type {source_type.value} has no cap"
            raise ValueError(msg)
        caps[source_type] = spec.default_feed_cap
    return caps


def url_base_for(source_type: feed_store.SourceType) -> str:
    """Return the configured collector URL base for ``source_type``."""
    spec = source_spec(source_type)
    if spec.url_base_env is None:
        return spec.url_base_default
    return os.environ.get(spec.url_base_env, spec.url_base_default)
