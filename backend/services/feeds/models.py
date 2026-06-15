from __future__ import annotations

import datetime  # noqa: TC003
import enum
import uuid  # noqa: TC003
from typing import Annotated, Literal, Union

from pydantic import BaseModel, ConfigDict, Field, field_validator

from backend.pipeline.storage.feed_store import (
    FeedStatus,
    FeedStatusReason,
    SourceType,
)


class BackendFeedStatusReason(enum.StrEnum):
    """Public feed status reasons exposed through the feed service API."""

    UNKNOWN = "unknown"
    PIPELINE_PUBLISH_AFTER_BOOKMARK_FAILED = (
        "pipeline_publish_after_bookmark_failed"
    )
    SOURCE_OFFLINE = "source_offline"
    SOURCE_UNREACHABLE = "source_unreachable"
    SOURCE_RATE_LIMITED = "source_rate_limited"
    SYSTEM_AUTHENTICATION_FAILED = "system_authentication_failed"
    SYSTEM_CONFIGURATION_INVALID = "system_configuration_invalid"
    SYSTEM_SOURCE_CONFIGURATION_INVALID = "system_source_configuration_invalid"
    SYSTEM_RUNTIME_CONFIGURATION_INVALID = (
        "system_runtime_configuration_invalid"
    )
    SYSTEM_CREDENTIAL_ACCESS_FAILED = "system_credential_access_failed"
    SYSTEM_SOURCE_PAYLOAD_INVALID = "system_source_payload_invalid"
    SYSTEM_COLLECTOR_ERROR = "system_collector_error"
    SYSTEM_PIPELINE_ERROR = "system_pipeline_error"
    SYSTEM_UNEXPECTED_ERROR = "system_unexpected_error"


def _public_status_reason(
    value: FeedStatusReason | BackendFeedStatusReason | str | None,
) -> BackendFeedStatusReason | None:
    """Map internal backend status reasons to the public API vocabulary."""
    if value is None:
        return None
    if isinstance(value, BackendFeedStatusReason):
        return value
    if isinstance(value, FeedStatusReason):
        value = value.value
    try:
        return BackendFeedStatusReason(str(value))
    except ValueError:
        return BackendFeedStatusReason.UNKNOWN


class Tag(BaseModel):
    """Key-value pair for any metadata on the feed.

    Attributes:
        key: The tag key.
        value: The tag value.
    """

    key: str
    value: str


class FeedBase(BaseModel):
    name: str
    source_type: SourceType
    tags: list[Tag] | None = None


class BcfyFeedsCreate(FeedBase):
    source_type: Literal[SourceType.BCFY_FEEDS]
    # Broadcastify feed ID (e.g., "12345")
    source_feed_id: str = Field(pattern=r"^\d+$")


class BcfyCallsCreate(FeedBase):
    source_type: Literal[SourceType.BCFY_CALLS]
    # Broadcastify Calls ID: sid-talkgroup (e.g., "123-456")
    source_feed_id: str = Field(pattern=r"^\d+-\d+$")


class EchoCreate(FeedBase):
    source_type: Literal[SourceType.ECHO]
    # Echo feed ID (e.g., "feed-123_abc")
    source_feed_id: str = Field(pattern=r"^[a-zA-Z0-9_-]+$")


class FireNotificationsCreate(FeedBase):
    source_type: Literal[SourceType.FIRE_NOTIFICATIONS]
    # Fire notification feed ID (e.g., "FIRE/DEPT-1(A)_B")
    source_feed_id: str = Field(pattern=r"^[A-Z0-9_\-/()]+$")


class OpenMhzCreate(FeedBase):
    source_type: Literal[SourceType.OPENMHZ]
    # OpenMHZ feed ID (e.g., "open_mhz_456")
    source_feed_id: str = Field(pattern=r"^\w+$")


FeedCreate = Annotated[
    Union[
        BcfyFeedsCreate,
        BcfyCallsCreate,
        EchoCreate,
        FireNotificationsCreate,
        OpenMhzCreate,
    ],
    Field(discriminator="source_type"),
]


class FeedUpdate(BaseModel):
    name: str
    tags: list[Tag] | None = None

    model_config = ConfigDict(extra="forbid")


class Feed(FeedBase):
    id: uuid.UUID
    source_feed_id: str
    status: FeedStatus
    last_heartbeat: datetime.datetime | None
    quarantine_reason: str | None = None
    status_reason: BackendFeedStatusReason | None = None

    @field_validator("status_reason", mode="before")
    @classmethod
    def _map_status_reason(
        cls,
        value: FeedStatusReason | BackendFeedStatusReason | str | None,
    ) -> BackendFeedStatusReason | None:
        return _public_status_reason(value)

    model_config = ConfigDict(from_attributes=True)


class ListFeedsResponse(BaseModel):
    feeds: list[Feed]
    next_token: str | None = None
    total: int
