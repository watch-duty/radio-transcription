from __future__ import annotations

import datetime  # noqa: TC003
import uuid  # noqa: TC003
from typing import Annotated, Literal, Union

from pydantic import BaseModel, ConfigDict, Field

from backend.pipeline.storage.feed_store import (
    FeedStatus,  # noqa: TC001
    FeedStatusReason,  # noqa: TC001
    SourceType,  # noqa: TC001
)


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
    """API projection of one feed.

    ``status`` and its reason/heartbeat fields are the raw child
    lifecycle used for admin action eligibility. The ``effective_*``
    fields carry the server-derived lease-aware health for maintained
    Broadcastify Calls SID members (mirroring the child for other rows)
    and should drive status display and filtering. ``bcfy_calls_sid``
    and the ``lease_*`` fields expose raw SID management metadata.
    """

    id: uuid.UUID
    source_feed_id: str
    status: FeedStatus
    last_heartbeat: datetime.datetime | None
    status_reason: FeedStatusReason | None = None
    status_reason_detail: str | None = None
    last_speech_segment_timestamp: datetime.datetime | None = None
    bcfy_calls_sid: str | None = None
    lease_status: FeedStatus | None = None
    lease_last_heartbeat: datetime.datetime | None = None
    lease_status_reason: FeedStatusReason | None = None
    effective_status: FeedStatus | None = None
    effective_status_reason: FeedStatusReason | None = None
    effective_status_reason_detail: str | None = None
    effective_last_heartbeat: datetime.datetime | None = None

    model_config = ConfigDict(from_attributes=True)


class ListFeedsResponse(BaseModel):
    feeds: list[Feed]
    next_token: str | None = None
    total: int


class FeedHistoryEvent(BaseModel):
    id: uuid.UUID
    feed_id: uuid.UUID
    action: str
    actor: str
    occurred_at: datetime.datetime
    feed_revision_num: int
    before_values: dict
    after_values: dict

    model_config = ConfigDict(from_attributes=True)


class ListFeedHistoryResponse(BaseModel):
    history_events: list[FeedHistoryEvent]
    next_token: str | None = None
    total: int


class FeedSearchOptionsResponse(BaseModel):
    source_types: list[str]
    statuses: list[str]
    tags: list[Tag]
