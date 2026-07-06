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
    id: uuid.UUID
    source_feed_id: str
    status: FeedStatus
    last_heartbeat: datetime.datetime | None
    status_reason: FeedStatusReason | None = None
    status_reason_detail: str | None = None
    last_speech_segment_timestamp: datetime.datetime | None = None

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
    feed_revision: int
    before_values: dict
    after_values: dict

    model_config = ConfigDict(from_attributes=True)


class ListFeedHistoryResponse(BaseModel):
    history_events: list[FeedHistoryEvent]
    next_token: str | None = None
    total: int
