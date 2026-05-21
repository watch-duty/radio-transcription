from __future__ import annotations

import datetime  # noqa: TC003
import uuid  # noqa: TC003

from pydantic import BaseModel, ConfigDict

from backend.pipeline.storage.feed_store import (  # noqa: TC001
    FeedStatus,
    SourceType,
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


class FeedCreate(FeedBase):
    # ID for each given data source.
    # For Broadcastify Feeds, it would be the last part of
    # https://partner.broadcastify.com/<FEED_ID>.
    # For Echo, it would be the first part of
    # <CHANNEL_NAME>/20260406/Santa_Clara_Co_Fire_Disp_20260406_102306.mp3.
    source_feed_id: str
    external_id: str
    tags: list[Tag] | None = None


class FeedUpdate(BaseModel):
    name: str
    external_id: str
    tags: list[Tag] | None = None


class Feed(FeedBase):
    id: uuid.UUID
    source_feed_id: str
    external_id: str
    status: FeedStatus
    last_heartbeat: datetime.datetime | None
    tags: list[Tag] | None = None

    model_config = ConfigDict(from_attributes=True)
