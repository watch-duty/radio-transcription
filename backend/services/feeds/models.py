from __future__ import annotations

import datetime  # noqa: TC003
import re
import uuid  # noqa: TC003
from typing import Self

from pydantic import BaseModel, ConfigDict, model_validator

from backend.pipeline.storage.feed_store import (
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

    @model_validator(mode="after")
    def validate_source_feed_id(self) -> Self:
        source_id = self.source_feed_id.strip()

        if self.source_type == SourceType.BCFY_CALLS:
            if not re.match(r"^\d+-\d+$", source_id):
                msg = "source_feed_id must only contain numbers with a dash in the middle for Broadcastify Calls."
                raise ValueError(msg)
        elif self.source_type == SourceType.BCFY_FEEDS:
            if not re.match(r"^\d+$", source_id):
                msg = "source_feed_id must be a number for Broadcastify Feeds."
                raise ValueError(msg)
        elif self.source_type == SourceType.ECHO:
            if not re.match(r"^[a-zA-Z0-9_-]+$", source_id):
                msg = "source_feed_id must only contain letters, numbers, and the special characters: - _ for Echo."
                raise ValueError(msg)
        elif self.source_type == SourceType.FIRE_NOTIFICATIONS:
            if not re.match(r"^[A-Z0-9_\-/()]+$", source_id):
                msg = "source_feed_id must only contain uppercase letters, numbers, and the special characters: / - ( ) _ for Fire Notifications."
                raise ValueError(msg)
        elif self.source_type == SourceType.OPENMHZ:
            if not re.match(r"^\w+$", source_id):
                msg = "source_feed_id must only contain letters, numbers, and underscores for OpenMHZ."
                raise ValueError(msg)

        return self


class FeedUpdate(BaseModel):
    name: str
    external_id: str
    tags: list[Tag] | None = None

    model_config = ConfigDict(extra="forbid")


class Feed(FeedBase):
    id: uuid.UUID
    source_feed_id: str
    external_id: str
    status: FeedStatus
    last_heartbeat: datetime.datetime | None
    tags: list[Tag] | None = None

    model_config = ConfigDict(from_attributes=True)
