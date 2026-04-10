from __future__ import annotations

import uuid  # noqa: TC003

from pydantic import BaseModel, ConfigDict

from backend.pipeline.storage.feed_store import SourceType  # noqa: TC001


class FeedBase(BaseModel):
    name: str
    source_type: SourceType


class FeedCreate(FeedBase):
    source_feed_id: str
    external_id: str


class Feed(FeedBase):
    id: uuid.UUID
    source_feed_id: str | None = None
    external_id: str | None = None

    model_config = ConfigDict(from_attributes=True)
