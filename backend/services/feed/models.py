import datetime
import uuid

from pydantic import BaseModel

from backend.pipeline.storage.feed_store import SourceType


class FeedUpdate(BaseModel):
    name: str | None = None
    source_type: SourceType | None = None
    status: str | None = None


class Feed(BaseModel):
    id: uuid.UUID
    name: str
    source_type: SourceType
    status: str
    failure_count: int
    created_at: datetime.datetime
    source_feed_id: str | None = None
    external_id: str | None = None
    worker_id: uuid.UUID | None = None
    last_heartbeat: datetime.datetime | None = None
    last_processed_filename: str | None = None

    class Config:
        from_attributes = True
