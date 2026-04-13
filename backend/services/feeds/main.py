from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

from fastapi import Depends, FastAPI, HTTPException, Request, status

from backend.pipeline.common.auth import verify_oidc_token
from backend.pipeline.storage.connection import (
    close_pool,
    create_pool_with_retry,
)
from backend.pipeline.storage.feed_store import FeedStore

from .models import Feed, FeedCreate
from .service import FeedService

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Manage the lifecycle of the AlloyDB connection pool."""
    pool = await create_pool_with_retry()
    store = FeedStore(pool)
    app.state.feed_service = FeedService(store)
    yield
    await close_pool(pool)


app = FastAPI(
    title="Feeds Management Service",
    description="API for creating, reading, and deleting feeds.",
    version="1.0.0",
    lifespan=lifespan,
    dependencies=[Depends(verify_oidc_token)],
)


@app.post(
    "/v1/feeds",
    status_code=status.HTTP_201_CREATED,
    response_model=Feed,
    tags=["feeds"],
)
async def create_feed(
    request: Request,
    feed_in: FeedCreate,
) -> Feed:
    """Create a new feed."""
    service: FeedService = request.app.state.feed_service
    try:
        return await service.create_feed(feed_in)
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )


@app.get(
    "/v1/feeds/{feed_id}",
    response_model=Feed,
    tags=["feeds"],
)
async def get_feed(
    request: Request,
    feed_id: str,
) -> Feed:
    """Fetch a specific feed by ID."""
    service: FeedService = request.app.state.feed_service
    feed = await service.get_feed(feed_id)
    if not feed:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Feed {feed_id} not found",
        )
    return feed


@app.get(
    "/v1/feeds",
    response_model=list[Feed],
    tags=["feeds"],
)
async def list_feeds(
    request: Request,
) -> list[Feed]:
    """List all feeds."""
    service: FeedService = request.app.state.feed_service
    return await service.list_feeds()


@app.delete(
    "/v1/feeds/{feed_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    tags=["feeds"],
)
async def delete_feed(
    request: Request,
    feed_id: str,
) -> None:
    """Delete a feed."""
    service: FeedService = request.app.state.feed_service
    success = await service.delete_feed(feed_id)
    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Feed {feed_id} not found",
        )
