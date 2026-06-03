from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Annotated

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

from fastapi import Depends, FastAPI, HTTPException, Query, Request, status

from backend.pipeline.common.auth import verify_oidc_token
from backend.pipeline.common.exceptions import (
    FeedAlreadyExistsError,
    FeedNameAlreadyExistsError,
)
from backend.pipeline.storage.connection import (
    close_pool,
    create_pool_with_retry,
)
from backend.pipeline.storage.feed_store import (
    FeedStatus,
    FeedStore,
    SourceType,
)
from backend.pipeline.storage.pagination_utils import SortOrder

from .models import Feed, FeedCreate, FeedUpdate, ListFeedsResponse
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
    description="API for creating, reading, and deactivating feeds.",
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
    except FeedAlreadyExistsError as e:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
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


@app.put(
    "/v1/feeds/{feed_id}",
    response_model=Feed,
    tags=["feeds"],
)
async def update_feed(
    request: Request,
    feed_id: str,
    feed_in: FeedUpdate,
) -> Feed:
    """Update an existing feed."""
    service: FeedService = request.app.state.feed_service
    try:
        feed = await service.update_feed(feed_id, feed_in)
        if not feed:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Feed {feed_id} not found",
            )
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )
    except FeedNameAlreadyExistsError as e:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=str(e),
        )
    else:
        return feed


@app.get(
    "/v1/feeds",
    response_model=ListFeedsResponse,
    tags=["feeds"],
)
async def list_feeds(
    request: Request,
    limit: int = 100,
    next_token: str | None = None,
    order: SortOrder = SortOrder.DESC,
    source_types: Annotated[list[SourceType] | None, Query()] = None,
    statuses: Annotated[list[FeedStatus] | None, Query()] = None,
) -> ListFeedsResponse:
    """List all feeds with pagination and optional filters."""
    service: FeedService = request.app.state.feed_service
    return await service.list_feeds(
        limit=limit,
        next_token=next_token,
        order=order,
        source_types=source_types,
        statuses=statuses,
    )


@app.post(
    "/v1/feeds/{feed_id}/deactivate",
    status_code=status.HTTP_204_NO_CONTENT,
    tags=["feeds"],
)
async def deactivate_feed(
    request: Request,
    feed_id: str,
) -> None:
    """Deactivate a feed until an explicit reset."""
    service: FeedService = request.app.state.feed_service
    success = await service.deactivate_feed(feed_id)
    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Feed {feed_id} not found",
        )


@app.delete(
    "/v1/feeds/{feed_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    tags=["feeds"],
)
async def delete_feed(
    request: Request,
    feed_id: str,
) -> None:
    """Hard delete a feed, along with all its transcripts and audio segments."""
    service: FeedService = request.app.state.feed_service
    success = await service.delete_feed(feed_id)
    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Feed {feed_id} not found",
        )


@app.post(
    "/v1/feeds/{feed_id}/reset",
    response_model=Feed,
    tags=["feeds"],
)
async def reset_feed(
    request: Request,
    feed_id: str,
) -> Feed:
    """Reset a feed to unclaimed status with zero failure count."""
    service: FeedService = request.app.state.feed_service
    feed = await service.reset_feed(feed_id)
    if not feed:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Feed {feed_id} not found",
        )
    return feed
