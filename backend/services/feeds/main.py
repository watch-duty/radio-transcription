from __future__ import annotations

import json
import logging
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

from fastapi import Depends, FastAPI, HTTPException, Request, status

from backend.pipeline.common.auth import verify_oidc_token
from backend.pipeline.common.exceptions import (
    FeedAlreadyExistsError,
    FeedNameAlreadyExistsError,
)
from backend.pipeline.common.fastapi_tracing import setup_fastapi_tracing
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

from .models import Feed, FeedCreate, FeedUpdate, ListFeedsResponse, Tag
from .service import FeedService

logger = logging.getLogger(__name__)

_INTERNAL_ACTOR_ID_HEADER = "X-WD-Actor-Id"
_ADMIN_ACTOR_ID_PREFIX = "user:google:"


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
setup_fastapi_tracing(app, service_name="feeds-service")


def _is_well_formed_admin_actor_id(actor_id: str) -> bool:
    if not actor_id.startswith(_ADMIN_ACTOR_ID_PREFIX):
        return False

    google_sub = actor_id[len(_ADMIN_ACTOR_ID_PREFIX) :]
    return bool(google_sub) and not any(char.isspace() for char in google_sub)


def _resolve_admin_actor_id(request: Request) -> str:
    actor_id = request.headers.get(_INTERNAL_ACTOR_ID_HEADER)
    if actor_id is None or not _is_well_formed_admin_actor_id(actor_id):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Trusted actor context required",
        )

    return actor_id


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
    actor_id = _resolve_admin_actor_id(request)
    try:
        return await service.create_feed(feed_in, actor_id=actor_id)
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
    actor_id = _resolve_admin_actor_id(request)
    try:
        feed = await service.update_feed(feed_id, feed_in, actor_id=actor_id)
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
    source_types: str | None = None,
    statuses: str | None = None,
    tags: str | None = None,
    name: str | None = None,
) -> ListFeedsResponse:
    """List all feeds with pagination and optional filters."""
    source_types_list = None
    if source_types:
        try:
            source_types_list = [
                SourceType(s.strip())
                for s in source_types.split(",")
                if s.strip()
            ]
        except ValueError as e:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid source_type: {e}",
            ) from e

    statuses_list = None
    if statuses:
        try:
            statuses_list = [
                FeedStatus(s.strip()) for s in statuses.split(",") if s.strip()
            ]
        except ValueError as e:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid status: {e}",
            ) from e

    tags_list = None
    if tags:
        try:
            tags_data = json.loads(tags)
        except json.JSONDecodeError as e:
            err_msg = f"Invalid format for tags: {e}"
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=err_msg,
            ) from e

        if not isinstance(tags_data, list):
            err_msg = "Tags must be a JSON list with key, value entries."
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=err_msg,
            )

        try:
            tags_list = [Tag.model_validate(t).model_dump() for t in tags_data]
        except ValueError as e:
            err_msg = f"Invalid format for tags: {e}"
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=err_msg,
            ) from e

    service: FeedService = request.app.state.feed_service
    return await service.list_feeds(
        limit=limit,
        next_token=next_token,
        order=order,
        source_types=source_types_list,
        statuses=statuses_list,
        tags=tags_list,
        name=name,
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
    actor_id = _resolve_admin_actor_id(request)
    success = await service.deactivate_feed(feed_id, actor_id=actor_id)
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
    actor_id = _resolve_admin_actor_id(request)
    success = await service.delete_feed(feed_id, actor_id=actor_id)
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
    actor_id = _resolve_admin_actor_id(request)
    feed = await service.reset_feed(feed_id, actor_id=actor_id)
    if not feed:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Feed {feed_id} not found",
        )
    return feed
