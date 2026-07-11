from __future__ import annotations

import json
import logging
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

from fastapi import Depends, FastAPI, HTTPException, Request, status
from google.cloud import storage

from backend.pipeline.common.actor_identity import (
    is_well_formed_google_user_actor_id,
)
from backend.pipeline.common.auth import verify_oidc_token
from backend.pipeline.common.exceptions import (
    FeedAlreadyExistsError,
    FeedNameAlreadyExistsError,
    FeedStateConflictError,
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

from .echo_client import EchoClient
from .models import (
    Feed,
    FeedCreate,
    FeedUpdate,
    ListFeedHistoryResponse,
    ListFeedsResponse,
    Tag,
)
from .service import FeedService

logger = logging.getLogger(__name__)

_INTERNAL_ACTOR_ID_HEADER = "X-WD-Actor-Id"


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Manage the lifecycle of the AlloyDB connection pool."""
    pool = await create_pool_with_retry()
    store = FeedStore(pool)

    echo_client = None
    try:
        storage_client = storage.Client()
        echo_client = EchoClient(storage_client)
    except Exception as e:
        logger.warning(
            "Failed to initialize GCS client for Echo verification. "
            "Echo validation will be disabled. Error: %s",
            e,
        )

    app.state.feed_service = FeedService(store, echo_client=echo_client)
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


def _resolve_admin_actor_id(request: Request) -> str:
    """Resolve the BFF-provided human actor for admin mutations.

    The BFF authenticates the user, checks admin access, and derives this
    header from the verified Google user email. The Authorization token on the
    BFF-to-feeds-service request authenticates the BFF service account, not the
    human user, so feeds-service cannot derive this actor from that token.
    Keep feeds-service admin mutation routes private to the BFF service account;
    any public ingress path to feeds-service must strip ``X-WD-Actor-Id``.
    """
    actor_id = request.headers.get(_INTERNAL_ACTOR_ID_HEADER)
    if actor_id is None or not is_well_formed_google_user_actor_id(actor_id):
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


@app.get(
    "/v1/feeds/{feed_id}/history",
    response_model=ListFeedHistoryResponse,
    tags=["feeds"],
)
async def list_feed_history(
    request: Request,
    feed_id: str,
    limit: int = 100,
    next_token: str | None = None,
    order: SortOrder = SortOrder.DESC,
) -> ListFeedHistoryResponse:
    """List state history for a specific feed with keyset pagination."""
    service: FeedService = request.app.state.feed_service
    try:
        res = await service.list_feed_history(
            feed_id=feed_id,
            limit=limit,
            next_token=next_token,
            order=order,
        )
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        ) from e
    if res is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Feed {feed_id} not found",
        )
    return res


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
    try:
        success = await service.delete_feed(feed_id, actor_id=actor_id)
    except FeedStateConflictError as e:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=str(e),
        ) from e
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
    try:
        feed = await service.reset_feed(feed_id, actor_id=actor_id)
    except FeedStateConflictError as e:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=str(e),
        ) from e
    if not feed:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Feed {feed_id} not found",
        )
    return feed
