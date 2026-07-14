from __future__ import annotations

import datetime  # noqa: TC003
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Annotated

from fastapi import Depends, FastAPI, HTTPException, Query, Request, status

from backend.pipeline.common.auth import verify_oidc_token
from backend.pipeline.common.fastapi_tracing import setup_fastapi_tracing
from backend.pipeline.storage.audio_segment_store import (
    AudioSegmentStore,
    SortOrder,
)
from backend.pipeline.storage.connection import (
    close_pool,
    create_pool_with_retry,
)

from .models import (
    Annotation,
    AnnotationCreate,
    AudioSegment,
    AudioSegmentCreate,
    ListAudioSegmentsResponse,
)
from .service import AudioSegmentService

if TYPE_CHECKING:
    from collections.abc import AsyncIterator


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Manage the lifecycle of the AlloyDB connection pool."""
    pool = await create_pool_with_retry()
    store = AudioSegmentStore(pool)
    app.state.audio_segment_service = AudioSegmentService(store)
    yield
    await close_pool(pool)


app = FastAPI(
    title="Audio Segments Service",
    description="API for listing and creating audio segments.",
    version="1.0.0",
    lifespan=lifespan,
    dependencies=[Depends(verify_oidc_token)],
)
setup_fastapi_tracing(app, service_name="audio-segments-service")


@app.get(
    "/v1/audio_segments",
    response_model=ListAudioSegmentsResponse,
    tags=["audio_segments"],
)
async def list_audio_segments(
    request: Request,
    feed_ids: Annotated[list[str] | None, Query()] = None,
    limit: int = 100,
    next_token: str | None = None,
    start_time: datetime.datetime | None = None,
    end_time: datetime.datetime | None = None,
    order: SortOrder = SortOrder.DESC,
    *,
    is_alert: Annotated[bool | None, Query(default=None)] = None,
    text_query: Annotated[
        str | None,
        Query(
            description=(
                "Search query to filter audio segments by transcript text "
                "(case-insensitive substring match)"
            )
        ),
    ] = None,
) -> ListAudioSegmentsResponse:
    """List audio segments with their annotations and pagination."""
    service: AudioSegmentService = request.app.state.audio_segment_service
    try:
        return await service.list_audio_segments(
            feed_ids=feed_ids,
            limit=limit,
            next_token=next_token,
            start_time=start_time,
            end_time=end_time,
            order=order,
            is_alert=is_alert,
            text_query=text_query,
        )
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )


@app.post(
    "/v1/audio_segments",
    response_model=AudioSegment,
    status_code=status.HTTP_201_CREATED,
    tags=["audio_segments"],
)
async def create_audio_segment(
    request: Request,
    segment: AudioSegmentCreate,
) -> AudioSegment:
    """Create a single audio segment."""
    service: AudioSegmentService = request.app.state.audio_segment_service
    try:
        return await service.create_audio_segment(segment)
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )


@app.post(
    "/v1/audio_segments/{audio_segment_id}/annotations",
    response_model=Annotation,
    status_code=status.HTTP_201_CREATED,
    tags=["audio_segments"],
)
async def add_annotation(
    request: Request,
    audio_segment_id: str,
    annotation: AnnotationCreate,
) -> Annotation:
    """Add an annotation to an audio segment."""
    service: AudioSegmentService = request.app.state.audio_segment_service
    try:
        return await service.add_annotation(audio_segment_id, annotation)
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )
