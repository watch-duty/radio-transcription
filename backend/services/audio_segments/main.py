from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING

from fastapi import Depends, FastAPI, HTTPException, Request, status

from backend.pipeline.common.auth import verify_oidc_token
from backend.pipeline.storage.audio_segment_store import AudioSegmentStore
from backend.pipeline.storage.connection import (
    close_pool,
    create_pool_with_retry,
)

from .models import (
    AudioSegment,
    BulkAddAudioSegmentsRequest,
    BulkAddAudioSegmentsResponse,
)
from .service import AudioSegmentService

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

logger = logging.getLogger(__name__)


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
    description="API for listing and bulk adding audio segments.",
    version="1.0.0",
    lifespan=lifespan,
    dependencies=[Depends(verify_oidc_token)],
)


@app.get(
    "/v1/list_audio_segments",
    response_model=list[AudioSegment],
    tags=["audio_segments"],
)
@app.get(
    "/v1/audio_segments",
    response_model=list[AudioSegment],
    tags=["audio_segments"],
)
async def list_audio_segments(
    request: Request,
    feed_ids: list[str] | None = None,
) -> list[AudioSegment]:
    """List audio segments with their annotations. Optionally filter by feed IDs."""
    service: AudioSegmentService = request.app.state.audio_segment_service
    try:
        return await service.list_audio_segments(feed_ids)
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )


@app.post(
    "/v1/audio_segments",
    response_model=BulkAddAudioSegmentsResponse,
    status_code=status.HTTP_201_CREATED,
    tags=["audio_segments"],
)
async def bulk_add_audio_segments(
    request: Request,
    payload: BulkAddAudioSegmentsRequest,
) -> BulkAddAudioSegmentsResponse:
    """Idempotently add multiple audio segments in bulk. Already existing IDs are skipped."""
    service: AudioSegmentService = request.app.state.audio_segment_service
    try:
        inserted = await service.bulk_add_audio_segments(payload.audio_segments)
        return BulkAddAudioSegmentsResponse(inserted_count=inserted)
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )
