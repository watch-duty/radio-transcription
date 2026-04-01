from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from datetime import datetime
from typing import TYPE_CHECKING, Annotated, Any

from fastapi import Depends, FastAPI, HTTPException, Request, status
from pydantic import BaseModel

from backend.pipeline.common.auth import verify_oidc_token
from backend.pipeline.storage.connection import (
    close_pool,
    create_pool_from_settings,
)
from backend.pipeline.storage.transcript_store import TranscriptStore

from .service import TranscriptService

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from datetime import datetime

logger = logging.getLogger(__name__)


class Transcript(BaseModel):
    """Transcript type used for API responses."""

    feed_id: str
    transmission_id: str
    transcript: str
    start_timestamp: datetime | None = None
    end_timestamp: datetime | None = None
    missing_prior_context: bool = False
    missing_post_context: bool = False
    source_audio_uris: list[str] = []
    canonical_audio_uri: str | None = None
    start_audio_offset: str | None = None
    end_audio_offset: str | None = None
    evaluation_decisions: list[str] = []


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Manage the lifecycle of the AlloyDB connection pool."""
    pool = await create_pool_from_settings()
    store = TranscriptStore(pool)
    app.state.transcript_service = TranscriptService(store)
    yield
    await close_pool(pool)


app = FastAPI(
    title="Transcripts Management Service",
    description="API for creating, reading, and deleting transcription records.",
    version="1.0.0",
    lifespan=lifespan,
    dependencies=[Depends(verify_oidc_token)],
)


@app.post(
    "/v1/transcripts",
    status_code=status.HTTP_201_CREATED,
    tags=["transcripts"],
)
async def create_transcript(
    request: Request,
    transcript_in: dict[str, Any],
    user: Annotated[dict[str, Any], Depends(verify_oidc_token)],
) -> Transcript:
    """Create a new transcription record."""
    service: TranscriptService = request.app.state.transcript_service
    try:
        data = await service.create_transcript(transcript_in)
        return Transcript(**data)
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )


@app.get(
    "/v1/transcripts/{transmission_id}",
    tags=["transcripts"],
)
async def get_transcript(
    request: Request,
    transmission_id: str,
) -> Transcript:
    """Fetch a specific transcript by transmission ID."""
    service: TranscriptService = request.app.state.transcript_service
    transcript_data = await service.get_transcript(transmission_id)
    if not transcript_data:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Transcript for transmission {transmission_id} not found",
        )
    return Transcript(**transcript_data)


@app.get(
    "/v1/transcripts",
    tags=["transcripts"],
)
async def list_transcripts(
    request: Request,
    feed_id: str | None = None,
) -> list[Transcript]:
    """List transcripts, optionally filtered by feed ID."""
    service: TranscriptService = request.app.state.transcript_service
    if feed_id:
        data = await service.list_transcripts_by_feed_id(feed_id)
    else:
        data = await service.list_transcripts()
    return [Transcript(**t) for t in data]


@app.delete(
    "/v1/transcripts/{transmission_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    tags=["transcripts"],
)
async def delete_transcript(
    request: Request,
    transmission_id: str,
) -> None:
    """Delete a transcription record."""
    service: TranscriptService = request.app.state.transcript_service
    success = await service.delete_transcript(transmission_id)
    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Transcript for transmission {transmission_id} not found",
        )
