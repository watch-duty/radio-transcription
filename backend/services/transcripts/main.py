import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, HTTPException, Request, status

from backend.pipeline.common.auth import verify_oidc_token
from backend.pipeline.storage.connection import (
    close_pool,
    create_pool_with_retry,
)
from backend.pipeline.storage.transcript_store import TranscriptStore

from .models import Transcript
from .service import TranscriptService

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Manage the lifecycle of the AlloyDB connection pool."""
    pool = await create_pool_with_retry()
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
    transcript: Transcript,
) -> Transcript:
    """Create a new transcription record."""
    service: TranscriptService = request.app.state.transcript_service
    try:
        return await service.create_transcript(transcript)
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
    transcript = await service.get_transcript(transmission_id)
    if not transcript:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Transcript for transmission {transmission_id} not found",
        )
    return transcript


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
        return await service.list_transcripts_by_feed_id(feed_id)
    return await service.list_transcripts()


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
