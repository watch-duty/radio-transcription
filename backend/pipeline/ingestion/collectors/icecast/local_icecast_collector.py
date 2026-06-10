from __future__ import annotations

import asyncio
import logging
import os
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

import aiohttp

from backend.pipeline.common.constants import AUDIO_FORMAT
from backend.pipeline.common.log_helper import setup_logging
from backend.pipeline.ingestion.collectors.icecast.icecast_collector import (
    capture_icecast_stream,
)
from backend.pipeline.ingestion.models import CaptureResources
from backend.pipeline.ingestion.router import BCFY_FEEDS_URL_BASE
from backend.pipeline.storage.feed_store import SourceType

if TYPE_CHECKING:
    from backend.pipeline.storage.feed_store import LeasedFeed

logger = logging.getLogger(__name__)


async def run_local_capture() -> None:
    """
    Run Icecast capture directly for local debugging, writes to disk instead of GCS.
    Does not use the AlloyDB feed claiming or write to pubsub.

    Environment variables:
    - ICECAST_SOURCE_FEED_ID: Required source feed ID. Example: "12345"
    - ICECAST_LOCAL_OUTPUT_DIR: Optional output directory for audio chunks.
      Defaults to the current working directory.
    """
    source_feed_id = os.getenv("ICECAST_SOURCE_FEED_ID")
    if not source_feed_id:
        msg = "ICECAST_SOURCE_FEED_ID must be set"
        raise ValueError(msg)

    output_dir = Path(os.getenv("ICECAST_LOCAL_OUTPUT_DIR") or Path.cwd())
    await asyncio.to_thread(output_dir.mkdir, parents=True, exist_ok=True)
    logger.info("Saving local audio chunks to %s", output_dir)

    feed: LeasedFeed = {
        "id": uuid.uuid4(),
        "name": "local-icecast-test",
        "source_type": SourceType.BCFY_FEEDS,
        "last_processed_filename": None,
        "last_bookmark_time": None,
        "fencing_token": 0,
        "failure_count": 0,
        "status_reason": None,
        "source_feed_id": source_feed_id,
    }
    shutdown_event = asyncio.Event()

    chunk_count = 0
    async with aiohttp.ClientSession() as http_session:
        resources = CaptureResources(http_session=http_session)
        async for captured_chunk in capture_icecast_stream(
            feed, shutdown_event, BCFY_FEEDS_URL_BASE, resources
        ):
            chunk_count += 1
            timestamp = datetime.now(UTC).isoformat(timespec="milliseconds")
            file_timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S_%fZ")
            file_name = (
                f"chunk_{chunk_count:06d}_{file_timestamp}.{AUDIO_FORMAT}"
            )
            file_path = output_dir / file_name
            await asyncio.to_thread(
                file_path.write_bytes, captured_chunk.audio_bytes
            )
            logger.info(
                "Local capture chunk %d received (%d bytes) at %s (start: %s) -> %s",
                chunk_count,
                len(captured_chunk.audio_bytes),
                timestamp,
                captured_chunk.chunk_start_time.isoformat(
                    timespec="milliseconds"
                ),
                file_path,
            )


def main() -> None:
    setup_logging()
    asyncio.run(run_local_capture())


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
