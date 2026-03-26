from __future__ import annotations

import asyncio
import logging
import os
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

from backend.pipeline.common.constants import AUDIO_FORMAT
from backend.pipeline.common.logging import setup_logging
from backend.pipeline.ingestion.collectors.icecast_collector import (
    capture_icecast_stream,
)

if TYPE_CHECKING:
    from backend.pipeline.storage.feed_store import LeasedFeed

logger = logging.getLogger(__name__)


async def run_local_capture() -> None:
    """
    Run Icecast capture directly for local debugging, writes to disk instead of GCS.
    Does not use the AlloyDB feed claiming or write to pubsub.

    Environment variables:
    - ICECAST_STREAM_URL: Required stream URL. Example: "https://example.com:8000/stream"
    - ICECAST_LOCAL_OUTPUT_DIR: Optional output directory for audio chunks.
      Defaults to the current working directory.
    """
    stream_url = os.getenv("ICECAST_STREAM_URL")
    if not stream_url:
        msg = "ICECAST_STREAM_URL must be set"
        raise ValueError(msg)

    output_dir = Path(os.getenv("ICECAST_LOCAL_OUTPUT_DIR") or Path.cwd())
    await asyncio.to_thread(output_dir.mkdir, parents=True, exist_ok=True)
    logger.info("Saving local audio chunks to %s", output_dir)

    feed: LeasedFeed = {
        "id": uuid.uuid4(),
        "name": "local-icecast-test",
        "source_type": "icecast",
        "last_processed_filename": None,
        "fencing_token": 0,
        "stream_url": stream_url,
    }
    shutdown_event = asyncio.Event()

    chunk_count = 0
    async for audio_data, _ts in capture_icecast_stream(feed, shutdown_event):
        chunk_count += 1
        timestamp = datetime.now(UTC).isoformat(timespec="milliseconds")
        file_timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S_%fZ")
        file_name = f"chunk_{chunk_count:06d}_{file_timestamp}.{AUDIO_FORMAT}"
        file_path = output_dir / file_name
        await asyncio.to_thread(file_path.write_bytes, audio_data)
        logger.info(
            "Local capture chunk %d received (%d bytes) at %s -> %s",
            chunk_count,
            len(audio_data),
            timestamp,
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
