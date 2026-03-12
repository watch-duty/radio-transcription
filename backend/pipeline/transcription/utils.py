"""
Utility functions for the radio transcription pipeline.
These functions handle external I/O (like fetching blobs from GCS)
that are required by the pipeline's core transforms.
"""

import base64
import logging

from google.cloud import storage

from backend.pipeline.schema_types.sed_metadata_pb2 import (
    SedMetadata,
)

logger = logging.getLogger(__name__)


def get_gcs_client() -> storage.Client:
    return storage.Client()


def read_sed_segments_from_blob(
    blob: storage.Blob,
) -> tuple[float | None, list[tuple[float, float]]]:
    """
    Parses a pre-computed Speech Activity Detection (SED) profile from a GCS blob's custom metadata.
    Returns the chunk's absolute start timestamp in seconds (if present) and a list of (start_sec, end_sec)
    tuples denoting periods of active speech relative to the start of the chunk.
    """
    blob.reload()  # Ensure metadata is loaded
    if not blob.metadata or "sed_metadata" not in blob.metadata:
        err_msg = f"SED metadata not found on blob: {blob.name}"
        logger.error(err_msg)
        raise FileNotFoundError(err_msg)

    metadata_b64 = blob.metadata["sed_metadata"]
    metadata_bytes = base64.b64decode(metadata_b64)
    sed_metadata = SedMetadata()
    sed_metadata.ParseFromString(metadata_bytes)

    chunk_start_sec = None
    if sed_metadata.HasField("start_timestamp"):
        chunk_start_sec = (
            sed_metadata.start_timestamp.seconds
            + sed_metadata.start_timestamp.nanos / 1e9
        )

    segments = [
        (
            seg.start_time.seconds + seg.start_time.nanos / 1e9,
            seg.start_time.seconds
            + seg.start_time.nanos / 1e9
            + seg.duration.seconds
            + seg.duration.nanos / 1e9,
        )
        for seg in sed_metadata.sound_events
    ]
    return chunk_start_sec, segments
