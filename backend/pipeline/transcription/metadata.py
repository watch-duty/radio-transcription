"""Utility functions for interacting with Google Cloud Storage in the radio transcription pipeline."""

import base64
import logging

from google.cloud import storage

from backend.pipeline.common.constants import MS_PER_SECOND, NANOS_PER_MS
from backend.pipeline.schema_types.sed_metadata_pb2 import (
    SedMetadata,
)
from backend.pipeline.transcription.datatypes import TimeRange

logger = logging.getLogger(__name__)


def get_gcs_client() -> storage.Client:
    """Initialize and return a GCS Client."""
    return storage.Client()


def read_sed_segments_from_blob(
    blob: storage.Blob,
) -> tuple[int, list[TimeRange]]:
    """Parse SED metadata from a GCS blob.

    Returns:
        tuple containing (start_ms, list of speech segment TimeRanges).

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

    if not sed_metadata.HasField("start_timestamp"):
        err_msg = f"SED metadata missing required 'start_timestamp' on blob: {blob.name}"
        logger.error(err_msg)
        raise ValueError(err_msg)

    file_start_ms = (
        sed_metadata.start_timestamp.seconds * MS_PER_SECOND
        + sed_metadata.start_timestamp.nanos // NANOS_PER_MS
    )

    segments = []
    for i, seg in enumerate(sed_metadata.sound_events):
        if not seg.HasField("start_time"):
            err_msg = f"SED metadata sound event {i} missing 'start_time' on blob: {blob.name}"
            logger.error(err_msg)
            raise ValueError(err_msg)
        if not seg.HasField("duration"):
            err_msg = f"SED metadata sound event {i} missing 'duration' on blob: {blob.name}"
            logger.error(err_msg)
            raise ValueError(err_msg)

        start_ms = (
            seg.start_time.seconds * MS_PER_SECOND
            + seg.start_time.nanos // NANOS_PER_MS
        )
        duration_ms = (
            seg.duration.seconds * MS_PER_SECOND
            + seg.duration.nanos // NANOS_PER_MS
        )
        segments.append(
            TimeRange(start_ms=start_ms, end_ms=start_ms + duration_ms)
        )

    return file_start_ms, segments
