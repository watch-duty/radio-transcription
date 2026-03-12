"""
Utility functions for the radio transcription pipeline.
These functions handle external I/O (like fetching blobs from GCS)
that are required by the pipeline's core transforms.
"""

import logging
import urllib.parse

from google.cloud import storage

from backend.pipeline.schema_types.sed_metadata_pb2 import (
    SedMetadata,
)

logger = logging.getLogger(__name__)


def get_gcs_client() -> storage.Client:
    return storage.Client()


import base64

def read_sed_segments_from_blob(blob: storage.Blob) -> list[tuple[float, float]]:
    """
    Parses a pre-computed Speech Activity Detection (SED) profile from a GCS blob's custom metadata.
    Returns a list of (start_sec, end_sec) tuples denoting periods of active speech.
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

    return [
        (
            seg.start_time.seconds + seg.start_time.nanos / 1e9,
            seg.start_time.seconds + seg.start_time.nanos / 1e9 + seg.duration.seconds + seg.duration.nanos / 1e9
        )
        for seg in sed_metadata.sound_events
    ]
