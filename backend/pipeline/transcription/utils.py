"""
Utility functions for the radio transcription pipeline.
These functions handle external I/O (like fetching blobs from GCS)
that are required by the pipeline's core transforms.
"""

import logging
import urllib.parse

from google.cloud import storage

from backend.pipeline.schema_types.sad_metadata_pb2 import (
    SadMetadata,
)

logger = logging.getLogger(__name__)


def get_gcs_client() -> storage.Client:
    return storage.Client()


def read_sad_segments_from_gcs(gcs_path: str) -> list[tuple[float, float]]:
    """
    Downloads and parses a pre-computed Speech Activity Detection (SAD) profile for an audio chunk.
    Returns a list of (start_sec, end_sec) tuples denoting periods of active speech.
    """
    gcs_client = get_gcs_client()
    parsed_uri = urllib.parse.urlparse(gcs_path)
    bucket_name = parsed_uri.netloc
    blob_name = parsed_uri.path.lstrip("/")
    blob = gcs_client.bucket(bucket_name).get_blob(blob_name)
    if not blob:
        err_msg = f"SAD GCS object not found: {gcs_path}"
        logger.error(err_msg)
        raise FileNotFoundError(err_msg)
    sad_bytes = blob.download_as_bytes()
    sad_metadata = SadMetadata.FromString(sad_bytes)
    return [(seg.start_sec, seg.end_sec) for seg in sad_metadata.segments]
