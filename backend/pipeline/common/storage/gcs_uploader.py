"""Utilities for uploading audio and other artifacts to Google Cloud Storage."""

import logging
from collections.abc import Callable

import numpy as np
from google.api_core.exceptions import PreconditionFailed
from google.cloud import storage

logger = logging.getLogger(__name__)


class GCSAudioUploader:
    """Generic manager for audio and artifact uploads to GCS."""

    def __init__(self, gcs_client: storage.Client) -> None:
        """Initializes the uploader with a GCS client.

        Args:
            gcs_client: An initialized Google Cloud Storage client.
        """
        self.gcs_client = gcs_client

    def upload_bytes(
        self,
        data: bytes,
        bucket_name: str,
        destination_path: str,
        content_type: str = "application/octet-stream",
    ) -> str:
        """Uploads raw bytes to a GCS bucket.

        Args:
            data: The bytes to upload.
            bucket_name: The target GCS bucket name.
            destination_path: The destination path (key) within the bucket.
            content_type: The MIME type of the content.

        Returns:
            The GCS URI (gs://...) of the uploaded object.
        """
        try:
            bucket = self.gcs_client.bucket(bucket_name)
            blob = bucket.blob(destination_path)
            blob.upload_from_string(
                data, content_type=content_type, if_generation_match=0
            )
            uri = f"gs://{bucket_name}/{destination_path}"
            logger.debug("Uploaded artifact to %s", uri)
        except PreconditionFailed:
            uri = f"gs://{bucket_name}/{destination_path}"
            logger.info(
                "GCS 412 (object already exists) for gs://%s/%s -- treating as success",
                bucket_name,
                destination_path,
            )
            return uri
        except Exception:
            logger.exception(
                "Failed to upload artifact to gs://%s/%s",
                bucket_name,
                destination_path,
            )
            raise
        else:
            return uri

    def upload_audio_derivatives(
        self,
        bucket_name: str,
        flac_path: str,
        m4a_path: str,
        flac_bytes: bytes,
        processed_audio: np.ndarray,
        export_m4a_fn: Callable[[np.ndarray], bytes],
    ) -> tuple[str, str]:
        """Uploads FLAC and M4A audio derivatives to GCS.

        Args:
            bucket_name: The destination bucket.
            flac_path: The GCS path for the FLAC file.
            m4a_path: The GCS path for the M4A file.
            flac_bytes: The lossless FLAC data.
            processed_audio: The numpy array for derivative generation.
            export_m4a_fn: Function handle to perform the M4A encoding.

        Returns:
            A tuple of (canonical_audio_uri, playback_audio_uri).
        """
        # Check if lossless FLAC already exists in GCS to skip work
        bucket = self.gcs_client.bucket(bucket_name)
        flac_blob = bucket.blob(flac_path)
        try:
            if flac_blob.exists():
                canonical_uri = f"gs://{bucket_name}/{flac_path}"
                playback_uri = f"gs://{bucket_name}/{m4a_path}"
                logger.info(
                    "GCS derivatives already exist for gs://%s/%s -- skipping upload and export",
                    bucket_name,
                    flac_path,
                )
                return canonical_uri, playback_uri
        except Exception:
            logger.warning(
                "Failed to check existence for gs://%s/%s, proceeding with upload",
                bucket_name,
                flac_path,
            )

        # Upload Lossless FLAC
        canonical_uri = self.upload_bytes(
            data=flac_bytes,
            bucket_name=bucket_name,
            destination_path=flac_path,
            content_type="audio/flac",
        )
        logger.info("Uploaded stitched audio to %s", canonical_uri)

        # Export & Upload Voice-Optimized M4A
        m4a_bytes = export_m4a_fn(processed_audio)
        playback_uri = self.upload_bytes(
            data=m4a_bytes,
            bucket_name=bucket_name,
            destination_path=m4a_path,
            content_type="audio/mp4",
        )
        logger.info("Uploaded playback audio to %s", playback_uri)

        return canonical_uri, playback_uri
