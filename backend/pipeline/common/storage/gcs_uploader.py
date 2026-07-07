"""Utilities for uploading audio and other artifacts to Google Cloud Storage."""

import logging

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
                "GCS 412 (object already exists) for gs://%s/%s -- "
                "treating as success",
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
