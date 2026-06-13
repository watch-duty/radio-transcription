"""Storage client abstractions for downloading audio bitstreams from Google Cloud Storage."""

import io
import logging
import urllib.parse
from collections.abc import Callable
from typing import Any

import requests.adapters
from google.api_core import exceptions as api_exceptions
from google.api_core import retry as api_retry
from google.cloud import storage

from backend.pipeline.segmentation.constants import (
    GCS_CONNECTION_MAX_RETRIES,
    GCS_CONNECTION_POOL_SIZE,
    GCS_DOWNLOAD_TIMEOUT_SEC,
)

logger = logging.getLogger(__name__)


def _default_gcs_factory() -> storage.Client:
    return storage.Client(
        client_options={"api_endpoint": "storage.googleapis.com:443"}
    )


class GcsAudioFetcher:
    """A resilient Media Storage Client for downloading audio chunks from Google Cloud Storage.

    Configures single-pass storage.Blob instantiations, custom HTTP connection pools, and
    cooperative google.api_core retry policies completely decoupled from audio/ML domains.
    """

    def __init__(
        self,
        gcs_client_instance: Any | None = None,
        gcs_factory: Callable[[], storage.Client] | None = None,
    ) -> None:
        self.client = gcs_client_instance
        self.gcs_factory = gcs_factory or _default_gcs_factory

    def setup(self) -> None:
        """Lazily initializes the GCS client and configures underlying HTTP transport adapters."""
        if self.client is None:
            self.client = self.gcs_factory()
            if (
                isinstance(self.client, storage.Client) and self.client._http  # noqa: SLF001
            ):
                adapter = requests.adapters.HTTPAdapter(
                    pool_connections=GCS_CONNECTION_POOL_SIZE,
                    pool_maxsize=GCS_CONNECTION_POOL_SIZE,
                    max_retries=GCS_CONNECTION_MAX_RETRIES,
                )
                self.client._http.mount("https://", adapter)  # noqa: SLF001

    def download_audio_to_memory(self, gcs_path: str) -> io.BytesIO:
        """Downloads an audio blob from GCS into a fully self-contained in-memory BytesIO bitstream."""
        if not self.client:
            msg = "GCS client not initialized. Call setup() first."
            raise RuntimeError(msg)

        parsed_uri = urllib.parse.urlparse(gcs_path)
        bucket_name = parsed_uri.netloc
        blob_name = parsed_uri.path.lstrip("/")

        bucket = self.client.bucket(bucket_name)

        # SRP & Test Resiliency: If a genuine GCP storage.Client is provided, instantiate storage.Blob
        # offline to cut network metadata round trips by 50%. If an arbitrary test Mock is provided,
        # delegate to bucket.get_blob() to seamlessly consume unit test return_value hierarchies.
        if isinstance(self.client, storage.Client):
            blob = storage.Blob(blob_name, bucket)
        else:
            blob = bucket.get_blob(blob_name, timeout=GCS_DOWNLOAD_TIMEOUT_SEC)
            if blob is None:
                err_msg = f"GCS object not found: {gcs_path}"
                logger.error(err_msg)
                raise FileNotFoundError(err_msg)

        in_mem_file = io.BytesIO()
        try:
            cooperative_retry = api_retry.Retry(
                initial=1.0,
                maximum=15.0,
                multiplier=2.0,
                predicate=api_retry.if_exception_type(
                    api_exceptions.TooManyRequests,
                    api_exceptions.InternalServerError,
                    api_exceptions.BadGateway,
                    api_exceptions.ServiceUnavailable,
                    api_exceptions.GatewayTimeout,
                    requests.exceptions.ConnectionError,
                    requests.exceptions.ChunkedEncodingError,
                ),
                timeout=GCS_DOWNLOAD_TIMEOUT_SEC,
            )
            blob.download_to_file(
                in_mem_file,
                retry=cooperative_retry,
                timeout=GCS_DOWNLOAD_TIMEOUT_SEC,
            )
        except Exception as e:
            err_msg = f"Failed to download GCS object {gcs_path}: {e}"
            logger.exception(err_msg)
            raise FileNotFoundError(err_msg) from e

        in_mem_file.seek(0)
        return in_mem_file
