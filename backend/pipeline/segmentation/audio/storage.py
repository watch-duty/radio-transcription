"""Storage client abstractions for downloading audio bitstreams from Google Cloud Storage."""

import io
import logging
import urllib.parse
from collections.abc import Callable
from typing import Any

import google.auth
import requests.adapters
import requests.exceptions
from apache_beam.utils.shared import Shared
from google.api_core import exceptions as api_exceptions
from google.auth.credentials import AnonymousCredentials
from google.auth.exceptions import DefaultCredentialsError
from google.auth.transport.requests import AuthorizedSession
from google.cloud import storage
from google.cloud.storage.retry import DEFAULT_RETRY

from backend.pipeline.segmentation.constants import (
    GCS_CONNECTION_MAX_RETRIES,
    GCS_CONNECTION_POOL_SIZE,
    GCS_DOWNLOAD_TIMEOUT_SEC,
)

logger = logging.getLogger(__name__)


def get_gcs_client(project_id: str | None = None) -> storage.Client:
    """Universal GCS Client factory configured with our authoritative connection pooling and resilient ADC CI fallbacks.

    Completely consolidates all HTTPAdapter mounting logic into this single, absolute source of truth.

    Note: This is a synchronous GCS client required for Dataflow's multi-threaded worker model.
    We cannot use the common async GcsClient (common/clients/gcs_client.py) here, as managing
    asynchronous resources inside synchronous Beam DoFns adds significant overhead and complexity.
    """
    try:
        credentials, project = google.auth.default()
        authed_session = AuthorizedSession(credentials)

        adapter = requests.adapters.HTTPAdapter(
            pool_connections=GCS_CONNECTION_POOL_SIZE,
            pool_maxsize=GCS_CONNECTION_POOL_SIZE,
            max_retries=GCS_CONNECTION_MAX_RETRIES,
        )
        authed_session.mount("https://", adapter)
        return storage.Client(
            project=project_id or project,
            credentials=credentials,
            _http=authed_session,
        )
    except DefaultCredentialsError:
        # Fallback for un-authenticated remote testing sandboxes (e.g., GitHub Actions CI)
        return storage.Client(
            project=project_id or "test-project",
            credentials=AnonymousCredentials(),
        )


UNIVERSAL_GCS_SHARED_HANDLE = Shared()


def acquire_shared_gcs_client(
    project_id: str | None = None,
    shared_handle: Shared | None = None,
) -> storage.Client:
    """Acquires a process-wide thread-safe storage.Client singleton using Apache Beam's Shared resource tracking.

    Ensures that all DoFn threads running inside the exact same physical Python worker VM share precisely
    one AuthorizedSession HTTP adapter connection pool, completely eliminating TCP socket exhaustion.
    """
    handle = shared_handle or UNIVERSAL_GCS_SHARED_HANDLE

    def _construct() -> storage.Client:
        return get_gcs_client(project_id=project_id)

    # Use a highly specific deterministic tag to avoid conflicting with other user shared assets
    return handle.acquire(
        _construct, tag=f"gcs_client_singleton_{project_id or 'default'}"
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
        self.gcs_factory = gcs_factory or acquire_shared_gcs_client
        # Flawlessly inherit official GCS idempotent retry predicates while enforcing our custom 30s timeout
        self._retry = DEFAULT_RETRY.with_timeout(GCS_DOWNLOAD_TIMEOUT_SEC)

    def setup(self) -> None:
        """Lazily initializes the GCS client via our universal pooled factory."""
        if self.client is None:
            self.client = self.gcs_factory()

    def download_audio_to_memory(self, gcs_path: str) -> io.BytesIO:
        """Downloads an audio blob from GCS into a fully self-contained in-memory BytesIO bitstream."""
        if not self.client:
            msg = "GCS client not initialized. Call setup() first."
            raise RuntimeError(msg)

        parsed_uri = urllib.parse.urlparse(gcs_path)
        bucket_name = parsed_uri.netloc
        blob_name = parsed_uri.path.lstrip("/")

        bucket = self.client.bucket(bucket_name)

        blob = storage.Blob(blob_name, bucket)

        in_mem_file = io.BytesIO()
        try:
            blob.download_to_file(
                in_mem_file,
                retry=self._retry,
                timeout=GCS_DOWNLOAD_TIMEOUT_SEC,
            )
        except (api_exceptions.NotFound, FileNotFoundError) as e:
            err_msg = f"GCS object not found: {gcs_path}"
            logger.exception(err_msg)
            raise FileNotFoundError(err_msg) from e

        in_mem_file.seek(0)
        return in_mem_file
