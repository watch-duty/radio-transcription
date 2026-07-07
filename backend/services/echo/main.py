from __future__ import annotations

import logging
import os
from typing import TYPE_CHECKING, Any

import functions_framework
from google.cloud import storage

from backend.pipeline.common.container_helper import ForkDetector, fork_checked
from backend.services.echo.storage import EchoStorageBackend

if TYPE_CHECKING:
    import flask

__all__ = ["verify_directory"]

logger = logging.getLogger(__name__)


class EchoServiceContainer:
    """Encapsulates the warm-started cached service container instances
    for GCF.
    """

    def __init__(self) -> None:
        self._fork_detector = ForkDetector(self.reset_clients)
        self._storage_client: storage.Client | None = None
        self._echo_backend: EchoStorageBackend | None = None

    def reset_clients(self) -> None:
        self._storage_client = None
        self._echo_backend = None

    @fork_checked
    def get_storage_client(self) -> storage.Client:
        """Warms up and caches the Storage Client instance."""
        if self._storage_client is None:
            logger.info("Initializing Google Cloud Storage Client")
            self._storage_client = storage.Client()
        return self._storage_client

    @property
    def bucket_name(self) -> str:
        name = os.environ.get("ECHO_RECORDINGS_BUCKET")
        if not name or not name.strip():
            msg = "ECHO_RECORDINGS_BUCKET environment variable is not set."
            logger.error(msg)
            raise ValueError(msg)
        return name.strip()

    @fork_checked
    def get_echo_backend(self) -> EchoStorageBackend:
        """Warms up and caches the EchoStorageBackend instance."""
        if self._echo_backend is None:
            logger.info("Initializing EchoStorageBackend")
            self._echo_backend = EchoStorageBackend(
                self.get_storage_client(), self.bucket_name
            )
        return self._echo_backend

    def eager_warmup(self) -> None:
        """Eagerly warms up and caches all clients during container
        initialization.
        """
        logger.info("Performing eager warm-start for container services...")
        try:
            self.get_echo_backend()
            logger.info("Container services eagerly warmed up successfully.")
        except Exception as e:
            logger.warning(
                "Eager warm-start skipped or failed "
                "(expected in some test/local envs): %s",
                e,
            )


# Global container instance managed by the GCF container instance lifecycle
container = EchoServiceContainer()
container.eager_warmup()


@functions_framework.http
def verify_directory(request: flask.Request) -> tuple[dict[str, Any], int]:
    """HTTP Cloud Function to verify Echo GCS directory."""
    try:
        request_json = request.get_json(silent=True)
        if not request_json or "source_feed_id" not in request_json:
            return ({"error": "Missing source_feed_id in request"}, 400)

        source_feed_id = request_json["source_feed_id"]

        try:
            backend = container.get_echo_backend()
        except ValueError as e:
            return ({"error": str(e)}, 500)

        exists = backend.verify_directory_exists(source_feed_id)
    except Exception as e:
        logger.exception("Error verifying directory")
        return ({"error": str(e)}, 500)
    else:
        return ({"exists": exists}, 200)
