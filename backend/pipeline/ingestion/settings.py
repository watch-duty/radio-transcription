from __future__ import annotations

import os
import uuid
from dataclasses import dataclass, field

from backend.pipeline.storage.settings import AlloyDBSettings


def _require_env(name: str) -> str:
    """Return an environment variable or raise if unset."""
    value = os.environ.get(name)
    if not value:
        msg = f"Required environment variable {name} is not set"
        raise ValueError(msg)
    return value


@dataclass(frozen=True, kw_only=True)
class NormalizerSettings:
    """
    Configuration for the NormalizerRuntime, loaded from environment variables.

    All fields have sensible defaults except ``audio_staging_bucket`` and
    ``pubsub_topic_path`` which are required. AlloyDB connection parameters
    are loaded via ``AlloyDBSettings``.

    """

    # Worker identity
    worker_id: uuid.UUID = field(
        default_factory=lambda: (
            uuid.UUID(os.environ["WORKER_ID"])
            if "WORKER_ID" in os.environ
            else uuid.uuid4()
        ),
    )

    # Feed orchestration
    max_feeds_per_worker: int = field(
        default_factory=lambda: int(
            os.environ.get("MAX_FEEDS_PER_WORKER", "250"),
        ),
    )
    lease_poll_interval_sec: float = field(
        default_factory=lambda: float(
            os.environ.get("LEASE_POLL_INTERVAL_SEC", "5.0"),
        ),
    )
    heartbeat_interval_sec: float = field(
        default_factory=lambda: float(
            os.environ.get("HEARTBEAT_INTERVAL_SEC", "15.0"),
        ),
    )
    heartbeat_stall_timeout_sec: float = field(
        default_factory=lambda: float(
            os.environ.get("HEARTBEAT_STALL_TIMEOUT_SEC", "45.0"),
        ),
    )
    graceful_shutdown_timeout_sec: float = field(
        default_factory=lambda: float(
            os.environ.get("GRACEFUL_SHUTDOWN_TIMEOUT_SEC", "10.0"),
        ),
    )

    # GCS
    audio_staging_bucket: str = field(
        default_factory=lambda: _require_env("AUDIO_STAGING_BUCKET"),
    )

    # Pub/Sub
    # topic_path is in the form `projects/{project_id}/topics/{topic_id}`
    pubsub_topic_path: str = field(
        default_factory=lambda: _require_env("PUBSUB_TOPIC_PATH"),
    )

    # Database pool
    db: AlloyDBSettings = field(default_factory=AlloyDBSettings)

    # Feed lifecycle
    feed_failure_threshold: int = field(
        default_factory=lambda: int(
            os.environ.get("FEED_FAILURE_THRESHOLD", "3"),
        ),
    )
    abandonment_window_sec: float = field(
        default_factory=lambda: float(
            os.environ.get("ABANDONMENT_WINDOW_SEC", "60.0"),
        ),
    )

    # Retry: GCS uploads
    gcs_upload_max_retries: int = field(
        default_factory=lambda: int(
            os.environ.get("GCS_UPLOAD_MAX_RETRIES", "3"),
        ),
    )
    gcs_upload_retry_base_delay_sec: float = field(
        default_factory=lambda: float(
            os.environ.get("GCS_UPLOAD_RETRY_BASE_DELAY_SEC", "0.5"),
        ),
    )
    gcs_upload_retry_max_delay_sec: float = field(
        default_factory=lambda: float(
            os.environ.get("GCS_UPLOAD_RETRY_MAX_DELAY_SEC", "8.0"),
        ),
    )

    # Retry: bookmark (AlloyDB progress writes)
    bookmark_max_retries: int = field(
        default_factory=lambda: int(
            os.environ.get("BOOKMARK_MAX_RETRIES", "2"),
        ),
    )
    bookmark_retry_base_delay_sec: float = field(
        default_factory=lambda: float(
            os.environ.get("BOOKMARK_RETRY_BASE_DELAY_SEC", "0.5"),
        ),
    )
    bookmark_retry_max_delay_sec: float = field(
        default_factory=lambda: float(
            os.environ.get("BOOKMARK_RETRY_MAX_DELAY_SEC", "4.0"),
        ),
    )


