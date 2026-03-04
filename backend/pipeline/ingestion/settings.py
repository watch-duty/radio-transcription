from __future__ import annotations

import os
import uuid
from dataclasses import dataclass, field


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

    All fields have sensible defaults except those marked as required
    (``final_staging_bucket`` and AlloyDB connection parameters).

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
    final_staging_bucket: str = field(
        default_factory=lambda: _require_env("FINAL_STAGING_BUCKET"),
    )

    # Database pool
    db_pool_min_size: int = field(
        default_factory=lambda: int(os.environ.get("DB_POOL_MIN_SIZE", "10")),
    )
    db_pool_max_size: int = field(
        default_factory=lambda: int(os.environ.get("DB_POOL_MAX_SIZE", "10")),
    )

    # Timeouts
    db_command_timeout_sec: float = field(
        default_factory=lambda: float(
            os.environ.get("DB_COMMAND_TIMEOUT_SEC", "30.0"),
        ),
    )
    db_connect_timeout_sec: float = field(
        default_factory=lambda: float(
            os.environ.get("DB_CONNECT_TIMEOUT_SEC", "10.0"),
        ),
    )

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

    # AlloyDB connection
    db_host: str = field(
        default_factory=lambda: _require_env("ALLOYDB_HOST"),
    )
    db_port: int = field(
        default_factory=lambda: int(os.environ.get("ALLOYDB_PORT", "5432")),
    )
    db_user: str = field(
        default_factory=lambda: _require_env("ALLOYDB_USER"),
    )
    db_name: str = field(
        default_factory=lambda: _require_env("ALLOYDB_DB"),
    )
    db_password: str = field(
        default_factory=lambda: os.environ.get("ALLOYDB_PASSWORD", ""),
    )
