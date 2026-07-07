from __future__ import annotations

import os
import uuid
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from backend.pipeline.ingestion import source_runtime_specs
from backend.pipeline.storage.settings import AlloyDBSettings

if TYPE_CHECKING:
    from backend.pipeline.storage.feed_store import SourceType


def _require_env(name: str) -> str:
    """Return an environment variable or raise if unset."""
    value = os.environ.get(name)
    if not value:
        msg = f"Required environment variable {name} is not set"
        raise ValueError(msg)
    return value


# Per-type claim-budget defaults. The key set comes from SourceRuntimeSpec so
# adding a VM-claimable source updates caps, topic routing metadata, and URL
# metadata in one place. ECHO is intentionally absent: Echo feeds are served by
# a separate cloud function, not VM-leased.
_DEFAULT_CAPS: dict[SourceType, int] = source_runtime_specs.default_caps()


def _load_caps_from_env() -> dict[SourceType, int]:
    """Build per-type caps from CAP_<NAME> env vars, defaulting via _DEFAULT_CAPS.

    Caps are clamped to ``max(0, ...)`` so a misconfigured negative env
    var can't propagate into the SQL as a negative ``LIMIT`` (PostgreSQL
    rejects negative LIMITs and the resulting error would block every
    leasing cycle indefinitely). A clamp at this boundary keeps the
    corrupt-input failure mode local: the affected branch claims nothing
    until ops fixes the env var, but the worker keeps processing other
    types and stays healthy.
    """
    return {
        t: max(0, int(os.environ.get(f"CAP_{t.name}", str(default))))
        for t, default in _DEFAULT_CAPS.items()
    }


@dataclass(frozen=True, kw_only=True)
class CollectorSettings:
    """
    Configuration for the CollectorRuntime, loaded from environment variables.

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
            os.environ.get("MAX_FEEDS_PER_WORKER", "800"),
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
    # Clamped to >=1.0: a non-positive timeout makes
    # concurrent.futures.Future.result() raise TimeoutError immediately
    # on every heartbeat tick, which the heartbeat thread interprets as
    # an event-loop stall and triggers os._exit(1). Clamping at 1 second
    # converts the misconfig failure mode from "instant fleet-wide death
    # on first tick" into "every-cycle log noise" — recoverable.
    heartbeat_stall_timeout_sec: float = field(
        default_factory=lambda: max(
            1.0,
            float(os.environ.get("HEARTBEAT_STALL_TIMEOUT_SEC", "45.0")),
        ),
    )
    # Documented overall shutdown-budget envelope (NOT an enforced timeout).
    # Used by `__post_init__` to bound `task_cancel_budget_sec + 2s settle`
    # at config time. Real enforcement comes from the container `--stop-timeout`
    # and GCE 120s ACPI cap (HANDOFF-01); inside `_shutdown_sequence` we
    # deliberately do NOT wrap the sequence in `asyncio.wait_for(...)` because
    # cancelling mid-`release_feeds_batch` would leave leases stuck for the
    # 60s abandonment window — letting the OS reap at 120s is safer.
    graceful_shutdown_timeout_sec: float = field(
        default_factory=lambda: float(
            os.environ.get("GRACEFUL_SHUTDOWN_TIMEOUT_SEC", "90.0"),
        ),
    )
    # Sub-timeout for the inner `asyncio.wait` of feed tasks during
    # shutdown (SHUTDOWN-02). Default 30s leaves 90 - 30 - 2 = 58s for
    # release_feeds_batch + pool/client closes inside the outer
    # graceful_shutdown_timeout_sec budget. Pitfall 9: when the
    # sub-timeout fires, pending tasks are explicitly re-cancelled and
    # given a 2s settle window (hardcoded, NOT a setting) before
    # release_feeds_batch — see _shutdown_sequence.
    task_cancel_budget_sec: float = field(
        default_factory=lambda: float(
            os.environ.get("TASK_CANCEL_BUDGET_SEC", "30.0"),
        ),
    )
    # Per-type claim budget caps (scaling plan §4/§6). Worker passes
    # min(cap, cap - held, total_slack) as each CTE branch's LIMIT so
    # PostgreSQL enforces the cap structurally via the query planner.
    # Defaults + claimable type set live in module-level _DEFAULT_CAPS;
    # CAP_<NAME> env vars override individual entries.
    caps: dict[SourceType, int] = field(default_factory=_load_caps_from_env)

    # GCS
    audio_staging_bucket: str = field(
        default_factory=lambda: _require_env("AUDIO_STAGING_BUCKET"),
    )

    # Pub/Sub
    # topic_path is in the form `projects/{project_id}/topics/{topic_id}`
    continuous_pubsub_topic_path: str = field(
        default_factory=lambda: _require_env("CONTINUOUS_PUBSUB_TOPIC_PATH"),
    )
    segmented_pubsub_topic_path: str | None = field(
        default_factory=lambda: os.environ.get("SEGMENTED_PUBSUB_TOPIC_PATH"),
    )

    # Google Cloud project ID for telemetry metric emission (None disables metrics)
    google_cloud_project: str | None = field(
        default_factory=lambda: os.environ.get("GOOGLE_CLOUD_PROJECT"),
    )

    # Database pool
    db: AlloyDBSettings = field(default_factory=AlloyDBSettings)

    # Feed lifecycle
    feed_failure_threshold: int = field(
        default_factory=lambda: int(
            os.environ.get("FEED_FAILURE_THRESHOLD", "5"),
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

    # Retry: Pub/Sub publish
    pubsub_publish_max_retries: int = field(
        default_factory=lambda: int(
            os.environ.get("PUBSUB_PUBLISH_MAX_RETRIES", "2"),
        ),
    )
    pubsub_publish_retry_base_delay_sec: float = field(
        default_factory=lambda: float(
            os.environ.get("PUBSUB_PUBLISH_RETRY_BASE_DELAY_SEC", "0.5"),
        ),
    )
    pubsub_publish_retry_max_delay_sec: float = field(
        default_factory=lambda: float(
            os.environ.get("PUBSUB_PUBLISH_RETRY_MAX_DELAY_SEC", "4.0"),
        ),
    )

    # Health check (GET /healthz). HEALTH_CHECK_PORT exists for local-dev and
    # test flexibility only. DO NOT override this in production: the docker
    # -p mapping (cloud_config.yaml.tftpl) and google_compute_health_check
    # (container_mig) both hardcode 8080, so overriding the env var would
    # move the Python listener off 8080 while GCP keeps probing 8080 — every
    # probe would fail with connection-refused and the autohealer would
    # replace the entire fleet simultaneously.
    #
    # Heartbeat max age is computed inline in the /healthz handler as
    # 2 * heartbeat_interval_sec (spec) — no separate setting.
    health_check_port: int = field(
        default_factory=lambda: int(
            os.environ.get("HEALTH_CHECK_PORT", "8080"),
        ),
    )
    health_check_startup_grace_sec: float = field(
        default_factory=lambda: float(
            os.environ.get("HEALTH_CHECK_STARTUP_GRACE_SEC", "120.0"),
        ),
    )

    # Memory watchdog (WATCHDOG-01). See memory_watchdog.py for cgroup detection,
    # pause/resume hysteresis, and graceful-shutdown trip behavior.
    rss_watchdog_poll_interval_sec: float = field(
        default_factory=lambda: float(
            os.environ.get("RSS_WATCHDOG_POLL_INTERVAL_SEC", "2.0"),
        ),
    )
    rss_watchdog_pause_threshold: float = field(
        default_factory=lambda: float(
            os.environ.get("RSS_WATCHDOG_PAUSE_THRESHOLD", "0.70"),
        ),
    )
    rss_watchdog_exit_threshold: float = field(
        default_factory=lambda: float(
            os.environ.get("RSS_WATCHDOG_EXIT_THRESHOLD", "0.90"),
        ),
    )
    rss_watchdog_pause_consecutive_samples: int = field(
        default_factory=lambda: int(
            os.environ.get("RSS_WATCHDOG_PAUSE_CONSECUTIVE_SAMPLES", "3"),
        ),
    )
    rss_watchdog_exit_consecutive_samples: int = field(
        default_factory=lambda: int(
            os.environ.get("RSS_WATCHDOG_EXIT_CONSECUTIVE_SAMPLES", "3"),
        ),
    )
    rss_watchdog_warmup_sec: float = field(
        default_factory=lambda: float(
            os.environ.get("RSS_WATCHDOG_WARMUP_SEC", "60.0"),
        ),
    )
    segment_temp_dir: str | None = field(
        default_factory=lambda: os.environ.get("ICECAST_SEGMENT_DIR"),
    )

    def __post_init__(self) -> None:
        # SHUTDOWN-02: validate the inner sub-timeout fits inside the
        # outer graceful budget with the hardcoded 2s settle window.
        # Triggers at construction time (immediate, clear error) so an
        # operator misconfiguration surfaces before the runtime starts
        # rather than as a "shutdown takes too long" symptom under load.
        # Uses `>` (not `>=`): equality is allowed at the boundary
        # because release_feeds_batch + pool closes have additional
        # implicit slack inside graceful_shutdown_timeout_sec; this
        # validation only guards against overtly invalid configs (e.g.
        # operator sets TASK_CANCEL_BUDGET_SEC=120 with graceful=90).
        if (
            self.task_cancel_budget_sec + 2.0
            > self.graceful_shutdown_timeout_sec
        ):
            msg = (
                f"task_cancel_budget_sec ({self.task_cancel_budget_sec}s) "
                f"+ 2s settle exceeds graceful_shutdown_timeout_sec "
                f"({self.graceful_shutdown_timeout_sec}s); adjust "
                f"TASK_CANCEL_BUDGET_SEC or GRACEFUL_SHUTDOWN_TIMEOUT_SEC."
            )
            raise ValueError(msg)
