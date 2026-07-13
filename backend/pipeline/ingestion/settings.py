from __future__ import annotations

import math
import os
import typing
import uuid
from dataclasses import dataclass, field
from types import MappingProxyType

from backend.pipeline.ingestion import (
    grant_control,
    source_runtime_specs,
    worker_profiles,
)
from backend.pipeline.storage import feed_store
from backend.pipeline.storage.settings import AlloyDBSettings

if typing.TYPE_CHECKING:
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


class _UnsetCaps(dict[feed_store.SourceType, int]):
    """Private sentinel distinguishing omitted caps from explicit emptiness."""


class _UnsetTopic(str):
    """Private sentinel distinguishing omitted topics from empty values."""

    __slots__ = ()


_CAPS_UNSET = _UnsetCaps()
_TOPIC_UNSET = _UnsetTopic("")


def _require_finite_range(
    field_name: str,
    value: float,
    *,
    minimum: float,
    inclusive: bool,
) -> None:
    """Validate one finite float against its operational lower bound."""
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        msg = f"{field_name} must be a number"
        raise TypeError(msg)
    below_minimum = value < minimum if inclusive else value <= minimum
    if not math.isfinite(value) or below_minimum:
        comparison = "at least" if inclusive else "greater than"
        msg = (
            f"{field_name} ({value}) must be finite and {comparison} {minimum}"
        )
        raise ValueError(msg)


def _require_integer_range(
    field_name: str,
    value: int,
    *,
    minimum: int,
) -> None:
    """Validate one integer against its inclusive operational minimum."""
    if not isinstance(value, int) or isinstance(value, bool):
        msg = f"{field_name} must be an integer"
        raise TypeError(msg)
    if value < minimum:
        msg = f"{field_name} ({value}) must be at least {minimum}"
        raise ValueError(msg)


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


def load_worker_profile_from_env() -> worker_profiles.WorkerProfile:
    """Resolve the closed worker profile and allocation env exactly once."""
    selector = os.environ.get("WORKER_PROFILE")
    template = (
        worker_profiles.LEGACY_PROFILE
        if selector is None
        else worker_profiles.WORKER_PROFILE_PRESETS.get(selector)
    )
    feed_selected = template is not None and (
        worker_profiles.allocation_for_domain(
            template,
            grant_control.DomainId.FEED,
        )
        is not None
    )
    sid_selected = template is not None and (
        worker_profiles.allocation_for_domain(
            template,
            grant_control.DomainId.SID,
        )
        is not None
    )
    return worker_profiles.resolve_worker_profile(
        selector,
        feed_owned_cap=(
            int(os.environ.get("MAX_FEEDS_PER_WORKER", "800"))
            if feed_selected
            else 800
        ),
        feed_claims_per_cycle=(
            int(os.environ.get("LEASE_ADMISSION_CYCLE_BUDGET", "20"))
            if feed_selected
            else 20
        ),
        sid_owned_cap=(
            int(os.environ.get("MAX_SIDS_PER_WORKER", "32"))
            if sid_selected
            else 32
        ),
        sid_claims_per_cycle=(
            int(os.environ.get("SID_LEASE_ADMISSION_CYCLE_BUDGET", "2"))
            if sid_selected
            else 2
        ),
    )


def _load_bcfy_calls_authority_mode_from_env() -> (
    worker_profiles.BcfyCallsAuthorityMode
):
    """Read the exact closed Calls authority mode once at construction."""
    value = os.environ.get("BCFY_CALLS_AUTHORITY_MODE")
    if value is None:
        return worker_profiles.BcfyCallsAuthorityMode.LEGACY_FEED
    try:
        return worker_profiles.BcfyCallsAuthorityMode(value)
    except ValueError as exc:
        msg = (
            "BCFY_CALLS_AUTHORITY_MODE must be exactly "
            "'legacy_feed' or 'sid_lease'"
        )
        raise ValueError(msg) from exc


@dataclass(frozen=True, kw_only=True)
class CollectorSettings:
    """
    Configuration for the CollectorRuntime, loaded from environment variables.

    All fields have sensible defaults except ``audio_staging_bucket`` and
    ``pubsub_topic_path`` which are required. AlloyDB connection parameters
    are loaded via ``AlloyDBSettings``.

    """

    # Immutable process topology, resolved before every other default factory.
    worker_profile: worker_profiles.WorkerProfile = field(
        default_factory=load_worker_profile_from_env,
    )
    bcfy_calls_authority_mode: worker_profiles.BcfyCallsAuthorityMode = field(
        default_factory=_load_bcfy_calls_authority_mode_from_env,
    )

    # Worker identity
    worker_id: uuid.UUID = field(
        default_factory=uuid.uuid4,
    )

    # Feed orchestration
    lease_poll_interval_sec: float = field(
        default_factory=lambda: float(
            os.environ.get("LEASE_POLL_INTERVAL_SEC", "5.0"),
        ),
    )
    startup_stagger_max_sec: float = field(
        default_factory=lambda: float(
            os.environ.get("STARTUP_STAGGER_MAX_SEC", "60.0"),
        ),
    )
    startup_jitter_max_sec: float = field(
        default_factory=lambda: float(
            os.environ.get("STARTUP_JITTER_MAX_SEC", "2.0"),
        ),
    )
    lease_poll_jitter_max_sec: float = field(
        default_factory=lambda: float(
            os.environ.get("LEASE_POLL_JITTER_MAX_SEC", "1.0"),
        ),
    )
    worker_index: int | None = field(
        default_factory=lambda: (
            int(os.environ["WORKER_INDEX"])
            if "WORKER_INDEX" in os.environ
            else None
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
    caps: dict[SourceType, int] = field(
        default_factory=lambda: _CAPS_UNSET,
    )
    feed_claim_caps: typing.Mapping[SourceType, int] = field(init=False)

    # GCS
    audio_staging_bucket: str = field(
        default_factory=lambda: _require_env("AUDIO_STAGING_BUCKET"),
    )

    # Pub/Sub
    # topic_path is in the form `projects/{project_id}/topics/{topic_id}`
    continuous_pubsub_topic_path: str = _TOPIC_UNSET
    segmented_pubsub_topic_path: str | None = _TOPIC_UNSET

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

    @property
    def max_feeds_per_worker(self) -> int:
        """Return the selected Feed allocation cap, or zero if unselected."""
        allocation = worker_profiles.allocation_for_domain(
            self.worker_profile,
            grant_control.DomainId.FEED,
        )
        return allocation.owned_cap if allocation is not None else 0

    @property
    def lease_admission_cycle_budget(self) -> int:
        """Return the selected Feed claim budget, or zero if unselected."""
        allocation = worker_profiles.allocation_for_domain(
            self.worker_profile,
            grant_control.DomainId.FEED,
        )
        return allocation.claims_per_cycle if allocation is not None else 0

    def __post_init__(self) -> None:
        if "WORKER_ID" in os.environ:
            msg = (
                "WORKER_ID must not be supplied; worker identity is generated "
                "for each process epoch"
            )
            raise ValueError(msg)
        object.__setattr__(
            self,
            "worker_profile",
            worker_profiles.derive_bcfy_calls_authority(
                self.worker_profile,
                self.bcfy_calls_authority_mode,
            ),
        )
        feed_allocation = worker_profiles.allocation_for_domain(
            self.worker_profile,
            grant_control.DomainId.FEED,
        )
        sid_allocation = worker_profiles.allocation_for_domain(
            self.worker_profile,
            grant_control.DomainId.SID,
        )
        if self.caps is _CAPS_UNSET:
            object.__setattr__(
                self,
                "caps",
                (
                    _load_caps_from_env()
                    if feed_allocation is not None
                    else dict(_DEFAULT_CAPS)
                ),
            )
        claim_caps = dict(self.caps) if feed_allocation is not None else {}
        if (
            self.bcfy_calls_authority_mode
            is worker_profiles.BcfyCallsAuthorityMode.SID_LEASE
        ):
            claim_caps.pop(feed_store.SourceType.BCFY_CALLS, None)
        object.__setattr__(
            self,
            "feed_claim_caps",
            MappingProxyType(claim_caps),
        )
        if self.continuous_pubsub_topic_path is _TOPIC_UNSET:
            if feed_allocation is not None:
                object.__setattr__(
                    self,
                    "continuous_pubsub_topic_path",
                    _require_env("CONTINUOUS_PUBSUB_TOPIC_PATH"),
                )
            else:
                object.__setattr__(self, "continuous_pubsub_topic_path", "")
        if self.segmented_pubsub_topic_path is _TOPIC_UNSET:
            if feed_allocation is not None or sid_allocation is not None:
                object.__setattr__(
                    self,
                    "segmented_pubsub_topic_path",
                    os.environ.get("SEGMENTED_PUBSUB_TOPIC_PATH"),
                )
            else:
                object.__setattr__(self, "segmented_pubsub_topic_path", None)
        self._validate_numeric_settings()

    def _validate_numeric_settings(self) -> None:
        """Reject unsafe numeric configuration before resource creation."""
        positive_intervals = (
            ("lease_poll_interval_sec", self.lease_poll_interval_sec),
            ("heartbeat_interval_sec", self.heartbeat_interval_sec),
            (
                "heartbeat_stall_timeout_sec",
                self.heartbeat_stall_timeout_sec,
            ),
            (
                "graceful_shutdown_timeout_sec",
                self.graceful_shutdown_timeout_sec,
            ),
            ("abandonment_window_sec", self.abandonment_window_sec),
            (
                "rss_watchdog_poll_interval_sec",
                self.rss_watchdog_poll_interval_sec,
            ),
        )
        for field_name, value in positive_intervals:
            _require_finite_range(
                field_name,
                value,
                minimum=0.0,
                inclusive=False,
            )

        non_negative_delays = (
            ("startup_stagger_max_sec", self.startup_stagger_max_sec),
            ("startup_jitter_max_sec", self.startup_jitter_max_sec),
            ("lease_poll_jitter_max_sec", self.lease_poll_jitter_max_sec),
            ("task_cancel_budget_sec", self.task_cancel_budget_sec),
            (
                "gcs_upload_retry_base_delay_sec",
                self.gcs_upload_retry_base_delay_sec,
            ),
            (
                "gcs_upload_retry_max_delay_sec",
                self.gcs_upload_retry_max_delay_sec,
            ),
            (
                "bookmark_retry_base_delay_sec",
                self.bookmark_retry_base_delay_sec,
            ),
            (
                "bookmark_retry_max_delay_sec",
                self.bookmark_retry_max_delay_sec,
            ),
            (
                "pubsub_publish_retry_base_delay_sec",
                self.pubsub_publish_retry_base_delay_sec,
            ),
            (
                "pubsub_publish_retry_max_delay_sec",
                self.pubsub_publish_retry_max_delay_sec,
            ),
            (
                "health_check_startup_grace_sec",
                self.health_check_startup_grace_sec,
            ),
            ("rss_watchdog_warmup_sec", self.rss_watchdog_warmup_sec),
        )
        for field_name, value in non_negative_delays:
            _require_finite_range(
                field_name,
                value,
                minimum=0.0,
                inclusive=True,
            )

        positive_integers = (
            ("feed_failure_threshold", self.feed_failure_threshold),
            (
                "rss_watchdog_pause_consecutive_samples",
                self.rss_watchdog_pause_consecutive_samples,
            ),
            (
                "rss_watchdog_exit_consecutive_samples",
                self.rss_watchdog_exit_consecutive_samples,
            ),
        )
        for field_name, value in positive_integers:
            _require_integer_range(field_name, value, minimum=1)

        retry_counts = (
            ("gcs_upload_max_retries", self.gcs_upload_max_retries),
            ("bookmark_max_retries", self.bookmark_max_retries),
            ("pubsub_publish_max_retries", self.pubsub_publish_max_retries),
        )
        for field_name, value in retry_counts:
            _require_integer_range(field_name, value, minimum=0)

        if self.worker_index is not None:
            _require_integer_range("worker_index", self.worker_index, minimum=0)
        if not 1 <= self.health_check_port <= 65535:
            msg = "health_check_port must be between 1 and 65535"
            raise ValueError(msg)

        for field_name, value in (
            ("rss_watchdog_pause_threshold", self.rss_watchdog_pause_threshold),
            ("rss_watchdog_exit_threshold", self.rss_watchdog_exit_threshold),
        ):
            _require_finite_range(
                field_name,
                value,
                minimum=0.0,
                inclusive=False,
            )
            if value > 1.0:
                msg = f"{field_name} ({value}) must not exceed 1.0"
                raise ValueError(msg)
        if (
            self.rss_watchdog_pause_threshold
            >= self.rss_watchdog_exit_threshold
        ):
            msg = (
                "rss_watchdog_pause_threshold must be less than "
                "rss_watchdog_exit_threshold"
            )
            raise ValueError(msg)

        retry_delays = (
            (
                "gcs_upload_retry_base_delay_sec",
                self.gcs_upload_retry_base_delay_sec,
                "gcs_upload_retry_max_delay_sec",
                self.gcs_upload_retry_max_delay_sec,
            ),
            (
                "bookmark_retry_base_delay_sec",
                self.bookmark_retry_base_delay_sec,
                "bookmark_retry_max_delay_sec",
                self.bookmark_retry_max_delay_sec,
            ),
            (
                "pubsub_publish_retry_base_delay_sec",
                self.pubsub_publish_retry_base_delay_sec,
                "pubsub_publish_retry_max_delay_sec",
                self.pubsub_publish_retry_max_delay_sec,
            ),
        )
        for base_name, base_delay, max_name, max_delay in retry_delays:
            if base_delay > max_delay:
                msg = f"{base_name} must not exceed {max_name}"
                raise ValueError(msg)

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
