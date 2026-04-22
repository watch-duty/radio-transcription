"""Periodic reporter for the ``active_feed_count`` GAUGE custom metric.

Ships the Phase 3 METRIC-01 / METRIC-02 signal: a standalone asyncio task
spawned from ``NormalizerRuntime._main()`` (see plan 03-03) that publishes
one ``custom.googleapis.com/ingestion/active_feed_count`` GAUGE INT64 point
per tick (default 60 s) on the ``gce_instance`` monitored resource.

Module shape follows ``quarantine_telemetry.py`` verbatim (D-20 of
03-CONTEXT.md): module-level ``_client: MonitoringClient | None`` seeded by
``configure(project_id)``; never-raises discipline on the emit path; all log
sites use ``extra={"json_fields": {...}}`` per Phase 2's D-11.

Public surface (D-22):
    * :func:`configure` — set ``_client`` from a GCP project ID (or ``None``).
    * :func:`resolve_gce_resource_labels` — one-shot metadata-server probe;
      returns ``{project_id, instance_id, zone}`` on success, ``None`` on
      failure (reporter stays dormant).
    * :func:`reporter_loop` — periodic emit loop with injectable
      ``count_fn`` / ``sleep_fn`` / ``resource_labels`` / ``interval_sec``
      so tests drive it without ``NormalizerRuntime``.

Error policy (D-23):
    * Transient Cloud Monitoring errors (``ResourceExhausted``,
      ``DeadlineExceeded``, ``ServiceUnavailable``, ``Aborted``):
      ``logger.warning`` every tick, continue.
    * Permanent errors (``PermissionDenied``, ``InvalidArgument``,
      ``NotFound``, ``FailedPrecondition``): first occurrence per class
      emits ``logger.error`` + traceback; subsequent same-class emits
      ``logger.debug`` (effectively silent in prod). Module-level
      ``_seen_error_classes`` tracks the set.
    * Unknown ``Exception``: ``logger.warning`` (treated as transient).
    * Recovery INFO on first success after a non-empty
      ``_seen_error_classes`` — operator visibility that the fix worked.

Cardinality gate (Pitfall 1):
    Metric labels dict passed to ``write_time_series`` is ALWAYS empty
    ``{}``. ``instance_id`` and ``zone`` live on the resource labels (via
    the monitored resource), not the metric labels. ``feed_id`` and
    ``source_type`` are explicitly banned here — they would multiply the
    active-series count by 300x per Pitfall 1's fleet-scale math.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import aiohttp
from google.api_core.exceptions import (
    Aborted,
    DeadlineExceeded,
    FailedPrecondition,
    InvalidArgument,
    NotFound,
    PermissionDenied,
    ResourceExhausted,
    ServiceUnavailable,
)

from backend.pipeline.common.clients.monitoring_client import MonitoringClient
from backend.pipeline.ingestion.slo_contract import (
    METRIC_LABEL_ALLOWLIST,
    METRIC_TYPE_ACTIVE_FEED_COUNT,
    MONITORED_RESOURCE_TYPE,
)

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from backend.pipeline.ingestion.settings import NormalizerSettings

logger = logging.getLogger(__name__)

# Module-level singleton (D-20 mirror pattern — own client, own configure).
_client: MonitoringClient | None = None

# Tracks permanent-error exception classes already seen this process — used
# for D-23's once-per-process ERROR policy. Cleared on recovery (first
# successful write after non-empty set).
_seen_error_classes: set[type[BaseException]] = set()

# Metadata-server endpoints — require `Metadata-Flavor: Google` header.
_METADATA_BASE_URL = "http://metadata.google.internal/computeMetadata/v1"
_METADATA_HEADERS = {"Metadata-Flavor": "Google"}
_METADATA_TIMEOUT_SEC = 2.0  # per-request; no retries per 03-CONTEXT.md


def configure(google_cloud_project: str | None) -> None:
    """Set the GCP project ID for metric emission.

    Pass ``None`` (or an empty string) to disable emission — the reporter
    loop will run but skip writes. Mirrors :func:`quarantine_telemetry.configure`
    exactly (D-20 of 03-CONTEXT.md).
    """
    global _client  # noqa: PLW0603
    _client = (
        MonitoringClient(google_cloud_project) if google_cloud_project else None
    )


async def resolve_gce_resource_labels(
    settings: NormalizerSettings,
) -> dict[str, str] | None:
    """Probe the GCE metadata server for ``{project_id, instance_id, zone}``.

    Makes three sequential HTTP GETs to ``metadata.google.internal``:
    ``/project/project-id``, ``/instance/id``, ``/instance/zone`` — each with
    a 2 s timeout and no retries. On ANY failure (ConnectionError, TimeoutError,
    non-200, missing header), logs one WARNING and returns ``None``; the caller
    (plan 03-03's ``_main``) then leaves the reporter task un-spawned.

    The zone endpoint returns a path like ``projects/NNNN/zones/us-central1-a``;
    we strip to the bare zone name ``us-central1-a`` because Cloud Monitoring's
    ``gce_instance`` resource expects the unprefixed form.

    Args:
        settings: unused except for the ``google_cloud_project`` fallback if
            the metadata server's project response is empty. (Shouldn't happen
            on GCE but keeps the function robust for tests that mock partial
            responses.)

    Returns:
        ``{"project_id": str, "instance_id": str, "zone": str}`` on success,
        else ``None``.
    """
    try:
        timeout = aiohttp.ClientTimeout(total=_METADATA_TIMEOUT_SEC)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(
                f"{_METADATA_BASE_URL}/project/project-id",
                headers=_METADATA_HEADERS,
            ) as resp:
                resp.raise_for_status()
                project_id = (await resp.text()).strip()
            async with session.get(
                f"{_METADATA_BASE_URL}/instance/id",
                headers=_METADATA_HEADERS,
            ) as resp:
                resp.raise_for_status()
                instance_id = (await resp.text()).strip()
            async with session.get(
                f"{_METADATA_BASE_URL}/instance/zone",
                headers=_METADATA_HEADERS,
            ) as resp:
                resp.raise_for_status()
                zone_path = (await resp.text()).strip()
                # Raw form: "projects/NNNN/zones/us-central1-a" — strip prefix.
                zone = zone_path.rsplit("/", 1)[-1]
    except Exception as exc:
        logger.warning(
            "metric reporter: metadata server unreachable — reporter disabled",
            extra={
                "json_fields": {
                    "event": "metric_reporter_metadata_unreachable",
                    "exc_class": type(exc).__name__,
                },
            },
            exc_info=True,
        )
        return None

    # If project/project-id came back empty (unusual on GCE), fall back to the
    # settings value so resource.labels still parses Cloud Monitoring's
    # required-label check.
    if not project_id:
        project_id = settings.google_cloud_project or ""

    return {
        "project_id": project_id,
        "instance_id": instance_id,
        "zone": zone,
    }


async def reporter_loop(
    *,
    count_fn: Callable[[], int],
    resource_labels: dict[str, str],
    interval_sec: float,
    sleep_fn: Callable[[float], Awaitable[bool]],
) -> None:
    """Periodic GAUGE emit loop for ``active_feed_count``.

    Args:
        count_fn: zero-arg callable returning the current active count
            (``len(self._feed_tasks)`` when wired in plan 03-03).
        resource_labels: dict from :func:`resolve_gce_resource_labels`;
            passed verbatim as the Cloud Monitoring resource labels.
        interval_sec: sleep between ticks (``NormalizerSettings.metric_reporter_interval_sec``).
        sleep_fn: awaitable ``(seconds) -> bool`` — returns ``True`` when
            shutdown is signalled, ``False`` on natural timeout. Plan 03-03
            wires ``self._sleep_or_shutdown`` here.

    Runs until ``sleep_fn`` returns ``True``. Never raises — all exceptions
    from ``write_time_series`` are caught and logged per D-23's policy.

    Note: no ``global _seen_error_classes`` declaration needed — the set is
    mutated in-place via ``.add()`` / ``.clear()`` (never rebound).
    """
    while True:
        if await sleep_fn(interval_sec):
            return  # shutdown signalled
        if _client is None:
            # configure() wasn't called with a project ID — stay dormant.
            continue
        try:
            count = count_fn()
            # SLO: active_feed_count emit — periodic GAUGE tick
            await _client.write_time_series(
                metric_type=METRIC_TYPE_ACTIVE_FEED_COUNT,
                labels={},  # cardinality gate — see Pitfall 1 + METRIC_LABEL_ALLOWLIST
                value=count,
                resource_type=MONITORED_RESOURCE_TYPE,
                resource_labels=resource_labels,
            )
        except (
            ResourceExhausted,
            DeadlineExceeded,
            ServiceUnavailable,
            Aborted,
        ) as exc:
            logger.warning(
                "metric reporter: transient write failure",
                extra={
                    "json_fields": {
                        "event": "metric_reporter_transient_error",
                        "exc_class": type(exc).__name__,
                    },
                },
                exc_info=True,
            )
        except (
            PermissionDenied,
            InvalidArgument,
            NotFound,
            FailedPrecondition,
        ) as exc:
            cls = type(exc)
            if cls in _seen_error_classes:
                logger.debug(
                    "metric reporter: recurring permanent error %s",
                    cls.__name__,
                )
            else:
                _seen_error_classes.add(cls)
                logger.exception(
                    "metric reporter: permanent write failure",
                    extra={
                        "json_fields": {
                            "event": "metric_reporter_permanent_error",
                            "exc_class": cls.__name__,
                        },
                    },
                )
        except Exception as exc:
            logger.warning(
                "metric reporter: unknown write failure",
                extra={
                    "json_fields": {
                        "event": "metric_reporter_unknown_error",
                        "exc_class": type(exc).__name__,
                    },
                },
                exc_info=True,
            )
        else:
            # Successful write. Emit recovery INFO if we had previously logged
            # one or more permanent errors — operator signal that the fix
            # worked. Then clear the set so the next streak logs ERROR again.
            if _seen_error_classes:
                previously_seen = sorted(
                    cls.__name__ for cls in _seen_error_classes
                )
                logger.info(
                    "metric reporter recovered",
                    extra={
                        "json_fields": {
                            "event": "metric_reporter_recovered",
                            "previously_seen": previously_seen,
                        },
                    },
                )
                _seen_error_classes.clear()


# METRIC_LABEL_ALLOWLIST is imported but deliberately unused at runtime — the
# metric `labels` dict is hardcoded empty `{}` above, which is a strictly
# stronger cardinality gate than checking against the allowlist at emit time.
# Keep the import so test_metric_reporter.py can assert at the module level
# that the allowlist is part of the contract (and so grep -r
# METRIC_LABEL_ALLOWLIST surfaces both the consumer and the constants module).
_: frozenset[str] = METRIC_LABEL_ALLOWLIST
