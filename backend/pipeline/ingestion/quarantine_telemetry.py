"""Telemetry for feed quarantine events.

Emits a structured log and an optional Cloud Monitoring custom metric
each time a feed transitions to ``quarantined`` status.

Call :func:`configure` once at runtime startup to enable metric
emission.  When *google_cloud_project* is ``None`` (the default), only the
structured log is emitted — no GCP API calls are made.
"""

from __future__ import annotations

import logging

from backend.pipeline.common.clients.monitoring_client import MonitoringClient
from backend.pipeline.ingestion.slo_contract import (
    EVENT_TYPE_FEED_QUARANTINED,
    METRIC_TYPE_QUARANTINE_EVENTS,
)

logger = logging.getLogger(__name__)

_client: MonitoringClient | None = None


def configure(google_cloud_project: str | None) -> None:
    """Set the GCP project for metric emission.

    Pass ``None`` to disable metric emission (structured log is still
    emitted).  The underlying :class:`MonitoringClient` is created here
    but the gRPC channel is not opened until the first metric write.
    """
    global _client  # noqa: PLW0603
    _client = (
        MonitoringClient(google_cloud_project) if google_cloud_project else None
    )


async def emit_quarantine_event(
    feed_id: str,
    feed_name: str,
    source_type: str,
    reason: str,
    status_reason: str,
    profile: str,
    profile_digest: str,
    domain_id: str,
    authority_kind: str,
) -> None:
    """Emit a quarantine event signal.  **Never raises.**

    1. Structured ERROR log (always, even when metrics are disabled).
       ``reason`` is interpolated into the message and included as
       ``json_fields.reason`` so on-callers see the failure cause in the
       Logs Explorer summary row without expanding the payload.
    2. Cloud Monitoring GAUGE metric (only when *configure* was called
       with a project ID). Labels are limited to immutable profile/domain
       context plus bounded source type; Feed identity, status, and reason
       remain log-only to avoid per-authority time-series cardinality.
    """
    try:
        logger.error(
            "Feed quarantined: %s",
            reason,
            extra={
                "json_fields": {
                    "event_type": EVENT_TYPE_FEED_QUARANTINED,
                    "feed_id": feed_id,
                    "feed_name": feed_name,
                    "reason": reason,
                    "status_reason": status_reason,
                    "source_type": source_type,
                    "profile": profile,
                    "profile_digest": profile_digest,
                    "domain_id": domain_id,
                    "authority_kind": authority_kind,
                },
            },
        )
    except Exception:  # noqa: S110
        pass

    if _client is None:
        return

    try:
        # Cardinality: only immutable process/domain dimensions and the
        # bounded SourceType vocabulary become metric labels. Feed identity,
        # status, and diagnostic reasons remain structured-log context only.
        await _client.write_time_series(
            metric_type=METRIC_TYPE_QUARANTINE_EVENTS,
            labels={
                "profile": profile,
                "profile_digest": profile_digest,
                "domain_id": domain_id,
                "authority_kind": authority_kind,
                "source_type": source_type,
            },
            value=1,
            resource_labels={"project_id": _client.project_id},
        )
    except Exception:
        try:
            logger.warning("Failed to emit quarantine metric", exc_info=True)
        except Exception:  # noqa: S110
            pass
