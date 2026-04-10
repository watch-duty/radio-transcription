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

logger = logging.getLogger(__name__)

_METRIC_TYPE = "custom.googleapis.com/feeds/quarantine_events"

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
) -> None:
    """Emit a quarantine event signal.  **Never raises.**

    1. Structured ERROR log (always, even when metrics are disabled).
    2. Cloud Monitoring GAUGE metric (only when *configure* was called
       with a project ID).
    """
    try:
        logger.error(
            "Feed quarantined",
            extra={
                "event_type": "feed_quarantined",
                "feed_id": feed_id,
                "feed_name": feed_name,
                "source_type": source_type,
            },
        )
    except Exception:  # noqa: S110
        pass

    if _client is None:
        return

    try:
        await _client.write_time_series(
            metric_type=_METRIC_TYPE,
            labels={
                "feed_id": feed_id,
                "feed_name": feed_name,
                "source_type": source_type,
            },
            value=1,
        )
    except Exception:
        try:
            logger.warning("Failed to emit quarantine metric", exc_info=True)
        except Exception:  # noqa: S110
            pass
