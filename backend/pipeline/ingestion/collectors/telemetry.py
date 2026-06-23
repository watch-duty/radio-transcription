"""Shared bounded collector telemetry helpers."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from backend.pipeline.ingestion.slo_contract import (
    EVENT_TYPE_CALL_DOWNLOAD_FAILED,
)

if TYPE_CHECKING:
    import logging


def emit_call_download_failed(
    logger: logging.Logger,
    *,
    feed_id: object,
    source_type: object,
) -> None:
    """Emit the call-download-failed SLO event with bounded fields only."""
    # SLO: call_download_failed emit
    logger.warning(
        "Call download failed",
        extra={
            "json_fields": _call_download_failed_json_fields(
                feed_id=feed_id,
                source_type=source_type,
            )
        },
    )


def _call_download_failed_json_fields(
    *,
    feed_id: object,
    source_type: object,
) -> dict[str, Any]:
    return {
        "event_type": EVENT_TYPE_CALL_DOWNLOAD_FAILED,
        "feed_id": str(feed_id),
        "source_type": source_type,
    }
