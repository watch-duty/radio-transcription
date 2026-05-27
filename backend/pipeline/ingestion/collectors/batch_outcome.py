"""Shared collector batch outcome telemetry helpers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from backend.pipeline.ingestion.slo_contract import (
    EVENT_TYPE_BATCH_UNPRODUCTIVE,
)

if TYPE_CHECKING:
    import logging
    from collections.abc import Mapping


@dataclass
class BatchOutcome:
    """Counts describing a collector batch's productive output."""

    attempted: int = 0
    produced: int = 0

    def record_item_attempt(self) -> None:
        self.attempted += 1

    def record_chunk_produced(self) -> None:
        self.produced += 1

    @property
    def is_unproductive(self) -> bool:
        return self.attempted > 0 and self.produced == 0


def emit_batch_unproductive(
    logger: logging.Logger,
    feed: Mapping[str, Any],
    outcome: BatchOutcome,
    *,
    reason: str,
) -> None:
    """Emit the shared unproductive-batch evidence event when applicable."""
    if not outcome.is_unproductive:
        return

    logger.warning(
        "Batch unproductive",
        extra={
            "json_fields": {
                "event_type": EVENT_TYPE_BATCH_UNPRODUCTIVE,
                "feed_id": str(feed["id"]),
                "source_type": str(feed["source_type"]),
                "attempted": outcome.attempted,
                "produced": outcome.produced,
                "reason": reason,
            },
        },
    )
