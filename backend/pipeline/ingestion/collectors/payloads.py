"""Helpers for validating collector source payloads."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from backend.pipeline.ingestion.collectors import failure_classification
from backend.pipeline.storage import feed_store

if TYPE_CHECKING:
    from collections.abc import Mapping


def extract_optional_item_list(
    payload: Mapping[str, Any],
    field: str,
    *,
    malformed_reason: str,
) -> list[Any]:
    """Return an optional source item list or raise a bounded feed failure."""
    if field not in payload:
        return []

    value = payload[field]
    if isinstance(value, list):
        return value

    raise failure_classification.collector_failure(
        feed_store.FeedStatusReason.SYSTEM_SOURCE_PAYLOAD_INVALID,
        malformed_reason,
    )
