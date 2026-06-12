"""Helpers for validating source polling payload item lists."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from backend.pipeline.ingestion.collectors import failure_classification
from backend.pipeline.storage import feed_store


def extract_optional_item_list(
    payload: Mapping[str, Any],
    field: str,
    *,
    malformed_reason: str,
) -> list[Any]:
    """Return an optional source item list or raise a bounded feed failure."""
    if not isinstance(payload, Mapping):
        raise failure_classification.collector_failure(
            feed_store.FeedStatusReason.SYSTEM_COLLECTOR_ERROR,
            malformed_reason,
        )

    if field not in payload:
        return []

    value = payload[field]
    if isinstance(value, list):
        return value

    raise failure_classification.collector_failure(
        feed_store.FeedStatusReason.SYSTEM_COLLECTOR_ERROR,
        malformed_reason,
    )
