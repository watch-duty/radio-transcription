"""Helpers for validating collector source payloads."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from backend.pipeline.ingestion import failure_policy
from backend.pipeline.ingestion.collectors import failure_classification
from backend.pipeline.ingestion.collectors.failure_classification import (
    policy_evidence_for_status_reason,
)
from backend.pipeline.storage import feed_store


def extract_optional_item_list(
    payload: Mapping[str, Any],
    field: str,
    *,
    malformed_reason: str,
    failure_scope: failure_policy.FailureScope,
    endpoint_kind: failure_policy.EndpointKind,
) -> list[Any]:
    """Return an optional source item list or raise a bounded feed failure."""
    if not isinstance(payload, Mapping):
        raise failure_classification.collector_failure(
            feed_store.FeedStatusReason.SYSTEM_COLLECTOR_ERROR,
            malformed_reason,
            policy_evidence=policy_evidence_for_status_reason(
                feed_store.FeedStatusReason.SYSTEM_COLLECTOR_ERROR,
                failure_scope=failure_scope,
                endpoint_kind=endpoint_kind,
            ),
        )

    if field not in payload:
        return []

    value = payload[field]
    if isinstance(value, list):
        return value

    raise failure_classification.collector_failure(
        feed_store.FeedStatusReason.SYSTEM_COLLECTOR_ERROR,
        malformed_reason,
        policy_evidence=policy_evidence_for_status_reason(
            feed_store.FeedStatusReason.SYSTEM_COLLECTOR_ERROR,
            failure_scope=failure_scope,
            endpoint_kind=endpoint_kind,
        ),
    )
