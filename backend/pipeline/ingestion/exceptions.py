"""Typed exception hierarchy for feed-pipeline errors (Phase 2 foundation).

These exceptions allow the normalizer runtime to distinguish source-side
failures (which count toward failure_count / quarantine) from pipeline-side
failures (publish, GCS) which are our side and should never affect feed state.

Phase 2 is strictly additive — the bare ``except Exception`` in
``normalizer_runtime.py:536`` catches all subclasses unchanged until Phase 3
replaces it with the three-phase handler.
"""
from __future__ import annotations

__all__ = ["FeedError", "SourceError", "PipelineError"]


class FeedError(Exception):
    """Base class for typed feed-pipeline errors."""


class SourceError(FeedError):
    """Feed source failure — counts toward failure_count + quarantine.

    Used by source-specific collectors (bcfy_feeds, bcfy_calls, openmhz, echo)
    when the upstream source is unreachable, auth has expired, the catalog
    rejects our request, etc.

    Args:
        reason: Short snake_case tag identifying the failure mode
            (e.g. ``"auth_failed"``, ``"source_unreachable"``).
            Must be operator-actionable: a human reading it should
            immediately know whose problem it is.
    """

    def __init__(self, reason: str, *args: object, **kwargs: object) -> None:
        super().__init__(reason, *args, **kwargs)
        self.reason = reason


class PipelineError(FeedError):
    """Pipeline failure (publish, GCS, schema) — log only, never affects feed state.

    Used by gcp_helper.publish_audio_chunk and GCS staging helpers when our
    side of the pipeline misbehaves (Pub/Sub schema mismatch, paused ordering
    keys, GCS upload failures, etc.). The feed source is innocent.

    Args:
        reason: Short snake_case tag identifying the failure mode
            (e.g. ``"publish_schema_validation"``, ``"gcs_upload"``).
    """

    def __init__(self, reason: str, *args: object, **kwargs: object) -> None:
        super().__init__(reason, *args, **kwargs)
        self.reason = reason
