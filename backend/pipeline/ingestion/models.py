"""Data models and contract for the ingestion pipeline.

Runtime / Capture Contract
==========================

The ``yield`` is the contract boundary.

**Runtime owns** (capture must never do these):
GCS upload, Pub/Sub publish, bookmarks, failure counting,
quarantine, heartbeats, lease release, timeouts.

**Capture owns:**
Connections, reconnection, retries, rate limits, data validation,
shutdown checks, session_id generation. The capture function must
appear as an infinite async generator — transport disconnections
are internal events the runtime never sees.

Error Handling
--------------

Collectors should keep source-specific failure classification at the collector
boundary. The runtime can tell "collector raised a typed feed failure" from
"pipeline failed after capture" and "unexpected bug", but it cannot safely
infer Broadcastify/OpenMHZ/Fire Notifications semantics from raw strings.

For any error inside a capture function, follow this priority:

1. **Configuration** (bad creds, wrong URL): raise ``FeedFailure``.
2. **Rate limit** (429): back off internally first. Raise
   ``FeedFailure`` only if collector policy says the feed is
   persistently rate-limited.
3. **Transient** (500, timeout, disconnect): retry internally.
   Skip the item if retries exhaust. Raise ``FeedFailure`` if the
   connection fails persistently (e.g. 10 consecutive transport failures).
4. **Item failure** (one download 404, corrupt data): skip, log,
   continue. Raise only if ALL eligible items in one observation boundary
   fail (systemic).
5. **Data quality** (zero-length, bad timestamp): filter silently.
6. **Unknown bug**: let it propagate. The runtime records
   ``system_unexpected_error`` as a defensive fallback; new collector code
   should not intentionally rely on that path for known source failures.

Session ID
----------

The capture function always generates ``session_id``. The runtime
treats it as opaque — stores but never generates or interprets it.

Choose a model based on your source type:

- **Continuous stream** (Icecast): one UUID per connection lifetime.
- **Discrete calls, persistent connection** (OpenMHZ): one UUID
  per (talkgroup, connection). New connection = new session.
- **Discrete calls, polling** (Broadcastify Calls): one UUID per
  collector function call. The function's lifetime = the session.
- **File-based** (Echo): one UUID per file.

Hard Rules
----------

The capture function must **never**:

1. Write to the database.
2. Catch ``CancelledError`` and suppress it (use ``try/finally``).
3. Use ``asyncio.sleep()`` — use shutdown-interruptible sleep.
4. Block the event loop (no sync I/O).
5. Hold unbounded state.
6. Yield chunks after ``shutdown.is_set()``.
"""

from __future__ import annotations

import dataclasses
from enum import StrEnum
from typing import TYPE_CHECKING

import aiohttp  # noqa: TC002 — runtime use: CaptureResources holds aiohttp.ClientSession

from backend.pipeline.storage.feed_store import FeedStatusReason

if TYPE_CHECKING:
    import asyncio
    import datetime
    from collections.abc import AsyncIterator, Callable
    from pathlib import Path

    from backend.pipeline.storage.feed_store import LeasedFeed


class AudioMimeType(StrEnum):
    """Enumeration of supported audio MIME types."""

    MPEG = "audio/mpeg"
    AAC = "audio/aac"
    WAV = "audio/x-wav"
    FLAC = "audio/flac"
    MP4 = "audio/mp4"
    OGG = "audio/ogg"

    @classmethod
    def from_string(cls, content_type: str | None) -> AudioMimeType | None:
        """Parses content_type defensively to resolve to AudioMimeType."""
        if not content_type:
            return None
        clean_type = content_type.split(";")[0].strip().lower()
        try:
            return cls(clean_type)
        except ValueError:
            aliases = {
                "audio/mp3": cls.MPEG,
                "audio/wav": cls.WAV,
                "audio/m4a": cls.MP4,
                "audio/x-m4a": cls.MP4,
            }
            return aliases.get(clean_type)


@dataclasses.dataclass(init=False, eq=False)
class FeedFailure(Exception):
    """Feed-level collector failure classified at the collector boundary.

    ``status_reason`` is the bounded operator grouping key. ``reason`` is the
    diagnostic text preserved for logs and status reason detail so users and
    engineers can debug the current failure episode.
    """

    status_reason: FeedStatusReason
    reason: str

    def __init__(
        self,
        status_reason: FeedStatusReason | str,
        reason: str,
    ) -> None:
        """Normalize collector-provided values before the runtime sees them."""
        try:
            normalized_status_reason = FeedStatusReason(status_reason)
        except (TypeError, ValueError) as e:
            msg = f"Unknown feed status reason: {status_reason!r}"
            raise ValueError(msg) from e

        if not isinstance(reason, str) or not reason:
            msg = "FeedFailure.reason must be a non-empty string"
            raise ValueError(msg)

        # Exception instances must remain runtime-mutable: Python sets
        # __traceback__, __context__, and __cause__ while propagating them.
        # A frozen dataclass breaks that machinery, so keep only the payload
        # normalized.
        self.status_reason = normalized_status_reason
        self.reason = reason
        Exception.__init__(self, self.reason)

    def __str__(self) -> str:
        return self.reason


@dataclasses.dataclass(frozen=True)
class CapturedChunk:
    """A single captured audio chunk yielded by a capture function.

    Attributes:
        audio_bytes: The raw audio file bytes for this segment.
        chunk_start_time: The UTC timestamp of the beginning of the audio window.
        chunk_end_time: The UTC timestamp of the end of the audio window.
        session_id: Opaque session identifier generated by the capture function.
            Groups chunks from a contiguous logical audio stream.
            Required for continuous feeds ONLY. For segmented feeds, this can be empty or omitted.
            The runtime stores it but never generates or interprets it.
        receipt_time: UTC-timezone-aware datetime stamped by the capture function
            at the moment the upstream-source event arrived at the collector
            (segment finalization for Icecast, WS event arrival for OpenMHZ,
            per-call iteration for bcfy_calls). ``None`` propagates silently:
            the downstream latency-emitting log OMITS ``processing_latency_sec``
            entirely from the payload when unset (the key is absent, not null).
            Required to be tz-aware UTC when set.
        mime_type: Optional validated HTTP Content-Type as an AudioMimeType enum value.
        resume_position: Source-specific position the runtime persists as the
            feed's resume cursor. ``None`` → the runtime falls back to
            ``chunk_end_time``. Set by cursor-paginated collectors (bcfy_calls);
            ``None`` for stream/push collectors.
        external_audio_segment_id: Optional external ID for tracking the source segment.
            Represents:
            - Echo: GCS bucket and object path (e.g. "bucket-name/channel-location/YYYYMMDD/filename.mp3").
            - Broadcastify Calls: Full source audio URL (e.g. "https://calls.broadcastify.com/.../123456.mp3").
            - Fire Notifications: Composite S3 file UUID and human-readable filename (e.g. "c1465213-2998-4ed7-a6a2-bf16ebf67265|SAN-JOSE-DISP 2026-06-09 18-38-41.mp3").
            - Broadcastify Feeds: Not applicable (omitted).
    """

    audio_bytes: bytes
    chunk_start_time: datetime.datetime
    chunk_end_time: datetime.datetime
    session_id: str | None = None
    receipt_time: datetime.datetime | None = None
    mime_type: AudioMimeType | None = None
    resume_position: datetime.datetime | None = None
    external_audio_segment_id: str | None = None


@dataclasses.dataclass(frozen=True)
class SourceObservation:
    """A non-audio source success observed by a capture function.

    Use this when a collector successfully reaches the source and confirms
    there is no audio item to emit. The runtime may use it to clear stale
    failure state, but it must never upload, publish, or treat it as audio
    progress.

    Attributes:
        resume_position: Optional source-specific cursor for the successful
            observation. For Broadcastify Calls this is the API ``lastPos``.
            ``None`` means the observation should not advance the persisted
            bookmark.
    """

    resume_position: datetime.datetime | None = None


type CaptureEvent = CapturedChunk | SourceObservation


@dataclasses.dataclass(frozen=True, kw_only=True)
class CaptureResources:
    """Runtime-owned resources passed to capture functions.

    Constructed once in CollectorRuntime._main() and lifecycle-managed
    by the runtime: http_session is closed in _shutdown_sequence after
    _gcs_client.close() (followed by a 250ms SSL-teardown sleep, per
    aiohttp's documented graceful-shutdown idiom for SSL).

    Forward-compatible container — additional runtime-owned resources
    (e.g. the deferred ffmpeg spawn semaphore) can be added without
    another signature break.

    Attributes:
        http_session: Shared aiohttp ClientSession with TCPConnector
            tuned for the bcfy_calls polling workload (limit=0,
            limit_per_host=64, ttl_dns_cache=300, keepalive_timeout=75).
            NOT Optional — collectors assume it is always set, and
            keeping the type strict avoids per-call None-checks.
    """

    http_session: aiohttp.ClientSession
    segment_temp_dir: Path | None = None


if TYPE_CHECKING:
    # 4-arg collector: (feed, shutdown_event, url_base, resources)
    # -> AsyncIterator[CaptureEvent]
    CollectorFn = Callable[
        [LeasedFeed, asyncio.Event, str, CaptureResources],
        AsyncIterator[CaptureEvent],
    ]
