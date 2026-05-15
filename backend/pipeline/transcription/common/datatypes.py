"""Domain objects and strongly-typed dataclasses for the transcription pipeline."""

from dataclasses import dataclass, field
from typing import Any

import numpy as np

from backend.pipeline.common.constants import (
    CHUNK_DURATION_SECONDS,
    MS_PER_SECOND,
)
from backend.pipeline.schema_types import streaming_state_pb2 as state_pb2
from backend.pipeline.transcription.common.constants import (
    DEFAULT_BACKFILL_LATENESS_THRESHOLD_MS,
    DEFAULT_SEGMENTED_OUT_OF_ORDER_TIMEOUT_MS,
)
from backend.pipeline.transcription.common.enums import TranscriberType


@dataclass(frozen=True)
class TimeRange:
    """Represents a time interval in integer milliseconds."""

    start_ms: int
    end_ms: int

    @property
    def duration_ms(self) -> int:
        """Calculates the duration of the time range in milliseconds."""
        return self.end_ms - self.start_ms

    def to_proto(self) -> Any:
        return state_pb2.TimeRangeProto(
            start_ms=self.start_ms, end_ms=self.end_ms
        )

    @classmethod
    def from_proto(cls, proto: Any) -> "TimeRange":
        return cls(start_ms=proto.start_ms, end_ms=proto.end_ms)


@dataclass(frozen=True, order=True)
class BufferedChunk:
    """Represents a chronologically sorted audio payload held in the jitter buffer."""

    timestamp_ms: int
    gcs_uri: str
    traceparent: str | None = None

    def to_proto(self) -> Any:
        return state_pb2.BufferedChunkProto(
            timestamp_ms=self.timestamp_ms,
            gcs_uri=self.gcs_uri,
            traceparent=self.traceparent or "",
        )

    @classmethod
    def from_proto(cls, proto: Any) -> "BufferedChunk":
        return cls(
            timestamp_ms=proto.timestamp_ms,
            gcs_uri=proto.gcs_uri,
            traceparent=proto.traceparent or None,
        )


@dataclass(frozen=True)
class AudioChunkData:
    """A domain model representing a single decoded audio chunk and its VAD metadata."""

    start_ms: int
    audio: np.ndarray
    speech_segments: list[TimeRange]
    gcs_uri: str
    duration_ms: int
    sample_rate: int


@dataclass(frozen=True)
class FeedMetadata:
    """Metadata about a feed, used for enriching the output."""

    feed_name: str
    external_id: str

    def to_proto(self) -> Any:
        return state_pb2.FeedMetadataProto(
            feed_name=self.feed_name, external_id=self.external_id
        )

    @classmethod
    def from_proto(cls, proto: Any) -> "FeedMetadata":
        return cls(feed_name=proto.feed_name, external_id=proto.external_id)


@dataclass(frozen=True)
class ChunkMetadata:
    """Metadata for an audio chunk before download."""

    gcs_uri: str
    session_id: str  # Required for continuous feeds ONLY.
    duration_ms: int
    feed_metadata: FeedMetadata
    is_continuous: bool = True
    traceparent: str | None = None

    def to_proto(self) -> Any:
        return state_pb2.ChunkMetadataProto(
            gcs_uri=self.gcs_uri,
            session_id=self.session_id,
            duration_ms=self.duration_ms,
            feed_metadata=self.feed_metadata.to_proto(),
            traceparent=self.traceparent or "",
        )

    @classmethod
    def from_proto(cls, proto: Any) -> "ChunkMetadata":
        return cls(
            gcs_uri=proto.gcs_uri,
            session_id=proto.session_id,
            duration_ms=proto.duration_ms,
            feed_metadata=FeedMetadata.from_proto(proto.feed_metadata),
            traceparent=proto.traceparent or None,
        )


@dataclass(frozen=True)
class DownloadedChunkPayload:
    """Payload for a downloaded audio chunk with its metadata."""

    gcs_uri: str
    chunk_data: AudioChunkData
    session_id: str


@dataclass(frozen=True)
class TranscriptionResult:
    """Intermediate transcription result holding payload data before Protobuf serialization, bypassing Protobuf pickling issues during Dataflow shuffle."""

    feed_id: str
    session_id: str
    contributing_audio_uris: list[str]
    transcript: str
    time_range: TimeRange
    transmission_id: str
    start_audio_offset_ms: int
    end_audio_offset_ms: int
    canonical_audio_uri: str
    playback_audio_uri: str
    feed_metadata: FeedMetadata
    missing_prior_context: bool = False
    missing_post_context: bool = False
    traceparent: str | None = None


@dataclass(frozen=True)
class IdleFeedState:
    """Represents an uninitialized or flushed feed state with no active transmission session."""

    out_of_order_buffer: list[BufferedChunk] = field(default_factory=list)
    order_timer_active: bool = False

    def to_proto(self) -> Any:
        return state_pb2.IdleFeedStateProto(
            order_timer_active=self.order_timer_active,
            out_of_order_buffer=[
                c.to_proto() for c in self.out_of_order_buffer
            ],
        )

    @classmethod
    def from_proto(cls, proto: Any) -> "IdleFeedState":
        return cls(
            order_timer_active=proto.order_timer_active,
            out_of_order_buffer=[
                BufferedChunk.from_proto(c) for c in proto.out_of_order_buffer
            ],
        )


@dataclass(frozen=True)
class ActiveStitchingState:
    """Represents an active audio transmission session with guaranteed feed metadata and session ID."""

    session_id: str
    feed_metadata: FeedMetadata
    last_end_time_ms: int | None = None
    stale_start_time_ms: int | None = None
    buffer_start_time_ms: int | None = None
    expected_next_chunk_start_ms: int | None = None
    start_audio_offset_ms: int | None = None
    end_audio_offset_ms: int | None = None
    contributing_audio_uris: list[str] = field(default_factory=list)
    missing_prior_context: bool = False
    missing_post_context: bool = False
    buffer_duration_ms: int = 0
    order_timer_active: bool = False
    out_of_order_buffer: list[BufferedChunk] = field(default_factory=list)
    last_transmission_start_ms: int | None = None
    speech_segments: list[TimeRange] = field(default_factory=list)
    prior_audio_tail: bytes | None = None
    traceparent: str | None = None
    sample_rate: int | None = None

    def to_proto(self) -> Any:
        proto = state_pb2.ActiveStitchingStateProto(
            session_id=self.session_id,
            feed_metadata=self.feed_metadata.to_proto(),
            contributing_audio_uris=self.contributing_audio_uris,
            missing_prior_context=self.missing_prior_context,
            missing_post_context=self.missing_post_context,
            buffer_duration_ms=self.buffer_duration_ms,
            order_timer_active=self.order_timer_active,
            out_of_order_buffer=[
                c.to_proto() for c in self.out_of_order_buffer
            ],
            speech_segments=[s.to_proto() for s in self.speech_segments],
        )
        if self.last_end_time_ms is not None:
            proto.last_end_time_ms = self.last_end_time_ms
        if self.stale_start_time_ms is not None:
            proto.stale_start_time_ms = self.stale_start_time_ms
        if self.buffer_start_time_ms is not None:
            proto.buffer_start_time_ms = self.buffer_start_time_ms
        if self.expected_next_chunk_start_ms is not None:
            proto.expected_next_chunk_start_ms = (
                self.expected_next_chunk_start_ms
            )
        if self.start_audio_offset_ms is not None:
            proto.start_audio_offset_ms = self.start_audio_offset_ms
        if self.end_audio_offset_ms is not None:
            proto.end_audio_offset_ms = self.end_audio_offset_ms
        if self.last_transmission_start_ms is not None:
            proto.last_transmission_start_ms = self.last_transmission_start_ms
        if self.prior_audio_tail is not None:
            proto.prior_audio_tail = self.prior_audio_tail
        if self.traceparent is not None:
            proto.traceparent = self.traceparent
        if self.sample_rate is not None:
            proto.sample_rate = self.sample_rate
        return proto

    @classmethod
    def from_proto(cls, proto: Any) -> "ActiveStitchingState":
        return cls(
            session_id=proto.session_id,
            feed_metadata=FeedMetadata.from_proto(proto.feed_metadata),
            last_end_time_ms=proto.last_end_time_ms
            if proto.HasField("last_end_time_ms")
            else None,
            stale_start_time_ms=proto.stale_start_time_ms
            if proto.HasField("stale_start_time_ms")
            else None,
            buffer_start_time_ms=proto.buffer_start_time_ms
            if proto.HasField("buffer_start_time_ms")
            else None,
            expected_next_chunk_start_ms=proto.expected_next_chunk_start_ms
            if proto.HasField("expected_next_chunk_start_ms")
            else None,
            start_audio_offset_ms=proto.start_audio_offset_ms
            if proto.HasField("start_audio_offset_ms")
            else None,
            end_audio_offset_ms=proto.end_audio_offset_ms
            if proto.HasField("end_audio_offset_ms")
            else None,
            contributing_audio_uris=list(proto.contributing_audio_uris),
            missing_prior_context=proto.missing_prior_context,
            missing_post_context=proto.missing_post_context,
            buffer_duration_ms=proto.buffer_duration_ms,
            order_timer_active=proto.order_timer_active,
            out_of_order_buffer=[
                BufferedChunk.from_proto(c) for c in proto.out_of_order_buffer
            ],
            last_transmission_start_ms=proto.last_transmission_start_ms
            if proto.HasField("last_transmission_start_ms")
            else None,
            speech_segments=[
                TimeRange.from_proto(s) for s in proto.speech_segments
            ],
            prior_audio_tail=proto.prior_audio_tail
            if proto.HasField("prior_audio_tail")
            else None,
            traceparent=proto.traceparent
            if proto.HasField("traceparent")
            else None,
            sample_rate=proto.sample_rate
            if proto.HasField("sample_rate")
            else None,
        )


TransmissionContext = IdleFeedState | ActiveStitchingState


@dataclass
class StitcherContext:
    """Groups context variables for processing a chunk to reduce function arguments."""

    feed_id: str
    # The fully qualified GCS URI of the raw audio file currently being parsed.
    current_gcs_uri: str
    session_id: str | None
    # Ordered list of URIs that have been accumulated into the current transmission buffer thus far.
    contributing_audio_uris: list[str]
    file_start_ms: int
    last_segment_end_time_ms: int | None
    transmission_start_time_ms: int | None
    buffer_start_time_ms: int | None
    missing_prior_context: bool
    expected_next_chunk_start_ms: int | None
    start_audio_offset_ms: int | None
    end_audio_offset_ms: int | None = None
    buffer_duration_ms: int = 0
    speech_segments: list[TimeRange] = field(default_factory=list)
    traceparent: str | None = None


@dataclass(frozen=True)
class OrderRestorerConfig:
    """Configuration parameters for the sequence Jitter Buffer."""

    out_of_order_timeout_ms: int = DEFAULT_SEGMENTED_OUT_OF_ORDER_TIMEOUT_MS
    chunk_duration_ms: int = CHUNK_DURATION_SECONDS * MS_PER_SECOND


@dataclass(frozen=True)
class StitchAudioConfig:
    """Groups pipeline-level configurations passed to the stateful DoFn."""

    project_id: str
    vad_config: str
    significant_gap_ms: int
    stale_timeout_ms: int
    max_transmission_duration_ms: int
    vad_pre_roll_ms: int
    vad_post_roll_ms: int
    route_to_dlq: bool = True
    backfill_lateness_threshold_ms: int = DEFAULT_BACKFILL_LATENESS_THRESHOLD_MS
    isolate_segmented_chunks: bool = False

    def __post_init__(self) -> None:
        """Validates the dataclass variables."""
        if self.significant_gap_ms <= 0:
            msg = "significant_gap_ms must be > 0"
            raise ValueError(msg)
        if self.stale_timeout_ms <= 0:
            msg = "stale_timeout_ms must be > 0"
            raise ValueError(msg)
        if self.max_transmission_duration_ms <= 0:
            msg = "max_transmission_duration_ms must be > 0"
            raise ValueError(msg)
        if self.significant_gap_ms >= self.max_transmission_duration_ms:
            msg = "significant_gap_ms must be strictly less than max_transmission_duration_ms"
            raise ValueError(msg)


@dataclass(frozen=True)
class TranscribeAudioConfig:
    """Groups pipeline-level configurations passed to the stateless DoFn."""

    project_id: str
    transcriber_type: TranscriberType
    transcriber_config: str
    vad_config: str
    route_to_dlq: bool = True
    canonical_audio_bucket: str | None = None


@dataclass(frozen=True)
class FlushRequest:
    """Encapsulates the data required to flush an audio buffer to the transcription API."""

    buffer: bytes
    feed_id: str
    session_id: str
    contributing_audio_uris: list[str]
    time_range: TimeRange
    transmission_id: str
    feed_metadata: FeedMetadata
    sample_rate: int
    missing_prior_context: bool
    missing_post_context: bool
    start_audio_offset_ms: int | None
    end_audio_offset_ms: int | None
    speech_segments: list[TimeRange] = field(default_factory=list)
    traceparent: str | None = None

    def to_proto(self) -> Any:
        proto = state_pb2.FlushRequestProto(
            buffer=self.buffer,
            feed_id=self.feed_id,
            session_id=self.session_id,
            contributing_audio_uris=self.contributing_audio_uris,
            time_range=self.time_range.to_proto(),
            transmission_id=self.transmission_id,
            feed_metadata=self.feed_metadata.to_proto(),
            sample_rate=self.sample_rate,
            missing_prior_context=self.missing_prior_context,
            missing_post_context=self.missing_post_context,
            speech_segments=[s.to_proto() for s in self.speech_segments],
            traceparent=self.traceparent or "",
        )
        if self.start_audio_offset_ms is not None:
            proto.start_audio_offset_ms = self.start_audio_offset_ms
        if self.end_audio_offset_ms is not None:
            proto.end_audio_offset_ms = self.end_audio_offset_ms
        return proto

    @classmethod
    def from_proto(cls, proto: Any) -> "FlushRequest":
        return cls(
            buffer=proto.buffer,
            feed_id=proto.feed_id,
            session_id=proto.session_id,
            contributing_audio_uris=list(proto.contributing_audio_uris),
            time_range=TimeRange.from_proto(proto.time_range),
            transmission_id=proto.transmission_id,
            feed_metadata=FeedMetadata.from_proto(proto.feed_metadata),
            sample_rate=proto.sample_rate,
            missing_prior_context=proto.missing_prior_context,
            missing_post_context=proto.missing_post_context,
            start_audio_offset_ms=proto.start_audio_offset_ms
            if proto.HasField("start_audio_offset_ms")
            else None,
            end_audio_offset_ms=proto.end_audio_offset_ms
            if proto.HasField("end_audio_offset_ms")
            else None,
            speech_segments=[
                TimeRange.from_proto(s) for s in proto.speech_segments
            ],
            traceparent=proto.traceparent
            if proto.HasField("traceparent")
            else None,
        )


@dataclass(frozen=True)
class StateMachineAction:
    """Base class for all actions emitted by the AudioStitchingStateMachine."""


@dataclass(frozen=True)
class DropAction(StateMachineAction):
    """Action emitted when a chunk violates chronological state and is permanently discarded."""

    reason: str


@dataclass(frozen=True)
class AppendBufferAction(StateMachineAction):
    """Signals that the provided audio segment should be appended to the active transmission buffer."""

    audio_buffer: np.ndarray


@dataclass(frozen=True)
class FlushAction(StateMachineAction):
    """Action emitted when a semantic transmission boundary is reached and the buffer must be processed."""

    reason: str
    feed_id: str
    time_range: TimeRange
    speech_time_range: TimeRange
    contributing_audio_uris: list[str]
    missing_prior_context: bool
    missing_post_context: bool
    start_audio_offset_ms: int
    end_audio_offset_ms: int
    clear_state: bool = True
    isolated_audio_buffer: list[np.ndarray] = field(default_factory=list)
    isolated_audio_buffer_uris: list[str] = field(default_factory=list)
    speech_segments: list[TimeRange] = field(default_factory=list)
    traceparent: str | None = None


@dataclass(frozen=True)
class UpdateStateAction(StateMachineAction):
    """Action emitted to explicitly persist localized Python state mutations up to Apache Beam."""


@dataclass(frozen=True)
class ScheduleStaleTimerAction(StateMachineAction):
    """Action emitted to adjust Beam Watermark timers for dead-transmission recovery."""

    deadline_ms: int
