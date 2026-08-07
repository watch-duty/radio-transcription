"""Domain objects and strongly-typed dataclasses for the transcription pipeline."""

import concurrent.futures
import logging
from dataclasses import dataclass, field
from typing import Any, Literal, TypedDict

import apache_beam as beam
import numpy as np

from backend.pipeline.common.constants import (
    CHUNK_DURATION_SECONDS,
    MS_PER_SECOND,
)
from backend.pipeline.schema_types import (
    streaming_state as bp_state,
)
from backend.pipeline.segmentation.constants import (
    DEFAULT_BACKFILL_LATENESS_THRESHOLD_MS,
    DEFAULT_SEGMENTED_OUT_OF_ORDER_TIMEOUT_MS,
    DEFAULT_VAD_POST_ROLL_MS,
)

logger = logging.getLogger(__name__)

# Type alias for the segmentation Dead Letter Queue tagged output
SegmentationDlqOutput = beam.pvalue.TaggedOutput[
    Literal["segmentation_dlq"],
    dict[str, Any],
]

# Type alias for the raw DLQ payload tuple yielded by StitcherEngine
SegmentationRawDlqOutput = tuple[Literal["segmentation_dlq"], dict[str, Any]]


TimeRange = bp_state.TimeRangeProto
BufferedChunk = bp_state.BufferedChunkProto


@dataclass(frozen=True)
class AudioSignal:
    """Represents an in-memory decoded audio signal."""

    samples: np.ndarray
    sample_rate: int

    @property
    def duration_seconds(self) -> float:
        return (
            len(self.samples) / float(self.sample_rate)
            if self.sample_rate > 0
            else 0.0
        )


@dataclass(frozen=True)
class TimedAudioSignal:
    """A decoded signal with CPU consumed by its executor thread."""

    signal: AudioSignal
    thread_cpu_us: int


AudioFutureMap = dict[str, concurrent.futures.Future[TimedAudioSignal]]


@dataclass(frozen=True)
class AudioChunkData:
    """A domain model representing a single decoded audio chunk and its VAD metadata."""

    start_ms: int
    audio: np.ndarray
    speech_segments: list[TimeRange]
    gcs_uri: str
    duration_ms: int
    sample_rate: int
    denoised_audio: np.ndarray | None = None


FeedMetadata = bp_state.FeedMetadataProto


ChunkMetadata = bp_state.ChunkMetadataProto


@dataclass(frozen=True)
class DownloadedChunkPayload:
    """Payload for a downloaded audio chunk with its metadata."""

    gcs_uri: str
    chunk_data: AudioChunkData
    session_id: str


IdleFeedState = bp_state.IdleFeedStateProto
ActiveStitchingState = bp_state.ActiveStitchingStateProto


TransmissionContext = IdleFeedState | ActiveStitchingState


@dataclass
class StitcherContext:
    """Groups context variables for processing a chunk to reduce function arguments."""

    feed_id: str
    # The fully qualified GCS URI of the raw audio file currently being parsed.
    current_gcs_uri: str
    session_id: str | None

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
    baggage: str | None = None
    prior_audio_tail: bytes | None = None
    # Ordered list of chunks that have been accumulated into the current transmission buffer.
    contributing_chunks: list[BufferedChunk] = field(default_factory=list)

    @property
    def contributing_audio_uris(self) -> list[str]:
        """Ordered list of URIs that have been accumulated into the current transmission buffer."""
        return [c.gcs_uri for c in self.contributing_chunks]

    def add_contributing_chunk(
        self,
        gcs_uri: str,
        timestamp_ms: int,
        receipt_time_ms: int | None = None,
    ) -> None:
        """Adds a chunk to the contributing lists if not already present.

        Args:
            gcs_uri: The fully qualified GCS URI of the contributing audio chunk.
            timestamp_ms: The absolute epoch start time of the audio chunk in milliseconds.
                This timestamp serves as the absolute temporal baseline for the chunk. The
                downstream stateless stage uses it to align relative segment offsets to
                absolute timeline boundaries, ensuring sample-accurate slicing and stitching
                across chunk boundaries.
            receipt_time_ms: Optional wall-clock arrival time in milliseconds at collector.
        """
        if not any(c.gcs_uri == gcs_uri for c in self.contributing_chunks):
            self.contributing_chunks.append(
                BufferedChunk(
                    timestamp_ms=timestamp_ms,
                    gcs_uri=gcs_uri,
                    traceparent=self.traceparent,
                    baggage=self.baggage,
                    receipt_time_ms=receipt_time_ms,
                )
            )


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
    route_to_dlq: bool = True
    backfill_lateness_threshold_ms: int = DEFAULT_BACKFILL_LATENESS_THRESHOLD_MS
    isolate_segmented_chunks: bool = False
    analyze_audio: bool = True

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
        if self.max_transmission_duration_ms + DEFAULT_VAD_POST_ROLL_MS > 60000:
            logger.warning(
                "max_transmission_duration_ms (%dms) + DEFAULT_VAD_POST_ROLL_MS (%dms) "
                "exceeds 60000ms (60 seconds). This is acceptable for models like Gemini "
                "but will cause synchronous transcription failures if using the Google Chirp V3 API.",
                self.max_transmission_duration_ms,
                DEFAULT_VAD_POST_ROLL_MS,
            )


FlushRequest = bp_state.FlushRequestProto
AudioClassification = bp_state.FlushRequestProtoAudioClassification


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
    contributing_chunks: list[BufferedChunk]
    missing_prior_context: bool
    missing_post_context: bool
    start_audio_offset_ms: int
    end_audio_offset_ms: int
    clear_state: bool = True
    isolated_audio_buffer: list[np.ndarray] = field(default_factory=list)
    isolated_audio_buffer_uris: list[str] = field(default_factory=list)
    speech_segments: list[TimeRange] = field(default_factory=list)
    traceparent: str | None = None
    baggage: str | None = None
    audio_classification: int = 0


@dataclass(frozen=True)
class UpdateStateAction(StateMachineAction):
    """Action emitted to explicitly persist localized Python state mutations up to Apache Beam."""


@dataclass(frozen=True)
class ScheduleStaleTimerAction(StateMachineAction):
    """Action emitted to adjust Beam Watermark timers for dead-transmission recovery."""

    deadline_ms: int


class StitcherDlqPayload(TypedDict):
    """Payload emitted to the Dead Letter Queue when chunk stitching fails."""

    feed_id: str
    gcs_uri: str
    session_id: str
    error_message: str
    traceparent: str | None


class PendingPubSubMessage(TypedDict):
    """A Pub/Sub message awaiting its turn in PubSubOrderRestorerFn.

    Carries the PubsubMessage fields rather than a PubsubMessage because
    instances of this are held in Beam BagState between bundles, and a plain
    mapping keeps that state encoding independent of any class definition.

    A tombstone marks a segment whose upload failed. It carries no payload and
    is never published, but still occupies its sequence number so a failed
    upload advances the sequence instead of blocking the feed behind a gap that
    will never fill.
    """

    data: bytes
    attributes: dict[str, str]
    ordering_key: str
    is_tombstone: bool


@dataclass(frozen=True)
class StitcherChunkResult:
    """Structured output returned after processing and stitching a chronological chunk."""

    outputs: list[tuple[str, FlushRequest] | tuple[str, StitcherDlqPayload]]
    next_context: TransmissionContext
    next_expected_ts: int
    next_last_start_ms: int | None
