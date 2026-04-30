"""Domain objects and strongly-typed dataclasses for the transcription pipeline."""

from dataclasses import dataclass, field

import numpy as np

from backend.pipeline.common.constants import (
    CHUNK_DURATION_SECONDS,
    MS_PER_SECOND,
)
from backend.pipeline.transcription.common.constants import (
    DEFAULT_SEGMENTED_OUT_OF_ORDER_TIMEOUT_MS,
)
from backend.pipeline.transcription.common.enums import TranscriberType, VadType


@dataclass(frozen=True)
class TimeRange:
    """Represents a time interval in integer milliseconds."""

    start_ms: int
    end_ms: int

    @property
    def duration_ms(self) -> int:
        """Calculates the duration of the time range in milliseconds."""
        return self.end_ms - self.start_ms


@dataclass(frozen=True, order=True)
class BufferedChunk:
    """Represents a chronologically sorted audio payload held in the jitter buffer."""

    timestamp_ms: int
    gcs_uri: str


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


@dataclass(frozen=True)
class ChunkMetadata:
    """Metadata for an audio chunk before download."""

    gcs_uri: str
    session_id: str  # Required for continuous feeds ONLY.
    duration_ms: int
    feed_metadata: FeedMetadata
    trace_id: str = ""


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


@dataclass(frozen=True)
class TransmissionContext:
    """Dataclass storing all metadata for the current audio transmission.

    This consolidated struct massively reduces I/O roundtrips to Dataflow's state storage.
    We use standard dataclasses here because native Protobuf classes cannot be cleanly pickled.
    """

    session_id: str | None = None
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
    feed_metadata: FeedMetadata | None = None
    last_transmission_start_ms: int | None = None
    trace_id: str = ""


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


@dataclass(frozen=True)
class OrderRestorerConfig:
    """Configuration parameters for the sequence Jitter Buffer."""

    out_of_order_timeout_ms: int = DEFAULT_SEGMENTED_OUT_OF_ORDER_TIMEOUT_MS
    chunk_duration_ms: int = CHUNK_DURATION_SECONDS * MS_PER_SECOND


@dataclass(frozen=True)
class StitchAudioConfig:
    """Groups pipeline-level configurations passed to the stateful DoFn."""

    project_id: str
    vad_type: VadType
    vad_config: str
    significant_gap_ms: int
    stale_timeout_ms: int
    max_transmission_duration_ms: int
    vad_pre_roll_ms: int
    vad_post_roll_ms: int
    route_to_dlq: bool = True
    backfill_lateness_threshold_ms: int = 300000
    bypass_stitching: bool = False

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
    vad_type: VadType
    vad_config: str
    route_to_dlq: bool = True
    canonical_audio_bucket: str | None = None


@dataclass(frozen=True)
class FlushRequest:
    """Encapsulates the data required to flush an audio buffer to the transcription API."""

    buffer: np.ndarray
    feed_id: str
    session_id: str
    contributing_audio_uris: list[str]
    time_range: TimeRange
    transmission_id: str
    feed_metadata: FeedMetadata
    missing_prior_context: bool
    missing_post_context: bool
    start_audio_offset_ms: int | None
    end_audio_offset_ms: int | None


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


@dataclass(frozen=True)
class UpdateStateAction(StateMachineAction):
    """Action emitted to explicitly persist localized Python state mutations up to Apache Beam."""


@dataclass(frozen=True)
class ScheduleStaleTimerAction(StateMachineAction):
    """Action emitted to adjust Beam Watermark timers for dead-transmission recovery."""

    deadline_ms: int
