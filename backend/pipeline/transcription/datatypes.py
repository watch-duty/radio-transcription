"""Domain objects and strongly-typed dataclasses for the transcription pipeline."""

from dataclasses import dataclass, field

from pydub import AudioSegment

from backend.pipeline.common.constants import (
    CHUNK_DURATION_SECONDS,
    MS_PER_SECOND,
)
from backend.pipeline.transcription.constants import (
    DEFAULT_OUT_OF_ORDER_TIMEOUT_MS,
)
from backend.pipeline.transcription.enums import TranscriberType, VadType


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
    audio: AudioSegment
    speech_segments: list[TimeRange]
    gcs_uri: str


@dataclass(frozen=True)
class TranscriptionResult:
    """Picklable dataclass to hold intermediate transcription results before Protobuf serialization."""

    feed_id: str
    contributing_audio_uris: list[str]
    transcript: str
    time_range: TimeRange
    missing_prior_context: bool = False
    missing_post_context: bool = False
    start_audio_offset_ms: int | None = None
    end_audio_offset_ms: int | None = None


@dataclass(frozen=True)
class TransmissionContext:
    """A Picklable dataclass storing all metadata for the current audio transmission.

    This consolidated struct massively reduces I/O roundtrips to Dataflow's state storage.
    """

    last_end_time_ms: int | None = None
    stale_start_time_ms: int | None = None
    contributing_audio_uris: list[str] = field(default_factory=list)
    missing_prior_context: bool = False
    missing_post_context: bool = False
    expected_next_chunk_start_ms: int | None = None
    start_audio_offset_ms: int | None = None
    end_audio_offset_ms: int | None = None


@dataclass
class StitcherContext:
    """Groups context variables for processing a chunk to reduce function arguments."""

    feed_id: str
    # The fully qualified GCS URI of the raw audio file currently being parsed.
    current_gcs_uri: str
    # Ordered list of URIs that have been accumulated into the current transmission buffer thus far.
    contributing_audio_uris: list[str]
    file_start_ms: int
    last_segment_end_time_ms: int | None = None
    transmission_start_time_ms: int | None = None
    missing_prior_context: bool = False
    expected_next_chunk_start_ms: int | None = None
    start_audio_offset_ms: int | None = None
    end_audio_offset_ms: int | None = None


@dataclass(frozen=True)
class OrderRestorerConfig:
    """Configuration parameters for the sequence Jitter Buffer."""

    out_of_order_timeout_ms: int = DEFAULT_OUT_OF_ORDER_TIMEOUT_MS
    chunk_duration_ms: int = CHUNK_DURATION_SECONDS * MS_PER_SECOND


@dataclass(frozen=True)
class StitchAudioConfig:
    """Groups pipeline-level configurations passed to the stateful DoFn."""

    project_id: str
    vad_type: VadType
    vad_config: str
    metrics_exporter_type: str
    metrics_config: str
    significant_gap_ms: int
    stale_timeout_ms: int
    max_transmission_duration_ms: int
    route_to_dlq: bool = True

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
    metrics_exporter_type: str
    metrics_config: str
    route_to_dlq: bool = True


@dataclass(frozen=True)
class FlushRequest:
    """Encapsulates the data required to flush an audio buffer to the transcription API."""

    buffer: AudioSegment
    feed_id: str
    contributing_audio_uris: list[str]
    time_range: TimeRange
    missing_prior_context: bool = False
    missing_post_context: bool = False
    start_audio_offset_ms: int | None = None
    end_audio_offset_ms: int | None = None


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

    audio_buffer: AudioSegment


@dataclass(frozen=True)
class FlushAction(StateMachineAction):
    """Action emitted when a semantic transmission boundary is reached and the buffer must be processed."""

    reason: str
    feed_id: str
    time_range: TimeRange
    contributing_audio_uris: list[str]
    missing_prior_context: bool
    missing_post_context: bool
    start_audio_offset_ms: int | None
    end_audio_offset_ms: int | None
    clear_state: bool = True
    isolated_audio_buffer: list[AudioSegment] | None = None


@dataclass(frozen=True)
class UpdateStateAction(StateMachineAction):
    """Action emitted to explicitly persist localized Python state mutations up to Apache Beam."""


@dataclass(frozen=True)
class ScheduleStaleTimerAction(StateMachineAction):
    """Action emitted to adjust Beam Watermark timers for dead-transmission recovery."""

    deadline_ms: int
