"""Domain objects and strongly-typed dataclasses for the transcription pipeline."""

from dataclasses import dataclass, field

import numpy as np

from backend.pipeline.common.constants import (
    CHUNK_DURATION_SECONDS,
    MS_PER_SECOND,
)
from backend.pipeline.transcription.constants import (
    DEFAULT_OUT_OF_ORDER_TIMEOUT_MS,
)
from backend.pipeline.transcription.enums import TranscriberType


@dataclass(frozen=True)
class TimeRange:
    """Represents a time interval in integer milliseconds."""

    start_ms: int
    end_ms: int

    @property
    def duration_ms(self) -> int:
        """Calculates the duration of the time range in milliseconds."""
        return self.end_ms - self.start_ms


@dataclass
class VadResult:
    speech_segments: list[TimeRange]
    silence_segments: list[TimeRange]


@dataclass(frozen=True, order=True)
class BufferedChunk:
    """Represents a chronologically sorted audio payload held in the jitter buffer."""

    timestamp_ms: int
    gcs_uri: str


@dataclass(frozen=True)
class PaddedSegment:
    """A speech segment that has been padded and verified to be clean."""

    raw_audio: np.ndarray
    denoised_audio: np.ndarray
    start_ms: int  # Absolute start time of the padded segment
    speech_start_ms: int  # Absolute start time of the speech within it
    speech_end_ms: int  # Absolute end time of the speech within it


@dataclass(frozen=True)
class AudioChunkData:
    """A domain model representing a single decoded audio chunk and its VAD metadata."""

    start_ms: int
    audio: np.ndarray
    gcs_uri: str
    stored_audio: np.ndarray = field(
        default_factory=lambda: np.array([], dtype=np.int16)
    )
    original_sr: int = 16000
    is_pure_silence: bool = False

    @property
    def duration_ms(self) -> int:
        """Returns the duration of the audio in milliseconds, assuming 16kHz."""
        return int(self.audio.size / 16)


@dataclass(frozen=True)
class TranscriptionResult:
    """Intermediate transcription result holding payload data before Protobuf serialization, bypassing Protobuf pickling issues during Dataflow shuffle."""

    feed_id: str
    contributing_audio_uris: list[str]
    transcript: str
    time_range: TimeRange
    missing_prior_context: bool = False
    missing_post_context: bool = False
    start_audio_offset_ms: int | None = None
    end_audio_offset_ms: int | None = None
    canonical_audio_uri: str | None = None
    playback_audio_uri: str | None = None


@dataclass(frozen=True)
class TransmissionContext:
    """Dataclass storing all metadata for the current audio transmission.

    This consolidated struct massively reduces I/O roundtrips to Dataflow's state storage.
    We use standard dataclasses here because native Protobuf classes cannot be cleanly pickled.
    """

    last_end_time_ms: int | None = None
    stale_start_time_ms: int | None = None
    buffer_start_time_ms: int | None = None
    contributing_audio_uris: list[str] = field(default_factory=list)
    missing_prior_context: bool = False
    missing_post_context: bool = False
    expected_next_chunk_start_ms: int | None = None
    start_audio_offset_ms: int | None = None
    end_audio_offset_ms: int | None = None
    original_sr: int = 16000
    buffer_duration_ms: int = 0


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
    buffer_start_time_ms: int | None = None
    missing_prior_context: bool = False
    expected_next_chunk_start_ms: int | None = None
    start_audio_offset_ms: int | None = None
    end_audio_offset_ms: int | None = None
    buffer_duration_ms: int = 0
    original_sr: int = 16000


@dataclass(frozen=True)
class OrderRestorerConfig:
    """Configuration parameters for the sequence Jitter Buffer."""

    out_of_order_timeout_ms: int = DEFAULT_OUT_OF_ORDER_TIMEOUT_MS
    chunk_duration_ms: int = CHUNK_DURATION_SECONDS * MS_PER_SECOND


@dataclass(frozen=True)
class StitchAudioConfig:
    """Groups pipeline-level configurations passed to the stateful DoFn."""

    project_id: str
    vad_config: str
    metrics_exporter_type: str
    metrics_config: str
    significant_gap_ms: int
    stale_timeout_ms: int
    max_transmission_duration_ms: int
    vad_pre_roll_ms: int
    vad_post_roll_ms: int
    vad_cache_size: int = 20
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
    vad_config: str
    metrics_exporter_type: str
    metrics_config: str
    route_to_dlq: bool = True
    stitched_audio_bucket: str | None = None


@dataclass(frozen=True)
class FlushRequest:
    """Encapsulates the data required to flush an audio buffer to the transcription API."""

    buffer: np.ndarray
    feed_id: str
    contributing_audio_uris: list[str]
    time_range: TimeRange
    stored_buffer: np.ndarray = field(
        default_factory=lambda: np.array([], dtype=np.int16)
    )
    original_sr: int = 16000
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
    """Signals that audio should be appended to the active transmission buffer."""

    raw_audio: np.ndarray
    denoised_audio: np.ndarray
    start_offset_ms: int
    end_offset_ms: int


@dataclass(frozen=True)
class AppendIsolatedBufferAction(StateMachineAction):
    """Signals that a slice of the primary audio chunk should be appended to an isolated temporary buffer."""

    start_offset_ms: int
    end_offset_ms: int


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
    start_audio_offset_ms: int | None
    end_audio_offset_ms: int | None
    clear_state: bool = True
    isolated_audio_buffer: list[tuple[np.ndarray, np.ndarray]] | None = None


@dataclass(frozen=True)
class UpdateStateAction(StateMachineAction):
    """Action emitted to explicitly persist localized Python state mutations up to Apache Beam."""


@dataclass(frozen=True)
class ScheduleStaleTimerAction(StateMachineAction):
    """Action emitted to adjust Beam Watermark timers for dead-transmission recovery."""

    deadline_ms: int
