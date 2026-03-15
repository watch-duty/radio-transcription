import uuid
from dataclasses import dataclass, field

from apache_beam.transforms.userstate import ReadModifyWriteRuntimeState, RuntimeTimer
from pydub import AudioSegment

from backend.pipeline.shared_constants import CHUNK_DURATION_SECONDS
from backend.pipeline.transcription.constants import MS_PER_SECOND
from backend.pipeline.transcription.enums import TranscriberType, VadType


@dataclass(frozen=True)
class TimeRange:
    """Represents a time interval in integer milliseconds."""

    start_ms: int
    end_ms: int

    @property
    def duration_ms(self) -> int:
        return self.end_ms - self.start_ms


@dataclass(frozen=True)
class AudioChunkData:
    start_ms: int
    audio: AudioSegment
    speech_segments: list[TimeRange]


@dataclass(frozen=True)
class TranscriptionResult:
    """Picklable dataclass to hold intermediate transcription results before Protobuf serialization."""

    feed_id: str
    audio_ids: list[uuid.UUID]
    transcript: str
    time_range: TimeRange
    missing_prior_context: bool = False
    start_chunk_id: uuid.UUID | None = None
    end_chunk_id: uuid.UUID | None = None



@dataclass
class TransmissionContext:
    """A Picklable dataclass storing all metadata for the current audio transmission.
    This consolidated struct massively reduces I/O roundtrips to Dataflow's state storage."""

    last_end_time_ms: int | None = None
    stale_start_time_ms: int | None = None
    contributing_uuids: set[uuid.UUID] = field(default_factory=set)
    missing_prior_context: bool = False
    expected_next_chunk_start_ms: int | None = None
    start_chunk_id: uuid.UUID | None = None


@dataclass
class StitcherContext:
    """Groups context variables for processing a chunk to reduce function arguments."""

    feed_id: str
    # The unique identifier of the raw audio file this file originated from.
    source_file_uuid: uuid.UUID
    current_buffer: AudioSegment | None
    # Set of unique source_file_uuids that have been accumulated into the current transmission buffer thus far.
    processed_uuids: set[uuid.UUID]
    last_segment_end_time_ms: int
    transmission_start_time_ms: int | None
    file_start_ms: int
    missing_prior_context: bool = False
    expected_next_chunk_start_ms: int | None = None
    start_chunk_id: uuid.UUID | None = None


@dataclass(frozen=True)
class OrderRestorerConfig:
    out_of_order_timeout_ms: int = 5000
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
    processed_uuids: set[uuid.UUID]
    time_range: TimeRange
    missing_prior_context: bool = False
    start_chunk_id: uuid.UUID | None = None
    end_chunk_id: uuid.UUID | None = None


@dataclass(frozen=True)
class StateMachineAction:
    """Base class for all actions emitted by the AudioStitchingStateMachine."""


@dataclass(frozen=True)
class DropAction(StateMachineAction):
    reason: str


@dataclass(frozen=True)
class FlushAction(StateMachineAction):
    reason: str
    flush_request: FlushRequest


@dataclass(frozen=True)
class UpdateStateAction(StateMachineAction):
    pass


@dataclass(frozen=True)
class ScheduleStaleTimerAction(StateMachineAction):
    deadline_ms: int
