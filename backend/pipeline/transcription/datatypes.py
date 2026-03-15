import uuid
from dataclasses import dataclass

from apache_beam.transforms.userstate import ReadModifyWriteRuntimeState, RuntimeTimer
from pydub import AudioSegment

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
class AudioFileData:
    start_ms: int
    audio: AudioSegment
    speech_segments: list[TimeRange]


@dataclass(frozen=True)
class TranscriptionResult:
    """Picklable dataclass to hold intermediate transcription results before Protobuf serialization."""

    feed_id: str
    audio_ids: list[uuid.UUID]
    transcript: str
    start_ms: int
    end_ms: int


@dataclass(frozen=True)
class TransmissionState:
    """Groups Beam state and timer parameters for easier passing between helper methods."""

    buffer: ReadModifyWriteRuntimeState
    last_end_time: ReadModifyWriteRuntimeState
    stale_start_time: ReadModifyWriteRuntimeState
    contributing_uuids: ReadModifyWriteRuntimeState
    stale_timer: RuntimeTimer | None = None

    def clear_all(self) -> None:
        """Unconditionally clears all transmission states and timers."""
        self.buffer.clear()
        self.last_end_time.clear()
        self.stale_start_time.clear()
        self.contributing_uuids.clear()
        if self.stale_timer:
            self.stale_timer.clear()


@dataclass
class StitcherContext:
    """Groups context variables for processing a chunk to reduce function arguments."""

    feed_id: str
    source_file_uuid: uuid.UUID
    """The unique identifier of the raw audio file this chunk originated from."""
    current_buffer: AudioSegment | None
    processed_uuids: set[uuid.UUID]
    """Set of unique source_file_uuids that have been accumulated into the current transmission buffer thus far."""
    last_segment_end_time_ms: int
    transmission_start_time_ms: int | None
    chunk_start_ms: int


@dataclass(frozen=True)
class StitchAndTranscribeConfig:
    """Groups pipeline-level configurations passed to the stateful DoFn."""

    project_id: str
    transcriber_type: TranscriberType
    transcriber_config: str
    vad_type: VadType
    vad_config: str
    metrics_exporter_type: str
    metrics_config: str
    significant_gap_ms: int
    stale_timeout_ms: int
    max_transmission_duration_ms: int

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
class FlushRequest:
    """Encapsulates the data required to flush an audio buffer to the transcription API."""

    buffer: AudioSegment
    feed_id: str
    processed_uuids: set[uuid.UUID]
    start_ms: int
    end_ms: int


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
