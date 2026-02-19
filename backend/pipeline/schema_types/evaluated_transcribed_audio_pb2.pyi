from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class EvaluatedTranscribedAudio(_message.Message):
    __slots__ = ("file_path", "location", "feed", "audio_id", "start_timestamp", "end_timestamp", "transcript", "evaluation_decisions")
    class Timestamp(_message.Message):
        __slots__ = ("seconds", "nanos")
        SECONDS_FIELD_NUMBER: _ClassVar[int]
        NANOS_FIELD_NUMBER: _ClassVar[int]
        seconds: int
        nanos: int
        def __init__(self, seconds: _Optional[int] = ..., nanos: _Optional[int] = ...) -> None: ...
    FILE_PATH_FIELD_NUMBER: _ClassVar[int]
    LOCATION_FIELD_NUMBER: _ClassVar[int]
    FEED_FIELD_NUMBER: _ClassVar[int]
    AUDIO_ID_FIELD_NUMBER: _ClassVar[int]
    START_TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    END_TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    TRANSCRIPT_FIELD_NUMBER: _ClassVar[int]
    EVALUATION_DECISIONS_FIELD_NUMBER: _ClassVar[int]
    file_path: str
    location: str
    feed: str
    audio_id: str
    start_timestamp: EvaluatedTranscribedAudio.Timestamp
    end_timestamp: EvaluatedTranscribedAudio.Timestamp
    transcript: str
    evaluation_decisions: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, file_path: _Optional[str] = ..., location: _Optional[str] = ..., feed: _Optional[str] = ..., audio_id: _Optional[str] = ..., start_timestamp: _Optional[_Union[EvaluatedTranscribedAudio.Timestamp, _Mapping]] = ..., end_timestamp: _Optional[_Union[EvaluatedTranscribedAudio.Timestamp, _Mapping]] = ..., transcript: _Optional[str] = ..., evaluation_decisions: _Optional[_Iterable[str]] = ...) -> None: ...
