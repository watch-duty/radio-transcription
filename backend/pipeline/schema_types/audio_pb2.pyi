from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class AudioChunk(_message.Message):
    __slots__ = ("gcs_file_path",)
    GCS_FILE_PATH_FIELD_NUMBER: _ClassVar[int]
    gcs_file_path: str
    def __init__(self, gcs_file_path: _Optional[str] = ...) -> None: ...

class SadMetadata(_message.Message):
    __slots__ = ("source_chunk_id", "processed_at", "segments")
    class Timestamp(_message.Message):
        __slots__ = ("seconds", "nanos")
        SECONDS_FIELD_NUMBER: _ClassVar[int]
        NANOS_FIELD_NUMBER: _ClassVar[int]
        seconds: int
        nanos: int
        def __init__(self, seconds: _Optional[int] = ..., nanos: _Optional[int] = ...) -> None: ...
    SOURCE_CHUNK_ID_FIELD_NUMBER: _ClassVar[int]
    PROCESSED_AT_FIELD_NUMBER: _ClassVar[int]
    SEGMENTS_FIELD_NUMBER: _ClassVar[int]
    source_chunk_id: str
    processed_at: SadMetadata.Timestamp
    segments: _containers.RepeatedCompositeFieldContainer[SpeechSegment]
    def __init__(self, source_chunk_id: _Optional[str] = ..., processed_at: _Optional[_Union[SadMetadata.Timestamp, _Mapping]] = ..., segments: _Optional[_Iterable[_Union[SpeechSegment, _Mapping]]] = ...) -> None: ...

class SpeechSegment(_message.Message):
    __slots__ = ("start_sec", "end_sec")
    START_SEC_FIELD_NUMBER: _ClassVar[int]
    END_SEC_FIELD_NUMBER: _ClassVar[int]
    start_sec: float
    end_sec: float
    def __init__(self, start_sec: _Optional[float] = ..., end_sec: _Optional[float] = ...) -> None: ...
