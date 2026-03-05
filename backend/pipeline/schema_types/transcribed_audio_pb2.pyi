from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class TranscribedAudio(_message.Message):
    __slots__ = ("file_path", "source", "feed_name", "feed_id", "audio_id", "start_timestamp", "end_timestamp", "transcript", "context")
    class Timestamp(_message.Message):
        __slots__ = ("seconds", "nanos")
        SECONDS_FIELD_NUMBER: _ClassVar[int]
        NANOS_FIELD_NUMBER: _ClassVar[int]
        seconds: int
        nanos: int
        def __init__(self, seconds: _Optional[int] = ..., nanos: _Optional[int] = ...) -> None: ...
    class ContextEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    FILE_PATH_FIELD_NUMBER: _ClassVar[int]
    SOURCE_FIELD_NUMBER: _ClassVar[int]
    FEED_NAME_FIELD_NUMBER: _ClassVar[int]
    FEED_ID_FIELD_NUMBER: _ClassVar[int]
    AUDIO_ID_FIELD_NUMBER: _ClassVar[int]
    START_TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    END_TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    TRANSCRIPT_FIELD_NUMBER: _ClassVar[int]
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    file_path: str
    source: str
    feed_name: str
    feed_id: str
    audio_id: str
    start_timestamp: TranscribedAudio.Timestamp
    end_timestamp: TranscribedAudio.Timestamp
    transcript: str
    context: _containers.ScalarMap[str, str]
    def __init__(self, file_path: _Optional[str] = ..., source: _Optional[str] = ..., feed_name: _Optional[str] = ..., feed_id: _Optional[str] = ..., audio_id: _Optional[str] = ..., start_timestamp: _Optional[_Union[TranscribedAudio.Timestamp, _Mapping]] = ..., end_timestamp: _Optional[_Union[TranscribedAudio.Timestamp, _Mapping]] = ..., transcript: _Optional[str] = ..., context: _Optional[_Mapping[str, str]] = ...) -> None: ...
