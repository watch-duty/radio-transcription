import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class AudioChunk(_message.Message):
    __slots__ = ("gcs_uri", "start_timestamp", "session_id", "duration_ms", "feed_name", "feed_id", "external_audio_segment_id")
    GCS_URI_FIELD_NUMBER: _ClassVar[int]
    START_TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    DURATION_MS_FIELD_NUMBER: _ClassVar[int]
    FEED_NAME_FIELD_NUMBER: _ClassVar[int]
    FEED_ID_FIELD_NUMBER: _ClassVar[int]
    EXTERNAL_AUDIO_SEGMENT_ID_FIELD_NUMBER: _ClassVar[int]
    gcs_uri: str
    start_timestamp: _timestamp_pb2.Timestamp
    session_id: str
    duration_ms: int
    feed_name: str
    feed_id: str
    external_audio_segment_id: str
    def __init__(self, gcs_uri: _Optional[str] = ..., start_timestamp: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., session_id: _Optional[str] = ..., duration_ms: _Optional[int] = ..., feed_name: _Optional[str] = ..., feed_id: _Optional[str] = ..., external_audio_segment_id: _Optional[str] = ...) -> None: ...
