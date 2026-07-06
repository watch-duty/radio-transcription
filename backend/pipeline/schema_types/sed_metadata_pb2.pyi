import datetime

from google.protobuf import duration_pb2 as _duration_pb2
from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class SedMetadata(_message.Message):
    __slots__ = ("source_chunk_id", "sound_events", "start_timestamp")
    SOURCE_CHUNK_ID_FIELD_NUMBER: _ClassVar[int]
    SOUND_EVENTS_FIELD_NUMBER: _ClassVar[int]
    START_TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    source_chunk_id: str
    sound_events: _containers.RepeatedCompositeFieldContainer[SoundEvent]
    start_timestamp: _timestamp_pb2.Timestamp
    def __init__(self, source_chunk_id: _Optional[str] = ..., sound_events: _Optional[_Iterable[_Union[SoundEvent, _Mapping]]] = ..., start_timestamp: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class SoundEvent(_message.Message):
    __slots__ = ("start_time", "duration")
    START_TIME_FIELD_NUMBER: _ClassVar[int]
    DURATION_FIELD_NUMBER: _ClassVar[int]
    start_time: _duration_pb2.Duration
    duration: _duration_pb2.Duration
    def __init__(self, start_time: _Optional[_Union[datetime.timedelta, _duration_pb2.Duration, _Mapping]] = ..., duration: _Optional[_Union[datetime.timedelta, _duration_pb2.Duration, _Mapping]] = ...) -> None: ...
