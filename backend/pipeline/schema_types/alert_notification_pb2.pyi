import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf import duration_pb2 as _duration_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class AlertNotification(_message.Message):
    __slots__ = ("feed_id", "segment_id", "transcript", "start_timestamp", "end_timestamp", "missing_prior_context", "missing_post_context", "source_audio_uris", "canonical_audio_uri", "start_audio_offset", "end_audio_offset", "evaluation_decisions", "playback_audio_uri", "app_url", "feed_name", "tags", "evaluation_errors")
    class Tag(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    FEED_ID_FIELD_NUMBER: _ClassVar[int]
    SEGMENT_ID_FIELD_NUMBER: _ClassVar[int]
    TRANSCRIPT_FIELD_NUMBER: _ClassVar[int]
    START_TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    END_TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    MISSING_PRIOR_CONTEXT_FIELD_NUMBER: _ClassVar[int]
    MISSING_POST_CONTEXT_FIELD_NUMBER: _ClassVar[int]
    SOURCE_AUDIO_URIS_FIELD_NUMBER: _ClassVar[int]
    CANONICAL_AUDIO_URI_FIELD_NUMBER: _ClassVar[int]
    START_AUDIO_OFFSET_FIELD_NUMBER: _ClassVar[int]
    END_AUDIO_OFFSET_FIELD_NUMBER: _ClassVar[int]
    EVALUATION_DECISIONS_FIELD_NUMBER: _ClassVar[int]
    PLAYBACK_AUDIO_URI_FIELD_NUMBER: _ClassVar[int]
    APP_URL_FIELD_NUMBER: _ClassVar[int]
    FEED_NAME_FIELD_NUMBER: _ClassVar[int]
    TAGS_FIELD_NUMBER: _ClassVar[int]
    EVALUATION_ERRORS_FIELD_NUMBER: _ClassVar[int]
    feed_id: str
    segment_id: str
    transcript: str
    start_timestamp: _timestamp_pb2.Timestamp
    end_timestamp: _timestamp_pb2.Timestamp
    missing_prior_context: bool
    missing_post_context: bool
    source_audio_uris: _containers.RepeatedScalarFieldContainer[str]
    canonical_audio_uri: str
    start_audio_offset: _duration_pb2.Duration
    end_audio_offset: _duration_pb2.Duration
    evaluation_decisions: _containers.RepeatedScalarFieldContainer[str]
    playback_audio_uri: str
    app_url: str
    feed_name: str
    tags: _containers.RepeatedCompositeFieldContainer[AlertNotification.Tag]
    evaluation_errors: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, feed_id: _Optional[str] = ..., segment_id: _Optional[str] = ..., transcript: _Optional[str] = ..., start_timestamp: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., end_timestamp: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., missing_prior_context: bool = ..., missing_post_context: bool = ..., source_audio_uris: _Optional[_Iterable[str]] = ..., canonical_audio_uri: _Optional[str] = ..., start_audio_offset: _Optional[_Union[datetime.timedelta, _duration_pb2.Duration, _Mapping]] = ..., end_audio_offset: _Optional[_Union[datetime.timedelta, _duration_pb2.Duration, _Mapping]] = ..., evaluation_decisions: _Optional[_Iterable[str]] = ..., playback_audio_uri: _Optional[str] = ..., app_url: _Optional[str] = ..., feed_name: _Optional[str] = ..., tags: _Optional[_Iterable[_Union[AlertNotification.Tag, _Mapping]]] = ..., evaluation_errors: _Optional[_Iterable[str]] = ...) -> None: ...
