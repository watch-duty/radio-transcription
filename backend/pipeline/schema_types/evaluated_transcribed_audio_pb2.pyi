import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf import duration_pb2 as _duration_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class EvaluatedTranscribedAudio(_message.Message):
    __slots__ = ("feed_id", "segment_id", "transcript", "start_timestamp", "end_timestamp", "missing_prior_context", "missing_post_context", "source_audio_uris", "start_audio_offset", "end_audio_offset", "canonical_audio_uri", "playback_audio_uri", "evaluation_decisions", "errors", "feed_name", "rule_annotations")
    class TextMatchSpan(_message.Message):
        __slots__ = ("start", "end", "matched_text")
        START_FIELD_NUMBER: _ClassVar[int]
        END_FIELD_NUMBER: _ClassVar[int]
        MATCHED_TEXT_FIELD_NUMBER: _ClassVar[int]
        start: int
        end: int
        matched_text: str
        def __init__(self, start: _Optional[int] = ..., end: _Optional[int] = ..., matched_text: _Optional[str] = ...) -> None: ...
    class TextMatchAnnotation(_message.Message):
        __slots__ = ("spans",)
        SPANS_FIELD_NUMBER: _ClassVar[int]
        spans: _containers.RepeatedCompositeFieldContainer[EvaluatedTranscribedAudio.TextMatchSpan]
        def __init__(self, spans: _Optional[_Iterable[_Union[EvaluatedTranscribedAudio.TextMatchSpan, _Mapping]]] = ...) -> None: ...
    class RuleAnnotation(_message.Message):
        __slots__ = ("text_match",)
        TEXT_MATCH_FIELD_NUMBER: _ClassVar[int]
        text_match: EvaluatedTranscribedAudio.TextMatchAnnotation
        def __init__(self, text_match: _Optional[_Union[EvaluatedTranscribedAudio.TextMatchAnnotation, _Mapping]] = ...) -> None: ...
    class RuleAnnotationsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: EvaluatedTranscribedAudio.RuleAnnotation
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[EvaluatedTranscribedAudio.RuleAnnotation, _Mapping]] = ...) -> None: ...
    FEED_ID_FIELD_NUMBER: _ClassVar[int]
    SEGMENT_ID_FIELD_NUMBER: _ClassVar[int]
    TRANSCRIPT_FIELD_NUMBER: _ClassVar[int]
    START_TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    END_TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    MISSING_PRIOR_CONTEXT_FIELD_NUMBER: _ClassVar[int]
    MISSING_POST_CONTEXT_FIELD_NUMBER: _ClassVar[int]
    SOURCE_AUDIO_URIS_FIELD_NUMBER: _ClassVar[int]
    START_AUDIO_OFFSET_FIELD_NUMBER: _ClassVar[int]
    END_AUDIO_OFFSET_FIELD_NUMBER: _ClassVar[int]
    CANONICAL_AUDIO_URI_FIELD_NUMBER: _ClassVar[int]
    PLAYBACK_AUDIO_URI_FIELD_NUMBER: _ClassVar[int]
    EVALUATION_DECISIONS_FIELD_NUMBER: _ClassVar[int]
    ERRORS_FIELD_NUMBER: _ClassVar[int]
    FEED_NAME_FIELD_NUMBER: _ClassVar[int]
    RULE_ANNOTATIONS_FIELD_NUMBER: _ClassVar[int]
    feed_id: str
    segment_id: str
    transcript: str
    start_timestamp: _timestamp_pb2.Timestamp
    end_timestamp: _timestamp_pb2.Timestamp
    missing_prior_context: bool
    missing_post_context: bool
    source_audio_uris: _containers.RepeatedScalarFieldContainer[str]
    start_audio_offset: _duration_pb2.Duration
    end_audio_offset: _duration_pb2.Duration
    canonical_audio_uri: str
    playback_audio_uri: str
    evaluation_decisions: _containers.RepeatedScalarFieldContainer[str]
    errors: _containers.RepeatedScalarFieldContainer[str]
    feed_name: str
    rule_annotations: _containers.MessageMap[str, EvaluatedTranscribedAudio.RuleAnnotation]
    def __init__(self, feed_id: _Optional[str] = ..., segment_id: _Optional[str] = ..., transcript: _Optional[str] = ..., start_timestamp: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., end_timestamp: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., missing_prior_context: bool = ..., missing_post_context: bool = ..., source_audio_uris: _Optional[_Iterable[str]] = ..., start_audio_offset: _Optional[_Union[datetime.timedelta, _duration_pb2.Duration, _Mapping]] = ..., end_audio_offset: _Optional[_Union[datetime.timedelta, _duration_pb2.Duration, _Mapping]] = ..., canonical_audio_uri: _Optional[str] = ..., playback_audio_uri: _Optional[str] = ..., evaluation_decisions: _Optional[_Iterable[str]] = ..., errors: _Optional[_Iterable[str]] = ..., feed_name: _Optional[str] = ..., rule_annotations: _Optional[_Mapping[str, EvaluatedTranscribedAudio.RuleAnnotation]] = ...) -> None: ...
