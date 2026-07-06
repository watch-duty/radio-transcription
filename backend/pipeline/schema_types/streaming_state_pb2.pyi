from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class TimeRangeProto(_message.Message):
    __slots__ = ("start_ms", "end_ms")
    START_MS_FIELD_NUMBER: _ClassVar[int]
    END_MS_FIELD_NUMBER: _ClassVar[int]
    start_ms: int
    end_ms: int
    def __init__(self, start_ms: _Optional[int] = ..., end_ms: _Optional[int] = ...) -> None: ...

class FeedMetadataProto(_message.Message):
    __slots__ = ("feed_name",)
    FEED_NAME_FIELD_NUMBER: _ClassVar[int]
    feed_name: str
    def __init__(self, feed_name: _Optional[str] = ...) -> None: ...

class BufferedChunkProto(_message.Message):
    __slots__ = ("timestamp_ms", "gcs_uri", "traceparent", "baggage")
    TIMESTAMP_MS_FIELD_NUMBER: _ClassVar[int]
    GCS_URI_FIELD_NUMBER: _ClassVar[int]
    TRACEPARENT_FIELD_NUMBER: _ClassVar[int]
    BAGGAGE_FIELD_NUMBER: _ClassVar[int]
    timestamp_ms: int
    gcs_uri: str
    traceparent: str
    baggage: str
    def __init__(self, timestamp_ms: _Optional[int] = ..., gcs_uri: _Optional[str] = ..., traceparent: _Optional[str] = ..., baggage: _Optional[str] = ...) -> None: ...

class ChunkMetadataProto(_message.Message):
    __slots__ = ("gcs_uri", "session_id", "duration_ms", "feed_metadata", "traceparent", "is_continuous", "timestamp_ms", "baggage")
    GCS_URI_FIELD_NUMBER: _ClassVar[int]
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    DURATION_MS_FIELD_NUMBER: _ClassVar[int]
    FEED_METADATA_FIELD_NUMBER: _ClassVar[int]
    TRACEPARENT_FIELD_NUMBER: _ClassVar[int]
    IS_CONTINUOUS_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_MS_FIELD_NUMBER: _ClassVar[int]
    BAGGAGE_FIELD_NUMBER: _ClassVar[int]
    gcs_uri: str
    session_id: str
    duration_ms: int
    feed_metadata: FeedMetadataProto
    traceparent: str
    is_continuous: bool
    timestamp_ms: int
    baggage: str
    def __init__(self, gcs_uri: _Optional[str] = ..., session_id: _Optional[str] = ..., duration_ms: _Optional[int] = ..., feed_metadata: _Optional[_Union[FeedMetadataProto, _Mapping]] = ..., traceparent: _Optional[str] = ..., is_continuous: bool = ..., timestamp_ms: _Optional[int] = ..., baggage: _Optional[str] = ...) -> None: ...

class FlushRequestProto(_message.Message):
    __slots__ = ("buffer", "feed_id", "session_id", "contributing_audio_uris", "time_range", "feed_metadata", "sample_rate", "missing_prior_context", "missing_post_context", "start_audio_offset_ms", "end_audio_offset_ms", "speech_segments", "traceparent", "audio_classification", "segment_id", "baggage", "contributing_chunks")
    class AudioClassification(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        AUDIO_CLASSIFICATION_UNSPECIFIED: _ClassVar[FlushRequestProto.AudioClassification]
        AUDIO_CLASSIFICATION_SPEECH: _ClassVar[FlushRequestProto.AudioClassification]
        AUDIO_CLASSIFICATION_OTHER: _ClassVar[FlushRequestProto.AudioClassification]
    AUDIO_CLASSIFICATION_UNSPECIFIED: FlushRequestProto.AudioClassification
    AUDIO_CLASSIFICATION_SPEECH: FlushRequestProto.AudioClassification
    AUDIO_CLASSIFICATION_OTHER: FlushRequestProto.AudioClassification
    BUFFER_FIELD_NUMBER: _ClassVar[int]
    FEED_ID_FIELD_NUMBER: _ClassVar[int]
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    CONTRIBUTING_AUDIO_URIS_FIELD_NUMBER: _ClassVar[int]
    TIME_RANGE_FIELD_NUMBER: _ClassVar[int]
    FEED_METADATA_FIELD_NUMBER: _ClassVar[int]
    SAMPLE_RATE_FIELD_NUMBER: _ClassVar[int]
    MISSING_PRIOR_CONTEXT_FIELD_NUMBER: _ClassVar[int]
    MISSING_POST_CONTEXT_FIELD_NUMBER: _ClassVar[int]
    START_AUDIO_OFFSET_MS_FIELD_NUMBER: _ClassVar[int]
    END_AUDIO_OFFSET_MS_FIELD_NUMBER: _ClassVar[int]
    SPEECH_SEGMENTS_FIELD_NUMBER: _ClassVar[int]
    TRACEPARENT_FIELD_NUMBER: _ClassVar[int]
    AUDIO_CLASSIFICATION_FIELD_NUMBER: _ClassVar[int]
    SEGMENT_ID_FIELD_NUMBER: _ClassVar[int]
    BAGGAGE_FIELD_NUMBER: _ClassVar[int]
    CONTRIBUTING_CHUNKS_FIELD_NUMBER: _ClassVar[int]
    buffer: bytes
    feed_id: str
    session_id: str
    contributing_audio_uris: _containers.RepeatedScalarFieldContainer[str]
    time_range: TimeRangeProto
    feed_metadata: FeedMetadataProto
    sample_rate: int
    missing_prior_context: bool
    missing_post_context: bool
    start_audio_offset_ms: int
    end_audio_offset_ms: int
    speech_segments: _containers.RepeatedCompositeFieldContainer[TimeRangeProto]
    traceparent: str
    audio_classification: FlushRequestProto.AudioClassification
    segment_id: str
    baggage: str
    contributing_chunks: _containers.RepeatedCompositeFieldContainer[BufferedChunkProto]
    def __init__(self, buffer: _Optional[bytes] = ..., feed_id: _Optional[str] = ..., session_id: _Optional[str] = ..., contributing_audio_uris: _Optional[_Iterable[str]] = ..., time_range: _Optional[_Union[TimeRangeProto, _Mapping]] = ..., feed_metadata: _Optional[_Union[FeedMetadataProto, _Mapping]] = ..., sample_rate: _Optional[int] = ..., missing_prior_context: bool = ..., missing_post_context: bool = ..., start_audio_offset_ms: _Optional[int] = ..., end_audio_offset_ms: _Optional[int] = ..., speech_segments: _Optional[_Iterable[_Union[TimeRangeProto, _Mapping]]] = ..., traceparent: _Optional[str] = ..., audio_classification: _Optional[_Union[FlushRequestProto.AudioClassification, str]] = ..., segment_id: _Optional[str] = ..., baggage: _Optional[str] = ..., contributing_chunks: _Optional[_Iterable[_Union[BufferedChunkProto, _Mapping]]] = ...) -> None: ...

class IdleFeedStateProto(_message.Message):
    __slots__ = ("out_of_order_buffer", "order_timer_active")
    OUT_OF_ORDER_BUFFER_FIELD_NUMBER: _ClassVar[int]
    ORDER_TIMER_ACTIVE_FIELD_NUMBER: _ClassVar[int]
    out_of_order_buffer: _containers.RepeatedCompositeFieldContainer[BufferedChunkProto]
    order_timer_active: bool
    def __init__(self, out_of_order_buffer: _Optional[_Iterable[_Union[BufferedChunkProto, _Mapping]]] = ..., order_timer_active: bool = ...) -> None: ...

class ActiveStitchingStateProto(_message.Message):
    __slots__ = ("session_id", "feed_metadata", "last_end_time_ms", "stale_start_time_ms", "buffer_start_time_ms", "expected_next_chunk_start_ms", "start_audio_offset_ms", "end_audio_offset_ms", "contributing_audio_uris", "missing_prior_context", "missing_post_context", "buffer_duration_ms", "order_timer_active", "out_of_order_buffer", "last_transmission_start_ms", "speech_segments", "prior_audio_tail", "traceparent", "sample_rate", "baggage", "contributing_chunks")
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    FEED_METADATA_FIELD_NUMBER: _ClassVar[int]
    LAST_END_TIME_MS_FIELD_NUMBER: _ClassVar[int]
    STALE_START_TIME_MS_FIELD_NUMBER: _ClassVar[int]
    BUFFER_START_TIME_MS_FIELD_NUMBER: _ClassVar[int]
    EXPECTED_NEXT_CHUNK_START_MS_FIELD_NUMBER: _ClassVar[int]
    START_AUDIO_OFFSET_MS_FIELD_NUMBER: _ClassVar[int]
    END_AUDIO_OFFSET_MS_FIELD_NUMBER: _ClassVar[int]
    CONTRIBUTING_AUDIO_URIS_FIELD_NUMBER: _ClassVar[int]
    MISSING_PRIOR_CONTEXT_FIELD_NUMBER: _ClassVar[int]
    MISSING_POST_CONTEXT_FIELD_NUMBER: _ClassVar[int]
    BUFFER_DURATION_MS_FIELD_NUMBER: _ClassVar[int]
    ORDER_TIMER_ACTIVE_FIELD_NUMBER: _ClassVar[int]
    OUT_OF_ORDER_BUFFER_FIELD_NUMBER: _ClassVar[int]
    LAST_TRANSMISSION_START_MS_FIELD_NUMBER: _ClassVar[int]
    SPEECH_SEGMENTS_FIELD_NUMBER: _ClassVar[int]
    PRIOR_AUDIO_TAIL_FIELD_NUMBER: _ClassVar[int]
    TRACEPARENT_FIELD_NUMBER: _ClassVar[int]
    SAMPLE_RATE_FIELD_NUMBER: _ClassVar[int]
    BAGGAGE_FIELD_NUMBER: _ClassVar[int]
    CONTRIBUTING_CHUNKS_FIELD_NUMBER: _ClassVar[int]
    session_id: str
    feed_metadata: FeedMetadataProto
    last_end_time_ms: int
    stale_start_time_ms: int
    buffer_start_time_ms: int
    expected_next_chunk_start_ms: int
    start_audio_offset_ms: int
    end_audio_offset_ms: int
    contributing_audio_uris: _containers.RepeatedScalarFieldContainer[str]
    missing_prior_context: bool
    missing_post_context: bool
    buffer_duration_ms: int
    order_timer_active: bool
    out_of_order_buffer: _containers.RepeatedCompositeFieldContainer[BufferedChunkProto]
    last_transmission_start_ms: int
    speech_segments: _containers.RepeatedCompositeFieldContainer[TimeRangeProto]
    prior_audio_tail: bytes
    traceparent: str
    sample_rate: int
    baggage: str
    contributing_chunks: _containers.RepeatedCompositeFieldContainer[BufferedChunkProto]
    def __init__(self, session_id: _Optional[str] = ..., feed_metadata: _Optional[_Union[FeedMetadataProto, _Mapping]] = ..., last_end_time_ms: _Optional[int] = ..., stale_start_time_ms: _Optional[int] = ..., buffer_start_time_ms: _Optional[int] = ..., expected_next_chunk_start_ms: _Optional[int] = ..., start_audio_offset_ms: _Optional[int] = ..., end_audio_offset_ms: _Optional[int] = ..., contributing_audio_uris: _Optional[_Iterable[str]] = ..., missing_prior_context: bool = ..., missing_post_context: bool = ..., buffer_duration_ms: _Optional[int] = ..., order_timer_active: bool = ..., out_of_order_buffer: _Optional[_Iterable[_Union[BufferedChunkProto, _Mapping]]] = ..., last_transmission_start_ms: _Optional[int] = ..., speech_segments: _Optional[_Iterable[_Union[TimeRangeProto, _Mapping]]] = ..., prior_audio_tail: _Optional[bytes] = ..., traceparent: _Optional[str] = ..., sample_rate: _Optional[int] = ..., baggage: _Optional[str] = ..., contributing_chunks: _Optional[_Iterable[_Union[BufferedChunkProto, _Mapping]]] = ...) -> None: ...

class TransmissionContextProto(_message.Message):
    __slots__ = ("idle_state", "active_state")
    IDLE_STATE_FIELD_NUMBER: _ClassVar[int]
    ACTIVE_STATE_FIELD_NUMBER: _ClassVar[int]
    idle_state: IdleFeedStateProto
    active_state: ActiveStitchingStateProto
    def __init__(self, idle_state: _Optional[_Union[IdleFeedStateProto, _Mapping]] = ..., active_state: _Optional[_Union[ActiveStitchingStateProto, _Mapping]] = ...) -> None: ...
