"""Custom Apache Beam Coders for Protobuf-backed streaming state models."""

from typing import Any

import apache_beam as beam

from backend.pipeline.schema_types import streaming_state_pb2 as state_pb2
from backend.pipeline.transcription.common import datatypes


class TransmissionContextCoder(beam.coders.Coder):
    """Binary Coder for TransmissionContext (Union of IdleFeedState and ActiveStitchingState)."""

    def encode(self, value: datatypes.TransmissionContext) -> bytes:
        wrapper = state_pb2.TransmissionContextProto()
        match value:
            case datatypes.IdleFeedState():
                wrapper.idle_state.CopyFrom(value.to_proto())
            case datatypes.ActiveStitchingState():
                wrapper.active_state.CopyFrom(value.to_proto())
            case _:
                msg = f"Unexpected TransmissionContext type: {type(value)}"
                raise TypeError(msg)
        return wrapper.SerializeToString()

    def decode(self, encoded: bytes) -> datatypes.TransmissionContext:
        wrapper = state_pb2.TransmissionContextProto()
        consumed = wrapper.ParseFromString(encoded)
        if consumed != len(encoded):
            msg = f"Protobuf decode error: consumed {consumed} bytes out of {len(encoded)}"
            raise ValueError(msg)
        match wrapper.WhichOneof("context"):
            case "idle_state":
                return datatypes.IdleFeedState.from_proto(wrapper.idle_state)
            case "active_state":
                return datatypes.ActiveStitchingState.from_proto(
                    wrapper.active_state
                )
            case which:
                msg = f"Unknown TransmissionContext oneof: {which}"
                raise ValueError(msg)

    def is_deterministic(self) -> bool:
        return True

    def to_type_hint(self) -> Any:
        return datatypes.TransmissionContext

    def __eq__(self, other: object) -> bool:
        return type(self) is type(other)

    def __hash__(self) -> int:
        return hash(type(self))


class FlushRequestCoder(beam.coders.Coder):
    """Binary Coder for FlushRequest."""

    def encode(self, value: datatypes.FlushRequest) -> bytes:
        return value.to_proto().SerializeToString()

    def decode(self, encoded: bytes) -> datatypes.FlushRequest:
        proto = state_pb2.FlushRequestProto()
        consumed = proto.ParseFromString(encoded)
        if consumed != len(encoded):
            msg = f"Protobuf decode error: consumed {consumed} bytes out of {len(encoded)}"
            raise ValueError(msg)
        return datatypes.FlushRequest.from_proto(proto)

    def is_deterministic(self) -> bool:
        return True

    def to_type_hint(self) -> Any:
        return datatypes.FlushRequest

    def __eq__(self, other: object) -> bool:
        return type(self) is type(other)

    def __hash__(self) -> int:
        return hash(type(self))


class ChunkMetadataCoder(beam.coders.Coder):
    """Binary Coder for ChunkMetadata."""

    def encode(self, value: datatypes.ChunkMetadata) -> bytes:
        return value.to_proto().SerializeToString()

    def decode(self, encoded: bytes) -> datatypes.ChunkMetadata:
        proto = state_pb2.ChunkMetadataProto()
        consumed = proto.ParseFromString(encoded)
        if consumed != len(encoded):
            msg = f"Protobuf decode error: consumed {consumed} bytes out of {len(encoded)}"
            raise ValueError(msg)
        return datatypes.ChunkMetadata.from_proto(proto)

    def is_deterministic(self) -> bool:
        return True

    def to_type_hint(self) -> Any:
        return datatypes.ChunkMetadata

    def __eq__(self, other: object) -> bool:
        return type(self) is type(other)

    def __hash__(self) -> int:
        return hash(type(self))


def register_custom_coders() -> None:
    """Registers custom Coders in Beam's global coder registry."""
    beam.coders.registry.register_coder(
        datatypes.FlushRequest, FlushRequestCoder
    )
    beam.coders.registry.register_coder(
        datatypes.ChunkMetadata, ChunkMetadataCoder
    )
    beam.coders.registry.register_coder(
        datatypes.IdleFeedState, TransmissionContextCoder
    )
    beam.coders.registry.register_coder(
        datatypes.ActiveStitchingState, TransmissionContextCoder
    )
    beam.coders.registry.register_coder(
        datatypes.TransmissionContext, TransmissionContextCoder
    )
