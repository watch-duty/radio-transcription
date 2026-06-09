"""Custom Apache Beam Coders for Protobuf-backed streaming state models."""

from typing import Any

import apache_beam as beam
import betterproto

from backend.pipeline.schema_types import (
    streaming_state as bp_state,
)
from backend.pipeline.segmentation import datatypes


class TransmissionContextCoder(beam.coders.Coder):
    """Binary Coder for TransmissionContext (Union of IdleFeedState and ActiveStitchingState).

    Note: Registering this Coder against the concrete subtypes (IdleFeedState, ActiveStitchingState)
    allows Beam to directly encode those concrete types when they are yielded.
    The `decode` method correctly returns whatever concrete type is found in the union payload,
    so pipeline elements will naturally emerge as their concrete types rather than a union type hint.
    """

    def encode(self, value: datatypes.TransmissionContext) -> bytes:
        wrapper = bp_state.TransmissionContextProto()
        match value:
            case datatypes.IdleFeedState():
                wrapper.idle_state = value
            case datatypes.ActiveStitchingState():
                wrapper.active_state = value
            case _:
                msg = f"Unexpected TransmissionContext type: {type(value)}"
                raise TypeError(msg)
        return bytes(wrapper)

    def decode(self, encoded: bytes) -> datatypes.TransmissionContext:
        wrapper = bp_state.TransmissionContextProto().parse(encoded)
        field_name, value = betterproto.which_one_of(wrapper, "context")
        if not field_name or value is None:
            msg = "Protobuf decode error: invalid or empty TransmissionContext payload"
            raise ValueError(msg)
        return value

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
        return bytes(value)

    def decode(self, encoded: bytes) -> datatypes.FlushRequest:
        decoded = datatypes.FlushRequest().parse(encoded)
        if not decoded.feed_id:
            msg = "Protobuf decode error: invalid or empty FlushRequest payload"
            raise ValueError(msg)
        return decoded

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
        return bytes(value)

    def decode(self, encoded: bytes) -> datatypes.ChunkMetadata:
        decoded = datatypes.ChunkMetadata().parse(encoded)
        if not decoded.gcs_uri or not decoded.session_id:
            msg = (
                "Protobuf decode error: invalid or empty ChunkMetadata payload"
            )
            raise ValueError(msg)
        return decoded

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
