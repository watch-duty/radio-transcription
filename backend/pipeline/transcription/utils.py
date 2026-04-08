"""Utility functions for the radio transcription pipeline."""

import logging
import uuid
from typing import Self

import pydantic

logger = logging.getLogger(__name__)


class ConfigBase(pydantic.BaseModel):
    """Base Pydantic model for JSON configuration classes."""

    model_config = pydantic.ConfigDict(extra="ignore", frozen=True)

    @classmethod
    def from_json(cls, json_str: str) -> Self:
        """Creates an instance from a JSON string."""
        if not json_str:
            return cls()
        try:
            return cls.model_validate_json(json_str)
        except pydantic.ValidationError as e:
            logger.exception(
                "Failed to parse config JSON for %s: %s", cls.__name__, json_str
            )
            msg = f"Invalid config JSON for {cls.__name__}: {e}"
            raise ValueError(msg) from e


def generate_transmission_id(feed_id: str, contributing_audio_uris: list[str]) -> str:
    """Creates a deterministic UUID from the feed and its contributing source audio URIs.

    Anchored on the actual GCS URIs rather than derived timing values, making the ID
    stable across VAD configuration changes, pre-roll tuning, and pipeline replays
    with consistent stitcher state. URIs are kept in their natural chronological
    order (guaranteed upstream by RestoreOrderFn) so the ID reflects the actual
    audio sequence.
    """
    deterministic_id = f"{feed_id}_{'|'.join(contributing_audio_uris)}"
    return str(uuid.uuid5(uuid.NAMESPACE_OID, deterministic_id))
