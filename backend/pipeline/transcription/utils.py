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


def generate_transmission_id(feed_id: str, start_ms: int, end_ms: int) -> str:
    """Creates a deterministic UUID string using uuid5 to ensure pipeline retries produce the exact same ID."""
    deterministic_id = f"{feed_id}_{start_ms}_{end_ms}"
    return str(uuid.uuid5(uuid.NAMESPACE_OID, deterministic_id))
