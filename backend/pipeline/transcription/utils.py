"""Utility functions for the radio transcription pipeline."""

import logging
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
