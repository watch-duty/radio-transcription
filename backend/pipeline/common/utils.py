"""Common utilities for all pipeline packages."""

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


def generate_segment_id(
    feed_or_session_id: str,
    unique_suffix: str,
) -> str:
    """Generates a deterministic UUID5 using feed/session and a unique
    identifier suffix.
    """
    deterministic_id = f"{feed_or_session_id}_{unique_suffix}"
    return str(uuid.uuid5(uuid.NAMESPACE_OID, deterministic_id))
