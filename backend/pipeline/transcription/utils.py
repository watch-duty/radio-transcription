"""
Utility functions for the radio transcription pipeline.
"""

import dataclasses
import json
import logging
from typing import Self

logger = logging.getLogger(__name__)


class JsonConfigMixin:
    """Provides a classmethod to parse JSON into a dataclass, filtering out unknown keys."""

    @classmethod
    def from_json(cls, json_str: str) -> Self:
        if not json_str:
            return cls()
        try:
            config_dict = json.loads(json_str)
            if not isinstance(config_dict, dict):
                msg = f"Expected a JSON object (dict), got {type(config_dict).__name__}"
                raise TypeError(msg)
            # Filter out unknown keys to prevent TypeError on instantiation
            valid_keys = {f.name for f in dataclasses.fields(cls)}
            filtered_dict = {k: v for k, v in config_dict.items() if k in valid_keys}
            return cls(**filtered_dict)
        except json.JSONDecodeError as e:
            logger.exception(
                "Failed to parse config JSON for %s: %s", cls.__name__, json_str
            )
            msg = f"Invalid config JSON for {cls.__name__}: {e}"
            raise ValueError(msg) from e
