"""Common utilities for all pipeline packages."""

import logging
import os
import uuid
from typing import Self

import pydantic

from backend.pipeline.common.env import is_gcp_env

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


def get_optimal_thread_pool_size(
    env_var: str, default_threads_per_core: int = 4
) -> int:
    """Determine thread pool size based on env override or CPU cores."""
    env_val = os.getenv(env_var)
    if env_val:
        try:
            return max(1, int(env_val))
        except ValueError:
            logger.warning(
                "Invalid %s value: %s, falling back to dynamic calculation",
                env_var,
                env_val,
            )

    if not is_gcp_env():
        return 16

    try:
        sched_getaffinity = getattr(os, "sched_getaffinity", None)
        if sched_getaffinity:
            core_count = len(sched_getaffinity(0))
        else:
            core_count = os.cpu_count() or 4
    except (AttributeError, NotImplementedError):
        core_count = os.cpu_count() or 4

    return max(4, min(32, core_count * default_threads_per_core))
