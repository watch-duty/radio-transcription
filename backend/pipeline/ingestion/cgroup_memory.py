"""Cgroup memory controller readers for ingestion memory protection."""

from __future__ import annotations

import pathlib

_CGROUP_V2_LIMIT_PATH = pathlib.Path("/sys/fs/cgroup/memory.max")
_CGROUP_V2_USAGE_PATH = pathlib.Path("/sys/fs/cgroup/memory.current")
_CGROUP_V1_LIMIT_PATH = pathlib.Path(
    "/sys/fs/cgroup/memory/memory.limit_in_bytes",
)
_CGROUP_V1_USAGE_PATH = pathlib.Path(
    "/sys/fs/cgroup/memory/memory.usage_in_bytes",
)
_CGROUP_V1_UNBOUNDED_SENTINEL_THRESHOLD = 2**62


def resolve_container_memory_limit_bytes() -> int | None:  # noqa: PLR0911
    """
    Return the container memory limit in bytes, or None if unbounded.

    Priority order:
      1. cgroup v2 unified hierarchy (/sys/fs/cgroup/memory.max).
         Literal "max" returns None. Do not fall back to host-namespaced
         memory.
      2. cgroup v1 fallback
         (/sys/fs/cgroup/memory/memory.limit_in_bytes). Values >= 2**62
         are the v1 unbounded sentinel and return None.
      3. None on OSError, malformed/non-integer content, or a non-positive
         limit.
    """
    try:
        raw = _CGROUP_V2_LIMIT_PATH.read_text().strip()
    except OSError:
        pass
    else:
        if raw == "max":
            return None
        try:
            value = int(raw)
        except ValueError:
            return None
        return value if value > 0 else None

    try:
        raw = _CGROUP_V1_LIMIT_PATH.read_text().strip()
    except OSError:
        return None

    try:
        value = int(raw)
    except ValueError:
        return None
    if value >= _CGROUP_V1_UNBOUNDED_SENTINEL_THRESHOLD or value <= 0:
        return None
    return value


def resolve_container_memory_usage_bytes() -> int | None:
    """
    Return current container memory usage in bytes, or None on read error.

    Tries cgroup v2 (/sys/fs/cgroup/memory.current) first, then falls back
    to cgroup v1 (/sys/fs/cgroup/memory/memory.usage_in_bytes).
    """
    try:
        return int(_CGROUP_V2_USAGE_PATH.read_text().strip())
    except (OSError, ValueError):
        pass
    try:
        return int(_CGROUP_V1_USAGE_PATH.read_text().strip())
    except (OSError, ValueError):
        return None
