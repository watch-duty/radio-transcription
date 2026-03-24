from __future__ import annotations

import os
from dataclasses import dataclass, field, replace
from typing import Any


def _require_env(name: str) -> str:
    """Return an environment variable or raise if unset."""
    value = os.environ.get(name)
    if not value:
        msg = f"Required environment variable {name} is not set"
        raise ValueError(msg)
    return value


@dataclass(frozen=True, kw_only=True)
class AlloyDBSettings:
    """
    Configuration for AlloyDB connection and pooling, loaded from environment variables.
    """

    host: str = field(
        default_factory=lambda: os.environ.get("ALLOYDB_HOST", "localhost"),
    )
    port: int = field(
        default_factory=lambda: int(os.environ.get("ALLOYDB_PORT", "6432")),
    )
    user: str = field(
        default_factory=lambda: os.environ.get("ALLOYDB_USER", "postgres"),
    )
    db_name: str = field(
        default_factory=lambda: os.environ.get("ALLOYDB_DB", "postgres"),
    )
    password: str = field(
        default_factory=lambda: os.environ.get("ALLOYDB_PASSWORD", ""),
    )

    pool_min_size: int = field(
        default_factory=lambda: int(
            os.environ.get("ALLOYDB_POOL_MIN_SIZE", "5")
        ),
    )
    pool_max_size: int = field(
        default_factory=lambda: int(
            os.environ.get("ALLOYDB_POOL_MAX_SIZE", "5")
        ),
    )

    command_timeout_sec: float = field(
        default_factory=lambda: float(
            os.environ.get("ALLOYDB_COMMAND_TIMEOUT_SEC", "30.0")
        ),
    )
    connect_timeout_sec: float = field(
        default_factory=lambda: float(
            os.environ.get("ALLOYDB_CONNECT_TIMEOUT_SEC", "10.0")
        ),
    )

    def replace(self, **kwargs: Any) -> AlloyDBSettings:
        """Return a new instance with replaced fields."""
        return replace(self, **kwargs)
