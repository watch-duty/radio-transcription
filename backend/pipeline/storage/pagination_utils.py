from __future__ import annotations

import base64
import datetime
import uuid
from enum import StrEnum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import collections.abc
    import typing


class SortOrder(StrEnum):
    ASC = "asc"
    DESC = "desc"


def decode_cursor(
    next_token: str,
) -> tuple[datetime.datetime, uuid.UUID]:
    """Decode a base64 pagination token into a timestamp and UUID."""
    try:
        decoded = base64.b64decode(next_token).decode("utf-8")
        ts_str, uid_str = decoded.split("|")
        return datetime.datetime.fromisoformat(ts_str), uuid.UUID(uid_str)
    except Exception as e:
        msg = f"Invalid next_token: {e}"
        raise ValueError(msg)


def decode_int_cursor(
    next_token: str,
) -> tuple[datetime.datetime, int]:
    """Decode a base64 pagination token into a timestamp and integer."""
    try:
        decoded = base64.b64decode(next_token).decode("utf-8")
        ts_str, int_str = decoded.split("|")
        return datetime.datetime.fromisoformat(ts_str), int(int_str)
    except Exception as e:
        msg = f"Invalid next_token: {e}"
        raise ValueError(msg)


def encode_cursor(ts: datetime.datetime, key: uuid.UUID | int) -> str:
    """Encode a timestamp and a UUID or integer key into a base64 pagination token."""
    token_str = f"{ts.isoformat()}|{key}"
    return base64.b64encode(token_str.encode("utf-8")).decode("utf-8")


def get_paginated_results(
    rows: collections.abc.Sequence[typing.Any],
    limit: int,
    timestamp_key: str,
    id_key: str,
) -> tuple[collections.abc.Sequence[typing.Any], str | None]:
    """Slice rows exceeding limit and generate a pagination token based on timestamp and ID."""
    if len(rows) > limit:
        sliced_rows = rows[:limit]
        last_row = sliced_rows[-1]
        token = encode_cursor(last_row[timestamp_key], last_row[id_key])
        return sliced_rows, token
    return rows, None
