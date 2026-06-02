from __future__ import annotations

from enum import StrEnum
import base64
import datetime
import uuid


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


def encode_cursor(ts: datetime.datetime, uid: uuid.UUID) -> str:
    """Encode a timestamp and UUID into a base64 pagination token."""
    token_str = f"{ts.isoformat()}|{uid}"
    return base64.b64encode(token_str.encode("utf-8")).decode("utf-8")
