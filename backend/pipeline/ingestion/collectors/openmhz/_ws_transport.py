from __future__ import annotations

import asyncio
import datetime
import json
import logging
import time
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any

from backend.pipeline.ingestion.collectors.openmhz._types import CallEvent

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

logger = logging.getLogger(__name__)

_START_PAYLOAD_TEMPLATE: dict[str, object] = {
    "filterCode": "",
    "filterType": "all",
    "filterName": "OpenMHz",
    "filterStarred": False,
}


def _parse_eio_open(frame: str) -> dict[str, Any]:
    """Parse Engine.IO v4 open packet ``0{...}``."""
    if not frame.startswith("0"):
        msg = f"Expected EIO open packet (0{{...}}), got: {frame[:60]}"
        raise ValueError(msg)
    return json.loads(frame[1:])


def _parse_sio_event(frame: str) -> CallEvent | None:
    """Parse Socket.IO v4 event ``42["new message","<json>"]``.

    Returns ``None`` for non-event frames or unknown event names.
    Double-parses: outer JSON array, then inner JSON string.
    """
    if not frame.startswith("42"):
        return None
    array = json.loads(frame[2:])
    if not isinstance(array, list) or len(array) < 2:
        return None
    if array[0] != "new message":
        return None
    call: dict[str, Any] = json.loads(array[1])
    return CallEvent(
        id=call["_id"],
        talkgroup_num=call["talkgroupNum"],
        url=call["url"],
        time=datetime.datetime.fromisoformat(call["time"]),
        length_sec=call["len"],
        freq=call["freq"],
        src_list=call.get("srcList", []),
        short_name=call.get("shortName", ""),
        emergency=call.get("emergency", False),
    )
