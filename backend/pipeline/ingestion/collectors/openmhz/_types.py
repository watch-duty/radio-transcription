from __future__ import annotations

import datetime
from collections.abc import AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class CallEvent:
    """A single call notification from OpenMHZ."""

    id: str
    talkgroup_num: int
    url: str
    time: datetime.datetime
    length_sec: int
    freq: int
    src_list: list[dict]
    short_name: str
    emergency: bool


TransportFactory = Callable[
    ...,
    AbstractAsyncContextManager[AsyncIterator[CallEvent]],
]
