from __future__ import annotations

from collections.abc import AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import datetime


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
