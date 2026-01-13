from dataclasses import dataclass
from enum import Enum


class BroadcastifyFeedGenre(Enum):
    PUBLIC_SAFETY = 1
    AVIATION = 2
    RAIL = 3
    AMATEUR_RADIO = 4
    MARINE = 5
    OTHER = 6
    SPECIAL_EVENT = 7
    DISASTER_EVENT = 8


@dataclass(slots=True)
class BroadcastifyFeed:
    feedId: int
    descr: str
    sdescr: str
    genreId: int
    status: int
    online: int
    tactical: bool
    listeners: int
    relay_server: str
    relay_mount: str
    trim_audio: bool
    archive_feed: bool
    last_alert: str
    last_alert_ts: int
    fbo: bool
    official: bool
    bitrate_mode: str
    channel_mode: str
    data_format: str
    sample_rate: str
    bitrate: str
    channels: str
    first_online: int
    last_check: int
    last_good: int
    last_return: int
    counties: list[int]
