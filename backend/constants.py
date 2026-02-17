from enum import StrEnum


class AudioSource(StrEnum):
    """Enum representing the source of the audio data."""

    ECHO = "Echo"
    BROADCASTIFY = "Broadcastify"
    FIRE_NOTIFICATIONS = "Fire Notifications"
