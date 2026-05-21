from __future__ import annotations

from datetime import datetime, timedelta  # noqa: TC003
from enum import StrEnum

from pydantic import BaseModel, Field


class AudioClassification(StrEnum):
    """Enum for audio segment classification."""

    SPEECH_DETECTED = "SPEECH_DETECTED"
    UNCLASSIFIED = "UNCLASSIFIED"


class AnnotationType(StrEnum):
    """Enum for annotation type."""

    TRANSCRIPT = "TRANSCRIPT"
    EVALUATION = "EVALUATION"


class Annotation(BaseModel):
    """Model for an annotation."""

    audio_segment_id: str
    type: AnnotationType
    data: dict
    created_at: datetime
    updated_at: datetime


class AudioSegment(BaseModel):
    """Model for an audio segment with its annotations."""

    id: str
    feed_id: str
    classification: AudioClassification
    start_timestamp: datetime
    end_timestamp: datetime
    missing_prior_context: bool
    missing_post_context: bool
    source_audio_uris: list[str]
    canonical_audio_uri: str | None = None
    start_audio_offset: timedelta | None = None
    end_audio_offset: timedelta | None = None
    playback_audio_uri: str | None = None
    created_at: datetime
    annotations: list[Annotation] = Field(default_factory=list)
