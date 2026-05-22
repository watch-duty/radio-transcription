from __future__ import annotations

from datetime import datetime, timedelta  # noqa: TC003
from enum import StrEnum
from typing import Annotated, Literal, Union

from pydantic import BaseModel, Field


class AudioClassification(StrEnum):
    """Enum for audio segment classification."""

    SPEECH_DETECTED = "SPEECH_DETECTED"
    UNCLASSIFIED = "UNCLASSIFIED"


class AnnotationType(StrEnum):
    """Enum for annotation type."""

    TRANSCRIPT = "TRANSCRIPT"
    EVALUATION = "EVALUATION"


class TranscriptAnnotationData(BaseModel):
    """Data for a transcript annotation."""

    text: str
    errors: list[str]


class EvaluationAnnotationData(BaseModel):
    """Data for an evaluation annotation."""

    decisions: list[str]
    errors: list[str]


class TranscriptAnnotation(BaseModel):
    """Annotation for a transcript."""

    audio_segment_id: str
    type: Literal[AnnotationType.TRANSCRIPT] = AnnotationType.TRANSCRIPT
    data: TranscriptAnnotationData
    created_at: datetime
    updated_at: datetime


class EvaluationAnnotation(BaseModel):
    """Annotation for an evaluation."""

    audio_segment_id: str
    type: Literal[AnnotationType.EVALUATION] = AnnotationType.EVALUATION
    data: EvaluationAnnotationData
    created_at: datetime
    updated_at: datetime


Annotation = Annotated[
    Union[TranscriptAnnotation, EvaluationAnnotation],
    Field(discriminator="type"),
]


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
