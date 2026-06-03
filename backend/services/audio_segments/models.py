from __future__ import annotations

from datetime import datetime, timedelta  # noqa: TC003
from enum import StrEnum
from typing import Annotated, Literal, Union

from pydantic import BaseModel, Field

from backend.pipeline.common.evaluation.annotations import RuleAnnotation


class AudioClassification(StrEnum):
    """Enum for audio segment classification."""

    UNSPECIFIED = "UNSPECIFIED"
    SPEECH = "SPEECH"
    OTHER = "OTHER"


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
    rule_annotations: list[RuleAnnotation] = Field(default_factory=list)


class TranscriptAnnotation(BaseModel):
    """Annotation for a transcript."""

    audio_segment_id: str
    type: Literal[AnnotationType.TRANSCRIPT] = AnnotationType.TRANSCRIPT
    data: TranscriptAnnotationData
    created_at: datetime


class EvaluationAnnotation(BaseModel):
    """Annotation for an evaluation."""

    audio_segment_id: str
    type: Literal[AnnotationType.EVALUATION] = AnnotationType.EVALUATION
    data: EvaluationAnnotationData
    created_at: datetime


Annotation = Annotated[
    Union[TranscriptAnnotation, EvaluationAnnotation],
    Field(discriminator="type"),
]


class TranscriptAnnotationCreate(BaseModel):
    """Model for creating a transcript annotation."""

    type: Literal[AnnotationType.TRANSCRIPT] = AnnotationType.TRANSCRIPT
    data: TranscriptAnnotationData


class EvaluationAnnotationCreate(BaseModel):
    """Model for creating an evaluation annotation."""

    type: Literal[AnnotationType.EVALUATION] = AnnotationType.EVALUATION
    data: EvaluationAnnotationData


AnnotationCreate = Annotated[
    Union[TranscriptAnnotationCreate, EvaluationAnnotationCreate],
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
    external_audio_segment_id: str | None = None
    created_at: datetime
    annotations: list[Annotation] = Field(default_factory=list)


class AudioSegmentCreate(BaseModel):
    """Model for creating an audio segment."""

    id: str
    feed_id: str
    classification: AudioClassification
    start_timestamp: datetime
    end_timestamp: datetime
    missing_prior_context: bool = False
    missing_post_context: bool = False
    source_audio_uris: list[str] = Field(default_factory=list)
    canonical_audio_uri: str | None = None
    start_audio_offset: timedelta | None = None
    end_audio_offset: timedelta | None = None
    playback_audio_uri: str | None = None
    external_audio_segment_id: str | None = None


class ListAudioSegmentsResponse(BaseModel):
    """Response model for listing audio segments."""

    segments: list[AudioSegment]
    next_token: str | None = None
