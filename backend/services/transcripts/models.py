import datetime
from typing import Any

from pydantic import BaseModel, model_validator

from backend.pipeline.common.evaluation.annotations import RuleAnnotation


class Transcript(BaseModel):
    """Transcript type used by the Transcript API."""

    feed_id: str
    segment_id: str
    transcript: str
    start_timestamp: datetime.datetime | None = None
    end_timestamp: datetime.datetime | None = None
    missing_prior_context: bool = False
    missing_post_context: bool = False
    source_audio_uris: list[str] = []
    canonical_audio_uri: str | None = None
    start_audio_offset: str | None = None
    end_audio_offset: str | None = None
    evaluation_decisions: list[str] = []
    playback_audio_uri: str | None = None
    evaluation_errors: list[str] = []
    rule_annotations: dict[str, RuleAnnotation] = {}
    errors: list[str] = []

    @model_validator(mode="before")
    @classmethod
    def _sync_errors(cls, values: Any) -> Any:
        if isinstance(values, dict):
            errs = values.get("errors") or values.get("evaluation_errors") or []
            values["errors"] = errs
            values["evaluation_errors"] = errs
        return values


class ListTranscriptsResponse(BaseModel):
    transcripts: list[Transcript]
    next_token: str | None = None
