import datetime

from pydantic import BaseModel


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


class ListTranscriptsResponse(BaseModel):
    transcripts: list[Transcript]
    next_token: str | None = None
