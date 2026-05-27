from __future__ import annotations

from dataclasses import dataclass


class SourceIdentityError(ValueError):
    """Raised when a manifest row cannot resolve a leak-safe source group."""


class RowValidationError(ValueError):
    """Raised when a manifest row is structurally invalid."""


@dataclass(frozen=True)
class LabeledSegment:
    dataset_name: str
    dataset_family: str
    source_strategy: str
    source_group: str
    audio_uri: str
    original_audio_uri: str
    text: str
    row_index: int
    offset: float = 0.0
    duration: float = 0.0
    timestamp: str | None = None
    example_id: str | None = None
    segment_id: str | None = None
    split: str | None = None
    model_ready_audio_uri: str | None = None
    derived_audio_uri: str | None = None
    transformation_metadata: dict[str, object] | None = None
    raw_row: dict[str, object] | None = None


@dataclass(frozen=True)
class ExcludedRow:
    dataset_name: str
    row_index: int
    audio_uri: str | None
    reason: str


@dataclass(frozen=True)
class NormalizationResult:
    segments: tuple[LabeledSegment, ...]
    excluded: tuple[ExcludedRow, ...]
