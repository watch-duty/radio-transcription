from __future__ import annotations

from typing import Any

from pydantic import BaseModel, field_validator


class TextMatchSpan(BaseModel):
    """A single substring within a transcript that matched a rule."""

    # Defaults absorb proto3 default-value omission during MessageToDict.
    start: int = 0
    end: int = 0
    matched_text: str = ""


class RuleAnnotation(BaseModel):
    """A single triggered rule's kind-specific annotation payload.

    The rule_id lives in the enclosing `rule_annotations` map key, not here. The
    annotation kind is identified by which optional field is set, mirroring the
    proto `oneof annotation` shape. Future rule kinds add new optional fields
    here alongside `text_match`.
    """

    text_match: list[TextMatchSpan] | None = None

    @field_validator("text_match", mode="before")
    @classmethod
    def _unwrap_spans(cls, value: Any) -> Any:
        # The proto wraps the spans in a TextMatchAnnotation message because a
        # `oneof` cannot hold a `repeated` field, so MessageToDict yields
        # {"spans": [...]}. The API exposes a flat list, so unwrap on the
        # way in.
        if isinstance(value, dict) and "spans" in value:
            return value["spans"]
        return value
