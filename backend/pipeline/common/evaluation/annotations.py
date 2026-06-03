from __future__ import annotations

from pydantic import BaseModel


class TextMatchSpan(BaseModel):
    """A single substring within a transcript that matched a rule."""

    # Defaults absorb proto3 default-value omission during MessageToDict.
    start: int = 0
    end: int = 0
    matched_text: str = ""


class TextMatchAnnotation(BaseModel):
    """Annotation payload for keyword/regex rules: the matched text spans."""

    spans: list[TextMatchSpan]


class RuleAnnotation(BaseModel):
    """A single triggered rule together with its kind-specific annotation.

    The annotation kind is identified by which optional field is set, mirroring
    the proto `oneof annotation` shape. Future rule kinds add new optional
    fields here alongside `text_match`.
    """

    rule_id: str
    text_match: TextMatchAnnotation | None = None
