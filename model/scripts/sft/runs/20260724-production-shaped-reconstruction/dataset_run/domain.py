"""Exact-frame domain objects for reconstruction."""

from __future__ import annotations

import dataclasses
import decimal
import enum
import hashlib
import json
from collections.abc import Iterable


class AtomClass(enum.StrEnum):
    """Annotation class after applying the frozen text/category contract."""

    SPEECH = "speech"
    PII = "pii"
    TARGETLESS = "targetless"


class Split(enum.StrEnum):
    """Final mutually exclusive dataset ownership."""

    TRAIN = "train"
    EVAL = "eval"


@dataclasses.dataclass(frozen=True, slots=True)
class SourceMedia:
    """Generation-locked decoded source geometry."""

    uri: str
    generation: int
    size_bytes: int
    md5_hash: str
    crc32c: str
    sha256: str
    sample_rate: int
    channels: int
    subtype: str
    frame_count: int


@dataclasses.dataclass(frozen=True, slots=True)
class Atom:
    """One raw annotation on a source-native frame lattice."""

    atom_id: str
    family: str
    source_uri: str
    raw_manifest: str
    raw_index: int
    start_frame: int
    end_frame: int
    atom_class: AtomClass
    text: str | None

    def __post_init__(self) -> None:
        if not self.atom_id:
            raise ValueError("atom_id must be non-empty")
        if self.start_frame < 0:
            raise ValueError("start_frame must be non-negative")
        if self.end_frame <= self.start_frame:
            raise ValueError("end_frame must exceed start_frame")
        if self.atom_class is AtomClass.SPEECH and not normalize_text(
            self.text
        ):
            raise ValueError("speech atoms require non-empty normalized text")
        if self.atom_class is not AtomClass.SPEECH and self.text is not None:
            raise ValueError("non-speech atoms cannot own target text")


@dataclasses.dataclass(frozen=True, slots=True)
class OwnedSpeech:
    """A speech atom with its final split and authoritative target."""

    atom: Atom
    split: Split
    text: str

    def __post_init__(self) -> None:
        if self.atom.atom_class is not AtomClass.SPEECH:
            raise ValueError("OwnedSpeech must reference a speech atom")
        if normalize_text(self.text) is None:
            raise ValueError("OwnedSpeech target must be non-empty")


@dataclasses.dataclass(frozen=True, slots=True)
class Request:
    """One continuous model request and its exact target ownership."""

    request_id: str
    family: str
    split: Split
    source_uri: str
    start_frame: int
    end_frame: int
    sample_rate: int
    owner_ids: tuple[str, ...]
    text: str

    def __post_init__(self) -> None:
        if not self.request_id:
            raise ValueError("request_id must be non-empty")
        if self.start_frame < 0:
            raise ValueError("start_frame must be non-negative")
        if self.end_frame <= self.start_frame:
            raise ValueError("end_frame must exceed start_frame")
        if self.sample_rate <= 0:
            raise ValueError("sample_rate must be positive")
        if not self.owner_ids:
            raise ValueError("a request must own at least one speech atom")
        if len(set(self.owner_ids)) != len(self.owner_ids):
            raise ValueError("request owner_ids must be unique")
        if normalize_text(self.text) is None:
            raise ValueError("request text must be non-empty")

    @property
    def duration_seconds(self) -> float:
        """Return exact-frame duration expressed in seconds."""

        return (self.end_frame - self.start_frame) / self.sample_rate


def normalize_text(value: object) -> str | None:
    """Normalize annotation text solely to decide target presence.

    Args:
        value: External manifest value.

    Returns:
        Whitespace-collapsed text, or ``None`` when it has no text.
    """

    if not isinstance(value, str):
        return None
    normalized = " ".join(value.split())
    return normalized or None


def classify_annotation(category: object, text: object) -> AtomClass:
    """Apply the frozen Speech-over-PII annotation classification.

    Non-empty normalized transcript is Speech even when its category is PII.
    A targetless category beginning with ``PII`` is PII. Every other
    targetless annotation is targetless context.
    """

    if normalize_text(text) is not None:
        return AtomClass.SPEECH
    if isinstance(category, str) and category.upper().startswith("PII"):
        return AtomClass.PII
    return AtomClass.TARGETLESS


def decimal_to_frame(value: object, sample_rate: int) -> int:
    """Convert an external decimal second value to a source-native frame."""

    if isinstance(value, bool):
        raise ValueError("boolean is not a valid timestamp")
    try:
        seconds = decimal.Decimal(str(value))
    except decimal.InvalidOperation as error:
        raise ValueError(f"invalid timestamp: {value!r}") from error
    if not seconds.is_finite() or seconds < 0:
        raise ValueError(f"timestamp must be finite and non-negative: {value}")
    frame = (seconds * sample_rate).quantize(
        decimal.Decimal(1),
        rounding=decimal.ROUND_HALF_UP,
    )
    return int(frame)


def stable_digest(value: object) -> str:
    """Return a deterministic SHA-256 digest for JSON-compatible data."""

    payload = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return hashlib.sha256(payload).hexdigest()


def ensure_unique(values: Iterable[str], *, label: str) -> tuple[str, ...]:
    """Return values as a tuple and fail on duplicate identifiers."""

    result = tuple(values)
    if len(result) != len(set(result)):
        raise ValueError(f"duplicate {label}")
    return result
