"""Annotation taxonomy and interval math for masked audio segmentation.

Ground-truth manifests exported by human annotators carry a ``category``
per segment. The masked segmentation notebook
(``model/colabs/common/chirp_and_gemini_segment_masked_audio.ipynb``) uses
that taxonomy to decide which regions of a transmission are replaced with
pink noise before the audio is exported for fine-tuning or evaluation.

This module owns the parts of that decision that are worth pinning down
with tests: the category taxonomy, the interval arithmetic that keeps
masks off transcribed speech, and the GCS prefix convention shared by the
notebook that writes masked audio and the notebooks that read it. The
audio DSP itself (pink noise synthesis, crossfades) stays in the notebook,
where it can be listened to.

Pure standard library by design — ``import common.audio_masking`` must
stay free of numpy and torch so the light-core contract documented in
``model/pyproject.toml`` continues to hold.
"""

import enum
import math

# Annotators export composite, multi-level labels (``PII/ADDRESS``,
# ``PII/ID/PHONE``, ``PII/MEDICAL``), so categories are matched by prefix
# rather than by equality. An audit of 60,533 entries across six ground
# truth manifests (PR #733) found prefix matching over these tuples
# covered every sensitive or untranscribed voice segment present.
SPEECH_PREFIXES = ("TRANSCRIPTION",)
MASKED_PREFIXES = (
    "PII",
    "UNINTELLIGIBLE",
    "FOREIGN_SPEECH",
    "UNKNOWN",
    "LAUGHTER",
)
PRESERVED_PREFIXES = ("DTMF", "RINGING", "STATIC")

SEGMENTED_AUDIO_ROOT = "segmented_audio"
MASKED_SUFFIX = "_masked"
RAW_SUFFIX = "_raw"


class Disposition(enum.Enum):
    """What the masking pipeline should do with an annotated segment.

    ``UNRECOGNIZED`` means the label is absent from this module's taxonomy
    — not to be confused with the ``UNKNOWN`` annotation category, which
    annotators use for unclassifiable human speech and which maps to
    ``MASK``. Callers are expected to treat ``UNRECOGNIZED`` as a signal
    to stop rather than as a synonym for ``PRESERVE``: a label nobody has
    classified may well be speech, and passing it through unmasked is the
    failure this taxonomy exists to prevent.
    """

    SPEECH = "SPEECH"
    MASK = "MASK"
    PRESERVE = "PRESERVE"
    UNRECOGNIZED = "UNRECOGNIZED"


def normalize_category(category: str | None) -> str:
    """Normalize a raw manifest category for prefix comparison.

    Args:
        category: Raw ``category`` value from a manifest entry, which may
            be missing, padded with whitespace, or inconsistently cased.

    Returns:
        The category uppercased with surrounding whitespace removed, or
        the empty string when no category was recorded.
    """
    if not category:
        return ""
    return category.strip().upper()


def classify(category: str | None) -> Disposition:
    """Classify a manifest category into a masking disposition.

    Speech is checked first: a composite label that names both speech and
    a maskable sub-type (``TRANSCRIPTION/PII``) is treated as speech,
    because the ground truth text for the segment is what the exported
    audio has to stay aligned with.

    Args:
        category: Raw ``category`` value from a manifest entry.

    Returns:
        The disposition for the segment. Categories outside the taxonomy
        return ``Disposition.UNRECOGNIZED``.
    """
    normalized = normalize_category(category)
    if not normalized:
        return Disposition.UNRECOGNIZED
    if normalized.startswith(SPEECH_PREFIXES):
        return Disposition.SPEECH
    if normalized.startswith(MASKED_PREFIXES):
        return Disposition.MASK
    if normalized.startswith(PRESERVED_PREFIXES):
        return Disposition.PRESERVE
    return Disposition.UNRECOGNIZED


def unrecognized_categories(categories: list[str | None]) -> list[str]:
    """Collect the distinct categories that fall outside the taxonomy.

    Intended as a pre-flight check: a manifest that introduces a label
    this module has never seen should be inspected before its audio is
    exported, since an unclassified segment is passed through unmasked.

    Args:
        categories: Raw ``category`` values from a manifest.

    Returns:
        Sorted distinct normalized categories classified as
        ``Disposition.UNRECOGNIZED``. Entries with no category at all are
        reported as ``"<missing>"``.
    """
    found = set()
    for category in categories:
        if classify(category) is not Disposition.UNRECOGNIZED:
            continue
        normalized = normalize_category(category)
        found.add(normalized or "<missing>")
    return sorted(found)


def subtract_intervals(
    target: tuple[int, int], protected: list[tuple[int, int]]
) -> list[tuple[int, int]]:
    """Remove protected spans from a target span.

    Used to carve transcribed speech out of a mask so the mask is applied
    as a set of sub-spans around the speech rather than over it. Handles a
    protected span that sits wholly inside the target, which is the case
    that a boundary-clamping approach silently gets wrong: clamping can
    only push a mask edge inward from one side, so a maskable annotation
    that spans an utterance ends up covering it.

    Args:
        target: Half-open ``(start, end)`` span to subtract from.
        protected: Spans to remove. May overlap each other and need not
            be sorted.

    Returns:
        The parts of ``target`` left uncovered, in ascending order.
        Empty when ``target`` is degenerate or fully covered.
    """
    target_start, target_end = target
    if target_start >= target_end:
        return []

    merged: list[list[int]] = []
    for start, end in sorted(protected):
        if start >= end:
            continue
        if merged and start <= merged[-1][1]:
            merged[-1][1] = max(merged[-1][1], end)
        else:
            merged.append([start, end])

    remaining = []
    cursor = target_start
    for start, end in merged:
        if end <= cursor:
            continue
        if start >= target_end:
            break
        if start > cursor:
            remaining.append((cursor, start))
        cursor = max(cursor, end)

    if cursor < target_end:
        remaining.append((cursor, target_end))
    return remaining


def to_sample_range(
    start_s: float,
    end_s: float,
    sample_rate: int,
    *,
    origin_s: float = 0.0,
    limit: int | None = None,
) -> tuple[int, int]:
    """Convert a time span to a half-open sample range.

    Rounds outward — start floored, end ceiled — so a span never loses a
    partial sample at either edge. Both masks and protected speech widen
    under this rule, which is safe because protected spans are subtracted
    from masks afterwards and therefore win any overlap.

    Args:
        start_s: Span start in seconds, on the same timeline as
            ``origin_s``.
        end_s: Span end in seconds.
        sample_rate: Sample rate of the audio in Hz.
        origin_s: Timeline position of sample zero, i.e. the start of the
            extracted segment.
        limit: Total samples available, used to clamp the upper bound.

    Returns:
        A half-open ``(start, end)`` sample range, clamped to
        ``[0, limit]``. The range is empty when the span falls outside the
        available audio.

    Raises:
        ValueError: If ``sample_rate`` is not positive.
    """
    if sample_rate <= 0:
        msg = f"sample_rate must be positive, got {sample_rate}"
        raise ValueError(msg)

    start = max(0, math.floor((start_s - origin_s) * sample_rate))
    end = math.ceil((end_s - origin_s) * sample_rate)
    if limit is not None:
        start = min(start, limit)
        end = min(end, limit)
    return (start, max(start, end))


def fade_bounds(
    mask: tuple[int, int],
    fade_samples: int,
    protected: list[tuple[int, int]],
    *,
    limit: int,
) -> tuple[int, int]:
    """Widen a mask by a crossfade ramp without entering protected spans.

    Pink noise is mixed across the mask plus a short ramp either side so
    the substitution does not click. That ramp attenuates whatever it
    covers, so letting it run into transcribed speech quietly replaces the
    leading and trailing milliseconds of an utterance with noise —
    subtracting protected spans from the mask alone does not prevent it.
    Ramps are therefore truncated at the nearest protected edge, which
    shortens the crossfade rather than eating into speech.

    Args:
        mask: Half-open ``(start, end)`` sample range to be masked, with
            protected spans already subtracted.
        fade_samples: Desired ramp length either side of the mask.
        protected: Spans that must not be attenuated.
        limit: Total samples available.

    Returns:
        A half-open ``(start, end)`` sample range covering the mask and
        whatever ramp room was available around it.
    """
    mask_start, mask_end = mask
    start = max(0, mask_start - fade_samples)
    end = min(limit, mask_end + fade_samples)

    for protected_start, protected_end in protected:
        if start < protected_end <= mask_start:
            start = protected_end
        if end > protected_start >= mask_end:
            end = protected_start

    return (start, max(start, end))


def segmented_audio_prefix(
    dataset_path: str,
    *,
    masked: bool = False,
    preprocessed: bool = False,
) -> str:
    """Build the canonical GCS prefix for a segmented audio dataset.

    The suffix distinguishes the three segmentation lanes that write under
    ``segmented_audio/`` and must be applied identically by the notebook
    that writes a dataset and the notebooks that read it. Owning it here
    keeps a producer from writing to ``foo`` while a consumer reads
    ``foo_masked``, and keeps masked output from landing on top of an
    unmasked dataset of the same name.

    Args:
        dataset_path: Operator-supplied dataset path relative to
            ``segmented_audio/``, given without a lane suffix (e.g.
            ``echo/eval``).
        masked: Whether pink-noise masking was applied.
        preprocessed: Whether resampling or downmixing was applied.

    Returns:
        The full prefix, e.g. ``segmented_audio/echo/eval_masked``.

    Raises:
        ValueError: If ``dataset_path`` is empty, already carries a lane
            suffix, or if both lanes are requested at once.
    """
    if masked and preprocessed:
        msg = (
            "A dataset is either masked or preprocessed, not both; "
            "pass only one of masked=True / preprocessed=True."
        )
        raise ValueError(msg)

    trimmed = dataset_path.strip().strip("/")
    if not trimmed:
        msg = "dataset_path must not be empty."
        raise ValueError(msg)

    for suffix in (MASKED_SUFFIX, RAW_SUFFIX):
        if trimmed.endswith(suffix):
            msg = (
                f"dataset_path {dataset_path!r} already ends in {suffix!r}. "
                "Pass the base path without a lane suffix — this function "
                "appends it."
            )
            raise ValueError(msg)

    prefix = f"{SEGMENTED_AUDIO_ROOT}/{trimmed}"
    if masked:
        return f"{prefix}{MASKED_SUFFIX}"
    if preprocessed:
        return prefix
    return f"{prefix}{RAW_SUFFIX}"
