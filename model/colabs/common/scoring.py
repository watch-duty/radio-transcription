"""WER normalizer and ASR scoring metrics for dispatch-domain radio transcription.

This module provides the re-derived dispatch text normalizer (ported faithfully from
``evaluate_transcriptions.ipynb`` cells 2/3/8 — the sole surviving normalizer source
after the ``echo_eval`` code-path was dropped).

**Baseline note (Pitfall 6):** The 44.93% Echo WER baseline was produced by the
now-absent ``echo_eval`` normalizer. That baseline is NOT preserved here — the
normalizer is re-derived from the notebook and the first Phase 2 ``eval`` run against
the base model becomes the new reference. ``nemo_text_processing`` is version-pinned in
the ``[scoring]`` extra; grammars are versioned with the package. Any version bump silently
changes normalization output (and therefore WER), so pin the version explicitly.

All public functions require the ``[scoring]`` extra (``jiwer`` + ``nemo_text_processing``).
Importing this module WITHOUT the extra is safe — the extra is loaded lazily so that
``import common.scoring`` never triggers NeMo when ``[scoring]`` is not installed.
"""

import logging
import re
from typing import Any

logger = logging.getLogger(__name__)

# Heavy deps behind the [scoring] extra — deferred so `import common.scoring`
# never triggers NeMo when [scoring] is not installed (Pitfall 8).
try:
    import jiwer
    from nemo_text_processing.text_normalization.normalize import (
        Normalizer as NemoNormalizer,
    )
except ImportError as _e:
    _SCORING_MISSING = _e
else:
    _SCORING_MISSING = None

# Module-level NeMo normalizer instance (lazy-initialized on first use, 30-60s).
_nemo_normalizer: "NemoNormalizer | None" = None


def _require_scoring() -> None:
    """Raise a clear error if the [scoring] extra is not installed."""
    if _SCORING_MISSING:
        raise ImportError(
            "scoring requires the [scoring] extra: pip install 'common[scoring]'"
        ) from _SCORING_MISSING


def _get_nemo_normalizer() -> "NemoNormalizer":
    """Lazy-initialize the NeMo normalizer (30-60s first call; cached thereafter).

    Returns:
        Cached NemoNormalizer instance initialized with cased English input.
    """
    global _nemo_normalizer
    if _nemo_normalizer is None:
        _nemo_normalizer = NemoNormalizer(input_case="cased", lang="en")
    return _nemo_normalizer


def build_normalizer() -> "jiwer.Compose":
    """Build the dispatch-domain ASR normalization pipeline (NeMo + dispatch quirks).

    Ported faithfully from evaluate_transcriptions.ipynb cells 2/3/8. Behavior is
    pinned by the Phase 2 golden tests (TEST-02). Do NOT improve or change this logic
    without updating the golden tests — the WER baseline is defined by this pipeline.

    The pipeline applies, in order:
      1. NormalizeDispatchQuirks — hallucination placeholder, filler stripping, hyphens
      2. NemoNormalization — NeMo WFST text normalizer (numbers → words, abbreviations)
      3. SubstituteRegexes — collapse newlines/tabs to spaces
      4. ToLowerCase
      5. RemovePunctuation
      6. RemoveWhiteSpace (replace by space)
      7. RemoveMultipleSpaces
      8. Strip

    Returns:
        A ``jiwer.Compose`` pipeline ready for normalizing ASR references and hypotheses.

    Raises:
        ImportError: If the ``[scoring]`` extra is not installed.
    """
    _require_scoring()

    # Function-local class definitions so `import common.scoring` stays light
    # even without [scoring] installed — jiwer.AbstractTransform would be
    # undefined at class-definition time if the try-except above caught an ImportError.

    class NemoNormalization(jiwer.AbstractTransform):
        """Apply NeMo WFST text normalization (verbatim from evaluate_transcriptions.ipynb cell 8)."""

        def process_string(self, s: str) -> str:
            return _get_nemo_normalizer().normalize(s, verbose=False)

    class NormalizeDispatchQuirks(jiwer.AbstractTransform):
        """Apply dispatch-domain quirk normalization (verbatim from evaluate_transcriptions.ipynb cell 8).

        Replaces long digit strings (10+ digits) with a hallucination placeholder,
        strips filler words (uh/um/ah/er), and turns hyphens into spaces.
        """

        def process_string(self, s: str) -> str:
            # Replace long digit strings (10+ digits) with a hallucination placeholder
            s = re.sub(r"\d{10,}", " [hallucination] ", s)
            # Strip fillers so models aren't penalized for readability
            s = re.sub(r"\b(uh|um|ah|er)\b", "", s, flags=re.IGNORECASE)
            # Turn hyphens into spaces
            return s.replace("-", " ")

    return jiwer.Compose(
        [
            NormalizeDispatchQuirks(),
            NemoNormalization(),
            jiwer.SubstituteRegexes({r"[\n\r\t]+": " "}),
            jiwer.ToLowerCase(),
            jiwer.RemovePunctuation(),
            jiwer.RemoveWhiteSpace(replace_by_space=True),
            jiwer.RemoveMultipleSpaces(),
            jiwer.Strip(),
        ]
    )


def compute_wer(
    references: list[str],
    hypotheses: list[str],
    normalizer: "jiwer.Compose | None" = None,
) -> dict[str, Any]:
    """Compute WER with optional normalization; returns WER + ins/del/sub breakdown.

    Args:
        references: Ground-truth transcript strings.
        hypotheses: Model-predicted transcript strings.
        normalizer: Optional ``jiwer.Compose`` pipeline; if provided, both references
            and hypotheses are normalized before scoring (apply the SAME normalizer to
            both — Pitfall 5 symmetric normalization).

    Returns:
        Dict with keys ``wer`` (float, percentage), ``insertions`` (int),
        ``deletions`` (int), ``substitutions`` (int), ``hits`` (int).

    Raises:
        ImportError: If the ``[scoring]`` extra is not installed.
    """
    _require_scoring()
    if normalizer is not None:
        references = [normalizer(r) for r in references]
        hypotheses = [normalizer(h) for h in hypotheses]
    output = jiwer.process_words(references, hypotheses)
    return {
        "wer": round(100 * output.wer, 2),
        "insertions": output.insertions,
        "deletions": output.deletions,
        "substitutions": output.substitutions,
        "hits": output.hits,
    }


def compute_cer(
    references: list[str],
    hypotheses: list[str],
    normalizer: "jiwer.Compose | None" = None,
) -> dict[str, Any]:
    """Compute CER with optional normalization.

    Args:
        references: Ground-truth transcript strings.
        hypotheses: Model-predicted transcript strings.
        normalizer: Optional ``jiwer.Compose`` pipeline applied to both before scoring.

    Returns:
        Dict with key ``cer`` (float, percentage).

    Raises:
        ImportError: If the ``[scoring]`` extra is not installed.
    """
    _require_scoring()
    if normalizer is not None:
        references = [normalizer(r) for r in references]
        hypotheses = [normalizer(h) for h in hypotheses]
    value = jiwer.cer(references, hypotheses)
    return {"cer": round(100 * value, 2)}


def hallucination_rate(hypotheses: list[str]) -> float:
    """Percentage of hypotheses that are empty or the [UNINTELLIGIBLE] token.

    Args:
        hypotheses: List of model-predicted transcript strings.

    Returns:
        Float between 0.0 and 100.0 (percentage of flagged hypotheses).
    """
    if not hypotheses:
        return 0.0
    flagged = sum(
        1 for h in hypotheses
        if not h.strip() or h.strip() == "[UNINTELLIGIBLE]"
    )
    return round(100 * flagged / len(hypotheses), 2)


def duration_bucket_wer(
    references: list[str],
    hypotheses: list[str],
    durations: list[float],
    normalizer: "jiwer.Compose | None" = None,
) -> list[dict[str, Any]]:
    """Compute WER and SER grouped by audio-duration bucket.

    Buckets are the duration edges from evaluate_transcriptions.ipynb's
    ``evaluate_by_duration_buckets``: bins ``[0, 2, 5, 15]`` seconds — segments
    whose duration is >= 15s (or negative) fall outside every bucket and are
    excluded. WER per bucket is delegated to :func:`compute_wer`; SER is the
    percentage of a bucket's segments whose individual WER is greater than zero.

    Args:
        references: Ground-truth transcript strings.
        hypotheses: Model-predicted transcript strings (parallel to references).
        durations: Audio-segment durations in seconds (parallel to references).
        normalizer: Optional jiwer.Compose pipeline; if given it is passed
            straight through to compute_wer (apply the SAME normalizer to refs
            and hyps — Pitfall 5 symmetric normalization).

    Returns:
        A list of per-bucket dicts, one per non-empty bucket, each with keys
        ``bucket`` (str label), ``wer`` (float, percentage), ``ser`` (float,
        percentage), and ``count`` (int, member-segment count). Empty buckets
        are omitted.

    Raises:
        ImportError: If the [scoring] extra is not installed.
        ValueError: If references, hypotheses, and durations differ in length.
    """
    _require_scoring()
    if not (len(references) == len(hypotheses) == len(durations)):
        raise ValueError(
            "references, hypotheses, and durations must have the same length"
        )

    bins = [0, 2, 5, 15]
    labels = ["Short (0-2s)", "Medium (2-5s)", "Long (5s+)"]
    results: list[dict[str, Any]] = []

    for i in range(len(bins) - 1):
        idx = [
            j for j in range(len(durations))
            if bins[i] <= durations[j] < bins[i + 1]
        ]
        if not idx:
            continue
        bucket_refs = [references[j] for j in idx]
        bucket_hyps = [hypotheses[j] for j in idx]
        bucket_wer = compute_wer(bucket_refs, bucket_hyps, normalizer)["wer"]
        # SER = fraction of segments with a non-zero per-segment WER.
        non_zero = 0
        for ref, hyp in zip(bucket_refs, bucket_hyps):
            if compute_wer([ref], [hyp], normalizer)["wer"] > 0:
                non_zero += 1
        results.append({
            "bucket": labels[i],
            "wer": bucket_wer,
            "ser": round(100 * non_zero / len(idx), 2),
            "count": len(idx),
        })
    return results
