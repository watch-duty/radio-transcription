"""WER normalizer and ASR scoring metrics for dispatch-domain radio transcription.

This module is the canonical home for the dispatch text normalizer
(``build_normalizer``) — notebooks and SFT eval import it rather than
re-implementing inline. The normalizer uses inverse text normalization because
radio traffic is number-heavy and references/predictions may spell the same
unit ID differently (for example, "engine forty one" vs. "Engine 41").
``nemo_text_processing`` is version-pinned in the ``[scoring]`` extra because
a version bump can silently change normalization output and therefore WER.

The WER-bearing functions (``build_normalizer``, ``compute_wer``,
``compute_cer``, ``duration_bucket_wer``, ``bootstrap_paired``) require the
``[scoring]`` extra (``jiwer`` + ``nemo_text_processing``). The pure-Python
helpers (``hallucination_rate``, ``empty_response_rate``,
``count_keyword_occurrences``, ``keyword_metrics``) work without it.
Importing this module WITHOUT the extra is always safe — the heavy deps
are loaded lazily so
``import common.scoring`` never triggers NeMo.
"""

import logging
import random
import re
from functools import cache
from typing import Any

logger = logging.getLogger(__name__)
_EMPTY_REFERENCE_TOKEN = "emptyreference"

# Heavy deps behind the [scoring] extra — deferred so `import common.scoring`
# never triggers NeMo when [scoring] is not installed.
try:
    import jiwer
    from nemo_text_processing.inverse_text_normalization.inverse_normalize import (
        InverseNormalizer as NemoInverseNormalizer,
    )
except ImportError as _e:
    _SCORING_MISSING = _e
else:
    _SCORING_MISSING = None


def _require_scoring() -> None:
    """Raise a clear error if the [scoring] extra is not installed."""
    if _SCORING_MISSING:
        msg = (
            "scoring requires the [scoring] extra: "
            "pip install 'radio-transcription-model[scoring]'"
        )
        raise ImportError(msg) from _SCORING_MISSING


@cache
def _get_nemo_inverse_normalizer() -> "NemoInverseNormalizer":
    """Lazy-initialize the NeMo inverse normalizer (30-60s first call; cached).

    Returns:
        Cached NemoInverseNormalizer (English) instance.
    """
    logger.warning(
        "Loading NeMo text normalizer (first use only; takes 30-60 "
        "seconds — this is not a hung kernel; cached for subsequent "
        "calls)."
    )
    return NemoInverseNormalizer(lang="en")


def build_normalizer() -> "jiwer.Compose":
    """Build the dispatch-domain ASR normalization pipeline.

    Canonical home for the dispatch normalizer. `evaluate_transcriptions.ipynb`
    imports this rather than re-implementing inline; future changes go here.

    Uses NeMo INVERSE normalization (words → digits) plus a manual small-number
    fallback and per-digit splitting, so number-heavy radio dispatch is scored
    at single-digit granularity — "engine forty one" and "engine 41" both
    normalize to "engine 4 1". Behavior is pinned by the golden tests in
    tests/test_scoring.py — do NOT change this logic without updating those
    goldens.

    The pipeline applies, in order:
      1. PreProcessCleanups — hallucination placeholder + filler stripping
      2. NumericStandardizer — NeMo ITN + manual small-number fallback
         + format-character cleanup + per-digit split
      3. ToLowerCase
      4. SubstituteRegexes — collapse newlines/tabs to spaces
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

    class PreProcessCleanups(jiwer.AbstractTransform):
        """Hallucination placeholder + filler stripping, before digit handling."""

        def process_string(self, s: str) -> str:
            # Handle model short-circuit tags: Strip out safety tokens entirely
            # so they do not count as word insertions or substitutions against the WER.
            s = re.sub(r"\[unintelligible\]", "", s, flags=re.IGNORECASE)
            # Hallucination placeholder for runaway digit strings (10+ digits)
            s = re.sub(r"\d{10,}", " [hallucination] ", s)
            # Strip fillers so models aren't penalized for readability
            return re.sub(r"\b(uh|um|ah|er|uhh)\b", "", s, flags=re.IGNORECASE)

    class NumericStandardizer(jiwer.AbstractTransform):
        """Inverse-normalize numbers (words → digits) and split per digit."""

        # Manual fallback for small number words NeMo ITN can miss.
        _SMALL_NUMBER_MAP = {
            "one": "1",
            "two": "2",
            "three": "3",
            "four": "4",
            "five": "5",
            "six": "6",
            "seven": "7",
            "eight": "8",
            "nine": "9",
            "ten": "10",
        }

        def process_string(self, s: str) -> str:
            # 1. NeMo ITN on natural text first
            s = _get_nemo_inverse_normalizer().inverse_normalize(
                s, verbose=False
            )
            # 2. Manual fallback for small number words missed by NeMo
            for word, digit in self._SMALL_NUMBER_MAP.items():
                s = re.sub(rf"\b{word}\b", digit, s, flags=re.IGNORECASE)
            # 3. Remove formatting characters that produce spurious WER errors
            s = re.sub(r"[:\-,\$]", " ", s)
            # 4. Split remaining digits into single-digit tokens
            return re.sub(r"(\d)", r" \1 ", s)

    return jiwer.Compose(
        [
            PreProcessCleanups(),
            NumericStandardizer(),
            jiwer.ToLowerCase(),
            jiwer.SubstituteRegexes({r"[\n\r\t]+": " "}),
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
            both — symmetric normalization).

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
    references, hypotheses = _protect_empty_references(
        references, hypotheses
    )
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
    references, hypotheses = _protect_empty_references(
        references, hypotheses
    )
    value = jiwer.cer(references, hypotheses)
    return {"cer": round(100 * value, 2)}


def _protect_empty_references(
    references: list[str], hypotheses: list[str]
) -> tuple[list[str], list[str]]:
    """Replace empty references with a sentinel so jiwer can score the row."""
    protected_refs: list[str] = []
    protected_hyps: list[str] = []
    for ref, hyp in zip(references, hypotheses, strict=True):
        if ref.strip():
            protected_refs.append(ref)
            protected_hyps.append(hyp)
        else:
            protected_refs.append(_EMPTY_REFERENCE_TOKEN)
            protected_hyps.append(
                _EMPTY_REFERENCE_TOKEN if not hyp.strip() else hyp
            )
    return protected_refs, protected_hyps


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
        1
        for h in hypotheses
        if not h.strip() or h.strip() == "[UNINTELLIGIBLE]"
    )
    return round(100 * flagged / len(hypotheses), 2)


def empty_response_rate(hypotheses: list[str]) -> float:
    """Percentage of hypotheses whose stripped text is exactly empty.

    Args:
        hypotheses: List of model-predicted transcript strings.

    Returns:
        Float between 0.0 and 100.0 (percentage of empty hypotheses).
    """
    if not hypotheses:
        return 0.0
    empty = sum(1 for h in hypotheses if not h.strip())
    return round(100 * empty / len(hypotheses), 2)


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
    excluded. Segments whose reference is empty or whitespace-only (after
    normalization) are also excluded from every bucket — mirroring
    ``evaluate_transcriptions.ipynb``'s empty-ground-truth filter
    (``run_jiwer_evaluation``'s ``if not ground_truth_text: continue`` and
    ``evaluate_by_duration_buckets``'s ``[s for s in subset if s["text_processed"]]``).
    Per-bucket WER and SER both come from one :func:`jiwer.process_words`
    alignment pass over the bucket; SER is the percentage of a bucket's
    segments whose individual WER is greater than zero.

    Args:
        references: Ground-truth transcript strings.
        hypotheses: Model-predicted transcript strings (parallel to references).
        durations: Audio-segment durations in seconds (parallel to references).
        normalizer: Optional jiwer.Compose pipeline; if given it is passed
            straight through to compute_wer (apply the SAME normalizer to refs
            and hyps — symmetric normalization).

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
        msg = "references, hypotheses, and durations must have the same length"
        raise ValueError(msg)

    if normalizer is not None:
        references = [normalizer(r) for r in references]
        hypotheses = [normalizer(h) for h in hypotheses]

    bins = [0, 2, 5, 15]
    labels = ["Short (0-2s)", "Medium (2-5s)", "Long (5s+)"]
    results: list[dict[str, Any]] = []

    for i in range(len(bins) - 1):
        idx = [
            j
            for j in range(len(durations))
            if bins[i] <= durations[j] < bins[i + 1] and references[j].strip()
        ]
        if not idx:
            continue
        bucket_refs = [references[j] for j in idx]
        bucket_hyps = [hypotheses[j] for j in idx]
        # One jiwer alignment pass over the whole bucket, reused for both the
        # aggregate WER and the per-segment SER — rather than an extra
        # process_words call per segment.
        output = jiwer.process_words(bucket_refs, bucket_hyps)
        bucket_wer = round(100 * output.wer, 2)
        # SER = fraction of segments with a non-zero per-segment WER; a segment
        # has an error iff its tokenized reference and hypothesis differ.
        non_zero = sum(
            1
            for ref_words, hyp_words in zip(
                output.references, output.hypotheses, strict=False
            )
            if ref_words != hyp_words
        )
        results.append(
            {
                "bucket": labels[i],
                "wer": bucket_wer,
                "ser": round(100 * non_zero / len(idx), 2),
                "count": len(idx),
            }
        )
    return results


def count_keyword_occurrences(keyword: str, text: str) -> int:
    r"""Count whole-word occurrences of ``keyword`` in ``text`` (case handled by caller).

    Verbatim port of evaluate_transcriptions.ipynb's count_keyword_occurrences:
    a hit is a whole-word regex match (``\\b<escaped-keyword>\\b``).

    Args:
        keyword: The keyword to search for.
        text: The text to search within.

    Returns:
        The number of whole-word occurrences.
    """
    return len(re.findall(rf"\b{re.escape(keyword)}\b", text))


def keyword_metrics(
    references: list[str],
    hypotheses: list[str],
    keywords: list[str],
) -> list[dict[str, Any]]:
    """Per-keyword identification accuracy over a domain keyword set.

    Faithfully ported from evaluate_transcriptions.ipynb's
    ``calculate_keyword_metrics``. For each keyword, counts whole-word
    occurrences in the references; for every reference occurrence, credits a
    match when the paired hypothesis contains the keyword too (capped at the
    reference count per segment: ``min(count_in_gt, count_in_pred)``). Accuracy
    is ``matches / occurrences`` — a recall over keyword instances. Matching is
    case-insensitive (both sides are lower-cased). Pure-Python ``re`` matching —
    requires NO extra.

    Args:
        references: Ground-truth transcript strings.
        hypotheses: Model-predicted transcript strings (parallel to references).
        keywords: Domain keyword set to score, e.g. common.prompts.CHIRP_TEN_CODES.

    Returns:
        A list of per-keyword dicts (one per keyword that occurs at least once
        in the references), each with keys ``keyword`` (str), ``occurrences``
        (int), ``correctly_identified`` (int), and ``accuracy`` (float,
        percentage). Keywords absent from the references are omitted.

    Raises:
        ValueError: If references and hypotheses differ in length.
    """
    if len(references) != len(hypotheses):
        msg = "references and hypotheses must have the same length"
        raise ValueError(msg)

    results: list[dict[str, Any]] = []
    for kw in keywords:
        kw_lower = kw.lower()
        occurrences = 0
        matches = 0
        for ref, hyp in zip(references, hypotheses, strict=False):
            count_in_ref = count_keyword_occurrences(kw_lower, ref.lower())
            if count_in_ref > 0:
                occurrences += count_in_ref
                count_in_hyp = count_keyword_occurrences(kw_lower, hyp.lower())
                matches += min(count_in_ref, count_in_hyp)
        if occurrences > 0:
            results.append(
                {
                    "keyword": kw_lower,
                    "occurrences": occurrences,
                    "correctly_identified": matches,
                    "accuracy": round(100 * matches / occurrences, 2),
                }
            )
    return results


def bootstrap_paired(
    references: list[str],
    hypotheses_a: list[str],
    hypotheses_b: list[str],
    normalizer: "jiwer.Compose | None" = None,
    n_resamples: int = 1000,
    confidence: float = 0.95,
    seed: int = 12345,
) -> dict[str, Any]:
    """Paired-bootstrap WER significance test for two systems on one eval set.

    Resamples the per-example (reference, hyp_a, hyp_b) triples WITH REPLACEMENT
    ``n_resamples`` times, recomputing the WER delta (system A minus system B) on
    each resample, and reports a one-sided, direction-aligned sign-flip significance
    value plus an empirical confidence interval on the delta. Deterministic: the
    resampling RNG is a local ``random.Random(seed)`` — the global random state is
    never touched.

    Args:
        references: Ground-truth transcript strings.
        hypotheses_a: System A predictions (parallel to references).
        hypotheses_b: System B predictions (parallel to references).
        normalizer: Optional jiwer.Compose passed through to compute_wer for
            every WER computation (observed and resampled).
        n_resamples: Number of bootstrap resamples (default 1000).
        confidence: Confidence level for the interval (default 0.95).
        seed: RNG seed — fixing it makes the result reproducible.

    Returns:
        A dict with keys: ``wer_a`` (float), ``wer_b`` (float), ``delta``
        (float, wer_a - wer_b), ``p_value_one_sided`` (float, 0-1) — a ONE-SIDED,
        direction-aligned significance value: the fraction of resamples that fail to
        reproduce the observed sign of the WER delta. This is NOT a two-sided p-value;
        do not interpret ``< 0.05`` as a two-sided 5% result. ``ci_low`` (float),
        ``ci_high`` (float), ``confidence`` (float), ``n_resamples`` (int).

    Raises:
        ImportError: If the [scoring] extra is not installed.
        ValueError: If the three input lists differ in length or are empty,
            if n_resamples is not positive, or if confidence is outside (0, 1).
    """
    _require_scoring()
    n = len(references)
    if not (n == len(hypotheses_a) == len(hypotheses_b)):
        msg = "references, hypotheses_a, and hypotheses_b must have the same length"
        raise ValueError(msg)
    if n == 0:
        msg = "cannot bootstrap an empty eval set"
        raise ValueError(msg)
    # `bool` is a subclass of `int` in Python, so isinstance(True, int) is True
    # — exclude it explicitly so `n_resamples=True` doesn't silently run a
    # single resample.
    if (
        isinstance(n_resamples, bool)
        or not isinstance(n_resamples, int)
        or n_resamples <= 0
    ):
        msg = "n_resamples must be a positive integer"
        raise ValueError(msg)
    if not 0.0 < confidence < 1.0:
        msg = "confidence must lie in the open interval (0, 1)"
        raise ValueError(msg)

    if normalizer is not None:
        references = [normalizer(r) for r in references]
        hypotheses_a = [normalizer(h) for h in hypotheses_a]
        hypotheses_b = [normalizer(h) for h in hypotheses_b]

    wer_a = compute_wer(references, hypotheses_a, normalizer=None)["wer"]
    wer_b = compute_wer(references, hypotheses_b, normalizer=None)["wer"]
    delta_obs = round(wer_a - wer_b, 4)

    rng = random.Random(seed)  # noqa: S311 - deterministic bootstrap, not security.
    deltas: list[float] = []
    for _ in range(n_resamples):
        idx = [rng.randrange(n) for _ in range(n)]
        rs_refs = [references[j] for j in idx]
        rs_a = [hypotheses_a[j] for j in idx]
        rs_b = [hypotheses_b[j] for j in idx]
        d = (
            compute_wer(rs_refs, rs_a, normalizer=None)["wer"]
            - compute_wer(rs_refs, rs_b, normalizer=None)["wer"]
        )
        deltas.append(d)

    if delta_obs > 0:
        p_value_one_sided = sum(1 for d in deltas if d <= 0) / n_resamples
    elif delta_obs < 0:
        p_value_one_sided = sum(1 for d in deltas if d >= 0) / n_resamples
    else:
        p_value_one_sided = 1.0

    deltas.sort()
    lo_frac = (1 - confidence) / 2
    hi_frac = 1 - lo_frac
    lo_i = max(0, min(n_resamples - 1, int(lo_frac * n_resamples)))
    hi_i = max(0, min(n_resamples - 1, int(hi_frac * n_resamples)))
    return {
        "wer_a": wer_a,
        "wer_b": wer_b,
        "delta": delta_obs,
        "p_value_one_sided": round(p_value_one_sided, 4),
        "ci_low": round(deltas[lo_i], 4),
        "ci_high": round(deltas[hi_i], 4),
        "confidence": confidence,
        "n_resamples": n_resamples,
    }
