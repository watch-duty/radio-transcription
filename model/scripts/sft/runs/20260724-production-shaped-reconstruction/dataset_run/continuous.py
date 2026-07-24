"""Outcome-blind reconstruction of continuous BCFY-feeds recordings.

The planner works exclusively on the source-native frame lattice.  It first
applies Speech-over-PII precedence, turns overlap-connected Speech owners into
indivisible components, and splits the source into PII-safe islands.  It then
compares three deterministic policies:

* one request per Speech component (``atomic``);
* the current 800 ms gap / 500 ms post-roll / 60 s reference geometry; and
* a bounded deterministic local search whose primary objective is exact
  Wasserstein-1 distance to the injected production duration ECDF.

The timing anchors and 30-second target are deliberately soft.  No duration
bucket, transcript wording, word count, model result, WER, finish reason, or
HTTP outcome participates in a boundary decision.
"""

from __future__ import annotations

import bisect
import concurrent.futures
import dataclasses
import decimal
import fractions
from collections.abc import Iterable, Mapping, Sequence
from typing import Any

from . import transcripts
from .domain import (
    Atom,
    AtomClass,
    OwnedSpeech,
    Request,
    SourceMedia,
    Split,
    stable_digest,
)

_GAP_ANCHOR_MS = 800
_POSTROLL_ANCHOR_MS = 500
_REFERENCE_MAX_MS = 60_000
_VERY_SOFT_TARGET_MS = 30_000
_LOCAL_SEARCH_PASSES = 16
_BOUND_SEARCH_PASSES = 4
_RANK_COST_CACHE_MAX_ENTRIES = 65_536


class ContinuousPlanningError(ValueError):
    """Raised when a source cannot satisfy the continuous-source contract."""


@dataclasses.dataclass(frozen=True, slots=True)
class CandidateObjective:
    """Exact lexicographic score for one complete source policy."""

    wasserstein_1_ms: fractions.Fraction
    reference_cut_mismatches: int
    maximum_reference_boundary_displacement_frames: int
    total_reference_boundary_displacement_frames: int
    over_30s_frames_descending: tuple[int, ...]
    total_over_30s_frames: int
    maximum_post_speech_latency_frames: int
    total_post_speech_latency_frames: int
    request_count: int
    stable_span_key: tuple[tuple[int, int, tuple[int, ...]], ...]

    @property
    def key(self) -> tuple[object, ...]:
        """Return the frozen objective order used for policy selection."""

        return (
            self.wasserstein_1_ms,
            self.reference_cut_mismatches,
            self.maximum_reference_boundary_displacement_frames,
            self.total_reference_boundary_displacement_frames,
            self.over_30s_frames_descending,
            self.total_over_30s_frames,
            self.maximum_post_speech_latency_frames,
            self.total_post_speech_latency_frames,
            self.request_count,
            self.stable_span_key,
        )

    def receipt(self) -> dict[str, Any]:
        """Return a JSON-compatible exact objective."""

        return {
            "objective_order": [
                "exact_wasserstein_1_ms",
                "reference_cut_mismatches",
                "maximum_reference_boundary_displacement_frames",
                "total_reference_boundary_displacement_frames",
                "descending_positive_30s_overage_frames",
                "total_positive_30s_overage_frames",
                "maximum_post_speech_latency_frames",
                "total_post_speech_latency_frames",
                "request_count",
                "stable_span_key",
            ],
            "exact_wasserstein_1_ms": _fraction_receipt(self.wasserstein_1_ms),
            "reference_cut_mismatches": self.reference_cut_mismatches,
            "maximum_reference_boundary_displacement_frames": (
                self.maximum_reference_boundary_displacement_frames
            ),
            "total_reference_boundary_displacement_frames": (
                self.total_reference_boundary_displacement_frames
            ),
            "descending_positive_30s_overage_frames": list(
                self.over_30s_frames_descending
            ),
            "total_positive_30s_overage_frames": (self.total_over_30s_frames),
            "maximum_post_speech_latency_frames": (
                self.maximum_post_speech_latency_frames
            ),
            "total_post_speech_latency_frames": (
                self.total_post_speech_latency_frames
            ),
            "request_count": self.request_count,
            "stable_span_key": [
                {
                    "start_frame": start,
                    "end_frame": end,
                    "component_indices": list(component_indices),
                }
                for start, end, component_indices in self.stable_span_key
            ],
        }


@dataclasses.dataclass(frozen=True, slots=True)
class ContinuousPlan:
    """Selected requests plus the complete policy-comparison receipt."""

    requests: tuple[Request, ...]
    receipt: dict[str, Any]


@dataclasses.dataclass(frozen=True, slots=True)
class ContinuousSourceInput:
    """All structural inputs for one source in one final split."""

    media: SourceMedia
    speeches: tuple[OwnedSpeech, ...]
    pii_atoms: tuple[Atom, ...] = ()
    targetless_atoms: tuple[Atom, ...] = ()


@dataclasses.dataclass(frozen=True, slots=True)
class ContinuousSplitPlan:
    """Globally duration-shaped requests and source/split receipts."""

    requests: tuple[Request, ...]
    source_plans: tuple[ContinuousPlan, ...]
    receipt: dict[str, Any]


@dataclasses.dataclass(frozen=True, slots=True)
class _Histogram:
    support_ms: tuple[fractions.Fraction, ...]
    counts: tuple[int, ...]
    cumulative: tuple[int, ...]
    area_prefix: tuple[fractions.Fraction, ...]
    total: int

    @classmethod
    def from_milliseconds(cls, raw: Mapping[int | str, int]) -> "_Histogram":
        normalized: dict[int, int] = {}
        for raw_duration, raw_count in raw.items():
            if isinstance(raw_duration, bool):
                raise ContinuousPlanningError(
                    "production duration must be an integer millisecond"
                )
            try:
                duration = int(raw_duration)
            except (TypeError, ValueError) as error:
                raise ContinuousPlanningError(
                    "production duration must be an integer millisecond"
                ) from error
            if str(duration) != str(raw_duration) and not isinstance(
                raw_duration, int
            ):
                raise ContinuousPlanningError(
                    f"noncanonical production duration: {raw_duration!r}"
                )
            if duration < 0:
                raise ContinuousPlanningError(
                    "production duration must be non-negative"
                )
            if (
                isinstance(raw_count, bool)
                or not isinstance(raw_count, int)
                or raw_count <= 0
            ):
                raise ContinuousPlanningError(
                    "production histogram counts must be positive integers"
                )
            normalized[duration] = normalized.get(duration, 0) + raw_count
        if not normalized:
            raise ContinuousPlanningError(
                "production duration histogram must be nonempty"
            )

        support = tuple(
            fractions.Fraction(duration) for duration in sorted(normalized)
        )
        counts = tuple(normalized[int(duration)] for duration in support)
        cumulative_values: list[int] = []
        running = 0
        for count in counts:
            running += count
            cumulative_values.append(running)

        # area_prefix[i] is integral(F) from support[0] to support[i].
        area_prefix: list[fractions.Fraction] = [fractions.Fraction()]
        for index in range(len(support) - 1):
            cdf = fractions.Fraction(cumulative_values[index], running)
            area_prefix.append(
                area_prefix[-1] + (support[index + 1] - support[index]) * cdf
            )
        return cls(
            support_ms=support,
            counts=counts,
            cumulative=tuple(cumulative_values),
            area_prefix=tuple(area_prefix),
            total=running,
        )

    def quantile(self, numerator: int, denominator: int) -> fractions.Fraction:
        if denominator <= 0 or numerator < 0 or numerator > denominator:
            raise ContinuousPlanningError("invalid quantile")
        if numerator == 0:
            return self.support_ms[0]
        rank = (numerator * self.total + denominator - 1) // denominator
        index = bisect.bisect_left(self.cumulative, rank)
        return self.support_ms[index]

    def nearby_support(
        self, value: fractions.Fraction
    ) -> tuple[fractions.Fraction, ...]:
        index = bisect.bisect_left(self.support_ms, value)
        lo = max(0, index - 2)
        hi = min(len(self.support_ms), index + 3)
        return self.support_ms[lo:hi]

    def wasserstein_1(
        self, observed_ms: Sequence[fractions.Fraction]
    ) -> fractions.Fraction:
        """Compute exact W1 in O(N log M), without expanding the histogram."""

        if not observed_ms:
            raise ContinuousPlanningError(
                "Wasserstein distance requires at least one request"
            )
        observed = sorted(observed_ms)
        lo = min(self.support_ms[0], observed[0])
        hi = max(self.support_ms[-1], observed[-1])
        total = fractions.Fraction()
        prior = lo
        observed_count = len(observed)
        index = 0
        while index < observed_count:
            point = observed[index]
            if point > prior:
                total += self._integral_absolute_cdf(
                    prior,
                    point,
                    fractions.Fraction(index, observed_count),
                )
            while index < observed_count and observed[index] == point:
                index += 1
            prior = point
        if prior < hi:
            total += self._integral_absolute_cdf(
                prior,
                hi,
                fractions.Fraction(1),
            )
        return total

    def _integral_cdf(
        self, start: fractions.Fraction, end: fractions.Fraction
    ) -> fractions.Fraction:
        return self._cdf_primitive(end) - self._cdf_primitive(start)

    def _cdf_primitive(self, point: fractions.Fraction) -> fractions.Fraction:
        if point <= self.support_ms[0]:
            return fractions.Fraction()
        if point >= self.support_ms[-1]:
            return self.area_prefix[-1] + (point - self.support_ms[-1])
        support_index = bisect.bisect_right(self.support_ms, point) - 1
        return self.area_prefix[support_index] + (
            point - self.support_ms[support_index]
        ) * fractions.Fraction(
            self.cumulative[support_index],
            self.total,
        )

    def _integral_absolute_cdf(
        self,
        start: fractions.Fraction,
        end: fractions.Fraction,
        constant: fractions.Fraction,
    ) -> fractions.Fraction:
        if end <= start:
            return fractions.Fraction()
        threshold = constant * self.total
        # First target support whose CDF is strictly greater than constant.
        crossing_index = bisect.bisect_right(
            self.cumulative,
            threshold,
        )
        crossing = (
            self.support_ms[crossing_index]
            if crossing_index < len(self.support_ms)
            else end
        )
        middle = min(end, max(start, crossing))
        left_area = self._integral_cdf(start, middle)
        right_area = self._integral_cdf(middle, end)
        return (
            constant * (middle - start)
            - left_area
            + right_area
            - constant * (end - middle)
        )


@dataclasses.dataclass(slots=True)
class _RankWassersteinScorer:
    """Exact 1-D W1 scorer with cached target-quantile rank costs.

    For ordered observations ``y[0] <= ... <= y[n-1]``, exact one-dimensional
    optimal transport decomposes into one independent target-quantile slice
    per observed rank.  The cost for ``(n, rank, value)`` therefore depends
    only on the frozen target histogram, and is safe to cache across every
    candidate partition for one source.
    """

    histogram: _Histogram
    _weighted_count_prefix: tuple[fractions.Fraction, ...] = (
        dataclasses.field(init=False)
    )
    _moment_cache: dict[fractions.Fraction, fractions.Fraction] = (
        dataclasses.field(default_factory=dict, init=False)
    )
    _rank_cost_cache: dict[
        tuple[int, int, fractions.Fraction],
        fractions.Fraction,
    ] = dataclasses.field(default_factory=dict, init=False)
    _rank_cost_cache_observed_count: int | None = dataclasses.field(
        default=None,
        init=False,
    )
    peak_rank_cost_cache_entries: int = dataclasses.field(
        default=0,
        init=False,
    )
    rank_cost_cache_clear_count: int = dataclasses.field(
        default=0,
        init=False,
    )

    def __post_init__(self) -> None:
        running = fractions.Fraction()
        values: list[fractions.Fraction] = []
        for support, count in zip(
            self.histogram.support_ms,
            self.histogram.counts,
            strict=True,
        ):
            running += support * count
            values.append(running)
        self._weighted_count_prefix = tuple(values)

    def wasserstein_1(
        self,
        observed_ms: Sequence[fractions.Fraction],
    ) -> fractions.Fraction:
        if not observed_ms:
            raise ContinuousPlanningError(
                "Wasserstein distance requires at least one request"
            )
        ordered = sorted(observed_ms)
        count = len(ordered)
        return sum(
            (
                self.rank_cost(count, rank, value)
                for rank, value in enumerate(ordered, start=1)
            ),
            fractions.Fraction(),
        )

    def rank_cost(
        self,
        observed_count: int,
        rank: int,
        value: fractions.Fraction,
    ) -> fractions.Fraction:
        if (
            observed_count <= 0
            or rank <= 0
            or rank > observed_count
        ):
            raise ContinuousPlanningError("invalid observed W1 rank")
        if self._rank_cost_cache_observed_count != observed_count:
            if self._rank_cost_cache:
                self._rank_cost_cache.clear()
                self.rank_cost_cache_clear_count += 1
            self._rank_cost_cache_observed_count = observed_count
        key = (observed_count, rank, value)
        cached = self._rank_cost_cache.get(key)
        if cached is not None:
            return cached

        lower = fractions.Fraction(rank - 1, observed_count)
        upper = fractions.Fraction(rank, observed_count)
        support_index = bisect.bisect_right(
            self.histogram.support_ms,
            value,
        )
        target_cdf = (
            fractions.Fraction()
            if support_index == 0
            else fractions.Fraction(
                self.histogram.cumulative[support_index - 1],
                self.histogram.total,
            )
        )
        middle = min(upper, max(lower, target_cdf))
        lower_moment = self._quantile_moment(lower)
        middle_moment = self._quantile_moment(middle)
        upper_moment = self._quantile_moment(upper)
        result = (
            value * (middle - lower)
            - (middle_moment - lower_moment)
            + (upper_moment - middle_moment)
            - value * (upper - middle)
        )
        if result < 0:
            raise AssertionError("exact rank W1 contribution became negative")
        if len(self._rank_cost_cache) >= _RANK_COST_CACHE_MAX_ENTRIES:
            self._rank_cost_cache.clear()
            self.rank_cost_cache_clear_count += 1
        self._rank_cost_cache[key] = result
        self.peak_rank_cost_cache_entries = max(
            self.peak_rank_cost_cache_entries,
            len(self._rank_cost_cache),
        )
        return result

    def _quantile_moment(
        self,
        probability: fractions.Fraction,
    ) -> fractions.Fraction:
        cached = self._moment_cache.get(probability)
        if cached is not None:
            return cached
        if probability < 0 or probability > 1:
            raise ContinuousPlanningError(
                "target quantile moment probability is out of range"
            )
        if probability == 0:
            result = fractions.Fraction()
        elif probability == 1:
            result = (
                self._weighted_count_prefix[-1] / self.histogram.total
            )
        else:
            target_count = probability * self.histogram.total
            support_index = bisect.bisect_left(
                self.histogram.cumulative,
                target_count,
            )
            prior_count = (
                0
                if support_index == 0
                else self.histogram.cumulative[support_index - 1]
            )
            prior_weight = (
                fractions.Fraction()
                if support_index == 0
                else self._weighted_count_prefix[support_index - 1]
            )
            result = (
                prior_weight
                + (target_count - prior_count)
                * self.histogram.support_ms[support_index]
            ) / self.histogram.total
        self._moment_cache[probability] = result
        return result


@dataclasses.dataclass(frozen=True, slots=True)
class _ObservedWassersteinState:
    """Exact W1 state supporting one-duration replacement deltas."""

    scorer: _RankWassersteinScorer
    ordered: tuple[fractions.Fraction, ...]
    contributions: tuple[fractions.Fraction, ...]
    shift_right_prefix: tuple[fractions.Fraction, ...]
    shift_left_prefix: tuple[fractions.Fraction, ...]
    wasserstein_1_ms: fractions.Fraction

    @classmethod
    def from_values(
        cls,
        scorer: _RankWassersteinScorer,
        values: Sequence[fractions.Fraction],
    ) -> "_ObservedWassersteinState":
        ordered = tuple(sorted(values))
        if not ordered:
            raise ContinuousPlanningError(
                "Wasserstein state requires at least one request"
            )
        count = len(ordered)
        contributions = tuple(
            scorer.rank_cost(count, rank, value)
            for rank, value in enumerate(ordered, start=1)
        )
        shift_right: list[fractions.Fraction] = []
        shift_left: list[fractions.Fraction] = []
        for index, value in enumerate(ordered):
            shift_right.append(
                fractions.Fraction()
                if index == count - 1
                else scorer.rank_cost(count, index + 2, value)
                - contributions[index]
            )
            shift_left.append(
                fractions.Fraction()
                if index == 0
                else scorer.rank_cost(count, index, value)
                - contributions[index]
            )
        return cls(
            scorer=scorer,
            ordered=ordered,
            contributions=contributions,
            shift_right_prefix=_prefix_sums(shift_right),
            shift_left_prefix=_prefix_sums(shift_left),
            wasserstein_1_ms=sum(
                contributions,
                fractions.Fraction(),
            ),
        )

    def replacing(
        self,
        old_value: fractions.Fraction,
        new_value: fractions.Fraction,
    ) -> fractions.Fraction:
        """Return exact W1 after replacing one observed duration."""

        old_index = bisect.bisect_left(self.ordered, old_value)
        if (
            old_index >= len(self.ordered)
            or self.ordered[old_index] != old_value
        ):
            raise AssertionError("old duration is absent from W1 state")
        insertion_index = bisect.bisect_left(self.ordered, new_value)
        new_index = insertion_index - int(insertion_index > old_index)
        count = len(self.ordered)
        result = (
            self.wasserstein_1_ms
            - self.contributions[old_index]
            + self.scorer.rank_cost(count, new_index + 1, new_value)
        )
        if new_index < old_index:
            result += (
                self.shift_right_prefix[old_index]
                - self.shift_right_prefix[new_index]
            )
        elif new_index > old_index:
            result += (
                self.shift_left_prefix[new_index + 1]
                - self.shift_left_prefix[old_index + 1]
            )
        return result


@dataclasses.dataclass(frozen=True, slots=True)
class _Component:
    index: int
    island_index: int
    start_frame: int
    end_frame: int
    owners: tuple[OwnedSpeech, ...]

    @property
    def owner_ids(self) -> tuple[str, ...]:
        return tuple(owner.atom.atom_id for owner in self.owners)


@dataclasses.dataclass(frozen=True, slots=True)
class _Group:
    component_indices: tuple[int, ...]
    island_index: int
    start_frame: int
    end_frame: int
    lower_context_frame: int
    upper_context_frame: int
    owners: tuple[OwnedSpeech, ...]

    @property
    def owner_ids(self) -> tuple[str, ...]:
        return tuple(owner.atom.atom_id for owner in self.owners)


@dataclasses.dataclass(frozen=True, slots=True)
class _Span:
    group: _Group
    start_frame: int
    end_frame: int


@dataclasses.dataclass(frozen=True, slots=True)
class _Candidate:
    name: str
    spans: tuple[_Span, ...]
    objective: CandidateObjective
    search_receipt: dict[str, Any]


@dataclasses.dataclass(frozen=True, slots=True)
class _SplitSourceState:
    source: ContinuousSourceInput
    split: Split
    speeches: tuple[OwnedSpeech, ...]
    effective_pii: tuple[tuple[int, int], ...]
    candidates: tuple[_Candidate, ...]
    local_choice_index: int


@dataclasses.dataclass(frozen=True, slots=True)
class _SplitObjective:
    aggregate_wasserstein_1_ms: fractions.Fraction
    summed_source_wasserstein_1_ms: fractions.Fraction
    reference_cut_mismatches: int
    maximum_reference_boundary_displacement_ms: fractions.Fraction
    total_reference_boundary_displacement_ms: fractions.Fraction
    over_30s_ms_descending: tuple[fractions.Fraction, ...]
    total_over_30s_ms: fractions.Fraction
    maximum_post_speech_latency_ms: fractions.Fraction
    total_post_speech_latency_ms: fractions.Fraction
    request_count: int
    stable_selection_key: tuple[
        tuple[
            str,
            int,
            str,
            tuple[tuple[int, int, tuple[int, ...]], ...],
        ],
        ...,
    ]

    @property
    def key(self) -> tuple[object, ...]:
        return (
            self.aggregate_wasserstein_1_ms,
            self.summed_source_wasserstein_1_ms,
            self.reference_cut_mismatches,
            self.maximum_reference_boundary_displacement_ms,
            self.total_reference_boundary_displacement_ms,
            self.over_30s_ms_descending,
            self.total_over_30s_ms,
            self.maximum_post_speech_latency_ms,
            self.total_post_speech_latency_ms,
            self.request_count,
            self.stable_selection_key,
        )

    def receipt(self) -> dict[str, Any]:
        return {
            "objective_order": [
                "aggregate_exact_wasserstein_1_ms",
                "summed_source_exact_wasserstein_1_ms",
                "aggregate_reference_cut_mismatches",
                "maximum_reference_boundary_displacement_ms",
                "total_reference_boundary_displacement_ms",
                "descending_positive_30s_overage_ms",
                "total_positive_30s_overage_ms",
                "maximum_post_speech_latency_ms",
                "total_post_speech_latency_ms",
                "request_count",
                "stable_source_policy_span_key_sha256",
            ],
            "aggregate_exact_wasserstein_1_ms": _fraction_receipt(
                self.aggregate_wasserstein_1_ms
            ),
            "summed_source_exact_wasserstein_1_ms": _fraction_receipt(
                self.summed_source_wasserstein_1_ms
            ),
            "aggregate_reference_cut_mismatches": (
                self.reference_cut_mismatches
            ),
            "maximum_reference_boundary_displacement_ms": _fraction_receipt(
                self.maximum_reference_boundary_displacement_ms
            ),
            "total_reference_boundary_displacement_ms": _fraction_receipt(
                self.total_reference_boundary_displacement_ms
            ),
            "descending_positive_30s_overage_ms": [
                _fraction_receipt(value)
                for value in self.over_30s_ms_descending
            ],
            "total_positive_30s_overage_ms": _fraction_receipt(
                self.total_over_30s_ms
            ),
            "maximum_post_speech_latency_ms": _fraction_receipt(
                self.maximum_post_speech_latency_ms
            ),
            "total_post_speech_latency_ms": _fraction_receipt(
                self.total_post_speech_latency_ms
            ),
            "request_count": self.request_count,
            "stable_source_policy_span_key_sha256": stable_digest(
                self.stable_selection_key
            ),
            "stable_source_policy_count": len(self.stable_selection_key),
        }


def exact_wasserstein_1_ms(
    production_histogram_ms: Mapping[int | str, int],
    observed_duration_ms: Sequence[fractions.Fraction],
) -> fractions.Fraction:
    """Public exact W1 helper used by the independent verifier and tests."""

    return _Histogram.from_milliseconds(production_histogram_ms).wasserstein_1(
        observed_duration_ms
    )


def continuous_source_inputs_sha256(
    sources: Sequence[ContinuousSourceInput],
    *,
    split: Split,
) -> str:
    """Bind every outcome-blind source and annotation geometry input.

    Transcript text, cache paths, and audio bytes are intentionally excluded.
    Ownership and source-inventory receipts bind those independently; this
    digest closes the persisted-plan seam for the exact frame geometry that
    can affect legal request boundaries.
    """
    if not isinstance(split, Split):
        raise ContinuousPlanningError("split must be a domain.Split")
    if not sources:
        raise ContinuousPlanningError(
            "continuous source input digest requires at least one source"
        )
    ordered = tuple(
        sorted(
            sources,
            key=lambda source: (
                source.media.uri,
                source.media.generation,
            ),
        )
    )
    uris = [source.media.uri for source in ordered]
    if len(uris) != len(set(uris)):
        raise ContinuousPlanningError(
            "continuous source input digest requires unique source URIs"
        )
    rows: list[dict[str, Any]] = []
    for source in ordered:
        for owner in source.speeches:
            if owner.split is not split:
                raise ContinuousPlanningError(
                    "continuous source input digest observed mixed ownership"
                )
            if owner.atom.source_uri != source.media.uri:
                raise ContinuousPlanningError(
                    "continuous source input digest observed mixed source URI"
                )
        for atom in (*source.pii_atoms, *source.targetless_atoms):
            if atom.source_uri != source.media.uri:
                raise ContinuousPlanningError(
                    "continuous source input digest observed mixed source URI"
                )
        rows.append(
            {
                "media": {
                    "uri": source.media.uri,
                    "generation": source.media.generation,
                    "size_bytes": source.media.size_bytes,
                    "md5_hash": source.media.md5_hash,
                    "crc32c": source.media.crc32c,
                    "sha256": source.media.sha256,
                    "sample_rate": source.media.sample_rate,
                    "channels": source.media.channels,
                    "subtype": source.media.subtype,
                    "frame_count": source.media.frame_count,
                },
                "speech": [
                    {
                        "atom_id": owner.atom.atom_id,
                        "start_frame": owner.atom.start_frame,
                        "end_frame": owner.atom.end_frame,
                        "owner_split": owner.split.value,
                    }
                    for owner in sorted(
                        source.speeches,
                        key=lambda value: (
                            value.atom.start_frame,
                            value.atom.end_frame,
                            value.atom.atom_id,
                        ),
                    )
                ],
                "pii": [
                    {
                        "atom_id": atom.atom_id,
                        "start_frame": atom.start_frame,
                        "end_frame": atom.end_frame,
                    }
                    for atom in sorted(
                        source.pii_atoms,
                        key=lambda value: (
                            value.start_frame,
                            value.end_frame,
                            value.atom_id,
                        ),
                    )
                ],
                "targetless": [
                    {
                        "atom_id": atom.atom_id,
                        "start_frame": atom.start_frame,
                        "end_frame": atom.end_frame,
                    }
                    for atom in sorted(
                        source.targetless_atoms,
                        key=lambda value: (
                            value.start_frame,
                            value.end_frame,
                            value.atom_id,
                        ),
                    )
                ],
            }
        )
    return stable_digest(
        {
            "schema_version": 1,
            "split": split.value,
            "sources": rows,
        }
    )


def plan_continuous_source(
    *,
    media: SourceMedia,
    speeches: Sequence[OwnedSpeech],
    pii_atoms: Sequence[Atom],
    targetless_atoms: Sequence[Atom] = (),
    production_histogram_ms: Mapping[int | str, int],
) -> ContinuousPlan:
    """Plan one source/split of continuous BCFY-feeds audio.

    Args:
        media: Generation-locked source geometry.
        speeches: Final authoritative Speech owners for exactly one split.
        pii_atoms: Targetless PII annotations.  Speech overlap wins.
        targetless_atoms: Blank/non-PII annotations, used only for the frozen
            structural receipt; uncovered frames are equally eligible context.
        production_histogram_ms: Exact production logical-request histogram.

    Returns:
        The selected model requests and a complete policy receipt.

    Raises:
        ContinuousPlanningError: On mixed source/split inputs, invalid bounds,
            PII conflicts, missing speech, or any conservation failure.
    """

    histogram = _Histogram.from_milliseconds(production_histogram_ms)
    checked_speeches, split = _validate_inputs(
        media=media,
        speeches=speeches,
        pii_atoms=pii_atoms,
        targetless_atoms=targetless_atoms,
    )
    speech_union = _merge_intervals(
        (owner.atom.start_frame, owner.atom.end_frame)
        for owner in checked_speeches
    )
    pii_union = _merge_intervals(
        (atom.start_frame, atom.end_frame) for atom in pii_atoms
    )
    effective_pii = _subtract_intervals(pii_union, speech_union)
    safe_islands = _complement_intervals(
        effective_pii,
        start=0,
        end=media.frame_count,
    )
    components = _speech_components(checked_speeches, safe_islands)
    island_component_indices = _island_component_indices(
        components, len(safe_islands)
    )
    nonempty_island_indices = tuple(
        index for index, values in enumerate(island_component_indices) if values
    )

    atomic_cuts = frozenset(_all_adjacencies(island_component_indices))
    reference_cuts = _reference_cuts(
        components=components,
        island_component_indices=island_component_indices,
        sample_rate=media.sample_rate,
    )

    atomic_groups = _groups_from_cuts(
        components=components,
        safe_islands=safe_islands,
        island_component_indices=island_component_indices,
        cuts=atomic_cuts,
    )
    atomic_spans = tuple(
        _Span(
            group=group,
            start_frame=group.start_frame,
            end_frame=group.end_frame,
        )
        for group in atomic_groups
    )
    atomic = _candidate(
        name="atomic",
        spans=atomic_spans,
        histogram=histogram,
        sample_rate=media.sample_rate,
        reference_cuts=reference_cuts,
        search_receipt={
            "method": "one exact Speech-overlap component per request",
        },
    )

    reference_groups = _groups_from_cuts(
        components=components,
        safe_islands=safe_islands,
        island_component_indices=island_component_indices,
        cuts=reference_cuts,
    )
    reference_spans = _reference_spans(
        reference_groups,
        sample_rate=media.sample_rate,
    )
    reference = _candidate(
        name="reference_800ms",
        spans=reference_spans,
        histogram=histogram,
        sample_rate=media.sample_rate,
        reference_cuts=reference_cuts,
        search_receipt={
            "method": (
                "gap<800ms grouping, 500ms post-roll, soft 60s reference"
            ),
            "gap_anchor_ms": _GAP_ANCHOR_MS,
            "postroll_anchor_ms": _POSTROLL_ANCHOR_MS,
            "reference_max_ms": _REFERENCE_MAX_MS,
        },
    )

    targeted, targeted_trials = _targeted_candidate(
        components=components,
        safe_islands=safe_islands,
        island_component_indices=island_component_indices,
        histogram=histogram,
        sample_rate=media.sample_rate,
        reference_cuts=reference_cuts,
        atomic_cuts=atomic_cuts,
    )
    candidates = (atomic, reference, targeted)
    selected = min(
        candidates,
        key=lambda candidate: (candidate.objective.key, candidate.name),
    )
    requests, transcript_receipts = _requests_from_spans(
        selected.spans,
        media=media,
        split=split,
    )
    _verify_selected(
        requests=requests,
        speeches=checked_speeches,
        effective_pii=effective_pii,
        media=media,
    )

    targetless_union = _merge_intervals(
        (atom.start_frame, atom.end_frame) for atom in targetless_atoms
    )
    receipt = {
        "schema_version": 1,
        "algorithm": "continuous_bcfy_feeds_exact_frame_v1",
        "source": {
            "uri": media.uri,
            "generation": media.generation,
            "sample_rate": media.sample_rate,
            "frame_count": media.frame_count,
            "split": split.value,
        },
        "outcome_blind_boundary_fields": [
            "source_uri",
            "split",
            "sample_rate",
            "frame_count",
            "atom_class",
            "start_frame",
            "end_frame",
        ],
        "forbidden_boundary_fields": [
            "transcript_wording",
            "transcript_word_count",
            "model_prediction",
            "wer",
            "finish_reason",
            "http_status_or_error",
            "fallback_outcome",
        ],
        "precedence": "Speech > PII > targetless/unannotated",
        "production_histogram": {
            "unit": "integer_millisecond",
            "request_count": histogram.total,
            "unique_duration_count": len(histogram.support_ms),
            "sha256": stable_digest(
                {
                    str(int(duration)): count
                    for duration, count in zip(
                        histogram.support_ms,
                        histogram.counts,
                        strict=True,
                    )
                }
            ),
        },
        "structure": {
            "speech_owner_count": len(checked_speeches),
            "speech_component_count": len(components),
            "pii_annotation_union": _interval_receipt(pii_union),
            "speech_union": _interval_receipt(speech_union),
            "effective_pii_only_union": _interval_receipt(effective_pii),
            "targetless_annotation_union": _interval_receipt(targetless_union),
            "safe_islands": [
                {
                    "island_index": index,
                    "start_frame": start,
                    "end_frame": end,
                    "contains_speech": index in nonempty_island_indices,
                }
                for index, (start, end) in enumerate(safe_islands)
            ],
            "speech_components": [
                {
                    "component_index": component.index,
                    "island_index": component.island_index,
                    "start_frame": component.start_frame,
                    "end_frame": component.end_frame,
                    "owner_ids": list(component.owner_ids),
                }
                for component in components
            ],
        },
        "soft_priority_order": [
            "exact production duration ECDF (Wasserstein-1)",
            "800ms/500ms/60s reference geometry",
            "very-soft 30s target",
            "post-speech availability latency",
            "request count",
            "stable source-frame ordering",
        ],
        "candidates": [
            _candidate_receipt(candidate, media.sample_rate)
            for candidate in candidates
        ],
        "targeted_search_trials": targeted_trials,
        "selected_policy": selected.name,
        "selection_reason": (
            "minimum frozen exact lexicographic objective across atomic, "
            "reference_800ms, and ecdf_targeted candidates; candidate name "
            "breaks only an otherwise exact tie"
        ),
        "selected_objective": selected.objective.receipt(),
        "selected_transcript_reconciliation": transcript_receipts,
        "hard_invariants": {
            "speech_owner_count_input": len(checked_speeches),
            "speech_owner_count_output": sum(
                len(request.owner_ids) for request in requests
            ),
            "every_owner_exactly_once": True,
            "requests_are_continuous": True,
            "requests_are_frame_disjoint": True,
            "speech_is_never_cut": True,
            "pii_only_frames_in_requests": 0,
            "standalone_targetless_requests": 0,
        },
    }
    return ContinuousPlan(requests=requests, receipt=receipt)


def plan_continuous_split(
    *,
    sources: Sequence[ContinuousSourceInput],
    production_histogram_ms: Mapping[int | str, int],
    max_workers: int = 1,
) -> ContinuousSplitPlan:
    """Choose source policies against one aggregate final-split ECDF.

    Each source still has independent hard constraints and independent
    atomic/reference/targeted candidates.  Only candidate selection is
    coordinated: exact W1 is computed from the union of requests in this call.
    Calling this function once for train and once for eval therefore shares no
    optimizer state, counts, or choices across final splits.
    """
    if not sources:
        raise ContinuousPlanningError(
            "split planning requires at least one continuous source"
        )
    if (
        isinstance(max_workers, bool)
        or not isinstance(max_workers, int)
        or max_workers <= 0
    ):
        raise ContinuousPlanningError("max_workers must be a positive integer")
    histogram = _Histogram.from_milliseconds(production_histogram_ms)
    ordered_sources = tuple(
        sorted(
            sources,
            key=lambda source: (
                source.media.uri,
                source.media.generation,
            ),
        )
    )
    uris = [source.media.uri for source in ordered_sources]
    if len(uris) != len(set(uris)):
        raise ContinuousPlanningError(
            "split planning requires unique source URIs"
        )
    if max_workers == 1:
        states = tuple(
            _enumerate_split_source(source, histogram)
            for source in ordered_sources
        )
    else:
        scheduled_indices = _continuous_source_enumeration_order(
            ordered_sources
        )
        arguments = tuple(
            (ordered_sources[index], histogram)
            for index in scheduled_indices
        )
        with concurrent.futures.ProcessPoolExecutor(
            max_workers=max_workers
        ) as executor:
            scheduled_states = tuple(
                executor.map(_enumerate_split_source_worker, arguments)
            )
        states_by_index = dict(
            zip(scheduled_indices, scheduled_states, strict=True)
        )
        states = tuple(
            states_by_index[index] for index in range(len(ordered_sources))
        )
    splits = {state.split for state in states}
    if len(splits) != 1:
        raise ContinuousPlanningError(
            "one split planner call cannot mix train and eval ownership"
        )
    split = next(iter(splits))
    source_inputs_digest = continuous_source_inputs_sha256(
        ordered_sources,
        split=split,
    )

    choices, objective, search_receipt = _coordinate_split_choices(
        states=states,
        histogram=histogram,
    )
    source_plans = tuple(
        _source_plan_from_split_choice(
            state=state,
            selected_index=selected_index,
            histogram=histogram,
            split_objective=objective,
        )
        for state, selected_index in zip(states, choices, strict=True)
    )
    requests = tuple(
        request
        for source_plan in source_plans
        for request in source_plan.requests
    )
    if len({request.request_id for request in requests}) != len(requests):
        raise ContinuousPlanningError(
            "global split planning produced duplicate request IDs"
        )

    receipt = {
        "schema_version": 1,
        "algorithm": "continuous_bcfy_feeds_global_split_ecdf_v1",
        "split": split.value,
        "continuous_source_inputs_sha256": source_inputs_digest,
        "state_isolation": (
            "this receipt and coordinate search contain only this final split; "
            "train and eval must invoke plan_continuous_split separately"
        ),
        "source_count": len(states),
        "speech_owner_count": sum(len(state.speeches) for state in states),
        "request_count": len(requests),
        "production_histogram": {
            "unit": "integer_millisecond",
            "request_count": histogram.total,
            "unique_duration_count": len(histogram.support_ms),
            "sha256": stable_digest(
                {
                    str(int(duration)): count
                    for duration, count in zip(
                        histogram.support_ms,
                        histogram.counts,
                        strict=True,
                    )
                }
            ),
        },
        "selection_scope": (
            "aggregate exact duration ECDF over every reconstructed "
            f"bcfy_feeds request in final {split.value}"
        ),
        "outcome_blind": True,
        "coordinate_search": search_receipt,
        "selected_objective": objective.receipt(),
        "source_selections": [
            {
                "source_uri": state.source.media.uri,
                "generation": state.source.media.generation,
                "locally_best_policy": state.candidates[
                    state.local_choice_index
                ].name,
                "globally_selected_policy": state.candidates[choice].name,
                "global_selection_differs_from_local": (
                    choice != state.local_choice_index
                ),
                "source_receipt_sha256": stable_digest(source_plan.receipt),
            }
            for state, choice, source_plan in zip(
                states,
                choices,
                source_plans,
                strict=True,
            )
        ],
        "hard_invariants": {
            "every_source_appears_once": True,
            "every_speech_owner_exactly_once_within_source": True,
            "no_cross_source_requests": True,
            "no_optimizer_state_shared_with_other_splits": True,
        },
    }
    return ContinuousSplitPlan(
        requests=requests,
        source_plans=source_plans,
        receipt=receipt,
    )


def _enumerate_split_source_worker(
    arguments: tuple[ContinuousSourceInput, _Histogram],
) -> _SplitSourceState:
    source, histogram = arguments
    return _enumerate_split_source(source, histogram)


def _continuous_source_enumeration_order(
    ordered_sources: Sequence[ContinuousSourceInput],
) -> tuple[int, ...]:
    """Schedule structurally largest sources first, preserving final order."""

    return tuple(
        sorted(
            range(len(ordered_sources)),
            key=lambda index: (
                -_continuous_component_count(ordered_sources[index]),
                -len(ordered_sources[index].speeches),
                -ordered_sources[index].media.frame_count,
                ordered_sources[index].media.uri,
                ordered_sources[index].media.generation,
            ),
        )
    )


def _continuous_component_count(source: ContinuousSourceInput) -> int:
    ordered = sorted(
        source.speeches,
        key=lambda owner: (
            owner.atom.start_frame,
            owner.atom.end_frame,
            owner.atom.atom_id,
        ),
    )
    component_count = 0
    current_end = -1
    for owner in ordered:
        if component_count == 0 or owner.atom.start_frame >= current_end:
            component_count += 1
            current_end = owner.atom.end_frame
        else:
            current_end = max(current_end, owner.atom.end_frame)
    return component_count


def _enumerate_split_source(
    source: ContinuousSourceInput,
    histogram: _Histogram,
) -> _SplitSourceState:
    checked_speeches, split = _validate_inputs(
        media=source.media,
        speeches=source.speeches,
        pii_atoms=source.pii_atoms,
        targetless_atoms=source.targetless_atoms,
    )
    speech_union = _merge_intervals(
        (owner.atom.start_frame, owner.atom.end_frame)
        for owner in checked_speeches
    )
    pii_union = _merge_intervals(
        (atom.start_frame, atom.end_frame) for atom in source.pii_atoms
    )
    effective_pii = _subtract_intervals(pii_union, speech_union)
    safe_islands = _complement_intervals(
        effective_pii,
        start=0,
        end=source.media.frame_count,
    )
    components = _speech_components(checked_speeches, safe_islands)
    island_component_indices = _island_component_indices(
        components, len(safe_islands)
    )
    atomic_cuts = frozenset(_all_adjacencies(island_component_indices))
    reference_cuts = _reference_cuts(
        components=components,
        island_component_indices=island_component_indices,
        sample_rate=source.media.sample_rate,
    )

    atomic_groups = _groups_from_cuts(
        components=components,
        safe_islands=safe_islands,
        island_component_indices=island_component_indices,
        cuts=atomic_cuts,
    )
    atomic = _candidate(
        name="atomic",
        spans=tuple(
            _Span(
                group=group,
                start_frame=group.start_frame,
                end_frame=group.end_frame,
            )
            for group in atomic_groups
        ),
        histogram=histogram,
        sample_rate=source.media.sample_rate,
        reference_cuts=reference_cuts,
        search_receipt={
            "method": "one exact Speech-overlap component per request",
        },
    )

    reference_groups = _groups_from_cuts(
        components=components,
        safe_islands=safe_islands,
        island_component_indices=island_component_indices,
        cuts=reference_cuts,
    )
    reference = _candidate(
        name="reference_800ms",
        spans=_reference_spans(
            reference_groups,
            sample_rate=source.media.sample_rate,
        ),
        histogram=histogram,
        sample_rate=source.media.sample_rate,
        reference_cuts=reference_cuts,
        search_receipt={
            "method": (
                "gap<800ms grouping, 500ms post-roll, soft 60s reference"
            ),
            "gap_anchor_ms": _GAP_ANCHOR_MS,
            "postroll_anchor_ms": _POSTROLL_ANCHOR_MS,
            "reference_max_ms": _REFERENCE_MAX_MS,
        },
    )
    targeted, targeted_trials = _targeted_candidate(
        components=components,
        safe_islands=safe_islands,
        island_component_indices=island_component_indices,
        histogram=histogram,
        sample_rate=source.media.sample_rate,
        reference_cuts=reference_cuts,
        atomic_cuts=atomic_cuts,
        wasserstein_scorer=_RankWassersteinScorer(histogram),
    )
    targeted = dataclasses.replace(
        targeted,
        search_receipt={
            **targeted.search_receipt,
            "trial_count": len(targeted_trials),
            "trials_sha256": stable_digest(targeted_trials),
        },
    )
    candidates = (atomic, reference, targeted)
    local_choice_index = min(
        range(len(candidates)),
        key=lambda index: (
            candidates[index].objective.key,
            candidates[index].name,
        ),
    )
    return _SplitSourceState(
        source=source,
        split=split,
        speeches=checked_speeches,
        effective_pii=effective_pii,
        candidates=candidates,
        local_choice_index=local_choice_index,
    )


def _coordinate_split_choices(
    *,
    states: Sequence[_SplitSourceState],
    histogram: _Histogram,
) -> tuple[tuple[int, ...], _SplitObjective, dict[str, Any]]:
    candidate_count = len(states[0].candidates)
    if any(len(state.candidates) != candidate_count for state in states):
        raise AssertionError("source candidate cardinality drifted")
    seeds = {
        tuple(state.local_choice_index for state in states),
        *(
            tuple(candidate_index for _ in states)
            for candidate_index in range(candidate_count)
        ),
    }
    score_cache: dict[tuple[int, ...], _SplitObjective] = {}

    def score(choices: tuple[int, ...]) -> _SplitObjective:
        cached = score_cache.get(choices)
        if cached is None:
            cached = _split_objective(
                states=states,
                choices=choices,
                histogram=histogram,
            )
            score_cache[choices] = cached
        return cached

    seed_rows = [
        {
            "choices": list(seed),
            "policies": [
                state.candidates[choice].name
                for state, choice in zip(states, seed, strict=True)
            ],
            "objective": score(seed).receipt(),
        }
        for seed in sorted(seeds)
    ]
    current = min(
        seeds,
        key=lambda choices: (score(choices).key, choices),
    )
    current_objective = score(current)
    accepted_moves: list[dict[str, Any]] = []
    maximum_passes = 32
    stopped_reason = "single-source coordinate optimum"
    for pass_index in range(maximum_passes):
        changed = False
        source_order = (
            range(len(states))
            if pass_index % 2 == 0
            else range(len(states) - 1, -1, -1)
        )
        for source_index in source_order:
            state = states[source_index]
            neighbors: list[tuple[tuple[int, ...], _SplitObjective, int]] = []
            for candidate_index in range(len(state.candidates)):
                if candidate_index == current[source_index]:
                    continue
                mutable = list(current)
                mutable[source_index] = candidate_index
                choices = tuple(mutable)
                neighbors.append(
                    (
                        choices,
                        score(choices),
                        candidate_index,
                    )
                )
            if not neighbors:
                continue
            best_choices, best_objective, candidate_index = min(
                neighbors,
                key=lambda row: (row[1].key, row[0]),
            )
            if best_objective.key >= current_objective.key:
                continue
            prior_index = current[source_index]
            current = best_choices
            current_objective = best_objective
            changed = True
            accepted_moves.append(
                {
                    "pass": pass_index,
                    "sweep_direction": (
                        "source_uri_ascending"
                        if pass_index % 2 == 0
                        else "source_uri_descending"
                    ),
                    "source_uri": states[source_index].source.media.uri,
                    "from_policy": states[source_index]
                    .candidates[prior_index]
                    .name,
                    "to_policy": states[source_index]
                    .candidates[candidate_index]
                    .name,
                    "objective": current_objective.receipt(),
                }
            )
        if not changed:
            break
    else:
        stopped_reason = (
            "deterministic pass limit reached; selected exact-scored incumbent"
        )

    return (
        current,
        current_objective,
        {
            "method": (
                "alternating-direction deterministic single-source coordinate "
                "sweeps over three frozen candidates per source"
            ),
            "global_optimality_claim": False,
            "seed_count": len(seeds),
            "seeds": seed_rows,
            "maximum_passes": maximum_passes,
            "accepted_move_count": len(accepted_moves),
            "accepted_moves": accepted_moves,
            "exact_assignment_scores_cached": len(score_cache),
            "stopped_reason": stopped_reason,
            "selected_choice_indices": list(current),
        },
    )


def _split_objective(
    *,
    states: Sequence[_SplitSourceState],
    choices: Sequence[int],
    histogram: _Histogram,
) -> _SplitObjective:
    selected = tuple(
        state.candidates[choice]
        for state, choice in zip(states, choices, strict=True)
    )
    duration_ms = [
        fractions.Fraction(
            (span.end_frame - span.start_frame) * 1000,
            state.source.media.sample_rate,
        )
        for state, candidate in zip(states, selected, strict=True)
        for span in candidate.spans
    ]
    displacement_maxima: list[fractions.Fraction] = []
    displacement_totals: list[fractions.Fraction] = []
    overages: list[fractions.Fraction] = []
    latency_maxima: list[fractions.Fraction] = []
    latency_totals: list[fractions.Fraction] = []
    stable_key: list[
        tuple[
            str,
            int,
            str,
            tuple[tuple[int, int, tuple[int, ...]], ...],
        ]
    ] = []
    for state, candidate in zip(states, selected, strict=True):
        rate = state.source.media.sample_rate
        objective = candidate.objective
        displacement_maxima.append(
            fractions.Fraction(
                objective.maximum_reference_boundary_displacement_frames * 1000,
                rate,
            )
        )
        displacement_totals.append(
            fractions.Fraction(
                objective.total_reference_boundary_displacement_frames * 1000,
                rate,
            )
        )
        overages.extend(
            fractions.Fraction(frames * 1000, rate)
            for frames in objective.over_30s_frames_descending
        )
        latency_maxima.append(
            fractions.Fraction(
                objective.maximum_post_speech_latency_frames * 1000,
                rate,
            )
        )
        latency_totals.append(
            fractions.Fraction(
                objective.total_post_speech_latency_frames * 1000,
                rate,
            )
        )
        stable_key.append(
            (
                state.source.media.uri,
                state.source.media.generation,
                candidate.name,
                objective.stable_span_key,
            )
        )
    return _SplitObjective(
        aggregate_wasserstein_1_ms=histogram.wasserstein_1(duration_ms),
        summed_source_wasserstein_1_ms=sum(
            (candidate.objective.wasserstein_1_ms for candidate in selected),
            fractions.Fraction(),
        ),
        reference_cut_mismatches=sum(
            candidate.objective.reference_cut_mismatches
            for candidate in selected
        ),
        maximum_reference_boundary_displacement_ms=max(
            displacement_maxima, default=fractions.Fraction()
        ),
        total_reference_boundary_displacement_ms=sum(
            displacement_totals, fractions.Fraction()
        ),
        over_30s_ms_descending=tuple(sorted(overages, reverse=True)),
        total_over_30s_ms=sum(overages, fractions.Fraction()),
        maximum_post_speech_latency_ms=max(
            latency_maxima, default=fractions.Fraction()
        ),
        total_post_speech_latency_ms=sum(latency_totals, fractions.Fraction()),
        request_count=sum(len(candidate.spans) for candidate in selected),
        stable_selection_key=tuple(stable_key),
    )


def _source_plan_from_split_choice(
    *,
    state: _SplitSourceState,
    selected_index: int,
    histogram: _Histogram,
    split_objective: _SplitObjective,
) -> ContinuousPlan:
    selected = state.candidates[selected_index]
    requests, transcript_receipts = _requests_from_spans(
        selected.spans,
        media=state.source.media,
        split=state.split,
    )
    _verify_selected(
        requests=requests,
        speeches=state.speeches,
        effective_pii=state.effective_pii,
        media=state.source.media,
    )
    receipt = {
        "schema_version": 1,
        "algorithm": "continuous_bcfy_feeds_global_split_source_v1",
        "source": {
            "uri": state.source.media.uri,
            "generation": state.source.media.generation,
            "sample_rate": state.source.media.sample_rate,
            "frame_count": state.source.media.frame_count,
            "split": state.split.value,
        },
        "selection_scope": "globally coordinated within final split",
        "locally_best_policy": state.candidates[state.local_choice_index].name,
        "globally_selected_policy": selected.name,
        "global_selection_differs_from_local": (
            selected_index != state.local_choice_index
        ),
        "aggregate_split_wasserstein_1_ms": _fraction_receipt(
            split_objective.aggregate_wasserstein_1_ms
        ),
        "effective_pii_only_union": _interval_receipt(state.effective_pii),
        "candidates": [
            _candidate_receipt(candidate, state.source.media.sample_rate)
            for candidate in state.candidates
        ],
        "selected_source_objective": selected.objective.receipt(),
        "selected_transcript_reconciliation": transcript_receipts,
        "production_histogram_sha256": stable_digest(
            {
                str(int(duration)): count
                for duration, count in zip(
                    histogram.support_ms,
                    histogram.counts,
                    strict=True,
                )
            }
        ),
        "hard_invariants": {
            "speech_owner_count_input": len(state.speeches),
            "speech_owner_count_output": sum(
                len(request.owner_ids) for request in requests
            ),
            "every_owner_exactly_once": True,
            "requests_are_continuous": True,
            "requests_are_frame_disjoint": True,
            "speech_is_never_cut": True,
            "pii_only_frames_in_requests": 0,
            "standalone_targetless_requests": 0,
        },
    }
    return ContinuousPlan(requests=requests, receipt=receipt)


def verify_persisted_continuous_split(
    plan: ContinuousSplitPlan,
    *,
    sources: Sequence[ContinuousSourceInput],
    production_histogram_ms: Mapping[int | str, int],
) -> dict[str, Any]:
    """Independently bind a loaded plan to inputs, candidates, and exact W1.

    This deliberately does not rerun candidate search.  It reconstructs the
    selected candidate's groups and exact objective from the frozen geometry,
    then proves that the persisted requests, source receipts, split receipt,
    and aggregate objective all describe that same candidate.
    """
    if not isinstance(plan, ContinuousSplitPlan):
        raise ContinuousPlanningError(
            "persisted continuous plan has an unexpected type"
        )
    if not sources:
        raise ContinuousPlanningError(
            "persisted continuous verification requires source inputs"
        )
    ordered_sources = tuple(
        sorted(
            sources,
            key=lambda source: (
                source.media.uri,
                source.media.generation,
            ),
        )
    )
    owner_splits = {
        owner.split
        for source in ordered_sources
        for owner in source.speeches
    }
    if len(owner_splits) != 1:
        raise ContinuousPlanningError(
            "persisted continuous verification observed mixed splits"
        )
    split = next(iter(owner_splits))
    input_digest = continuous_source_inputs_sha256(
        ordered_sources,
        split=split,
    )
    if plan.receipt.get("split") != split.value:
        raise ContinuousPlanningError(
            "persisted continuous split receipt names the wrong split"
        )
    if plan.receipt.get("continuous_source_inputs_sha256") != input_digest:
        raise ContinuousPlanningError(
            "persisted continuous source-input geometry digest mismatch"
        )
    frozen_guards = plan.receipt.get("frozen_input_guards")
    if not isinstance(frozen_guards, Mapping) or frozen_guards.get(
        "continuous_source_inputs_sha256"
    ) != input_digest:
        raise ContinuousPlanningError(
            "persisted continuous frozen guards omit current source geometry"
        )
    if len(plan.source_plans) != len(ordered_sources):
        raise ContinuousPlanningError(
            "persisted continuous source-plan cardinality mismatch"
        )

    histogram = _Histogram.from_milliseconds(production_histogram_ms)
    histogram_digest = stable_digest(
        {
            str(int(duration)): count
            for duration, count in zip(
                histogram.support_ms,
                histogram.counts,
                strict=True,
            )
        }
    )
    production_receipt = plan.receipt.get("production_histogram")
    if not isinstance(production_receipt, Mapping) or (
        production_receipt.get("unit"),
        production_receipt.get("request_count"),
        production_receipt.get("unique_duration_count"),
        production_receipt.get("sha256"),
    ) != (
        "integer_millisecond",
        histogram.total,
        len(histogram.support_ms),
        histogram_digest,
    ):
        raise ContinuousPlanningError(
            "persisted continuous production histogram mismatch"
        )
    source_selections = plan.receipt.get("source_selections")
    if not isinstance(source_selections, list) or len(
        source_selections
    ) != len(ordered_sources):
        raise ContinuousPlanningError(
            "persisted continuous source selections are malformed"
        )
    coordinate_search = plan.receipt.get("coordinate_search")
    if not isinstance(coordinate_search, Mapping):
        raise ContinuousPlanningError(
            "persisted continuous coordinate-search receipt is malformed"
        )
    selected_choice_indices = coordinate_search.get(
        "selected_choice_indices"
    )
    if not isinstance(selected_choice_indices, list) or len(
        selected_choice_indices
    ) != len(ordered_sources):
        raise ContinuousPlanningError(
            "persisted continuous selected choice indices are malformed"
        )

    reconstructed_states: list[_SplitSourceState] = []
    flattened_requests: list[Request] = []
    for source_index, (source, source_plan, source_selection) in enumerate(
        zip(
        ordered_sources,
        plan.source_plans,
            source_selections,
        strict=True,
        )
    ):
        receipt = source_plan.receipt
        source_receipt = receipt.get("source")
        if not isinstance(source_receipt, Mapping) or (
            source_receipt.get("uri"),
            source_receipt.get("generation"),
            source_receipt.get("sample_rate"),
            source_receipt.get("frame_count"),
            source_receipt.get("split"),
        ) != (
            source.media.uri,
            source.media.generation,
            source.media.sample_rate,
            source.media.frame_count,
            split.value,
        ):
            raise ContinuousPlanningError(
                "persisted continuous source receipt identity mismatch"
            )
        selected_name = receipt.get("globally_selected_policy")
        if not isinstance(selected_name, str):
            raise ContinuousPlanningError(
                "persisted continuous selected policy is malformed"
            )
        candidate_rows = receipt.get("candidates")
        if not isinstance(candidate_rows, list):
            raise ContinuousPlanningError(
                "persisted continuous source receipt lacks candidates"
            )
        candidate_names = [
            row.get("name") for row in candidate_rows if isinstance(row, Mapping)
        ]
        if len(candidate_names) != len(candidate_rows) or len(
            candidate_names
        ) != len(set(candidate_names)):
            raise ContinuousPlanningError(
                "persisted continuous candidate names are malformed"
            )
        matches = [
            row
            for row in candidate_rows
            if isinstance(row, Mapping) and row.get("name") == selected_name
        ]
        if len(matches) != 1:
            raise ContinuousPlanningError(
                "persisted continuous selected candidate is not unique"
            )
        candidate = matches[0]
        candidate_requests = candidate.get("requests")
        candidate_objective = candidate.get("objective")
        if not isinstance(candidate_requests, list) or not isinstance(
            candidate_objective, Mapping
        ):
            raise ContinuousPlanningError(
                "persisted continuous selected candidate is malformed"
            )
        if receipt.get("selected_source_objective") != candidate_objective:
            raise ContinuousPlanningError(
                "persisted continuous source objective differs from candidate"
            )
        selected_candidate_index = candidate_rows.index(candidate)
        if selected_choice_indices[source_index] != selected_candidate_index:
            raise ContinuousPlanningError(
                "persisted selected choice index differs from source policy"
            )
        if not isinstance(source_selection, Mapping) or (
            source_selection.get("source_uri"),
            source_selection.get("generation"),
            source_selection.get("locally_best_policy"),
            source_selection.get("globally_selected_policy"),
            source_selection.get("global_selection_differs_from_local"),
            source_selection.get("source_receipt_sha256"),
        ) != (
            source.media.uri,
            source.media.generation,
            receipt.get("locally_best_policy"),
            selected_name,
            receipt.get("global_selection_differs_from_local"),
            stable_digest(receipt),
        ):
            raise ContinuousPlanningError(
                "persisted split/source selection receipts disagree"
            )
        if receipt.get("production_histogram_sha256") != histogram_digest:
            raise ContinuousPlanningError(
                "persisted source production histogram mismatch"
            )

        checked_speeches, checked_split = _validate_inputs(
            media=source.media,
            speeches=source.speeches,
            pii_atoms=source.pii_atoms,
            targetless_atoms=source.targetless_atoms,
        )
        if checked_split is not split:
            raise ContinuousPlanningError(
                "persisted source ownership differs from split"
            )
        speech_union = _merge_intervals(
            (owner.atom.start_frame, owner.atom.end_frame)
            for owner in checked_speeches
        )
        effective_pii = _subtract_intervals(
            _merge_intervals(
                (atom.start_frame, atom.end_frame)
                for atom in source.pii_atoms
            ),
            speech_union,
        )
        safe_islands = _complement_intervals(
            effective_pii,
            start=0,
            end=source.media.frame_count,
        )
        components = _speech_components(checked_speeches, safe_islands)
        island_component_indices = _island_component_indices(
            components,
            len(safe_islands),
        )
        reference_cuts = _reference_cuts(
            components=components,
            island_component_indices=island_component_indices,
            sample_rate=source.media.sample_rate,
        )

        component_groups: list[tuple[int, ...]] = []
        parsed_bounds: list[tuple[int, int]] = []
        for request_row in candidate_requests:
            if not isinstance(request_row, Mapping):
                raise ContinuousPlanningError(
                    "persisted continuous candidate request is malformed"
                )
            raw_indices = request_row.get("component_indices")
            start_frame = request_row.get("start_frame")
            end_frame = request_row.get("end_frame")
            if (
                not isinstance(raw_indices, list)
                or not raw_indices
                or not all(
                    isinstance(value, int) and not isinstance(value, bool)
                    for value in raw_indices
                )
                or not isinstance(start_frame, int)
                or isinstance(start_frame, bool)
                or not isinstance(end_frame, int)
                or isinstance(end_frame, bool)
            ):
                raise ContinuousPlanningError(
                    "persisted continuous candidate request geometry is invalid"
                )
            indices = tuple(raw_indices)
            if indices != tuple(range(indices[0], indices[-1] + 1)):
                raise ContinuousPlanningError(
                    "persisted candidate components are not contiguous"
                )
            if indices[0] < 0 or indices[-1] >= len(components):
                raise ContinuousPlanningError(
                    "persisted candidate component index is out of range"
                )
            component_groups.append(indices)
            parsed_bounds.append((start_frame, end_frame))
        if tuple(
            component_index
            for values in component_groups
            for component_index in values
        ) != tuple(range(len(components))):
            raise ContinuousPlanningError(
                "persisted candidate does not partition Speech components"
            )
        if any(
            len({components[index].island_index for index in values}) != 1
            for values in component_groups
        ):
            raise ContinuousPlanningError(
                "persisted candidate crosses a PII-safe island"
            )
        selected_cuts = frozenset(
            (left[-1], right[0])
            for left, right in zip(
                component_groups,
                component_groups[1:],
                strict=False,
            )
            if components[left[-1]].island_index
            == components[right[0]].island_index
        )
        reconstructed_groups = _groups_from_cuts(
            components=components,
            safe_islands=safe_islands,
            island_component_indices=island_component_indices,
            cuts=selected_cuts,
        )
        if tuple(
            group.component_indices for group in reconstructed_groups
        ) != tuple(component_groups):
            raise ContinuousPlanningError(
                "persisted candidate group partition is not reconstructible"
            )
        spans = tuple(
            _Span(group=group, start_frame=start, end_frame=end)
            for group, (start, end) in zip(
                reconstructed_groups,
                parsed_bounds,
                strict=True,
            )
        )
        if any(
            not (
                span.group.lower_context_frame
                <= span.start_frame
                <= span.group.start_frame
                < span.group.end_frame
                <= span.end_frame
                <= span.group.upper_context_frame
            )
            for span in spans
        ):
            raise ContinuousPlanningError(
                "persisted candidate span is outside legal context bounds"
            )
        reconstructed_objective = _score(
            spans=spans,
            histogram=histogram,
            sample_rate=source.media.sample_rate,
            reference_cuts=reference_cuts,
        )
        if candidate_objective != reconstructed_objective.receipt():
            raise ContinuousPlanningError(
                "persisted selected candidate objective failed recomputation"
            )
        reconstructed_candidate = _Candidate(
            name=selected_name,
            spans=spans,
            objective=reconstructed_objective,
            search_receipt={},
        )
        canonical_candidate_requests = _candidate_receipt(
            reconstructed_candidate,
            source.media.sample_rate,
        )["requests"]
        if candidate_requests != canonical_candidate_requests:
            raise ContinuousPlanningError(
                "persisted selected candidate requests failed recomputation"
            )
        expected_requests, expected_reconciliation = _requests_from_spans(
            spans,
            media=source.media,
            split=split,
        )
        if source_plan.requests != expected_requests:
            raise ContinuousPlanningError(
                "persisted requests differ from globally selected candidate"
            )
        if receipt.get(
            "selected_transcript_reconciliation"
        ) != expected_reconciliation:
            raise ContinuousPlanningError(
                "persisted transcript reconciliation differs from requests"
            )
        if receipt.get("effective_pii_only_union") != _interval_receipt(
            effective_pii
        ):
            raise ContinuousPlanningError(
                "persisted effective PII receipt differs from current geometry"
            )
        _verify_selected(
            requests=source_plan.requests,
            speeches=checked_speeches,
            effective_pii=effective_pii,
            media=source.media,
        )
        reconstructed_states.append(
            _SplitSourceState(
                source=source,
                split=split,
                speeches=checked_speeches,
                effective_pii=effective_pii,
                candidates=(reconstructed_candidate,),
                local_choice_index=0,
            )
        )
        flattened_requests.extend(source_plan.requests)

    if tuple(flattened_requests) != plan.requests:
        raise ContinuousPlanningError(
            "persisted split requests differ from source-plan concatenation"
        )
    expected_objective = _split_objective(
        states=reconstructed_states,
        choices=(0,) * len(reconstructed_states),
        histogram=histogram,
    )
    if plan.receipt.get("selected_objective") != expected_objective.receipt():
        raise ContinuousPlanningError(
            "persisted split selected objective failed exact recomputation"
        )
    for source_plan in plan.source_plans:
        if source_plan.receipt.get(
            "aggregate_split_wasserstein_1_ms"
        ) != _fraction_receipt(expected_objective.aggregate_wasserstein_1_ms):
            raise ContinuousPlanningError(
                "persisted source receipt has the wrong aggregate W1"
            )
    if (
        plan.receipt.get("source_count"),
        plan.receipt.get("speech_owner_count"),
        plan.receipt.get("request_count"),
    ) != (
        len(ordered_sources),
        sum(len(source.speeches) for source in ordered_sources),
        len(plan.requests),
    ):
        raise ContinuousPlanningError(
            "persisted continuous split counts failed recomputation"
        )
    return {
        "continuous_source_inputs_sha256": input_digest,
        "source_count": len(ordered_sources),
        "request_count": len(plan.requests),
        "speech_owner_count": sum(
            len(source.speeches) for source in ordered_sources
        ),
        "aggregate_exact_wasserstein_1_ms": _fraction_receipt(
            expected_objective.aggregate_wasserstein_1_ms
        ),
        "selected_candidate_count": len(reconstructed_states),
        "verified": True,
    }


def _validate_inputs(
    *,
    media: SourceMedia,
    speeches: Sequence[OwnedSpeech],
    pii_atoms: Sequence[Atom],
    targetless_atoms: Sequence[Atom],
) -> tuple[tuple[OwnedSpeech, ...], Split]:
    if media.sample_rate <= 0 or media.frame_count <= 0:
        raise ContinuousPlanningError("source media geometry must be positive")
    if not speeches:
        raise ContinuousPlanningError(
            "continuous planning requires at least one Speech owner"
        )
    ordered = tuple(
        sorted(
            speeches,
            key=lambda owner: (
                owner.atom.start_frame,
                owner.atom.end_frame,
                owner.atom.atom_id,
            ),
        )
    )
    split = ordered[0].split
    owner_ids: set[str] = set()
    for owner in ordered:
        atom = owner.atom
        if atom.atom_id in owner_ids:
            raise ContinuousPlanningError(
                f"duplicate Speech owner: {atom.atom_id}"
            )
        owner_ids.add(atom.atom_id)
        if atom.family != "bcfy_feeds":
            raise ContinuousPlanningError(
                "continuous planner accepts only bcfy_feeds"
            )
        if atom.source_uri != media.uri:
            raise ContinuousPlanningError("mixed source URI")
        if owner.split is not split:
            raise ContinuousPlanningError("mixed split ownership")
        _validate_bounds(atom, media)

    annotation_ids = set(owner_ids)
    for expected_class, atoms in (
        (AtomClass.PII, pii_atoms),
        (AtomClass.TARGETLESS, targetless_atoms),
    ):
        for atom in atoms:
            if atom.atom_id in annotation_ids:
                raise ContinuousPlanningError(
                    f"duplicate annotation ID: {atom.atom_id}"
                )
            annotation_ids.add(atom.atom_id)
            if atom.atom_class is not expected_class:
                raise ContinuousPlanningError(
                    f"{atom.atom_id} has unexpected annotation class"
                )
            if atom.source_uri != media.uri:
                raise ContinuousPlanningError("mixed source URI")
            _validate_bounds(atom, media)
    return ordered, split


def _validate_bounds(atom: Atom, media: SourceMedia) -> None:
    if atom.start_frame < 0 or atom.end_frame > media.frame_count:
        raise ContinuousPlanningError(
            f"{atom.atom_id} lies outside decoded source frames"
        )


def _speech_components(
    speeches: Sequence[OwnedSpeech],
    safe_islands: Sequence[tuple[int, int]],
) -> tuple[_Component, ...]:
    raw_components: list[list[OwnedSpeech]] = []
    current_end = -1
    for owner in speeches:
        if not raw_components or owner.atom.start_frame >= current_end:
            raw_components.append([owner])
            current_end = owner.atom.end_frame
        else:
            raw_components[-1].append(owner)
            current_end = max(current_end, owner.atom.end_frame)

    result: list[_Component] = []
    for index, owners in enumerate(raw_components):
        start = min(owner.atom.start_frame for owner in owners)
        end = max(owner.atom.end_frame for owner in owners)
        island_matches = [
            island_index
            for island_index, (island_start, island_end) in enumerate(
                safe_islands
            )
            if island_start <= start and end <= island_end
        ]
        if len(island_matches) != 1:
            raise ContinuousPlanningError(
                "Speech component does not belong to exactly one PII-safe island"
            )
        result.append(
            _Component(
                index=index,
                island_index=island_matches[0],
                start_frame=start,
                end_frame=end,
                owners=tuple(owners),
            )
        )
    return tuple(result)


def _island_component_indices(
    components: Sequence[_Component], island_count: int
) -> tuple[tuple[int, ...], ...]:
    grouped: list[list[int]] = [[] for _ in range(island_count)]
    for component in components:
        grouped[component.island_index].append(component.index)
    return tuple(tuple(values) for values in grouped)


def _all_adjacencies(
    island_component_indices: Sequence[Sequence[int]],
) -> tuple[tuple[int, int], ...]:
    return tuple(
        (left, right)
        for indices in island_component_indices
        for left, right in zip(indices, indices[1:], strict=False)
    )


def _reference_cuts(
    *,
    components: Sequence[_Component],
    island_component_indices: Sequence[Sequence[int]],
    sample_rate: int,
) -> frozenset[tuple[int, int]]:
    cuts: set[tuple[int, int]] = set()
    gap_anchor = _ms_to_frames(_GAP_ANCHOR_MS, sample_rate)
    max_anchor = _ms_to_frames(_REFERENCE_MAX_MS, sample_rate)
    for indices in island_component_indices:
        if not indices:
            continue
        group_start = components[indices[0]].start_frame
        prior = indices[0]
        for current in indices[1:]:
            left = components[prior]
            right = components[current]
            gap = right.start_frame - left.end_frame
            prospective = right.end_frame - group_start
            if gap >= gap_anchor or prospective > max_anchor:
                cuts.add((prior, current))
                group_start = right.start_frame
            prior = current
    return frozenset(cuts)


def _groups_from_cuts(
    *,
    components: Sequence[_Component],
    safe_islands: Sequence[tuple[int, int]],
    island_component_indices: Sequence[Sequence[int]],
    cuts: frozenset[tuple[int, int]],
) -> tuple[_Group, ...]:
    result: list[_Group] = []
    for island_index, indices in enumerate(island_component_indices):
        if not indices:
            continue
        grouped_indices: list[list[int]] = [[indices[0]]]
        for prior, current in zip(indices, indices[1:], strict=False):
            if (prior, current) in cuts:
                grouped_indices.append([current])
            else:
                grouped_indices[-1].append(current)

        fences: list[int] = []
        for left_group, right_group in zip(
            grouped_indices, grouped_indices[1:], strict=False
        ):
            left_end = components[left_group[-1]].end_frame
            right_start = components[right_group[0]].start_frame
            fences.append(left_end + (right_start - left_end) // 2)

        island_start, island_end = safe_islands[island_index]
        for group_index, values in enumerate(grouped_indices):
            first = components[values[0]]
            last = components[values[-1]]
            lower = (
                island_start if group_index == 0 else fences[group_index - 1]
            )
            upper = (
                island_end
                if group_index == len(grouped_indices) - 1
                else fences[group_index]
            )
            owners = tuple(
                owner
                for component_index in values
                for owner in components[component_index].owners
            )
            result.append(
                _Group(
                    component_indices=tuple(values),
                    island_index=island_index,
                    start_frame=first.start_frame,
                    end_frame=last.end_frame,
                    lower_context_frame=lower,
                    upper_context_frame=upper,
                    owners=owners,
                )
            )
    return tuple(result)


def _reference_spans(
    groups: Sequence[_Group], *, sample_rate: int
) -> tuple[_Span, ...]:
    postroll = _ms_to_frames(_POSTROLL_ANCHOR_MS, sample_rate)
    return tuple(
        _Span(
            group=group,
            start_frame=group.start_frame,
            end_frame=min(
                group.upper_context_frame,
                group.end_frame + postroll,
            ),
        )
        for group in groups
    )


def _targeted_candidate(
    *,
    components: Sequence[_Component],
    safe_islands: Sequence[tuple[int, int]],
    island_component_indices: Sequence[Sequence[int]],
    histogram: _Histogram,
    sample_rate: int,
    reference_cuts: frozenset[tuple[int, int]],
    atomic_cuts: frozenset[tuple[int, int]],
    memoize_optimized_partitions: bool = True,
    wasserstein_scorer: _RankWassersteinScorer | None = None,
    use_incremental_wasserstein: bool = True,
) -> tuple[_Candidate, list[dict[str, Any]]]:
    trials: list[dict[str, Any]] = []
    optimized_cache: dict[frozenset[tuple[int, int]], _Candidate] = {}
    cache_hits = 0
    if wasserstein_scorer is None:
        wasserstein_scorer = _RankWassersteinScorer(histogram)

    def optimized(cuts: frozenset[tuple[int, int]]) -> _Candidate:
        nonlocal cache_hits
        if memoize_optimized_partitions:
            cached = optimized_cache.get(cuts)
            if cached is not None:
                cache_hits += 1
                return cached
        candidate = _optimized_candidate_for_cuts(
            name="ecdf_targeted",
            cuts=cuts,
            components=components,
            safe_islands=safe_islands,
            island_component_indices=island_component_indices,
            histogram=histogram,
            sample_rate=sample_rate,
            reference_cuts=reference_cuts,
            wasserstein_scorer=wasserstein_scorer,
            use_incremental_wasserstein=use_incremental_wasserstein,
        )
        if memoize_optimized_partitions:
            optimized_cache[cuts] = candidate
        return candidate

    starts: list[tuple[frozenset[tuple[int, int]], _Candidate]] = []
    for label, cuts in (
        ("atomic_partition", atomic_cuts),
        ("reference_partition", reference_cuts),
    ):
        candidate = optimized(cuts)
        starts.append((cuts, candidate))
        trials.append(
            {
                "phase": "initial",
                "partition": label,
                "objective": candidate.objective.receipt(),
            }
        )

    current_cuts, current = min(
        starts,
        key=lambda pair: (pair[1].objective.key, _cuts_key(pair[0])),
    )
    all_adjacencies = _all_adjacencies(island_component_indices)
    accepted_moves = 0
    evaluated_neighbors = 0
    for pass_index in range(_LOCAL_SEARCH_PASSES):
        neighbors: list[
            tuple[frozenset[tuple[int, int]], _Candidate, tuple[int, int]]
        ] = []
        for adjacency in all_adjacencies:
            mutable = set(current_cuts)
            if adjacency in mutable:
                mutable.remove(adjacency)
            else:
                mutable.add(adjacency)
            neighbor_cuts = frozenset(mutable)
            neighbor = optimized(neighbor_cuts)
            evaluated_neighbors += 1
            neighbors.append((neighbor_cuts, neighbor, adjacency))
        if not neighbors:
            break
        best_cuts, best, moved_adjacency = min(
            neighbors,
            key=lambda item: (
                item[1].objective.key,
                _cuts_key(item[0]),
            ),
        )
        if best.objective.key >= current.objective.key:
            trials.append(
                {
                    "phase": "local_search_stop",
                    "pass": pass_index,
                    "reason": "no exact lexicographic single-cut improvement",
                }
            )
            break
        current_cuts, current = best_cuts, best
        accepted_moves += 1
        trials.append(
            {
                "phase": "accepted_single_cut_toggle",
                "pass": pass_index,
                "adjacency": list(moved_adjacency),
                "objective": current.objective.receipt(),
            }
        )

    current = dataclasses.replace(
        current,
        search_receipt={
            "method": (
                "every initial and single-cut-neighbor partition receives the "
                "same exact deterministic coordinate-bound optimization; "
                "memoization only reuses byte-identical cut-set results"
            ),
            "maximum_partition_passes": _LOCAL_SEARCH_PASSES,
            "maximum_bound_passes": _BOUND_SEARCH_PASSES,
            "accepted_partition_moves": accepted_moves,
            "evaluated_partition_neighbors": evaluated_neighbors,
            "optimized_partition_cache_enabled": (memoize_optimized_partitions),
            "optimized_partition_cache_hits": cache_hits,
            "unique_optimized_partition_count": (
                len(optimized_cache)
                if memoize_optimized_partitions
                else evaluated_neighbors + 2
            ),
            "final_cuts": [list(value) for value in sorted(current_cuts)],
            "status": (
                "exact single-cut local optimum under the frozen coordinate-"
                "bound optimizer; no global optimum claim"
            ),
        },
    )
    return current, trials


def _optimized_candidate_for_cuts(
    *,
    name: str,
    cuts: frozenset[tuple[int, int]],
    components: Sequence[_Component],
    safe_islands: Sequence[tuple[int, int]],
    island_component_indices: Sequence[Sequence[int]],
    histogram: _Histogram,
    sample_rate: int,
    reference_cuts: frozenset[tuple[int, int]],
    wasserstein_scorer: _RankWassersteinScorer,
    use_incremental_wasserstein: bool = True,
) -> _Candidate:
    groups = _groups_from_cuts(
        components=components,
        safe_islands=safe_islands,
        island_component_indices=island_component_indices,
        cuts=cuts,
    )
    spans = list(_reference_spans(groups, sample_rate=sample_rate))
    current = _candidate(
        name=name,
        spans=tuple(spans),
        histogram=histogram,
        sample_rate=sample_rate,
        reference_cuts=reference_cuts,
        search_receipt={},
    )
    wasserstein_state = (
        _ObservedWassersteinState.from_values(
            wasserstein_scorer,
            _span_duration_ms(spans, sample_rate),
        )
        if use_incremental_wasserstein
        else None
    )
    if (
        wasserstein_state is not None
        and wasserstein_state.wasserstein_1_ms
        != current.objective.wasserstein_1_ms
    ):
        raise AssertionError("rank-coupling W1 differs from full ECDF W1")
    for _ in range(_BOUND_SEARCH_PASSES):
        improved = False
        for span_index in range(len(spans)):
            alternatives = _bound_candidates(
                spans=spans,
                span_index=span_index,
                histogram=histogram,
                sample_rate=sample_rate,
            )
            evaluated: list[_Candidate] = []
            old_duration_ms = _one_span_duration_ms(
                spans[span_index],
                sample_rate,
            )
            if wasserstein_state is None:
                exact_primary_scores = [
                    (start, end, None) for start, end in alternatives
                ]
            else:
                exact_primary_scores = []
                for start, end in alternatives:
                    new_duration_ms = fractions.Fraction(
                        (end - start) * 1000,
                        sample_rate,
                    )
                    exact_primary_scores.append(
                        (
                            start,
                            end,
                            wasserstein_state.replacing(
                                old_duration_ms,
                                new_duration_ms,
                            ),
                        )
                    )
                minimum_wasserstein = min(
                    value
                    for _, _, value in exact_primary_scores
                    if value is not None
                )
                # W1 is the first lexicographic field.  Alternatives with a
                # strictly larger exact W1 cannot win, so only exact primary
                # ties need the remaining full integer objective scan.
                exact_primary_scores = [
                    row
                    for row in exact_primary_scores
                    if row[2] == minimum_wasserstein
                ]
            for start, end, exact_wasserstein in exact_primary_scores:
                trial_spans = list(spans)
                trial_spans[span_index] = _Span(
                    group=spans[span_index].group,
                    start_frame=start,
                    end_frame=end,
                )
                evaluated.append(
                    _candidate(
                        name=name,
                        spans=tuple(trial_spans),
                        histogram=histogram,
                        sample_rate=sample_rate,
                        reference_cuts=reference_cuts,
                        search_receipt={},
                        exact_wasserstein_1_ms=exact_wasserstein,
                    )
                )
            best = min(
                evaluated,
                key=lambda candidate: candidate.objective.key,
            )
            if best.objective.key < current.objective.key:
                spans = list(best.spans)
                current = best
                if wasserstein_state is not None:
                    wasserstein_state = (
                        _ObservedWassersteinState.from_values(
                            wasserstein_scorer,
                            _span_duration_ms(spans, sample_rate),
                        )
                    )
                    if (
                        wasserstein_state.wasserstein_1_ms
                        != current.objective.wasserstein_1_ms
                    ):
                        raise AssertionError(
                            "accepted incremental W1 differs from candidate"
                        )
                improved = True
        if not improved:
            break
    return current


def _bound_candidates(
    *,
    spans: Sequence[_Span],
    span_index: int,
    histogram: _Histogram,
    sample_rate: int,
) -> tuple[tuple[int, int], ...]:
    span = spans[span_index]
    group = span.group
    candidates: set[tuple[int, int]] = {
        (group.start_frame, group.end_frame),
        (
            group.start_frame,
            min(
                group.upper_context_frame,
                group.end_frame
                + _ms_to_frames(_POSTROLL_ANCHOR_MS, sample_rate),
            ),
        ),
        (group.lower_context_frame, group.upper_context_frame),
        (span.start_frame, span.end_frame),
    }
    duration_ms = fractions.Fraction(
        (span.end_frame - span.start_frame) * 1000,
        sample_rate,
    )
    sorted_durations = sorted(
        fractions.Fraction(
            (item.end_frame - item.start_frame) * 1000,
            sample_rate,
        )
        for item in spans
    )
    rank = bisect.bisect_left(sorted_durations, duration_ms)
    request_count = len(spans)
    target_values: set[fractions.Fraction] = set()
    for candidate_rank in range(
        max(0, rank - 2),
        min(request_count, rank + 3),
    ):
        target_values.add(
            histogram.quantile(2 * candidate_rank + 1, 2 * request_count)
        )
    for numerator, denominator in (
        (1, 10),
        (1, 4),
        (1, 2),
        (3, 4),
        (9, 10),
        (19, 20),
        (99, 100),
    ):
        target_values.add(histogram.quantile(numerator, denominator))
    for value in (
        duration_ms,
        fractions.Fraction(
            (group.end_frame - group.start_frame) * 1000,
            sample_rate,
        ),
        fractions.Fraction(
            (group.upper_context_frame - group.lower_context_frame) * 1000,
            sample_rate,
        ),
    ):
        target_values.update(histogram.nearby_support(value))

    core_frames = group.end_frame - group.start_frame
    available_frames = group.upper_context_frame - group.lower_context_frame
    for target_ms in target_values:
        target_frames = _fraction_ms_to_frames(target_ms, sample_rate)
        target_frames = min(
            available_frames,
            max(core_frames, target_frames),
        )
        # Trailing-first mirrors the post-roll reference.
        trailing_end = min(
            group.upper_context_frame,
            group.start_frame + target_frames,
        )
        trailing_start = trailing_end - target_frames
        if trailing_start > group.start_frame:
            trailing_start = group.start_frame
            trailing_end = trailing_start + target_frames
        candidates.add((trailing_start, trailing_end))

        # Leading-first is also legal natural context and can be closer to the
        # target when the trailing side is constrained by a PII island.
        leading_start = max(
            group.lower_context_frame,
            group.end_frame - target_frames,
        )
        leading_end = leading_start + target_frames
        if leading_end < group.end_frame:
            leading_end = group.end_frame
            leading_start = leading_end - target_frames
        candidates.add((leading_start, leading_end))

    checked = tuple(
        sorted(
            (start, end)
            for start, end in candidates
            if (
                group.lower_context_frame <= start <= group.start_frame
                and group.end_frame <= end <= group.upper_context_frame
                and start < end
            )
        )
    )
    if not checked:
        raise AssertionError("every Speech group has at least its core bounds")
    return checked


def _candidate(
    *,
    name: str,
    spans: tuple[_Span, ...],
    histogram: _Histogram,
    sample_rate: int,
    reference_cuts: frozenset[tuple[int, int]],
    search_receipt: dict[str, Any],
    exact_wasserstein_1_ms: fractions.Fraction | None = None,
) -> _Candidate:
    objective = _score(
        spans=spans,
        histogram=histogram,
        sample_rate=sample_rate,
        reference_cuts=reference_cuts,
        exact_wasserstein_1_ms=exact_wasserstein_1_ms,
    )
    return _Candidate(
        name=name,
        spans=spans,
        objective=objective,
        search_receipt=search_receipt,
    )


def _score(
    *,
    spans: Sequence[_Span],
    histogram: _Histogram,
    sample_rate: int,
    reference_cuts: frozenset[tuple[int, int]],
    exact_wasserstein_1_ms: fractions.Fraction | None = None,
) -> CandidateObjective:
    actual_cuts = frozenset(
        (left.group.component_indices[-1], right.group.component_indices[0])
        for left, right in zip(spans, spans[1:], strict=False)
        if left.group.island_index == right.group.island_index
    )
    cut_mismatches = len(actual_cuts.symmetric_difference(reference_cuts))
    postroll = _ms_to_frames(_POSTROLL_ANCHOR_MS, sample_rate)
    displacements: list[int] = []
    latencies: list[int] = []
    overages: list[int] = []
    duration_ms: list[fractions.Fraction] | None = (
        [] if exact_wasserstein_1_ms is None else None
    )
    stable_key: list[tuple[int, int, tuple[int, ...]]] = []
    target_30_frames = _ms_to_frames(_VERY_SOFT_TARGET_MS, sample_rate)
    for span in spans:
        reference_end = min(
            span.group.upper_context_frame,
            span.group.end_frame + postroll,
        )
        displacements.append(
            abs(span.start_frame - span.group.start_frame)
            + abs(span.end_frame - reference_end)
        )
        latencies.append(span.end_frame - span.group.end_frame)
        frames = span.end_frame - span.start_frame
        if duration_ms is not None:
            duration_ms.append(
                fractions.Fraction(frames * 1000, sample_rate)
            )
        if frames > target_30_frames:
            overages.append(frames - target_30_frames)
        stable_key.append(
            (
                span.start_frame,
                span.end_frame,
                span.group.component_indices,
            )
        )
    return CandidateObjective(
        wasserstein_1_ms=(
            histogram.wasserstein_1(duration_ms)
            if duration_ms is not None
            else exact_wasserstein_1_ms
        ),
        reference_cut_mismatches=cut_mismatches,
        maximum_reference_boundary_displacement_frames=max(
            displacements, default=0
        ),
        total_reference_boundary_displacement_frames=sum(displacements),
        over_30s_frames_descending=tuple(sorted(overages, reverse=True)),
        total_over_30s_frames=sum(overages),
        maximum_post_speech_latency_frames=max(latencies, default=0),
        total_post_speech_latency_frames=sum(latencies),
        request_count=len(spans),
        stable_span_key=tuple(stable_key),
    )


def _one_span_duration_ms(
    span: _Span,
    sample_rate: int,
) -> fractions.Fraction:
    return fractions.Fraction(
        (span.end_frame - span.start_frame) * 1000,
        sample_rate,
    )


def _span_duration_ms(
    spans: Sequence[_Span],
    sample_rate: int,
) -> tuple[fractions.Fraction, ...]:
    return tuple(
        _one_span_duration_ms(span, sample_rate) for span in spans
    )


def _requests_from_spans(
    spans: Sequence[_Span],
    *,
    media: SourceMedia,
    split: Split,
) -> tuple[tuple[Request, ...], list[dict[str, Any]]]:
    result: list[Request] = []
    receipts: list[dict[str, Any]] = []
    for ordinal, span in enumerate(spans):
        reconciled = transcripts.reconcile_component(span.group.owners)
        if tuple(reconciled.owner_ids) != span.group.owner_ids:
            raise ContinuousPlanningError(
                "transcript reconciliation changed owner identity/order"
            )
        request_identity = {
            "algorithm": "continuous_bcfy_feeds_exact_frame_v1",
            "source_uri": media.uri,
            "generation": media.generation,
            "split": split.value,
            "ordinal": ordinal,
            "start_frame": span.start_frame,
            "end_frame": span.end_frame,
            "owner_ids": list(span.group.owner_ids),
        }
        request_id = f"bcfy-feeds-{stable_digest(request_identity)[:24]}"
        result.append(
            Request(
                request_id=request_id,
                family="bcfy_feeds",
                split=split,
                source_uri=media.uri,
                start_frame=span.start_frame,
                end_frame=span.end_frame,
                sample_rate=media.sample_rate,
                owner_ids=span.group.owner_ids,
                text=reconciled.text,
            )
        )
        receipts.append(
            {
                "request_id": request_id,
                "reconciliation": reconciled.receipt,
            }
        )
    return tuple(result), receipts


def _verify_selected(
    *,
    requests: Sequence[Request],
    speeches: Sequence[OwnedSpeech],
    effective_pii: Sequence[tuple[int, int]],
    media: SourceMedia,
) -> None:
    expected = sorted(owner.atom.atom_id for owner in speeches)
    observed = sorted(
        owner_id for request in requests for owner_id in request.owner_ids
    )
    if observed != expected:
        raise ContinuousPlanningError(
            "selected requests do not preserve every Speech owner exactly once"
        )
    by_id = {owner.atom.atom_id: owner for owner in speeches}
    prior_end = -1
    for request in requests:
        if request.source_uri != media.uri:
            raise ContinuousPlanningError("selected request changed source")
        if request.start_frame < prior_end:
            raise ContinuousPlanningError("selected requests overlap frames")
        prior_end = request.end_frame
        for owner_id in request.owner_ids:
            owner = by_id[owner_id]
            if not (
                request.start_frame <= owner.atom.start_frame
                and owner.atom.end_frame <= request.end_frame
            ):
                raise ContinuousPlanningError(
                    f"selected request cuts Speech owner {owner_id}"
                )
        for pii_start, pii_end in effective_pii:
            if request.start_frame < pii_end and pii_start < request.end_frame:
                raise ContinuousPlanningError(
                    "selected request contains PII-only frames"
                )


def _candidate_receipt(
    candidate: _Candidate, sample_rate: int
) -> dict[str, Any]:
    return {
        "name": candidate.name,
        "objective": candidate.objective.receipt(),
        "search": candidate.search_receipt,
        "requests": [
            {
                "start_frame": span.start_frame,
                "end_frame": span.end_frame,
                "duration_frames": span.end_frame - span.start_frame,
                "duration_ms_exact": _fraction_receipt(
                    fractions.Fraction(
                        (span.end_frame - span.start_frame) * 1000,
                        sample_rate,
                    )
                ),
                "island_index": span.group.island_index,
                "component_indices": list(span.group.component_indices),
                "owner_ids": list(span.group.owner_ids),
            }
            for span in candidate.spans
        ],
    }


def _merge_intervals(
    intervals: Iterable[tuple[int, int]],
) -> tuple[tuple[int, int], ...]:
    ordered = sorted(intervals)
    result: list[list[int]] = []
    for start, end in ordered:
        if start < 0 or end <= start:
            raise ContinuousPlanningError("invalid half-open frame interval")
        if not result or start > result[-1][1]:
            result.append([start, end])
        else:
            result[-1][1] = max(result[-1][1], end)
    return tuple((start, end) for start, end in result)


def _subtract_intervals(
    bases: Sequence[tuple[int, int]],
    cutters: Sequence[tuple[int, int]],
) -> tuple[tuple[int, int], ...]:
    result: list[tuple[int, int]] = []
    for base_start, base_end in bases:
        cursor = base_start
        for cut_start, cut_end in cutters:
            if cut_end <= cursor:
                continue
            if cut_start >= base_end:
                break
            if cut_start > cursor:
                result.append((cursor, min(cut_start, base_end)))
            cursor = max(cursor, min(cut_end, base_end))
            if cursor >= base_end:
                break
        if cursor < base_end:
            result.append((cursor, base_end))
    return _merge_intervals(result)


def _complement_intervals(
    intervals: Sequence[tuple[int, int]],
    *,
    start: int,
    end: int,
) -> tuple[tuple[int, int], ...]:
    result: list[tuple[int, int]] = []
    cursor = start
    for interval_start, interval_end in intervals:
        if interval_start < start or interval_end > end:
            raise ContinuousPlanningError(
                "PII interval lies outside source geometry"
            )
        if cursor < interval_start:
            result.append((cursor, interval_start))
        cursor = max(cursor, interval_end)
    if cursor < end:
        result.append((cursor, end))
    return tuple(result)


def _interval_receipt(
    intervals: Sequence[tuple[int, int]],
) -> list[dict[str, int]]:
    return [
        {"start_frame": start, "end_frame": end} for start, end in intervals
    ]


def _ms_to_frames(milliseconds: int, sample_rate: int) -> int:
    return (milliseconds * sample_rate + 500) // 1000


def _fraction_ms_to_frames(
    milliseconds: fractions.Fraction, sample_rate: int
) -> int:
    value = milliseconds * sample_rate / 1000
    quotient, remainder = divmod(value.numerator, value.denominator)
    return quotient + int(2 * remainder >= value.denominator)


def _fraction_receipt(value: fractions.Fraction) -> dict[str, Any]:
    with decimal.localcontext() as context:
        context.prec = 30
        rendered = format(
            decimal.Decimal(value.numerator) / value.denominator,
            "f",
        )
    if "." in rendered:
        rendered = rendered.rstrip("0").rstrip(".")
    return {
        "numerator": value.numerator,
        "denominator": value.denominator,
        "decimal": rendered or "0",
    }


def _prefix_sums(
    values: Sequence[fractions.Fraction],
) -> tuple[fractions.Fraction, ...]:
    result = [fractions.Fraction()]
    for value in values:
        result.append(result[-1] + value)
    return tuple(result)


def _cuts_key(
    cuts: frozenset[tuple[int, int]],
) -> tuple[tuple[int, int], ...]:
    return tuple(sorted(cuts))
