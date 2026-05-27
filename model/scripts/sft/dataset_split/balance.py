from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime

from dataset_split.types import LabeledSegment

DURATION_BUCKETS_SECONDS = (
    (0.0, 5.0, "0_5s"),
    (5.0, 15.0, "5_15s"),
    (15.0, 30.0, "15_30s"),
    (30.0, None, "30s_plus"),
)
TRANSCRIPT_WORD_BUCKETS = (
    (0, 3, "0_3_words"),
    (4, 10, "4_10_words"),
    (11, 25, "11_25_words"),
    (26, None, "26_plus_words"),
)
DEFAULT_BALANCE_WEIGHTS = {
    "dataset_rows": 1000.0,
    "dataset_duration": 1000.0,
    "dataset_sources": 750.0,
    "family_rows": 500.0,
    "family_duration": 500.0,
    "global_rows": 250.0,
    "global_duration": 250.0,
    "global_sources": 250.0,
    "duration_bucket_rows": 400.0,
    "transcript_bucket_rows": 400.0,
}

_SPLITS = {"train", "eval"}


@dataclass(frozen=True)
class BalanceComponentDelta:
    name: str
    train_value: float
    eval_value: float
    target_eval_value: float
    absolute_delta: float

    def to_dict(self) -> dict[str, float | str]:
        return {
            "name": self.name,
            "train_value": self.train_value,
            "eval_value": self.eval_value,
            "target_eval_value": self.target_eval_value,
            "absolute_delta": self.absolute_delta,
        }


@dataclass(frozen=True)
class BalanceReport:
    weighted_score: float
    component_deltas: tuple[BalanceComponentDelta, ...]
    duration_buckets: dict[str, dict[str, float]]
    transcript_length_buckets: dict[str, dict[str, float]]
    time_distribution: dict[str, object]

    def to_dict(self) -> dict[str, object]:
        return {
            "weighted_score": self.weighted_score,
            "component_deltas": [
                component.to_dict() for component in self.component_deltas
            ],
            "duration_buckets": self.duration_buckets,
            "transcript_length_buckets": self.transcript_length_buckets,
            "time_distribution": self.time_distribution,
        }


def bucket_duration_seconds(duration: float) -> str:
    for lower, upper, name in DURATION_BUCKETS_SECONDS:
        if duration >= lower and (upper is None or duration < upper):
            return name
    return DURATION_BUCKETS_SECONDS[0][2]


def bucket_transcript_words(text: str) -> str:
    word_count = len(text.strip().split())
    for lower, upper, name in TRANSCRIPT_WORD_BUCKETS:
        if word_count >= lower and (upper is None or word_count <= upper):
            return name
    return TRANSCRIPT_WORD_BUCKETS[0][2]


def build_balance_report(
    segments: tuple[LabeledSegment, ...],
    *,
    train_ratio: float = 0.8,
    eval_ratio: float = 0.2,
    weights: Mapping[str, float] | None = None,
) -> BalanceReport:
    if abs((train_ratio + eval_ratio) - 1.0) > 1e-9:
        raise ValueError("train_ratio + eval_ratio must equal 1.0")
    for segment in segments:
        _require_split(segment)

    effective_weights = dict(DEFAULT_BALANCE_WEIGHTS)
    if weights is not None:
        effective_weights.update(weights)

    component_deltas: list[BalanceComponentDelta] = []
    for dataset_name in sorted({segment.dataset_name for segment in segments}):
        component_deltas.extend(
            (
                _metric_delta(
                    f"dataset:{dataset_name}:rows",
                    segments,
                    lambda segment, expected=dataset_name: (
                        segment.dataset_name == expected
                    ),
                    lambda segment: 1.0,
                    eval_ratio,
                ),
                _metric_delta(
                    f"dataset:{dataset_name}:duration",
                    segments,
                    lambda segment, expected=dataset_name: (
                        segment.dataset_name == expected
                    ),
                    lambda segment: segment.duration,
                    eval_ratio,
                ),
                _source_count_delta(
                    f"dataset:{dataset_name}:sources",
                    segments,
                    lambda segment, expected=dataset_name: (
                        segment.dataset_name == expected
                    ),
                    eval_ratio,
                ),
            )
        )

    for family in sorted({segment.dataset_family for segment in segments}):
        component_deltas.extend(
            (
                _metric_delta(
                    f"family:{family}:rows",
                    segments,
                    lambda segment, expected=family: segment.dataset_family
                    == expected,
                    lambda segment: 1.0,
                    eval_ratio,
                ),
                _metric_delta(
                    f"family:{family}:duration",
                    segments,
                    lambda segment, expected=family: segment.dataset_family
                    == expected,
                    lambda segment: segment.duration,
                    eval_ratio,
                ),
            )
        )

    component_deltas.extend(
        (
            _metric_delta(
                "global:rows",
                segments,
                lambda segment: True,
                lambda segment: 1.0,
                eval_ratio,
            ),
            _metric_delta(
                "global:duration",
                segments,
                lambda segment: True,
                lambda segment: segment.duration,
                eval_ratio,
            ),
            _source_count_delta(
                "global:sources",
                segments,
                lambda segment: True,
                eval_ratio,
            ),
        )
    )

    duration_bucket_report: dict[str, dict[str, float]] = {}
    for _, _, bucket_name in DURATION_BUCKETS_SECONDS:
        component = _metric_delta(
            f"duration_bucket:{bucket_name}:rows",
            segments,
            lambda segment, expected=bucket_name: bucket_duration_seconds(
                segment.duration
            )
            == expected,
            lambda segment: 1.0,
            eval_ratio,
        )
        component_deltas.append(component)
        duration_bucket_report[bucket_name] = _bucket_summary(component)

    transcript_bucket_report: dict[str, dict[str, float]] = {}
    for _, _, bucket_name in TRANSCRIPT_WORD_BUCKETS:
        component = _metric_delta(
            f"transcript_words:{bucket_name}:rows",
            segments,
            lambda segment, expected=bucket_name: bucket_transcript_words(
                segment.text
            )
            == expected,
            lambda segment: 1.0,
            eval_ratio,
        )
        component_deltas.append(component)
        transcript_bucket_report[bucket_name] = _bucket_summary(component)

    weighted_score = sum(
        _weight_for_component(component.name, effective_weights)
        * component.absolute_delta
        for component in component_deltas
    )

    return BalanceReport(
        weighted_score=weighted_score,
        component_deltas=tuple(component_deltas),
        duration_buckets=duration_bucket_report,
        transcript_length_buckets=transcript_bucket_report,
        time_distribution=_time_distribution(segments),
    )


def _metric_delta(
    name: str,
    segments: tuple[LabeledSegment, ...],
    predicate: Callable[[LabeledSegment], bool],
    value: Callable[[LabeledSegment], float],
    eval_ratio: float,
) -> BalanceComponentDelta:
    train_value = 0.0
    eval_value = 0.0
    for segment in segments:
        if not predicate(segment):
            continue
        if segment.split == "train":
            train_value += value(segment)
        else:
            eval_value += value(segment)
    return _delta(name, train_value, eval_value, eval_ratio)


def _source_count_delta(
    name: str,
    segments: tuple[LabeledSegment, ...],
    predicate: Callable[[LabeledSegment], bool],
    eval_ratio: float,
) -> BalanceComponentDelta:
    train_sources: set[str] = set()
    eval_sources: set[str] = set()
    for segment in segments:
        if not predicate(segment):
            continue
        if segment.split == "train":
            train_sources.add(segment.source_group)
        else:
            eval_sources.add(segment.source_group)
    return _delta(name, float(len(train_sources)), float(len(eval_sources)), eval_ratio)


def _delta(
    name: str, train_value: float, eval_value: float, eval_ratio: float
) -> BalanceComponentDelta:
    target_eval_value = (train_value + eval_value) * eval_ratio
    return BalanceComponentDelta(
        name=name,
        train_value=train_value,
        eval_value=eval_value,
        target_eval_value=target_eval_value,
        absolute_delta=abs(eval_value - target_eval_value),
    )


def _bucket_summary(
    component: BalanceComponentDelta,
) -> dict[str, float]:
    return {
        "train": component.train_value,
        "eval": component.eval_value,
        "target_eval": component.target_eval_value,
        "absolute_delta": component.absolute_delta,
    }


def _weight_for_component(
    component_name: str, weights: Mapping[str, float]
) -> float:
    if component_name.startswith("dataset:") and component_name.endswith(":rows"):
        return weights["dataset_rows"]
    if component_name.startswith("dataset:") and component_name.endswith(
        ":duration"
    ):
        return weights["dataset_duration"]
    if component_name.startswith("dataset:") and component_name.endswith(
        ":sources"
    ):
        return weights["dataset_sources"]
    if component_name.startswith("family:") and component_name.endswith(":rows"):
        return weights["family_rows"]
    if component_name.startswith("family:") and component_name.endswith(
        ":duration"
    ):
        return weights["family_duration"]
    if component_name == "global:rows":
        return weights["global_rows"]
    if component_name == "global:duration":
        return weights["global_duration"]
    if component_name == "global:sources":
        return weights["global_sources"]
    if component_name.startswith("duration_bucket:"):
        return weights["duration_bucket_rows"]
    if component_name.startswith("transcript_words:"):
        return weights["transcript_bucket_rows"]
    return 0.0


def _time_distribution(
    segments: tuple[LabeledSegment, ...]
) -> dict[str, object]:
    by_month = _empty_split_nested_counter()
    by_hour = _empty_split_nested_counter()
    for segment in segments:
        parsed = _parse_timestamp(segment.timestamp)
        if parsed is None:
            continue
        split = _require_split(segment)
        by_month[parsed.strftime("%Y-%m")][split] += 1.0
        by_hour[str(parsed.hour)][split] += 1.0

    return {
        "report_only": True,
        "month": _counter_to_regular_dict(by_month),
        "hour_of_day": _counter_to_regular_dict(by_hour),
    }


def _empty_split_nested_counter():
    return defaultdict(lambda: {"train": 0.0, "eval": 0.0})


def _counter_to_regular_dict(counter) -> dict[str, dict[str, float]]:
    return {key: dict(value) for key, value in sorted(counter.items())}


def _parse_timestamp(value: str | None) -> datetime | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def _require_split(segment: LabeledSegment) -> str:
    if segment.split not in _SPLITS:
        raise ValueError(f"row_index={segment.row_index} missing split")
    return segment.split
