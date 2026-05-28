from __future__ import annotations

import re
from collections import defaultdict
from dataclasses import dataclass, replace

from dataset_split.balance import (
    DEFAULT_BALANCE_WEIGHTS,
    DURATION_BUCKETS_SECONDS,
    TRANSCRIPT_WORD_BUCKETS,
    bucket_duration_seconds,
    bucket_transcript_words,
    build_balance_report,
)
from dataset_split.types import LabeledSegment

_MILLISECONDS_PER_SECOND = 1000


def _count_objective_weight(weight_key: str) -> int:
    # Duration components are measured in milliseconds for CP-SAT precision.
    # Count-like components are scaled to keep one count delta comparable to
    # one second of duration under equal human-readable balance weights.
    return int(DEFAULT_BALANCE_WEIGHTS[weight_key] * _MILLISECONDS_PER_SECOND)


_OBJECTIVE_WEIGHTS = {
    "dataset_row_count": _count_objective_weight("dataset_rows"),
    "dataset_duration": int(DEFAULT_BALANCE_WEIGHTS["dataset_duration"]),
    "dataset_source_count": _count_objective_weight("dataset_sources"),
    "family_row_count": _count_objective_weight("family_rows"),
    "family_duration": int(DEFAULT_BALANCE_WEIGHTS["family_duration"]),
    "global_row_count": _count_objective_weight("global_rows"),
    "global_duration": int(DEFAULT_BALANCE_WEIGHTS["global_duration"]),
    "global_source_count": _count_objective_weight("global_sources"),
    "duration_bucket_count": _count_objective_weight("duration_bucket_rows"),
    "transcript_bucket_count": _count_objective_weight(
        "transcript_bucket_rows"
    ),
}


class SplitAssignmentError(ValueError):
    """Raised when Source Groups cannot be assigned to train/eval."""


@dataclass(frozen=True)
class SourceGroupStats:
    source_group: str
    dataset_names: tuple[str, ...]
    dataset_families: tuple[str, ...]
    row_count: int
    duration_ms: int
    duration_buckets: dict[str, int]
    transcript_word_buckets: dict[str, int]


@dataclass(frozen=True)
class SplitAlgorithmMetadata:
    algorithm: str
    solver_status: str
    objective_value: float
    solver_time_limit_seconds: float
    wall_time_seconds: float
    weights: dict[str, int]


@dataclass(frozen=True)
class SplitResult:
    segments: tuple[LabeledSegment, ...]
    source_group_splits: dict[str, str]
    metadata: SplitAlgorithmMetadata
    balance_report: dict[str, object] | None = None


@dataclass(frozen=True)
class _ObjectiveComponent:
    name: str
    weight: int
    total: int
    values_by_source_group: dict[str, int]


def assign_train_eval_split(
    segments: tuple[LabeledSegment, ...],
    *,
    train_ratio: float = 0.8,
    eval_ratio: float = 0.2,
    solver_time_limit_seconds: float = 10.0,
) -> SplitResult:
    try:
        from ortools.sat.python import cp_model
    except ImportError as exc:
        raise SplitAssignmentError(
            "ortools is required for split assignment; install model[optimizer]"
        ) from exc

    if not segments:
        raise SplitAssignmentError("segments must not be empty")
    if abs((train_ratio + eval_ratio) - 1.0) > 1e-9:
        raise SplitAssignmentError("train_ratio + eval_ratio must equal 1.0")
    if train_ratio <= 0.0 or eval_ratio <= 0.0:
        raise SplitAssignmentError(
            "train_ratio and eval_ratio must be positive"
        )

    segments_by_source_group = _segments_by_source_group(segments)
    dataset_source_groups = _dataset_source_groups(segments)
    for dataset_name, source_groups in dataset_source_groups.items():
        if len(source_groups) < 2:
            raise SplitAssignmentError(
                f"dataset={dataset_name} has fewer than 2 source groups"
            )

    model = cp_model.CpModel()
    is_eval = {
        source_group: model.NewBoolVar(f"is_eval_{_safe_cp_name(source_group)}")
        for source_group in sorted(segments_by_source_group)
    }

    for dataset_name, source_groups in dataset_source_groups.items():
        eval_count = sum(
            is_eval[source_group] for source_group in source_groups
        )
        model.Add(eval_count >= 1)
        model.Add(eval_count <= len(source_groups) - 1)

    objective_terms = []
    components = _objective_components(segments, eval_ratio)
    for component in components:
        eval_value = sum(
            is_eval[source_group] * value
            for source_group, value in component.values_by_source_group.items()
        )
        target_eval_value = round(component.total * eval_ratio)
        delta = model.NewIntVar(
            0,
            max(component.total, target_eval_value, 1),
            f"delta_{_safe_cp_name(component.name)}",
        )
        model.Add(delta >= eval_value - target_eval_value)
        model.Add(delta >= target_eval_value - eval_value)
        objective_terms.append(component.weight * delta)

    model.Minimize(sum(objective_terms))

    solver = cp_model.CpSolver()
    solver.parameters.max_time_in_seconds = solver_time_limit_seconds
    status = solver.Solve(model)
    if status not in (cp_model.OPTIMAL, cp_model.FEASIBLE):
        raise SplitAssignmentError("no feasible train/eval split found")

    source_group_splits = {
        source_group: "eval" if solver.Value(variable) else "train"
        for source_group, variable in is_eval.items()
    }
    assigned_segments = tuple(
        replace(segment, split=source_group_splits[segment.source_group])
        for segment in segments
    )
    return SplitResult(
        segments=assigned_segments,
        source_group_splits=source_group_splits,
        metadata=SplitAlgorithmMetadata(
            algorithm="ortools_cp_sat_source_group_v1",
            solver_status=solver.StatusName(status),
            objective_value=float(solver.ObjectiveValue()),
            solver_time_limit_seconds=solver_time_limit_seconds,
            wall_time_seconds=float(solver.WallTime()),
            weights=dict(_OBJECTIVE_WEIGHTS),
        ),
        balance_report=build_balance_report(
            assigned_segments,
            train_ratio=train_ratio,
            eval_ratio=eval_ratio,
        ).to_dict(),
    )


def _segments_by_source_group(
    segments: tuple[LabeledSegment, ...],
) -> dict[str, list[LabeledSegment]]:
    grouped: dict[str, list[LabeledSegment]] = defaultdict(list)
    for segment in segments:
        grouped[segment.source_group].append(segment)
    return dict(grouped)


def _dataset_source_groups(
    segments: tuple[LabeledSegment, ...],
) -> dict[str, tuple[str, ...]]:
    grouped: dict[str, set[str]] = defaultdict(set)
    for segment in segments:
        grouped[segment.dataset_name].add(segment.source_group)
    return {
        dataset_name: tuple(sorted(source_groups))
        for dataset_name, source_groups in grouped.items()
    }


def _objective_components(
    segments: tuple[LabeledSegment, ...], eval_ratio: float
) -> tuple[_ObjectiveComponent, ...]:
    del eval_ratio
    components: list[_ObjectiveComponent] = []
    source_group_stats = _source_group_stats(segments)

    for dataset_name in sorted({segment.dataset_name for segment in segments}):
        components.append(
            _component_for_scope(
                f"dataset_row_count:{dataset_name}",
                "dataset_row_count",
                segments,
                lambda segment, expected=dataset_name: (
                    segment.dataset_name == expected
                ),
                lambda segment: 1,
            )
        )
        components.append(
            _component_for_scope(
                f"dataset_duration:{dataset_name}",
                "dataset_duration",
                segments,
                lambda segment, expected=dataset_name: (
                    segment.dataset_name == expected
                ),
                _duration_ms,
            )
        )
        components.append(
            _component_for_source_scope(
                f"dataset_source_count:{dataset_name}",
                "dataset_source_count",
                segments,
                lambda segment, expected=dataset_name: (
                    segment.dataset_name == expected
                ),
            )
        )

    for family in sorted({segment.dataset_family for segment in segments}):
        components.append(
            _component_for_scope(
                f"family_row_count:{family}",
                "family_row_count",
                segments,
                lambda segment, expected=family: (
                    segment.dataset_family == expected
                ),
                lambda segment: 1,
            )
        )
        components.append(
            _component_for_scope(
                f"family_duration:{family}",
                "family_duration",
                segments,
                lambda segment, expected=family: (
                    segment.dataset_family == expected
                ),
                _duration_ms,
            )
        )

    components.extend(
        (
            _ObjectiveComponent(
                name="global_row_count",
                weight=_OBJECTIVE_WEIGHTS["global_row_count"],
                total=sum(stats.row_count for stats in source_group_stats),
                values_by_source_group={
                    stats.source_group: stats.row_count
                    for stats in source_group_stats
                },
            ),
            _ObjectiveComponent(
                name="global_duration",
                weight=_OBJECTIVE_WEIGHTS["global_duration"],
                total=sum(stats.duration_ms for stats in source_group_stats),
                values_by_source_group={
                    stats.source_group: stats.duration_ms
                    for stats in source_group_stats
                },
            ),
            _ObjectiveComponent(
                name="global_source_count",
                weight=_OBJECTIVE_WEIGHTS["global_source_count"],
                total=len(source_group_stats),
                values_by_source_group={
                    stats.source_group: 1 for stats in source_group_stats
                },
            ),
        )
    )

    for _, _, bucket_name in DURATION_BUCKETS_SECONDS:
        components.append(
            _component_for_scope(
                f"duration_bucket:{bucket_name}",
                "duration_bucket_count",
                segments,
                lambda segment, expected=bucket_name: (
                    bucket_duration_seconds(segment.duration) == expected
                ),
                lambda segment: 1,
            )
        )

    for _, _, bucket_name in TRANSCRIPT_WORD_BUCKETS:
        components.append(
            _component_for_scope(
                f"transcript_words:{bucket_name}",
                "transcript_bucket_count",
                segments,
                lambda segment, expected=bucket_name: (
                    bucket_transcript_words(segment.text) == expected
                ),
                lambda segment: 1,
            )
        )

    return tuple(components)


def _source_group_stats(
    segments: tuple[LabeledSegment, ...],
) -> tuple[SourceGroupStats, ...]:
    stats: list[SourceGroupStats] = []
    for source_group, group_segments in sorted(
        _segments_by_source_group(segments).items()
    ):
        duration_buckets = {name: 0 for _, _, name in DURATION_BUCKETS_SECONDS}
        transcript_word_buckets = {
            name: 0 for _, _, name in TRANSCRIPT_WORD_BUCKETS
        }
        for segment in group_segments:
            duration_buckets[bucket_duration_seconds(segment.duration)] += 1
            transcript_word_buckets[bucket_transcript_words(segment.text)] += 1

        stats.append(
            SourceGroupStats(
                source_group=source_group,
                dataset_names=tuple(
                    sorted({segment.dataset_name for segment in group_segments})
                ),
                dataset_families=tuple(
                    sorted(
                        {segment.dataset_family for segment in group_segments}
                    )
                ),
                row_count=len(group_segments),
                duration_ms=sum(
                    _duration_ms(segment) for segment in group_segments
                ),
                duration_buckets=duration_buckets,
                transcript_word_buckets=transcript_word_buckets,
            )
        )
    return tuple(stats)


def _component_for_scope(
    name: str,
    weight_key: str,
    segments: tuple[LabeledSegment, ...],
    predicate,
    value,
) -> _ObjectiveComponent:
    values_by_source_group: dict[str, int] = defaultdict(int)
    total = 0
    for segment in segments:
        if not predicate(segment):
            continue
        item_value = value(segment)
        values_by_source_group[segment.source_group] += item_value
        total += item_value
    return _ObjectiveComponent(
        name=name,
        weight=_OBJECTIVE_WEIGHTS[weight_key],
        total=total,
        values_by_source_group=dict(values_by_source_group),
    )


def _component_for_source_scope(
    name: str,
    weight_key: str,
    segments: tuple[LabeledSegment, ...],
    predicate,
) -> _ObjectiveComponent:
    values_by_source_group = {
        source_group: 1
        for source_group, group_segments in _segments_by_source_group(
            segments
        ).items()
        if any(predicate(segment) for segment in group_segments)
    }
    return _ObjectiveComponent(
        name=name,
        weight=_OBJECTIVE_WEIGHTS[weight_key],
        total=sum(values_by_source_group.values()),
        values_by_source_group=values_by_source_group,
    )


def _duration_ms(segment: LabeledSegment) -> int:
    return round(segment.duration * 1000)


def _safe_cp_name(value: str) -> str:
    safe = re.sub(r"[^A-Za-z0-9_]+", "_", value).strip("_")
    return safe[:100] or "value"
