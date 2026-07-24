"""Local and GCS artifact helpers for Gemini SFT runs."""

from __future__ import annotations

import dataclasses
import datetime
import json
import math
import pathlib
import typing

from common import gcs_utils, manifest
from common.gemini import context

from gemini_sft import records

if typing.TYPE_CHECKING:
    import collections.abc

    from google.cloud import storage

    from gemini_sft import config

DEFAULT_RESULTS_DIR = pathlib.Path("results")
EVALS_README_TEXT = "Reserved for Gemini SFT eval artifacts."


@dataclasses.dataclass(frozen=True)
class PreparedRunArtifacts:
    """Local paths and counts produced by preparing a config-driven run.

    Attributes:
        run_config_path: Local copy of the operator TOML.
        canonical_train_path: Local canonical training manifest.
        canonical_validation_path: Local canonical validation manifest.
        canonical_eval_path: Local canonical evaluation manifest.
        gemini_train_path: Local Gemini training JSONL.
        gemini_validation_path: Local Gemini validation JSONL.
        preflight_report_path: Local preparation preflight report.
        total_train_duration_seconds: Total duration of canonical training
            audio.
        canonical_train_rows: Number of validated canonical training rows.
        canonical_validation_rows: Number of validated canonical validation
            rows.
        canonical_eval_rows: Number of validated canonical evaluation rows.
    """

    run_config_path: pathlib.Path
    canonical_train_path: pathlib.Path
    canonical_validation_path: pathlib.Path
    canonical_eval_path: pathlib.Path
    gemini_train_path: pathlib.Path
    gemini_validation_path: pathlib.Path
    preflight_report_path: pathlib.Path
    total_train_duration_seconds: float
    canonical_train_rows: int
    canonical_validation_rows: int
    canonical_eval_rows: int


@dataclasses.dataclass(frozen=True)
class PreparedEvalArtifacts:
    """Local paths and count produced by preparing an eval-only round.

    Attributes:
        run_config_path: Local copy of the operator TOML.
        canonical_eval_path: Local validated canonical eval manifest.
        canonical_eval_rows: Number of validated canonical eval rows.
    """

    run_config_path: pathlib.Path
    canonical_eval_path: pathlib.Path
    canonical_eval_rows: int


@dataclasses.dataclass(frozen=True)
class EvalRowsForInference:
    """Reference-bearing scoring rows plus transcript-free provider inputs.

    Attributes:
        source_rows: Validated raw rows retained only for scoring and output.
        eval_rows: Typed canonical rows retained only for scoring.
        segments: Transcript-free rows that may cross the inference boundary.
    """

    source_rows: list[dict[str, typing.Any]]
    eval_rows: list[manifest.CanonicalRow]
    segments: list[context.EvaluationSegment]


# Temporary stacked-PR bridge for latest-main evaluation. PR #1003 removes
# this history-bearing loader when evaluate.py consumes transcript-free rows.
@dataclasses.dataclass(frozen=True)
class EvalRowsWithHistory:
    """Canonical eval rows plus aligned generic prior-context histories.

    Attributes:
        source_rows: Validated raw eval rows preserved for normalized output.
        eval_rows: Typed canonical rows aligned with ``source_rows``.
        histories: Generic prior turns aligned with each evaluation row.
    """

    source_rows: list[dict[str, typing.Any]]
    eval_rows: list[manifest.CanonicalRow]
    histories: list[list[context.ContextTurn]]


def eval_rows_with_histories_from_entries(
    entries: list[dict[str, typing.Any]],
    *,
    source: str,
    prior_context_count: int,
    limit: int | None = None,
) -> EvalRowsWithHistory:
    """Return legacy scoring rows and aligned generic histories.

    Args:
        entries: Raw canonical eval manifest dictionaries.
        source: Human-readable manifest source used in validation errors.
        prior_context_count: Maximum prior same-source turns per eval row.
        limit: Optional maximum number of aligned eval rows to return.

    Returns:
        Validated rows and legacy histories in matching order.
    """
    source_rows, eval_rows = canonical_rows_from_entries(
        entries,
        split="eval",
        source=source,
    )
    histories = context.build_context_histories(
        source_rows,
        max_turns=prior_context_count,
    )
    if limit is not None:
        source_rows = source_rows[:limit]
        eval_rows = eval_rows[:limit]
        histories = histories[:limit]
    return EvalRowsWithHistory(
        source_rows=source_rows,
        eval_rows=eval_rows,
        histories=histories,
    )


def utc_now() -> str:
    """Return an ISO UTC timestamp."""
    return datetime.datetime.now(datetime.UTC).isoformat()


def local_run_dir(results_dir: pathlib.Path, round_id: str) -> pathlib.Path:
    """Return the local mirror directory for a run."""
    return results_dir / round_id


def local_config_path(
    results_dir: pathlib.Path,
    round_id: str,
) -> pathlib.Path:
    """Return the local mirror config path for a run."""
    return local_run_dir(results_dir, round_id) / "config.json"


def write_json_artifact(
    local_path: pathlib.Path,
    storage_client: storage.Client,
    gcs_uri: str,
    obj: dict[str, typing.Any],
) -> None:
    """Write JSON locally and upload it to GCS."""
    local_path.parent.mkdir(parents=True, exist_ok=True)
    local_path.write_text(
        json.dumps(obj, indent=2, default=str), encoding="utf-8"
    )
    gcs_utils.upload_local_file(storage_client, local_path, gcs_uri)


def write_text_artifact(
    local_path: pathlib.Path,
    storage_client: storage.Client,
    gcs_uri: str,
    text: str,
) -> None:
    """Write text locally and upload it to GCS."""
    local_path.parent.mkdir(parents=True, exist_ok=True)
    local_path.write_text(text, encoding="utf-8")
    gcs_utils.upload_local_file(storage_client, local_path, gcs_uri)


def write_status(
    run_dir: pathlib.Path,
    storage_client: storage.Client,
    status_uri: str,
    status: dict[str, typing.Any],
) -> None:
    """Write the run root status artifact locally and to GCS."""
    write_json_artifact(
        run_dir / "status.json", storage_client, status_uri, status
    )


def write_and_upload_config(
    *,
    results_dir: pathlib.Path,
    run_cfg: config.RunConfig,
    storage_client: storage.Client,
    config: dict[str, typing.Any],
) -> dict[str, typing.Any]:
    """Write config.json with metadata and upload it to the run prefix."""
    written = records.write_config(results_dir, run_cfg.round_id, config)
    gcs_utils.upload_local_file(
        storage_client,
        local_config_path(results_dir, run_cfg.round_id),
        run_cfg.paths.config_uri,
    )
    return written


def load_canonical_rows(
    path: pathlib.Path, split: str
) -> tuple[list[dict[str, typing.Any]], list[manifest.CanonicalRow]]:
    """Load a canonical manifest and return raw entries plus parsed rows."""
    entries = manifest.load_manifest_strict(str(path))
    return canonical_rows_from_entries(entries, split=split, source=str(path))


def canonical_rows_from_entries(
    entries: list[dict[str, typing.Any]],
    *,
    split: str,
    source: str,
) -> tuple[list[dict[str, typing.Any]], list[manifest.CanonicalRow]]:
    """Validate and convert canonical manifest entries for packaged flows."""
    if not entries:
        msg = f"{split} manifest has zero parsed rows: {source}"
        raise ValueError(msg)
    return manifest.strict_canonical_rows_from_manifest(
        entries,
        expected_split=split,
        source=source,
    )


def causal_segments_from_rows(
    source_rows: collections.abc.Sequence[dict[str, typing.Any]],
    canonical_rows: collections.abc.Sequence[manifest.CanonicalRow],
    *,
    split: str,
) -> list[context.EvaluationSegment]:
    """Normalize aligned contextual rows into transcript-free segments.

    Args:
        source_rows: Raw manifest rows carrying source-level provenance.
        canonical_rows: Strict canonical rows aligned with ``source_rows``.
        split: Expected contextual split for rows without an explicit split.

    Returns:
        Transcript-free segments aligned in authoritative manifest order.

    Raises:
        ValueError: If alignment, split, source identity, or timing is invalid.
    """
    source_values = tuple(source_rows)
    canonical_values = tuple(canonical_rows)
    if len(source_values) != len(canonical_values):
        msg = "source and canonical rows must have equal lengths"
        raise ValueError(msg)
    if not isinstance(split, str) or not split.strip():
        msg = "causal segment split must be a non-empty string"
        raise ValueError(msg)
    return [
        _causal_segment(
            source_row,
            canonical_row,
            manifest_index,
            split=split.strip(),
        )
        for manifest_index, (source_row, canonical_row) in enumerate(
            zip(source_values, canonical_values, strict=True)
        )
    ]


def eval_rows_for_inference_from_entries(
    entries: list[dict[str, typing.Any]],
    *,
    source: str,
    limit: int | None = None,
    prior_context_count: int = 0,
) -> EvalRowsForInference:
    """Return scoring rows and a transcript-free evaluation provider view.

    The complete manifest is validated first. The optional row limit is then
    applied before any rolling-history schedule is constructed by the caller.
    Reference transcripts remain present only in ``source_rows`` and
    ``eval_rows``; ``segments`` intentionally cannot carry them.

    Args:
        entries: Raw canonical eval manifest dictionaries.
        source: Human-readable manifest source used in validation errors.
        limit: Optional maximum number of aligned rows to return before
            scheduling.
        prior_context_count: Maximum number of prior predicted turns. Positive
            values require causal source and timing metadata; zero preserves
            the stateless evaluator's canonical-row contract.

    Returns:
        Validated scoring rows and transcript-free inference segments in
        matching order.

    Raises:
        TypeError: If ``limit`` or ``prior_context_count`` is a boolean or
            non-integer.
        ValueError: If the eval manifest is empty or invalid, ``limit`` is not
            positive, the context count is negative, or required causal
            source/timing metadata is malformed.
    """
    source_rows, eval_rows = canonical_rows_from_entries(
        entries,
        split="eval",
        source=source,
    )
    if limit is not None:
        if isinstance(limit, bool) or not isinstance(limit, int):
            msg = "eval limit must be a positive integer"
            raise TypeError(msg)
        if limit <= 0:
            msg = "eval limit must be a positive integer"
            raise ValueError(msg)
        source_rows = source_rows[:limit]
        eval_rows = eval_rows[:limit]
    if isinstance(prior_context_count, bool) or not isinstance(
        prior_context_count, int
    ):
        msg = "prior_context_count must be a non-negative integer"
        raise TypeError(msg)
    if prior_context_count < 0:
        msg = "prior_context_count must be a non-negative integer"
        raise ValueError(msg)
    if prior_context_count > 0:
        segments = causal_segments_from_rows(
            source_rows,
            eval_rows,
            split="eval",
        )
    else:
        segments = [
            _stateless_evaluation_segment(eval_row, manifest_index)
            for manifest_index, eval_row in enumerate(eval_rows)
        ]
    return EvalRowsForInference(
        source_rows=source_rows,
        eval_rows=eval_rows,
        segments=segments,
    )


def _stateless_evaluation_segment(
    eval_row: manifest.CanonicalRow,
    manifest_index: int,
) -> context.EvaluationSegment:
    """Build a provider-safe descriptor without rolling metadata.

    Args:
        eval_row: Validated canonical row for the current segment.
        manifest_index: Authoritative position in the evaluation manifest.

    Returns:
        A transcript-free descriptor for stateless provider inference.
    """
    start_seconds = float(eval_row.offset)
    return context.EvaluationSegment(
        audio_uri=eval_row.audio_filepath,
        split=eval_row.split or "eval",
        source_key=eval_row.example_id,
        start_seconds=start_seconds,
        end_seconds=start_seconds + float(eval_row.duration),
        manifest_index=manifest_index,
    )


def _causal_segment(
    source_row: dict[str, typing.Any],
    canonical_row: manifest.CanonicalRow,
    manifest_index: int,
    *,
    split: str,
) -> context.EvaluationSegment:
    """Build a transcript-free segment with strict causal metadata.

    Args:
        source_row: Raw manifest row containing optional source-level timing.
        canonical_row: Validated row supplying current-audio identity.
        manifest_index: Authoritative position in the contextual manifest.
        split: Expected split when the canonical row omits one.

    Returns:
        A descriptor suitable for strict-causal rolling scheduling.

    Raises:
        TypeError: If preferred source or timing metadata has an invalid type.
        ValueError: If source identity or derived timing is invalid.
    """
    source_key, start_seconds, duration_seconds = _causal_provenance(
        source_row,
        canonical_row,
    )
    end_seconds = start_seconds + duration_seconds
    if not math.isfinite(end_seconds) or end_seconds <= start_seconds:
        msg = (
            "contextual row has invalid derived end time at manifest index "
            f"{manifest_index}"
        )
        raise ValueError(msg)
    return context.EvaluationSegment(
        audio_uri=canonical_row.audio_filepath,
        split=canonical_row.split or split,
        source_key=source_key,
        start_seconds=start_seconds,
        end_seconds=end_seconds,
        manifest_index=manifest_index,
    )


def _causal_provenance(
    source_row: dict[str, typing.Any],
    canonical_row: manifest.CanonicalRow,
) -> tuple[str, float, float]:
    """Return one atomic source, offset, and duration provenance tuple.

    Args:
        source_row: Raw manifest row with optional original-audio identity.
        canonical_row: Validated row with optional source-audio metadata.

    Returns:
        Source URI, start seconds, and duration seconds from one complete
        provenance representation.

    Raises:
        ValueError: If original or canonical source metadata is partial,
            malformed, or absent. ``example_id`` is intentionally not a
            fallback because it identifies an example, not a conversation.
    """
    original_audio_uri = source_row.get("original_audio_uri")
    original_offset = source_row.get("original_offset")
    if original_audio_uri is not None:
        if not isinstance(original_audio_uri, str) or not (
            source_key := original_audio_uri.strip()
        ):
            msg = "contextual row original_audio_uri must be a non-empty string"
            raise ValueError(msg)
        if original_offset is None:
            msg = (
                "contextual row requires complete original provenance: "
                "original_audio_uri and original_offset"
            )
            raise ValueError(msg)
        return (
            source_key,
            _finite_nonnegative_number(original_offset, "original_offset"),
            _finite_positive_number(canonical_row.duration, "duration"),
        )
    if original_offset is not None:
        msg = (
            "contextual row requires complete original provenance: "
            "original_audio_uri and original_offset"
        )
        raise ValueError(msg)

    source_audio = canonical_row.source_audio
    if source_audio is not None:
        source_audio_uri = source_audio.get("audio_filepath")
        source_audio_offset = source_audio.get("offset")
        source_audio_duration = source_audio.get("duration")
        if (
            source_audio_uri is None
            or source_audio_offset is None
            or source_audio_duration is None
        ):
            msg = (
                "contextual row requires complete source_audio "
                "provenance: audio_filepath, offset, and duration"
            )
            raise ValueError(msg)
        if not isinstance(source_audio_uri, str) or not (
            source_key := source_audio_uri.strip()
        ):
            msg = (
                "contextual row source_audio.audio_filepath must be a "
                "non-empty string"
            )
            raise ValueError(msg)
        return (
            source_key,
            _finite_nonnegative_number(
                source_audio_offset,
                "source_audio.offset",
            ),
            _finite_positive_number(
                source_audio_duration,
                "source_audio.duration",
            ),
        )
    msg = (
        "contextual row requires a complete durable source identity and "
        "timing provenance tuple via original or source_audio metadata; "
        "example_id is not a conversation key"
    )
    raise ValueError(msg)


def _finite_nonnegative_number(value: typing.Any, field: str) -> float:
    number = _finite_number(value, field)
    if number < 0:
        msg = f"contextual row {field} must be non-negative"
        raise ValueError(msg)
    return number


def _finite_positive_number(value: typing.Any, field: str) -> float:
    number = _finite_number(value, field)
    if number <= 0:
        msg = f"contextual row {field} must be greater than zero"
        raise ValueError(msg)
    return number


def _finite_number(value: typing.Any, field: str) -> float:
    """Convert one manifest value to a finite floating-point number.

    Args:
        value: Candidate numeric value.
        field: Manifest field name included in validation errors.

    Returns:
        The finite floating-point representation of ``value``.

    Raises:
        TypeError: If ``value`` is a boolean.
        ValueError: If ``value`` cannot be converted or is not finite.
    """
    if isinstance(value, bool):
        msg = f"contextual row {field} must be a finite number"
        raise TypeError(msg)
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        msg = f"contextual row {field} must be a finite number"
        raise ValueError(msg) from exc
    if not math.isfinite(number):
        msg = f"contextual row {field} must be a finite number"
        raise ValueError(msg)
    return number


def reject_split_overlap(
    left_name: str,
    left_rows: list[manifest.CanonicalRow],
    right_name: str,
    right_rows: list[manifest.CanonicalRow],
) -> None:
    """Reject audio URI or logical identity overlap between two splits."""
    left_uris = {row.audio_filepath for row in left_rows}
    right_uris = {row.audio_filepath for row in right_rows}
    uri_overlap = sorted(left_uris & right_uris)
    left_identities = {
        manifest.canonical_row_identity(row) for row in left_rows
    }
    right_identities = {
        manifest.canonical_row_identity(row) for row in right_rows
    }
    identity_overlap = sorted(left_identities & right_identities)
    if not uri_overlap and not identity_overlap:
        return
    parts = [f"{left_name} and {right_name} manifests overlap"]
    if uri_overlap:
        uri_sample = ", ".join(uri_overlap[:5])
        parts.append(f"{len(uri_overlap)} audio URI(s): {uri_sample}")
    if identity_overlap:
        identity_sample = ", ".join(
            _format_identity(identity) for identity in identity_overlap[:5]
        )
        parts.append(
            f"{len(identity_overlap)} identity value(s): {identity_sample}"
        )
    msg = "; ".join(parts)
    raise ValueError(msg)


def _format_identity(identity: tuple[str, str]) -> str:
    return f"{identity[0]}/{identity[1]}"
