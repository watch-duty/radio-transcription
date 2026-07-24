"""Evaluation views for the production-shaped reconstruction.

This module is deliberately provider-free.  It freezes the three evaluation
lanes, prepares a reference-free rolling-context schedule, and removes masked
rows whose resolved physical recording group is training-owned.
"""

# One-time fail-loud constructor: direct diagnostic exceptions are intentional.
# ruff: noqa: EM101, EM102, TRY003, TRY004, TRY301

from __future__ import annotations

import collections
import collections.abc
import copy
import dataclasses
import decimal
import fractions
import hashlib
import json
import re
from typing import Protocol, runtime_checkable

from common import manifest
from common import recording_groups as groups

FINAL_EVAL_LANES = (
    "reconstructed_primary",
    "predicted_prior_context_10",
    "masked_v2",
)
MASKED_DATASET_FAMILIES = (
    "bcfy_calls",
    "bcfy_feeds",
    "echo",
    "fire_notifications",
)
MAX_PRIOR_CONTEXT = 10
PROVIDER_VALIDATION_CAP = 5_000
PROVIDER_VALIDATION_SMALL_EVAL_THRESHOLD = 1_000
PROVIDER_VALIDATION_TRAIN_FRACTION = fractions.Fraction(3, 10)
_REFERENCE_KEYS = frozenset(
    {
        "answer",
        "ground_truth",
        "label",
        "reference",
        "reference_text",
        "target",
        "target_text",
        "text",
        "transcript",
    }
)
_EMPTY_PREDICTION_MARKERS = frozenset(
    {
        "[inaudible]",
        "[unintelligible]",
        "<inaudible>",
        "<unintelligible>",
        "inaudible",
        "unintelligible",
    }
)
_SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")


@dataclasses.dataclass(frozen=True, slots=True)
class PrimaryEvalView:
    """Complete canonical reconstructed eval retained for scoring."""

    rows: tuple[dict[str, object], ...]
    eval_jsonl: bytes
    receipt: dict[str, object]


@dataclasses.dataclass(frozen=True, slots=True)
class ProviderValidationView:
    """Deterministic, outcome-blind tuning-validation projection."""

    rows: tuple[dict[str, object], ...]
    jsonl: bytes
    receipt: dict[str, object]


@dataclasses.dataclass(frozen=True, slots=True)
class ScheduledRequest:
    """Reference-free identity and deterministic position for one eval row."""

    manifest_index: int
    request_id: str
    context_episode_id: str
    source_uri: str
    source_ordinal: int
    audio_uri: str
    example_id: str | None
    segment_id: str | None

    def model_request(self) -> dict[str, object]:
        """Return only fields needed to identify and transcribe current audio."""
        result: dict[str, object] = {
            "audio_filepath": self.audio_uri,
            "request_id": self.request_id,
        }
        if self.example_id is not None:
            result["example_id"] = self.example_id
        if self.segment_id is not None:
            result["segment_id"] = self.segment_id
        return result


@dataclasses.dataclass(frozen=True, slots=True)
class PriorContextSchedule:
    """Deterministic production-episode order, independent of a checkpoint."""

    requests: tuple[ScheduledRequest, ...]
    receipt: dict[str, object]


@dataclasses.dataclass(frozen=True, slots=True)
class PredictionContextTurn:
    """One prior successful prediction retained as transcript-only context."""

    request_id: str
    audio_uri: str
    prediction: str
    prediction_sha256: str

    def model_turn(self) -> dict[str, str]:
        """Return the exact transcript-only turn supplied to the model."""
        return {"prediction": self.prediction}


@dataclasses.dataclass(frozen=True, slots=True)
class PredictionOutcome:
    """Checkpoint-bound result of the exact model input opened by the builder."""

    checkpoint_id: str
    request_id: str
    model_input_sha256: str
    prediction: str | None
    accepted: bool
    provider_request_fingerprint: str | None = None

    def __post_init__(self) -> None:
        _required_string(self.checkpoint_id, "outcome checkpoint_id")
        _required_string(self.request_id, "outcome request_id")
        if _SHA256_RE.fullmatch(self.model_input_sha256) is None:
            raise ValueError("outcome model_input_sha256 is invalid")
        if not isinstance(self.accepted, bool):
            raise TypeError("outcome accepted must be boolean")
        if self.prediction is not None and not isinstance(self.prediction, str):
            raise TypeError("outcome prediction must be a string or None")
        if self.provider_request_fingerprint is not None:
            _required_string(
                self.provider_request_fingerprint,
                "provider_request_fingerprint",
            )


@dataclasses.dataclass(frozen=True, slots=True)
class PriorContextEnvelope:
    """One current request plus its same-checkpoint predicted history."""

    checkpoint_id: str
    request: ScheduledRequest
    prior_predictions: tuple[PredictionContextTurn, ...]
    receipt: dict[str, object]

    def model_input(self) -> dict[str, object]:
        """Return an auditable request with no reference-bearing fields."""
        result: dict[str, object] = {
            "checkpoint_id": self.checkpoint_id,
            "current": self.request.model_request(),
            "prior_predictions": [
                turn.model_turn() for turn in self.prior_predictions
            ],
        }
        if _contains_reference_key(result):
            raise AssertionError("reference field entered prior-context input")
        return result


@dataclasses.dataclass(frozen=True, slots=True)
class MaskedFamilyResult:
    """Unchanged kept rows and exclusion evidence for one masked source."""

    family: str
    kept_rows: tuple[dict[str, object], ...]
    excluded_rows: tuple[dict[str, object], ...]
    receipt: dict[str, object]


@dataclasses.dataclass(frozen=True, slots=True)
class MaskedEvalFilterResult:
    """Leak-refiltered four-source masked eval and complete receipts."""

    families: tuple[MaskedFamilyResult, ...]
    receipt: dict[str, object]

    def kept_rows_by_family(self) -> dict[str, tuple[dict[str, object], ...]]:
        """Return retained rows keyed by canonical dataset family."""
        return {item.family: item.kept_rows for item in self.families}


@runtime_checkable
class BindingGroupResolver(Protocol):
    """Small injectable recording-group index used by unit tests."""

    @property
    def sha256(self) -> str:
        """Return the immutable resolver-input digest."""

    @property
    def policy_receipt(self) -> collections.abc.Mapping[str, object]:
        """Return the frozen policy identity used to create group bindings."""

    def group_ids_for_binding(
        self,
        binding: groups.RowBinding,
    ) -> collections.abc.Iterable[str]:
        """Resolve one complete row binding to physical group identifiers."""


@dataclasses.dataclass(slots=True)
class _EpisodeState:
    next_ordinal: int = 0
    in_flight_request_id: str | None = None
    history: list[PredictionContextTurn] = dataclasses.field(
        default_factory=list
    )


class PredictedPriorContextBuilder:
    """Build up to ten prior turns from this checkpoint's predictions only.

    Episodes can be evaluated concurrently, but requests within an episode
    must be opened and committed in frozen schedule order.  A terminal/blank
    outcome advances the episode without adding context.
    """

    def __init__(
        self,
        schedule: PriorContextSchedule,
        *,
        checkpoint_id: str,
        max_context: int = MAX_PRIOR_CONTEXT,
    ) -> None:
        if not isinstance(checkpoint_id, str) or not checkpoint_id.strip():
            raise ValueError("checkpoint_id must be nonblank")
        if (
            isinstance(max_context, bool)
            or not isinstance(max_context, int)
            or max_context != MAX_PRIOR_CONTEXT
        ):
            raise ValueError(
                "predicted-prior-context lane must use max_context=10"
            )
        self._schedule = schedule
        self._checkpoint_id = checkpoint_id.strip()
        self._max_context = max_context
        self._by_id = {
            request.request_id: request for request in schedule.requests
        }
        if len(self._by_id) != len(schedule.requests):
            raise ValueError("schedule contains duplicate request IDs")
        self._states = {
            episode_id: _EpisodeState()
            for episode_id in sorted(
                {request.context_episode_id for request in schedule.requests}
            )
        }
        self._opened: dict[str, PriorContextEnvelope] = {}
        self._request_receipts: list[dict[str, object]] = []
        self._commits: list[dict[str, object]] = []

    def open_request(self, request_id: str) -> PriorContextEnvelope:
        """Open the next request for its episode without reading a reference."""
        try:
            request = self._by_id[request_id]
        except KeyError:
            raise ValueError(f"unknown request_id: {request_id!r}") from None
        state = self._states[request.context_episode_id]
        if state.in_flight_request_id is not None:
            if state.in_flight_request_id != request_id:
                raise ValueError(
                    "another request is already in flight for this episode"
                )
            return self._opened[request_id]
        if request.source_ordinal != state.next_ordinal:
            raise ValueError(
                "out-of-order request for context episode "
                f"{request.context_episode_id!r}: "
                f"expected ordinal {state.next_ordinal}, "
                f"got {request.source_ordinal}"
            )
        retained = tuple(state.history[-self._max_context :])
        context_basis = [
            {
                "audio_uri": turn.audio_uri,
                "context_episode_id": request.context_episode_id,
                "prediction_sha256": turn.prediction_sha256,
                "request_id": turn.request_id,
                "source": "same_checkpoint_prior_prediction",
            }
            for turn in retained
        ]
        receipt: dict[str, object] = {
            "checkpoint_id": self._checkpoint_id,
            "context_count": len(retained),
            "context_episode_id": request.context_episode_id,
            "context_origins": context_basis,
            "context_source": "same_checkpoint_prior_predictions_only",
            "max_context": self._max_context,
            "reference_field_names_in_model_input": [],
            "reference_transcript_accessed_for_context": False,
            "request_id": request.request_id,
            "schema_version": 1,
            "source_ordinal": request.source_ordinal,
            "source_uri": request.source_uri,
        }
        model_input = {
            "checkpoint_id": self._checkpoint_id,
            "current": request.model_request(),
            "prior_predictions": [turn.model_turn() for turn in retained],
        }
        if _contains_reference_key(model_input):
            raise AssertionError("reference-bearing prior-context model input")
        receipt["model_input_sha256"] = _sha256(_canonical_json(model_input))
        envelope = PriorContextEnvelope(
            checkpoint_id=self._checkpoint_id,
            request=request,
            prior_predictions=retained,
            receipt=receipt,
        )
        if envelope.model_input() != model_input:
            raise AssertionError(
                "prior-context model input changed after receipt"
            )
        state.in_flight_request_id = request_id
        self._opened[request_id] = envelope
        self._request_receipts.append(copy.deepcopy(receipt))
        return envelope

    def record_prediction(self, outcome: PredictionOutcome) -> None:
        """Commit one outcome and make its usable prediction future context."""
        if not isinstance(outcome, PredictionOutcome):
            raise TypeError("outcome must be a PredictionOutcome")
        request_id = outcome.request_id
        if outcome.checkpoint_id != self._checkpoint_id:
            raise ValueError("prediction outcome belongs to another checkpoint")
        try:
            request = self._by_id[request_id]
        except KeyError:
            raise ValueError(f"unknown request_id: {request_id!r}") from None
        state = self._states[request.context_episode_id]
        if state.in_flight_request_id != request_id:
            raise ValueError(
                "request must be opened before recording its result"
            )
        envelope = self._opened[request_id]
        expected_input_sha256 = envelope.receipt["model_input_sha256"]
        if outcome.model_input_sha256 != expected_input_sha256:
            raise ValueError(
                "prediction outcome belongs to another model input"
            )
        normalized = (
            _normalize_prediction(outcome.prediction)
            if outcome.accepted
            else None
        )
        retained = normalized is not None
        if normalized is not None:
            state.history.append(
                PredictionContextTurn(
                    request_id=request.request_id,
                    audio_uri=request.audio_uri,
                    prediction=normalized,
                    prediction_sha256=_sha256(normalized.encode()),
                )
            )
        self._commits.append(
            {
                "checkpoint_id": self._checkpoint_id,
                "context_episode_id": request.context_episode_id,
                "model_input_sha256": outcome.model_input_sha256,
                "prediction_retained_for_context": retained,
                "prediction_sha256": (
                    _sha256(normalized.encode())
                    if normalized is not None
                    else None
                ),
                "request_id": request_id,
                "source": "checkpoint_prediction",
                "source_ordinal": request.source_ordinal,
                "source_uri": request.source_uri,
                "provider_request_fingerprint": (
                    outcome.provider_request_fingerprint
                ),
            }
        )
        state.next_ordinal += 1
        state.in_flight_request_id = None
        del self._opened[request_id]

    def receipt(self, *, require_complete: bool = False) -> dict[str, object]:
        """Return proof that all context originated in prediction commits."""
        incomplete = [
            request.request_id
            for request in self._schedule.requests
            if self._states[request.context_episode_id].next_ordinal
            <= request.source_ordinal
        ]
        if require_complete and incomplete:
            raise ValueError(
                f"prior-context checkpoint is incomplete: {len(incomplete)} rows"
            )
        return {
            "checkpoint_id": self._checkpoint_id,
            "commits": copy.deepcopy(self._commits),
            "completed_request_count": len(self._commits),
            "context_source": "same_checkpoint_prior_predictions_only",
            "incomplete_request_ids": incomplete,
            "max_context": self._max_context,
            "reference_field_names_in_any_context": [],
            "reference_transcript_accessed_for_context": False,
            "request_count": len(self._schedule.requests),
            "request_receipts": copy.deepcopy(self._request_receipts),
            "schema_version": 1,
        }


class UnresolvedMaskedLineageError(ValueError):
    """A masked or training row could not resolve through the frozen index."""


def prepare_primary_eval(
    rows: collections.abc.Sequence[collections.abc.Mapping[str, object]],
) -> PrimaryEvalView:
    """Freeze the complete reconstructed primary scoring eval."""
    if not rows:
        raise ValueError("reconstructed primary eval cannot be empty")
    copied = tuple(copy.deepcopy(dict(row)) for row in rows)
    if any(row.get("split") not in (None, "eval") for row in copied):
        raise ValueError(
            "reconstructed primary rows may only declare split=eval"
        )
    payload = _canonical_jsonl(copied)
    return PrimaryEvalView(
        rows=copied,
        eval_jsonl=payload,
        receipt={
            "eval_lane": FINAL_EVAL_LANES[0],
            "eval_sha256": _sha256(payload),
            "row_count": len(copied),
        },
    )


def _provider_validation_target(*, eval_count: int, train_count: int) -> int:
    if (
        isinstance(train_count, bool)
        or not isinstance(train_count, int)
        or train_count <= 0
    ):
        raise ValueError("train_count must be a positive integer")
    train_limited = (
        train_count
        * PROVIDER_VALIDATION_TRAIN_FRACTION.numerator
        // PROVIDER_VALIDATION_TRAIN_FRACTION.denominator
    )
    return min(
        eval_count,
        PROVIDER_VALIDATION_CAP,
        max(PROVIDER_VALIDATION_SMALL_EVAL_THRESHOLD, train_limited),
    )


def _validation_duration(
    row: collections.abc.Mapping[str, object],
) -> fractions.Fraction:
    source_audio = row.get("source_audio")
    if not isinstance(source_audio, collections.abc.Mapping):
        raise ValueError("provider validation row lacks source_audio metadata")
    start_frame = source_audio.get("start_frame")
    end_frame = source_audio.get("end_frame")
    sample_rate = source_audio.get("sample_rate")
    if (
        isinstance(start_frame, bool)
        or not isinstance(start_frame, int)
        or start_frame < 0
        or isinstance(end_frame, bool)
        or not isinstance(end_frame, int)
        or end_frame <= start_frame
        or isinstance(sample_rate, bool)
        or not isinstance(sample_rate, int)
        or sample_rate <= 0
    ):
        raise ValueError(
            "provider validation row lacks valid exact-frame duration"
        )
    return fractions.Fraction(end_frame - start_frame, sample_rate)


def _validation_duration_bucket(
    row: collections.abc.Mapping[str, object],
) -> str:
    duration = _validation_duration(row)
    if duration < 2:
        return "<2"
    if duration < 5:
        return "2-<5"
    if duration < 15:
        return "5-<15"
    return ">=15"


def _validation_word_bucket(
    row: collections.abc.Mapping[str, object],
) -> str:
    text = row.get("text")
    if not isinstance(text, str) or not text.strip():
        raise ValueError(
            "provider validation word bucketing requires a nonblank target"
        )
    count = len(text.split())
    if count <= 3:
        return "1-3"
    if count <= 9:
        return "4-9"
    if count <= 16:
        return "10-16"
    if count <= 24:
        return "17-24"
    if count <= 37:
        return "25-37"
    return ">=38"


def _validation_stratum_key(
    row: collections.abc.Mapping[str, object],
) -> tuple[str, str, str]:
    dataset = row.get("dataset")
    if not isinstance(dataset, collections.abc.Mapping):
        raise ValueError(
            "provider validation requires canonical dataset metadata"
        )
    dataset_name = _required_string(dataset.get("name"), "dataset.name")
    return (
        dataset_name,
        _validation_duration_bucket(row),
        _validation_word_bucket(row),
    )


def _validation_stable_row_id(
    row: collections.abc.Mapping[str, object],
) -> str:
    example_id, segment_id = manifest.canonical_row_identity(dict(row))
    source_audio = row.get("source_audio")
    if not isinstance(source_audio, collections.abc.Mapping):
        raise ValueError("provider validation row lacks source_audio metadata")
    identity = [
        example_id,
        segment_id,
        _source_uri(row),
        source_audio.get("start_frame"),
        source_audio.get("end_frame"),
        source_audio.get("sample_rate"),
    ]
    payload = json.dumps(
        identity,
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode()
    return hashlib.sha256(payload).hexdigest()


def _validation_row_rank(
    row: collections.abc.Mapping[str, object],
    *,
    input_lock_digest: str,
) -> str:
    payload = (
        f"{input_lock_digest}\0{_source_uri(row)}\0"
        f"{_validation_stable_row_id(row)}"
    ).encode()
    return hashlib.sha256(payload).hexdigest()


def _largest_remainder_quotas(
    stratum_sizes: collections.abc.Mapping[tuple[str, str, str], int],
    *,
    target_count: int,
) -> dict[tuple[str, str, str], int]:
    population = sum(stratum_sizes.values())
    if target_count < 0 or target_count > population:
        raise ValueError("invalid provider-validation target count")
    if population == 0:
        return dict.fromkeys(sorted(stratum_sizes), 0)
    quotas: dict[tuple[str, str, str], int] = {}
    remainders: list[tuple[int, tuple[str, str, str]]] = []
    for key in sorted(stratum_sizes):
        floor, remainder = divmod(
            stratum_sizes[key] * target_count,
            population,
        )
        quotas[key] = floor
        remainders.append((remainder, key))
    unallocated = target_count - sum(quotas.values())
    for _remainder, key in sorted(
        remainders,
        key=lambda item: (-item[0], item[1]),
    ):
        if unallocated == 0:
            break
        if quotas[key] < stratum_sizes[key]:
            quotas[key] += 1
            unallocated -= 1
    if unallocated:
        raise AssertionError(
            f"largest-remainder allocation left {unallocated} rows"
        )
    return quotas


def _select_validation_stratum(
    rows: collections.abc.Sequence[dict[str, object]],
    *,
    input_lock_digest: str,
    quota: int,
) -> list[dict[str, object]]:
    by_source: dict[str, list[dict[str, object]]] = {}
    for row in rows:
        by_source.setdefault(_source_uri(row), []).append(row)
    for source_rows in by_source.values():
        source_rows.sort(
            key=lambda row: (
                _validation_row_rank(
                    row,
                    input_lock_digest=input_lock_digest,
                ),
                _validation_stable_row_id(row),
            )
        )
    ranked_sources = sorted(
        by_source,
        key=lambda source: (
            min(
                _validation_row_rank(
                    row,
                    input_lock_digest=input_lock_digest,
                )
                for row in by_source[source]
            ),
            source,
        ),
    )
    selected: list[dict[str, object]] = []
    round_index = 0
    while len(selected) < quota:
        rows_added = 0
        for source in ranked_sources:
            source_rows = by_source[source]
            if round_index >= len(source_rows):
                continue
            selected.append(source_rows[round_index])
            rows_added += 1
            if len(selected) == quota:
                return selected
        if rows_added == 0:
            raise AssertionError(
                "source round-robin exhausted before filling quota"
            )
        round_index += 1
    return selected


def prepare_provider_validation(
    primary_rows: collections.abc.Sequence[
        collections.abc.Mapping[str, object]
    ],
    *,
    train_count: int,
    input_lock_digest: str,
) -> ProviderValidationView:
    """Apply the proven July 12 stratified-selection structure.

    This reconstruction intentionally strengthens two identities: the salt is
    the exact full-primary payload hash, and source timing uses exact frames
    rather than rounded binary-float seconds.
    """
    if not primary_rows:
        raise ValueError("provider validation requires nonempty primary eval")
    if (
        not isinstance(input_lock_digest, str)
        or _SHA256_RE.fullmatch(input_lock_digest) is None
    ):
        raise ValueError("input_lock_digest must be a lowercase SHA-256")
    copied = tuple(copy.deepcopy(dict(row)) for row in primary_rows)
    full_payload = _canonical_jsonl(copied)
    if _sha256(full_payload) != input_lock_digest:
        raise ValueError(
            "provider validation salt must equal full primary-eval SHA-256"
        )
    target_count = _provider_validation_target(
        eval_count=len(copied),
        train_count=train_count,
    )
    strata: dict[tuple[str, str, str], list[dict[str, object]]] = {}
    for row in copied:
        strata.setdefault(_validation_stratum_key(row), []).append(row)
    quotas = _largest_remainder_quotas(
        {key: len(rows) for key, rows in strata.items()},
        target_count=target_count,
    )
    selected: list[dict[str, object]] = []
    for key in sorted(strata):
        selected.extend(
            _select_validation_stratum(
                strata[key],
                input_lock_digest=input_lock_digest,
                quota=quotas[key],
            )
        )
    if len(selected) != target_count:
        raise AssertionError("provider-validation selection count drifted")
    rows = tuple(copy.deepcopy(row) for row in selected)
    payload = _canonical_jsonl(rows)
    selected_request_ids = tuple(_request_id(row) for row in rows)
    selected_stable_ids = tuple(_validation_stable_row_id(row) for row in rows)
    strata_receipt: list[dict[str, object]] = []
    for key in sorted(strata):
        population_rows = strata[key]
        selected_rows = [
            row for row in rows if _validation_stratum_key(row) == key
        ]
        population_sources = collections.Counter(
            _source_uri(row) for row in population_rows
        )
        selected_sources = collections.Counter(
            _source_uri(row) for row in selected_rows
        )
        strata_receipt.append(
            {
                "chosen_stable_ids": [
                    _validation_stable_row_id(row) for row in selected_rows
                ],
                "dataset": key[0],
                "duration_bucket": key[1],
                "population_size": len(population_rows),
                "quota": quotas[key],
                "sources": [
                    {
                        "chosen_count": selected_sources[source],
                        "population_size": population_sources[source],
                        "source_uri": source,
                    }
                    for source in sorted(population_sources)
                ],
                "word_bucket": key[2],
            }
        )
    return ProviderValidationView(
        rows=rows,
        jsonl=payload,
        receipt={
            "algorithm": (
                "production_shape_dataset_duration_word_"
                "largest_remainder_source_round_robin_v1"
            ),
            "cap": PROVIDER_VALIDATION_CAP,
            "chosen_stable_ids": list(selected_stable_ids),
            "duration_buckets": ["<2", "2-<5", "5-<15", ">=15"],
            "full_primary_count": len(copied),
            "full_primary_sha256": _sha256(full_payload),
            "input_lock_digest": input_lock_digest,
            "precedent": (
                "20260712 dataset-duration-word Hamilton plus "
                "hash-ranked source round-robin"
            ),
            "precedent_adaptations": [
                "salt_is_full_primary_eval_sha256",
                "timing_identity_uses_exact_source_frames",
            ],
            "max_train_fraction": "3/10",
            "model_outcome_fields_used": [],
            "schema_version": 1,
            "selected_count": len(rows),
            "selected_request_ids": list(selected_request_ids),
            "selected_request_ids_sha256": _sha256(
                _canonical_json(list(selected_request_ids))
            ),
            "selected_rows_sha256": _sha256(payload),
            "selection_order": (
                "lexical_stratum_then_hash_ranked_source_round_robin"
            ),
            "selector_fields": [
                "dataset.name",
                "exact_duration_frames/sample_rate",
                "whitespace_target_word_count",
                "source_audio.audio_filepath",
                "stable_row_id",
            ],
            "strata": strata_receipt,
            "target_count": target_count,
            "train_count": train_count,
            "word_buckets": [
                "1-3",
                "4-9",
                "10-16",
                "17-24",
                "25-37",
                ">=38",
            ],
        },
    )


def build_prior_context_schedule(
    rows: collections.abc.Sequence[collections.abc.Mapping[str, object]],
) -> PriorContextSchedule:
    """Group reconstructed eval by frozen production context episode."""
    if not rows:
        raise ValueError("prior-context eval cannot be empty")
    grouped: dict[str, list[tuple[tuple[object, ...], ScheduledRequest]]] = (
        collections.defaultdict(list)
    )
    request_ids: set[str] = set()
    for manifest_index, row in enumerate(rows):
        request_id = _request_id(row)
        if request_id in request_ids:
            raise ValueError(f"duplicate request_id: {request_id!r}")
        request_ids.add(request_id)
        source_uri = _source_uri(row)
        context_episode_id = _context_episode_id(
            row,
            request_id=request_id,
            source_uri=source_uri,
        )
        audio_uri = _required_string(
            row.get("audio_filepath"), "audio_filepath"
        )
        example_id = _optional_string(row.get("example_id"))
        segment_id = _optional_string(row.get("segment_id"))
        grouped[context_episode_id].append(
            (
                _source_order_key(row, request_id),
                ScheduledRequest(
                    manifest_index=manifest_index,
                    request_id=request_id,
                    context_episode_id=context_episode_id,
                    source_uri=source_uri,
                    source_ordinal=-1,
                    audio_uri=audio_uri,
                    example_id=example_id,
                    segment_id=segment_id,
                ),
            )
        )
    requests: list[ScheduledRequest] = []
    episode_sizes: list[int] = []
    for context_episode_id in sorted(grouped):
        ordered = sorted(
            grouped[context_episode_id],
            key=lambda item: item[0],
        )
        episode_sizes.append(len(ordered))
        for source_ordinal, (_sort_key, request) in enumerate(ordered):
            requests.append(
                dataclasses.replace(
                    request,
                    source_ordinal=source_ordinal,
                )
            )
    schedule = tuple(requests)
    request_rows = [request.model_request() for request in schedule]
    if _contains_reference_key(request_rows):
        raise AssertionError("reference field entered prior-context schedule")
    schedule_records = [
        {
            "audio_uri": request.audio_uri,
            "context_episode_id": request.context_episode_id,
            "example_id": request.example_id,
            "manifest_index": request.manifest_index,
            "request_id": request.request_id,
            "segment_id": request.segment_id,
            "source_ordinal": request.source_ordinal,
            "source_uri": request.source_uri,
        }
        for request in schedule
    ]
    return PriorContextSchedule(
        requests=schedule,
        receipt={
            "context_cap": MAX_PRIOR_CONTEXT,
            "context_source": "same_checkpoint_prior_predictions_only",
            "episode_count": len(grouped),
            "max_episode_request_count": max(episode_sizes, default=0),
            "ordering": (
                "context episode ID, explicit request order or exact source "
                "position, stable request ID; Echo resets per request"
            ),
            "reference_field_names_in_request_schedule": [],
            "reference_transcript_accessed_for_context": False,
            "request_count": len(schedule),
            "request_schedule_sha256": _sha256(
                _canonical_jsonl(schedule_records)
            ),
            "schema_version": 1,
        },
    )


def refilter_masked_eval(
    *,
    train_rows: collections.abc.Sequence[collections.abc.Mapping[str, object]],
    masked_rows_by_family: collections.abc.Mapping[
        str,
        collections.abc.Sequence[collections.abc.Mapping[str, object]],
    ],
    index: groups.SourceIndex | BindingGroupResolver,
) -> MaskedEvalFilterResult:
    """Keep masked rows only when their physical group is not training-owned.

    Rows are copied without changing any field.  The approved common index is
    the production path; an injectable resolver with the same binding contract
    keeps unit tests small.
    """
    canonical_expected = MASKED_DATASET_FAMILIES
    canonical_inputs: dict[
        str,
        collections.abc.Sequence[collections.abc.Mapping[str, object]],
    ] = {}
    for raw_family, rows in masked_rows_by_family.items():
        family = _normalize_family(raw_family)
        if family in canonical_inputs:
            raise ValueError(f"duplicate masked family alias: {raw_family!r}")
        canonical_inputs[family] = rows
    if set(canonical_inputs) != set(canonical_expected):
        raise ValueError(
            "masked eval must contain exactly the expected four sources: "
            f"expected {sorted(canonical_expected)!r}, "
            f"got {sorted(canonical_inputs)!r}"
        )
    train_groups: set[str] = set()
    train_bindings: list[groups.RowBinding] = []
    for row_index, row in enumerate(train_rows):
        binding = groups.canonical_row_binding(
            dict(row),
            split="train",
            row_index=row_index,
        )
        train_bindings.append(binding)
        train_groups.update(
            _resolve_groups(
                index,
                binding,
                label=f"train row {row_index}",
            )
        )
    family_results: list[MaskedFamilyResult] = []
    retained_bindings: list[groups.RowBinding] = []
    global_row_index = 0
    for family in canonical_expected:
        kept: list[dict[str, object]] = []
        excluded: list[dict[str, object]] = []
        kept_digests: list[str] = []
        for family_row_index, row in enumerate(canonical_inputs[family]):
            copied = copy.deepcopy(dict(row))
            if copied.get("split") != "eval":
                raise ValueError(
                    f"{family} masked row {family_row_index} is not split=eval"
                )
            _validate_masked_family(copied, expected=family)
            example_id, segment_id = manifest.canonical_row_identity(copied)
            binding = groups.locator_row_binding(
                split="eval",
                row_index=global_row_index,
                row_identity=(example_id, segment_id),
                locator=groups.SourceLocator(
                    "masked_example_id",
                    example_id,
                    family,
                ),
            )
            resolved = _resolve_groups(
                index,
                binding,
                label=(
                    f"{family} masked row {family_row_index} "
                    f"({example_id}, {segment_id})"
                ),
            )
            if len(resolved) != 1:
                raise UnresolvedMaskedLineageError(
                    f"masked row must resolve to exactly one group: "
                    f"{family}/{example_id}/{segment_id}: {sorted(resolved)!r}"
                )
            intersecting = sorted(resolved & train_groups)
            row_sha256 = _sha256(_canonical_json(copied))
            if intersecting:
                excluded.append(
                    {
                        "example_id": example_id,
                        "family": family,
                        "intersecting_train_group_ids": intersecting,
                        "reason": (
                            "resolved_recording_group_intersects_final_training"
                        ),
                        "row_sha256": row_sha256,
                        "segment_id": segment_id,
                    }
                )
            else:
                kept.append(copied)
                kept_digests.append(row_sha256)
                retained_bindings.append(binding)
            global_row_index += 1
        family_receipt: dict[str, object] = {
            "excluded_count": len(excluded),
            "excluded_rows": copy.deepcopy(excluded),
            "family": family,
            "input_count": len(canonical_inputs[family]),
            "kept_count": len(kept),
            "kept_rows": [
                {
                    "example_id": manifest.canonical_row_identity(row)[0],
                    "row_sha256": digest,
                    "segment_id": manifest.canonical_row_identity(row)[1],
                }
                for row, digest in zip(kept, kept_digests, strict=True)
            ],
            "rows_resegmented": False,
            "schema_version": 1,
        }
        family_results.append(
            MaskedFamilyResult(
                family=family,
                kept_rows=tuple(kept),
                excluded_rows=tuple(excluded),
                receipt=family_receipt,
            )
        )
    gate_receipt = _gate_retained_rows(
        index=index,
        train_bindings=train_bindings,
        retained_bindings=retained_bindings,
        train_groups=train_groups,
    )
    total_input = sum(
        int(item.receipt["input_count"]) for item in family_results
    )
    total_kept = sum(int(item.receipt["kept_count"]) for item in family_results)
    total_excluded = sum(
        int(item.receipt["excluded_count"]) for item in family_results
    )
    return MaskedEvalFilterResult(
        families=tuple(family_results),
        receipt={
            "eval_lane": FINAL_EVAL_LANES[2],
            "excluded_count": total_excluded,
            "families": [
                copy.deepcopy(item.receipt) for item in family_results
            ],
            "input_count": total_input,
            "kept_count": total_kept,
            "recording_group_gate": gate_receipt,
            "recording_group_index": _index_receipt(index),
            "rows_resegmented": False,
            "schema_version": 1,
        },
    )


def final_eval_scope_receipt(
    *,
    primary: PrimaryEvalView,
    provider_validation: ProviderValidationView,
    prior_context: PriorContextSchedule,
    masked: MaskedEvalFilterResult,
) -> dict[str, object]:
    """Return the exhaustive final eval package inventory."""
    expected_schedule = build_prior_context_schedule(primary.rows)
    if expected_schedule.requests != prior_context.requests:
        raise ValueError(
            "prior-context schedule is not derived from primary eval"
        )
    primary_request_ids = {_request_id(row) for row in primary.rows}
    provider_request_ids = [
        _request_id(row) for row in provider_validation.rows
    ]
    if (
        len(provider_request_ids) != len(set(provider_request_ids))
        or not set(provider_request_ids).issubset(primary_request_ids)
        or provider_validation.receipt.get("full_primary_sha256")
        != primary.receipt["eval_sha256"]
    ):
        raise ValueError(
            "provider validation is not a bound subset of primary eval"
        )
    observed_masked_families = tuple(item.family for item in masked.families)
    if observed_masked_families != MASKED_DATASET_FAMILIES:
        raise ValueError("masked eval does not contain the frozen four sources")
    index_receipt = masked.receipt.get("recording_group_index")
    if (
        not isinstance(index_receipt, collections.abc.Mapping)
        or _SHA256_RE.fullmatch(str(index_receipt.get("index_sha256"))) is None
        or not isinstance(
            index_receipt.get("policy"),
            collections.abc.Mapping,
        )
    ):
        raise ValueError("masked eval lacks a frozen recording-group index")
    return {
        "atomic_unmasked_eval_retained": False,
        "eval_lanes": {
            "masked_v2": {
                "kind": "masked_v2",
                "kept_count": masked.receipt["kept_count"],
                "rows_resegmented": False,
            },
            "predicted_prior_context_10": {
                "base_lane": "reconstructed_primary",
                "kind": "inference_view",
                "max_prior_segments": MAX_PRIOR_CONTEXT,
                "prior_transcript_source": "predicted_transcription",
                "request_count": len(prior_context.requests),
                "schedule_sha256": prior_context.receipt[
                    "request_schedule_sha256"
                ],
                "uses_reference_transcription": False,
            },
            "reconstructed_primary": {
                "kind": "manifest",
                "row_count": len(primary.rows),
                "sha256": primary.receipt["eval_sha256"],
            },
        },
        "masked_kept_count": masked.receipt["kept_count"],
        "primary_row_count": len(primary.rows),
        "prior_context_request_count": len(prior_context.requests),
        "provider_validation": {
            "algorithm": provider_validation.receipt["algorithm"],
            "full_primary_sha256": primary.receipt["eval_sha256"],
            "kind": "deterministic_subset_of_reconstructed_primary",
            "row_count": len(provider_validation.rows),
            "sha256": provider_validation.receipt["selected_rows_sha256"],
        },
        "schema_version": 1,
        "validation": "deterministic_subset_of_reconstructed_primary",
    }


def _resolve_groups(
    index: groups.SourceIndex | BindingGroupResolver,
    binding: groups.RowBinding,
    *,
    label: str,
) -> frozenset[str]:
    try:
        if isinstance(index, groups.SourceIndex):
            resolved = groups.group_ids_for_binding(binding, index)
        elif isinstance(index, BindingGroupResolver):
            resolved = frozenset(index.group_ids_for_binding(binding))
        else:
            raise TypeError(
                "index must be common.recording_groups.SourceIndex or "
                "BindingGroupResolver"
            )
    except (TypeError, ValueError) as error:
        raise UnresolvedMaskedLineageError(
            f"{label} did not resolve through frozen recording groups: {error}"
        ) from error
    if not resolved or any(
        not isinstance(value, str) or not value.strip() for value in resolved
    ):
        raise UnresolvedMaskedLineageError(
            f"{label} resolved to no valid recording group"
        )
    return frozenset(resolved)


def _gate_retained_rows(
    *,
    index: groups.SourceIndex | BindingGroupResolver,
    train_bindings: collections.abc.Sequence[groups.RowBinding],
    retained_bindings: collections.abc.Sequence[groups.RowBinding],
    train_groups: set[str],
) -> dict[str, object]:
    if isinstance(index, groups.SourceIndex):
        return groups.require_training_boundary_disjoint(
            bindings_by_split={
                "eval": retained_bindings,
                "train": train_bindings,
                "validation": (),
            },
            index=index,
            allow_validation_eval_overlap=True,
        )
    retained_groups: set[str] = set()
    for binding in retained_bindings:
        retained_groups.update(
            _resolve_groups(index, binding, label="retained masked row")
        )
    intersections = sorted(train_groups & retained_groups)
    if intersections:
        raise ValueError(
            f"retained masked rows intersect training groups: {intersections!r}"
        )
    return {
        "all_rows_resolved": True,
        "status": "pass",
        "train_eval_intersections": [],
    }


def _index_receipt(
    index: groups.SourceIndex | BindingGroupResolver,
) -> dict[str, object]:
    if isinstance(index, groups.SourceIndex):
        record = index.record
        return {
            "grouping_summary_sha256": index.grouping_summary_sha256,
            "index_sha256": index.sha256,
            "policy": copy.deepcopy(record["policy"]),
            "resolver": "common.recording_groups.SourceIndex",
        }
    raw_sha256 = index.sha256
    if _SHA256_RE.fullmatch(raw_sha256) is None:
        raise ValueError("injected recording-group index SHA-256 is invalid")
    raw_policy = index.policy_receipt
    if not isinstance(raw_policy, collections.abc.Mapping) or not raw_policy:
        raise ValueError("injected recording-group policy receipt is missing")
    return {
        "index_sha256": raw_sha256,
        "policy": copy.deepcopy(dict(raw_policy)),
        "resolver": f"{type(index).__module__}.{type(index).__qualname__}",
    }


def _request_id(row: collections.abc.Mapping[str, object]) -> str:
    explicit = _optional_string(row.get("request_id"))
    if explicit is not None:
        return explicit
    example_id = _optional_string(row.get("example_id"))
    segment_id = _optional_string(row.get("segment_id"))
    if example_id is not None and segment_id is not None:
        return f"{example_id}::{segment_id}"
    raise ValueError(
        "eval row requires request_id or example_id plus segment_id"
    )


def _source_uri(row: collections.abc.Mapping[str, object]) -> str:
    source_audio = row.get("source_audio")
    if isinstance(source_audio, collections.abc.Mapping):
        value = source_audio.get("audio_filepath")
        if value is None:
            value = source_audio.get("original_audio_uri")
        if value is not None:
            return _required_string(value, "source_audio URI")
    original = row.get("original_audio_uri")
    if original is not None:
        return _required_string(original, "original_audio_uri")
    raise ValueError(
        "eval row lacks exact source_audio.audio_filepath or "
        "original_audio_uri for prior-context boundary"
    )


def _context_episode_id(
    row: collections.abc.Mapping[str, object],
    *,
    request_id: str,
    source_uri: str,
) -> str:
    dataset = row.get("dataset")
    if not isinstance(dataset, collections.abc.Mapping):
        raise ValueError("eval row lacks canonical dataset metadata")
    family_value = dataset.get("family")
    if family_value is None:
        family_value = dataset.get("name")
    family = _normalize_family(family_value)
    expected = request_id if family == "echo" else source_uri
    explicit = _required_string(
        row.get("context_episode_id"),
        "context_episode_id",
    )
    source_lineage = row.get("source_lineage")
    if not isinstance(source_lineage, collections.abc.Mapping):
        raise ValueError("eval row lacks source_lineage metadata")
    lineage = _required_string(
        source_lineage.get("context_episode_id"),
        "source_lineage.context_episode_id",
    )
    if explicit != expected or lineage != expected:
        raise ValueError(
            f"{request_id}: context episode drift; expected {expected!r}"
        )
    return expected


def _source_order_key(
    row: collections.abc.Mapping[str, object],
    request_id: str,
) -> tuple[object, ...]:
    request_order = row.get("request_order")
    if request_order is not None:
        if (
            isinstance(request_order, bool)
            or not isinstance(request_order, int)
            or request_order < 0
        ):
            raise ValueError("request_order must be a non-negative integer")
        return (0, decimal.Decimal(request_order), request_id)
    source_audio = row.get("source_audio")
    if isinstance(source_audio, collections.abc.Mapping):
        start_frame = source_audio.get("start_frame")
        if start_frame is not None:
            if (
                isinstance(start_frame, bool)
                or not isinstance(start_frame, int)
                or start_frame < 0
            ):
                raise ValueError(
                    "source_audio.start_frame must be a non-negative integer"
                )
            return (1, decimal.Decimal(start_frame), request_id)
        offset = source_audio.get("offset")
        if offset is not None:
            return (
                2,
                _nonnegative_decimal(offset, "source_audio.offset"),
                request_id,
            )
    start_frame = row.get("start_frame")
    if start_frame is not None:
        if (
            isinstance(start_frame, bool)
            or not isinstance(start_frame, int)
            or start_frame < 0
        ):
            raise ValueError("start_frame must be a non-negative integer")
        return (3, decimal.Decimal(start_frame), request_id)
    offset = row.get("offset")
    if offset is not None:
        return (4, _nonnegative_decimal(offset, "offset"), request_id)
    raise ValueError(
        "eval row lacks request_order, start_frame, or offset for ordering"
    )


def _nonnegative_decimal(value: object, label: str) -> decimal.Decimal:
    if isinstance(value, bool):
        raise ValueError(f"{label} must be numeric")
    try:
        result = decimal.Decimal(str(value))
    except decimal.InvalidOperation as error:
        raise ValueError(f"{label} must be numeric") from error
    if not result.is_finite() or result < 0:
        raise ValueError(f"{label} must be finite and non-negative")
    return result


def _normalize_prediction(value: str | None) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise TypeError("prediction must be a string or None")
    normalized = " ".join(value.split())
    if not normalized or normalized.casefold() in _EMPTY_PREDICTION_MARKERS:
        return None
    return normalized


def _normalize_family(value: object) -> str:
    required = _required_string(value, "dataset family")
    normalized = required.casefold().replace("-", "_").replace(" ", "_")
    aliases = {
        "bcfy_calls": "bcfy_calls",
        "bcfy_feeds": "bcfy_feeds",
        "broadcastify_calls": "bcfy_calls",
        "broadcastify_feeds": "bcfy_feeds",
        "echo": "echo",
        "fire_notification": "fire_notifications",
        "fire_notifications": "fire_notifications",
    }
    try:
        return aliases[normalized]
    except KeyError:
        raise ValueError(
            f"unsupported masked dataset family: {value!r}"
        ) from None


def _validate_masked_family(
    row: collections.abc.Mapping[str, object],
    *,
    expected: str,
) -> None:
    declared: list[str] = []
    for key in ("dataset_family", "dataset_name", "source_dataset"):
        if row.get(key) is not None:
            declared.append(_normalize_family(row[key]))
    dataset = row.get("dataset")
    if isinstance(dataset, collections.abc.Mapping):
        for key in ("family", "name"):
            if dataset.get(key) is not None:
                declared.append(_normalize_family(dataset[key]))
    if not declared:
        raise ValueError("masked row lacks dataset-family metadata")
    if any(value != expected for value in declared):
        raise ValueError(
            f"masked row family does not match {expected!r}: {declared!r}"
        )


def _required_string(value: object, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{label} must be a nonblank string")
    if value != value.strip():
        raise ValueError(f"{label} must not have surrounding whitespace")
    return value


def _optional_string(value: object) -> str | None:
    if value is None:
        return None
    return _required_string(value, "identity field")


def _contains_reference_key(value: object) -> bool:
    if isinstance(value, collections.abc.Mapping):
        for key, item in value.items():
            if str(key).strip().casefold() in _REFERENCE_KEYS:
                return True
            if _contains_reference_key(item):
                return True
    elif isinstance(value, (list, tuple)):
        return any(_contains_reference_key(item) for item in value)
    return False


def _canonical_json(record: object) -> bytes:
    try:
        return (
            json.dumps(
                record,
                allow_nan=False,
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            )
            + "\n"
        ).encode()
    except (TypeError, ValueError) as error:
        raise ValueError(
            "eval row is not canonical-JSON serializable"
        ) from error


def _canonical_jsonl(
    rows: collections.abc.Iterable[collections.abc.Mapping[str, object]],
) -> bytes:
    return b"".join(_canonical_json(dict(row)) for row in rows)


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()
