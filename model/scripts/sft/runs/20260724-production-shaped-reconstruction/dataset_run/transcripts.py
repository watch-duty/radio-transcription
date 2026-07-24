"""Deterministic transcript reconciliation for reconstructed requests.

The reconstruction keeps every speech owner for lineage and frame coverage.
This module only prevents duplicated *target text* when source annotations
overlap or touch.  Speech annotations separated by even one frame are
different utterances and are therefore concatenated without deduplication.
"""

from __future__ import annotations

import dataclasses
import enum
import json
import unicodedata
from collections.abc import Mapping, Sequence
from typing import Any

from .domain import normalize_text

# One shared token is frequently an ordinary repeated word ("the", "engine",
# etc.).  Requiring two exact normalized tokens makes suffix-prefix merging
# deliberately conservative: ambiguity preserves both targets.
MIN_CLEAR_OVERLAP_TOKENS = 2


class ReconciliationRule(enum.StrEnum):
    """Frozen target-text reconciliation rules."""

    SINGLE = "single"
    DUPLICATE = "duplicate"
    CONTAINED = "contained"
    SUFFIX_PREFIX = "suffix_prefix_overlap"
    CONCATENATE = "chronological_concat"


@dataclasses.dataclass(frozen=True, slots=True)
class TranscriptOwner:
    """One speech target and the authority metadata used to reconcile it.

    ``authority_rank`` is an explicit, caller-supplied revision ordering:
    larger values are newer or otherwise more authoritative.  A corrected
    target outranks an uncorrected equivalent target regardless of rank.
    Authority is only used to choose among duplicate targets; it never removes
    words that appear only in a longer target.
    """

    atom_id: str
    text: str
    start_frame: int
    end_frame: int
    corrected: bool = False
    authority_rank: int = 0
    provenance: Mapping[str, object] | None = None

    def __post_init__(self) -> None:
        if not self.atom_id:
            raise ValueError("atom_id must be non-empty")
        if self.start_frame < 0:
            raise ValueError("start_frame must be non-negative")
        if self.end_frame <= self.start_frame:
            raise ValueError("end_frame must exceed start_frame")
        if normalize_text(self.text) is None:
            raise ValueError("transcript owner text must be non-empty")
        if not isinstance(self.corrected, bool):
            raise ValueError("corrected must be a boolean")
        if isinstance(self.authority_rank, bool) or not isinstance(
            self.authority_rank,
            int,
        ):
            raise ValueError("authority_rank must be an integer")
        if self.provenance is not None and not isinstance(
            self.provenance,
            Mapping,
        ):
            raise ValueError("provenance must be a mapping")


@dataclasses.dataclass(frozen=True, slots=True)
class ReconciledTranscript:
    """Final target plus an immutable, JSON-serializable decision receipt."""

    text: str
    owner_ids: tuple[str, ...]
    covered_intervals: tuple[tuple[int, int], ...]
    covered_frame_count: int
    _receipt_json: str = dataclasses.field(repr=False)

    @property
    def receipt(self) -> dict[str, object]:
        """Return a fresh JSON-compatible receipt for audit or persistence."""
        value = json.loads(self._receipt_json)
        if not isinstance(value, dict):  # pragma: no cover - constructor guard
            raise AssertionError("reconciliation receipt must be an object")
        return value

    @property
    def receipt_json(self) -> str:
        """Return the canonical JSON encoding of :attr:`receipt`."""
        return self._receipt_json


@dataclasses.dataclass(frozen=True, slots=True)
class _TextState:
    text: str
    tokens: tuple[str, ...]
    token_keys: tuple[str, ...]
    owner_ids: tuple[str, ...]


def reconcile_component(
    owners: Sequence[object],
) -> ReconciledTranscript:
    """Reconcile speech targets in one reconstructed request.

    Args:
        owners: Speech owners.  Inputs may be :class:`TranscriptOwner`,
            ``domain.OwnedSpeech`` objects, mappings, or objects exposing
            ``atom_id``, ``text``, ``start_frame``, and ``end_frame``.
            Corrected authority may be supplied with ``corrected`` (or
            ``is_corrected``) and ``authority_rank`` (or ``revision_rank``).

    Returns:
        A deterministic target and a complete reconciliation receipt.

    Raises:
        ValueError: If no owners are supplied, identifiers repeat, input
            geometry is invalid, or a target is blank.

    Notes:
        Duplicate/containment/suffix-prefix rules operate only inside a
        connected component of overlapping or exactly adjacent speech
        intervals.  Disconnected speech is always concatenated chronologically
        so repeated radio phrases are not accidentally discarded.
    """
    normalized_owners = tuple(_coerce_owner(owner) for owner in owners)
    if not normalized_owners:
        raise ValueError("reconcile_component requires at least one owner")

    ids = [owner.atom_id for owner in normalized_owners]
    if len(ids) != len(set(ids)):
        raise ValueError("transcript owner atom_id values must be unique")

    ordered = tuple(sorted(normalized_owners, key=_chronological_key))
    interval_components = _partition_touching_components(ordered)
    component_results = tuple(
        _reconcile_touching_component(index, component)
        for index, component in enumerate(interval_components)
    )
    final_text = " ".join(result["text"] for result in component_results)
    owner_ids = tuple(owner.atom_id for owner in ordered)
    covered_intervals = _union_intervals(ordered)
    covered_frames = sum(end - start for start, end in covered_intervals)
    raw_owner_frames = sum(
        owner.end_frame - owner.start_frame for owner in ordered
    )

    receipt: dict[str, object] = {
        "version": 1,
        "policy": {
            "comparison_scope": "overlapping_or_adjacent_speech_only",
            "duplicate": (
                "corrected_then_authority_rank_then_deterministic_tie_break"
            ),
            "contained": "longer_token_sequence_once",
            "suffix_prefix": (
                f"maximal_exact_overlap_at_least_"
                f"{MIN_CLEAR_OVERLAP_TOKENS}_tokens"
            ),
            "ambiguous": "chronological_whitespace_normalized_concat",
            "disconnected": "chronological_whitespace_normalized_concat",
        },
        "owner_order": list(owner_ids),
        "owners": [_owner_receipt(owner) for owner in ordered],
        "audio_coverage": {
            "covered_intervals": [
                {"start_frame": start, "end_frame": end}
                for start, end in covered_intervals
            ],
            "covered_frame_count": covered_frames,
            "raw_owner_frame_count": raw_owner_frames,
            "overlap_frames_not_double_counted": (
                raw_owner_frames - covered_frames
            ),
        },
        "components": list(component_results),
        "component_join_rule": ReconciliationRule.CONCATENATE.value,
        "text": final_text,
    }
    receipt_json = json.dumps(
        receipt,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return ReconciledTranscript(
        text=final_text,
        owner_ids=owner_ids,
        covered_intervals=covered_intervals,
        covered_frame_count=covered_frames,
        _receipt_json=receipt_json,
    )


def _reconcile_touching_component(
    component_index: int,
    owners: tuple[TranscriptOwner, ...],
) -> dict[str, object]:
    pair_decisions, suppressed_ids = _suppression_decisions(owners)
    survivors = tuple(
        owner for owner in owners if owner.atom_id not in suppressed_ids
    )
    if not survivors:  # pragma: no cover - strict ordering prevents cycles
        raise AssertionError("transcript suppression removed every owner")

    first = survivors[0]
    state = _state_from_owner(first)
    fold_decisions: list[dict[str, object]] = []
    if len(survivors) == 1 and not pair_decisions:
        fold_decisions.append(
            {
                "rule": ReconciliationRule.SINGLE.value,
                "left_owner_ids": [],
                "right_owner_ids": [first.atom_id],
                "overlap_token_count": 0,
                "result_text": state.text,
                "reason": "component contains one speech owner",
            }
        )

    for owner in survivors[1:]:
        right = _state_from_owner(owner)
        overlap = _maximal_suffix_prefix_overlap(
            state.token_keys,
            right.token_keys,
        )
        if overlap >= MIN_CLEAR_OVERLAP_TOKENS:
            merged_tokens = state.tokens + right.tokens[overlap:]
            rule = ReconciliationRule.SUFFIX_PREFIX
            reason = (
                "maximal exact normalized token suffix-prefix overlap "
                "was merged once"
            )
        else:
            merged_tokens = state.tokens + right.tokens
            rule = ReconciliationRule.CONCATENATE
            if overlap:
                reason = (
                    "one-token overlap is ambiguous and therefore both "
                    "targets were preserved"
                )
            else:
                reason = (
                    "no clear suffix-prefix overlap; both targets were "
                    "preserved chronologically"
                )
        result_text = " ".join(merged_tokens)
        fold_decisions.append(
            {
                "rule": rule.value,
                "left_owner_ids": list(state.owner_ids),
                "right_owner_ids": [owner.atom_id],
                "overlap_token_count": overlap,
                "result_text": result_text,
                "reason": reason,
            }
        )
        state = _TextState(
            text=result_text,
            tokens=merged_tokens,
            token_keys=tuple(_token_key(token) for token in merged_tokens),
            owner_ids=state.owner_ids + (owner.atom_id,),
        )

    start_frame = min(owner.start_frame for owner in owners)
    end_frame = max(owner.end_frame for owner in owners)
    intervals = _union_intervals(owners)
    return {
        "component_index": component_index,
        "start_frame": start_frame,
        "end_frame": end_frame,
        "owner_ids": [owner.atom_id for owner in owners],
        "surviving_text_owner_ids": [owner.atom_id for owner in survivors],
        "suppressed_text_owner_ids": sorted(suppressed_ids),
        "covered_intervals": [
            {"start_frame": start, "end_frame": end} for start, end in intervals
        ],
        "covered_frame_count": sum(end - start for start, end in intervals),
        "pair_decisions": pair_decisions,
        "fold_decisions": fold_decisions,
        "text": state.text,
    }


def _suppression_decisions(
    owners: tuple[TranscriptOwner, ...],
) -> tuple[list[dict[str, object]], set[str]]:
    """Return atomic duplicate/containment decisions before text folding."""
    decisions: list[dict[str, object]] = []
    losers: set[str] = set()
    for left_index, left in enumerate(owners):
        for right in owners[left_index + 1 :]:
            if not _intervals_touch(left, right):
                continue
            left_keys = _owner_token_keys(left)
            right_keys = _owner_token_keys(right)
            if left_keys == right_keys:
                winner, loser, tie_break = _select_duplicate_authority(
                    left,
                    right,
                )
                rule = ReconciliationRule.DUPLICATE
                reason = (
                    f"normalized targets are duplicate; selected {tie_break}"
                )
            elif _contains_tokens(left_keys, right_keys):
                winner, loser = left, right
                rule = ReconciliationRule.CONTAINED
                tie_break = "longer_token_sequence"
                reason = (
                    "right normalized target is fully contained in the "
                    "longer left target"
                )
            elif _contains_tokens(right_keys, left_keys):
                winner, loser = right, left
                rule = ReconciliationRule.CONTAINED
                tie_break = "longer_token_sequence"
                reason = (
                    "left normalized target is fully contained in the "
                    "longer right target"
                )
            else:
                continue
            losers.add(loser.atom_id)
            decisions.append(
                {
                    "rule": rule.value,
                    "left_owner_ids": [left.atom_id],
                    "right_owner_ids": [right.atom_id],
                    "selected_owner_id": winner.atom_id,
                    "suppressed_owner_ids": [loser.atom_id],
                    "tie_break": tie_break,
                    "overlap_token_count": 0,
                    "result_text": normalize_text(winner.text),
                    "reason": reason,
                }
            )
    return decisions, losers


def _select_duplicate_authority(
    left: TranscriptOwner,
    right: TranscriptOwner,
) -> tuple[TranscriptOwner, TranscriptOwner, str]:
    if left.corrected != right.corrected:
        winner = left if left.corrected else right
        reason = "corrected_authority"
    elif left.authority_rank != right.authority_rank:
        winner = left if left.authority_rank > right.authority_rank else right
        reason = "higher_authority_rank"
    else:
        left_text = normalize_text(left.text)
        right_text = normalize_text(right.text)
        assert left_text is not None and right_text is not None
        if len(left_text) != len(right_text):
            winner = left if len(left_text) > len(right_text) else right
            reason = "longer_normalized_target"
        else:
            left_duration = left.end_frame - left.start_frame
            right_duration = right.end_frame - right.start_frame
            if left_duration != right_duration:
                winner = left if left_duration > right_duration else right
                reason = "longer_annotated_span"
            else:
                winner = min((left, right), key=_chronological_key)
                reason = "chronology_then_atom_id"
    loser = right if winner is left else left
    return winner, loser, reason


def _partition_touching_components(
    owners: tuple[TranscriptOwner, ...],
) -> tuple[tuple[TranscriptOwner, ...], ...]:
    components: list[list[TranscriptOwner]] = []
    current: list[TranscriptOwner] = []
    current_end = -1
    for owner in owners:
        if current and owner.start_frame > current_end:
            components.append(current)
            current = []
            current_end = -1
        current.append(owner)
        current_end = max(current_end, owner.end_frame)
    if current:
        components.append(current)
    return tuple(tuple(component) for component in components)


def _union_intervals(
    owners: Sequence[TranscriptOwner],
) -> tuple[tuple[int, int], ...]:
    intervals = sorted((owner.start_frame, owner.end_frame) for owner in owners)
    merged: list[tuple[int, int]] = []
    for start, end in intervals:
        if not merged or start > merged[-1][1]:
            merged.append((start, end))
            continue
        previous_start, previous_end = merged[-1]
        merged[-1] = (previous_start, max(previous_end, end))
    return tuple(merged)


def _intervals_touch(
    left: TranscriptOwner,
    right: TranscriptOwner,
) -> bool:
    return (
        left.start_frame <= right.end_frame
        and right.start_frame <= left.end_frame
    )


def _contains_tokens(
    container: tuple[str, ...],
    contained: tuple[str, ...],
) -> bool:
    if len(container) <= len(contained):
        return False
    width = len(contained)
    return any(
        container[index : index + width] == contained
        for index in range(len(container) - width + 1)
    )


def _maximal_suffix_prefix_overlap(
    left: tuple[str, ...],
    right: tuple[str, ...],
) -> int:
    for width in range(min(len(left), len(right)), 0, -1):
        if left[-width:] == right[:width]:
            return width
    return 0


def _state_from_owner(owner: TranscriptOwner) -> _TextState:
    text = normalize_text(owner.text)
    assert text is not None
    tokens = tuple(text.split(" "))
    return _TextState(
        text=text,
        tokens=tokens,
        token_keys=tuple(_token_key(token) for token in tokens),
        owner_ids=(owner.atom_id,),
    )


def _owner_token_keys(owner: TranscriptOwner) -> tuple[str, ...]:
    return _state_from_owner(owner).token_keys


def _token_key(token: str) -> str:
    return unicodedata.normalize("NFKC", token).casefold()


def _chronological_key(
    owner: TranscriptOwner,
) -> tuple[int, int, str]:
    return (owner.start_frame, owner.end_frame, owner.atom_id)


def _owner_receipt(owner: TranscriptOwner) -> dict[str, object]:
    text = normalize_text(owner.text)
    assert text is not None
    return {
        "owner_id": owner.atom_id,
        "start_frame": owner.start_frame,
        "end_frame": owner.end_frame,
        "normalized_text": text,
        "corrected_authority": owner.corrected,
        "authority_rank": owner.authority_rank,
        "provenance": _json_compatible(owner.provenance or {}),
    }


def _coerce_owner(value: object) -> TranscriptOwner:
    if isinstance(value, TranscriptOwner):
        return value

    atom = _read_optional(value, "atom")
    atom_source = atom if atom is not None else value
    atom_id = _first_present(atom_source, ("atom_id", "owner_id", "id"))
    text = _first_present(value, ("text", "target", "transcript"))
    start_frame = _first_present(atom_source, ("start_frame",))
    end_frame = _first_present(atom_source, ("end_frame",))

    provenance_value = _read_optional(value, "provenance")
    if provenance_value is None:
        provenance_value = _read_optional(atom_source, "provenance")
    if provenance_value is not None and not isinstance(
        provenance_value,
        Mapping,
    ):
        raise ValueError("owner provenance must be a mapping")
    provenance = provenance_value

    corrected = _optional_bool(
        value,
        (
            "corrected",
            "is_corrected",
            "corrected_authority",
            "authoritative_correction",
        ),
    )
    if corrected is None and atom_source is not value:
        corrected = _optional_bool(
            atom_source,
            (
                "corrected",
                "is_corrected",
                "corrected_authority",
                "authoritative_correction",
            ),
        )
    if corrected is None and provenance is not None:
        corrected = _optional_bool(
            provenance,
            (
                "corrected",
                "is_corrected",
                "corrected_authority",
                "authoritative_correction",
            ),
        )

    authority_rank = _optional_integer(
        value,
        (
            "authority_rank",
            "authoritative_rank",
            "revision_rank",
            "latest_rank",
        ),
    )
    if authority_rank is None and atom_source is not value:
        authority_rank = _optional_integer(
            atom_source,
            (
                "authority_rank",
                "authoritative_rank",
                "revision_rank",
                "latest_rank",
            ),
        )
    if authority_rank is None and provenance is not None:
        authority_rank = _optional_integer(
            provenance,
            (
                "authority_rank",
                "authoritative_rank",
                "revision_rank",
                "latest_rank",
            ),
        )

    if not isinstance(atom_id, str):
        raise ValueError("owner atom_id must be a string")
    if not isinstance(text, str):
        raise ValueError(f"owner {atom_id!r} text must be a string")
    if isinstance(start_frame, bool) or not isinstance(start_frame, int):
        raise ValueError(f"owner {atom_id!r} start_frame must be an integer")
    if isinstance(end_frame, bool) or not isinstance(end_frame, int):
        raise ValueError(f"owner {atom_id!r} end_frame must be an integer")
    return TranscriptOwner(
        atom_id=atom_id,
        text=text,
        start_frame=start_frame,
        end_frame=end_frame,
        corrected=corrected or False,
        authority_rank=authority_rank or 0,
        provenance=provenance,
    )


def _first_present(value: object, names: Sequence[str]) -> object:
    for name in names:
        candidate = _read_optional(value, name)
        if candidate is not None:
            return candidate
    joined = ", ".join(names)
    raise ValueError(f"owner is missing required field ({joined})")


def _read_optional(value: object, name: str) -> object | None:
    if isinstance(value, Mapping):
        return value.get(name)
    return getattr(value, name, None)


def _optional_bool(
    value: object,
    names: Sequence[str],
) -> bool | None:
    for name in names:
        candidate = _read_optional(value, name)
        if candidate is None:
            continue
        if not isinstance(candidate, bool):
            raise ValueError(f"{name} must be a boolean")
        return candidate
    return None


def _optional_integer(
    value: object,
    names: Sequence[str],
) -> int | None:
    for name in names:
        candidate = _read_optional(value, name)
        if candidate is None:
            continue
        if isinstance(candidate, bool) or not isinstance(candidate, int):
            raise ValueError(f"{name} must be an integer")
        return candidate
    return None


def _json_compatible(value: object) -> Any:
    """Return a deterministic JSON value or fail on unsupported provenance."""
    try:
        payload = json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
    except (TypeError, ValueError) as error:
        raise ValueError("provenance must be JSON-compatible") from error
    return json.loads(payload)
