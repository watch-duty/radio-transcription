"""One-time local staging for the production-shaped reconstruction.

This module joins the already-frozen construction pieces.  It deliberately
does not inventory, decode-audit, upload, publish, tune, or run inference.
Frozen source inventory is trusted; only slices selected for the final request
universe are decoded and materialized.
"""

# One-time fail-loud orchestration: direct diagnostic exceptions are intended.
# ruff: noqa: D202, EM101, EM102, PLR0911, PLR0912, PLR0915, TRY003, TRY301

from __future__ import annotations

import concurrent.futures
import copy
import dataclasses
import decimal
import enum
import fractions
import hashlib
import json
import pathlib
from collections import defaultdict
from collections.abc import Callable, Iterable, Mapping, Sequence
from typing import Any

from common import manifest as common_manifest
from common import recording_groups

from . import (
    artifacts,
    continuous,
    domain,
    eval_views,
    inputs,
    manifests,
    media,
    ownership,
    presegmented,
    transcripts,
    verify,
)


class StagingError(RuntimeError):
    """Raised when the frozen local build cannot be staged exactly."""


MASKED_RETAINED_COUNT = 2_043
MASKED_RETAINED_SHA256 = (
    "2bb7e826124864e965530c23a14d799d1e4256f5bfd33917890b73c22b97c601"
)
MASKED_INDEX_SHA256 = (
    "00cc72aa0e26f82bd367e901f8c22681cf0c927a610c43714f5aacd710ba3c5b"
)
_ENCODER_PROFILE_BY_FAMILY = {
    "bcfy_feeds": media.EncoderProfile.BCFY_FEEDS_SOUNDFILE,
    "bcfy_calls": media.EncoderProfile.PRESEGMENTED_FFMPEG,
    "echo": media.EncoderProfile.PRESEGMENTED_FFMPEG,
    "fire_notifications": media.EncoderProfile.PRESEGMENTED_FFMPEG,
}


@dataclasses.dataclass(frozen=True, slots=True)
class StagePaths:
    """All local inputs and outputs for the one-time staging pass."""

    input_paths: inputs.InputPaths
    source_inventory_receipt: pathlib.Path
    production_duration_target: pathlib.Path
    construction_contract: pathlib.Path
    encoder_toolchain_receipt: pathlib.Path
    ffmpeg_binary: pathlib.Path
    ownership_receipt: pathlib.Path
    masked_paths_by_family: Mapping[str, pathlib.Path]
    masked_retained_proof: pathlib.Path
    masked_filter_report: pathlib.Path
    output_root: pathlib.Path
    repo_root: pathlib.Path
    proposed_gcs_prefix: str
    continuous_plan_root: pathlib.Path | None = None


@dataclasses.dataclass(frozen=True, slots=True)
class FrameUniverse:
    """The complete protected universe on source-native frame lattices."""

    sources: tuple[media.SourceInventory, ...]
    source_media: Mapping[str, domain.SourceMedia]
    atoms: tuple[domain.Atom, ...]
    owned_speech: tuple[StagedOwnedSpeech, ...]
    ownership: ownership.OwnershipResult

    @property
    def pii_atoms(self) -> tuple[domain.Atom, ...]:
        """Return all targetless PII annotations on represented sources."""

        return tuple(
            atom
            for atom in self.atoms
            if atom.atom_class is domain.AtomClass.PII
        )

    @property
    def speech_atoms(self) -> tuple[domain.Atom, ...]:
        """Return the exact protected Speech owner geometry."""

        return tuple(owner.atom for owner in self.owned_speech)


@dataclasses.dataclass(frozen=True, slots=True)
class PlanningResult:
    """Final requests plus the constructor receipts that selected them."""

    requests: tuple[domain.Request, ...]
    presegmented: presegmented.PresegmentedResult
    continuous_train: continuous.ContinuousSplitPlan
    continuous_eval: continuous.ContinuousSplitPlan


@dataclasses.dataclass(frozen=True, slots=True)
class EvaluationArtifacts:
    """The exact three-lane final evaluation package."""

    primary: eval_views.PrimaryEvalView
    provider_validation: eval_views.ProviderValidationView
    prior_context: eval_views.PriorContextSchedule
    masked: eval_views.MaskedEvalFilterResult
    masked_selection_proof_sha256: str
    masked_retained_payload_sha256: str
    scope_receipt: Mapping[str, object]
    masked_verifier_rows: tuple[dict[str, object], ...]
    masked_physical_groups: Mapping[str, str]


@dataclasses.dataclass(frozen=True, slots=True)
class StagingResult:
    """Locally materialized and independently verified build result."""

    requests: tuple[domain.Request, ...]
    materialization_bindings: tuple[manifests.MaterializationBinding, ...]
    manifest_artifacts: manifests.ManifestArtifacts
    evaluation: EvaluationArtifacts
    verification_bundle: Mapping[str, object]
    verification_report: verify.VerificationReport
    artifact_receipts: tuple[artifacts.ArtifactReceipt, ...]
    completion_marker: artifacts.ArtifactReceipt


@dataclasses.dataclass(frozen=True, slots=True)
class StagedOwnedSpeech:
    """Owned Speech plus transcript authority retained through planning."""

    atom: domain.Atom
    split: domain.Split
    text: str
    corrected: bool
    authority_rank: int
    provenance: Mapping[str, object]


def _domain_source(source: media.SourceInventory) -> domain.SourceMedia:
    return domain.SourceMedia(
        uri=source.uri,
        generation=source.generation,
        size_bytes=source.size_bytes,
        md5_hash=source.md5_hash,
        crc32c=source.crc32c,
        sha256=source.sha256,
        sample_rate=source.sample_rate,
        channels=source.channels,
        subtype=source.subtype,
        frame_count=source.frame_count,
    )


def build_frame_universe(
    bundle: inputs.InputBundle,
    owner_result: ownership.OwnershipResult,
    sources: Sequence[media.SourceInventory],
) -> FrameUniverse:
    """Convert raw annotations and authoritative owners to exact frames.

    Raw Speech timing/text is replaced by its authoritative final owner.
    Targetless and PII annotations retain their raw geometry.  Annotations on
    sources without protected Speech are not admitted to the reconstruction.
    """

    source_inventory = media.source_inventory_by_uri(sources)
    expected_sources = set(bundle.source_uris)
    observed_sources = set(source_inventory)
    if observed_sources != expected_sources:
        raise StagingError(
            "frozen source inventory does not match protected source universe; "
            f"missing={sorted(expected_sources - observed_sources)[:5]}, "
            f"extra={sorted(observed_sources - expected_sources)[:5]}"
        )

    owner_by_locator = owner_result.owner_by_locator()
    expected_locators = {
        annotation.locator for annotation in bundle.speech_annotations
    }
    if set(owner_by_locator) != expected_locators:
        raise StagingError("ownership is not exact over raw Speech annotations")

    source_media = {
        uri: _domain_source(source)
        for uri, source in sorted(source_inventory.items())
    }
    atoms: list[domain.Atom] = []
    owned_speech: list[StagedOwnedSpeech] = []
    for annotation in bundle.raw_annotations:
        source = source_inventory.get(annotation.source_uri)
        if source is None:
            # Raw-only targetless sources are outside the protected universe.
            if annotation.atom_class is domain.AtomClass.SPEECH:
                raise StagingError(
                    f"Speech source is absent from inventory: "
                    f"{annotation.source_uri}"
                )
            continue
        if annotation.atom_class is domain.AtomClass.SPEECH:
            try:
                owned_atom = owner_by_locator[annotation.locator]
                base = owned_atom.to_owned_speech(source.sample_rate)
            except KeyError:
                raise StagingError(
                    f"Speech annotation has no owner: "
                    f"{annotation.locator.atom_id}"
                ) from None
            owned = StagedOwnedSpeech(
                atom=base.atom,
                split=base.split,
                text=base.text,
                corrected=owned_atom.target_origin == "closure",
                authority_rank=(
                    owned_atom.closure_row_index
                    if owned_atom.closure_row_index is not None
                    else owned_atom.canonical_row_index
                ),
                provenance={
                    "canonical_manifest_role": (
                        owned_atom.canonical_manifest_role
                    ),
                    "canonical_row_index": owned_atom.canonical_row_index,
                    "closure_row_index": owned_atom.closure_row_index,
                    "source_group_id": owned_atom.source_group_id,
                    "target_origin": owned_atom.target_origin,
                },
            )
            atoms.append(owned.atom)
            owned_speech.append(owned)
        else:
            atoms.append(annotation.to_atom(source.sample_rate))

    atom_ids = [atom.atom_id for atom in atoms]
    if len(atom_ids) != len(set(atom_ids)):
        raise StagingError("frame conversion produced duplicate atom IDs")
    speech_ids = {
        atom.atom_id
        for atom in atoms
        if atom.atom_class is domain.AtomClass.SPEECH
    }
    owner_ids = {owner.atom.atom_id for owner in owned_speech}
    if speech_ids != owner_ids or len(owner_ids) != len(owner_result.owners):
        raise StagingError(
            "frame conversion changed the protected owner universe"
        )

    return FrameUniverse(
        sources=tuple(sources),
        source_media=source_media,
        atoms=tuple(atoms),
        owned_speech=tuple(owned_speech),
        ownership=owner_result,
    )


def _group_by_source(
    values: Iterable[Any],
    *,
    source_uri: Callable[[Any], str] | None = None,
) -> dict[str, list[Any]]:
    extract = source_uri or (lambda value: value.source_uri)
    grouped: dict[str, list[Any]] = defaultdict(list)
    for value in values:
        grouped[extract(value)].append(value)
    return grouped


_CONTINUOUS_PLAN_ARTIFACTS = (
    "requests.jsonl",
    "source-receipts.jsonl",
    "split-receipt.json",
)
_CONTINUOUS_GUARD_KEYS = frozenset(
    {
        "continuous_source_inputs_sha256",
        "ownership_assignment_sha256",
        "ownership_receipt_sha256",
        "production_duration_histogram_sha256",
        "production_duration_target_sha256",
        "source_inventory_sha256",
    }
)


def _file_sha256(path: pathlib.Path, *, label: str) -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as source:
            while chunk := source.read(1024 * 1024):
                digest.update(chunk)
    except OSError as error:
        raise StagingError(f"cannot hash {label}: {path}") from error
    return digest.hexdigest()


def _continuous_plan_guards(
    *,
    universe: FrameUniverse,
    production_histogram_ms: Mapping[int | str, int],
    ownership_receipt: pathlib.Path,
    source_inventory_receipt: pathlib.Path,
    production_duration_target: pathlib.Path,
) -> dict[str, str]:
    ownership_record = _load_object(
        ownership_receipt,
        label="ownership receipt",
    )
    if ownership_record != universe.ownership.receipt:
        raise StagingError(
            "ownership receipt bytes do not describe current ownership"
        )
    assignment = universe.ownership.receipt.get("owner_assignment_sha256")
    if not isinstance(assignment, str) or len(assignment) != 64:
        raise StagingError("current ownership assignment digest is invalid")
    return {
        "ownership_assignment_sha256": assignment,
        "ownership_receipt_sha256": _file_sha256(
            ownership_receipt,
            label="ownership receipt",
        ),
        "production_duration_histogram_sha256": domain.stable_digest(
            dict(production_histogram_ms)
        ),
        "production_duration_target_sha256": _file_sha256(
            production_duration_target,
            label="production duration target",
        ),
        "source_inventory_sha256": _file_sha256(
            source_inventory_receipt,
            label="source inventory receipt",
        ),
    }


def _read_canonical_json(
    path: pathlib.Path,
    *,
    label: str,
) -> tuple[dict[str, object], bytes]:
    try:
        payload = path.read_bytes()
        value = json.loads(payload)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise StagingError(f"cannot read {label}: {path}") from error
    if not isinstance(value, dict):
        raise StagingError(f"{label} must be a JSON object")
    if artifacts.canonical_json_bytes(value) != payload:
        raise StagingError(f"{label} is not canonical JSON")
    return value, payload


def _read_canonical_jsonl(
    path: pathlib.Path,
    *,
    label: str,
) -> tuple[tuple[dict[str, object], ...], bytes]:
    try:
        payload = path.read_bytes()
        lines = payload.splitlines(keepends=True)
    except OSError as error:
        raise StagingError(f"cannot read {label}: {path}") from error
    if not lines:
        raise StagingError(f"{label} cannot be empty")
    rows: list[dict[str, object]] = []
    for line_number, line in enumerate(lines, start=1):
        try:
            value = json.loads(line)
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise StagingError(
                f"{label}:{line_number}: invalid JSON"
            ) from error
        if not isinstance(value, dict):
            raise StagingError(f"{label}:{line_number}: row must be an object")
        if artifacts.canonical_json_bytes(value) != line:
            raise StagingError(
                f"{label}:{line_number}: row is not canonical JSON"
            )
        rows.append(value)
    return tuple(rows), payload


def _fraction_record(
    value: object,
    *,
    label: str,
) -> fractions.Fraction:
    if not isinstance(value, Mapping):
        raise StagingError(f"{label} must be an exact fraction record")
    numerator = value.get("numerator")
    denominator = value.get("denominator")
    if (
        isinstance(numerator, bool)
        or not isinstance(numerator, int)
        or isinstance(denominator, bool)
        or not isinstance(denominator, int)
        or denominator <= 0
    ):
        raise StagingError(f"{label} has invalid numerator/denominator")
    result = fractions.Fraction(numerator, denominator)
    if result.numerator != numerator or result.denominator != denominator:
        raise StagingError(f"{label} fraction is not reduced")
    return result


def _request_from_plan_row(
    row: Mapping[str, object],
    *,
    expected_split: domain.Split,
) -> domain.Request:
    required = {
        "duration_frames",
        "duration_ms_exact",
        "end_frame",
        "family",
        "owner_ids",
        "request_id",
        "sample_rate",
        "source_uri",
        "split",
        "start_frame",
        "text",
    }
    if set(row) != required:
        raise StagingError("persisted request row has unsupported fields")
    raw_owner_ids = row["owner_ids"]
    if not isinstance(raw_owner_ids, list) or not all(
        isinstance(owner_id, str) for owner_id in raw_owner_ids
    ):
        raise StagingError("persisted request owner_ids are invalid")
    try:
        request = domain.Request(
            request_id=str(row["request_id"]),
            family=str(row["family"]),
            split=domain.Split(str(row["split"])),
            source_uri=str(row["source_uri"]),
            start_frame=int(row["start_frame"]),
            end_frame=int(row["end_frame"]),
            sample_rate=int(row["sample_rate"]),
            owner_ids=tuple(raw_owner_ids),
            text=str(row["text"]),
        )
    except (TypeError, ValueError) as error:
        raise StagingError("persisted request is invalid") from error
    if request.split is not expected_split:
        raise StagingError("persisted request belongs to the wrong split")
    frame_count = request.end_frame - request.start_frame
    if (
        isinstance(row["duration_frames"], bool)
        or row["duration_frames"] != frame_count
    ):
        raise StagingError("persisted request duration_frames drifted")
    expected_ms = fractions.Fraction(
        frame_count * 1_000,
        request.sample_rate,
    )
    if (
        _fraction_record(
            row["duration_ms_exact"],
            label="persisted request duration_ms_exact",
        )
        != expected_ms
    ):
        raise StagingError("persisted request duration milliseconds drifted")
    return request


def _subtract_interval(
    interval: tuple[int, int],
    blockers: Sequence[tuple[int, int]],
) -> tuple[tuple[int, int], ...]:
    remaining = [interval]
    for blocker_start, blocker_end in sorted(blockers):
        updated: list[tuple[int, int]] = []
        for start, end in remaining:
            if blocker_end <= start or end <= blocker_start:
                updated.append((start, end))
                continue
            if start < blocker_start:
                updated.append((start, blocker_start))
            if blocker_end < end:
                updated.append((blocker_end, end))
        remaining = updated
    return tuple(remaining)


def _gate_reused_source_plan(
    *,
    source: continuous.ContinuousSourceInput,
    requests: Sequence[domain.Request],
    receipt: Mapping[str, object],
    expected_split: domain.Split,
) -> None:
    source_record = receipt.get("source")
    hard = receipt.get("hard_invariants")
    if not isinstance(source_record, Mapping) or not isinstance(hard, Mapping):
        raise StagingError("persisted source receipt is incomplete")
    expected_source = {
        "frame_count": source.media.frame_count,
        "generation": source.media.generation,
        "sample_rate": source.media.sample_rate,
        "split": expected_split.value,
        "uri": source.media.uri,
    }
    if dict(source_record) != expected_source:
        raise StagingError(
            f"persisted source receipt guard drifted: {source.media.uri}"
        )
    required_true = {
        "every_owner_exactly_once",
        "pii_only_frames_in_requests",
        "requests_are_continuous",
        "requests_are_frame_disjoint",
        "speech_is_never_cut",
        "standalone_targetless_requests",
    }
    if (
        any(
            key not in hard
            for key in (
                *required_true,
                "speech_owner_count_input",
                "speech_owner_count_output",
            )
        )
        or hard["every_owner_exactly_once"] is not True
        or hard["requests_are_continuous"] is not True
        or hard["requests_are_frame_disjoint"] is not True
        or hard["speech_is_never_cut"] is not True
        or hard["pii_only_frames_in_requests"] != 0
        or hard["standalone_targetless_requests"] != 0
    ):
        raise StagingError("persisted source hard-invariant receipt failed")

    owners = {owner.atom.atom_id: owner for owner in source.speeches}
    emitted: list[str] = []
    ordered_requests = sorted(
        requests,
        key=lambda request: (
            request.start_frame,
            request.end_frame,
            request.request_id,
        ),
    )
    previous_end = -1
    speech_intervals = [
        (owner.atom.start_frame, owner.atom.end_frame)
        for owner in source.speeches
    ]
    effective_pii = tuple(
        residual
        for atom in source.pii_atoms
        for residual in _subtract_interval(
            (atom.start_frame, atom.end_frame),
            speech_intervals,
        )
    )
    for request in ordered_requests:
        if (
            request.source_uri != source.media.uri
            or request.family != "bcfy_feeds"
            or request.split is not expected_split
            or request.sample_rate != source.media.sample_rate
            or request.end_frame > source.media.frame_count
            or request.start_frame < previous_end
        ):
            raise StagingError(
                f"persisted request geometry drifted: {request.request_id}"
            )
        previous_end = request.end_frame
        component_owners: list[StagedOwnedSpeech] = []
        for owner_id in request.owner_ids:
            try:
                owner = owners[owner_id]
            except KeyError:
                raise StagingError(
                    f"persisted request references unknown owner: {owner_id}"
                ) from None
            if not (
                request.start_frame <= owner.atom.start_frame
                and owner.atom.end_frame <= request.end_frame
            ):
                raise StagingError(f"persisted request cuts owner: {owner_id}")
            component_owners.append(owner)
            emitted.append(owner_id)
        expected_text = transcripts.reconcile_component(component_owners).text
        if request.text != expected_text:
            raise StagingError(
                f"persisted request target drifted: {request.request_id}"
            )
        if any(
            request.start_frame < pii_end and pii_start < request.end_frame
            for pii_start, pii_end in effective_pii
        ):
            raise StagingError(
                f"persisted request includes PII-only frames: "
                f"{request.request_id}"
            )
    if (
        len(emitted) != len(set(emitted))
        or set(emitted) != set(owners)
        or hard["speech_owner_count_input"] != len(owners)
        or hard["speech_owner_count_output"] != len(emitted)
    ):
        raise StagingError(
            f"persisted source does not conserve owners: {source.media.uri}"
        )


def load_guarded_continuous_plan(
    plan_directory: pathlib.Path,
    *,
    split: domain.Split,
    sources: Sequence[continuous.ContinuousSourceInput],
    expected_guards: Mapping[str, str],
    production_histogram_ms: Mapping[int | str, int],
) -> continuous.ContinuousSplitPlan:
    """Load one immutable persisted split plan or fail without recomputing."""
    directory = pathlib.Path(plan_directory)
    success, _ = _read_canonical_json(
        directory / "_SUCCESS.json",
        label=f"{split.value} continuous-plan success marker",
    )
    if (
        success.get("schema_version") != 1
        or success.get("kind") != "continuous_planning_only_success"
        or success.get("algorithm")
        != "continuous_bcfy_feeds_global_split_ecdf_v1"
        or success.get("split") != split.value
    ):
        raise StagingError(
            f"{split.value} continuous-plan success marker is unsupported"
        )
    artifacts_record = success.get("artifacts")
    guards = success.get("frozen_input_guards")
    if (
        not isinstance(artifacts_record, Mapping)
        or set(artifacts_record) != set(_CONTINUOUS_PLAN_ARTIFACTS)
        or not isinstance(guards, Mapping)
        or set(guards) != _CONTINUOUS_GUARD_KEYS
        or dict(guards) != dict(expected_guards)
    ):
        raise StagingError(f"{split.value} continuous-plan guards drifted")

    request_rows: tuple[dict[str, object], ...] = ()
    source_rows: tuple[dict[str, object], ...] = ()
    split_receipt: dict[str, object] | None = None
    for artifact_name in _CONTINUOUS_PLAN_ARTIFACTS:
        path = directory / artifact_name
        if artifact_name.endswith(".jsonl"):
            rows, payload = _read_canonical_jsonl(
                path,
                label=f"{split.value} {artifact_name}",
            )
            if artifact_name == "requests.jsonl":
                request_rows = rows
            else:
                source_rows = rows
        else:
            value, payload = _read_canonical_json(
                path,
                label=f"{split.value} {artifact_name}",
            )
            split_receipt = value
        expected_hash = artifacts_record[artifact_name]
        if (
            not isinstance(expected_hash, str)
            or hashlib.sha256(payload).hexdigest() != expected_hash
        ):
            raise StagingError(
                f"{split.value} continuous-plan artifact hash drifted: "
                f"{artifact_name}"
            )
    if split_receipt is None:
        raise StagingError(f"{split.value} split receipt is missing")
    if split_receipt.get("frozen_input_guards") != dict(expected_guards):
        raise StagingError(f"{split.value} split receipt guards drifted")
    expected_histogram_sha = domain.stable_digest(dict(production_histogram_ms))
    production_record = split_receipt.get("production_histogram")
    split_hard = split_receipt.get("hard_invariants")
    if (
        split_receipt.get("schema_version") != 1
        or split_receipt.get("algorithm")
        != "continuous_bcfy_feeds_global_split_ecdf_v1"
        or split_receipt.get("split") != split.value
        or not isinstance(production_record, Mapping)
        or production_record.get("sha256") != expected_histogram_sha
        or not isinstance(split_hard, Mapping)
        or any(value is not True for value in split_hard.values())
    ):
        raise StagingError(f"{split.value} split receipt invariants drifted")

    requests = tuple(
        _request_from_plan_row(row, expected_split=split)
        for row in request_rows
    )
    expected_sources = tuple(
        sorted(
            sources,
            key=lambda source: (
                source.media.uri,
                source.media.generation,
            ),
        )
    )
    expected_uris = tuple(source.media.uri for source in expected_sources)
    source_receipts: dict[str, dict[str, object]] = {}
    for receipt in source_rows:
        source_record = receipt.get("source")
        if not isinstance(source_record, Mapping):
            raise StagingError("persisted source receipt lacks source guard")
        source_uri = source_record.get("uri")
        if not isinstance(source_uri, str) or source_uri in source_receipts:
            raise StagingError(
                "persisted source receipts contain invalid or duplicate URI"
            )
        source_receipts[source_uri] = receipt
    if tuple(source_receipts) != expected_uris:
        raise StagingError(
            f"{split.value} persisted source order/universe drifted"
        )
    requests_by_source = _group_by_source(requests)
    if set(requests_by_source) != set(expected_uris):
        raise StagingError(
            f"{split.value} persisted request source universe drifted"
        )
    if (
        tuple(
            request
            for source_uri in expected_uris
            for request in requests_by_source[source_uri]
        )
        != requests
    ):
        raise StagingError(f"{split.value} persisted request ordering drifted")
    source_selections = split_receipt.get("source_selections")
    if not isinstance(source_selections, list):
        raise StagingError("persisted split receipt lacks source selections")
    selection_by_uri: dict[str, Mapping[str, object]] = {}
    for selection in source_selections:
        if not isinstance(selection, Mapping):
            raise StagingError("persisted source selection is not an object")
        source_uri = selection.get("source_uri")
        if not isinstance(source_uri, str) or source_uri in selection_by_uri:
            raise StagingError("persisted source selection URI is invalid")
        selection_by_uri[source_uri] = selection
    if tuple(selection_by_uri) != expected_uris:
        raise StagingError("persisted source selections drifted")

    source_plans: list[continuous.ContinuousPlan] = []
    for source in expected_sources:
        source_uri = source.media.uri
        source_requests = tuple(requests_by_source.get(source_uri, ()))
        source_receipt = source_receipts[source_uri]
        _gate_reused_source_plan(
            source=source,
            requests=source_requests,
            receipt=source_receipt,
            expected_split=split,
        )
        selection = selection_by_uri[source_uri]
        if selection.get(
            "generation"
        ) != source.media.generation or selection.get(
            "source_receipt_sha256"
        ) != domain.stable_digest(source_receipt):
            raise StagingError(
                f"persisted source receipt digest drifted: {source_uri}"
            )
        source_plans.append(
            continuous.ContinuousPlan(
                requests=source_requests,
                receipt=source_receipt,
            )
        )
    expected_owner_count = sum(
        len(source.speeches) for source in expected_sources
    )
    metrics = success.get("metrics")
    if (
        not isinstance(metrics, Mapping)
        or split_receipt.get("source_count") != len(expected_sources)
        or split_receipt.get("speech_owner_count") != expected_owner_count
        or split_receipt.get("request_count") != len(requests)
        or metrics.get("source_count") != len(expected_sources)
        or metrics.get("speech_owner_count") != expected_owner_count
        or metrics.get("request_count") != len(requests)
    ):
        raise StagingError(f"{split.value} persisted plan counts drifted")
    plan = continuous.ContinuousSplitPlan(
        requests=requests,
        source_plans=tuple(source_plans),
        receipt=split_receipt,
    )
    try:
        continuous.verify_persisted_continuous_split(
            plan,
            sources=expected_sources,
            production_histogram_ms=production_histogram_ms,
        )
    except continuous.ContinuousPlanningError as error:
        raise StagingError(
            f"{split.value} persisted plan failed independent receipt binding"
        ) from error
    return plan


def load_guarded_continuous_plans(
    plan_root: pathlib.Path,
    *,
    universe: FrameUniverse,
    sources_by_split: Mapping[
        domain.Split, Sequence[continuous.ContinuousSourceInput]
    ],
    production_histogram_ms: Mapping[int | str, int],
    ownership_receipt: pathlib.Path,
    source_inventory_receipt: pathlib.Path,
    production_duration_target: pathlib.Path,
) -> dict[domain.Split, continuous.ContinuousSplitPlan]:
    """Load both split plans under one current frozen-input guard set."""
    guards = _continuous_plan_guards(
        universe=universe,
        production_histogram_ms=production_histogram_ms,
        ownership_receipt=ownership_receipt,
        source_inventory_receipt=source_inventory_receipt,
        production_duration_target=production_duration_target,
    )
    result: dict[domain.Split, continuous.ContinuousSplitPlan] = {}
    for split in (domain.Split.TRAIN, domain.Split.EVAL):
        split_sources = tuple(sources_by_split[split])
        split_guards = {
            **guards,
            "continuous_source_inputs_sha256": (
                continuous.continuous_source_inputs_sha256(
                    split_sources,
                    split=split,
                )
            ),
        }
        result[split] = load_guarded_continuous_plan(
            pathlib.Path(plan_root) / split.value,
            split=split,
            sources=split_sources,
            expected_guards=split_guards,
            production_histogram_ms=production_histogram_ms,
        )
    return result


def _continuous_inputs_by_split(
    universe: FrameUniverse,
) -> dict[domain.Split, tuple[continuous.ContinuousSourceInput, ...]]:
    """Build the exact per-split inputs shared by planning and plan reuse."""

    atoms_by_source = _group_by_source(universe.atoms)
    owners_by_source = _group_by_source(
        universe.owned_speech,
        source_uri=lambda owner: owner.atom.source_uri,
    )
    values: dict[domain.Split, list[continuous.ContinuousSourceInput]] = {
        domain.Split.TRAIN: [],
        domain.Split.EVAL: [],
    }
    continuous_source_uris = {
        owner.atom.source_uri
        for owner in universe.owned_speech
        if owner.atom.family == "bcfy_feeds"
    }
    for source_uri in sorted(continuous_source_uris):
        source_owners = tuple(owners_by_source[source_uri])
        splits = {owner.split for owner in source_owners}
        if len(splits) != 1:
            raise StagingError(
                f"continuous source has mixed ownership: {source_uri}"
            )
        split = next(iter(splits))
        source_atoms = atoms_by_source[source_uri]
        values[split].append(
            continuous.ContinuousSourceInput(
                media=universe.source_media[source_uri],
                speeches=source_owners,
                pii_atoms=tuple(
                    atom
                    for atom in source_atoms
                    if atom.atom_class is domain.AtomClass.PII
                ),
                targetless_atoms=tuple(
                    atom
                    for atom in source_atoms
                    if atom.atom_class is domain.AtomClass.TARGETLESS
                ),
            )
        )
    return {
        split: tuple(values[split])
        for split in (domain.Split.TRAIN, domain.Split.EVAL)
    }


def plan_final_requests(
    universe: FrameUniverse,
    production_histogram_ms: Mapping[int | str, int],
    *,
    reused_continuous_plans: Mapping[
        domain.Split, continuous.ContinuousSplitPlan
    ]
    | None = None,
) -> PlanningResult:
    """Run each frozen constructor and conserve every owner exactly once."""

    atoms_by_source = _group_by_source(universe.atoms)
    owners_by_source = _group_by_source(
        universe.owned_speech,
        source_uri=lambda owner: owner.atom.source_uri,
    )

    presegmented_source_uris = {
        owner.atom.source_uri
        for owner in universe.owned_speech
        if owner.atom.family in presegmented.PRESEGMENTED_FAMILIES
    }
    presegmented_result = presegmented.reconstruct_presegmented(
        atoms=tuple(
            atom
            for source_uri in sorted(presegmented_source_uris)
            for atom in atoms_by_source[source_uri]
        ),
        owned_speech=tuple(
            owner
            for source_uri in sorted(presegmented_source_uris)
            for owner in owners_by_source[source_uri]
        ),
        source_media={
            source_uri: universe.source_media[source_uri]
            for source_uri in sorted(presegmented_source_uris)
        },
    )

    continuous_inputs = _continuous_inputs_by_split(universe)

    if reused_continuous_plans is None:
        # These calls intentionally have no shared optimizer state.
        continuous_train = continuous.plan_continuous_split(
            sources=tuple(continuous_inputs[domain.Split.TRAIN]),
            production_histogram_ms=production_histogram_ms,
        )
        continuous_eval = continuous.plan_continuous_split(
            sources=tuple(continuous_inputs[domain.Split.EVAL]),
            production_histogram_ms=production_histogram_ms,
        )
    else:
        if set(reused_continuous_plans) != {
            domain.Split.TRAIN,
            domain.Split.EVAL,
        }:
            raise StagingError(
                "reused continuous plans must contain exactly train and eval"
            )
        continuous_train = reused_continuous_plans[domain.Split.TRAIN]
        continuous_eval = reused_continuous_plans[domain.Split.EVAL]

    requests = tuple(
        sorted(
            (
                *presegmented_result.requests,
                *continuous_train.requests,
                *continuous_eval.requests,
            ),
            key=lambda request: (
                request.split.value,
                request.family,
                request.source_uri,
                request.start_frame,
                request.end_frame,
                request.request_id,
            ),
        )
    )
    request_ids = [request.request_id for request in requests]
    if len(request_ids) != len(set(request_ids)):
        raise StagingError("constructors produced duplicate request IDs")
    emitted_owner_ids = [
        owner_id for request in requests for owner_id in request.owner_ids
    ]
    expected_owner_ids = [owner.atom.atom_id for owner in universe.owned_speech]
    if len(emitted_owner_ids) != len(set(emitted_owner_ids)) or set(
        emitted_owner_ids
    ) != set(expected_owner_ids):
        raise StagingError(
            "final request universe does not conserve every owner exactly once"
        )
    return PlanningResult(
        requests=requests,
        presegmented=presegmented_result,
        continuous_train=continuous_train,
        continuous_eval=continuous_eval,
    )


def materialize_final_requests(
    requests: Sequence[domain.Request],
    sources: Sequence[media.SourceInventory],
    output_root: pathlib.Path,
    *,
    toolchain: media.EncoderToolchain,
    max_workers: int = 1,
) -> tuple[manifests.MaterializationBinding, ...]:
    """Materialize only the final continuous request intervals."""

    if (
        isinstance(max_workers, bool)
        or not isinstance(max_workers, int)
        or max_workers <= 0
    ):
        raise StagingError("max_workers must be a positive integer")
    source_by_uri = media.source_inventory_by_uri(sources)
    ordered = tuple(requests)
    if not ordered:
        raise StagingError("cannot materialize an empty request universe")

    def materialize(
        request: domain.Request,
    ) -> manifests.MaterializationBinding:
        try:
            source = source_by_uri[request.source_uri]
        except KeyError:
            raise StagingError(
                f"request source is absent from frozen inventory: "
                f"{request.source_uri}"
            ) from None
        output_path = (
            pathlib.Path(output_root)
            / "audio"
            / request.split.value
            / request.family
            / f"{request.request_id}.flac"
        )
        try:
            encoder_profile = _ENCODER_PROFILE_BY_FAMILY[request.family]
        except KeyError:
            raise StagingError(
                f"request family has no frozen encoder profile: {request.family}"
            ) from None
        receipt = media.materialize_slice(
            source,
            request.start_frame,
            request.end_frame,
            output_path,
            encoder_profile=encoder_profile,
            toolchain=toolchain,
        )
        return manifests.MaterializationBinding(
            request_id=request.request_id,
            receipt=receipt,
        )

    if max_workers == 1:
        return tuple(materialize(request) for request in ordered)
    with concurrent.futures.ThreadPoolExecutor(
        max_workers=max_workers
    ) as executor:
        return tuple(executor.map(materialize, ordered))


def _resolve_binding_groups(
    index: recording_groups.SourceIndex | eval_views.BindingGroupResolver,
    binding: recording_groups.RowBinding,
) -> frozenset[str]:
    if isinstance(index, recording_groups.SourceIndex):
        result = recording_groups.group_ids_for_binding(binding, index)
    elif isinstance(index, eval_views.BindingGroupResolver):
        result = frozenset(index.group_ids_for_binding(binding))
    else:
        raise TypeError(
            "masked index must be SourceIndex or BindingGroupResolver"
        )
    if not result or any(
        not isinstance(value, str) or not value.strip() for value in result
    ):
        raise StagingError("masked row resolved to no valid physical group")
    return frozenset(result)


def _masked_verifier_projection(
    filtered: eval_views.MaskedEvalFilterResult,
    index: recording_groups.SourceIndex | eval_views.BindingGroupResolver,
    *,
    current_physical_groups: Mapping[str, str],
    train_group_ids: frozenset[str],
) -> tuple[tuple[dict[str, object], ...], dict[str, str]]:
    indexed_sources: dict[tuple[str, str], tuple[str, str]] = {}
    if isinstance(index, recording_groups.SourceIndex):
        raw_sources = index.record.get("sources")
        if not isinstance(raw_sources, list):
            raise StagingError("frozen recording-group index lacks sources")
        for raw_source in raw_sources:
            if not isinstance(raw_source, Mapping):
                raise StagingError(
                    "frozen recording-group source is not an object"
                )
            raw_uri = raw_source.get("source_uri")
            raw_group = raw_source.get("split_group_id")
            raw_locators = raw_source.get("locators")
            if (
                not isinstance(raw_uri, str)
                or not isinstance(raw_group, str)
                or not isinstance(raw_locators, list)
            ):
                raise StagingError(
                    "frozen recording-group source is incomplete"
                )
            for locator in raw_locators:
                if (
                    isinstance(locator, Mapping)
                    and locator.get("kind") == "masked_example_id"
                    and isinstance(locator.get("dataset_family"), str)
                    and isinstance(locator.get("value"), str)
                ):
                    key = (
                        str(locator["dataset_family"]),
                        str(locator["value"]),
                    )
                    value = (raw_uri, raw_group)
                    previous = indexed_sources.setdefault(key, value)
                    if previous != value:
                        raise StagingError(
                            f"masked locator maps to multiple sources: {key}"
                        )

    rows: list[dict[str, object]] = []
    physical_groups: dict[str, str] = {}
    row_index = 0
    for family_result in filtered.families:
        for row in family_result.kept_rows:
            example_id, segment_id = common_manifest.canonical_row_identity(
                dict(row)
            )
            binding = recording_groups.locator_row_binding(
                split="eval",
                row_index=row_index,
                row_identity=(example_id, segment_id),
                locator=recording_groups.SourceLocator(
                    "masked_example_id",
                    example_id,
                    family_result.family,
                ),
            )
            group_ids = _resolve_binding_groups(index, binding)
            if len(group_ids) != 1:
                raise StagingError(
                    "retained masked row must resolve to one physical group: "
                    f"{family_result.family}/{example_id}/{segment_id}"
                )
            group_id = next(iter(group_ids))
            indexed = indexed_sources.get((family_result.family, example_id))
            if indexed is not None:
                source_uri, indexed_group = indexed
                if indexed_group != group_id:
                    raise StagingError(
                        "masked locator and binding resolve to different groups"
                    )
            else:
                # Small injected test resolvers do not expose a source index.
                # They must name a source already present in current ownership.
                raw_uri = row.get("source_uri", row.get("audio_filepath"))
                if (
                    not isinstance(raw_uri, str)
                    or raw_uri not in current_physical_groups
                ):
                    raise StagingError(
                        "masked test resolver cannot identify original source"
                    )
                source_uri = raw_uri
            try:
                current_group = current_physical_groups[source_uri]
            except KeyError:
                raise StagingError(
                    "retained masked row source is outside current ownership: "
                    f"{source_uri}"
                ) from None
            if current_group in train_group_ids:
                raise StagingError(
                    f"retained masked row intersects final training: "
                    f"{family_result.family}/{example_id}/{segment_id}"
                )
            prior_group = physical_groups.setdefault(source_uri, current_group)
            if prior_group != current_group:
                raise StagingError(
                    f"masked audio URI resolves to multiple groups: {source_uri}"
                )
            rows.append(
                {
                    "family": family_result.family,
                    "group_id": current_group,
                    "payload_sha256": artifacts.canonical_json_sha256(
                        dict(row)
                    ),
                    "row_id": (
                        f"{family_result.family}:{example_id}:{segment_id}"
                    ),
                    "source_uri": source_uri,
                    "split": "eval",
                }
            )
            row_index += 1
    return tuple(rows), physical_groups


def _masked_family(row: Mapping[str, object]) -> str:
    dataset = row.get("dataset")
    candidate = (
        dataset.get("family")
        if isinstance(dataset, Mapping)
        else row.get("dataset_family") or row.get("dataset_name")
    )
    if candidate == "fire_notification":
        candidate = "fire_notifications"
    if candidate not in eval_views.MASKED_DATASET_FAMILIES:
        raise StagingError(
            f"retained masked row has invalid family: {candidate}"
        )
    return str(candidate)


def _validate_masked_selection_proof(
    filtered: eval_views.MaskedEvalFilterResult,
    retained_proof_jsonl: bytes,
    report: Mapping[str, object],
    index: recording_groups.SourceIndex | eval_views.BindingGroupResolver,
) -> tuple[str, str]:
    observed_sha256 = hashlib.sha256(retained_proof_jsonl).hexdigest()
    if (
        observed_sha256 != MASKED_RETAINED_SHA256
        or report.get("retained_manifest_sha256") != observed_sha256
    ):
        raise StagingError("frozen retained masked manifest SHA-256 drifted")
    if (
        report.get("kept_row_count") != MASKED_RETAINED_COUNT
        or report.get("index_sha256") != MASKED_INDEX_SHA256
        or index.sha256 != MASKED_INDEX_SHA256
    ):
        raise StagingError(
            "frozen masked count or recording-group index drifted"
        )
    try:
        decoded = retained_proof_jsonl.decode("utf-8")
    except UnicodeDecodeError as error:
        raise StagingError(
            "frozen retained masked manifest is not UTF-8"
        ) from error
    proof_rows: list[dict[str, object]] = []
    for line_number, line in enumerate(decoded.splitlines(), start=1):
        if not line:
            raise StagingError(
                f"frozen retained masked row {line_number} is blank"
            )
        try:
            row = json.loads(line)
        except json.JSONDecodeError as error:
            raise StagingError(
                f"frozen retained masked row {line_number} is invalid"
            ) from error
        if not isinstance(row, dict):
            raise StagingError(
                f"frozen retained masked row {line_number} is not an object"
            )
        proof_rows.append(row)
    if len(proof_rows) != MASKED_RETAINED_COUNT:
        raise StagingError("frozen retained masked row count drifted")
    canonical = _jsonl_bytes(proof_rows)
    if canonical != retained_proof_jsonl:
        raise StagingError(
            "frozen retained masked manifest is not canonical byte-for-byte"
        )

    kept_counts = report.get("kept_counts")
    if not isinstance(kept_counts, Mapping):
        raise StagingError("frozen masked report is incomplete")
    proof_identities = tuple(
        (
            _masked_family(row),
            *common_manifest.canonical_row_identity(row),
        )
        for row in proof_rows
    )
    selected_identities = tuple(
        (
            family.family,
            *common_manifest.canonical_row_identity(dict(row)),
        )
        for family in filtered.families
        for row in family.kept_rows
    )
    if selected_identities != proof_identities:
        raise StagingError(
            "new final-train masked selection differs from frozen proof"
        )
    selected_rows = tuple(
        dict(row) for family in filtered.families for row in family.kept_rows
    )
    projected_proof_rows: list[dict[str, object]] = []
    for row_number, (proof_row, selected_row) in enumerate(
        zip(proof_rows, selected_rows, strict=True),
        start=1,
    ):
        projected = copy.deepcopy(proof_row)
        dataset = projected.pop("dataset", None)
        source_audio = projected.pop("source_audio", None)
        if not isinstance(dataset, Mapping) or not isinstance(
            source_audio, Mapping
        ):
            raise StagingError(
                "frozen masked proof row lacks its two lineage-only fields: "
                f"{row_number}"
            )
        if projected != selected_row:
            raise StagingError(
                "raw retained masked payload differs from the frozen proof "
                f"after removing lineage-only fields: {row_number}"
            )
        projected_proof_rows.append(projected)
    projected_payload = _jsonl_bytes(projected_proof_rows)
    selected_payload = _jsonl_bytes(selected_rows)
    if selected_payload != projected_payload:
        raise StagingError(
            "raw retained masked payload serialization differs from its "
            "frozen proof projection"
        )
    if int(filtered.receipt["kept_count"]) != MASKED_RETAINED_COUNT:
        raise StagingError("new masked selection does not contain 2,043 rows")
    for family in filtered.families:
        if kept_counts.get(family.family) != len(family.kept_rows):
            raise StagingError(
                f"new masked family count differs from proof: {family.family}"
            )
    return observed_sha256, hashlib.sha256(selected_payload).hexdigest()


def build_evaluation_artifacts(
    manifest_artifacts: manifests.ManifestArtifacts,
    *,
    masked_rows_by_family: Mapping[str, Sequence[Mapping[str, object]]],
    masked_retained_proof_jsonl: bytes,
    masked_filter_report: Mapping[str, object],
    masked_group_index: recording_groups.SourceIndex
    | eval_views.BindingGroupResolver,
    current_physical_groups: Mapping[str, str],
    train_group_ids: frozenset[str],
) -> EvaluationArtifacts:
    """Build the primary, predicted-context schedule, and masked lanes."""

    primary = eval_views.prepare_primary_eval(
        manifest_artifacts.canonical_eval_rows
    )
    provider_validation = eval_views.prepare_provider_validation(
        primary.rows,
        train_count=len(manifest_artifacts.canonical_train_rows),
        input_lock_digest=str(primary.receipt["eval_sha256"]),
    )
    if primary.eval_jsonl != manifest_artifacts.primary_eval_jsonl:
        raise StagingError("primary eval serialization changed")
    if (
        provider_validation.rows != manifest_artifacts.provider_validation_rows
        or provider_validation.jsonl
        != manifest_artifacts.canonical_validation_jsonl
        or provider_validation.receipt
        != manifest_artifacts.provider_validation_receipt
    ):
        raise StagingError(
            "provider validation is not the frozen primary-eval projection"
        )
    prior_context = eval_views.build_prior_context_schedule(primary.rows)
    masked = eval_views.refilter_masked_eval(
        train_rows=manifest_artifacts.canonical_train_rows,
        masked_rows_by_family=masked_rows_by_family,
        index=masked_group_index,
    )
    proof_sha256, retained_payload_sha256 = _validate_masked_selection_proof(
        masked,
        masked_retained_proof_jsonl,
        masked_filter_report,
        masked_group_index,
    )
    scope_receipt = eval_views.final_eval_scope_receipt(
        primary=primary,
        provider_validation=provider_validation,
        prior_context=prior_context,
        masked=masked,
    )
    verifier_rows, masked_groups = _masked_verifier_projection(
        masked,
        masked_group_index,
        current_physical_groups=current_physical_groups,
        train_group_ids=train_group_ids,
    )
    return EvaluationArtifacts(
        primary=primary,
        provider_validation=provider_validation,
        prior_context=prior_context,
        masked=masked,
        masked_selection_proof_sha256=proof_sha256,
        masked_retained_payload_sha256=retained_payload_sha256,
        scope_receipt=scope_receipt,
        masked_verifier_rows=verifier_rows,
        masked_physical_groups=masked_groups,
    )


def _source_records(
    sources: Sequence[media.SourceInventory],
) -> list[dict[str, object]]:
    return [
        {
            "channels": source.channels,
            "frame_count": source.frame_count,
            "generation": source.generation,
            "sample_rate": source.sample_rate,
            "sha256": source.sha256,
            "size_bytes": source.size_bytes,
            "subtype": source.subtype,
            "uri": source.uri,
        }
        for source in sources
    ]


def _owner_records(universe: FrameUniverse) -> list[dict[str, object]]:
    records: list[dict[str, object]] = []
    for owner in universe.owned_speech:
        record: dict[str, object] = {
            "constituent_ids": [owner.atom.atom_id],
            "end_frame": owner.atom.end_frame,
            "family": owner.atom.family,
            "owner_id": owner.atom.atom_id,
            "source_uri": owner.atom.source_uri,
            "split": owner.split.value,
            "start_frame": owner.atom.start_frame,
            "text": owner.text,
        }
        if owner.atom.family == "echo":
            record["archive_source_id"] = owner.atom.source_uri
        elif owner.atom.family in {"bcfy_calls", "fire_notifications"}:
            record["provider_item_id"] = owner.atom.source_uri
        records.append(record)
    return records


def _request_records(
    requests: Sequence[domain.Request],
) -> list[dict[str, object]]:
    result: list[dict[str, object]] = []
    for request in requests:
        record: dict[str, object] = {
            "end_frame": request.end_frame,
            "family": request.family,
            "owner_ids": list(request.owner_ids),
            "request_id": request.request_id,
            "sample_rate": request.sample_rate,
            "source_uri": request.source_uri,
            "split": request.split.value,
            "start_frame": request.start_frame,
            "text": request.text,
        }
        if request.family == "echo":
            record["archive_source_id"] = request.source_uri
        elif request.family in {"bcfy_calls", "fire_notifications"}:
            record["provider_item_id"] = request.source_uri
        result.append(record)
    return result


def _materialization_records(
    bindings: Sequence[manifests.MaterializationBinding],
) -> list[dict[str, object]]:
    return [
        {
            "end_frame": binding.receipt.end_frame,
            "encoder_binary_sha256": (binding.receipt.encoder_binary_sha256),
            "encoder_cffi_artifact_sha256": (
                binding.receipt.encoder_cffi_artifact_sha256
            ),
            "encoder_command_sha256": (binding.receipt.encoder_command_sha256),
            "encoder_contract_sha256": (
                binding.receipt.encoder_contract_sha256
            ),
            "encoder_embedded_libflac_version": (
                binding.receipt.encoder_embedded_libflac_version
            ),
            "encoder_input_pcm_format": (
                binding.receipt.encoder_input_pcm_format
            ),
            "encoder_production_image_uri": (
                binding.receipt.encoder_production_image_uri
            ),
            "encoder_production_revision": (
                binding.receipt.encoder_production_revision
            ),
            "encoder_profile": binding.receipt.encoder_profile,
            "encoder_python_artifact_sha256": (
                binding.receipt.encoder_python_artifact_sha256
            ),
            "encoder_version": binding.receipt.encoder_version,
            "output_channels": binding.receipt.channels,
            "output_codec": binding.receipt.output_codec,
            "output_encoded_sha256": (binding.receipt.output_encoded_sha256),
            "output_frame_count": binding.receipt.frame_count,
            "output_encoded_size_bytes": (
                binding.receipt.output_encoded_size_bytes
            ),
            "output_format": binding.receipt.output_format,
            "output_pcm_sha256": (binding.receipt.output_decoded_pcm_sha256),
            "output_sample_rate": binding.receipt.sample_rate,
            "output_subtype": binding.receipt.subtype,
            "request_id": binding.request_id,
            "source_pcm_sha256": binding.receipt.source_pcm_sha256,
            "source_uri": binding.receipt.source_uri,
            "start_frame": binding.receipt.start_frame,
        }
        for binding in bindings
    ]


def build_verification_bundle(
    *,
    universe: FrameUniverse,
    requests: Sequence[domain.Request],
    materialization_bindings: Sequence[manifests.MaterializationBinding],
    evaluation: EvaluationArtifacts,
    construction_contract: Mapping[str, object],
) -> dict[str, object]:
    """Assemble the optimizer-independent verifier's canonical input."""

    group_by_source = universe.ownership.group_by_source_uri()
    physical_groups = {
        source_uri: group.group_id
        for source_uri, group in sorted(group_by_source.items())
    }
    for source_uri, group_id in evaluation.masked_physical_groups.items():
        existing = physical_groups.setdefault(source_uri, group_id)
        if existing != group_id:
            raise StagingError(
                f"source has conflicting physical groups: {source_uri}"
            )

    return {
        "construction_contract": copy.deepcopy(dict(construction_contract)),
        "eval_lanes": copy.deepcopy(
            dict(evaluation.scope_receipt["eval_lanes"])
        ),
        "manifest_sha256": {
            "reconstructed_primary": evaluation.primary.receipt["eval_sha256"],
            "validation": evaluation.provider_validation.receipt[
                "selected_rows_sha256"
            ],
        },
        "masked_filter_receipt": copy.deepcopy(evaluation.masked.receipt),
        "masked_row_projections": [
            copy.deepcopy(row) for row in evaluation.masked_verifier_rows
        ],
        "masked_rows_by_family": {
            family.family: [copy.deepcopy(row) for row in family.kept_rows]
            for family in evaluation.masked.families
        },
        "masked_selection_proof_sha256": (
            evaluation.masked_selection_proof_sha256
        ),
        "masked_retained_payload_sha256": (
            evaluation.masked_retained_payload_sha256
        ),
        "masked_rows": [
            copy.deepcopy(row) for row in evaluation.masked_verifier_rows
        ],
        "materialization_receipts": _materialization_records(
            materialization_bindings
        ),
        "owner_ledger": _owner_records(universe),
        "physical_groups": physical_groups,
        "pii_intervals": [
            {
                "end_frame": atom.end_frame,
                "source_uri": atom.source_uri,
                "start_frame": atom.start_frame,
            }
            for atom in universe.pii_atoms
        ],
        "raw_atoms": [
            {
                "atom_id": atom.atom_id,
                "end_frame": atom.end_frame,
                "source_uri": atom.source_uri,
                "start_frame": atom.start_frame,
            }
            for atom in universe.speech_atoms
        ],
        "primary_eval_rows": [
            copy.deepcopy(row) for row in evaluation.primary.rows
        ],
        "provider_validation_receipt": copy.deepcopy(
            evaluation.provider_validation.receipt
        ),
        "provider_validation_rows": [
            copy.deepcopy(row) for row in evaluation.provider_validation.rows
        ],
        "prior_context_schedule_rows": [
            copy.deepcopy(row)
            for row in _prior_schedule_rows(evaluation.prior_context)
        ],
        "requests": _request_records(requests),
        "source_inventory": _source_records(universe.sources),
        "validation_rows": [
            copy.deepcopy(row) for row in evaluation.provider_validation.rows
        ],
    }


def _json_value(value: object) -> object:
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return _json_value(dataclasses.asdict(value))
    if isinstance(value, pathlib.Path):
        return str(value)
    if isinstance(value, enum.Enum):
        return value.value
    if isinstance(value, decimal.Decimal):
        return str(value)
    if isinstance(value, Mapping):
        return {
            str(key): _json_value(item)
            for key, item in sorted(
                value.items(), key=lambda pair: str(pair[0])
            )
        }
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    raise TypeError(f"cannot serialize staging receipt: {type(value).__name__}")


def _jsonl_bytes(rows: Iterable[Mapping[str, object]]) -> bytes:
    return b"".join(artifacts.canonical_json_bytes(dict(row)) for row in rows)


def _prior_schedule_rows(
    schedule: eval_views.PriorContextSchedule,
) -> tuple[dict[str, object], ...]:
    return tuple(
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
        for request in schedule.requests
    )


def write_local_artifacts(
    *,
    output_root: pathlib.Path,
    universe: FrameUniverse,
    planning: PlanningResult,
    materialization_bindings: Sequence[manifests.MaterializationBinding],
    manifest_artifacts: manifests.ManifestArtifacts,
    evaluation: EvaluationArtifacts,
    verification_bundle: Mapping[str, object],
    verification_report: verify.VerificationReport,
) -> tuple[artifacts.ArtifactReceipt, ...]:
    """Write deterministic local artifacts without any external mutation."""

    files = dict(manifest_artifacts.files())
    files["receipts/eval_lanes.json"] = artifacts.canonical_json_bytes(
        evaluation.scope_receipt
    )
    files.update(
        {
            "eval/predicted_prior_context_10/schedule.jsonl": (
                _jsonl_bytes(_prior_schedule_rows(evaluation.prior_context))
            ),
            "eval/masked_v2/all.jsonl": _jsonl_bytes(
                row
                for family in evaluation.masked.families
                for row in family.kept_rows
            ),
            "receipts/constructors/presegmented.json": (
                artifacts.canonical_json_bytes(
                    _json_value(planning.presegmented.receipt)
                )
            ),
            "receipts/constructors/continuous_train.json": (
                artifacts.canonical_json_bytes(
                    _json_value(planning.continuous_train.receipt)
                )
            ),
            "receipts/constructors/continuous_eval.json": (
                artifacts.canonical_json_bytes(
                    _json_value(planning.continuous_eval.receipt)
                )
            ),
            "receipts/eval/primary.json": artifacts.canonical_json_bytes(
                evaluation.primary.receipt
            ),
            "receipts/eval/prior_context_10.json": (
                artifacts.canonical_json_bytes(evaluation.prior_context.receipt)
            ),
            "receipts/eval/masked_v2.json": artifacts.canonical_json_bytes(
                evaluation.masked.receipt
            ),
            "receipts/eval/masked_selection_proof.json": (
                artifacts.canonical_json_bytes(
                    {
                        "lineage_only_fields_excluded_from_payload": [
                            "dataset",
                            "source_audio",
                        ],
                        "normalized_selection_proof_sha256": (
                            evaluation.masked_selection_proof_sha256
                        ),
                        "raw_retained_payload_sha256": (
                            evaluation.masked_retained_payload_sha256
                        ),
                        "retained_identity_count": (MASKED_RETAINED_COUNT),
                        "schema_version": 2,
                        "raw_payload_rows_are_unchanged": True,
                    }
                )
            ),
            "receipts/eval/scope.json": artifacts.canonical_json_bytes(
                evaluation.scope_receipt
            ),
            "receipts/materialization.jsonl": _jsonl_bytes(
                {
                    "request_id": binding.request_id,
                    **dict(_json_value(binding.receipt)),
                }
                for binding in materialization_bindings
            ),
            "receipts/ownership/canonical_mapping.jsonl": _jsonl_bytes(
                universe.ownership.canonical_mapping_receipt_rows()
            ),
            "receipts/ownership/owners.jsonl": _jsonl_bytes(
                _owner_records(universe)
            ),
            "receipts/ownership/owners_source_seconds.jsonl": _jsonl_bytes(
                universe.ownership.owner_receipt_rows()
            ),
            "receipts/ownership/summary.json": artifacts.canonical_json_bytes(
                universe.ownership.receipt
            ),
            "receipts/ownership/source_groups.jsonl": _jsonl_bytes(
                universe.ownership.source_group_receipt_rows()
            ),
            "receipts/source_inventory.jsonl": _jsonl_bytes(
                dict(_json_value(source)) for source in universe.sources
            ),
            "receipts/transcripts/presegmented.jsonl": _jsonl_bytes(
                {
                    "request_id": binding.request_id,
                    "receipt": _json_value(binding.receipt),
                }
                for binding in planning.presegmented.transcript_receipts
            ),
            "receipts/constructors/continuous_train_sources.jsonl": (
                _jsonl_bytes(
                    plan.receipt
                    for plan in planning.continuous_train.source_plans
                )
            ),
            "receipts/constructors/continuous_eval_sources.jsonl": (
                _jsonl_bytes(
                    plan.receipt
                    for plan in planning.continuous_eval.source_plans
                )
            ),
            "receipts/requests.jsonl": _jsonl_bytes(
                _request_records(planning.requests)
            ),
            "verification/bundle.json": artifacts.canonical_json_bytes(
                verification_bundle
            ),
            "verification/report.json": artifacts.canonical_json_bytes(
                verification_report.as_dict()
            ),
        }
    )
    for family in evaluation.masked.families:
        files[f"eval/masked_v2/{family.family}.jsonl"] = _jsonl_bytes(
            family.kept_rows
        )

    receipts = [
        artifacts.write_bytes_create_only(
            pathlib.Path(output_root) / relative_path,
            payload,
        )
        for relative_path, payload in sorted(files.items())
    ]
    return tuple(receipts)


def write_completion_marker(
    output_root: pathlib.Path,
    *,
    verification_report: verify.VerificationReport,
    request_count: int,
    audio_count: int,
    artifact_receipts: Sequence[artifacts.ArtifactReceipt],
) -> artifacts.ArtifactReceipt:
    """Write cheap recovery evidence after the verified build is complete.

    Artifact identities come directly from the create-only write receipts.
    Frozen sources and materialized FLACs are deliberately not reread here.
    """

    if request_count <= 0 or audio_count != request_count:
        raise StagingError(
            "completion marker requires one materialized audio per request"
        )
    root = pathlib.Path(output_root)
    evidence: list[dict[str, object]] = []
    seen_paths: set[str] = set()
    materialization_sha256: str | None = None
    for receipt in artifact_receipts:
        try:
            relative = receipt.path.relative_to(root).as_posix()
        except ValueError as error:
            raise StagingError(
                f"artifact is outside completion root: {receipt.path}"
            ) from error
        if relative == "_SUCCESS.json" or relative in seen_paths:
            raise StagingError(f"invalid completion artifact path: {relative}")
        seen_paths.add(relative)
        evidence.append(
            {
                "path": relative,
                "sha256": receipt.sha256,
                "size": receipt.size,
            }
        )
        if relative == "receipts/materialization.jsonl":
            materialization_sha256 = receipt.sha256
    if materialization_sha256 is None:
        raise StagingError(
            "completion marker lacks materialization receipt artifact"
        )
    evidence.sort(key=lambda value: str(value["path"]))
    marker = {
        "artifact_count": len(evidence),
        "artifacts": evidence,
        "audio_count": audio_count,
        "kind": "local_reconstruction_success",
        "materialization_receipt_artifact_sha256": materialization_sha256,
        "request_count": request_count,
        "schema_version": 1,
        "verification": verification_report.as_dict(),
        "verified": True,
    }
    return artifacts.write_bytes_create_only(
        root / "_SUCCESS.json",
        artifacts.canonical_json_bytes(marker),
    )


def _load_object(path: pathlib.Path, *, label: str) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise StagingError(f"cannot load {label}: {path}") from error
    if not isinstance(value, dict):
        raise StagingError(f"{label} must be a JSON object")
    return value


def _load_jsonl(
    path: pathlib.Path, *, label: str
) -> tuple[dict[str, object], ...]:
    rows: list[dict[str, object]] = []
    try:
        with path.open(encoding="utf-8") as source:
            for line_number, line in enumerate(source, start=1):
                if not line.strip():
                    raise StagingError(
                        f"{label}:{line_number}: blank JSONL row"
                    )
                row = json.loads(line)
                if not isinstance(row, dict):
                    raise StagingError(
                        f"{label}:{line_number}: row is not an object"
                    )
                rows.append(row)
    except StagingError:
        raise
    except (OSError, json.JSONDecodeError) as error:
        raise StagingError(f"cannot load {label}: {path}") from error
    if not rows:
        raise StagingError(f"{label} cannot be empty")
    return tuple(rows)


def stage_local_dataset(
    paths: StagePaths,
    *,
    masked_group_index: recording_groups.SourceIndex
    | eval_views.BindingGroupResolver,
    max_materialization_workers: int = 1,
) -> StagingResult:
    """Build, materialize, verify, and stage the complete local dataset.

    The function has no provider-side write path.  It consumes the existing
    frozen source-inventory receipt and therefore performs no whole-source
    decode or checksum preflight.
    """

    completion_path = pathlib.Path(paths.output_root) / "_SUCCESS.json"
    if completion_path.exists():
        raise StagingError(
            f"staged dataset is already complete: {completion_path}"
        )
    toolchain = media.validate_encoder_toolchain(
        paths.ffmpeg_binary,
        paths.encoder_toolchain_receipt,
    )
    bundle = inputs.load_inputs(paths.input_paths)
    owner_result = ownership.freeze_ownership(bundle)
    sources = media.read_inventory_receipt(paths.source_inventory_receipt)
    universe = build_frame_universe(bundle, owner_result, sources)
    production_target = _load_object(
        paths.production_duration_target,
        label="production duration target",
    )
    histogram = production_target.get("duration_histogram_ms")
    if not isinstance(histogram, Mapping) or not histogram:
        raise StagingError(
            "production duration target lacks duration_histogram_ms"
        )
    reused_continuous_plans = None
    if paths.continuous_plan_root is not None:
        reused_continuous_plans = load_guarded_continuous_plans(
            paths.continuous_plan_root,
            universe=universe,
            sources_by_split=_continuous_inputs_by_split(universe),
            production_histogram_ms=histogram,
            ownership_receipt=paths.ownership_receipt,
            source_inventory_receipt=paths.source_inventory_receipt,
            production_duration_target=paths.production_duration_target,
        )
    planning = plan_final_requests(
        universe,
        histogram,
        reused_continuous_plans=reused_continuous_plans,
    )
    bindings = materialize_final_requests(
        planning.requests,
        sources,
        paths.output_root,
        toolchain=toolchain,
        max_workers=max_materialization_workers,
    )
    topology_ids_by_request: dict[str, tuple[str, ...]] = {}
    group_by_source = owner_result.group_by_source_uri()
    for request in planning.requests:
        topology_ids_by_request[request.request_id] = (
            group_by_source[request.source_uri].group_id,
        )
    manifest_artifacts = manifests.build_manifest_artifacts(
        requests=planning.requests,
        materialization_bindings=bindings,
        proposed_gcs_prefix=paths.proposed_gcs_prefix,
        topology_ids_by_request=topology_ids_by_request,
        repo_root=paths.repo_root,
    )
    expected_masked_families = set(eval_views.MASKED_DATASET_FAMILIES)
    if set(paths.masked_paths_by_family) != expected_masked_families:
        raise StagingError(
            "masked paths must contain exactly the frozen four families"
        )
    masked_rows = {
        family: _load_jsonl(
            paths.masked_paths_by_family[family],
            label=f"masked {family}",
        )
        for family in eval_views.MASKED_DATASET_FAMILIES
    }
    try:
        masked_retained_proof = paths.masked_retained_proof.read_bytes()
    except OSError as error:
        raise StagingError(
            f"cannot read masked selection proof: {paths.masked_retained_proof}"
        ) from error
    masked_filter_report = _load_object(
        paths.masked_filter_report,
        label="masked filter report",
    )
    current_physical_groups = {
        source_uri: group.group_id
        for source_uri, group in group_by_source.items()
    }
    train_group_ids = frozenset(
        group.group_id
        for group in owner_result.source_groups
        if group.final_split is domain.Split.TRAIN
    )
    evaluation = build_evaluation_artifacts(
        manifest_artifacts,
        masked_rows_by_family=masked_rows,
        masked_retained_proof_jsonl=masked_retained_proof,
        masked_filter_report=masked_filter_report,
        masked_group_index=masked_group_index,
        current_physical_groups=current_physical_groups,
        train_group_ids=train_group_ids,
    )
    construction_contract = _load_object(
        paths.construction_contract,
        label="construction contract",
    )
    verification_bundle = build_verification_bundle(
        universe=universe,
        requests=planning.requests,
        materialization_bindings=bindings,
        evaluation=evaluation,
        construction_contract=construction_contract,
    )
    verification_report = verify.verify_artifacts(verification_bundle)
    artifact_receipts = write_local_artifacts(
        output_root=paths.output_root,
        universe=universe,
        planning=planning,
        materialization_bindings=bindings,
        manifest_artifacts=manifest_artifacts,
        evaluation=evaluation,
        verification_bundle=verification_bundle,
        verification_report=verification_report,
    )
    completion_marker = write_completion_marker(
        paths.output_root,
        verification_report=verification_report,
        request_count=len(planning.requests),
        audio_count=len(bindings),
        artifact_receipts=artifact_receipts,
    )
    return StagingResult(
        requests=planning.requests,
        materialization_bindings=bindings,
        manifest_artifacts=manifest_artifacts,
        evaluation=evaluation,
        verification_bundle=verification_bundle,
        verification_report=verification_report,
        artifact_receipts=artifact_receipts,
        completion_marker=completion_marker,
    )
