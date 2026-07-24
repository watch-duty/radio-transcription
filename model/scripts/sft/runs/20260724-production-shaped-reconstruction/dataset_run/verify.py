"""Independent verification of frozen reconstruction artifacts.

The verifier deliberately consumes only JSON-compatible records.  It does not
import the constructor or optimizer, so a bug in either cannot make the same
assumption true on both sides of the audit.

Canonical bundle keys:

``source_inventory``
    Generation-locked decoded source records.  Each record has ``uri``,
    ``generation``, ``frame_count``, ``sample_rate``, ``channels``, and
    ``subtype``.
``owner_ledger``
    The protected speech universe.  Each record has ``owner_id``, ``split``,
    ``family``, ``source_uri``, ``start_frame``, ``end_frame``, and nonblank
    ``text``.  ``constituent_ids`` is optional for an atomic ledger and defaults
    to ``[owner_id]``.
``raw_atoms``
    Optional but recommended atom records keyed by ``atom_id``.  When present,
    their identifiers must be in exact one-to-one correspondence with all
    owner ``constituent_ids``.
``requests``
    Continuous reconstructed train and primary-eval requests.
``pii_intervals``
    Raw targetless-PII intervals.  Speech intervals are subtracted before
    checking exclusion, implementing Speech-over-PII.
``physical_groups``
    A mapping from every represented source URI to its physical recording
    group.
``materialization_receipts``
    One exact-geometry receipt per request.  The decoded source-slice and
    decoded output PCM SHA-256 values must match.  Each receipt must also bind
    the exact frozen family encoder profile, artifact identities, normalized
    command, source-native PCM geometry, and FLAC output evidence.
``primary_eval_rows`` and ``validation_rows``
    The actual full primary-eval payload and capped provider-validation
    payload.  The primary must cover every reconstructed eval request exactly
    once; both hashes are independently recomputed.
``provider_validation_rows`` and ``provider_validation_receipt``
    The actual capped validation projection and its frozen selector receipt.
    The verifier independently recomputes the size cap, largest-remainder
    quotas over dataset/duration/target-word strata, stable-hash source
    round-robin selection, and every binding hash without consulting model
    outcomes.
``prior_context_schedule_rows``
    The actual reference-free schedule.  It must be the exact independently
    derived source/order projection of ``primary_eval_rows``.
``eval_lanes``
    Exactly ``reconstructed_primary``, ``predicted_prior_context_10``, and
    ``masked_v2``.
``masked_rows_by_family``, ``masked_row_projections``,
``masked_filter_receipt``, ``masked_selection_proof_sha256``, and
``masked_retained_payload_sha256``
    The unchanged final masked-v2 payloads, their physical-lineage projections,
    the per-row payload hashes produced by the frozen filter, and the hash of
    both the normalized selection proof and unchanged raw retained payload.

Only :func:`verify_and_print` emits ``COMPLETE``, and it does so after every
check succeeds.
"""

from __future__ import annotations

import argparse
import dataclasses
import decimal
import fractions
import hashlib
import json
import pathlib
import re
import sys
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from typing import Any, NoReturn, TextIO

PRESEGMENTED_FAMILIES = frozenset({"bcfy_calls", "echo", "fire_notifications"})
PROVIDER_ITEM_RESIDUAL_FAMILIES = frozenset(
    {"bcfy_calls", "fire_notifications"}
)
EXPECTED_EVAL_LANES = frozenset(
    {
        "reconstructed_primary",
        "predicted_prior_context_10",
        "masked_v2",
    }
)
MASKED_DATASET_FAMILIES = (
    "bcfy_calls",
    "bcfy_feeds",
    "echo",
    "fire_notifications",
)
_SCHEDULE_KEYS = frozenset(
    {
        "audio_uri",
        "context_episode_id",
        "example_id",
        "manifest_index",
        "request_id",
        "segment_id",
        "source_ordinal",
        "source_uri",
    }
)
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
_SHA256 = re.compile(r"[0-9a-f]{64}")
_OUTCOME_KEY_PARTS = (
    "prediction",
    "reference_context",
    "wer",
    "finish_reason",
    "http",
    "status",
    "fallback",
    "model_output",
)
_ENCODER_CONTRACT_SHA256 = (
    "f0d22af2c589c36fdff9dd1da1b4f82cc93207582b72d102030ef059e3feab29"
)
_BCFY_FEEDS_ENCODER_PROFILE = "bcfy_feeds_soundfile_libflac_pcm_preserve_v1"
_BCFY_FEEDS_ENCODER_VERSION = (
    "python-soundfile 0.13.1; libsndfile 1.2.2; libFLAC 1.4.3"
)
_PINNED_LIBSNDFILE_BINARY_SHA256 = (
    "c872fb84af2afc0e96b603e0000f0f407af86c49ad1126f2785e5722c7bb27d7"
)
_PINNED_SOUNDFILE_PYTHON_ARTIFACT_SHA256 = (
    "e349c3858d6a0481fe8078d7a0132603261e7bef2b0facfb9596aeb44c51c563"
)
_PINNED_SOUNDFILE_CFFI_ARTIFACT_SHA256 = (
    "31490aab77c158db180b7a6084256a1d9c6e36c2ff9ea98779494df1161f7474"
)
_PINNED_EMBEDDED_LIBFLAC_VERSION = "1.4.3"
_PRESEGMENTED_ENCODER_PROFILE = (
    "presegmented_ffmpeg_6_1_1_flac5_pcm_preserve_v1"
)
_PRESEGMENTED_ENCODER_VERSION = "6.1.1"
_PINNED_FFMPEG_SHA256 = (
    "dcf4c806caad507b11c73754b86a8cbd0ce686ae12f90bf8ba235d145f53edc0"
)
_PINNED_FFMPEG_IMAGE_URI = (
    "us-central1-docker.pkg.dev/automatic-hawk-481415-m9/"
    "radio-transcription-services-prod/"
    "normalization@sha256:"
    "415da267117f80dcf13bf7d3518cb5c0ca80aedd75bb3b4211e1adc4837e0a62"
)
_PINNED_FFMPEG_REVISION = "normalization-service-prod-00032-zmz"
_PCM_ENCODER_FORMATS = {
    "PCM_16": ("s16le", "s16"),
    "PCM_24": ("s32le", "s32"),
}
_MATERIALIZATION_RECEIPT_KEYS = frozenset(
    {
        "end_frame",
        "encoder_binary_sha256",
        "encoder_cffi_artifact_sha256",
        "encoder_command_sha256",
        "encoder_contract_sha256",
        "encoder_embedded_libflac_version",
        "encoder_input_pcm_format",
        "encoder_production_image_uri",
        "encoder_production_revision",
        "encoder_profile",
        "encoder_python_artifact_sha256",
        "encoder_version",
        "output_channels",
        "output_codec",
        "output_encoded_sha256",
        "output_encoded_size_bytes",
        "output_format",
        "output_frame_count",
        "output_pcm_sha256",
        "output_sample_rate",
        "output_subtype",
        "request_id",
        "source_pcm_sha256",
        "source_uri",
        "start_frame",
    }
)


class VerificationError(ValueError):
    """The frozen artifact bundle violates the reconstruction contract."""


@dataclasses.dataclass(frozen=True, slots=True)
class VerificationReport:
    """Counts from a successfully verified artifact bundle."""

    source_count: int
    owner_count: int
    raw_atom_count: int
    train_request_count: int
    eval_request_count: int
    masked_row_count: int

    def as_dict(self) -> dict[str, int]:
        """Return a JSON-compatible report."""
        return dataclasses.asdict(self)


@dataclasses.dataclass(frozen=True, slots=True)
class _Source:
    uri: str
    generation: int
    frame_count: int
    sample_rate: int
    channels: int
    subtype: str


@dataclasses.dataclass(frozen=True, slots=True)
class _Interval:
    source_uri: str
    start_frame: int
    end_frame: int


@dataclasses.dataclass(frozen=True, slots=True)
class _Atom(_Interval):
    atom_id: str


@dataclasses.dataclass(frozen=True, slots=True)
class _Owner(_Interval):
    owner_id: str
    split: str
    family: str
    text: str
    constituent_ids: tuple[str, ...]
    provider_item_id: str | None
    archive_source_id: str | None


@dataclasses.dataclass(frozen=True, slots=True)
class _Request(_Interval):
    request_id: str
    split: str
    family: str
    sample_rate: int
    owner_ids: tuple[str, ...]
    text: str
    provider_item_id: str | None
    archive_source_id: str | None


@dataclasses.dataclass(frozen=True, slots=True)
class _ExpectedEncoder:
    profile: str
    version: str
    binary_sha256: str
    python_artifact_sha256: str | None
    cffi_artifact_sha256: str | None
    embedded_libflac_version: str | None
    production_image_uri: str | None
    production_revision: str | None
    input_pcm_format: str
    command_sha256: str


def _error(message: str) -> NoReturn:
    raise VerificationError(message)


def _mapping(value: object, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        _error(f"{label} must be an object")
    return value


def _list(value: object, label: str) -> list[Any]:
    if not isinstance(value, list):
        _error(f"{label} must be an array")
    return value


def _field(record: Mapping[str, Any], name: str, label: str) -> Any:
    if name not in record:
        _error(f"{label} is missing {name}")
    return record[name]


def _string(value: object, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        _error(f"{label} must be a nonblank string")
    return value


def _integer(
    value: object,
    label: str,
    *,
    minimum: int | None = None,
) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        _error(f"{label} must be an integer")
    if minimum is not None and value < minimum:
        _error(f"{label} must be at least {minimum}")
    return value


def _text(value: object, label: str) -> str:
    text = _string(value, label)
    if not " ".join(text.split()):
        _error(f"{label} must contain non-whitespace text")
    return text


def _sha256(value: object, label: str) -> str:
    digest = _string(value, label).lower()
    if _SHA256.fullmatch(digest) is None:
        _error(f"{label} must be a lowercase hexadecimal SHA-256")
    return digest


def _interval(
    record: Mapping[str, Any],
    label: str,
) -> tuple[str, int, int]:
    source_uri = _string(
        _field(record, "source_uri", label), f"{label}.source_uri"
    )
    start = _integer(
        _field(record, "start_frame", label),
        f"{label}.start_frame",
        minimum=0,
    )
    end = _integer(
        _field(record, "end_frame", label),
        f"{label}.end_frame",
        minimum=1,
    )
    if end <= start:
        _error(f"{label} must have end_frame > start_frame")
    return source_uri, start, end


def _unique_by_id(
    records: Sequence[Any],
    *,
    id_name: str,
    label: str,
) -> dict[str, Mapping[str, Any]]:
    result: dict[str, Mapping[str, Any]] = {}
    for index, raw_record in enumerate(records):
        record = _mapping(raw_record, f"{label}[{index}]")
        identifier = _string(
            _field(record, id_name, f"{label}[{index}]"),
            f"{label}[{index}].{id_name}",
        )
        if identifier in result:
            _error(f"duplicate {label} {id_name}: {identifier}")
        result[identifier] = record
    return result


def _union(intervals: Sequence[tuple[int, int]]) -> list[tuple[int, int]]:
    if not intervals:
        return []
    result: list[tuple[int, int]] = []
    for start, end in sorted(intervals):
        if not result or start > result[-1][1]:
            result.append((start, end))
            continue
        previous_start, previous_end = result[-1]
        result[-1] = (previous_start, max(previous_end, end))
    return result


def _subtract(
    intervals: Sequence[tuple[int, int]],
    cuts: Sequence[tuple[int, int]],
) -> list[tuple[int, int]]:
    """Return half-open ``intervals`` minus half-open ``cuts``."""
    remaining = _union(intervals)
    for cut_start, cut_end in _union(cuts):
        next_remaining: list[tuple[int, int]] = []
        for start, end in remaining:
            if cut_end <= start or cut_start >= end:
                next_remaining.append((start, end))
                continue
            if start < cut_start:
                next_remaining.append((start, cut_start))
            if cut_end < end:
                next_remaining.append((cut_end, end))
        remaining = next_remaining
    return remaining


def _intersects(
    left_start: int,
    left_end: int,
    right_start: int,
    right_end: int,
) -> bool:
    return left_start < right_end and right_start < left_end


def _contains(
    outer_start: int,
    outer_end: int,
    inner_start: int,
    inner_end: int,
) -> bool:
    return outer_start <= inner_start and inner_end <= outer_end


def _check_outcome_blind_metadata(bundle: Mapping[str, Any]) -> None:
    """Reject outcome-conditioned keys from construction-only metadata."""
    roots: list[tuple[str, object]] = []
    for name in ("construction_contract", "construction_metadata"):
        if name in bundle:
            roots.append((name, bundle[name]))

    def visit(path: str, value: object) -> None:
        if isinstance(value, Mapping):
            for raw_key, child in value.items():
                if not isinstance(raw_key, str):
                    _error(f"{path} contains a non-string key")
                normalized = raw_key.lower().replace("-", "_")
                if any(part in normalized for part in _OUTCOME_KEY_PARTS):
                    _error(
                        "outcome-dependent construction field is forbidden: "
                        f"{path}.{raw_key}"
                    )
                visit(f"{path}.{raw_key}", child)
        elif isinstance(value, list):
            for index, child in enumerate(value):
                visit(f"{path}[{index}]", child)

    for root_name, root in roots:
        visit(root_name, root)


def _parse_inventory(bundle: Mapping[str, Any]) -> dict[str, _Source]:
    raw_inventory = _list(
        _field(bundle, "source_inventory", "bundle"),
        "source_inventory",
    )
    inventory: dict[str, _Source] = {}
    for index, raw_record in enumerate(raw_inventory):
        label = f"source_inventory[{index}]"
        record = _mapping(raw_record, label)
        uri = _string(_field(record, "uri", label), f"{label}.uri")
        if uri in inventory:
            _error(f"duplicate source inventory URI: {uri}")
        source = _Source(
            uri=uri,
            generation=_integer(
                _field(record, "generation", label),
                f"{label}.generation",
                minimum=1,
            ),
            frame_count=_integer(
                _field(record, "frame_count", label),
                f"{label}.frame_count",
                minimum=1,
            ),
            sample_rate=_integer(
                _field(record, "sample_rate", label),
                f"{label}.sample_rate",
                minimum=1,
            ),
            channels=_integer(
                _field(record, "channels", label),
                f"{label}.channels",
                minimum=1,
            ),
            subtype=_string(
                _field(record, "subtype", label),
                f"{label}.subtype",
            ),
        )
        if "size_bytes" in record:
            _integer(record["size_bytes"], f"{label}.size_bytes", minimum=1)
        for hash_name in ("sha256", "decoded_pcm_sha256"):
            if hash_name in record:
                _sha256(record[hash_name], f"{label}.{hash_name}")
        inventory[uri] = source
    if not inventory:
        _error("source_inventory must not be empty")
    return inventory


def _check_bounds(
    interval: _Interval,
    inventory: Mapping[str, _Source],
    label: str,
) -> None:
    source = inventory.get(interval.source_uri)
    if source is None:
        _error(
            f"{label} references source absent from inventory: {interval.source_uri}"
        )
    if interval.end_frame > source.frame_count:
        _error(
            f"{label} is out of source bounds: {interval.end_frame} > "
            f"{source.frame_count}"
        )


def _parse_raw_atoms(
    bundle: Mapping[str, Any],
    inventory: Mapping[str, _Source],
) -> dict[str, _Atom]:
    if "raw_atoms" not in bundle:
        return {}
    raw_records = _list(bundle["raw_atoms"], "raw_atoms")
    records = _unique_by_id(
        raw_records,
        id_name="atom_id",
        label="raw_atoms",
    )
    atoms: dict[str, _Atom] = {}
    for atom_id, record in records.items():
        source_uri, start, end = _interval(record, f"raw atom {atom_id}")
        atom = _Atom(
            source_uri=source_uri,
            start_frame=start,
            end_frame=end,
            atom_id=atom_id,
        )
        _check_bounds(atom, inventory, f"raw atom {atom_id}")
        atoms[atom_id] = atom
    return atoms


def _optional_lineage_id(
    record: Mapping[str, Any],
    field_name: str,
    label: str,
) -> str | None:
    if field_name not in record:
        return None
    return _string(record[field_name], f"{label}.{field_name}")


def _check_family_lineage(
    *,
    family: str,
    source_uri: str,
    provider_item_id: str | None,
    archive_source_id: str | None,
    label: str,
) -> None:
    if family == "echo":
        if provider_item_id is not None:
            _error(
                f"{label} falsely claims provider_item_id for an Echo archive"
            )
        if archive_source_id != source_uri:
            _error(f"{label} must identify its exact Echo archive_source_id")
        return
    if family in PROVIDER_ITEM_RESIDUAL_FAMILIES:
        if provider_item_id != source_uri:
            _error(f"{label}.provider_item_id must equal its source URI")
        if archive_source_id is not None:
            _error(f"{label} falsely claims archive_source_id")
        return
    if family == "bcfy_feeds":
        if provider_item_id is not None or archive_source_id is not None:
            _error(
                f"{label} must not claim provider or archive identity for "
                "continuous BCFY Feeds"
            )
        return
    _error(f"{label} has unsupported dataset family: {family}")


def _parse_owners(
    bundle: Mapping[str, Any],
    inventory: Mapping[str, _Source],
) -> dict[str, _Owner]:
    raw_records = _list(
        _field(bundle, "owner_ledger", "bundle"),
        "owner_ledger",
    )
    records = _unique_by_id(
        raw_records,
        id_name="owner_id",
        label="owner_ledger",
    )
    owners: dict[str, _Owner] = {}
    for owner_id, record in records.items():
        label = f"owner {owner_id}"
        source_uri, start, end = _interval(record, label)
        split = _string(_field(record, "split", label), f"{label}.split")
        if split not in {"train", "eval"}:
            _error(f"{label}.split must be train or eval")
        family = _string(_field(record, "family", label), f"{label}.family")
        text = _text(_field(record, "text", label), f"{label}.text")
        raw_constituents = record.get("constituent_ids", [owner_id])
        if not isinstance(raw_constituents, list) or not raw_constituents:
            _error(f"{label}.constituent_ids must be a nonempty array")
        constituent_ids = tuple(
            _string(value, f"{label}.constituent_ids[{index}]")
            for index, value in enumerate(raw_constituents)
        )
        if len(set(constituent_ids)) != len(constituent_ids):
            _error(f"{label} repeats a constituent")
        provider_item_id = _optional_lineage_id(
            record,
            "provider_item_id",
            label,
        )
        archive_source_id = _optional_lineage_id(
            record,
            "archive_source_id",
            label,
        )
        _check_family_lineage(
            family=family,
            source_uri=source_uri,
            provider_item_id=provider_item_id,
            archive_source_id=archive_source_id,
            label=label,
        )
        owner = _Owner(
            source_uri=source_uri,
            start_frame=start,
            end_frame=end,
            owner_id=owner_id,
            split=split,
            family=family,
            text=text,
            constituent_ids=constituent_ids,
            provider_item_id=provider_item_id,
            archive_source_id=archive_source_id,
        )
        _check_bounds(owner, inventory, label)
        owners[owner_id] = owner
    if not owners:
        _error("owner_ledger must not be empty")
    return owners


def _check_constituents(
    owners: Mapping[str, _Owner],
    atoms: Mapping[str, _Atom],
) -> None:
    constituent_owner: dict[str, str] = {}
    for owner in owners.values():
        for constituent_id in owner.constituent_ids:
            previous = constituent_owner.get(constituent_id)
            if previous is not None:
                _error(
                    "constituent/composite double inclusion: "
                    f"{constituent_id} belongs to {previous} and {owner.owner_id}"
                )
            constituent_owner[constituent_id] = owner.owner_id
            if atoms:
                atom = atoms.get(constituent_id)
                if atom is None:
                    _error(
                        f"owner {owner.owner_id} references unknown raw atom "
                        f"{constituent_id}"
                    )
                if atom.source_uri != owner.source_uri:
                    _error(
                        f"raw atom {constituent_id} and owner {owner.owner_id} "
                        "use different sources"
                    )
                if not _contains(
                    owner.start_frame,
                    owner.end_frame,
                    atom.start_frame,
                    atom.end_frame,
                ):
                    _error(
                        f"owner {owner.owner_id} does not contain raw atom "
                        f"{constituent_id}"
                    )
    if atoms:
        referenced = set(constituent_owner)
        atom_ids = set(atoms)
        if referenced != atom_ids:
            missing = sorted(atom_ids - referenced)
            extra = sorted(referenced - atom_ids)
            _error(
                "raw atom/constituent universe mismatch; "
                f"unowned={missing[:5]}, unknown={extra[:5]}"
            )


def _parse_pii(
    bundle: Mapping[str, Any],
    inventory: Mapping[str, _Source],
) -> list[_Interval]:
    raw_records = _list(
        _field(bundle, "pii_intervals", "bundle"),
        "pii_intervals",
    )
    intervals: list[_Interval] = []
    for index, raw_record in enumerate(raw_records):
        label = f"pii_intervals[{index}]"
        record = _mapping(raw_record, label)
        source_uri, start, end = _interval(record, label)
        interval = _Interval(source_uri, start, end)
        _check_bounds(interval, inventory, label)
        intervals.append(interval)
    return intervals


def _parse_requests(
    bundle: Mapping[str, Any],
    inventory: Mapping[str, _Source],
) -> dict[str, _Request]:
    raw_records = _list(_field(bundle, "requests", "bundle"), "requests")
    records = _unique_by_id(
        raw_records,
        id_name="request_id",
        label="requests",
    )
    requests: dict[str, _Request] = {}
    for request_id, record in records.items():
        label = f"request {request_id}"
        for forbidden_shape in ("intervals", "source_intervals", "segments"):
            if forbidden_shape in record:
                _error(
                    f"{label} is not represented as one continuous interval "
                    f"because it contains {forbidden_shape}"
                )
        source_uri, start, end = _interval(record, label)
        split = _string(_field(record, "split", label), f"{label}.split")
        if split not in {"train", "eval"}:
            _error(f"{label}.split must be train or eval")
        family = _string(_field(record, "family", label), f"{label}.family")
        sample_rate = _integer(
            _field(record, "sample_rate", label),
            f"{label}.sample_rate",
            minimum=1,
        )
        raw_owner_ids = _list(
            _field(record, "owner_ids", label),
            f"{label}.owner_ids",
        )
        if not raw_owner_ids:
            _error(f"{label}.owner_ids must not be empty")
        owner_ids = tuple(
            _string(value, f"{label}.owner_ids[{index}]")
            for index, value in enumerate(raw_owner_ids)
        )
        if len(set(owner_ids)) != len(owner_ids):
            _error(f"{label} repeats an owner")
        text = _text(_field(record, "text", label), f"{label}.text")
        provider_item_id = _optional_lineage_id(
            record,
            "provider_item_id",
            label,
        )
        archive_source_id = _optional_lineage_id(
            record,
            "archive_source_id",
            label,
        )
        _check_family_lineage(
            family=family,
            source_uri=source_uri,
            provider_item_id=provider_item_id,
            archive_source_id=archive_source_id,
            label=label,
        )
        request = _Request(
            source_uri=source_uri,
            start_frame=start,
            end_frame=end,
            request_id=request_id,
            split=split,
            family=family,
            sample_rate=sample_rate,
            owner_ids=owner_ids,
            text=text,
            provider_item_id=provider_item_id,
            archive_source_id=archive_source_id,
        )
        _check_bounds(request, inventory, label)
        source = inventory[source_uri]
        if sample_rate != source.sample_rate:
            _error(
                f"{label}.sample_rate does not match source inventory: "
                f"{sample_rate} != {source.sample_rate}"
            )
        requests[request_id] = request
    if not requests:
        _error("requests must not be empty")
    return requests


def _check_owner_coverage(
    owners: Mapping[str, _Owner],
    atoms: Mapping[str, _Atom],
    requests: Mapping[str, _Request],
) -> None:
    occurrences: Counter[str] = Counter()
    for request in requests.values():
        for owner_id in request.owner_ids:
            owner = owners.get(owner_id)
            if owner is None:
                _error(
                    f"request {request.request_id} references unknown owner {owner_id}"
                )
            occurrences[owner_id] += 1
            if owner.source_uri != request.source_uri:
                _error(
                    f"request {request.request_id} merges owner {owner_id} "
                    "from another source/provider item"
                )
            if owner.split != request.split:
                _error(
                    f"request {request.request_id} changes owner {owner_id} split"
                )
            if owner.family != request.family:
                _error(
                    f"request {request.request_id} changes owner {owner_id} family"
                )
            if not _contains(
                request.start_frame,
                request.end_frame,
                owner.start_frame,
                owner.end_frame,
            ):
                _error(f"request {request.request_id} cuts owner {owner_id}")
            for constituent_id in owner.constituent_ids:
                atom = atoms.get(constituent_id)
                if atom is not None and not _contains(
                    request.start_frame,
                    request.end_frame,
                    atom.start_frame,
                    atom.end_frame,
                ):
                    _error(
                        f"request {request.request_id} cuts constituent "
                        f"{constituent_id}"
                    )
    for owner_id in owners:
        count = occurrences[owner_id]
        if count != 1:
            _error(
                f"protected owner {owner_id} appears {count} times; expected exactly 1"
            )


def _check_request_nonoverlap(requests: Mapping[str, _Request]) -> None:
    grouped: dict[tuple[str, str], list[_Request]] = defaultdict(list)
    for request in requests.values():
        grouped[(request.source_uri, request.split)].append(request)
    for (source_uri, split), source_requests in grouped.items():
        source_requests.sort(
            key=lambda item: (item.start_frame, item.end_frame, item.request_id)
        )
        previous = source_requests[0]
        for current in source_requests[1:]:
            if current.start_frame < previous.end_frame:
                _error(
                    "duplicate request frame overlap for "
                    f"{source_uri} in {split}: {previous.request_id} and "
                    f"{current.request_id}"
                )
            previous = current


def _pii_only_by_source(
    owners: Mapping[str, _Owner],
    atoms: Mapping[str, _Atom],
    pii_intervals: Sequence[_Interval],
) -> dict[str, list[tuple[int, int]]]:
    speech: dict[str, list[tuple[int, int]]] = defaultdict(list)
    raw_pii: dict[str, list[tuple[int, int]]] = defaultdict(list)
    if atoms:
        for atom in atoms.values():
            speech[atom.source_uri].append((atom.start_frame, atom.end_frame))
    else:
        # Without an explicit raw-atom table, the ledger is necessarily
        # interpreted as atomic.
        for owner in owners.values():
            speech[owner.source_uri].append(
                (owner.start_frame, owner.end_frame)
            )
    for interval in pii_intervals:
        raw_pii[interval.source_uri].append(
            (interval.start_frame, interval.end_frame)
        )
    return {
        source_uri: _subtract(intervals, speech.get(source_uri, []))
        for source_uri, intervals in raw_pii.items()
    }


def _check_pii_exclusion(
    requests: Mapping[str, _Request],
    pii_only: Mapping[str, Sequence[tuple[int, int]]],
) -> None:
    for request in requests.values():
        for start, end in pii_only.get(request.source_uri, []):
            if _intersects(
                request.start_frame,
                request.end_frame,
                start,
                end,
            ):
                _error(
                    f"request {request.request_id} includes PII-only frames "
                    f"[{start}, {end})"
                )


def _strict_overlap_components(
    owners: Sequence[_Owner],
) -> list[tuple[int, int, set[str]]]:
    """Build Echo Speech components; touching intervals stay separate."""
    ordered = sorted(
        owners,
        key=lambda owner: (
            owner.start_frame,
            owner.end_frame,
            owner.owner_id,
        ),
    )
    components: list[tuple[int, int, set[str]]] = []
    for owner in ordered:
        if not components or owner.start_frame >= components[-1][1]:
            components.append(
                (
                    owner.start_frame,
                    owner.end_frame,
                    {owner.owner_id},
                )
            )
            continue
        start, end, owner_ids = components[-1]
        owner_ids.add(owner.owner_id)
        components[-1] = (start, max(end, owner.end_frame), owner_ids)
    return components


def _check_echo_geometry(  # noqa: PLR0912
    owners: Mapping[str, _Owner],
    requests: Mapping[str, _Request],
) -> None:
    """Require one exact, gap-free request per strict-overlap Echo component."""
    owners_by_source: dict[str, list[_Owner]] = defaultdict(list)
    requests_by_source: dict[str, list[_Request]] = defaultdict(list)
    for owner in owners.values():
        if owner.family == "echo":
            owners_by_source[owner.source_uri].append(owner)
    for request in requests.values():
        if request.family == "echo":
            requests_by_source[request.source_uri].append(request)

    for source_uri, source_owners in owners_by_source.items():
        components = _strict_overlap_components(source_owners)
        expected_by_geometry = {
            (start, end): owner_ids for start, end, owner_ids in components
        }
        source_requests = requests_by_source.get(source_uri, [])
        if len(source_requests) != len(components):
            _error(
                f"Echo archive {source_uri} has {len(source_requests)} "
                f"requests but {len(components)} strict-overlap Speech "
                "components"
            )
        seen: set[tuple[int, int]] = set()
        for request in source_requests:
            geometry = (request.start_frame, request.end_frame)
            expected_owners = expected_by_geometry.get(geometry)
            if expected_owners is None:
                _error(
                    f"Echo request {request.request_id} is not the exact "
                    "strict-overlap Speech component and includes a positive "
                    "blank/unannotated gap or cuts Speech"
                )
            if geometry in seen:
                _error(
                    f"Echo component {source_uri} {geometry} is represented "
                    "more than once"
                )
            seen.add(geometry)
            if set(request.owner_ids) != expected_owners:
                _error(
                    f"Echo request {request.request_id} does not own exactly "
                    "its strict-overlap Speech component"
                )
            if request.provider_item_id is not None:
                _error(
                    f"Echo request {request.request_id} falsely claims a "
                    "provider_item_id for an archive source"
                )
            if request.archive_source_id != source_uri:
                _error(
                    f"Echo request {request.request_id} must identify its "
                    "exact archive_source_id"
                )
    extra_sources = set(requests_by_source) - set(owners_by_source)
    if extra_sources:
        _error(
            "Echo requests exist without protected owners: "
            f"{sorted(extra_sources)[:5]}"
        )


def _check_presegmented_geometry(  # noqa: PLR0912
    owners: Mapping[str, _Owner],
    requests: Mapping[str, _Request],
    inventory: Mapping[str, _Source],
    pii_only: Mapping[str, Sequence[tuple[int, int]]],
) -> None:
    owners_by_source: dict[str, list[_Owner]] = defaultdict(list)
    requests_by_source: dict[str, list[_Request]] = defaultdict(list)
    for owner in owners.values():
        if owner.family in PROVIDER_ITEM_RESIDUAL_FAMILIES:
            owners_by_source[owner.source_uri].append(owner)
    for request in requests.values():
        if request.family in PROVIDER_ITEM_RESIDUAL_FAMILIES:
            requests_by_source[request.source_uri].append(request)

    for source_uri, source_owners in owners_by_source.items():
        families = {owner.family for owner in source_owners}
        if len(families) != 1:
            _error(
                f"presegmented provider item {source_uri} has multiple families"
            )
        source = inventory[source_uri]
        residual_components = _subtract(
            [(0, source.frame_count)],
            pii_only.get(source_uri, []),
        )
        owners_by_component: dict[tuple[int, int], set[str]] = defaultdict(set)
        for owner in source_owners:
            matching = [
                component
                for component in residual_components
                if _contains(
                    component[0],
                    component[1],
                    owner.start_frame,
                    owner.end_frame,
                )
            ]
            if len(matching) != 1:
                _error(
                    f"presegmented owner {owner.owner_id} is not wholly inside "
                    "one PII-residual component"
                )
            owners_by_component[matching[0]].add(owner.owner_id)

        source_requests = requests_by_source.get(source_uri, [])
        if len(source_requests) != len(owners_by_component):
            _error(
                f"presegmented provider item {source_uri} has "
                f"{len(source_requests)} requests but "
                f"{len(owners_by_component)} speech-bearing PII-residual "
                "components"
            )
        seen_components: set[tuple[int, int]] = set()
        for request in source_requests:
            component = (request.start_frame, request.end_frame)
            expected_owners = owners_by_component.get(component)
            if expected_owners is None:
                _error(
                    f"presegmented request {request.request_id} is split at a "
                    "boundary that is not a PII-only interval"
                )
            if component in seen_components:
                _error(
                    f"presegmented component {source_uri} {component} is "
                    "represented more than once"
                )
            seen_components.add(component)
            if set(request.owner_ids) != expected_owners:
                _error(
                    f"presegmented request {request.request_id} does not own "
                    "exactly the speech in its PII-residual component"
                )
            provider_ids = {
                owners[owner_id].provider_item_id
                for owner_id in request.owner_ids
            }
            if len(provider_ids) != 1:
                _error(
                    f"presegmented request {request.request_id} merges provider "
                    "items"
                )
            expected_provider = next(iter(provider_ids))
            if request.provider_item_id != expected_provider:
                _error(
                    f"presegmented request {request.request_id} has the wrong "
                    "provider_item_id"
                )
            if request.archive_source_id is not None:
                _error(
                    f"presegmented request {request.request_id} falsely "
                    "claims archive_source_id"
                )

    extra_sources = set(requests_by_source) - set(owners_by_source)
    if extra_sources:
        _error(
            "presegmented requests exist without protected owners: "
            f"{sorted(extra_sources)[:5]}"
        )
    _check_echo_geometry(owners, requests)


def _parse_physical_groups(
    bundle: Mapping[str, Any],
) -> dict[str, str]:
    raw_groups = _mapping(
        _field(bundle, "physical_groups", "bundle"),
        "physical_groups",
    )
    groups: dict[str, str] = {}
    for raw_uri, raw_group in raw_groups.items():
        uri = _string(raw_uri, "physical_groups key")
        groups[uri] = _string(
            raw_group,
            f"physical_groups[{uri!r}]",
        )
    return groups


def _check_split_groups(
    owners: Mapping[str, _Owner],
    requests: Mapping[str, _Request],
    groups: Mapping[str, str],
) -> tuple[set[str], set[str], set[str]]:
    groups_by_split: dict[str, set[str]] = {
        "train": set(),
        "eval": set(),
    }
    sources_by_split: dict[str, set[str]] = {
        "train": set(),
        "eval": set(),
    }
    for owner in owners.values():
        group = groups.get(owner.source_uri)
        if group is None:
            _error(
                f"owner {owner.owner_id} source has no physical group: "
                f"{owner.source_uri}"
            )
        groups_by_split[owner.split].add(group)
        sources_by_split[owner.split].add(owner.source_uri)
    for request in requests.values():
        group = groups.get(request.source_uri)
        if group is None:
            _error(
                f"request {request.request_id} source has no physical group: "
                f"{request.source_uri}"
            )
        groups_by_split[request.split].add(group)
        sources_by_split[request.split].add(request.source_uri)

    leaked = groups_by_split["train"] & groups_by_split["eval"]
    if leaked:
        _error(f"train/eval physical-group leak: {sorted(leaked)[:5]}")
    return (
        groups_by_split["train"],
        sources_by_split["train"],
        sources_by_split["eval"],
    )


def _encoder_command_sha256(command: Sequence[str]) -> str:
    payload = json.dumps(
        list(command),
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode()
    return hashlib.sha256(payload).hexdigest()


def _expected_encoder(request: _Request, source: _Source) -> _ExpectedEncoder:
    pcm_formats = _PCM_ENCODER_FORMATS.get(source.subtype)
    if pcm_formats is None:
        _error(
            f"materialization receipt {request.request_id} has "
            f"unsupported source PCM subtype: {source.subtype}"
        )
    input_format, sample_format = pcm_formats
    if request.family == "bcfy_feeds":
        command = (
            "python-soundfile.SoundFile",
            "mode=w",
            f"samplerate={source.sample_rate}",
            f"channels={source.channels}",
            "format=FLAC",
            f"subtype={source.subtype}",
            "{output}",
        )
        return _ExpectedEncoder(
            profile=_BCFY_FEEDS_ENCODER_PROFILE,
            version=_BCFY_FEEDS_ENCODER_VERSION,
            binary_sha256=_PINNED_LIBSNDFILE_BINARY_SHA256,
            python_artifact_sha256=(_PINNED_SOUNDFILE_PYTHON_ARTIFACT_SHA256),
            cffi_artifact_sha256=_PINNED_SOUNDFILE_CFFI_ARTIFACT_SHA256,
            embedded_libflac_version=_PINNED_EMBEDDED_LIBFLAC_VERSION,
            production_image_uri=None,
            production_revision=None,
            input_pcm_format=input_format,
            command_sha256=_encoder_command_sha256(command),
        )
    if request.family not in PRESEGMENTED_FAMILIES:
        _error(
            f"materialization receipt {request.request_id} has "
            f"unsupported encoder family: {request.family}"
        )
    command = (
        "{ffmpeg}",
        "-y",
        "-f",
        input_format,
        "-ar",
        str(source.sample_rate),
        "-ac",
        str(source.channels),
        "-i",
        "pipe:0",
        "-map",
        "0:a:0",
        "-c:a",
        "flac",
        "-compression_level",
        "5",
        "-sample_fmt",
        sample_format,
        "-f",
        "flac",
        "{output}",
    )
    return _ExpectedEncoder(
        profile=_PRESEGMENTED_ENCODER_PROFILE,
        version=_PRESEGMENTED_ENCODER_VERSION,
        binary_sha256=_PINNED_FFMPEG_SHA256,
        python_artifact_sha256=None,
        cffi_artifact_sha256=None,
        embedded_libflac_version=None,
        production_image_uri=_PINNED_FFMPEG_IMAGE_URI,
        production_revision=_PINNED_FFMPEG_REVISION,
        input_pcm_format=input_format,
        command_sha256=_encoder_command_sha256(command),
    )


def _check_receipt_media(
    record: Mapping[str, Any],
    request: _Request,
    source: _Source,
    label: str,
) -> None:
    source_uri = _string(
        _field(record, "source_uri", label),
        f"{label}.source_uri",
    )
    start = _integer(
        _field(record, "start_frame", label),
        f"{label}.start_frame",
        minimum=0,
    )
    end = _integer(
        _field(record, "end_frame", label),
        f"{label}.end_frame",
        minimum=1,
    )
    if (source_uri, start, end) != (
        request.source_uri,
        request.start_frame,
        request.end_frame,
    ):
        _error(f"{label} geometry does not match request {request.request_id}")
    source_pcm = _sha256(
        _field(record, "source_pcm_sha256", label),
        f"{label}.source_pcm_sha256",
    )
    output_pcm = _sha256(
        _field(record, "output_pcm_sha256", label),
        f"{label}.output_pcm_sha256",
    )
    if source_pcm != output_pcm:
        _error(f"{label} decoded PCM differs between source slice and output")
    _sha256(
        _field(record, "output_encoded_sha256", label),
        f"{label}.output_encoded_sha256",
    )
    _integer(
        _field(record, "output_encoded_size_bytes", label),
        f"{label}.output_encoded_size_bytes",
        minimum=1,
    )
    output_frames = _integer(
        _field(record, "output_frame_count", label),
        f"{label}.output_frame_count",
        minimum=1,
    )
    if output_frames != request.end_frame - request.start_frame:
        _error(f"{label}.output_frame_count does not match request length")
    output_rate = _integer(
        _field(record, "output_sample_rate", label),
        f"{label}.output_sample_rate",
        minimum=1,
    )
    if output_rate != request.sample_rate or output_rate != source.sample_rate:
        _error(f"{label}.output_sample_rate does not preserve source rate")
    output_channels = _integer(
        _field(record, "output_channels", label),
        f"{label}.output_channels",
        minimum=1,
    )
    if output_channels != source.channels:
        _error(f"{label}.output_channels does not preserve source channels")
    output_subtype = _string(
        _field(record, "output_subtype", label),
        f"{label}.output_subtype",
    )
    if output_subtype != source.subtype:
        _error(f"{label}.output_subtype does not preserve source subtype")
    output_format = _string(
        _field(record, "output_format", label),
        f"{label}.output_format",
    )
    output_codec = _string(
        _field(record, "output_codec", label),
        f"{label}.output_codec",
    )
    if output_format != "FLAC" or output_codec != "flac":
        _error(f"{label} output must be the frozen FLAC container/codec")


def _check_optional_frozen_sha256(
    record: Mapping[str, Any],
    field_name: str,
    expected: str | None,
    label: str,
) -> None:
    observed = record[field_name]
    if expected is None:
        if observed is not None:
            _error(
                f"{label}.{field_name} must be null for this encoder profile"
            )
        return
    if _sha256(observed, f"{label}.{field_name}") != expected:
        _error(f"{label}.{field_name} is not frozen")


def _check_optional_frozen_string(
    record: Mapping[str, Any],
    field_name: str,
    expected: str | None,
    label: str,
) -> None:
    observed = record[field_name]
    if expected is None:
        if observed is not None:
            _error(
                f"{label}.{field_name} must be null for this encoder profile"
            )
        return
    if _string(observed, f"{label}.{field_name}") != expected:
        _error(f"{label}.{field_name} is not frozen")


def _check_receipt_encoder(
    record: Mapping[str, Any],
    request: _Request,
    source: _Source,
    label: str,
) -> None:
    expected = _expected_encoder(request, source)
    observed_profile = _string(
        _field(record, "encoder_profile", label),
        f"{label}.encoder_profile",
    )
    if observed_profile != expected.profile:
        _error(f"{label}.encoder_profile does not match request family")
    observed_version = _string(
        _field(record, "encoder_version", label),
        f"{label}.encoder_version",
    )
    if observed_version != expected.version:
        _error(f"{label}.encoder_version does not match frozen artifact")
    contract_sha256 = _sha256(
        _field(record, "encoder_contract_sha256", label),
        f"{label}.encoder_contract_sha256",
    )
    if contract_sha256 != _ENCODER_CONTRACT_SHA256:
        _error(f"{label}.encoder_contract_sha256 is not frozen")
    command_sha256 = _sha256(
        _field(record, "encoder_command_sha256", label),
        f"{label}.encoder_command_sha256",
    )
    if command_sha256 != expected.command_sha256:
        _error(f"{label}.encoder_command_sha256 does not match frozen command")
    input_format = _string(
        _field(record, "encoder_input_pcm_format", label),
        f"{label}.encoder_input_pcm_format",
    )
    if input_format != expected.input_pcm_format:
        _error(
            f"{label}.encoder_input_pcm_format does not preserve source subtype"
        )
    binary_sha256 = _sha256(
        record["encoder_binary_sha256"],
        f"{label}.encoder_binary_sha256",
    )
    if binary_sha256 != expected.binary_sha256:
        _error(f"{label}.encoder_binary_sha256 is not frozen")
    _check_optional_frozen_sha256(
        record,
        "encoder_python_artifact_sha256",
        expected.python_artifact_sha256,
        label,
    )
    _check_optional_frozen_sha256(
        record,
        "encoder_cffi_artifact_sha256",
        expected.cffi_artifact_sha256,
        label,
    )
    _check_optional_frozen_string(
        record,
        "encoder_embedded_libflac_version",
        expected.embedded_libflac_version,
        label,
    )
    _check_optional_frozen_string(
        record,
        "encoder_production_image_uri",
        expected.production_image_uri,
        label,
    )
    _check_optional_frozen_string(
        record,
        "encoder_production_revision",
        expected.production_revision,
        label,
    )


def _check_receipts(
    bundle: Mapping[str, Any],
    requests: Mapping[str, _Request],
    inventory: Mapping[str, _Source],
) -> None:
    raw_receipts = _list(
        _field(bundle, "materialization_receipts", "bundle"),
        "materialization_receipts",
    )
    receipts = _unique_by_id(
        raw_receipts,
        id_name="request_id",
        label="materialization_receipts",
    )
    request_ids = set(requests)
    receipt_ids = set(receipts)
    if request_ids != receipt_ids:
        _error(
            "materialization receipt/request mismatch; "
            f"missing={sorted(request_ids - receipt_ids)[:5]}, "
            f"extra={sorted(receipt_ids - request_ids)[:5]}"
        )
    for request_id, record in receipts.items():
        request = requests[request_id]
        label = f"materialization receipt {request_id}"
        if set(record) != _MATERIALIZATION_RECEIPT_KEYS:
            missing = sorted(_MATERIALIZATION_RECEIPT_KEYS - set(record))
            extra = sorted(set(record) - _MATERIALIZATION_RECEIPT_KEYS)
            _error(f"{label} schema mismatch; missing={missing}, extra={extra}")
        source = inventory.get(request.source_uri)
        if source is None:
            _error(f"{label} source is absent from source inventory")
        _check_receipt_media(record, request, source, label)
        _check_receipt_encoder(record, request, source, label)


def _canonical_json_bytes(value: object, label: str) -> bytes:
    try:
        rendered = json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
    except (TypeError, ValueError) as error:
        _error(f"{label} is not canonical-JSON serializable: {error}")
    return rendered.encode("utf-8") + b"\n"


def _canonical_jsonl_bytes(
    rows: Sequence[Mapping[str, Any]],
    label: str,
) -> bytes:
    return b"".join(
        _canonical_json_bytes(dict(row), f"{label}[{index}]")
        for index, row in enumerate(rows)
    )


def _actual_rows(
    bundle: Mapping[str, Any],
    field_name: str,
    *,
    allow_empty: bool = False,
) -> list[Mapping[str, Any]]:
    raw_rows = _list(_field(bundle, field_name, "bundle"), field_name)
    if not allow_empty and not raw_rows:
        _error(f"{field_name} must not be empty")
    rows = [
        _mapping(raw_row, f"{field_name}[{index}]")
        for index, raw_row in enumerate(raw_rows)
    ]
    _canonical_jsonl_bytes(rows, field_name)
    return rows


def _identity(value: object, label: str) -> str:
    return _string(value, label).strip()


def _optional_identity(value: object, label: str) -> str | None:
    if value is None:
        return None
    return _identity(value, label)


def _primary_request_id(
    row: Mapping[str, Any],
    label: str,
) -> str:
    explicit = _optional_identity(row.get("request_id"), f"{label}.request_id")
    if explicit is not None:
        return explicit
    example_id = _optional_identity(
        row.get("example_id"),
        f"{label}.example_id",
    )
    segment_id = _optional_identity(
        row.get("segment_id"),
        f"{label}.segment_id",
    )
    if example_id is None or segment_id is None:
        _error(f"{label} requires request_id or example_id plus segment_id")
    return f"{example_id}::{segment_id}"


def _primary_source_uri(
    row: Mapping[str, Any],
    label: str,
) -> str:
    source_audio = row.get("source_audio")
    if isinstance(source_audio, Mapping):
        value = source_audio.get("audio_filepath")
        if value is None:
            value = source_audio.get("original_audio_uri")
        if value is not None:
            return _identity(value, f"{label}.source_audio URI")
    if row.get("original_audio_uri") is not None:
        return _identity(
            row["original_audio_uri"],
            f"{label}.original_audio_uri",
        )
    _error(f"{label} lacks an exact original source URI")


def _nonnegative_decimal(value: object, label: str) -> decimal.Decimal:
    if isinstance(value, bool):
        _error(f"{label} must be numeric")
    try:
        result = decimal.Decimal(str(value))
    except decimal.InvalidOperation:
        _error(f"{label} must be numeric")
    if not result.is_finite() or result < 0:
        _error(f"{label} must be finite and non-negative")
    return result


def _primary_order_key(
    row: Mapping[str, Any],
    request_id: str,
    label: str,
) -> tuple[object, ...]:
    request_order = row.get("request_order")
    if request_order is not None:
        order = _integer(
            request_order,
            f"{label}.request_order",
            minimum=0,
        )
        return (0, decimal.Decimal(order), request_id)
    source_audio = row.get("source_audio")
    if isinstance(source_audio, Mapping):
        start_frame = source_audio.get("start_frame")
        if start_frame is not None:
            start = _integer(
                start_frame,
                f"{label}.source_audio.start_frame",
                minimum=0,
            )
            return (1, decimal.Decimal(start), request_id)
        offset = source_audio.get("offset")
        if offset is not None:
            return (
                2,
                _nonnegative_decimal(
                    offset,
                    f"{label}.source_audio.offset",
                ),
                request_id,
            )
    start_frame = row.get("start_frame")
    if start_frame is not None:
        start = _integer(
            start_frame,
            f"{label}.start_frame",
            minimum=0,
        )
        return (3, decimal.Decimal(start), request_id)
    if row.get("offset") is not None:
        return (
            4,
            _nonnegative_decimal(row["offset"], f"{label}.offset"),
            request_id,
        )
    _error(f"{label} lacks request_order, start_frame, or offset for ordering")


def _check_primary_row_binding(
    row: Mapping[str, Any],
    *,
    index: int,
    request: _Request,
) -> None:
    label = f"primary_eval_rows[{index}]"
    split = row.get("split")
    if split not in {None, "eval"}:
        _error(f"{label}.split contradicts its eval role")
    audio_uri = _identity(
        _field(row, "audio_filepath", label),
        f"{label}.audio_filepath",
    )
    if audio_uri == request.source_uri:
        _error(
            f"{label}.audio_filepath must identify materialized request audio"
        )
    source_uri = _primary_source_uri(row, label)
    if source_uri != request.source_uri:
        _error(f"{label} source does not match reconstructed request")
    source_lineage = _mapping(
        _field(row, "source_lineage", label),
        f"{label}.source_lineage",
    )
    expected_episode_id = (
        request.request_id if request.family == "echo" else request.source_uri
    )
    explicit_episode_id = _identity(
        _field(row, "context_episode_id", label),
        f"{label}.context_episode_id",
    )
    observed_episode_id = _identity(
        _field(
            source_lineage,
            "context_episode_id",
            f"{label}.source_lineage",
        ),
        f"{label}.source_lineage.context_episode_id",
    )
    if (
        explicit_episode_id != expected_episode_id
        or observed_episode_id != expected_episode_id
    ):
        _error(
            f"{label} context_episode_id does not match family request "
            "semantics"
        )
    if _text(_field(row, "text", label), f"{label}.text") != request.text:
        _error(f"{label}.text does not match reconstructed request")

    source_audio = _mapping(
        _field(row, "source_audio", label),
        f"{label}.source_audio",
    )
    for field_name, expected in (
        ("start_frame", request.start_frame),
        ("end_frame", request.end_frame),
        ("sample_rate", request.sample_rate),
    ):
        observed = _integer(
            _field(source_audio, field_name, f"{label}.source_audio"),
            f"{label}.source_audio.{field_name}",
            minimum=0 if field_name == "start_frame" else 1,
        )
        if observed != expected:
            _error(f"{label}.source_audio.{field_name} does not match request")
    raw_owner_ids = _list(
        _field(row, "owner_ids", label),
        f"{label}.owner_ids",
    )
    owner_ids = tuple(
        _identity(value, f"{label}.owner_ids[{owner_index}]")
        for owner_index, value in enumerate(raw_owner_ids)
    )
    if owner_ids != request.owner_ids:
        _error(f"{label}.owner_ids do not match reconstructed request")


def _check_primary_and_validation(
    bundle: Mapping[str, Any],
    requests: Mapping[str, _Request],
) -> tuple[list[Mapping[str, Any]], list[Mapping[str, Any]]]:
    primary_rows = _actual_rows(bundle, "primary_eval_rows")
    validation_rows = _actual_rows(bundle, "validation_rows")
    primary_payload = _canonical_jsonl_bytes(
        primary_rows,
        "primary_eval_rows",
    )
    validation_payload = _canonical_jsonl_bytes(
        validation_rows,
        "validation_rows",
    )
    eval_requests = {
        request_id: request
        for request_id, request in requests.items()
        if request.split == "eval"
    }
    row_ids: list[str] = []
    seen_row_ids: set[str] = set()
    for index, row in enumerate(primary_rows):
        request_id = _primary_request_id(
            row,
            f"primary_eval_rows[{index}]",
        )
        if request_id in seen_row_ids:
            _error(f"duplicate primary eval request ID: {request_id}")
        row_ids.append(request_id)
        seen_row_ids.add(request_id)
        request = eval_requests.get(request_id)
        if request is None:
            _error(
                f"primary eval row references unknown eval request {request_id}"
            )
        _check_primary_row_binding(row, index=index, request=request)
    if set(row_ids) != set(eval_requests):
        _error(
            "primary eval/request mismatch; "
            f"missing={sorted(set(eval_requests) - set(row_ids))[:5]}, "
            f"extra={sorted(set(row_ids) - set(eval_requests))[:5]}"
        )

    digests = _mapping(
        _field(bundle, "manifest_sha256", "bundle"),
        "manifest_sha256",
    )
    primary = _sha256(
        _field(
            digests,
            "reconstructed_primary",
            "manifest_sha256",
        ),
        "manifest_sha256.reconstructed_primary",
    )
    validation = _sha256(
        _field(digests, "validation", "manifest_sha256"),
        "manifest_sha256.validation",
    )
    observed_primary = hashlib.sha256(primary_payload).hexdigest()
    observed_validation = hashlib.sha256(validation_payload).hexdigest()
    if primary != observed_primary or validation != observed_validation:
        _error(
            "manifest SHA-256 metadata is not bound to actual "
            "primary/validation rows"
        )
    return primary_rows, validation_rows


_DURATION_BUCKETS = ("<2", "2-<5", "5-<15", ">=15")
_WORD_BUCKETS = ("1-3", "4-9", "10-16", "17-24", "25-37", ">=38")


def _provider_frame_geometry(
    row: Mapping[str, Any],
    label: str,
) -> tuple[int, int, int]:
    source_audio = _mapping(
        _field(row, "source_audio", label),
        f"{label}.source_audio",
    )
    start_frame = _integer(
        _field(source_audio, "start_frame", f"{label}.source_audio"),
        f"{label}.source_audio.start_frame",
        minimum=0,
    )
    end_frame = _integer(
        _field(source_audio, "end_frame", f"{label}.source_audio"),
        f"{label}.source_audio.end_frame",
        minimum=1,
    )
    sample_rate = _integer(
        _field(source_audio, "sample_rate", f"{label}.source_audio"),
        f"{label}.source_audio.sample_rate",
        minimum=1,
    )
    if end_frame <= start_frame:
        _error(f"{label} has non-positive exact-frame duration")
    return start_frame, end_frame, sample_rate


def _provider_duration_bucket(
    row: Mapping[str, Any],
    label: str,
) -> str:
    start_frame, end_frame, sample_rate = _provider_frame_geometry(row, label)
    duration = fractions.Fraction(end_frame - start_frame, sample_rate)
    if duration < 2:
        return _DURATION_BUCKETS[0]
    if duration < 5:
        return _DURATION_BUCKETS[1]
    if duration < 15:
        return _DURATION_BUCKETS[2]
    return _DURATION_BUCKETS[3]


def _provider_word_bucket(value: object, label: str) -> str:
    text = _text(value, label)
    count = len(text.split())
    if count <= 3:
        return _WORD_BUCKETS[0]
    if count <= 9:
        return _WORD_BUCKETS[1]
    if count <= 16:
        return _WORD_BUCKETS[2]
    if count <= 24:
        return _WORD_BUCKETS[3]
    if count <= 37:
        return _WORD_BUCKETS[4]
    return _WORD_BUCKETS[5]


def _provider_source_uri(row: Mapping[str, Any], label: str) -> str:
    source_audio = row.get("source_audio")
    if isinstance(source_audio, Mapping):
        value = source_audio.get("audio_filepath")
        if value is None:
            value = source_audio.get("original_audio_uri")
        if value is not None:
            return _identity(value, f"{label}.source_audio URI")
    return _identity(
        _field(row, "original_audio_uri", label),
        f"{label}.original_audio_uri",
    )


def _provider_stable_row_id(
    row: Mapping[str, Any],
    label: str,
) -> str:
    example_id = _identity(
        _field(row, "example_id", label),
        f"{label}.example_id",
    )
    segment_id = _identity(
        _field(row, "segment_id", label),
        f"{label}.segment_id",
    )
    start_frame, end_frame, sample_rate = _provider_frame_geometry(row, label)
    identity = [
        example_id,
        segment_id,
        _provider_source_uri(row, label),
        start_frame,
        end_frame,
        sample_rate,
    ]
    payload = json.dumps(
        identity,
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode()
    return hashlib.sha256(payload).hexdigest()


def _provider_stratum_key(
    row: Mapping[str, Any],
    label: str,
) -> tuple[str, str, str]:
    dataset = _mapping(_field(row, "dataset", label), f"{label}.dataset")
    dataset_name = _identity(
        _field(dataset, "name", f"{label}.dataset"),
        f"{label}.dataset.name",
    )
    return (
        dataset_name,
        _provider_duration_bucket(row, label),
        _provider_word_bucket(_field(row, "text", label), f"{label}.text"),
    )


def _largest_remainder_quotas(
    stratum_sizes: Mapping[tuple[str, str, str], int],
    target_count: int,
) -> dict[tuple[str, str, str], int]:
    population = sum(stratum_sizes.values())
    quotas: dict[tuple[str, str, str], int] = {}
    remainders: list[tuple[int, tuple[str, str, str]]] = []
    for key in sorted(stratum_sizes):
        floor, remainder = divmod(
            stratum_sizes[key] * target_count,
            population,
        )
        quotas[key] = floor
        remainders.append((remainder, key))
    unassigned = target_count - sum(quotas.values())
    for _remainder, key in sorted(
        remainders,
        key=lambda item: (-item[0], item[1]),
    ):
        if unassigned == 0:
            break
        if quotas[key] < stratum_sizes[key]:
            quotas[key] += 1
            unassigned -= 1
    if unassigned:
        _error("provider-validation largest-remainder allocation is incomplete")
    return quotas


def _provider_target(eval_count: int, train_count: int) -> int:
    train_limited = max(1_000, train_count * 3 // 10)
    return min(eval_count, 5_000, train_limited)


def _expected_provider_validation(
    primary_rows: Sequence[Mapping[str, Any]],
    *,
    train_count: int,
) -> tuple[
    list[Mapping[str, Any]],
    list[str],
    list[dict[str, object]],
    str,
]:
    primary_payload = _canonical_jsonl_bytes(
        primary_rows,
        "primary_eval_rows",
    )
    salt = hashlib.sha256(primary_payload).hexdigest()
    records: list[dict[str, object]] = []
    strata: dict[tuple[str, str, str], list[dict[str, object]]] = defaultdict(
        list
    )
    for primary_index, row in enumerate(primary_rows):
        label = f"primary_eval_rows[{primary_index}]"
        stable_id = _provider_stable_row_id(row, label)
        source_uri = _provider_source_uri(row, label)
        rank = hashlib.sha256(
            f"{salt}\0{source_uri}\0{stable_id}".encode()
        ).hexdigest()
        stratum_key = _provider_stratum_key(row, label)
        record: dict[str, object] = {
            "primary_index": primary_index,
            "rank": rank,
            "source_uri": source_uri,
            "stable_id": stable_id,
            "stratum": stratum_key,
        }
        records.append(record)
        strata[stratum_key].append(record)

    target_count = _provider_target(len(primary_rows), train_count)
    stratum_sizes = {key: len(rows) for key, rows in sorted(strata.items())}
    quotas = _largest_remainder_quotas(stratum_sizes, target_count)
    selected_records: list[dict[str, object]] = []
    for key in sorted(strata):
        by_source: dict[str, list[dict[str, object]]] = defaultdict(list)
        for record in strata[key]:
            by_source[str(record["source_uri"])].append(record)
        for source_rows in by_source.values():
            source_rows.sort(
                key=lambda record: (
                    str(record["rank"]),
                    str(record["stable_id"]),
                )
            )
        ranked_sources = sorted(
            by_source,
            key=lambda source: (
                min(str(record["rank"]) for record in by_source[source]),
                source,
            ),
        )
        quota = quotas[key]
        round_index = 0
        chosen_in_stratum = 0
        while chosen_in_stratum < quota:
            rows_added = 0
            for source in ranked_sources:
                source_rows = by_source[source]
                if round_index >= len(source_rows):
                    continue
                selected_records.append(source_rows[round_index])
                chosen_in_stratum += 1
                rows_added += 1
                if chosen_in_stratum == quota:
                    break
            if rows_added == 0:
                _error("provider-validation source round-robin exhausted early")
            round_index += 1
    if len(selected_records) != target_count:
        _error("provider-validation selector produced the wrong row count")
    selected_indices = [
        int(record["primary_index"]) for record in selected_records
    ]
    strata_receipt: list[dict[str, object]] = []
    for key in sorted(strata):
        population_records = strata[key]
        chosen_records = [
            record for record in selected_records if record["stratum"] == key
        ]
        population_sources = Counter(
            str(record["source_uri"]) for record in population_records
        )
        chosen_sources = Counter(
            str(record["source_uri"]) for record in chosen_records
        )
        strata_receipt.append(
            {
                "chosen_stable_ids": [
                    str(record["stable_id"]) for record in chosen_records
                ],
                "dataset": key[0],
                "duration_bucket": key[1],
                "population_size": len(population_records),
                "quota": quotas[key],
                "sources": [
                    {
                        "chosen_count": chosen_sources[source],
                        "population_size": population_sources[source],
                        "source_uri": source,
                    }
                    for source in sorted(population_sources)
                ],
                "word_bucket": key[2],
            }
        )
    return (
        [primary_rows[index] for index in selected_indices],
        [str(record["stable_id"]) for record in selected_records],
        strata_receipt,
        salt,
    )


def _check_provider_validation(
    bundle: Mapping[str, Any],
    *,
    primary_rows: Sequence[Mapping[str, Any]],
    validation_rows: Sequence[Mapping[str, Any]],
    train_count: int,
) -> None:
    provider_rows = _actual_rows(bundle, "provider_validation_rows")
    provider_payload = _canonical_jsonl_bytes(
        provider_rows,
        "provider_validation_rows",
    )
    validation_payload = _canonical_jsonl_bytes(
        validation_rows,
        "validation_rows",
    )
    if provider_payload != validation_payload:
        _error(
            "canonical validation rows are not byte-identical to the actual "
            "provider-validation projection"
        )
    (
        expected_rows,
        selected_stable_ids,
        strata_receipt,
        salt,
    ) = _expected_provider_validation(primary_rows, train_count=train_count)
    expected_payload = _canonical_jsonl_bytes(
        expected_rows,
        "expected provider-validation rows",
    )
    selected_request_ids = [
        _primary_request_id(
            row,
            f"provider_validation_rows[{index}]",
        )
        for index, row in enumerate(provider_rows)
    ]
    if len(selected_request_ids) != len(set(selected_request_ids)):
        _error("provider-validation projection contains duplicate requests")
    if provider_payload != expected_payload:
        _error(
            "provider-validation rows are not the exact production-shape "
            "selector using the July12 stratification/source-round-robin "
            "precedent"
        )

    receipt = _mapping(
        _field(bundle, "provider_validation_receipt", "bundle"),
        "provider_validation_receipt",
    )
    primary_payload = _canonical_jsonl_bytes(
        primary_rows,
        "primary_eval_rows",
    )
    expected_receipt: dict[str, object] = {
        "algorithm": (
            "production_shape_dataset_duration_word_"
            "largest_remainder_source_round_robin_v1"
        ),
        "cap": 5_000,
        "chosen_stable_ids": selected_stable_ids,
        "duration_buckets": list(_DURATION_BUCKETS),
        "full_primary_count": len(primary_rows),
        "full_primary_sha256": hashlib.sha256(primary_payload).hexdigest(),
        "input_lock_digest": salt,
        "max_train_fraction": "3/10",
        "model_outcome_fields_used": [],
        "precedent": (
            "20260712 dataset-duration-word Hamilton plus "
            "hash-ranked source round-robin"
        ),
        "precedent_adaptations": [
            "salt_is_full_primary_eval_sha256",
            "timing_identity_uses_exact_source_frames",
        ],
        "schema_version": 1,
        "selected_count": len(provider_rows),
        "selected_request_ids": selected_request_ids,
        "selected_request_ids_sha256": hashlib.sha256(
            _canonical_json_bytes(
                selected_request_ids,
                "provider-validation selected request IDs",
            )
        ).hexdigest(),
        "selected_rows_sha256": hashlib.sha256(provider_payload).hexdigest(),
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
        "target_count": _provider_target(len(primary_rows), train_count),
        "train_count": train_count,
        "word_buckets": list(_WORD_BUCKETS),
    }
    if dict(receipt) != expected_receipt:
        _error(
            "provider-validation receipt is not fully bound to the primary "
            "manifest and independently recomputed production-shape selector "
            "using the July12 stratification/source-round-robin precedent"
        )


def _contains_reference_key(value: object) -> bool:
    if isinstance(value, Mapping):
        for raw_key, child in value.items():
            if str(raw_key).strip().casefold() in _REFERENCE_KEYS:
                return True
            if _contains_reference_key(child):
                return True
    elif isinstance(value, (list, tuple)):
        return any(_contains_reference_key(child) for child in value)
    return False


def _expected_prior_schedule(
    primary_rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, object]]:
    grouped: dict[
        str,
        list[tuple[tuple[object, ...], dict[str, object]]],
    ] = defaultdict(list)
    for manifest_index, row in enumerate(primary_rows):
        label = f"primary_eval_rows[{manifest_index}]"
        request_id = _primary_request_id(row, label)
        source_uri = _primary_source_uri(row, label)
        dataset = _mapping(_field(row, "dataset", label), f"{label}.dataset")
        family = _normalize_masked_family(
            _field(dataset, "family", f"{label}.dataset"),
            f"{label}.dataset.family",
        )
        context_episode_id = request_id if family == "echo" else source_uri
        record: dict[str, object] = {
            "audio_uri": _identity(
                _field(row, "audio_filepath", label),
                f"{label}.audio_filepath",
            ),
            "context_episode_id": context_episode_id,
            "example_id": _optional_identity(
                row.get("example_id"),
                f"{label}.example_id",
            ),
            "manifest_index": manifest_index,
            "request_id": request_id,
            "segment_id": _optional_identity(
                row.get("segment_id"),
                f"{label}.segment_id",
            ),
            "source_ordinal": -1,
            "source_uri": source_uri,
        }
        grouped[context_episode_id].append(
            (_primary_order_key(row, request_id, label), record)
        )

    expected: list[dict[str, object]] = []
    for context_episode_id in sorted(grouped):
        ordered = sorted(grouped[context_episode_id], key=lambda item: item[0])
        for source_ordinal, (_order_key, record) in enumerate(ordered):
            expected.append(
                {
                    **record,
                    "source_ordinal": source_ordinal,
                }
            )
    return expected


def _check_prior_context_schedule(
    bundle: Mapping[str, Any],
    primary_rows: Sequence[Mapping[str, Any]],
    prior_lane: Mapping[str, Any],
) -> None:
    schedule_rows = _actual_rows(bundle, "prior_context_schedule_rows")
    normalized: list[dict[str, object]] = []
    for index, row in enumerate(schedule_rows):
        label = f"prior_context_schedule_rows[{index}]"
        if _contains_reference_key(row):
            _error(f"{label} contains a reference transcription field")
        if set(row) != _SCHEDULE_KEYS:
            _error(
                f"{label} must contain only the frozen reference-free "
                f"schedule fields; got {sorted(row)}"
            )
        normalized.append(dict(row))

    expected = _expected_prior_schedule(primary_rows)
    if normalized != expected:
        _error(
            "prior-context-10 schedule does not have exact one-to-one "
            "primary request coverage/order/context-episode resets"
        )
    payload = _canonical_jsonl_bytes(
        schedule_rows,
        "prior_context_schedule_rows",
    )
    observed_sha256 = hashlib.sha256(payload).hexdigest()
    if prior_lane.get("request_count") != len(schedule_rows):
        _error("prior-context-10 request_count does not match actual schedule")
    if prior_lane.get("schedule_sha256") != observed_sha256:
        _error("prior-context-10 schedule SHA-256 does not match actual rows")


def _check_eval_lanes(
    bundle: Mapping[str, Any],
    primary_rows: Sequence[Mapping[str, Any]],
) -> None:
    lanes = _mapping(
        _field(bundle, "eval_lanes", "bundle"),
        "eval_lanes",
    )
    lane_names = set(lanes)
    if lane_names != EXPECTED_EVAL_LANES:
        _error(
            "eval lanes must be exactly reconstructed_primary, "
            "predicted_prior_context_10, and masked_v2; "
            f"got {sorted(lane_names)}"
        )
    for name in EXPECTED_EVAL_LANES:
        _mapping(lanes[name], f"eval_lanes.{name}")
    prior = _mapping(
        lanes["predicted_prior_context_10"],
        "eval_lanes.predicted_prior_context_10",
    )
    if prior.get("base_lane") != "reconstructed_primary":
        _error("prior-context-10 must derive from reconstructed_primary")
    if prior.get("max_prior_segments") != 10:
        _error("prior-context-10 must use up to exactly 10 prior segments")
    if prior.get("prior_transcript_source") not in {
        "prediction",
        "predicted_transcription",
    }:
        _error(
            "prior-context-10 must use predicted transcriptions, never "
            "reference transcriptions"
        )
    if prior.get("uses_reference_transcription", False) is not False:
        _error("prior-context-10 must not use reference transcriptions")
    primary_lane = _mapping(
        lanes["reconstructed_primary"],
        "eval_lanes.reconstructed_primary",
    )
    primary_payload = _canonical_jsonl_bytes(
        primary_rows,
        "primary_eval_rows",
    )
    primary_sha256 = hashlib.sha256(primary_payload).hexdigest()
    if primary_lane.get("row_count") != len(primary_rows):
        _error("reconstructed primary row_count does not match actual rows")
    if primary_lane.get("sha256") != primary_sha256:
        _error("reconstructed primary SHA-256 does not match actual rows")
    masked = _mapping(lanes["masked_v2"], "eval_lanes.masked_v2")
    if masked.get("rows_resegmented") is not False:
        _error("masked-v2 retained rows must not be resegmented")
    _check_prior_context_schedule(bundle, primary_rows, prior)


def _identifier_set(
    record: Mapping[str, Any],
    field_name: str,
    label: str,
) -> set[str]:
    if field_name not in record:
        return set()
    raw_values = _list(record[field_name], f"{label}.{field_name}")
    return {
        _string(value, f"{label}.{field_name}[{index}]")
        for index, value in enumerate(raw_values)
    }


def _normalize_masked_family(value: object, label: str) -> str:
    raw = _identity(value, label)
    normalized = raw.casefold().replace("-", "_").replace(" ", "_")
    aliases = {
        "bcfy_calls": "bcfy_calls",
        "bcfy_feeds": "bcfy_feeds",
        "broadcastify_calls": "bcfy_calls",
        "broadcastify_feeds": "bcfy_feeds",
        "echo": "echo",
        "fire_notification": "fire_notifications",
        "fire_notifications": "fire_notifications",
    }
    family = aliases.get(normalized)
    if family is None:
        _error(f"{label} has unsupported masked dataset family {raw!r}")
    return family


def _check_masked_row_family(
    row: Mapping[str, Any],
    *,
    expected: str,
    label: str,
) -> None:
    declared: list[str] = []
    for key in ("dataset_family", "dataset_name", "source_dataset"):
        if row.get(key) is not None:
            declared.append(
                _normalize_masked_family(row[key], f"{label}.{key}")
            )
    dataset = row.get("dataset")
    if isinstance(dataset, Mapping):
        for key in ("family", "name"):
            if dataset.get(key) is not None:
                declared.append(
                    _normalize_masked_family(
                        dataset[key],
                        f"{label}.dataset.{key}",
                    )
                )
    if not declared:
        _error(f"{label} lacks masked dataset-family metadata")
    if any(family != expected for family in declared):
        _error(f"{label} family metadata does not match {expected}")


def _masked_identity(
    row: Mapping[str, Any],
    label: str,
) -> tuple[str, str]:
    return (
        _identity(_field(row, "example_id", label), f"{label}.example_id"),
        _identity(_field(row, "segment_id", label), f"{label}.segment_id"),
    )


def _masked_receipts_by_family(
    bundle: Mapping[str, Any],
) -> tuple[Mapping[str, Any], dict[str, Mapping[str, Any]]]:
    receipt = _mapping(
        _field(bundle, "masked_filter_receipt", "bundle"),
        "masked_filter_receipt",
    )
    raw_families = _list(
        _field(receipt, "families", "masked_filter_receipt"),
        "masked_filter_receipt.families",
    )
    families: dict[str, Mapping[str, Any]] = {}
    observed_order: list[str] = []
    for index, raw_family in enumerate(raw_families):
        label = f"masked_filter_receipt.families[{index}]"
        family_receipt = _mapping(raw_family, label)
        family = _normalize_masked_family(
            _field(family_receipt, "family", label),
            f"{label}.family",
        )
        if family in families:
            _error(f"duplicate masked family receipt: {family}")
        observed_order.append(family)
        families[family] = family_receipt
    if tuple(observed_order) != MASKED_DATASET_FAMILIES:
        _error(
            "masked family receipts must contain the exact four sources in "
            "canonical order"
        )
    return receipt, families


def _check_masked_rows(  # noqa: PLR0912, PLR0915
    bundle: Mapping[str, Any],
    groups: Mapping[str, str],
    train_groups: set[str],
    train_sources: set[str],
    owners: Mapping[str, _Owner],
) -> int:
    raw_rows_by_family = _mapping(
        _field(bundle, "masked_rows_by_family", "bundle"),
        "masked_rows_by_family",
    )
    if set(raw_rows_by_family) != set(MASKED_DATASET_FAMILIES):
        _error(
            "masked_rows_by_family must contain exactly bcfy_calls, "
            "bcfy_feeds, echo, and fire_notifications"
        )
    filter_receipt, receipts_by_family = _masked_receipts_by_family(bundle)
    flattened: list[tuple[str, Mapping[str, Any], str, str, str]] = []
    masked_identities: set[tuple[str, str, str]] = set()
    total_input = 0
    total_excluded = 0
    for family in MASKED_DATASET_FAMILIES:
        rows = _list(
            raw_rows_by_family[family],
            f"masked_rows_by_family.{family}",
        )
        family_receipt = receipts_by_family[family]
        kept_receipts = _list(
            _field(
                family_receipt,
                "kept_rows",
                f"masked family receipt {family}",
            ),
            f"masked family receipt {family}.kept_rows",
        )
        kept_count = _integer(
            _field(
                family_receipt,
                "kept_count",
                f"masked family receipt {family}",
            ),
            f"masked family receipt {family}.kept_count",
            minimum=0,
        )
        input_count = _integer(
            _field(
                family_receipt,
                "input_count",
                f"masked family receipt {family}",
            ),
            f"masked family receipt {family}.input_count",
            minimum=0,
        )
        excluded_count = _integer(
            _field(
                family_receipt,
                "excluded_count",
                f"masked family receipt {family}",
            ),
            f"masked family receipt {family}.excluded_count",
            minimum=0,
        )
        if (
            kept_count != len(rows)
            or len(kept_receipts) != len(rows)
            or input_count != kept_count + excluded_count
        ):
            _error(f"masked family {family} count receipt does not match rows")
        if family_receipt.get("rows_resegmented") is not False:
            _error(f"masked family {family} rows must remain unmodified")
        total_input += input_count
        total_excluded += excluded_count
        for row_index, (raw_row, raw_kept_receipt) in enumerate(
            zip(rows, kept_receipts, strict=True)
        ):
            label = f"masked_rows_by_family.{family}[{row_index}]"
            row = _mapping(raw_row, label)
            _check_masked_row_family(
                row,
                expected=family,
                label=label,
            )
            if row.get("split") != "eval":
                _error(f"{label}.split must be eval")
            example_id, segment_id = _masked_identity(row, label)
            identity = (family, example_id, segment_id)
            if identity in masked_identities:
                _error(f"duplicate masked kept-row identity: {identity}")
            masked_identities.add(identity)
            payload_sha256 = hashlib.sha256(
                _canonical_json_bytes(dict(row), label)
            ).hexdigest()
            kept_receipt = _mapping(
                raw_kept_receipt,
                f"masked family {family} kept_rows[{row_index}]",
            )
            expected_receipt = {
                "example_id": example_id,
                "row_sha256": payload_sha256,
                "segment_id": segment_id,
            }
            if dict(kept_receipt) != expected_receipt:
                _error(
                    f"{label} was changed after the masked filter receipt "
                    "was frozen"
                )
            flattened.append(
                (
                    family,
                    row,
                    example_id,
                    segment_id,
                    payload_sha256,
                )
            )

    if filter_receipt.get("rows_resegmented") is not False:
        _error("masked filter receipt permits resegmentation")
    if filter_receipt.get("input_count") != total_input:
        _error("masked aggregate input_count does not match family receipts")
    if filter_receipt.get("kept_count") != len(flattened):
        _error("masked aggregate kept_count does not match actual rows")
    if filter_receipt.get("excluded_count") != total_excluded:
        _error("masked aggregate excluded_count does not match family receipts")

    _sha256(
        _field(bundle, "masked_selection_proof_sha256", "bundle"),
        "masked_selection_proof_sha256",
    )
    retained_payload_sha256 = _sha256(
        _field(bundle, "masked_retained_payload_sha256", "bundle"),
        "masked_retained_payload_sha256",
    )
    flattened_payload = _canonical_jsonl_bytes(
        [row for _family, row, _example, _segment, _digest in flattened],
        "masked retained rows",
    )
    if hashlib.sha256(flattened_payload).hexdigest() != retained_payload_sha256:
        _error(
            "masked retained payload rows do not match their frozen retained "
            "payload SHA-256"
        )

    raw_projections = _list(
        _field(bundle, "masked_row_projections", "bundle"),
        "masked_row_projections",
    )
    if len(raw_projections) != len(flattened):
        _error("masked row projection count does not match actual kept rows")
    train_provenance: set[str] = set()
    for owner in owners.values():
        if owner.split == "train":
            train_provenance.add(owner.owner_id)
            train_provenance.update(owner.constituent_ids)
    for index, (raw_projection, actual) in enumerate(
        zip(raw_projections, flattened, strict=True)
    ):
        family, actual_row, example_id, segment_id, payload_sha256 = actual
        label = f"masked_row_projections[{index}]"
        row = _mapping(raw_projection, label)
        expected_keys = {
            "family",
            "group_id",
            "payload_sha256",
            "row_id",
            "source_uri",
            "split",
        }
        if set(row) != expected_keys:
            _error(f"{label} does not have the exact frozen projection fields")
        if row.get("family") != family:
            _error(f"{label}.family does not match kept-row family")
        if row.get("row_id") != f"{family}:{example_id}:{segment_id}":
            _error(f"{label}.row_id does not match kept-row identity")
        if row.get("payload_sha256") != payload_sha256:
            _error(f"{label}.payload_sha256 does not match kept-row payload")
        source_uri = _string(
            _field(row, "source_uri", label),
            f"{label}.source_uri",
        )
        group_id = _string(
            _field(row, "group_id", label),
            f"{label}.group_id",
        )
        expected_group = groups.get(source_uri)
        if expected_group is None:
            _error(f"{label} source has no physical group: {source_uri}")
        if group_id != expected_group:
            _error(f"{label}.group_id does not match physical_groups")
        if source_uri in train_sources or group_id in train_groups:
            _error(f"{label} leaks a training physical source/group")
        if row.get("split", "eval") != "eval":
            _error(f"{label}.split must be eval when present")
        provenance: set[str] = set()
        for field_name in (
            "owner_ids",
            "constituent_ids",
            "provenance_ids",
            "transcript_provenance_ids",
        ):
            provenance.update(
                _identifier_set(actual_row, field_name, f"masked row {index}")
            )
        leaked_ids = provenance & train_provenance
        if leaked_ids:
            _error(
                f"{label} leaks training provenance: {sorted(leaked_ids)[:5]}"
            )
    lanes = _mapping(_field(bundle, "eval_lanes", "bundle"), "eval_lanes")
    masked_lane = _mapping(lanes["masked_v2"], "eval_lanes.masked_v2")
    if masked_lane.get("kept_count") != len(flattened):
        _error("masked-v2 lane kept_count does not match actual rows")
    return len(flattened)


def verify_artifacts(bundle: Mapping[str, Any]) -> VerificationReport:
    """Verify a complete frozen reconstruction bundle.

    Args:
        bundle: JSON-compatible artifact records described in the module
            docstring.

    Returns:
        Counts for the verified bundle.

    Raises:
        VerificationError: On the first violated invariant.
    """
    bundle = _mapping(bundle, "bundle")
    _check_outcome_blind_metadata(bundle)
    inventory = _parse_inventory(bundle)
    atoms = _parse_raw_atoms(bundle, inventory)
    owners = _parse_owners(bundle, inventory)
    _check_constituents(owners, atoms)
    pii_intervals = _parse_pii(bundle, inventory)
    requests = _parse_requests(bundle, inventory)
    _check_owner_coverage(owners, atoms, requests)
    _check_request_nonoverlap(requests)
    pii_only = _pii_only_by_source(owners, atoms, pii_intervals)
    _check_pii_exclusion(requests, pii_only)
    _check_presegmented_geometry(
        owners,
        requests,
        inventory,
        pii_only,
    )
    physical_groups = _parse_physical_groups(bundle)
    train_groups, train_sources, _ = _check_split_groups(
        owners,
        requests,
        physical_groups,
    )
    _check_receipts(bundle, requests, inventory)
    primary_rows, validation_rows = _check_primary_and_validation(
        bundle,
        requests,
    )
    train_request_count = sum(
        request.split == "train" for request in requests.values()
    )
    _check_provider_validation(
        bundle,
        primary_rows=primary_rows,
        validation_rows=validation_rows,
        train_count=train_request_count,
    )
    _check_eval_lanes(bundle, primary_rows)
    masked_count = _check_masked_rows(
        bundle,
        physical_groups,
        train_groups,
        train_sources,
        owners,
    )

    train_requests = train_request_count
    eval_requests = len(requests) - train_requests
    return VerificationReport(
        source_count=len(inventory),
        owner_count=len(owners),
        raw_atom_count=len(atoms),
        train_request_count=train_requests,
        eval_request_count=eval_requests,
        masked_row_count=masked_count,
    )


def verify_and_print(
    bundle: Mapping[str, Any],
    *,
    output: TextIO | None = None,
) -> VerificationReport:
    """Verify ``bundle`` and print ``COMPLETE`` only after success."""
    report = verify_artifacts(bundle)
    destination = output if output is not None else sys.stdout
    destination.write("COMPLETE\n")
    return report


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Independently verify frozen reconstruction artifacts.",
    )
    parser.add_argument(
        "bundle",
        type=pathlib.Path,
        help="Path to the JSON verification bundle.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Verify one on-disk JSON bundle."""
    arguments = _parser().parse_args(argv)
    try:
        with arguments.bundle.open(encoding="utf-8") as source:
            bundle = json.load(source)
        verify_and_print(bundle)
    except (OSError, json.JSONDecodeError, VerificationError) as error:
        sys.stderr.write(f"verification failed: {error}\n")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
