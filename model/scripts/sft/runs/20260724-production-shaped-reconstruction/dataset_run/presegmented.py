"""Reconstruct non-continuous dataset families with frozen source semantics.

BCFY Calls and Fire Notifications have authoritative provider-item source
objects.  For those families reconstruction:

* starts from the complete decoded provider item;
* lets non-empty Speech win any overlap with a targetless PII annotation;
* subtracts only the remaining PII-only frames; and
* emits every speech-bearing residual component as one continuous request.

Echo URIs are different: each URI is a legacy hour archive, not the identity
of one production provider item.  Echo therefore uses a conservative policy:
each connected component of *strictly overlapping* protected Speech intervals
is one request.  Exactly touching intervals remain separate and no positive
targetless or unannotated gap is absorbed.

Request boundaries depend only on source geometry and target presence.  Target
wording is passed to :mod:`dataset_run.transcripts` only after the boundaries
have been frozen.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Iterable, Mapping
from typing import Any

from . import domain, transcripts

PRESEGMENTED_FAMILIES = frozenset(
    {
        "bcfy_calls",
        "echo",
        "fire_notifications",
    }
)
ECHO_GEOMETRY_POLICY = "echo_conservative_speech_component"
AUTHORITATIVE_PROVIDER_ITEM_GEOMETRY_POLICY = (
    "authoritative_provider_item_minus_pii"
)

Interval = tuple[int, int]


@dataclasses.dataclass(frozen=True, slots=True)
class RequestCoverageReceipt:
    """Exact-frame coverage attributed to one emitted request."""

    request_id: str
    source_uri: str
    split: domain.Split
    geometry_policy: str
    start_frame: int
    end_frame: int
    owner_ids: tuple[str, ...]
    speech_union_frames: int
    targetless_annotation_frames: int
    unannotated_frames: int
    transcript_receipt_digest: str


@dataclasses.dataclass(frozen=True, slots=True)
class SourceTopologyReceipt:
    """Topology and conservation measurements for one source object."""

    source_uri: str
    family: str
    split: domain.Split
    geometry_policy: str
    source_frames: int
    atom_count: int
    speech_atom_count: int
    pii_atom_count: int
    targetless_atom_count: int
    owner_count: int
    speech_annotation_frames: int
    speech_union_frames: int
    speech_duplicate_annotation_frames: int
    pii_union_frames: int
    speech_pii_overlap_frames: int
    pii_only_frames: int
    targetless_union_frames: int
    targetless_effective_frames: int
    annotated_union_frames: int
    unannotated_source_frames: int
    residual_component_count: int
    emitted_request_count: int
    dropped_context_component_count: int
    emitted_request_frames: int
    retained_speech_frames: int
    retained_targetless_annotation_frames: int
    retained_unannotated_frames: int
    dropped_context_frames: int
    request_receipts: tuple[RequestCoverageReceipt, ...]


@dataclasses.dataclass(frozen=True, slots=True)
class PresegmentedReceipt:
    """Aggregate receipt for a complete presegmented reconstruction."""

    source_count: int
    atom_count: int
    owner_count: int
    request_count: int
    pii_only_frames: int
    emitted_request_frames: int
    retained_speech_frames: int
    retained_targetless_annotation_frames: int
    retained_unannotated_frames: int
    source_receipts: tuple[SourceTopologyReceipt, ...]


@dataclasses.dataclass(frozen=True, slots=True)
class TranscriptBinding:
    """Bind a transcript-module receipt to its final request identifier."""

    request_id: str
    receipt: object


@dataclasses.dataclass(frozen=True, slots=True)
class PresegmentedResult:
    """Constructed requests and their independently auditable receipts."""

    requests: tuple[domain.Request, ...]
    receipt: PresegmentedReceipt
    transcript_receipts: tuple[TranscriptBinding, ...]


def _merge_intervals(intervals: Iterable[Interval]) -> tuple[Interval, ...]:
    """Return the ordered union of half-open integer-frame intervals."""
    ordered = sorted(intervals)
    if not ordered:
        return ()
    merged: list[Interval] = []
    start, end = ordered[0]
    if start < 0 or end <= start:
        raise ValueError(f"invalid interval: {(start, end)}")
    for next_start, next_end in ordered[1:]:
        if next_start < 0 or next_end <= next_start:
            raise ValueError(f"invalid interval: {(next_start, next_end)}")
        if next_start <= end:
            end = max(end, next_end)
            continue
        merged.append((start, end))
        start, end = next_start, next_end
    merged.append((start, end))
    return tuple(merged)


def _strictly_overlapping_owner_components(
    owners: Iterable[Any],
) -> tuple[tuple[Any, ...], ...]:
    """Partition owners by strict half-open interval overlap.

    A new owner joins the current component only when its start is strictly
    before the component's current end.  Therefore ``[a, b)`` and ``[b, c)``
    are separate components even though they touch.
    """
    ordered = sorted(
        owners,
        key=lambda owner: (
            owner.atom.start_frame,
            owner.atom.end_frame,
            owner.atom.raw_index,
            owner.atom.atom_id,
        ),
    )
    if not ordered:
        return ()

    components: list[tuple[Any, ...]] = []
    current = [ordered[0]]
    current_end = ordered[0].atom.end_frame
    for owner in ordered[1:]:
        if owner.atom.start_frame < current_end:
            current.append(owner)
            current_end = max(current_end, owner.atom.end_frame)
            continue
        components.append(tuple(current))
        current = [owner]
        current_end = owner.atom.end_frame
    components.append(tuple(current))
    return tuple(components)


def _subtract_intervals(
    intervals: Iterable[Interval],
    blockers: Iterable[Interval],
) -> tuple[Interval, ...]:
    """Subtract an interval union from another interval union."""
    bases = _merge_intervals(intervals)
    removed = _merge_intervals(blockers)
    if not bases or not removed:
        return bases

    result: list[Interval] = []
    blocker_index = 0
    for base_start, base_end in bases:
        cursor = base_start
        while (
            blocker_index < len(removed) and removed[blocker_index][1] <= cursor
        ):
            blocker_index += 1
        scan_index = blocker_index
        while scan_index < len(removed) and removed[scan_index][0] < base_end:
            blocker_start, blocker_end = removed[scan_index]
            if blocker_start > cursor:
                result.append((cursor, min(blocker_start, base_end)))
            cursor = max(cursor, blocker_end)
            if cursor >= base_end:
                break
            scan_index += 1
        if cursor < base_end:
            result.append((cursor, base_end))
    return tuple(result)


def _complement(
    blocked: Iterable[Interval], end_frame: int
) -> tuple[Interval, ...]:
    """Return ``[0, end_frame)`` after subtracting ``blocked``."""
    if end_frame <= 0:
        raise ValueError("source frame count must be positive")
    result: list[Interval] = []
    cursor = 0
    for start, end in _merge_intervals(blocked):
        if start < 0 or end > end_frame:
            raise ValueError(
                f"blocked interval {(start, end)} outside source [0, {end_frame})"
            )
        if cursor < start:
            result.append((cursor, start))
        cursor = end
    if cursor < end_frame:
        result.append((cursor, end_frame))
    return tuple(result)


def _interval_length(intervals: Iterable[Interval]) -> int:
    return sum(end - start for start, end in _merge_intervals(intervals))


def _intersection_length(
    left: Iterable[Interval],
    right: Iterable[Interval],
) -> int:
    left_union = _merge_intervals(left)
    right_union = _merge_intervals(right)
    left_index = 0
    right_index = 0
    total = 0
    while left_index < len(left_union) and right_index < len(right_union):
        left_start, left_end = left_union[left_index]
        right_start, right_end = right_union[right_index]
        total += max(0, min(left_end, right_end) - max(left_start, right_start))
        if left_end <= right_end:
            left_index += 1
        else:
            right_index += 1
    return total


def _atom_class(atom: Any) -> domain.AtomClass:
    value = atom.atom_class
    if isinstance(value, domain.AtomClass):
        return value
    return domain.AtomClass(value)


def _split(owner: Any) -> domain.Split:
    value = owner.split
    if isinstance(value, domain.Split):
        return value
    return domain.Split(value)


def _media_by_uri(
    source_media: Mapping[str, Any] | Iterable[Any],
) -> dict[str, Any]:
    if isinstance(source_media, Mapping):
        result = dict(source_media)
        for key, media in result.items():
            if key != media.uri:
                raise ValueError(
                    f"source media key {key!r} does not match URI {media.uri!r}"
                )
    else:
        result = {}
        for media in source_media:
            if media.uri in result:
                raise ValueError(f"duplicate source media URI: {media.uri}")
            result[media.uri] = media
    return result


def _receipt_json_value(value: object) -> object:
    """Convert a transcript receipt to stable JSON-compatible content."""
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return _receipt_json_value(dataclasses.asdict(value))
    if isinstance(value, Mapping):
        return {
            str(key): _receipt_json_value(item)
            for key, item in sorted(
                value.items(), key=lambda pair: str(pair[0])
            )
        }
    if isinstance(value, (list, tuple)):
        return [_receipt_json_value(item) for item in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if isinstance(value, domain.Split | domain.AtomClass):
        return value.value
    raise TypeError(
        "transcript receipt must contain only dataclasses or JSON-compatible "
        f"values, got {type(value).__name__}"
    )


def _reconcile_component(owners: tuple[Any, ...]) -> tuple[str, object]:
    """Call the shared transcript reconciler after geometry is frozen."""
    reconciled = transcripts.reconcile_component(owners)
    normalized = domain.normalize_text(reconciled.text)
    if normalized is None:
        raise ValueError("transcript reconciliation returned an empty target")
    return normalized, reconciled.receipt


def _validate_owner_matches_atom(owner: Any, atom: Any) -> None:
    owned_atom = owner.atom
    comparable_fields = (
        "atom_id",
        "family",
        "source_uri",
        "raw_manifest",
        "raw_index",
        "start_frame",
        "end_frame",
        "atom_class",
    )
    for field in comparable_fields:
        owned_value = getattr(owned_atom, field)
        atom_value = getattr(atom, field)
        if field == "atom_class":
            owned_value = domain.AtomClass(owned_value)
            atom_value = domain.AtomClass(atom_value)
        if owned_value != atom_value:
            raise ValueError(
                f"owner {owned_atom.atom_id} disagrees with atom inventory "
                f"on {field}"
            )


def reconstruct_presegmented(
    *,
    atoms: Iterable[domain.Atom],
    owned_speech: Iterable[domain.OwnedSpeech],
    source_media: Mapping[str, domain.SourceMedia]
    | Iterable[domain.SourceMedia],
) -> PresegmentedResult:
    """Reconstruct Calls/Fire provider items and conservative Echo components.

    Every Speech atom must have exactly one supplied owner.  A source archive
    must be owned by one split.  Any inconsistency is fatal rather than being
    repaired by an outcome- or text-dependent heuristic.
    """
    atom_inventory = tuple(atoms)
    owners = tuple(owned_speech)
    media_inventory = _media_by_uri(source_media)

    atoms_by_id: dict[str, Any] = {}
    atoms_by_source: dict[str, list[Any]] = {}
    for atom in atom_inventory:
        if atom.atom_id in atoms_by_id:
            raise ValueError(f"duplicate atom id: {atom.atom_id}")
        atoms_by_id[atom.atom_id] = atom
        atoms_by_source.setdefault(atom.source_uri, []).append(atom)

    owners_by_id: dict[str, Any] = {}
    owners_by_source: dict[str, list[Any]] = {}
    for owner in owners:
        atom_id = owner.atom.atom_id
        if atom_id in owners_by_id:
            raise ValueError(f"duplicate speech owner: {atom_id}")
        if atom_id not in atoms_by_id:
            raise ValueError(
                f"speech owner absent from atom inventory: {atom_id}"
            )
        inventory_atom = atoms_by_id[atom_id]
        _validate_owner_matches_atom(owner, inventory_atom)
        if _atom_class(inventory_atom) is not domain.AtomClass.SPEECH:
            raise ValueError(f"owner does not reference Speech: {atom_id}")
        if domain.normalize_text(owner.text) is None:
            raise ValueError(f"speech owner has empty target: {atom_id}")
        owners_by_id[atom_id] = owner
        owners_by_source.setdefault(inventory_atom.source_uri, []).append(owner)

    speech_ids = {
        atom.atom_id
        for atom in atom_inventory
        if _atom_class(atom) is domain.AtomClass.SPEECH
    }
    if speech_ids != set(owners_by_id):
        missing = sorted(speech_ids - set(owners_by_id))
        extra = sorted(set(owners_by_id) - speech_ids)
        raise ValueError(
            "Speech ownership must be exact; "
            f"missing={missing[:10]}, extra={extra[:10]}"
        )

    requests: list[domain.Request] = []
    transcript_bindings: list[TranscriptBinding] = []
    source_receipts: list[SourceTopologyReceipt] = []
    emitted_owner_ids: list[str] = []

    for source_uri in sorted(atoms_by_source):
        if source_uri not in media_inventory:
            raise ValueError(f"missing source media inventory: {source_uri}")
        media = media_inventory[source_uri]
        if media.frame_count <= 0:
            raise ValueError(
                f"source has non-positive frame count: {source_uri}"
            )
        if media.sample_rate <= 0:
            raise ValueError(
                f"source has non-positive sample rate: {source_uri}"
            )

        source_atoms = sorted(
            atoms_by_source[source_uri],
            key=lambda atom: (
                atom.start_frame,
                atom.end_frame,
                atom.raw_index,
                atom.atom_id,
            ),
        )
        source_owners = sorted(
            owners_by_source.get(source_uri, ()),
            key=lambda owner: (
                owner.atom.start_frame,
                owner.atom.end_frame,
                owner.atom.raw_index,
                owner.atom.atom_id,
            ),
        )
        families = {atom.family for atom in source_atoms}
        if len(families) != 1:
            raise ValueError(
                f"provider item has multiple dataset families: {source_uri}"
            )
        family = next(iter(families))
        if family not in PRESEGMENTED_FAMILIES:
            raise ValueError(
                f"continuous or unsupported family passed to presegmented "
                f"constructor: {family}"
            )
        source_splits = {_split(owner) for owner in source_owners}
        if len(source_splits) != 1:
            raise ValueError(
                f"provider item must have exactly one split owner: {source_uri}"
            )
        split = next(iter(source_splits))

        for atom in source_atoms:
            if atom.start_frame < 0 or atom.end_frame > media.frame_count:
                raise ValueError(
                    f"atom {atom.atom_id} outside decoded source bounds "
                    f"[0, {media.frame_count})"
                )

        speech_atoms = [
            atom
            for atom in source_atoms
            if _atom_class(atom) is domain.AtomClass.SPEECH
        ]
        pii_atoms = [
            atom
            for atom in source_atoms
            if _atom_class(atom) is domain.AtomClass.PII
        ]
        targetless_atoms = [
            atom
            for atom in source_atoms
            if _atom_class(atom) is domain.AtomClass.TARGETLESS
        ]
        speech_intervals = _merge_intervals(
            (atom.start_frame, atom.end_frame) for atom in speech_atoms
        )
        pii_union = _merge_intervals(
            (atom.start_frame, atom.end_frame) for atom in pii_atoms
        )
        pii_only = _subtract_intervals(pii_union, speech_intervals)
        targetless_union = _merge_intervals(
            (atom.start_frame, atom.end_frame) for atom in targetless_atoms
        )
        targetless_effective = _subtract_intervals(
            targetless_union,
            (*speech_intervals, *pii_only),
        )
        annotated_union = _merge_intervals(
            (atom.start_frame, atom.end_frame) for atom in source_atoms
        )
        residual_components = _complement(pii_only, media.frame_count)
        component_plans: list[tuple[int, int, tuple[Any, ...]]] = []
        if family == "echo":
            geometry_policy = ECHO_GEOMETRY_POLICY
            for component_owners in _strictly_overlapping_owner_components(
                source_owners
            ):
                component_plans.append(
                    (
                        min(
                            owner.atom.start_frame
                            for owner in component_owners
                        ),
                        max(
                            owner.atom.end_frame for owner in component_owners
                        ),
                        component_owners,
                    )
                )
            # Echo archive context is not evidence of provider-request
            # membership.  Everything outside protected Speech is dropped;
            # PII-only frames are accounted separately from other context.
            echo_spans = [
                (start, end) for start, end, _ in component_plans
            ]
            dropped_components = list(
                _subtract_intervals(
                    _complement(echo_spans, media.frame_count),
                    pii_only,
                )
            )
        else:
            geometry_policy = AUTHORITATIVE_PROVIDER_ITEM_GEOMETRY_POLICY
            dropped_components = []
            for component_start, component_end in residual_components:
                component_owners = tuple(
                    owner
                    for owner in source_owners
                    if (
                        component_start <= owner.atom.start_frame
                        and owner.atom.end_frame <= component_end
                    )
                )
                crossing_owners = [
                    owner.atom.atom_id
                    for owner in source_owners
                    if (
                        owner.atom.start_frame < component_end
                        and component_start < owner.atom.end_frame
                        and owner not in component_owners
                    )
                ]
                if crossing_owners:
                    raise AssertionError(
                        "PII-only subtraction cut protected Speech owners: "
                        f"{crossing_owners}"
                    )
                if not component_owners:
                    dropped_components.append(
                        (component_start, component_end)
                    )
                    continue
                component_plans.append(
                    (component_start, component_end, component_owners)
                )

        emitted_components: list[Interval] = []
        source_request_receipts: list[RequestCoverageReceipt] = []

        for component_start, component_end, component_owners in component_plans:
            component_splits = {_split(owner) for owner in component_owners}
            if component_splits != {split}:
                raise ValueError(
                    f"residual component has mixed splits: {source_uri} "
                    f"[{component_start}, {component_end})"
                )

            text, transcript_receipt = _reconcile_component(component_owners)
            receipt_json = _receipt_json_value(transcript_receipt)
            owner_ids = tuple(owner.atom.atom_id for owner in component_owners)
            request_id = (
                "presegmented-"
                + domain.stable_digest(
                    {
                        "family": family,
                        "geometry_policy": geometry_policy,
                        "generation": media.generation,
                        "source_uri": source_uri,
                        "split": split.value,
                        "start_frame": component_start,
                        "end_frame": component_end,
                        "owner_ids": owner_ids,
                    }
                )[:24]
            )
            request = domain.Request(
                request_id=request_id,
                family=family,
                split=split,
                source_uri=source_uri,
                start_frame=component_start,
                end_frame=component_end,
                sample_rate=media.sample_rate,
                owner_ids=owner_ids,
                text=text,
            )

            request_span = ((component_start, component_end),)
            request_speech_frames = _intersection_length(
                request_span,
                speech_intervals,
            )
            request_targetless_frames = _intersection_length(
                request_span,
                targetless_effective,
            )
            request_annotated_frames = _intersection_length(
                request_span,
                annotated_union,
            )
            request_unannotated_frames = (
                component_end - component_start - request_annotated_frames
            )
            if (
                geometry_policy == ECHO_GEOMETRY_POLICY
                and request_speech_frames != component_end - component_start
            ):
                raise AssertionError(
                    "Echo conservative component absorbed non-Speech frames: "
                    f"{request_id}"
                )
            if (
                request_speech_frames
                + request_targetless_frames
                + request_unannotated_frames
                != component_end - component_start
            ):
                raise AssertionError(
                    f"coverage classes do not conserve request frames: "
                    f"{request_id}"
                )

            requests.append(request)
            emitted_owner_ids.extend(owner_ids)
            emitted_components.append((component_start, component_end))
            transcript_bindings.append(
                TranscriptBinding(
                    request_id=request_id,
                    receipt=transcript_receipt,
                )
            )
            source_request_receipts.append(
                RequestCoverageReceipt(
                    request_id=request_id,
                    source_uri=source_uri,
                    split=split,
                    geometry_policy=geometry_policy,
                    start_frame=component_start,
                    end_frame=component_end,
                    owner_ids=owner_ids,
                    speech_union_frames=request_speech_frames,
                    targetless_annotation_frames=request_targetless_frames,
                    unannotated_frames=request_unannotated_frames,
                    transcript_receipt_digest=domain.stable_digest(
                        receipt_json
                    ),
                )
            )

        emitted_request_frames = _interval_length(emitted_components)
        retained_speech_frames = _intersection_length(
            emitted_components,
            speech_intervals,
        )
        retained_targetless_frames = _intersection_length(
            emitted_components,
            targetless_effective,
        )
        retained_annotated_frames = _intersection_length(
            emitted_components,
            annotated_union,
        )
        retained_unannotated_frames = (
            emitted_request_frames - retained_annotated_frames
        )
        if (
            retained_speech_frames
            + retained_targetless_frames
            + retained_unannotated_frames
            != emitted_request_frames
        ):
            raise AssertionError(
                f"source coverage classes do not conserve frames: {source_uri}"
            )
        if retained_speech_frames != _interval_length(speech_intervals):
            raise AssertionError(
                f"not all Speech audio was retained for source: {source_uri}"
            )

        speech_annotation_frames = sum(
            atom.end_frame - atom.start_frame for atom in speech_atoms
        )
        source_receipts.append(
            SourceTopologyReceipt(
                source_uri=source_uri,
                family=family,
                split=split,
                geometry_policy=geometry_policy,
                source_frames=media.frame_count,
                atom_count=len(source_atoms),
                speech_atom_count=len(speech_atoms),
                pii_atom_count=len(pii_atoms),
                targetless_atom_count=len(targetless_atoms),
                owner_count=len(source_owners),
                speech_annotation_frames=speech_annotation_frames,
                speech_union_frames=_interval_length(speech_intervals),
                speech_duplicate_annotation_frames=(
                    speech_annotation_frames
                    - _interval_length(speech_intervals)
                ),
                pii_union_frames=_interval_length(pii_union),
                speech_pii_overlap_frames=_intersection_length(
                    speech_intervals,
                    pii_union,
                ),
                pii_only_frames=_interval_length(pii_only),
                targetless_union_frames=_interval_length(targetless_union),
                targetless_effective_frames=_interval_length(
                    targetless_effective
                ),
                annotated_union_frames=_interval_length(annotated_union),
                unannotated_source_frames=(
                    media.frame_count - _interval_length(annotated_union)
                ),
                residual_component_count=len(residual_components),
                emitted_request_count=len(emitted_components),
                dropped_context_component_count=len(dropped_components),
                emitted_request_frames=emitted_request_frames,
                retained_speech_frames=retained_speech_frames,
                retained_targetless_annotation_frames=(
                    retained_targetless_frames
                ),
                retained_unannotated_frames=retained_unannotated_frames,
                dropped_context_frames=_interval_length(dropped_components),
                request_receipts=tuple(source_request_receipts),
            )
        )

    expected_owner_ids = set(owners_by_id)
    if len(emitted_owner_ids) != len(set(emitted_owner_ids)):
        raise AssertionError("a Speech owner was emitted by multiple requests")
    if set(emitted_owner_ids) != expected_owner_ids:
        missing = sorted(expected_owner_ids - set(emitted_owner_ids))
        extra = sorted(set(emitted_owner_ids) - expected_owner_ids)
        raise AssertionError(
            "Speech owner conservation failed; "
            f"missing={missing[:10]}, extra={extra[:10]}"
        )
    request_ids = [request.request_id for request in requests]
    if len(request_ids) != len(set(request_ids)):
        raise AssertionError("deterministic request identifier collision")

    aggregate_receipt = PresegmentedReceipt(
        source_count=len(source_receipts),
        atom_count=len(atom_inventory),
        owner_count=len(owners),
        request_count=len(requests),
        pii_only_frames=sum(
            receipt.pii_only_frames for receipt in source_receipts
        ),
        emitted_request_frames=sum(
            receipt.emitted_request_frames for receipt in source_receipts
        ),
        retained_speech_frames=sum(
            receipt.retained_speech_frames for receipt in source_receipts
        ),
        retained_targetless_annotation_frames=sum(
            receipt.retained_targetless_annotation_frames
            for receipt in source_receipts
        ),
        retained_unannotated_frames=sum(
            receipt.retained_unannotated_frames for receipt in source_receipts
        ),
        source_receipts=tuple(source_receipts),
    )
    return PresegmentedResult(
        requests=tuple(requests),
        receipt=aggregate_receipt,
        transcript_receipts=tuple(transcript_bindings),
    )
