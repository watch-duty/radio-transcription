# One-time constructor: direct fail-loud diagnostics are intentional.
# ruff: noqa: D202, EM101, EM102, PLR0912, S101, TC003, TRY003, TRY004
"""Map the latest SFT rows onto raw atoms and freeze leak-safe ownership."""

from __future__ import annotations

import dataclasses
import decimal
from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping, Sequence
from typing import Any, Final

from common import recording_groups

from dataset_run import domain, inputs

EXPECTED_INITIAL_BY_SPLIT_FAMILY: Final = {
    ("eval", "bcfy_calls"): 204,
    ("eval", "bcfy_feeds"): 970,
    ("eval", "echo"): 6_490,
    ("eval", "fire_notifications"): 8_016,
    ("train", "bcfy_calls"): 2_914,
    ("train", "bcfy_feeds"): 14_238,
    ("train", "echo"): 15_909,
    ("train", "fire_notifications"): 16_583,
}
EXPECTED_FINAL_BY_SPLIT_FAMILY: Final = {
    ("eval", "bcfy_calls"): 204,
    ("eval", "bcfy_feeds"): 970,
    ("eval", "echo"): 6_490,
    ("eval", "fire_notifications"): 7_074,
    ("train", "bcfy_calls"): 2_914,
    ("train", "bcfy_feeds"): 14_238,
    ("train", "echo"): 15_909,
    ("train", "fire_notifications"): 17_525,
}
EXPECTED_RAW_SPEECH_COUNT: Final = 65_324
EXPECTED_SPEECH_SOURCE_COUNT: Final = 8_709
EXPECTED_CLOSURE_COUNT: Final = 560

_COMPOSITE_POOL_TO_RAW_MANIFEST: Final = {
    "20260619": "feeds_train",
    "v20260528": "feeds_eval",
}


@dataclasses.dataclass(frozen=True, slots=True)
class MappedCanonicalRow:
    """One canonical row resolved to one or more raw speech atoms."""

    manifest_role: str
    row_index: int
    family: str
    source_uri: str
    offset_seconds: decimal.Decimal
    duration_seconds: decimal.Decimal
    text: str
    locators: tuple[inputs.RawLocator, ...]
    composite: bool

    def __post_init__(self) -> None:
        if self.manifest_role not in {"train", "eval", "closure"}:
            raise ValueError("invalid canonical manifest role")
        if not self.locators:
            raise ValueError(
                "canonical protected row mapped to no speech atoms"
            )
        if len(set(self.locators)) != len(self.locators):
            raise ValueError("canonical row maps the same raw atom twice")
        if domain.normalize_text(self.text) is None:
            raise ValueError("mapped canonical row requires non-empty text")


@dataclasses.dataclass(frozen=True, slots=True)
class SourceGroup:
    """One complete ownership component under current recording-group rules."""

    group_id: str
    source_uris: tuple[str, ...]
    initial_splits: tuple[domain.Split, ...]
    final_split: domain.Split

    def __post_init__(self) -> None:
        if not self.group_id or not self.source_uris:
            raise ValueError("source group identity and members are required")
        if tuple(sorted(set(self.source_uris))) != self.source_uris:
            raise ValueError("source group members must be sorted and unique")
        if tuple(sorted(set(self.initial_splits))) != self.initial_splits:
            raise ValueError("source group splits must be sorted and unique")
        expected = (
            domain.Split.TRAIN
            if domain.Split.TRAIN in self.initial_splits
            else domain.Split.EVAL
        )
        if self.final_split is not expected:
            raise ValueError("source group does not apply train-wins ownership")


@dataclasses.dataclass(frozen=True, slots=True)
class OwnedAtom:
    """One protected raw Speech atom with final ownership and target overlay."""

    raw: inputs.RawAnnotation
    initial_split: domain.Split
    final_split: domain.Split
    source_group_id: str
    authoritative_offset_seconds: decimal.Decimal
    authoritative_duration_seconds: decimal.Decimal
    authoritative_text: str
    canonical_manifest_role: str
    canonical_row_index: int
    target_origin: str
    closure_row_index: int | None = None

    def __post_init__(self) -> None:
        if self.raw.atom_class is not domain.AtomClass.SPEECH:
            raise ValueError("OwnedAtom must reference raw Speech")
        if domain.normalize_text(self.authoritative_text) is None:
            raise ValueError("OwnedAtom target must be non-empty")
        if self.authoritative_offset_seconds < 0:
            raise ValueError("authoritative offset must be non-negative")
        if self.authoritative_duration_seconds <= 0:
            raise ValueError("authoritative duration must be positive")
        if self.target_origin not in {
            "canonical",
            "closure",
            "composite_constituent",
        }:
            raise ValueError(f"unsupported target origin: {self.target_origin}")
        if (self.target_origin == "closure") != (
            self.closure_row_index is not None
        ):
            raise ValueError("closure target must name exactly one closure row")

    @property
    def locator(self) -> inputs.RawLocator:
        """Return the raw owner locator."""

        return self.raw.locator

    def to_owned_speech(self, sample_rate: int) -> domain.OwnedSpeech:
        """Convert this authoritative owner to source-native frames."""

        atom = self.raw.to_atom(
            sample_rate,
            offset_seconds=self.authoritative_offset_seconds,
            duration_seconds=self.authoritative_duration_seconds,
            text=self.authoritative_text,
        )
        return domain.OwnedSpeech(
            atom=atom,
            split=self.final_split,
            text=self.authoritative_text,
        )


@dataclasses.dataclass(frozen=True, slots=True)
class OwnershipResult:
    """Complete, audited result of the ownership freeze."""

    owners: tuple[OwnedAtom, ...]
    source_groups: tuple[SourceGroup, ...]
    mapped_rows: tuple[MappedCanonicalRow, ...]
    closure_rows: tuple[MappedCanonicalRow, ...]
    receipt: Mapping[str, Any]

    def owner_by_locator(self) -> dict[inputs.RawLocator, OwnedAtom]:
        """Return an exact owner lookup, failing if the result is corrupt."""

        result = {owner.locator: owner for owner in self.owners}
        if len(result) != len(self.owners):
            raise ValueError("ownership result contains duplicate raw locators")
        return result

    def group_by_source_uri(self) -> dict[str, SourceGroup]:
        """Return each source's one complete ownership component."""

        result: dict[str, SourceGroup] = {}
        for group in self.source_groups:
            for source_uri in group.source_uris:
                if source_uri in result:
                    raise ValueError(
                        f"source belongs to multiple groups: {source_uri}"
                    )
                result[source_uri] = group
        return result

    def owner_receipt_rows(self) -> tuple[dict[str, Any], ...]:
        """Return complete JSON-compatible row-level ownership evidence."""

        return tuple(
            {
                "authoritative_duration_seconds": str(
                    owner.authoritative_duration_seconds
                ),
                "authoritative_offset_seconds": str(
                    owner.authoritative_offset_seconds
                ),
                "authoritative_text": owner.authoritative_text,
                "canonical_manifest_role": owner.canonical_manifest_role,
                "canonical_row_index": owner.canonical_row_index,
                "closure_row_index": owner.closure_row_index,
                "family": owner.raw.family,
                "final_split": owner.final_split.value,
                "initial_split": owner.initial_split.value,
                "raw_duration_seconds": str(owner.raw.duration_seconds),
                "raw_index": owner.locator.raw_index,
                "raw_manifest": owner.locator.raw_manifest,
                "raw_offset_seconds": str(owner.raw.offset_seconds),
                "raw_source_split": owner.raw.source_split,
                "source_group_id": owner.source_group_id,
                "source_uri": owner.raw.source_uri,
                "target_origin": owner.target_origin,
            }
            for owner in self.owners
        )

    def source_group_receipt_rows(self) -> tuple[dict[str, Any], ...]:
        """Return complete JSON-compatible recording-group evidence."""

        return tuple(
            {
                "final_split": group.final_split.value,
                "group_id": group.group_id,
                "initial_splits": [
                    split.value for split in group.initial_splits
                ],
                "source_uris": list(group.source_uris),
            }
            for group in self.source_groups
        )

    def canonical_mapping_receipt_rows(self) -> tuple[dict[str, Any], ...]:
        """Return exact canonical-parent to raw-owner mapping evidence."""

        return tuple(
            {
                "composite": row.composite,
                "duration_seconds": str(row.duration_seconds),
                "family": row.family,
                "locators": [locator.atom_id for locator in row.locators],
                "manifest_role": row.manifest_role,
                "offset_seconds": str(row.offset_seconds),
                "row_index": row.row_index,
                "source_uri": row.source_uri,
                "text": row.text,
            }
            for row in (*self.mapped_rows, *self.closure_rows)
        )


class _UnionFind:
    def __init__(self, values: Iterable[str]) -> None:
        self.parent = {value: value for value in values}

    def find(self, value: str) -> str:
        parent = self.parent[value]
        while parent != self.parent[parent]:
            parent = self.parent[parent]
        while value != parent:
            next_value = self.parent[value]
            self.parent[value] = parent
            value = next_value
        return parent

    def union(self, left: str, right: str) -> None:
        left_root = self.find(left)
        right_root = self.find(right)
        if left_root == right_root:
            return
        low, high = sorted((left_root, right_root))
        self.parent[high] = low


def _dataset_provenance(row: Mapping[str, Any]) -> Mapping[str, Any] | None:
    dataset = row.get("dataset")
    if not isinstance(dataset, Mapping):
        return None
    provenance = dataset.get("oneoff_provenance")
    if provenance is None:
        return None
    if not isinstance(provenance, Mapping):
        raise TypeError("dataset.oneoff_provenance must be an object")
    return provenance


def _strict_index(value: object, *, name: str) -> int:
    if isinstance(value, bool):
        raise ValueError(f"{name} must be an integer")
    if isinstance(value, int):
        result = value
    elif isinstance(value, str) and value.isdigit():
        result = int(value)
    else:
        raise ValueError(f"{name} must contain only decimal digits")
    if result < 0:
        raise ValueError(f"{name} must be non-negative")
    return result


def _composite_locators(
    bundle: inputs.InputBundle,
    row_ref: inputs.CanonicalManifestRow,
    provenance: Mapping[str, Any],
    *,
    family: str,
    source_uri: str,
) -> tuple[inputs.RawLocator, ...]:
    if family != "bcfy_feeds":
        raise ValueError("only bcfy_feeds may contain frozen composites")
    required = {
        "construction",
        "first_raw_row",
        "last_raw_row",
        "pool",
    }
    if set(provenance) != required:
        raise ValueError(
            "composite provenance must contain exactly "
            f"{sorted(required)!r}; row={row_ref.row_index}"
        )
    pool = provenance["pool"]
    try:
        raw_manifest = _COMPOSITE_POOL_TO_RAW_MANIFEST[pool]
    except (KeyError, TypeError):
        raise ValueError(f"unsupported composite pool: {pool!r}") from None
    first = _strict_index(
        provenance["first_raw_row"], name="composite first_raw_row"
    )
    last = _strict_index(
        provenance["last_raw_row"], name="composite last_raw_row"
    )
    if last < first:
        raise ValueError("composite last_raw_row precedes first_raw_row")

    annotations: list[inputs.RawAnnotation] = []
    for raw_index in range(first, last + 1):
        locator = inputs.RawLocator(raw_manifest, raw_index)
        try:
            annotation = bundle.raw_by_locator[locator]
        except KeyError:
            raise ValueError(
                f"composite raw locator does not exist: {locator}"
            ) from None
        if annotation.family != family or annotation.source_uri != source_uri:
            raise ValueError(
                f"composite range crosses family or source boundary: {locator}"
            )
        annotations.append(annotation)

    observed_pattern = "-".join(
        "S"
        if annotation.atom_class is domain.AtomClass.SPEECH
        else "N"
        if annotation.atom_class is domain.AtomClass.TARGETLESS
        else "P"
        for annotation in annotations
    )
    construction = provenance["construction"]
    if construction != observed_pattern:
        raise ValueError(
            f"composite pattern mismatch: {construction!r} != "
            f"{observed_pattern!r}"
        )
    speech = tuple(
        annotation.locator
        for annotation in annotations
        if annotation.atom_class is domain.AtomClass.SPEECH
    )
    if len(speech) < 2:
        raise ValueError("composite must contain multiple Speech atoms")
    expected_text = domain.normalize_text(
        " ".join(
            annotation.text or ""
            for annotation in annotations
            if annotation.atom_class is domain.AtomClass.SPEECH
        )
    )
    observed_text = domain.normalize_text(inputs.canonical_text(row_ref.row))
    if expected_text != observed_text:
        raise ValueError(
            f"composite target does not equal constituent targets at "
            f"{row_ref.manifest_role}[{row_ref.row_index}]"
        )
    return speech


def map_canonical_row(
    bundle: inputs.InputBundle,
    row_ref: inputs.CanonicalManifestRow,
) -> MappedCanonicalRow:
    """Resolve one latest-run protected row to exact raw Speech locators."""

    family = inputs.canonical_family(row_ref.row)
    source_uri, offset, duration = inputs.canonical_source_geometry(row_ref.row)
    text = inputs.canonical_text(row_ref.row)
    provenance = _dataset_provenance(row_ref.row)
    composite = provenance is not None and "construction" in provenance
    if composite:
        assert provenance is not None
        locators = _composite_locators(
            bundle,
            row_ref,
            provenance,
            family=family,
            source_uri=source_uri,
        )
    else:
        raw_index_value = (
            provenance.get("row_index")
            if provenance is not None
            else row_ref.row.get("segment_id")
        )
        raw_index = _strict_index(
            raw_index_value,
            name=(
                "dataset.oneoff_provenance.row_index"
                if provenance is not None
                else "segment_id"
            ),
        )
        candidates = bundle.locators_by_family_uri_index.get(
            (family, source_uri, raw_index), ()
        )
        if len(candidates) != 1:
            raise ValueError(
                "canonical locator must resolve to exactly one raw row: "
                f"family={family}, source={source_uri}, index={raw_index}, "
                f"matches={list(candidates)!r}"
            )
        locators = (candidates[0],)

    for locator in locators:
        annotation = bundle.raw_by_locator[locator]
        if annotation.atom_class is not domain.AtomClass.SPEECH:
            raise ValueError(
                f"canonical protected row resolved to non-Speech: {locator}"
            )
    return MappedCanonicalRow(
        manifest_role=row_ref.manifest_role,
        row_index=row_ref.row_index,
        family=family,
        source_uri=source_uri,
        offset_seconds=offset,
        duration_seconds=duration,
        text=text,
        locators=locators,
        composite=composite,
    )


def map_all_canonical_rows(
    bundle: inputs.InputBundle,
) -> tuple[
    tuple[MappedCanonicalRow, ...],
    tuple[MappedCanonicalRow, ...],
]:
    """Map train/eval and closure, proving exact raw Speech coverage."""

    mapped = tuple(
        map_canonical_row(bundle, row_ref)
        for row_ref in (*bundle.canonical_train, *bundle.canonical_eval)
    )
    locator_parent: dict[inputs.RawLocator, MappedCanonicalRow] = {}
    for row in mapped:
        for locator in row.locators:
            previous = locator_parent.get(locator)
            if previous is not None:
                raise ValueError(
                    f"raw Speech atom appears in multiple canonical rows: "
                    f"{locator}; parents={previous.manifest_role}"
                    f"[{previous.row_index}], {row.manifest_role}"
                    f"[{row.row_index}]"
                )
            locator_parent[locator] = row

    speech_locators = {
        annotation.locator for annotation in bundle.speech_annotations
    }
    mapped_locators = set(locator_parent)
    missing = sorted(speech_locators - mapped_locators)
    extra = sorted(mapped_locators - speech_locators)
    if missing or extra:
        raise ValueError(
            "canonical/raw Speech coverage is not exact: "
            f"missing={len(missing)} sample={missing[:5]!r}; "
            f"extra={len(extra)} sample={extra[:5]!r}"
        )

    closure = tuple(
        map_canonical_row(bundle, row_ref)
        for row_ref in bundle.canonical_closure
    )
    seen_closure: set[inputs.RawLocator] = set()
    for row in closure:
        if len(row.locators) != 1 or row.composite:
            raise ValueError("closure rows must map to one non-composite atom")
        locator = row.locators[0]
        if locator in seen_closure:
            raise ValueError(f"duplicate closure owner: {locator}")
        seen_closure.add(locator)
        parent = locator_parent.get(locator)
        if parent is None or parent.manifest_role != "train":
            raise ValueError(
                f"closure owner is not present in latest train: {locator}"
            )
        if parent.family != row.family or parent.source_uri != row.source_uri:
            raise ValueError(f"closure lineage disagrees with train: {locator}")
    return mapped, closure


def _build_current_source_groups(
    bundle: inputs.InputBundle,
    mapped_rows: Sequence[MappedCanonicalRow],
    *,
    validate_frozen_policy: bool,
) -> tuple[tuple[SourceGroup, ...], Mapping[str, Any]]:
    """Build current legacy groups, then add every frozen confirmed alias.

    The shared engine needs generation/size fields for its general source-index
    contract.  This one-time ownership pass deliberately supplies neutral
    values because grouping here uses only legacy basename and Fire capture
    timestamp rules.  The prior source audit established the scoped objects;
    re-running object hashes would not change train-wins ownership.
    """

    row_counts: Counter[tuple[str, str, str]] = Counter()
    source_split: dict[str, domain.Split] = {}
    source_family: dict[str, str] = {}
    for row in mapped_rows:
        split = domain.Split(row.manifest_role)
        previous_split = source_split.setdefault(row.source_uri, split)
        if previous_split is not split:
            raise ValueError(
                f"one source appears in both canonical splits: {row.source_uri}"
            )
        previous_family = source_family.setdefault(row.source_uri, row.family)
        if previous_family != row.family:
            raise ValueError(
                f"one source claims multiple families: {row.source_uri}"
            )
        row_counts[(split.value, row.family, row.source_uri)] += 1

    occurrences = tuple(
        recording_groups.SourceOccurrence(
            split=split,
            dataset_family=family,
            source_uri=source_uri,
            gcs_generation=1,
            gcs_size=0,
            gcs_md5=None,
            row_count=row_count,
            locators=(),
            recording_lineage_id=None,
            legacy=True,
        )
        for (split, family, source_uri), row_count in sorted(row_counts.items())
    )
    candidate = recording_groups.build_source_index_candidate(
        occurrences,
        authoritative_lineages=(),
        source_universe=(),
    )
    candidate_record = candidate.record
    candidate_members: dict[str, list[str]] = defaultdict(list)
    for source in candidate_record["sources"]:
        if source["kind"] != "gcs_source":
            continue
        candidate_members[source["split_group_id"]].append(source["source_uri"])

    union_find = _UnionFind(source_split)
    for members in candidate_members.values():
        for source_uri in members[1:]:
            union_find.union(members[0], source_uri)
    aliases_already_grouped = 0
    for alias in bundle.fire_aliases:
        for source_uri in (alias.train_source_uri, alias.eval_source_uri):
            if source_uri not in source_split:
                raise ValueError(
                    f"frozen Fire alias is outside source universe: {source_uri}"
                )
        if union_find.find(alias.train_source_uri) == union_find.find(
            alias.eval_source_uri
        ):
            aliases_already_grouped += 1
        union_find.union(alias.train_source_uri, alias.eval_source_uri)

    members_by_root: dict[str, list[str]] = defaultdict(list)
    for source_uri in sorted(source_split):
        members_by_root[union_find.find(source_uri)].append(source_uri)
    groups: list[SourceGroup] = []
    for members in members_by_root.values():
        ordered = tuple(sorted(members))
        initial_splits = tuple(
            sorted({source_split[source_uri] for source_uri in ordered})
        )
        final_split = (
            domain.Split.TRAIN
            if domain.Split.TRAIN in initial_splits
            else domain.Split.EVAL
        )
        groups.append(
            SourceGroup(
                group_id=(
                    "ownership-group-v1-"
                    + domain.stable_digest(
                        {
                            "algorithm": "current-recording-groups-plus-frozen-aliases",
                            "source_uris": ordered,
                        }
                    )
                ),
                source_uris=ordered,
                initial_splits=initial_splits,
                final_split=final_split,
            )
        )
    groups.sort(key=lambda group: group.group_id)

    frozen_summary = bundle.recording_group_policy["assembly"][
        "grouping_summary"
    ]
    candidate_summary = candidate.grouping_summary
    # These fields determine source scope and train-wins behavior.  The frozen
    # graph has one additional same-split MD5 edge; the user explicitly chose
    # not to repeat exact-hash auditing, and that edge cannot move an eval row.
    ownership_relevant_fields = (
        "affected_rows_by_split",
        "affected_sources_by_split",
        "maximum_component_size",
        "maximum_transitive_fire_clock_span_seconds",
        "mixed_component_count",
        "parse_coverage",
        "physical_source_count",
        "rows_by_split",
        "sources_by_split",
    )
    if validate_frozen_policy:
        split_mapping_fields = {
            "affected_rows_by_split",
            "affected_sources_by_split",
            "rows_by_split",
            "sources_by_split",
        }

        def comparable_value(field: str, value: object) -> object:
            if field not in split_mapping_fields or not isinstance(
                value, Mapping
            ):
                return value
            return {
                split: value.get(split)
                for split in ("eval", "train")
                if split in value
            }

        mismatches = {
            field: {
                "current": candidate_summary.get(field),
                "frozen": frozen_summary.get(field),
            }
            for field in ownership_relevant_fields
            if comparable_value(field, candidate_summary.get(field))
            != comparable_value(field, frozen_summary.get(field))
        }
        if mismatches:
            raise ValueError(
                f"current recording-group ownership differs from frozen policy: "
                f"{mismatches!r}"
            )
    receipt = {
        "aliases_already_grouped_by_current_rules": aliases_already_grouped,
        "confirmed_fire_alias_count": len(bundle.fire_aliases),
        "current_candidate_component_count": candidate_summary[
            "component_count"
        ],
        "current_candidate_grouping_summary_sha256": (
            candidate.grouping_summary_sha256
        ),
        "frozen_component_count": frozen_summary.get("component_count"),
        "same_split_hash_edge_reaudit_skipped": True,
        "source_group_count_after_confirmed_aliases": len(groups),
    }
    return tuple(groups), receipt


def _target_by_locator(
    bundle: inputs.InputBundle,
    mapped_rows: Sequence[MappedCanonicalRow],
    closure_rows: Sequence[MappedCanonicalRow],
) -> dict[
    inputs.RawLocator,
    tuple[
        decimal.Decimal,
        decimal.Decimal,
        str,
        str,
        int,
        str,
        int | None,
    ],
]:
    closure_by_locator = {row.locators[0]: row for row in closure_rows}
    result: dict[
        inputs.RawLocator,
        tuple[
            decimal.Decimal,
            decimal.Decimal,
            str,
            str,
            int,
            str,
            int | None,
        ],
    ] = {}
    for row in mapped_rows:
        for locator in row.locators:
            raw = bundle.raw_by_locator[locator]
            closure = closure_by_locator.get(locator)
            if closure is not None:
                target = (
                    closure.offset_seconds,
                    closure.duration_seconds,
                    closure.text,
                    row.manifest_role,
                    row.row_index,
                    "closure",
                    closure.row_index,
                )
            elif row.composite:
                assert raw.text is not None
                target = (
                    raw.offset_seconds,
                    raw.duration_seconds,
                    raw.text,
                    row.manifest_role,
                    row.row_index,
                    "composite_constituent",
                    None,
                )
            else:
                target = (
                    row.offset_seconds,
                    row.duration_seconds,
                    row.text,
                    row.manifest_role,
                    row.row_index,
                    "canonical",
                    None,
                )
            if locator in result:
                raise ValueError(f"target overlay assigned twice: {locator}")
            result[locator] = target
    return result


def _count_by_split_family(
    owners: Iterable[OwnedAtom],
    *,
    final: bool,
) -> dict[tuple[str, str], int]:
    counter: Counter[tuple[str, str]] = Counter()
    for owner in owners:
        split = owner.final_split if final else owner.initial_split
        counter[(split.value, owner.raw.family)] += 1
    return dict(sorted(counter.items()))


def _serialize_split_family_counts(
    counts: Mapping[tuple[str, str], int],
) -> dict[str, dict[str, int]]:
    result: dict[str, dict[str, int]] = defaultdict(dict)
    for (split, family), count in sorted(counts.items()):
        result[split][family] = count
    return dict(result)


def _enforce_known_run(result: OwnershipResult) -> None:
    if len(result.owners) != EXPECTED_RAW_SPEECH_COUNT:
        raise ValueError(
            f"expected {EXPECTED_RAW_SPEECH_COUNT} Speech owners, "
            f"found {len(result.owners)}"
        )
    source_count = sum(len(group.source_uris) for group in result.source_groups)
    if source_count != EXPECTED_SPEECH_SOURCE_COUNT:
        raise ValueError(
            f"expected {EXPECTED_SPEECH_SOURCE_COUNT} speech sources, "
            f"found {source_count}"
        )
    if len(result.closure_rows) != EXPECTED_CLOSURE_COUNT:
        raise ValueError(
            f"expected {EXPECTED_CLOSURE_COUNT} closure rows, "
            f"found {len(result.closure_rows)}"
        )
    initial = _count_by_split_family(result.owners, final=False)
    final = _count_by_split_family(result.owners, final=True)
    if initial != EXPECTED_INITIAL_BY_SPLIT_FAMILY:
        raise ValueError(f"unexpected initial ownership counts: {initial!r}")
    if final != EXPECTED_FINAL_BY_SPLIT_FAMILY:
        raise ValueError(f"unexpected final ownership counts: {final!r}")


def freeze_ownership(
    bundle: inputs.InputBundle,
    *,
    enforce_known_run: bool = True,
    validate_frozen_policy: bool = True,
) -> OwnershipResult:
    """Freeze every protected Speech atom exactly once under train-wins."""

    mapped_rows, closure_rows = map_all_canonical_rows(bundle)
    source_groups, group_receipt = _build_current_source_groups(
        bundle,
        mapped_rows,
        validate_frozen_policy=validate_frozen_policy,
    )
    group_by_source: dict[str, SourceGroup] = {}
    for group in source_groups:
        for source_uri in group.source_uris:
            if source_uri in group_by_source:
                raise ValueError(
                    f"source appears in multiple groups: {source_uri}"
                )
            group_by_source[source_uri] = group

    target_by_locator = _target_by_locator(bundle, mapped_rows, closure_rows)
    initial_split_by_locator: dict[inputs.RawLocator, domain.Split] = {}
    for row in mapped_rows:
        split = domain.Split(row.manifest_role)
        for locator in row.locators:
            if locator in initial_split_by_locator:
                raise ValueError(f"duplicate initial owner: {locator}")
            initial_split_by_locator[locator] = split

    owners: list[OwnedAtom] = []
    for annotation in sorted(
        bundle.speech_annotations, key=lambda item: item.locator
    ):
        locator = annotation.locator
        try:
            initial_split = initial_split_by_locator[locator]
            group = group_by_source[annotation.source_uri]
            target = target_by_locator[locator]
        except KeyError as error:
            raise ValueError(
                f"Speech atom lacks complete ownership data: {locator}"
            ) from error
        (
            offset,
            duration,
            text,
            canonical_role,
            canonical_row_index,
            target_origin,
            closure_row_index,
        ) = target
        owners.append(
            OwnedAtom(
                raw=annotation,
                initial_split=initial_split,
                final_split=group.final_split,
                source_group_id=group.group_id,
                authoritative_offset_seconds=offset,
                authoritative_duration_seconds=duration,
                authoritative_text=text,
                canonical_manifest_role=canonical_role,
                canonical_row_index=canonical_row_index,
                target_origin=target_origin,
                closure_row_index=closure_row_index,
            )
        )

    initial_counts = _count_by_split_family(owners, final=False)
    final_counts = _count_by_split_family(owners, final=True)
    moved_counts: Counter[str] = Counter(
        owner.raw.family
        for owner in owners
        if owner.initial_split is domain.Split.EVAL
        and owner.final_split is domain.Split.TRAIN
    )
    origin_counts: Counter[str] = Counter(
        owner.target_origin for owner in owners
    )
    mixed_groups = tuple(
        group for group in source_groups if len(group.initial_splits) > 1
    )
    owner_digest_rows = [
        {
            "authoritative_duration_seconds": str(
                owner.authoritative_duration_seconds
            ),
            "authoritative_offset_seconds": str(
                owner.authoritative_offset_seconds
            ),
            "authoritative_text": owner.authoritative_text,
            "final_split": owner.final_split.value,
            "initial_split": owner.initial_split.value,
            "locator": owner.locator.atom_id,
            "source_group_id": owner.source_group_id,
            "source_uri": owner.raw.source_uri,
            "target_origin": owner.target_origin,
        }
        for owner in owners
    ]
    group_digest_rows = [
        {
            "final_split": group.final_split.value,
            "group_id": group.group_id,
            "initial_splits": [split.value for split in group.initial_splits],
            "source_uris": list(group.source_uris),
        }
        for group in source_groups
    ]
    receipt: dict[str, Any] = {
        "closure_overlay_count": len(closure_rows),
        "composite_canonical_row_count": sum(
            row.composite for row in mapped_rows
        ),
        "composite_speech_owner_count": sum(
            len(row.locators) for row in mapped_rows if row.composite
        ),
        "final_counts": _serialize_split_family_counts(final_counts),
        "initial_counts": _serialize_split_family_counts(initial_counts),
        "mapped_canonical_row_count": len(mapped_rows),
        "mixed_source_group_count": len(mixed_groups),
        "moved_eval_to_train_by_family": dict(sorted(moved_counts.items())),
        "owner_assignment_sha256": domain.stable_digest(owner_digest_rows),
        "protected_speech_owner_count": len(owners),
        "recording_groups": group_receipt,
        "source_group_membership_sha256": domain.stable_digest(
            group_digest_rows
        ),
        "source_group_count": len(source_groups),
        "target_origin_counts": dict(sorted(origin_counts.items())),
    }
    result = OwnershipResult(
        owners=tuple(owners),
        source_groups=source_groups,
        mapped_rows=mapped_rows,
        closure_rows=closure_rows,
        receipt=receipt,
    )
    if len(result.owner_by_locator()) != len(bundle.speech_annotations):
        raise ValueError("Speech ownership is not exact-once")
    if enforce_known_run:
        _enforce_known_run(result)
    return result
