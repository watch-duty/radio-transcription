# ruff: noqa: EM101, EM102, PLR0912, PLR0915, S101, SLF001, TRY003, TRY004
"""Immutable recording-lineage indexing and training-boundary leakage gates.

The module deliberately separates source-graph construction from row-level
membership.  A composite row can reference several independent recordings
without asserting that those recordings are the same physical source.
"""

from __future__ import annotations

import copy
import datetime as datetime_lib
import hashlib
import json
import re
import unicodedata
import urllib.parse
from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Final

from common.manifest import CanonicalRow, canonical_row_identity

INDEX_SCHEMA_VERSION: Final = 1
GROUP_ALGORITHM: Final = "recording-split-groups-v1"
FIRE_FILENAME_SCHEMA: Final = "fire_filename_v1"
FIRE_CAPTURE_START_SKEW_SECONDS: Final = 10.0
RECOGNIZED_AUDIO_SUFFIXES: Final = frozenset(
    {".aac", ".flac", ".m4a", ".mp3", ".ogg", ".opus", ".wav", ".webm"}
)

_SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")
_FIRE_FILENAME_RE = re.compile(
    r"_20(?P<date>[0-9]{4}-[0-9]{2}-[0-9]{2})"
    r"_20(?P<time>[0-9]{2}-[0-9]{2}-[0-9]{2})$"
)
_MALFORMED_PERCENT_RE = re.compile(r"%(?![0-9A-Fa-f]{2})")
_LINEAGE_KEYS = frozenset(
    {"producer_namespace", "lineage_schema_version", "recording_id"}
)
_TOP_LEVEL_KEYS = frozenset(
    {
        "schema_version",
        "policy",
        "source_universe",
        "sources",
        "edges",
        "grouping_summary",
    }
)


def _nonblank(value: object, name: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{name} must be a string")
    if not value.strip():
        raise ValueError(f"{name} must be nonblank")
    if value != value.strip():
        raise ValueError(f"{name} must not have surrounding whitespace")
    return value


def _nonnegative_int(
    value: object, name: str, *, positive: bool = False
) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be an integer")
    minimum = 1 if positive else 0
    if value < minimum:
        comparator = "positive" if positive else "non-negative"
        raise ValueError(f"{name} must be {comparator}")
    return value


def _validate_sha256(value: object, name: str) -> str:
    value = _nonblank(value, name)
    if _SHA256_RE.fullmatch(value) is None:
        raise ValueError(f"{name} must be a lowercase SHA-256 hex digest")
    return value


def _validate_gcs_object_uri(uri: object, name: str = "source_uri") -> str:
    uri = _nonblank(uri, name)
    parsed = urllib.parse.urlsplit(uri)
    if (
        parsed.scheme != "gs"
        or not parsed.netloc
        or not parsed.path
        or parsed.path == "/"
        or parsed.path.endswith("/")
        or parsed.query
        or parsed.fragment
        or parsed.username is not None
        or parsed.password is not None
    ):
        raise ValueError(f"{name} must be a single-object gs:// URI")
    return uri


@dataclass(frozen=True, order=True)
class LineageId:
    """Immutable recording identifier assigned before any dataset split."""

    producer_namespace: str
    lineage_schema_version: str
    recording_id: str

    def __post_init__(self) -> None:
        _nonblank(self.producer_namespace, "producer_namespace")
        _nonblank(self.lineage_schema_version, "lineage_schema_version")
        _nonblank(self.recording_id, "recording_id")


@dataclass(frozen=True, order=True)
class SourceLocator:
    """Exact runtime locator for one or more audited source records."""

    kind: str
    value: str
    dataset_family: str | None = None

    def __post_init__(self) -> None:
        _nonblank(self.kind, "locator kind")
        _nonblank(self.value, "locator value")
        if self.kind == "source_uri":
            _validate_gcs_object_uri(self.value, "source_uri locator")
            if self.dataset_family is not None:
                raise ValueError(
                    "source_uri locators are global and cannot name a dataset family"
                )
        elif self.kind == "masked_example_id":
            if self.dataset_family is None:
                raise ValueError(
                    "masked_example_id locators require a dataset family"
                )
            _nonblank(self.dataset_family, "locator dataset_family")
        else:
            raise ValueError(f"unsupported source locator kind: {self.kind!r}")


@dataclass(frozen=True, order=True)
class ManifestLock:
    """Immutable manifest identity included in an index source universe."""

    role: str
    manifest_uri: str
    gcs_generation: int | None
    sha256: str

    def __post_init__(self) -> None:
        _nonblank(self.role, "manifest role")
        _validate_gcs_object_uri(self.manifest_uri, "manifest_uri")
        if self.gcs_generation is not None:
            _nonnegative_int(
                self.gcs_generation, "gcs_generation", positive=True
            )
        _validate_sha256(self.sha256, "manifest sha256")


@dataclass(frozen=True, order=True)
class LineageSource:
    """Logical graph node for authoritative lineage without a physical row."""

    lineage_id: LineageId

    def __post_init__(self) -> None:
        if not isinstance(self.lineage_id, LineageId):
            raise TypeError("lineage_id must be a LineageId")


@dataclass(frozen=True, order=True)
class SourceOccurrence:
    """One split's observation of one immutable physical GCS source."""

    split: str
    dataset_family: str
    source_uri: str
    gcs_generation: int
    gcs_size: int
    gcs_md5: str | None
    row_count: int
    locators: tuple[SourceLocator, ...]
    recording_lineage_id: LineageId | None
    legacy: bool

    def __post_init__(self) -> None:
        _nonblank(self.split, "split")
        _nonblank(self.dataset_family, "dataset_family")
        _validate_gcs_object_uri(self.source_uri)
        _nonnegative_int(self.gcs_generation, "gcs_generation", positive=True)
        _nonnegative_int(self.gcs_size, "gcs_size")
        _nonnegative_int(self.row_count, "row_count", positive=True)
        if self.gcs_md5 is not None:
            _nonblank(self.gcs_md5, "gcs_md5")
        if not isinstance(self.locators, tuple) or not all(
            isinstance(locator, SourceLocator) for locator in self.locators
        ):
            raise TypeError("locators must be a tuple of SourceLocator values")
        if len(set(self.locators)) != len(self.locators):
            raise ValueError("locators must not contain duplicates")
        if self.recording_lineage_id is not None and not isinstance(
            self.recording_lineage_id, LineageId
        ):
            raise TypeError("recording_lineage_id must be a LineageId or None")
        if not isinstance(self.legacy, bool):
            raise TypeError("legacy must be a boolean")
        if self.legacy != (self.recording_lineage_id is None):
            raise ValueError(
                "legacy must be true exactly when authoritative lineage is absent"
            )


@dataclass(frozen=True, order=True)
class RowBinding:
    """Row-layer membership, including multi-recording composites."""

    split: str
    row_index: int
    row_identity: tuple[str, str]
    locators: tuple[SourceLocator, ...]
    recording_lineage_ids: tuple[LineageId, ...]

    def __post_init__(self) -> None:
        _nonblank(self.split, "split")
        _nonnegative_int(self.row_index, "row_index")
        if (
            not isinstance(self.row_identity, tuple)
            or len(self.row_identity) != 2
        ):
            raise TypeError("row_identity must be a two-item tuple")
        _nonblank(self.row_identity[0], "example_id")
        _nonblank(self.row_identity[1], "segment_id")
        if not isinstance(self.locators, tuple) or not all(
            isinstance(locator, SourceLocator) for locator in self.locators
        ):
            raise TypeError("locators must be a tuple of SourceLocator values")
        if not isinstance(self.recording_lineage_ids, tuple) or not all(
            isinstance(lineage, LineageId)
            for lineage in self.recording_lineage_ids
        ):
            raise TypeError(
                "recording_lineage_ids must be a tuple of LineageId values"
            )
        if tuple(sorted(set(self.locators))) != self.locators:
            raise ValueError("locators must be sorted and unique")
        if (
            tuple(sorted(set(self.recording_lineage_ids)))
            != self.recording_lineage_ids
        ):
            raise ValueError("recording_lineage_ids must be sorted and unique")
        if not self.locators and not self.recording_lineage_ids:
            raise ValueError(
                "a row binding must contain at least one locator or lineage"
            )


@dataclass(frozen=True, order=True)
class LegacyBasename:
    source_uri: str
    decoded_filename: str
    exact_stem: str
    normalized_stem: str
    audio_suffix: str


@dataclass(frozen=True, order=True)
class SourceObjectLock:
    """Physical source metadata returned for provider-side verification."""

    dataset_family: str
    source_uri: str
    gcs_generation: int
    gcs_size: int
    gcs_md5: str | None


def lineage_ids_from_row(
    row: CanonicalRow | Mapping[str, Any],
) -> tuple[LineageId, ...]:
    """Read and strictly validate authoritative lineage attached to a row."""
    source_audio: object
    if isinstance(row, CanonicalRow):
        source_audio = row.source_audio
    elif isinstance(row, Mapping):
        source_audio = row.get("source_audio")
    else:
        raise TypeError("row must be a CanonicalRow or mapping")
    if (
        not isinstance(source_audio, Mapping)
        or "recording_lineage_ids" not in source_audio
    ):
        return ()
    raw = source_audio["recording_lineage_ids"]
    if not isinstance(raw, list) or not raw:
        raise ValueError(
            "source_audio.recording_lineage_ids must be a nonempty list"
        )
    result: list[LineageId] = []
    for position, item in enumerate(raw):
        if not isinstance(item, Mapping):
            raise TypeError(
                f"recording_lineage_ids[{position}] must be an object"
            )
        if frozenset(item) != _LINEAGE_KEYS:
            raise ValueError(
                f"recording_lineage_ids[{position}] must contain exactly {sorted(_LINEAGE_KEYS)!r}"
            )
        result.append(
            LineageId(
                producer_namespace=item["producer_namespace"],
                lineage_schema_version=item["lineage_schema_version"],
                recording_id=item["recording_id"],
            )
        )
    if len(set(result)) != len(result):
        raise ValueError(
            "source_audio.recording_lineage_ids contains duplicates"
        )
    return tuple(sorted(result))


def parse_legacy_audio_basename(source_uri: str) -> LegacyBasename:
    """Parse one GCS object basename without letting decoding alter its path."""
    source_uri = _validate_gcs_object_uri(source_uri)
    parsed = urllib.parse.urlsplit(source_uri)
    encoded_filename = parsed.path.rsplit("/", 1)[-1]
    if _MALFORMED_PERCENT_RE.search(encoded_filename):
        raise ValueError(
            "source URI filename contains malformed percent encoding"
        )
    try:
        decoded_filename = urllib.parse.unquote(
            encoded_filename, errors="strict"
        )
    except UnicodeDecodeError as exc:
        raise ValueError("source URI filename is not valid UTF-8") from exc
    suffix = next(
        (
            candidate
            for candidate in sorted(
                RECOGNIZED_AUDIO_SUFFIXES, key=len, reverse=True
            )
            if decoded_filename.lower().endswith(candidate)
        ),
        None,
    )
    if suffix is None:
        raise ValueError(
            "legacy source filename has an unrecognized audio suffix"
        )
    exact_stem = decoded_filename[: -len(suffix)]
    if not exact_stem:
        raise ValueError("legacy source filename has a blank stem")
    normalized_stem = " ".join(
        unicodedata.normalize("NFKC", exact_stem).casefold().split()
    )
    if not normalized_stem:
        raise ValueError("legacy source filename has a blank normalized stem")
    return LegacyBasename(
        source_uri=source_uri,
        decoded_filename=decoded_filename,
        exact_stem=exact_stem,
        normalized_stem=normalized_stem,
        audio_suffix=suffix,
    )


def parse_fire_filename_v1(source_uri: str) -> datetime_lib.datetime:
    """Parse the timezone-uninterpreted capture clock from a legacy fire URI."""
    basename = parse_legacy_audio_basename(source_uri)
    match = _FIRE_FILENAME_RE.search(basename.exact_stem)
    if match is None:
        raise ValueError(f"source URI does not match {FIRE_FILENAME_SCHEMA}")
    try:
        return datetime_lib.datetime.strptime(
            f"{match.group('date')} {match.group('time')}",
            "%Y-%m-%d %H-%M-%S",
        )
    except ValueError as exc:
        raise ValueError(
            f"source URI has an invalid {FIRE_FILENAME_SCHEMA} clock"
        ) from exc


def _canonical_json_bytes(record: object) -> bytes:
    return (
        json.dumps(
            record, sort_keys=True, separators=(",", ":"), ensure_ascii=False
        )
        + "\n"
    ).encode("utf-8")


def _sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _lineage_record(lineage: LineageId) -> dict[str, str]:
    return {
        "lineage_schema_version": lineage.lineage_schema_version,
        "producer_namespace": lineage.producer_namespace,
        "recording_id": lineage.recording_id,
    }


def _locator_record(locator: SourceLocator) -> dict[str, str | None]:
    return {
        "dataset_family": locator.dataset_family,
        "kind": locator.kind,
        "value": locator.value,
    }


def _manifest_lock_record(lock: ManifestLock) -> dict[str, object]:
    return {
        "gcs_generation": lock.gcs_generation,
        "manifest_uri": lock.manifest_uri,
        "role": lock.role,
        "sha256": lock.sha256,
    }


class _UnionFind:
    def __init__(self, values: Iterable[str]) -> None:
        self._parent = {value: value for value in values}

    def find(self, value: str) -> str:
        parent = self._parent[value]
        if parent != value:
            self._parent[value] = self.find(parent)
        return self._parent[value]

    def union(self, left: str, right: str) -> None:
        left_root = self.find(left)
        right_root = self.find(right)
        if left_root == right_root:
            return
        low, high = sorted((left_root, right_root))
        self._parent[high] = low


@dataclass(frozen=True)
class SourceIndexCandidate:
    """Canonical but explicitly unapproved graph-construction output."""

    _payload: bytes = field(repr=False)
    grouping_summary_sha256: str
    _record_cache: dict[str, object] = field(
        init=False, repr=False, compare=False
    )

    def __post_init__(self) -> None:
        record = _validate_owned_index_payload(
            self._payload,
            expected_approval_status="unapproved",
        )
        observed = _sha256_bytes(
            _canonical_json_bytes(record["grouping_summary"])
        )
        declared = _validate_sha256(
            self.grouping_summary_sha256,
            "candidate grouping summary sha256",
        )
        if observed != declared:
            raise ValueError(
                "candidate grouping summary SHA does not match its canonical payload"
            )
        object.__setattr__(self, "_record_cache", record)

    @property
    def record(self) -> dict[str, object]:
        return copy.deepcopy(self._record_cache)

    @property
    def grouping_summary(self) -> dict[str, object]:
        return copy.deepcopy(self.record["grouping_summary"])


@dataclass(frozen=True)
class SourceIndex:
    """Approved, canonical source index accepted by runtime gates."""

    _payload: bytes = field(repr=False)
    _record_cache: dict[str, object] = field(
        init=False, repr=False, compare=False
    )
    _locator_groups: dict[SourceLocator, tuple[str, ...]] = field(
        init=False, repr=False, compare=False
    )
    _lineage_groups: dict[LineageId, tuple[str, ...]] = field(
        init=False, repr=False, compare=False
    )
    _source_locks: tuple[SourceObjectLock, ...] = field(
        init=False, repr=False, compare=False
    )

    def __post_init__(self) -> None:
        record = _validate_owned_index_payload(
            self._payload,
            expected_approval_status="approved",
        )
        locator_groups: dict[SourceLocator, list[str]] = defaultdict(list)
        lineage_groups: dict[LineageId, list[str]] = defaultdict(list)
        source_locks: list[SourceObjectLock] = []
        for source in record["sources"]:
            if source["kind"] == "gcs_source":
                for locator_record in source["locators"]:
                    locator_groups[_locator_from_record(locator_record)].append(
                        source["split_group_id"]
                    )
                source_locks.append(
                    SourceObjectLock(
                        dataset_family=source["dataset_family"],
                        source_uri=source["source_uri"],
                        gcs_generation=source["gcs_generation"],
                        gcs_size=source["gcs_size"],
                        gcs_md5=source["gcs_md5"],
                    )
                )
            else:
                lineage_groups[
                    _lineage_from_record(source["lineage_id"], "lineage_id")
                ].append(source["split_group_id"])
        object.__setattr__(self, "_record_cache", record)
        object.__setattr__(
            self,
            "_locator_groups",
            {
                locator: tuple(group_ids)
                for locator, group_ids in locator_groups.items()
            },
        )
        object.__setattr__(
            self,
            "_lineage_groups",
            {
                lineage: tuple(group_ids)
                for lineage, group_ids in lineage_groups.items()
            },
        )
        object.__setattr__(self, "_source_locks", tuple(sorted(source_locks)))

    @property
    def sha256(self) -> str:
        return _sha256_bytes(self._payload)

    @property
    def record(self) -> dict[str, object]:
        return copy.deepcopy(self._record_cache)

    @property
    def grouping_summary_sha256(self) -> str:
        policy = self.record["policy"]
        assert isinstance(policy, dict)
        value = policy["approved_grouping_summary_sha256"]
        assert isinstance(value, str)
        return value

    def group_ids_for_locator(self, locator: SourceLocator) -> frozenset[str]:
        if not isinstance(locator, SourceLocator):
            raise TypeError("locator must be a SourceLocator")
        matches = self._locator_groups.get(locator, ())
        if not matches:
            raise ValueError(f"locator did not resolve: {locator!r}")
        if locator.kind == "masked_example_id" and len(matches) != 1:
            raise ValueError(
                f"masked_example_id locator is ambiguous across {len(matches)} source records"
            )
        group_ids = frozenset(matches)
        if locator.kind == "source_uri" and len(group_ids) != 1:
            raise ValueError(
                f"global source_uri locator is ambiguous across {len(group_ids)} groups"
            )
        if len(group_ids) != 1:
            raise ValueError(f"locator is ambiguous: {locator!r}")
        return group_ids

    def group_ids_for_lineage(self, lineage: LineageId) -> frozenset[str]:
        if not isinstance(lineage, LineageId):
            raise TypeError("lineage must be a LineageId")
        matches = self._lineage_groups.get(lineage, ())
        if not matches:
            raise ValueError(f"lineage did not resolve: {lineage!r}")
        if len(matches) != 1:
            raise ValueError(f"lineage is ambiguous: {lineage!r}")
        return frozenset({matches[0]})

    def source_object_locks(self) -> tuple[SourceObjectLock, ...]:
        return self._source_locks


@dataclass
class _PhysicalNode:
    source_id: str
    dataset_family: str
    source_uri: str
    gcs_generation: int
    gcs_size: int
    gcs_md5: str | None
    recording_lineage_id: LineageId | None
    legacy: bool
    locators: set[SourceLocator]
    split_row_counts: Counter[str]
    basename: LegacyBasename | None = None
    capture_started_at: datetime_lib.datetime | None = None


def _stable_id(prefix: str, identity: object) -> str:
    return f"{prefix}{_sha256_bytes(_canonical_json_bytes(identity))}"


def _physical_source_id(
    dataset_family: str, source_uri: str, generation: int
) -> str:
    return _stable_id(
        "src-v1-",
        {
            "dataset_family": dataset_family,
            "gcs_generation": generation,
            "kind": "gcs_source",
            "source_uri": source_uri,
        },
    )


def _lineage_source_id(lineage: LineageId) -> str:
    return _stable_id(
        "lineage-v1-",
        {"kind": "lineage_source", "lineage_id": _lineage_record(lineage)},
    )


def _split_group_id(source_ids: Sequence[str]) -> str:
    return _stable_id(
        "splitgrp-v1-",
        {"algorithm": GROUP_ALGORITHM, "source_ids": sorted(source_ids)},
    )


def _normalize_lineage_catalog(
    values: Iterable[LineageId | LineageSource],
) -> tuple[LineageId, ...]:
    raw = list(values)
    normalized: list[LineageId] = []
    for value in raw:
        if isinstance(value, LineageSource):
            normalized.append(value.lineage_id)
        elif isinstance(value, LineageId):
            normalized.append(value)
        else:
            raise TypeError(
                "authoritative_lineages must contain LineageId values"
            )
    if len(set(normalized)) != len(normalized):
        raise ValueError("authoritative lineage catalog contains duplicates")
    return tuple(sorted(normalized))


def _normalize_source_universe(
    values: Iterable[ManifestLock],
) -> tuple[ManifestLock, ...]:
    raw = list(values)
    if not all(isinstance(value, ManifestLock) for value in raw):
        raise TypeError("source_universe must contain ManifestLock values")
    if len(set(raw)) != len(raw):
        raise ValueError("source universe contains duplicate manifest locks")
    by_role: dict[str, ManifestLock] = {}
    for lock in raw:
        previous = by_role.get(lock.role)
        if previous is not None and previous != lock:
            raise ValueError(
                f"source universe contains contradictory role {lock.role!r}"
            )
        by_role[lock.role] = lock
    return tuple(sorted(raw))


def _add_edge(
    edges: dict[tuple[str, str], set[str]],
    left: str,
    right: str,
    evidence: str,
) -> None:
    if left == right:
        return
    key = tuple(sorted((left, right)))
    edges[key].add(evidence)


def _add_pair_edges(
    edges: dict[tuple[str, str], set[str]],
    source_ids: Iterable[str],
    evidence: str,
) -> None:
    ordered = sorted(set(source_ids))
    for left_index, left in enumerate(ordered):
        for right in ordered[left_index + 1 :]:
            _add_edge(edges, left, right, evidence)


def build_source_index_candidate(
    occurrences: Iterable[SourceOccurrence],
    *,
    authoritative_lineages: Iterable[LineageId | LineageSource],
    source_universe: Iterable[ManifestLock],
) -> SourceIndexCandidate:
    """Build deterministic graph evidence without granting runtime approval."""
    occurrence_list = list(occurrences)
    if not all(isinstance(item, SourceOccurrence) for item in occurrence_list):
        raise TypeError("occurrences must contain SourceOccurrence values")
    catalog = _normalize_lineage_catalog(authoritative_lineages)
    catalog_set = set(catalog)
    universe = _normalize_source_universe(source_universe)

    physical_by_key: dict[tuple[str, str, int], _PhysicalNode] = {}
    seen_masked_locators: set[SourceLocator] = set()
    for occurrence in occurrence_list:
        if (
            occurrence.recording_lineage_id is not None
            and occurrence.recording_lineage_id not in catalog_set
        ):
            raise ValueError(
                "source occurrence lineage is missing from the authoritative lineage catalog"
            )
        for locator in occurrence.locators:
            if locator.kind != "masked_example_id":
                raise ValueError(
                    "caller-supplied locators may only be masked_example_id values"
                )
            if locator in seen_masked_locators:
                raise ValueError(
                    f"duplicate masked_example_id locator: {locator!r}"
                )
            seen_masked_locators.add(locator)
            if locator.dataset_family != occurrence.dataset_family:
                raise ValueError(
                    "masked_example_id locator family must match its source occurrence"
                )
        key = (
            occurrence.dataset_family,
            occurrence.source_uri,
            occurrence.gcs_generation,
        )
        node = physical_by_key.get(key)
        if node is None:
            node = _PhysicalNode(
                source_id=_physical_source_id(*key),
                dataset_family=occurrence.dataset_family,
                source_uri=occurrence.source_uri,
                gcs_generation=occurrence.gcs_generation,
                gcs_size=occurrence.gcs_size,
                gcs_md5=occurrence.gcs_md5,
                recording_lineage_id=occurrence.recording_lineage_id,
                legacy=occurrence.legacy,
                locators=set(),
                split_row_counts=Counter(),
            )
            physical_by_key[key] = node
        elif (
            node.gcs_size != occurrence.gcs_size
            or node.gcs_md5 != occurrence.gcs_md5
            or node.recording_lineage_id != occurrence.recording_lineage_id
            or node.legacy != occurrence.legacy
        ):
            raise ValueError(
                "contradictory metadata or lineage mode for one physical source occurrence"
            )
        node.locators.update(occurrence.locators)
        node.locators.add(SourceLocator("source_uri", occurrence.source_uri))
        node.split_row_counts[occurrence.split] += occurrence.row_count

    physical_nodes = sorted(
        physical_by_key.values(), key=lambda node: node.source_id
    )
    lineage_source_ids = {
        lineage: _lineage_source_id(lineage) for lineage in catalog
    }
    edges: dict[tuple[str, str], set[str]] = defaultdict(set)

    # Exact GCS object identity connects separate audit-family records.
    exact_objects: dict[tuple[str, int], list[str]] = defaultdict(list)
    exact_object_metadata: dict[tuple[str, int], tuple[int, str | None]] = {}
    md5_sources: dict[str, list[str]] = defaultdict(list)
    for node in physical_nodes:
        exact_key = (node.source_uri, node.gcs_generation)
        metadata = (node.gcs_size, node.gcs_md5)
        previous_metadata = exact_object_metadata.get(exact_key)
        if previous_metadata is not None and previous_metadata != metadata:
            raise ValueError(
                "contradictory metadata for one exact GCS object generation"
            )
        exact_object_metadata[exact_key] = metadata
        exact_objects[exact_key].append(node.source_id)
        if node.gcs_md5 is not None:
            md5_sources[node.gcs_md5].append(node.source_id)
        if node.recording_lineage_id is not None:
            _add_edge(
                edges,
                node.source_id,
                lineage_source_ids[node.recording_lineage_id],
                "authoritative_lineage",
            )
    for source_ids in exact_objects.values():
        _add_pair_edges(edges, source_ids, "exact_object")
    for source_ids in md5_sources.values():
        _add_pair_edges(edges, source_ids, "equal_nonblank_md5")

    # Legacy-only heuristics. Authoritative nodes do not invoke either parser.
    exact_basenames: dict[tuple[str, str], list[str]] = defaultdict(list)
    normalized_basenames: dict[tuple[str, str], list[str]] = defaultdict(list)
    fire_by_family: dict[str, list[_PhysicalNode]] = defaultdict(list)
    for node in physical_nodes:
        if not node.legacy:
            continue
        node.basename = parse_legacy_audio_basename(node.source_uri)
        exact_basenames[(node.dataset_family, node.basename.exact_stem)].append(
            node.source_id
        )
        normalized_basenames[
            (node.dataset_family, node.basename.normalized_stem)
        ].append(node.source_id)
        if node.dataset_family == "fire_notifications":
            node.capture_started_at = parse_fire_filename_v1(node.source_uri)
            fire_by_family[node.dataset_family].append(node)
    for source_ids in exact_basenames.values():
        _add_pair_edges(edges, source_ids, "legacy_exact_basename")
    for source_ids in normalized_basenames.values():
        _add_pair_edges(edges, source_ids, "legacy_normalized_basename")
    for family_nodes in fire_by_family.values():
        timed = sorted(
            family_nodes,
            key=lambda node: (node.capture_started_at, node.source_id),
        )
        left = 0
        for right, right_node in enumerate(timed):
            assert right_node.capture_started_at is not None
            while left < right:
                left_clock = timed[left].capture_started_at
                assert left_clock is not None
                if (
                    right_node.capture_started_at - left_clock
                ).total_seconds() <= FIRE_CAPTURE_START_SKEW_SECONDS:
                    break
                left += 1
            for left_node in timed[left:right]:
                _add_edge(
                    edges,
                    left_node.source_id,
                    right_node.source_id,
                    "fire_capture_start_within_10_seconds",
                )

    all_source_ids = [node.source_id for node in physical_nodes] + sorted(
        lineage_source_ids.values()
    )
    union_find = _UnionFind(all_source_ids)
    for left, right in sorted(edges):
        union_find.union(left, right)
    component_members: dict[str, list[str]] = defaultdict(list)
    for source_id in sorted(all_source_ids):
        component_members[union_find.find(source_id)].append(source_id)
    group_by_source: dict[str, str] = {}
    members_by_group: dict[str, list[str]] = {}
    for members in component_members.values():
        group_id = _split_group_id(members)
        members_by_group[group_id] = sorted(members)
        for source_id in members:
            group_by_source[source_id] = group_id

    physical_records: list[dict[str, object]] = []
    for node in physical_nodes:
        basename_record = None
        if node.basename is not None:
            basename_record = {
                "audio_suffix": node.basename.audio_suffix,
                "decoded_filename": node.basename.decoded_filename,
                "exact_stem": node.basename.exact_stem,
                "normalized_stem": node.basename.normalized_stem,
            }
        physical_records.append(
            {
                "capture_started_at": (
                    node.capture_started_at.isoformat()
                    if node.capture_started_at is not None
                    else None
                ),
                "dataset_family": node.dataset_family,
                "gcs_generation": node.gcs_generation,
                "gcs_md5": node.gcs_md5,
                "gcs_size": node.gcs_size,
                "kind": "gcs_source",
                "legacy": node.legacy,
                "legacy_basename": basename_record,
                "locators": [
                    _locator_record(locator)
                    for locator in sorted(node.locators)
                ],
                "recording_lineage_id": (
                    _lineage_record(node.recording_lineage_id)
                    if node.recording_lineage_id is not None
                    else None
                ),
                "source_id": node.source_id,
                "source_uri": node.source_uri,
                "split_group_id": group_by_source[node.source_id],
                "split_row_counts": dict(sorted(node.split_row_counts.items())),
            }
        )
    authoritative_edge_counts = Counter(
        source_id
        for pair, reasons in edges.items()
        if "authoritative_lineage" in reasons
        for source_id in pair
        if source_id.startswith("lineage-v1-")
    )
    lineage_records: list[dict[str, object]] = []
    for lineage in catalog:
        source_id = lineage_source_ids[lineage]
        lineage_records.append(
            {
                "authoritative_edge_count": authoritative_edge_counts[
                    source_id
                ],
                "kind": "lineage_source",
                "lineage_id": _lineage_record(lineage),
                "source_id": source_id,
                "split_group_id": group_by_source[source_id],
            }
        )

    edge_records = [
        {
            "evidence": sorted(evidence),
            "source_id_a": left,
            "source_id_b": right,
        }
        for (left, right), evidence in sorted(edges.items())
    ]
    summary = _build_grouping_summary(
        physical_nodes=physical_nodes,
        lineage_source_ids=lineage_source_ids,
        edge_records=edge_records,
        members_by_group=members_by_group,
        group_by_source=group_by_source,
    )
    policy = {
        "approval_status": "unapproved",
        "fire_capture_start_skew_seconds": FIRE_CAPTURE_START_SKEW_SECONDS,
        "fire_filename_schema": FIRE_FILENAME_SCHEMA,
        "group_algorithm": GROUP_ALGORITHM,
    }
    record = {
        "edges": edge_records,
        "grouping_summary": summary,
        "policy": policy,
        "schema_version": INDEX_SCHEMA_VERSION,
        "source_universe": [_manifest_lock_record(lock) for lock in universe],
        "sources": sorted(
            [*physical_records, *lineage_records],
            key=lambda item: item["source_id"],
        ),
    }
    summary_sha = _sha256_bytes(_canonical_json_bytes(summary))
    return SourceIndexCandidate(
        _payload=_canonical_json_bytes(record),
        grouping_summary_sha256=summary_sha,
    )


def _build_grouping_summary(
    *,
    physical_nodes: Sequence[_PhysicalNode],
    lineage_source_ids: Mapping[LineageId, str],
    edge_records: Sequence[Mapping[str, object]],
    members_by_group: Mapping[str, Sequence[str]],
    group_by_source: Mapping[str, str],
) -> dict[str, object]:
    node_by_id = {node.source_id: node for node in physical_nodes}
    rows_by_split: Counter[str] = Counter()
    sources_by_split: Counter[str] = Counter()
    for node in physical_nodes:
        for split, rows in node.split_row_counts.items():
            rows_by_split[split] += rows
            sources_by_split[split] += 1

    edge_counts_by_evidence: Counter[str] = Counter()
    edge_counts_by_split_pair: Counter[str] = Counter()

    def reporting_split(node: _PhysicalNode) -> str:
        """Collapse validation's permitted eval-subset membership for audits."""
        splits = set(node.split_row_counts)
        if "train" in splits:
            return "train"
        if "eval" in splits:
            return "eval"
        if "validation" in splits:
            return "validation"
        return "/".join(sorted(splits))

    for edge in edge_records:
        for evidence in edge["evidence"]:
            edge_counts_by_evidence[evidence] += 1
        involved_splits: set[str] = set()
        for source_id in (edge["source_id_a"], edge["source_id_b"]):
            node = node_by_id.get(source_id)
            if node is not None:
                involved_splits.add(reporting_split(node))
        split_pair = "/".join(sorted(involved_splits)) or "lineage_only"
        edge_counts_by_split_pair[split_pair] += 1

    singleton_count = 0
    non_singleton_count = 0
    mixed_groups: set[str] = set()
    max_component_size = 0
    max_fire_span = 0.0
    for group_id, members in members_by_group.items():
        max_component_size = max(max_component_size, len(members))
        if len(members) == 1:
            singleton_count += 1
        else:
            non_singleton_count += 1
        component_nodes = [
            node_by_id[item] for item in members if item in node_by_id
        ]
        splits = {
            split for node in component_nodes for split in node.split_row_counts
        }
        # Validation is allowed to be an exact eval subset.  "Mixed" in this
        # summary therefore means crossing the gradient-training boundary,
        # not merely appearing in more than one holdout view.
        if "train" in splits and ({"validation", "eval"} & splits):
            mixed_groups.add(group_id)

    # Report the transitive reach of the versioned +/-10-second rule itself.
    # Other independent evidence (for example equal MD5) may intentionally
    # connect recordings with distant capture clocks and must not inflate this
    # diagnostic of timestamp-window chaining.
    timestamp_source_ids = {
        source_id
        for edge in edge_records
        if "fire_capture_start_within_10_seconds" in edge["evidence"]
        for source_id in (edge["source_id_a"], edge["source_id_b"])
    }
    timestamp_components = _UnionFind(timestamp_source_ids)
    for edge in edge_records:
        if "fire_capture_start_within_10_seconds" in edge["evidence"]:
            timestamp_components.union(edge["source_id_a"], edge["source_id_b"])
    clocks_by_timestamp_component: dict[str, list[datetime_lib.datetime]] = (
        defaultdict(list)
    )
    for source_id in sorted(timestamp_source_ids):
        clock = node_by_id[source_id].capture_started_at
        if clock is not None:
            clocks_by_timestamp_component[
                timestamp_components.find(source_id)
            ].append(clock)
    for clocks in clocks_by_timestamp_component.values():
        if len(clocks) >= 2:
            clocks.sort()
            max_fire_span = max(
                max_fire_span, (clocks[-1] - clocks[0]).total_seconds()
            )

    affected_rows_by_split: Counter[str] = Counter()
    affected_sources_by_split: Counter[str] = Counter()
    for node in physical_nodes:
        if group_by_source[node.source_id] not in mixed_groups:
            continue
        for split, rows in node.split_row_counts.items():
            affected_rows_by_split[split] += rows
            affected_sources_by_split[split] += 1

    legacy_nodes = [node for node in physical_nodes if node.legacy]
    fire_legacy_nodes = [
        node
        for node in legacy_nodes
        if node.dataset_family == "fire_notifications"
    ]
    return {
        "affected_rows_by_split": dict(sorted(affected_rows_by_split.items())),
        "affected_sources_by_split": dict(
            sorted(affected_sources_by_split.items())
        ),
        "component_count": len(members_by_group),
        "edge_count": len(edge_records),
        "edge_counts_by_evidence": dict(
            sorted(edge_counts_by_evidence.items())
        ),
        "edge_counts_by_split_pair": dict(
            sorted(edge_counts_by_split_pair.items())
        ),
        "lineage_only_source_count": len(lineage_source_ids),
        "maximum_component_size": max_component_size,
        "maximum_transitive_fire_clock_span_seconds": max_fire_span,
        "mixed_component_count": len(mixed_groups),
        "non_singleton_component_count": non_singleton_count,
        "parse_coverage": {
            "authoritative_physical_sources": len(physical_nodes)
            - len(legacy_nodes),
            "fire_capture_clocks_parsed": sum(
                node.capture_started_at is not None
                for node in fire_legacy_nodes
            ),
            "fire_legacy_sources": len(fire_legacy_nodes),
            "legacy_basenames_parsed": sum(
                node.basename is not None for node in legacy_nodes
            ),
            "legacy_physical_sources": len(legacy_nodes),
        },
        "physical_source_count": len(physical_nodes),
        "rows_by_split": dict(sorted(rows_by_split.items())),
        "singleton_component_count": singleton_count,
        "sources_by_split": dict(sorted(sources_by_split.items())),
    }


def source_index_candidate_to_bytes(candidate: SourceIndexCandidate) -> bytes:
    """Return canonical audit bytes that remain unusable at runtime."""
    if not isinstance(candidate, SourceIndexCandidate):
        raise TypeError("candidate must be a SourceIndexCandidate")
    return bytes(candidate._payload)


def approve_source_index(
    candidate: SourceIndexCandidate,
    *,
    expected_grouping_summary_sha256: str,
) -> SourceIndex:
    """Bind a reviewed summary digest and produce an approved runtime index."""
    if not isinstance(candidate, SourceIndexCandidate):
        raise TypeError("candidate must be a SourceIndexCandidate")
    expected = _validate_sha256(
        expected_grouping_summary_sha256,
        "expected grouping summary sha256",
    )
    if expected != candidate.grouping_summary_sha256:
        raise ValueError(
            "grouping summary SHA mismatch: "
            f"expected {expected}, observed {candidate.grouping_summary_sha256}"
        )
    record = candidate.record
    policy = record["policy"]
    assert isinstance(policy, dict)
    if policy.get("approval_status") != "unapproved":
        raise ValueError("candidate policy is not explicitly unapproved")
    policy["approval_status"] = "approved"
    policy["approved_grouping_summary_sha256"] = expected
    payload = _canonical_json_bytes(record)
    return SourceIndex(_payload=payload)


def source_index_to_bytes(index: SourceIndex) -> bytes:
    """Return canonical bytes only for an approved source index."""
    if not isinstance(index, SourceIndex):
        raise TypeError("index must be an approved SourceIndex")
    return bytes(index._payload)


def _lineage_from_record(record: object, name: str) -> LineageId:
    if not isinstance(record, dict) or frozenset(record) != _LINEAGE_KEYS:
        raise ValueError(f"{name} must contain exactly the lineage fields")
    return LineageId(
        producer_namespace=record["producer_namespace"],
        lineage_schema_version=record["lineage_schema_version"],
        recording_id=record["recording_id"],
    )


def _locator_from_record(record: object) -> SourceLocator:
    if not isinstance(record, dict) or frozenset(record) != {
        "dataset_family",
        "kind",
        "value",
    }:
        raise ValueError("serialized locator has unsupported fields")
    return SourceLocator(
        kind=record["kind"],
        value=record["value"],
        dataset_family=record["dataset_family"],
    )


def _lock_from_record(record: object) -> ManifestLock:
    if not isinstance(record, dict) or frozenset(record) != {
        "gcs_generation",
        "manifest_uri",
        "role",
        "sha256",
    }:
        raise ValueError("serialized manifest lock has unsupported fields")
    return ManifestLock(
        role=record["role"],
        manifest_uri=record["manifest_uri"],
        gcs_generation=record["gcs_generation"],
        sha256=record["sha256"],
    )


def source_index_from_bytes(
    payload: bytes,
    *,
    expected_source_universe: Iterable[ManifestLock],
    expected_sha256: str | None = None,
) -> SourceIndex:
    """Strictly load a canonical, approved, source-universe-locked index."""
    if not isinstance(payload, bytes):
        raise TypeError("source index payload must be bytes")
    try:
        record = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("source index is not valid UTF-8 JSON") from exc
    if _canonical_json_bytes(record) != payload:
        raise ValueError("source index JSON is not canonical")
    if not isinstance(record, dict) or frozenset(record) != _TOP_LEVEL_KEYS:
        raise ValueError("source index has unsupported top-level fields")
    if record["schema_version"] != INDEX_SCHEMA_VERSION:
        raise ValueError("unsupported source index schema_version")
    if expected_sha256 is not None:
        expected_digest = _validate_sha256(
            expected_sha256, "expected source index sha256"
        )
        observed = _sha256_bytes(payload)
        if observed != expected_digest:
            raise ValueError(
                f"source index SHA mismatch: expected {expected_digest}, observed {observed}"
            )

    policy = record["policy"]
    if (
        isinstance(policy, dict)
        and policy.get("approval_status") == "unapproved"
    ):
        raise ValueError("source index is unapproved")
    expected_policy_keys = {
        "approval_status",
        "approved_grouping_summary_sha256",
        "fire_capture_start_skew_seconds",
        "fire_filename_schema",
        "group_algorithm",
    }
    if not isinstance(policy, dict) or set(policy) != expected_policy_keys:
        raise ValueError("source index policy has unsupported fields")
    if policy["approval_status"] != "approved":
        raise ValueError("source index is unapproved")
    if (
        policy["group_algorithm"] != GROUP_ALGORITHM
        or policy["fire_filename_schema"] != FIRE_FILENAME_SCHEMA
        or policy["fire_capture_start_skew_seconds"]
        != FIRE_CAPTURE_START_SKEW_SECONDS
    ):
        raise ValueError("source index policy does not match this runtime")
    approved_summary_sha = _validate_sha256(
        policy["approved_grouping_summary_sha256"],
        "approved grouping summary sha256",
    )
    if not isinstance(record["grouping_summary"], dict):
        raise ValueError("grouping_summary must be an object")
    observed_summary_sha = _sha256_bytes(
        _canonical_json_bytes(record["grouping_summary"])
    )
    if approved_summary_sha != observed_summary_sha:
        raise ValueError(
            "approved grouping summary SHA does not match index summary"
        )

    if not isinstance(record["source_universe"], list):
        raise ValueError("source_universe must be a list")
    observed_universe = tuple(
        sorted(_lock_from_record(item) for item in record["source_universe"])
    )
    if len(set(observed_universe)) != len(observed_universe):
        raise ValueError("serialized source universe contains duplicates")
    expected_universe = _normalize_source_universe(expected_source_universe)
    if observed_universe != expected_universe:
        raise ValueError(
            "source universe does not exactly match expected manifest locks"
        )

    _validate_serialized_sources_and_edges(record)
    return SourceIndex(_payload=bytes(payload))


def _validate_serialized_sources_and_edges(
    record: Mapping[str, object],
) -> None:
    sources = record["sources"]
    edges = record["edges"]
    if not isinstance(sources, list) or not isinstance(edges, list):
        raise ValueError("sources and edges must be lists")
    source_ids: list[str] = []
    masked_owners: dict[SourceLocator, str] = {}
    for source in sources:
        if not isinstance(source, dict):
            raise ValueError("every serialized source must be an object")
        kind = source.get("kind")
        if kind == "gcs_source":
            expected_keys = {
                "capture_started_at",
                "dataset_family",
                "gcs_generation",
                "gcs_md5",
                "gcs_size",
                "kind",
                "legacy",
                "legacy_basename",
                "locators",
                "recording_lineage_id",
                "source_id",
                "source_uri",
                "split_group_id",
                "split_row_counts",
            }
            if set(source) != expected_keys:
                raise ValueError("gcs_source has unsupported fields")
            family = _nonblank(source["dataset_family"], "dataset_family")
            uri = _validate_gcs_object_uri(source["source_uri"])
            generation = _nonnegative_int(
                source["gcs_generation"], "gcs_generation", positive=True
            )
            _nonnegative_int(source["gcs_size"], "gcs_size")
            if source["gcs_md5"] is not None:
                _nonblank(source["gcs_md5"], "gcs_md5")
            if not isinstance(source["legacy"], bool):
                raise TypeError("legacy must be a boolean")
            lineage = None
            if source["recording_lineage_id"] is not None:
                lineage = _lineage_from_record(
                    source["recording_lineage_id"], "recording_lineage_id"
                )
            if source["legacy"] != (lineage is None):
                raise ValueError(
                    "serialized source has contradictory lineage mode"
                )
            expected_source_id = _physical_source_id(family, uri, generation)
            if source["source_id"] != expected_source_id:
                raise ValueError(
                    "physical source_id does not match stable GCS identity"
                )
            if not isinstance(source["locators"], list):
                raise ValueError("source locators must be a list")
            locators = tuple(
                _locator_from_record(item) for item in source["locators"]
            )
            if locators != tuple(sorted(set(locators))):
                raise ValueError(
                    "serialized source locators must be sorted and unique"
                )
            if SourceLocator("source_uri", uri) not in locators:
                raise ValueError(
                    "physical source is missing its global source_uri locator"
                )
            for locator in locators:
                if locator.kind != "masked_example_id":
                    continue
                previous = masked_owners.get(locator)
                if previous is not None and previous != source["source_id"]:
                    raise ValueError("masked_example_id locator is ambiguous")
                masked_owners[locator] = source["source_id"]
            split_counts = source["split_row_counts"]
            if not isinstance(split_counts, dict) or not split_counts:
                raise ValueError(
                    "physical source split_row_counts must be nonempty"
                )
            for split, count in split_counts.items():
                _nonblank(split, "split")
                _nonnegative_int(count, "split row count", positive=True)
            if list(split_counts) != sorted(split_counts):
                raise ValueError("split_row_counts keys must be sorted")
            if source["legacy"]:
                basename = source["legacy_basename"]
                if not isinstance(basename, dict) or set(basename) != {
                    "audio_suffix",
                    "decoded_filename",
                    "exact_stem",
                    "normalized_stem",
                }:
                    raise ValueError(
                        "legacy source lacks canonical basename evidence"
                    )
                if basename["audio_suffix"] not in RECOGNIZED_AUDIO_SUFFIXES:
                    raise ValueError(
                        "legacy basename has an unsupported suffix"
                    )
                for field_name in ("decoded_filename", "exact_stem"):
                    field_value = basename[field_name]
                    if (
                        not isinstance(field_value, str)
                        or not field_value.strip()
                    ):
                        raise ValueError(
                            f"legacy_basename.{field_name} must be a nonblank string"
                        )
                _nonblank(
                    basename["normalized_stem"],
                    "legacy_basename.normalized_stem",
                )
                if family == "fire_notifications":
                    capture_started_at = _nonblank(
                        source["capture_started_at"], "capture_started_at"
                    )
                    try:
                        capture_clock = datetime_lib.datetime.fromisoformat(
                            capture_started_at
                        )
                    except ValueError as exc:
                        raise ValueError(
                            "fire capture clock is not an ISO local datetime"
                        ) from exc
                    if (
                        capture_clock.tzinfo is not None
                        or capture_clock.microsecond != 0
                    ):
                        raise ValueError(
                            "fire capture clock must be a whole-second naive datetime"
                        )
                elif source["capture_started_at"] is not None:
                    raise ValueError(
                        "non-fire source cannot carry a fire capture clock"
                    )
            elif (
                source["legacy_basename"] is not None
                or source["capture_started_at"] is not None
            ):
                raise ValueError(
                    "authoritative source cannot carry legacy parser evidence"
                )
        elif kind == "lineage_source":
            if set(source) != {
                "authoritative_edge_count",
                "kind",
                "lineage_id",
                "source_id",
                "split_group_id",
            }:
                raise ValueError("lineage_source has unsupported fields")
            lineage = _lineage_from_record(source["lineage_id"], "lineage_id")
            if source["source_id"] != _lineage_source_id(lineage):
                raise ValueError(
                    "lineage source_id does not match stable lineage identity"
                )
            _nonnegative_int(
                source["authoritative_edge_count"], "authoritative_edge_count"
            )
        else:
            raise ValueError(f"unsupported serialized source kind: {kind!r}")
        source_id = _nonblank(source["source_id"], "source_id")
        group_id = _nonblank(source["split_group_id"], "split_group_id")
        if not group_id.startswith("splitgrp-v1-"):
            raise ValueError("unsupported split_group_id")
        source_ids.append(source_id)
    if source_ids != sorted(set(source_ids)):
        raise ValueError(
            "serialized sources must be sorted with unique source IDs"
        )

    known_evidence = {
        "authoritative_lineage",
        "equal_nonblank_md5",
        "exact_object",
        "fire_capture_start_within_10_seconds",
        "legacy_exact_basename",
        "legacy_normalized_basename",
    }
    normalized_edges: list[tuple[str, str, tuple[str, ...]]] = []
    source_id_set = set(source_ids)
    for edge in edges:
        if not isinstance(edge, dict) or set(edge) != {
            "evidence",
            "source_id_a",
            "source_id_b",
        }:
            raise ValueError("serialized edge has unsupported fields")
        left = _nonblank(edge["source_id_a"], "source_id_a")
        right = _nonblank(edge["source_id_b"], "source_id_b")
        if (
            left >= right
            or left not in source_id_set
            or right not in source_id_set
        ):
            raise ValueError(
                "serialized edge endpoints are invalid or unsorted"
            )
        evidence = edge["evidence"]
        if (
            not isinstance(evidence, list)
            or not evidence
            or not all(isinstance(item, str) for item in evidence)
            or evidence != sorted(set(evidence))
            or not set(evidence).issubset(known_evidence)
        ):
            raise ValueError("serialized edge evidence is invalid")
        normalized_edges.append((left, right, tuple(evidence)))
    if normalized_edges != sorted(set(normalized_edges)):
        raise ValueError("serialized edges must be sorted and unique")

    union_find = _UnionFind(source_ids)
    for left, right, _ in normalized_edges:
        union_find.union(left, right)
    component_members: dict[str, list[str]] = defaultdict(list)
    for source_id in source_ids:
        component_members[union_find.find(source_id)].append(source_id)
    expected_groups = {
        source_id: _split_group_id(members)
        for members in component_members.values()
        for source_id in members
    }
    for source in sources:
        if source["split_group_id"] != expected_groups[source["source_id"]]:
            raise ValueError(
                "split_group_id does not match serialized graph component"
            )


def _validate_owned_index_payload(
    payload: bytes,
    *,
    expected_approval_status: str,
) -> dict[str, object]:
    """Validate bytes held by the otherwise-public frozen index classes."""
    if not isinstance(payload, bytes):
        raise TypeError("source index payload must be bytes")
    try:
        record = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("source index is not valid UTF-8 JSON") from exc
    if _canonical_json_bytes(record) != payload:
        raise ValueError("source index JSON is not canonical")
    if not isinstance(record, dict) or frozenset(record) != _TOP_LEVEL_KEYS:
        raise ValueError("source index has unsupported top-level fields")
    if record["schema_version"] != INDEX_SCHEMA_VERSION:
        raise ValueError("unsupported source index schema_version")
    policy = record["policy"]
    common_policy = {
        "approval_status",
        "fire_capture_start_skew_seconds",
        "fire_filename_schema",
        "group_algorithm",
    }
    expected_policy_keys = (
        common_policy
        if expected_approval_status == "unapproved"
        else common_policy | {"approved_grouping_summary_sha256"}
    )
    if (
        not isinstance(policy, dict)
        or set(policy) != expected_policy_keys
        or policy["approval_status"] != expected_approval_status
        or policy["group_algorithm"] != GROUP_ALGORITHM
        or policy["fire_filename_schema"] != FIRE_FILENAME_SCHEMA
        or policy["fire_capture_start_skew_seconds"]
        != FIRE_CAPTURE_START_SKEW_SECONDS
    ):
        raise ValueError(
            "source index policy does not match its declared state"
        )
    if not isinstance(record["grouping_summary"], dict):
        raise ValueError("grouping_summary must be an object")
    summary_sha = _sha256_bytes(
        _canonical_json_bytes(record["grouping_summary"])
    )
    if (
        expected_approval_status == "approved"
        and _validate_sha256(
            policy["approved_grouping_summary_sha256"],
            "approved grouping summary sha256",
        )
        != summary_sha
    ):
        raise ValueError(
            "approved grouping summary SHA does not match index summary"
        )
    if not isinstance(record["source_universe"], list):
        raise ValueError("source_universe must be a list")
    observed_locks = tuple(
        _lock_from_record(item) for item in record["source_universe"]
    )
    if observed_locks != tuple(sorted(set(observed_locks))):
        raise ValueError("serialized source universe must be sorted and unique")
    _validate_serialized_sources_and_edges(record)
    return record


def canonical_row_binding(
    row: CanonicalRow | Mapping[str, Any],
    *,
    split: str,
    row_index: int,
) -> RowBinding:
    """Adapt a canonical row into exact lineage or source-URI membership."""
    identity = canonical_row_identity(row)
    lineages = lineage_ids_from_row(row)
    if lineages:
        return RowBinding(
            split=split,
            row_index=row_index,
            row_identity=identity,
            locators=(),
            recording_lineage_ids=lineages,
        )
    source_audio: object
    original_audio_uri: object = None
    if isinstance(row, CanonicalRow):
        source_audio = row.source_audio
    elif isinstance(row, Mapping):
        source_audio = row.get("source_audio")
        original_audio_uri = row.get("original_audio_uri")
    else:
        raise TypeError("row must be a CanonicalRow or mapping")
    source_uri: object = None
    if isinstance(source_audio, Mapping):
        source_uri = source_audio.get("audio_filepath")
        if source_uri is None:
            source_uri = source_audio.get("original_audio_uri")
    if source_uri is None:
        source_uri = original_audio_uri
    if not isinstance(source_uri, str) or not source_uri.strip():
        raise ValueError(
            "canonical row lacks source_audio.audio_filepath or original_audio_uri"
        )
    if source_uri != source_uri.strip():
        raise ValueError(
            "canonical source URI must not have surrounding whitespace"
        )
    return RowBinding(
        split=split,
        row_index=row_index,
        row_identity=identity,
        locators=(SourceLocator("source_uri", source_uri),),
        recording_lineage_ids=(),
    )


def locator_row_binding(
    *,
    split: str,
    row_index: int,
    row_identity: tuple[str, str],
    locator: SourceLocator,
) -> RowBinding:
    """Create a row binding from an exact frozen locator."""
    if not isinstance(locator, SourceLocator):
        raise TypeError("locator must be a SourceLocator")
    return RowBinding(
        split=split,
        row_index=row_index,
        row_identity=row_identity,
        locators=(locator,),
        recording_lineage_ids=(),
    )


def group_ids_for_locator(
    locator: SourceLocator, index: SourceIndex
) -> frozenset[str]:
    """Resolve a locator through an approved index under kind-specific rules."""
    if not isinstance(index, SourceIndex):
        raise TypeError("index must be an approved SourceIndex")
    return index.group_ids_for_locator(locator)


def group_ids_for_binding(
    binding: RowBinding, index: SourceIndex
) -> frozenset[str]:
    """Resolve every independent constituent referenced by a row binding."""
    if not isinstance(binding, RowBinding):
        raise TypeError("binding must be a RowBinding")
    if not isinstance(index, SourceIndex):
        raise TypeError("index must be an approved SourceIndex")
    group_ids: set[str] = set()
    for lineage in binding.recording_lineage_ids:
        group_ids.update(index.group_ids_for_lineage(lineage))
    for locator in binding.locators:
        group_ids.update(index.group_ids_for_locator(locator))
    if not group_ids:
        raise ValueError("row binding resolved to no recording groups")
    return frozenset(group_ids)


class RecordingGroupLeakageError(ValueError):
    """Bounded human exception carrying a complete machine-readable report."""

    def __init__(self, report: dict[str, object]) -> None:
        self.report = copy.deepcopy(report)
        affected = report.get("affected_groups", [])
        errors = report.get("resolution_errors", [])
        assert isinstance(affected, list)
        assert isinstance(errors, list)
        sample_ids = [group["split_group_id"] for group in affected[:5]]
        message = (
            "recording-group leakage gate failed: "
            f"{len(affected)} affected group(s), {len(errors)} resolution error(s)"
        )
        if sample_ids:
            message += f"; sample groups: {', '.join(sample_ids)}"
        super().__init__(message)


def _row_report(binding: RowBinding) -> dict[str, object]:
    return {
        "example_id": binding.row_identity[0],
        "row_index": binding.row_index,
        "segment_id": binding.row_identity[1],
        "split": binding.split,
    }


def build_training_boundary_report(
    *,
    bindings_by_split: Mapping[str, Iterable[RowBinding]],
    index: SourceIndex,
    allow_validation_eval_overlap: bool,
) -> dict[str, object]:
    """Resolve every row and return complete pass/fail evidence without raising."""
    if not isinstance(index, SourceIndex):
        raise TypeError("index must be an approved SourceIndex")
    if not isinstance(bindings_by_split, Mapping):
        raise TypeError("bindings_by_split must be a mapping")
    if not isinstance(allow_validation_eval_overlap, bool):
        raise TypeError("allow_validation_eval_overlap must be a boolean")
    allowed_splits = {"train", "validation", "eval"}
    unknown_splits = set(bindings_by_split) - allowed_splits
    if unknown_splits:
        raise ValueError(f"unsupported split keys: {sorted(unknown_splits)!r}")

    normalized_bindings: dict[str, tuple[RowBinding, ...]] = {}
    for split in sorted(allowed_splits):
        values = tuple(bindings_by_split.get(split, ()))
        if not all(isinstance(value, RowBinding) for value in values):
            raise TypeError(f"bindings for {split!r} must be RowBinding values")
        if any(value.split != split for value in values):
            raise ValueError(
                f"binding split does not match mapping key {split!r}"
            )
        row_keys = [(value.row_index, value.row_identity) for value in values]
        if len(set(row_keys)) != len(row_keys):
            raise ValueError(
                f"bindings for {split!r} contain duplicate row identities"
            )
        normalized_bindings[split] = tuple(
            sorted(
                values, key=lambda value: (value.row_index, value.row_identity)
            )
        )

    groups_by_split: dict[str, set[str]] = {
        split: set() for split in sorted(allowed_splits)
    }
    rows_by_group: dict[str, list[dict[str, object]]] = defaultdict(list)
    resolution_errors: list[dict[str, object]] = []
    resolved_row_count = 0
    resolved_membership_count = 0
    for split in sorted(allowed_splits):
        for binding in normalized_bindings[split]:
            try:
                group_ids = group_ids_for_binding(binding, index)
            except (TypeError, ValueError) as exc:
                resolution_errors.append(
                    {
                        **_row_report(binding),
                        "error": str(exc),
                    }
                )
                continue
            resolved_row_count += 1
            resolved_membership_count += len(group_ids)
            for group_id in sorted(group_ids):
                groups_by_split[split].add(group_id)
                rows_by_group[group_id].append(_row_report(binding))

    train_validation = sorted(
        groups_by_split["train"] & groups_by_split["validation"]
    )
    train_eval = sorted(groups_by_split["train"] & groups_by_split["eval"])
    validation_eval = sorted(
        groups_by_split["validation"] & groups_by_split["eval"]
    )
    forbidden_groups = set(train_validation) | set(train_eval)
    if not allow_validation_eval_overlap:
        forbidden_groups.update(validation_eval)

    index_record = index.record
    sources = index_record["sources"]
    edges = index_record["edges"]
    assert isinstance(sources, list)
    assert isinstance(edges, list)
    affected_groups: list[dict[str, object]] = []
    for group_id in sorted(forbidden_groups):
        group_sources = [
            copy.deepcopy(source)
            for source in sources
            if source["split_group_id"] == group_id
        ]
        source_ids = {source["source_id"] for source in group_sources}
        forming_edges = [
            copy.deepcopy(edge)
            for edge in edges
            if edge["source_id_a"] in source_ids
            and edge["source_id_b"] in source_ids
        ]
        affected_rows = sorted(
            rows_by_group[group_id],
            key=lambda row: (
                row["split"],
                row["row_index"],
                row["example_id"],
                row["segment_id"],
            ),
        )
        split_row_counts = {
            split: sum(row["split"] == split for row in affected_rows)
            for split in ("eval", "train", "validation")
        }
        reasons: list[str] = []
        if group_id in train_validation:
            reasons.append("train_validation")
        if group_id in train_eval:
            reasons.append("train_eval")
        if group_id in validation_eval and not allow_validation_eval_overlap:
            reasons.append("validation_eval_not_allowed")
        affected_groups.append(
            {
                "affected_rows": affected_rows,
                "forming_edges": forming_edges,
                "intersection_reasons": reasons,
                "sources": group_sources,
                "split_group_id": group_id,
                "split_row_counts": split_row_counts,
            }
        )

    input_row_count = sum(
        len(values) for values in normalized_bindings.values()
    )
    all_rows_resolved = resolved_row_count == input_row_count
    report: dict[str, object] = {
        "affected_groups": affected_groups,
        "all_rows_resolved": all_rows_resolved,
        "index_sha256": index.sha256,
        "input_row_count": input_row_count,
        "manifest_universe_locks": copy.deepcopy(
            index_record["source_universe"]
        ),
        "policy_version": GROUP_ALGORITHM,
        "resolution_errors": resolution_errors,
        "resolved_group_membership_count": resolved_membership_count,
        "resolved_row_count": resolved_row_count,
        "rows_by_split": {
            split: len(normalized_bindings[split])
            for split in sorted(allowed_splits)
        },
        "status": (
            "pass" if all_rows_resolved and not affected_groups else "fail"
        ),
        "training_boundary_intersections": {
            "train_eval": train_eval,
            "train_validation": train_validation,
        },
        "validation_eval_intersection": validation_eval,
        "validation_eval_overlap_allowed": allow_validation_eval_overlap,
    }
    return report


def require_training_boundary_disjoint(
    *,
    bindings_by_split: Mapping[str, Iterable[RowBinding]],
    index: SourceIndex,
    allow_validation_eval_overlap: bool,
) -> dict[str, object]:
    """Return the complete passing report or raise with the complete failure."""
    report = build_training_boundary_report(
        bindings_by_split=bindings_by_split,
        index=index,
        allow_validation_eval_overlap=allow_validation_eval_overlap,
    )
    if report["status"] != "pass":
        raise RecordingGroupLeakageError(report)
    return report
