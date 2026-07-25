"""Physical-recording split leakage checks for Gemini SFT manifests."""

from __future__ import annotations

import collections.abc
import re
import typing
import unicodedata
import urllib.parse

_AUDIO_SUFFIXES: typing.Final = (
    ".flac",
    ".wav",
    ".mp3",
    ".m4a",
    ".aac",
    ".ogg",
    ".opus",
    ".webm",
)
_SHA256_PATTERN: typing.Final = re.compile(r"[0-9a-f]{64}\Z")
_PhysicalSourceKey = tuple[str, str]


def reject_split_leakage(
    rows_by_split: collections.abc.Mapping[
        str, collections.abc.Sequence[dict[str, typing.Any]]
    ],
) -> None:
    """Reject physical recordings shared by training and a holdout split.

    Rows are joined by their best existing source URI and, when present, the
    reconstructed dataset's source-encoding SHA-256. A dataset with any row
    lacking that hash also uses normalized source filenames as compatibility
    evidence. Validation and eval may intentionally share recordings.

    Args:
        rows_by_split: Canonical manifest rows keyed by split.

    Returns:
        None after all split pairs pass validation.

    Raises:
        ValueError: If source metadata is invalid or a training recording also
            appears in validation or eval.
    """
    nodes_by_split: dict[str, list[_PhysicalSourceKey]] = {}
    physical_nodes: set[_PhysicalSourceKey] = set()
    sha_by_physical: dict[_PhysicalSourceKey, str | None] = {}
    for split in ("train", "validation", "eval"):
        nodes: list[_PhysicalSourceKey] = []
        for row_index, row in enumerate(rows_by_split.get(split, ())):
            physical = (
                _dataset_name(row) or "",
                _source_uri(row, split=split, row_index=row_index),
            )
            physical_nodes.add(physical)
            source_sha = _source_sha(row, split=split, row_index=row_index)
            if source_sha is not None and sha_by_physical.get(physical) not in (
                None,
                source_sha,
            ):
                msg = (
                    f"{split} row {row_index} contradicts source SHA-256 "
                    "for one physical source"
                )
                raise ValueError(msg)
            if source_sha is not None or physical not in sha_by_physical:
                sha_by_physical[physical] = source_sha
            nodes.append(physical)
        nodes_by_split[split] = nodes

    groups = _UnionFind(physical_nodes)
    by_uri: dict[str, list[_PhysicalSourceKey]] = {}
    by_sha: dict[str, list[_PhysicalSourceKey]] = {}
    by_basename: dict[tuple[str, str], list[_PhysicalSourceKey]] = {}
    legacy_datasets = {
        dataset
        for (dataset, _), source_sha in sha_by_physical.items()
        if source_sha is None and dataset
    }
    for node in physical_nodes:
        dataset, source_uri = node
        by_uri.setdefault(source_uri, []).append(node)
        source_sha = sha_by_physical[node]
        if source_sha is not None:
            by_sha.setdefault(source_sha, []).append(node)
        if dataset not in legacy_datasets:
            continue
        normalized_basename = _normalized_basename(source_uri)
        if normalized_basename is None:
            continue
        basename_key = (dataset, normalized_basename)
        by_basename.setdefault(basename_key, []).append(node)
    for matches in (
        *by_uri.values(),
        *by_sha.values(),
        *by_basename.values(),
    ):
        groups.merge_all(matches)

    sources_by_split = {
        split: {groups.root(node) for node in nodes}
        for split, nodes in nodes_by_split.items()
    }
    for holdout in ("validation", "eval"):
        overlap = sources_by_split["train"] & sources_by_split[holdout]
        if overlap:
            msg = (
                f"train and {holdout} share {len(overlap)} physical "
                "recording group(s); sample source(s): "
                f"{_source_sample(physical_nodes, groups, overlap)}"
            )
            raise ValueError(msg)


def _source_sample(
    physical_nodes: collections.abc.Iterable[_PhysicalSourceKey],
    groups: _UnionFind,
    overlap: collections.abc.Set[_PhysicalSourceKey],
) -> str:
    """Format representative physical sources from overlapping groups."""
    matches = sorted(
        node for node in physical_nodes if groups.root(node) in overlap
    )[:5]
    return ", ".join(
        f"{dataset or '<unknown>'}: {source_uri}"
        for dataset, source_uri in matches
    )


def _source_uri(
    row: collections.abc.Mapping[str, typing.Any],
    *,
    split: str,
    row_index: int,
) -> str:
    """Return the best available physical-source locator for a manifest row.

    Args:
        row: Canonical manifest row.
        split: Split name used in validation errors.
        row_index: Zero-based row index used in validation errors.

    Returns:
        The first nonblank source locator in precedence order.

    Raises:
        ValueError: If the row has no usable physical-source locator.
    """
    candidates: list[object] = [row.get("original_audio_uri")]
    source_audio = row.get("source_audio")
    if isinstance(source_audio, collections.abc.Mapping):
        candidates.extend(
            (
                source_audio.get("audio_filepath"),
                source_audio.get("original_audio_uri"),
            )
        )
    candidates.append(row.get("audio_filepath"))
    for value in candidates:
        if isinstance(value, str) and value.strip():
            return value.strip()
    msg = f"{split} row {row_index} lacks an original physical source URI"
    raise ValueError(msg)


def _dataset_name(
    row: collections.abc.Mapping[str, typing.Any],
) -> str | None:
    """Return a validated dataset name when the row provides one.

    Args:
        row: Canonical manifest row.

    Returns:
        The stripped dataset name, or None when absent.

    Raises:
        ValueError: If a provided dataset name is not a nonblank string.
    """
    dataset = row.get("dataset")
    value = (
        dataset.get("name")
        if isinstance(dataset, collections.abc.Mapping)
        else None
    )
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        msg = "dataset.name must be a nonblank string when provided"
        raise ValueError(msg)
    return value.strip()


def _source_sha(
    row: collections.abc.Mapping[str, typing.Any],
    *,
    split: str,
    row_index: int,
) -> str | None:
    """Return a validated physical-source SHA-256 when available.

    Args:
        row: Canonical manifest row.
        split: Split name used in validation errors.
        row_index: Zero-based row index used in validation errors.

    Returns:
        The lowercase SHA-256, or None when lineage metadata omits it.

    Raises:
        ValueError: If the provided SHA-256 is malformed.
    """
    source_lineage = row.get("source_lineage")
    if not isinstance(source_lineage, collections.abc.Mapping):
        return None
    value = source_lineage.get("source_encoded_sha256")
    if value is None:
        return None
    if not isinstance(value, str) or _SHA256_PATTERN.fullmatch(value) is None:
        msg = (
            f"{split} row {row_index} source_lineage.source_encoded_sha256 "
            "must be a lowercase SHA-256"
        )
        raise ValueError(msg)
    return value


def _normalized_basename(source_uri: str) -> str | None:
    """Return optional basename evidence for a supported GCS audio object.

    Args:
        source_uri: Physical-source locator.

    Returns:
        A normalized filename stem, or None when it cannot be derived safely.
    """
    stem = _filename_stem(source_uri)
    if stem is None:
        return None
    normalized = " ".join(
        unicodedata.normalize("NFKC", stem).casefold().split()
    )
    return normalized or None


def _filename_stem(source_uri: str) -> str | None:
    """Extract a known audio filename stem without narrowing valid locators.

    Args:
        source_uri: Physical-source locator.

    Returns:
        The decoded filename stem for a supported GCS audio object, or None
        when the locator is unsuitable for conservative basename matching.
    """
    try:
        parsed = urllib.parse.urlsplit(source_uri)
    except ValueError:
        return None
    if (
        parsed.scheme != "gs"
        or not parsed.netloc
        or not parsed.path
        or parsed.path.endswith("/")
    ):
        return None
    encoded_name = parsed.path.rsplit("/", maxsplit=1)[-1]
    try:
        filename = urllib.parse.unquote(encoded_name, errors="strict")
    except UnicodeDecodeError:
        return None
    lowered = filename.casefold()
    suffix = next(
        (value for value in _AUDIO_SUFFIXES if lowered.endswith(value)),
        None,
    )
    if suffix is None:
        return None
    stem = filename[: -len(suffix)]
    return stem or None


class _UnionFind:
    """Track transitive physical-source aliases."""

    def __init__(
        self,
        values: collections.abc.Iterable[_PhysicalSourceKey],
    ) -> None:
        self._parent = {value: value for value in values}

    def root(self, value: _PhysicalSourceKey) -> _PhysicalSourceKey:
        parent = self._parent[value]
        if parent != value:
            self._parent[value] = self.root(parent)
        return self._parent[value]

    def merge_all(
        self,
        values: collections.abc.Sequence[_PhysicalSourceKey],
    ) -> None:
        if not values:
            return
        first = values[0]
        for value in values[1:]:
            left = self.root(first)
            right = self.root(value)
            if left != right:
                self._parent[right] = left
