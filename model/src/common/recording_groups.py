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
_Node = tuple[str, str]


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

    Raises:
        ValueError: If source metadata is invalid or a training recording also
            appears in validation or eval.
    """
    nodes_by_split: dict[str, list[_Node]] = {}
    physical_nodes: set[_Node] = set()
    sha_by_physical: dict[_Node, str | None] = {}
    for split in ("train", "validation", "eval"):
        nodes: list[_Node] = []
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
    by_uri: dict[str, list[_Node]] = {}
    by_sha: dict[str, list[_Node]] = {}
    by_basename: dict[tuple[str, str], list[_Node]] = {}
    legacy_datasets = {
        node[0]
        for node, source_sha in sha_by_physical.items()
        if source_sha is None and node[0]
    }
    for node in physical_nodes:
        dataset, source_uri = node
        by_uri.setdefault(source_uri, []).append(node)
        source_sha = sha_by_physical[node]
        if source_sha is not None:
            by_sha.setdefault(source_sha, []).append(node)
        if dataset not in legacy_datasets:
            continue
        basename_key = (dataset, _normalized_basename(source_uri))
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
    physical_nodes: collections.abc.Iterable[_Node],
    groups: _UnionFind,
    overlap: collections.abc.Set[object],
) -> str:
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


def _normalized_basename(source_uri: str) -> str:
    stem = _filename_stem(source_uri)
    normalized = " ".join(
        unicodedata.normalize("NFKC", stem).casefold().split()
    )
    if not normalized:
        msg = "original physical source has a blank filename stem"
        raise ValueError(msg)
    return normalized


def _filename_stem(source_uri: str) -> str:
    parsed = urllib.parse.urlsplit(source_uri)
    if (
        parsed.scheme != "gs"
        or not parsed.netloc
        or not parsed.path
        or parsed.path.endswith("/")
    ):
        msg = "original physical source must be a single-object gs:// URI"
        raise ValueError(msg)
    encoded_name = parsed.path.rsplit("/", maxsplit=1)[-1]
    try:
        filename = urllib.parse.unquote(encoded_name, errors="strict")
    except UnicodeDecodeError as exc:
        msg = "original physical source filename is not valid UTF-8"
        raise ValueError(msg) from exc
    lowered = filename.casefold()
    suffix = next(
        (value for value in _AUDIO_SUFFIXES if lowered.endswith(value)),
        None,
    )
    if suffix is None:
        msg = "original physical source has an unsupported audio suffix"
        raise ValueError(msg)
    stem = filename[: -len(suffix)]
    if not stem:
        msg = "original physical source has a blank filename stem"
        raise ValueError(msg)
    return stem


class _UnionFind:
    def __init__(self, values: collections.abc.Iterable[object]) -> None:
        self._parent = {value: value for value in values}

    def root(self, value: object) -> object:
        parent = self._parent[value]
        if parent != value:
            self._parent[value] = self.root(parent)
        return self._parent[value]

    def merge_all(self, values: collections.abc.Sequence[object]) -> None:
        if not values:
            return
        first = values[0]
        for value in values[1:]:
            left = self.root(first)
            right = self.root(value)
            if left != right:
                self._parent[right] = left
