"""Physical-recording split leakage checks for Gemini SFT manifests."""

from __future__ import annotations

import collections.abc
import itertools
import re
import typing

_SHA256_PATTERN: typing.Final = re.compile(r"[0-9a-fA-F]{64}\Z")
_PhysicalSourceKey = tuple[str, str | None]


def reject_split_leakage(
    rows_by_split: collections.abc.Mapping[
        str, collections.abc.Sequence[dict[str, typing.Any]]
    ],
) -> None:
    """Reject physical recordings shared by training and a holdout split.

    A row's explicit physical-source URIs alias each other. Rows are also joined
    by equal URI and, when present, equal source-encoding SHA-256. Different
    hashes never disprove an exact URI match. Validation and eval may
    intentionally share recordings.

    Args:
        rows_by_split: Canonical manifest rows keyed by split.

    Returns:
        None after all split pairs pass validation.

    Raises:
        TypeError: If source-lineage metadata has the wrong type.
        ValueError: If source metadata is invalid or a training recording also
            appears in validation or eval.
    """
    nodes_by_split: dict[str, list[tuple[_PhysicalSourceKey, ...]]] = {}
    physical_nodes: set[_PhysicalSourceKey] = set()
    for split in ("train", "validation", "eval"):
        nodes: list[tuple[_PhysicalSourceKey, ...]] = []
        for row_index, row in enumerate(rows_by_split.get(split, ())):
            source_uris = _source_uris(
                row,
                split=split,
                row_index=row_index,
            )
            source_sha = _source_sha(
                row,
                split=split,
                row_index=row_index,
            )
            aliases = tuple(
                (source_uri, source_sha) for source_uri in source_uris
            )
            physical_nodes.update(aliases)
            nodes.append(aliases)
        nodes_by_split[split] = nodes

    groups = _UnionFind(physical_nodes)
    by_uri: dict[str, list[_PhysicalSourceKey]] = {}
    by_sha: dict[str, list[_PhysicalSourceKey]] = {}
    for node in physical_nodes:
        source_uri, source_sha = node
        by_uri.setdefault(source_uri, []).append(node)
        if source_sha is not None:
            by_sha.setdefault(source_sha, []).append(node)
    for matches in itertools.chain(
        itertools.chain.from_iterable(nodes_by_split.values()),
        by_uri.values(),
        by_sha.values(),
    ):
        groups.merge_all(matches)

    sources_by_split = {
        split: {
            groups.root(node) for node in itertools.chain.from_iterable(nodes)
        }
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
    """Format representative physical sources from overlapping groups.

    Args:
        physical_nodes: All explicit source URI and optional hash pairs.
        groups: Transitive physical-source alias groups.
        overlap: Group representatives shared by train and one holdout.

    Returns:
        Up to five sorted source locators for operator diagnostics.
    """
    matches = sorted(
        {
            source_uri
            for source_uri, source_sha in physical_nodes
            if groups.root((source_uri, source_sha)) in overlap
        }
    )
    return ", ".join(matches[:5])


def _source_uris(
    row: collections.abc.Mapping[str, typing.Any],
    *,
    split: str,
    row_index: int,
) -> tuple[str, ...]:
    """Return every explicit physical-source locator for a manifest row.

    Args:
        row: Canonical manifest row.
        split: Split name used in validation errors.
        row_index: Zero-based row index used in validation errors.

    Returns:
        One or two source locators. When both representations are present,
        they are aliases for the same physical recording.

    Raises:
        ValueError: If authoritative source metadata is blank or absent.
    """
    source_uris: list[str] = []
    original_audio_uri = row.get("original_audio_uri")
    if original_audio_uri is not None:
        if (
            not isinstance(original_audio_uri, str)
            or not original_audio_uri.strip()
        ):
            msg = (
                f"{split} row {row_index} original_audio_uri must be a "
                "nonblank string"
            )
            raise ValueError(msg)
        source_uris.append(original_audio_uri.strip())

    source_audio = row.get("source_audio")
    if isinstance(source_audio, collections.abc.Mapping):
        source_audio_uri = source_audio.get("audio_filepath")
        if source_audio_uri is not None:
            if (
                not isinstance(source_audio_uri, str)
                or not source_audio_uri.strip()
            ):
                msg = (
                    f"{split} row {row_index} "
                    "source_audio.audio_filepath must be a nonblank string"
                )
                raise ValueError(msg)
            source_uris.append(source_audio_uri.strip())
    if source_uris:
        return tuple(dict.fromkeys(source_uris))
    msg = (
        f"{split} row {row_index} lacks physical source provenance: "
        "original_audio_uri or source_audio.audio_filepath"
    )
    raise ValueError(msg)


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
        TypeError: If provided lineage metadata is not an object.
        ValueError: If a provided source SHA-256 is malformed.
    """
    source_lineage = row.get("source_lineage")
    if source_lineage is None:
        return None
    if not isinstance(source_lineage, collections.abc.Mapping):
        msg = f"{split} row {row_index} source_lineage must be an object"
        raise TypeError(msg)
    value = source_lineage.get("source_encoded_sha256")
    if value is None:
        return None
    if not isinstance(value, str) or _SHA256_PATTERN.fullmatch(value) is None:
        msg = (
            f"{split} row {row_index} source_lineage.source_encoded_sha256 "
            "must be a hexadecimal SHA-256"
        )
        raise ValueError(msg)
    return value.lower()


class _UnionFind:
    """Track transitive physical-source aliases."""

    def __init__(
        self,
        values: collections.abc.Iterable[_PhysicalSourceKey],
    ) -> None:
        self._parent = {value: value for value in values}

    def root(self, value: _PhysicalSourceKey) -> _PhysicalSourceKey:
        """Return a group's representative while compressing its path.

        Args:
            value: Physical-source key whose representative is required.

        Returns:
            The representative key for the value's alias group.
        """
        root = value
        while self._parent[root] != root:
            root = self._parent[root]

        current = value
        while current != root:
            parent = self._parent[current]
            self._parent[current] = root
            current = parent
        return root

    def merge_all(
        self,
        values: collections.abc.Sequence[_PhysicalSourceKey],
    ) -> None:
        """Merge all supplied physical-source keys into one alias group.

        Args:
            values: Physical-source keys proven to identify one recording.

        Returns:
            None after the keys are merged.
        """
        if not values:
            return
        left = self.root(values[0])
        for value in values[1:]:
            right = self.root(value)
            if left != right:
                self._parent[right] = left
