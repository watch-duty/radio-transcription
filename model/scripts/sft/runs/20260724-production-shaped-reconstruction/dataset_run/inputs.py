# One-time constructor: direct fail-loud diagnostics are intentional.
# ruff: noqa: D202, EM101, EM102, TRY003, TRY004
"""Strict input loading for the one-time production-shaped reconstruction.

The raw annotation manifests are the complete atom universe.  The latest SFT
manifests do not add atoms; they assign every raw speech atom to train or eval
and provide the authoritative text/timing overlay for reviewed rows.
"""

from __future__ import annotations

import dataclasses
import decimal
import json
import pathlib
from collections import Counter, defaultdict
from collections.abc import Mapping
from typing import Any, Final

from dataset_run import domain

SUPPORTED_FAMILIES: Final = (
    "bcfy_calls",
    "bcfy_feeds",
    "echo",
    "fire_notifications",
)


@dataclasses.dataclass(frozen=True, slots=True)
class RawManifestSpec:
    """Identity and path for one raw annotation manifest."""

    key: str
    family: str
    source_split: str
    path: pathlib.Path

    def __post_init__(self) -> None:
        if not self.key:
            raise ValueError("raw manifest key must be non-empty")
        if self.family not in SUPPORTED_FAMILIES:
            raise ValueError(f"unsupported raw family: {self.family!r}")
        if self.source_split not in {"train", "eval"}:
            raise ValueError("raw source_split must be train or eval")


@dataclasses.dataclass(frozen=True, order=True, slots=True)
class RawLocator:
    """Stable, exact locator for one raw annotation row."""

    raw_manifest: str
    raw_index: int

    def __post_init__(self) -> None:
        if not self.raw_manifest:
            raise ValueError("raw_manifest must be non-empty")
        if self.raw_index < 0:
            raise ValueError("raw_index must be non-negative")

    @property
    def atom_id(self) -> str:
        """Return a readable identifier that is stable within this run."""

        return f"{self.raw_manifest}:{self.raw_index}"


@dataclasses.dataclass(frozen=True, slots=True)
class RawAnnotation:
    """One validated raw annotation before media-frame conversion."""

    locator: RawLocator
    family: str
    source_split: str
    source_uri: str
    offset_seconds: decimal.Decimal
    duration_seconds: decimal.Decimal
    atom_class: domain.AtomClass
    category: str | None
    text: str | None

    def __post_init__(self) -> None:
        if self.family not in SUPPORTED_FAMILIES:
            raise ValueError(f"unsupported raw family: {self.family!r}")
        if self.source_split not in {"train", "eval"}:
            raise ValueError("raw source_split must be train or eval")
        if not self.source_uri.startswith("gs://"):
            raise ValueError("raw source_uri must be a gs:// URI")
        if self.offset_seconds < 0:
            raise ValueError("raw offset must be non-negative")
        if self.duration_seconds <= 0:
            raise ValueError("raw duration must be positive")
        if self.atom_class is domain.AtomClass.SPEECH:
            if domain.normalize_text(self.text) is None:
                raise ValueError("raw speech annotation requires text")
        elif self.text is not None:
            raise ValueError("targetless raw annotations cannot own text")

    @property
    def end_seconds(self) -> decimal.Decimal:
        """Return the annotation end on the source timeline."""

        return self.offset_seconds + self.duration_seconds

    def to_atom(
        self,
        sample_rate: int,
        *,
        offset_seconds: decimal.Decimal | None = None,
        duration_seconds: decimal.Decimal | None = None,
        text: str | None = None,
    ) -> domain.Atom:
        """Convert this annotation or an authoritative overlay to frames."""

        offset = (
            self.offset_seconds if offset_seconds is None else offset_seconds
        )
        duration = (
            self.duration_seconds
            if duration_seconds is None
            else duration_seconds
        )
        target = self.text if text is None else text
        start = domain.decimal_to_frame(offset, sample_rate)
        end = domain.decimal_to_frame(offset + duration, sample_rate)
        return domain.Atom(
            atom_id=self.locator.atom_id,
            family=self.family,
            source_uri=self.source_uri,
            raw_manifest=self.locator.raw_manifest,
            raw_index=self.locator.raw_index,
            start_frame=start,
            end_frame=end,
            atom_class=self.atom_class,
            text=target if self.atom_class is domain.AtomClass.SPEECH else None,
        )


@dataclasses.dataclass(frozen=True, slots=True)
class CanonicalManifestRow:
    """One latest-run row with its manifest role and stable row position."""

    manifest_role: str
    row_index: int
    row: Mapping[str, Any]

    def __post_init__(self) -> None:
        if self.manifest_role not in {"train", "eval", "closure"}:
            raise ValueError(
                f"unsupported canonical role: {self.manifest_role}"
            )
        if self.row_index < 0:
            raise ValueError("canonical row_index must be non-negative")
        if not isinstance(self.row, Mapping):
            raise TypeError("canonical row must be an object")


@dataclasses.dataclass(frozen=True, slots=True)
class FireAlias:
    """One frozen, confirmed pair of Fire source objects."""

    train_source_uri: str
    eval_source_uri: str
    filename_timestamp_delta_seconds: decimal.Decimal

    def __post_init__(self) -> None:
        if self.train_source_uri == self.eval_source_uri:
            raise ValueError("Fire alias endpoints must differ")
        if "/fire_notifications/training/" not in self.train_source_uri:
            raise ValueError("Fire alias train endpoint has an unexpected URI")
        if "/fire_notifications/eval/" not in self.eval_source_uri:
            raise ValueError("Fire alias eval endpoint has an unexpected URI")


@dataclasses.dataclass(frozen=True, slots=True)
class InputPaths:
    """Every local artifact required to freeze reconstruction ownership."""

    calls_train: pathlib.Path
    calls_eval: pathlib.Path
    feeds_train: pathlib.Path
    feeds_eval: pathlib.Path
    echo_train: pathlib.Path
    echo_eval: pathlib.Path
    fire_train: pathlib.Path
    fire_eval: pathlib.Path
    canonical_train: pathlib.Path
    canonical_eval: pathlib.Path
    canonical_closure: pathlib.Path
    confirmed_fire_aliases: pathlib.Path
    recording_group_policy: pathlib.Path

    @classmethod
    def from_download_root(
        cls,
        root: pathlib.Path | str,
        *,
        frozen_root: pathlib.Path | str,
    ) -> InputPaths:
        """Create paths for the established local download layout."""

        root = pathlib.Path(root)
        frozen_root = pathlib.Path(frozen_root)
        raw = root / "raw"
        latest = root / "latest-run"
        return cls(
            calls_train=raw / "calls_train.json",
            calls_eval=raw / "calls_eval.json",
            feeds_train=raw / "feeds_train.json",
            feeds_eval=raw / "feeds_eval.json",
            echo_train=raw / "echo_train.json",
            echo_eval=raw / "echo_eval.json",
            fire_train=raw / "fire_train.json",
            fire_eval=raw / "fire_eval.json",
            canonical_train=latest / "train.jsonl",
            canonical_eval=latest / "eval.jsonl",
            canonical_closure=latest / "closure.jsonl",
            confirmed_fire_aliases=(
                frozen_root / "confirmed_fire_alias_pairs.json"
            ),
            recording_group_policy=frozen_root / "recording_group_policy.json",
        )

    def raw_specs(self) -> tuple[RawManifestSpec, ...]:
        """Return all eight raw inputs in one deterministic order."""

        return (
            RawManifestSpec(
                "calls_train", "bcfy_calls", "train", self.calls_train
            ),
            RawManifestSpec(
                "calls_eval", "bcfy_calls", "eval", self.calls_eval
            ),
            RawManifestSpec(
                "feeds_train", "bcfy_feeds", "train", self.feeds_train
            ),
            RawManifestSpec(
                "feeds_eval", "bcfy_feeds", "eval", self.feeds_eval
            ),
            RawManifestSpec("echo_train", "echo", "train", self.echo_train),
            RawManifestSpec("echo_eval", "echo", "eval", self.echo_eval),
            RawManifestSpec(
                "fire_train",
                "fire_notifications",
                "train",
                self.fire_train,
            ),
            RawManifestSpec(
                "fire_eval", "fire_notifications", "eval", self.fire_eval
            ),
        )


@dataclasses.dataclass(frozen=True, slots=True)
class InputBundle:
    """Fully parsed inputs plus indexes used by ownership mapping."""

    raw_annotations: tuple[RawAnnotation, ...]
    canonical_train: tuple[CanonicalManifestRow, ...]
    canonical_eval: tuple[CanonicalManifestRow, ...]
    canonical_closure: tuple[CanonicalManifestRow, ...]
    fire_aliases: tuple[FireAlias, ...]
    recording_group_policy: Mapping[str, Any]
    raw_by_locator: Mapping[RawLocator, RawAnnotation]
    locators_by_family_uri_index: Mapping[
        tuple[str, str, int], tuple[RawLocator, ...]
    ]
    receipt: Mapping[str, Any]

    @property
    def speech_annotations(self) -> tuple[RawAnnotation, ...]:
        """Return the complete protected-speech universe."""

        return tuple(
            annotation
            for annotation in self.raw_annotations
            if annotation.atom_class is domain.AtomClass.SPEECH
        )

    @property
    def source_uris(self) -> tuple[str, ...]:
        """Return sources containing at least one protected speech atom."""

        return tuple(
            sorted(
                {
                    annotation.source_uri
                    for annotation in self.raw_annotations
                    if annotation.atom_class is domain.AtomClass.SPEECH
                }
            )
        )


def _decimal_value(
    value: object,
    *,
    name: str,
    positive: bool = False,
    allow_negative: bool = False,
) -> decimal.Decimal:
    if isinstance(value, bool):
        raise ValueError(f"{name} must be numeric, not boolean")
    try:
        result = decimal.Decimal(str(value))
    except (decimal.InvalidOperation, ValueError) as error:
        raise ValueError(
            f"{name} is not a decimal number: {value!r}"
        ) from error
    if not result.is_finite():
        raise ValueError(f"{name} must be finite")
    if (not allow_negative and result < 0) or (positive and result == 0):
        qualifier = "positive" if positive else "non-negative"
        raise ValueError(f"{name} must be {qualifier}")
    return result


def _load_json(path: pathlib.Path) -> Any:
    try:
        with path.open(encoding="utf-8") as handle:
            return json.load(handle)
    except FileNotFoundError:
        raise FileNotFoundError(
            f"required input does not exist: {path}"
        ) from None
    except json.JSONDecodeError as error:
        raise ValueError(f"invalid JSON in {path}: {error}") from error


def _load_jsonl(
    path: pathlib.Path,
    *,
    manifest_role: str,
) -> tuple[CanonicalManifestRow, ...]:
    result: list[CanonicalManifestRow] = []
    try:
        with path.open(encoding="utf-8") as handle:
            for row_index, line in enumerate(handle):
                if not line.strip():
                    raise ValueError(
                        f"{path}:{row_index + 1}: blank JSONL line"
                    )
                try:
                    row = json.loads(line)
                except json.JSONDecodeError as error:
                    raise ValueError(
                        f"{path}:{row_index + 1}: invalid JSON: {error}"
                    ) from error
                if not isinstance(row, dict):
                    raise TypeError(
                        f"{path}:{row_index + 1}: row must be an object"
                    )
                result.append(
                    CanonicalManifestRow(manifest_role, row_index, row)
                )
    except FileNotFoundError:
        raise FileNotFoundError(
            f"required input does not exist: {path}"
        ) from None
    return tuple(result)


def _load_raw_manifest(spec: RawManifestSpec) -> tuple[RawAnnotation, ...]:
    payload = _load_json(spec.path)
    if not isinstance(payload, list):
        raise TypeError(f"{spec.path}: raw manifest must be a JSON array")
    result: list[RawAnnotation] = []
    for raw_index, row in enumerate(payload):
        if not isinstance(row, dict):
            raise TypeError(f"{spec.path}[{raw_index}] must be an object")
        source_uri = row.get("audio_filepath")
        if not isinstance(source_uri, str) or not source_uri.startswith(
            "gs://"
        ):
            raise ValueError(
                f"{spec.path}[{raw_index}].audio_filepath must be a gs:// URI"
            )
        category_value = row.get("category")
        if category_value is not None and not isinstance(category_value, str):
            raise TypeError(
                f"{spec.path}[{raw_index}].category must be a string or null"
            )
        atom_class = domain.classify_annotation(category_value, row.get("text"))
        raw_text = row.get("text")
        text: str | None
        if atom_class is domain.AtomClass.SPEECH:
            if not isinstance(raw_text, str):
                raise TypeError(
                    f"{spec.path}[{raw_index}].text must be a string"
                )
            text = raw_text
        else:
            text = None
        result.append(
            RawAnnotation(
                locator=RawLocator(spec.key, raw_index),
                family=spec.family,
                source_split=spec.source_split,
                source_uri=source_uri,
                offset_seconds=_decimal_value(
                    row.get("offset"),
                    name=f"{spec.path}[{raw_index}].offset",
                ),
                duration_seconds=_decimal_value(
                    row.get("duration"),
                    name=f"{spec.path}[{raw_index}].duration",
                    positive=True,
                ),
                atom_class=atom_class,
                category=category_value,
                text=text,
            )
        )
    return tuple(result)


def _load_fire_aliases(path: pathlib.Path) -> tuple[FireAlias, ...]:
    payload = _load_json(path)
    if not isinstance(payload, dict) or payload.get("schema_version") != 1:
        raise ValueError(f"{path}: unsupported Fire alias schema")
    pairs = payload.get("pairs")
    if not isinstance(pairs, list):
        raise TypeError(f"{path}: pairs must be a list")
    result: list[FireAlias] = []
    seen: set[tuple[str, str]] = set()
    expected_keys = {
        "train_source_uri",
        "eval_source_uri",
        "filename_timestamp_delta_seconds",
    }
    for index, pair in enumerate(pairs):
        if not isinstance(pair, dict) or set(pair) != expected_keys:
            raise ValueError(
                f"{path}: pairs[{index}] must contain exactly "
                f"{sorted(expected_keys)!r}"
            )
        alias = FireAlias(
            train_source_uri=pair["train_source_uri"],
            eval_source_uri=pair["eval_source_uri"],
            filename_timestamp_delta_seconds=_decimal_value(
                pair["filename_timestamp_delta_seconds"],
                name=f"{path}: pairs[{index}].filename_timestamp_delta_seconds",
                allow_negative=True,
            ),
        )
        key = tuple(sorted((alias.train_source_uri, alias.eval_source_uri)))
        if key in seen:
            raise ValueError(f"{path}: duplicate Fire alias pair {key!r}")
        seen.add(key)
        result.append(alias)
    return tuple(sorted(result, key=lambda item: item.train_source_uri))


def _load_recording_group_policy(path: pathlib.Path) -> Mapping[str, Any]:
    payload = _load_json(path)
    if not isinstance(payload, dict) or payload.get("schema_version") != 1:
        raise ValueError(f"{path}: unsupported recording-group policy schema")
    assembly = payload.get("assembly")
    if not isinstance(assembly, dict) or not isinstance(
        assembly.get("grouping_summary"), dict
    ):
        raise ValueError(f"{path}: assembly grouping summary is missing")
    return payload


def load_inputs(paths: InputPaths) -> InputBundle:
    """Load and fail-loudly validate the complete frozen input universe."""

    if not isinstance(paths, InputPaths):
        raise TypeError("paths must be InputPaths")
    raw_annotations = tuple(
        annotation
        for spec in paths.raw_specs()
        for annotation in _load_raw_manifest(spec)
    )
    raw_by_locator: dict[RawLocator, RawAnnotation] = {}
    locators_by_family_uri_index: dict[
        tuple[str, str, int], list[RawLocator]
    ] = defaultdict(list)
    for annotation in raw_annotations:
        if annotation.locator in raw_by_locator:
            raise ValueError(f"duplicate raw locator: {annotation.locator}")
        raw_by_locator[annotation.locator] = annotation
        locators_by_family_uri_index[
            (
                annotation.family,
                annotation.source_uri,
                annotation.locator.raw_index,
            )
        ].append(annotation.locator)

    canonical_train = _load_jsonl(paths.canonical_train, manifest_role="train")
    canonical_eval = _load_jsonl(paths.canonical_eval, manifest_role="eval")
    canonical_closure = _load_jsonl(
        paths.canonical_closure, manifest_role="closure"
    )
    fire_aliases = _load_fire_aliases(paths.confirmed_fire_aliases)
    recording_group_policy = _load_recording_group_policy(
        paths.recording_group_policy
    )

    class_counts: Counter[str] = Counter(
        annotation.atom_class.value for annotation in raw_annotations
    )
    speech_by_family: Counter[str] = Counter(
        annotation.family
        for annotation in raw_annotations
        if annotation.atom_class is domain.AtomClass.SPEECH
    )
    rows_by_raw_manifest: Counter[str] = Counter(
        annotation.locator.raw_manifest for annotation in raw_annotations
    )
    speech_source_count = len(
        {
            (annotation.family, annotation.source_uri)
            for annotation in raw_annotations
            if annotation.atom_class is domain.AtomClass.SPEECH
        }
    )
    receipt: dict[str, Any] = {
        "canonical_rows": {
            "closure": len(canonical_closure),
            "eval": len(canonical_eval),
            "train": len(canonical_train),
        },
        "fire_alias_pair_count": len(fire_aliases),
        "raw_annotation_class_counts": dict(sorted(class_counts.items())),
        "raw_rows_by_manifest": dict(sorted(rows_by_raw_manifest.items())),
        "raw_speech_by_family": dict(sorted(speech_by_family.items())),
        "raw_speech_count": sum(speech_by_family.values()),
        "speech_source_count": speech_source_count,
    }
    return InputBundle(
        raw_annotations=raw_annotations,
        canonical_train=canonical_train,
        canonical_eval=canonical_eval,
        canonical_closure=canonical_closure,
        fire_aliases=fire_aliases,
        recording_group_policy=recording_group_policy,
        raw_by_locator=raw_by_locator,
        locators_by_family_uri_index={
            key: tuple(value)
            for key, value in sorted(locators_by_family_uri_index.items())
        },
        receipt=receipt,
    )


def canonical_family(row: Mapping[str, Any]) -> str:
    """Normalize the latest-run family without inferring from file paths."""

    dataset = row.get("dataset")
    if isinstance(dataset, Mapping):
        raw_family = dataset.get("family")
        raw_name = dataset.get("name")
        raw = (
            raw_name if raw_family == "broadcastify" else raw_family or raw_name
        )
    else:
        raw = row.get("dataset_family") or row.get("dataset_name")
    if not isinstance(raw, str) or not raw.strip():
        raise ValueError("canonical row lacks dataset family metadata")
    normalized = raw.strip().casefold().replace("-", "_").replace(" ", "_")
    aliases = {
        "bcfy_calls": "bcfy_calls",
        "broadcastify_calls": "bcfy_calls",
        "bcfy_feeds": "bcfy_feeds",
        "broadcastify_feeds": "bcfy_feeds",
        "echo": "echo",
        "fire_notification": "fire_notifications",
        "fire_notifications": "fire_notifications",
    }
    try:
        return aliases[normalized]
    except KeyError:
        raise ValueError(f"unsupported canonical family: {raw!r}") from None


def canonical_source_geometry(
    row: Mapping[str, Any],
) -> tuple[str, decimal.Decimal, decimal.Decimal]:
    """Read the authoritative source URI, offset, and duration from a row."""

    source_audio = row.get("source_audio")
    if not isinstance(source_audio, Mapping):
        raise ValueError("canonical row lacks source_audio")
    source_uri = source_audio.get("audio_filepath")
    if not isinstance(source_uri, str) or not source_uri.startswith("gs://"):
        raise ValueError(
            "canonical source_audio.audio_filepath must be a gs:// URI"
        )
    return (
        source_uri,
        _decimal_value(
            source_audio.get("offset"), name="canonical source_audio.offset"
        ),
        _decimal_value(
            source_audio.get("duration"),
            name="canonical source_audio.duration",
            positive=True,
        ),
    )


def canonical_text(row: Mapping[str, Any]) -> str:
    """Return target text while preserving its authoritative spelling."""

    value = row.get("text")
    if not isinstance(value, str) or domain.normalize_text(value) is None:
        raise ValueError("canonical protected row must have non-empty text")
    return value
