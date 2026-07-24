"""Strict configuration for the one-time production-shaped reconstruction.

``run.toml`` is a frozen input lock, not a user-facing compatibility surface.
The parser therefore rejects missing, extra, or legacy fields instead of
guessing what they mean.
"""

from __future__ import annotations

import dataclasses
import pathlib
import re
import tomllib
from collections.abc import Mapping

_TOP_LEVEL_KEYS = frozenset(
    {
        "run_id",
        "workspace",
        "source_cache",
        "proposed_gcs_prefix",
        "local_inputs",
        "canonical_inputs",
        "raw_inputs",
        "masked_inputs",
    }
)
_LOCK_KEYS = frozenset({"label", "uri", "generation", "size", "sha256"})
_LOCAL_LOCK_KEYS = frozenset({"label", "path", "size", "sha256"})
_RAW_LOCK_KEYS = _LOCK_KEYS | frozenset({"family", "source_split"})
_MASKED_LOCK_KEYS = frozenset({"family", "uri", "generation", "size", "sha256"})
_FAMILIES = (
    "bcfy_calls",
    "bcfy_feeds",
    "echo",
    "fire_notifications",
)
_CANONICAL_LABELS = frozenset(
    {
        "latest_train",
        "latest_eval",
        "latest_validation",
        "correction_closure",
        "composite_selection",
        "prior_materialization",
    }
)
_RUN_ID_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*\Z")
_HEX_DIGITS = frozenset("0123456789abcdef")


class ConfigError(ValueError):
    """Raised when the frozen run configuration is incomplete or ambiguous."""


@dataclasses.dataclass(frozen=True, slots=True)
class LockedInput:
    """Immutable byte identity of one input object."""

    label: str
    uri: str
    generation: int | None
    size: int
    sha256: str

    def __post_init__(self) -> None:
        if (
            not isinstance(self.label, str)
            or not self.label.strip()
            or self.label != self.label.strip()
        ):
            raise ConfigError("locked input label must be a non-blank string")
        if _RUN_ID_PATTERN.fullmatch(self.label) is None:
            raise ConfigError(
                "locked input label may contain only letters, digits, '.', "
                "'_', and '-'"
            )
        if (
            not isinstance(self.uri, str)
            or not self.uri.strip()
            or self.uri != self.uri.strip()
        ):
            raise ConfigError("locked input URI must be a non-blank string")
        if self.generation is not None and (
            isinstance(self.generation, bool)
            or not isinstance(self.generation, int)
            or self.generation <= 0
        ):
            raise ConfigError(
                "locked input generation must be a positive integer or None"
            )
        if (
            isinstance(self.size, bool)
            or not isinstance(self.size, int)
            or self.size <= 0
        ):
            raise ConfigError("locked input size must be a positive integer")
        if (
            not isinstance(self.sha256, str)
            or len(self.sha256) != 64
            or any(character not in _HEX_DIGITS for character in self.sha256)
        ):
            raise ConfigError(
                "locked input SHA-256 must be 64 lowercase hexadecimal "
                "characters"
            )
        if self.uri.startswith("gs://") and self.generation is None:
            raise ConfigError("GCS locked input must include its generation")


@dataclasses.dataclass(frozen=True, slots=True)
class CanonicalInput(LockedInput):
    """One canonical artifact from the latest SFT dataset."""


@dataclasses.dataclass(frozen=True, slots=True)
class LocalInput(LockedInput):
    """One repository-local, byte-locked construction input."""


@dataclasses.dataclass(frozen=True, slots=True)
class RawInput(LockedInput):
    """One raw annotation manifest and its source split."""

    family: str
    source_split: str


@dataclasses.dataclass(frozen=True, slots=True)
class MaskedInput(LockedInput):
    """One masked-v2 eval manifest."""

    family: str


@dataclasses.dataclass(frozen=True, slots=True)
class RunConfig:
    """Fully validated, current-schema reconstruction configuration."""

    run_id: str
    workspace: pathlib.Path
    source_cache: pathlib.Path
    proposed_gcs_prefix: str
    local_inputs: tuple[LocalInput, ...]
    canonical_inputs: tuple[CanonicalInput, ...]
    raw_inputs: tuple[RawInput, ...]
    masked_inputs: tuple[MaskedInput, ...]

    @property
    def all_inputs(self) -> tuple[LockedInput, ...]:
        """Return every locked input in deterministic configuration order."""

        return (
            *self.local_inputs,
            *self.canonical_inputs,
            *self.raw_inputs,
            *self.masked_inputs,
        )

    def input_by_label(self) -> dict[str, LockedInput]:
        """Index all locks and fail rather than shadow a duplicate label."""

        result: dict[str, LockedInput] = {}
        for item in self.all_inputs:
            if item.label in result:
                raise ConfigError(f"duplicate input label: {item.label}")
            result[item.label] = item
        return result


def _require_exact_keys(
    value: Mapping[str, object],
    expected: frozenset[str],
    *,
    where: str,
) -> None:
    observed = frozenset(value)
    missing = sorted(expected - observed)
    extra = sorted(observed - expected)
    if missing or extra:
        details: list[str] = []
        if missing:
            details.append(f"missing {missing}")
        if extra:
            details.append(f"unexpected {extra}")
        raise ConfigError(f"{where}: {'; '.join(details)}")


def _mapping(value: object, *, where: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise ConfigError(f"{where} must be a TOML table")
    for key in value:
        if not isinstance(key, str):
            raise ConfigError(f"{where} contains a non-string key")
    return value


def _tables(value: object, *, where: str) -> tuple[Mapping[str, object], ...]:
    if not isinstance(value, list) or not value:
        raise ConfigError(f"{where} must be a non-empty array of tables")
    return tuple(
        _mapping(item, where=f"{where}[{index}]")
        for index, item in enumerate(value)
    )


def _string(value: object, *, where: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ConfigError(f"{where} must be a non-blank string")
    if value != value.strip():
        raise ConfigError(f"{where} must not contain surrounding whitespace")
    return value


def _positive_integer(value: object, *, where: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ConfigError(f"{where} must be a positive integer")
    return value


def _absolute_path(value: object, *, where: str) -> pathlib.Path:
    rendered = _string(value, where=where)
    path = pathlib.Path(rendered)
    if not path.is_absolute():
        raise ConfigError(f"{where} must be an absolute path")
    return path


def _object_uri(value: object, *, where: str) -> str:
    uri = _string(value, where=where)
    if not uri.startswith("gs://"):
        raise ConfigError(f"{where} must be a gs:// object URI")
    bucket_and_object = uri.removeprefix("gs://").split("/", 1)
    if (
        len(bucket_and_object) != 2
        or not bucket_and_object[0]
        or not bucket_and_object[1]
        or uri.endswith("/")
    ):
        raise ConfigError(f"{where} must name exactly one GCS object")
    return uri


def _prefix_uri(value: object, *, where: str) -> str:
    uri = _string(value, where=where)
    if not uri.startswith("gs://") or not uri.endswith("/"):
        raise ConfigError(f"{where} must be a gs:// prefix ending in '/'")
    bucket_and_prefix = uri.removeprefix("gs://").split("/", 1)
    if (
        len(bucket_and_prefix) != 2
        or not bucket_and_prefix[0]
        or not bucket_and_prefix[1]
    ):
        raise ConfigError(f"{where} must include a bucket and non-empty prefix")
    return uri


def _run_relative_path(
    value: object,
    *,
    where: str,
    run_root: pathlib.Path,
) -> pathlib.Path:
    rendered = _string(value, where=where)
    if "\\" in rendered:
        raise ConfigError(f"{where} must use POSIX path separators")
    relative = pathlib.PurePosixPath(rendered)
    if relative.is_absolute() or any(
        part in {"", ".", ".."} for part in relative.parts
    ):
        raise ConfigError(
            f"{where} must be a normalized path below the run directory"
        )
    resolved_root = run_root.resolve()
    resolved = resolved_root.joinpath(*relative.parts).resolve()
    if not resolved.is_relative_to(resolved_root):
        raise ConfigError(f"{where} escapes the run directory")
    return resolved


def _sha256(value: object, *, where: str) -> str:
    digest = _string(value, where=where)
    if len(digest) != 64 or any(
        character not in _HEX_DIGITS for character in digest
    ):
        raise ConfigError(
            f"{where} must be 64 lowercase hexadecimal characters"
        )
    return digest


def _base_lock(
    table: Mapping[str, object],
    *,
    where: str,
    label: str | None = None,
) -> dict[str, object]:
    actual_label = (
        _string(table["label"], where=f"{where}.label")
        if label is None
        else label
    )
    return {
        "label": actual_label,
        "uri": _object_uri(table["uri"], where=f"{where}.uri"),
        "generation": _positive_integer(
            table["generation"],
            where=f"{where}.generation",
        ),
        "size": _positive_integer(table["size"], where=f"{where}.size"),
        "sha256": _sha256(table["sha256"], where=f"{where}.sha256"),
    }


def _parse_local(
    value: object,
    *,
    run_root: pathlib.Path,
) -> tuple[LocalInput, ...]:
    result: list[LocalInput] = []
    for index, table in enumerate(_tables(value, where="local_inputs")):
        where = f"local_inputs[{index}]"
        _require_exact_keys(table, _LOCAL_LOCK_KEYS, where=where)
        result.append(
            LocalInput(
                label=_string(table["label"], where=f"{where}.label"),
                uri=str(
                    _run_relative_path(
                        table["path"],
                        where=f"{where}.path",
                        run_root=run_root,
                    )
                ),
                generation=None,
                size=_positive_integer(
                    table["size"],
                    where=f"{where}.size",
                ),
                sha256=_sha256(
                    table["sha256"],
                    where=f"{where}.sha256",
                ),
            )
        )
    if [item.label for item in result] != ["production_duration_target"]:
        raise ConfigError(
            "local_inputs must contain exactly production_duration_target"
        )
    return tuple(result)


def _parse_canonical(value: object) -> tuple[CanonicalInput, ...]:
    result: list[CanonicalInput] = []
    for index, table in enumerate(_tables(value, where="canonical_inputs")):
        where = f"canonical_inputs[{index}]"
        _require_exact_keys(table, _LOCK_KEYS, where=where)
        result.append(CanonicalInput(**_base_lock(table, where=where)))
    labels = [item.label for item in result]
    if len(labels) != len(set(labels)):
        raise ConfigError("canonical_inputs contain duplicate labels")
    if frozenset(labels) != _CANONICAL_LABELS:
        raise ConfigError(
            f"canonical_inputs must contain exactly {sorted(_CANONICAL_LABELS)}"
        )
    return tuple(result)


def _parse_raw(value: object) -> tuple[RawInput, ...]:
    result: list[RawInput] = []
    for index, table in enumerate(_tables(value, where="raw_inputs")):
        where = f"raw_inputs[{index}]"
        _require_exact_keys(table, _RAW_LOCK_KEYS, where=where)
        family = _string(table["family"], where=f"{where}.family")
        if family not in _FAMILIES:
            raise ConfigError(f"{where}.family is unsupported: {family}")
        source_split = _string(
            table["source_split"],
            where=f"{where}.source_split",
        )
        if source_split not in {"train", "eval"}:
            raise ConfigError(f"{where}.source_split must be 'train' or 'eval'")
        result.append(
            RawInput(
                **_base_lock(table, where=where),
                family=family,
                source_split=source_split,
            )
        )
    pairs = [(item.family, item.source_split) for item in result]
    expected = {
        (family, split) for family in _FAMILIES for split in ("train", "eval")
    }
    if len(pairs) != len(set(pairs)) or set(pairs) != expected:
        raise ConfigError(
            "raw_inputs must contain exactly one train and eval manifest "
            "for every supported family"
        )
    return tuple(result)


def _parse_masked(value: object) -> tuple[MaskedInput, ...]:
    result: list[MaskedInput] = []
    for index, table in enumerate(_tables(value, where="masked_inputs")):
        where = f"masked_inputs[{index}]"
        _require_exact_keys(table, _MASKED_LOCK_KEYS, where=where)
        family = _string(table["family"], where=f"{where}.family")
        if family not in _FAMILIES:
            raise ConfigError(f"{where}.family is unsupported: {family}")
        result.append(
            MaskedInput(
                **_base_lock(
                    table,
                    where=where,
                    label=f"masked_{family}_eval",
                ),
                family=family,
            )
        )
    families = [item.family for item in result]
    if len(families) != len(set(families)) or set(families) != set(_FAMILIES):
        raise ConfigError(
            "masked_inputs must contain exactly one eval manifest for every "
            "supported family"
        )
    return tuple(result)


def load_run_config(path: str | pathlib.Path) -> RunConfig:
    """Parse exactly the current ``run.toml`` schema."""

    config_path = pathlib.Path(path)
    try:
        with config_path.open("rb") as source:
            raw = tomllib.load(source)
    except (OSError, tomllib.TOMLDecodeError) as error:
        raise ConfigError(f"cannot parse run config: {config_path}") from error
    table = _mapping(raw, where="run.toml")
    _require_exact_keys(table, _TOP_LEVEL_KEYS, where="run.toml")

    run_id = _string(table["run_id"], where="run_id")
    if _RUN_ID_PATTERN.fullmatch(run_id) is None:
        raise ConfigError(
            "run_id may contain only letters, digits, '.', '_', and '-'"
        )

    result = RunConfig(
        run_id=run_id,
        workspace=_absolute_path(table["workspace"], where="workspace"),
        source_cache=_absolute_path(
            table["source_cache"],
            where="source_cache",
        ),
        proposed_gcs_prefix=_prefix_uri(
            table["proposed_gcs_prefix"],
            where="proposed_gcs_prefix",
        ),
        local_inputs=_parse_local(
            table["local_inputs"],
            run_root=config_path.parent,
        ),
        canonical_inputs=_parse_canonical(table["canonical_inputs"]),
        raw_inputs=_parse_raw(table["raw_inputs"]),
        masked_inputs=_parse_masked(table["masked_inputs"]),
    )
    labels = [item.label for item in result.all_inputs]
    if len(labels) != len(set(labels)):
        raise ConfigError("input labels must be globally unique")
    uris = [item.uri for item in result.all_inputs]
    if len(uris) != len(set(uris)):
        raise ConfigError("input URIs must be globally unique")
    return result
