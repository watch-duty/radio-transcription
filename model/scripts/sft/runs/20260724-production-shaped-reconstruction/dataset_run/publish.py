"""Create-only publication for one completed reconstructed dataset build.

This module is deliberately separate from staging.  It performs a complete
local, offline audit before the first provider call, uploads immutable objects,
and publishes ``_SUCCESS.json`` only after every other object is durable.
"""

# One-time fail-loud publisher: direct diagnostics and a small CLI are intended.
# ruff: noqa: D202, EM101, EM102, PLC0415, PLR0912, PLR0915, T201, TC003
# ruff: noqa: TRY003, TRY300, TRY301

from __future__ import annotations

import argparse
import dataclasses
import hashlib
import json
import os
import pathlib
import re
from collections.abc import Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed

from . import artifacts

_SHA256 = re.compile(r"[0-9a-f]{64}")
_SUCCESS_PATH = "_SUCCESS.json"
_MAPPING_PATH = "receipts/materialization_mapping.jsonl"
_MATERIALIZATION_PATH = "receipts/materialization.jsonl"


class PublicationError(RuntimeError):
    """Raised when an immutable local or remote publication contract fails."""


@dataclasses.dataclass(frozen=True, slots=True)
class ExpectedObject:
    """One byte-exact object in the approved publication universe."""

    relative_path: str
    local_path: pathlib.Path
    uri: str
    object_name: str
    size: int
    sha256: str
    content_type: str


@dataclasses.dataclass(frozen=True, slots=True)
class PublicationPlan:
    """Fully validated local publication plan; constructing it is offline."""

    build_root: pathlib.Path
    gcs_prefix: str
    bucket_name: str
    object_prefix: str
    success_sha256: str
    objects: tuple[ExpectedObject, ...]

    @property
    def completion_marker(self) -> ExpectedObject:
        return next(
            item for item in self.objects if item.relative_path == _SUCCESS_PATH
        )


@dataclasses.dataclass(frozen=True, slots=True)
class PublicationResult:
    """Stable identity of one complete remote publication."""

    gcs_prefix: str
    object_count: int
    aggregate_sha256: str
    receipt_path: pathlib.Path
    receipt_sha256: str


def _strict_object(payload: bytes, *, label: str) -> dict[str, object]:
    def reject_duplicate_keys(
        pairs: list[tuple[str, object]],
    ) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in pairs:
            if key in result:
                raise PublicationError(f"{label}: duplicate JSON key {key!r}")
            result[key] = value
        return result

    try:
        value = json.loads(payload, object_pairs_hook=reject_duplicate_keys)
    except PublicationError:
        raise
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise PublicationError(f"{label}: invalid JSON") from error
    if not isinstance(value, dict):
        raise PublicationError(f"{label}: expected a JSON object")
    return value


def _read_bytes(path: pathlib.Path, *, label: str) -> bytes:
    try:
        if path.is_symlink() or not path.is_file():
            raise PublicationError(f"{label}: not a regular local file")
        return path.read_bytes()
    except PublicationError:
        raise
    except OSError as error:
        raise PublicationError(f"{label}: cannot read local file") from error


def _positive_integer(value: object, *, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise PublicationError(f"{label}: expected a positive integer")
    return value


def _sha256(value: object, *, label: str) -> str:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise PublicationError(f"{label}: expected a lowercase SHA-256")
    return value


def _relative_path(value: object, *, label: str) -> str:
    if (
        not isinstance(value, str)
        or not value
        or value.startswith("/")
        or "\\" in value
    ):
        raise PublicationError(f"{label}: invalid relative object path")
    parts = value.split("/")
    if any(part in {"", ".", ".."} for part in parts):
        raise PublicationError(f"{label}: invalid relative object path")
    return value


def _parse_prefix(gcs_prefix: str) -> tuple[str, str]:
    if (
        not isinstance(gcs_prefix, str)
        or not gcs_prefix.startswith("gs://")
        or not gcs_prefix.endswith("/")
    ):
        raise PublicationError(
            "GCS prefix must be a gs:// URI ending with one slash"
        )
    remainder = gcs_prefix.removeprefix("gs://")
    try:
        bucket_name, object_prefix = remainder.split("/", 1)
    except ValueError as error:
        raise PublicationError(
            "GCS prefix must include a bucket and non-empty object prefix"
        ) from error
    if (
        not bucket_name
        or not object_prefix
        or object_prefix.startswith("/")
        or "//" in object_prefix
        or any(part in {".", ".."} for part in object_prefix.split("/"))
    ):
        raise PublicationError(
            "GCS prefix must include a bucket and normalized object prefix"
        )
    return bucket_name, object_prefix


def _content_type(relative_path: str) -> str:
    suffix = pathlib.PurePosixPath(relative_path).suffix.lower()
    if suffix == ".flac":
        return "audio/flac"
    if suffix == ".jsonl":
        return "application/x-ndjson"
    if suffix == ".json":
        return "application/json"
    return "application/octet-stream"


def _identity(
    local_path: pathlib.Path,
    *,
    expected_size: int,
    expected_sha256: str,
    label: str,
) -> None:
    if not local_path.exists():
        raise PublicationError(f"{label}: missing local file")
    try:
        observed = artifacts.inspect_file(local_path)
    except artifacts.ArtifactError as error:
        raise PublicationError(f"{label}: {error}") from error
    if observed.size != expected_size:
        raise PublicationError(
            f"{label}: size drift; expected {expected_size}, "
            f"observed {observed.size}"
        )
    if observed.sha256 != expected_sha256:
        raise PublicationError(
            f"{label}: SHA-256 drift; expected {expected_sha256}, "
            f"observed {observed.sha256}"
        )


def _success_artifacts(
    build_root: pathlib.Path,
    success: Mapping[str, object],
) -> dict[str, ExpectedObject]:
    raw_artifacts = success.get("artifacts")
    if not isinstance(raw_artifacts, list) or not raw_artifacts:
        raise PublicationError("_SUCCESS.json: artifacts must be non-empty")
    artifact_count = _positive_integer(
        success.get("artifact_count"),
        label="_SUCCESS.json artifact_count",
    )
    if len(raw_artifacts) != artifact_count:
        raise PublicationError(
            "_SUCCESS.json: artifact_count differs from artifacts"
        )

    result: dict[str, ExpectedObject] = {}
    for index, raw in enumerate(raw_artifacts):
        label = f"_SUCCESS.json artifacts[{index}]"
        if not isinstance(raw, dict) or set(raw) != {
            "path",
            "sha256",
            "size",
        }:
            raise PublicationError(f"{label}: invalid artifact evidence")
        relative = _relative_path(raw["path"], label=f"{label} path")
        if relative == _SUCCESS_PATH or relative.startswith("audio/"):
            raise PublicationError(f"{label}: invalid artifact path")
        if relative in result:
            raise PublicationError(f"{label}: duplicate artifact path")
        size = _positive_integer(raw["size"], label=f"{label} size")
        digest = _sha256(raw["sha256"], label=f"{label} sha256")
        local_path = build_root.joinpath(*relative.split("/"))
        _identity(
            local_path,
            expected_size=size,
            expected_sha256=digest,
            label=relative,
        )
        result[relative] = ExpectedObject(
            relative_path=relative,
            local_path=local_path,
            uri="",
            object_name="",
            size=size,
            sha256=digest,
            content_type=_content_type(relative),
        )
    return result


def _mapping_audio(
    build_root: pathlib.Path,
    *,
    gcs_prefix: str,
    mapping_path: pathlib.Path,
    expected_count: int,
) -> dict[str, ExpectedObject]:
    rows: list[dict[str, object]] = []
    try:
        with mapping_path.open("rb") as source:
            for line_number, line in enumerate(source, start=1):
                if not line.strip():
                    raise PublicationError(
                        f"{_MAPPING_PATH}:{line_number}: blank JSONL row"
                    )
                rows.append(
                    _strict_object(
                        line,
                        label=f"{_MAPPING_PATH}:{line_number}",
                    )
                )
    except PublicationError:
        raise
    except OSError as error:
        raise PublicationError(
            f"{_MAPPING_PATH}: cannot read mapping"
        ) from error
    if len(rows) != expected_count:
        raise PublicationError(
            f"{_MAPPING_PATH}: expected {expected_count} rows, "
            f"observed {len(rows)}"
        )

    result: dict[str, ExpectedObject] = {}
    request_ids: set[str] = set()
    local_paths: set[pathlib.Path] = set()
    resolved_root = build_root.resolve()
    for line_number, row in enumerate(rows, start=1):
        label = f"{_MAPPING_PATH}:{line_number}"
        request_id = row.get("request_id")
        if (
            not isinstance(request_id, str)
            or not request_id.strip()
            or request_id != request_id.strip()
            or request_id in request_ids
        ):
            raise PublicationError(f"{label}: invalid or duplicate request_id")
        request_ids.add(request_id)

        uri = row.get("canonical_audio_uri")
        if not isinstance(uri, str) or not uri.startswith(gcs_prefix):
            raise PublicationError(
                f"{label}: canonical_audio_uri is outside approved prefix"
            )
        relative = _relative_path(
            uri.removeprefix(gcs_prefix),
            label=f"{label} canonical_audio_uri",
        )
        if (
            not relative.startswith("audio/")
            or not relative.endswith(".flac")
            or relative in result
        ):
            raise PublicationError(
                f"{label}: invalid or duplicate canonical audio path"
            )

        raw_local = row.get("local_output_path")
        if (
            not isinstance(raw_local, str)
            or not pathlib.Path(raw_local).is_absolute()
        ):
            raise PublicationError(
                f"{label}: local_output_path is not absolute"
            )
        local_path = pathlib.Path(raw_local)
        expected_local = build_root.joinpath(*relative.split("/"))
        try:
            matches_expected = (
                local_path.resolve() == expected_local.resolve()
                and expected_local.resolve().is_relative_to(resolved_root)
            )
        except OSError as error:
            raise PublicationError(
                f"{label}: cannot resolve local_output_path"
            ) from error
        if not matches_expected or local_path in local_paths:
            raise PublicationError(
                f"{label}: local_output_path does not match canonical path"
            )
        local_paths.add(local_path)

        output = row.get("output")
        source = row.get("source")
        if not isinstance(output, dict) or not isinstance(source, dict):
            raise PublicationError(f"{label}: missing output/source evidence")
        size = _positive_integer(
            output.get("encoded_size_bytes"),
            label=f"{label} output encoded_size_bytes",
        )
        encoded_sha256 = _sha256(
            output.get("encoded_sha256"),
            label=f"{label} output encoded_sha256",
        )
        decoded_sha256 = _sha256(
            output.get("decoded_pcm_sha256"),
            label=f"{label} output decoded_pcm_sha256",
        )
        source_pcm_sha256 = _sha256(
            source.get("pcm_sha256"),
            label=f"{label} source pcm_sha256",
        )
        if decoded_sha256 != source_pcm_sha256:
            raise PublicationError(
                f"{label}: output/source decoded PCM hash mismatch"
            )
        _identity(
            local_path,
            expected_size=size,
            expected_sha256=encoded_sha256,
            label=relative,
        )
        try:
            with local_path.open("rb") as audio:
                signature = audio.read(4)
        except OSError as error:
            raise PublicationError(f"{label}: cannot read audio") from error
        if signature != b"fLaC":
            raise PublicationError(f"{label}: audio lacks FLAC signature")
        result[relative] = ExpectedObject(
            relative_path=relative,
            local_path=local_path,
            uri=uri,
            object_name="",
            size=size,
            sha256=encoded_sha256,
            content_type="audio/flac",
        )
    return result


def _local_file_universe(build_root: pathlib.Path) -> set[str]:
    if build_root.is_symlink() or not build_root.is_dir():
        raise PublicationError("build root must be a regular directory")
    files: set[str] = set()
    for current, directories, names in os.walk(
        build_root,
        followlinks=False,
    ):
        current_path = pathlib.Path(current)
        for directory in directories:
            if (current_path / directory).is_symlink():
                raise PublicationError(
                    f"extra local symlink: "
                    f"{(current_path / directory).relative_to(build_root)}"
                )
        for name in names:
            path = current_path / name
            relative = path.relative_to(build_root).as_posix()
            if path.is_symlink() or not path.is_file():
                raise PublicationError(
                    f"extra local non-regular file: {relative}"
                )
            files.add(relative)
    return files


def prepare_publication(
    build_root: str | pathlib.Path,
    *,
    gcs_prefix: str,
) -> PublicationPlan:
    """Derive and byte-validate the exact publication universe offline."""

    root = pathlib.Path(build_root)
    bucket_name, object_prefix = _parse_prefix(gcs_prefix)
    success_path = root / _SUCCESS_PATH
    success_payload = _read_bytes(success_path, label=_SUCCESS_PATH)
    success = _strict_object(success_payload, label=_SUCCESS_PATH)
    if success.get("schema_version") != 1:
        raise PublicationError("_SUCCESS.json: unsupported schema_version")
    if success.get("kind") != "local_reconstruction_success":
        raise PublicationError("_SUCCESS.json: unexpected completion kind")
    if success.get("verified") is not True:
        raise PublicationError("_SUCCESS.json: build is not verified")
    request_count = _positive_integer(
        success.get("request_count"),
        label="_SUCCESS.json request_count",
    )
    audio_count = _positive_integer(
        success.get("audio_count"),
        label="_SUCCESS.json audio_count",
    )
    if request_count != audio_count:
        raise PublicationError(
            "_SUCCESS.json: request_count and audio_count differ"
        )

    expected = _success_artifacts(root, success)
    if _MAPPING_PATH not in expected:
        raise PublicationError(
            f"_SUCCESS.json: missing required {_MAPPING_PATH} artifact"
        )
    if _MATERIALIZATION_PATH not in expected:
        raise PublicationError(
            f"_SUCCESS.json: missing required {_MATERIALIZATION_PATH} artifact"
        )
    expected_materialization_sha256 = _sha256(
        success.get("materialization_receipt_artifact_sha256"),
        label="_SUCCESS.json materialization receipt SHA-256",
    )
    if (
        expected[_MATERIALIZATION_PATH].sha256
        != expected_materialization_sha256
    ):
        raise PublicationError(
            "_SUCCESS.json: materialization receipt SHA-256 differs "
            "from artifact evidence"
        )
    audio = _mapping_audio(
        root,
        gcs_prefix=gcs_prefix,
        mapping_path=expected[_MAPPING_PATH].local_path,
        expected_count=audio_count,
    )
    overlap = set(expected).intersection(audio)
    if overlap:
        raise PublicationError(
            f"artifact/audio object paths overlap: {sorted(overlap)}"
        )
    expected.update(audio)

    success_identity = artifacts.FileIdentity(
        size=len(success_payload),
        sha256=hashlib.sha256(success_payload).hexdigest(),
    )
    expected[_SUCCESS_PATH] = ExpectedObject(
        relative_path=_SUCCESS_PATH,
        local_path=success_path,
        uri="",
        object_name="",
        size=success_identity.size,
        sha256=success_identity.sha256,
        content_type="application/json",
    )

    actual_files = _local_file_universe(root)
    expected_files = set(expected)
    missing = sorted(expected_files - actual_files)
    extra = sorted(actual_files - expected_files)
    if missing or extra:
        raise PublicationError(
            f"local file universe differs; missing={missing}; extra={extra}"
        )

    objects: list[ExpectedObject] = []
    for relative, item in sorted(expected.items()):
        object_name = f"{object_prefix}{relative}"
        uri = f"gs://{bucket_name}/{object_name}"
        if item.uri and item.uri != uri:
            raise PublicationError(
                f"{relative}: mapping URI differs from derived object URI"
            )
        objects.append(
            dataclasses.replace(
                item,
                uri=uri,
                object_name=object_name,
            )
        )
    return PublicationPlan(
        build_root=root.resolve(),
        gcs_prefix=gcs_prefix,
        bucket_name=bucket_name,
        object_prefix=object_prefix,
        success_sha256=success_identity.sha256,
        objects=tuple(objects),
    )


def _list_remote(
    storage_client: object,
    plan: PublicationPlan,
) -> dict[str, tuple[int, int]]:
    try:
        blobs = storage_client.list_blobs(
            plan.bucket_name,
            prefix=plan.object_prefix,
        )
        result: dict[str, tuple[int, int]] = {}
        for blob in blobs:
            name = getattr(blob, "name", None)
            size = getattr(blob, "size", None)
            generation = getattr(blob, "generation", None)
            if not isinstance(name, str) or not name.startswith(
                plan.object_prefix
            ):
                raise PublicationError("provider listed an invalid object name")
            if name in result:
                raise PublicationError(
                    f"provider listed duplicate object: gs://"
                    f"{plan.bucket_name}/{name}"
                )
            observed_size = _positive_integer(
                size,
                label=f"gs://{plan.bucket_name}/{name} size",
            )
            observed_generation = _positive_integer(
                generation,
                label=f"gs://{plan.bucket_name}/{name} generation",
            )
            result[name] = (observed_size, observed_generation)
        return result
    except PublicationError:
        raise
    except Exception as error:
        raise PublicationError("cannot list publication prefix") from error


def _validate_remote_subset(
    remote: Mapping[str, tuple[int, int]],
    *,
    expected: Mapping[str, ExpectedObject],
) -> None:
    extras = sorted(set(remote) - set(expected))
    if extras:
        raise PublicationError(f"remote extra objects: {extras}")
    for name, (size, _) in remote.items():
        if size != expected[name].size:
            raise PublicationError(
                f"remote size conflict for {name}: expected "
                f"{expected[name].size}, observed {size}"
            )


def _publish_one(
    storage_client: object,
    item: ExpectedObject,
) -> artifacts.PublicationReceipt:
    try:
        receipt = artifacts.publish_file_create_only(
            storage_client,
            item.local_path,
            item.uri,
            content_type=item.content_type,
        )
    except artifacts.ArtifactError as error:
        raise PublicationError(str(error)) from error
    if receipt.size != item.size or receipt.sha256 != item.sha256:
        raise PublicationError(
            f"local bytes changed after publication planning: "
            f"{item.relative_path}"
        )
    return receipt


def _receipt_object(
    item: ExpectedObject,
    receipt: artifacts.PublicationReceipt,
) -> dict[str, object]:
    return {
        "generation": receipt.generation,
        "path": item.relative_path,
        "sha256": receipt.sha256,
        "size": receipt.size,
        "uri": receipt.uri,
    }


def publish_completed_build(
    storage_client: object,
    *,
    build_root: str | pathlib.Path,
    gcs_prefix: str,
    receipt_path: str | pathlib.Path,
    max_workers: int = 8,
) -> PublicationResult:
    """Create-only publish a completed build and verify its remote universe."""

    if (
        isinstance(max_workers, bool)
        or not isinstance(max_workers, int)
        or max_workers <= 0
    ):
        raise PublicationError("max_workers must be a positive integer")
    plan = prepare_publication(build_root, gcs_prefix=gcs_prefix)
    destination = pathlib.Path(receipt_path)
    resolved_destination = destination.resolve()
    if resolved_destination.is_relative_to(plan.build_root):
        raise PublicationError(
            "publication receipt must be written outside the build root"
        )

    expected_by_name = {item.object_name: item for item in plan.objects}
    initial_remote = _list_remote(storage_client, plan)
    _validate_remote_subset(initial_remote, expected=expected_by_name)
    marker_name = plan.completion_marker.object_name
    if marker_name in initial_remote and set(initial_remote) != set(
        expected_by_name
    ):
        raise PublicationError(
            "remote completion marker exists but its object universe "
            "is incomplete"
        )

    marker = plan.completion_marker
    regular = tuple(
        item for item in plan.objects if item.relative_path != _SUCCESS_PATH
    )
    audio_objects = tuple(
        item for item in regular if item.relative_path.startswith("audio/")
    )
    artifact_objects = tuple(
        item
        for item in regular
        if not item.relative_path.startswith("audio/")
    )
    receipts: dict[str, artifacts.PublicationReceipt] = {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(_publish_one, storage_client, item): item
            for item in audio_objects
        }
        for future in as_completed(futures):
            item = futures[future]
            receipts[item.relative_path] = future.result()
    for item in artifact_objects:
        receipts[item.relative_path] = _publish_one(storage_client, item)

    before_marker = _list_remote(storage_client, plan)
    _validate_remote_subset(before_marker, expected=expected_by_name)
    expected_before_marker = set(expected_by_name) - {marker_name}
    observed_before_marker = set(before_marker)
    if (
        observed_before_marker != expected_before_marker
        and observed_before_marker != set(expected_by_name)
    ):
        missing = sorted(expected_before_marker - set(before_marker))
        raise PublicationError(
            f"remote data universe incomplete before marker: {missing}"
        )

    receipts[marker.relative_path] = _publish_one(storage_client, marker)
    final_remote = _list_remote(storage_client, plan)
    _validate_remote_subset(final_remote, expected=expected_by_name)
    if set(final_remote) != set(expected_by_name):
        missing = sorted(set(expected_by_name) - set(final_remote))
        raise PublicationError(
            f"remote publication universe is missing objects: {missing}"
        )

    receipt_objects: list[dict[str, object]] = []
    for item in plan.objects:
        receipt = receipts[item.relative_path]
        remote_size, remote_generation = final_remote[item.object_name]
        if (
            remote_size != receipt.size
            or remote_generation != receipt.generation
        ):
            raise PublicationError(
                f"remote identity drift after publication: {item.uri}"
            )
        receipt_objects.append(_receipt_object(item, receipt))
    receipt_objects.sort(key=lambda value: str(value["path"]))
    aggregate_basis = {
        "gcs_prefix": plan.gcs_prefix,
        "objects": receipt_objects,
        "schema_version": 1,
    }
    aggregate_sha256 = artifacts.canonical_json_sha256(aggregate_basis)
    receipt_value = {
        "aggregate_sha256": aggregate_sha256,
        "build_success_sha256": plan.success_sha256,
        "gcs_prefix": plan.gcs_prefix,
        "kind": "gcs_create_only_publication",
        "object_count": len(receipt_objects),
        "objects": receipt_objects,
        "remote_universe_verified": True,
        "schema_version": 1,
    }
    try:
        local_receipt = artifacts.write_json_create_only(
            destination,
            receipt_value,
        )
    except artifacts.ArtifactError as error:
        raise PublicationError(str(error)) from error
    return PublicationResult(
        gcs_prefix=plan.gcs_prefix,
        object_count=len(receipt_objects),
        aggregate_sha256=aggregate_sha256,
        receipt_path=local_receipt.path,
        receipt_sha256=local_receipt.sha256,
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Create-only publish a completed reconstructed SFT dataset; "
            "no inference or tuning is performed."
        )
    )
    parser.add_argument("--build-root", type=pathlib.Path, required=True)
    parser.add_argument("--gcs-prefix", required=True)
    parser.add_argument("--receipt-path", type=pathlib.Path, required=True)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--project")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run the deliberately small one-time publication CLI."""

    arguments = _parser().parse_args(argv)
    try:
        from google.cloud import storage

        client = storage.Client(project=arguments.project)
        result = publish_completed_build(
            client,
            build_root=arguments.build_root,
            gcs_prefix=arguments.gcs_prefix,
            receipt_path=arguments.receipt_path,
            max_workers=arguments.workers,
        )
    except PublicationError as error:
        raise SystemExit(str(error)) from error
    print(
        json.dumps(
            {
                "aggregate_sha256": result.aggregate_sha256,
                "gcs_prefix": result.gcs_prefix,
                "object_count": result.object_count,
                "receipt_path": str(result.receipt_path),
                "receipt_sha256": result.receipt_sha256,
                "status": "COMPLETE",
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
