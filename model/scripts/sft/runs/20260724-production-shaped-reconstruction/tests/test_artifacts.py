"""Tests for strict configuration and immutable artifact handling."""

from __future__ import annotations

import dataclasses
import hashlib
import pathlib
import sys

import pytest
from google.api_core.exceptions import PreconditionFailed

RUN_ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(RUN_ROOT) not in sys.path:
    sys.path.insert(0, str(RUN_ROOT))

from dataset_run import artifacts, config  # noqa: E402


def _lock(path: pathlib.Path, *, label: str = "local") -> config.LockedInput:
    payload = path.read_bytes()
    return config.LockedInput(
        label=label,
        uri=str(path),
        generation=None,
        size=len(payload),
        sha256=hashlib.sha256(payload).hexdigest(),
    )


def test_current_run_toml_parses_without_legacy_defaults() -> None:
    parsed = config.load_run_config(RUN_ROOT / "run.toml")

    assert parsed.run_id == "20260724-production-shaped-reconstruction"
    assert [item.label for item in parsed.local_inputs] == [
        "production_duration_target"
    ]
    assert (
        parsed.local_inputs[0].sha256
        == "3226cdde044ca1ff8b65806173fe01eacfe0c89b2af1a5563a5f7e6ccd2ccd88"
    )
    assert len(parsed.canonical_inputs) == 6
    assert len(parsed.raw_inputs) == 8
    assert len(parsed.masked_inputs) == 4
    assert parsed.masked_inputs[0].label.startswith("masked_")
    assert len(parsed.input_by_label()) == 19


def test_config_rejects_unknown_field(tmp_path: pathlib.Path) -> None:
    text = (RUN_ROOT / "run.toml").read_text()
    path = tmp_path / "run.toml"
    path.write_text(
        text.replace('run_id = "', 'legacy_mode = true\nrun_id = "', 1)
    )

    with pytest.raises(config.ConfigError, match="unexpected"):
        config.load_run_config(path)


def test_local_input_cache_is_deterministic_and_detects_drift(
    tmp_path: pathlib.Path,
) -> None:
    source = tmp_path / "input.jsonl"
    source.write_bytes(b'{"row":1}\n')
    lock = _lock(source)
    cache = tmp_path / "cache"

    first = artifacts.acquire_locked_input(lock, cache)
    second = artifacts.acquire_locked_input(lock, cache)
    assert first.path == second.path
    assert first.path.read_bytes() == source.read_bytes()

    source.write_bytes(b"source drift")
    with pytest.raises(artifacts.ArtifactError, match="size drift"):
        artifacts.acquire_locked_input(lock, cache)
    source.write_bytes(b'{"row":1}\n')

    first.path.write_bytes(b"x" * lock.size)
    with pytest.raises(artifacts.ArtifactError, match="SHA-256 drift"):
        artifacts.acquire_locked_input(lock, cache)


def test_local_input_never_adopts_wrong_bytes(
    tmp_path: pathlib.Path,
) -> None:
    source = tmp_path / "input.json"
    source.write_bytes(b"expected")
    lock = _lock(source)
    source.write_bytes(b"drifted!")

    with pytest.raises(artifacts.ArtifactError, match="SHA-256 drift"):
        artifacts.acquire_locked_input(lock, tmp_path / "cache")
    assert not any(
        item.name == "payload" for item in (tmp_path / "cache").rglob("*")
    )


def test_cache_receipt_must_remain_byte_canonical(
    tmp_path: pathlib.Path,
) -> None:
    source = tmp_path / "input.json"
    source.write_bytes(b"locked")
    lock = _lock(source)
    cached = artifacts.acquire_locked_input(lock, tmp_path / "cache")
    receipt = cached.path.parent / "receipt.json"
    receipt.write_text(
        receipt.read_text().replace('{"generation"', '{ "generation"')
    )

    with pytest.raises(artifacts.ArtifactError, match="canonicalization drift"):
        artifacts.acquire_locked_input(lock, tmp_path / "cache")


@dataclasses.dataclass
class _Blob:
    payload: bytes
    generation: int = 11
    size: int | None = None
    downloaded_generation: int | None = None

    def __post_init__(self) -> None:
        if self.size is None:
            self.size = len(self.payload)

    def reload(self, *, if_generation_match: int | None = None) -> None:
        if (
            if_generation_match is not None
            and if_generation_match != self.generation
        ):
            raise RuntimeError("generation mismatch")

    def download_to_filename(
        self,
        filename: str,
        *,
        if_generation_match: int,
    ) -> None:
        self.downloaded_generation = if_generation_match
        pathlib.Path(filename).write_bytes(self.payload)


@dataclasses.dataclass
class _Bucket:
    blob_value: _Blob

    def blob(self, _: str) -> _Blob:
        return self.blob_value


@dataclasses.dataclass
class _Client:
    blob_value: _Blob

    def bucket(self, _: str) -> _Bucket:
        return _Bucket(self.blob_value)


def test_gcs_acquisition_locks_generation_size_and_sha(
    tmp_path: pathlib.Path,
) -> None:
    payload = b'{"locked":true}\n'
    blob = _Blob(payload)
    lock = config.LockedInput(
        label="gcs",
        uri="gs://bucket/input.jsonl",
        generation=11,
        size=len(payload),
        sha256=hashlib.sha256(payload).hexdigest(),
    )

    cached = artifacts.acquire_locked_input(
        lock,
        tmp_path / "cache",
        storage_client=_Client(blob),
    )
    assert cached.path.read_bytes() == payload
    assert blob.downloaded_generation == 11

    drift = dataclasses.replace(lock, generation=10)
    with pytest.raises(artifacts.ArtifactError, match="generation drift"):
        artifacts.acquire_locked_input(
            drift,
            tmp_path / "other-cache",
            storage_client=_Client(blob),
        )


def test_json_jsonl_and_flac_writes_are_create_only(
    tmp_path: pathlib.Path,
) -> None:
    json_path = tmp_path / "artifact.json"
    first = artifacts.write_json_create_only(json_path, {"z": 1, "a": 2})
    second = artifacts.write_json_create_only(json_path, {"a": 2, "z": 1})
    assert first.created is True
    assert second.created is False
    assert json_path.read_bytes() == b'{"a":2,"z":1}\n'
    assert artifacts.canonical_json_sha256({"z": 1, "a": 2}) == first.sha256

    with pytest.raises(artifacts.ArtifactError, match="drift"):
        artifacts.write_json_create_only(json_path, {"a": 3})

    jsonl_path = tmp_path / "artifact.jsonl"
    artifacts.write_jsonl_create_only(jsonl_path, [{"b": 2}, {"a": 1}])
    assert jsonl_path.read_bytes() == b'{"b":2}\n{"a":1}\n'

    flac_path = tmp_path / "artifact.flac"
    artifacts.write_flac_create_only(flac_path, b"fLaCpayload")
    with pytest.raises(artifacts.ArtifactError, match="drift"):
        artifacts.write_flac_create_only(flac_path, b"fLaCdifferent")


def test_stage_marker_is_written_only_after_successful_verification(
    tmp_path: pathlib.Path,
) -> None:
    artifact = artifacts.write_json_create_only(
        tmp_path / "output.json",
        {"complete": True},
    )
    marker = tmp_path / "stage.done.json"

    with pytest.raises(RuntimeError, match="not verified"):
        artifacts.write_stage_marker(
            "assemble",
            marker,
            artifacts=[artifact],
            verify=lambda: (_ for _ in ()).throw(RuntimeError("not verified")),
        )
    assert not marker.exists()

    receipt = artifacts.write_stage_marker(
        "assemble",
        marker,
        artifacts=[artifact],
        verify=lambda: {"owners_exact_once": True},
    )
    assert receipt.created is True
    assert b'"owners_exact_once":true' in marker.read_bytes()

    artifact.path.write_bytes(b"tampered")
    with pytest.raises(artifacts.ArtifactError, match="drift"):
        artifacts.write_stage_marker(
            "assemble",
            marker,
            artifacts=[artifact],
            verify=lambda: True,
        )


def test_stage_marker_rejects_artifact_mutation_during_verification(
    tmp_path: pathlib.Path,
) -> None:
    artifact = artifacts.write_json_create_only(
        tmp_path / "output.json",
        {"complete": True},
    )
    marker = tmp_path / "stage.done.json"

    def mutate() -> bool:
        artifact.path.write_bytes(b"mutated")
        return True

    with pytest.raises(artifacts.ArtifactError, match="drift"):
        artifacts.write_stage_marker(
            "assemble",
            marker,
            artifacts=[artifact],
            verify=mutate,
        )
    assert not marker.exists()


def test_copy_file_create_only_checks_expected_identity(
    tmp_path: pathlib.Path,
) -> None:
    source = tmp_path / "source"
    source.write_bytes(b"payload")
    destination = tmp_path / "destination"
    identity = artifacts.inspect_file(source)

    artifacts.copy_file_create_only(
        source,
        destination,
        expected_size=identity.size,
        expected_sha256=identity.sha256,
    )
    source.write_bytes(b"changed")
    with pytest.raises(artifacts.ArtifactError, match="SHA-256 drift"):
        artifacts.copy_file_create_only(
            source,
            tmp_path / "other",
            expected_size=len(b"changed"),
            expected_sha256=identity.sha256,
        )


class _PublishBlob:
    def __init__(self) -> None:
        self.payload: bytes | None = None
        self.generation = 23
        self.content_type: str | None = None

    @property
    def size(self) -> int:
        return 0 if self.payload is None else len(self.payload)

    def upload_from_string(
        self,
        payload: bytes,
        *,
        content_type: str,
        if_generation_match: int,
        timeout: float,
    ) -> None:
        assert if_generation_match == 0
        assert timeout >= 600
        if self.payload is not None:
            raise PreconditionFailed("already exists")
        self.payload = payload
        self.content_type = content_type

    def reload(self, *, timeout: float) -> None:
        assert timeout >= 600
        if self.payload is None:
            raise FileNotFoundError("missing")

    def download_as_bytes(
        self,
        *,
        if_generation_match: int,
        timeout: float,
    ) -> bytes:
        assert timeout >= 600
        if if_generation_match != self.generation or self.payload is None:
            raise RuntimeError("generation mismatch")
        return self.payload


@dataclasses.dataclass
class _PublishBucket:
    blob_value: _PublishBlob

    def blob(self, _: str) -> _PublishBlob:
        return self.blob_value


@dataclasses.dataclass
class _PublishClient:
    blob_value: _PublishBlob

    def bucket(self, _: str) -> _PublishBucket:
        return _PublishBucket(self.blob_value)


def test_publication_is_explicit_create_only_and_byte_verified(
    tmp_path: pathlib.Path,
) -> None:
    source = tmp_path / "artifact"
    source.write_bytes(b"published bytes")
    blob = _PublishBlob()
    client = _PublishClient(blob)

    first = artifacts.publish_file_create_only(
        client,
        source,
        "gs://bucket/run/artifact",
        content_type="application/octet-stream",
    )
    second = artifacts.publish_file_create_only(
        client,
        source,
        "gs://bucket/run/artifact",
        content_type="application/octet-stream",
    )
    assert first == second
    assert first.generation == 23
    assert blob.content_type == "application/octet-stream"

    blob.payload = b"mismatched objs"
    with pytest.raises(artifacts.ArtifactError, match="mismatched"):
        artifacts.publish_file_create_only(
            client,
            source,
            "gs://bucket/run/artifact",
            content_type="application/octet-stream",
        )

    blob.payload = source.read_bytes()
    blob.content_type = "text/plain"
    with pytest.raises(artifacts.ArtifactError, match="content-type drift"):
        artifacts.publish_file_create_only(
            client,
            source,
            "gs://bucket/run/artifact",
            content_type="application/octet-stream",
        )
