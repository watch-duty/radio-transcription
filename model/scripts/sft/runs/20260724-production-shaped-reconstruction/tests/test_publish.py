"""Focused create-only publication contract tests."""

# ruff: noqa: D202, EM101, INP001, TRY003

from __future__ import annotations

import hashlib
import json
import pathlib
import threading

import pytest
from dataset_run import artifacts, publish
from google.api_core.exceptions import PreconditionFailed

_PREFIX = "gs://bucket/datasets/reconstruction/"
_OBJECT_PREFIX = "datasets/reconstruction/"


def _write(path: pathlib.Path, payload: bytes) -> dict[str, object]:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)
    return {
        "path": path.as_posix(),
        "sha256": hashlib.sha256(payload).hexdigest(),
        "size": len(payload),
    }


def _completed_build(tmp_path: pathlib.Path) -> pathlib.Path:
    build = tmp_path / "build"
    audio = b"fLaC\x00fixture-audio"
    audio_path = build / "audio/train/bcfy_feeds/request-1.flac"
    _write(audio_path, audio)

    materialization = b'{"request_id":"request-1"}\n'
    materialization_identity = _write(
        build / "receipts/materialization.jsonl",
        materialization,
    )
    mapping_row = {
        "canonical_audio_uri": (
            f"{_PREFIX}audio/train/bcfy_feeds/request-1.flac"
        ),
        "local_output_path": str(audio_path),
        "output": {
            "decoded_pcm_sha256": "a" * 64,
            "encoded_sha256": hashlib.sha256(audio).hexdigest(),
            "encoded_size_bytes": len(audio),
        },
        "request_id": "request-1",
        "source": {"pcm_sha256": "a" * 64},
    }
    mapping = artifacts.canonical_json_bytes(mapping_row)
    mapping_identity = _write(
        build / "receipts/materialization_mapping.jsonl",
        mapping,
    )
    manifest_identity = _write(
        build / "manifests/canonical/train.jsonl",
        b'{"audio_filepath":"gs://fixture"}\n',
    )
    artifact_identities = []
    for identity in (
        manifest_identity,
        materialization_identity,
        mapping_identity,
    ):
        artifact_identities.append(
            {
                **identity,
                "path": pathlib.Path(str(identity["path"]))
                .relative_to(build)
                .as_posix(),
            }
        )
    artifact_identities.sort(key=lambda item: str(item["path"]))
    success = {
        "artifact_count": len(artifact_identities),
        "artifacts": artifact_identities,
        "audio_count": 1,
        "kind": "local_reconstruction_success",
        "materialization_receipt_artifact_sha256": (
            materialization_identity["sha256"]
        ),
        "request_count": 1,
        "schema_version": 1,
        "verification": {"train_request_count": 1},
        "verified": True,
    }
    _write(build / "_SUCCESS.json", artifacts.canonical_json_bytes(success))
    return build


class _FakeRemoteObject:
    def __init__(
        self,
        *,
        payload: bytes,
        content_type: str,
        generation: int,
    ) -> None:
        self.payload = payload
        self.content_type = content_type
        self.generation = generation

    @property
    def size(self) -> int:
        return len(self.payload)


class _FakeBlob:
    def __init__(
        self,
        client: _FakeStorageClient,
        bucket_name: str,
        name: str,
    ) -> None:
        self._client = client
        self._bucket_name = bucket_name
        self.name = name
        self.generation: int | None = None
        self.size: int | None = None
        self.content_type: str | None = None

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
        key = (self._bucket_name, self.name)
        with self._client.lock:
            self._client.upload_attempts.append(self.name)
            if key in self._client.objects:
                raise PreconditionFailed("already exists")
            generation = self._client.next_generation
            self._client.next_generation += 1
            self._client.objects[key] = _FakeRemoteObject(
                payload=payload,
                content_type=content_type,
                generation=generation,
            )

    def reload(self, *, timeout: float) -> None:
        assert timeout >= 600
        key = (self._bucket_name, self.name)
        with self._client.lock:
            remote = self._client.objects[key]
            self.generation = remote.generation
            self.size = remote.size
            self.content_type = remote.content_type

    def download_as_bytes(
        self,
        *,
        if_generation_match: int,
        timeout: float,
    ) -> bytes:
        assert timeout >= 600
        key = (self._bucket_name, self.name)
        with self._client.lock:
            remote = self._client.objects[key]
            if remote.generation != if_generation_match:
                raise RuntimeError("generation mismatch")
            return remote.payload


class _FakeBucket:
    def __init__(
        self,
        client: _FakeStorageClient,
        bucket_name: str,
    ) -> None:
        self._client = client
        self._bucket_name = bucket_name

    def blob(self, name: str) -> _FakeBlob:
        return _FakeBlob(self._client, self._bucket_name, name)


class _ListedBlob:
    def __init__(self, name: str, remote: _FakeRemoteObject) -> None:
        self.name = name
        self.size = remote.size
        self.generation = remote.generation


class _FakeStorageClient:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], _FakeRemoteObject] = {}
        self.upload_attempts: list[str] = []
        self.list_calls = 0
        self.next_generation = 100
        self.lock = threading.Lock()

    def bucket(self, bucket_name: str) -> _FakeBucket:
        return _FakeBucket(self, bucket_name)

    def list_blobs(
        self,
        bucket_name: str,
        *,
        prefix: str,
    ) -> list[_ListedBlob]:
        with self.lock:
            self.list_calls += 1
            return [
                _ListedBlob(name, remote)
                for (bucket, name), remote in sorted(self.objects.items())
                if bucket == bucket_name and name.startswith(prefix)
            ]

    def seed(
        self,
        name: str,
        payload: bytes,
        content_type: str,
    ) -> int:
        generation = self.next_generation
        self.next_generation += 1
        self.objects[("bucket", name)] = _FakeRemoteObject(
            payload=payload,
            content_type=content_type,
            generation=generation,
        )
        return generation


def test_completion_marker_is_published_last_and_receipt_covers_universe(
    tmp_path: pathlib.Path,
) -> None:
    """Catches publishing the success marker before any data object."""

    build = _completed_build(tmp_path)
    receipt_path = tmp_path / "publication-receipt.json"
    client = _FakeStorageClient()

    result = publish.publish_completed_build(
        client,
        build_root=build,
        gcs_prefix=_PREFIX,
        receipt_path=receipt_path,
        max_workers=2,
    )

    assert client.upload_attempts == [
        f"{_OBJECT_PREFIX}audio/train/bcfy_feeds/request-1.flac",
        f"{_OBJECT_PREFIX}manifests/canonical/train.jsonl",
        f"{_OBJECT_PREFIX}receipts/materialization.jsonl",
        f"{_OBJECT_PREFIX}receipts/materialization_mapping.jsonl",
        f"{_OBJECT_PREFIX}_SUCCESS.json",
    ]
    assert len(client.objects) == 5
    assert result.object_count == 5
    receipt = json.loads(receipt_path.read_bytes())
    assert receipt["aggregate_sha256"] == result.aggregate_sha256
    assert receipt["gcs_prefix"] == _PREFIX
    assert receipt["object_count"] == 5
    assert receipt["remote_universe_verified"] is True
    assert [item["path"] for item in receipt["objects"]] == [
        "_SUCCESS.json",
        "audio/train/bcfy_feeds/request-1.flac",
        "manifests/canonical/train.jsonl",
        "receipts/materialization.jsonl",
        "receipts/materialization_mapping.jsonl",
    ]


def test_partial_publication_and_complete_rerun_are_idempotently_resumed(
    tmp_path: pathlib.Path,
) -> None:
    """Catches overwriting exact objects or changing their generations."""

    build = _completed_build(tmp_path)
    receipt_path = tmp_path / "publication-receipt.json"
    client = _FakeStorageClient()
    manifest_path = build / "manifests/canonical/train.jsonl"
    initial_generation = client.seed(
        f"{_OBJECT_PREFIX}manifests/canonical/train.jsonl",
        manifest_path.read_bytes(),
        "application/x-ndjson",
    )

    first = publish.publish_completed_build(
        client,
        build_root=build,
        gcs_prefix=_PREFIX,
        receipt_path=receipt_path,
        max_workers=1,
    )
    generations_after_first = {
        name: value.generation for (_, name), value in client.objects.items()
    }
    second = publish.publish_completed_build(
        client,
        build_root=build,
        gcs_prefix=_PREFIX,
        receipt_path=receipt_path,
        max_workers=3,
    )

    assert generations_after_first == {
        name: value.generation for (_, name), value in client.objects.items()
    }
    assert (
        generations_after_first[
            f"{_OBJECT_PREFIX}manifests/canonical/train.jsonl"
        ]
        == initial_generation
    )
    assert first == second


def test_conflicting_existing_object_is_never_adopted(
    tmp_path: pathlib.Path,
) -> None:
    """Catches treating equal-size but unequal bytes as an idempotent resume."""

    build = _completed_build(tmp_path)
    audio_path = build / "audio/train/bcfy_feeds/request-1.flac"
    client = _FakeStorageClient()
    client.seed(
        f"{_OBJECT_PREFIX}audio/train/bcfy_feeds/request-1.flac",
        b"x" * audio_path.stat().st_size,
        "audio/flac",
    )

    with pytest.raises(
        publish.PublicationError,
        match="mismatched object",
    ):
        publish.publish_completed_build(
            client,
            build_root=build,
            gcs_prefix=_PREFIX,
            receipt_path=tmp_path / "receipt.json",
            max_workers=1,
        )

    assert f"{_OBJECT_PREFIX}_SUCCESS.json" not in {
        name for _, name in client.objects
    }


@pytest.mark.parametrize("mutation", ["missing", "extra"])
def test_local_universe_drift_fails_before_any_network_call(
    tmp_path: pathlib.Path,
    mutation: str,
) -> None:
    """Catches publishing an incomplete or contaminated local build."""

    build = _completed_build(tmp_path)
    if mutation == "missing":
        (build / "audio/train/bcfy_feeds/request-1.flac").unlink()
    else:
        (build / "unexpected.tmp").write_bytes(b"extra")
    client = _FakeStorageClient()

    with pytest.raises(publish.PublicationError, match=mutation):
        publish.publish_completed_build(
            client,
            build_root=build,
            gcs_prefix=_PREFIX,
            receipt_path=tmp_path / "receipt.json",
        )

    assert client.list_calls == 0
    assert client.upload_attempts == []


def test_remote_extra_fails_before_any_upload(
    tmp_path: pathlib.Path,
) -> None:
    """Catches blessing an object prefix with an undeclared object."""

    build = _completed_build(tmp_path)
    client = _FakeStorageClient()
    client.seed(
        f"{_OBJECT_PREFIX}unexpected.bin",
        b"extra",
        "application/octet-stream",
    )

    with pytest.raises(publish.PublicationError, match="remote extra"):
        publish.publish_completed_build(
            client,
            build_root=build,
            gcs_prefix=_PREFIX,
            receipt_path=tmp_path / "receipt.json",
        )

    assert client.upload_attempts == []
