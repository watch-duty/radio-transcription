from __future__ import annotations

import types
from pathlib import Path
from typing import Any


def split_gcs(uri: str) -> tuple[str, str]:
    assert uri.startswith("gs://")
    bucket, blob = uri[len("gs://") :].split("/", maxsplit=1)
    return bucket, blob


class FakeBlob:
    def __init__(
        self,
        store: dict[tuple[str, str], str],
        uploads: list[str],
        bucket: str,
        name: str,
    ) -> None:
        self._store = store
        self._uploads = uploads
        self._bucket = bucket
        self.name = name

    def exists(self, **_: Any) -> bool:
        return (self._bucket, self.name) in self._store

    def upload_from_filename(self, filename: str, **_: Any) -> None:
        self._store[(self._bucket, self.name)] = Path(filename).read_text(
            encoding="utf-8"
        )
        self._uploads.append(f"gs://{self._bucket}/{self.name}")

    def upload_from_string(
        self, data: str, content_type: str | None = None, **_: Any
    ) -> None:
        del content_type
        self._store[(self._bucket, self.name)] = data
        self._uploads.append(f"gs://{self._bucket}/{self.name}")

    def download_to_filename(self, filename: str, **_: Any) -> None:
        Path(filename).parent.mkdir(parents=True, exist_ok=True)
        Path(filename).write_text(
            self._store[(self._bucket, self.name)], encoding="utf-8"
        )

    def download_as_text(self, **_: Any) -> str:
        return self._store[(self._bucket, self.name)]


class FakeBucket:
    def __init__(
        self,
        store: dict[tuple[str, str], str],
        uploads: list[str],
        name: str,
    ) -> None:
        self._store = store
        self._uploads = uploads
        self.name = name

    def blob(self, name: str) -> FakeBlob:
        return FakeBlob(self._store, self._uploads, self.name, name)

    def list_blobs(
        self, prefix: str = "", max_results: int | None = None
    ) -> list[types.SimpleNamespace]:
        names = [
            blob_name
            for bucket_name, blob_name in self._store
            if bucket_name == self.name and blob_name.startswith(prefix)
        ]
        if max_results is not None:
            names = names[:max_results]
        return [types.SimpleNamespace(name=name) for name in names]


class FakeStorageClient:
    def __init__(self) -> None:
        self.store: dict[tuple[str, str], str] = {}
        self.uploads: list[str] = []

    def bucket(self, name: str) -> FakeBucket:
        return FakeBucket(self.store, self.uploads, name)

    def put(self, uri: str, text: str) -> None:
        self.store[split_gcs(uri)] = text

    def get(self, uri: str) -> str:
        return self.store[split_gcs(uri)]

    def has(self, uri: str) -> bool:
        return split_gcs(uri) in self.store
