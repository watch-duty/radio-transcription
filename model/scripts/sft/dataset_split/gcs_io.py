from __future__ import annotations

import json
from typing import Any, Protocol


class GcsInputError(ValueError):
    """Raised when configured GCS input cannot be read or parsed."""


class TextReader(Protocol):
    def read_text(self, uri: str) -> str:
        """Read the configured URI as text."""


def require_gs_uri(uri: str, *, label: str) -> None:
    if not uri.startswith("gs://"):
        raise GcsInputError(f"{label} must be a gs:// URI: {uri}")


class GoogleCloudTextReader:
    def __init__(self, storage_client: object) -> None:
        self._storage_client = storage_client

    def read_text(self, uri: str) -> str:
        require_gs_uri(uri, label="uri")
        try:
            from common.gcs_utils import parse_gcs_uri
            from google.cloud.storage.retry import DEFAULT_RETRY

            bucket_name, blob_path = parse_gcs_uri(uri)
            bucket = self._storage_client.bucket(bucket_name)
            blob = bucket.blob(blob_path)
            return blob.download_as_text(retry=DEFAULT_RETRY)
        except Exception as exc:
            raise GcsInputError(f"failed to read {uri}: {exc}") from exc


def read_json_or_jsonl(uri: str, reader: TextReader) -> list[dict[str, Any]]:
    require_gs_uri(uri, label="uri")
    try:
        content = reader.read_text(uri)
    except GcsInputError:
        raise
    except Exception as exc:
        raise GcsInputError(f"failed to read {uri}: {exc}") from exc

    content = content.strip()
    if not content:
        raise GcsInputError(f"{uri} is empty")

    if content.startswith("["):
        return _parse_json_array(uri, content)
    return _parse_jsonl(uri, content)


def _parse_json_array(uri: str, content: str) -> list[dict[str, Any]]:
    try:
        rows = json.loads(content)
    except json.JSONDecodeError as exc:
        raise GcsInputError(f"{uri}: invalid JSON: {exc}") from exc
    if not isinstance(rows, list):
        raise GcsInputError(f"{uri}: JSON content must be an array")
    parsed_rows: list[dict[str, Any]] = []
    for index, row in enumerate(rows, start=1):
        if not isinstance(row, dict):
            raise GcsInputError(f"{uri}: line {index} must be a JSON object")
        parsed_rows.append(row)
    return parsed_rows


def _parse_jsonl(uri: str, content: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for line_number, line in enumerate(content.splitlines(), start=1):
        line = line.strip()
        if not line:
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError as exc:
            raise GcsInputError(
                f"{uri}: line {line_number} contains malformed JSON: {exc}"
            ) from exc
        if not isinstance(row, dict):
            raise GcsInputError(
                f"{uri}: line {line_number} must be a JSON object"
            )
        rows.append(row)
    if not rows:
        raise GcsInputError(f"{uri} is empty")
    return rows
