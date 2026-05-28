from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any

from common.gcs_utils import parse_gcs_uri

try:
    from google.api_core.exceptions import PreconditionFailed
except ImportError:  # pragma: no cover - google-cloud-storage is a model dep.
    PreconditionFailed = None  # type: ignore[assignment]


DATASET_VERSION_ROOT = "gs://wd-transcription-data/sft"
_SAFE_PATH_PART = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")


class DatasetArtifactError(ValueError):
    """Raised when dataset-version artifact planning or upload fails."""


class DatasetVersionExistsError(DatasetArtifactError):
    """Raised when immutable dataset-version publication would overwrite."""


@dataclass(frozen=True)
class DatasetArtifactLayout:
    dataset_version_id: str
    root_uri: str
    config_uri: str
    metadata_uri: str
    canonical_train_uri: str
    canonical_eval_uri: str
    reports_json_uri: str
    reports_markdown_uri: str
    audio_prefix_uri: str

    @classmethod
    def for_dataset_version(
        cls,
        dataset_version_id: str,
        *,
        root_prefix: str = DATASET_VERSION_ROOT,
    ) -> DatasetArtifactLayout:
        normalized_id = _clean_path_part(
            dataset_version_id, label="dataset_version_id"
        )

        normalized_root = _clean_root_prefix(root_prefix)

        root_uri = f"{normalized_root}/{normalized_id}/"
        return cls(
            dataset_version_id=normalized_id,
            root_uri=root_uri,
            config_uri=_join_uri(root_uri, "config", "resolved_config.json"),
            metadata_uri=_join_uri(
                root_uri, "metadata", "dataset_version.json"
            ),
            canonical_train_uri=_join_uri(
                root_uri, "manifests", "canonical", "train.jsonl"
            ),
            canonical_eval_uri=_join_uri(
                root_uri, "manifests", "canonical", "eval.jsonl"
            ),
            reports_json_uri=_join_uri(
                root_uri, "reports", "dataset_version_report.json"
            ),
            reports_markdown_uri=_join_uri(
                root_uri, "reports", "dataset_version_report.md"
            ),
            audio_prefix_uri=_join_uri(root_uri, "audio", trailing=True),
        )

    def per_dataset_manifest_uri(self, dataset_name: str, split: str) -> str:
        dataset_name = _clean_path_part(dataset_name, label="dataset_name")
        split = _clean_path_part(split, label="split")
        return _join_uri(
            self.root_uri,
            "manifests",
            "per_dataset",
            dataset_name,
            f"{split}.jsonl",
        )

    def model_input_uri(
        self, writer: str, split: str, suffix: str = "jsonl"
    ) -> str:
        writer = _clean_path_part(writer, label="writer")
        split = _clean_path_part(split, label="split")
        suffix = _clean_suffix(suffix)
        return _join_uri(
            self.root_uri, "model_inputs", writer, f"{split}.{suffix}"
        )

    def artifact_uri_inventory(self) -> dict[str, object]:
        return {
            "root": self.root_uri,
            "config": self.config_uri,
            "metadata": self.metadata_uri,
            "canonical": {
                "train": self.canonical_train_uri,
                "eval": self.canonical_eval_uri,
            },
            "reports": {
                "json": self.reports_json_uri,
                "markdown": self.reports_markdown_uri,
            },
            "model_inputs": {
                "nemo_train": self.model_input_uri("nemo", "train"),
                "nemo_eval": self.model_input_uri("nemo", "eval"),
                "whisper_train": self.model_input_uri("whisper", "train"),
                "whisper_eval": self.model_input_uri("whisper", "eval"),
                "gemini_train": self.model_input_uri("gemini", "train"),
                "gemini_eval": self.model_input_uri("gemini", "eval"),
            },
            "audio_prefix": self.audio_prefix_uri,
        }


def prefix_exists(storage_client: object, root_uri: str) -> bool:
    bucket_name, prefix = parse_gcs_uri(root_uri)
    prefix = _prefix_with_trailing_slash(prefix)
    blobs = storage_client.list_blobs(bucket_name, prefix=prefix, max_results=1)
    return next(iter(blobs), None) is not None


def ensure_dataset_version_absent(
    storage_client: object, root_uri: str
) -> None:
    if prefix_exists(storage_client, root_uri):
        raise DatasetVersionExistsError(
            f"dataset version already exists at {root_uri}"
        )


def upload_text_create_only(
    storage_client: object,
    uri: str,
    text: str,
    *,
    content_type: str,
) -> str:
    bucket_name, blob_path = parse_gcs_uri(uri)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    try:
        blob.upload_from_string(
            text, content_type=content_type, if_generation_match=0
        )
    except Exception as exc:
        if _is_precondition_failure(exc):
            raise DatasetVersionExistsError(
                f"dataset version object already exists at {uri}"
            ) from exc
        raise
    return uri


def _join_uri(root_uri: str, *parts: str, trailing: bool = False) -> str:
    root = root_uri.rstrip("/")
    path = "/".join(part.strip("/") for part in parts)
    uri = f"{root}/{path}" if path else root
    if trailing:
        return f"{uri}/"
    return uri


def _clean_path_part(value: str, *, label: str) -> str:
    cleaned = value.strip()
    if not cleaned:
        raise DatasetArtifactError(f"{label} must not be empty")
    if cleaned in {".", ".."} or not _SAFE_PATH_PART.fullmatch(cleaned):
        raise DatasetArtifactError(
            f"{label} must contain only letters, numbers, '.', '_', or '-'"
        )
    return cleaned


def _clean_suffix(value: str) -> str:
    cleaned = value.strip().lstrip(".")
    _clean_path_part(cleaned, label="suffix")
    return cleaned


def _clean_root_prefix(root_prefix: str) -> str:
    normalized_root = root_prefix.strip().rstrip("/")
    if not normalized_root:
        raise DatasetArtifactError("root_prefix must not be empty")
    try:
        bucket_name, prefix = parse_gcs_uri(normalized_root)
    except ValueError as exc:
        raise DatasetArtifactError("root_prefix must be a gs:// URI") from exc
    if not bucket_name.strip():
        raise DatasetArtifactError("root_prefix bucket must not be empty")
    for part in prefix.split("/"):
        if part:
            _clean_path_part(part, label="root_prefix path segment")
    return normalized_root


def _prefix_with_trailing_slash(prefix: str) -> str:
    if not prefix:
        return ""
    return f"{prefix.rstrip('/')}/"


def _is_precondition_failure(exc: Exception) -> bool:
    if PreconditionFailed is not None and isinstance(exc, PreconditionFailed):
        return True
    if "Precondition" in exc.__class__.__name__:
        return True
    code: Any = getattr(exc, "code", None)
    return str(code) == "412"
