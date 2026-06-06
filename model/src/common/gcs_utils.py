import json
import logging
import os
import tempfile
from pathlib import Path
from typing import Any

from google.cloud import storage
from google.cloud.storage.retry import DEFAULT_RETRY

logger = logging.getLogger(__name__)


def parse_gcs_uri(gcs_uri: str) -> tuple[str, str]:
    """Parses a GCS URI into bucket name and blob path."""
    if not gcs_uri.startswith("gs://"):
        msg = "GCS URI must start with 'gs://'"
        raise ValueError(msg)
    parts = gcs_uri[len("gs://") :].split("/", 1)
    bucket_name = parts[0]
    blob_path = parts[1] if len(parts) > 1 else ""
    return bucket_name, blob_path


def blob_exists(
    storage_client: storage.Client,
    gcs_uri: str,
    *,
    timeout: float | tuple[float, float] | None = None,
) -> bool:
    """Return True if the GCS object at gcs_uri exists and is reachable.

    Args:
        storage_client: Initialized GCS storage client.
        gcs_uri: A gs:// URI to a single object.
        timeout: Optional request timeout, in seconds, passed to GCS.

    Returns:
        True if the object exists, False otherwise.
    """
    bucket_name, blob_path = parse_gcs_uri(gcs_uri)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    if timeout is None:
        return blob.exists(retry=DEFAULT_RETRY)
    return blob.exists(retry=DEFAULT_RETRY, timeout=timeout)


def download_blob_to_file(
    storage_client: storage.Client,
    bucket_name: str,
    blob_path: str,
    destination_file_name: str,
) -> None:
    """Downloads a blob from GCS to a local file with default retry policy."""
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    blob.download_to_filename(destination_file_name, retry=DEFAULT_RETRY)

    logger.info(f"Downloaded {blob_path} to {destination_file_name}")


def upload_file_to_blob(
    storage_client: storage.Client,
    bucket_name: str,
    blob_path: str,
    source_file_name: str,
) -> None:
    """Uploads a local file to a GCS blob."""
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    blob.upload_from_filename(source_file_name, retry=DEFAULT_RETRY)
    logger.info(
        f"File {source_file_name} uploaded to gs://{bucket_name}/{blob_path}."
    )


def download_jsonl_manifest(
    storage_client: storage.Client, gcs_manifest_uri: str
) -> list[dict[str, Any]]:
    """Downloads and parses a JSONL manifest from GCS."""
    bucket_name, blob_path = parse_gcs_uri(gcs_manifest_uri)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    content = blob.download_as_text(retry=DEFAULT_RETRY).strip()

    manifest_entries = []
    for line in content.split("\n"):
        if line.strip():
            manifest_entries.append(json.loads(line))
    logger.info(
        f"Downloaded {len(manifest_entries)} entries from {gcs_manifest_uri}"
    )
    return manifest_entries


def upload_inference_results(
    storage_client: storage.Client,
    bucket_name: str,
    project_name: str,
    model_name: str,
    experiment_name: str,
    results_list: list[dict[str, Any]],
) -> str:
    """
    Uploads inference results directly from memory to GCS using the standard path structure.
    """
    blob_path = f"inference_manifests/{project_name}/{model_name}/{experiment_name}_results.jsonl"

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_path)

    # Convert list of dicts to JSONL string in memory
    jsonl_content = "\n".join(json.dumps(row) for row in results_list) + "\n"

    blob.upload_from_string(
        jsonl_content, content_type="application/jsonl", retry=DEFAULT_RETRY
    )

    logger.info(f"Uploaded results to gs://{bucket_name}/{blob_path}")
    return f"gs://{bucket_name}/{blob_path}"


def download_to_scratch(
    storage_client: storage.Client, gcs_uri: str, scratch_dir: str
) -> str:
    """Download a GCS object into ``scratch_dir`` under a fresh, unique filename.

    The local filename is allocated with :func:`tempfile.mkstemp`, so two source
    objects that share a basename in different GCS folders can never collide on
    one local path. The original extension is preserved for downstream
    audio-format detection.

    The caller owns ``scratch_dir``'s lifetime — pass a directory managed by a
    :class:`tempfile.TemporaryDirectory` so the file is removed automatically,
    including on a mid-run exception.

    Args:
        storage_client: Initialized GCS client.
        gcs_uri: A ``gs://`` URI to a single object.
        scratch_dir: An existing directory to download into.

    Returns:
        Absolute local path of the downloaded file.

    Raises:
        ValueError: If ``gcs_uri`` does not start with ``gs://``.
    """
    bucket_name, blob_path = parse_gcs_uri(gcs_uri)
    suffix = Path(blob_path).suffix or ".audio"
    fd, local_path = tempfile.mkstemp(dir=scratch_dir, suffix=suffix)
    os.close(fd)
    download_blob_to_file(storage_client, bucket_name, blob_path, local_path)
    return local_path
