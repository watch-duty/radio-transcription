import json
import logging
import os
import pathlib
import tempfile
import typing

from google.cloud import storage
from google.cloud.storage import retry as storage_retry

from common import manifest

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
        return blob.exists(retry=storage_retry.DEFAULT_RETRY)
    return blob.exists(retry=storage_retry.DEFAULT_RETRY, timeout=timeout)


def gcs_uri_exists(storage_client: storage.Client, uri: str) -> bool:
    """Return whether a GCS object exists."""
    return blob_exists(storage_client, uri)


def gcs_prefix_has_any_blob(
    storage_client: storage.Client, prefix_uri: str
) -> bool:
    """Return whether any object exists under a GCS prefix."""
    bucket_name, blob_prefix = parse_gcs_uri(prefix_uri)
    for _ in storage_client.bucket(bucket_name).list_blobs(
        prefix=blob_prefix, max_results=1
    ):
        return True
    return False


def download_blob_to_file(
    storage_client: storage.Client,
    bucket_name: str,
    blob_path: str,
    destination_file_name: str,
) -> None:
    """Downloads a blob from GCS to a local file with default retry policy."""
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    blob.download_to_filename(
        destination_file_name,
        retry=storage_retry.DEFAULT_RETRY,
    )

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
    blob.upload_from_filename(
        source_file_name,
        retry=storage_retry.DEFAULT_RETRY,
    )
    logger.info(
        f"File {source_file_name} uploaded to gs://{bucket_name}/{blob_path}."
    )


def download_gcs_uri(
    storage_client: storage.Client,
    uri: str,
    local_path: pathlib.Path,
) -> None:
    """Download a GCS object to a local path."""
    bucket_name, blob_path = parse_gcs_uri(uri)
    local_path.parent.mkdir(parents=True, exist_ok=True)
    download_blob_to_file(
        storage_client, bucket_name, blob_path, str(local_path)
    )


def download_gcs_directory(
    storage_client: storage.Client,
    gcs_uri: str,
    local_dir: str | pathlib.Path,
) -> None:
    """Downloads all blobs under a GCS prefix (directory) to a local directory.

    Args:
        storage_client: Initialized GCS client.
        gcs_uri: GCS prefix URI (e.g. gs://bucket/path/to/dir or gs://bucket/path/to/dir/).
        local_dir: The local directory to download files into.

    Raises:
        FileNotFoundError: If no files are found under the specified GCS URI.
    """
    bucket_name, blob_prefix = parse_gcs_uri(gcs_uri)
    # Strip trailing slash for consistent prefix matching.
    blob_prefix = blob_prefix.rstrip("/")
    bucket = storage_client.bucket(bucket_name)
    blobs = list(bucket.list_blobs(prefix=blob_prefix + "/"))
    if not blobs:
        msg = f"No files found under {gcs_uri}"
        raise FileNotFoundError(msg)

    pathlib.Path(local_dir).mkdir(parents=True, exist_ok=True)
    for blob in blobs:
        # Compute relative path within the directory.
        rel_path = blob.name[len(blob_prefix) :].lstrip("/")
        if not rel_path:
            continue
        local_file = pathlib.Path(local_dir) / rel_path
        local_file.parent.mkdir(parents=True, exist_ok=True)
        blob.download_to_filename(
            str(local_file),
            retry=storage_retry.DEFAULT_RETRY,
        )
        logger.info(
            f"Downloaded gs://{bucket_name}/{blob.name} to {local_file}"
        )


def upload_local_file(
    storage_client: storage.Client,
    local_path: pathlib.Path,
    gcs_uri: str,
) -> None:
    """Upload a local file to a GCS object."""
    bucket_name, blob_path = parse_gcs_uri(gcs_uri)
    upload_file_to_blob(storage_client, bucket_name, blob_path, str(local_path))


def upload_text(
    storage_client: storage.Client,
    text: str,
    gcs_uri: str,
    *,
    content_type: str = "text/plain",
) -> None:
    """Upload text directly to a GCS object."""
    bucket_name, blob_path = parse_gcs_uri(gcs_uri)
    storage_client.bucket(bucket_name).blob(blob_path).upload_from_string(
        text,
        content_type=content_type,
        retry=storage_retry.DEFAULT_RETRY,
    )


def download_json_text(
    storage_client: storage.Client, gcs_uri: str
) -> dict[str, typing.Any]:
    """Download a JSON object from GCS."""
    bucket_name, blob_path = parse_gcs_uri(gcs_uri)
    obj = json.loads(
        storage_client.bucket(bucket_name)
        .blob(blob_path)
        .download_as_text(retry=storage_retry.DEFAULT_RETRY)
    )
    if not isinstance(obj, dict):
        msg = f"Expected JSON object at {gcs_uri}"
        raise TypeError(msg)
    return obj


def download_jsonl_manifest(
    storage_client: storage.Client, gcs_manifest_uri: str
) -> list[dict[str, typing.Any]]:
    """Downloads and parses a JSONL manifest from GCS."""
    bucket_name, blob_path = parse_gcs_uri(gcs_manifest_uri)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    content = blob.download_as_text(retry=storage_retry.DEFAULT_RETRY)
    manifest_entries = manifest.parse_manifest_text(
        content,
        source=gcs_manifest_uri,
    )
    logger.info(
        f"Downloaded {len(manifest_entries)} entries from {gcs_manifest_uri}"
    )
    return manifest_entries


def download_jsonl_manifest_strict(
    storage_client: storage.Client,
    gcs_manifest_uri: str,
) -> list[dict[str, typing.Any]]:
    """Download and parse a GCS manifest without skipping invalid rows."""
    bucket_name, blob_path = parse_gcs_uri(gcs_manifest_uri)
    content = (
        storage_client.bucket(bucket_name)
        .blob(blob_path)
        .download_as_text(retry=storage_retry.DEFAULT_RETRY)
    )
    manifest_entries = manifest.parse_manifest_text_strict(
        content,
        source=gcs_manifest_uri,
    )
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
    results_list: list[dict[str, typing.Any]],
) -> str:
    """Upload legacy Colab inference results to GCS.

    Deprecated for new pipelines: use ``common.inference_manifest`` so output
    paths and ``pred_text_*`` semantics match the shared scorer contract.
    """
    blob_path = f"inference_manifests/{project_name}/{model_name}/{experiment_name}_results.jsonl"

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_path)

    # Convert list of dicts to JSONL string in memory
    jsonl_content = "\n".join(json.dumps(row) for row in results_list) + "\n"

    blob.upload_from_string(
        jsonl_content,
        content_type="application/jsonl",
        retry=storage_retry.DEFAULT_RETRY,
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
    suffix = pathlib.Path(blob_path).suffix or ".audio"
    fd, local_path = tempfile.mkstemp(dir=scratch_dir, suffix=suffix)
    os.close(fd)
    download_blob_to_file(storage_client, bucket_name, blob_path, local_path)
    return local_path
