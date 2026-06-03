"""Build review-pool JSONL from canonical manifests."""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import pathlib
import sys
import tempfile

from common import gcs_utils, review

DEFAULT_METADATA_WORKERS = 32


def main(argv: list[str] | None = None) -> int:
    """Run the review-pool builder CLI.

    Args:
        argv: Optional command-line argument list. Defaults to `sys.argv[1:]`.

    Returns:
        Integer process exit code.
    """
    parser = _build_parser()
    args = parser.parse_args(argv)
    storage_client = _new_storage_client()
    manifest_rows_by_name = {}
    for index, manifest_uri in enumerate(args.manifest_jsonl, start=1):
        manifest_rows_by_name[f"manifest-{index}"] = _load_jsonl(
            manifest_uri,
            storage_client,
        )
    manifest_rows = review.load_review_manifest_rows(
        manifest_rows_by_name,
    )
    metadata_by_uri = _fetch_metadata_by_uri(
        storage_client,
        manifest_rows,
        max_workers=args.metadata_workers,
    )

    try:
        review_pool_rows = review.build_review_pool(
            manifest_rows,
            metadata_by_uri=metadata_by_uri,
        )
    except review.DuplicateAudioSegmentError as exc:
        _write_jsonl(args.duplicates_jsonl, exc.duplicates)
        return 1

    _write_jsonl(args.review_pool_jsonl, review_pool_rows)
    _write_jsonl(args.duplicates_jsonl, [])
    return 0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build review-pool rows from canonical manifests.",
    )
    parser.add_argument(
        "--manifest-jsonl",
        action="append",
        required=True,
        type=_gcs_uri,
        help="Input canonical manifest JSONL GCS path. Repeatable.",
    )
    parser.add_argument(
        "--review-pool-jsonl",
        required=True,
        type=_gcs_uri,
        help="Output enriched review-pool JSONL path.",
    )
    parser.add_argument(
        "--duplicates-jsonl",
        required=True,
        type=_gcs_uri,
        help="Output duplicate exact-audio report JSONL path.",
    )
    parser.add_argument(
        "--metadata-workers",
        type=_positive_int,
        default=DEFAULT_METADATA_WORKERS,
        help=(
            "Maximum concurrent GCS metadata fetches. "
            f"Defaults to {DEFAULT_METADATA_WORKERS}."
        ),
    )
    return parser


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def _gcs_uri(value: str) -> str:
    if not _is_gcs_uri(value):
        raise argparse.ArgumentTypeError("value must be a gs:// URI")
    return value


def _load_jsonl(
    path: str,
    storage_client: object,
) -> list[dict[str, object]]:
    if _is_gcs_uri(path):
        return gcs_utils.download_jsonl_manifest(storage_client, path)

    rows = []
    with pathlib.Path(path).open(encoding="utf-8") as input_file:
        for raw_line in input_file:
            line = raw_line.strip()
            if not line:
                continue
            row = json.loads(line)
            if not isinstance(row, dict):
                raise TypeError("canonical manifest JSONL rows must be objects")
            rows.append(row)
    return rows


def _fetch_metadata_by_uri(
    storage_client: object,
    manifest_rows: list[review.ReviewManifestRow],
    *,
    max_workers: int,
) -> dict[str, review.GcsObjectMetadata]:
    unique_uris = list(
        dict.fromkeys(row.model_ready_audio_uri for row in manifest_rows)
    )
    if not unique_uris:
        return {}

    def fetch(uri: str) -> tuple[str, review.GcsObjectMetadata]:
        return uri, review.fetch_gcs_object_metadata(storage_client, uri)

    metadata_by_uri = {}
    worker_count = min(max_workers, len(unique_uris))
    if worker_count == 1:
        for uri in unique_uris:
            fetched_uri, metadata = fetch(uri)
            metadata_by_uri[fetched_uri] = metadata
        return metadata_by_uri

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=worker_count,
    ) as executor:
        return dict(executor.map(fetch, unique_uris))


def _write_jsonl(path: str, rows: list[dict[str, object]]) -> None:
    if _is_gcs_uri(path):
        storage_client = _new_storage_client()
        with tempfile.NamedTemporaryFile(
            "w",
            encoding="utf-8",
        ) as temp_file:
            _write_jsonl_to_local(pathlib.Path(temp_file.name), rows)
            temp_file.flush()
            _upload_file(storage_client, path, temp_file.name)
        return

    local_path = pathlib.Path(path)
    local_path.parent.mkdir(parents=True, exist_ok=True)
    _write_jsonl_to_local(local_path, rows)


def _write_jsonl_to_local(
    local_path: pathlib.Path,
    rows: list[dict[str, object]],
) -> None:
    with local_path.open("w", encoding="utf-8") as output_file:
        for row in rows:
            output_file.write(
                json.dumps(row, ensure_ascii=True, sort_keys=True)
            )
            output_file.write("\n")


def _upload_file(storage_client: object, gcs_uri: str, local_path: str) -> None:
    bucket_name, blob_path = gcs_utils.parse_gcs_uri(gcs_uri)
    gcs_utils.upload_file_to_blob(
        storage_client,
        bucket_name,
        blob_path,
        local_path,
    )


def _new_storage_client() -> object:
    from google.cloud import storage

    return storage.Client()


def _is_gcs_uri(path: str) -> bool:
    return path.startswith("gs://")


if __name__ == "__main__":
    sys.exit(main())
