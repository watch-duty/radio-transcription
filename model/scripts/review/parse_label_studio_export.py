"""Parse raw Label Studio export JSON into reviewed JSONL artifacts."""

from __future__ import annotations

import argparse
import json
import pathlib
import sys
import tempfile

from common import gcs_utils, label_studio_export


def main(argv: list[str] | None = None) -> int:
    """Run the Phase 4 Label Studio export parser CLI.

    Args:
        argv: Optional command-line argument list. Defaults to `sys.argv[1:]`.

    Returns:
        Integer process exit code.
    """
    parser = _build_parser()
    args = parser.parse_args(argv)
    tasks = _load_json_export(args.label_studio_export_json)
    result = label_studio_export.parse_export_tasks(tasks)
    if result.error_rows:
        _write_jsonl(args.reviewed_jsonl, [])
        _write_jsonl(args.errors_jsonl, result.error_rows)
        return 1

    _write_jsonl(args.reviewed_jsonl, result.reviewed_rows)
    _write_jsonl(args.errors_jsonl, [])
    return 0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Parse Label Studio transcript review exports.",
    )
    parser.add_argument(
        "--label-studio-export-json",
        required=True,
        help="Raw Label Studio JSON export path.",
    )
    parser.add_argument(
        "--reviewed-jsonl",
        required=True,
        type=_gcs_uri,
        help="Output reviewed transcript fact JSONL path.",
    )
    parser.add_argument(
        "--errors-jsonl",
        required=True,
        type=_gcs_uri,
        help="Output structured parse error JSONL path.",
    )
    return parser


def _load_json_export(path: str) -> list[dict[str, object]]:
    if _is_gcs_uri(path):
        storage_client = _new_storage_client()
        text = _download_gcs_text(storage_client, path)
    else:
        text = pathlib.Path(path).read_text(encoding="utf-8")

    tasks = json.loads(text)
    if not isinstance(tasks, list):
        raise TypeError("Label Studio export JSON must be an array")
    return tasks


def _download_gcs_text(storage_client: object, gcs_uri: str) -> str:
    bucket_name, blob_path = gcs_utils.parse_gcs_uri(gcs_uri)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    return blob.download_as_text()


def _write_jsonl(path: str, rows: list[dict[str, object]]) -> None:
    storage_client = _new_storage_client()
    with tempfile.NamedTemporaryFile(
        "w",
        encoding="utf-8",
    ) as temp_file:
        _write_jsonl_to_local(pathlib.Path(temp_file.name), rows)
        temp_file.flush()
        _upload_file(storage_client, path, temp_file.name)


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


def _gcs_uri(value: str) -> str:
    if not _is_gcs_uri(value):
        raise argparse.ArgumentTypeError("must be a gs:// URI")
    return value


if __name__ == "__main__":
    sys.exit(main())
