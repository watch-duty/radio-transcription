"""Build latest correction overlay JSONL from reviewed transcript rows."""

from __future__ import annotations

import argparse
import json
import pathlib
import sys
import tempfile

from common import correction_overlay, gcs_utils


def main(argv: list[str] | None = None) -> int:
    """Run the Phase 5 correction overlay builder CLI.

    Args:
        argv: Optional command-line argument list. Defaults to `sys.argv[1:]`.

    Returns:
        Integer process exit code.
    """
    parser = _build_parser()
    args = parser.parse_args(argv)
    reviewed_rows = _load_jsonl(args.reviewed_jsonl)
    result = correction_overlay.build_latest_overlay(reviewed_rows)
    if result.error_rows:
        _write_json(args.summary_json, result.summary)
        return 1

    _write_jsonl(args.overlay_jsonl, result.overlay_rows)
    _write_json(args.summary_json, result.summary)
    return 0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build latest transcript correction overlay artifacts.",
    )
    parser.add_argument(
        "--reviewed-jsonl",
        required=True,
        help="Input Phase 4 reviewed transcript fact JSONL path.",
    )
    parser.add_argument(
        "--overlay-jsonl",
        required=True,
        help="Output latest-only correction overlay JSONL path.",
    )
    parser.add_argument(
        "--summary-json",
        required=True,
        help="Output compact correction overlay summary JSON path.",
    )
    return parser


def _load_jsonl(path: str) -> list[dict[str, object]]:
    if _is_gcs_uri(path):
        storage_client = _new_storage_client()
        return gcs_utils.download_jsonl_manifest(storage_client, path)

    rows = []
    for raw_line in pathlib.Path(path).read_text(encoding="utf-8").splitlines():
        if not raw_line.strip():
            continue
        row = json.loads(raw_line)
        if not isinstance(row, dict):
            raise TypeError("reviewed JSONL rows must be objects")
        rows.append(row)
    return rows


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


def _write_json(path: str, value: dict[str, object]) -> None:
    if _is_gcs_uri(path):
        storage_client = _new_storage_client()
        with tempfile.NamedTemporaryFile(
            "w",
            encoding="utf-8",
        ) as temp_file:
            _write_json_to_local(pathlib.Path(temp_file.name), value)
            temp_file.flush()
            _upload_file(storage_client, path, temp_file.name)
        return

    local_path = pathlib.Path(path)
    local_path.parent.mkdir(parents=True, exist_ok=True)
    _write_json_to_local(local_path, value)


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


def _write_json_to_local(
    local_path: pathlib.Path,
    value: dict[str, object],
) -> None:
    local_path.write_text(
        json.dumps(
            value,
            ensure_ascii=True,
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )


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
