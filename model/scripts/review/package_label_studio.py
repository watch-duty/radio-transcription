"""Generate Label Studio review package artifacts from ranked rows."""

from __future__ import annotations

import argparse
import csv
import json
import pathlib
import sys
import tempfile

from common import gcs_utils
from common import label_studio_review


DEFAULT_BUCKET_URI = "gs://wd-transcription-data"


def main(argv: list[str] | None = None) -> int:
    """Run the Phase 3 Label Studio package CLI.

    Args:
        argv: Optional command-line argument list. Defaults to `sys.argv[1:]`.

    Returns:
        Integer process exit code.
    """
    parser = _build_parser()
    args = parser.parse_args(argv)
    ranked_rows = _load_jsonl(args.ranked_jsonl)
    package = label_studio_review.build_package(
        ranked_rows,
        limit=args.limit,
        bucket_uri=args.bucket_uri,
    )

    _write_tasks_json(args.tasks_json, package.tasks)
    _write_text(args.label_config_xml, package.label_config_xml)
    _write_text(args.readme_md, package.readme_text)
    _write_preview_csv(args.preview_csv, package.preview_rows)
    return 0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Generate a Label Studio transcript review package.",
    )
    parser.add_argument(
        "--ranked-jsonl",
        required=True,
        help="Phase 2 ranked JSONL input path.",
    )
    parser.add_argument(
        "--tasks-json",
        required=True,
        help="Output Label Studio import task JSON path.",
    )
    parser.add_argument(
        "--label-config-xml",
        required=True,
        help="Output Label Studio labeling config XML path.",
    )
    parser.add_argument(
        "--readme-md",
        required=True,
        help="Output operator README path.",
    )
    parser.add_argument(
        "--preview-csv",
        required=True,
        help="Output human-readable preview CSV path.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=label_studio_review.DEFAULT_LIMIT,
        help="Maximum top-ranked rows to package.",
    )
    parser.add_argument(
        "--bucket-uri",
        default=DEFAULT_BUCKET_URI,
        help="Private GCS bucket URI for README access instructions.",
    )
    return parser


def _load_jsonl(path: str) -> list[dict[str, object]]:
    if _is_gcs_uri(path):
        storage_client = _new_storage_client()
        return gcs_utils.download_jsonl_manifest(storage_client, path)

    rows: list[dict[str, object]] = []
    local_path = pathlib.Path(path)
    with local_path.open(encoding="utf-8") as input_file:
        for raw_line in input_file:
            line = raw_line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def _write_tasks_json(
    path: str,
    tasks: list[dict[str, dict[str, object]]],
) -> None:
    def write(local_path: pathlib.Path) -> None:
        with local_path.open("w", encoding="utf-8") as output_file:
            json.dump(
                tasks,
                output_file,
                ensure_ascii=True,
                indent=2,
                sort_keys=True,
            )
            output_file.write("\n")

    _write_local_or_gcs(path, write)


def _write_text(path: str, text: str) -> None:
    def write(local_path: pathlib.Path) -> None:
        with local_path.open("w", encoding="utf-8") as output_file:
            output_file.write(text)

    _write_local_or_gcs(path, write)


def _write_preview_csv(
    path: str,
    rows: list[dict[str, object]],
) -> None:
    def write(local_path: pathlib.Path) -> None:
        with local_path.open(
            "w",
            encoding="utf-8",
            newline="",
        ) as output_file:
            writer = csv.DictWriter(
                output_file,
                fieldnames=label_studio_review.PREVIEW_CSV_FIELDS,
            )
            writer.writeheader()
            writer.writerows(rows)

    _write_local_or_gcs(path, write)


def _write_local_or_gcs(
    path: str,
    writer: object,
) -> None:
    if _is_gcs_uri(path):
        storage_client = _new_storage_client()
        with tempfile.NamedTemporaryFile() as temp_file:
            temp_path = pathlib.Path(temp_file.name)
            writer(temp_path)
            _upload_file(storage_client, path, temp_file.name)
        return

    local_path = pathlib.Path(path)
    local_path.parent.mkdir(parents=True, exist_ok=True)
    writer(local_path)


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
