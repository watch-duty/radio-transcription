"""Generate Label Studio review package artifacts from ranked rows."""

from __future__ import annotations

import argparse
import collections.abc
import csv
import json
import pathlib
import sys
import tempfile

from common import gcs_utils, label_studio_review

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
    reviewed_audio_segment_ids = _load_reviewed_audio_segment_ids(
        args.correction_overlay_jsonl,
    )
    package = label_studio_review.build_package(
        ranked_rows,
        limit=args.limit,
        bucket_uri=args.bucket_uri,
        reviewed_audio_segment_ids=reviewed_audio_segment_ids,
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
        type=_gcs_uri,
        help="Phase 2 ranked JSONL input path.",
    )
    parser.add_argument(
        "--correction-overlay-jsonl",
        type=_gcs_uri,
        help=(
            "Optional latest correction overlay JSONL. Audio IDs present in "
            "this file are skipped from the package."
        ),
    )
    parser.add_argument(
        "--tasks-json",
        required=True,
        type=_gcs_uri,
        help="Output Label Studio import task JSON path.",
    )
    parser.add_argument(
        "--label-config-xml",
        required=True,
        type=_gcs_uri,
        help="Output Label Studio labeling config XML path.",
    )
    parser.add_argument(
        "--readme-md",
        required=True,
        type=_gcs_uri,
        help="Output operator README path.",
    )
    parser.add_argument(
        "--preview-csv",
        required=True,
        type=_gcs_uri,
        help="Output human-readable preview CSV path.",
    )
    parser.add_argument(
        "--limit",
        required=True,
        type=_positive_int,
        help="Explicit maximum top-ranked unreviewed rows to package.",
    )
    parser.add_argument(
        "--bucket-uri",
        type=_gcs_uri,
        default=DEFAULT_BUCKET_URI,
        help="Private GCS bucket URI for README access instructions.",
    )
    return parser


def _load_jsonl(path: str) -> list[dict[str, object]]:
    storage_client = _new_storage_client()
    return gcs_utils.download_jsonl_manifest(storage_client, path)


def _load_reviewed_audio_segment_ids(path: str | None) -> set[object]:
    if path is None:
        return set()
    return {
        row["audio_segment_id"]
        for row in _load_jsonl(path)
        if "audio_segment_id" in row
    }


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
    writer: collections.abc.Callable[[pathlib.Path], None],
) -> None:
    storage_client = _new_storage_client()
    with tempfile.NamedTemporaryFile() as temp_file:
        temp_path = pathlib.Path(temp_file.name)
        writer(temp_path)
        _upload_file(storage_client, path, temp_file.name)


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


def _positive_int(value: str) -> int:
    parsed_value = int(value)
    if parsed_value <= 0:
        raise argparse.ArgumentTypeError("must be positive")
    return parsed_value


if __name__ == "__main__":
    sys.exit(main())
