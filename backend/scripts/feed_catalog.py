# ruff: noqa: T201
"""Broadcastify Feed Catalog Loader.

Loads authentic unique Broadcastify live feeds from all_bcfy_feeds_202601.csv.
"""

import csv
import subprocess
import sys
from pathlib import Path
from typing import NamedTuple

PROJECT_ROOT = Path(__file__).resolve().parents[2]
BCFY_ALL_FEEDS_CSV_PATH = (
    PROJECT_ROOT / "model/data_sources/broadcastify/all_bcfy_feeds_202601.csv"
)


class RawFeedSpec(NamedTuple):
    """Raw template feed specification."""

    name: str
    source_type: str
    source_feed_id: str
    tags: str


def load_broadcastify_all_feeds_csv(
    csv_path: Path | None = None,
) -> list[RawFeedSpec]:
    """Load authentic unique Broadcastify feeds from catalog CSV."""
    path = csv_path or BCFY_ALL_FEEDS_CSV_PATH
    if not path.exists():
        print(
            f"Warning: Broadcastify feeds file not found at {path}",
            file=sys.stderr,
        )
        return []

    specs: list[RawFeedSpec] = []
    with path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            feed_id = str(row.get("feedId", "")).strip()
            descr = str(row.get("descr", "")).strip()
            if feed_id and descr:
                specs.append(
                    RawFeedSpec(
                        name=descr,
                        source_type="bcfy_feeds",
                        source_feed_id=feed_id,
                        tags="[]",
                    )
                )

    return specs


def load_echo_all_streams_csv(
    echo_bucket: str,
    gcp_project: str,
) -> list[RawFeedSpec]:
    """Load authentic Echo directory stream names from GCS bucket."""
    bucket_url = (
        echo_bucket
        if echo_bucket.startswith("gs://")
        else f"gs://{echo_bucket}"
    )
    bucket_name = bucket_url.rstrip("/").split("/")[-1]
    dirs: list[str] = []

    # Attempt to list directory prefixes via gcloud CLI
    try:
        cmd = [
            "gcloud",
            "storage",
            "ls",
            "--project",
            gcp_project,
            f"{bucket_url}/",
        ]
        output = subprocess.check_output(
            cmd, text=True, stderr=subprocess.DEVNULL
        )
        for raw_line in output.splitlines():
            line = raw_line.strip()
            if line.startswith("gs://"):
                folder = line.rstrip("/").split("/")[-1]
                if folder and folder != bucket_name:
                    dirs.append(folder)
    except (subprocess.SubprocessError, OSError):
        pass

    dirs = sorted(set(dirs))
    return [
        RawFeedSpec(
            name=folder,
            source_type="echo",
            source_feed_id=folder,
            tags="[]",
        )
        for folder in dirs
    ]
