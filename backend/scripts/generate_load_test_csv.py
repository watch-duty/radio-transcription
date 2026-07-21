#!/usr/bin/env python3
# ruff: noqa: T201
"""Generate a CSV dataset for radio transcription pipeline load testing.

Uses authentic production feed templates extracted from feed_properties and
feeds tables so every feed retains its exact real source_feed_id, source_type,
and associated tags.
"""

import argparse
import csv
import json
import sys
from pathlib import Path
from typing import Any, NamedTuple

from backend.scripts.feed_catalog import (
    load_broadcastify_all_feeds_csv,
    load_echo_all_streams_csv,
)

SCRIPT_DIR = Path(__file__).resolve().parent
TEMPLATES_JSON_PATH = SCRIPT_DIR / "test_data" / "feed_templates.json"
DEFAULT_OUTPUT_PATH = SCRIPT_DIR / "test_data" / "load_test_feeds_15k.csv"


def sanitize_source_feed_id(source_type: str, source_feed_id: str) -> str:
    """Sanitize source_feed_id to ensure Pydantic regex compliance."""
    if source_type == "fire_notifications" and "dir=" in source_feed_id:
        param = source_feed_id.split("dir=", 1)[1]
        param = param.split("&", 1)[0]
        return param.lstrip("/")
    return source_feed_id.strip()


class FeedSpec(NamedTuple):
    """Specification of a feed row to write to CSV."""

    name: str
    source_type: str
    source_feed_id: str
    tags: str


def calculate_mix_counts(
    total: int, max_echo: int | None = None
) -> dict[str, int]:
    """Calculate feed count distribution based on implementation plan mix.

    Mix:
      25% echo (capped at max_echo unique feeds)
      25% bcfy_feeds
      50% + echo shortfall for fire_notifications
      0% openmhz
    """
    target_echo = int(total * 0.25)
    bcfy_count = int(total * 0.25)
    if max_echo is not None and target_echo > max_echo:
        echo_count = max_echo
    else:
        echo_count = target_echo

    fn_count = total - echo_count - bcfy_count
    return {
        "fire_notifications": fn_count,
        "echo": echo_count,
        "bcfy_feeds": bcfy_count,
    }


def load_authentic_templates(
    echo_bucket: str,
    gcp_project: str,
) -> dict[str, list[dict[str, Any]]]:
    """Load authentic templates and unique Broadcastify/Echo catalogs."""
    categorized: dict[str, list[dict[str, Any]]] = {
        "fire_notifications": [],
        "echo": [],
        "bcfy_feeds": [],
    }

    # Load 5,458 unique Broadcastify live feeds from catalog CSV
    bcfy_specs = load_broadcastify_all_feeds_csv()
    for spec in bcfy_specs:
        categorized["bcfy_feeds"].append(
            {
                "name": spec.name,
                "source_type": spec.source_type,
                "source_feed_id": spec.source_feed_id,
                "tags": spec.tags,
            }
        )

    # Load Echo directory streams from GCS bucket
    echo_specs = load_echo_all_streams_csv(
        echo_bucket=echo_bucket, gcp_project=gcp_project
    )
    for spec in echo_specs:
        categorized["echo"].append(
            {
                "name": spec.name,
                "source_type": spec.source_type,
                "source_feed_id": spec.source_feed_id,
                "tags": spec.tags,
            }
        )

    # Load fire_notifications templates from JSON resource file
    if TEMPLATES_JSON_PATH.exists():
        with TEMPLATES_JSON_PATH.open("r", encoding="utf-8") as f:
            data: list[dict[str, Any]] = json.load(f)

        for item in data:
            st = item["source_type"]
            if st == "fire_notifications":
                sanitized_item = dict(item)
                sanitized_item["source_feed_id"] = sanitize_source_feed_id(
                    st, item["source_feed_id"]
                )
                categorized[st].append(sanitized_item)

    return categorized


def generate_csv(
    output_path: Path,
    total: int,
    prefix: str,
    echo_bucket: str,
    gcp_project: str,
) -> int:
    """Generate CSV using authentic production feed templates and exact tags."""
    categorized = load_authentic_templates(
        echo_bucket=echo_bucket, gcp_project=gcp_project
    )
    max_echo = len(categorized.get("echo", []))
    counts = calculate_mix_counts(total, max_echo=max_echo)
    rows: list[FeedSpec] = []

    for source_type, target_count in counts.items():
        templates = categorized.get(source_type, [])
        if not templates:
            print(
                f"Error: No templates found for {source_type}",
                file=sys.stderr,
            )
            return 1

        template_counts: dict[str, int] = {}
        for i in range(target_count):
            tpl = templates[i % len(templates)]
            source_feed_id = tpl["source_feed_id"]
            template_counts[source_feed_id] = (
                template_counts.get(source_feed_id, 0) + 1
            )
            instance_num = template_counts[source_feed_id]

            name = f"{prefix}{tpl['name']}-{source_type}-#{instance_num}"

            rows.append(
                FeedSpec(
                    name=name,
                    source_type=source_type,
                    source_feed_id=source_feed_id,
                    tags=tpl["tags"],
                )
            )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
            ["source_feed_id", "display_name", "source_type", "tags"]
        )
        for spec in rows:
            writer.writerow(
                [spec.source_feed_id, spec.name, spec.source_type, spec.tags]
            )

    print(
        f"Generated {len(rows)} load test feed specifications in {output_path}:"
    )
    for st, count in counts.items():
        print(f"  {st}: {count}")
    return 0


def main(argv: list[str] | None = None) -> int:
    """CLI main entry point."""
    parser = argparse.ArgumentParser(
        description="Generate load test feeds CSV from authentic metadata."
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT_PATH,
        help="Path for output CSV file.",
    )
    parser.add_argument(
        "--total",
        type=int,
        default=15000,
        help="Total feed volume to generate.",
    )
    parser.add_argument(
        "--prefix",
        default="loadtest-",
        help="Feed display name prefix.",
    )
    parser.add_argument(
        "--echo-bucket",
        default="gs://wd-echo-recordings-dev",
        help="GCS bucket URL or name for Echo feed streams.",
    )
    parser.add_argument(
        "--gcp-project",
        default="probable-symbol-492218-i7",
        help="GCP project ID owning the Echo GCS bucket.",
    )
    args = parser.parse_args(argv)
    return generate_csv(
        args.output,
        args.total,
        args.prefix,
        echo_bucket=args.echo_bucket,
        gcp_project=args.gcp_project,
    )


if __name__ == "__main__":
    sys.exit(main())
