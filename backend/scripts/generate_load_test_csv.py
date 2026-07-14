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

SCRIPT_DIR = Path(__file__).resolve().parent
TEMPLATES_JSON_PATH = (
    SCRIPT_DIR / "test_data" / "production_feed_templates.json"
)
DEFAULT_OUTPUT_PATH = SCRIPT_DIR / "test_data" / "load_test_feeds_15k.csv"


class FeedSpec(NamedTuple):
    """Specification of a feed row to write to CSV."""

    name: str
    source_type: str
    source_feed_id: str
    tags: str


def calculate_mix_counts(total: int) -> dict[str, int]:
    """Calculate feed count distribution based on implementation plan mix.

    Mix:
      50% fire_notifications
      25% echo
      25% bcfy_feeds
      0% openmhz
    """
    fn_count = int(total * 0.50)
    echo_count = int(total * 0.25)
    bcfy_count = total - fn_count - echo_count
    return {
        "fire_notifications": fn_count,
        "echo": echo_count,
        "bcfy_feeds": bcfy_count,
    }


def load_authentic_templates() -> dict[str, list[dict[str, Any]]]:
    """Load authentic production templates from JSON resource file."""
    if not TEMPLATES_JSON_PATH.exists():
        msg = (
            f"Missing template file: {TEMPLATES_JSON_PATH}. "
            "Please run backend/scripts/extract_templates.py first."
        )
        raise FileNotFoundError(msg)

    with TEMPLATES_JSON_PATH.open("r", encoding="utf-8") as f:
        data: list[dict[str, Any]] = json.load(f)

    categorized: dict[str, list[dict[str, Any]]] = {
        "fire_notifications": [],
        "echo": [],
        "bcfy_feeds": [],
    }

    for item in data:
        st = item["source_type"]
        if st in categorized:
            categorized[st].append(item)
        elif st == "bcfy_calls":
            categorized["bcfy_feeds"].append(item)

    return categorized


def generate_csv(output_path: Path, total: int, prefix: str) -> int:
    """Generate CSV using authentic production feed templates and exact tags."""
    categorized = load_authentic_templates()
    counts = calculate_mix_counts(total)
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
            base_id = tpl["source_feed_id"]
            template_counts[base_id] = template_counts.get(base_id, 0) + 1
            instance_num = template_counts[base_id]

            if instance_num == 1:
                source_feed_id = base_id
                name = f"{prefix}{tpl['name']}"
            else:
                source_feed_id = f"{base_id}-lt-{instance_num:04d}"
                name = f"{prefix}{tpl['name']} #{instance_num}"

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
    args = parser.parse_args(argv)
    return generate_csv(args.output, args.total, args.prefix)


if __name__ == "__main__":
    sys.exit(main())
