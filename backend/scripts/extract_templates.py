#!/usr/bin/env python3
# ruff: noqa: T201
"""Extract authentic feed metadata from sample CSV files into JSON resource.

Reads directly from sample_feed_properties.csv and sample_feeds.csv, outputting
to production_feed_templates.json. Filters tags to only include allowed keys
('region', 'system/timezone', 'stereo').
"""

import argparse
import csv
import json
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_PROPERTIES_CSV_PATH = (
    SCRIPT_DIR / "test_data" / "sample_feed_properties.csv"
)
DEFAULT_FEEDS_CSV_PATH = SCRIPT_DIR / "test_data" / "sample_feeds.csv"
DEFAULT_OUTPUT_JSON_PATH = (
    SCRIPT_DIR / "test_data" / "production_feed_templates.json"
)

ALLOWED_TAG_KEYS = {"region", "system/timezone", "stereo"}


def filter_tags(tags_json_str: str) -> str:
    """Filter tag objects to retain only allowed tag keys."""
    if not tags_json_str:
        return "[]"
    try:
        tags = json.loads(tags_json_str)
        if not isinstance(tags, list):
            return "[]"
        filtered = [t for t in tags if t.get("key") in ALLOWED_TAG_KEYS]
        return json.dumps(filtered)
    except json.JSONDecodeError:
        return "[]"


def load_feeds(feeds_csv_path: Path) -> dict[str, str]:
    """Load feed ID to name mapping from feeds CSV file."""
    id_to_name: dict[str, str] = {}
    with feeds_csv_path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            feed_id = row.get("id", "").strip()
            name = row.get("name", "").strip()
            if feed_id and name:
                id_to_name[feed_id] = name
    return id_to_name


def extract_templates(
    properties_csv_path: Path,
    feeds_csv_path: Path,
    output_json_path: Path,
) -> list[dict[str, str]]:
    """Extract feed templates joining feed_properties with feed names."""
    id_to_name = load_feeds(feeds_csv_path)
    templates: list[dict[str, str]] = []

    with properties_csv_path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            feed_id = row.get("feed_id", "").strip()
            source_feed_id = row.get("source_feed_id", "").strip()
            source_type = row.get("source_type", "").strip()
            raw_tags = row.get("tags", "").strip()
            filtered_tags_json = filter_tags(raw_tags)

            name = id_to_name.get(feed_id, f"Feed {source_feed_id}")
            if feed_id and source_type:
                templates.append(
                    {
                        "name": name,
                        "source_feed_id": source_feed_id,
                        "source_type": source_type,
                        "tags": filtered_tags_json,
                    }
                )

    output_json_path.parent.mkdir(parents=True, exist_ok=True)
    with output_json_path.open("w", encoding="utf-8") as out:
        json.dump(templates, out, indent=2)

    print(
        f"Extracted {len(templates)} authentic feed templates to"
        f" {output_json_path}"
    )
    return templates


def main(argv: list[str] | None = None) -> int:
    """CLI execution entry point."""
    parser = argparse.ArgumentParser(
        description="Extract feed metadata templates from input CSV files."
    )
    parser.add_argument(
        "--properties-csv",
        type=Path,
        default=DEFAULT_PROPERTIES_CSV_PATH,
        help="Path to sample_feed_properties.csv",
    )
    parser.add_argument(
        "--feeds-csv",
        type=Path,
        default=DEFAULT_FEEDS_CSV_PATH,
        help="Path to sample_feeds.csv",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT_JSON_PATH,
        help="Path to output production_feed_templates.json",
    )
    args = parser.parse_args(argv)
    extract_templates(args.properties_csv, args.feeds_csv, args.output)
    return 0


if __name__ == "__main__":
    sys.exit(main())
