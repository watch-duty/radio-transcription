#! python
# ruff: noqa: T201
import argparse
import csv
import json
import sys
from typing import Any, TypedDict
from urllib.parse import urlparse

import requests


class Entry(TypedDict):
    url: str
    display_name: str
    tags: dict[str, str]


def parse_tags(columns: list[str]) -> dict[str, str]:
    tags: dict[str, str] = {}
    for col in columns:
        col_clean = col.strip()
        if not col_clean:
            continue
        if "=" not in col_clean:
            print(
                f"Warning: skipping malformed tag (expected key=value): {col_clean!r}",
                file=sys.stderr,
            )
            continue
        key, _, value = col_clean.partition("=")
        tags[key.strip()] = value.strip()
    return tags


def validate_url(url: str) -> bool:
    try:
        result = urlparse(url)
        return result.scheme in ("http", "https") and bool(result.netloc)
    except Exception:
        return False


def load_csv(path: str) -> list[Entry]:
    rows: list[Entry] = []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        for line_num, row in enumerate(reader, start=1):
            if not row or all(cell.strip() == "" for cell in row):
                continue

            if len(row) < 2:
                print(
                    f"Error: line {line_num}: expected at least 2 columns, got {len(row)}",
                    file=sys.stderr,
                )
                sys.exit(1)

            url, display_name, *tag_columns = row
            url = url.strip()
            display_name = display_name.strip()

            if not validate_url(url):
                print(
                    f"Error: line {line_num}: invalid URL: {url!r}",
                    file=sys.stderr,
                )
                sys.exit(1)

            if not display_name:
                print(
                    f"Error: line {line_num}: display name is empty",
                    file=sys.stderr,
                )
                sys.exit(1)

            tags = parse_tags(tag_columns)
            rows.append(Entry(url=url, display_name=display_name, tags=tags))

    return rows


def parse_feed_metadata_payload(entry: Entry) -> dict[str, Any]:
    url = entry["url"]
    if "www.broadcastify.com/listen/feed/" in url:
        _, _, feed_id = url.partition("/feed/")
        return {
            "sourceType": "bcfy_feeds",
            "sourceFeedId": str(feed_id),
        }
    if "www.broadcastify.com/webPlayer/" in url:
        _, _, feed_id = url.partition("/webPlayer/")
        return {
            "sourceType": "bcfy_feeds",
            "sourceFeedId": str(feed_id),
        }
    if "www.broadcastify.com/webPlayerAdv/" in url:
        _, _, feed_id = url.partition("/webPlayerAdv/")
        return {
            "sourceType": "bcfy_feeds",
            "sourceFeedId": str(feed_id),
        }
    if "www.broadcastify.com/calls/tg/" in url:
        _, _, tg_info = url.partition("/tg/")
        return {
            "sourceType": "bcfy_calls",
            "sourceFeedId": tg_info.replace(
                "/", "-"
            ),  # expected format: 1234-12
        }
    if (
        "https://audioplay.textmefires.info/audioplay/folder_play.html?dir="
        in url
    ):
        _, _, feed_id = url.partition("dir=")
        return {
            "sourceType": "fn",
            "sourceFeedId": feed_id,
        }
    if (
        "https://storage.googleapis.com/wd-echo-recordings-prod/index.html#"
        in url
    ):
        _, _, path_part = url.partition("index.html#")
        feed_id = path_part.split("/").pop(0)
        return {
            "sourceType": "echo",
            "sourceFeedId": feed_id,
        }

    raise ValueError("Unknown URL format: " + url)


def parse_payload(entry: Entry) -> dict[str, Any]:
    return {
        **parse_feed_metadata_payload(entry),
        "name": entry["display_name"],
        "tags": [
            {"key": key, "value": value} for key, value in entry["tags"].items()
        ],
    }


def save_feed(entry: Entry, server: str, token: str) -> None:
    try:
        payload = parse_payload(entry)
    except ValueError as e:
        print(f"NOTE: Skipping feed: {entry['url']}: {e}")
        return

    if payload["sourceType"] == "fn":
        print(f"NOTE: FN feed skipped: {payload['sourceFeedId']}")
        return

    try:
        response = requests.post(
            f"{server}/api/v1/feeds",
            json=payload,
            headers={"Authorization": f"Bearer {token}"},
            timeout=10,
        )
        response.raise_for_status()
    except Exception as e:
        print(f"ERROR: {e.args}")
        print("Payload: ")
        print(json.dumps(payload, indent=2))
        raise


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Bulk import feeds from a CSV file"
    )
    parser.add_argument(
        "--token", required=True, help="JWT authentication token"
    )
    parser.add_argument("--server", required=True, help="API server base URL")
    parser.add_argument(
        "csv_file",
        help="Path to CSV file (columns: url, display_name, [key=value ...])",
    )
    args = parser.parse_args()

    if not validate_url(args.server):
        print(f"Error: invalid server URL: {args.server!r}", file=sys.stderr)
        sys.exit(1)

    rows = load_csv(args.csv_file)

    if not rows:
        print("No rows found in CSV file.", file=sys.stderr)
        sys.exit(1)

    print(f"Loaded {len(rows)} feed(s) from {args.csv_file}")
    for row in rows:
        print(
            f"  {row['url']!r}  name={row['display_name']!r}  tags={row['tags']}"
        )
        save_feed(row, args.server.rstrip("/"), args.token)


if __name__ == "__main__":
    main()
