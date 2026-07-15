#!/usr/bin/env python3
# ruff: noqa: T201
"""FE Proxy API Load Test Controller for Radio Transcription Pipeline.

Executes controlled ramp-up, listing, phase activations, deactivations, and
deletions strictly via Frontend Proxy API endpoints.
"""

import argparse
import csv
import json
import sys
import time
from typing import Any, NamedTuple

import requests

LOAD_TEST_SOURCE_TYPES = ("fire_notifications", "echo", "bcfy_feeds")


class FeedEntry(NamedTuple):
    """Specification of a feed row read from input CSV."""

    source_feed_id: str
    display_name: str
    source_type: str
    tags: list[dict[str, str]]


def parse_tags_column(col_value: str) -> list[dict[str, str]]:
    """Parse tags column string into list of key/value dictionaries."""
    clean = col_value.strip()
    if not clean:
        return []

    try:
        parsed = json.loads(clean)
        if isinstance(parsed, list):
            res: list[dict[str, str]] = []
            for item in parsed:
                if isinstance(item, dict) and "key" in item and "value" in item:
                    res.append(
                        {"key": str(item["key"]), "value": str(item["value"])}
                    )
            return res
    except json.JSONDecodeError:
        pass

    tags: list[dict[str, str]] = []
    for raw_part in clean.split(","):
        part = raw_part.strip()
        if "=" in part:
            k, _, v = part.partition("=")
            tags.append({"key": k.strip(), "value": v.strip()})
    return tags


def load_feeds_csv(csv_path: str) -> list[FeedEntry]:
    """Load and validate feed rows from CSV file."""
    entries: list[FeedEntry] = []
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        for line_num, row in enumerate(reader, start=1):
            if not row or all(cell.strip() == "" for cell in row):
                continue
            if line_num == 1 and "display_name" in row[1].lower():
                continue
            if len(row) < 3:
                print(
                    f"Warning: line {line_num}: expected at least 3 columns",
                    file=sys.stderr,
                )
                continue

            source_feed_id = row[0].strip()
            display_name = row[1].strip()
            source_type = row[2].strip()
            tag_col = row[3] if len(row) > 3 else ""

            source_type_aliases = {
                "fn": "fire_notifications",
                "fire_notification": "fire_notifications",
                "bcfy": "bcfy_feeds",
                "bcfy_feed": "bcfy_feeds",
            }
            source_type = source_type_aliases.get(source_type, source_type)

            tags = parse_tags_column(tag_col)
            entries.append(
                FeedEntry(
                    source_feed_id=source_feed_id,
                    display_name=display_name,
                    source_type=source_type,
                    tags=tags,
                )
            )
    return entries


class FeedsApiClient:
    """Client for performing HTTP requests against the FE Proxy API."""

    def __init__(self, server_url: str, token: str) -> None:
        self.server_url = server_url.rstrip("/")
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            }
        )

    def create_feed(
        self,
        name: str,
        source_type: str,
        source_feed_id: str,
        tags: list[dict[str, str]],
    ) -> dict[str, Any]:
        """Create a new feed via POST /api/v1/feeds."""
        url = f"{self.server_url}/api/v1/feeds"
        payload = {
            "name": name,
            "sourceType": source_type,
            "sourceFeedId": source_feed_id,
            "tags": tags,
        }
        resp = self.session.post(url, json=payload, timeout=15)
        resp.raise_for_status()
        return resp.json()

    def list_feeds(
        self, name_prefix: str | None = None
    ) -> list[dict[str, Any]]:
        """List feeds from GET /api/v1/feeds using official name parameter."""
        feeds: list[dict[str, Any]] = []
        next_token: str | None = None
        base_url = f"{self.server_url}/api/v1/feeds"

        while True:
            params: dict[str, Any] = {"limit": 200}
            if name_prefix:
                params["name"] = name_prefix
            if next_token:
                params["nextToken"] = next_token

            resp = self.session.get(base_url, params=params, timeout=15)
            resp.raise_for_status()
            data = resp.json()

            if isinstance(data, list):
                page_feeds = data
                next_token = None
            else:
                page_feeds = data.get("feeds", [])
                next_token = data.get("nextToken")

            for feed in page_feeds:
                if not name_prefix or feed.get("name", "").startswith(
                    name_prefix
                ):
                    feeds.append(feed)

            if not next_token:
                break
        return feeds

    def reset_feed(self, feed_id: str) -> dict[str, Any]:
        """Reset/activate a feed via POST /api/v1/feeds/{feedId}/reset."""
        url = f"{self.server_url}/api/v1/feeds/{feed_id}/reset"
        resp = self.session.post(url, timeout=15)
        resp.raise_for_status()
        return resp.json()

    def deactivate_feed(self, feed_id: str) -> None:
        """Deactivate a feed via POST /api/v1/feeds/{feedId}/deactivate."""
        url = f"{self.server_url}/api/v1/feeds/{feed_id}/deactivate"
        resp = self.session.post(url, timeout=15)
        resp.raise_for_status()

    def delete_feed(self, feed_id: str) -> None:
        """Hard delete a feed via DELETE /api/v1/feeds/{feedId}."""
        url = f"{self.server_url}/api/v1/feeds/{feed_id}"
        resp = self.session.delete(url, timeout=15)
        resp.raise_for_status()


def allocate_target_mix(total: int) -> dict[str, int]:
    """Calculate mix counts: 50% FN, 25% Echo, 25% BCFY."""
    fn_count = int(total * 0.50)
    echo_count = int(total * 0.25)
    bcfy_count = total - fn_count - echo_count
    return {
        "fire_notifications": fn_count,
        "echo": echo_count,
        "bcfy_feeds": bcfy_count,
    }


def _group_existing_feeds(
    existing_feeds: list[dict[str, Any]],
) -> tuple[dict[str, list[dict[str, Any]]], dict[str, int]]:
    """Group existing feeds into active lists and total count by type."""
    active_by_type: dict[str, list[dict[str, Any]]] = {
        st: [] for st in LOAD_TEST_SOURCE_TYPES
    }
    total_registered_by_type: dict[str, int] = dict.fromkeys(
        LOAD_TEST_SOURCE_TYPES, 0
    )

    for feed in existing_feeds:
        st = feed.get("sourceType")
        if st not in active_by_type:
            continue

        total_registered_by_type[st] += 1
        status = feed.get("status")
        substatus = feed.get("substatus")
        is_active = status in ("active", "unclaimed") or substatus in (
            "active",
            "unclaimed",
        )
        if is_active:
            active_by_type[st].append(feed)

    return active_by_type, total_registered_by_type


def cmd_activate(args: argparse.Namespace) -> int:
    """Ramp up feed activation on-demand without spec overlap."""
    client = FeedsApiClient(args.server, args.token)
    existing_feeds = client.list_feeds(args.prefix)

    target_counts = allocate_target_mix(args.target_total)
    print(f"Cumulative activation target (Total {args.target_total}):")
    for st, count in sorted(target_counts.items()):
        print(f"  {st}: {count}")

    active_by_type, total_registered_by_type = _group_existing_feeds(
        existing_feeds
    )

    print("Current active/unclaimed feeds in system:")
    for st in LOAD_TEST_SOURCE_TYPES:
        print(f"  {st}: {len(active_by_type[st])}")

    csv_entries = load_feeds_csv(args.csv) if args.csv else []
    csv_by_type: dict[str, list[FeedEntry]] = {
        st: [] for st in LOAD_TEST_SOURCE_TYPES
    }
    for entry in csv_entries:
        if entry.source_type in csv_by_type:
            csv_by_type[entry.source_type].append(entry)

    total_created = 0

    for st in LOAD_TEST_SOURCE_TYPES:
        target = target_counts[st]
        active_list = active_by_type[st]
        total_registered = total_registered_by_type[st]

        needed_active = max(0, target - len(active_list))
        if needed_active == 0:
            print(
                f"[{st}] Active target already met"
                f" ({len(active_list)} active >= {target} target)."
            )
            continue

        start_idx = total_registered
        end_idx = start_idx + needed_active
        new_entries_slice = csv_by_type[st][start_idx:end_idx]

        created_for_st = 0
        total_entries = len(new_entries_slice)
        for idx, entry in enumerate(new_entries_slice, 1):
            try:
                client.create_feed(
                    name=entry.display_name,
                    source_type=entry.source_type,
                    source_feed_id=entry.source_feed_id,
                    tags=entry.tags,
                )
                created_for_st += 1
                total_created += 1
            except Exception as err:
                print(
                    f"Error creating feed {entry.display_name!r}: {err}",
                    file=sys.stderr,
                )

            if idx % 500 == 0 and idx < total_entries:
                print(
                    f"[{st}] Created batch {idx // 500}"
                    f" ({idx}/{total_entries}). Waiting 5s..."
                )
                time.sleep(5)

        print(
            f"[{st}] Registered {created_for_st} new feed(s)"
            f" (specs [{start_idx}:{end_idx}])."
        )

    print(f"Phase execution complete: registered {total_created} new feeds.")
    return 0


def cmd_deactivate(args: argparse.Namespace) -> int:
    """Deactivate all feeds matching prefix in batches of 500 with 5s delay."""
    client = FeedsApiClient(args.server, args.token)
    feeds = client.list_feeds(args.prefix)

    active_feeds = [
        f
        for f in feeds
        if f.get("status") != "inactive" and f.get("substatus") != "deactivated"
    ]

    total_active = len(active_feeds)
    print(
        f"Found {total_active} active/unclaimed feed(s) out of"
        f" {len(feeds)} total."
    )

    deactivated = 0
    for idx, feed in enumerate(active_feeds, 1):
        feed_id = feed["id"]
        try:
            client.deactivate_feed(feed_id)
            deactivated += 1
        except Exception as err:
            print(
                f"Error deactivating feed {feed_id}: {err}",
                file=sys.stderr,
            )

        if idx % 500 == 0 and idx < total_active:
            print(
                f"Deactivated batch {idx // 500} ({idx}/{total_active})."
                " Waiting 5s..."
            )
            time.sleep(5)

    print(f"Deactivated {deactivated} feed(s).")
    return 0


def cmd_delete(args: argparse.Namespace) -> int:
    """Hard delete all feeds matching prefix via FE Proxy API in batches."""
    client = FeedsApiClient(args.server, args.token)
    feeds = client.list_feeds(args.prefix)
    total_feeds = len(feeds)

    print(
        f"Found {total_feeds} feed(s) matching prefix {args.prefix!r}"
        " for deletion."
    )

    if not feeds:
        print("No feeds found to delete.")
        return 0

    # Pass 1: Deactivate all matching feeds in batches of 500
    print(f"Deactivating {total_feeds} feed(s)...")
    deactivated = 0
    for idx, feed in enumerate(feeds, 1):
        feed_id = feed["id"]
        try:
            client.deactivate_feed(feed_id)
            deactivated += 1
        except Exception as err:
            print(
                f"Warning: Deactivating feed {feed_id} failed: {err}",
                file=sys.stderr,
            )

        if idx % 500 == 0 and idx < total_feeds:
            print(
                f"Deactivated batch {idx // 500} ({idx}/{total_feeds})."
                " Waiting 5s..."
            )
            time.sleep(5)

    print(
        f"Deactivated {deactivated}/{total_feeds} feed(s)."
        " Waiting 10 seconds..."
    )
    time.sleep(10)

    # Pass 2: Delete all matching feeds in batches of 500
    print(f"Deleting {total_feeds} feed(s)...")
    deleted = 0
    for idx, feed in enumerate(feeds, 1):
        feed_id = feed["id"]
        try:
            client.delete_feed(feed_id)
            deleted += 1
        except Exception as err:
            print(
                f"Error deleting feed {feed_id}: {err}",
                file=sys.stderr,
            )

        if idx % 500 == 0 and idx < total_feeds:
            print(
                f"Deleted batch {idx // 500} ({idx}/{total_feeds})."
                " Waiting 5s..."
            )
            time.sleep(5)

    print(f"Completed deletion: deleted {deleted}/{total_feeds} feed(s).")
    return 0


def build_parser() -> argparse.ArgumentParser:
    """Construct command-line argument parser."""
    parser = argparse.ArgumentParser(
        description="Radio Transcription Load Test Controller."
    )
    parser.add_argument("--server", required=True, help="FE Proxy API base URL")
    parser.add_argument("--token", required=True, help="Bearer Auth JWT token")
    parser.add_argument(
        "--prefix",
        required=True,
        help="Load test prefix (e.g. loadtest-20260714-)",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    # Activate command
    act_p = subparsers.add_parser(
        "activate", help="Ramp activation up to target total"
    )
    act_p.add_argument(
        "--target-total",
        type=int,
        required=True,
        help="Target total active feeds",
    )
    act_p.add_argument(
        "--csv",
        default="backend/scripts/test_data/load_test_feeds_15k.csv",
        help="Path to feeds CSV for on-demand feed creation",
    )
    act_p.set_defaults(func=cmd_activate)

    # Deactivate command
    deact_p = subparsers.add_parser(
        "deactivate", help="Deactivate active load test feeds"
    )
    deact_p.set_defaults(func=cmd_deactivate)

    # Delete command
    del_p = subparsers.add_parser(
        "delete", help="Hard delete load test feeds matching prefix"
    )
    del_p.set_defaults(func=cmd_delete)

    return parser


def main(argv: list[str] | None = None) -> int:
    """CLI execution entry point."""
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
