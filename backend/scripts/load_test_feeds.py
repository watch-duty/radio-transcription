# ruff: noqa: T201
from __future__ import annotations

import argparse
import csv
import json
import os
import subprocess
import sys
import uuid
from collections import defaultdict
from pathlib import Path
from typing import NamedTuple

import psycopg

LOAD_TEST_SOURCE_TYPES = frozenset(
    {"bcfy_feeds", "bcfy_calls", "openmhz", "fire_notifications"},
)
DEFAULT_TAG_COLUMNS = (
    "catalog_id",
    "priority_tier",
    "state",
    "county",
    "agency_name",
    "service_tags",
)
DEFAULT_CATALOG_OUTPUT_DIR = Path("model/data/wildfire_catalog/output")
GENERATED_CATALOG_FILENAME = "wildfire_feed_catalog_admin_review.csv"

_INSERT_LOAD_TEST_FEED_SQL = """
WITH new_feed AS (
    INSERT INTO feeds (id, name, source_type, status, audit_revision)
    VALUES (%s, %s, %s, 'deactivated'::feed_status, 1)
    RETURNING id, name, source_type, status, audit_revision
),
new_props AS (
    INSERT INTO feed_properties (feed_id, source_feed_id, source_type, tags)
    SELECT id, %s, source_type, %s::jsonb FROM new_feed
),
audit AS (
    INSERT INTO feed_audit_events (
        feed_id,
        action,
        actor_id,
        feed_revision,
        before_values,
        after_values
    )
    SELECT
        id,
        'feed.created',
        %s,
        audit_revision,
        '{}'::jsonb,
        jsonb_build_object(
            'name', name,
            'source_type', source_type,
            'status', status
        )
    FROM new_feed
)
SELECT id, name, source_type FROM new_feed
"""

_COUNT_SQL = """
SELECT source_type, status, COUNT(*) AS n
FROM feeds
WHERE name LIKE %s
GROUP BY source_type, status
ORDER BY source_type, status
"""

_CURRENT_ACTIVEISH_SQL = """
SELECT source_type, COUNT(*) AS n
FROM feeds
WHERE name LIKE %s
  AND status <> 'deactivated'::feed_status
GROUP BY source_type
"""

_TOTAL_SEEDED_SQL = """
SELECT source_type, COUNT(*) AS n
FROM feeds
WHERE name LIKE %s
GROUP BY source_type
"""

_ACTIVATE_SQL = """
WITH selected AS (
    SELECT id, name, source_type, status, audit_revision
    FROM feeds
    WHERE name LIKE %s
      AND source_type = %s
      AND status = 'deactivated'::feed_status
    ORDER BY name, id
    LIMIT %s
    FOR UPDATE
),
updated AS (
    UPDATE feeds
    SET status = 'unclaimed'::feed_status,
        worker_id = NULL,
        last_heartbeat = NULL,
        failure_count = 0,
        retry_after = NULL,
        unclaimed_since = NOW(),
        status_reason = NULL,
        status_reason_detail = NULL,
        status_reason_updated_at = CASE
            WHEN feeds.status_reason IS NOT NULL
              OR feeds.status_reason_detail IS NOT NULL
            THEN NOW()
            ELSE feeds.status_reason_updated_at
        END,
        audit_revision = feeds.audit_revision + 1
    FROM selected
    WHERE feeds.id = selected.id
    RETURNING
        feeds.id,
        feeds.name,
        feeds.source_type,
        selected.status AS before_status,
        feeds.status AS after_status,
        feeds.audit_revision
),
audit AS (
    INSERT INTO feed_audit_events (
        feed_id,
        action,
        actor_id,
        feed_revision,
        before_values,
        after_values
    )
    SELECT
        id,
        'feed.reset',
        %s,
        audit_revision,
        jsonb_build_object('status', before_status),
        jsonb_build_object('status', after_status)
    FROM updated
)
SELECT id, name, source_type FROM updated
"""

_DEACTIVATE_SQL = """
WITH selected AS (
    SELECT id, name, source_type, status, audit_revision
    FROM feeds
    WHERE name LIKE %s
      AND status <> 'deactivated'::feed_status
    FOR UPDATE
),
updated AS (
    UPDATE feeds
    SET status = 'deactivated'::feed_status,
        worker_id = NULL,
        last_heartbeat = NULL,
        retry_after = NULL,
        status_reason = NULL,
        status_reason_detail = NULL,
        status_reason_updated_at = CASE
            WHEN feeds.status_reason IS NOT NULL
              OR feeds.status_reason_detail IS NOT NULL
            THEN NOW()
            ELSE feeds.status_reason_updated_at
        END,
        audit_revision = feeds.audit_revision + 1
    FROM selected
    WHERE feeds.id = selected.id
    RETURNING
        feeds.id,
        feeds.name,
        feeds.source_type,
        selected.status AS before_status,
        feeds.status AS after_status,
        feeds.audit_revision
),
audit AS (
    INSERT INTO feed_audit_events (
        feed_id,
        action,
        actor_id,
        feed_revision,
        before_values,
        after_values
    )
    SELECT
        id,
        'feed.deactivated',
        %s,
        audit_revision,
        jsonb_build_object('status', before_status),
        jsonb_build_object('status', after_status)
    FROM updated
)
SELECT id, name, source_type FROM updated
"""


class FeedRow(NamedTuple):
    name: str
    source_type: str
    source_feed_id: str
    tags: list[dict[str, str]]


def validate_prefix(prefix: str) -> None:
    if not prefix.startswith("loadtest-"):
        msg = "prefix must start with 'loadtest-'"
        raise ValueError(msg)
    if "%" in prefix or "_" in prefix:
        msg = "prefix must not contain SQL LIKE wildcards '%' or '_'"
        raise ValueError(msg)
    if len(prefix) < len("loadtest-YYYYMMDDT000000Z-"):
        msg = "prefix is too short; use a timestamped value"
        raise ValueError(msg)


def parse_source_counts(raw: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    if not raw.strip():
        return counts
    for item in raw.split(","):
        key, separator, value = item.partition("=")
        if not separator:
            msg = f"Expected source=count item, got {item!r}"
            raise ValueError(msg)
        source_type = key.strip()
        if source_type not in LOAD_TEST_SOURCE_TYPES:
            msg = f"Unsupported source type: {source_type}"
            raise ValueError(msg)
        try:
            count = int(value)
        except ValueError as exc:
            msg = f"Invalid count for {source_type}: {value!r}"
            raise ValueError(msg) from exc
        if count < 0:
            msg = f"Count for {source_type} must be >= 0"
            raise ValueError(msg)
        counts[source_type] = count
    return counts


def activation_delta(
    current: dict[str, int],
    target: dict[str, int],
) -> dict[str, int]:
    return {
        source_type: max(0, target_count - current.get(source_type, 0))
        for source_type, target_count in target.items()
    }


def allocate_total_by_available(
    available: dict[str, int],
    target_total: int,
) -> dict[str, int]:
    if target_total <= 0:
        msg = "target total must be positive"
        raise ValueError(msg)

    clean_available = {
        source_type: count
        for source_type, count in available.items()
        if count > 0
    }
    available_total = sum(clean_available.values())
    if target_total > available_total:
        msg = (
            f"target total {target_total} exceeds seeded row total "
            f"{available_total}"
        )
        raise ValueError(msg)

    allocated: dict[str, int] = dict.fromkeys(clean_available, 0)
    residual_available = clean_available.copy()
    remaining = target_total
    if target_total >= len(clean_available):
        for source_type in clean_available:
            allocated[source_type] = 1
            residual_available[source_type] -= 1
            remaining -= 1

    remainders: list[tuple[float, str]] = []
    residual_total = sum(residual_available.values())
    residual_target = remaining
    for source_type, count in sorted(residual_available.items()):
        if count <= 0:
            continue
        exact = residual_target * count / residual_total
        floor = int(exact)
        allocated[source_type] += floor
        remaining -= floor
        remainders.append((exact - floor, source_type))

    for _fraction, source_type in sorted(remainders, reverse=True):
        if remaining <= 0:
            break
        if allocated[source_type] < clean_available[source_type]:
            allocated[source_type] += 1
            remaining -= 1

    return {
        source_type: count
        for source_type, count in allocated.items()
        if count > 0
    }


def build_catalog_command(
    output_dir: Path,
    *,
    cache_only: bool,
    skip_echo: bool,
) -> list[str]:
    command = [
        sys.executable,
        "model/data/wildfire_catalog/run_catalog.py",
        "--output-dir",
        str(output_dir),
    ]
    if cache_only:
        command.append("--cache-only")
    if skip_echo:
        command.append("--skip-echo")
    return command


def _csv_value(row: dict[str, str], *keys: str) -> str:
    for key in keys:
        value = row.get(key)
        if value is not None and value.strip():
            return value.strip()
    return ""


def _normalize_source_feed_id(source_type: str, source_feed_id: str) -> str:
    if source_type == "openmhz":
        return source_feed_id.rsplit(":", 1)[0].strip()
    return source_feed_id.strip()


def _build_feed_name(
    *,
    name_prefix: str,
    catalog_id: str,
    feed_name: str,
    source_type: str,
    source_feed_id: str,
) -> str:
    stable_id = catalog_id or f"{source_type}-{source_feed_id}"
    display_name = feed_name or f"{source_type}/{source_feed_id}"
    return f"{name_prefix}{stable_id}-{display_name}"[:255]


def _tags_from_row(
    row: dict[str, str],
    tag_columns: tuple[str, ...],
) -> list[dict[str, str]]:
    tags: list[dict[str, str]] = []
    for key in tag_columns:
        value = row.get(key, "").strip()
        if value:
            tags.append({"key": key, "value": value})
    return tags


def load_feed_rows(
    csv_path: Path,
    *,
    source_types: set[str],
    limits: dict[str, int],
    name_prefix: str,
    tag_columns: tuple[str, ...],
    total_limit: int | None = None,
) -> list[FeedRow]:
    rows: list[FeedRow] = []
    counts: defaultdict[str, int] = defaultdict(int)
    seen: set[tuple[str, str]] = set()

    with csv_path.open(newline="", encoding="utf-8") as fp:
        reader = csv.DictReader(fp)
        for raw_row in reader:
            source_type = _csv_value(raw_row, "source", "source_type")
            if source_type not in source_types:
                continue

            source_feed_id = _csv_value(raw_row, "source_feed_id")
            if not source_feed_id:
                continue
            source_feed_id = _normalize_source_feed_id(
                source_type,
                source_feed_id,
            )
            key = (source_type, source_feed_id)
            if key in seen:
                continue
            if counts[source_type] >= limits.get(source_type, sys.maxsize):
                continue

            feed_name = _csv_value(raw_row, "feed_name", "name", "display_name")
            catalog_id = _csv_value(raw_row, "catalog_id")
            rows.append(
                FeedRow(
                    name=_build_feed_name(
                        name_prefix=name_prefix,
                        catalog_id=catalog_id,
                        feed_name=feed_name,
                        source_type=source_type,
                        source_feed_id=source_feed_id,
                    ),
                    source_type=source_type,
                    source_feed_id=source_feed_id,
                    tags=_tags_from_row(raw_row, tag_columns),
                ),
            )
            seen.add(key)
            counts[source_type] += 1
            if total_limit is not None and len(rows) >= total_limit:
                break

    return rows


def _dsn_from_env() -> str:
    database_url = os.environ.get("DATABASE_URL")
    if database_url:
        return database_url

    required = [
        "ALLOYDB_HOST",
        "ALLOYDB_PORT",
        "ALLOYDB_USER",
        "ALLOYDB_PASSWORD",
        "ALLOYDB_DB",
    ]
    missing = [name for name in required if not os.environ.get(name)]
    if missing:
        msg = (
            "Missing database env vars: "
            + ", ".join(missing)
            + " (or set DATABASE_URL)"
        )
        raise SystemExit(msg)

    return (
        f"host={os.environ['ALLOYDB_HOST']} "
        f"port={os.environ['ALLOYDB_PORT']} "
        f"user={os.environ['ALLOYDB_USER']} "
        f"password={os.environ['ALLOYDB_PASSWORD']} "
        f"dbname={os.environ['ALLOYDB_DB']}"
    )


def _prefix_like(name_prefix: str) -> str:
    return f"{name_prefix}%"


def _print_counts(rows: list[tuple[str, str, int]]) -> None:
    if not rows:
        print("No matching load-test rows.")
        return
    for source_type, status, count in rows:
        print(f"{source_type:18s} {status:13s} {count}")


def _connect() -> psycopg.Connection:
    return psycopg.connect(_dsn_from_env())


def import_feeds(args: argparse.Namespace) -> int:
    source_types = {
        item.strip() for item in args.source_types.split(",") if item.strip()
    }
    unsupported = source_types - LOAD_TEST_SOURCE_TYPES
    if unsupported:
        print(
            f"Unsupported source types: {', '.join(sorted(unsupported))}",
            file=sys.stderr,
        )
        return 2
    if not source_types:
        print("No source types requested.", file=sys.stderr)
        return 2

    rows = load_feed_rows(
        Path(args.csv),
        source_types=source_types,
        limits=parse_source_counts(args.max_per_source),
        name_prefix=args.prefix,
        tag_columns=tuple(c.strip() for c in args.tag_columns.split(",") if c),
        total_limit=args.target_total,
    )
    if not rows:
        print("No feed rows selected.", file=sys.stderr)
        return 1
    if args.target_total is not None and len(rows) < args.target_total:
        print(
            f"Selected only {len(rows)} rows; --target-total requested "
            f"{args.target_total}.",
            file=sys.stderr,
        )
        return 1

    counts: defaultdict[str, int] = defaultdict(int)
    for row in rows:
        counts[row.source_type] += 1
    print(f"Selected {len(rows)} feed rows:")
    for source_type, count in sorted(counts.items()):
        print(f"  {source_type}: {count}")

    if args.dry_run:
        print("--dry-run: not inserting")
        return 0

    actor_id = args.actor_id
    with _connect() as conn, conn.cursor() as cur:
        inserted = 0
        for row in rows:
            cur.execute(
                _INSERT_LOAD_TEST_FEED_SQL,
                (
                    uuid.uuid4(),
                    row.name,
                    row.source_type,
                    row.source_feed_id,
                    json.dumps(row.tags),
                    actor_id,
                ),
            )
            inserted += 1
            if inserted % args.commit_every == 0:
                conn.commit()
                print(f"  inserted {inserted}/{len(rows)}")
        conn.commit()
    print(f"Inserted {inserted} deactivated load-test feeds.")
    return 0


def seed_catalog(args: argparse.Namespace) -> int:
    output_dir = Path(args.catalog_output_dir)
    if not args.skip_catalog_build:
        command = build_catalog_command(
            output_dir,
            cache_only=args.cache_only,
            skip_echo=True,
        )
        print("Building load-test catalog:")
        print("  " + " ".join(command))
        try:
            subprocess.run(command, check=True)
        except subprocess.CalledProcessError as exc:
            print(
                f"Catalog build failed with exit code {exc.returncode}.",
                file=sys.stderr,
            )
            return exc.returncode or 1

    catalog_csv = output_dir / GENERATED_CATALOG_FILENAME
    if not catalog_csv.exists():
        print(
            f"Generated catalog not found: {catalog_csv}. Run without "
            "--skip-catalog-build or inspect catalog generation logs.",
            file=sys.stderr,
        )
        return 1

    args.csv = str(catalog_csv)
    return import_feeds(args)


def count_feeds(args: argparse.Namespace) -> int:
    with _connect() as conn, conn.cursor() as cur:
        cur.execute(_COUNT_SQL, (_prefix_like(args.prefix),))
        rows = [(r[0], r[1], r[2]) for r in cur.fetchall()]
    _print_counts(rows)
    return 0


def _current_activeish_counts(
    cur: psycopg.Cursor,
    name_prefix: str,
) -> dict[str, int]:
    cur.execute(_CURRENT_ACTIVEISH_SQL, (_prefix_like(name_prefix),))
    return dict(cur.fetchall())


def _total_seeded_counts(
    cur: psycopg.Cursor,
    name_prefix: str,
) -> dict[str, int]:
    cur.execute(_TOTAL_SEEDED_SQL, (_prefix_like(name_prefix),))
    return dict(cur.fetchall())


def activate_feeds(args: argparse.Namespace) -> int:
    with _connect() as conn, conn.cursor() as cur:
        if args.target_total is not None:
            try:
                target = allocate_total_by_available(
                    _total_seeded_counts(cur, args.prefix),
                    args.target_total,
                )
            except ValueError as exc:
                print(str(exc), file=sys.stderr)
                return 1
            print("Computed cumulative target from seeded source mix:")
            for source_type, count in sorted(target.items()):
                print(f"  {source_type}: {count}")
        else:
            target = parse_source_counts(args.target)

        if not target:
            print("No target counts requested.", file=sys.stderr)
            return 1

        current = _current_activeish_counts(cur, args.prefix)
        delta = activation_delta(current=current, target=target)
        print("Current non-deactivated counts:")
        for source_type, count in sorted(current.items()):
            print(f"  {source_type}: {count}")
        print("Activation delta:")
        for source_type, count in sorted(delta.items()):
            print(f"  {source_type}: {count}")

        if args.dry_run:
            print("--dry-run: not activating")
            return 0

        total = 0
        for source_type, count in sorted(delta.items()):
            if count == 0:
                continue
            cur.execute(
                _ACTIVATE_SQL,
                (
                    _prefix_like(args.prefix),
                    source_type,
                    count,
                    args.actor_id,
                ),
            )
            affected = len(cur.fetchall())
            total += affected
            print(f"  activated {affected}/{count} {source_type}")
        conn.commit()
    print(f"Activated {total} feeds.")
    return 0


def _positive_int(raw: str) -> int:
    value = int(raw)
    if value <= 0:
        msg = "value must be positive"
        raise argparse.ArgumentTypeError(msg)
    return value


def _positive_optional_int(raw: str) -> int:
    return _positive_int(raw)


def _commit_every(raw: str) -> int:
    return _positive_int(raw)


def _add_import_options(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--source-types",
        default="bcfy_feeds,bcfy_calls,openmhz",
        help="Comma-separated source types to import.",
    )
    parser.add_argument(
        "--max-per-source",
        default="",
        help="Optional per-source limits, e.g. bcfy_feeds=1000,openmhz=100.",
    )
    parser.add_argument(
        "--target-total",
        type=_positive_optional_int,
        default=None,
        help="Optional total selected rows cap, preserving catalog order.",
    )
    parser.add_argument(
        "--tag-columns",
        default=",".join(DEFAULT_TAG_COLUMNS),
        help="CSV columns to copy into feed_properties.tags.",
    )
    parser.add_argument(
        "--commit-every",
        type=_commit_every,
        default=500,
        help="Commit interval for large imports.",
    )
    parser.add_argument("--dry-run", action="store_true")


def deactivate_feeds(args: argparse.Namespace) -> int:
    with _connect() as conn, conn.cursor() as cur:
        if args.dry_run:
            cur.execute(
                """
                SELECT COUNT(*)
                FROM feeds
                WHERE name LIKE %s
                  AND status <> 'deactivated'::feed_status
                """,
                (_prefix_like(args.prefix),),
            )
            print(f"--dry-run: would deactivate {cur.fetchone()[0]} feeds")
            return 0

        cur.execute(_DEACTIVATE_SQL, (_prefix_like(args.prefix), args.actor_id))
        affected = len(cur.fetchall())
        conn.commit()
    print(f"Deactivated {affected} feeds.")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Seed, count, activate, and deactivate ingestion load-test feeds."
        ),
    )
    parser.add_argument(
        "--prefix",
        required=True,
        help=(
            "Unique feed-name prefix for this load test, "
            "e.g. loadtest-20260707T120000Z-"
        ),
    )
    parser.add_argument(
        "--actor-id",
        default="script:load_test_feeds",
        help="Actor ID written to feed_audit_events.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    import_parser = subparsers.add_parser(
        "import",
        help="Insert CSV feeds as deactivated load-test rows.",
    )
    import_parser.add_argument("--csv", required=True, help="Catalog CSV path.")
    _add_import_options(import_parser)
    import_parser.set_defaults(func=import_feeds)

    seed_parser = subparsers.add_parser(
        "seed-catalog",
        help="Build the wildfire catalog and import its Tier 1+2 rows.",
    )
    seed_parser.add_argument(
        "--catalog-output-dir",
        default=str(DEFAULT_CATALOG_OUTPUT_DIR),
        help="Directory where the catalog builder writes generated CSVs.",
    )
    seed_parser.add_argument(
        "--cache-only",
        action="store_true",
        help="Use the catalog cache without making API calls.",
    )
    seed_parser.add_argument(
        "--skip-catalog-build",
        action="store_true",
        help="Reuse an existing generated catalog CSV in --catalog-output-dir.",
    )
    _add_import_options(seed_parser)
    seed_parser.set_defaults(func=seed_catalog)

    activate_parser = subparsers.add_parser(
        "activate",
        help="Activate up to cumulative target counts.",
    )
    activate_targets = activate_parser.add_mutually_exclusive_group(
        required=True,
    )
    activate_targets.add_argument(
        "--target",
        help="Cumulative targets, e.g. bcfy_feeds=41,bcfy_calls=55,openmhz=4.",
    )
    activate_targets.add_argument(
        "--target-total",
        type=_positive_optional_int,
        help="Cumulative total target allocated from the seeded source mix.",
    )
    activate_parser.add_argument("--dry-run", action="store_true")
    activate_parser.set_defaults(func=activate_feeds)

    counts_parser = subparsers.add_parser(
        "counts",
        help="Print load-test row counts by source type and status.",
    )
    counts_parser.set_defaults(func=count_feeds)

    deactivate_parser = subparsers.add_parser(
        "deactivate",
        help="Deactivate all non-deactivated rows for the prefix.",
    )
    deactivate_parser.add_argument("--dry-run", action="store_true")
    deactivate_parser.set_defaults(func=deactivate_feeds)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        validate_prefix(args.prefix)
    except ValueError as exc:
        parser.error(str(exc))
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
