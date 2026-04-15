"""Bulk-insert the Tier 1+2 catalog into the prod AlloyDB feeds table.

Experiment 1b Step 0.1.  Run from a VPC-authorized host (cloudshell,
bastion, or a GCE VM that can reach AlloyDB on port 5432/6432).

What it does:
1. Reads `output/wildfire_feed_catalog_admin_review.csv` (Tier 1+2 only).
2. Skips `source=echo` rows (Echo runs on Cloud Run, not MIG).
3. Inserts each row into `feeds` with `status='deactivated'`.
4. Inserts the matching `feed_properties` row (source_feed_id, external_id).
5. Prints the boundary `EXP_INSERT_TS` timestamp for Step 6.2 cleanup.

Idempotency: uniqueness is on `feeds.name` — this script prefixes every
name with `"exp1b-"` so re-runs raise UNIQUE violations loudly rather
than silently skipping (safer than ON CONFLICT DO NOTHING, which would
hide drift between runs).

Required env vars (same shape as the prod ingestion SA):
    ALLOYDB_HOST, ALLOYDB_PORT, ALLOYDB_USER, ALLOYDB_PASSWORD, ALLOYDB_DB

Usage:
    python experiment_1b_bulk_insert.py \\
        --catalog output/wildfire_feed_catalog_admin_review.csv \\
        --source-types bcfy_feeds,bcfy_calls,openmhz

See: EXPERIMENT_1B_PLAN.md §0.1
"""
from __future__ import annotations

import argparse
import csv
import datetime
import os
import sys
import uuid

import psycopg

_INSERT_FEED_SQL = """
INSERT INTO feeds (id, name, source_type, status)
VALUES (%s, %s, %s, 'deactivated'::feed_status)
"""

_INSERT_PROPERTIES_SQL = """
INSERT INTO feed_properties (feed_id, source_feed_id, external_id, source_type)
VALUES (%s, %s, %s, %s)
"""


def _dsn() -> str:
    required = ["ALLOYDB_HOST", "ALLOYDB_PORT", "ALLOYDB_USER",
                "ALLOYDB_PASSWORD", "ALLOYDB_DB"]
    missing = [k for k in required if not os.environ.get(k)]
    if missing:
        sys.exit(f"Missing required env vars: {', '.join(missing)}")
    return (
        f"host={os.environ['ALLOYDB_HOST']} "
        f"port={os.environ['ALLOYDB_PORT']} "
        f"user={os.environ['ALLOYDB_USER']} "
        f"password={os.environ['ALLOYDB_PASSWORD']} "
        f"dbname={os.environ['ALLOYDB_DB']}"
    )


def _load_rows(csv_path: str, source_types: set[str]) -> list[dict]:
    """Load Tier 1+2 catalog rows and normalize to one-row-per-feed-record.

    Key transform for openmhz: the catalog has one row per talkgroup
    (source_feed_id = "{short_name}:{num}"), but the production collector
    leases per-system.  We dedupe by short_name, keep the row with the
    best (lowest) priority_tier as the canonical system feed, and set
    source_feed_id to the bare short_name so the collector routes correctly.

    bcfy_feeds (1 stream per row) and bcfy_calls (1 groupId per row) are
    already at the correct granularity — passed through unchanged.
    """
    rows: list[dict] = []
    openmhz_by_system: dict[str, dict] = {}

    with open(csv_path) as f:
        for row in csv.DictReader(f):
            if row["source"] not in source_types:
                continue

            if row["source"] == "openmhz":
                # source_feed_id = "{short_name}:{num}" — take short_name
                short_name = row["source_feed_id"].rsplit(":", 1)[0]
                try:
                    row_tier = int(row["priority_tier"])
                except (ValueError, KeyError):
                    row_tier = 3
                existing = openmhz_by_system.get(short_name)
                if existing is None:
                    existing_tier = 99
                else:
                    try:
                        existing_tier = int(existing.get("priority_tier", 3))
                    except (ValueError, KeyError):
                        existing_tier = 3
                if existing is None or row_tier < existing_tier:
                    # Rewrite source_feed_id so the collector leases by
                    # shortName alone (matches collector.py:119 contract).
                    row = dict(row)
                    row["source_feed_id"] = short_name
                    # Rename the feed so it reads cleanly in the table.
                    row["feed_name"] = f"openmhz/{short_name}"
                    openmhz_by_system[short_name] = row
            else:
                rows.append(row)

    rows.extend(openmhz_by_system.values())
    return rows


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--catalog", required=True,
        help="Path to wildfire_feed_catalog_admin_review.csv",
    )
    ap.add_argument(
        "--source-types", default="bcfy_feeds,bcfy_calls,openmhz",
        help="Comma-separated source types to include (excludes echo by default)",
    )
    ap.add_argument(
        "--dry-run", action="store_true",
        help="Parse + validate, don't insert",
    )
    args = ap.parse_args()

    source_types = set(args.source_types.split(","))
    rows = _load_rows(args.catalog, source_types)
    print(f"Parsed {len(rows)} rows for source_types={sorted(source_types)}",
          file=sys.stderr)

    counts: dict[str, int] = {}
    for r in rows:
        counts[r["source"]] = counts.get(r["source"], 0) + 1
    for st, n in sorted(counts.items()):
        print(f"  {st}: {n}", file=sys.stderr)

    if args.dry_run:
        print("--dry-run: not inserting", file=sys.stderr)
        return

    insert_start_utc = datetime.datetime.now(datetime.UTC)
    print(f"Bulk insert started at {insert_start_utc.isoformat()}",
          file=sys.stderr)

    with psycopg.connect(_dsn()) as conn, conn.cursor() as cur:
        inserted = 0
        for r in rows:
            feed_id = uuid.uuid4()
            # Prefix all names to collision-fail loudly if re-run, and to
            # make experiment rows trivially filterable from prod rows.
            name = f"exp1b-{r['catalog_id']}-{r['feed_name'][:200]}"[:255]
            source_feed_id = r["source_feed_id"]
            source_type = r["source"]
            external_id = source_feed_id  # matches production's pattern

            cur.execute(_INSERT_FEED_SQL, (feed_id, name, source_type))
            cur.execute(
                _INSERT_PROPERTIES_SQL,
                (feed_id, source_feed_id, external_id, source_type),
            )
            inserted += 1
            if inserted % 500 == 0:
                conn.commit()
                print(f"  inserted {inserted}/{len(rows)}", file=sys.stderr)

        conn.commit()
        print(f"Bulk insert finished: {inserted} rows", file=sys.stderr)

    # 2-second buffer on either side of the recorded boundary catches any
    # late commits.  Step 6.2's cleanup uses '>=' so being inclusive here
    # is safe.
    exp_insert_ts = (insert_start_utc - datetime.timedelta(seconds=2)).isoformat()
    print(f"\nEXP_INSERT_TS={exp_insert_ts}")
    print("\nSave this timestamp for Step 6.2 cleanup:", file=sys.stderr)
    print(f"  echo 'EXP_INSERT_TS={exp_insert_ts}' | tee -a experiment_1b.env",
          file=sys.stderr)


if __name__ == "__main__":
    main()
