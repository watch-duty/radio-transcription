"""
US Wildfire Radio Feed Catalog builder.

Discovers US radio scanner feeds from four sources (Broadcastify Feeds,
Broadcastify Calls, OpenMHZ, Echo), scores them by wildfire relevance,
and produces CSV + JSON catalogs.

Usage:
    python run_catalog.py [--skip-bcfy-feeds] [--skip-bcfy-calls]
                          [--skip-openmhz] [--skip-echo]
                          [--cache-only] [--output-dir ./output]
"""
import argparse
import logging
import os
import sys
from pathlib import Path

# Allow imports of sibling modules when invoked from the dir
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import cache
import dedup
import geo_risk
import output
import scoring
from config import BCFY_QPS, OPENMHZ_QPS
from rate_limiter import RateLimiter
from sources import bcfy_calls, bcfy_feeds, echo, openmhz


def parse_args():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--skip-bcfy-feeds", action="store_true")
    p.add_argument("--skip-bcfy-calls", action="store_true")
    p.add_argument("--skip-openmhz", action="store_true")
    p.add_argument("--skip-echo", action="store_true")
    p.add_argument(
        "--cache-only", action="store_true",
        help="Use only cached data, no API calls",
    )
    p.add_argument("--output-dir", default="./output")
    p.add_argument(
        "--log-level", default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    return p.parse_args()


def collect(args, bcfy_limiter, openmhz_limiter):
    """Phase 1 — Collection. Returns combined list of CatalogFeed."""
    feeds = []

    if not args.skip_bcfy_feeds:
        print("Phase 1a: bcfy_feeds", file=sys.stderr)
        feeds.extend(bcfy_feeds.fetch(bcfy_limiter))

    if not args.skip_echo:
        print("Phase 1b: echo", file=sys.stderr)
        feeds.extend(echo.fetch())

    if not args.skip_openmhz:
        print("Phase 1c: openmhz", file=sys.stderr)
        feeds.extend(openmhz.fetch(openmhz_limiter))

    if not args.skip_bcfy_calls:
        print("Phase 1d: bcfy_calls (longest — ~27 min uncached)", file=sys.stderr)
        feeds.extend(bcfy_calls.fetch(bcfy_limiter))

    return feeds


def score_all(feeds):
    """Phase 2 — Scoring. Mutates feeds in-place."""
    print("Phase 2: scoring", file=sys.stderr)

    county_risk, state_avg = geo_risk.load_nri_data()
    listener_percentiles = scoring.build_listener_percentiles(feeds)

    print(
        f"  scoring: {len(listener_percentiles)} feeds have listener counts",
        file=sys.stderr,
    )

    t1 = t2 = t3 = 0
    for f in feeds:
        # Use combined text for keyword/agency matching
        text = f"{f.feed_name} {f.description} {f.service_tags}"

        kw_score, matched = scoring.score_keywords(text)
        agency = scoring.score_agency_tier(text)
        geo = geo_risk.score_geo_risk(
            f.county_fips, f.state, county_risk, state_avg
        )
        if f.source == "bcfy_feeds":
            quality = scoring.score_quality(f.listener_count, listener_percentiles)
        else:
            quality = 0.5

        composite, tier = scoring.compute_composite(
            kw_score, geo, agency, quality
        )

        f.keyword_score = kw_score
        f.matched_keywords = matched
        f.agency_tier_score = agency
        f.geographic_risk_score = geo
        f.quality_score = quality
        f.composite_priority = composite
        f.priority_tier = tier

        if tier == 1:
            t1 += 1
        elif tier == 2:
            t2 += 1
        else:
            t3 += 1

    print(f"  scoring: tier_1={t1}, tier_2={t2}, tier_3={t3}", file=sys.stderr)


def write_outputs(feeds, output_dir):
    """Phase 4 — Output."""
    print(f"Phase 4: writing outputs to {output_dir}", file=sys.stderr)
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    # Sort by composite priority descending so top feeds surface first
    feeds_sorted = sorted(feeds, key=lambda f: f.composite_priority, reverse=True)

    output.write_catalog_csv(feeds_sorted, out / "wildfire_feed_catalog.csv")
    output.write_admin_review_csv(
        feeds_sorted, out / "wildfire_feed_catalog_admin_review.csv"
    )
    output.write_summary_csv(feeds_sorted, out / "wildfire_feed_catalog_summary.csv")
    output.write_catalog_json(feeds_sorted, out / "wildfire_feed_catalog.json")
    output.write_classification_log(feeds_sorted, out / "classification_log.jsonl")


def main():
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stderr,
    )

    # Empty-cache guard for --cache-only
    if args.cache_only and cache.is_empty():
        print(
            "ERROR: --cache-only requested but cache directory is empty.\n"
            "Run without --cache-only first to populate the cache.",
            file=sys.stderr,
        )
        return 2

    bcfy_limiter = RateLimiter(BCFY_QPS)
    openmhz_limiter = RateLimiter(OPENMHZ_QPS)

    feeds = collect(args, bcfy_limiter, openmhz_limiter)
    print(f"Collected {len(feeds)} total feeds", file=sys.stderr)

    score_all(feeds)

    print("Phase 3: dedup", file=sys.stderr)
    dedup.find_duplicates(feeds)

    write_outputs(feeds, args.output_dir)

    print(f"Done. {len(feeds)} feeds written to {args.output_dir}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
