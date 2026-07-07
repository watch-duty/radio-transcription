import csv
import json
import sys
from collections import defaultdict
from dataclasses import asdict
from datetime import datetime, timezone

_CSV_COLUMNS = [
    "catalog_id",
    "canonical_id",
    "source",
    "source_feed_id",
    "feed_name",
    "description",
    "state",
    "county",
    "county_fips",
    "category",
    "agency_name",
    "agency_type",
    "service_tags",
    "keyword_score",
    "geographic_risk_score",
    "composite_priority",
    "priority_tier",
    "is_encrypted",
    "is_online",
    "listener_count",
    "has_duplicate",
    "duplicate_sources",
    "bcfy_calls_group_id",
    "admin_selected",
    "admin_notes",
]


def _feed_to_csv_row(feed):
    d = asdict(feed)
    row = {col: d.get(col, "") for col in _CSV_COLUMNS}
    # listener_count can be None → empty string
    if row["listener_count"] is None:
        row["listener_count"] = ""
    # round scores
    for k in ("keyword_score", "geographic_risk_score", "composite_priority"):
        v = row[k]
        if isinstance(v, float):
            row[k] = round(v, 4)
    return row


def write_catalog_csv(feeds, path):
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=_CSV_COLUMNS)
        writer.writeheader()
        for feed in feeds:
            writer.writerow(_feed_to_csv_row(feed))
    print(f"  wrote {path}: {len(feeds)} rows", file=sys.stderr)


def write_admin_review_csv(feeds, path):
    """Write a focused subset for admin review: Tier 1 + Tier 2 only."""
    filtered = [f for f in feeds if f.priority_tier <= 2]
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=_CSV_COLUMNS)
        writer.writeheader()
        for feed in filtered:
            writer.writerow(_feed_to_csv_row(feed))
    print(
        f"  wrote {path}: {len(filtered)} rows (Tier 1+2 only)",
        file=sys.stderr,
    )


def write_summary_csv(feeds, path):
    by_state = defaultdict(lambda: {
        "total": 0, "t1": 0, "t2": 0, "t3": 0,
        "bcfy_feeds": 0, "bcfy_calls": 0, "openmhz": 0, "echo": 0,
        "risk_sum": 0.0, "risk_count": 0,
    })

    for f in feeds:
        st = f.state or "UNKNOWN"
        s = by_state[st]
        s["total"] += 1
        if f.priority_tier == 1:
            s["t1"] += 1
        elif f.priority_tier == 2:
            s["t2"] += 1
        else:
            s["t3"] += 1
        if f.source in s:
            s[f.source] += 1
        if f.geographic_risk_score > 0:
            s["risk_sum"] += f.geographic_risk_score
            s["risk_count"] += 1

    with open(path, "w", newline="") as fp:
        writer = csv.writer(fp)
        writer.writerow([
            "state", "total_feeds", "tier_1_count", "tier_2_count", "tier_3_count",
            "bcfy_feeds_count", "bcfy_calls_count", "openmhz_count", "echo_count",
            "avg_wildfire_risk",
        ])
        for state in sorted(by_state):
            s = by_state[state]
            avg = round(s["risk_sum"] / s["risk_count"], 4) if s["risk_count"] else 0.0
            writer.writerow([
                state, s["total"], s["t1"], s["t2"], s["t3"],
                s["bcfy_feeds"], s["bcfy_calls"], s["openmhz"], s["echo"],
                avg,
            ])
    print(f"  wrote {path}", file=sys.stderr)


def write_catalog_json(feeds, path):
    feeds_by_tier = {"tier_1": 0, "tier_2": 0, "tier_3": 0}
    feeds_by_source = {}
    feed_dicts = []

    for f in feeds:
        tier_key = f"tier_{f.priority_tier}"
        feeds_by_tier[tier_key] = feeds_by_tier.get(tier_key, 0) + 1
        feeds_by_source[f.source] = feeds_by_source.get(f.source, 0) + 1

        d = asdict(f)
        # Drop internal fields from output
        d.pop("matched_keywords", None)
        d.pop("raw_metadata", None)
        d.pop("agency_tier_score", None)
        d.pop("quality_score", None)
        feed_dicts.append(d)

    out = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "total_feeds": len(feeds),
        "feeds_by_tier": feeds_by_tier,
        "feeds_by_source": feeds_by_source,
        "feeds": feed_dicts,
    }

    with open(path, "w") as fp:
        json.dump(out, fp, indent=2)
    print(f"  wrote {path}", file=sys.stderr)


def write_classification_log(feeds, path):
    with open(path, "w") as fp:
        for f in feeds:
            entry = {
                "catalog_id": f.catalog_id,
                "source": f.source,
                "source_feed_id": f.source_feed_id,
                "feed_name": f.feed_name,
                "state": f.state,
                "county_fips": f.county_fips,
                "keyword_score": f.keyword_score,
                "geographic_risk_score": f.geographic_risk_score,
                "agency_tier_score": f.agency_tier_score,
                "quality_score": f.quality_score,
                "composite_priority": f.composite_priority,
                "priority_tier": f.priority_tier,
                "matched_keywords": f.matched_keywords,
                "service_tags": f.service_tags,
                "raw_metadata": f.raw_metadata,
            }
            fp.write(json.dumps(entry, default=str) + "\n")
    print(f"  wrote {path}", file=sys.stderr)
