from __future__ import annotations

import csv
import importlib.util
from pathlib import Path

import pytest

_SCRIPT_PATH = Path(__file__).parents[1] / "load_test_feeds.py"
_SPEC = importlib.util.spec_from_file_location("load_test_feeds", _SCRIPT_PATH)
assert _SPEC is not None
load_test_feeds = importlib.util.module_from_spec(_SPEC)
assert _SPEC.loader is not None
_SPEC.loader.exec_module(load_test_feeds)


def _write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    fieldnames = [
        "catalog_id",
        "source",
        "source_feed_id",
        "feed_name",
        "priority_tier",
        "state",
        "county",
    ]
    with path.open("w", newline="", encoding="utf-8") as fp:
        writer = csv.DictWriter(fp, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def test_parse_source_counts() -> None:
    counts = load_test_feeds.parse_source_counts(
        "bcfy_feeds=41,bcfy_calls=55,openmhz=4",
    )

    assert counts == {"bcfy_feeds": 41, "bcfy_calls": 55, "openmhz": 4}


def test_parse_source_counts_rejects_unknown_source() -> None:
    with pytest.raises(ValueError, match="Unsupported source type"):
        load_test_feeds.parse_source_counts("echo=10")


def test_load_feed_rows_filters_limits_and_deduplicates_openmhz(
    tmp_path: Path,
) -> None:
    csv_path = tmp_path / "feeds.csv"
    _write_csv(
        csv_path,
        [
            {
                "catalog_id": "om-1",
                "source": "openmhz",
                "source_feed_id": "svrcs2:2301",
                "feed_name": "SVRCS Fire 1",
                "priority_tier": "1",
                "state": "CA",
                "county": "Santa Clara",
            },
            {
                "catalog_id": "om-2",
                "source": "openmhz",
                "source_feed_id": "svrcs2:2302",
                "feed_name": "SVRCS Fire 2",
                "priority_tier": "2",
                "state": "CA",
                "county": "Santa Clara",
            },
            {
                "catalog_id": "bf-1",
                "source": "bcfy_feeds",
                "source_feed_id": "12345",
                "feed_name": "County Fire Dispatch",
                "priority_tier": "1",
                "state": "CA",
                "county": "Butte",
            },
            {
                "catalog_id": "bc-1",
                "source": "bcfy_calls",
                "source_feed_id": "12-34",
                "feed_name": "Calls Talkgroup",
                "priority_tier": "1",
                "state": "CA",
                "county": "Butte",
            },
            {
                "catalog_id": "echo-1",
                "source": "echo",
                "source_feed_id": "ignored",
                "feed_name": "Echo Feed",
                "priority_tier": "1",
                "state": "CA",
                "county": "Butte",
            },
        ],
    )

    rows = load_test_feeds.load_feed_rows(
        csv_path,
        source_types={"bcfy_feeds", "bcfy_calls", "openmhz"},
        limits={"bcfy_feeds": 10, "bcfy_calls": 10, "openmhz": 10},
        name_prefix="lt-",
        tag_columns=("catalog_id", "priority_tier", "state", "county"),
    )

    assert [(r.source_type, r.source_feed_id) for r in rows] == [
        ("openmhz", "svrcs2"),
        ("bcfy_feeds", "12345"),
        ("bcfy_calls", "12-34"),
    ]
    assert rows[0].name == "lt-om-1-SVRCS Fire 1"
    assert rows[0].tags == [
        {"key": "catalog_id", "value": "om-1"},
        {"key": "priority_tier", "value": "1"},
        {"key": "state", "value": "CA"},
        {"key": "county", "value": "Santa Clara"},
    ]


def test_load_feed_rows_enforces_per_source_limits(tmp_path: Path) -> None:
    csv_path = tmp_path / "feeds.csv"
    _write_csv(
        csv_path,
        [
            {
                "catalog_id": "bf-1",
                "source": "bcfy_feeds",
                "source_feed_id": "111",
                "feed_name": "Feed One",
                "priority_tier": "1",
                "state": "CA",
                "county": "A",
            },
            {
                "catalog_id": "bf-2",
                "source": "bcfy_feeds",
                "source_feed_id": "222",
                "feed_name": "Feed Two",
                "priority_tier": "1",
                "state": "CA",
                "county": "B",
            },
        ],
    )

    rows = load_test_feeds.load_feed_rows(
        csv_path,
        source_types={"bcfy_feeds"},
        limits={"bcfy_feeds": 1},
        name_prefix="lt-",
        tag_columns=("catalog_id",),
    )

    assert len(rows) == 1
    assert rows[0].source_feed_id == "111"


def test_activation_delta_uses_cumulative_targets() -> None:
    delta = load_test_feeds.activation_delta(
        current={"bcfy_feeds": 41, "bcfy_calls": 55},
        target={"bcfy_feeds": 103, "bcfy_calls": 138, "openmhz": 9},
    )

    assert delta == {"bcfy_feeds": 62, "bcfy_calls": 83, "openmhz": 9}


def test_validate_prefix_rejects_broad_or_wildcard_prefixes() -> None:
    for prefix in ("", "exp-", "loadtest-%", "loadtest-_"):
        with pytest.raises(ValueError, match="prefix"):
            load_test_feeds.validate_prefix(prefix)

    load_test_feeds.validate_prefix("loadtest-20260707T120000Z-")
