"""Unit tests for load test feed generator and controller scripts."""

import json

from backend.scripts.generate_load_test_csv import calculate_mix_counts
from backend.scripts.load_test_feeds import (
    allocate_target_mix,
    parse_tags_column,
)


def test_calculate_mix_counts_proportions() -> None:
    """Verify 50% FN, 25% Echo, 25% BCFY mix calculation for 15k feeds."""
    counts = calculate_mix_counts(15000)
    assert counts["fire_notifications"] == 7500
    assert counts["echo"] == 3750
    assert counts["bcfy_feeds"] == 3750
    assert sum(counts.values()) == 15000


def test_allocate_target_mix() -> None:
    """Verify target mix allocation function."""
    mix = allocate_target_mix(100)
    assert mix["fire_notifications"] == 50
    assert mix["echo"] == 25
    assert mix["bcfy_feeds"] == 25


def test_parse_tags_column_key_value() -> None:
    """Verify key=value tag parsing."""
    tags = parse_tags_column("region=spokane-WA_US, env=loadtest")
    assert len(tags) == 2
    assert tags[0] == {"key": "region", "value": "spokane-WA_US"}
    assert tags[1] == {"key": "env", "value": "loadtest"}


def test_parse_tags_column_json() -> None:
    """Verify JSON tag array parsing."""
    json_str = json.dumps([{"key": "region", "value": "williamson-TX_US"}])
    tags = parse_tags_column(json_str)
    assert len(tags) == 1
    assert tags[0] == {"key": "region", "value": "williamson-TX_US"}
