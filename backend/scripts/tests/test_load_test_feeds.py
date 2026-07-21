"""Unit tests for load test feed generator and controller scripts."""

import json

from backend.scripts.generate_load_test_csv import calculate_mix_counts
from backend.scripts.load_test_feeds import (
    allocate_target_mix,
    build_parser,
    cmd_activate,
    cmd_create,
    parse_tags_column,
)


def test_calculate_mix_counts_proportions() -> None:
    """Verify mix calculation with capped unique Echo feeds."""
    # Capped at 47 unique Echo feeds
    counts = calculate_mix_counts(15000, max_echo=47)
    assert counts["echo"] == 47
    assert counts["bcfy_feeds"] == 3750
    assert counts["fire_notifications"] == 11203
    assert sum(counts.values()) == 15000


def test_allocate_target_mix() -> None:
    """Verify target mix allocation function."""
    mix = allocate_target_mix(100)
    assert mix["fire_notifications"] == 50
    assert mix["echo"] == 25
    assert mix["bcfy_feeds"] == 25

    mix_capped = allocate_target_mix(15000, max_echo=47)
    assert mix_capped["echo"] == 47
    assert mix_capped["bcfy_feeds"] == 3750
    assert mix_capped["fire_notifications"] == 11203
    assert sum(mix_capped.values()) == 15000


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


def test_build_parser_activate_flags() -> None:
    """Verify activate parser accepts --target and --target-total flags and defaults to None when omitted."""
    parser = build_parser()
    args1 = parser.parse_args(
        [
            "--server",
            "http://localhost",
            "--token",
            "abc",
            "--prefix",
            "test-",
            "activate",
            "--target",
            "100",
        ]
    )
    assert args1.target == 100
    assert args1.func == cmd_activate

    args2 = parser.parse_args(
        [
            "--server",
            "http://localhost",
            "--token",
            "abc",
            "--prefix",
            "test-",
            "create",
            "--target-total",
            "500",
        ]
    )
    assert args2.target_total == 500
    assert args2.func == cmd_create

    args3 = parser.parse_args(
        [
            "--server",
            "http://localhost",
            "--token",
            "abc",
            "--prefix",
            "test-",
            "activate",
        ]
    )
    assert args3.target is None
    assert args3.func == cmd_activate
