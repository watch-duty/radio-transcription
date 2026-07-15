"""Unit tests for the Gemini context preparation module."""

from common.gemini.context import add_prior_context_to_manifest


def test_add_prior_context_sorting() -> None:
    rows = [
        {
            "source_group": "group_1",
            "original_offset": 10.5,
            "text": "Third segment.",
        },
        {
            "source_group": "group_1",
            "original_offset": 2.1,
            "text": "First segment.",
        },
        {
            "source_group": "group_1",
            "original_offset": 5.0,
            "text": "Second segment.",
        },
    ]

    result = add_prior_context_to_manifest(rows, context_turns=2)

    # Verify group remains sorted by original_offset
    assert result[0]["text"] == "First segment."
    assert result[0]["prior_context"] == []

    assert result[1]["text"] == "Second segment."
    assert result[1]["prior_context"] == ["First segment."]

    assert result[2]["text"] == "Third segment."
    assert result[2]["prior_context"] == ["First segment.", "Second segment."]


def test_add_prior_context_turns_cap() -> None:
    rows = [
        {"source_group": "g1", "original_offset": 1.0, "text": "T1"},
        {"source_group": "g1", "original_offset": 2.0, "text": "T2"},
        {"source_group": "g1", "original_offset": 3.0, "text": "T3"},
        {"source_group": "g1", "original_offset": 4.0, "text": "T4"},
    ]

    # Cap at 2 turns
    result = add_prior_context_to_manifest(rows, context_turns=2)

    assert result[3]["text"] == "T4"
    assert result[3]["prior_context"] == ["T2", "T3"]


def test_add_prior_context_multiple_groups() -> None:
    rows = [
        {"source_group": "group_A", "original_offset": 1.0, "text": "A1"},
        {"source_group": "group_B", "original_offset": 1.0, "text": "B1"},
        {"source_group": "group_A", "original_offset": 2.0, "text": "A2"},
    ]

    result = add_prior_context_to_manifest(rows, context_turns=5)

    # Group A segments should be clustered together at the start
    assert result[0]["text"] == "A1"
    assert result[0]["prior_context"] == []

    assert result[1]["text"] == "A2"
    assert result[1]["prior_context"] == ["A1"]

    # Group B segment should be placed after Group A's cluster
    assert result[2]["text"] == "B1"
    assert result[2]["prior_context"] == []
