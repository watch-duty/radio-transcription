"""Tests for deterministic transcript reconciliation."""

from __future__ import annotations

import itertools
import pathlib
import sys

import pytest

RUN_ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(RUN_ROOT) not in sys.path:
    sys.path.insert(0, str(RUN_ROOT))

from dataset_run import domain, transcripts  # noqa: E402


def owner(
    atom_id: str,
    text: str,
    start: int,
    end: int,
    *,
    corrected: bool = False,
    authority_rank: int = 0,
) -> transcripts.TranscriptOwner:
    return transcripts.TranscriptOwner(
        atom_id=atom_id,
        text=text,
        start_frame=start,
        end_frame=end,
        corrected=corrected,
        authority_rank=authority_rank,
        provenance={"reviewed": corrected},
    )


def test_corrected_duplicate_is_authoritative_and_receipted() -> None:
    result = transcripts.reconcile_component(
        [
            owner("raw", "Engine SEVEN", 0, 100),
            owner("corrected", "engine seven", 20, 120, corrected=True),
        ]
    )

    assert result.text == "engine seven"
    pair = result.receipt["components"][0]["pair_decisions"][0]
    assert pair["rule"] == "duplicate"
    assert pair["selected_owner_id"] == "corrected"
    assert pair["suppressed_owner_ids"] == ["raw"]
    assert pair["tie_break"] == "corrected_authority"
    corrected_receipt = next(
        item
        for item in result.receipt["owners"]
        if item["owner_id"] == "corrected"
    )
    assert corrected_receipt["corrected_authority"] is True
    assert corrected_receipt["provenance"] == {"reviewed": True}


def test_latest_duplicate_wins_when_neither_is_corrected() -> None:
    result = transcripts.reconcile_component(
        [
            owner("revision-1", "Medic 4 respond", 0, 100, authority_rank=1),
            owner("revision-2", "Medic 4 respond", 0, 100, authority_rank=2),
        ]
    )

    pair = result.receipt["components"][0]["pair_decisions"][0]
    assert result.text == "Medic 4 respond"
    assert pair["selected_owner_id"] == "revision-2"
    assert pair["tie_break"] == "higher_authority_rank"


def test_contained_target_keeps_longer_text_once() -> None:
    result = transcripts.reconcile_component(
        [
            owner("short", "to Main Street", 10, 80, corrected=True),
            owner("long", "Engine 2 respond to Main Street", 0, 100),
        ]
    )

    assert result.text == "Engine 2 respond to Main Street"
    pair = result.receipt["components"][0]["pair_decisions"][0]
    assert pair["rule"] == "contained"
    assert pair["selected_owner_id"] == "long"
    assert pair["suppressed_owner_ids"] == ["short"]
    assert pair["tie_break"] == "longer_token_sequence"


def test_clear_suffix_prefix_overlap_is_merged_once() -> None:
    result = transcripts.reconcile_component(
        [
            owner("left", "Engine 7 respond to Oak", 0, 100),
            owner("right", "to Oak and First", 100, 200),
        ]
    )

    assert result.text == "Engine 7 respond to Oak and First"
    fold = result.receipt["components"][0]["fold_decisions"][0]
    assert fold["rule"] == "suffix_prefix_overlap"
    assert fold["overlap_token_count"] == 2


def test_one_token_overlap_is_ambiguous_and_preserves_both() -> None:
    result = transcripts.reconcile_component(
        [
            owner("left", "respond to Oak", 0, 100),
            owner("right", "Oak and First", 90, 200),
        ]
    )

    assert result.text == "respond to Oak Oak and First"
    fold = result.receipt["components"][0]["fold_decisions"][0]
    assert fold["rule"] == "chronological_concat"
    assert fold["overlap_token_count"] == 1
    assert "ambiguous" in fold["reason"]


def test_unrelated_overlapping_targets_are_concatenated() -> None:
    result = transcripts.reconcile_component(
        [
            owner("a", "Engine seven", 0, 100),
            owner("b", "Medic four", 50, 150),
        ]
    )

    assert result.text == "Engine seven Medic four"
    assert (
        result.receipt["components"][0]["fold_decisions"][0]["rule"]
        == "chronological_concat"
    )


def test_disconnected_duplicate_phrases_are_not_deduplicated() -> None:
    result = transcripts.reconcile_component(
        [
            owner("first", "copy that", 0, 100),
            owner("second", "copy that", 101, 200),
        ]
    )

    assert result.text == "copy that copy that"
    assert len(result.receipt["components"]) == 2
    assert all(
        not component["pair_decisions"]
        for component in result.receipt["components"]
    )


def test_audio_union_does_not_double_count_overlapping_frames() -> None:
    result = transcripts.reconcile_component(
        [
            owner("a", "alpha bravo", 0, 100),
            owner("b", "bravo charlie", 50, 150),
            owner("c", "delta echo", 200, 250),
        ]
    )

    assert result.covered_intervals == ((0, 150), (200, 250))
    assert result.covered_frame_count == 200
    assert (
        result.receipt["audio_coverage"]["overlap_frames_not_double_counted"]
        == 50
    )


def test_result_is_deterministic_across_input_permutations() -> None:
    owners = [
        owner("b", "alpha bravo charlie", 0, 100),
        owner("a", "alpha bravo charlie", 0, 100),
        owner("c", "bravo charlie delta", 90, 200),
    ]
    results = [
        transcripts.reconcile_component(list(permutation))
        for permutation in itertools.permutations(owners)
    ]

    assert {result.text for result in results} == {"alpha bravo charlie delta"}
    assert len({result.receipt_json for result in results}) == 1
    pair = results[0].receipt["components"][0]["pair_decisions"][0]
    assert pair["selected_owner_id"] == "a"
    assert pair["tie_break"] == "chronology_then_atom_id"


def test_domain_owned_speech_input_is_supported() -> None:
    atom = domain.Atom(
        atom_id="owned",
        family="echo",
        source_uri="gs://bucket/source.wav",
        raw_manifest="echo_eval",
        raw_index=5,
        start_frame=10,
        end_frame=20,
        atom_class=domain.AtomClass.SPEECH,
        text="raw target",
    )
    speech = domain.OwnedSpeech(
        atom=atom,
        split=domain.Split.EVAL,
        text="corrected target",
    )

    result = transcripts.reconcile_component([speech])

    assert result.text == "corrected target"
    assert result.owner_ids == ("owned",)


def test_mapping_input_can_carry_nested_authority_provenance() -> None:
    result = transcripts.reconcile_component(
        [
            {
                "atom_id": "old",
                "text": "ALPHA BRAVO",
                "start_frame": 0,
                "end_frame": 10,
            },
            {
                "atom_id": "new",
                "text": "alpha bravo",
                "start_frame": 0,
                "end_frame": 10,
                "provenance": {
                    "corrected": True,
                    "review_id": "review-12",
                },
            },
        ]
    )

    assert result.text == "alpha bravo"
    pair = result.receipt["components"][0]["pair_decisions"][0]
    assert pair["selected_owner_id"] == "new"


def test_blank_and_duplicate_owner_inputs_fail_immediately() -> None:
    with pytest.raises(ValueError, match="at least one"):
        transcripts.reconcile_component([])
    with pytest.raises(ValueError, match="non-empty"):
        owner("blank", " \n ", 0, 1)
    with pytest.raises(ValueError, match="must be unique"):
        transcripts.reconcile_component(
            [
                owner("same", "alpha", 0, 1),
                owner("same", "bravo", 1, 2),
            ]
        )
