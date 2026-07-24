# ruff: noqa: INP001
"""Tests for strict raw/canonical mapping and final train-wins ownership."""

from __future__ import annotations

import json
import pathlib
import sys
from typing import Any

import pytest

RUN_ROOT = pathlib.Path(__file__).resolve().parents[1]
MODEL_SRC = RUN_ROOT.parents[3] / "src"
for path in (RUN_ROOT, MODEL_SRC):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from dataset_run import domain, inputs, ownership  # noqa: E402

RAW_FILE_NAMES = {
    "calls_train": "calls_train.json",
    "calls_eval": "calls_eval.json",
    "feeds_train": "feeds_train.json",
    "feeds_eval": "feeds_eval.json",
    "echo_train": "echo_train.json",
    "echo_eval": "echo_eval.json",
    "fire_train": "fire_train.json",
    "fire_eval": "fire_eval.json",
}


def _raw(
    uri: str,
    *,
    offset: float,
    duration: float,
    text: str | None = None,
    category: str = "TRANSCRIPTION",
) -> dict[str, Any]:
    row: dict[str, Any] = {
        "audio_filepath": uri,
        "category": category,
        "duration": duration,
        "lang": "en_us",
        "offset": offset,
    }
    if text is not None:
        row["text"] = text
    return row


def _canonical(
    uri: str,
    *,
    family: str,
    segment_id: str,
    text: str,
    split: str,
    offset: float,
    duration: float,
    provenance: dict[str, Any] | None = None,
) -> dict[str, Any]:
    dataset_family = (
        "broadcastify"
        if family in {"broadcastify_calls", "broadcastify_feeds"}
        else family
    )
    dataset: dict[str, Any] = {
        "family": dataset_family,
        "name": family,
    }
    if provenance is not None:
        dataset["oneoff_provenance"] = provenance
    return {
        "audio_filepath": f"gs://derived/{segment_id}.flac",
        "dataset": dataset,
        "duration": duration,
        "example_id": uri.rsplit("/", 1)[-1],
        "offset": 0.0,
        "segment_id": segment_id,
        "source_audio": {
            "audio_filepath": uri,
            "duration": duration,
            "offset": offset,
        },
        "split": split,
        "text": text,
    }


def _write_bundle(
    tmp_path: pathlib.Path,
    *,
    raw_rows: dict[str, list[dict[str, Any]]],
    train: list[dict[str, Any]],
    eval_rows: list[dict[str, Any]],
    closure: list[dict[str, Any]],
    aliases: list[dict[str, Any]] | None = None,
) -> inputs.InputBundle:
    paths: dict[str, pathlib.Path] = {}
    for key, filename in RAW_FILE_NAMES.items():
        path = tmp_path / filename
        path.write_text(json.dumps(raw_rows.get(key, [])), encoding="utf-8")
        paths[key] = path
    for role, rows in (
        ("canonical_train", train),
        ("canonical_eval", eval_rows),
        ("canonical_closure", closure),
    ):
        path = tmp_path / f"{role}.jsonl"
        path.write_text(
            "".join(json.dumps(row, sort_keys=True) + "\n" for row in rows),
            encoding="utf-8",
        )
        paths[role] = path
    alias_path = tmp_path / "aliases.json"
    alias_path.write_text(
        json.dumps(
            {
                "pairs": aliases or [],
                "schema_version": 1,
                "source_audit_sha256": "test",
            }
        ),
        encoding="utf-8",
    )
    policy_path = tmp_path / "policy.json"
    policy_path.write_text(
        json.dumps(
            {
                "assembly": {"grouping_summary": {}},
                "masked_eval": {},
                "schema_version": 1,
            }
        ),
        encoding="utf-8",
    )
    return inputs.load_inputs(
        inputs.InputPaths(
            **paths,
            confirmed_fire_aliases=alias_path,
            recording_group_policy=policy_path,
        )
    )


def test_load_inputs_applies_speech_over_pii_contract(
    tmp_path: pathlib.Path,
) -> None:
    uri = "gs://bucket/calls/training/a.wav"
    bundle = _write_bundle(
        tmp_path,
        raw_rows={
            "calls_train": [
                _raw(
                    uri,
                    offset=0,
                    duration=1,
                    text=" Dispatcher ",
                    category="PII/ADDRESS",
                ),
                _raw(uri, offset=1, duration=1, category="PII/ADDRESS"),
                _raw(
                    uri,
                    offset=2,
                    duration=1,
                    text=" \t ",
                    category="UNINTELLIGIBLE",
                ),
            ]
        },
        train=[],
        eval_rows=[],
        closure=[],
    )

    assert [item.atom_class for item in bundle.raw_annotations] == [
        domain.AtomClass.SPEECH,
        domain.AtomClass.PII,
        domain.AtomClass.TARGETLESS,
    ]
    assert bundle.receipt["raw_speech_count"] == 1


def test_maps_current_legacy_and_composite_and_applies_closure(
    tmp_path: pathlib.Path,
) -> None:
    calls_train_uri = "gs://bucket/calls/training/current.wav"
    calls_eval_uri = "gs://bucket/calls/eval/legacy.wav"
    feeds_uri = "gs://bucket/feeds/training/feed.wav"
    current = _canonical(
        calls_train_uri,
        family="broadcastify_calls",
        segment_id="000000",
        text="uncorrected",
        split="train",
        offset=0.1,
        duration=1.0,
    )
    legacy = _canonical(
        calls_eval_uri,
        family="bcfy_calls",
        segment_id="0",
        text="eval speech",
        split="eval",
        offset=0.2,
        duration=1.0,
        provenance={"row_index": 0},
    )
    composite = _canonical(
        feeds_uri,
        family="bcfy_feeds",
        segment_id="composite",
        text="alpha beta",
        split="train",
        offset=0.0,
        duration=3.0,
        provenance={
            "construction": "S-N-S",
            "first_raw_row": 0,
            "last_raw_row": 2,
            "pool": "20260619",
        },
    )
    corrected = _canonical(
        calls_train_uri,
        family="broadcastify_calls",
        segment_id="000000",
        text="corrected target",
        split="train",
        offset=0.25,
        duration=0.75,
    )
    bundle = _write_bundle(
        tmp_path,
        raw_rows={
            "calls_train": [
                _raw(
                    calls_train_uri,
                    offset=0.1,
                    duration=1.0,
                    text="raw target",
                )
            ],
            "calls_eval": [
                _raw(
                    calls_eval_uri,
                    offset=0.2,
                    duration=1.0,
                    text="eval speech",
                )
            ],
            "feeds_train": [
                _raw(feeds_uri, offset=0, duration=1, text="alpha"),
                _raw(
                    feeds_uri,
                    offset=1,
                    duration=1,
                    category="UNINTELLIGIBLE",
                ),
                _raw(feeds_uri, offset=2, duration=1, text="beta"),
            ],
        },
        train=[current, composite],
        eval_rows=[legacy],
        closure=[corrected],
    )

    result = ownership.freeze_ownership(
        bundle,
        enforce_known_run=False,
        validate_frozen_policy=False,
    )

    assert len(result.owners) == 4
    by_locator = result.owner_by_locator()
    corrected_owner = by_locator[inputs.RawLocator("calls_train", 0)]
    assert corrected_owner.authoritative_text == "corrected target"
    assert str(corrected_owner.authoritative_offset_seconds) == "0.25"
    assert corrected_owner.target_origin == "closure"
    assert corrected_owner.closure_row_index == 0
    composite_owners = [
        by_locator[inputs.RawLocator("feeds_train", index)] for index in (0, 2)
    ]
    assert [owner.authoritative_text for owner in composite_owners] == [
        "alpha",
        "beta",
    ]
    assert all(
        owner.target_origin == "composite_constituent"
        for owner in composite_owners
    )
    assert result.receipt["composite_canonical_row_count"] == 1
    assert result.receipt["composite_speech_owner_count"] == 2
    assert len(result.owner_receipt_rows()) == 4
    assert len(result.canonical_mapping_receipt_rows()) == 4
    assert len(result.source_group_receipt_rows()) == len(result.source_groups)


def test_train_wins_complete_fire_recording_group(
    tmp_path: pathlib.Path,
) -> None:
    train_uri = (
        "gs://bucket/fire_notifications/training/"
        "AL-TEST-DISP_202026-04-13_2012-10-27.wav"
    )
    eval_uri = (
        "gs://bucket/fire_notifications/eval/"
        "AL-TEST-DISP_202026-04-13_2012-10-26.wav"
    )
    bundle = _write_bundle(
        tmp_path,
        raw_rows={
            "fire_train": [_raw(train_uri, offset=0, duration=1, text="train")],
            "fire_eval": [_raw(eval_uri, offset=0, duration=1, text="eval")],
        },
        train=[
            _canonical(
                train_uri,
                family="fire_notifications",
                segment_id="000000",
                text="train",
                split="train",
                offset=0,
                duration=1,
            )
        ],
        eval_rows=[
            _canonical(
                eval_uri,
                family="fire_notifications",
                segment_id="000000",
                text="eval",
                split="eval",
                offset=0,
                duration=1,
            )
        ],
        closure=[],
        aliases=[
            {
                "eval_source_uri": eval_uri,
                "filename_timestamp_delta_seconds": -1,
                "train_source_uri": train_uri,
            }
        ],
    )

    result = ownership.freeze_ownership(
        bundle,
        enforce_known_run=False,
        validate_frozen_policy=False,
    )

    assert {owner.final_split for owner in result.owners} == {
        domain.Split.TRAIN
    }
    assert result.receipt["moved_eval_to_train_by_family"] == {
        "fire_notifications": 1
    }
    assert result.receipt["mixed_source_group_count"] == 1


def test_missing_canonical_speech_owner_fails(tmp_path: pathlib.Path) -> None:
    bundle = _write_bundle(
        tmp_path,
        raw_rows={
            "echo_train": [
                _raw(
                    "gs://bucket/echo/training/a.wav",
                    offset=0,
                    duration=1,
                    text="orphan",
                )
            ]
        },
        train=[],
        eval_rows=[],
        closure=[],
    )

    with pytest.raises(ValueError, match="coverage is not exact"):
        ownership.freeze_ownership(
            bundle,
            enforce_known_run=False,
            validate_frozen_policy=False,
        )


def test_duplicate_canonical_owner_fails(tmp_path: pathlib.Path) -> None:
    uri = "gs://bucket/echo/training/a.wav"
    row = _canonical(
        uri,
        family="echo",
        segment_id="000000",
        text="duplicate",
        split="train",
        offset=0,
        duration=1,
    )
    bundle = _write_bundle(
        tmp_path,
        raw_rows={
            "echo_train": [_raw(uri, offset=0, duration=1, text="duplicate")]
        },
        train=[row, row],
        eval_rows=[],
        closure=[],
    )

    with pytest.raises(ValueError, match="multiple canonical rows"):
        ownership.freeze_ownership(
            bundle,
            enforce_known_run=False,
            validate_frozen_policy=False,
        )
