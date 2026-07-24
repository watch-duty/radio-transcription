"""Regression tests for immutable recording-group construction and gating."""

from __future__ import annotations

import hashlib
import json
import random
from unittest import mock

import pytest
from common import recording_groups as rg


def _lock(role: str) -> rg.ManifestLock:
    return rg.ManifestLock(
        role=role,
        manifest_uri=f"gs://manifests/{role}.jsonl",
        gcs_generation={"train": 11, "validation": 12, "eval": 13}[role],
        sha256=hashlib.sha256(role.encode()).hexdigest(),
    )


def _lineage(
    namespace: str = "dispatch", recording_id: str = "42"
) -> rg.LineageId:
    return rg.LineageId(namespace, "1", recording_id)


def _occurrence(
    uri: str,
    *,
    split: str = "train",
    family: str = "bcfy_feeds",
    generation: int = 1,
    md5: str | None = None,
    rows: int = 1,
    lineage: rg.LineageId | None = None,
    locators: tuple[rg.SourceLocator, ...] = (),
) -> rg.SourceOccurrence:
    return rg.SourceOccurrence(
        split=split,
        dataset_family=family,
        source_uri=uri,
        gcs_generation=generation,
        gcs_size=100,
        gcs_md5=md5,
        row_count=rows,
        locators=locators,
        recording_lineage_id=lineage,
        legacy=lineage is None,
    )


def _approved(
    occurrences: list[rg.SourceOccurrence],
    *,
    lineages: tuple[rg.LineageId, ...] = (),
) -> rg.SourceIndex:
    candidate = rg.build_source_index_candidate(
        occurrences,
        authoritative_lineages=lineages,
        source_universe=(_lock("train"), _lock("validation"), _lock("eval")),
    )
    return rg.approve_source_index(
        candidate,
        expected_grouping_summary_sha256=candidate.grouping_summary_sha256,
    )


def _binding(
    split: str,
    row_index: int,
    example_id: str,
    *,
    locator: rg.SourceLocator | None = None,
    lineages: tuple[rg.LineageId, ...] = (),
) -> rg.RowBinding:
    return rg.RowBinding(
        split=split,
        row_index=row_index,
        row_identity=(example_id, "001"),
        locators=() if locator is None else (locator,),
        recording_lineage_ids=lineages,
    )


def test_lineage_is_namespaced_and_composite_union_is_sorted() -> None:
    row = {
        "source_audio": {
            "recording_lineage_ids": [
                {
                    "producer_namespace": "dispatch-b",
                    "lineage_schema_version": "1",
                    "recording_id": "42",
                },
                {
                    "producer_namespace": "dispatch-a",
                    "lineage_schema_version": "1",
                    "recording_id": "42",
                },
            ]
        }
    }
    assert rg.lineage_ids_from_row(row) == (
        rg.LineageId("dispatch-a", "1", "42"),
        rg.LineageId("dispatch-b", "1", "42"),
    )
    assert _lineage("dispatch-a") != _lineage("dispatch-b")


@pytest.mark.parametrize(
    "value",
    [
        [],
        [{}],
        [
            {
                "producer_namespace": " ",
                "lineage_schema_version": "1",
                "recording_id": "x",
            }
        ],
        [
            {
                "producer_namespace": "p",
                "lineage_schema_version": "1",
                "recording_id": "x",
                "extra": 1,
            }
        ],
        [
            {
                "producer_namespace": "p",
                "lineage_schema_version": "1",
                "recording_id": "x",
            },
            {
                "producer_namespace": "p",
                "lineage_schema_version": "1",
                "recording_id": "x",
            },
        ],
    ],
)
def test_lineage_rejects_empty_malformed_unknown_and_duplicate_values(
    value: object,
) -> None:
    with pytest.raises((TypeError, ValueError)):
        rg.lineage_ids_from_row(
            {"source_audio": {"recording_lineage_ids": value}}
        )


def test_legacy_basename_splits_before_decoding_and_normalizes_only_stem() -> (
    None
):
    parsed = rg.parse_legacy_audio_basename(
        "gs://Bucket/parent%2Fkept/Dispatch%2F%20  CALL%20.wav"
    )
    assert parsed.decoded_filename == "Dispatch/   CALL .wav"
    assert parsed.exact_stem == "Dispatch/   CALL "
    assert parsed.normalized_stem == "dispatch/ call"
    assert parsed.audio_suffix == ".wav"
    assert parsed.source_uri.startswith("gs://Bucket/parent%2Fkept/")


@pytest.mark.parametrize(
    "uri",
    [
        "gs://bucket",
        "gs://bucket/",
        "https://bucket/name.wav",
        "gs:///name.wav",
        "gs://bucket/name.txt",
        "gs://bucket/name.wav?generation=1",
    ],
)
def test_legacy_basename_rejects_invalid_object_or_suffix(uri: str) -> None:
    with pytest.raises(ValueError, match=r"gs://|suffix"):
        rg.parse_legacy_audio_basename(uri)


def test_fire_parser_handles_rollover_without_time_buckets() -> None:
    before = rg.parse_fire_filename_v1(
        "gs://b/train/X_202026-04-13_2023-59-58.wav"
    )
    after = rg.parse_fire_filename_v1(
        "gs://b/eval/Y_202026-04-14_2000-00-03.wav"
    )
    assert (after - before).total_seconds() == 5
    later = rg.parse_fire_filename_v1(
        "gs://b/eval/Y_202026-04-14_2012-10-27.wav"
    )
    earlier = rg.parse_fire_filename_v1(
        "gs://b/eval/Y_202026-04-14_2012-10-26.wav"
    )
    assert (later - earlier).total_seconds() == 1


@pytest.mark.parametrize(
    "uri",
    [
        "gs://b/X_202026-02-30_2012-10-27.wav",
        "gs://b/X_2026-04-14_12-10-27.wav",
        "gs://b/X_202026-04-14_2012-10-27-extra.wav",
    ],
)
def test_fire_parser_rejects_invalid_or_non_v1_names(uri: str) -> None:
    with pytest.raises(ValueError, match="fire_filename_v1"):
        rg.parse_fire_filename_v1(uri)


def test_graph_deduplicates_occurrences_and_keeps_cross_family_exact_object() -> (
    None
):
    uri = "gs://audio/source.wav"
    occurrences = [
        _occurrence(uri, rows=2),
        _occurrence(uri, split="eval", rows=3),
        _occurrence(uri, split="eval", family="bcfy_calls", rows=4),
    ]
    index = _approved(occurrences)
    physical = [s for s in index.record["sources"] if s["kind"] == "gcs_source"]
    assert len(physical) == 2
    feeds = next(s for s in physical if s["dataset_family"] == "bcfy_feeds")
    assert feeds["split_row_counts"] == {"eval": 3, "train": 2}
    assert (
        len(index.group_ids_for_locator(rg.SourceLocator("source_uri", uri)))
        == 1
    )
    assert any(
        "exact_object" in edge["evidence"] for edge in index.record["edges"]
    )


def test_md5_and_legacy_basename_edges_are_auditable_and_family_scoped() -> (
    None
):
    occurrences = [
        _occurrence("gs://a/path/SAME.wav", md5="abc"),
        _occurrence("gs://b/path/other.wav", split="eval", md5="abc"),
        _occurrence("gs://c/path/ same .mp3", split="eval"),
        _occurrence("gs://d/path/SAME.wav", split="eval", family="echo"),
    ]
    index = _approved(occurrences)
    evidence = {
        reason for edge in index.record["edges"] for reason in edge["evidence"]
    }
    assert "equal_nonblank_md5" in evidence
    assert "legacy_normalized_basename" in evidence
    first_group = index.group_ids_for_locator(
        rg.SourceLocator("source_uri", occurrences[0].source_uri)
    )
    cross_family_group = index.group_ids_for_locator(
        rg.SourceLocator("source_uri", occurrences[3].source_uri)
    )
    assert first_group.isdisjoint(cross_family_group)


def test_authoritative_sources_use_lineage_and_never_call_legacy_parsers() -> (
    None
):
    lineage = _lineage()
    occurrences = [
        _occurrence("gs://a/not-a-legacy-name.bin", lineage=lineage),
        _occurrence("gs://b/also-new.bin", split="eval", lineage=lineage),
    ]
    with (
        mock.patch.object(
            rg,
            "parse_legacy_audio_basename",
            side_effect=AssertionError("heuristic called"),
        ),
        mock.patch.object(
            rg,
            "parse_fire_filename_v1",
            side_effect=AssertionError("heuristic called"),
        ),
    ):
        index = _approved(occurrences, lineages=(lineage,))
    assert len(index.group_ids_for_lineage(lineage)) == 1
    assert index.group_ids_for_locator(
        rg.SourceLocator("source_uri", occurrences[0].source_uri)
    ) == index.group_ids_for_lineage(lineage)


def test_mixed_authoritative_and_legacy_connect_only_by_exact_object_or_md5() -> (
    None
):
    lineage = _lineage()
    legacy = _occurrence(
        "gs://legacy/path/same.wav", split="eval", md5="same-bytes"
    )
    authoritative = _occurrence(
        "gs://new/path/same.wav", lineage=lineage, md5="same-bytes"
    )
    index = _approved([legacy, authoritative], lineages=(lineage,))
    evidence = {
        reason for edge in index.record["edges"] for reason in edge["evidence"]
    }
    assert "equal_nonblank_md5" in evidence
    assert "legacy_exact_basename" not in evidence


def test_composite_only_lineages_resolve_as_two_unmerged_groups() -> None:
    left, right = _lineage("a"), _lineage("b")
    index = _approved([], lineages=(left, right))
    binding = _binding("train", 0, "composite", lineages=(left, right))
    assert len(rg.group_ids_for_binding(binding, index)) == 2
    assert index.group_ids_for_lineage(left).isdisjoint(
        index.group_ids_for_lineage(right)
    )
    assert index.source_object_locks() == ()


def test_fire_window_is_inclusive_transitive_and_channel_independent() -> None:
    base = "_202026-04-14_2012-10-"
    occurrences = [
        _occurrence(
            f"gs://a/CHANNEL-A{base}00.wav", family="fire_notifications"
        ),
        _occurrence(
            f"gs://b/OTHER{base}10.wav",
            split="validation",
            family="fire_notifications",
        ),
        _occurrence(
            f"gs://c/THIRD{base}20.wav",
            split="eval",
            family="fire_notifications",
        ),
        _occurrence(
            f"gs://d/FOURTH{base}30.001.wav",
            split="eval",
            family="fire_notifications",
        ),
    ]
    with pytest.raises(ValueError, match="fire_filename_v1"):
        _approved(occurrences)
    occurrences[-1] = _occurrence(
        "gs://d/FOURTH_202026-04-14_2012-10-31.wav",
        split="eval",
        family="fire_notifications",
    )
    index = _approved(occurrences)
    groups = [
        index.group_ids_for_locator(
            rg.SourceLocator("source_uri", occurrence.source_uri)
        )
        for occurrence in occurrences
    ]
    assert groups[0] == groups[1] == groups[2]
    assert groups[3].isdisjoint(groups[2])
    assert (
        index.record["grouping_summary"][
            "maximum_transitive_fire_clock_span_seconds"
        ]
        == 20.0
    )


def test_grouping_summary_treats_validation_as_an_eval_view() -> None:
    shared_md5 = "shared"
    occurrences = [
        _occurrence("gs://a/A.wav", split="eval", md5=shared_md5),
        _occurrence("gs://a/A.wav", split="validation", md5=shared_md5),
        _occurrence("gs://c/C.wav", split="eval", md5=shared_md5),
        _occurrence("gs://c/C.wav", split="validation", md5=shared_md5),
        _occurrence("gs://b/B.wav", split="train"),
    ]

    summary = _approved(occurrences).record["grouping_summary"]

    assert summary["mixed_component_count"] == 0
    assert summary["affected_rows_by_split"] == {}
    assert summary["edge_counts_by_split_pair"] == {"eval": 1}


def test_fire_span_measures_timestamp_edges_not_other_evidence() -> None:
    occurrences = [
        _occurrence(
            "gs://a/A_202026-04-14_2012-10-00.wav",
            family="fire_notifications",
            md5="same",
        ),
        _occurrence(
            "gs://b/B_202026-04-14_2013-10-00.wav",
            split="eval",
            family="fire_notifications",
            md5="same",
        ),
    ]

    summary = _approved(occurrences).record["grouping_summary"]

    assert summary["mixed_component_count"] == 1
    assert summary["maximum_transitive_fire_clock_span_seconds"] == 0.0


def test_index_is_order_invariant_and_approval_and_loading_fail_closed() -> (
    None
):
    occurrences = [
        _occurrence("gs://a/A.wav", split="train", md5="m1"),
        _occurrence("gs://b/B.wav", split="eval", md5="m1"),
        _occurrence("gs://c/C.wav", split="validation"),
    ]
    payloads = set()
    for seed in range(25):
        shuffled = list(occurrences)
        random.Random(seed).shuffle(shuffled)  # noqa: S311 - deterministic order test
        index = _approved(shuffled)
        payloads.add(rg.source_index_to_bytes(index))
    assert len(payloads) == 1
    payload = payloads.pop()
    with (
        mock.patch.object(
            rg,
            "parse_legacy_audio_basename",
            side_effect=AssertionError("runtime heuristic called"),
        ),
        mock.patch.object(
            rg,
            "parse_fire_filename_v1",
            side_effect=AssertionError("runtime heuristic called"),
        ),
    ):
        loaded = rg.source_index_from_bytes(
            payload,
            expected_source_universe=(
                _lock("eval"),
                _lock("train"),
                _lock("validation"),
            ),
            expected_sha256=hashlib.sha256(payload).hexdigest(),
        )
    assert loaded.sha256 == hashlib.sha256(payload).hexdigest()

    candidate = rg.build_source_index_candidate(
        occurrences,
        authoritative_lineages=(),
        source_universe=(_lock("train"), _lock("validation"), _lock("eval")),
    )
    with pytest.raises(ValueError, match="summary"):
        rg.approve_source_index(
            candidate, expected_grouping_summary_sha256="0" * 64
        )
    with pytest.raises(TypeError):
        rg.source_index_to_bytes(candidate)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="unapproved"):
        rg.source_index_from_bytes(
            rg.source_index_candidate_to_bytes(candidate),
            expected_source_universe=(
                _lock("train"),
                _lock("validation"),
                _lock("eval"),
            ),
        )
    noncanonical = json.dumps(json.loads(payload), indent=2).encode()
    with pytest.raises(ValueError, match="canonical"):
        rg.source_index_from_bytes(
            noncanonical,
            expected_source_universe=(
                _lock("train"),
                _lock("validation"),
                _lock("eval"),
            ),
        )
    with pytest.raises(ValueError, match="source universe"):
        rg.source_index_from_bytes(
            payload, expected_source_universe=(_lock("train"),)
        )


def test_builder_rejects_bad_metadata_catalog_and_masked_ambiguity() -> None:
    lineage = _lineage()
    with pytest.raises(ValueError, match="catalog"):
        _approved([_occurrence("gs://a/A.wav", lineage=lineage)])
    duplicate_locator = rg.SourceLocator("masked_example_id", "same", "echo")
    with pytest.raises(ValueError, match="masked_example_id"):
        _approved(
            [
                _occurrence(
                    "gs://a/A.wav",
                    family="echo",
                    locators=(duplicate_locator,),
                ),
                _occurrence(
                    "gs://b/B.wav",
                    split="eval",
                    family="echo",
                    locators=(duplicate_locator,),
                ),
            ]
        )
    with pytest.raises(ValueError, match="duplicate masked_example_id"):
        _approved(
            [
                _occurrence(
                    "gs://a/A.wav",
                    family="echo",
                    locators=(duplicate_locator,),
                ),
                _occurrence(
                    "gs://a/A.wav",
                    split="eval",
                    family="echo",
                    locators=(duplicate_locator,),
                ),
            ]
        )
    with pytest.raises(ValueError, match="metadata"):
        _approved(
            [
                _occurrence("gs://a/A.wav", md5="one"),
                _occurrence("gs://a/A.wav", split="eval", md5="two"),
            ]
        )
    cross_family = _occurrence(
        "gs://same/object.wav",
        family="echo",
        split="eval",
    )
    with pytest.raises(ValueError, match="exact GCS object"):
        _approved(
            [
                _occurrence("gs://same/object.wav", family="bcfy_calls"),
                rg.SourceOccurrence(
                    split=cross_family.split,
                    dataset_family=cross_family.dataset_family,
                    source_uri=cross_family.source_uri,
                    gcs_generation=cross_family.gcs_generation,
                    gcs_size=cross_family.gcs_size + 1,
                    gcs_md5=cross_family.gcs_md5,
                    row_count=cross_family.row_count,
                    locators=cross_family.locators,
                    recording_lineage_id=cross_family.recording_lineage_id,
                    legacy=cross_family.legacy,
                ),
            ]
        )


def test_canonical_and_locator_bindings_use_exact_contracts() -> None:
    lineage = _lineage()
    row = {
        "example_id": "example",
        "segment_id": "segment",
        "original_audio_uri": "gs://fallback/ignored.wav",
        "source_audio": {
            "audio_filepath": "gs://source/original.wav",
            "recording_lineage_ids": [
                {
                    "producer_namespace": lineage.producer_namespace,
                    "lineage_schema_version": lineage.lineage_schema_version,
                    "recording_id": lineage.recording_id,
                }
            ],
        },
    }
    binding = rg.canonical_row_binding(row, split="train", row_index=7)
    assert binding.row_identity == ("example", "segment")
    assert binding.recording_lineage_ids == (lineage,)
    assert binding.locators == ()

    row["source_audio"].pop("recording_lineage_ids")
    binding = rg.canonical_row_binding(row, split="train", row_index=7)
    assert binding.locators == (
        rg.SourceLocator("source_uri", "gs://source/original.wav"),
    )

    locator = rg.SourceLocator("masked_example_id", "raw-id", "bcfy_feeds")
    masked = rg.locator_row_binding(
        split="eval",
        row_index=2,
        row_identity=("masked", "001"),
        locator=locator,
    )
    assert masked.locators == (locator,)


def test_source_uri_multi_record_resolution_requires_one_exact_object_group() -> (
    None
):
    uri = "gs://same/object.wav"
    index = _approved(
        [
            _occurrence(uri, family="a", generation=1),
            _occurrence(uri, family="b", generation=1, split="eval"),
        ]
    )
    with mock.patch.object(
        rg.json,
        "loads",
        side_effect=AssertionError("runtime reparsed the complete index"),
    ):
        assert (
            len(
                rg.group_ids_for_locator(
                    rg.SourceLocator("source_uri", uri), index
                )
            )
            == 1
        )
        assert index.record["schema_version"] == rg.INDEX_SCHEMA_VERSION

    ambiguous = _approved(
        [
            _occurrence(uri, family="a", generation=1),
            _occurrence(uri, family="b", generation=2, split="eval"),
        ]
    )
    with pytest.raises(ValueError, match="ambiguous"):
        rg.group_ids_for_locator(rg.SourceLocator("source_uri", uri), ambiguous)


def test_masked_locator_requires_exactly_one_record_even_when_groups_match() -> (
    None
):
    locator = rg.SourceLocator("masked_example_id", "x", "echo")
    # Builder rejects the ambiguity before group construction, even if MD5 would merge it.
    with pytest.raises(ValueError, match="masked_example_id"):
        _approved(
            [
                _occurrence("gs://a/A.wav", md5="same", locators=(locator,)),
                _occurrence("gs://b/B.wav", md5="same", locators=(locator,)),
            ]
        )
    index = _approved([_occurrence("gs://a/A.wav")])
    with pytest.raises(ValueError, match="did not resolve"):
        rg.group_ids_for_locator(locator, index)


def test_training_boundary_report_is_complete_and_exception_is_bounded() -> (
    None
):
    occurrences: list[rg.SourceOccurrence] = []
    bindings: dict[str, list[rg.RowBinding]] = {
        "train": [],
        "validation": [],
        "eval": [],
    }
    for number in range(7):
        train_uri = f"gs://train/{number}.wav"
        eval_uri = f"gs://eval/{number}.wav"
        occurrences.extend(
            [
                _occurrence(train_uri, split="train", md5=f"same-{number}"),
                _occurrence(eval_uri, split="eval", md5=f"same-{number}"),
            ]
        )
        bindings["train"].append(
            _binding(
                "train",
                number,
                f"train-{number}",
                locator=rg.SourceLocator("source_uri", train_uri),
            )
        )
        bindings["eval"].append(
            _binding(
                "eval",
                number,
                f"eval-{number}",
                locator=rg.SourceLocator("source_uri", eval_uri),
            )
        )
    index = _approved(occurrences)
    report = rg.build_training_boundary_report(
        bindings_by_split=bindings,
        index=index,
        allow_validation_eval_overlap=True,
    )
    assert report["status"] == "fail"
    assert report["all_rows_resolved"] is True
    assert len(report["affected_groups"]) == 7
    for group in report["affected_groups"]:
        assert len(group["sources"]) == 2
        assert len(group["forming_edges"]) == 1
        assert len(group["affected_rows"]) == 2
        assert group["split_row_counts"] == {
            "eval": 1,
            "train": 1,
            "validation": 0,
        }
    with pytest.raises(rg.RecordingGroupLeakageError) as caught:
        rg.require_training_boundary_disjoint(
            bindings_by_split=bindings,
            index=index,
            allow_validation_eval_overlap=True,
        )
    assert caught.value.report == report
    assert "7 affected group" in str(caught.value)
    assert str(caught.value).count("splitgrp-v1-") <= 5


def test_validation_eval_overlap_requires_opt_in_and_passing_report_resolves_all() -> (
    None
):
    uri = "gs://shared/eval.wav"
    train_uri = "gs://train/only.wav"
    index = _approved(
        [
            _occurrence(train_uri),
            _occurrence(uri, split="validation"),
            _occurrence(uri, split="eval"),
        ]
    )
    bindings = {
        "train": [
            _binding(
                "train",
                0,
                "train",
                locator=rg.SourceLocator("source_uri", train_uri),
            )
        ],
        "validation": [
            _binding(
                "validation",
                0,
                "validation",
                locator=rg.SourceLocator("source_uri", uri),
            )
        ],
        "eval": [
            _binding(
                "eval", 0, "eval", locator=rg.SourceLocator("source_uri", uri)
            )
        ],
    }
    with pytest.raises(rg.RecordingGroupLeakageError):
        rg.require_training_boundary_disjoint(
            bindings_by_split=bindings,
            index=index,
            allow_validation_eval_overlap=False,
        )
    report = rg.require_training_boundary_disjoint(
        bindings_by_split=bindings,
        index=index,
        allow_validation_eval_overlap=True,
    )
    assert report["status"] == "pass"
    assert report["all_rows_resolved"] is True
    assert report["training_boundary_intersections"] == {
        "train_eval": [],
        "train_validation": [],
    }


def test_missing_coverage_is_reported_before_the_only_raising_wrapper() -> None:
    index = _approved([_occurrence("gs://train/only.wav")])
    bindings = {
        "train": [
            _binding(
                "train",
                0,
                "missing",
                locator=rg.SourceLocator(
                    "source_uri", "gs://missing/source.wav"
                ),
            )
        ],
        "validation": [],
        "eval": [],
    }
    report = rg.build_training_boundary_report(
        bindings_by_split=bindings,
        index=index,
        allow_validation_eval_overlap=True,
    )
    assert report["status"] == "fail"
    assert report["all_rows_resolved"] is False
    assert report["resolution_errors"][0]["example_id"] == "missing"
    with pytest.raises(rg.RecordingGroupLeakageError) as caught:
        rg.require_training_boundary_disjoint(
            bindings_by_split=bindings,
            index=index,
            allow_validation_eval_overlap=True,
        )
    assert caught.value.report == report
