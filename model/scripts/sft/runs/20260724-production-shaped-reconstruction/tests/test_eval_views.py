"""Tests for the frozen reconstructed evaluation views."""

# ruff: noqa: EM101, INP001, TC002, TRY003

from __future__ import annotations

import pathlib
import sys

import pytest

RUN_ROOT = pathlib.Path(__file__).resolve().parents[1]
MODEL_SRC = pathlib.Path(__file__).resolve().parents[5] / "src"
for path in (RUN_ROOT, MODEL_SRC):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from common import recording_groups as groups  # noqa: E402
from dataset_run import eval_views  # noqa: E402


class FakeIndex:
    """Small exact binding resolver for filter unit tests."""

    sha256 = "a" * 64
    policy_receipt = {"policy": "frozen-test-fire-alias-policy"}

    def __init__(self, mapping: dict[tuple[str, str | None, str], str]) -> None:
        self._mapping = mapping

    def group_ids_for_binding(
        self,
        binding: groups.RowBinding,
    ) -> frozenset[str]:
        resolved: set[str] = set()
        for locator in binding.locators:
            key = (locator.kind, locator.dataset_family, locator.value)
            try:
                resolved.add(self._mapping[key])
            except KeyError:
                continue
        if not resolved:
            raise ValueError("unresolved test binding")
        return frozenset(resolved)


def _commit(
    builder: eval_views.PredictedPriorContextBuilder,
    envelope: eval_views.PriorContextEnvelope,
    prediction: str | None,
    *,
    checkpoint_id: str,
    accepted: bool = True,
) -> None:
    builder.record_prediction(
        eval_views.PredictionOutcome(
            checkpoint_id=checkpoint_id,
            request_id=envelope.request.request_id,
            model_input_sha256=envelope.receipt["model_input_sha256"],
            prediction=prediction,
            accepted=accepted,
            provider_request_fingerprint=(
                f"provider-{envelope.request.request_id}"
            ),
        )
    )


def _primary_row(
    request_id: str,
    *,
    source_uri: str,
    offset: float,
    text: str,
    family: str = "bcfy_feeds",
) -> dict[str, object]:
    start_frame = int(offset * 100)
    end_frame = start_frame + 100
    context_episode_id = request_id if family == "echo" else source_uri
    return {
        "audio_filepath": f"gs://materialized/{request_id}.flac",
        "context_episode_id": context_episode_id,
        "dataset": {"family": family, "name": family},
        "duration": 1.0,
        "example_id": request_id,
        "offset": offset,
        "request_id": request_id,
        "segment_id": "0",
        "source_audio": {
            "audio_filepath": source_uri,
            "duration": 1.0,
            "end_frame": end_frame,
            "offset": offset,
            "sample_rate": 100,
            "start_frame": start_frame,
        },
        "source_lineage": {"context_episode_id": context_episode_id},
        "split": "eval",
        "text": text,
    }


def _train_row(
    request_id: str,
    source_uri: str,
) -> dict[str, object]:
    row = _primary_row(
        request_id,
        source_uri=source_uri,
        offset=0.0,
        text="train target",
    )
    row["split"] = "train"
    return row


def _masked_row(
    family: str,
    example_id: str,
) -> dict[str, object]:
    return {
        "audio_filepath": f"gs://masked/{example_id}.flac",
        "dataset_family": family,
        "duration": 2.5,
        "example_id": example_id,
        "offset": 3.25,
        "segment_id": "7",
        "split": "eval",
        "text": "masked reference",
    }


def test_primary_eval_and_provider_validation_are_separate_views() -> None:
    primary = eval_views.prepare_primary_eval(
        [
            _primary_row(
                "a",
                source_uri="gs://raw/source.wav",
                offset=0.0,
                text="reference",
            )
        ]
    )

    validation = eval_views.prepare_provider_validation(
        primary.rows,
        train_count=10,
        input_lock_digest=str(primary.receipt["eval_sha256"]),
    )

    assert len(primary.rows) == 1
    assert validation.rows == primary.rows
    assert (
        validation.receipt["full_primary_sha256"]
        == (primary.receipt["eval_sha256"])
    )


def test_provider_validation_uses_provider_safe_target_and_exact_subset() -> (
    None
):
    rows = [
        _primary_row(
            f"row-{index:04d}",
            source_uri=f"gs://raw/source-{index % 100:03d}.wav",
            offset=float(index),
            text=(
                "one two"
                if index % 2 == 0
                else "one two three four five six seven eight nine ten"
            ),
            family="bcfy_calls" if index % 3 == 0 else "bcfy_feeds",
        )
        for index in range(1_001)
    ]
    primary = eval_views.prepare_primary_eval(rows)

    validation = eval_views.prepare_provider_validation(
        primary.rows,
        train_count=3_334,
        input_lock_digest=str(primary.receipt["eval_sha256"]),
    )

    assert len(primary.rows) == 1_001
    assert len(validation.rows) == 1_000
    assert validation.receipt["target_count"] == 1_000
    assert (
        validation.receipt["input_lock_digest"]
        == (primary.receipt["eval_sha256"])
    )
    assert validation.receipt["selection_order"] == (
        "lexical_stratum_then_hash_ranked_source_round_robin"
    )
    primary_by_id = {row["request_id"]: row for row in primary.rows}
    assert all(
        row == primary_by_id[row["request_id"]] for row in validation.rows
    )
    assert sum(item["quota"] for item in validation.receipt["strata"]) == 1_000


def test_provider_validation_target_reaches_5000_for_real_run_scale() -> None:
    assert (
        eval_views._provider_validation_target(
            eval_count=10_131,
            train_count=50_000,
        )
        == 5_000
    )
    assert (
        eval_views._provider_validation_target(
            eval_count=731,
            train_count=1,
        )
        == 731
    )


def test_echo_resets_context_episode_per_reconstructed_request() -> None:
    source_uri = "gs://raw/echo-hour.wav"
    rows = [
        _primary_row(
            f"echo-{index}",
            source_uri=source_uri,
            offset=float(index),
            text="reference",
            family="echo",
        )
        for index in range(2)
    ]
    schedule = eval_views.build_prior_context_schedule(rows)

    assert [request.context_episode_id for request in schedule.requests] == [
        "echo-0",
        "echo-1",
    ]
    assert [request.source_ordinal for request in schedule.requests] == [0, 0]
    builder = eval_views.PredictedPriorContextBuilder(
        schedule,
        checkpoint_id="checkpoint",
    )
    first = builder.open_request("echo-0")
    _commit(builder, first, "first prediction", checkpoint_id="checkpoint")
    second = builder.open_request("echo-1")
    assert second.prior_predictions == ()


def test_predicted_context_is_ordered_capped_and_resets_by_source() -> None:
    source_a = "gs://raw/a.wav"
    source_b = "gs://raw/b.wav"
    rows = [
        _primary_row(
            f"a-{index}",
            source_uri=source_a,
            offset=float(index),
            text=f"never use reference {index}",
        )
        for index in reversed(range(12))
    ]
    rows.append(
        _primary_row(
            "b-0",
            source_uri=source_b,
            offset=0.0,
            text="never use reference b",
        )
    )
    schedule = eval_views.build_prior_context_schedule(rows)
    builder = eval_views.PredictedPriorContextBuilder(
        schedule,
        checkpoint_id="checkpoint-5",
    )

    first_b = builder.open_request("b-0")
    assert first_b.prior_predictions == ()
    _commit(
        builder,
        first_b,
        "predicted b",
        checkpoint_id="checkpoint-5",
    )

    for index in range(11):
        request_id = f"a-{index}"
        envelope = builder.open_request(request_id)
        assert "never use reference" not in str(envelope.model_input())
        _commit(
            builder,
            envelope,
            f" predicted {index} ",
            checkpoint_id="checkpoint-5",
        )

    last = builder.open_request("a-11")
    assert [turn.prediction for turn in last.prior_predictions] == [
        f"predicted {index}" for index in range(1, 11)
    ]
    assert last.receipt["reference_transcript_accessed_for_context"] is False
    _commit(
        builder,
        last,
        None,
        checkpoint_id="checkpoint-5",
    )
    receipt = builder.receipt(require_complete=True)
    assert receipt["reference_field_names_in_any_context"] == []
    assert receipt["completed_request_count"] == 13
    assert len(receipt["request_receipts"]) == 13
    assert all(
        item["model_input_sha256"] for item in receipt["request_receipts"]
    )


def test_unintelligible_prediction_is_not_retained_as_context() -> None:
    rows = [
        _primary_row(
            f"a-{index}",
            source_uri="gs://raw/a.wav",
            offset=float(index),
            text="reference",
        )
        for index in range(2)
    ]
    schedule = eval_views.build_prior_context_schedule(rows)
    builder = eval_views.PredictedPriorContextBuilder(
        schedule,
        checkpoint_id="checkpoint-2",
    )

    first = builder.open_request("a-0")
    _commit(
        builder,
        first,
        " [UNINTELLIGIBLE] ",
        checkpoint_id="checkpoint-2",
    )

    assert builder.open_request("a-1").prior_predictions == ()


def test_checkpoint_builders_never_share_prediction_state() -> None:
    rows = [
        _primary_row(
            f"a-{index}",
            source_uri="gs://raw/a.wav",
            offset=float(index),
            text="reference",
        )
        for index in range(2)
    ]
    schedule = eval_views.build_prior_context_schedule(rows)
    first = eval_views.PredictedPriorContextBuilder(
        schedule,
        checkpoint_id="checkpoint-1",
    )
    second = eval_views.PredictedPriorContextBuilder(
        schedule,
        checkpoint_id="checkpoint-2",
    )

    envelope = first.open_request("a-0")
    _commit(
        first,
        envelope,
        "only checkpoint one predicted this",
        checkpoint_id="checkpoint-1",
    )

    assert second.open_request("a-0").prior_predictions == ()


def test_builder_rejects_cross_checkpoint_outcome() -> None:
    rows = [
        _primary_row(
            "a-0",
            source_uri="gs://raw/a.wav",
            offset=0.0,
            text="reference",
        )
    ]
    schedule = eval_views.build_prior_context_schedule(rows)
    builder = eval_views.PredictedPriorContextBuilder(
        schedule,
        checkpoint_id="checkpoint-1",
    )
    envelope = builder.open_request("a-0")
    outcome = eval_views.PredictionOutcome(
        checkpoint_id="checkpoint-2",
        request_id="a-0",
        model_input_sha256=envelope.receipt["model_input_sha256"],
        prediction="wrong checkpoint",
        accepted=True,
    )

    with pytest.raises(ValueError, match="another checkpoint"):
        builder.record_prediction(outcome)


def test_builder_requires_frozen_context_cap_of_ten() -> None:
    rows = [
        _primary_row(
            "a-0",
            source_uri="gs://raw/a.wav",
            offset=0.0,
            text="reference",
        )
    ]
    schedule = eval_views.build_prior_context_schedule(rows)

    with pytest.raises(ValueError, match="max_context=10"):
        eval_views.PredictedPriorContextBuilder(
            schedule,
            checkpoint_id="checkpoint-1",
            max_context=9,
        )


def test_predicted_context_requires_source_chronological_order() -> None:
    rows = [
        _primary_row(
            f"a-{index}",
            source_uri="gs://raw/a.wav",
            offset=float(index),
            text="reference",
        )
        for index in range(2)
    ]
    schedule = eval_views.build_prior_context_schedule(rows)
    builder = eval_views.PredictedPriorContextBuilder(
        schedule,
        checkpoint_id="checkpoint-1",
    )

    with pytest.raises(ValueError, match="out-of-order"):
        builder.open_request("a-1")


def test_schedule_digest_binds_source_boundaries_and_order() -> None:
    rows = [
        _primary_row(
            f"a-{index}",
            source_uri="gs://raw/a.wav",
            offset=float(index),
            text="reference",
        )
        for index in range(2)
    ]
    original = eval_views.build_prior_context_schedule(rows)
    changed_source = [dict(row) for row in rows]
    changed_source[1] = {
        **changed_source[1],
        "context_episode_id": "gs://raw/b.wav",
        "source_audio": {
            **changed_source[1]["source_audio"],
            "audio_filepath": "gs://raw/b.wav",
        },
        "source_lineage": {"context_episode_id": "gs://raw/b.wav"},
    }
    changed = eval_views.build_prior_context_schedule(changed_source)

    assert (
        original.receipt["request_schedule_sha256"]
        != changed.receipt["request_schedule_sha256"]
    )


def test_masked_filter_is_group_disjoint_and_does_not_resegment() -> None:
    train_uri = "gs://raw/train.wav"
    train_rows = [_train_row("train", train_uri)]
    masked = {
        family: [_masked_row(family, f"{family}-kept")]
        for family in eval_views.MASKED_DATASET_FAMILIES
    }
    masked["bcfy_calls"].append(
        _masked_row("bcfy_calls", "bcfy-calls-excluded")
    )
    mapping = {
        ("source_uri", None, train_uri): "training-group",
        (
            "masked_example_id",
            "bcfy_calls",
            "bcfy-calls-excluded",
        ): "training-group",
    }
    for family in eval_views.MASKED_DATASET_FAMILIES:
        mapping[
            (
                "masked_example_id",
                family,
                f"{family}-kept",
            )
        ] = f"eval-group-{family}"
    index = FakeIndex(mapping)

    result = eval_views.refilter_masked_eval(
        train_rows=train_rows,
        masked_rows_by_family=masked,
        index=index,
    )

    kept = result.kept_rows_by_family()
    assert len(kept["bcfy_calls"]) == 1
    assert kept["bcfy_calls"][0] == masked["bcfy_calls"][0]
    assert kept["bcfy_calls"][0]["offset"] == 3.25
    assert kept["bcfy_calls"][0]["duration"] == 2.5
    assert result.receipt["excluded_count"] == 1
    assert result.receipt["rows_resegmented"] is False
    per_family = {item["family"]: item for item in result.receipt["families"]}
    assert per_family["bcfy_calls"]["kept_count"] == 1
    assert per_family["bcfy_calls"]["excluded_count"] == 1


def test_masked_filter_fails_on_unresolved_lineage() -> None:
    train_uri = "gs://raw/train.wav"
    masked = {
        family: [_masked_row(family, f"{family}-row")]
        for family in eval_views.MASKED_DATASET_FAMILIES
    }
    index = FakeIndex({("source_uri", None, train_uri): "training-group"})

    with pytest.raises(
        eval_views.UnresolvedMaskedLineageError,
        match="did not resolve",
    ):
        eval_views.refilter_masked_eval(
            train_rows=[_train_row("train", train_uri)],
            masked_rows_by_family=masked,
            index=index,
        )


def test_masked_filter_requires_exactly_four_frozen_sources() -> None:
    masked = {
        family: [_masked_row(family, f"{family}-row")]
        for family in eval_views.MASKED_DATASET_FAMILIES
        if family != "echo"
    }

    with pytest.raises(ValueError, match="exactly the expected four"):
        eval_views.refilter_masked_eval(
            train_rows=[],
            masked_rows_by_family=masked,
            index=FakeIndex({}),
        )


def test_final_scope_has_only_three_eval_lanes() -> None:
    rows = [
        _primary_row(
            "eval",
            source_uri="gs://raw/eval.wav",
            offset=0.0,
            text="reference",
        )
    ]
    primary = eval_views.prepare_primary_eval(rows)
    provider_validation = eval_views.prepare_provider_validation(
        primary.rows,
        train_count=10,
        input_lock_digest=str(primary.receipt["eval_sha256"]),
    )
    schedule = eval_views.build_prior_context_schedule(rows)
    masked = {
        family: [_masked_row(family, f"{family}-row")]
        for family in eval_views.MASKED_DATASET_FAMILIES
    }
    mapping = {
        (
            "masked_example_id",
            family,
            f"{family}-row",
        ): f"group-{family}"
        for family in eval_views.MASKED_DATASET_FAMILIES
    }
    filtered = eval_views.refilter_masked_eval(
        train_rows=[],
        masked_rows_by_family=masked,
        index=FakeIndex(mapping),
    )

    receipt = eval_views.final_eval_scope_receipt(
        primary=primary,
        provider_validation=provider_validation,
        prior_context=schedule,
        masked=filtered,
    )

    assert set(receipt["eval_lanes"]) == set(eval_views.FINAL_EVAL_LANES)
    assert receipt["eval_lanes"]["predicted_prior_context_10"] == {
        "base_lane": "reconstructed_primary",
        "kind": "inference_view",
        "max_prior_segments": 10,
        "prior_transcript_source": "predicted_transcription",
        "request_count": 1,
        "schedule_sha256": schedule.receipt["request_schedule_sha256"],
        "uses_reference_transcription": False,
    }
    assert receipt["atomic_unmasked_eval_retained"] is False
    assert receipt["provider_validation"]["kind"] == (
        "deterministic_subset_of_reconstructed_primary"
    )
