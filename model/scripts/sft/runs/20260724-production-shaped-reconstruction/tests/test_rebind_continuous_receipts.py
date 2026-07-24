"""Regression test for rich-owner continuous-plan receipt rebinding."""

# ruff: noqa: INP001

from __future__ import annotations

import dataclasses
import fractions
import hashlib
import json
import pathlib
import sys

import pytest

RUN_ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(RUN_ROOT) not in sys.path:
    sys.path.insert(0, str(RUN_ROOT))

from dataset_run import (  # noqa: E402
    artifacts,
    continuous,
    domain,
    media,
    ownership,
    rebind_continuous_receipts,
    stage,
    transcripts,
)


def _source(
    *,
    uri: str,
    generation: int,
    cache_path: pathlib.Path,
) -> tuple[media.SourceInventory, domain.SourceMedia]:
    inventory = media.SourceInventory(
        uri=uri,
        generation=generation,
        size_bytes=100,
        md5_hash="AAAAAAAAAAAAAAAAAAAAAA==",
        crc32c="AAAAAA==",
        sha256=f"{generation:064x}",
        format="WAV",
        sample_rate=1_000,
        channels=1,
        subtype="PCM_16",
        frame_count=5_000,
        duration_seconds="5",
        cache_path=cache_path,
    )
    represented = domain.SourceMedia(
        uri=uri,
        generation=generation,
        size_bytes=inventory.size_bytes,
        md5_hash=inventory.md5_hash,
        crc32c=inventory.crc32c,
        sha256=inventory.sha256,
        sample_rate=inventory.sample_rate,
        channels=inventory.channels,
        subtype=inventory.subtype,
        frame_count=inventory.frame_count,
    )
    return inventory, represented


def _rich_owner(
    *,
    atom_id: str,
    uri: str,
    split: domain.Split,
    raw_index: int,
) -> stage.StagedOwnedSpeech:
    atom = domain.Atom(
        atom_id=atom_id,
        family="bcfy_feeds",
        source_uri=uri,
        raw_manifest="fixture",
        raw_index=raw_index,
        start_frame=1_000,
        end_frame=2_000,
        atom_class=domain.AtomClass.SPEECH,
        text=f"dispatch {split.value}",
    )
    return stage.StagedOwnedSpeech(
        atom=atom,
        split=split,
        text=f"dispatch {split.value}",
        corrected=True,
        authority_rank=17 + raw_index,
        provenance={
            "canonical_manifest_role": "closure",
            "canonical_row_index": 17 + raw_index,
            "closure_row_index": raw_index,
            "source_group_id": f"group-{split.value}",
            "target_origin": "closure",
        },
    )


def _lean_source(
    source: continuous.ContinuousSourceInput,
) -> continuous.ContinuousSourceInput:
    return dataclasses.replace(
        source,
        speeches=tuple(
            domain.OwnedSpeech(
                atom=owner.atom,
                split=owner.split,
                text=owner.text,
            )
            for owner in source.speeches
        ),
    )


def _fraction(value: fractions.Fraction) -> dict[str, object]:
    return {
        "numerator": value.numerator,
        "denominator": value.denominator,
        "decimal": float(value),
    }


def _request_row(request: domain.Request) -> dict[str, object]:
    duration_frames = request.end_frame - request.start_frame
    return {
        "duration_frames": duration_frames,
        "duration_ms_exact": _fraction(
            fractions.Fraction(
                duration_frames * 1_000,
                request.sample_rate,
            )
        ),
        "end_frame": request.end_frame,
        "family": request.family,
        "owner_ids": list(request.owner_ids),
        "request_id": request.request_id,
        "sample_rate": request.sample_rate,
        "source_uri": request.source_uri,
        "split": request.split.value,
        "start_frame": request.start_frame,
        "text": request.text,
    }


def _write_plan(
    *,
    directory: pathlib.Path,
    plan: continuous.ContinuousSplitPlan,
    guards: dict[str, str],
    metric_sentinel: str,
) -> None:
    directory.mkdir(parents=True)
    split_receipt = {
        **plan.receipt,
        "frozen_input_guards": dict(guards),
        "planning_only_smoke_metrics": {
            "metric_sentinel": metric_sentinel,
        },
    }
    artifacts.write_jsonl_create_only(
        directory / "requests.jsonl",
        (_request_row(request) for request in plan.requests),
    )
    artifacts.write_jsonl_create_only(
        directory / "source-receipts.jsonl",
        (source.receipt for source in plan.source_plans),
    )
    artifacts.write_json_create_only(
        directory / "split-receipt.json",
        split_receipt,
    )
    artifact_hashes = {
        name: artifacts.inspect_file(directory / name).sha256
        for name in (
            "requests.jsonl",
            "source-receipts.jsonl",
            "split-receipt.json",
        )
    }
    artifacts.write_json_create_only(
        directory / "_SUCCESS.json",
        {
            "algorithm": "continuous_bcfy_feeds_global_split_ecdf_v1",
            "artifacts": artifact_hashes,
            "frozen_input_guards": dict(guards),
            "kind": "continuous_planning_only_success",
            "metrics": {
                "metric_sentinel": metric_sentinel,
                "request_count": len(plan.requests),
                "source_count": len(plan.source_plans),
                "speech_owner_count": sum(
                    len(request.owner_ids) for request in plan.requests
                ),
            },
            "schema_version": 1,
            "split": plan.receipt["split"],
        },
    )


def _json(path: pathlib.Path) -> dict[str, object]:
    value = json.loads(path.read_bytes())
    assert isinstance(value, dict)
    return value


def _jsonl(path: pathlib.Path) -> list[dict[str, object]]:
    rows = [json.loads(line) for line in path.read_bytes().splitlines()]
    assert all(isinstance(row, dict) for row in rows)
    return rows


def _without_reconciliation(
    receipt: dict[str, object],
) -> dict[str, object]:
    return {
        key: value
        for key, value in receipt.items()
        if key != "selected_transcript_reconciliation"
    }


def _without_source_receipt_hashes(
    receipt: dict[str, object],
) -> dict[str, object]:
    result = json.loads(json.dumps(receipt))
    for selection in result["source_selections"]:
        selection.pop("source_receipt_sha256")
    return result


def _without_rebound_artifact_hashes(
    success: dict[str, object],
) -> dict[str, object]:
    result = json.loads(json.dumps(success))
    artifacts_record = result["artifacts"]
    artifacts_record.pop("source-receipts.jsonl")
    artifacts_record.pop("split-receipt.json")
    return result


def _fixture(
    tmp_path: pathlib.Path,
) -> tuple[
    pathlib.Path,
    stage.FrameUniverse,
    dict[domain.Split, tuple[continuous.ContinuousSourceInput, ...]],
    dict[str, int],
    pathlib.Path,
    pathlib.Path,
    pathlib.Path,
]:
    train_inventory, train_media = _source(
        uri="gs://fixture/train.wav",
        generation=1,
        cache_path=tmp_path / "train.wav",
    )
    eval_inventory, eval_media = _source(
        uri="gs://fixture/eval.wav",
        generation=2,
        cache_path=tmp_path / "eval.wav",
    )
    train_owner = _rich_owner(
        atom_id="train-owner",
        uri=train_media.uri,
        split=domain.Split.TRAIN,
        raw_index=1,
    )
    eval_owner = _rich_owner(
        atom_id="eval-owner",
        uri=eval_media.uri,
        split=domain.Split.EVAL,
        raw_index=2,
    )
    owner_receipt = {"owner_assignment_sha256": "a" * 64}
    owner_result = ownership.OwnershipResult(
        owners=(),
        source_groups=(),
        mapped_rows=(),
        closure_rows=(),
        receipt=owner_receipt,
    )
    universe = stage.FrameUniverse(
        sources=(train_inventory, eval_inventory),
        source_media={
            train_media.uri: train_media,
            eval_media.uri: eval_media,
        },
        atoms=(train_owner.atom, eval_owner.atom),
        owned_speech=(train_owner, eval_owner),
        ownership=owner_result,
    )
    rich_sources = stage._continuous_inputs_by_split(universe)
    target = {"1": 10, "2": 1}

    ownership_receipt = tmp_path / "ownership-receipt.json"
    source_inventory_receipt = tmp_path / "source-inventory.jsonl"
    production_target = tmp_path / "production-target.json"
    ownership_receipt.write_bytes(artifacts.canonical_json_bytes(owner_receipt))
    source_inventory_receipt.write_bytes(b'{"inventory":"fixture"}\n')
    production_target.write_bytes(b'{"duration_histogram_ms":{}}\n')
    base_guards = stage._continuous_plan_guards(
        universe=universe,
        production_histogram_ms=target,
        ownership_receipt=ownership_receipt,
        source_inventory_receipt=source_inventory_receipt,
        production_duration_target=production_target,
    )

    plan_root = tmp_path / "continuous-plans"
    for split in (domain.Split.TRAIN, domain.Split.EVAL):
        lean_sources = tuple(
            _lean_source(source) for source in rich_sources[split]
        )
        plan = continuous.plan_continuous_split(
            sources=lean_sources,
            production_histogram_ms=target,
        )
        guards = {
            **base_guards,
            "continuous_source_inputs_sha256": (
                continuous.continuous_source_inputs_sha256(
                    rich_sources[split],
                    split=split,
                )
            ),
        }
        plan = dataclasses.replace(
            plan,
            receipt={
                **plan.receipt,
                "frozen_input_guards": guards,
            },
        )
        continuous.verify_persisted_continuous_split(
            plan,
            sources=lean_sources,
            production_histogram_ms=target,
        )
        _write_plan(
            directory=plan_root / split.value,
            plan=plan,
            guards=guards,
            metric_sentinel=f"unchanged-{split.value}",
        )
    return (
        plan_root,
        universe,
        rich_sources,
        target,
        ownership_receipt,
        source_inventory_receipt,
        production_target,
    )


def test_rebinds_only_rich_owner_receipts_then_promotes_verified_root(
    tmp_path: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    (
        plan_root,
        universe,
        rich_sources,
        target,
        ownership_receipt,
        source_inventory_receipt,
        production_target,
    ) = _fixture(tmp_path)

    # This is the real failure mode: all persisted requests are valid and
    # byte-stable, but the old lean-owner reconciliation receipts omit the
    # rich Stage authority metadata.
    with pytest.raises(
        stage.StagingError,
        match="independent receipt binding",
    ):
        stage.load_guarded_continuous_plans(
            plan_root,
            universe=universe,
            sources_by_split=rich_sources,
            production_histogram_ms=target,
            ownership_receipt=ownership_receipt,
            source_inventory_receipt=source_inventory_receipt,
            production_duration_target=production_target,
        )

    before: dict[str, dict[str, object]] = {}
    for split in ("train", "eval"):
        directory = plan_root / split
        before[split] = {
            "requests_bytes": (directory / "requests.jsonl").read_bytes(),
            "source_receipts_bytes": (
                directory / "source-receipts.jsonl"
            ).read_bytes(),
            "split_receipt_bytes": (
                directory / "split-receipt.json"
            ).read_bytes(),
            "success_bytes": (directory / "_SUCCESS.json").read_bytes(),
            "source_receipts": _jsonl(directory / "source-receipts.jsonl"),
            "split_receipt": _json(directory / "split-receipt.json"),
            "success": _json(directory / "_SUCCESS.json"),
        }

    def _planning_is_forbidden(*args: object, **kwargs: object) -> None:
        message = "receipt rebind must never run planning"
        raise AssertionError(message)

    monkeypatch.setattr(
        continuous,
        "plan_continuous_split",
        _planning_is_forbidden,
    )
    result = rebind_continuous_receipts.rebind_plan_root(
        plan_root,
        universe=universe,
        sources_by_split=rich_sources,
        production_histogram_ms=target,
        ownership_receipt=ownership_receipt,
        source_inventory_receipt=source_inventory_receipt,
        production_duration_target=production_target,
    )

    assert result.plan_root == plan_root
    assert result.stale_plan_root.is_dir()
    assert result.repair_receipt_path == plan_root / "receipt-rebind.json"

    loaded = stage.load_guarded_continuous_plans(
        plan_root,
        universe=universe,
        sources_by_split=rich_sources,
        production_histogram_ms=target,
        ownership_receipt=ownership_receipt,
        source_inventory_receipt=source_inventory_receipt,
        production_duration_target=production_target,
    )
    assert set(loaded) == {domain.Split.TRAIN, domain.Split.EVAL}

    for split in ("train", "eval"):
        directory = plan_root / split
        stale_directory = result.stale_plan_root / split
        old = before[split]
        assert (directory / "requests.jsonl").read_bytes() == old[
            "requests_bytes"
        ]
        assert (stale_directory / "requests.jsonl").read_bytes() == old[
            "requests_bytes"
        ]
        assert (stale_directory / "source-receipts.jsonl").read_bytes() == old[
            "source_receipts_bytes"
        ]
        assert (stale_directory / "split-receipt.json").read_bytes() == old[
            "split_receipt_bytes"
        ]
        assert (stale_directory / "_SUCCESS.json").read_bytes() == old[
            "success_bytes"
        ]

        repaired_sources = _jsonl(directory / "source-receipts.jsonl")
        assert len(repaired_sources) == len(old["source_receipts"])
        for old_source, repaired_source in zip(
            old["source_receipts"],
            repaired_sources,
            strict=True,
        ):
            assert _without_reconciliation(
                repaired_source
            ) == _without_reconciliation(old_source)
            assert (
                repaired_source["selected_transcript_reconciliation"]
                != old_source["selected_transcript_reconciliation"]
            )
        split_enum = domain.Split(split)
        expected_rich = transcripts.reconcile_component(
            rich_sources[split_enum][0].speeches
        ).receipt
        assert (
            repaired_sources[0]["selected_transcript_reconciliation"][0][
                "reconciliation"
            ]
            == expected_rich
        )

        repaired_split = _json(directory / "split-receipt.json")
        assert _without_source_receipt_hashes(
            repaired_split
        ) == _without_source_receipt_hashes(old["split_receipt"])
        repaired_success = _json(directory / "_SUCCESS.json")
        assert _without_rebound_artifact_hashes(
            repaired_success
        ) == _without_rebound_artifact_hashes(old["success"])
        assert repaired_success["metrics"] == old["success"]["metrics"]
        assert (
            repaired_success["frozen_input_guards"]
            == old["success"]["frozen_input_guards"]
        )
        assert (
            repaired_success["artifacts"]["requests.jsonl"]
            == hashlib.sha256(old["requests_bytes"]).hexdigest()
        )
        assert (
            repaired_success["artifacts"]["source-receipts.jsonl"]
            != old["success"]["artifacts"]["source-receipts.jsonl"]
        )
        assert (
            repaired_success["artifacts"]["split-receipt.json"]
            != old["success"]["artifacts"]["split-receipt.json"]
        )

    repair = _json(result.repair_receipt_path)
    assert repair["kind"] == "continuous_transcript_receipt_rebind"
    assert repair["request_bytes_unchanged"] is True
    assert repair["request_tuples_unchanged"] is True
    assert repair["candidate_choices_unchanged"] is True
    assert repair["objectives_unchanged"] is True
    assert repair["metrics_unchanged"] is True
    assert repair["public_guarded_loader_verified"] is True
