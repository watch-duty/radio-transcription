"""One-time repair for continuous-plan transcript authority receipts.

The exact planner was originally invoked with the minimal
``domain.OwnedSpeech`` projection.  That projection preserved every field that
can affect boundaries, request identities, or target text, but omitted the
rich transcript authority metadata carried by ``stage.StagedOwnedSpeech``.
The normal guarded loader correctly rejected those receipts.

This module does not plan requests.  It validates the old root with the exact
minimal representation that created it, rebinds only transcript
reconciliation receipts to the canonical rich Stage universe, independently
verifies both splits, and promotes a newly verified root while retaining the
old root unchanged under a stale name.
"""

# ruff: noqa: D202, EM101, EM102, PLR0912, PTH105, SLF001, T201, TRY003, TRY004, TRY301

from __future__ import annotations

import argparse
import dataclasses
import hashlib
import json
import os
import pathlib
import shutil
import tempfile
from collections.abc import Mapping, Sequence

from . import (
    artifacts,
    continuous,
    domain,
    inputs,
    media,
    ownership,
    stage,
    transcripts,
)

_SPLITS = (domain.Split.TRAIN, domain.Split.EVAL)
_PLAN_ARTIFACTS = (
    "requests.jsonl",
    "source-receipts.jsonl",
    "split-receipt.json",
)


class ReceiptRebindError(RuntimeError):
    """Raised when a receipt-only repair cannot be proven exact."""


@dataclasses.dataclass(frozen=True, slots=True)
class RebindResult:
    """Paths and verified plans produced by one successful promotion."""

    plan_root: pathlib.Path
    stale_plan_root: pathlib.Path
    repair_receipt_path: pathlib.Path
    plans: Mapping[domain.Split, continuous.ContinuousSplitPlan]


@dataclasses.dataclass(frozen=True, slots=True)
class _PreparedSplit:
    split: domain.Split
    plan: continuous.ContinuousSplitPlan
    request_bytes: bytes
    source_receipts: tuple[dict[str, object], ...]
    split_receipt: dict[str, object]
    success: dict[str, object]
    changed_reconciliation_count: int


def _sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _fsync_directory(path: pathlib.Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _lean_sources(
    sources_by_split: Mapping[
        domain.Split,
        Sequence[continuous.ContinuousSourceInput],
    ],
) -> dict[domain.Split, tuple[continuous.ContinuousSourceInput, ...]]:
    """Project rich owners to the exact minimal representation used before."""

    if set(sources_by_split) != set(_SPLITS):
        raise ReceiptRebindError(
            "continuous sources must contain exactly train and eval"
        )
    result: dict[
        domain.Split,
        tuple[continuous.ContinuousSourceInput, ...],
    ] = {}
    for split in _SPLITS:
        result[split] = tuple(
            dataclasses.replace(
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
            for source in sources_by_split[split]
        )
        rich_digest = continuous.continuous_source_inputs_sha256(
            sources_by_split[split],
            split=split,
        )
        lean_digest = continuous.continuous_source_inputs_sha256(
            result[split],
            split=split,
        )
        if lean_digest != rich_digest:
            raise ReceiptRebindError(
                f"{split.value} lean projection changes frozen geometry"
            )
    return result


def _canonical_reconciliation(
    *,
    request: domain.Request,
    owner_by_id: Mapping[str, object],
) -> dict[str, object]:
    try:
        owners = tuple(owner_by_id[value] for value in request.owner_ids)
    except KeyError as error:
        raise ReceiptRebindError(
            f"request references missing rich owner: {request.request_id}"
        ) from error
    reconciled = transcripts.reconcile_component(owners)
    if (
        reconciled.owner_ids != request.owner_ids
        or reconciled.text != request.text
    ):
        raise ReceiptRebindError(
            "rich reconciliation changes persisted request tuple/text: "
            f"{request.request_id}"
        )
    return {
        "request_id": request.request_id,
        "reconciliation": reconciled.receipt,
    }


def _without_key(
    value: Mapping[str, object],
    key: str,
) -> dict[str, object]:
    return {name: item for name, item in value.items() if name != key}


def _without_source_receipt_hashes(
    receipt: Mapping[str, object],
) -> dict[str, object]:
    copied = json.loads(json.dumps(receipt))
    if not isinstance(copied, dict):
        raise AssertionError("split receipt must remain an object")
    selections = copied.get("source_selections")
    if not isinstance(selections, list):
        raise ReceiptRebindError("split receipt is missing source selections")
    for selection in selections:
        if not isinstance(selection, dict):
            raise ReceiptRebindError("source selection must be an object")
        selection.pop("source_receipt_sha256", None)
    return copied


def _rebind_split(
    *,
    split: domain.Split,
    old_plan: continuous.ContinuousSplitPlan,
    rich_sources: Sequence[continuous.ContinuousSourceInput],
    production_histogram_ms: Mapping[int | str, int],
    request_bytes: bytes,
    old_success: Mapping[str, object],
) -> _PreparedSplit:
    ordered_sources = tuple(
        sorted(
            rich_sources,
            key=lambda source: (
                source.media.uri,
                source.media.generation,
            ),
        )
    )
    if len(old_plan.source_plans) != len(ordered_sources):
        raise ReceiptRebindError(f"{split.value} source cardinality changed")

    repaired_source_plans: list[continuous.ContinuousPlan] = []
    repaired_source_receipts: list[dict[str, object]] = []
    changed_reconciliation_count = 0
    for source, old_source_plan in zip(
        ordered_sources,
        old_plan.source_plans,
        strict=True,
    ):
        source_record = old_source_plan.receipt.get("source")
        if (
            not isinstance(source_record, Mapping)
            or source_record.get("uri") != source.media.uri
            or source_record.get("generation") != source.media.generation
        ):
            raise ReceiptRebindError(
                f"{split.value} source order/identity changed"
            )
        owner_by_id = {owner.atom.atom_id: owner for owner in source.speeches}
        reconciliation = [
            _canonical_reconciliation(
                request=request,
                owner_by_id=owner_by_id,
            )
            for request in old_source_plan.requests
        ]
        old_reconciliation = old_source_plan.receipt.get(
            "selected_transcript_reconciliation"
        )
        changed_reconciliation_count += sum(
            before != after
            for before, after in zip(
                (
                    old_reconciliation
                    if isinstance(old_reconciliation, list)
                    else ()
                ),
                reconciliation,
                strict=False,
            )
        )
        repaired_receipt = {
            **old_source_plan.receipt,
            "selected_transcript_reconciliation": reconciliation,
        }
        if _without_key(
            repaired_receipt,
            "selected_transcript_reconciliation",
        ) != _without_key(
            old_source_plan.receipt,
            "selected_transcript_reconciliation",
        ):
            raise AssertionError(
                "source receipt changed outside reconciliation"
            )
        repaired_plan = continuous.ContinuousPlan(
            requests=old_source_plan.requests,
            receipt=repaired_receipt,
        )
        if repaired_plan.requests != old_source_plan.requests:
            raise AssertionError("receipt repair changed source requests")
        repaired_source_plans.append(repaired_plan)
        repaired_source_receipts.append(repaired_receipt)

    selections = old_plan.receipt.get("source_selections")
    if not isinstance(selections, list) or len(selections) != len(
        repaired_source_receipts
    ):
        raise ReceiptRebindError(
            f"{split.value} source selections are incomplete"
        )
    repaired_selections: list[dict[str, object]] = []
    for source, selection, source_receipt in zip(
        ordered_sources,
        selections,
        repaired_source_receipts,
        strict=True,
    ):
        if (
            not isinstance(selection, Mapping)
            or selection.get("source_uri") != source.media.uri
        ):
            raise ReceiptRebindError(
                f"{split.value} source selection order changed"
            )
        repaired_selections.append(
            {
                **selection,
                "source_receipt_sha256": domain.stable_digest(source_receipt),
            }
        )
    repaired_split_receipt = {
        **old_plan.receipt,
        "source_selections": repaired_selections,
    }
    if _without_source_receipt_hashes(
        repaired_split_receipt
    ) != _without_source_receipt_hashes(old_plan.receipt):
        raise AssertionError(
            "split receipt changed outside dependent source hashes"
        )

    repaired_plan = continuous.ContinuousSplitPlan(
        requests=old_plan.requests,
        source_plans=tuple(repaired_source_plans),
        receipt=repaired_split_receipt,
    )
    if repaired_plan.requests != old_plan.requests:
        raise AssertionError("receipt repair changed split requests")
    continuous.verify_persisted_continuous_split(
        repaired_plan,
        sources=ordered_sources,
        production_histogram_ms=production_histogram_ms,
    )

    success = dict(old_success)
    old_request_hash = old_success.get("artifacts", {}).get("requests.jsonl")
    if old_request_hash != _sha256_bytes(request_bytes):
        raise ReceiptRebindError(
            f"{split.value} request bytes do not match guarded hash"
        )
    return _PreparedSplit(
        split=split,
        plan=repaired_plan,
        request_bytes=request_bytes,
        source_receipts=tuple(repaired_source_receipts),
        split_receipt=repaired_split_receipt,
        success=success,
        changed_reconciliation_count=changed_reconciliation_count,
    )


def _read_success(
    directory: pathlib.Path,
) -> dict[str, object]:
    payload = (directory / "_SUCCESS.json").read_bytes()
    try:
        value = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ReceiptRebindError(
            f"invalid success marker: {directory}"
        ) from error
    if (
        not isinstance(value, dict)
        or artifacts.canonical_json_bytes(value) != payload
    ):
        raise ReceiptRebindError(f"noncanonical success marker: {directory}")
    return value


def _write_split(
    *,
    directory: pathlib.Path,
    prepared: _PreparedSplit,
) -> dict[str, object]:
    directory.mkdir(parents=True)
    artifacts.write_bytes_create_only(
        directory / "requests.jsonl",
        prepared.request_bytes,
    )
    artifacts.write_jsonl_create_only(
        directory / "source-receipts.jsonl",
        prepared.source_receipts,
    )
    artifacts.write_json_create_only(
        directory / "split-receipt.json",
        prepared.split_receipt,
    )
    artifact_hashes = {
        name: artifacts.inspect_file(directory / name).sha256
        for name in _PLAN_ARTIFACTS
    }
    old_artifacts = prepared.success.get("artifacts")
    if not isinstance(old_artifacts, Mapping):
        raise ReceiptRebindError("old success marker lacks artifact hashes")
    if artifact_hashes["requests.jsonl"] != old_artifacts.get("requests.jsonl"):
        raise AssertionError("receipt repair changed request bytes/hash")
    repaired_success = {
        **prepared.success,
        "artifacts": artifact_hashes,
    }
    if _without_key(
        repaired_success,
        "artifacts",
    ) != _without_key(prepared.success, "artifacts"):
        raise AssertionError("success marker changed outside artifact hashes")
    artifacts.write_json_create_only(
        directory / "_SUCCESS.json",
        repaired_success,
    )
    return repaired_success


def _load_verified(
    plan_root: pathlib.Path,
    *,
    universe: stage.FrameUniverse,
    sources_by_split: Mapping[
        domain.Split,
        Sequence[continuous.ContinuousSourceInput],
    ],
    production_histogram_ms: Mapping[int | str, int],
    ownership_receipt: pathlib.Path,
    source_inventory_receipt: pathlib.Path,
    production_duration_target: pathlib.Path,
) -> dict[domain.Split, continuous.ContinuousSplitPlan]:
    return stage.load_guarded_continuous_plans(
        plan_root,
        universe=universe,
        sources_by_split=sources_by_split,
        production_histogram_ms=production_histogram_ms,
        ownership_receipt=ownership_receipt,
        source_inventory_receipt=source_inventory_receipt,
        production_duration_target=production_duration_target,
    )


def _repair_receipt(
    *,
    stale_plan_root: pathlib.Path,
    prepared: Mapping[domain.Split, _PreparedSplit],
    repaired_success: Mapping[domain.Split, Mapping[str, object]],
) -> dict[str, object]:
    splits: dict[str, object] = {}
    for split in _SPLITS:
        old_success_bytes = (
            stale_plan_root / split.value / "_SUCCESS.json"
        ).read_bytes()
        new_success_bytes = artifacts.canonical_json_bytes(
            repaired_success[split]
        )
        splits[split.value] = {
            "changed_reconciliation_count": prepared[
                split
            ].changed_reconciliation_count,
            "request_count": len(prepared[split].plan.requests),
            "request_sha256": _sha256_bytes(prepared[split].request_bytes),
            "source_count": len(prepared[split].plan.source_plans),
            "old_success_sha256": _sha256_bytes(old_success_bytes),
            "new_success_sha256": _sha256_bytes(new_success_bytes),
            "old_source_receipts_sha256": artifacts.inspect_file(
                stale_plan_root / split.value / "source-receipts.jsonl"
            ).sha256,
            "new_source_receipts_sha256": repaired_success[split]["artifacts"][
                "source-receipts.jsonl"
            ],
            "old_split_receipt_sha256": artifacts.inspect_file(
                stale_plan_root / split.value / "split-receipt.json"
            ).sha256,
            "new_split_receipt_sha256": repaired_success[split]["artifacts"][
                "split-receipt.json"
            ],
        }
    return {
        "schema_version": 1,
        "kind": "continuous_transcript_receipt_rebind",
        "reason": (
            "bind persisted transcript reconciliation to canonical rich "
            "Stage authority metadata"
        ),
        "allowed_changes": [
            "source receipt selected_transcript_reconciliation",
            "split receipt source_selections[].source_receipt_sha256",
            "source-receipts.jsonl bytes",
            "split-receipt.json bytes",
            "_SUCCESS.json artifact hashes",
            "new root receipt-rebind.json",
        ],
        "stale_plan_root": str(stale_plan_root),
        "request_bytes_unchanged": True,
        "request_tuples_unchanged": True,
        "candidate_choices_unchanged": True,
        "objectives_unchanged": True,
        "metrics_unchanged": True,
        "public_guarded_loader_verified": True,
        "independent_exact_verifier_verified": True,
        "splits": splits,
    }


def rebind_plan_root(
    plan_root: pathlib.Path,
    *,
    universe: stage.FrameUniverse,
    sources_by_split: Mapping[
        domain.Split,
        Sequence[continuous.ContinuousSourceInput],
    ],
    production_histogram_ms: Mapping[int | str, int],
    ownership_receipt: pathlib.Path,
    source_inventory_receipt: pathlib.Path,
    production_duration_target: pathlib.Path,
) -> RebindResult:
    """Rebind transcript receipts and atomically promote a verified root."""

    root = pathlib.Path(plan_root)
    if not root.is_dir():
        raise ReceiptRebindError(f"plan root does not exist: {root}")
    rich_sources = {split: tuple(sources_by_split[split]) for split in _SPLITS}
    lean_sources = _lean_sources(rich_sources)

    # The unmodified guarded loader first proves that the source artifacts are
    # exactly valid under the minimal representation that created them.
    old_plans = _load_verified(
        root,
        universe=universe,
        sources_by_split=lean_sources,
        production_histogram_ms=production_histogram_ms,
        ownership_receipt=ownership_receipt,
        source_inventory_receipt=source_inventory_receipt,
        production_duration_target=production_duration_target,
    )

    prepared: dict[domain.Split, _PreparedSplit] = {}
    for split in _SPLITS:
        directory = root / split.value
        prepared[split] = _rebind_split(
            split=split,
            old_plan=old_plans[split],
            rich_sources=rich_sources[split],
            production_histogram_ms=production_histogram_ms,
            request_bytes=(directory / "requests.jsonl").read_bytes(),
            old_success=_read_success(directory),
        )

    root_fingerprint = domain.stable_digest(
        {
            split.value: _sha256_bytes(
                (root / split.value / "_SUCCESS.json").read_bytes()
            )
            for split in _SPLITS
        }
    )
    stale_root = root.with_name(
        f"{root.name}.stale-reduced-owner-receipts-{root_fingerprint[:12]}"
    )
    if stale_root.exists():
        raise ReceiptRebindError(
            f"stale plan destination already exists: {stale_root}"
        )

    temporary_root = pathlib.Path(
        tempfile.mkdtemp(
            prefix=f".{root.name}.receipt-rebind.",
            dir=root.parent,
        )
    )
    try:
        repaired_success = {
            split: _write_split(
                directory=temporary_root / split.value,
                prepared=prepared[split],
            )
            for split in _SPLITS
        }
        verified = _load_verified(
            temporary_root,
            universe=universe,
            sources_by_split=rich_sources,
            production_histogram_ms=production_histogram_ms,
            ownership_receipt=ownership_receipt,
            source_inventory_receipt=source_inventory_receipt,
            production_duration_target=production_duration_target,
        )
        for split in _SPLITS:
            if verified[split].requests != old_plans[split].requests:
                raise AssertionError(
                    f"{split.value} public loader observed changed requests"
                )

        # The stale path is deterministic and will hold the unchanged source
        # root after promotion, so it is safe to bind into the repair receipt
        # before the atomic renames.
        os.replace(root, stale_root)
        try:
            repair_receipt = _repair_receipt(
                stale_plan_root=stale_root,
                prepared=prepared,
                repaired_success=repaired_success,
            )
            artifacts.write_json_create_only(
                temporary_root / "receipt-rebind.json",
                repair_receipt,
            )
            _fsync_directory(temporary_root)
            os.replace(temporary_root, root)
            _fsync_directory(root.parent)
        except BaseException:
            if not root.exists() and stale_root.exists():
                os.replace(stale_root, root)
                _fsync_directory(root.parent)
            raise
    except BaseException:
        if temporary_root.exists():
            shutil.rmtree(temporary_root, ignore_errors=True)
        raise

    try:
        final_plans = _load_verified(
            root,
            universe=universe,
            sources_by_split=rich_sources,
            production_histogram_ms=production_histogram_ms,
            ownership_receipt=ownership_receipt,
            source_inventory_receipt=source_inventory_receipt,
            production_duration_target=production_duration_target,
        )
        for split in _SPLITS:
            if final_plans[split].requests != old_plans[split].requests:
                raise AssertionError(f"{split.value} promoted requests changed")
    except BaseException:
        failed_root = root.with_name(
            f".{root.name}.failed-receipt-rebind-{root_fingerprint[:12]}"
        )
        if failed_root.exists():
            raise ReceiptRebindError(
                f"cannot roll back; failed root exists: {failed_root}"
            )
        os.replace(root, failed_root)
        os.replace(stale_root, root)
        _fsync_directory(root.parent)
        raise

    return RebindResult(
        plan_root=root,
        stale_plan_root=stale_root,
        repair_receipt_path=root / "receipt-rebind.json",
        plans=final_plans,
    )


def rebind_from_frozen_inputs(
    *,
    plan_root: pathlib.Path,
    download_root: pathlib.Path,
    frozen_root: pathlib.Path,
    source_inventory_receipt: pathlib.Path,
    ownership_receipt: pathlib.Path,
    production_duration_target: pathlib.Path,
) -> RebindResult:
    """Build the canonical rich Stage universe and run the one-time repair."""

    input_paths = inputs.InputPaths.from_download_root(
        download_root,
        frozen_root=frozen_root,
    )
    bundle = inputs.load_inputs(input_paths)
    owner_result = ownership.freeze_ownership(bundle)
    inventory = media.read_inventory_receipt(source_inventory_receipt)
    universe = stage.build_frame_universe(
        bundle,
        owner_result,
        inventory,
    )
    sources_by_split = stage._continuous_inputs_by_split(universe)
    try:
        target_value = json.loads(
            production_duration_target.read_text(encoding="utf-8")
        )
        production_histogram_ms = target_value["duration_histogram_ms"]
    except (
        OSError,
        UnicodeDecodeError,
        json.JSONDecodeError,
        KeyError,
        TypeError,
    ) as error:
        raise ReceiptRebindError(
            "cannot load production duration histogram"
        ) from error
    if not isinstance(production_histogram_ms, dict):
        raise ReceiptRebindError(
            "production duration histogram must be an object"
        )
    return rebind_plan_root(
        plan_root,
        universe=universe,
        sources_by_split=sources_by_split,
        production_histogram_ms=production_histogram_ms,
        ownership_receipt=ownership_receipt,
        source_inventory_receipt=source_inventory_receipt,
        production_duration_target=production_duration_target,
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Rebind continuous transcript receipts to the canonical rich "
            "Stage owner representation."
        )
    )
    parser.add_argument("--plan-root", required=True, type=pathlib.Path)
    parser.add_argument("--download-root", required=True, type=pathlib.Path)
    parser.add_argument("--frozen-root", required=True, type=pathlib.Path)
    parser.add_argument(
        "--source-inventory-receipt",
        required=True,
        type=pathlib.Path,
    )
    parser.add_argument(
        "--ownership-receipt",
        required=True,
        type=pathlib.Path,
    )
    parser.add_argument(
        "--production-duration-target",
        required=True,
        type=pathlib.Path,
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    result = rebind_from_frozen_inputs(
        plan_root=args.plan_root,
        download_root=args.download_root,
        frozen_root=args.frozen_root,
        source_inventory_receipt=args.source_inventory_receipt,
        ownership_receipt=args.ownership_receipt,
        production_duration_target=args.production_duration_target,
    )
    print(
        json.dumps(
            {
                "plan_root": str(result.plan_root),
                "stale_plan_root": str(result.stale_plan_root),
                "repair_receipt": str(result.repair_receipt_path),
                "splits": {
                    split.value: {
                        "request_count": len(result.plans[split].requests),
                        "source_count": len(result.plans[split].source_plans),
                    }
                    for split in _SPLITS
                },
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
