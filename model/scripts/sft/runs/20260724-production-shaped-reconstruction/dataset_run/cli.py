"""Command-line orchestration for the one-time reconstruction."""

# One-time local CLI: direct diagnostics and fixed /tmp paths are intentional.
# ruff: noqa: D202, EM101, EM102, S108, T201, TC003, TRY003, TRY004

from __future__ import annotations

import argparse
import asyncio
import dataclasses
import json
import pathlib
from collections.abc import Sequence

from common import recording_groups

from . import (
    config,
    eval_views,
    inputs,
    media,
    prior_context_driver,
    stage,
)

RUN_ROOT = pathlib.Path(__file__).resolve().parents[1]
REPO_ROOT = RUN_ROOT.parents[4]
LATEST_FROZEN_ROOT = (
    RUN_ROOT.parent / "20260712-gemini31-flash-lite-a16-lr05-e14" / "frozen"
)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build and verify the next-round SFT datasets.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser(
        "describe",
        help="Print the frozen construction and evaluation scope.",
    )
    stage_parser = subparsers.add_parser(
        "stage",
        help="Materialize and independently verify local artifacts only.",
    )
    stage_parser.add_argument(
        "--run-config",
        type=pathlib.Path,
        default=RUN_ROOT / "run.toml",
    )
    stage_parser.add_argument(
        "--input-root",
        type=pathlib.Path,
        default=pathlib.Path("/tmp/next-round-reconstruction"),
    )
    stage_parser.add_argument(
        "--latest-frozen-root",
        type=pathlib.Path,
        default=LATEST_FROZEN_ROOT,
    )
    stage_parser.add_argument(
        "--source-inventory-receipt",
        type=pathlib.Path,
        required=True,
    )
    stage_parser.add_argument(
        "--ownership-receipt",
        type=pathlib.Path,
        help=(
            "Frozen ownership receipt; defaults to "
            "<input-root>/ownership-receipt.json."
        ),
    )
    stage_parser.add_argument(
        "--continuous-plan-root",
        type=pathlib.Path,
        help=(
            "Reuse completed train/eval continuous plans under this root. "
            "Missing or stale plans fail; they are never recomputed."
        ),
    )
    stage_parser.add_argument(
        "--recording-groups-index",
        type=pathlib.Path,
        required=True,
    )
    stage_parser.add_argument(
        "--masked-retained-proof",
        type=pathlib.Path,
        required=True,
    )
    stage_parser.add_argument(
        "--masked-filter-report",
        type=pathlib.Path,
        required=True,
    )
    stage_parser.add_argument(
        "--output-root",
        type=pathlib.Path,
        help="Override run.toml workspace for local artifacts.",
    )
    stage_parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help="Bounded final-slice materialization workers.",
    )
    stage_parser.add_argument(
        "--ffmpeg-binary",
        type=pathlib.Path,
        default=media.DEFAULT_FFMPEG_BINARY,
        help=(
            "Exact production FFmpeg 6.1.1 binary; SHA-256 and version are "
            "validated against the frozen encoder receipt."
        ),
    )
    prior_parser = subparsers.add_parser(
        "prior-context-eval",
        help=(
            "Run all tuned checkpoints on the staged predicted-context lane; "
            "an existing evidence root is resumed create-only."
        ),
    )
    prior_parser.add_argument(
        "--staged-root",
        type=pathlib.Path,
        required=True,
        help="Completed local reconstruction staging root.",
    )
    prior_parser.add_argument(
        "--checkpoint-config",
        type=pathlib.Path,
        required=True,
        help="Explicit project, endpoint, location, and concurrency JSON.",
    )
    prior_parser.add_argument(
        "--evidence-root",
        type=pathlib.Path,
        required=True,
        help="Create-only attempt/result root; reuse it to resume a crash.",
    )
    prior_parser.add_argument(
        "--repo-root",
        type=pathlib.Path,
        default=REPO_ROOT,
        help="Repository root used to load the current production prompt.",
    )
    return parser


def _load_source_index(path: pathlib.Path) -> recording_groups.SourceIndex:
    try:
        payload = path.read_bytes()
        record = json.loads(payload)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError(
            f"cannot read approved source index: {path}"
        ) from error
    if not isinstance(record, dict):
        raise ValueError("approved source index must be an object")
    raw_universe = record.get("source_universe")
    if not isinstance(raw_universe, list) or not raw_universe:
        raise ValueError("approved source index lacks its source universe")
    try:
        source_universe = tuple(
            recording_groups.ManifestLock(**item) for item in raw_universe
        )
    except (TypeError, ValueError) as error:
        raise ValueError(
            "approved source index contains invalid source-universe locks"
        ) from error
    return recording_groups.source_index_from_bytes(
        payload,
        expected_source_universe=source_universe,
        expected_sha256=stage.MASKED_INDEX_SHA256,
    )


def _stage(arguments: argparse.Namespace) -> int:
    run_config = config.load_run_config(arguments.run_config)
    input_root = arguments.input_root
    output_root = arguments.output_root or run_config.workspace
    masked_root = input_root / "masked"
    index = _load_source_index(arguments.recording_groups_index)
    paths = stage.StagePaths(
        input_paths=inputs.InputPaths.from_download_root(
            input_root,
            frozen_root=arguments.latest_frozen_root,
        ),
        source_inventory_receipt=arguments.source_inventory_receipt,
        ownership_receipt=(
            arguments.ownership_receipt or input_root / "ownership-receipt.json"
        ),
        production_duration_target=(
            RUN_ROOT / "frozen/production_duration_target.json"
        ),
        construction_contract=(RUN_ROOT / "frozen/construction_contract.json"),
        encoder_toolchain_receipt=(RUN_ROOT / "frozen/encoder_toolchain.json"),
        ffmpeg_binary=arguments.ffmpeg_binary,
        masked_paths_by_family={
            family: masked_root / f"{family}.jsonl"
            for family in eval_views.MASKED_DATASET_FAMILIES
        },
        masked_retained_proof=arguments.masked_retained_proof,
        masked_filter_report=arguments.masked_filter_report,
        output_root=output_root,
        repo_root=REPO_ROOT,
        proposed_gcs_prefix=run_config.proposed_gcs_prefix,
        continuous_plan_root=arguments.continuous_plan_root,
    )
    result = stage.stage_local_dataset(
        paths,
        masked_group_index=index,
        max_materialization_workers=arguments.workers,
    )
    print(
        json.dumps(
            {
                "artifact_count": len(result.artifact_receipts),
                "completion_marker": str(result.completion_marker.path),
                "completion_marker_sha256": result.completion_marker.sha256,
                "output_root": str(output_root),
                "status": "COMPLETE",
                "verification": result.verification_report.as_dict(),
            },
            sort_keys=True,
        )
    )
    return 0


@dataclasses.dataclass(frozen=True, slots=True)
class _PriorContextCLIConfig:
    project: str
    targets: tuple[prior_context_driver.CheckpointTarget, ...]
    max_parallel_episodes_per_checkpoint: int


def _load_prior_context_cli_config(
    path: pathlib.Path,
) -> _PriorContextCLIConfig:
    try:
        payload = json.loads(path.read_bytes())
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError(
            f"cannot read prior-context checkpoint config: {path}"
        ) from error
    if not isinstance(payload, dict):
        raise ValueError("prior-context checkpoint config must be an object")
    expected_keys = {
        "max_parallel_episodes_per_checkpoint",
        "project",
        "targets",
    }
    if set(payload) != expected_keys:
        raise ValueError(
            "prior-context checkpoint config keys differ; "
            f"missing={sorted(expected_keys - set(payload))}; "
            f"unexpected={sorted(set(payload) - expected_keys)}"
        )
    project = payload["project"]
    if not isinstance(project, str) or not project.strip():
        raise ValueError("prior-context project must be a nonblank string")
    project = project.strip()
    maximum = payload["max_parallel_episodes_per_checkpoint"]
    if (
        isinstance(maximum, bool)
        or not isinstance(maximum, int)
        or maximum <= 0
    ):
        raise ValueError(
            "max_parallel_episodes_per_checkpoint must be positive"
        )
    raw_targets = payload["targets"]
    if not isinstance(raw_targets, list) or not raw_targets:
        raise ValueError("prior-context targets must be a nonempty list")
    targets: list[prior_context_driver.CheckpointTarget] = []
    target_keys = {"checkpoint_id", "location", "model_resource"}
    for raw_target in raw_targets:
        if not isinstance(raw_target, dict) or set(raw_target) != target_keys:
            raise ValueError(
                "each prior-context target must contain exactly "
                "checkpoint_id, location, and model_resource"
            )
        try:
            target = prior_context_driver.CheckpointTarget(**raw_target)
        except (TypeError, ValueError) as error:
            raise ValueError("prior-context target is invalid") from error
        targets.append(target)
    checkpoint_ids = [target.checkpoint_id for target in targets]
    if len(checkpoint_ids) != len(set(checkpoint_ids)):
        raise ValueError("prior-context checkpoint IDs must be unique")
    return _PriorContextCLIConfig(
        project=project,
        targets=tuple(targets),
        max_parallel_episodes_per_checkpoint=maximum,
    )


async def _run_prior_context_eval_async(
    *,
    settings: _PriorContextCLIConfig,
    schedule: eval_views.PriorContextSchedule,
    media_by_request: dict[
        str,
        prior_context_driver.RequestMediaEvidence,
    ],
    contract: prior_context_driver.ProductionRequestContract,
    evidence_root: pathlib.Path,
) -> tuple[prior_context_driver.CheckpointRunResult, ...]:
    clients: dict[str, object] = {}
    try:
        adapters: dict[str, prior_context_driver.ProviderAdapter] = {}
        for target in settings.targets:
            client = prior_context_driver.create_production_genai_client(
                project=settings.project,
                location=target.location,
            )
            clients[target.checkpoint_id] = client
            adapters[target.checkpoint_id] = (
                prior_context_driver.GoogleGenAIProviderAdapter(
                    client,
                    location=target.location,
                )
            )
        return (
            await prior_context_driver.run_predicted_prior_context_checkpoints(
                schedule=schedule,
                targets=settings.targets,
                adapters_by_checkpoint=adapters,
                contract=contract,
                media_by_request=media_by_request,
                max_parallel_episodes_per_checkpoint=(
                    settings.max_parallel_episodes_per_checkpoint
                ),
                evidence_root=evidence_root,
            )
        )
    finally:
        await asyncio.gather(
            *(
                client.aio.aclose()
                for client in clients.values()
            ),
            return_exceptions=True,
        )


def _prior_context_eval(arguments: argparse.Namespace) -> int:
    settings = _load_prior_context_cli_config(arguments.checkpoint_config)
    schedule, media_by_request = (
        prior_context_driver.load_staged_prior_context_inputs(
            arguments.staged_root
        )
    )
    contract = prior_context_driver.load_production_request_contract(
        arguments.repo_root
    )
    results = asyncio.run(
        _run_prior_context_eval_async(
            settings=settings,
            schedule=schedule,
            media_by_request=media_by_request,
            contract=contract,
            evidence_root=arguments.evidence_root,
        )
    )
    print(
        json.dumps(
            {
                "checkpoints": [
                    {
                        "attempt_category_counts": (
                            result.attempt_category_counts()
                        ),
                        "checkpoint_id": result.target.checkpoint_id,
                        "logical_attempt_count": len(result.attempts),
                        "request_count": len(result.requests),
                        "terminal_category_counts": (
                            result.terminal_category_counts()
                        ),
                    }
                    for result in results
                ],
                "evidence_root": str(arguments.evidence_root),
                "status": "COMPLETE",
            },
            sort_keys=True,
        )
    )
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    """Run one reconstruction command."""

    arguments = _parser().parse_args(argv)
    if arguments.command == "describe":
        print(
            "train=reconstructed; "
            "provider-validation=deterministic-primary-eval-subset; "
            "eval=reconstructed,prior-context-10,masked-v2; "
            "atomic-unmasked=excluded"
        )
        return 0
    if arguments.command == "stage":
        return _stage(arguments)
    if arguments.command == "prior-context-eval":
        return _prior_context_eval(arguments)
    raise ValueError(f"unsupported command: {arguments.command}")
