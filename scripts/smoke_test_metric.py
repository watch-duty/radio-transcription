r"""Smoke-test runbook: write one active_feed_count point, read it back.

Phase 4 VERIFY-02 runbook per 04-CONTEXT.md D-26. Invoke manually during
the release cycle AFTER ops-team Terraform changes that could affect the
custom metric descriptor or label schema. NEVER invoke against production.

    python scripts/smoke_test_metric.py \\
        --project <dev-project-id> \\
        --confirm-project <dev-project-id> \\
        [--instance-id <id> --zone <zone>]

Two independent safety gates defend against accidental production writes:
    1. argparse requires BOTH --project and --confirm-project with no
       defaults. The script asserts they match BEFORE any GCP call.
    2. An interactive y/N prompt echoes the project-id one more time
       before the write.

Resolve vs synthesize labels (Open Tension 3 from 04-CONTEXT.md):
    * If --instance-id AND --zone are both provided, use them as
      synthetic labels (off-GCE path — local laptop, CI container).
      Synthetic path validates descriptor-create + write + read-back
      contract but does NOT prove GCE-specific auth.
    * Else, call metric_reporter.resolve_gce_resource_labels(settings)
      to probe the live metadata server (on-GCE path). If it returns
      None (off-GCE or metadata flake), exit 1 with an error message
      explaining the two options.

Exit codes:
    0 — write + read-back succeeded, descriptor + labels + value match
    1 — mismatch on read-back OR metadata unreachable without overrides
    2 — --project != --confirm-project
    3 — interactive prompt declined (anything other than 'y' / 'Y')
    4 — list_time_series returned no points within the 60s read-back window

Runbook style — NOT a CI gate. See scripts/README.md for details.
"""  # noqa: INP001 — scripts/ is an operator runbook dir, not a package.

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
import time
from dataclasses import dataclass

from google.cloud import monitoring_v3

from backend.pipeline.common.clients.monitoring_client import MonitoringClient
from backend.pipeline.ingestion import metric_reporter
from backend.pipeline.ingestion.slo_contract import (
    METRIC_TYPE_ACTIVE_FEED_COUNT,
    MONITORED_RESOURCE_TYPE,
)

logger = logging.getLogger(__name__)

_EXIT_OK = 0
_EXIT_MISMATCH_OR_UNREACHABLE = 1
_EXIT_PROJECT_MISMATCH = 2
_EXIT_DECLINED = 3
_EXIT_TIMEOUT = 4

_READ_BACK_WAIT_SEC = 60.0
_READ_BACK_WINDOW_SEC = 120.0  # 2-minute look-back for list_time_series
_WRITE_VALUE = 42


@dataclass
class _FakeSettings:
    """Minimal stand-in for NormalizerSettings.

    metric_reporter.resolve_gce_resource_labels only reads
    .google_cloud_project for its fallback path; everything else it pulls
    from the metadata server directly. This dataclass provides exactly
    that attribute so the probe call signature is satisfied.
    """

    google_cloud_project: str


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Write one active_feed_count point to the dev project's Cloud "
            "Monitoring and read it back; asserts descriptor + label "
            "schema match. Refuses production writes via echo-match."
        ),
    )
    parser.add_argument(
        "--project",
        required=True,
        help="GCP project ID to write to. Must match --confirm-project.",
    )
    parser.add_argument(
        "--confirm-project",
        required=True,
        dest="confirm_project",
        help=(
            "Must exactly equal --project. Safety gate against typos "
            "and wrong-project invocations."
        ),
    )
    parser.add_argument(
        "--instance-id",
        default=None,
        dest="instance_id",
        help=(
            "Synthetic instance_id for off-GCE invocations. Pair with "
            "--zone. If absent, script probes GCE metadata server."
        ),
    )
    parser.add_argument(
        "--zone",
        default=None,
        help=(
            "Synthetic zone for off-GCE invocations (e.g. us-central1-a). "
            "Pair with --instance-id."
        ),
    )
    return parser.parse_args()


def _prompt_continue(project: str) -> bool:
    """Interactive y/N confirmation — returns True only on 'y' / 'Y'."""
    print(f"WILL WRITE A TIME SERIES TO PROJECT: {project}")  # noqa: T201
    print("CONTINUE? [y/N] ", end="", flush=True)  # noqa: T201
    try:
        response = input().strip().lower()
    except EOFError:
        return False
    return response == "y"


async def _resolve_labels(args: argparse.Namespace) -> dict[str, str] | None:
    """Pick between synthetic (overrides) and live GCE metadata probe."""
    if args.instance_id and args.zone:
        return {
            "project_id": args.project,
            "instance_id": args.instance_id,
            "zone": args.zone,
        }
    settings = _FakeSettings(google_cloud_project=args.project)
    return await metric_reporter.resolve_gce_resource_labels(settings)  # type: ignore[arg-type]


async def _read_back_one_point(
    client: MonitoringClient,
    project: str,
) -> monitoring_v3.Point | None:
    """Query list_time_series for the single point we just wrote.

    Returns the matching monitoring_v3.Point on success, None if the
    2-minute window is empty (read-back timeout).
    """
    now = time.time()
    interval = monitoring_v3.TimeInterval(
        start_time={"seconds": int(now - _READ_BACK_WINDOW_SEC)},
        end_time={"seconds": int(now)},
    )
    request = monitoring_v3.ListTimeSeriesRequest(
        name=f"projects/{project}",
        filter=f'metric.type = "{METRIC_TYPE_ACTIVE_FEED_COUNT}"',
        interval=interval,
        view=monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL,
    )
    async_client = client._get_client()  # noqa: SLF001 — read-back reuses client
    pager = await async_client.list_time_series(request=request)
    async for series in pager:
        if series.metric.type != METRIC_TYPE_ACTIVE_FEED_COUNT:
            continue
        if series.resource.type != MONITORED_RESOURCE_TYPE:
            continue
        for point in series.points:
            if point.value.int64_value == _WRITE_VALUE:
                return point
    return None


async def _run(args: argparse.Namespace) -> int:
    # Gate 1: echo-match (pre-validated by argparse requiredness, but
    # equality isn't).
    if args.project != args.confirm_project:
        print(  # noqa: T201
            f"--project ({args.project!r}) does not match "
            f"--confirm-project ({args.confirm_project!r}) — refusing to "
            "run.",
            file=sys.stderr,
        )
        return _EXIT_PROJECT_MISMATCH

    # Gate 2: interactive y/N.
    if not _prompt_continue(args.project):
        print("Declined — exiting without writing.", file=sys.stderr)  # noqa: T201
        return _EXIT_DECLINED

    labels = await _resolve_labels(args)
    if labels is None:
        print(  # noqa: T201
            "metadata server unreachable — pass --instance-id AND --zone "
            "to use synthetic labels",
            file=sys.stderr,
        )
        return _EXIT_MISMATCH_OR_UNREACHABLE

    client = MonitoringClient(args.project)
    await client.write_time_series(
        metric_type=METRIC_TYPE_ACTIVE_FEED_COUNT,
        labels={},
        value=_WRITE_VALUE,
        resource_type=MONITORED_RESOURCE_TYPE,
        resource_labels=labels,
    )
    print(  # noqa: T201
        f"Wrote 1 point (value={_WRITE_VALUE}); waiting "
        f"{_READ_BACK_WAIT_SEC:.0f}s for propagation...",
    )
    await asyncio.sleep(_READ_BACK_WAIT_SEC)

    point = await _read_back_one_point(client, args.project)
    if point is None:
        print(  # noqa: T201
            f"read-back timeout: no points returned within "
            f"{_READ_BACK_WINDOW_SEC:.0f}s window",
            file=sys.stderr,
        )
        return _EXIT_TIMEOUT

    if point.value.int64_value != _WRITE_VALUE:
        print(  # noqa: T201
            f"value mismatch: expected {_WRITE_VALUE}, got "
            f"{point.value.int64_value}",
            file=sys.stderr,
        )
        return _EXIT_MISMATCH_OR_UNREACHABLE

    print("SMOKE PASS: point written and read back")  # noqa: T201
    return _EXIT_OK


def main() -> int:
    logging.basicConfig(level=logging.INFO)
    args = _parse_args()
    return asyncio.run(_run(args))


if __name__ == "__main__":
    sys.exit(main())
