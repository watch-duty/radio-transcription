"""Offline build of per-talkgroup context records for the bcfy_calls eval set.

Stages (run in order from `main()`):
1. Load eval manifest, extract unique (sid, call_ts) pairs.
2. Probe the live endpoint for historical-pos support (M1 viability).
3. Recover groupId per call: M1 if probe passed, else M2 (group_archives
   enumeration).
4. Fetch group + node metadata for each unique recovered groupId.
5. Write `context_records.json` keyed by groupId.

Outputs (under bcfy_calls_eval/results/<EXPERIMENT_NAME>/):
- context_records.json    -- per-talkgroup descriptive metadata
- recovered_groupids.json -- mapping "{sid}-{ts}" -> groupId
- recovery_report.json    -- recovery rate + method used
"""

from __future__ import annotations

import argparse
import json
import logging
import subprocess
import sys
from pathlib import Path
from typing import Iterable

from bcfy_calls_eval import _broadcastify_client as bc

log = logging.getLogger(__name__)

SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_MANIFEST = (
    "gs://wd-transcription-data/segmented_audio/broadcastify/calls/eval/"
    "audio_raw/batch_manifest.jsonl"
)
EXPERIMENT_NAME = "framing_context_05_2026"  # matches run_eval.py
RESULTS_DIR = SCRIPT_DIR / "results" / EXPERIMENT_NAME


def load_manifest(uri: str) -> list[dict]:
    """Load the eval manifest from gs:// or a local path."""
    if uri.startswith("gs://"):
        raw = subprocess.check_output(["gsutil", "cat", uri], text=True)
    else:
        raw = Path(uri).read_text()
    return [json.loads(line) for line in raw.splitlines() if line.strip()]


def extract_call_pairs(manifest: Iterable[dict]) -> list[tuple[int, int]]:
    """One (sid, call_ts) per unique example_id ('{call_ts}-{sid}')."""
    seen: set[tuple[int, int]] = set()
    pairs: list[tuple[int, int]] = []
    for row in manifest:
        eid = row["example_id"]
        call_ts_str, sid_str = eid.rsplit("-", 1)
        pair = (int(sid_str), int(call_ts_str))
        if pair not in seen:
            seen.add(pair)
            pairs.append(pair)
    return pairs


def probe_m1(sid: int, old_ts: int) -> bc.ProbeResult:
    """Test whether `/calls/v1/live/?sid={sid}&pos={old_ts-1}` returns
    historical calls.

    One API call total. The result drives the Task 5 (M1) vs Task 6 (M2)
    branch in main().
    """
    try:
        calls = bc.live(sid=sid, pos=old_ts - 1)
    except Exception as exc:  # noqa: BLE001
        return bc.ProbeResult(
            works=False, calls_returned=0, note=f"error: {exc}"
        )

    matching = [c for c in calls if abs(int(c.get("ts", 0)) - old_ts) < 60]
    if matching:
        return bc.ProbeResult(
            works=True,
            calls_returned=len(calls),
            note=f"matched {len(matching)} call(s) within +/-60s of target ts",
        )
    if calls:
        return bc.ProbeResult(
            works=True,
            calls_returned=len(calls),
            note="endpoint returned calls but none matched the target ts; "
            "still consistent with historical-pos support",
        )
    return bc.ProbeResult(
        works=False,
        calls_returned=0,
        note="empty response -- likely live-only / no historical lookback",
    )


def _setup_logging() -> None:
    logging.basicConfig(
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        level=logging.INFO,
    )


def main() -> int:
    _setup_logging()
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--manifest", default=DEFAULT_MANIFEST)
    p.add_argument(
        "--probe-only",
        action="store_true",
        help="run only the M1 probe and exit",
    )
    args = p.parse_args()

    log.info("loading manifest %s", args.manifest)
    manifest = load_manifest(args.manifest)
    pairs = extract_call_pairs(manifest)
    log.info(
        "manifest: %d segments, %d unique (sid, ts) pairs, %d unique sids",
        len(manifest),
        len(pairs),
        len({sid for sid, _ in pairs}),
    )

    # Probe -- use the earliest (sid, ts) so we maximally exercise historical
    # lookback (the worst-case scenario for M1).
    pairs_sorted = sorted(pairs, key=lambda p: p[1])
    probe_sid, probe_ts = pairs_sorted[0]
    log.info("probing M1 with sid=%s pos=%s-1", probe_sid, probe_ts)
    res = probe_m1(probe_sid, probe_ts)
    log.info(
        "M1 probe: works=%s calls=%d note=%s",
        res.works,
        res.calls_returned,
        res.note,
    )

    if args.probe_only:
        return 0 if res.works else 2

    # (recovery + metadata fetch are Tasks 5-7)
    raise NotImplementedError("recovery stages added in Tasks 5-7")


if __name__ == "__main__":
    raise SystemExit(main())
