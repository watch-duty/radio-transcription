"""Offline build of per-talkgroup context records for the bcfy_calls eval set.

Stages (run in order from `main()`):
1. Load eval manifest, extract unique (sid, call_ts) pairs.
2. Probe the live endpoint for historical-pos support (M1 viability).
3. Recover groupId per call:
   - M1 if probe passed: paginated live polls per sid (cheap).
   - M2 if probe failed: enumerate candidate talkgroups per sid (via the
     county-scoped `groups_ctid`), then `group_archives` per (group, <=8h
     window). Match returned calls by ts.
4. Fetch group + node metadata for each unique recovered groupId
   (`group_get` + `node_get` on the most-active node for county/state).
5. Write `context_records.json` + `recovery_report.json`.

Throttling lives in `_broadcastify_client.py` (5s for live per Broadcastify
spec, 1s defensive for other Calls endpoints).

Outputs (under bcfy_calls_eval/results/<EXPERIMENT_NAME>/):
- context_records.json    -- per-talkgroup descriptive metadata (keyed by groupId)
- recovered_groupids.json -- mapping "{sid}-{ts}" -> groupId
- recovery_report.json    -- recovery rate + method used + per-sid breakdown
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


# ---------------------------------------------------------------------------
# Manifest loading + pair extraction
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# M1 probe
# ---------------------------------------------------------------------------


def probe_m1(sid: int, old_ts: int) -> bc.ProbeResult:
    """Test whether `/calls/v1/live/?sid={sid}&pos={old_ts-1}` returns
    historical calls. One API call total.
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


# ---------------------------------------------------------------------------
# M1 recovery (paginated per-sid live polls — kept for completeness)
# ---------------------------------------------------------------------------


def recover_via_m1(
    pairs: list[tuple[int, int]],
) -> dict[tuple[int, int], str]:
    by_sid: dict[int, list[int]] = {}
    for sid, ts in pairs:
        by_sid.setdefault(sid, []).append(ts)

    out: dict[tuple[int, int], str] = {}
    for sid, ts_list in by_sid.items():
        start = min(ts_list) - 1
        log.info("M1 poll: sid=%s pos=%s (covers %d calls)", sid, start, len(ts_list))
        pos = start
        seen_lastpos: set[int] = set()
        while True:
            calls = bc.live(sid=sid, pos=pos)
            if not calls:
                break
            for c in calls:
                key = (sid, int(c["ts"]))
                if key in out or "groupId" not in c:
                    continue
                out[key] = c["groupId"]
            new_pos = max(int(c["ts"]) for c in calls)
            if new_pos in seen_lastpos or new_pos <= pos:
                break
            seen_lastpos.add(new_pos)
            pos = new_pos
            if pos > max(ts_list):
                break
        matched = sum(1 for (s, _) in out if s == sid)
        log.info("  sid=%s recovered %d/%d", sid, matched, len(ts_list))
    return out


# ---------------------------------------------------------------------------
# M2 recovery (group_archives enumeration — primary path for this eval set)
# ---------------------------------------------------------------------------


def find_ctids_for_sid(sid: int) -> set[int]:
    """Find counties associated with a sid via a one-shot live(sid) poll
    + node_get on each unique nodeId seen.

    Returns empty set if sid is currently quiet (no recent calls). Those sids
    cannot be M2-recovered and are reported as lost in the recovery report.
    """
    try:
        calls = bc.live(sid=sid)
    except Exception as exc:  # noqa: BLE001
        log.warning("live(sid=%s) failed: %s", sid, exc)
        return set()
    if not calls:
        return set()
    node_ids = {int(c["nodeId"]) for c in calls if "nodeId" in c}
    ctids: set[int] = set()
    for nid in node_ids:
        try:
            n = bc.node(nid)
            ctids.add(int(n["ctid"]))
        except Exception as exc:  # noqa: BLE001
            log.warning("node(nodeId=%s) failed: %s", nid, exc)
    return ctids


def enumerate_talkgroups_by_sid(sids: list[int]) -> dict[int, list[str]]:
    """For each sid in `sids`, build the list of candidate groupIds — talkgroups
    on that sid as known to Broadcastify (via county enumeration).

    Strategy: find each sid's counties (live -> nodeId -> node_get -> ctid), then
    one `groups_ctid` per unique county, partition results by sid.
    """
    sid_to_ctids: dict[int, set[int]] = {}
    for sid in sids:
        ctids = find_ctids_for_sid(sid)
        sid_to_ctids[sid] = ctids
        log.info("sid=%s -> %d counties: %s", sid, len(ctids), sorted(ctids))

    all_ctids = set().union(*sid_to_ctids.values()) if sid_to_ctids else set()
    log.info(
        "fetching groups_ctid for %d unique counties (one call each, throttled)",
        len(all_ctids),
    )
    county_groups: dict[int, list[dict]] = {}
    for ctid in sorted(all_ctids):
        try:
            groups = bc.groups_ctid(ctid)
            county_groups[ctid] = groups
            log.info("  ctid=%s -> %d talkgroups", ctid, len(groups))
        except Exception as exc:  # noqa: BLE001
            log.warning("groups_ctid(%s) failed: %s", ctid, exc)
            county_groups[ctid] = []

    sid_to_groups: dict[int, list[str]] = {}
    for sid, ctids in sid_to_ctids.items():
        candidates: set[str] = set()
        for ctid in ctids:
            for g in county_groups.get(ctid, []):
                if int(g.get("sid", 0)) == sid:
                    candidates.add(g["groupId"])
        sid_to_groups[sid] = sorted(candidates)
        log.info("sid=%s -> %d candidate talkgroups", sid, len(candidates))
    return sid_to_groups


def _eight_hour_windows(ts_sorted: list[int]) -> list[tuple[int, int]]:
    """Cover the range of ts_sorted with non-overlapping <=28800s windows."""
    if not ts_sorted:
        return []
    windows: list[tuple[int, int]] = []
    start = ts_sorted[0] - 1
    end_limit = ts_sorted[-1] + 1
    while start < end_limit:
        end = min(start + 28800, end_limit)
        windows.append((start, end))
        start = end
    return windows


def recover_via_m2(
    pairs: list[tuple[int, int]],
    sid_to_groups: dict[int, list[str]],
) -> dict[tuple[int, int], str]:
    """Per-sid: query group_archives on each candidate talkgroup over the
    relevant time-windows, match returned calls by ts.

    Optimization: once all of a sid's manifest ts values are recovered,
    skip remaining candidate talkgroups for that sid.
    """
    by_sid: dict[int, list[int]] = {}
    for sid, ts in pairs:
        by_sid.setdefault(sid, []).append(ts)

    out: dict[tuple[int, int], str] = {}
    for sid, ts_list in by_sid.items():
        candidates = sid_to_groups.get(sid, [])
        if not candidates:
            log.warning(
                "sid=%s has no candidate talkgroups; cannot recover its %d calls",
                sid, len(ts_list),
            )
            continue
        windows = _eight_hour_windows(sorted(ts_list))
        log.info(
            "sid=%s: %d candidates x %d windows for %d calls",
            sid, len(candidates), len(windows), len(ts_list),
        )
        remaining = set(ts_list)
        for gid in candidates:
            if not remaining:
                break
            for (w_start, w_end) in windows:
                if not remaining:
                    break
                try:
                    arch = bc.group_archives(gid, w_start, w_end)
                except Exception as exc:  # noqa: BLE001
                    log.warning(
                        "group_archives(%s, %s, %s) failed: %s",
                        gid, w_start, w_end, exc,
                    )
                    continue
                for call in arch.get("calls", []):
                    call_ts = int(call["ts"])
                    if call_ts in remaining:
                        out[(sid, call_ts)] = gid
                        remaining.discard(call_ts)
        matched = len(ts_list) - len(remaining)
        log.info("  sid=%s recovered %d/%d", sid, matched, len(ts_list))
    return out


# ---------------------------------------------------------------------------
# Per-talkgroup metadata fetch + record assembly
# ---------------------------------------------------------------------------


def _pick_most_active_node(nodes: list[dict]) -> dict | None:
    if not nodes:
        return None
    return max(nodes, key=lambda n: float(n.get("activeSecs") or 0))


def assemble_records(
    recovered: dict[tuple[int, int], str],
) -> dict[str, dict]:
    """For each unique groupId, fetch group_get + node_get (on the most-active
    node) and build the descriptive record consumed by the framing renderer.
    """
    unique_groups = sorted(set(recovered.values()))
    log.info(
        "fetching metadata for %d unique talkgroups (group_get + node_get each)",
        len(unique_groups),
    )
    records: dict[str, dict] = {}
    for gid in unique_groups:
        try:
            g = bc.group(gid)
        except Exception as exc:  # noqa: BLE001
            log.warning("group_get(%s) failed: %s", gid, exc)
            continue
        n_pick = _pick_most_active_node(g.get("nodes") or [])
        county = state = None
        if n_pick is not None:
            try:
                nd = bc.node(int(n_pick["nodeId"]))
                county = nd.get("countyName")
                state = nd.get("stateName")
            except Exception as exc:  # noqa: BLE001
                log.warning(
                    "node_get(%s) failed: %s", n_pick.get("nodeId"), exc
                )
        records[gid] = {
            "groupId": gid,
            "tgCname": g.get("tgCname"),
            "tgDescr": g.get("tgDescr"),
            "tgDisplay": g.get("tgDisplay"),
            "tagDescr": g.get("tagDescr"),
            "sName": g.get("sName"),
            "county": county,
            "state": state,
        }
    return records


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


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

    # ---- Recovery ---------------------------------------------------------
    sids = sorted({sid for sid, _ in pairs})
    if res.works:
        log.info("recovering via M1 (live + historical pos)")
        recovered = recover_via_m1(pairs)
    else:
        log.info("recovering via M2 (group_archives enumeration)")
        sid_to_groups = enumerate_talkgroups_by_sid(sids)
        recovered = recover_via_m2(pairs, sid_to_groups)
    log.info(
        "recovered %d/%d unique calls (%.1f%%)",
        len(recovered),
        len(pairs),
        100.0 * len(recovered) / max(1, len(pairs)),
    )

    # ---- Metadata fetch ---------------------------------------------------
    records = assemble_records(recovered)

    # ---- Persist ----------------------------------------------------------
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    ctx_path = RESULTS_DIR / "context_records.json"
    ctx_path.write_text(json.dumps(records, indent=2, sort_keys=True))
    log.info("wrote %s (%d records)", ctx_path, len(records))

    rec_path = RESULTS_DIR / "recovered_groupids.json"
    rec_path.write_text(
        json.dumps(
            {f"{sid}-{ts}": gid for (sid, ts), gid in recovered.items()},
            indent=2, sort_keys=True,
        )
    )
    log.info("wrote %s", rec_path)

    # Per-sid breakdown for the report
    pairs_by_sid: dict[int, int] = {}
    recovered_by_sid: dict[int, int] = {}
    for sid, _ in pairs:
        pairs_by_sid[sid] = pairs_by_sid.get(sid, 0) + 1
    for (sid, _), _ in recovered.items():
        recovered_by_sid[sid] = recovered_by_sid.get(sid, 0) + 1
    per_sid = sorted(
        (
            {
                "sid": sid,
                "calls": pairs_by_sid[sid],
                "recovered": recovered_by_sid.get(sid, 0),
                "rate_pct": round(
                    100.0 * recovered_by_sid.get(sid, 0) / pairs_by_sid[sid], 1
                ),
            }
            for sid in pairs_by_sid
        ),
        key=lambda r: (-r["rate_pct"], -r["calls"]),
    )

    report = {
        "manifest": args.manifest,
        "unique_segments": len(manifest),
        "unique_calls": len(pairs),
        "unique_sids": len({sid for sid, _ in pairs}),
        "recovered_calls": len(recovered),
        "unique_groupids": len(records),
        "recovery_rate_pct": round(
            100.0 * len(recovered) / max(1, len(pairs)), 1
        ),
        "method": "M1" if res.works else "M2",
        "per_sid": per_sid,
    }
    rep_path = RESULTS_DIR / "recovery_report.json"
    rep_path.write_text(json.dumps(report, indent=2))
    log.info(
        "wrote %s -- recovery rate: %s%%",
        rep_path,
        report["recovery_rate_pct"],
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
