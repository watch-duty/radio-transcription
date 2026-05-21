# BLOCKER: talkgroup metadata unrecoverable for the eval-set sids

**Status:** Tasks 8-12 of the experiment plan are paused.
**Date:** 2026-05-20

## TL;DR

The bcfy_calls eval-set manifest's `example_id = "{call_ts}-{sid}"` carries
only a bare integer `sid`, never the `tgDec`. Three recovery paths were
tried — all failed structurally, not for timing or throttling reasons:

1. **M1 (live + historical `pos`):** the endpoint has no historical
   lookback; probe returned empty.
2. **M2-per-sid (live-derived ctids → groups_ctid → group_archives):** all 57
   sids return 0 calls from `live()` right now, so no ctid candidates can be
   discovered. Repeated across the day.
3. **M2-global (enumerate every US county + `groups_ctid(secs_ago=365d)`):**
   ~3000 US counties scanned, ~50 min wall time. Only **229 unique sids**
   surfaced globally — `secs_ago` is effectively last-hour-ish, not a year.
   **0 of the 57 eval-set sids appear in the global index.**

Cross-checks confirm the eval sids are not in the Calls namespace at all
right now:
- `live(sid)` returns `0 calls` for every eval sid; a known-active control
  (`sid=7702`) returns 70 calls with full metadata.
- `node_get(sid)` returns the API's "not found" null-fields shape for every
  eval sid; control `nodeId=5620` returns full county/state.
- `fetch_archive_days(feedId=sid)` returns HTTP 400 for every eval sid —
  not the Feeds-API namespace either.

The production `bcfy_calls_collector` uses `groups={source_feed_id}` where
`source_feed_id` is the *groupId* (e.g. `"7702-4059"`). The eval set was
**not** produced by that collector — Jake's collection script (not in the
repo) recorded only the bare `sid`, discarding the `tgDec`. Those tg
identifiers cannot be reconstructed from the public API months later.

## What the experiment needs

Per-talkgroup descriptive framing (`tgCname`, `tgDescr`, `tgDisplay`,
`tagDescr`, `sName`, county, state) requires at minimum a valid `groupId`
per call. We have none, and have no upstream source to derive them from.

## Pivot options (decision needed)

### A. Cancel the experiment, write up the negative finding
The spec's recovery assumption proved false. Land this BLOCKER, document
the dead-end as a constraint on future bcfy_calls evals, stop here.

### B. Build a fresh, small eval set from currently-active sids
Pick ~5-10 active Bcfy Calls systems (we know `7702` works), pull a few
hours of `group_archives` per group, manually transcribe a subset (or
ASR + curate), preserve `groupId` at collection time. Run the 6-arm A/B
on that. New spec, multi-week scope.

### C. Drop the bcfy_calls eval, use a different existing eval set
We currently have no other tg-aware short-clip eval set. Same blocker.

### D. Drop per-talkgroup granularity, re-frame at the "system" level
Doesn't help — we can't even recover `sName` for these dead sids
(group_get needs groupId; node_get on bare sid returns nulls).

## Artifacts (left in place for forensics)

- `results/framing_context_05_2026/recovery_report.json` — 0/277 recovery,
  per-sid breakdown.
- `results/framing_context_05_2026/global_groups_index.json` — 229-sid
  global cache; can be reused if we want to enumerate the *active*
  Broadcastify Calls fleet for Option B.
- `_broadcastify_client.py` — fully working Calls + Common API client; will
  be reusable for Option B.
- `framing_renderer.py` — pure renderer (12 tests passing); reusable as-is
  for any future framing-context experiment.

## What I am not doing

- Not deleting any artifacts (commits, code, results) — Option B reuses
  most of them.
- Not running `run_eval.py` against 0 records — there is nothing to
  test framing against.
