# Findings: where the framing context actually lives

**Resolved 2026-05-20.** The framing context for the bcfy_calls eval set is
**embedded in each clip's WAV metadata**, not recoverable from the
Broadcastify API. This doc records the investigation so the dead-ends aren't
re-walked.

## The wrong turn

The spec assumed `example_id = "{call_ts}-{sid}"` and that per-talkgroup
metadata could be recovered from the Broadcastify Calls API via the system
`sid`. Every API recovery path failed — **0/277 calls recovered**:

| Method | Result |
|---|---|
| M1 — `live` + historical `pos` | endpoint has no historical lookback |
| M2 — per-sid `groups_ctid` → `group_archives` | the integers return 0 from `live()` |
| M2-global — every US county `groups_ctid(1y)` (≈Jake's own enumeration) | 229 sids / 40,004 groups; **0 match** |

Cross-checks (`live`, `node_get`, Feeds-API `archives`, WD prod DB
`feed_properties`) all denied the integers. The integers are **not** Bcfy
Calls sids, Bcfy feedIds, or WD `source_feed_id`s.

## Root cause (confirmed with Jake)

Jake's collection (two non-repo scripts) did:
1. `enumerate_us_groups_threaded` — same global `groups_ctid` enumeration we
   re-ran, producing a pool of `groupId`s (`{sid}-{tgDec}`).
2. `build_random_audio_dataset` — random group × random 8h window over the
   last 30 days, download `call['url']`, save as **`{groupId}_{call_ts}`** =
   `{sid}-{tgDec}_{call_ts}`.

A later rename to `{call_ts}-{tgDec}.wav` **dropped the `sid`**. So the
integer in our filenames is a **talkgroup decimal (`tgDec`)**, which:
- collides across systems (tg `2001` exists in 24 systems; only 16/57 of our
  tgDecs are unambiguous), and
- can't be turned back into a `groupId` without the `sid`.

Reverse-mapping `tgDec` → `groupId` via the global index is **unsafe**: it
mis-resolved Palmetto 800 (tg 9603) to "Baltimore County", and WyoLink
(tg 13215) to a New York system, because the true source systems aren't even
in the current enumeration and `tgDec` collides.

## What works: the embedded WAV tags

The Bcfy-Calls MP3s carry the uploader's scanner-software tags (ProScan /
SDRTrunk), preserved through download and the WAV conversion. They state each
call's identity directly — a **more faithful per-call source than any API
round-trip**:

```
comment: Date:..;System:Palmetto 800;Site:Chesterfield;Name:T-Chesterfield Site;..
title:   P:9603 [9601]"SCHP Florence","SCHP Darln"
→ "Law enforcement dispatch traffic for SCHP Florence in Chesterfield County,
   South Carolina on the Palmetto 800."
```

`wav_context.py` parses the two tag dialects into the renderer's field shape;
`build_context_records.py` does download + ffprobe + assembly.

**Coverage (277 unique calls):** 202 (72.9%) yield a framing descriptor;
236 have a system name, 233 a state. The ~75 that fall back to baseline are
bare-tgDec / empty-tag / no-inferable-service clips — they get the production
prompt unchanged in every arm (neutral to the A/B), and are excluded from the
primary framing analysis per the spec.

## Why not enrich from the Bcfy API

The clean curated fields (`tgDescr`, `tagDescr`, county/state) live in
`group_get`/`node_get`, but we can't key into them reliably (sid dropped +
tgDec collisions + incomplete enumeration). A curated system→state table,
verified against in-band agency abbreviations (SCHP→SC, KHP→KS, MHP→MS), is
safer than fuzzy name-matching (which false-matched "Wisconsin Interoperable
System" → "California Radio Interoperable System"). Bcfy enrichment is left as
a documented future option, not a dependency.

## Retained artifacts

- `_broadcastify_client.py` — working Bcfy Calls + Common client. **Not used**
  by this experiment; kept for a possible future enrichment pass.
- `results/framing_context_05_2026/global_groups_index.json` — 229-sid global
  enumeration; forensic evidence + reusable.
- `results/framing_context_05_2026/recovery_report.json` — the 0/277 API
  recovery record.
