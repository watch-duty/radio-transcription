# FN Quarantine Escalation

## What This Is

This project improves ingestion feed-health handling for Fire Notifications and related batch-style collectors in the existing radio transcription pipeline. It makes broken FN feeds operator-visible by quarantining sustained unproductive polls, while preserving item-level log-and-continue behavior so one bad audio object does not block later chunks.

The work also adds shared low-volume evidence for unproductive batches so bcfy_calls can be evaluated without changing its quarantine policy in the same change.

## Core Value

Broken feeds must become visible to operators without turning a single bad upstream item into head-of-line blocking or permanent poison-pill quarantine.

## Requirements

### Validated

- ✓ Ingestion runtime leases feeds, owns upload/publish/bookmark/failure counting, and quarantines repeated feed failures — existing.
- ✓ Source collectors yield `CapturedChunk` values and keep source-specific connection, retry, and validation behavior inside collector code — existing.
- ✓ Fire Notifications polls a listing endpoint and downloads new `.mp3` files through `backend/pipeline/ingestion/collectors/fire_notifications/collector.py` — existing.
- ✓ Broadcastify Calls polls bounded API pages, downloads call audio, logs per-call download failures, and participates in the download SLO — existing.
- ✓ Feed quarantine telemetry emits `feed_quarantined` events consumed by the existing alert path — existing.
- ✓ The download SLO is source-type scoped and already covers bcfy_calls/openmhz but not fire_notifications — existing.

### Active

- [ ] Restore/keep FN per-file download failures as log-and-continue behavior; do not raise on the first failed download.
- [ ] Track per-poll attempted vs produced chunk counts for FN.
- [ ] Treat FN silent polls as healthy when no new candidate file is attempted.
- [ ] Treat FN polls with attempted files and zero produced chunks as produce failures.
- [ ] Escalate sustained FN produce failures through the existing consecutive-failure path with reason `downloads_failing`.
- [ ] Preserve existing FN poll-endpoint failure behavior with reason `source_unreachable`.
- [ ] Add shared `batch_unproductive` structured telemetry for attempted > 0 and produced == 0 batches.
- [ ] Use shared batch outcome telemetry for bcfy_calls evidence only; do not change bcfy_calls quarantine behavior in this milestone.
- [ ] Update glossary/ADR docs to use “quarantine escalation,” define silent vs unproductive polls/pages, and correct OpenMHZ classification.
- [ ] Add focused unit tests for FN all-download-failed escalation, FN mixed failure/success behavior, and bcfy_calls instrumentation-only behavior.

### Out of Scope

- Applying bcfy_calls all-downloads-failed quarantine behavior now — bcfy_calls already has download SLO coverage and production logs show isolated failures with nearby successful chunks.
- Applying unproductive-poll semantics to OpenMHZ — OpenMHZ is event/WebSocket-driven rather than a bounded batch-polling collector.
- Adding FN to the download SLO — useful follow-up, but outside this scoped collector fix.
- Detecting wholesale FN filename parse drift — unparseable filenames cannot safely be classified as new without a timestamp; keep as follow-up.
- Creating a failed-item ledger or dead-letter replay path — item-level loss is accepted for this fix to avoid head-of-line blocking.
- Changing runtime feed failure thresholds or adding new operational knobs — reuse the existing conservative debounce.
- Running integration or e2e tests for this work — verification is focused on unit/contract tests unless explicitly requested later.

## Context

The existing ingestion runtime owns feed lifecycle concerns: GCS upload, Pub/Sub publish, bookmarks, failure counting, quarantine, heartbeats, lease release, and timeouts. Collectors own source-specific polling/connection behavior and yield `CapturedChunk` objects to the runtime.

The original attempted fix for FN download failures raised on the first failed audio file. That addressed the visibility symptom but introduced a worse behavior: a single 404/missing/corrupt object could abort the poll, block chronologically later files, remain unprocessed across restarts, and repeatedly drive quarantine.

The deeper problem is FN observability. FN is excluded from the download/chunk SLO universe, so a feed that polls successfully but produces no chunks can remain invisible unless it reaches quarantine. The correct control signal is whether a batch with new attempted items produced usable chunks, not whether the listing endpoint returned HTTP 200 or whether any individual item failed.

bcfy_calls has similar bounded-page mechanics, but different operational coverage. It already emits `call_download_failed` and `chunk_ingested` events that feed the download SLO. GCP log review for May 2026 found bcfy_calls download failures were isolated to two feeds with thousands of successful chunks around them; the observed bcfy_calls quarantine was an API JSON/content-type failure, not all audio downloads failing. This supports instrumentation now and policy change later if evidence warrants it.

## Constraints

- **Collector contract**: Item failures skip, log, and continue; systemic all-items-failed batches may escalate.
- **Runtime ownership**: Collectors should not write DB state or directly quarantine; they raise reasons and let the runtime record feed failure.
- **Operational vocabulary**: Use “quarantine escalation,” not “circuit breaker,” because current feed quarantine is sticky and has no half-open auto-recovery behavior.
- **Reason strings**: Poll endpoint failures use `source_unreachable`; successful polls that produce no chunks from attempted items use `downloads_failing`.
- **Counter policy**: Use one shared unhealthy streak counter for FN; healthy/silent polls reset it, and the latest failure reason is raised at threshold.
- **Telemetry scope**: `batch_unproductive` is evidence telemetry, not a new Cloud Monitoring SLO metric in this milestone.
- **Global FN S3 base**: A bad `FIRE_NOTIFICATIONS_S3_BASE` can make all FN feeds unproductive and quarantine them all; this is accepted because every feed is genuinely broken and previously invisible.
- **Verification boundary**: Do not run integration or e2e tests for this project unless the user explicitly asks; use focused unit/contract tests.

## Key Decisions

| Decision | Rationale | Outcome |
|----------|-----------|---------|
| Fix FN first | FN has the concrete alerting gap; bcfy_calls already has download SLO coverage. | — Pending |
| Log and continue on single failed files | Prevents one bad object from blocking later chunks and causing poison-pill quarantine. | — Pending |
| Define success as produced chunk | Downloaded bytes that fail decode/duration/handoff are not useful output. | — Pending |
| Reuse existing FN failure threshold | Keeps poll failures and produce failures on the same conservative debounce path. | — Pending |
| Add `batch_unproductive` telemetry | Gives shared evidence for FN and bcfy without changing existing SLO metric semantics. | — Pending |
| Do not quarantine bcfy_calls yet | Production evidence does not show sustained all-download-failed bcfy pages, and sticky quarantine would be a behavior change. | — Pending |
| Document fleet-wide FN quarantine as accepted | A global S3 base misconfig breaks every FN feed; visible mass quarantine is better than silent zero output. | — Pending |

## Evolution

This document evolves at phase transitions and milestone boundaries.

**After each phase transition** (via `$gsd-transition`):
1. Requirements invalidated? → Move to Out of Scope with reason
2. Requirements validated? → Move to Validated with phase reference
3. New requirements emerged? → Add to Active
4. Decisions to log? → Add to Key Decisions
5. "What This Is" still accurate? → Update if drifted

**After each milestone** (via `$gsd-complete-milestone`):
1. Full review of all sections
2. Core Value check — still the right priority?
3. Audit Out of Scope — reasons still valid?
4. Update Context with current state

---
*Last updated: 2026-05-24 after initialization*
