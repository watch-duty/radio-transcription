# Experiment 1b — redo + report (final plan)

## Context

The 2026-04-15T22:56Z–23:57Z clean ramp on `icecast-collector-dev-v24q` (n2-standard-4) produced data for steps 3–6 but is **not publishable as-is** because two of the three source types were effectively non-functional:

- **openmhz**: zero GCS uploads across all four measured steps — catalog `source_feed_id` used `system:talkgroup` form (e.g. `svrcs2:2301`), but `openmhz/collector.py:119` uses the whole field as the websocket `shortName`, so subscriptions hit non-existent shortnames.
- **bcfy_calls**: 4 988 `Auth failure (401/403)` warnings (log query saturated at 5 000 rows) — dev and prod share one Broadcastify account, prod rotates the JWT every 30 min, the 120-s laptop sync missed the invalidation window.
- Two abort rules (CPU > 75 %, ffmpeg crash > 1 %) silently no-op'd because `bc` is not installed on Container-Optimised OS and `(( $(echo "..." | bc -l) ))` evaluates the empty substitution as `0`.
- A MIG opportunistic update at 22:51 UTC replaced the VM mid-experiment and wiped the Step-1 metrics archive. `updatePolicy.type: OPPORTUNISTIC` reconciles on any MIG config change (we triggered it by clearing autohealing).

The user's instruction: "Make sure everything is working before the actual experiment run, so we don't waste time." The plan below front-loads verification so the ramp only runs once the full three-source-type path is green.

Fixes already committed in this session (outside the plan file, before re-entering plan mode):
- **openmhz catalog**: 100 bare short_names from `cache/openmhz/systems.json` ranked by `callAvg`, written to `feed_properties.source_feed_id` via the SQL in `/tmp/fix_openmhz.sql`. Verified: 100/100 bare, 0 colon-form.
- **JWT sync cadence**: laptop loop at `/tmp/laptop_jwt_sync.sh` now sleeps 30 s (was 120 s), PID 51535, writing `sync ok` lines to `/tmp/exp1b_jwt_sync.log` every 30 s.
- **Ramp script v2**: `/tmp/exp_ramp_v2.sh` — same step structure, but float comparisons use `awk 'BEGIN{exit !(a>b)}'` instead of `bc`. CPU abort reframed as VM-normalised (>300 % of `docker stats` sum-of-cores, i.e. 75 % of 4 vCPU); a soft NOTE fires at ≥100 % to mark single-core saturation without stopping.

## Phase 0 — persist this plan into the repo

Copy this plan file to `/home/shuojing/watch-duty-repo/radio-transcription/model/data/wildfire_catalog/EXPERIMENT_1B_REDO_PLAN.md` (alongside `EXPERIMENT_1B_PLAN.md` and the post-experiment report target).  The session's plan file under `~/.claude/plans/` is ephemeral and will not survive a new conversation; the in-repo copy becomes the durable record that future sessions (and coworkers) can read.  No git commit — leave the file staged on the working tree on whichever branch is active (`wildfire-catalog-findings`, where `EXPERIMENT_1B_PLAN.md` already lives).

## Phase A — environment fixes (must execute before smoke test)

### A1. Abandon the dev VM from the MIG

Purpose: insulate the VM from MIG reconciliation (opportunistic update policy triggered the 22:51 UTC replacement).  `gcloud compute instance-groups managed abandon-instances icecast-collector-dev-mig --project=probable-symbol-492218-i7 --region=us-central1 --instances=icecast-collector-dev-v24q` detaches the VM entirely.  The MIG will spawn a replacement instance to satisfy its target size, but the abandoned VM stays up unmanaged.  After the run, `create-instance` or `manage-instances` re-binds it; acceptable deferred cleanup.

### A2. Sidecar GCS copy of metrics.tsv

Purpose: persist measurement data across any unexpected VM loss.  Deploy a tiny loop in a separate background process on the VM:

```bash
gsutil_sync.sh:
#!/bin/bash
while true; do
  gcloud storage cp /var/lib/exp1b/metrics.tsv gs://ingestion-staging-bucket-dev/exp1b/metrics.tsv 2>/dev/null
  gcloud storage cp /var/lib/exp1b/ramp.log gs://ingestion-staging-bucket-dev/exp1b/ramp.log 2>/dev/null
  sleep 60
done
```

Run under `nohup` + `disown`.  Read-back via `gcloud storage cat` from the laptop for any check.

### A3 — skipped

Pre-probing bcfy_feeds source URLs is deliberately skipped.  The ~1 % ffmpeg crash rate is representative prod behaviour and measuring it is a feature, not a bug.

## Phase B — pre-flight smoke test (~8 min, must pass 100 %)

### B0. Inputs ready

- Container `icecast-collector-experiment-1b` is running the `:experiment-1b` image tag at digest `sha256:6b10a30d…` with env overrides: `DISABLE_PUBSUB=true`, `EXPERIMENT_1B_EVENT_LOOP_MONITOR=true`, `MAX_FEEDS_PER_WORKER=2000`, `ALLOYDB_POOL_MAX_SIZE=50`, `ALLOYDB_POOL_MIN_SIZE=10`.  Verify with `sudo docker exec icecast-collector-experiment-1b env | grep -E 'DISABLE_PUBSUB|MAX_FEEDS|ALLOYDB_POOL'` — four lines expected.
- Laptop JWT sync alive at 30 s cadence: `ps -p 51535` and `tail -n 3 /tmp/exp1b_jwt_sync.log` (expect 3 timestamps within the last 2 minutes).

### B1. Activate 30 probe feeds (10 of each source_type)

```sql
BEGIN;
WITH probes AS (
  (SELECT id FROM feeds WHERE source_type='bcfy_feeds' AND name LIKE 'exp1b-dev-catalog-%'
   ORDER BY name LIMIT 10)
  UNION ALL
  (SELECT id FROM feeds WHERE source_type='bcfy_calls' AND name LIKE 'exp1b-dev-catalog-%'
   ORDER BY name LIMIT 10)
  UNION ALL
  (SELECT id FROM feeds WHERE source_type='openmhz'    AND name LIKE 'exp1b-dev-catalog-%'
   ORDER BY name LIMIT 10)
)
UPDATE feeds SET status='unclaimed'::feed_status WHERE id IN (SELECT id FROM probes);
COMMIT;
```

Wait 180 s for lease + first upload cycle.

### B2. Green-light checks (all must pass)

Run from laptop, treat as a single atomic gate:

1. **Per-source GCS chunks exist** — `gcloud storage ls --recursive 'gs://ingestion-staging-bucket-dev/{bcfy_feeds,bcfy_calls,openmhz}/'` returns ≥1 object per prefix with a timestamp within the probe window.  Concretely, at least one of the 10 activated UUIDs per source_type has a chunk.  If openmhz has zero — **abort the smoke test**; investigate catalog state.
2. **INFO-level lifecycle log lines** — Cloud Logging filter `resource.labels.instance_id="3580347511970251552" AND labels.python_logger=~"backend.pipeline" AND severity=INFO AND timestamp>=<B1_timestamp>` returns ≥20 rows covering `Started ffmpeg`, `Subscribed to system`, and `Published message pubsub-disabled` patterns.
3. **event_loop_health flowing** — `jsonPayload.type="event_loop_health" AND timestamp>=<B1>` returns ≥15 rows (10-s cadence × 3 min = 18 expected, tolerate 15).  Confirms Change 5 is live.
4. **Zero bcfy_calls auth failures in this window** — `labels.python_logger="backend.pipeline.ingestion.collectors.bcfy_calls.bcfy_calls_collector" AND severity>=WARNING AND timestamp>=<B1>` returns 0.  This validates the 30-s JWT sync is keeping ahead of prod rotation.
5. **Zero ffmpeg non-zero exits** — `jsonPayload.message="ffmpeg exited non-zero" AND timestamp>=<B1>` returns 0 at probe scale.
6. **cgroup RSS within envelope** — `docker stats --no-stream` reports between 250 and 500 MiB (extrapolated from 128 + 7.2·30 ≈ 344 MiB).  Wildly different value means something's off.
7. **awk comparator unit test** — on the VM: `bash -c 'source /tmp/exp_ramp_v2.sh-fragment; gt 99.9 75 && echo Y || echo N; gt 50 75 && echo Y || echo N'` returns `Y\nN`.  Inline-test the function by sourcing the snippet rather than re-exec'ing the whole ramp script.

Each failed gate gets a diagnose-and-retry loop before the ramp.  No "best-effort" launches.

### B3. Deactivate probes and drain leases

```sql
UPDATE feeds SET status='deactivated'::feed_status,
                worker_id=NULL, last_heartbeat=NULL, failure_count=0
WHERE name LIKE 'exp1b-dev-catalog-%' AND status IN ('active'::feed_status, 'unclaimed'::feed_status, 'failing'::feed_status);
```

Wait 90 s for the worker to release leases before Phase C.

## Phase C — ramp execution

`/tmp/exp_ramp_v2.sh`, steps 1→7.  Per-step: 5-min warmup + 10-min measurement at 30-s cadence = 15 min/step, ~1 h 45 m total if no abort fires.  Abort criteria in the v2 script:

| Signal | Threshold | Meaning |
|---|---|---|
| CPU (`docker stats` sum-of-cores) rolling 5-min avg | > 300 % (= 75 % × 4 vCPU) | VM-wide saturation, **abort** |
| CPU (same) rolling 5-min avg | > 100 % | one vCPU saturated, **NOTE** (informational) |
| cgroup RSS rolling 2-min avg | > 14 336 MiB | approaching OOM, **abort** |

The NOTE distinguishes "asyncio event-loop ceiling reached" from "the VM is out of headroom" — the central methodology fix vs the v1 ramp.

Progress check cadence from the operator side: one wakeup ~17 min after start (step 1 finish), then a second at step-2-finish, then one per step until abort or step 7 completes.  Do not interrupt the ramp unless CPU stays ≥ 300 % for > 2 min without the script noticing, or RSS > 14 GiB.

## Phase D — post-ramp data harvest + report

1. Pull `/var/lib/exp1b/metrics.tsv` and `ramp.log` (and the GCS-mirrored copies, as independent cross-check) to `/tmp/exp1b_report/`.
2. Run the Cloud Logging queries already scripted in the prior harvest (gcs_upload_ok per step, event_loop_health, ffmpeg_exit, bcfy_calls warnings, gcs_upload_failed) and regenerate `facts.md` and `stats.json`.
3. Verify the facts ledger against the headline table — no discrepancies allowed.
4. Draft the conference-quality report at `radio-transcription/model/data/wildfire_catalog/EXPERIMENT_1B_REPORT.md` using the outline already established on the previous pass (Abstract → Introduction → Background → Methodology → Setup → Results (per-step + scaling + asyncio evidence + per-source breakdown + latency CDFs + reliability) → Discussion (fleet sizing, abort-criterion revision, memory linearity, lessons) → Limitations → Related Work → Conclusion → References → AI Disclosure).  Every numeric claim cites `metrics.tsv` rows, Cloud Logging filters, or `file.py:line`.
5. Internal integrity check: every `file.py:line` verified by re-opening the file; every number traced to a raw-source line in `facts.md`; no "today"/"yesterday" absolutisms; AI-disclosure paragraph included.
6. Produce `EXPERIMENT_1B_REPORT_review_response.md` beside it if Phase 4 of the pipeline surfaced review points (skip if unnecessary — user said "final optimized report", not "review-cycled report").
7. No git commit without explicit user instruction.

## Phase E — clean-up

1. Deactivate all `exp1b-dev-catalog-%` feeds.
2. Kill the laptop JWT sync (30-s loop, PID 51535 or its successor).
3. Kill the VM GCS-sync sidecar from A2.
4. Restore systemd :latest on the VM (`sudo systemctl start icecast-collector-dev.service`).
5. Restore MIG state: `gcloud compute instance-groups managed create-instance` with the original template, or re-add the abandoned VM.  (If the MIG already spawned a replacement to satisfy its target, just delete one of the two — the dev VM that now exists outside the MIG, since we kept it for reporting.)
6. Re-enable autohealing with the original health check: `gcloud compute instance-groups managed update ... --health-check=icecast-collector-dev-healthz --initial-delay=300`.
7. Resume dev's JWT rotation scheduler: `gcloud scheduler jobs resume broadcastify-credential-rotation-dev --project=probable-symbol-492218-i7 --location=us-central1`.

## Files to modify / create

- `/tmp/exp_ramp_v2.sh` (exists; already copied to VM)
- `/tmp/full_redo.sh` (exists; already copied to VM)
- `/tmp/gsutil_sync.sh` (new, Phase A2)
- `/tmp/smoke_test.sh` (new, Phase B orchestrator — optional convenience)
- `/home/shuojing/watch-duty-repo/radio-transcription/model/data/wildfire_catalog/EXPERIMENT_1B_REPORT.md` (deliverable)
- `/home/shuojing/watch-duty-repo/radio-transcription/model/data/wildfire_catalog/EXPERIMENT_1B_REPORT_review_response.md` (optional supplement)

No code changes (no edits under `backend/pipeline/`).

## What this plan is NOT

- Not a code-change plan: all fixes are operational / data / script.
- Not a re-derivation of the methodology: `EXPERIMENT_1B_PLAN.md` v3 remains authoritative for ramp cadence, abort-criteria semantics, and deliverables.
- Not a redo of the single-core-bottleneck hypothesis: the asyncio finding from the v1 run is expected to reproduce cleanly with all three source types functional.

## Verification before declaring Phase B passed

`/tmp/exp1b_report/preflight_pass.md` written with one line per gate check.  If even one gate is `FAIL`, Phase C does not start until the operator reviews and green-lights.  Do not amortize — every gate is blocking.
