# Experiment 1b — Operator Runbook

**Purpose:** step-by-step commands for whoever has VPC / prod access to execute what the author of this plan cannot reach from their workstation.

**Spec:** [`EXPERIMENT_1B_PLAN.md`](./EXPERIMENT_1B_PLAN.md) is the source of truth for what the experiment measures, abort criteria, and deliverables. This runbook is pure operations.

**Prep already done on `experiment/1b-stream-copy` branch:**
- 5 code changes (ffmpeg `-c copy`, skip FLAC on bcfy_calls/openmhz, `DISABLE_PUBSUB` flag, event-loop monitor)
- Bulk-insert script at `model/data/wildfire_catalog/experiment_1b_bulk_insert.py`
- SIGTERM handler verified: graceful shutdown + `release_feeds_batch` at `normalizer_runtime.py:678-744`

**Before you start, confirm:**
- [ ] **P1:** transcription team has said MP3/M4A is acceptable (OR this is a throwaway capacity measurement, findings not-shippable)
- [ ] **P2:** already verified — SIGTERM release works
- [ ] **P3:** you know the production ingestion SA email and have IAM admin to grant bucket/topic roles

---

## Step 0.1 — Populate the feeds table (~2 min)

From a VPC-authorized host (Cloud Shell, bastion, or a temporary GCE VM with AlloyDB Client IAM):

```bash
# Set AlloyDB connection env (same values the prod ingestion SA uses)
export ALLOYDB_HOST=<primary_instance_ip>
export ALLOYDB_PORT=5432
export ALLOYDB_USER=<ingestion_user>
export ALLOYDB_PASSWORD="$(gcloud secrets versions access latest \
    --secret=alloydb-worker-password-prod)"
export ALLOYDB_DB=ingestion

# Pull the catalog CSV (checked in at this path on wildfire-catalog-findings branch)
git checkout wildfire-catalog-findings -- \
    model/data/wildfire_catalog/output/wildfire_feed_catalog_admin_review.csv \
    model/data/wildfire_catalog/experiment_1b_bulk_insert.py

# Dry run first
python model/data/wildfire_catalog/experiment_1b_bulk_insert.py \
    --catalog model/data/wildfire_catalog/output/wildfire_feed_catalog_admin_review.csv \
    --dry-run

# Expected output:
#   Parsed 11473 rows for source_types=['bcfy_calls', 'bcfy_feeds', 'openmhz']
#     bcfy_calls: 6335
#     bcfy_feeds: 4757
#     openmhz: 381

# If the counts match, insert for real
python model/data/wildfire_catalog/experiment_1b_bulk_insert.py \
    --catalog model/data/wildfire_catalog/output/wildfire_feed_catalog_admin_review.csv
```

The script prints the `EXP_INSERT_TS` timestamp at the end. **Save it** — Step 6.2 cleanup filters on it:

```bash
# The script prints a line like:
#   EXP_INSERT_TS=2026-04-15T14:32:15+00:00
# Capture into an env file for later steps:
echo 'EXP_INSERT_TS=<paste-the-timestamp>' | tee -a experiment_1b.env
```

Verify what got inserted:

```sql
SELECT source_type, status, COUNT(*)
FROM feeds
WHERE name LIKE 'exp1b-%'
GROUP BY source_type, status
ORDER BY source_type;

-- Expected:
--   bcfy_calls  | deactivated | 6335
--   bcfy_feeds  | deactivated | 4757
--   openmhz     | deactivated |  381
```

---

## Step 0.2 — Branch is ready

The experiment branch `experiment/1b-stream-copy` already has the 5 code changes pushed. To deploy it you'll build the ingestion container image from this branch — use whatever CI/CD path the team normally uses (same path as production, just with a tagged experiment image).

No need to merge to main. This branch stays open until the experiment completes.

---

## Step 0.3 — Isolated test resources (~1 min)

```bash
PROJECT=<gcp_project_id>
BUCKET="audio-experiment-1b-$(date +%Y%m%d)"

# Test GCS bucket with 7-day auto-delete lifecycle
gcloud storage buckets create gs://${BUCKET} \
    --project=${PROJECT} \
    --location=us-central1 \
    --uniform-bucket-level-access
gcloud storage buckets update gs://${BUCKET} \
    --lifecycle-file=<(echo '{"rule":[{"action":{"type":"Delete"},"condition":{"age":7}}]}')

# Test Pub/Sub topic (we set DISABLE_PUBSUB=true anyway, but the env
# needs the name set; this is a cheap belt-and-braces)
gcloud pubsub topics create audio-experiment-1b-sink --project=${PROJECT}

# Grant the experiment SA access to both
SA="<INGESTION_SA_EMAIL>"
gcloud storage buckets add-iam-policy-binding gs://${BUCKET} \
    --member="serviceAccount:${SA}" \
    --role="roles/storage.objectCreator"
gcloud pubsub topics add-iam-policy-binding audio-experiment-1b-sink \
    --member="serviceAccount:${SA}" \
    --role="roles/pubsub.publisher"
```

---

## Step 0.4 — Capture production baseline

```bash
# Find the production worker_id (the e2-small ingestion VM has ~250 active leases)
PROD_WORKER_ID=$(psql -t -c "
    SELECT worker_id FROM feeds
    WHERE status = 'active'::feed_status
    GROUP BY worker_id
    ORDER BY COUNT(*) DESC
    LIMIT 1;
" | tr -d ' ')

PROD_BASELINE=$(psql -t -c "
    SELECT COUNT(*) FROM feeds
    WHERE worker_id = '${PROD_WORKER_ID}'
      AND status = 'active'::feed_status;
" | tr -d ' ')

echo "PROD_WORKER_ID=${PROD_WORKER_ID}" >> experiment_1b.env
echo "PROD_BASELINE=${PROD_BASELINE}" >> experiment_1b.env
echo "Captured: prod VM holds ${PROD_BASELINE} active feeds"
```

**No feeds are flipped to `unclaimed` at this point** — ramp activation in Step 3 handles that per-step.

---

## Step 1 — Provision the experiment VM

```bash
gcloud compute instances create experiment-1b \
    --project=${PROJECT} \
    --machine-type=n2-standard-4 \
    --zone=us-central1-a \
    --image-family=debian-12 \
    --image-project=debian-cloud \
    --boot-disk-size=20GB \
    --scopes=cloud-platform \
    --service-account=${SA} \
    --metadata=startup-script='#!/bin/bash
      echo "fs.file-max = 1000000" >> /etc/sysctl.conf
      echo "net.ipv4.tcp_keepalive_time = 60" >> /etc/sysctl.conf
      echo "net.ipv4.tcp_keepalive_intvl = 10" >> /etc/sysctl.conf
      echo "net.ipv4.tcp_keepalive_probes = 6" >> /etc/sysctl.conf
      sysctl -p
      ulimit -n 65535
      apt-get update && apt-get install -y libjemalloc2
    '

gcloud compute ssh experiment-1b --zone=us-central1-a
```

On the VM, pull and run the experiment branch's ingestion image (however CI/CD serves it), with these env vars set:

```bash
export GCS_BUCKET="audio-experiment-1b-YYYYMMDD"     # from Step 0.3
export PUBSUB_TOPIC_PATH="projects/${PROJECT}/topics/audio-experiment-1b-sink"
export DISABLE_PUBSUB="true"                          # short-circuit Pub/Sub
export MAX_FEEDS_PER_WORKER="2000"                    # set ONCE, ramp is DB-driven
export EXPERIMENT_1B_EVENT_LOOP_MONITOR="true"        # logs loop health every 10s
export EXPERIMENT_1B_MONITOR_INTERVAL_SEC="10.0"      # optional override
export LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so.2
# AlloyDB env vars: same as prod ingestion (SA already authorized)
```

Start the ingestion process. At this point the VM leases **zero feeds** (nothing is `unclaimed` yet). Healthy idle state.

---

## Step 2 — Start measurement

Open three tmux panes on the VM:

**Pane A — system resource sampler:**
```bash
./measure.sh 0 | tee -a experiment_1b_results.tsv  # step 0 = baseline
```
(the `measure.sh` script is in `EXPERIMENT_1B_PLAN.md` §2.1 — copy-paste it)

**Pane B — event-loop health:**
```bash
journalctl -u ingestion-service -f -o cat \
    | grep --line-buffered event_loop_health \
    > event_loop.jsonl
```

**Pane C — AlloyDB Cloud Console:**
In a browser tab, open **AlloyDB → instance → Overview** and **Query Insights → Load**. Check CPU + p99 latency at minute 10 / 30 / 60 / 90 of each ramp step.

---

## Step 3 — Ramp

From a psql shell on the VPC-authorized host, run one activation SQL per step. Wait ~10 seconds after each for the lease loop to pick up, then 5 minutes for steady state, then measure for 2 hours before advancing.

**Step 1 (target 100 feeds):**
```sql
WITH to_activate AS (
  (SELECT id FROM feeds WHERE source_type = 'bcfy_feeds' AND status = 'deactivated' AND name LIKE 'exp1b-%' ORDER BY id LIMIT 41)
  UNION ALL
  (SELECT id FROM feeds WHERE source_type = 'bcfy_calls' AND status = 'deactivated' AND name LIKE 'exp1b-%' ORDER BY id LIMIT 55)
  UNION ALL
  (SELECT id FROM feeds WHERE source_type = 'openmhz' AND status = 'deactivated' AND name LIKE 'exp1b-%' ORDER BY id LIMIT 4)
)
UPDATE feeds SET status = 'unclaimed'::feed_status WHERE id IN (SELECT id FROM to_activate);
```

**Step 2 (target 250, add +62/+83/+5):** same template, LIMIT values 62 / 83 / 5.
**Step 3 (target 500, add +104/+138/+8):** 104 / 138 / 8.
**Step 4 (target 750, add +104/+138/+8):** 104 / 138 / 8.
**Step 5 (target 1000, add +103/+138/+9):** 103 / 138 / 9.
**Step 6 (target 1250, add +104/+138/+8):** 104 / 138 / 8.
**Step 7 (target 1500, add +103/+138/+9):** 103 / 138 / 9.
**Step 8 (target 2000, add +207/+276/+17):** 207 / 276 / 17.

After each activation, verify on the VM:

```bash
# Match actual leased composition against expected
psql -t -c "
    SELECT source_type, COUNT(*)
    FROM feeds
    WHERE name LIKE 'exp1b-%'
      AND status = 'active'::feed_status
    GROUP BY source_type;
"
```

And restart the step measurement:
```bash
./measure.sh <N> | tee -a experiment_1b_results.tsv
```

**Every 15 min, check prod health (Step 2.3 from the plan):**
```bash
CURRENT=$(psql -t -c "
    SELECT COUNT(*) FROM feeds
    WHERE worker_id = '${PROD_WORKER_ID}'
      AND status = 'active'::feed_status;
" | tr -d ' ')
THRESHOLD=$(( PROD_BASELINE * 80 / 100 ))
if (( CURRENT < THRESHOLD )); then
    echo "⚠ ABORT: prod active feeds ${CURRENT} < 80% of baseline ${PROD_BASELINE}"
fi
```

**Stop criteria (per-step):** see `EXPERIMENT_1B_PLAN.md` §3 stop criteria table. If any trip, stop the ramp at the current step.

---

## Step 4 — 72-hour soak

At the highest step that passed, hold. No further SQL activations. Run the RSS-drift sampler from `EXPERIMENT_1B_PLAN.md` §4.

---

## Step 5 — Deliverables

Aggregate `experiment_1b_results.tsv` into the tables in `EXPERIMENT_1B_PLAN.md` §5. Per-source-type resource profile is the key output.

---

## Step 6 — Cleanup

### 6.1 — Force-release experiment VM's leases

```sql
UPDATE feeds
SET status = 'unclaimed'::feed_status,
    worker_id = NULL,
    last_heartbeat = NULL
WHERE worker_id = '<experiment_vm_worker_id>';
```
(The experiment VM's worker_id is in its env or in its logs; also findable via the query at the top of Step 2.)

### 6.2 — Return experiment-inserted feeds to `deactivated`

```bash
source experiment_1b.env   # loads EXP_INSERT_TS

psql <<EOF
UPDATE feeds
SET status = 'deactivated'::feed_status,
    worker_id = NULL,
    last_heartbeat = NULL
WHERE source_type IN ('bcfy_feeds','bcfy_calls','openmhz')
  AND name LIKE 'exp1b-%'
  AND created_at >= '${EXP_INSERT_TS}';
EOF
```

Two filter predicates (`name LIKE 'exp1b-%'` AND `created_at >= '${EXP_INSERT_TS}'`) are belt-and-braces — they should agree, and disagreement would flag a data-integrity issue worth investigating before the DELETE below.

Optional hard-delete (if the team doesn't want the Tier 1+2 rows retained):

```sql
DELETE FROM feed_properties WHERE feed_id IN (
    SELECT id FROM feeds WHERE name LIKE 'exp1b-%'
);
DELETE FROM feeds WHERE name LIKE 'exp1b-%';
```

### 6.3 — Teardown

```bash
gcloud compute instances delete experiment-1b --zone=us-central1-a --quiet
gcloud storage rm -r gs://audio-experiment-1b-YYYYMMDD
gcloud pubsub topics delete audio-experiment-1b-sink
# Branch stays on remote — do not merge to main
```

Verify prod unaffected:
```sql
SELECT COUNT(*) FROM feeds
WHERE worker_id = '${PROD_WORKER_ID}'
  AND status = 'active'::feed_status;
-- Should be back within ~5% of PROD_BASELINE
```

---

## Known caveats baked into this experiment

| Caveat | What it means | When to worry |
|---|---|---|
| GCS objects carry `.flac` extension but hold MP3/M4A bytes | `gcp_helper.py` hardcodes `.flac` in the object_name; the actual bytes are stream-copied. Doesn't affect capacity measurement (bytes are what matter). | If someone tries to download and play these objects expecting FLAC, they won't work. |
| Experiment VM adds ~133/sec heartbeats to prod AlloyDB at peak | 2,000 feeds × (1 heartbeat / 15s). Fine vs the ~4,000 TPS headroom, but monitor. | If AlloyDB CPU climbs past 60%, the experiment is meaningfully loading the DB. |
| bcfy_calls poll rate hits 200/sec at peak | 2,000 feeds × (1/10s) under the Tier 1+2 ratio → ~110/sec at peak, or ~200/sec if we extrapolate to 3,500 bcfy_calls | Watch for 429s in the bcfy_calls collector logs. Step 7 of the plan already has an abort for this. |
| SIGTERM grace period is 10s (`graceful_shutdown_timeout_sec`) | GCE default grace before SIGKILL is 30s — safe margin. | If you increase `graceful_shutdown_timeout_sec` above 25s, GCE may SIGKILL mid-release. |
| jemalloc is installed but only active with `LD_PRELOAD` set | Already in the env export above. If an A/B is desired, toggle `LD_PRELOAD` between runs. | — |

## Files in this experiment's footprint

| File | Role | Location |
|---|---|---|
| `EXPERIMENT_1B_PLAN.md` | Spec (what we're measuring, why) | `model/data/wildfire_catalog/` |
| `EXPERIMENT_1B_RUNBOOK.md` | This file — step-by-step operator commands | `model/data/wildfire_catalog/` |
| `experiment_1b_bulk_insert.py` | Populates the feeds table with Tier 1+2 catalog | `model/data/wildfire_catalog/` |
| `output/wildfire_feed_catalog_admin_review.csv` | 12,027 Tier 1+2 rows (catalog tool output) | `model/data/wildfire_catalog/` |
| 5 code changes | ffmpeg stream-copy, skip FLAC (×2), `DISABLE_PUBSUB`, event-loop monitor | `experiment/1b-stream-copy` branch |
