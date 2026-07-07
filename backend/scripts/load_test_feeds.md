# Manual Ingestion Load Test From `main`

This guide is self-contained. You do not need any context from the Experiment
1b discussion to use it.

Use this when you want to:

1. Insert load-test feed rows into AlloyDB.
2. Keep those rows dormant until you are ready.
3. Start the latest `main` ingestion worker.
4. Activate feeds in explicit ramp steps.
5. Deactivate the test rows when the run is over.

## Files In This Branch

- `backend/scripts/load_test_feeds.py`:
  Direct AlloyDB utility for importing, counting, activating, and deactivating
  load-test feeds.

- `backend/scripts/load_test_feeds.md`:
  This runbook.

- `model/data/load_tests/ingestion_load_test_sample.csv`:
  Tiny schema example. It is not a real load-test catalog.

## What This Script Does

The script writes rows directly to:

- `feeds`
- `feed_properties`
- `feed_audit_events`

Imported feeds start as `status = 'deactivated'`, so ingestion workers cannot
claim them until you run `activate`. Activation changes selected rows to
`status = 'unclaimed'`, which allows the current `main` worker lease loop to
pick them up.

The script only touches feeds whose `name` starts with your `--prefix`. For
safety, the prefix must start with `loadtest-`, must include a timestamp, and
must not contain SQL wildcard characters.

## What This Script Does Not Do

- It does not create real Broadcastify/OpenMHz credentials.
- It does not build or deploy the ingestion container.
- It does not create GCS buckets or Pub/Sub topics.
- It does not include the original Experiment 1b private generated catalog.
- It does not delete rows. Cleanup deactivates rows so operators can inspect
  them later.

## Required Access

You need:

- A branch or checkout containing this file set.
- A host that can connect to the target AlloyDB instance.
- Permission to insert and update `feeds`, `feed_properties`, and
  `feed_audit_events`.
- A CSV of real feed IDs to load.
- A load-test ingestion worker built from the latest `main` code.
- Broadcastify credentials if testing `bcfy_feeds` or `bcfy_calls`.
- A staging GCS bucket and Pub/Sub topics. Use isolated test resources unless
  you deliberately intend to exercise production resources.

## CSV Format

Minimum supported columns:

```csv
source,source_feed_id,feed_name
bcfy_feeds,12345,County Fire Dispatch
bcfy_calls,12-34,County Fire Calls
openmhz,svrcs2,SVRCS
```

Historical wildfire catalog CSVs are also supported. The loader recognizes:

- `source` or `source_type`
- `source_feed_id`
- `feed_name`, `name`, or `display_name`
- `catalog_id`
- `priority_tier`
- `state`
- `county`
- `agency_name`
- `service_tags`

The optional metadata columns are copied to `feed_properties.tags`.

### Source ID Rules

- `bcfy_feeds`: Broadcastify live feed ID, for example `12345`.
- `bcfy_calls`: Broadcastify Calls group ID in current service format,
  for example `12-34`.
- `openmhz`: OpenMHz system short name, for example `svrcs2`.

OpenMHz rows may also use `shortName:talkgroup`, for example
`svrcs2:2301`. The loader normalizes that to `svrcs2` and deduplicates to one
feed per OpenMHz system because the current collector subscribes per system.

## Step 0: Get Onto This Branch

```bash
git fetch origin
git checkout experiment/main-load-test-feed-loader
git pull --ff-only
```

Confirm the script exists:

```bash
test -f backend/scripts/load_test_feeds.py
test -f backend/scripts/load_test_feeds.md
```

## Step 1: Choose A Unique Prefix

Use one new prefix per run:

```bash
export LOAD_PREFIX="loadtest-$(date -u +%Y%m%dT%H%M%SZ)-"
echo "$LOAD_PREFIX"
```

Do not reuse a prefix. Feed names are unique; rerunning an import with the same
prefix is expected to fail.

## Step 2: Configure AlloyDB Access

Set either `DATABASE_URL`:

```bash
export DATABASE_URL='postgresql://USER:PASSWORD@HOST:5432/ingestion'
```

Or set individual AlloyDB variables:

```bash
export ALLOYDB_HOST=<primary-or-pooler-host>
export ALLOYDB_PORT=5432
export ALLOYDB_USER=<ingestion-user>
export ALLOYDB_PASSWORD=<password>
export ALLOYDB_DB=ingestion
```

Smoke-test connectivity before changing anything:

```bash
uv run python backend/scripts/load_test_feeds.py \
  --prefix "$LOAD_PREFIX" \
  counts
```

Expected first-run output:

```text
No matching load-test rows.
```

## Step 3: Dry-Run Import

Replace `/path/to/feed_catalog.csv` with your real CSV.

```bash
uv run python backend/scripts/load_test_feeds.py \
  --prefix "$LOAD_PREFIX" \
  import \
  --csv /path/to/feed_catalog.csv \
  --max-per-source bcfy_feeds=1000,bcfy_calls=1300,openmhz=100 \
  --dry-run
```

Expected output shape:

```text
Selected 2400 feed rows:
  bcfy_calls: 1300
  bcfy_feeds: 1000
  openmhz: 100
--dry-run: not inserting
```

If counts are wrong, stop and fix the CSV or the `--max-per-source` values.

## Step 4: Insert Dormant Feeds

```bash
uv run python backend/scripts/load_test_feeds.py \
  --prefix "$LOAD_PREFIX" \
  import \
  --csv /path/to/feed_catalog.csv \
  --max-per-source bcfy_feeds=1000,bcfy_calls=1300,openmhz=100
```

Then verify rows are still dormant:

```bash
uv run python backend/scripts/load_test_feeds.py \
  --prefix "$LOAD_PREFIX" \
  counts
```

Expected output shape:

```text
bcfy_calls         deactivated   1300
bcfy_feeds         deactivated   1000
openmhz            deactivated   100
```

At this point no worker should claim the new rows.

## Step 5: Start The Latest `main` Worker

Build and run the ingestion worker from current `main`. Do not use the old
`experiment/1b-stream-copy` image for this test.

The current `main` worker requires these environment variables:

```bash
export AUDIO_STAGING_BUCKET=<load-test-gcs-bucket>
export CONTINUOUS_PUBSUB_TOPIC_PATH=projects/<project>/topics/<continuous-topic>
export SEGMENTED_PUBSUB_TOPIC_PATH=projects/<project>/topics/<segmented-topic>

export MAX_FEEDS_PER_WORKER=2000
export CAP_BCFY_FEEDS=1000
export CAP_BCFY_CALLS=1300
export CAP_OPENMHZ=100
```

Use the normal deployment path for the project. If you run manually from a VM,
start the current worker entrypoint:

```bash
uv run python -m backend.pipeline.ingestion.main
```

Keep this worker isolated from production unless you have explicitly planned a
production-path test.

## Step 6: Probe Before The Real Ramp

Activate a small probe that stays at or below the first ramp target:

```bash
uv run python backend/scripts/load_test_feeds.py \
  --prefix "$LOAD_PREFIX" \
  activate \
  --target bcfy_feeds=4,bcfy_calls=4,openmhz=4
```

Before continuing, verify:

- `counts` shows 12 non-deactivated rows.
- Worker logs show feed acquisition.
- GCS receives objects for every source type you intend to test.
- `bcfy_calls` has no systematic 401/403 failures.
- `bcfy_feeds` ffmpeg exits are not systemic.
- AlloyDB latency is acceptable.
- The worker is writing heartbeats.

If any check fails, stop. Run cleanup, fix the issue, and start a new prefix.
If you want a larger probe, use a separate prefix for that probe or deactivate
the probe rows before starting Step 7.

## Step 7: Run The Ramp

`activate --target` is cumulative. If the current prefix already has 41
non-deactivated `bcfy_feeds` and the next target is 103, the script activates
only 62 more.

Experiment 1b-style ramp targets:

```bash
# Step 1: 100 total.
uv run python backend/scripts/load_test_feeds.py \
  --prefix "$LOAD_PREFIX" activate \
  --target bcfy_feeds=41,bcfy_calls=55,openmhz=4

# Step 2: 250 total.
uv run python backend/scripts/load_test_feeds.py \
  --prefix "$LOAD_PREFIX" activate \
  --target bcfy_feeds=103,bcfy_calls=138,openmhz=9

# Step 3: 500 total.
uv run python backend/scripts/load_test_feeds.py \
  --prefix "$LOAD_PREFIX" activate \
  --target bcfy_feeds=207,bcfy_calls=276,openmhz=17

# Step 4: 750 total.
uv run python backend/scripts/load_test_feeds.py \
  --prefix "$LOAD_PREFIX" activate \
  --target bcfy_feeds=311,bcfy_calls=414,openmhz=25

# Step 5: 1000 total.
uv run python backend/scripts/load_test_feeds.py \
  --prefix "$LOAD_PREFIX" activate \
  --target bcfy_feeds=414,bcfy_calls=552,openmhz=34

# Step 6: 1500 total.
uv run python backend/scripts/load_test_feeds.py \
  --prefix "$LOAD_PREFIX" activate \
  --target bcfy_feeds=621,bcfy_calls=828,openmhz=51
```

Recommended cadence:

1. Activate a step.
2. Wait for lease acquisition.
3. Wait for warmup.
4. Measure for the chosen window.
5. Save metrics before moving to the next step.

Track at least:

- Worker CPU and RSS.
- Active feed count by source type.
- `bcfy_feeds` ffmpeg process count.
- GCS upload success and latency.
- Pub/Sub publish success and latency.
- AlloyDB CPU and query latency.
- Broadcastify/OpenMHz error rates.

## Step 8: Cleanup

Stop the load-test worker first, then deactivate rows:

```bash
uv run python backend/scripts/load_test_feeds.py \
  --prefix "$LOAD_PREFIX" \
  deactivate
```

Verify:

```bash
uv run python backend/scripts/load_test_feeds.py \
  --prefix "$LOAD_PREFIX" \
  counts
```

Expected final shape:

```text
bcfy_calls         deactivated   1300
bcfy_feeds         deactivated   1000
openmhz            deactivated   100
```

## Common Problems

### The import selected zero rows

Check that your CSV has `source`, `source_feed_id`, and feed-name columns. If
the CSV uses `source_type` instead of `source`, that is supported.

### OpenMHz count is lower than expected

The loader deduplicates OpenMHz by system short name. Multiple talkgroups from
the same system intentionally become one feed row.

### Activation updates fewer rows than requested

Run `counts`. You probably imported fewer dormant rows than the requested
target, or rows from an earlier step are already non-deactivated.

### The worker does not claim rows

Check:

- The worker is running latest `main`.
- `CAP_BCFY_FEEDS`, `CAP_BCFY_CALLS`, and `CAP_OPENMHZ` are high enough.
- `MAX_FEEDS_PER_WORKER` is high enough.
- The rows are `unclaimed`, not still `deactivated`.
- The worker database points at the same AlloyDB instance as the loader.

### You need to abort mid-run

Stop the worker, then run `deactivate`. If the worker is still running while
you deactivate, current `main` should cancel deactivated feed tasks, but stopping
the worker first is clearer during an experiment.
