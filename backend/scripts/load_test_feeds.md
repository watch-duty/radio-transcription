# Ingestion Load Test Feed Seeding

This guide is self-contained. You do not need any context from Experiment 1b
or from the conversation that created this branch.

The default path does not require you to provide a feed CSV. It builds a fresh
wildfire feed catalog from the included crawler, inserts the generated feeds
into AlloyDB as dormant rows, and lets you activate by a total count such as
10,000.

You still need database access and provider credentials. "No input" means no
handwritten feed list.

## Files

- `backend/scripts/load_test_feeds.py`: one CLI for catalog seeding, CSV import,
  counts, activation, and cleanup.
- `backend/scripts/load_test_feeds.md`: this runbook.
- `model/data/wildfire_catalog/`: catalog crawler copied from the historical
  wildfire catalog work. Generated `cache/` and `output/` directories are
  intentionally gitignored.
- `model/data/load_tests/ingestion_load_test_sample.csv`: tiny CSV schema
  example for the optional manual import path. It is not real load-test data.

## What Happens

`seed-catalog` does this:

1. Runs `model/data/wildfire_catalog/run_catalog.py`.
2. Writes `model/data/wildfire_catalog/output/wildfire_feed_catalog_admin_review.csv`.
3. Reads that generated CSV.
4. Skips Echo because Echo is not VM-leased by the current ingestion worker.
5. Normalizes OpenMHz `shortName:talkgroup` rows to one feed per OpenMHz system.
6. Inserts selected rows into `feeds`, `feed_properties`, and
   `feed_audit_events`.
7. Leaves every inserted feed at `status = 'deactivated'`.

`activate` then changes selected rows to `status = 'unclaimed'`, allowing the
latest `main` ingestion worker to claim them.

The script only touches rows whose feed `name` starts with your `--prefix`. The
prefix must start with `loadtest-`, include a timestamp, and contain no SQL
wildcards.

## Required Access

Run from a host that can reach AlloyDB, such as a bastion, Cloud Shell with
private connectivity, or a temporary GCE VM.

You need:

- This branch checked out.
- Permission to insert/update `feeds`, `feed_properties`, and
  `feed_audit_events`.
- Broadcastify API credentials for catalog generation and for `bcfy_*`
  ingestion.
- GCS bucket and Pub/Sub topics for the ingestion worker.
- A worker image built from latest `main`, not from `experiment/1b-stream-copy`.

## Step 0: Check Out The Branch

```bash
git fetch origin
git checkout experiment/main-load-test-feed-loader
git pull --ff-only
```

Confirm the files exist:

```bash
test -f backend/scripts/load_test_feeds.py
test -f model/data/wildfire_catalog/run_catalog.py
```

## Step 1: Pick A Fresh Prefix

Use a new prefix for every run:

```bash
export LOAD_PREFIX="loadtest-$(date -u +%Y%m%dT%H%M%SZ)-"
echo "$LOAD_PREFIX"
```

Do not reuse a prefix. Feed names are unique; a second import with the same
prefix is expected to fail.

## Step 2: Configure Credentials

Set AlloyDB access with `DATABASE_URL`:

```bash
export DATABASE_URL='postgresql://USER:PASSWORD@HOST:5432/ingestion'
```

Or set individual variables:

```bash
export ALLOYDB_HOST=<primary-or-pooler-host>
export ALLOYDB_PORT=5432
export ALLOYDB_USER=<ingestion-user>
export ALLOYDB_PASSWORD=<password>
export ALLOYDB_DB=ingestion
```

Set Broadcastify catalog credentials:

```bash
export BCFY_API_KEY=<broadcastify-api-key>
export BROADCASTIFY_API_KEY_ID=<broadcastify-api-key-id>
export BROADCASTIFY_APP_ID=<broadcastify-app-id>
```

Smoke-test the DB connection before changing anything:

```bash
uv run python backend/scripts/load_test_feeds.py \
  --prefix "$LOAD_PREFIX" \
  counts
```

Expected first-run output:

```text
No matching load-test rows.
```

## Step 3: Dry-Run A >10k Catalog Seed

This command can take about 35 minutes with an empty cache because it crawls
Broadcastify county/group endpoints at a low request rate.

```bash
uv run python backend/scripts/load_test_feeds.py \
  --prefix "$LOAD_PREFIX" \
  seed-catalog \
  --dry-run
```

Expected shape:

```text
Building load-test catalog:
  ... model/data/wildfire_catalog/run_catalog.py ...
Selected 11000+ feed rows:
  bcfy_calls: ...
  bcfy_feeds: ...
  openmhz: ...
--dry-run: not inserting
```

Historical reference from the old catalog run, after skipping Echo:

```text
bcfy_calls: 6335
bcfy_feeds: 4757
openmhz: 381
total: 11473
```

Exact counts can change as provider catalogs change. Stop if the selected
total is below 10,000, if any source is unexpectedly zero, or if the catalog
builder logs sustained API failures.

Useful variants:

```bash
# Reuse cached provider responses only. Fails if the cache is empty.
uv run python backend/scripts/load_test_feeds.py \
  --prefix "$LOAD_PREFIX" \
  seed-catalog \
  --cache-only \
  --dry-run

# Reuse an already generated output CSV without crawling again.
uv run python backend/scripts/load_test_feeds.py \
  --prefix "$LOAD_PREFIX" \
  seed-catalog \
  --skip-catalog-build \
  --dry-run

# Seed exactly 10,000 generated rows instead of the whole Tier 1+2 catalog.
uv run python backend/scripts/load_test_feeds.py \
  --prefix "$LOAD_PREFIX" \
  seed-catalog \
  --target-total 10000 \
  --dry-run
```

## Step 4: Insert Dormant Rows

If the dry run looks right, insert for real:

```bash
uv run python backend/scripts/load_test_feeds.py \
  --prefix "$LOAD_PREFIX" \
  seed-catalog
```

For exactly 10,000 seed rows:

```bash
uv run python backend/scripts/load_test_feeds.py \
  --prefix "$LOAD_PREFIX" \
  seed-catalog \
  --target-total 10000
```

Verify the rows are dormant:

```bash
uv run python backend/scripts/load_test_feeds.py \
  --prefix "$LOAD_PREFIX" \
  counts
```

Expected shape:

```text
bcfy_calls         deactivated   6335
bcfy_feeds         deactivated   4757
openmhz            deactivated    381
```

Your exact numbers may differ. The important checks are:

- Total seeded rows are above your planned activation target.
- Every row is still `deactivated`.
- No worker has claimed anything yet.

## Step 5: Start The Latest `main` Worker

Build and run the ingestion worker from latest `main`. Do not use the old
`experiment/1b-stream-copy` image.

Use isolated load-test resources unless this is explicitly a production-path
test:

```bash
export AUDIO_STAGING_BUCKET=<load-test-gcs-bucket>
export CONTINUOUS_PUBSUB_TOPIC_PATH=projects/<project>/topics/<continuous-topic>
export SEGMENTED_PUBSUB_TOPIC_PATH=projects/<project>/topics/<segmented-topic>
```

Set caps high enough for your target. For a one-worker 10k test:

```bash
export MAX_FEEDS_PER_WORKER=10000
export CAP_BCFY_FEEDS=4200
export CAP_BCFY_CALLS=5600
export CAP_OPENMHZ=400
export CAP_FIRE_NOTIFICATIONS=0
```

For a full historical Tier 1+2 seed of roughly 11.5k:

```bash
export MAX_FEEDS_PER_WORKER=12000
export CAP_BCFY_FEEDS=5000
export CAP_BCFY_CALLS=6500
export CAP_OPENMHZ=500
export CAP_FIRE_NOTIFICATIONS=0
```

Start the current worker entrypoint if running manually:

```bash
uv run python -m backend.pipeline.ingestion.main
```

## Step 6: Probe

Activate a tiny probe:

```bash
uv run python backend/scripts/load_test_feeds.py \
  --prefix "$LOAD_PREFIX" \
  activate \
  --target-total 12
```

Before continuing, verify:

- `counts` shows 12 non-deactivated rows.
- Worker logs show feed acquisition.
- GCS receives objects for each source type you intend to test.
- `bcfy_calls` has no systematic 401/403 failures.
- `bcfy_feeds` ffmpeg exits are not systemic.
- AlloyDB latency is acceptable.
- The worker is writing heartbeats.

If any check fails, stop the worker, run cleanup, fix the issue, and start a
new prefix.

## Step 7: Activate By Total Count

`activate --target-total` is cumulative. If 12 rows are already active-ish and
you request `--target-total 100`, the script activates 88 more rows.

Example ramp:

```bash
uv run python backend/scripts/load_test_feeds.py \
  --prefix "$LOAD_PREFIX" activate --target-total 100

uv run python backend/scripts/load_test_feeds.py \
  --prefix "$LOAD_PREFIX" activate --target-total 250

uv run python backend/scripts/load_test_feeds.py \
  --prefix "$LOAD_PREFIX" activate --target-total 500

uv run python backend/scripts/load_test_feeds.py \
  --prefix "$LOAD_PREFIX" activate --target-total 1000

uv run python backend/scripts/load_test_feeds.py \
  --prefix "$LOAD_PREFIX" activate --target-total 2500

uv run python backend/scripts/load_test_feeds.py \
  --prefix "$LOAD_PREFIX" activate --target-total 5000

uv run python backend/scripts/load_test_feeds.py \
  --prefix "$LOAD_PREFIX" activate --target-total 10000
```

The script allocates the total across source types using the seeded source mix.
For example, if the prefix has roughly 4,757 `bcfy_feeds`, 6,335
`bcfy_calls`, and 381 `openmhz`, then `--target-total 10000` computes about:

```text
bcfy_feeds: 4146
bcfy_calls: 5522
openmhz: 332
```

Use `--dry-run` first if you want to inspect the computed activation delta:

```bash
uv run python backend/scripts/load_test_feeds.py \
  --prefix "$LOAD_PREFIX" \
  activate \
  --target-total 10000 \
  --dry-run
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
bcfy_calls         deactivated   ...
bcfy_feeds         deactivated   ...
openmhz            deactivated   ...
```

## Optional: Manual CSV Import

Use this only when you deliberately want a hand-curated feed list.

Minimum CSV columns:

```csv
source,source_feed_id,feed_name
bcfy_feeds,12345,County Fire Dispatch
bcfy_calls,12-34,County Fire Calls
openmhz,svrcs2,SVRCS
```

Dry run:

```bash
uv run python backend/scripts/load_test_feeds.py \
  --prefix "$LOAD_PREFIX" \
  import \
  --csv /path/to/feed_catalog.csv \
  --dry-run
```

Insert:

```bash
uv run python backend/scripts/load_test_feeds.py \
  --prefix "$LOAD_PREFIX" \
  import \
  --csv /path/to/feed_catalog.csv
```

## Common Problems

### `seed-catalog` selected fewer than 10,000 rows

Check the catalog logs for Broadcastify authentication failures, provider API
errors, or an accidental `--target-total` cap. Re-run with `--cache-only` only
after a successful full crawl has populated `model/data/wildfire_catalog/cache/`.

### OpenMHz count is lower than expected

The loader deduplicates OpenMHz by system short name. Multiple talkgroups from
the same system intentionally become one feed row.

### Activation updates fewer rows than requested

Run `counts`. You probably seeded fewer rows than the requested target, caps
are lower than the target, or rows from an earlier step are already
non-deactivated.

### The worker does not claim rows

Check:

- The worker is running latest `main`.
- `MAX_FEEDS_PER_WORKER` is at least your target.
- `CAP_BCFY_FEEDS`, `CAP_BCFY_CALLS`, and `CAP_OPENMHZ` are high enough.
- Rows are `unclaimed`, not still `deactivated`.
- The worker points at the same AlloyDB instance as the loader.

### You need to abort mid-run

Stop the worker, then run `deactivate`. If the worker is still running while
you deactivate, current `main` should cancel deactivated feed tasks, but
stopping the worker first is clearer during an experiment.
