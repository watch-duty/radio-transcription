# Radio Transcription Pipeline E2E Load Test Runbook

This runbook details the controlled ramp-up strategy for load testing the radio transcription pipeline up to and exceeding **10,000 - 15,000 concurrent feeds** (with capacity up to 20,000 feeds).

## Feed Distribution Mix

To simulate production workloads, load test feeds reflect the target real-world distribution across persistent radio feeds emitting 15-second audio chunks:

- **50% Fire Notifications (`fire_notifications`)**
- **25% Echo (`echo`)**
- **25% Broadcastify Feeds (`bcfy_feeds`)**
- **0% OpenMHZ (`openmhz`)**

> [!NOTE]
> All feed registration and state updates are performed through the Frontend Proxy API (`/api/v1/feeds`) using individual user credentials / Bearer JWT token. Newly created feeds automatically enter `unclaimed` (active) status and are created on-demand as each phase ramps up.

---

## Phase Execution Overview

| Phase | Percentage | Cumulative Target Feeds | Wait Window | Focus / Verification |
|---|---|---|---|---|
| **Pre-soak** | ~1% | **100** | 30 - 60 min | Baseline functionality, initial pipeline claim & heartbeat check |
| **Phase 1** | 5% | **500** | 1 hour | Monitor initial auto-scaling & worker connection pooling |
| **Phase 2** | 10% | **1,000** | 1 hour | Verify worker lease acquisition & GCS upload latency |
| **Phase 3** | 25% | **2,500** | 1 hour | Check Pub/Sub throughput & Dataflow worker allocations |
| **Phase 4** | 50% | **5,000** | 1 hour | Evaluate AlloyDB connection pool and query latency under mid-load |
| **Phase 5** | 80% | **8,000** | 1 hour | Stress test ingestion collector concurrency & memory consumption |
| **Phase 6** | 100% | **10,000** | 1 hour | Validate end-to-end 10k target SLA metrics |
| **Phase 7+** | 150% | **15,000** | 1 hour | Upper target ramp-up stress testing |
| **Phase 8+** | 200% | **20,000** | Optional | Stress test upper bounds and failure breaking points |

---

## Prerequisites & Setup

Set the necessary environment variables before executing scripts:

```bash
export SERVER_URL="<FE_PROXY_API_URL>"  # Replace with target FE proxy API URL
export TOKEN="<YOUR_FE_PROXY_BEARER_TOKEN>"
export LOAD_PREFIX="loadtest-$(date +%Y%m%dT%H%M%SZ)-"
```

---

## Step 1: Extract Templates & Generate Feed Catalog CSV

Extract authentic feed templates from `sample_feed_properties.csv` and `sample_feeds.csv`:

```bash
python3 backend/scripts/extract_templates.py \
  --properties-csv backend/scripts/test_data/sample_feed_properties.csv \
  --feeds-csv backend/scripts/test_data/sample_feeds.csv \
  --output backend/scripts/test_data/production_feed_templates.json
```

Generate a 15,000-feed dataset adhering to the required 50/25/25 mix output to `backend/scripts/test_data/`:

```bash
python3 backend/scripts/generate_load_test_csv.py \
  --output backend/scripts/test_data/load_test_feeds_15k.csv \
  --total 15000 \
  --prefix "$LOAD_PREFIX" \
  --echo-bucket "$ECHO_BUCKET" \
  --gcp-project "$GCP_PROJECT"
```

---

## Step 2: Execute On-Demand Phased Ramp-Up Strategy

Feeds are registered and enabled on-demand from the CSV catalog at the exact moment each phase is triggered. Run the activation script sequentially to ramp up active feeds. Allow at least **1 hour** between each phase to monitor pipeline stabilization.

### Pre-soak (100 Feeds)
```bash
python3 backend/scripts/load_test_feeds.py \
  --server "$SERVER_URL" --token "$TOKEN" --prefix "$LOAD_PREFIX" \
  activate --target-total 100 --csv backend/scripts/test_data/load_test_feeds_15k.csv
```

### Phase 1 (500 Feeds)
```bash
python3 backend/scripts/load_test_feeds.py \
  --server "$SERVER_URL" --token "$TOKEN" --prefix "$LOAD_PREFIX" \
  activate --target-total 500 --csv backend/scripts/test_data/load_test_feeds_15k.csv
```

### Phase 2 (1,000 Feeds)
```bash
python3 backend/scripts/load_test_feeds.py \
  --server "$SERVER_URL" --token "$TOKEN" --prefix "$LOAD_PREFIX" \
  activate --target-total 1000 --csv backend/scripts/test_data/load_test_feeds_15k.csv
```

### Phase 3 (2,500 Feeds)
```bash
python3 backend/scripts/load_test_feeds.py \
  --server "$SERVER_URL" --token "$TOKEN" --prefix "$LOAD_PREFIX" \
  activate --target-total 2500 --csv backend/scripts/test_data/load_test_feeds_15k.csv
```

### Phase 4 (5,000 Feeds)
```bash
python3 backend/scripts/load_test_feeds.py \
  --server "$SERVER_URL" --token "$TOKEN" --prefix "$LOAD_PREFIX" \
  activate --target-total 5000 --csv backend/scripts/test_data/load_test_feeds_15k.csv
```

### Phase 5 (8,000 Feeds)
```bash
python3 backend/scripts/load_test_feeds.py \
  --server "$SERVER_URL" --token "$TOKEN" --prefix "$LOAD_PREFIX" \
  activate --target-total 8000 --csv backend/scripts/test_data/load_test_feeds_15k.csv
```

### Phase 6 (10,000 Target Feeds)
```bash
python3 backend/scripts/load_test_feeds.py \
  --server "$SERVER_URL" --token "$TOKEN" --prefix "$LOAD_PREFIX" \
  activate --target-total 10000 --csv backend/scripts/test_data/load_test_feeds_15k.csv
```

### Phase 7+ (15,000 Feeds Upper Stress Test)
```bash
python3 backend/scripts/load_test_feeds.py \
  --server "$SERVER_URL" --token "$TOKEN" --prefix "$LOAD_PREFIX" \
  activate --target-total 15000 --csv backend/scripts/test_data/load_test_feeds_15k.csv
```



---

## Step 3: Key Metrics & Monitoring

During each 1-hour soak window, observe and record the following metrics:

1. **Ingestion Worker Cluster**:
   - CPU utilization and RSS memory footprint per container.
   - Active process counts for audio collectors.
   - Heartbeat submission success rate.
2. **Storage & Messaging**:
   - GCS audio segment upload latency and error rate.
   - Pub/Sub message publishing throughput and latency.
3. **Database Performance (AlloyDB)**:
   - Active connection pool utilization.
   - Query response latency for lease renewals and progress updates.
   - Absence of connection timeouts or lock contention.

---

## Step 4: Teardown & Cleanup

After completion of the load test:

### Deactivate Load Test Feeds
```bash
python3 backend/scripts/load_test_feeds.py \
  --server "$SERVER_URL" \
  --token "$TOKEN" \
  --prefix "$LOAD_PREFIX" \
  deactivate
```

### Hard Delete Load Test Feeds
```bash
python3 backend/scripts/load_test_feeds.py \
  --server "$SERVER_URL" \
  --token "$TOKEN" \
  --prefix "$LOAD_PREFIX" \
  delete
```
