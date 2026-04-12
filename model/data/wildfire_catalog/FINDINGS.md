# Wildfire Radio Feed Catalog — Findings & 10K Plan Review

**Date:** 2026-04-11
**Owner:** Data / Ingestion team
**Audience:** Eng leadership, Infra, Product
**Artifact:** Catalog output at `output/wildfire_feed_catalog.csv` + this brief

## Executive summary

| Finding | Decision enabled |
|---|---|
| **Ecosystem ceiling: 87,028 fire-relevant feeds** across 3 of 4 planned sources | 10K target is feasible on the supply side — but source mix in the original plan is not |
| **6,171 Tier 1 feeds** (high-confidence wildfire-relevant) | Tier 1 alone is a deployable starting set with no post-filtering |
| **`bcfy_calls` is 0 enumerable** | Cannot ship a bcfy_calls catalog without new API work — revise the 3,500 bcfy_calls target |
| **Echo ceiling: 718 device-channels** | 1,400-echo target is **infeasible** — need 2× more physical Echo devices first |
| **OpenMHZ is per-SYSTEM, not per-talkgroup, at the connection layer** | 3,500 openmhz feeds ≠ 3,500 WebSockets; multiplexing collector change needed before scaling |
| **Duty cycle varies 0.14/sec (echo) to 400/sec (bcfy_feeds)** | GCS and AlloyDB sizing must be per-source, not flat — current pipeline has no batching |
| **87% of Tier 1 sits in 15 wildfire-prone states** | Regional sharding is trivial |

---

## 1. Ecosystem vs. current deployment

The catalog (this repo's tool) enumerates what's **available** in the ecosystem. To compare vs. what's **currently deployed**, run this against prod AlloyDB:

```sql
-- Current feed mix (run from a VPC-authorized shell)
SELECT st.name AS source_type,
       COUNT(*) FILTER (WHERE f.status = 'active')      AS active,
       COUNT(*) FILTER (WHERE f.status = 'failing')     AS failing,
       COUNT(*) FILTER (WHERE f.status = 'quarantined') AS quarantined,
       COUNT(*) FILTER (WHERE f.status = 'deactivated') AS deactivated,
       COUNT(*) AS total
FROM feeds f
JOIN source_types st ON f.source_type = st.id
GROUP BY st.name
ORDER BY total DESC;
```

### Ecosystem ceiling (from this catalog)

| Source | Ecosystem total | Fire-relevant | Tier 1 | Tier 1+2 | Collector status |
|---|---:|---:|---:|---:|---|
| bcfy_feeds | 7,582 | 7,582 | 590 | 4,757 | ✅ Icecast collector shipped |
| openmhz | 461 systems / 272,512 TGs | 78,728 TGs (29%) | 5,408 | 66,258 | ✅ WebSocket collector shipped |
| bcfy_calls | **0 enumerable** | 0 | 0 | 0 | ⚠ Polling-only collector exists; no catalog API |
| echo | 23 devices / 718 channels | 718 | 173 | 554 | ✅ Cloud Function shipped |
| **Total** | 87,028 | 87,028 | **6,171** | **71,569** | |

---

## 2. OpenMHZ resource model (new section — previously under-specified)

The original 10K plan targets "75 openmhz systems". The revised mix targets "3,500 openmhz talkgroups". These are not the same thing at the infrastructure layer.

### Connection model: one WebSocket per system, **not** per talkgroup

Evidence from production code:

- **WebSocket subscribe payload uses `shortName` only** — no talkgroup filter: `_ws_transport.py:115` → `["start", {**_START_PAYLOAD_TEMPLATE, "shortName": short_name}]`
- **Collector extracts `short_name` from the feed's `source_feed_id`** and opens one WS per feed: `collector.py:119` → `short_name = source_feed_id.strip()`
- **Catalog emits one feed record per talkgroup**: `wildfire_catalog/sources/openmhz.py:72` → `source_feed_id=f"{short_name}:{num}"`
- **Cloudflare bypass**: `_ws_transport.py:88` → `AsyncSession(impersonate="chrome")` — confirmed working via `curl_cffi`

### What this means for 3,500 openmhz talkgroups

| Scenario | Active WebSockets | Cloudflare surface | Notes |
|---|---:|---|---|
| **Today (no multiplexing)** | up to **3,500 WS** | High — 47× redundant connections to same systems | Each feed-worker opens its own WS to the same `shortName` |
| **With multiplexing (proposed)** | **~75 WS** | Low — one connection per system | Requires refactor of lease-to-connection mapping |

The collector subscribes to the full-system firehose and currently **does not filter by talkgroup_num** (`collector.py:133-172` yields every call received). So if 50 fire talkgroups in the same system are leased, those 50 feed-workers independently open 50 WebSockets to the same endpoint and each re-processes the same firehose.

### Blockers this adds

1. **OpenMHZ multiplexing refactor** — share one WebSocket per `shortName` across feed leases. Estimated 1–2 weeks; touches the lease/worker contract. **Required before shipping >a few hundred openmhz feeds.**
2. **Cross-connection reconnect jitter** — `collector.py:197-200` applies per-connection exponential backoff, but every worker reconnects on the same schedule after a transient outage. Stampede risk; add jittered reconnect before scaling.

### Download / conversion bottleneck (even with multiplexing)

Per-call audio download is sequential: `collector.py:139-142` awaits `_download_m4a()` with 30s timeout, then `collector.py:146-148` runs `convert_to_flac` via `asyncio.to_thread` (one conversion at a time per feed). A busy urban system firing 50 calls/min across 50 talkgroups is bounded by download-and-convert throughput per feed worker, not by WebSocket count. This shifts the scaling constraint from network to CPU/IO on the VM.

---

## 3. Duty cycle per source type (new section — previously absent)

Settings that drive the load model:

| Parameter | Value | Source |
|---|---|---|
| Heartbeat interval | **15 sec, regardless of duty cycle** | `settings.py:55` (`HEARTBEAT_INTERVAL_SEC`) |
| Bookmark write cadence | **Once per chunk** | `normalizer_runtime.py:386` → `update_feed_progress` |
| GCS write per chunk | **1 PUT, no batching** | `normalizer_runtime.py:352` (`upload_staged_audio`) |
| Pub/Sub per chunk | **1 publish, no batching** | `normalizer_runtime.py:371` (`publish_audio_chunk`) |
| bcfy_feeds chunk duration | **15 sec fixed segments** | `icecast_collector.py:307` (`-segment_time 15`) |
| bcfy_calls chunk | **1 chunk per call (variable)** | `bcfy_calls_collector.py` |
| openmhz chunk | **1 chunk per call (variable)** | `openmhz/collector.py` |
| echo chunk | **1 chunk per whole MP3 (hourly)** | `echo/main.py` |
| Max feeds per VM worker | **250 (asyncio concurrent)** | `settings.py:45` (`MAX_FEEDS_PER_WORKER`) |

### Aggregate rates at the proposed 10K mix

Duty cycle estimates anchored in the code behavior above; echo rate is **measured from the live S3 bucket** (files arrive hourly, one per channel):

| Source | Count | Connections | Duty cycle | Chunk cadence | Chunks/sec (agg) |
|---|---:|---:|---|---|---:|
| bcfy_feeds | 6,000 | 6,000 TCP streams | ~100% (silence counts) | 1 chunk / 15s / feed | **400** |
| openmhz | 3,500 TGs / ~75 systems | 75 WS (w/ mux) OR 3,500 (today) | 5–15% (transmission-triggered) | 1 chunk / call, ~10s avg | **18–52** |
| echo | 500 channels | 0 (Eventarc/Cloud Function) | — (1 MP3/hour ≠ chunk/sec duty cycle) | 1 chunk / MP3 | **~0.14** |
| bcfy_calls | 0 | — | — | — | — |
| **Total** | **10,000** | **75 (mux) or 6,075 (current)** | | | **~418–452** |

**Math (for verifiability):**
- bcfy_feeds: 6,000 / 15 = 400 chunks/sec
- openmhz: 3,500 TGs × duty_pct / 10s avg call = 18 (5%) to 52 (15%) calls/sec
- echo: 500 channels / 3,600s per hour = 0.14 chunks/sec *(verified: samples from `s3://echo-recordings/ca_chico/20260410/`, `az_flagstaff/`, `wa_cathlamet/` all show one file per channel per hour)*

### Where the prior brief was off

The original 10K plan assumed uniform 60% duty cycle across all 10K feeds → 333 chunks/sec and ~$2,628/month GCS. Two corrections:

1. **bcfy_feeds is always 100% duty cycle** because ffmpeg emits a 15s segment whether or not there's voice. You can't save GCS operations by reducing duty cycle on bcfy_feeds — the 400 chunks/sec baseline is fixed by segment_time.
2. **echo is not 10–30% of a 15s-chunk duty cycle** — it's hourly uploads that each become a single, much-longer FLAC chunk. Real contribution to chunks/sec is ~0.14, not 5–15.

Net: at 10K feeds on the revised mix, real chunks/sec ≈ **418–452** (not 333).

---

## 4. Cost-model deltas this unlocks

### GCS Class A operations

At current no-batching runtime:

| Rate | Monthly PUTs | Cost @ $0.005/1K ops |
|---|---:|---:|
| 418/sec (low) | 1.08 B | **~$5,400/month** |
| 452/sec (high) | 1.17 B | **~$5,900/month** |

If we add **2:1 batching** (group 2 chunks per upload — not currently implemented):

| Rate | Monthly PUTs | Cost @ $0.005/1K ops |
|---|---:|---:|
| 209/sec (low) | 0.54 B | **~$2,700/month** |
| 226/sec (high) | 0.59 B | **~$2,950/month** |

Batching adds ~15s latency to audio arrival in Pub/Sub — acceptable for our transcription SLA but **not free**: the runtime needs a small rewrite (batch-write then Pub/Sub publish). Worth ~$2,700/month if the math holds.

> Pricing assumption: Google Cloud Storage Class A operations at US multi-region Standard tier, $0.005 per 1,000 operations. Substitute negotiated rate if different.

### AlloyDB write load

Heartbeats are fixed by the 15s timer, regardless of duty cycle:

| Component | Rate |
|---|---:|
| Heartbeat floor (10K feeds / 15s) | **667/sec** |
| Bookmark writes (= chunks/sec) | 418–452/sec |
| **Total writes/sec** | **~1,084–1,119/sec** |

The original 10K plan projected 1,200/sec. We're ~100/sec under, so re-run the AlloyDB sizing test before committing to a vCPU upgrade. Current 2 vCPU instance may be sufficient.

### Network bandwidth

bcfy_feeds dominates ingress (continuous MP3):
- 6,000 feeds × 16 kbps = **96 Mbps sustained**
- openmhz + echo intermittent, negligible average

Well under any reasonable egress tier.

---

## 5. The 10K plan, revised

| Original target | Available | Realistic | Delta |
|---|---|---|---|
| 5,000 bcfy_feeds | 7,582 | **6,000** ✅ | +1,000 |
| 3,500 bcfy_calls | **0 enumerable** | **0** ❌ | −3,500 (blocker) |
| 75 openmhz systems | 333 active | **3,500 talkgroups / ~75 systems** ✅ | +3,425 TGs |
| 1,400 echo | 718 | **500** (all Tier 1+2) ⚠ | −900 (HW-bound) |
| **Total: 10,000** | | **Total: 10,000** | Mix shifted from systems→talkgroups on openmhz |

Same headline number; mix shifted to what's actually shippable.

---

## 6. Geographic coverage (Tier 1)

87% of Tier 1 sits in 15 wildfire-prone states — regional sharding is trivial.

| State | Tier 1 | Notes |
|---|---:|---|
| TX | 1,927 | OpenMHZ-heavy |
| CO | 1,346 | OpenMHZ-heavy (CO DTRS talkgroups) |
| CA | 657 | All 3 sources (CAL FIRE, NIFC) |
| UT | 463 | OpenMHZ-heavy |
| OK | 401 | OpenMHZ-only — was **0 under the prior binary state heuristic** |
| FL | 251 | OpenMHZ + bcfy_feeds |
| MS | 216 | OpenMHZ-only — was **0 under the prior heuristic** |
| AZ | 174 | All 3 sources |
| SC | 132 | — |
| OR | 130 | Echo + bcfy_feeds |

Oklahoma and Mississippi previously scored 0 Tier 1 under the binary "15 high-risk states = 0.8, everything else = 0.3" heuristic. With USDA WRC county-level data now wired up, they both show significant Tier 1 volume. This confirms the user's concern about heuristic coarseness was valid.

---

## 7. Blockers (ordered by urgency)

### Critical (blocks 10K rollout)

1. **OpenMHZ WebSocket multiplexing**
   - Without it, 3,500 openmhz feeds = 3,500 WebSockets = 47× redundant connections per system
   - Violates reasonable read of Cloudflare acceptable-use for a community-maintained open service
   - **Action:** refactor `backend/pipeline/ingestion/collectors/openmhz/collector.py` to share one WebSocket per `shortName` across feed leases
   - **Effort:** 1–2 weeks (touches lease/worker contract)

2. **Memory-per-feed benchmark**
   - Unmeasured; blocks VM sizing decision (n2-standard-4 @ 500 feeds/VM vs. n2-highmem-4 @ 300 feeds/VM)
   - **Action:** run 50-100 representative feeds on one n2-standard-4 for 24h, measure RSS growth. Single highest-value data point missing.
   - **Effort:** 2–3 days

3. **`bcfy_calls` enumeration**
   - No working catalog endpoint in Broadcastify Calls API (verified exhaustively — only `/calls/v1/live/` polling and `/calls/v1/group_get/{groupId}` detail work)
   - **Options:** (a) ask Broadcastify for an endpoint, (b) scrape broadcastify.com/calls/ web UI, (c) buy RadioReference export
   - **Recommendation:** drop from 10K plan, pursue (c) on a separate track

### Important (refines sizing)

4. **Duty cycle validation from prod**
   - Estimates above are from-the-code reasoning. Validate against prod telemetry:
   ```sql
   SELECT st.name,
          COUNT(*) AS feeds,
          AVG(EXTRACT(EPOCH FROM (last_bookmark_time - created_at))) AS avg_active_sec,
          AVG(failure_count) AS avg_failures
   FROM feeds f
   JOIN source_types st ON f.source_type = st.id
   WHERE f.last_bookmark_time IS NOT NULL
   GROUP BY st.name;
   ```
   Cross-reference with AlloyDB Insights for actual bookmark-update rate per source type.

5. **FEMA NRI URL fragility**
   - FEMA's direct-download URL returns an HTML landing page; we're using USDA WRC as fallback (3,143 counties, stable URL at wildfirerisk.org)
   - If leadership wants FEMA-authoritative data, someone needs to chase the current URL with FEMA. WRC data is good enough for ranking.

### Out of eng control

6. **Echo device hardware expansion**
   - 1,400-echo target in the 10K plan requires 2× more Echo devices deployed
   - Catalog can't manufacture devices — partner/hardware work
   - Realistic target with current hardware: 500 channels

---

## 8. What's shippable today

- **6,171 Tier 1 feeds** with priority scores, canonical IDs, scoring breakdowns (`output/wildfire_feed_catalog.csv`, 87% in 15 states)
- **71,569 Tier 1+2 admin-review catalog** (`output/wildfire_feed_catalog_admin_review.csv`)
- **Per-county wildfire risk from USDA WRC** (3,143 counties, 0–1 percentile)
- **Cached raw API responses** — re-runs complete in seconds
- **JSONL classification log** — every feed's matched keywords + scoring rationale

Build tool is idempotent and cache-backed: the team can iterate on scoring/filtering without re-hitting APIs. Location: `radio-transcription/model/data/wildfire_catalog/`.

---

## 9. Recommended next actions

| # | Action | Owner | Timeline |
|---|---|---|---|
| 1 | Run memory-per-feed benchmark (50–100 feeds, 24h) | Infra | This week |
| 2 | Query prod `feeds` table, validate current mix vs. plan | Data/Ingestion | This week |
| 3 | Run the duty cycle validation SQL above | Data/Ingestion | This week |
| 4 | Decide `bcfy_calls` fate (drop / ask / buy RadioReference) | Leadership | Next sprint |
| 5 | Scope OpenMHZ multiplexing refactor | Ingestion | Next sprint |
| 6 | Admins flag `admin_selected=true` in Tier 1 CSV | Ops | Rolling |

---

## Appendix A — Code citations (for the curious)

All cited with repo-relative paths + line numbers, verified 2026-04-11:

| Claim | File | Line |
|---|---|---:|
| OpenMHZ WebSocket subscribes by shortName only | `backend/pipeline/ingestion/collectors/openmhz/_ws_transport.py` | 115 |
| OpenMHZ uses curl_cffi chrome impersonation | `backend/pipeline/ingestion/collectors/openmhz/_ws_transport.py` | 88 |
| OpenMHZ collector strips feed ID to short_name | `backend/pipeline/ingestion/collectors/openmhz/collector.py` | 119 |
| OpenMHZ sequential download + conversion | `backend/pipeline/ingestion/collectors/openmhz/collector.py` | 139–148 |
| OpenMHZ per-connection backoff (no cross jitter) | `backend/pipeline/ingestion/collectors/openmhz/collector.py` | 197–200 |
| Heartbeat interval 15s default | `backend/pipeline/ingestion/settings.py` | 55 |
| Max feeds per VM worker = 250 | `backend/pipeline/ingestion/settings.py` | 45 |
| Bookmark write on every chunk | `backend/pipeline/ingestion/normalizer_runtime.py` | 386 |
| GCS upload per chunk (no batching) | `backend/pipeline/ingestion/normalizer_runtime.py` | 352 |
| Pub/Sub publish per chunk | `backend/pipeline/ingestion/normalizer_runtime.py` | 371 |
| ffmpeg segment time = 15s | `backend/pipeline/ingestion/collectors/icecast_collector.py` | 307 |
| Chunk duration constant | `backend/pipeline/common/constants.py` | 4 |
| Catalog emits feed per talkgroup | `model/data/wildfire_catalog/sources/openmhz.py` | 72 |

## Appendix B — Running the catalog

```bash
cd radio-transcription/model/data/wildfire_catalog
# Secrets from GCP Secret Manager
export BCFY_API_KEY=$(gcloud secrets versions access latest --secret=broadcastify-api-key-prod)
export BROADCASTIFY_API_KEY_ID=$(gcloud secrets versions access latest --secret=broadcastify-api-key-id-prod)
export BROADCASTIFY_APP_ID=$(gcloud secrets versions access latest --secret=broadcastify-api-app-id-prod)

./.venv/bin/python run_catalog.py --skip-bcfy-calls --output-dir ./output
```

Full run: ~15 minutes uncached, seconds with warm cache.
