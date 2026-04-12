# Wildfire Radio Feed Catalog — Findings & 10K Plan Review

**Date:** 2026-04-11
**Owner:** Data / Ingestion team
**Audience:** Eng leadership, Infra, Product
**Artifact:** Catalog output at `output/wildfire_feed_catalog.csv` + this brief

## Executive summary

| Finding | Decision enabled |
|---|---|
| **Ecosystem ceiling: 8,688 fire-relevant feeds** across 3 of 4 planned sources | The original 10K headline is unreachable from supply alone — max realistic is ~8.4K |
| **863 Tier 1 feeds** (high-confidence wildfire-relevant) | Tier 1 alone is a deployable starting set with no post-filtering |
| **`bcfy_calls` is 0 enumerable** | Cannot ship a bcfy_calls catalog without new API work — the bcfy_calls target needs revision |
| **Echo ceiling: 718 channels across 23 devices** | 1,400-echo target is **infeasible** — need ~2× more physical Echo devices first |
| **OpenMHZ unit = 1 system = 1 WebSocket** | Feed bookkeeping, VM sizing, and Cloudflare surface all scale with system count, not anything finer |
| **Duty cycle varies 0.14/sec (echo) to 400/sec (bcfy_feeds)** | GCS and AlloyDB sizing must be per-source, not flat — current pipeline has no batching |
| **86% of Tier 1 sits in 15 wildfire-prone states** | Regional sharding is trivial |

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

The unit in every row below is a **feed record** as it will land in the `feeds` table — one bcfy_feeds Icecast stream, one OpenMHZ system, or one Echo channel. Tier is assigned by the best-scoring audio source within that feed.

| Source | Ecosystem total | Fire-relevant | Tier 1 | Tier 1+2 | Collector status |
|---|---:|---:|---:|---:|---|
| bcfy_feeds | 7,582 streams | 7,582 | 590 | 4,757 | ✅ Icecast collector shipped |
| openmhz | 461 systems (333 active) | 388 systems | 100 | 381 | ✅ WebSocket collector shipped |
| bcfy_calls | **0 enumerable** | 0 | 0 | 0 | ⚠ Polling-only collector exists; no catalog API |
| echo | 23 devices / 718 channels | 718 | 173 | 554 | ✅ Cloud Function shipped |
| **Total** | 8,761 | 8,688 | **863** | **5,692** | |

---

## 2. OpenMHZ resource model

### The unit is a system, not anything finer

An OpenMHZ feed, in our pipeline, is **one system**: one `shortName`, one WebSocket connection, one row in the `feeds` table. Everything the infrastructure has to count — connections, file descriptors, memory, heartbeats, leases, Cloudflare rate-limit headroom — scales with system count. Using anything finer as the feed unit creates redundant WebSocket connections to the same endpoint with no data-volume benefit.

Evidence from production code:

- **WebSocket subscribe payload is keyed by `shortName` only** — `_ws_transport.py:115` → `["start", {**_START_PAYLOAD_TEMPLATE, "shortName": short_name}]`. One subscription delivers every call from that system.
- **Collector opens one WebSocket per feed lease** — `collector.py:119` → `short_name = source_feed_id.strip()`. With one feed per system, that's one WebSocket per system.
- **Cloudflare bypass via TLS impersonation** — `_ws_transport.py:88` → `AsyncSession(impersonate="chrome")`. Confirmed working end-to-end via `curl_cffi`.

### What the OpenMHZ slice looks like at scale

| Metric | Value at proposed 75-system deployment |
|---|---|
| Active WebSocket connections | **75** (one per feed) |
| File descriptors per VM (250 feeds/VM) | 75 if all OpenMHZ on one VM; typically mixed across VMs |
| Cloudflare surface | 75 concurrent long-lived TLS sessions with Chrome fingerprint |
| Reconnect stampede risk | Per-connection exponential backoff at `collector.py:197-200`, **no cross-connection jitter** — all workers reconnect on the same schedule after a transient outage |

### Within each system: the real bottleneck

Each WebSocket delivers the system's full call firehose. Per call, the worker sequentially: downloads the m4a (`collector.py:139-142`, 30s timeout), converts to FLAC via `asyncio.to_thread` (`collector.py:146-148`, one at a time), uploads to GCS, publishes to Pub/Sub. A busy urban system firing 50+ calls/min saturates that pipeline on CPU/IO, not on network. This is the constraint that drives per-VM feed density, not WebSocket count.

### Known behaviors to validate before scaling

1. **Cross-connection reconnect jitter.** Add jittered reconnect before scaling to 75+ concurrent OpenMHZ WebSockets so a transient Cloudflare block doesn't trigger a stampede.
2. **Call filtering.** The collector currently yields every call received on the WebSocket (`collector.py:133-172`). Since the fire-relevance signal is in each call's metadata (alpha tag, description), filter at the collector rather than downstream to reduce GCS writes and transcription cost.

---

## 3. Duty cycle per source type

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

### Aggregate rates at the proposed mix

Duty cycle estimates anchored in the code behavior above; echo rate is **measured from the live S3 bucket** (files arrive hourly, one per channel):

| Source | Count | Connections | Duty cycle | Chunk cadence | Chunks/sec (agg) |
|---|---:|---:|---|---|---:|
| bcfy_feeds | 6,000 | 6,000 TCP streams | ~100% (silence counts) | 1 chunk / 15s / feed | **400** |
| openmhz | 75 systems | 75 WS | per-system varies; each system fires ~5–15% of the day on active calls | 1 chunk / call, ~10s avg | **18–52** |
| echo | 500 channels | 0 (Eventarc/Cloud Function) | — (1 MP3/hour ≠ chunk/sec duty cycle) | 1 chunk / MP3 | **~0.14** |
| bcfy_calls | 0 | — | — | — | — |
| **Total** | **6,575** | **6,075** | | | **~418–452** |

**Math (for verifiability):**
- bcfy_feeds: 6,000 / 15 = 400 chunks/sec
- openmhz: 75 systems × ~0.25–0.7 fire-relevant calls/sec per system = 18–52 calls/sec aggregate. Per-system rate varies widely (rural systems fire <1 call/min, busy urban systems 50+ calls/min). Validate against prod telemetry (see §7).
- echo: 500 channels / 3,600s per hour = 0.14 chunks/sec *(verified: samples from `s3://echo-recordings/ca_chico/20260410/`, `az_flagstaff/`, `wa_cathlamet/` all show one file per channel per hour)*

### Where the prior brief was off

The original 10K plan assumed uniform 60% duty cycle across all 10K feeds → 333 chunks/sec and ~$2,628/month GCS. Two corrections:

1. **bcfy_feeds is always 100% duty cycle** because ffmpeg emits a 15s segment whether or not there's voice. You can't save GCS operations by reducing duty cycle on bcfy_feeds — the 400 chunks/sec baseline is fixed by segment_time.
2. **echo is not 10–30% of a 15s-chunk duty cycle** — it's hourly uploads that each become a single, much-longer FLAC chunk. Real contribution to chunks/sec is ~0.14, not 5–15.

Net: at the revised mix of **6,575 feeds**, real chunks/sec ≈ **418–452** (the original plan's 333 was for a hypothetical 10K-feed deployment; the supply-side constraints in §5 bring the realistic count down).

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

| Component | Rate at 6,575 feeds |
|---|---:|
| Heartbeat floor (6,575 feeds / 15s) | **438/sec** |
| Bookmark writes (= chunks/sec) | 418–452/sec |
| **Total writes/sec** | **~856–890/sec** |

The original 10K plan projected 1,200/sec AlloyDB writes. At the revised 6,575-feed mix we're 300+/sec under that projection. The 2 vCPU AlloyDB instance very likely remains sufficient — **do not provision the vCPU upgrade without re-running the sizing test**.

### Network bandwidth

bcfy_feeds dominates ingress (continuous MP3):
- 6,000 feeds × 16 kbps = **96 Mbps sustained**
- openmhz + echo intermittent, negligible average

Well under any reasonable egress tier.

---

## 5. The 10K plan, revised

| Original target | Available | Realistic | Delta |
|---|---|---|---|
| 5,000 bcfy_feeds | 7,582 streams | **6,000** ✅ | +1,000 |
| 3,500 bcfy_calls | **0 enumerable** | **0** ❌ | −3,500 (blocker) |
| 75 openmhz systems | 333 active | **75** ✅ (all Tier 1 systems + highest-duty Tier 2) | on target |
| 1,400 echo | 718 channels | **500** (all Tier 1+2) ⚠ | −900 (HW-bound) |
| **Total: 10,000** | | **Total: 6,575** | 10K headline unreachable from current supply |

**The 10K headline number does not survive contact with the supply side.** With `bcfy_calls` blocked and Echo hardware-bound, max realistic deployment is ~6.5K feeds from today's ecosystem. To close the gap to 10K leadership would need to either (a) unblock `bcfy_calls` enumeration, (b) expand Echo hardware deployment, or (c) accept the 6.5K figure as the real target. Option (c) still delivers the full Tier 1 set (863 feeds) and wide geographic coverage of wildfire-prone states.

---

## 6. Geographic coverage (Tier 1)

Tier 1 feed counts per state, counted at the feed-record level (bcfy_feeds + openmhz systems + echo channels):

| State | Tier 1 total | bcfy_feeds | openmhz systems | echo |
|---|---:|---:|---:|---:|
| CA | 288 | 227 | 15 | 46 |
| WA | 80 | 14 | 7 | 59 |
| TX | 76 | 62 | 14 | 0 |
| OR | 66 | 26 | 7 | 33 |
| AZ | 52 | 17 | 5 | 30 |
| CO | 41 | 21 | 20 | 0 |
| FL | 38 | 34 | 4 | 0 |
| OK | 33 | 30 | 3 | 0 |
| SC | 16 | 14 | 2 | 0 |
| NC | 16 | 13 | 3 | 0 |

**Geographic concentration.** Using the 15 wildfire-prone states (CA, OR, WA, CO, MT, ID, AZ, NM, NV, UT, TX, FL, GA, SC, NC) as the sharding boundary, those states capture the large majority of Tier 1 feeds. Regional sharding is straightforward.

**Heuristic-to-data win.** Oklahoma and Mississippi both scored 0 Tier 1 under the earlier binary "15 high-risk states = 0.8, everything else = 0.3" heuristic. With USDA WRC county-level data wired up, OK has 33 Tier 1 feeds — still modest, but meaningfully non-zero. This confirms the prior concern about heuristic coarseness was warranted.

---

## 7. Blockers (ordered by urgency)

### Critical (blocks 10K rollout)

1. **Memory-per-feed benchmark**
   - Unmeasured; blocks VM sizing decision (n2-standard-4 @ 500 feeds/VM vs. n2-highmem-4 @ 300 feeds/VM)
   - **Action:** run 50-100 representative feeds on one n2-standard-4 for 24h, measure RSS growth. Single highest-value data point missing.
   - **Effort:** 2–3 days

2. **`bcfy_calls` enumeration**
   - No working catalog endpoint in Broadcastify Calls API (verified exhaustively — only `/calls/v1/live/` polling and `/calls/v1/group_get/{groupId}` detail work)
   - **Options:** (a) ask Broadcastify for an endpoint, (b) scrape broadcastify.com/calls/ web UI, (c) buy RadioReference export
   - **Recommendation:** drop from the 6.5K plan; pursue (c) on a separate track to reopen the path to 10K

### Important (refines sizing)

3. **Duty cycle validation from prod**
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

4. **FEMA NRI URL fragility**
   - FEMA's direct-download URL returns an HTML landing page; we're using USDA WRC as fallback (3,143 counties, stable URL at wildfirerisk.org)
   - If leadership wants FEMA-authoritative data, someone needs to chase the current URL with FEMA. WRC data is good enough for ranking.

### Out of eng control

5. **Echo device hardware expansion**
   - 1,400-echo target in the 10K plan requires ~2× more Echo devices deployed
   - Catalog can't manufacture devices — partner/hardware work
   - Realistic target with current hardware: 500 channels

---

## 8. What's shippable today

- **863 Tier 1 feeds** ready for enablement: 590 bcfy_feeds + 100 openmhz systems + 173 echo channels, all scored with priority, agency tier, and geographic risk
- **5,692 Tier 1+2 feeds** in the admin review CSV for broader selection
- **Per-county wildfire risk from USDA WRC** (3,143 counties, 0–1 percentile)
- **Cached raw API responses** — re-runs complete in seconds
- **Classification log (JSONL)** — every feed's matched keywords + scoring rationale, one JSON line per entry

Build tool is idempotent and cache-backed: the team can iterate on scoring/filtering without re-hitting APIs. Location: `radio-transcription/model/data/wildfire_catalog/`.

---

## 9. Recommended next actions

| # | Action | Owner | Timeline |
|---|---|---|---|
| 1 | Run memory-per-feed benchmark (50–100 feeds, 24h) | Infra | This week |
| 2 | Query prod `feeds` table, validate current mix vs. plan | Data/Ingestion | This week |
| 3 | Run the duty cycle validation SQL above | Data/Ingestion | This week |
| 4 | Decide `bcfy_calls` fate (drop / ask / buy RadioReference) | Leadership | Next sprint |
| 5 | Add jittered reconnect to OpenMHZ collector before scaling to 75 concurrent systems | Ingestion | Next sprint |
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
