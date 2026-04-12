# Wildfire Radio Feed Catalog — Findings & 10K Plan Review

**Date:** 2026-04-11
**Owner:** Data / Ingestion team
**Audience:** Eng leadership, Infra, Product
**Artifact:** Catalog output at `output/wildfire_feed_catalog.csv` + this brief

## Executive summary

| Finding | Decision enabled |
|---|---|
| **All four sources enumerable.** 16,790 fire-relevant feeds discovered across bcfy_feeds, openmhz, bcfy_calls, echo | 10K headline is now **achievable** with a realistic source mix |
| **2,026 Tier 1 feeds** (high-confidence wildfire-relevant) | Tier 1 alone is a deployable starting set with no post-filtering |
| **`bcfy_calls` catalog endpoints now in use** — `POST /calls/v1/groups_ctid/{ctid}` + `GET /calls/v1/playlists_public` | Catalog coverage for bcfy_calls is no longer a blocker (was the big gap previously) |
| **Echo ceiling: 718 channels across 23 devices** | 1,400-echo target is infeasible — need ~2× more physical Echo devices first |
| **OpenMHZ unit = 1 system = 1 WebSocket** | Feed bookkeeping, VM sizing, and Cloudflare surface all scale with system count |
| **bcfy_calls unit = 1 groupId = 1 polling loop** (every 10s per feed) | 3,500 bcfy_calls feeds = **350 polls/sec aggregate**. This is a non-trivial per-endpoint load on Broadcastify — worth flagging to them before scale-up. |
| **Duty cycle varies 0.14/sec (echo) to 400/sec (bcfy_feeds)** | GCS and AlloyDB sizing must be per-source, not flat — current pipeline has no batching |
| **88% of Tier 1 sits in 15 wildfire-prone states** | Regional sharding is trivial |

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

Each source has a natural unit at the collector layer — bcfy_feeds = 1 Icecast stream, openmhz = 1 system (one WebSocket carries all of a system's calls), bcfy_calls = 1 groupId (one polling loop per groupId), echo = 1 device-channel. Tier is assigned by the best-scoring content within that feed.

| Source | Ecosystem total | Fire-relevant | Tier 1 | Tier 1+2 | Collector status |
|---|---:|---:|---:|---:|---|
| bcfy_feeds | 7,582 streams | 7,582 | 590 | 4,757 | ✅ Icecast collector shipped |
| openmhz | 461 systems (333 active) | 388 systems | 100 | 381 | ✅ WebSocket collector shipped |
| bcfy_calls | 8,102 groups (across 234 RR systems) | 8,102 | 1,171 | 6,335 | ✅ Polling collector shipped |
| echo | 23 devices / 718 channels | 718 | 173 | 554 | ✅ Cloud Function shipped |
| **Total** | 16,863 | 16,790 | **2,034** | **12,027** | |

The Tier 1 total of 2,034 above is per-source sums; when the same radio channel appears on more than one source (canonical_id match on `rr:{sid}:{tg}`), the **unique** Tier 1 count is **2,026** — a handful of bcfy_calls ↔ openmhz cross-platform duplicates.

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
| Cloudflare surface | 75 concurrent long-lived TLS sessions with Chrome fingerprint |
| Reconnect stampede risk | Per-connection exponential backoff at `collector.py:197-200`, **no cross-connection jitter** — all workers reconnect on the same schedule after a transient outage |

### Within each system: the real bottleneck

Each WebSocket delivers the system's full call firehose. Per call, the worker sequentially: downloads the m4a (`collector.py:139-142`, 30s timeout), converts to FLAC via `asyncio.to_thread` (`collector.py:146-148`, one at a time), uploads to GCS, publishes to Pub/Sub. A busy urban system firing 50+ calls/min saturates that pipeline on CPU/IO, not on network. This is the constraint that drives per-VM feed density.

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
| bcfy_calls chunk | **1 chunk per call** (variable duration) | `bcfy_calls_collector.py` |
| bcfy_calls poll interval | **10 sec per feed** | `bcfy_calls_collector.py:26` (`_POLL_INTERVAL_SEC = 10.0`) |
| openmhz chunk | **1 chunk per call** (variable duration) | `openmhz/collector.py` |
| echo chunk | **1 chunk per whole MP3** (hourly uploads) | `echo/main.py` |
| Max feeds per VM worker | **250 (asyncio concurrent)** | `settings.py:45` (`MAX_FEEDS_PER_WORKER`) |

### Aggregate rates at the proposed 10K mix

| Source | Count | Connections | Duty cycle | Chunk cadence | Chunks/sec (agg) |
|---|---:|---:|---|---|---:|
| bcfy_feeds | 6,000 | 6,000 TCP streams | ~100% (silence counts) | 1 chunk / 15s / feed | **400** |
| openmhz | 75 systems | 75 WS | per-system varies; each fires ~5–15% of the day on active calls | 1 chunk / call, ~10s avg | **18–52** |
| bcfy_calls | 3,500 groups | 3,500 polling loops @ 10s interval | 5–15% (transmission-triggered) | 1 chunk / call, ~10s avg | **18–52** |
| echo | 500 channels | 0 (Eventarc/Cloud Function) | — (1 MP3/hour) | 1 chunk / MP3 | **~0.14** |
| **Total** | **10,075** | **6,075 persistent + 3,500 pollers** | | | **~436–504** |

**Math (for verifiability):**
- bcfy_feeds: 6,000 / 15 = 400 chunks/sec
- openmhz: 75 systems × ~0.25–0.7 fire-relevant calls/sec per system = 18–52 chunks/sec
- bcfy_calls: 3,500 groups × ~0.005–0.015 calls/sec per group = 18–52 chunks/sec *(similar-shape load to openmhz in aggregate)*
- echo: 500 channels / 3,600s per hour = 0.14 chunks/sec *(verified from live S3 listings)*

### Broadcastify API polling load — a new concern

At 3,500 bcfy_calls feeds polling every 10 seconds (`_POLL_INTERVAL_SEC = 10.0`), aggregate poll rate is **350 requests/sec to Broadcastify's `/calls/v1/live/` endpoint**. This is orders of magnitude above the 2 QPS catalog-build rate limit and is sustained, not bursty. Before ramping past a few hundred bcfy_calls feeds we should:

1. Confirm acceptable-use with Broadcastify (their docs don't publish an explicit per-partner rate limit).
2. Consider increasing `_POLL_INTERVAL_SEC` — doubling to 20s halves load to 175/sec while adding ≤20s latency.
3. Monitor for 429 responses in prod; the existing retry logic handles transient 429s but sustained 429s would degrade ingestion.

### Where the prior brief was off

Previous iterations of this brief assumed uniform 60% duty cycle across a hypothetical 10K deployment → 333 chunks/sec. Two corrections stand:

1. **bcfy_feeds is always 100% duty cycle** because ffmpeg emits a 15s segment whether or not there's voice. You can't save GCS operations by reducing duty cycle on bcfy_feeds — the 400 chunks/sec baseline is fixed by segment_time.
2. **echo is not 10–30% of a 15s-chunk duty cycle** — it's hourly uploads that each become a single long FLAC chunk. Real contribution to chunks/sec is ~0.14, not 5–15.

Net at the realistic 10,075-feed mix: **~436–504 chunks/sec**, dominated by bcfy_feeds.

---

## 4. Cost-model deltas this unlocks

### GCS Class A operations

At current no-batching runtime:

| Rate | Monthly PUTs | Cost @ $0.005/1K ops |
|---|---:|---:|
| 436/sec (low) | 1.13 B | **~$5,650/month** |
| 504/sec (high) | 1.31 B | **~$6,530/month** |

If we add **2:1 batching** (group 2 chunks per upload — not currently implemented):

| Rate | Monthly PUTs | Cost @ $0.005/1K ops |
|---|---:|---:|
| 218/sec (low) | 0.57 B | **~$2,830/month** |
| 252/sec (high) | 0.65 B | **~$3,270/month** |

Batching adds ~15s latency to audio arrival in Pub/Sub — acceptable for our transcription SLA but not free: the runtime needs a small rewrite (batch-write then Pub/Sub publish). Worth ~$2,800/month if the math holds.

> Pricing assumption: Google Cloud Storage Class A operations at US multi-region Standard tier, $0.005 per 1,000 operations. Substitute negotiated rate if different.

### AlloyDB write load

Heartbeats are fixed by the 15s timer, regardless of duty cycle:

| Component | Rate at 10,075 feeds |
|---|---:|
| Heartbeat floor (10,075 / 15s) | **672/sec** |
| Bookmark writes (= chunks/sec) | 436–504/sec |
| **Total writes/sec** | **~1,108–1,176/sec** |

Within the 1,200/sec original projection. Current 2 vCPU AlloyDB *may* be sufficient — **do not provision the vCPU upgrade without re-running the sizing test** (see §7 duty-cycle validation query).

### Network bandwidth

bcfy_feeds dominates ingress (continuous MP3):
- 6,000 feeds × 16 kbps = **96 Mbps sustained**
- openmhz + bcfy_calls + echo: intermittent, each well under 10 Mbps average

Well under any reasonable egress tier.

---

## 5. The 10K plan, revised

With `bcfy_calls` enumeration now working, the original 10K headline is **achievable from current ecosystem supply**:

| Original target | Ecosystem available | Proposed | Delta |
|---|---|---|---|
| 5,000 bcfy_feeds | 7,582 streams | **6,000** ✅ | +1,000 |
| 3,500 bcfy_calls | 8,102 fire-relevant groups | **3,500** ✅ (all 1,171 Tier 1 + 2,329 Tier 2) | on target |
| 75 openmhz systems | 461 systems (333 active) | **75** ✅ (all 100 Tier 1 systems narrowed to top 75) | on target |
| 1,400 echo | 718 channels | **500** (all Tier 1+2) ⚠ | −900 (HW-bound) |
| **Total: 10,000** | | **Total: 10,075** ✅ | — |

### Why we can't quite hit 1,400 echo

The Echo fleet physically has 718 channels across 23 devices today. Expanding requires either deploying new Echo devices (partner/hardware work, outside eng) or removing a channel here in exchange for two there (no real benefit). The practical ceiling for echo is 718; any number above that is speculative. 500 is Tier 1+2 and matches what we can validate today.

To actually reach 1,400 echo someday, that's a hardware roadmap question — can surface to Product if they want to push for it.

---

## 6. Geographic coverage (Tier 1)

Tier 1 feed counts per state, counted at the feed-record level across all four sources:

| State | Tier 1 total | bcfy_feeds | openmhz systems | bcfy_calls | echo |
|---|---:|---:|---:|---:|---:|
| CA | 545 | 227 | 15 | 257 | 46 |
| TX | 535 | 62 | 14 | 459 | 0 |
| FL | 198 | 34 | 4 | 160 | 0 |
| CO | 94 | 21 | 20 | 53 | 0 |
| WA | 93 | 14 | 7 | 13 | 59 |
| OK | 92 | 30 | 3 | 59 | 0 |
| OR | 75 | 26 | 7 | 9 | 33 |
| AZ | 62 | 17 | 5 | 10 | 30 |
| KS | 49 | 12 | 1 | 36 | 0 |
| SC | 44 | 14 | 2 | 28 | 0 |

**Geographic concentration.** 88% (1,777/2,026) of Tier 1 feeds fall within the 15 wildfire-prone states (CA, OR, WA, CO, MT, ID, AZ, NM, NV, UT, TX, FL, GA, SC, NC). Regional sharding of the ingestion pipeline is straightforward.

**Source complementarity.** Most states draw Tier 1 volume from ≥2 sources, which validates the multi-source strategy — no single source dominates everywhere. CA and TX stand out: CA has strong contribution from all four sources; TX is bcfy_calls-heavy due to the Alamo Area Regional Radio System (AARRS) and similar P25 statewide systems with rich fire-tagged talkgroups.

**Heuristic-to-data win.** Oklahoma scored 0 Tier 1 under the earlier binary "15 high-risk states = 0.8, everything else = 0.3" heuristic. With USDA WRC county-level data wired up, OK has 92 Tier 1 feeds — confirming the prior concern about heuristic coarseness.

---

## 7. Blockers (ordered by urgency)

### Critical (blocks 10K rollout)

1. **Memory-per-feed benchmark**
   - Unmeasured; blocks VM sizing decision (n2-standard-4 @ 500 feeds/VM vs. n2-highmem-4 @ 300 feeds/VM)
   - **Action:** run 50-100 representative feeds on one n2-standard-4 for 24h, measure RSS growth. Single highest-value data point missing.
   - **Effort:** 2–3 days

2. **Broadcastify `/calls/v1/live/` polling-rate acceptable-use**
   - At 3,500 bcfy_calls feeds with `_POLL_INTERVAL_SEC=10`, aggregate poll rate = 350/sec sustained
   - No published per-partner rate limit in Broadcastify docs
   - **Action:** confirm with Broadcastify support before scaling past ~500 bcfy_calls feeds. If 350/sec is too high, bump `_POLL_INTERVAL_SEC` to 20s (halves load, adds 10s latency).
   - **Effort:** 1 email + config change

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
   - 1,400-echo target requires ~2× more Echo devices deployed
   - Catalog can't manufacture devices — partner/hardware work
   - Realistic target with current hardware: 500 channels

---

## 8. What's shippable today

- **2,026 unique Tier 1 feeds** ready for enablement: 590 bcfy_feeds + 100 openmhz systems + 1,171 bcfy_calls groups + 173 echo channels, all scored with priority, agency tier, and geographic risk
- **12,027 Tier 1+2 feeds** in the admin review CSV for broader selection
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
| 4 | Confirm `/calls/v1/live/` polling rate with Broadcastify | Ingestion | This week |
| 5 | Add jittered reconnect to OpenMHZ collector before scaling to 75 concurrent systems | Ingestion | Next sprint |
| 6 | Admins flag `admin_selected=true` in Tier 1 CSV | Ops | Rolling |

---

## Appendix A — Code + API citations (for the curious)

All cited with repo-relative paths + line numbers, verified 2026-04-11:

| Claim | File | Line |
|---|---|---:|
| OpenMHZ WebSocket subscribes by shortName only | `backend/pipeline/ingestion/collectors/openmhz/_ws_transport.py` | 115 |
| OpenMHZ uses curl_cffi chrome impersonation | `backend/pipeline/ingestion/collectors/openmhz/_ws_transport.py` | 88 |
| OpenMHZ collector strips feed ID to short_name | `backend/pipeline/ingestion/collectors/openmhz/collector.py` | 119 |
| OpenMHZ sequential download + conversion | `backend/pipeline/ingestion/collectors/openmhz/collector.py` | 139–148 |
| OpenMHZ per-connection backoff (no cross jitter) | `backend/pipeline/ingestion/collectors/openmhz/collector.py` | 197–200 |
| bcfy_calls poll interval = 10s | `backend/pipeline/ingestion/collectors/bcfy_calls/bcfy_calls_collector.py` | 26 |
| Heartbeat interval 15s default | `backend/pipeline/ingestion/settings.py` | 55 |
| Max feeds per VM worker = 250 | `backend/pipeline/ingestion/settings.py` | 45 |
| Bookmark write on every chunk | `backend/pipeline/ingestion/normalizer_runtime.py` | 386 |
| GCS upload per chunk (no batching) | `backend/pipeline/ingestion/normalizer_runtime.py` | 352 |
| Pub/Sub publish per chunk | `backend/pipeline/ingestion/normalizer_runtime.py` | 371 |
| ffmpeg segment time = 15s | `backend/pipeline/ingestion/collectors/icecast_collector.py` | 307 |
| Chunk duration constant | `backend/pipeline/common/constants.py` | 4 |

### Broadcastify Calls catalog endpoints used

| Purpose | Method + Path | Rate-limited to |
|---|---|---|
| State list | `GET /common/v1/states/1` | 2 QPS (catalog build only) |
| Counties per state | `GET /common/v1/counties/{stid}` | 2 QPS |
| Service tag definitions (1=Multi-Dispatch, 3=Fire Dispatch, 8=Fire-Tac, …) | `GET /common/v1/tags` | 2 QPS |
| Public playlists (human-curated) | `GET /calls/v1/playlists_public` | 2 QPS |
| Playlist detail with group list | `GET /calls/v1/playlist_get/{uuid}` | 2 QPS |
| **Groups captured in county** | `POST /calls/v1/groups_ctid/{ctid}` | 2 QPS |
| Group detail (one group) | `GET /calls/v1/group_get/{groupId}` | 2 QPS (not used in bulk catalog build) |

## Appendix B — Running the catalog

```bash
cd radio-transcription/model/data/wildfire_catalog
# Secrets from GCP Secret Manager
export BCFY_API_KEY=$(gcloud secrets versions access latest --secret=broadcastify-api-key-prod)
export BROADCASTIFY_API_KEY_ID=$(gcloud secrets versions access latest --secret=broadcastify-api-key-id-prod)
export BROADCASTIFY_APP_ID=$(gcloud secrets versions access latest --secret=broadcastify-api-app-id-prod)

./.venv/bin/python run_catalog.py --output-dir ./output
```

Full run: ~35 minutes uncached (dominated by the 3,275-county bcfy_calls crawl at 2 QPS), seconds with warm cache.
