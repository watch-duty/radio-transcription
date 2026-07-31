# Apache Beam Segmentation & Stitching Pipeline

## Component Overview
The `segmentation` component is a stateful streaming Apache Beam application running on Google Cloud Dataflow. It acts as the critical processing bridge between raw audio ingestion and downstream normalization/transcription by:
1. **Restoring Upstream Order:** Unmarshaling incoming Pub/Sub elements and buffering them in a stateful Jitter Buffer (`SequenceBuffer`) to correct out-of-order delivery.
2. **Evaluating Voice Activity:** Running high-performance Silero Voice Activity Detection (VAD) models across downloaded audio chunks.
3. **Stitching Sequences:** Aggregating adjacent speech or silence slices into unified, canonical transmissions.

---

## Continuous Audio Retention Policy (CRITICAL MANDATE)

For continuous audio sources (specifically `bcfy_feeds` and native `icecast` streams captured via [`icecast_collector.py`](../ingestion/collectors/icecast/icecast_collector.py)), our foundational operational requirement is to **retain 100% of all incoming audio**.

> [!NOTE]
> **Dataflow Pipeline Input Scope**: Only continuous streaming feeds (`bcfy_feeds` and `icecast`) pass through this Dataflow segmentation pipeline. Discrete call feeds (`bcfy_calls`, `openmhz`) and notifications do NOT pass through Dataflow segmentation. 

Every single sample captured by an Ingestion continuous scraper must be preserved in canonical GCS storage and committed to the database under one of two canonical classifications:
- **`SPEECH`**: Audio intervals containing active voice activity.
- **`OTHER`**: Audio intervals representing non-speech (ie, noise, tones, etc) or silence.

### Prohibition of Intra-Chunk Discards
When an Ingestion scraper publishes fixed-length audio files (e.g., 15-second continuous FLAC chunks), the underlying state machine (**`AudioStitchingStateMachine`**) must exhaustively process all audio samples within the file. 

Discarding non-speech audio samples that fall between speech utterances within a chunk or dropping silent intervals between consecutive speech windows represents unacceptable data loss and directly violates our continuous audio retention policy.

---

## Core Abstractions & FSM Architecture

To maximize testability and guarantee 100% serialization (pickling) safety across distributed Dataflow workers, our core domain logic is strictly decoupled from Apache Beam DoFn execution boundaries:

### 1. `OrderedContinuousStitchAudioFn` (`transforms/stateful.py`)
The Apache Beam stateful Windmill DoFn. Manages persistent state cells, watermark timers (`out_of_order_timer`, `stale_timer`), and bundle lease limits, delegating all domain evaluation to the stateless engine.

### 2. `StitcherEngine` (`transforms/stitcher_engine.py`)
The intermediate stateless domain execution orchestrator. Coordinates downloading raw GCS staging audio files, lazily initializing ONNXRuntime Silero VAD inference sessions, and emitting structured `FlushRequest` payloads.

### 3. `AudioStitchingStateMachine` (`state/stitcher_state.py`)
The framework-agnostic finite state machine (FSM). Emits imperative state transition actions (`AppendBufferAction`, `FlushAction`, `DropAction`) based on active VAD evaluations and sequence context tracking.

---

## 🧪 Testing Strategy
Because the core FSM (`AudioStitchingStateMachine`) is entirely framework-agnostic, you do not need to execute resource-heavy Apache Beam test pipelines or Dataflow Local Runners to validate sequence stitching behavior.

Execute targeted, local unit validation via:
```bash
uv run pytest backend/pipeline/segmentation/tests/
```

---

## 🔍 VAD Diagnostic & Speech Drop Tooling

The segmentation pipeline includes a dedicated diagnostic CLI tool ([`diagnose_feed_drop.py`](scripts/diagnose_feed_drop.py)) for diagnosing dropped speech or timeframe discrepancies on live continuous feeds.

### Usage
Run the diagnostic script using `uv run`:

```bash
# Audit by pasting Segment ID copied from UI (Info ⓘ popover) with --feed-id:
uv run python3 -m backend.pipeline.segmentation.scripts.diagnose_feed_drop \
  --env prod \
  --feed-id "2d948330-02b3-4bed-a133-0977b167d2b1" \
  --segment-id "3453201f-d417-5e29-3b87-187796e4e03c"

# Audit by pasting full transcript URL (Share -> Copy Link):
uv run python3 -m backend.pipeline.segmentation.scripts.diagnose_feed_drop \
  --env prod \
  --url "https://radio.watchduty.org/transcripts?feedId=2d948330-02b3-4bed-a133-0977b167d2b1&segmentId=3453201f-d417-5e29-3b87-187796e4e03c"

# Timeframe interval audit by Segment ID range:
uv run python3 -m backend.pipeline.segmentation.scripts.diagnose_feed_drop \
  --env prod \
  --feed-id "2d948330-02b3-4bed-a133-0977b167d2b1" \
  --start-segment-id <START_SEGMENT_ID> \
  --end-segment-id <END_SEGMENT_ID> \
  --backup-sec 45.0
```

### Key Capabilities
- **Tape Backup Warmup**: Pre-rolls preceding feed audio (default: 45s) to warm up UL-UNAS neural denoiser and Silero VAD recurrent state memory.
- **Granular Metric Auditing**: Tracks dynamic spikiness click rejections, sub-audible RMS floors, and tone interference rejections.
- **Surgical Timeframe Reconciliation**: Emits a structured `TIMEFRAME RECONCILIATION AUDIT` identifying dropped sub-intervals and providing parameter tuning recommendations (e.g., `VAD_DEFAULT_THRESHOLD_ONSET` or software volume normalization targets).
