# Apache Beam Segmentation & Stitching Pipeline

## Component Overview
The `segmentation` component is a stateful streaming Apache Beam application running on Google Cloud Dataflow. It acts as the critical processing bridge between raw audio ingestion and downstream normalization/transcription by:
1. **Restoring Upstream Order:** Unmarshaling incoming Pub/Sub elements and buffering them in a stateful Jitter Buffer (`SequenceBuffer`) to correct out-of-order delivery.
2. **Evaluating Voice Activity:** Running high-performance Silero Voice Activity Detection (VAD) models across downloaded audio chunks.
3. **Stitching Sequences:** Aggregating adjacent speech or silence slices into unified, canonical transmissions.

---

## Continuous Audio Retention Policy (CRITICAL MANDATE)

For continuous audio sources (specifically `bcfy_feeds`), our foundational operational requirement is to **retain 100% of all incoming audio**. 

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
