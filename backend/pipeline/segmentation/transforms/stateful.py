"""Stateful Apache Beam transforms for the radio transcription pipeline.

This module defines the core stateful and chronological restoration boundary
DoFns in our Apache Beam DAG. It houses transforms responsible for:
1. unmarshaling incoming Pub/Sub JSON elements.
2. restoration of out-of-order segment arrivals using SequenceBuffer.
3. stateful timer management for processing window timeout flushes.
4. execution of speech-to-text transcription API requests.

All high-level audio download/concatenation/VAD heuristics are cleanly
decoupled from Beam timer variables and delegated to StitcherEngine.


## Decoupled Stateful/Stateless Hybrid Architecture (Stage 2 & Stage 3)

To prevent Dataflow Windmill state locking and GIL contention bottlenecks, the audio
segmentation pipeline uses a decoupled, hybrid metadata/physical-retrieval flow:
1. **Stage 2 (Stateful - OrderedStitchAudioFn)**: Performs chronological sequencing,
   session FSM tracking, and VAD segment calculations. To keep persistent state sizes
   extremely small (<1 KB) and lock times under microseconds, **no raw audio bytes are stored
   in stateful cell persistent bag states**.
2. **Stage 3 (Stateless - UploadRawSegmentFn)**: Performs the heavy physical work of
   downloading contributing audio chunks from GCS, slicing them according to Stage 2's
   VAD segments, stitching them, and compressing the result to FLAC. Since this stage is
   completely stateless, Dataflow can distribute and execute these tasks in parallel across
   unlimited worker threads.


## Dataflow Windmill Execution Model

**This section is critical reading for anyone familiar with Beam on other runners
(Flink, Spark, Direct) but new to GCP Dataflow Streaming.**

### What is Windmill?

Windmill is Dataflow's internal streaming execution engine — the component that
manages work distribution, state persistence, and exactly-once semantics. It is
entirely invisible at the Beam API level: your DoFn code looks identical whether
it runs on Dataflow, Flink, or the Direct runner. However, Windmill has execution
constraints that don't exist on other runners and that materially affect how
stateful DoFns should be written.

### Bundle Leases

Windmill executes DoFn work in *bundles*. Each bundle is a unit of work assigned
to a worker thread with a **hard 300-second wall-clock lease**. If a bundle does
not complete and commit its outputs within that window, Windmill considers it
failed, rolls back all state changes from that bundle, and re-enqueues the work
for retry on another worker.

On other runners, bundles are either unbounded or their timeout behavior is
configurable and generally much more forgiving. On Dataflow Streaming, the 300s
limit is non-negotiable.

### Implications for Stateful DoFns

Stateful DoFns (those using `@StateSpec` and `@TimerSpec`) are particularly
affected because all elements sharing the same key are processed sequentially
within a single bundle. This means:

- If a feed's `out_of_order_buffer` contains N chunks and draining it takes
  longer than 300 seconds, the entire bundle is aborted and retried.
- Large backlogs can accumulate after pipeline restarts, upstream traffic spikes,
  or processing slowdowns (e.g., lock contention in worker threads). When the
  pipeline tries to drain a large backlog in a single bundle, it risks breaching
  the lease.
- Unlike element-level failures (which surface as exceptions and can be sent to
  a DLQ), a lease timeout produces no output at all — the work simply replays.

### The Self-Chaining Timer Pattern

To work around the lease limit without discarding data, we use a *self-chaining
timer* pattern:

1. `drain_ready_elements` is called with `max_emit=MAX_CHUNKS_PER_WINDMILL_BUNDLE`
   to cap the number of chunks processed in a single bundle.
2. If the buffer still has remaining chunks after the cap is hit (`clamped=True`),
   the DoFn immediately sets `gap_timer_event` to the current watermark
   timestamp (`gap_timer_event.set(timestamp)`). If running in live
   streaming, it also schedules `gap_timer_proc` in processing time.
3. Because the watermark timer fires at or before the current watermark, Windmill schedules
   a new bundle immediately, which calls the gap timeout handler and drains the next
   slice of up to `MAX_CHUNKS_PER_WINDMILL_BUNDLE` chunks.
4. This continues until the buffer is empty, at which point both timers are cleared and
   normal gap-timeout behavior resumes.

The constant `MAX_CHUNKS_PER_WINDMILL_BUNDLE` (defined in `constants.py`) is
sized so that `N_chunks x per_chunk_latency` stays well under 300 seconds. If
per-chunk processing or external I/O latency increases significantly in the
future, this value should be reduced accordingly.
"""

import logging as std_logging
import time
from collections.abc import Iterable, Iterator
from dataclasses import replace
from typing import Any, override

import apache_beam as beam
from apache_beam.metrics import Metrics
from apache_beam.transforms.userstate import (
    BagRuntimeState,
    BagStateSpec,
    ReadModifyWriteRuntimeState,
    ReadModifyWriteStateSpec,
    RuntimeTimer,
    TimerSpec,
    on_timer,
)
from apache_beam.utils.shared import Shared
from apache_beam.utils.timestamp import Timestamp

from backend.pipeline.common import constants as common_constants
from backend.pipeline.common import tracing_utils
from backend.pipeline.common.log_helper import get_logger, get_task_logger
from backend.pipeline.segmentation import coders as trans_coders
from backend.pipeline.segmentation import constants as trans_constants
from backend.pipeline.segmentation import datatypes, log_helper
from backend.pipeline.segmentation.audio.processor import get_vad_engine
from backend.pipeline.segmentation.constants import (
    MAX_CHUNKS_PER_WINDMILL_BUNDLE,
    WINDMILL_TIMER_MIN_ADVANCE_SECS,
)
from backend.pipeline.segmentation.state import sequence_buffer
from backend.pipeline.segmentation.storage import (
    acquire_shared_download_executor,
    acquire_shared_gcs_client,
)
from backend.pipeline.segmentation.transforms import stitcher_engine

SHARED_VAD_HANDLE = Shared()

# Jitter-buffer depth, recorded wherever the out-of-order BagState is
# materialized: the main ordering path, the gap-timeout handler, the
# deferred-drain handler, and the budget-exhausted path. Declared at module
# scope (string namespace) because process_ordering() is a free function, not
# an OrderedStitchAudioFn method -- a per-instance Metrics.distribution
# wouldn't be reachable from there, and splitting the same logical metric
# across two namespaces would defeat the point of watching it as one series.
JITTER_BUFFER_DEPTH = Metrics.distribution(
    "OrderedStitchAudioFn", "jitter_buffer_depth"
)

# WARNING: Do NOT remove or bypass setup_logging().
# It explicitly configures structured log propagation for the
# Dataflow worker harness. Removing this will cause all worker logs
# to be rendered as DEBUG severity in Cloud Logging.
log_helper.setup_logging()

logger = get_task_logger(
    __name__, {"system": "transcription", "component": "ordered-stitcher"}
)


def _get_task_logger(
    feed_id: str, session_id: str | None, component: str
) -> std_logging.LoggerAdapter:
    """Creates a contextual LoggerAdapter for tracing items through the pipeline."""
    return std_logging.LoggerAdapter(
        get_logger(__name__),
        {
            "system": "transcription",
            "component": component,
            "feed_id": feed_id,
            "session_id": session_id or "unknown",
        },
    )


def _write_transmission_context(
    state_cell: Any,
    context: datatypes.TransmissionContext,
    last_start_ms_state: Any = None,
    out_of_order_buffer_state: Any = None,
) -> None:
    """Writes the context to transmission state or clears it completely if it is an empty IdleFeedState.

    Pruning completely empty IdleFeedStates from the database prevents session memory leaks and
    ensures that subsequent independent audio chunks start with a clean slate (initiating a new
    ActiveStitchingState and a fresh traceparent, effectively preventing Trace Context Hijacking).
    """
    if (
        isinstance(context, datatypes.IdleFeedState)
        and not context.order_timer_active
    ):
        state_cell.clear()
        if last_start_ms_state:
            last_start_ms_state.clear()
        if out_of_order_buffer_state:
            out_of_order_buffer_state.clear()
    else:
        state_cell.write(context)


def _migrate_legacy_buffer[
    T: (datatypes.IdleFeedState, datatypes.ActiveStitchingState)
](
    curr_context: T,
    out_of_order_buffer_state: Any,
) -> tuple[T, bool]:
    """Migrates legacy out_of_order_buffer from TransmissionContext to BagState if present."""
    if curr_context.out_of_order_buffer:
        for chunk in curr_context.out_of_order_buffer:
            out_of_order_buffer_state.add(chunk)
        return (
            replace(
                curr_context, out_of_order_buffer=[], order_timer_active=True
            ),
            True,
        )
    return curr_context, False


class StaleTimerManager:
    """Helper class to manage both event-time and processing-time stale timers."""

    def __init__(
        self,
        event_timer: RuntimeTimer,
        proc_timer: RuntimeTimer,
        config: datatypes.StitchAudioConfig,
    ) -> None:
        self.event_timer = event_timer
        self.proc_timer = proc_timer
        self.config = config

    def schedule(self, deadline_ms: int, *, is_backfill: bool) -> None:
        """Schedules either or both timers based on the backfill mode."""
        if is_backfill:
            # In backfill mode, use ONLY Event Time (Watermark) timer.
            self.proc_timer.clear()
            if deadline_ms > 0:
                deadline_s = deadline_ms / common_constants.MS_PER_SECOND
                self.event_timer.set(Timestamp(seconds=deadline_s))
            else:
                self.event_timer.clear()
        elif deadline_ms > 0:
            # Set event time timer based on data timeline
            deadline_s = deadline_ms / common_constants.MS_PER_SECOND
            self.event_timer.set(Timestamp(seconds=deadline_s))

            # Set processing time timer based on wall-clock time
            deadline_proc_s = (
                time.time()
                + self.config.stale_timeout_ms
                / float(common_constants.MS_PER_SECOND)
            )
            self.proc_timer.set(Timestamp(seconds=deadline_proc_s))
        else:
            self.event_timer.clear()
            self.proc_timer.clear()

    def clear(self) -> None:
        """Clears both timers."""
        self.event_timer.clear()
        self.proc_timer.clear()


def _handle_session_transition(
    curr_context: datatypes.TransmissionContext,
    metadata: datatypes.ChunkMetadata,
    gap_timer_event: RuntimeTimer,
    gap_timer_proc: RuntimeTimer,
    task_logger: Any,
) -> tuple[datatypes.ActiveStitchingState, bool]:
    """Handles transitions between active sessions or initialization from IdleFeedState."""
    if isinstance(curr_context, datatypes.IdleFeedState):
        new_context = datatypes.ActiveStitchingState(
            session_id=metadata.session_id,
            feed_metadata=metadata.feed_metadata,
            out_of_order_buffer=curr_context.out_of_order_buffer,
            order_timer_active=curr_context.order_timer_active,
            traceparent=metadata.traceparent
            or tracing_utils.get_current_traceparent(),
            baggage=metadata.baggage,
        )
        gap_timer_event.clear()
        gap_timer_proc.clear()
        return new_context, True

    if curr_context.session_id != metadata.session_id:
        task_logger.info(
            f"Session ID changed from {curr_context.session_id} to {metadata.session_id}. Resetting state."
        )
        gap_timer_event.clear()
        gap_timer_proc.clear()
        new_context = datatypes.ActiveStitchingState(
            session_id=metadata.session_id,
            feed_metadata=metadata.feed_metadata,
            traceparent=metadata.traceparent
            or tracing_utils.get_current_traceparent(),
            baggage=metadata.baggage,
            missing_prior_context=True,
        )
        return new_context, True

    if not isinstance(curr_context, datatypes.ActiveStitchingState):
        msg = "Invalid context type"
        raise TypeError(msg)
    return curr_context, False


def _manage_out_of_order_timers(
    gap_timer_event: RuntimeTimer,
    gap_timer_proc: RuntimeTimer,
    order_config: datatypes.OrderRestorerConfig,
    *,
    timestamp: Timestamp,
    clamped: bool,
    has_buffer_elements: bool,
    order_timer_active: bool,
    is_backfill: bool,
    old_expected_ts: int | None,
    new_expected_next_ts: int | None,
) -> bool:
    """Manages scheduling and clearing of out-of-order restoration timers.

    Timer management after a clamped or normal drain — three cases:

    1. CLAMPED (buffer still has chunks AND drain hit MAX_CHUNKS_PER_WINDMILL_BUNDLE):
       The previous drain stopped early to stay under Windmill's 300-second bundle
       lease. Setting the timer to `timestamp` (current watermark) causes Dataflow
       to fire handle_gap_timeout in the very next bundle, draining the next slice.
       Bundles chain this way until the buffer is empty. This prevents
       emitting oversized bundles that could exceed Windmill's bundle lease
       and get aborted/replayed (e.g., triggered by pipeline restarts,
       lock-induced slowdowns, or traffic spikes) by processing the queue in
       bounded bites and chaining to the next bundle.

    2. NORMAL GAP (buffer has chunks, drain was not clamped): schedule the
       standard gap-timeout deadline so we eventually flush even if some
       predecessors never arrive.

    3. DRAINED (buffer is now empty): clear the timer to avoid spurious firing.

    Returns the new state of order_timer_active.
    """
    if has_buffer_elements:
        if clamped:
            # Loop control: Always advance by the minimum 1ms safety epsilon
            # to satisfy the Runner V2 gate without triggering artificial
            # watermark delays or Pub/Sub source gridlocks.
            gap_timer_event.set(timestamp + WINDMILL_TIMER_MIN_ADVANCE_SECS)
            if is_backfill:
                gap_timer_proc.clear()
            else:
                gap_timer_proc.set(
                    Timestamp(
                        seconds=time.time() + WINDMILL_TIMER_MIN_ADVANCE_SECS
                    )
                )
            return True

        if not order_timer_active:
            deadline_watermark = timestamp + (
                order_config.out_of_order_timeout_ms
                / float(common_constants.MS_PER_SECOND)
            )
            gap_timer_event.set(deadline_watermark)
            if is_backfill:
                gap_timer_proc.clear()
            else:
                deadline_proc = time.time() + (
                    order_config.out_of_order_timeout_ms
                    / float(common_constants.MS_PER_SECOND)
                )
                gap_timer_proc.set(Timestamp(seconds=deadline_proc))
            return True

        return order_timer_active

    if order_timer_active:
        gap_timer_event.clear()
        gap_timer_proc.clear()
        return False

    return order_timer_active


def process_ordering(  # noqa: PLR0912, PLR0915
    element: tuple[str, datatypes.ChunkMetadata],
    timestamp: Timestamp,
    curr_context: datatypes.ActiveStitchingState,
    out_of_order_buffer_state: Any,
    gap_timer_event: Any,
    gap_timer_proc: Any,
    order_config: datatypes.OrderRestorerConfig,
    *,
    is_backfill: bool,
    session_changed: bool = False,
    max_emit: int = MAX_CHUNKS_PER_WINDMILL_BUNDLE,
    deadline_monotonic: float | None = None,
) -> tuple[list[datatypes.BufferedChunk], datatypes.ActiveStitchingState, bool]:
    """Handles chronological ordering via SequenceBuffer."""
    key_str, metadata = element
    if "#" in key_str:
        feed_id, _ = key_str.split("#", 1)
    else:
        feed_id = key_str
    task_logger = _get_task_logger(
        feed_id, metadata.session_id, "sequence-buffer"
    )

    seq_buf = sequence_buffer.SequenceBuffer(order_config)

    current_ts_ms = (
        metadata.timestamp_ms
        if metadata.timestamp_ms is not None
        else int(float(timestamp) * common_constants.MS_PER_SECOND)
    )

    expected_next_ts = curr_context.expected_next_chunk_start_ms
    if expected_next_ts is None:
        expected_next_ts = current_ts_ms

    epsilon_ms = trans_constants.DEFAULT_FLOAT_TOLERANCE_MS
    difference = current_ts_ms - expected_next_ts

    to_emit = []
    was_late = False
    was_buffered = False

    if abs(difference) <= epsilon_ms:
        # HAPPY PATH: The chunk matches our mathematical expectation exactly.
        to_emit.append(
            datatypes.BufferedChunk(
                timestamp_ms=current_ts_ms,
                gcs_uri=metadata.gcs_uri,
                traceparent=metadata.traceparent,
                baggage=metadata.baggage,
            )
        )
        duration = (
            metadata.duration_ms
            if metadata.duration_ms is not None
            else order_config.chunk_duration_ms
        )
        expected_next_ts = current_ts_ms + duration

        # Drain from bag
        buffer_elements = []
        if curr_context.order_timer_active:
            buffer_elements = list(out_of_order_buffer_state.read())

        if buffer_elements:
            expected_next_ts, remaining_elements, drained = (
                seq_buf.drain_ready_elements(
                    expected_next_ts=expected_next_ts,
                    buffer_elements=buffer_elements,
                    epsilon_ms=epsilon_ms,
                    max_emit=max_emit - 1,  # -1 for the current chunk
                    deadline_monotonic=deadline_monotonic,
                )
            )
            to_emit.extend(drained)

            # Update BagState
            out_of_order_buffer_state.clear()
            for c in remaining_elements:
                out_of_order_buffer_state.add(c)
            JITTER_BUFFER_DEPTH.update(len(remaining_elements))
            has_buffer_elements = len(remaining_elements) > 0
            clamped = bool(
                len(drained) >= (max_emit - 1)
                or (
                    deadline_monotonic is not None
                    and time.monotonic() >= deadline_monotonic
                    and remaining_elements
                )
            )
        else:
            has_buffer_elements = False
            clamped = False

    elif difference < -epsilon_ms:
        # LATE PATH
        was_late = True
        logger.info(
            f"Yielding late chunk at {current_ts_ms} (expected {expected_next_ts}) for isolated transcription."
        )
        to_emit.append(
            datatypes.BufferedChunk(
                timestamp_ms=current_ts_ms,
                gcs_uri=metadata.gcs_uri,
                traceparent=metadata.traceparent,
                baggage=metadata.baggage,
            )
        )
        has_buffer_elements = (
            curr_context.order_timer_active
        )  # Keep whatever it was
        clamped = False
    else:
        # FUTURE PATH
        was_buffered = True
        out_of_order_buffer_state.add(
            datatypes.BufferedChunk(
                timestamp_ms=current_ts_ms,
                gcs_uri=metadata.gcs_uri,
                traceparent=metadata.traceparent,
                baggage=metadata.baggage,
            )
        )
        has_buffer_elements = True
        clamped = False

    if was_late:
        task_logger.debug(f"[Order] Late chunk: {metadata.gcs_uri}")
    if was_buffered:
        task_logger.debug(
            f"[Order] Buffered chunk from future: {metadata.gcs_uri}"
        )
    if to_emit:
        task_logger.debug(f"[Order] Releasing {len(to_emit)} chunks")

    # Update context
    old_expected_ts = curr_context.expected_next_chunk_start_ms

    new_timer_active = _manage_out_of_order_timers(
        gap_timer_event=gap_timer_event,
        gap_timer_proc=gap_timer_proc,
        order_config=order_config,
        timestamp=timestamp,
        clamped=clamped,
        has_buffer_elements=has_buffer_elements,
        order_timer_active=curr_context.order_timer_active,
        is_backfill=is_backfill,
        old_expected_ts=old_expected_ts,
        new_expected_next_ts=expected_next_ts,
    )

    curr_context = replace(
        curr_context,
        expected_next_chunk_start_ms=expected_next_ts,
        order_timer_active=new_timer_active,
    )

    return to_emit, curr_context, session_changed


def _evaluate_is_backfill(current_ts_ms: int, threshold_ms: int) -> bool:
    """Determines if a chunk is being processed in backfill/catch-up mode.

    In this context, "backfill" refers to catch-up processing when the pipeline
    comes back online after an outage, maintenance, or deployment. If a chunk's
    event time lags significantly behind current wall-clock processing time, it is
    flagged as backfill so downstream logic skips overlap validation and state updates.
    This prevents older catch-up timestamps from corrupting mainline sequence tracking.

    Args:
        current_ts_ms: The event timestamp of the incoming audio chunk in milliseconds.
        threshold_ms: Lateness threshold (in ms) beyond which a chunk is considered backfill.

    Returns:
        True if the chunk lateness meets or exceeds the backfill threshold.
    """
    lateness = time.time() * common_constants.MS_PER_SECOND - current_ts_ms
    return lateness >= threshold_ms


def _reschedule_gap_timeout(
    gap_timer_event: RuntimeTimer,
    gap_timer_proc: RuntimeTimer,
    order_config: datatypes.OrderRestorerConfig,
    *,
    timestamp: Timestamp,
    clamped: bool,
    is_backfill: bool,
    new_expected: int | None,
    new_expected_next_ts: int | None,
) -> bool:
    """Reschedules out-of-order restoration timers after a timeout drain."""
    if clamped:
        emitted_duration_ms = (
            (new_expected_next_ts - new_expected)
            if (new_expected is not None and new_expected_next_ts is not None)
            else 0
        )
        advance_sec = max(
            WINDMILL_TIMER_MIN_ADVANCE_SECS,
            float(emitted_duration_ms) / float(common_constants.MS_PER_SECOND),
        )
        deadline_watermark = timestamp + advance_sec
        deadline_proc = time.time() + advance_sec
    else:
        timeout_sec = order_config.out_of_order_timeout_ms / float(
            common_constants.MS_PER_SECOND
        )
        deadline_watermark = timestamp + timeout_sec
        deadline_proc = time.time() + timeout_sec

    gap_timer_event.set(deadline_watermark)
    if is_backfill:
        gap_timer_proc.clear()
    else:
        gap_timer_proc.set(Timestamp(seconds=deadline_proc))
    return True


@beam.typehints.with_input_types(tuple[str, datatypes.ChunkMetadata])
@beam.typehints.with_output_types(tuple[str, datatypes.FlushRequest])
class OrderedStitchAudioFn(beam.DoFn):
    """Stateful Apache Beam DoFn orchestrating out-of-order and stale windowing for continuous audio feeds.

    Enterprise Architectural Rationale: Why implement an explicit Jitter Buffer in Beam User State (`BagState`)
    rather than simply enabling native GCP Pub/Sub Subscription Ordering Keys?
    1. Total Autoscaler Head-of-Line Starvation: Pub/Sub subscription ordering strictly blocks delivering message #2
       until message #1 is fully computed and its official distributed network Acknowledgement (`Ack()`) is returned.
       In Beam, workers pull tens of thousands of messages in highly parallel, un-ordered work-stealing bundles.
       Enabling Pub/Sub ordering completely Head-of-Line starves the auto-scaled fleet (e.g., 99 worker pods sit
       100% completely idle waiting for a single pod to compute, complete Beam DAG traversal, and network `Ack()` chunk #1).
       Decoupling ordering into Beam State empowers our 100 worker machines to asynchronously ingest millions of chunks at maximum velocity.
    2. Exactly-Once ML FSM Protection: Pub/Sub fundamentally only guarantees `At-Least-Once Delivery`. Any transient
       network blip or VM preemption forces Pub/Sub to actively re-deliver un-acked duplicates. Our isolated Beam
       SequenceBuffer beautifully filters duplicate frames, entirely preventing false positive VAD speech boundaries.
    3. Bounded Self-Chaining Drains: When unrolling out-of-order backlogs, emissions are
       clamped to `MAX_CHUNKS_PER_WINDMILL_BUNDLE` and a watermark timer is
       re-armed to open fresh bundles, preventing 300-second bundle lease evictions
       while reducing intermediate timer queuing delays during catch-up.
    """

    processed_in_bundle: int

    # --- State Specs ---

    TRANSMISSION_CONTEXT_SPEC = ReadModifyWriteStateSpec(
        "transmission_context", trans_coders.TransmissionContextCoder()
    )
    TRANSMISSION_CONTEXT_STATE = beam.DoFn.StateParam(TRANSMISSION_CONTEXT_SPEC)

    LAST_START_SPEC = ReadModifyWriteStateSpec(
        "last_start_ms", beam.coders.VarIntCoder()
    )
    LAST_START_MS_STATE = beam.DoFn.StateParam(LAST_START_SPEC)

    OUT_OF_ORDER_BUFFER_SPEC = BagStateSpec(
        "out_of_order_buffer_bag", trans_coders.BufferedChunkCoder()
    )
    OUT_OF_ORDER_BUFFER_STATE = beam.DoFn.StateParam(OUT_OF_ORDER_BUFFER_SPEC)

    # --- Timers ---

    # Watermark timer for out-of-order restoration / gap timeouts.
    GAP_TIMER_EVENT_SPEC = TimerSpec(
        "gap_timer_event", beam.TimeDomain.WATERMARK
    )
    GAP_TIMER_EVENT = beam.DoFn.TimerParam(GAP_TIMER_EVENT_SPEC)

    GAP_TIMER_PROC_SPEC = TimerSpec("gap_timer_proc", beam.TimeDomain.REAL_TIME)
    GAP_TIMER_PROC = beam.DoFn.TimerParam(GAP_TIMER_PROC_SPEC)

    STALE_TIMER_EVENT_SPEC = TimerSpec(
        "stale_timer_event", beam.TimeDomain.WATERMARK
    )
    STALE_TIMER_EVENT_PARAM = beam.DoFn.TimerParam(STALE_TIMER_EVENT_SPEC)

    STALE_TIMER_PROC_SPEC = TimerSpec(
        "stale_timer_proc", beam.TimeDomain.REAL_TIME
    )
    STALE_TIMER_PROC_PARAM = beam.DoFn.TimerParam(STALE_TIMER_PROC_SPEC)

    DEFERRED_DRAIN_TIMER_SPEC = TimerSpec(
        "deferred_drain_timer", beam.TimeDomain.WATERMARK
    )
    DEFERRED_DRAIN_TIMER = beam.DoFn.TimerParam(DEFERRED_DRAIN_TIMER_SPEC)

    def __init__(
        self,
        order_config: datatypes.OrderRestorerConfig,
        stitch_config: datatypes.StitchAudioConfig,
    ) -> None:
        self.order_config = order_config
        self.stitch_config = stitch_config
        self.processed_in_bundle = 0
        self.bundle_start_time_monotonic: float = 0.0
        self.bundle_clamped_item_limit = Metrics.counter(
            self.__class__, "bundle_clamped_item_limit"
        )
        self.bundle_clamped_duration_limit = Metrics.counter(
            self.__class__, "bundle_clamped_duration_limit"
        )
        self.deferred_drain_invocations = Metrics.counter(
            self.__class__, "deferred_drain_invocations"
        )
        self.deferred_drain_chunks_emitted = Metrics.distribution(
            self.__class__, "deferred_drain_chunks_emitted"
        )
        self.deferred_drain_empty_while_buffered = Metrics.counter(
            self.__class__, "deferred_drain_empty_while_buffered"
        )

    @property
    def engine(self) -> Any:
        if not hasattr(self, "_engine_lazy"):
            self._engine_lazy = stitcher_engine.StitcherEngine(
                stitch_config=self.stitch_config,
                order_config=self.order_config,
                vad_config=self.stitch_config.vad_config,
            )
        return self._engine_lazy

    @engine.setter
    def engine(self, val: Any) -> None:
        self._engine_lazy = val

    def __getstate__(self) -> dict[str, Any]:
        state = self.__dict__.copy()
        if "_engine_lazy" in state:
            del state["_engine_lazy"]
        return state

    @override
    def setup(self) -> None:
        tracing_utils.setup_tracing(service_name="segmentation-pipeline")
        # Acquire process-level singletons natively via dedicated Beam Shared handles
        vad_config = self.stitch_config.vad_config
        shared_vad = SHARED_VAD_HANDLE.acquire(
            lambda: get_vad_engine(vad_config),
            tag=f"vad_{vad_config or 'default'}",
        )

        shared_gcs = acquire_shared_gcs_client(
            project_id=self.stitch_config.project_id,
        )
        self.engine.processor.vad = shared_vad
        self.engine.processor.gcs_client = shared_gcs

        self._executor = acquire_shared_download_executor()
        self.engine.executor = self._executor
        self.engine.setup()

    def start_bundle(self) -> None:
        """Initializes the bundle-level counters and monotonic start timestamp to enforce lease limits."""
        self.processed_in_bundle = 0
        self.bundle_start_time_monotonic = time.monotonic()

    def _get_bundle_start_time(self) -> float:
        """Returns the monotonic start timestamp for the current bundle execution."""
        if not self.bundle_start_time_monotonic:
            self.bundle_start_time_monotonic = time.monotonic()
        return self.bundle_start_time_monotonic

    def _get_bundle_deadline_monotonic(self) -> float:
        """Returns the monotonic timestamp at which the current worker bundle budget expires."""
        return (
            self._get_bundle_start_time()
            + trans_constants.MAX_WINDMILL_BUNDLE_DURATION_SEC
        )

    def _yield_tagged_outputs(
        self,
        results: Iterable[
            tuple[str, datatypes.FlushRequest]
            | tuple[str, datatypes.StitcherDlqPayload]
            | beam.pvalue.TaggedOutput
        ],
    ) -> Iterator[
        tuple[str, datatypes.FlushRequest] | beam.pvalue.TaggedOutput
    ]:
        """Yields results, tagging DLQ outputs appropriately."""
        for res in results:
            if (
                isinstance(res, tuple)
                and res[0] == trans_constants.DEAD_LETTER_QUEUE_TAG
            ):
                yield beam.pvalue.TaggedOutput(
                    trans_constants.DEAD_LETTER_QUEUE_TAG, res[1]
                )
            else:
                yield res

    def _is_bundle_budget_exhausted(self) -> bool:
        """Check if the current worker bundle has exhausted its time or prefetch budget."""
        elapsed_sec = time.monotonic() - self._get_bundle_start_time()
        return (
            elapsed_sec >= trans_constants.MAX_WINDMILL_BUNDLE_DURATION_SEC
            or self.processed_in_bundle
            >= trans_constants.MAX_CHUNKS_PER_WINDMILL_BUNDLE
        )

    def _reschedule_after_deferred_drain(
        self,
        *,
        elements_to_emit: list[datatypes.BufferedChunk],
        new_buffer_elements: list[datatypes.BufferedChunk],
        initial_expected_ts: int | None,
        new_expected_next_ts: int | None,
        timestamp: Timestamp,
        deferred_drain_timer: RuntimeTimer,
        gap_timer_event: RuntimeTimer,
        gap_timer_proc: RuntimeTimer,
        task_logger: std_logging.LoggerAdapter,
    ) -> None:
        """Re-arms the deferred-drain timer or the gap timeout after a drain, depending on whether the drain was clamped."""
        clamped_by_items = len(elements_to_emit) >= (
            trans_constants.MAX_CHUNKS_PER_WINDMILL_BUNDLE
            - self.processed_in_bundle
        )
        clamped_by_time = (
            time.monotonic() >= self._get_bundle_deadline_monotonic()
        )
        clamped = bool(
            new_buffer_elements and (clamped_by_items or clamped_by_time)
        )
        if new_buffer_elements and clamped:
            # Still clamped, re-arm the deferral timer to self-chain into
            # another bundle!
            # Dynamic leap-frog: Align the timer deadline with the start time
            # of the oldest unprocessed chunk currently waiting in the buffer.
            # If there's a gap (e.g. downtime), this leaps the entire gap in exactly 1 step!
            oldest_chunk_ts_sec = (
                new_buffer_elements[0].timestamp_ms
                / common_constants.MS_PER_SECOND
            )
            next_deadline = max(
                timestamp + trans_constants.WINDMILL_TIMER_MIN_ADVANCE_SECS,
                Timestamp(seconds=oldest_chunk_ts_sec),
            )
            deferred_drain_timer.set(next_deadline)

            self._record_clamping_diagnostics(
                task_logger=task_logger,
                clamped_by_items=clamped_by_items,
                clamped_by_time=clamped_by_time,
                elements_to_emit_count=len(elements_to_emit),
                remaining_buffer_count=len(new_buffer_elements),
                context_label="Deferred drain clamped",
                rescheduled_deadline=next_deadline,
            )
        elif new_buffer_elements:
            # Not clamped: the remaining buffered chunks are a genuine
            # gap (not yet ready to drain), so arm the gap timeout
            # rather than immediately re-chaining the deferred drain.
            _reschedule_gap_timeout(
                gap_timer_event=gap_timer_event,
                gap_timer_proc=gap_timer_proc,
                order_config=self.order_config,
                timestamp=timestamp,
                clamped=False,
                is_backfill=_evaluate_is_backfill(
                    new_buffer_elements[0].timestamp_ms,
                    self.stitch_config.backfill_lateness_threshold_ms,
                ),
                new_expected=initial_expected_ts,
                new_expected_next_ts=new_expected_next_ts,
            )

    def _record_deferred_drain_wedge_candidate(
        self,
        *,
        elements_to_emit: list[datatypes.BufferedChunk],
        new_buffer_elements: list[datatypes.BufferedChunk],
        task_logger: std_logging.LoggerAdapter,
    ) -> None:
        """Flags a deferred drain that found nothing ready while chunks remain buffered.

        Not a guaranteed wedge: this invocation was specifically scheduled to
        check the buffer and found nothing ready to drain -- the oldest chunk
        is still waiting on a predecessor that hasn't arrived. A single
        occurrence is normal; the gap-timeout path re-armed by the caller is
        expected to resolve it within one out_of_order_timeout_ms window,
        either by the predecessor arriving or by forcibly advancing past it.
        A *sustained* rate of this counter for the same feed without the
        buffer ever clearing is the signal that indicates a wedge (i.e. finding #6,
        where a deferred drain execution left an out-of-order gap unresolved
        without re-arming a timer to check again, causing the buffer to remain
        stuck indefinitely until a new chunk arrived).
        """
        # A wedge candidate occurs when nothing was ready to emit (elements_to_emit is empty),
        # but chunks still remain buffered waiting for predecessors.
        if not elements_to_emit and new_buffer_elements:
            self.deferred_drain_empty_while_buffered.inc()
            task_logger.warning(
                "[Deferred Drain] Fired but nothing ready to drain; "
                "%d chunk(s) still buffered, oldest at %d. Gap timeout "
                "re-armed by caller to force resolution.",
                len(new_buffer_elements),
                new_buffer_elements[0].timestamp_ms,
            )

    def _record_clamping_diagnostics(
        self,
        *,
        clamped_by_items: bool,
        clamped_by_time: bool,
        elements_to_emit_count: int,
        remaining_buffer_count: int,
        context_label: str,
        task_logger: std_logging.LoggerAdapter | None = None,
        rescheduled_deadline: Timestamp | None = None,
        elapsed_sec: float | None = None,
    ) -> None:
        """Records Beam metrics counters and structured logging when bundle drain is clamped."""
        if elapsed_sec is None:
            elapsed_sec = time.monotonic() - self._get_bundle_start_time()
        reasons = []
        if clamped_by_items:
            self.bundle_clamped_item_limit.inc()
            reasons.append(
                f"item_count_limit (emitted={elements_to_emit_count}, bundle_processed={self.processed_in_bundle}/{trans_constants.MAX_CHUNKS_PER_WINDMILL_BUNDLE})"
            )
        if clamped_by_time:
            self.bundle_clamped_duration_limit.inc()
            reasons.append(
                f"wall_clock_timeout ({elapsed_sec:.2f}s/{trans_constants.MAX_WINDMILL_BUNDLE_DURATION_SEC:.0f}s)"
            )

        if task_logger is not None:
            reason_str = ", ".join(reasons) or "clamped"
            if rescheduled_deadline is not None:
                task_logger.info(
                    "[Windmill Clamp] %s (%s). Buffer remaining: %d. Rescheduled timer at %s",
                    context_label,
                    reason_str,
                    remaining_buffer_count,
                    rescheduled_deadline,
                )
            else:
                task_logger.info(
                    "[Windmill Clamp] %s (%s). Buffer remaining: %d",
                    context_label,
                    reason_str,
                    remaining_buffer_count,
                )

    def _handle_budget_exhausted(
        self,
        metadata: datatypes.ChunkMetadata,
        timestamp: Timestamp,
        curr_context: datatypes.ActiveStitchingState | datatypes.IdleFeedState,
        transmission_context_state: ReadModifyWriteRuntimeState,
        last_start_ms_state: ReadModifyWriteRuntimeState,
        out_of_order_buffer_state: BagRuntimeState,
        deferred_drain_timer: RuntimeTimer,
        *,
        state_changed: bool,
        task_logger: std_logging.LoggerAdapter | None = None,
    ) -> None:
        """Handles bundle budget exhaustion by buffering the chunk and setting the deferred drain timer."""
        current_ts_ms = (
            metadata.timestamp_ms
            if metadata.timestamp_ms is not None
            else int(float(timestamp) * common_constants.MS_PER_SECOND)
        )
        new_chunk = datatypes.BufferedChunk(
            timestamp_ms=current_ts_ms,
            gcs_uri=metadata.gcs_uri,
            traceparent=metadata.traceparent,
            baggage=metadata.baggage,
        )
        out_of_order_buffer_state.add(new_chunk)
        if not curr_context.order_timer_active:
            curr_context = replace(curr_context, order_timer_active=True)
            state_changed = True

        if state_changed:
            _write_transmission_context(
                transmission_context_state,
                curr_context,
                last_start_ms_state,
                out_of_order_buffer_state,
            )

        buffer_elements = list(out_of_order_buffer_state.read())
        if new_chunk not in buffer_elements:
            buffer_elements.append(new_chunk)
        JITTER_BUFFER_DEPTH.update(len(buffer_elements))

        oldest_chunk_ts_sec = (
            min(c.timestamp_ms for c in buffer_elements)
            / common_constants.MS_PER_SECOND
        )
        next_deadline = max(
            timestamp + trans_constants.WINDMILL_TIMER_MIN_ADVANCE_SECS,
            Timestamp(seconds=oldest_chunk_ts_sec),
        )
        deferred_drain_timer.set(next_deadline)

        # Diagnostics & Metrics
        clamped_by_items = (
            self.processed_in_bundle
            >= trans_constants.MAX_CHUNKS_PER_WINDMILL_BUNDLE
        )
        elapsed_sec = time.monotonic() - self._get_bundle_start_time()
        clamped_by_time = (
            elapsed_sec >= trans_constants.MAX_WINDMILL_BUNDLE_DURATION_SEC
        )
        self._record_clamping_diagnostics(
            task_logger=task_logger,
            clamped_by_items=clamped_by_items,
            clamped_by_time=clamped_by_time,
            elements_to_emit_count=0,
            remaining_buffer_count=len(buffer_elements),
            context_label="Process budget exhausted",
            rescheduled_deadline=next_deadline,
            elapsed_sec=elapsed_sec,
        )

    @override
    def process(
        self,
        element: tuple[str, datatypes.ChunkMetadata],
        timestamp: Timestamp = beam.DoFn.TimestampParam,  # type: ignore
        transmission_context_state: ReadModifyWriteRuntimeState = TRANSMISSION_CONTEXT_STATE,  # type: ignore
        last_start_ms_state: ReadModifyWriteRuntimeState = LAST_START_MS_STATE,  # type: ignore
        out_of_order_buffer_state: BagRuntimeState = OUT_OF_ORDER_BUFFER_STATE,  # type: ignore
        gap_timer_event: RuntimeTimer = GAP_TIMER_EVENT,  # type: ignore
        gap_timer_proc: RuntimeTimer = GAP_TIMER_PROC,  # type: ignore
        stale_timer_event: RuntimeTimer = STALE_TIMER_EVENT_PARAM,  # type: ignore
        stale_timer_proc: RuntimeTimer = STALE_TIMER_PROC_PARAM,  # type: ignore
        deferred_drain_timer: RuntimeTimer = DEFERRED_DRAIN_TIMER,  # type: ignore
    ) -> Iterator[
        tuple[str, datatypes.FlushRequest] | beam.pvalue.TaggedOutput
    ]:
        """Intercepts chunk arrival, resolves chronological ordering, and delegates to StitcherEngine."""
        key_str, metadata = element
        if "#" in key_str:
            feed_id, _ = key_str.split("#", 1)
        else:
            feed_id = key_str

        trace_attrs = {}
        if metadata.traceparent:
            trace_attrs["traceparent"] = metadata.traceparent
        if metadata.baggage:
            trace_attrs["baggage"] = metadata.baggage

        # Load context and handle migration
        curr_context = (
            transmission_context_state.read() or datatypes.IdleFeedState()
        )
        initial_expected_ts = (
            curr_context.expected_next_chunk_start_ms
            if isinstance(curr_context, datatypes.ActiveStitchingState)
            else None
        )

        curr_context, state_changed = _migrate_legacy_buffer(
            curr_context, out_of_order_buffer_state
        )

        task_logger = _get_task_logger(
            feed_id, metadata.session_id, "ordered-stitcher"
        )
        curr_context, session_changed = _handle_session_transition(
            curr_context=curr_context,
            metadata=metadata,
            gap_timer_event=gap_timer_event,
            gap_timer_proc=gap_timer_proc,
            task_logger=task_logger,
        )
        if session_changed:
            state_changed = True
            stale_timer_event.clear()
            stale_timer_proc.clear()
            curr_context = replace(
                curr_context, prior_audio_tail=None, sample_rate=None
            )
            out_of_order_buffer_state.clear()

        # Windmill lease guard
        if self._is_bundle_budget_exhausted():
            self._handle_budget_exhausted(
                metadata=metadata,
                timestamp=timestamp,
                curr_context=curr_context,
                transmission_context_state=transmission_context_state,
                last_start_ms_state=last_start_ms_state,
                out_of_order_buffer_state=out_of_order_buffer_state,
                deferred_drain_timer=deferred_drain_timer,
                state_changed=state_changed,
                task_logger=task_logger,
            )
            return

        results: list[
            tuple[str, datatypes.FlushRequest]
            | tuple[str, datatypes.StitcherDlqPayload]
        ] = []
        with tracing_utils.with_tracer_context(
            trace_attrs, "stitching_process", __name__
        ):
            current_ts_ms = int(
                float(timestamp) * common_constants.MS_PER_SECOND
            )
            is_backfill = _evaluate_is_backfill(
                current_ts_ms,
                self.stitch_config.backfill_lateness_threshold_ms,
            )

            elements_to_emit, curr_context, session_changed = process_ordering(
                element,
                timestamp,
                curr_context,
                out_of_order_buffer_state,
                gap_timer_event,
                gap_timer_proc,
                self.order_config,
                is_backfill=is_backfill,
                session_changed=session_changed,
                max_emit=trans_constants.MAX_CHUNKS_PER_WINDMILL_BUNDLE
                - self.processed_in_bundle,
                deadline_monotonic=self._get_bundle_deadline_monotonic(),
            )
            task_logger = _get_task_logger(
                feed_id,
                curr_context.session_id
                if isinstance(curr_context, datatypes.ActiveStitchingState)
                else metadata.session_id,
                "transcription-stitcher",
            )
            task_logger.debug(f"[Process] Processing chunk {metadata.gcs_uri}")

            # Delegate chunk elements to the execution engine
            if elements_to_emit:
                timer_manager = StaleTimerManager(
                    stale_timer_event, stale_timer_proc, self.stitch_config
                )

                is_backfill = _evaluate_is_backfill(
                    current_ts_ms,
                    self.stitch_config.backfill_lateness_threshold_ms,
                )

                previous_expected_ts = initial_expected_ts
                last_start_ms = last_start_ms_state.read()
                (
                    outputs,
                    curr_context,
                    last_start_ms,
                ) = self._execute_bundle_chunks(
                    elements_to_emit=elements_to_emit,
                    feed_id=feed_id,
                    curr_context=curr_context,
                    last_start_ms=last_start_ms,
                    timer_manager=timer_manager,
                    previous_expected_ts=previous_expected_ts,
                    is_backfill=is_backfill,
                    session_changed=session_changed,
                    active_session_id=metadata.session_id,
                    active_feed_metadata=metadata.feed_metadata,
                    active_traceparent=metadata.traceparent,
                    active_baggage=metadata.baggage,
                    increment_processed=True,
                    out_of_order_buffer_state=out_of_order_buffer_state,
                    deferred_drain_timer=deferred_drain_timer,
                    timestamp=timestamp,
                    transmission_context_state=transmission_context_state,
                    last_start_ms_state=last_start_ms_state,
                    gap_timer_event=gap_timer_event,
                    gap_timer_proc=gap_timer_proc,
                    timer_type="main",
                )
                results.extend(outputs)

                if last_start_ms is not None:
                    last_start_ms_state.write(last_start_ms)

            # Commit sequence context updates
            _write_transmission_context(
                transmission_context_state,
                curr_context,
                last_start_ms_state,
                out_of_order_buffer_state,
            )

        yield from self._yield_tagged_outputs(results)

    @on_timer(GAP_TIMER_EVENT_SPEC)
    def handle_gap_timeout_event(
        self,
        feed_id: str = beam.DoFn.KeyParam,  # type: ignore
        transmission_context_state: ReadModifyWriteRuntimeState = TRANSMISSION_CONTEXT_STATE,  # type: ignore
        last_start_ms_state: ReadModifyWriteRuntimeState = LAST_START_MS_STATE,  # type: ignore
        out_of_order_buffer_state: BagRuntimeState = OUT_OF_ORDER_BUFFER_STATE,  # type: ignore
        stale_timer_event: RuntimeTimer = STALE_TIMER_EVENT_PARAM,  # type: ignore
        stale_timer_proc: RuntimeTimer = STALE_TIMER_PROC_PARAM,  # type: ignore
        timestamp: Timestamp = beam.DoFn.TimestampParam,  # type: ignore
        gap_timer_event: RuntimeTimer = GAP_TIMER_EVENT,  # type: ignore
        gap_timer_proc: RuntimeTimer = GAP_TIMER_PROC,  # type: ignore
        deferred_drain_timer: RuntimeTimer = DEFERRED_DRAIN_TIMER,  # type: ignore
    ) -> Iterator[
        tuple[str, datatypes.FlushRequest] | beam.pvalue.TaggedOutput
    ]:
        """Handles the gap timeout triggered by the event-time watermark."""
        yield from self._handle_gap_timeout_common(
            key_str=feed_id,
            transmission_context_state=transmission_context_state,
            last_start_ms_state=last_start_ms_state,
            out_of_order_buffer_state=out_of_order_buffer_state,
            stale_timer_event=stale_timer_event,
            stale_timer_proc=stale_timer_proc,
            timestamp=timestamp,
            gap_timer_event=gap_timer_event,
            gap_timer_proc=gap_timer_proc,
            deferred_drain_timer=deferred_drain_timer,
            timer_type="event",
        )

    @on_timer(GAP_TIMER_PROC_SPEC)
    def handle_gap_timeout_processing(
        self,
        feed_id: str = beam.DoFn.KeyParam,  # type: ignore
        transmission_context_state: ReadModifyWriteRuntimeState = TRANSMISSION_CONTEXT_STATE,  # type: ignore
        last_start_ms_state: ReadModifyWriteRuntimeState = LAST_START_MS_STATE,  # type: ignore
        out_of_order_buffer_state: BagRuntimeState = OUT_OF_ORDER_BUFFER_STATE,  # type: ignore
        stale_timer_event: RuntimeTimer = STALE_TIMER_EVENT_PARAM,  # type: ignore
        stale_timer_proc: RuntimeTimer = STALE_TIMER_PROC_PARAM,  # type: ignore
        timestamp: Timestamp = beam.DoFn.TimestampParam,  # type: ignore
        gap_timer_event: RuntimeTimer = GAP_TIMER_EVENT,  # type: ignore
        gap_timer_proc: RuntimeTimer = GAP_TIMER_PROC,  # type: ignore
        deferred_drain_timer: RuntimeTimer = DEFERRED_DRAIN_TIMER,  # type: ignore
    ) -> Iterator[
        tuple[str, datatypes.FlushRequest] | beam.pvalue.TaggedOutput
    ]:
        """Handles the gap timeout triggered by the processing-time clock."""
        gap_timer_event.clear()
        yield from self._handle_gap_timeout_common(
            key_str=feed_id,
            transmission_context_state=transmission_context_state,
            last_start_ms_state=last_start_ms_state,
            out_of_order_buffer_state=out_of_order_buffer_state,
            stale_timer_event=stale_timer_event,
            stale_timer_proc=stale_timer_proc,
            timestamp=timestamp,
            gap_timer_event=gap_timer_event,
            gap_timer_proc=gap_timer_proc,
            deferred_drain_timer=deferred_drain_timer,
            timer_type="processing",
        )

    def _execute_bundle_chunks(
        self,
        elements_to_emit: list[datatypes.BufferedChunk],
        feed_id: str,
        curr_context: datatypes.TransmissionContext,
        last_start_ms: int | None,
        timer_manager: Any,
        previous_expected_ts: int | None,
        *,
        is_backfill: bool,
        session_changed: bool = False,
        active_session_id: str | None = None,
        active_feed_metadata: datatypes.FeedMetadata | None = None,
        active_traceparent: str | None = None,
        active_baggage: str | None = None,
        increment_processed: bool = True,
        out_of_order_buffer_state: Any = None,
        deferred_drain_timer: Any = None,
        timestamp: Timestamp | None = None,
        transmission_context_state: Any = None,
        last_start_ms_state: Any = None,
        gap_timer_event: Any = None,
        gap_timer_proc: Any = None,
        timer_type: str = "main",
    ) -> tuple[
        list[
            tuple[str, datatypes.FlushRequest]
            | tuple[str, datatypes.StitcherDlqPayload]
        ],
        datatypes.TransmissionContext,
        int | None,
    ]:
        """Processes and stitches a batch of chunks for a feed in local memory."""
        results: list[
            tuple[str, datatypes.FlushRequest]
            | tuple[str, datatypes.StitcherDlqPayload]
        ] = []
        task_logger = _get_task_logger(
            feed_id,
            curr_context.session_id
            if isinstance(curr_context, datatypes.ActiveStitchingState)
            else active_session_id,
            "transcription-stitcher",
        )
        # Prefetch only up to PREFETCH_WINDOW_SIZE chunks initially
        initial_prefetch_chunks = elements_to_emit[
            : trans_constants.PREFETCH_WINDOW_SIZE
        ]
        prefetched_futures = (
            self.engine.prefetch_audio_futures(
                initial_prefetch_chunks, task_logger
            )
            or {}
        )
        original_expected_ts = previous_expected_ts
        for i, chunk in enumerate(elements_to_emit):
            if i > 0 and self._is_bundle_budget_exhausted():
                for remaining_chunk in elements_to_emit[i:]:
                    out_of_order_buffer_state.add(remaining_chunk)
                    if remaining_chunk.gcs_uri in prefetched_futures:
                        prefetched_futures[remaining_chunk.gcs_uri].cancel()
                next_deadline: Timestamp | None = None
                if deferred_drain_timer is not None and timestamp is not None:
                    oldest_chunk_ts_sec = (
                        chunk.timestamp_ms / common_constants.MS_PER_SECOND
                    )
                    next_deadline = max(
                        timestamp
                        + trans_constants.WINDMILL_TIMER_MIN_ADVANCE_SECS,
                        Timestamp(seconds=oldest_chunk_ts_sec),
                    )
                    deferred_drain_timer.set(next_deadline)

                clamped_by_items = (
                    self.processed_in_bundle
                    >= trans_constants.MAX_CHUNKS_PER_WINDMILL_BUNDLE
                )
                elapsed_sec = time.monotonic() - self._get_bundle_start_time()
                clamped_by_time = (
                    elapsed_sec
                    >= trans_constants.MAX_WINDMILL_BUNDLE_DURATION_SEC
                )
                remaining_elements = list(out_of_order_buffer_state.read())
                clamp_logger = _get_task_logger(
                    feed_id,
                    curr_context.session_id
                    if isinstance(curr_context, datatypes.ActiveStitchingState)
                    else active_session_id,
                    "ordered-stitcher",
                )
                self._record_clamping_diagnostics(
                    task_logger=clamp_logger,
                    clamped_by_items=clamped_by_items,
                    clamped_by_time=clamped_by_time,
                    elements_to_emit_count=i,
                    remaining_buffer_count=len(remaining_elements),
                    context_label="Mid-execution bundle budget exhausted",
                    rescheduled_deadline=next_deadline,
                    elapsed_sec=elapsed_sec,
                )
                if not isinstance(curr_context, datatypes.ActiveStitchingState):
                    msg = "curr_context must be an ActiveStitchingState"
                    raise TypeError(msg)
                curr_context = replace(
                    curr_context,
                    expected_next_chunk_start_ms=previous_expected_ts,
                    order_timer_active=True,
                )
                if (
                    timer_type == "gap"
                    and gap_timer_event is not None
                    and timestamp is not None
                ):
                    _reschedule_gap_timeout(
                        gap_timer_event=gap_timer_event,
                        gap_timer_proc=gap_timer_proc,
                        order_config=self.order_config,
                        timestamp=timestamp,
                        clamped=True,
                        is_backfill=is_backfill,
                        new_expected=original_expected_ts,
                        new_expected_next_ts=previous_expected_ts,
                    )
                _write_transmission_context(
                    transmission_context_state,
                    curr_context,
                    last_start_ms_state,
                    out_of_order_buffer_state,
                )
                break

            # Enqueue next chunk ahead in sliding window
            prefetch_idx = i + trans_constants.PREFETCH_WINDOW_SIZE
            if prefetch_idx < len(elements_to_emit):
                next_chunk = elements_to_emit[prefetch_idx]
                if next_chunk.gcs_uri not in prefetched_futures:
                    future = self.engine.submit_single_prefetch(
                        next_chunk.gcs_uri, task_logger
                    )
                    if future is not None:
                        prefetched_futures[next_chunk.gcs_uri] = future

            if isinstance(curr_context, datatypes.IdleFeedState):
                curr_context = datatypes.ActiveStitchingState(
                    session_id=active_session_id or "unknown",
                    feed_metadata=active_feed_metadata
                    or datatypes.FeedMetadata(feed_name=feed_id),
                    out_of_order_buffer=[],
                    order_timer_active=curr_context.order_timer_active,
                    traceparent=active_traceparent,
                    baggage=active_baggage,
                )
            chunk_res = self.engine.process_ordering_chunk(
                chunk=chunk,
                feed_id=feed_id,
                curr_context=curr_context,
                last_start_ms=last_start_ms,
                timer_manager=timer_manager,
                previous_expected_ts=previous_expected_ts,
                is_backfill=is_backfill,
                clear_buffer=(session_changed and i == 0),
                prefetched_futures=prefetched_futures,
            )
            results.extend(chunk_res.outputs)
            curr_context = chunk_res.next_context
            previous_expected_ts = chunk_res.next_expected_ts
            last_start_ms = chunk_res.next_last_start_ms
            if increment_processed:
                self.processed_in_bundle += 1

        return results, curr_context, last_start_ms

    def _drain_and_update_buffer(
        self,
        *,
        seq_buf: sequence_buffer.SequenceBuffer,
        new_expected: int,
        buffer_elements: list[datatypes.BufferedChunk],
        out_of_order_buffer_state: BagRuntimeState,
        feed_id: str,
        active_session_id: str,
    ) -> tuple[
        int | None,
        list[datatypes.BufferedChunk],
        list[datatypes.BufferedChunk],
        bool,
    ]:
        """Drains ready elements from SequenceBuffer and updates persistent bag state."""
        new_expected_next_ts, new_buffer_elements, elements_to_emit = (
            seq_buf.drain_ready_elements(
                expected_next_ts=new_expected,
                buffer_elements=buffer_elements,
                epsilon_ms=trans_constants.DEFAULT_FLOAT_TOLERANCE_MS,
                max_emit=trans_constants.MAX_CHUNKS_PER_WINDMILL_BUNDLE
                - self.processed_in_bundle,
                deadline_monotonic=self._get_bundle_deadline_monotonic(),
            )
        )
        out_of_order_buffer_state.clear()
        for c in new_buffer_elements:
            out_of_order_buffer_state.add(c)

        clamped_by_items = len(elements_to_emit) >= (
            trans_constants.MAX_CHUNKS_PER_WINDMILL_BUNDLE
            - self.processed_in_bundle
        )
        clamped_by_time = (
            time.monotonic() >= self._get_bundle_deadline_monotonic()
        )
        clamped = bool(
            new_buffer_elements and (clamped_by_items or clamped_by_time)
        )
        if new_buffer_elements and clamped:
            clamp_logger = _get_task_logger(
                feed_id, active_session_id, "ordered-stitcher"
            )
            self._record_clamping_diagnostics(
                task_logger=clamp_logger,
                clamped_by_items=clamped_by_items,
                clamped_by_time=clamped_by_time,
                elements_to_emit_count=len(elements_to_emit),
                remaining_buffer_count=len(new_buffer_elements),
                context_label="Backlog drain clamped during process",
            )
        return (
            new_expected_next_ts,
            new_buffer_elements,
            elements_to_emit,
            clamped,
        )

    def _advance_context_for_gap(
        self,
        *,
        curr_context: datatypes.ActiveStitchingState,
        feed_id: str,
        timer_type: str,
        sorted_elements: list[datatypes.BufferedChunk],
    ) -> tuple[datatypes.ActiveStitchingState, int]:
        """Advances expected_next_chunk_start_ms upon an audio gap timeout."""
        new_expected = sorted_elements[0].timestamp_ms
        logger.warning(
            f"[{feed_id}] Gap timeout ({timer_type})! Advancing expected from {curr_context.expected_next_chunk_start_ms} to {new_expected}."
        )
        updated_context = replace(
            curr_context,
            expected_next_chunk_start_ms=new_expected,
            missing_prior_context=True,
        )
        return updated_context, new_expected

    def _handle_gap_timeout_common(
        self,
        key_str: str,
        transmission_context_state: ReadModifyWriteRuntimeState,
        last_start_ms_state: ReadModifyWriteRuntimeState,
        out_of_order_buffer_state: BagRuntimeState,
        stale_timer_event: RuntimeTimer,
        stale_timer_proc: RuntimeTimer,
        timestamp: Timestamp,
        gap_timer_event: RuntimeTimer,
        gap_timer_proc: RuntimeTimer,
        deferred_drain_timer: RuntimeTimer,
        *,
        timer_type: str,
    ) -> Iterator[
        tuple[str, datatypes.FlushRequest] | beam.pvalue.TaggedOutput
    ]:
        """Handles the gap timeout by advancing the expected sequence (common logic)."""
        if "#" in key_str:
            feed_id, _ = key_str.split("#", 1)
        else:
            feed_id = key_str
        curr_context = (
            transmission_context_state.read() or datatypes.IdleFeedState()
        )
        if isinstance(curr_context, datatypes.IdleFeedState):
            return
        curr_context, _ = _migrate_legacy_buffer(
            curr_context, out_of_order_buffer_state
        )
        if not isinstance(curr_context, datatypes.ActiveStitchingState):
            msg = "Expected ActiveStitchingState after migration"
            raise TypeError(msg)
        trace_attrs: dict[str, str] = {}
        if curr_context.traceparent:
            trace_attrs["traceparent"] = curr_context.traceparent
        if curr_context.baggage:
            trace_attrs["baggage"] = curr_context.baggage
        (
            active_session_id,
            active_feed_metadata,
            active_traceparent,
            active_baggage,
        ) = (
            curr_context.session_id,
            curr_context.feed_metadata,
            curr_context.traceparent,
            curr_context.baggage,
        )

        results: list[
            tuple[str, datatypes.FlushRequest]
            | tuple[str, datatypes.StitcherDlqPayload]
        ] = []
        with tracing_utils.with_tracer_context(
            trace_attrs, "handle_audio_gap", __name__
        ):
            gap_timer_event.clear()
            gap_timer_proc.clear()
            curr_context = replace(curr_context, order_timer_active=False)

            buffer_elements = list(out_of_order_buffer_state.read())
            if buffer_elements:
                sorted_elements = sorted(
                    buffer_elements, key=lambda x: x.timestamp_ms
                )
                curr_context, new_expected = self._advance_context_for_gap(
                    curr_context=curr_context,
                    feed_id=feed_id,
                    timer_type=timer_type,
                    sorted_elements=sorted_elements,
                )

                seq_buf = sequence_buffer.SequenceBuffer(self.order_config)

                (
                    new_expected_next_ts,
                    new_buffer_elements,
                    elements_to_emit,
                    clamped,
                ) = self._drain_and_update_buffer(
                    seq_buf=seq_buf,
                    new_expected=new_expected,
                    buffer_elements=buffer_elements,
                    out_of_order_buffer_state=out_of_order_buffer_state,
                    feed_id=feed_id,
                    active_session_id=active_session_id,
                )
                JITTER_BUFFER_DEPTH.update(len(new_buffer_elements))

                first_chunk_ts = sorted_elements[0].timestamp_ms
                is_backfill = _evaluate_is_backfill(
                    first_chunk_ts,
                    self.stitch_config.backfill_lateness_threshold_ms,
                )
                if new_buffer_elements:
                    timer_active = _reschedule_gap_timeout(
                        gap_timer_event=gap_timer_event,
                        gap_timer_proc=gap_timer_proc,
                        order_config=self.order_config,
                        timestamp=timestamp,
                        clamped=clamped,
                        is_backfill=is_backfill,
                        new_expected=new_expected,
                        new_expected_next_ts=new_expected_next_ts,
                    )
                    curr_context = replace(
                        curr_context, order_timer_active=timer_active
                    )
                else:
                    curr_context = replace(
                        curr_context, order_timer_active=False
                    )

                curr_context = replace(
                    curr_context,
                    expected_next_chunk_start_ms=new_expected_next_ts,
                )

                # Handle ready elements
                if elements_to_emit:
                    timer_manager = StaleTimerManager(
                        stale_timer_event, stale_timer_proc, self.stitch_config
                    )

                    previous_expected_ts = new_expected
                    last_start_ms = last_start_ms_state.read()
                    (
                        outputs,
                        curr_context,
                        last_start_ms,
                    ) = self._execute_bundle_chunks(
                        elements_to_emit=elements_to_emit,
                        feed_id=feed_id,
                        curr_context=curr_context,
                        last_start_ms=last_start_ms,
                        timer_manager=timer_manager,
                        previous_expected_ts=previous_expected_ts,
                        is_backfill=is_backfill,
                        session_changed=False,
                        active_session_id=active_session_id,
                        active_feed_metadata=active_feed_metadata,
                        active_traceparent=active_traceparent,
                        active_baggage=active_baggage,
                        increment_processed=False,
                        out_of_order_buffer_state=out_of_order_buffer_state,
                        deferred_drain_timer=deferred_drain_timer,
                        timestamp=timestamp,
                        transmission_context_state=transmission_context_state,
                        last_start_ms_state=last_start_ms_state,
                        gap_timer_event=gap_timer_event,
                        gap_timer_proc=gap_timer_proc,
                        timer_type="gap",
                    )
                    results.extend(outputs)

                    if last_start_ms is not None:
                        last_start_ms_state.write(last_start_ms)

                _write_transmission_context(
                    transmission_context_state,
                    curr_context,
                    last_start_ms_state,
                    out_of_order_buffer_state,
                )
            else:
                _write_transmission_context(
                    transmission_context_state,
                    curr_context,
                    last_start_ms_state,
                    out_of_order_buffer_state,
                )

        yield from self._yield_tagged_outputs(results)

    @on_timer(DEFERRED_DRAIN_TIMER_SPEC)
    def handle_deferred_drain(
        self,
        feed_id: str = beam.DoFn.KeyParam,  # type: ignore
        transmission_context_state: ReadModifyWriteRuntimeState = TRANSMISSION_CONTEXT_STATE,  # type: ignore
        last_start_ms_state: ReadModifyWriteRuntimeState = LAST_START_MS_STATE,  # type: ignore
        out_of_order_buffer_state: BagRuntimeState = OUT_OF_ORDER_BUFFER_STATE,  # type: ignore
        stale_timer_event: RuntimeTimer = STALE_TIMER_EVENT_PARAM,  # type: ignore
        stale_timer_proc: RuntimeTimer = STALE_TIMER_PROC_PARAM,  # type: ignore
        timestamp: Timestamp = beam.DoFn.TimestampParam,  # type: ignore
        gap_timer_event: RuntimeTimer = GAP_TIMER_EVENT,  # type: ignore
        gap_timer_proc: RuntimeTimer = GAP_TIMER_PROC,  # type: ignore
        deferred_drain_timer: RuntimeTimer = DEFERRED_DRAIN_TIMER,  # type: ignore
    ) -> Iterator[
        tuple[str, datatypes.FlushRequest] | beam.pvalue.TaggedOutput
    ]:
        """Drains deferred chunks from the sequence buffer in a fresh bundle."""
        if "#" in feed_id:
            feed_id, _ = feed_id.split("#", 1)
        curr_context = (
            transmission_context_state.read() or datatypes.IdleFeedState()
        )
        if isinstance(curr_context, datatypes.IdleFeedState):
            return
        curr_context, _ = _migrate_legacy_buffer(
            curr_context, out_of_order_buffer_state
        )
        if not isinstance(curr_context, datatypes.ActiveStitchingState):
            msg = "Expected ActiveStitchingState after migration"
            raise TypeError(msg)
        trace_attrs: dict[str, str] = {}
        if curr_context.traceparent:
            trace_attrs["traceparent"] = curr_context.traceparent
        if curr_context.baggage:
            trace_attrs["baggage"] = curr_context.baggage
        active_session_id = curr_context.session_id
        active_feed_metadata = curr_context.feed_metadata
        active_traceparent = curr_context.traceparent
        active_baggage = curr_context.baggage
        task_logger = _get_task_logger(
            feed_id, active_session_id, "ordered-stitcher"
        )

        results: list[
            tuple[str, datatypes.FlushRequest]
            | tuple[str, datatypes.StitcherDlqPayload]
        ] = []
        with tracing_utils.with_tracer_context(
            trace_attrs, "deferred_drain", __name__
        ):
            self.deferred_drain_invocations.inc()

            # We do NOT set missing_prior_context=True, keeping the continuous tail!
            seq_buf = sequence_buffer.SequenceBuffer(self.order_config)
            buffer_elements = list(out_of_order_buffer_state.read())

            # Cap the drain based on our remaining bundle capacity.
            # In a fresh timer-activated bundle, processed_in_bundle starts at 0, so
            # we can drain up to the full MAX_CHUNKS_PER_WINDMILL_BUNDLE.
            initial_expected_ts = curr_context.expected_next_chunk_start_ms
            new_expected_next_ts, new_buffer_elements, elements_to_emit = (
                seq_buf.drain_ready_elements(
                    expected_next_ts=initial_expected_ts,
                    buffer_elements=buffer_elements,
                    epsilon_ms=trans_constants.DEFAULT_FLOAT_TOLERANCE_MS,
                    max_emit=trans_constants.MAX_CHUNKS_PER_WINDMILL_BUNDLE
                    - self.processed_in_bundle,
                    deadline_monotonic=self._get_bundle_deadline_monotonic(),
                )
            )
            self.deferred_drain_chunks_emitted.update(len(elements_to_emit))

            out_of_order_buffer_state.clear()
            for c in new_buffer_elements:
                out_of_order_buffer_state.add(c)
            JITTER_BUFFER_DEPTH.update(len(new_buffer_elements))

            self._record_deferred_drain_wedge_candidate(
                elements_to_emit=elements_to_emit,
                new_buffer_elements=new_buffer_elements,
                task_logger=task_logger,
            )

            self._reschedule_after_deferred_drain(
                elements_to_emit=elements_to_emit,
                new_buffer_elements=new_buffer_elements,
                initial_expected_ts=initial_expected_ts,
                new_expected_next_ts=new_expected_next_ts,
                timestamp=timestamp,
                deferred_drain_timer=deferred_drain_timer,
                gap_timer_event=gap_timer_event,
                gap_timer_proc=gap_timer_proc,
                task_logger=task_logger,
            )

            curr_context = replace(
                curr_context,
                expected_next_chunk_start_ms=new_expected_next_ts,
                order_timer_active=len(new_buffer_elements) > 0,
            )
            if elements_to_emit:
                timer_manager = StaleTimerManager(
                    stale_timer_event, stale_timer_proc, self.stitch_config
                )

                is_backfill = _evaluate_is_backfill(
                    elements_to_emit[0].timestamp_ms,
                    self.stitch_config.backfill_lateness_threshold_ms,
                )
                previous_expected_ts = initial_expected_ts
                last_start_ms = last_start_ms_state.read()
                (
                    outputs,
                    curr_context,
                    last_start_ms,
                ) = self._execute_bundle_chunks(
                    elements_to_emit=elements_to_emit,
                    feed_id=feed_id,
                    curr_context=curr_context,
                    last_start_ms=last_start_ms,
                    timer_manager=timer_manager,
                    previous_expected_ts=previous_expected_ts,
                    is_backfill=is_backfill,
                    session_changed=False,
                    active_session_id=active_session_id,
                    active_feed_metadata=active_feed_metadata,
                    active_traceparent=active_traceparent,
                    active_baggage=active_baggage,
                    increment_processed=True,
                    out_of_order_buffer_state=out_of_order_buffer_state,
                    deferred_drain_timer=deferred_drain_timer,
                    timestamp=timestamp,
                    transmission_context_state=transmission_context_state,
                    last_start_ms_state=last_start_ms_state,
                    timer_type="main",
                )
                results.extend(outputs)

                if last_start_ms is not None:
                    last_start_ms_state.write(last_start_ms)

            _write_transmission_context(
                transmission_context_state,
                curr_context,
                last_start_ms_state,
                out_of_order_buffer_state,
            )

        yield from self._yield_tagged_outputs(results)

    @on_timer(STALE_TIMER_EVENT_SPEC)
    def handle_stale_transmission_event(
        self,
        key: str = beam.DoFn.KeyParam,  # type: ignore
        transmission_context: ReadModifyWriteRuntimeState = TRANSMISSION_CONTEXT_STATE,  # type: ignore
        last_start_ms_state: ReadModifyWriteRuntimeState = LAST_START_MS_STATE,  # type: ignore
        out_of_order_buffer_state: BagRuntimeState = OUT_OF_ORDER_BUFFER_STATE,  # type: ignore
        stale_timer_event: RuntimeTimer = STALE_TIMER_EVENT_PARAM,  # type: ignore
        stale_timer_proc: RuntimeTimer = STALE_TIMER_PROC_PARAM,  # type: ignore
    ) -> Iterator[
        tuple[str, datatypes.FlushRequest] | beam.pvalue.TaggedOutput
    ]:
        """Watermark crossed stale duration, delegate flush to StitcherEngine."""
        if "#" in key:
            feed_id, _ = key.split("#", 1)
        else:
            feed_id = key
        timer_manager = StaleTimerManager(
            stale_timer_event, stale_timer_proc, self.stitch_config
        )
        yield from self._yield_tagged_outputs(
            self.engine.handle_stale_transmission(
                feed_id,
                transmission_context,
                last_start_ms_state,
                timer_manager,
                out_of_order_buffer_state,
            )
        )

    @on_timer(STALE_TIMER_PROC_SPEC)
    def handle_stale_transmission_proc(
        self,
        key: str = beam.DoFn.KeyParam,  # type: ignore
        transmission_context: ReadModifyWriteRuntimeState = TRANSMISSION_CONTEXT_STATE,  # type: ignore
        last_start_ms_state: ReadModifyWriteRuntimeState = LAST_START_MS_STATE,  # type: ignore
        out_of_order_buffer_state: BagRuntimeState = OUT_OF_ORDER_BUFFER_STATE,  # type: ignore
        stale_timer_event: RuntimeTimer = STALE_TIMER_EVENT_PARAM,  # type: ignore
        stale_timer_proc: RuntimeTimer = STALE_TIMER_PROC_PARAM,  # type: ignore
    ) -> Iterator[
        tuple[str, datatypes.FlushRequest] | beam.pvalue.TaggedOutput
    ]:
        """Wall-clock crossed stale duration, delegate flush to StitcherEngine."""
        if "#" in key:
            feed_id, _ = key.split("#", 1)
        else:
            feed_id = key
        timer_manager = StaleTimerManager(
            stale_timer_event, stale_timer_proc, self.stitch_config
        )
        yield from self._yield_tagged_outputs(
            self.engine.handle_stale_transmission(
                feed_id,
                transmission_context,
                last_start_ms_state,
                timer_manager,
                out_of_order_buffer_state,
            )
        )

    @property
    def audio_processor(self) -> Any:
        if hasattr(self, "engine"):
            return self.engine.processor
        return None

    @audio_processor.setter
    def audio_processor(self, val: Any) -> None:
        if hasattr(self, "engine"):
            self.engine.processor = val
