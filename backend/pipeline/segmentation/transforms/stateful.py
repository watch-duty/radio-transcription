"""Stateful Apache Beam transforms for the radio transcription pipeline.

This module defines the core stateful and chronological restoration boundary
DoFns in our Apache Beam DAG. It houses transforms responsible for:
1. unmarshaling incoming Pub/Sub JSON elements.
2. restoration of out-of-order segment arrivals using SequenceBuffer.
3. stateful timer management for processing window timeout flushes.
4. execution of speech-to-text transcription API requests.

All high-level audio download/concatenation/VAD heuristics are cleanly
decoupled from Beam timer variables and delegated to StitcherEngine.


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
   the DoFn immediately sets `out_of_order_timer` to the current watermark
   timestamp (`out_of_order_timer.set(timestamp)`).
3. Because the timer fires at or before the current watermark, Windmill schedules
   a new bundle immediately, which calls `handle_gap_timeout` and drains the next
   slice of up to `MAX_CHUNKS_PER_WINDMILL_BUNDLE` chunks.
4. This continues until the buffer is empty, at which point no timer is set and
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
from google.cloud import storage

from backend.pipeline.common import constants as common_constants
from backend.pipeline.common import tracing_utils
from backend.pipeline.common.log_helper import get_logger, get_task_logger
from backend.pipeline.segmentation import coders as trans_coders
from backend.pipeline.segmentation import constants as trans_constants
from backend.pipeline.segmentation import datatypes
from backend.pipeline.segmentation.audio import vad
from backend.pipeline.segmentation.constants import (
    MAX_CHUNKS_PER_WINDMILL_BUNDLE,
)
from backend.pipeline.segmentation.state import sequence_buffer
from backend.pipeline.segmentation.transforms import stitcher_engine

SHARED_RESOURCE_HANDLE = Shared()

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
) -> None:
    """Writes the context to transmission state or clears it completely if it is an empty IdleFeedState.

    Pruning completely empty IdleFeedStates from the database prevents session memory leaks and
    ensures that subsequent independent audio chunks start with a clean slate (initiating a new
    ActiveStitchingState and a fresh traceparent, effectively preventing Trace Context Hijacking).
    """
    if (
        isinstance(context, datatypes.IdleFeedState)
        and not context.out_of_order_buffer
        and not context.order_timer_active
    ):
        state_cell.clear()
    else:
        state_cell.write(context)


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


def process_ordering(
    element: tuple[str, datatypes.ChunkMetadata],
    timestamp: Timestamp,
    curr_context: datatypes.TransmissionContext,
    out_of_order_timer: RuntimeTimer,
    order_config: datatypes.OrderRestorerConfig,
) -> tuple[list[datatypes.BufferedChunk], datatypes.ActiveStitchingState, bool]:
    """Handles session change detection and chronological ordering via SequenceBuffer."""
    feed_id, metadata = element
    session_changed = False

    task_logger = _get_task_logger(
        feed_id, metadata.session_id, "sequence-buffer"
    )

    # Explicitly transition IdleFeedState to ActiveStitchingState on startup
    if isinstance(curr_context, datatypes.IdleFeedState):
        curr_context = datatypes.ActiveStitchingState(
            session_id=metadata.session_id,
            feed_metadata=metadata.feed_metadata,
            out_of_order_buffer=curr_context.out_of_order_buffer,
            order_timer_active=curr_context.order_timer_active,
            traceparent=metadata.traceparent
            or tracing_utils.get_current_traceparent(),
        )
        session_changed = True
        out_of_order_timer.clear()
    elif curr_context.session_id != metadata.session_id:
        task_logger.info(
            f"Session ID changed from {curr_context.session_id} to {metadata.session_id}. Resetting state."
        )
        session_changed = True
        out_of_order_timer.clear()
        curr_context = datatypes.ActiveStitchingState(
            session_id=metadata.session_id,
            feed_metadata=metadata.feed_metadata,
            traceparent=metadata.traceparent
            or tracing_utils.get_current_traceparent(),
        )

    seq_buf = sequence_buffer.SequenceBuffer(order_config)
    buffer_elements = curr_context.out_of_order_buffer
    current_ts_ms = (
        metadata.timestamp_ms
        if metadata.timestamp_ms is not None
        else int(float(timestamp) * common_constants.MS_PER_SECOND)
    )

    # Process chunk through jitter buffer. max_emit caps this bundle's output at
    # ~75 s of audio (MAX_CHUNKS_PER_WINDMILL_BUNDLE × ~15 s/chunk under load),
    # safely under Windmill's 300-second lease limit. If the drain hits the cap,
    # the timer block below re-arms immediately so the next bundle drains the rest.
    (
        new_expected_next_ts,
        new_buffer_elements,
        elements_to_emit,
        was_late,
        was_buffered,
    ) = seq_buf.process_chunk(
        current_ts_ms=current_ts_ms,
        gcs_uri=metadata.gcs_uri,
        expected_next_ts=curr_context.expected_next_chunk_start_ms,
        buffer_elements=buffer_elements,
        chunk_duration_ms=metadata.duration_ms,
        traceparent=metadata.traceparent,
        max_emit=MAX_CHUNKS_PER_WINDMILL_BUNDLE,
    )

    if was_late:
        task_logger.debug(f"[Order] Late chunk: {metadata.gcs_uri}")
    if was_buffered:
        task_logger.debug(
            f"[Order] Buffered chunk from future: {metadata.gcs_uri}"
        )
    if elements_to_emit:
        task_logger.debug(f"[Order] Releasing {len(elements_to_emit)} chunks")

    # Update jitter buffer state
    curr_context = replace(
        curr_context,
        expected_next_chunk_start_ms=new_expected_next_ts,
        out_of_order_buffer=new_buffer_elements,
    )

    # Timer management after a clamped or normal drain — three cases:
    #
    # 1. CLAMPED (buffer still has chunks AND drain hit MAX_CHUNKS_PER_WINDMILL_BUNDLE):
    #    The previous drain stopped early to stay under Windmill's 300-second bundle
    #    lease. Setting the timer to `timestamp` (current watermark) causes Dataflow
    #    to fire handle_gap_timeout in the very next bundle, draining the next slice.
    #    Bundles chain this way until the buffer is empty. This is the core of the
    #    Windmill poison-pill fix: instead of one oversized bundle that gets aborted
    #    and replayed (triggered by pipeline restarts, lock-induced slowdowns, or
    #    traffic spikes), we emit in bounded bites and chain to the next bundle.
    #
    # 2. NORMAL GAP (buffer has chunks, drain was not clamped): schedule the
    #    standard gap-timeout deadline so we eventually flush even if some
    #    predecessors never arrive.
    #
    # 3. DRAINED (buffer is now empty): clear the timer to avoid spurious firing.
    clamped = len(elements_to_emit) >= MAX_CHUNKS_PER_WINDMILL_BUNDLE
    if new_buffer_elements:
        if clamped:
            # Immediate self-chaining timer — next bundle drains the next slice.
            out_of_order_timer.set(timestamp)
            curr_context = replace(curr_context, order_timer_active=True)
        elif not curr_context.order_timer_active:
            deadline = timestamp + (
                order_config.out_of_order_timeout_ms
                / float(common_constants.MS_PER_SECOND)
            )
            out_of_order_timer.set(deadline)
            curr_context = replace(curr_context, order_timer_active=True)
    elif not new_buffer_elements and curr_context.order_timer_active:
        out_of_order_timer.clear()
        curr_context = replace(curr_context, order_timer_active=False)

    return elements_to_emit, curr_context, session_changed


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


@beam.typehints.with_input_types(tuple[str, datatypes.ChunkMetadata])
@beam.typehints.with_output_types(tuple[str, datatypes.FlushRequest])
class OrderedStitchAudioFn(beam.DoFn):
    """Stateful Apache Beam DoFn orchestrating out-of-order and stale windowing for continuous audio feeds.

    Key Implementation Rationale (see ARCHITECTURE.md for full exhaustive documentation):
    1. Bounded Windmill Bundles (Self-Chaining): When unrolling massive catch-up/backfill backlogs,
       emissions are clamped to 500 chunks and a timer is re-armed at `timestamp` (current watermark)
       to open fresh worker bundles, avoiding Windmill 300-second commit lease expiry ("poison pills").
    2. Business Logic Invariant Protection: `_evaluate_is_backfill` suppresses application-level sequence
       state overwrites and overlap log spam when historical catch-up slices are redriven.
    3. Dual Stale Timers: Maintains both Event Time (`WATERMARK`) and wall-clock Processing Time (`REAL_TIME`)
       timers to guarantee transmission flush recovery even if a physical radio stream goes silent/offline.

    Delegates core audio segment calculations to entirely decoupled `stitcher_engine.StitcherEngine`.
    """

    # --- State Specs ---

    TRANSMISSION_BUFFER_SPEC = BagStateSpec(
        "transmission_buffer", beam.coders.BytesCoder()
    )
    TRANSMISSION_BUFFER_STATE = beam.DoFn.StateParam(TRANSMISSION_BUFFER_SPEC)

    TRANSMISSION_CONTEXT_SPEC = ReadModifyWriteStateSpec(
        "transmission_context", trans_coders.TransmissionContextCoder()
    )
    TRANSMISSION_CONTEXT_STATE = beam.DoFn.StateParam(TRANSMISSION_CONTEXT_SPEC)

    LAST_START_SPEC = ReadModifyWriteStateSpec(
        "last_start_ms", beam.coders.VarIntCoder()
    )
    LAST_START_MS_STATE = beam.DoFn.StateParam(LAST_START_SPEC)

    # --- Timers ---

    OUT_OF_ORDER_TIMER_SPEC = TimerSpec(
        "out_of_order_timer", beam.TimeDomain.WATERMARK
    )
    OUT_OF_ORDER_TIMER = beam.DoFn.TimerParam(OUT_OF_ORDER_TIMER_SPEC)

    STALE_TIMER_EVENT_SPEC = TimerSpec(
        "stale_timer_event", beam.TimeDomain.WATERMARK
    )
    STALE_TIMER_EVENT_PARAM = beam.DoFn.TimerParam(STALE_TIMER_EVENT_SPEC)

    STALE_TIMER_PROC_SPEC = TimerSpec(
        "stale_timer_proc", beam.TimeDomain.REAL_TIME
    )
    STALE_TIMER_PROC_PARAM = beam.DoFn.TimerParam(STALE_TIMER_PROC_SPEC)

    def __init__(
        self,
        order_config: datatypes.OrderRestorerConfig,
        stitch_config: datatypes.StitchAudioConfig,
    ) -> None:
        self.order_config = order_config
        self.stitch_config = stitch_config

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
        import requests  # noqa: PLC0415
        import requests.adapters  # noqa: PLC0415

        tracing_utils.setup_tracing(service_name="normalization-pipeline")
        # Acquire process-level singletons natively via Beam's Shared handle with a 100-connection pool
        shared_vad = SHARED_RESOURCE_HANDLE.acquire(
            lambda: vad.VoiceActivityDetector(models_dir=vad.MODELS_DIR),
            tag="vad",
        )

        def _create_gcs() -> storage.Client:
            client = storage.Client(project=self.stitch_config.project_id)
            adapter = requests.adapters.HTTPAdapter(
                pool_connections=100, pool_maxsize=100, max_retries=3
            )
            client._http.mount("https://", adapter)  # noqa: SLF001
            return client

        shared_gcs = SHARED_RESOURCE_HANDLE.acquire(
            _create_gcs,
            tag="gcs_pool_100",
        )
        self.engine.processor.vad = shared_vad
        self.engine.processor.gcs_client = shared_gcs
        self.engine.setup()

    def _yield_tagged_outputs(
        self,
        results: Iterable[Any],
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

    @override
    def process(
        self,
        element: tuple[str, datatypes.ChunkMetadata],
        timestamp: Timestamp = beam.DoFn.TimestampParam,  # type: ignore
        transmission_buffer_state: BagRuntimeState = TRANSMISSION_BUFFER_STATE,  # type: ignore
        transmission_context_state: ReadModifyWriteRuntimeState = TRANSMISSION_CONTEXT_STATE,  # type: ignore
        last_start_ms_state: ReadModifyWriteRuntimeState = LAST_START_MS_STATE,  # type: ignore
        out_of_order_timer: RuntimeTimer = OUT_OF_ORDER_TIMER,  # type: ignore
        stale_timer_event: RuntimeTimer = STALE_TIMER_EVENT_PARAM,  # type: ignore
        stale_timer_proc: RuntimeTimer = STALE_TIMER_PROC_PARAM,  # type: ignore
    ) -> Iterator[
        tuple[str, datatypes.FlushRequest] | beam.pvalue.TaggedOutput
    ]:
        """Intercepts chunk arrival, resolves chronological ordering, and delegates to StitcherEngine."""
        feed_id, metadata = element
        traceparent = metadata.traceparent or ""

        results = []
        with tracing_utils.with_tracer_context(
            traceparent, "stitching_process", __name__
        ):
            current_ts_ms = int(
                float(timestamp) * common_constants.MS_PER_SECOND
            )
            curr_context = (
                transmission_context_state.read() or datatypes.IdleFeedState()
            )
            previous_expected_ts = (
                curr_context.expected_next_chunk_start_ms
                if isinstance(curr_context, datatypes.ActiveStitchingState)
                else None
            )

            # Handle chronological sequence buffering
            elements_to_emit, curr_context, session_changed = process_ordering(
                element,
                timestamp,
                curr_context,
                out_of_order_timer,
                self.order_config,
            )

            task_logger = _get_task_logger(
                feed_id, curr_context.session_id, "transcription-stitcher"
            )
            task_logger.debug(f"[Process] Processing chunk {metadata.gcs_uri}")

            if session_changed:
                transmission_buffer_state.clear()
                stale_timer_event.clear()
                stale_timer_proc.clear()
                curr_context = replace(
                    curr_context, prior_audio_tail=None, sample_rate=None
                )

            # Commit initial sequence context updates
            _write_transmission_context(
                transmission_context_state, curr_context
            )

            # Delegate chunk elements to the execution engine
            if elements_to_emit:
                timer_manager = StaleTimerManager(
                    stale_timer_event, stale_timer_proc, self.stitch_config
                )

                is_backfill = _evaluate_is_backfill(
                    current_ts_ms,
                    self.stitch_config.backfill_lateness_threshold_ms,
                )

                for chunk in elements_to_emit:
                    # Fetch current state context
                    curr_context = (
                        transmission_context_state.read()
                        or datatypes.IdleFeedState()
                    )
                    if isinstance(curr_context, datatypes.IdleFeedState):
                        curr_context = datatypes.ActiveStitchingState(
                            session_id=metadata.session_id,
                            feed_metadata=metadata.feed_metadata,
                            out_of_order_buffer=curr_context.out_of_order_buffer,
                            order_timer_active=curr_context.order_timer_active,
                            traceparent=metadata.traceparent,
                        )
                    outputs, next_expected_ts = (
                        self.engine.process_ordering_chunk(
                            chunk=chunk,
                            feed_id=feed_id,
                            curr_context=curr_context,
                            transmission_context_state=transmission_context_state,
                            transmission_buffer_state=transmission_buffer_state,
                            last_start_ms_state=last_start_ms_state,
                            timer_manager=timer_manager,
                            previous_expected_ts=previous_expected_ts,
                            is_backfill=is_backfill,
                        )
                    )
                    results.extend(outputs)
                    previous_expected_ts = next_expected_ts

        yield from self._yield_tagged_outputs(results)

    @on_timer(OUT_OF_ORDER_TIMER_SPEC)
    def handle_gap_timeout(
        self,
        feed_id: str = beam.DoFn.KeyParam,  # type: ignore
        transmission_buffer_state: BagRuntimeState = TRANSMISSION_BUFFER_STATE,  # type: ignore
        transmission_context_state: ReadModifyWriteRuntimeState = TRANSMISSION_CONTEXT_STATE,  # type: ignore
        last_start_ms_state: ReadModifyWriteRuntimeState = LAST_START_MS_STATE,  # type: ignore
        stale_timer_event: RuntimeTimer = STALE_TIMER_EVENT_PARAM,  # type: ignore
        stale_timer_proc: RuntimeTimer = STALE_TIMER_PROC_PARAM,  # type: ignore
        timestamp: Timestamp = beam.DoFn.TimestampParam,  # type: ignore
        out_of_order_timer: RuntimeTimer = OUT_OF_ORDER_TIMER,  # type: ignore
    ) -> Iterator[
        tuple[str, datatypes.FlushRequest] | beam.pvalue.TaggedOutput
    ]:
        """Handles the gap timeout by advancing the expected sequence."""
        curr_context = (
            transmission_context_state.read() or datatypes.IdleFeedState()
        )
        if isinstance(curr_context, datatypes.IdleFeedState):
            return
        traceparent = curr_context.traceparent or ""
        active_session_id = curr_context.session_id
        active_feed_metadata = curr_context.feed_metadata
        active_traceparent = curr_context.traceparent

        results = []
        with tracing_utils.with_tracer_context(
            traceparent, "handle_audio_gap", __name__
        ):
            curr_context = replace(curr_context, order_timer_active=False)
            _write_transmission_context(
                transmission_context_state, curr_context
            )

            buffer_elements = curr_context.out_of_order_buffer
            if buffer_elements:
                sorted_elements = sorted(
                    buffer_elements, key=lambda x: x.timestamp_ms
                )
                new_expected = sorted_elements[0].timestamp_ms

                logger.warning(
                    f"[{feed_id}] Gap timeout! Advancing expected from {curr_context.expected_next_chunk_start_ms} to {new_expected}."
                )

                curr_context = replace(
                    curr_context,
                    expected_next_chunk_start_ms=new_expected,
                    missing_prior_context=True,
                )
                _write_transmission_context(
                    transmission_context_state, curr_context
                )

                seq_buf = sequence_buffer.SequenceBuffer(self.order_config)

                new_expected_next_ts, new_buffer_elements, elements_to_emit = (
                    seq_buf.drain_ready_elements(
                        expected_next_ts=new_expected,
                        buffer_elements=buffer_elements,
                        epsilon_ms=trans_constants.DEFAULT_FLOAT_TOLERANCE_MS,
                        max_emit=MAX_CHUNKS_PER_WINDMILL_BUNDLE,
                    )
                )

                clamped = (
                    len(elements_to_emit) >= MAX_CHUNKS_PER_WINDMILL_BUNDLE
                )
                if clamped and new_buffer_elements:
                    # The drain was capped at MAX_CHUNKS_PER_WINDMILL_BUNDLE to
                    # stay under Windmill's 300-second bundle lease. Entering
                    # this handler clears order_timer_active, so the timer is
                    # no longer set. Re-arming it at `timestamp` (this bundle's
                    # firing time) schedules an immediate Windmill callback so
                    # Dataflow opens a fresh bundle to drain the next slice.
                    # Self-chaining continues until new_buffer_elements is empty.
                    out_of_order_timer.set(timestamp)
                    curr_context = replace(
                        curr_context, order_timer_active=True
                    )

                curr_context = replace(
                    curr_context,
                    expected_next_chunk_start_ms=new_expected_next_ts,
                    out_of_order_buffer=new_buffer_elements,
                )
                _write_transmission_context(
                    transmission_context_state, curr_context
                )

                # Handle ready elements
                if elements_to_emit:
                    timer_manager = StaleTimerManager(
                        stale_timer_event, stale_timer_proc, self.stitch_config
                    )

                    # Assume backfill in timeout!
                    is_backfill = True
                    previous_expected_ts = new_expected

                    for chunk in elements_to_emit:
                        curr_context = (
                            transmission_context_state.read()
                            or datatypes.IdleFeedState()
                        )
                        if isinstance(curr_context, datatypes.IdleFeedState):
                            curr_context = datatypes.ActiveStitchingState(
                                session_id=active_session_id,
                                feed_metadata=active_feed_metadata,
                                out_of_order_buffer=curr_context.out_of_order_buffer,
                                order_timer_active=curr_context.order_timer_active,
                                traceparent=active_traceparent,
                            )
                        outputs, next_expected_ts = (
                            self.engine.process_ordering_chunk(
                                chunk=chunk,
                                feed_id=feed_id,
                                curr_context=curr_context,
                                transmission_context_state=transmission_context_state,
                                transmission_buffer_state=transmission_buffer_state,
                                last_start_ms_state=last_start_ms_state,
                                timer_manager=timer_manager,
                                previous_expected_ts=previous_expected_ts,
                                is_backfill=is_backfill,
                            )
                        )
                        results.extend(outputs)
                        previous_expected_ts = next_expected_ts

        yield from self._yield_tagged_outputs(results)

    @on_timer(STALE_TIMER_EVENT_SPEC)
    def handle_stale_transmission_event(
        self,
        key: str = beam.DoFn.KeyParam,  # type: ignore
        transmission_buffer: BagRuntimeState = TRANSMISSION_BUFFER_STATE,  # type: ignore
        transmission_context: ReadModifyWriteRuntimeState = TRANSMISSION_CONTEXT_STATE,  # type: ignore
        last_start_ms_state: ReadModifyWriteRuntimeState = LAST_START_MS_STATE,  # type: ignore
        stale_timer_event: RuntimeTimer = STALE_TIMER_EVENT_PARAM,  # type: ignore
        stale_timer_proc: RuntimeTimer = STALE_TIMER_PROC_PARAM,  # type: ignore
    ) -> Iterator[
        tuple[str, datatypes.FlushRequest] | beam.pvalue.TaggedOutput
    ]:
        """Watermark crossed stale duration, delegate flush to StitcherEngine."""
        timer_manager = StaleTimerManager(
            stale_timer_event, stale_timer_proc, self.stitch_config
        )
        yield from self._yield_tagged_outputs(
            self.engine.handle_stale_transmission(
                key,
                transmission_buffer,
                transmission_context,
                last_start_ms_state,
                timer_manager,
            )
        )

    @on_timer(STALE_TIMER_PROC_SPEC)
    def handle_stale_transmission_proc(
        self,
        key: str = beam.DoFn.KeyParam,  # type: ignore
        transmission_buffer: BagRuntimeState = TRANSMISSION_BUFFER_STATE,  # type: ignore
        transmission_context: ReadModifyWriteRuntimeState = TRANSMISSION_CONTEXT_STATE,  # type: ignore
        last_start_ms_state: ReadModifyWriteRuntimeState = LAST_START_MS_STATE,  # type: ignore
        stale_timer_event: RuntimeTimer = STALE_TIMER_EVENT_PARAM,  # type: ignore
        stale_timer_proc: RuntimeTimer = STALE_TIMER_PROC_PARAM,  # type: ignore
    ) -> Iterator[
        tuple[str, datatypes.FlushRequest] | beam.pvalue.TaggedOutput
    ]:
        """Wall-clock crossed stale duration, delegate flush to StitcherEngine."""
        timer_manager = StaleTimerManager(
            stale_timer_event, stale_timer_proc, self.stitch_config
        )
        yield from self._yield_tagged_outputs(
            self.engine.handle_stale_transmission(
                key,
                transmission_buffer,
                transmission_context,
                last_start_ms_state,
                timer_manager,
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
