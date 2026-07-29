"""Core Stitcher Engine executing non-Beam stitching, VAD analysis, and flushes.

This module defines the pure-Python domain logic for audio segment stitching
completely decoupled from Apache Beam runtime boundaries. It exposes:
- StitcherEngine: Coordinates GCS audio downloads, lazy resource initialization,
  Voice Activity Detection (VAD) evaluation, and FSM transition tracking.
- StaleTimerManager: A context helper encapsulating event-time and wall-clock
  timer setups for high-level stateful flushes.

By abstracting all non-Beam logic into this stateless execution engine, we
enforce 100% pickling/serialization safety for Dataflow workers and achieve
straightforward, light-speed unit testing capabilities.
"""

import logging as std_logging
from collections.abc import Callable, Iterator
from dataclasses import replace
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import concurrent.futures

import numpy as np
from apache_beam.metrics import Metrics
from google.cloud import storage
from opentelemetry import context as otel_context

from backend.pipeline.common import constants as common_constants
from backend.pipeline.common.log_helper import get_task_logger
from backend.pipeline.segmentation import constants as trans_constants
from backend.pipeline.segmentation import datatypes, log_helper
from backend.pipeline.segmentation import utils as trans_utils
from backend.pipeline.segmentation.audio import processor as audio_processor
from backend.pipeline.segmentation.audio import vad
from backend.pipeline.segmentation.datatypes import (
    AudioFutureMap,
    AudioSignal,
)
from backend.pipeline.segmentation.state import stitcher_state

# WARNING: Do NOT remove or bypass setup_logging().
# It explicitly configures structured log propagation for the
# Dataflow worker harness. Removing this will cause all worker logs
# to be rendered as DEBUG severity in Cloud Logging.
log_helper.setup_logging()

logger = get_task_logger(
    __name__, {"system": "transcription", "component": "stitcher-engine"}
)


def _get_task_logger(
    feed_id: str, session_id: str | None, component: str
) -> std_logging.LoggerAdapter[std_logging.Logger]:
    """Contextual logger creation helper."""
    return get_task_logger(
        __name__,
        {
            "system": "transcription",
            "component": component,
            "feed_id": feed_id,
            "session_id": session_id or "none",
        },
    )


class StitcherEngine:
    """Pure Python stitching engine completely decoupled from Apache Beam watermark timers.

    Encapsulates GCS downloads, VAD execution, FSM state tracking, and chunk flushes.
    """

    def __init__(
        self,
        stitch_config: datatypes.StitchAudioConfig,
        order_config: datatypes.OrderRestorerConfig,
        vad_config: str = "{}",
        vad_instance: vad.VoiceActivityDetector | None = None,
        gcs_client_instance: storage.Client | None = None,
        vad_factory: Callable[[str], vad.VoiceActivityDetector] | None = None,
        gcs_factory: Callable[[], storage.Client] | None = None,
    ) -> None:
        self.stitch_config = stitch_config
        self.order_config = order_config
        self.vad_config = vad_config

        # Incoming chunk counters
        self.chunks_received = Metrics.counter(
            self.__class__, "chunks_received"
        )

        # VAD evaluated chunk counters
        self.vad_speech_chunks = Metrics.counter(
            self.__class__, "vad_speech_chunks"
        )
        self.vad_silence_chunks = Metrics.counter(
            self.__class__, "vad_silence_chunks"
        )

        # Total speech utterances/segments count
        self.speech_segments_count = Metrics.counter(
            self.__class__, "speech_segments_count"
        )

        # Pipeline health & flushes
        self.stale_flushes = Metrics.counter(self.__class__, "stale_flushes")
        self.oversized_audio_chunks = Metrics.counter(
            self.__class__, "oversized_audio_chunks"
        )

        # Instantiate the stateless AudioProcessor
        self.processor = audio_processor.SegmentationAudioProcessor(
            vad_config=vad_config,
            vad_instance=vad_instance,
            gcs_client_instance=gcs_client_instance,
            vad_factory=vad_factory,
            gcs_factory=gcs_factory,
        )
        self.executor: concurrent.futures.ThreadPoolExecutor | None = None

    def setup(self) -> None:
        """Initializes the processor ONNXRuntime inference sessions and Numba compilation."""
        self.processor.setup()

    def submit_single_prefetch(
        self,
        uri: str,
        task_logger: Any,
    ) -> Any | None:
        """Submits a single GCS download and decoding task to the background thread pool."""
        if not self.executor:
            return None

        # Apply global backpressure: if the shared executor's pending queue size is
        # above our threshold, bypass prefetching to avoid pool starvation.
        try:
            qsize = self.executor._work_queue.qsize()  # noqa: SLF001

            if qsize >= trans_constants.MAX_PREFETCH_QUEUE_DEPTH:
                task_logger.debug(
                    f"[Prefetch] Shared executor queue size is {qsize} (threshold: "
                    f"{trans_constants.MAX_PREFETCH_QUEUE_DEPTH}), bypassing prefetch for {uri}"
                )
                return None
        except (AttributeError, TypeError):
            # Fallback if executor does not have _work_queue or qsize is not supported
            pass

        parent_context = otel_context.get_current()

        def _fetch_one() -> AudioSignal:
            token = otel_context.attach(parent_context)
            try:
                task_logger.debug(
                    f"[Prefetch] Background fetch starting for {uri}"
                )
                res = self.processor.fetch_and_decode_audio(uri)
                task_logger.debug(
                    f"[Prefetch] Background fetch completed for {uri}"
                )
            except Exception as e:
                task_logger.warning(
                    f"[Prefetch] Failed background fetch for {uri}: {e}"
                )
                raise
            else:
                return res
            finally:
                otel_context.detach(token)

        return self.executor.submit(_fetch_one)

    def prefetch_audio_futures(
        self,
        chunks: list[datatypes.BufferedChunk],
        task_logger: Any,
    ) -> AudioFutureMap | None:
        """Submits GCS download and decoding tasks to the shared background thread pool for all chunks in the bundle.

        Args:
            chunks: List of buffered audio chunks to be processed in the current bundle.
            task_logger: Contextual logger instance for recording task execution details.

        Returns:
            A mapping of GCS URI to future AudioSignal results, or None if pre-fetching is bypassed.
        """
        if not self.executor or len(chunks) <= 1:
            return None

        futures: AudioFutureMap = {}
        for chunk in chunks:
            if chunk.gcs_uri not in futures:
                future = self.submit_single_prefetch(chunk.gcs_uri, task_logger)
                if future is not None:
                    futures[chunk.gcs_uri] = future
        return futures

    def process_ordering_chunk(
        self,
        chunk: datatypes.BufferedChunk,
        feed_id: str,
        curr_context: datatypes.ActiveStitchingState,
        last_start_ms: int | None,
        timer_manager: Any,
        previous_expected_ts: int | None,
        *,
        is_backfill: bool,
        clear_buffer: bool = False,
        prefetched_futures: AudioFutureMap | None = None,
    ) -> datatypes.StitcherChunkResult:
        """Stitches a single element after session validation and OOO buffer restoral.

        Args:
            chunk: The buffered chunk payload.
            feed_id: Unique identifier of the active feed.
            curr_context: The current transmission sequence context.
            last_start_ms: In-memory timestamp of the last transmission start.
            timer_manager: Contextual timer scheduler interface.
            previous_expected_ts: The expected next sequence timestamp baseline.
            is_backfill: True if the chunk falls behind the real-time watermark.
            clear_buffer: If True, ignore existing state and treat the buffer as empty.
            prefetched_futures: Optional mapping of GCS URI to background download futures.

        Returns:
            A StitcherChunkResult containing emitted outputs, next context, and timestamps.
        """
        task_logger = _get_task_logger(
            feed_id, curr_context.session_id, "transcription-stitcher"
        )

        # Instantiate our state machine on top of our stitch settings
        state_machine = stitcher_state.AudioStitchingStateMachine(
            self.stitch_config
        )

        return self._process_single_stitch_chunk(
            chunk=chunk,
            feed_id=feed_id,
            curr_context=curr_context,
            last_start_ms=last_start_ms,
            timer_manager=timer_manager,
            state_machine=state_machine,
            previous_expected_ts=previous_expected_ts,
            task_logger=task_logger,
            is_backfill=is_backfill,
            clear_buffer=clear_buffer,
            prefetched_futures=prefetched_futures,
        )

    def handle_stale_transmission(
        self,
        key: str,
        transmission_context: Any,
        last_start_ms_state: Any,
        timer_manager: Any,
        out_of_order_buffer_state: Any = None,
    ) -> Iterator[
        tuple[str, datatypes.FlushRequest] | tuple[str, dict[str, Any]]
    ]:
        """Orchestrates stale flushes when watermarks cross the timeout threshold.

        Args:
            key: Unique key of the active feed partition.
            transmission_context: Runtime Beam state mapping for contexts.
            last_start_ms_state: Runtime Beam state mapping for last start time.
            timer_manager: Contextual timer scheduler interface.
            out_of_order_buffer_state: Runtime Beam state mapping for out-of-order buffer.

        Yields:
            Emitted elements (FlushRequest or TaggedOutput DLQ).
        """
        feed_id = key
        curr_ctx = transmission_context.read() or datatypes.IdleFeedState()
        if isinstance(curr_ctx, datatypes.IdleFeedState):
            return

        session_id = curr_ctx.session_id

        task_logger = _get_task_logger(
            feed_id, session_id, "transcription-stitcher"
        )

        start_time_ms = curr_ctx.stale_start_time_ms
        processed_uris = curr_ctx.contributing_audio_uris

        if (
            start_time_ms is not None
            and curr_ctx.buffer_start_time_ms is not None
        ):
            try:
                time_range = datatypes.TimeRange(
                    start_ms=curr_ctx.buffer_start_time_ms,
                    end_ms=curr_ctx.buffer_start_time_ms
                    + curr_ctx.buffer_duration_ms,
                )
                segment_id = trans_utils.generate_segment_id(
                    curr_ctx.session_id,
                    time_range,
                    curr_ctx.buffer_duration_ms,
                )

                task_logger.info(
                    f"[Stale Timer] Fired for session {session_id}. Emitting buffered contents {segment_id}."
                )
                self.stale_flushes.inc()

                audio_classification = (
                    datatypes.AudioClassification.AUDIO_CLASSIFICATION_SPEECH
                    if curr_ctx.speech_segments
                    else datatypes.AudioClassification.AUDIO_CLASSIFICATION_OTHER
                )
                is_speech = trans_utils.is_speech_classification(
                    audio_classification
                )

                yield (
                    feed_id,
                    datatypes.FlushRequest(
                        feed_id=feed_id,
                        session_id=curr_ctx.session_id,
                        contributing_audio_uris=processed_uris,
                        contributing_chunks=list(curr_ctx.contributing_chunks),
                        time_range=time_range,
                        missing_prior_context=curr_ctx.missing_prior_context
                        if is_speech
                        else False,
                        missing_post_context=is_speech,
                        start_audio_offset_ms=max(
                            0, curr_ctx.start_audio_offset_ms or 0
                        ),
                        end_audio_offset_ms=trans_utils.compute_end_audio_offset_ms(
                            time_range.end_ms,
                            curr_ctx.contributing_chunks,
                            curr_ctx.buffer_start_time_ms,
                        ),
                        speech_segments=curr_ctx.speech_segments,
                        segment_id=segment_id,
                        feed_metadata=curr_ctx.feed_metadata,
                        sample_rate=curr_ctx.sample_rate
                        or common_constants.SAMPLE_RATE_HZ,
                        traceparent=curr_ctx.traceparent,
                        baggage=curr_ctx.baggage,
                        audio_classification=audio_classification,
                    ),
                )
            except Exception as e:
                if not self.stitch_config.route_to_dlq:
                    raise
                task_logger.exception(
                    "Error yielding stale buffer for feed %s", feed_id
                )
                yield (
                    trans_constants.DEAD_LETTER_QUEUE_TAG,
                    {"error": str(e), "feed_id": feed_id, "stale_flush": True},
                )

        # Clear state context cleanly
        transmission_context.clear()
        last_start_ms_state.clear()
        if out_of_order_buffer_state:
            out_of_order_buffer_state.clear()
        timer_manager.clear()

    def _apply_flush_action(
        self,
        action: datatypes.FlushAction,
        last_start_ms: int | None,
        session_id: str,
        curr_context: datatypes.ActiveStitchingState,
        chunk_data: datatypes.AudioChunkData | None = None,
        *,
        is_backfill: bool = False,
    ) -> tuple[list[tuple[str, datatypes.FlushRequest]], int | None]:
        """Emits a structured FlushRequest payload downstream and resets internal state fields."""
        task_logger = _get_task_logger(
            action.feed_id, session_id, "transcription-stitcher"
        )

        processed_uris = action.contributing_audio_uris or list(
            curr_context.contributing_audio_uris
        )

        contributing_chunks = action.contributing_chunks or list(
            curr_context.contributing_chunks
        )

        if processed_uris or contributing_chunks:
            segment_id = trans_utils.generate_segment_id(
                session_id,
                action.time_range,
                trans_utils.get_duration_ms(action.time_range),
            )
            task_logger.info(
                f"[Flush] Emitting segment {segment_id} with {len(processed_uris)} chunks"
            )

            # In backfill/catch-up mode (e.g., pipeline recovering from maintenance or outage),
            # we skip overlap validations and avoid updating `last_start_ms`.
            # This prevents older backlogged audio timestamps from corrupting the sequence tracking
            # of the active mainline stream, which would otherwise trigger false overlap warnings
            # and redundant segment outputs once the pipeline catches up to real-time.
            if action.clear_state and not is_backfill:
                current_start_ms = action.time_range.start_ms

                if (
                    last_start_ms is not None
                    and abs(current_start_ms - last_start_ms)
                    < trans_constants.OVERLAPPING_TRANSMISSION_TOLERANCE_MS
                ):
                    task_logger.warning(
                        f"Potential growing/overlapping transmission detected! "
                        f"Starts at nearly the same time ({current_start_ms}ms) as previous ({last_start_ms}ms)."
                    )

                last_start_ms = current_start_ms

            return [
                (
                    action.feed_id,
                    datatypes.FlushRequest(
                        feed_id=action.feed_id,
                        session_id=session_id,
                        contributing_audio_uris=processed_uris,
                        contributing_chunks=contributing_chunks,
                        time_range=action.time_range,
                        missing_prior_context=action.missing_prior_context,
                        missing_post_context=action.missing_post_context,
                        start_audio_offset_ms=action.start_audio_offset_ms,
                        end_audio_offset_ms=action.end_audio_offset_ms,
                        speech_segments=action.speech_segments,
                        segment_id=segment_id,
                        feed_metadata=curr_context.feed_metadata,
                        sample_rate=curr_context.sample_rate
                        or (chunk_data.sample_rate if chunk_data else None)
                        or common_constants.SAMPLE_RATE_HZ,
                        traceparent=action.traceparent,
                        baggage=action.baggage,
                        audio_classification=datatypes.AudioClassification(
                            action.audio_classification
                        ),
                    ),
                )
            ], last_start_ms
        return [], last_start_ms

    def _record_chunk_evaluation_metrics(
        self, chunk_data: datatypes.AudioChunkData
    ) -> None:
        """Records VAD evaluation outcomes and chunk volume."""
        self.chunks_received.inc()
        if chunk_data.speech_segments:
            self.vad_speech_chunks.inc()
            self.speech_segments_count.inc(len(chunk_data.speech_segments))
        else:
            self.vad_silence_chunks.inc()

    def _process_single_stitch_chunk(
        self,
        chunk: datatypes.BufferedChunk,
        feed_id: str,
        curr_context: datatypes.ActiveStitchingState,
        last_start_ms: int | None,
        timer_manager: Any,
        state_machine: stitcher_state.AudioStitchingStateMachine,
        previous_expected_ts: int | None,
        task_logger: Any,
        *,
        is_backfill: bool,
        clear_buffer: bool = False,
        prefetched_futures: AudioFutureMap | None = None,
    ) -> datatypes.StitcherChunkResult:
        """Downloads and stitches a single chunk through the state machine.

        Args:
            chunk: The buffered chunk payload.
            feed_id: Unique identifier of the active feed.
            curr_context: The current transmission sequence context.
            last_start_ms: In-memory timestamp of the last transmission start.
            timer_manager: Contextual timer scheduler interface.
            state_machine: Audio stitching FSM logic.
            previous_expected_ts: The expected next sequence timestamp baseline.
            task_logger: Contextual logger instance.
            is_backfill: True if the chunk falls behind the real-time watermark.
            clear_buffer: If True, ignore existing state and treat the buffer as empty.
            prefetched_futures: Optional mapping of GCS URI to background download futures.

        Returns:
            A StitcherChunkResult containing emitted outputs, next context, and timestamps.
        """
        from backend.pipeline.common.tracing_utils import (  # noqa: PLC0415
            get_current_traceparent,
            with_tracer_context,
        )

        trace_attrs: dict[str, str] = {}
        if chunk.traceparent:
            trace_attrs["traceparent"] = chunk.traceparent
        baggage_val = chunk.baggage
        if baggage_val:
            trace_attrs["baggage"] = baggage_val

        if curr_context.session_id is None:
            msg = "Session ID cannot be None in _process_single_stitch_chunk"
            raise ValueError(msg)

        feed_metadata = curr_context.feed_metadata
        if feed_metadata is None:
            msg = "feed_metadata cannot be None in _process_single_stitch_chunk"
            raise ValueError(msg)

        try:
            with with_tracer_context(
                trace_attrs,
                "stitching_single_chunk",
                "backend.pipeline.segmentation.transforms.stateful",
            ):
                if clear_buffer:
                    previous_expected_ts = None

                if (
                    previous_expected_ts is not None
                    and chunk.timestamp_ms > previous_expected_ts
                ):
                    # watermark contiguous gap, clear prior tail primordial state
                    curr_context = replace(curr_context, prior_audio_tail=None)

                prefetched_audio = None
                if (
                    prefetched_futures is not None
                    and chunk.gcs_uri in prefetched_futures
                ):
                    try:
                        prefetched_audio = prefetched_futures[
                            chunk.gcs_uri
                        ].result()
                    except Exception as e:
                        task_logger.warning(
                            f"[Prefetch Fallback] Background fetch failed for {chunk.gcs_uri}: {e}. Retrying synchronously."
                        )

                # 1. Download audio and run speech detection
                task_logger.debug(
                    f"[Download] Downloading audio for {chunk.gcs_uri}"
                )
                chunk_data = self.processor.download_audio_and_detect(
                    chunk.gcs_uri,
                    chunk.timestamp_ms,
                    prior_audio=curr_context.prior_audio_tail,
                    prefetched_audio=prefetched_audio,
                )
                self._record_chunk_evaluation_metrics(chunk_data)
                task_logger.debug(
                    f"[Download] Downloaded audio for {chunk.gcs_uri}"
                )

                payload = datatypes.DownloadedChunkPayload(
                    chunk.gcs_uri,
                    chunk_data,
                    curr_context.session_id or "unknown",
                )

                # 3. Initialize State Machine context
                # Load contributing chunks. We implement a strict backward-compatibility
                # fallback for rolling upgrades in production:
                # - Old checkpoints (ActiveStitchingStateProto) serialized by un-upgraded workers
                #   only contain the legacy `contributing_audio_uris` string list.
                # - When an upgraded worker restores this state, it detects the empty `contributing_chunks`
                #   and automatically reconstructs `BufferedChunk` objects using a fallback timestamp_ms of 0.
                # - This prevents any deserialization or runtime failures during deployment and allows
                #   active sessions to be drained seamlessly.
                # NOTE: This fallback block can be safely retired in Phase 2 once this release is 100% stable.
                contributing_chunks = list(curr_context.contributing_chunks)
                if (
                    not contributing_chunks
                    and curr_context.contributing_audio_uris
                ):
                    contributing_chunks = [
                        datatypes.BufferedChunk(gcs_uri=uri, timestamp_ms=0)
                        for uri in curr_context.contributing_audio_uris
                    ]

                ctx = datatypes.StitcherContext(
                    feed_id=feed_id,
                    current_gcs_uri=chunk.gcs_uri,
                    session_id=curr_context.session_id,
                    contributing_chunks=contributing_chunks,
                    file_start_ms=chunk.timestamp_ms,
                    last_segment_end_time_ms=curr_context.last_end_time_ms,
                    transmission_start_time_ms=curr_context.stale_start_time_ms,
                    buffer_start_time_ms=curr_context.buffer_start_time_ms,
                    missing_prior_context=curr_context.missing_prior_context,
                    expected_next_chunk_start_ms=previous_expected_ts,
                    start_audio_offset_ms=curr_context.start_audio_offset_ms,
                    end_audio_offset_ms=None,
                    buffer_duration_ms=curr_context.buffer_duration_ms,
                    speech_segments=curr_context.speech_segments.copy(),
                    traceparent=chunk.traceparent
                    or curr_context.traceparent
                    or get_current_traceparent(),
                    baggage=chunk.baggage or curr_context.baggage,
                    prior_audio_tail=curr_context.prior_audio_tail,
                )

                actions = state_machine.process_chunk(payload.chunk_data, ctx)

                # 4. Apply outputs
                new_context: datatypes.TransmissionContext
                outputs, new_context, new_last_start_ms = (
                    self._apply_stitcher_actions(
                        actions=actions,
                        curr_context=curr_context,
                        last_start_ms=last_start_ms,
                        timer_manager=timer_manager,
                        chunk_data=chunk_data,
                        ctx=ctx,
                        is_backfill=is_backfill,
                        clear_buffer=clear_buffer,
                    )
                )

                return datatypes.StitcherChunkResult(
                    outputs=outputs,
                    next_context=new_context,
                    next_expected_ts=chunk.timestamp_ms
                    + chunk_data.duration_ms,
                    next_last_start_ms=new_last_start_ms,
                )

        except Exception as e:
            if "exceeds maximum limit" in str(e):
                self.oversized_audio_chunks.inc()

            if not self.stitch_config.route_to_dlq:
                raise
            task_logger.exception(
                "Error processing chunk %s for feed %s",
                chunk.gcs_uri,
                feed_id,
            )
            dlq_payload: datatypes.StitcherDlqPayload = {
                "feed_id": feed_id,
                "gcs_uri": chunk.gcs_uri,
                "session_id": curr_context.session_id or "unknown",
                "error_message": str(e),
                "traceparent": curr_context.traceparent,
            }
            fallback_expected = previous_expected_ts or (
                chunk.timestamp_ms + common_constants.MS_PER_SECOND
            )
            return datatypes.StitcherChunkResult(
                outputs=[(trans_constants.DEAD_LETTER_QUEUE_TAG, dlq_payload)],
                next_context=curr_context,
                next_expected_ts=fallback_expected,
                next_last_start_ms=last_start_ms,
            )

    def _apply_stitcher_actions(
        self,
        actions: list[datatypes.StateMachineAction],
        curr_context: datatypes.ActiveStitchingState,
        last_start_ms: int | None,
        timer_manager: Any,
        chunk_data: datatypes.AudioChunkData,
        ctx: datatypes.StitcherContext,
        *,
        is_backfill: bool,
        clear_buffer: bool = False,
    ) -> tuple[
        list[tuple[str, Any]], datatypes.TransmissionContext, int | None
    ]:
        """Processes and executes state transitions generated by the stitching state machine."""
        chunk_outputs = []
        # Stash original active context properties before any actions reassign or clear them.
        # This ensures that sequential flushes within a single chunk processing run (such as split segmented chunks)
        # always resolve native session metadata without throwing TypeErrors/AttributeErrors.
        active_context = curr_context
        active_session_id = curr_context.session_id
        active_feed_metadata = curr_context.feed_metadata
        active_sample_rate = curr_context.sample_rate

        new_context: datatypes.TransmissionContext = curr_context

        for action in actions:
            match action:
                case datatypes.FlushAction():
                    outputs, last_start_ms = self._apply_flush_action(
                        action,
                        last_start_ms,
                        active_session_id or "unknown",
                        active_context,
                        chunk_data,
                        is_backfill=is_backfill,
                    )
                    chunk_outputs.extend(outputs)
                    if action.clear_state:
                        # Force-transition the context back to IdleFeedState to prevent
                        # Trace Context Hijacking and session ID leaks into subsequent independent chunks.
                        new_context = datatypes.IdleFeedState(
                            out_of_order_buffer=list(
                                new_context.out_of_order_buffer
                            ),
                            order_timer_active=new_context.order_timer_active,
                        )
                case datatypes.AppendBufferAction():
                    pass
                case datatypes.UpdateStateAction():
                    # If we flushed/cleared active state but the same chunk immediately starts a new active
                    # transmission window (e.g. during split-segment forced flushes), we must transition the
                    # IdleFeedState back to ActiveStitchingState using the original stashed details before writing fields.
                    if (
                        isinstance(new_context, datatypes.IdleFeedState)
                        and ctx.transmission_start_time_ms is not None
                    ):
                        new_context = datatypes.ActiveStitchingState(
                            session_id=ctx.session_id or "unknown",
                            feed_metadata=active_feed_metadata,
                            out_of_order_buffer=list(
                                new_context.out_of_order_buffer
                            ),
                            order_timer_active=new_context.order_timer_active,
                            traceparent=ctx.traceparent,
                            baggage=ctx.baggage,
                            sample_rate=active_sample_rate
                            or chunk_data.sample_rate,
                        )

                    if isinstance(new_context, datatypes.ActiveStitchingState):
                        # Lookback Priming Cache:
                        # We cache the post-denoiser preprocessed audio tail (converted to int16 PCM)
                        # to prime the VAD LSTM lookback buffer in the next chunk.
                        # Because this tail is already denoised, it bypasses the UL-UNAS denoiser
                        # input in the next chunk, permanently eliminating the risk of denoiser
                        # deafening on prior channel static. As a result, we no longer need the
                        # legacy `ended_in_speech` discard rule, guaranteeing continuous lookback
                        # warmup for every chunk without cold-start onset clipping.
                        priming_samples_16k = int(
                            trans_constants.VAD_DEFAULT_PRIMING_SEC * 16000
                        )
                        priming_samples_native = int(
                            trans_constants.VAD_DEFAULT_PRIMING_SEC
                            * chunk_data.sample_rate
                        )
                        if (
                            chunk_data.denoised_audio is not None
                            and len(chunk_data.denoised_audio) > 0
                        ):
                            denoised_tail = chunk_data.denoised_audio[
                                -priming_samples_16k:
                            ]
                            prior_tail = (
                                np.clip(
                                    denoised_tail * 32767.0,
                                    -32768.0,
                                    32767.0,
                                )
                                .astype(np.int16)
                                .tobytes()
                            )
                        elif len(chunk_data.audio) > 0:
                            prior_tail = chunk_data.audio[
                                -priming_samples_native:
                            ].tobytes()
                        else:
                            prior_tail = None
                        new_context = replace(
                            new_context,
                            contributing_audio_uris=ctx.contributing_audio_uris,
                            contributing_chunks=ctx.contributing_chunks,
                            last_end_time_ms=ctx.last_segment_end_time_ms,
                            stale_start_time_ms=ctx.transmission_start_time_ms,
                            buffer_start_time_ms=ctx.buffer_start_time_ms,
                            missing_prior_context=ctx.missing_prior_context,
                            start_audio_offset_ms=ctx.start_audio_offset_ms,
                            buffer_duration_ms=ctx.buffer_duration_ms,
                            speech_segments=ctx.speech_segments,
                            prior_audio_tail=prior_tail,
                            sample_rate=chunk_data.sample_rate,
                            traceparent=ctx.traceparent,
                            baggage=ctx.baggage,
                        )
                case datatypes.ScheduleStaleTimerAction():
                    timer_manager.schedule(
                        deadline_ms=action.deadline_ms,
                        is_backfill=is_backfill,
                    )
        # Commit final context updates (no buffer updates needed)

        return chunk_outputs, new_context, last_start_ms
