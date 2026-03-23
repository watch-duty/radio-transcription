"""Stateful Apache Beam DoFns for chronological sequence timeline logic and stitch generation."""

import logging
import time
from collections.abc import Callable, Generator
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
from apache_beam.utils.timestamp import Timestamp

from backend.pipeline.common.constants import MS_PER_SECOND
from backend.pipeline.transcription.audio_processor import AudioProcessor
from backend.pipeline.transcription.constants import (
    DEAD_LETTER_QUEUE_TAG,
)
from backend.pipeline.transcription.datatypes import (
    AppendBufferAction,
    AudioChunkData,
    DropAction,
    FlushAction,
    FlushRequest,
    ScheduleStaleTimerAction,
    StateMachineAction,
    StitchAudioConfig,
    StitcherContext,
    TimeRange,
    TranscribeAudioConfig,
    TranscriptionResult,
    TransmissionContext,
    UpdateStateAction,
)
from backend.pipeline.transcription.enums import (
    MetricsExporterType,
    TranscriberType,
)
from backend.pipeline.transcription.stitcher_state import (
    AudioStitchingStateMachine,
)
from backend.pipeline.transcription.telemetry import get_metrics_exporter
from backend.pipeline.transcription.transcribers import (
    Transcriber,
    get_transcriber,
)

logger = logging.getLogger(__name__)

TRANSMISSION_BUFFER_SPEC = BagStateSpec(
    "transmission_buffer", beam.coders.PickleCoder()
)
# Accumulates continuous speech audio for a single transmission using O(1) list appending. Cleared when a significant gap is detected, or the stale timer fires.
TRANSMISSION_BUFFER_STATE = beam.DoFn.StateParam(TRANSMISSION_BUFFER_SPEC)

TRANSMISSION_CONTEXT_SPEC = ReadModifyWriteStateSpec(
    "transmission_context", beam.coders.PickleCoder()
)
# A unified TransmissionContext dataclass encapsulating all scalar metadata (timestamps, UUIDs) for the current transmission.
TRANSMISSION_CONTEXT_STATE = beam.DoFn.StateParam(TRANSMISSION_CONTEXT_SPEC)

STALE_TIMER_SPEC = TimerSpec("stale_timer", beam.TimeDomain.WATERMARK)
# A Beam Watermark timer. If no new data advances the watermark past its deadline, it fires to flush whatever audio is stranded in the TRANSMISSION_BUFFER.
STALE_TIMER_PARAM = beam.DoFn.TimerParam(STALE_TIMER_SPEC)

SEQUENTIAL_BARRIER_SPEC = ReadModifyWriteStateSpec(
    "sequential_barrier", beam.coders.BooleanCoder()
)
# A dummy state parameter used exclusively to enforce chronological processing constraints on the Beam Runner per feed_id.
SEQUENTIAL_BARRIER_STATE = beam.DoFn.StateParam(SEQUENTIAL_BARRIER_SPEC)


class StitchAudioFn(beam.DoFn):
    """A stateful Beam DoFn responsible for maintaining chronological continuous audio state per radio feed.

    Delegates core state transition logic to `AudioStitchingStateMachine` while mapping the
    resulting imperative actions to Apache Beam's State and Timer APIs. Yields evaluated
    `FlushRequest` objects triggered by chronologic gaps, max duration limits, or watermark timeouts.
    """

    def __init__(
        self,
        config: StitchAudioConfig,
    ) -> None:
        """Binds the StitchAudioConfig and initializes Beam metrics counters."""
        self.config = config

        self.audio_processor: AudioProcessor | None = None
        self.metrics_exporter: Any | None = None

        # Pipeline Telemetry (Beam Metrics)
        self.stale_flush_count = Metrics.counter(
            "StitchAudioFn", "stale_flush_count"
        )
        self.gap_flush_count = Metrics.counter(
            "StitchAudioFn", "gap_flush_count"
        )
        self.max_duration_flush_count = Metrics.counter(
            "StitchAudioFn", "max_duration_flush_count"
        )
        self.multiple_transmissions_count = Metrics.counter(
            "StitchAudioFn", "multiple_transmissions_per_chunk_count"
        )
        self.dlq_count = Metrics.counter("StitchAudioFn", "dlq_count")

        self.stitching_time_ms = Metrics.distribution(
            "StitchAudioFn", "stitching_time_ms"
        )

    @override
    def setup(self) -> None:
        """Initializes internal clients once per worker."""
        self.audio_processor = AudioProcessor(
            self.config.vad_type, self.config.vad_config
        )
        self.audio_processor.setup()

        parsed_exporters = []
        if self.config.metrics_exporter_type:
            types = [
                t.strip() for t in self.config.metrics_exporter_type.split(",")
            ]
            for t in types:
                if not t:
                    continue
                try:
                    parsed_exporters.append(MetricsExporterType(t))
                except ValueError:
                    logger.warning("Unknown metrics exporter type: %s", t)

        self.metrics_exporter = get_metrics_exporter(
            parsed_exporters,
            self.config.project_id,
            self.config.metrics_config,
        )
        self.metrics_exporter.setup()

    def _apply_flush_action(
        self,
        action: FlushAction,
        transmission_context: ReadModifyWriteRuntimeState,
        transmission_buffer: BagRuntimeState,
        stale_timer: RuntimeTimer,
    ) -> Generator[tuple[str, FlushRequest], None, None]:  # noqa: UP043
        """Clears current internal state arrays and yields a compiled FlushRequest downstream."""
        if "Maximum transmission duration" in action.reason:
            self.max_duration_flush_count.inc()
        elif "Significant gap" in action.reason:
            self.gap_flush_count.inc()
        logger.info(f"{action.reason}. Flushing preceding continuous audio.")

        buffered_audio = action.isolated_audio_buffer or list(
            transmission_buffer.read()
        )
        if buffered_audio:
            yield (
                action.feed_id,
                FlushRequest(
                    buffer=sum(buffered_audio[1:], buffered_audio[0]),
                    feed_id=action.feed_id,
                    contributing_audio_uris=action.contributing_audio_uris,
                    time_range=action.time_range,
                    missing_prior_context=action.missing_prior_context,
                    missing_post_context=action.missing_post_context,
                    start_audio_offset_ms=action.start_audio_offset_ms,
                    end_audio_offset_ms=action.end_audio_offset_ms,
                ),
            )
        else:
            logger.warning(
                f"FlushAction emitted but BagState was empty for feed {action.feed_id}."
            )

        if action.clear_state:
            transmission_context.clear()
            transmission_buffer.clear()
            stale_timer.clear()

    def _apply_update_state_action(
        self,
        transmission_context: ReadModifyWriteRuntimeState,
        ctx: StitcherContext,
    ) -> None:
        """Persists local Python state machine objects back to Apache Beam state API endpoints."""
        new_context = TransmissionContext(
            last_end_time_ms=ctx.last_segment_end_time_ms,
            stale_start_time_ms=ctx.transmission_start_time_ms,
            contributing_audio_uris=ctx.contributing_audio_uris,
            missing_prior_context=ctx.missing_prior_context,
            expected_next_chunk_start_ms=ctx.expected_next_chunk_start_ms,
            start_audio_offset_ms=ctx.start_audio_offset_ms,
            end_audio_offset_ms=ctx.end_audio_offset_ms,
        )
        transmission_context.write(new_context)

    def _apply_append_buffer_action(
        self,
        action: AppendBufferAction,
        transmission_buffer: BagRuntimeState,
    ) -> None:
        """Appends the isolated speech audio directly to the stateful sequence bag."""
        transmission_buffer.add(action.audio_buffer)

    def _apply_schedule_stale_timer_action(
        self, action: ScheduleStaleTimerAction, stale_timer: RuntimeTimer
    ) -> None:
        """Re-registers the latency and expiration watermark timer based on expected event-time timestamps."""
        if stale_timer is not None:
            if action.deadline_ms > 0:
                deadline_s = action.deadline_ms / MS_PER_SECOND
                stale_timer.set(Timestamp(seconds=deadline_s))
            else:
                stale_timer.clear()

    def _apply_state_actions(
        self,
        *,
        actions: list[StateMachineAction],
        transmission_context: ReadModifyWriteRuntimeState,
        transmission_buffer: BagRuntimeState,
        stale_timer: RuntimeTimer,
        ctx: StitcherContext,
        gcs_path: str,
    ) -> Generator[tuple[str, FlushRequest], None, None]:  # noqa: UP043
        """Routes individual StateMachineAction results to appropriate Apache Beam side-effects and emitters."""
        flush_count = sum(1 for a in actions if isinstance(a, FlushAction))
        if flush_count > 1:
            self.multiple_transmissions_count.inc()

        for action in actions:
            match action:
                case FlushAction():
                    yield from self._apply_flush_action(
                        action,
                        transmission_context,
                        transmission_buffer,
                        stale_timer,
                    )
                case AppendBufferAction():
                    self._apply_append_buffer_action(
                        action, transmission_buffer
                    )
                case UpdateStateAction():
                    self._apply_update_state_action(transmission_context, ctx)
                case ScheduleStaleTimerAction():
                    self._apply_schedule_stale_timer_action(action, stale_timer)
                case DropAction(reason=reason):
                    logger.info(f"{reason}: {gcs_path}")

    def _process_audio_chunk(
        self,
        *,
        feed_id: str,
        gcs_path: str,
        chunk_data: AudioChunkData,
        transmission_context: ReadModifyWriteRuntimeState,
        transmission_buffer: BagRuntimeState,
        stale_timer: RuntimeTimer,
    ) -> Generator[tuple[str, FlushRequest] | beam.pvalue.TaggedOutput]:
        """Top-level executor managing chunk ingestion, VAD decoding, state persistence, and flush delegation."""
        file_start_ms = chunk_data.start_ms

        curr_context: TransmissionContext = (
            transmission_context.read() or TransmissionContext()
        )

        ctx = StitcherContext(
            feed_id=feed_id,
            current_gcs_uri=gcs_path,
            contributing_audio_uris=curr_context.contributing_audio_uris.copy(),
            last_segment_end_time_ms=curr_context.last_end_time_ms,
            transmission_start_time_ms=curr_context.stale_start_time_ms,
            file_start_ms=file_start_ms,
            missing_prior_context=curr_context.missing_prior_context,
            expected_next_chunk_start_ms=curr_context.expected_next_chunk_start_ms,
            start_audio_offset_ms=curr_context.start_audio_offset_ms,
            end_audio_offset_ms=curr_context.end_audio_offset_ms,
        )

        pipeline = AudioStitchingStateMachine(self.config)

        start_time = time.time()
        actions = pipeline.process_chunk(chunk_data, ctx)
        stitching_duration = int((time.time() - start_time) * MS_PER_SECOND)
        self.stitching_time_ms.update(stitching_duration)
        if self.metrics_exporter:
            self.metrics_exporter.record_stitching_time(
                feed_id=feed_id, duration_ms=stitching_duration
            )

        yield from self._apply_state_actions(
            actions=actions,
            transmission_context=transmission_context,
            transmission_buffer=transmission_buffer,
            stale_timer=stale_timer,
            ctx=ctx,
            gcs_path=gcs_path,
        )

    @override
    def process(  # type: ignore[override]
        self,
        element: tuple[str, tuple[str, AudioChunkData]],
        transmission_buffer: BagRuntimeState = TRANSMISSION_BUFFER_STATE,  # type: ignore
        transmission_context: ReadModifyWriteRuntimeState = TRANSMISSION_CONTEXT_STATE,  # type: ignore
        stale_timer: RuntimeTimer = STALE_TIMER_PARAM,  # type: ignore
    ) -> Generator[tuple[str, FlushRequest] | beam.pvalue.TaggedOutput]:
        """Delegates the incoming audio chunk to the internal state machine for evaluation."""
        key, (gcs_path, chunk_data) = element

        try:
            yield from self._process_audio_chunk(
                feed_id=key,
                gcs_path=gcs_path,
                chunk_data=chunk_data,
                transmission_context=transmission_context,
                transmission_buffer=transmission_buffer,
                stale_timer=stale_timer,
            )
        except Exception as e:
            if not self.config.route_to_dlq:
                raise
            self.dlq_count.inc()
            logger.exception(
                "Error processing chunk %s for feed %s", gcs_path, key
            )
            msg = str(e)
            yield beam.pvalue.TaggedOutput(
                DEAD_LETTER_QUEUE_TAG, {"error": msg, "feed_id": key}
            )

    @on_timer(STALE_TIMER_SPEC)
    def handle_stale_transmission(
        self,
        key: str = beam.DoFn.KeyParam,  # type: ignore
        transmission_buffer: BagRuntimeState = TRANSMISSION_BUFFER_STATE,  # type: ignore
        transmission_context: ReadModifyWriteRuntimeState = TRANSMISSION_CONTEXT_STATE,  # type: ignore
        stale_timer: RuntimeTimer = STALE_TIMER_PARAM,  # type: ignore
    ) -> Generator[tuple[str, FlushRequest] | beam.pvalue.TaggedOutput]:
        """Invoked asynchronously by the Beam Runner when the event-time watermark.

        passes the timestamp previously scheduled on the `stale_timer`. This provides a critical
        safety net: if a radio feed abruptly drops offline, this timer guarantees that any
        audio remaining in the buffer will eventually be flushed and transcribed, preventing
        data loss from stranded state.
        """
        curr_context: TransmissionContext = (
            transmission_context.read() or TransmissionContext()
        )
        start_time_ms = curr_context.stale_start_time_ms
        end_time_ms = curr_context.last_end_time_ms
        processed_uris = curr_context.contributing_audio_uris
        audio_buffer = list(transmission_buffer.read())

        if audio_buffer and start_time_ms and end_time_ms:
            try:
                self.stale_flush_count.inc()
                logger.info(
                    f"STALE FLUSH: start={start_time_ms}, end={end_time_ms}, len(uris)={len(processed_uris)}, len(buffer)={len(audio_buffer)}"
                )

                yield (
                    key,
                    FlushRequest(
                        buffer=sum(audio_buffer[1:], audio_buffer[0]),
                        feed_id=key,
                        contributing_audio_uris=processed_uris,
                        time_range=TimeRange(
                            start_ms=int(start_time_ms),
                            end_ms=int(end_time_ms),
                        ),
                        missing_prior_context=bool(
                            curr_context.missing_prior_context
                        ),
                        missing_post_context=True,  # Flushed by timer cutoff, so we assume the tail is missing context.
                        start_audio_offset_ms=curr_context.start_audio_offset_ms,
                        end_audio_offset_ms=curr_context.end_audio_offset_ms,
                    ),
                )
            except Exception as e:
                if not self.config.route_to_dlq:
                    raise
                self.dlq_count.inc()
                logger.exception("Error yielding stale buffer for feed %s", key)
                msg = str(e)
                yield beam.pvalue.TaggedOutput(
                    DEAD_LETTER_QUEUE_TAG,
                    {"error": msg, "feed_id": key, "stale_flush": True},
                )

        transmission_context.clear()
        transmission_buffer.clear()
        stale_timer.clear()


class TranscribeAudioFn(beam.DoFn):
    """Submits the concatenated FLAC buffers generated by the upstream Stitcher to the
    transcription API and serializes the transcripts into a TranscriptionResult dataclass.
    """

    def __init__(
        self,
        config: TranscribeAudioConfig,
        transcriber_factory: Callable[[TranscriberType, str, str], Transcriber]
        | None = None,
    ) -> None:
        """Binds the TranscribeAudioConfig and initializes Beam metrics counters."""
        self.config = config
        self.transcriber_factory = transcriber_factory or get_transcriber

        self.audio_processor: AudioProcessor | None = None
        self.transcriber: Transcriber | None = None
        self.metrics_exporter: Any | None = None

        self.vad_speech_count = Metrics.counter(
            "TranscribeAudioFn", "vad_speech_count"
        )
        self.vad_silence_count = Metrics.counter(
            "TranscribeAudioFn", "vad_silence_count"
        )
        self.transcription_count = Metrics.counter(
            "TranscribeAudioFn", "transcription_count"
        )
        self.dlq_count = Metrics.counter("TranscribeAudioFn", "dlq_count")

        self.speech_duration_sec_dist = Metrics.distribution(
            "TranscribeAudioFn", "speech_duration_sec"
        )
        self.vad_eval_time_ms = Metrics.distribution(
            "TranscribeAudioFn", "vad_eval_time_ms"
        )
        self.transcription_time_ms = Metrics.distribution(
            "TranscribeAudioFn", "transcription_time_ms"
        )

    @override
    def setup(self) -> None:
        """Initializes internal clients once per worker."""
        self.audio_processor = AudioProcessor(
            self.config.vad_type, self.config.vad_config
        )
        self.audio_processor.setup()

        self.transcriber = self.transcriber_factory(
            self.config.transcriber_type,
            self.config.project_id,
            self.config.transcriber_config,
        )
        self.transcriber.setup()

        parsed_exporters = []
        if self.config.metrics_exporter_type:
            types = [
                t.strip() for t in self.config.metrics_exporter_type.split(",")
            ]
            for t in types:
                if not t:
                    continue
                try:
                    parsed_exporters.append(MetricsExporterType(t))
                except ValueError:
                    logger.warning("Unknown metrics exporter type: %s", t)

        self.metrics_exporter = get_metrics_exporter(
            parsed_exporters,
            self.config.project_id,
            self.config.metrics_config,
        )
        self.metrics_exporter.setup()

    def _export_and_transcribe(
        self,
        request: FlushRequest,
    ) -> TranscriptionResult | None:
        """Concatenates buffered chunks, renders to an intermediate audio format, and dispatches to the inference client."""
        if self.audio_processor is None:
            msg = "AudioProcessor not initialized. setup() must be called."
            raise RuntimeError(msg)
        if self.transcriber is None:
            msg = "Transcriber not initialized. setup() must be called."
            raise RuntimeError(msg)
        if self.metrics_exporter is None:
            msg = "MetricsExporter not initialized. setup() must be called."
            raise RuntimeError(msg)

        if not request.buffer or len(request.buffer) == 0:
            return None

        processed_audio = self.audio_processor.preprocess_audio(request.buffer)

        vad_start = time.time()
        has_speech = self.audio_processor.check_vad(processed_audio)
        self.vad_eval_time_ms.update(
            int((time.time() - vad_start) * MS_PER_SECOND)
        )

        if not has_speech:
            self.vad_silence_count.inc()
            logger.info(
                "VAD detected no speech in buffer. Dropping transmission."
            )
            return None

        self.vad_speech_count.inc()
        duration_sec = len(processed_audio) / float(MS_PER_SECOND)
        self.speech_duration_sec_dist.update(int(duration_sec))

        flac_bytes = self.audio_processor.export_flac(processed_audio)

        transcribe_start = time.time()
        transcript = self.transcriber.transcribe(
            audio_data=flac_bytes,
        )
        if transcript is None:
            logger.info("Transcription yielded no text. Dropping transmission.")
            return None
        duration_ms = int((time.time() - transcribe_start) * MS_PER_SECOND)
        self.transcription_time_ms.update(duration_ms)
        self.metrics_exporter.record_transcription_time(
            feed_id=request.feed_id, duration_ms=duration_ms
        )

        return TranscriptionResult(
            feed_id=request.feed_id,
            contributing_audio_uris=request.contributing_audio_uris,
            transcript=transcript,
            time_range=request.time_range,
            missing_prior_context=request.missing_prior_context,
            missing_post_context=request.missing_post_context,
            start_audio_offset_ms=request.start_audio_offset_ms,
            end_audio_offset_ms=request.end_audio_offset_ms,
        )

    @override
    def process(  # type: ignore[override]
        self,
        element: tuple[str, FlushRequest],
        sequential_barrier: ReadModifyWriteRuntimeState = SEQUENTIAL_BARRIER_STATE,  # type: ignore
        *args: Any,
        **kwargs: Any,
    ) -> Generator[TranscriptionResult | beam.pvalue.TaggedOutput, None, None]:  # noqa: UP043
        """Submits the consolidated flushed buffer strictly sequentially to the external transcription API."""
        feed_id, request = element
        try:
            transcribed = self._export_and_transcribe(request)
            if transcribed:
                self.transcription_count.inc()
                yield transcribed
        except Exception as e:
            if not self.config.route_to_dlq:
                raise
            self.dlq_count.inc()
            logger.exception("Error transcribing buffer for feed %s", feed_id)
            msg = str(e)
            yield beam.pvalue.TaggedOutput(
                DEAD_LETTER_QUEUE_TAG, {"error": msg, "feed_id": feed_id}
            )
