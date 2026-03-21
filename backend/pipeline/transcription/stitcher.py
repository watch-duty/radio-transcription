import io
import logging
import time
from collections.abc import Callable, Generator
from concurrent.futures import ThreadPoolExecutor
from typing import Any, override

import apache_beam as beam
from apache_beam.metrics import Metrics
from apache_beam.transforms.userstate import (
    ReadModifyWriteRuntimeState,
    ReadModifyWriteStateSpec,
    RuntimeTimer,
    TimerSpec,
    on_timer,
)
from apache_beam.utils.timestamp import Timestamp
from pydub import AudioSegment

from backend.pipeline.shared_constants import AUDIO_FORMAT
from backend.pipeline.transcription.audio_processor import AudioProcessor
from backend.pipeline.transcription.constants import (
    DEAD_LETTER_QUEUE_TAG,
    MS_PER_SECOND,
)
from backend.pipeline.transcription.datatypes import (
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
from backend.pipeline.transcription.stitcher_state import AudioStitchingStateMachine
from backend.pipeline.transcription.telemetry import get_metrics_exporter
from backend.pipeline.transcription.transcribers import Transcriber, get_transcriber

logger = logging.getLogger(__name__)


TRANSMISSION_BUFFER_SPEC = ReadModifyWriteStateSpec(
    "transmission_buffer", beam.coders.BytesCoder()
)
# Accumulates continuous speech audio for a single transmission. Cleared when a significant gap is detected, or the stale timer fires.
TRANSMISSION_BUFFER_STATE = beam.DoFn.StateParam(TRANSMISSION_BUFFER_SPEC)

TRANSMISSION_CONTEXT_SPEC = ReadModifyWriteStateSpec(
    "transmission_context", beam.coders.PickleCoder()
)
# A unified TransmissionContext dataclass encapsulating all scalar metadata (timestamps, UUIDs) for the current transmission.
TRANSMISSION_CONTEXT_STATE = beam.DoFn.StateParam(TRANSMISSION_CONTEXT_SPEC)

STALE_TIMER_SPEC = TimerSpec("stale_timer", beam.TimeDomain.WATERMARK)
# A Beam Watermark timer. If no new data advances the watermark past its deadline, it fires to flush whatever audio is stranded in the TRANSMISSION_BUFFER.
STALE_TIMER_PARAM = beam.DoFn.TimerParam(STALE_TIMER_SPEC)


class StitchAudioFn(beam.DoFn):
    """
    A stateful Beam DoFn responsible for maintaining chronological continuous audio state per radio feed.
    It delegates the core state transition logic to `AudioStitchingStateMachine` while mapping the resulting
    actions to Beam's state and timer APIs.

    It buffers incoming audio chunks, runs Voice Activity Detection (VAD) to identify speech segments,
    and yields complete `FlushRequest`s to downstream transforms.

    Pipeline overview:
    1. Stateful Accumulation: As independent chunks arrive for a `feed_id`, they are decompressed,
       evaluated for speech via VAD, and appended to the persistent `transmission_buffer` state.
    2. Gap Detection: If an incoming chunk's start time is significantly later than the end time
       of the previous chunk, we assume the previous transmission has ended. We flush the buffer
       downstream and clear all states to start a fresh transmission.
    3. Stale Timers: In distributed systems, a stream might silently disconnect. We register a
       latency watermark timer (`stale_timer`). If no new data arrives before the timer fires,
       Beam automatically invokes `handle_stale_transmission` to flush any stranded audio.
    """

    def __init__(
        self,
        config: StitchAudioConfig,
    ) -> None:
        self.config = config

        self.audio_processor: AudioProcessor | None = None
        self.metrics_exporter: Any | None = None

        # Pipeline Telemetry (Beam Metrics)
        self.stale_flush_count = Metrics.counter("StitchAudioFn", "stale_flush_count")
        self.gap_flush_count = Metrics.counter("StitchAudioFn", "gap_flush_count")
        self.max_duration_flush_count = Metrics.counter(
            "StitchAudioFn", "max_duration_flush_count"
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
            types = [t.strip() for t in self.config.metrics_exporter_type.split(",")]
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

    # Method removed because UUIDs are natively embedded in URIs now.
    def _load_current_buffer(
        self,
        transmission_buffer: ReadModifyWriteRuntimeState,
    ) -> AudioSegment | None:
        buffered_audio_bytes = transmission_buffer.read()
        return (
            AudioSegment.from_file(
                io.BytesIO(buffered_audio_bytes), format=AUDIO_FORMAT
            )
            if buffered_audio_bytes
            else None
        )

    def _fetch_and_validate_audio(
        self, *, feed_id: str, gcs_path: str, processed_uris: set[str]
    ) -> Generator[tuple[str, AudioChunkData] | beam.pvalue.TaggedOutput]:
        """
        Downloads audio bytes and SED metadata from GCS.
        Yields `beam.pvalue.TaggedOutput` strings to the DLQ if an exception occurs.
        """
        if self.audio_processor is None:
            msg = "AudioProcessor not initialized. setup() must be called."
            raise RuntimeError(msg)

        try:
            if gcs_path in processed_uris:
                logger.info("Dropping duplicate message for URI: %s", gcs_path)
                return

            logger.info(
                "Processing file: %s for feed: %s",
                gcs_path,
                feed_id,
            )
            chunk_data = self.audio_processor.download_audio_and_sed(gcs_path)
            # start_ms is guaranteed to be present by strict validation in utils.py

            yield gcs_path, chunk_data
        except FileNotFoundError:
            logger.info(
                "GCS object not found yet (eventual consistency?). Re-raising to NACK Pub/Sub message."
            )
            raise
        except Exception as e:
            if not getattr(self.config, "route_to_dlq", True):
                raise
            self.dlq_count.inc()
            action = "processing"
            msg = str(e)
            logger.exception("Error %s %s for feed %s", action, gcs_path, feed_id)
            yield beam.pvalue.TaggedOutput(
                DEAD_LETTER_QUEUE_TAG, {"error": msg, "feed_id": feed_id}
            )

    def _apply_flush_action(
        self,
        action: FlushAction,
        transmission_context: ReadModifyWriteRuntimeState,
        transmission_buffer: ReadModifyWriteRuntimeState,
        stale_timer: RuntimeTimer,
    ) -> Generator[FlushRequest]:
        if "Maximum transmission duration" in action.reason:
            self.max_duration_flush_count.inc()
        elif "Significant gap" in action.reason:
            self.gap_flush_count.inc()
        logger.info(f"{action.reason}. Flushing preceding continuous audio.")
        yield action.flush_request
        if action.clear_state:
            transmission_context.clear()
            transmission_buffer.clear()
            stale_timer.clear()

    def _apply_update_state_action(
        self,
        transmission_context: ReadModifyWriteRuntimeState,
        transmission_buffer: ReadModifyWriteRuntimeState,
        ctx: StitcherContext,
    ) -> None:
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

        if ctx.current_buffer:
            byte_stream = io.BytesIO()
            ctx.current_buffer.export(byte_stream, format=AUDIO_FORMAT)
            transmission_buffer.write(byte_stream.getvalue())
        else:
            transmission_buffer.clear()

    def _apply_schedule_stale_timer_action(
        self, action: ScheduleStaleTimerAction, stale_timer: RuntimeTimer
    ) -> None:
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
        transmission_buffer: ReadModifyWriteRuntimeState,
        stale_timer: RuntimeTimer,
        ctx: StitcherContext,
        gcs_path: str,
    ) -> Generator[FlushRequest]:
        for action in actions:
            match action:
                case FlushAction():
                    yield from self._apply_flush_action(
                        action, transmission_context, transmission_buffer, stale_timer
                    )
                case UpdateStateAction():
                    self._apply_update_state_action(
                        transmission_context, transmission_buffer, ctx
                    )
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
        transmission_buffer: ReadModifyWriteRuntimeState,
        stale_timer: RuntimeTimer,
    ) -> Generator[FlushRequest | beam.pvalue.TaggedOutput]:
        file_start_ms = chunk_data.start_ms

        curr_context: TransmissionContext = (
            transmission_context.read() or TransmissionContext()
        )
        current_buffer = self._load_current_buffer(transmission_buffer)

        ctx = StitcherContext(
            feed_id=feed_id,
            current_gcs_uri=gcs_path,
            current_buffer=current_buffer,
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
        element: tuple[str, str],
        transmission_buffer: ReadModifyWriteRuntimeState = TRANSMISSION_BUFFER_STATE,  # type: ignore
        transmission_context: ReadModifyWriteRuntimeState = TRANSMISSION_CONTEXT_STATE,  # type: ignore
        stale_timer: RuntimeTimer = STALE_TIMER_PARAM,  # type: ignore
    ) -> Generator[FlushRequest | beam.pvalue.TaggedOutput]:
        key, gcs_path = element

        curr_context: TransmissionContext = (
            transmission_context.read() or TransmissionContext()
        )

        fetched_results = self._fetch_and_validate_audio(
            feed_id=key,
            gcs_path=gcs_path,
            processed_uris=set(curr_context.contributing_audio_uris),
        )

        for result in fetched_results:
            match result:
                case beam.pvalue.TaggedOutput():
                    yield result
                case (gcs_uri, chunk_data):
                    yield from self._process_audio_chunk(
                        feed_id=key,
                        gcs_path=gcs_uri,
                        chunk_data=chunk_data,
                        transmission_context=transmission_context,
                        transmission_buffer=transmission_buffer,
                        stale_timer=stale_timer,
                    )

    @on_timer(STALE_TIMER_SPEC)
    def handle_stale_transmission(
        self,
        key: str = beam.DoFn.KeyParam,  # type: ignore
        transmission_buffer: ReadModifyWriteRuntimeState = TRANSMISSION_BUFFER_STATE,  # type: ignore
        transmission_context: ReadModifyWriteRuntimeState = TRANSMISSION_CONTEXT_STATE,  # type: ignore
        stale_timer: RuntimeTimer = STALE_TIMER_PARAM,  # type: ignore
    ) -> Generator[FlushRequest | beam.pvalue.TaggedOutput]:
        """
        Invoked asynchronously by the Beam Runner when the event-time watermark
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
        audio_buffer = self._load_current_buffer(transmission_buffer)

        if audio_buffer and start_time_ms and end_time_ms:
            try:
                self.stale_flush_count.inc()
                logger.info(
                    f"STALE FLUSH: start={start_time_ms}, end={end_time_ms}, len(uris)={len(processed_uris)}, len(buffer)={len(audio_buffer)}"
                )

                yield FlushRequest(
                    buffer=audio_buffer,
                    feed_id=key,
                    contributing_audio_uris=processed_uris,
                    time_range=TimeRange(
                        start_ms=int(start_time_ms),
                        end_ms=int(end_time_ms),
                    ),
                    missing_prior_context=bool(curr_context.missing_prior_context),
                    missing_post_context=True,  # Flushed by timer cutoff, so we assume the tail is missing context.
                    start_audio_offset_ms=curr_context.start_audio_offset_ms,
                    end_audio_offset_ms=curr_context.end_audio_offset_ms,
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
    """
    A stateless Beam DoFn responsible for executing purely stateless execution of VAD and
    Cloud Speech-to-Text inference over continuous FlushRequests.
    """

    def __init__(
        self,
        config: TranscribeAudioConfig,
        transcriber_factory: Callable[[TranscriberType, str, str], Transcriber]
        | None = None,
    ) -> None:
        self.config = config
        self.transcriber_factory = transcriber_factory or get_transcriber

        self.audio_processor: AudioProcessor | None = None
        self.transcriber: Transcriber | None = None
        self.metrics_exporter: Any | None = None

        self.vad_speech_count = Metrics.counter("TranscribeAudioFn", "vad_speech_count")
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
            types = [t.strip() for t in self.config.metrics_exporter_type.split(",")]
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

        # Allocate an internal ThreadPool to concurrently fire transcribe API calls for elements
        # in the input bundle, preventing blocking the python worker.
        self.executor = ThreadPoolExecutor()

    @override
    def teardown(self) -> None:
        self.executor.shutdown(wait=True)

    def _export_and_transcribe(
        self,
        request: FlushRequest,
    ) -> TranscriptionResult | None:
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
        self.vad_eval_time_ms.update(int((time.time() - vad_start) * MS_PER_SECOND))

        if not has_speech:
            self.vad_silence_count.inc()
            logger.info("VAD detected no speech in buffer. Dropping transmission.")
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
    def process(
        self,
        element: FlushRequest,
        *args: Any,
        **kwargs: Any,
    ) -> Generator[TranscriptionResult | beam.pvalue.TaggedOutput]:

        try:
            # We defer execution to a thread pool so we don't block the DoFn event loop
            future = self.executor.submit(
                self._export_and_transcribe,
                request=element,
            )
            transcribed = future.result()

            if transcribed:
                self.transcription_count.inc()
                yield transcribed

        except Exception as e:
            if not getattr(self.config, "route_to_dlq", True):
                raise
            self.dlq_count.inc()
            logger.exception(
                "Error concurrently flushing buffer for feed %s", element.feed_id
            )

            # If the exception was raised inside the ThreadPool future, unwrap it
            msg = str(e)
            if hasattr(e, "__cause__") and e.__cause__:
                msg = str(e.__cause__)

            yield beam.pvalue.TaggedOutput(
                DEAD_LETTER_QUEUE_TAG, {"error": msg, "feed_id": element.feed_id}
            )
