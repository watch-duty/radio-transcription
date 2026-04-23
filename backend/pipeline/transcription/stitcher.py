"""Stateful Apache Beam DoFns for chronological sequence timeline logic and stitch generation."""

import logging
import time
from collections.abc import Iterator
from datetime import UTC, datetime
from typing import Any, override

import apache_beam as beam
import numpy as np
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

from backend.pipeline.common.constants import (
    MS_PER_SECOND,
    SAMPLE_RATE_HZ,
)
from backend.pipeline.common.storage.gcs_uploader import GCSAudioUploader
from backend.pipeline.transcription.audio_processor import AudioProcessor
from backend.pipeline.transcription.constants import (
    DEAD_LETTER_QUEUE_TAG,
)
from backend.pipeline.transcription.datatypes import (
    AppendBufferAction,
    AudioChunkData,
    DownloadedChunkPayload,
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

# Force Dataflow workers to load TranscriptionOptions so it recognizes custom flags
from backend.pipeline.transcription.options import (
    TranscriptionOptions,  # noqa: F401
)
from backend.pipeline.transcription.resources import (
    SHARED_RESOURCE_HANDLE,
    SharedResources,
)
from backend.pipeline.transcription.stitcher_state import (
    AudioStitchingStateMachine,
)
from backend.pipeline.transcription.transcribers import (
    Transcriber,
    get_transcriber,
)
from backend.pipeline.transcription.utils import generate_transmission_id

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

STALE_TIMER_EVENT_SPEC = TimerSpec(
    "stale_timer_event", beam.TimeDomain.WATERMARK
)
# A Beam Watermark timer used during backfill to flush stranded audio based on data time.
STALE_TIMER_EVENT_PARAM = beam.DoFn.TimerParam(STALE_TIMER_EVENT_SPEC)

STALE_TIMER_PROC_SPEC = TimerSpec("stale_timer_proc", beam.TimeDomain.REAL_TIME)
# A Processing Time timer used during streaming to flush stranded audio based on wall-clock time, avoiding noisy neighbor stalls.
STALE_TIMER_PROC_PARAM = beam.DoFn.TimerParam(STALE_TIMER_PROC_SPEC)

SEQUENTIAL_BARRIER_SPEC = ReadModifyWriteStateSpec(
    "sequential_barrier", beam.coders.BooleanCoder()
)
# A dummy state parameter used exclusively to enforce chronological processing constraints on the Beam Runner per feed_id.
SEQUENTIAL_BARRIER_STATE = beam.DoFn.StateParam(SEQUENTIAL_BARRIER_SPEC)


@beam.typehints.with_input_types(tuple[str, DownloadedChunkPayload])
@beam.typehints.with_output_types(tuple[str, FlushRequest])
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

        # Pipeline Telemetry (Beam Metrics)
        self.stale_flush_count = Metrics.counter(
            "StitchAudioFn", "stale_flush_count"
        )
        self.silence_gap_flush_count = Metrics.counter(
            "StitchAudioFn", "silence_gap_flush_count"
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
        # Beam forces a single globally-referenced Shared handle to prevent GC
        self.shared_resources = SHARED_RESOURCE_HANDLE.acquire(SharedResources)

        self.audio_processor = AudioProcessor(
            self.config.vad_type,
            self.config.vad_config,
            shared_resources=self.shared_resources,
        )
        self.audio_processor.setup()

    def _apply_flush_action(
        self,
        action: FlushAction,
        transmission_context: ReadModifyWriteRuntimeState,
        transmission_buffer: BagRuntimeState,
        stale_timer_event: RuntimeTimer,
        stale_timer_proc: RuntimeTimer,
    ) -> Iterator[tuple[str, FlushRequest]]:
        """Clears current internal state arrays and yields a compiled FlushRequest downstream."""
        if "Maximum transmission duration" in action.reason:
            self.max_duration_flush_count.inc()
        elif "Significant gap" in action.reason:
            self.silence_gap_flush_count.inc()
        logger.info(
            "%s. Flushing preceding continuous audio. Range: %s. URIs: %s",
            action.reason,
            action.speech_time_range,
            action.contributing_audio_uris,
        )

        buffered_audio = action.isolated_audio_buffer or list(
            transmission_buffer.read()
        )
        if buffered_audio:
            # Create a deterministic UUID using our shared helper so that Beam retries produce the exact same ID
            transmission_id = generate_transmission_id(
                action.feed_id,
                action.speech_time_range.start_ms,
            )
            logger.info(
                "Generated transmission_id: %s for feed: %s",
                transmission_id,
                action.feed_id,
            )

            yield (
                action.feed_id,
                FlushRequest(
                    buffer=np.concatenate(buffered_audio),
                    feed_id=action.feed_id,
                    contributing_audio_uris=action.contributing_audio_uris,
                    time_range=action.time_range,
                    missing_prior_context=action.missing_prior_context,
                    missing_post_context=action.missing_post_context,
                    start_audio_offset_ms=action.start_audio_offset_ms,
                    end_audio_offset_ms=action.end_audio_offset_ms,
                    transmission_id=transmission_id,
                ),
            )
        else:
            logger.warning(
                f"FlushAction emitted but BagState was empty for feed {action.feed_id}."
            )

        if action.clear_state:
            transmission_context.clear()
            transmission_buffer.clear()
            stale_timer_event.clear()
            stale_timer_proc.clear()

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
            buffer_start_time_ms=ctx.buffer_start_time_ms,
            buffer_duration_ms=ctx.buffer_duration_ms,
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
        self,
        action: ScheduleStaleTimerAction,
        stale_timer_event: RuntimeTimer,
        stale_timer_proc: RuntimeTimer,
        *,
        is_backfill: bool,
    ) -> None:
        """Re-registers the latency and expiration timer based on mode."""
        if is_backfill:
            # In backfill mode, use Event Time (Watermark) timer
            stale_timer_proc.clear()
            if action.deadline_ms > 0:
                deadline_s = action.deadline_ms / MS_PER_SECOND
                stale_timer_event.set(Timestamp(seconds=deadline_s))
            else:
                stale_timer_event.clear()
        else:
            # In streaming mode, use Processing Time timer
            stale_timer_event.clear()
            if action.deadline_ms > 0:
                # Set timer relative to current wall-clock time
                deadline_s = time.time() + self.config.stale_timeout_ms / 1000.0
                stale_timer_proc.set(Timestamp(seconds=deadline_s))
            else:
                stale_timer_proc.clear()

    def _apply_state_actions(
        self,
        *,
        actions: list[StateMachineAction],
        transmission_context: ReadModifyWriteRuntimeState,
        transmission_buffer: BagRuntimeState,
        stale_timer_event: RuntimeTimer,
        stale_timer_proc: RuntimeTimer,
        ctx: StitcherContext,
        gcs_path: str,
        is_backfill: bool,
    ) -> Iterator[tuple[str, FlushRequest]]:
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
                        stale_timer_event,
                        stale_timer_proc,
                    )
                case AppendBufferAction():
                    self._apply_append_buffer_action(
                        action, transmission_buffer
                    )
                case UpdateStateAction():
                    self._apply_update_state_action(transmission_context, ctx)
                case ScheduleStaleTimerAction():
                    self._apply_schedule_stale_timer_action(
                        action,
                        stale_timer_event,
                        stale_timer_proc,
                        is_backfill=is_backfill,
                    )
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
        stale_timer_event: RuntimeTimer,
        stale_timer_proc: RuntimeTimer,
        is_backfill: bool,
    ) -> Iterator[tuple[str, FlushRequest] | beam.pvalue.TaggedOutput]:
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
            buffer_start_time_ms=curr_context.buffer_start_time_ms,
            buffer_duration_ms=curr_context.buffer_duration_ms,
        )

        pipeline = AudioStitchingStateMachine(self.config)

        start_time = time.time()
        actions = pipeline.process_chunk(chunk_data, ctx)
        stitching_duration = int((time.time() - start_time) * MS_PER_SECOND)
        self.stitching_time_ms.update(stitching_duration)

        yield from self._apply_state_actions(
            actions=actions,
            transmission_context=transmission_context,
            transmission_buffer=transmission_buffer,
            stale_timer_event=stale_timer_event,
            stale_timer_proc=stale_timer_proc,
            ctx=ctx,
            gcs_path=gcs_path,
            is_backfill=is_backfill,
        )

    @override
    def process(  # type: ignore[override]
        self,
        element: tuple[str, DownloadedChunkPayload],
        transmission_buffer: BagRuntimeState = TRANSMISSION_BUFFER_STATE,  # type: ignore
        transmission_context: ReadModifyWriteRuntimeState = TRANSMISSION_CONTEXT_STATE,  # type: ignore
        stale_timer_event: RuntimeTimer = STALE_TIMER_EVENT_PARAM,  # type: ignore
        stale_timer_proc: RuntimeTimer = STALE_TIMER_PROC_PARAM,  # type: ignore
    ) -> Iterator[tuple[str, FlushRequest] | beam.pvalue.TaggedOutput]:
        """Delegates the incoming audio chunk to the internal state machine for evaluation."""
        key, payload = element
        gcs_path = payload.gcs_uri
        chunk_data = payload.chunk_data

        current_wall_clock_time = time.time() * MS_PER_SECOND
        chunk_event_time = chunk_data.start_ms
        lateness = current_wall_clock_time - chunk_event_time
        is_backfill = lateness >= 5 * 60 * MS_PER_SECOND  # 5 minutes in ms

        try:
            yield from self._process_audio_chunk(
                feed_id=key,
                gcs_path=gcs_path,
                chunk_data=chunk_data,
                transmission_context=transmission_context,
                transmission_buffer=transmission_buffer,
                stale_timer_event=stale_timer_event,
                stale_timer_proc=stale_timer_proc,
                is_backfill=is_backfill,
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

    def _handle_stale_transmission(
        self,
        key: str,
        transmission_buffer: BagRuntimeState,
        transmission_context: ReadModifyWriteRuntimeState,
        stale_timer_event: RuntimeTimer,
        stale_timer_proc: RuntimeTimer,
    ) -> Iterator[tuple[str, FlushRequest] | beam.pvalue.TaggedOutput]:
        """Common logic for handling stale transmissions from both timers."""
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

                # Create a deterministic UUID using our shared helper so that Beam retries produce the exact same ID
                transmission_id = generate_transmission_id(
                    key,
                    int(start_time_ms),
                )

                yield (
                    key,
                    FlushRequest(
                        buffer=np.concatenate(audio_buffer),
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
                        transmission_id=transmission_id,
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
        stale_timer_event.clear()
        stale_timer_proc.clear()

    @on_timer(STALE_TIMER_EVENT_SPEC)
    def handle_stale_transmission_event(
        self,
        key: str = beam.DoFn.KeyParam,  # type: ignore
        transmission_buffer: BagRuntimeState = TRANSMISSION_BUFFER_STATE,  # type: ignore
        transmission_context: ReadModifyWriteRuntimeState = TRANSMISSION_CONTEXT_STATE,  # type: ignore
        stale_timer_event: RuntimeTimer = STALE_TIMER_EVENT_PARAM,  # type: ignore
        stale_timer_proc: RuntimeTimer = STALE_TIMER_PROC_PARAM,  # type: ignore
    ) -> Iterator[tuple[str, FlushRequest] | beam.pvalue.TaggedOutput]:
        """Invoked when the event-time watermark passes the deadline."""
        yield from self._handle_stale_transmission(
            key,
            transmission_buffer,
            transmission_context,
            stale_timer_event,
            stale_timer_proc,
        )

    @on_timer(STALE_TIMER_PROC_SPEC)
    def handle_stale_transmission_proc(
        self,
        key: str = beam.DoFn.KeyParam,  # type: ignore
        transmission_buffer: BagRuntimeState = TRANSMISSION_BUFFER_STATE,  # type: ignore
        transmission_context: ReadModifyWriteRuntimeState = TRANSMISSION_CONTEXT_STATE,  # type: ignore
        stale_timer_event: RuntimeTimer = STALE_TIMER_EVENT_PARAM,  # type: ignore
        stale_timer_proc: RuntimeTimer = STALE_TIMER_PROC_PARAM,  # type: ignore
    ) -> Iterator[tuple[str, FlushRequest] | beam.pvalue.TaggedOutput]:
        """Invoked when processing time passes the deadline."""
        yield from self._handle_stale_transmission(
            key,
            transmission_buffer,
            transmission_context,
            stale_timer_event,
            stale_timer_proc,
        )


@beam.typehints.with_input_types(tuple[str, FlushRequest])
@beam.typehints.with_output_types(TranscriptionResult)
class TranscribeAudioFn(beam.DoFn):
    """Submits the concatenated FLAC buffers generated by the upstream Stitcher to the
    transcription API and serializes the transcripts into a TranscriptionResult dataclass.
    """

    def __init__(
        self,
        config: TranscribeAudioConfig,
        transcriber_factory: Any | None = None,
    ) -> None:
        """Binds the TranscribeAudioConfig and initializes Beam metrics counters."""
        self.config = config
        self.transcriber_factory = transcriber_factory or get_transcriber

        self.audio_processor: AudioProcessor | None = None
        self.transcriber: Transcriber | None = None

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
        # Grab the global pool singleton
        self.shared_resources = SHARED_RESOURCE_HANDLE.acquire(SharedResources)

        self.audio_processor = AudioProcessor(
            self.config.vad_type,
            self.config.vad_config,
            shared_resources=self.shared_resources,
        )
        self.audio_processor.setup()

        self.transcriber = self.shared_resources.get_transcriber(
            factory=self.transcriber_factory,
            transcriber_type=self.config.transcriber_type,
            project_id=self.config.project_id,
            config_json=self.config.transcriber_config,
        )

        if self.audio_processor.gcs_client is None:
            msg = "GCS client not found in AudioProcessor. must call setup() first."
            raise RuntimeError(msg)

        self.audio_uploader = GCSAudioUploader(
            gcs_client=self.audio_processor.gcs_client,
        )

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

        if request.buffer is None or request.buffer.size == 0:
            return None

        success, flac_bytes, processed_audio = (
            self.audio_processor.process_buffer(request.buffer)
        )
        if not success or flac_bytes is None or processed_audio is None:
            self.vad_silence_count.inc()
            logger.info(
                "VAD detected no speech in buffer. Dropping transmission."
            )
            return None

        self.vad_speech_count.inc()
        duration_sec = len(processed_audio) / float(SAMPLE_RATE_HZ)
        self.speech_duration_sec_dist.update(int(duration_sec))

        if not self.config.canonical_audio_bucket:
            canonical_audio_uri, playback_audio_uri = None, None
        else:
            dt = datetime.fromtimestamp(
                request.time_range.start_ms / 1000.0, tz=UTC
            )

            flac_path = f"lossless/{request.feed_id}/{dt:%Y/%m/%d}/{request.transmission_id}.flac"
            m4a_path = f"playback/{request.feed_id}/{dt:%Y/%m/%d}/{request.transmission_id}.m4a"

            canonical_audio_uri, playback_audio_uri = (
                self.audio_uploader.upload_audio_derivatives(
                    bucket_name=self.config.canonical_audio_bucket,
                    flac_path=flac_path,
                    m4a_path=m4a_path,
                    flac_bytes=flac_bytes,
                    processed_audio=processed_audio,
                    export_m4a_fn=self.audio_processor.export_m4a,
                )
            )
        if not canonical_audio_uri and (
            request.contributing_audio_uris
            and len(request.contributing_audio_uris) == 1
        ):
            canonical_audio_uri = request.contributing_audio_uris[0]

        transcribe_start = time.time()

        transcript = self.transcriber.transcribe(
            audio_data=flac_bytes,
        )
        if transcript is None:
            logger.info("Transcription yielded no text. Dropping transmission.")
            return None
        duration_ms = int((time.time() - transcribe_start) * MS_PER_SECOND)
        self.transcription_time_ms.update(duration_ms)

        logger.info(f"TRANSCRIPT [{request.feed_id}]: {transcript}")

        return TranscriptionResult(
            feed_id=request.feed_id,
            contributing_audio_uris=request.contributing_audio_uris,
            transcript=transcript,
            time_range=request.time_range,
            transmission_id=request.transmission_id,
            missing_prior_context=request.missing_prior_context,
            missing_post_context=request.missing_post_context,
            start_audio_offset_ms=request.start_audio_offset_ms,
            end_audio_offset_ms=request.end_audio_offset_ms,
            canonical_audio_uri=canonical_audio_uri,
            playback_audio_uri=playback_audio_uri,
        )

    @override
    def process(  # type: ignore[override]
        self,
        element: tuple[str, FlushRequest],
        sequential_barrier: ReadModifyWriteRuntimeState = SEQUENTIAL_BARRIER_STATE,  # type: ignore
        *args: Any,
        **kwargs: Any,
    ) -> Iterator[TranscriptionResult | beam.pvalue.TaggedOutput]:
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
