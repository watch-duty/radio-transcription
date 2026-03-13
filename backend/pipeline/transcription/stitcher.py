import io
import logging
import time
from collections.abc import Callable, Generator
from concurrent.futures import ThreadPoolExecutor
from typing import Any

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

from backend.pipeline.transcription.audio_processor import AudioProcessor
from backend.pipeline.transcription.constants import (
    AUDIO_FORMAT,
    DEAD_LETTER_QUEUE_TAG,
    MS_PER_SECOND,
)
from backend.pipeline.transcription.datatypes import (
    AudioChunkData,
    ChunkContext,
    FlushRequest,
    StitchAndTranscribeConfig,
    TranscriptionResult,
    TransmissionState,
)
from backend.pipeline.transcription.enums import (
    MetricsExporterType,
    TranscriberType,
)
from backend.pipeline.transcription.telemetry import get_metrics_exporter
from backend.pipeline.transcription.transcribers import Transcriber, get_transcriber

logger = logging.getLogger(__name__)

# ==============================================================================
# Stitching overview
# ==============================================================================
# In a distributed streaming pipeline, individual elements (audio chunks) are processed
# independently and statelessly by default. To stitch audio together, we must explicitly
# declare State and Timer specifications that Beam will persist between processing 
# discrete elements for the same key (feed_id). When running on Google Cloud Dataflow, 
# this state is natively managed by Dataflow's worker disks or Streaming Engine—no 
# external databases are required.
# ==============================================================================

# ==============================================================================
# State variables breakdown:
# 1. TRANSMISSION_BUFFER (Bytes): Accumulates continuous speech audio for a single transmission.
#    Cleared when a significant gap is detected, or the stale timer fires.
# 2. LAST_END_TIME (Float): Tracks the absolute end time (ms) of the most recently processed
#    audio chunk. Used to calculate "gaps" between incoming chunks.
# 3. CONTRIBUTING_UUIDS (Iterable[String]): Tracks which GCS files have been successfully processed
#    to prevent duplicate processing if Pub/Sub delivers the same event multiple times.
# 4. STALE_START_TIME (Float): The absolute start time (ms) of the *current* transmission buffer.
#    Used to calculate the transcriber API deadline for the stale timer.
# 5. STALE_TIMER (Timer): A Beam Watermark timer. If no new data advances the watermark past 
#    its deadline, it fires to flush whatever audio is stranded in the TRANSMISSION_BUFFER.
# ==============================================================================

TRANSMISSION_BUFFER_SPEC = ReadModifyWriteStateSpec(
    "transmission_buffer", beam.coders.BytesCoder()
)
TRANSMISSION_BUFFER_STATE = beam.DoFn.StateParam(TRANSMISSION_BUFFER_SPEC)

LAST_END_TIME_SPEC = ReadModifyWriteStateSpec("last_end_time", beam.coders.FloatCoder())
LAST_END_TIME_STATE = beam.DoFn.StateParam(LAST_END_TIME_SPEC)

CONTRIBUTING_UUIDS_SPEC = ReadModifyWriteStateSpec(
    "contributing_uuids", beam.coders.IterableCoder(beam.coders.StrUtf8Coder())
)
CONTRIBUTING_UUIDS_STATE = beam.DoFn.StateParam(CONTRIBUTING_UUIDS_SPEC)

STALE_START_TIME_SPEC = ReadModifyWriteStateSpec(
    "stale_start_time", beam.coders.FloatCoder()
)
STALE_START_TIME_STATE = beam.DoFn.StateParam(STALE_START_TIME_SPEC)

STALE_TIMER_SPEC = TimerSpec("stale_timer", beam.TimeDomain.WATERMARK)
STALE_TIMER_PARAM = beam.DoFn.TimerParam(STALE_TIMER_SPEC)


class StitchAndTranscribeFn(beam.DoFn):
    """
    A stateful Beam DoFn responsible for maintaining chronological continuous audio state per radio feed.
    It buffers incoming audio chunks, runs Voice Activity Detection (VAD) to identify speech segments,
    and flushes completed speech segments concurrently to the configured `Transcriber` plugin.
    Outputs `TranscriptionResult` objects containing the final transcripts.

    Pipeline overview:
    1. Stateful Accumulation: As independent chunks arrive for a `feed_id`, they are decompressed,
       evaluated for speech via VAD, and appended to the persistent `transmission_buffer` state.
    2. Gap Detection: If an incoming chunk's start time is significantly later than the end time
       of the previous chunk, we assume the previous transmission has ended. We flush the buffer
       to the transcriber API and clear all states to start a fresh transmission.
    3. Stale Timers: In distributed systems, a stream might silently disconnect. We register a 
       latency watermark timer (`stale_timer`). If no new data arrives before the timer fires, 
       Beam automatically invokes `handle_stale_transmission` to flush any stranded audio.
    4. Concurrency: Transcription API calls (e.g., Google Cloud Speech) block execution. We use
       a ThreadPoolExecutor to batch process these network calls asynchronously to prevent stalling
       the Beam worker's main event loop.
    """

    def __init__(
        self,
        config: StitchAndTranscribeConfig,
        transcriber_factory: Callable[[TranscriberType, str, str], Transcriber]
        | None = None,
    ) -> None:
        self.config = config
        self.transcriber_factory = transcriber_factory or get_transcriber
        self.audio_processor: AudioProcessor | None = None
        self.transcriber: Transcriber | None = None
        self.metrics_exporter: Any | None = None

        self.vad_speech_count = Metrics.counter(
            "StitchAndTranscribeFn", "vad_speech_count"
        )
        self.vad_silence_count = Metrics.counter(
            "StitchAndTranscribeFn", "vad_silence_count"
        )
        self.transcription_count = Metrics.counter(
            "StitchAndTranscribeFn", "transcription_count"
        )
        self.stale_flush_count = Metrics.counter(
            "StitchAndTranscribeFn", "stale_flush_count"
        )
        self.dlq_count = Metrics.counter("StitchAndTranscribeFn", "dlq_count")

        self.speech_duration_sec_dist = Metrics.distribution(
            "StitchAndTranscribeFn", "speech_duration_sec"
        )
        self.vad_eval_time_ms = Metrics.distribution(
            "StitchAndTranscribeFn", "vad_eval_time_ms"
        )
        self.transcription_time_ms = Metrics.distribution(
            "StitchAndTranscribeFn", "transcription_time_ms"
        )

    def setup(self) -> None:
        """
        Initializes transcriber, VAD, and metrics exporter clients once per worker.
        """
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
                t.strip().lower() for t in self.config.metrics_exporter_type.split(",")
            ]
            for t in types:
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

        self.executor = ThreadPoolExecutor()

    def teardown(self) -> None:
        """Cleans up the ThreadPoolExecutor on worker teardown."""
        self.executor.shutdown(wait=True)

    def _export_and_transcribe(
        self,
        *,
        audio_buffer: AudioSegment,
        feed_id: str,
        source_uuids: set[str],
        start_ms: int,
        end_ms: int,
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

        processed_audio = self.audio_processor.preprocess_audio(audio_buffer)

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
        duration_ms = int((time.time() - transcribe_start) * MS_PER_SECOND)
        self.transcription_time_ms.update(duration_ms)
        self.metrics_exporter.record_transcription_time(
            feed_id=feed_id, duration_ms=duration_ms
        )

        return TranscriptionResult(
            feed_id=feed_id,
            audio_ids=sorted(source_uuids),
            transcript=transcript,
            start_ms=start_ms,
            end_ms=end_ms,
        )

    def _extract_source_uuid(self, gcs_path: str) -> str:
        _, _, filename = gcs_path.rpartition("/")
        _, sep, tail = filename.partition("-")
        if not sep:
            msg = f"Could not extract UUID from filename: {filename}"
            raise ValueError(msg)
        return tail.replace(f".{AUDIO_FORMAT}", "")

    def _flush_buffer(
        self,
        *,
        buffer: AudioSegment,
        feed_id: str,
        processed_uuids: set[str],
        start_ms: int,
        end_ms: int,
    ) -> TranscriptionResult | None:
        if not buffer or len(buffer) == 0:
            return None

        transcribed_bytes = self._export_and_transcribe(
            audio_buffer=buffer,
            feed_id=feed_id,
            source_uuids=processed_uuids,
            start_ms=start_ms,
            end_ms=end_ms,
        )

        if transcribed_bytes is None:
            # VAD legitimately dropped the audio
            return None

        if not transcribed_bytes:
            msg = "Transcriber produced an empty payload buffer."
            raise ValueError(msg)

        self.transcription_count.inc()
        return transcribed_bytes

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
        self, *, feed_id: str, gcs_path: str, processed_uuids: set[str]
    ) -> Generator[
        tuple[str, AudioChunkData] | beam.pvalue.TaggedOutput,
        None,
        None,
    ]:
        """
        Downloads audio bytes and SED metadata from GCS.
        Yields `beam.pvalue.TaggedOutput` strings to the DLQ if an exception occurs.
        """
        if self.audio_processor is None:
            msg = "AudioProcessor not initialized. setup() must be called."
            raise RuntimeError(msg)

        try:
            source_file_uuid = self._extract_source_uuid(gcs_path)

            if source_file_uuid in processed_uuids:
                logger.info("Dropping duplicate message for UUID: %s", source_file_uuid)
                return

            logger.info(
                "Processing file: %s (UUID: %s) for feed: %s",
                gcs_path,
                source_file_uuid,
                feed_id,
            )
            chunk_data = (
                self.audio_processor.download_audio_and_sed(gcs_path)
            )
            # start_ms is guaranteed to be present by strict validation in utils.py

            yield source_file_uuid, chunk_data
        except FileNotFoundError:
            logger.info("GCS object not found yet (eventual consistency?). Re-raising to NACK Pub/Sub message.")
            raise
        except Exception as e:
            self.dlq_count.inc()
            action = "processing"
            msg = str(e)
            logger.exception("Error %s %s for feed %s", action, gcs_path, feed_id)
            yield beam.pvalue.TaggedOutput(
                DEAD_LETTER_QUEUE_TAG, {"error": msg, "feed_id": feed_id}
            )

    def _handle_silent_file(
        self,
        *,
        feed_id: str,
        gcs_path: str,
        ctx: ChunkContext,
        state: TransmissionState,
    ) -> Generator[FlushRequest | beam.pvalue.TaggedOutput, None, None]:
        """
        Appends the UUID to the current segment if one exists for continuity. Otherwise discards.
        Does not flush the buffer. Ensures the stale timer is pushed backward if a buffer exists.
        """
        if not ctx.current_buffer:
            logger.info("Discarding silent file: %s", gcs_path)
            if state.stale_timer is not None:
                state.stale_timer.clear()
            return

        logger.info(
            "Appending silent file to existing buffer for continuity: %s", gcs_path
        )
        ctx.processed_uuids.add(ctx.source_file_uuid)
        state.contributing_uuids.write(ctx.processed_uuids)

        if ctx.transmission_start_time_ms is None:
            yield beam.pvalue.TaggedOutput(
                DEAD_LETTER_QUEUE_TAG,
                {
                    "error": "State mismatch: transmission has buffer but no start_time.",
                    "feed_id": feed_id,
                },
            )
            return

        expected_stale_deadline = (
            ctx.transmission_start_time_ms / MS_PER_SECOND
        ) + self.config.stale_timeout_sec
        if state.stale_timer is not None:
            state.stale_timer.set(Timestamp(seconds=expected_stale_deadline))

    def _append_speech_segment(
        self,
        *,
        chunk_data: AudioChunkData,
        ctx: ChunkContext,
        state: TransmissionState,
    ) -> None:
        logger.info(
            "Found SED speech segment. Extracting bounding audio for UUID: %s",
            ctx.source_file_uuid,
        )

        global_start_ms = chunk_data.speech_segments[0].start_ms
        global_end_ms = chunk_data.speech_segments[-1].end_ms

        speech_audio = chunk_data.audio[global_start_ms:global_end_ms]

        if not ctx.current_buffer:
            ctx.current_buffer = speech_audio
            # Lock in the chronological start time of this transmission
            state.stale_start_time.write(ctx.chunk_start_ms + global_start_ms)
        else:
            ctx.current_buffer += speech_audio

        ctx.processed_uuids.add(ctx.source_file_uuid)
        # Persist this UUID so we don't accidentally append it again on a PubSub retry
        state.contributing_uuids.write(ctx.processed_uuids)
        # Continuously bump the end marker so the Gap Detection logic 
        # in the next processing step knows exactly where this transmission left off.
        state.last_end_time.write(ctx.chunk_start_ms + global_end_ms)

        # Append the raw audio bytes to the persistent buffer
        byte_stream = io.BytesIO()
        ctx.current_buffer.export(byte_stream, format=AUDIO_FORMAT)
        state.buffer.write(byte_stream.getvalue())

    def _process_audio_chunk(
        self,
        *,
        feed_id: str,
        gcs_path: str,
        source_file_uuid: str,
        chunk_data: AudioChunkData,
        state: TransmissionState,
    ) -> Generator[FlushRequest | beam.pvalue.TaggedOutput, None, None]:
        chunk_start_ms = chunk_data.start_ms

        processed_uuids = set(state.contributing_uuids.read() or [])
        current_buffer = self._load_current_buffer(state.buffer)
        transmission_start_time_ms = state.stale_start_time.read()
        last_segment_end_time_ms = state.last_end_time.read()

        ctx = ChunkContext(
            feed_id=feed_id,
            source_file_uuid=source_file_uuid,
            current_buffer=current_buffer,
            processed_uuids=processed_uuids,
            last_segment_end_time_ms=int(last_segment_end_time_ms)
            if last_segment_end_time_ms
            else 0,
            transmission_start_time_ms=int(transmission_start_time_ms)
            if transmission_start_time_ms
            else None,
            chunk_start_ms=chunk_start_ms,
        )

        if not chunk_data.speech_segments:
            yield from self._handle_silent_file(
                feed_id=feed_id,
                gcs_path=gcs_path,
                ctx=ctx,
                state=state,
            )
            return

        # Because we establish event-time ordering upstream, we can evaluate the semantic gap 
        # between sequential chunks. If the gap exceeds our configured threshold, the current
        # transmission has logically ended. We must yield a FlushRequest to transcribe the 
        # preceding buffer before appending the current segment to a fresh state.
        is_significant_gap = (
            ctx.last_segment_end_time_ms
            and (chunk_start_ms - ctx.last_segment_end_time_ms)
            >= self.config.significant_gap_sec * MS_PER_SECOND
        )

        if is_significant_gap and ctx.current_buffer:
            logger.info("Significant gap detected. Flushing preceding continuous audio.")
            if ctx.transmission_start_time_ms is None:
                yield beam.pvalue.TaggedOutput(
                    DEAD_LETTER_QUEUE_TAG,
                    {
                        "error": "State mismatch: transmission has buffer but no start_time. Dropping segment.",
                        "feed_id": feed_id,
                    },
                )
            else:
                yield FlushRequest(
                    buffer=ctx.current_buffer,
                    feed_id=feed_id,
                    processed_uuids=set(ctx.processed_uuids),
                    start_ms=ctx.transmission_start_time_ms,
                    end_ms=ctx.last_segment_end_time_ms,
                )

            # Reset state for the current new segment following the gap
            state.clear_all()
            ctx.current_buffer = None
            ctx.processed_uuids = set()
            ctx.transmission_start_time_ms = None

        self._append_speech_segment(
            chunk_data=chunk_data,
            ctx=ctx,
            state=state,
        )

        # Because the buffer was successfully mutated without triggering a flush, we must 
        # explicitly schedule the stale timer to ensure Dataflow eventually flushes this 
        # stranded audio if the radio stream suddenly goes offline.
        if state.stale_timer:
            transmission_start_time_ms = state.stale_start_time.read()
            if transmission_start_time_ms:
                expected_stale_deadline = (
                    transmission_start_time_ms / MS_PER_SECOND
                ) + self.config.stale_timeout_sec
                if state.stale_timer is not None:
                    state.stale_timer.set(Timestamp(seconds=expected_stale_deadline))

    def process(  # type: ignore[override] # noqa: PLR0913
        self,
        element: tuple[str, str],
        transmission_buffer: ReadModifyWriteRuntimeState = TRANSMISSION_BUFFER_STATE,  # type: ignore
        last_end_time: ReadModifyWriteRuntimeState = LAST_END_TIME_STATE,  # type: ignore
        stale_start_time: ReadModifyWriteRuntimeState = STALE_START_TIME_STATE,  # type: ignore
        contributing_uuids: ReadModifyWriteRuntimeState = CONTRIBUTING_UUIDS_STATE,  # type: ignore
        stale_timer: RuntimeTimer = STALE_TIMER_PARAM,  # type: ignore
    ) -> Generator[TranscriptionResult | beam.pvalue.TaggedOutput, None, None]:
        key, gcs_path = element
        state = TransmissionState(
            buffer=transmission_buffer,
            last_end_time=last_end_time,
            stale_start_time=stale_start_time,
            contributing_uuids=contributing_uuids,
            stale_timer=stale_timer,
        )

        fetched_results = self._fetch_and_validate_audio(
            feed_id=key,
            gcs_path=gcs_path,
            processed_uuids=set(state.contributing_uuids.read() or []),
        )

        flush_queue = []

        for result in fetched_results:
            if isinstance(result, beam.pvalue.TaggedOutput):
                yield result
            else:
                source_file_uuid, chunk_data = result
                chunk_outputs = self._process_audio_chunk(
                    feed_id=key,
                    gcs_path=gcs_path,
                    source_file_uuid=source_file_uuid,
                    chunk_data=chunk_data,
                    state=state,
                )
                for out in chunk_outputs:
                    if isinstance(out, beam.pvalue.TaggedOutput):
                        yield out
                    elif isinstance(out, FlushRequest):
                        flush_queue.append(out)

        # Batch execute all transcription API calls concurrently
        if flush_queue:
            futures = [
                self.executor.submit(
                    self._flush_buffer,
                    buffer=req.buffer,
                    feed_id=req.feed_id,
                    processed_uuids=req.processed_uuids,
                    start_ms=req.start_ms,
                    end_ms=req.end_ms,
                )
                for req in flush_queue
            ]
            for future in futures:
                try:
                    transcribed = future.result()
                    if transcribed:
                        yield transcribed
                except Exception as e:
                    self.dlq_count.inc()
                    logger.exception("Error concurrently flushing buffer for feed %s", key)
                    msg = str(e)
                    yield beam.pvalue.TaggedOutput(
                        DEAD_LETTER_QUEUE_TAG, {"error": msg, "feed_id": key}
                    )

    @on_timer(STALE_TIMER_SPEC)
    def handle_stale_transmission(
        self,
        key: str = beam.DoFn.KeyParam,  # type: ignore
        transmission_buffer: ReadModifyWriteRuntimeState = TRANSMISSION_BUFFER_STATE,  # type: ignore
        last_end_time: ReadModifyWriteRuntimeState = LAST_END_TIME_STATE,  # type: ignore
        stale_start_time: ReadModifyWriteRuntimeState = STALE_START_TIME_STATE,  # type: ignore
        contributing_uuids: ReadModifyWriteRuntimeState = CONTRIBUTING_UUIDS_STATE,  # type: ignore
    ) -> Generator[TranscriptionResult | beam.pvalue.TaggedOutput, None, None]:
        """
        Invoked asynchronously by the Beam Runner when the event-time watermark 
        passes the timestamp previously scheduled on the `stale_timer`. This provides a critical 
        safety net: if a radio feed abruptly drops offline, this timer guarantees that any 
        audio remaining in the buffer will eventually be flushed and transcribed, preventing 
        data loss from stranded state.
        """
        state = TransmissionState(
            buffer=transmission_buffer,
            last_end_time=last_end_time,
            stale_start_time=stale_start_time,
            contributing_uuids=contributing_uuids,
        )

        start_time_ms = state.stale_start_time.read()
        end_time_ms = state.last_end_time.read()
        processed_uuids = set(state.contributing_uuids.read() or [])
        audio_buffer = self._load_current_buffer(state.buffer)

        if audio_buffer and start_time_ms and end_time_ms:
            try:
                self.stale_flush_count.inc()
                logger.info("Flushing stale transmission buffer for feed: %s", key)

                transcribed = self._flush_buffer(
                    buffer=audio_buffer,
                    feed_id=key,
                    processed_uuids=processed_uuids,
                    start_ms=int(start_time_ms),
                    end_ms=int(end_time_ms),
                )
                if transcribed:
                    yield transcribed
            except Exception as e:
                self.dlq_count.inc()
                logger.exception("Error flushing stale buffer for feed %s", key)
                msg = str(e)
                yield beam.pvalue.TaggedOutput(
                    DEAD_LETTER_QUEUE_TAG,
                    {"error": msg, "feed_id": key, "stale_flush": True},
                )

        state.clear_all()
