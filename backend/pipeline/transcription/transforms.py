import io
import logging
import time
import uuid
from collections.abc import Callable, Generator
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any

import apache_beam as beam
from apache_beam import window  # type: ignore[import-untyped, unresolved-import]
from apache_beam.io.gcp.pubsub import PubsubMessage
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

from backend.pipeline.schema_types.transcribed_audio_pb2 import (
    TranscribedAudio,
)
from backend.pipeline.transcription.audio_processor import AudioProcessor, AudioChunkData
from backend.pipeline.transcription.constants import (
    AUDIO_FORMAT,
    DEAD_LETTER_QUEUE_TAG,
    MS_PER_SECOND,
)
from backend.pipeline.transcription.enums import (
    MetricsExporterType,
    TranscriberType,
    VadType,
)
from backend.pipeline.transcription.telemetry import get_metrics_exporter
from backend.pipeline.transcription.transcribers import Transcriber, get_transcriber

logger = logging.getLogger(__name__)

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

STALE_TIMER_SPEC = TimerSpec("stale_timer", beam.TimeDomain.REAL_TIME)
STALE_TIMER_PARAM = beam.DoFn.TimerParam(STALE_TIMER_SPEC)


@dataclass(frozen=True)
class TranscriptionResult:
    """Picklable dataclass to hold intermediate transcription results before Protobuf serialization."""

    feed_id: str
    audio_ids: list[str]
    transcript: str
    start_sec: float
    end_sec: float


@dataclass(frozen=True)
class TransmissionState:
    """Groups Beam state and timer parameters for easier passing between helper methods."""

    buffer: ReadModifyWriteRuntimeState
    last_end_time: ReadModifyWriteRuntimeState
    stale_start_time: ReadModifyWriteRuntimeState
    contributing_uuids: ReadModifyWriteRuntimeState
    stale_timer: RuntimeTimer | None = None

    def clear_all(self) -> None:
        """Unconditionally clears all transmission states and timers."""
        self.buffer.clear()
        self.last_end_time.clear()
        self.stale_start_time.clear()
        self.contributing_uuids.clear()
        if self.stale_timer:
            self.stale_timer.clear()


@dataclass
class ChunkContext:
    """Groups context variables for processing a chunk to reduce function arguments."""

    feed_id: str
    source_file_uuid: str
    current_buffer: AudioSegment | None
    processed_uuids: set[str]
    last_segment_end_time: float
    transmission_start_time: float | None
    chunk_start_sec: float


@dataclass(frozen=True)
class StitchAndTranscribeConfig:
    """Groups pipeline-level configurations passed to the stateful DoFn."""

    project_id: str
    transcriber_type: TranscriberType
    transcriber_config: str
    vad_type: VadType
    vad_config: str
    metrics_exporter_type: str
    metrics_config: str
    significant_gap_sec: float
    stale_timeout_sec: float


@dataclass(frozen=True)
class FlushRequest:
    """Encapsulates the data required to flush an audio buffer to the transcription API."""

    buffer: AudioSegment
    feed_id: str
    processed_uuids: set[str]
    start_sec: float
    end_sec: float


class ParseAndKeyFn(beam.DoFn):
    """
    Extracts the feed_id and builds the GCS URI from Pub/Sub attributes.
    Routes messages missing required attributes to the DLQ.
    """

    def process(
        self, element: PubsubMessage, *args: Any, **kwargs: Any
    ) -> Generator[tuple[str, str] | beam.pvalue.TaggedOutput, None, None]:
        try:
            feed_id = element.attributes["feed_id"]
            bucket_id = element.attributes["bucketId"]
            object_id = element.attributes["objectId"]
            yield (feed_id, f"gs://{bucket_id}/{object_id}")
        except KeyError as e:
            msg = f"Missing required payload attribute: {e}"
            logger.exception(msg)
            yield beam.pvalue.TaggedOutput(
                DEAD_LETTER_QUEUE_TAG,
                {
                    "error": msg,
                    "attributes": dict(element.attributes),
                },
            )


class AddEventTimestamp(beam.DoFn):
    """
    Extracts the event timestamp from the actual string filename of the audio chunk and
    assigns it as the Beam windowing `TimestampedValue`.
    Assumes format: gs://bucket/hash/feed_id/YYYY-MM-DD/<unix-timestamp>-uuid.flac
    """

    def process(
        self, element: tuple[str, str], *args: Any, **kwargs: Any
    ) -> Generator[tuple[str, str] | beam.pvalue.TaggedOutput, None, None]:
        _, gcs_path = element
        _, _, filename = gcs_path.rpartition("/")
        timestamp_str, _, _ = filename.partition("-")

        if timestamp_str.isdigit():
            yield window.TimestampedValue(element, int(timestamp_str))
        else:
            msg = f"Could not parse timestamp from {gcs_path}"
            raise ValueError(msg)


class SerializeToPubSubMessageFn(beam.DoFn):
    """
    Converts a `TranscriptionResult` dataclass into a serialized `TranscribedAudio` Protobuf payload
    and wraps it in a `PubsubMessage` for downstream publishing.
    """

    def process(
        self, element: TranscriptionResult, *args: Any, **kwargs: Any
    ) -> Generator[PubsubMessage, None, None]:
        proto = TranscribedAudio(
            feed_id=element.feed_id,
            source_chunk_ids=element.audio_ids,
            transmission_id=str(uuid.uuid4()),
            transcript=element.transcript,
        )
        proto.start_timestamp.FromMicroseconds(int(element.start_sec * 1e6))
        proto.end_timestamp.FromMicroseconds(int(element.end_sec * 1e6))
        yield PubsubMessage(
            data=proto.SerializeToString(),
            attributes={},
            ordering_key=element.feed_id,
        )


class StitchAndTranscribeFn(beam.DoFn):
    """
    A stateful Beam DoFn responsible for maintaining chronological continuous audio state per radio feed.
    It buffers incoming audio chunks, runs Voice Activity Detection (VAD) to identify speech segments,
    and flushes completed speech segments concurrently to the configured `Transcriber` plugin.
    Outputs `TranscriptionResult` objects containing the final transcripts.
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
        start_sec: float,
        end_sec: float,
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
            start_sec=start_sec,
            end_sec=end_sec,
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
        start_sec: float,
        end_sec: float,
    ) -> TranscriptionResult | None:
        if not buffer or len(buffer) == 0:
            return None

        transcribed_bytes = self._export_and_transcribe(
            audio_buffer=buffer,
            feed_id=feed_id,
            source_uuids=processed_uuids,
            start_sec=start_sec,
            end_sec=end_sec,
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
        Downloads audio bytes and SAD metadata from GCS.
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
            # Fallback to parsing filename for chunk_start_sec if absent in proto
            chunk_start_sec = chunk_data.start_sec
            if chunk_start_sec is None:
                _, _, filename = gcs_path.rpartition("/")
                timestamp_str, _, _ = filename.partition("-")
                chunk_start_sec = float(timestamp_str) if timestamp_str.isdigit() else 0.0

            # Override with fallback if needed
            chunk_data = AudioChunkData(
                start_sec=chunk_start_sec,
                audio=chunk_data.audio,
                speech_segments=chunk_data.speech_segments,
            )

            yield source_file_uuid, chunk_data
        except Exception as e:
            self.dlq_count.inc()
            action = "downloading" if isinstance(e, FileNotFoundError) else "processing"
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
    ) -> Generator[TranscriptionResult | beam.pvalue.TaggedOutput, None, None]:
        """
        Since this file has no speech, we immediately flush any unfinished transmission
        we were holding in state up to this point and then clear all state parameters.
        """
        if ctx.current_buffer:
            logger.info("Silent file received. Flushing previous transmission.")
            start_time = (
                ctx.transmission_start_time if ctx.transmission_start_time is not None else ctx.chunk_start_sec
            )
            end_time = ctx.last_segment_end_time or start_time + (
                len(ctx.current_buffer) / float(MS_PER_SECOND)
            )
            try:
                transcribed = self._flush_buffer(
                    buffer=ctx.current_buffer,
                    feed_id=ctx.feed_id,
                    processed_uuids=ctx.processed_uuids,
                    start_sec=start_time,
                    end_sec=end_time,
                )
                if transcribed:
                    yield transcribed
            except Exception as e:
                self.dlq_count.inc()
                logger.exception("Error processing %s for feed %s", gcs_path, feed_id)
                msg = str(e)
                yield beam.pvalue.TaggedOutput(
                    DEAD_LETTER_QUEUE_TAG, {"error": msg, "feed_id": feed_id}
                )

        state.clear_all()

    def _trigger_gap_flush(
        self,
        *,
        ctx: ChunkContext,
        state: TransmissionState,
    ) -> tuple[FlushRequest, set[str]]:
        logger.info("Gap detected. Flushing complete transmission.")
        emit_start = state.stale_start_time.read()
        if emit_start is None:
            emit_start = (
                ctx.transmission_start_time if ctx.transmission_start_time is not None else ctx.chunk_start_sec
            )

        if ctx.current_buffer is None:
            msg = "Cannot flush empty buffer."
            raise RuntimeError(msg)

        flush_req = FlushRequest(
            buffer=ctx.current_buffer,
            feed_id=ctx.feed_id,
            processed_uuids=set(
                ctx.processed_uuids
            ),  # Create copy before clearing state
            start_sec=emit_start,
            end_sec=ctx.last_segment_end_time,
        )

        state.clear_all()
        return flush_req, set()

    def _stitch_speech_segments(
        self,
        *,
        speech_segments: list[tuple[float, float]],
        full_audio_segment: AudioSegment,
        ctx: ChunkContext,
        state: TransmissionState,
    ) -> list[FlushRequest]:
        pending_flushes = []

        for rel_start_sec, rel_end_sec in speech_segments:
            abs_start_sec = ctx.chunk_start_sec + rel_start_sec
            abs_end_sec = ctx.chunk_start_sec + rel_end_sec

            if ctx.last_segment_end_time > 0 and (
                abs_start_sec - ctx.last_segment_end_time
            ) > self.config.significant_gap_sec:
                if ctx.current_buffer:
                    flush_req, new_processed_uuids = self._trigger_gap_flush(
                        ctx=ctx,
                        state=state,
                    )
                    pending_flushes.append(flush_req)
                    ctx.processed_uuids = new_processed_uuids

                ctx.current_buffer = None

            if ctx.current_buffer is None and ctx.transmission_start_time is None:
                ctx.transmission_start_time = abs_start_sec

            new_segment = full_audio_segment[
                int(rel_start_sec * MS_PER_SECOND) : int(rel_end_sec * MS_PER_SECOND)
            ]
            ctx.current_buffer = (
                ctx.current_buffer + new_segment if ctx.current_buffer else new_segment
            )
            ctx.last_segment_end_time = abs_end_sec

            ctx.processed_uuids.add(ctx.source_file_uuid)

        return pending_flushes

    def _execute_concurrent_flushes(
        self, flush_queue: list[FlushRequest], *, is_stale_flush: bool = False
    ) -> Generator[TranscriptionResult | beam.pvalue.TaggedOutput, None, None]:
        logger.info(
            "Executing %d concurrent API transciptions.",
            len(flush_queue),
        )
        futures = [
            (
                req.feed_id,
                self.executor.submit(
                    self._flush_buffer,
                    buffer=req.buffer,
                    feed_id=req.feed_id,
                    processed_uuids=req.processed_uuids,
                    start_sec=req.start_sec,
                    end_sec=req.end_sec,
                ),
            )
            for req in flush_queue
        ]
        for feed_id, future in futures:
            try:
                transcribed = future.result()
                if transcribed:
                    yield transcribed
            except Exception as e:
                self.dlq_count.inc()
                error_msg = f"Error processing flush for feed {feed_id}"
                if is_stale_flush:
                    error_msg += " (stale flush)"
                logger.exception(error_msg)
                msg = str(e)
                yield beam.pvalue.TaggedOutput(
                    DEAD_LETTER_QUEUE_TAG,
                    {
                        "error": msg,
                        "feed_id": feed_id,
                        "is_stale_flush": is_stale_flush,
                    },
                )

    def _persist_or_clear_state(
        self,
        *,
        ctx: ChunkContext,
        state: TransmissionState,
        force: bool = False,
    ) -> None:
        if ctx.current_buffer and (force or len(ctx.current_buffer) > 0):
            logger.info("Persisting potentially incomplete transmission to state.")
            buf = io.BytesIO()
            ctx.current_buffer.export(buf, format=AUDIO_FORMAT)
            state.buffer.write(buf.getvalue())
            state.last_end_time.write(ctx.last_segment_end_time)

            if ctx.transmission_start_time is not None:
                existing_start = state.stale_start_time.read()
                if existing_start is None:
                    state.stale_start_time.write(ctx.transmission_start_time)

            state.contributing_uuids.write(ctx.processed_uuids)
            if state.stale_timer:
                state.stale_timer.set(
                    Timestamp(time.time() + self.config.stale_timeout_sec)
                )
        else:
            state.clear_all()

    # The Beam State API uses argument injection, inextricably inflating the argument count.
    def process(  # noqa: PLR0913
        self,
        element: tuple[str, str],
        transmission_buffer: ReadModifyWriteRuntimeState = TRANSMISSION_BUFFER_STATE,  # type: ignore[invalid-parameter-default]
        last_end_time_state: ReadModifyWriteRuntimeState = LAST_END_TIME_STATE,  # type: ignore[invalid-parameter-default]
        stale_start_time_state: ReadModifyWriteRuntimeState = STALE_START_TIME_STATE,  # type: ignore[invalid-parameter-default]
        contributing_uuids_state: ReadModifyWriteRuntimeState = CONTRIBUTING_UUIDS_STATE,  # type: ignore[invalid-parameter-default]
        stale_timer: RuntimeTimer = STALE_TIMER_PARAM,  # type: ignore[invalid-parameter-default]
        *args: Any,
        **kwargs: Any,
    ) -> Generator[TranscriptionResult | beam.pvalue.TaggedOutput, None, None]:  # type: ignore[invalid-method-override]
        feed_id, gcs_path = element
        state = TransmissionState(
            buffer=transmission_buffer,
            last_end_time=last_end_time_state,
            stale_start_time=stale_start_time_state,
            contributing_uuids=contributing_uuids_state,
            stale_timer=stale_timer,
        )

        processed_uuids = set(state.contributing_uuids.read() or [])

        audio_data = None
        for r in self._fetch_and_validate_audio(
            feed_id=feed_id, gcs_path=gcs_path, processed_uuids=processed_uuids
        ):
            if isinstance(r, beam.pvalue.TaggedOutput):
                yield r
            else:
                audio_data = r

        if not audio_data:
            return

        source_file_uuid, chunk_data = audio_data

        current_buffer = self._load_current_buffer(state.buffer)
        last_segment_end_time = state.last_end_time.read() or 0.0

        ctx = ChunkContext(
            feed_id=feed_id,
            source_file_uuid=source_file_uuid,
            current_buffer=current_buffer,
            processed_uuids=processed_uuids,
            last_segment_end_time=last_segment_end_time,
            transmission_start_time=state.stale_start_time.read() or chunk_data.start_sec
            if not chunk_data.speech_segments
            else None,
            chunk_start_sec=chunk_data.start_sec,
        )

        if not chunk_data.speech_segments:
            logger.info("No speech segments in chunk %s. Dropping.", gcs_path)
            self.vad_silence_count.inc()
            yield from self._handle_silent_file(
                feed_id=feed_id,
                gcs_path=gcs_path,
                ctx=ctx,
                state=state,
            )
            return

        pending_flushes = self._stitch_speech_segments(
            speech_segments=chunk_data.speech_segments,
            full_audio_segment=chunk_data.audio,
            ctx=ctx,
            state=state,
        )
        if pending_flushes:
            yield from self._execute_concurrent_flushes(flush_queue=pending_flushes)

        self._persist_or_clear_state(
            ctx=ctx,
            state=state,
        )

    # The Beam Timer API also relies on dependency injection, hence the argument inflation.
    @on_timer(STALE_TIMER_SPEC)
    def handle_stale_buffer(  # noqa: PLR0913
        self,
        key: str = beam.DoFn.KeyParam,  # type: ignore[invalid-parameter-default]
        timestamp: Timestamp = beam.DoFn.TimestampParam,  # type: ignore[invalid-parameter-default]
        transmission_buffer: ReadModifyWriteRuntimeState = TRANSMISSION_BUFFER_STATE,  # type: ignore[invalid-parameter-default]
        last_end_time_state: ReadModifyWriteRuntimeState = LAST_END_TIME_STATE,  # type: ignore[invalid-parameter-default]
        stale_start_time_state: ReadModifyWriteRuntimeState = STALE_START_TIME_STATE,  # type: ignore[invalid-parameter-default]
        contributing_uuids_state: ReadModifyWriteRuntimeState = CONTRIBUTING_UUIDS_STATE,  # type: ignore[invalid-parameter-default]
    ) -> Generator[TranscriptionResult | beam.pvalue.TaggedOutput, None, None]:
        state = TransmissionState(
            buffer=transmission_buffer,
            last_end_time=last_end_time_state,
            stale_start_time=stale_start_time_state,
            contributing_uuids=contributing_uuids_state,
            stale_timer=None,
        )

        buffered_bytes = state.buffer.read()

        if buffered_bytes:
            logger.info(
                "Stale timer fired for feed %s. Flushing incomplete transmission.", key
            )
            processed_uuids = set(state.contributing_uuids.read() or [])
            start_time = state.stale_start_time.read() or 0.0

            try:
                self.stale_flush_count.inc()
                audio_buffer = AudioSegment.from_file(
                    io.BytesIO(buffered_bytes), format=AUDIO_FORMAT
                )
                end_time = last_end_time_state.read() or start_time + (
                    len(audio_buffer) / float(MS_PER_SECOND)
                )
                transcribed = self._flush_buffer(
                    buffer=audio_buffer,
                    feed_id=key,
                    processed_uuids=processed_uuids,
                    start_sec=start_time,
                    end_sec=end_time,
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
