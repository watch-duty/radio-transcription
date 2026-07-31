"""Serverless Cloud Run / Cloud Function transcription entry point.

Triggered by Pub/Sub push events containing serialized NormalizedAudio
claim-check metadata. Delegates processing to TranscriptionEventProcessor.
"""

import asyncio
import logging
import os
from collections.abc import AsyncGenerator
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request, Response, status
from google.cloud import pubsub_v1

from backend.pipeline.common.clients import audio_segments_client
from backend.pipeline.common.container_helper import ForkDetector, fork_checked
from backend.pipeline.common.log_helper import setup_logging
from backend.pipeline.common.tracing_utils import (
    setup_tracing,
)
from backend.pipeline.transcription.enums import TranscriberType
from backend.pipeline.transcription.processor import (
    TranscriptionEventProcessor,
    is_transient_exception,
)
from backend.pipeline.transcription.transcribers.base import Transcriber
from backend.pipeline.transcription.transcribers.factory import get_transcriber

# Setup Logging
setup_logging()
logger = logging.getLogger(__name__)


class MissingOutputTopicError(ValueError):
    """Raised when OUTPUT_TOPIC is unset.

    This is expected in local/test environments that don't provision Pub/Sub
    topics, so eager_warmup() tolerates specifically this exception type and
    lets the service start in a degraded state. Other configuration failures
    (e.g. an invalid TRANSCRIBER_CONFIG) raise plain ValueError/pydantic
    ValidationError and are deliberately left uncaught, since those are
    deterministic and will never succeed on retry.
    """


class TranscriptionServiceContainer:
    """Encapsulates the warm-started cached service container instances for GCF."""

    def __init__(self) -> None:
        self._fork_detector = ForkDetector(self.reset_clients)
        self._transcriber: Transcriber | None = None
        self._publisher: pubsub_v1.PublisherClient | None = None
        self._processor: TranscriptionEventProcessor | None = None

    def reset_clients(self) -> None:
        if self._publisher is not None:
            try:
                close_fn = getattr(self._publisher, "close", None)
                if close_fn is not None:
                    close_fn()
            except Exception:
                logger.exception(
                    "Failed to close Pub/Sub publisher client on fork reset"
                )
        self._transcriber = None
        self._publisher = None
        self._processor = None

    @property
    def processor(self) -> TranscriptionEventProcessor | None:
        """Returns the cached processor if it has been warmed up, otherwise None."""
        return self._processor

    @fork_checked
    def get_transcriber(self, project_id: str) -> Transcriber:
        """Warms up and caches the transcriber instance.

        Args:
            project_id: The Google Cloud Project ID.

        Returns:
            The cached Transcriber instance.
        """
        if self._transcriber is None:
            t_type_str = os.environ.get("TRANSCRIBER_TYPE", "GOOGLE_CHIRP_V3")
            t_config_json = os.environ.get("TRANSCRIBER_CONFIG", "{}")
            t_type = TranscriberType(t_type_str.lower())
            logger.info("Initializing transcriber type %s", t_type.name)
            self._transcriber = get_transcriber(
                t_type, project_id, t_config_json
            )
            self._transcriber.setup()
        return self._transcriber

    @fork_checked
    def get_publisher(self) -> pubsub_v1.PublisherClient:
        """Warms up and caches the Pub/Sub publisher client with ordering enabled.

        Returns:
            The cached PubSub PublisherClient instance.
        """
        if self._publisher is None:
            logger.info("Initializing Pub/Sub PublisherClient")
            publisher_options = pubsub_v1.types.PublisherOptions(
                enable_message_ordering=True
            )
            self._publisher = pubsub_v1.PublisherClient(
                publisher_options=publisher_options
            )
        return self._publisher

    @fork_checked
    def get_processor(self) -> TranscriptionEventProcessor:
        """Warms up and caches the Event Processor instance.

        Returns:
            The cached TranscriptionEventProcessor instance.

        Raises:
            MissingOutputTopicError: If the OUTPUT_TOPIC environment variable
                is not set.
        """
        if self._processor is None:
            project_id = os.environ.get("PROJECT_ID", "watch-duty-dev")
            output_topic = os.environ.get("OUTPUT_TOPIC")
            if not output_topic:
                msg = "OUTPUT_TOPIC environment variable must be set"
                logger.error(msg)
                raise MissingOutputTopicError(msg)

            transcriber = self.get_transcriber(project_id)
            publisher = self.get_publisher()

            api_url = os.environ.get("AUDIO_SEGMENTS_API_URL")
            audio_segments_client_instance = None
            if api_url:
                logger.info(
                    "Initializing AsyncAudioSegmentsClient at: %s", api_url
                )
                audio_segments_client_instance = (
                    audio_segments_client.AsyncAudioSegmentsClient(
                        api_url=api_url
                    )
                )
            else:
                logger.error(
                    "Missing AUDIO_SEGMENTS_API_URL environment variable."
                    "Transcripts will not be written to DB."
                )

            self._processor = TranscriptionEventProcessor(
                project_id=project_id,
                output_topic=output_topic,
                transcriber=transcriber,
                publisher=publisher,
                audio_segments_client=audio_segments_client_instance,
            )
        return self._processor

    def eager_warmup(self) -> None:
        """Eagerly warms up and caches all gRPC clients during container initialization.

        Only MissingOutputTopicError is tolerated here, since that's the one
        expected/benign gap in local and test environments that don't
        provision Pub/Sub topics. Everything else — including invalid
        transcriber configuration (e.g. GEMINI_USER_PROMPT unset,
        unrecognized TRANSCRIBER_TYPE) — is deterministic and will never
        resolve on retry, so it's left to propagate and fail container
        startup outright. That way a misconfigured deployment fails to come
        up (and Cloud Run keeps routing to the last good revision) instead of
        starting "successfully" and then returning 500 for every request.
        """
        logger.info("Performing eager warm-start for container services...")
        try:
            self.get_processor()
        except MissingOutputTopicError as e:
            logger.warning(
                "Eager warm-start skipped: %s (expected in some test/local envs).",
                e,
            )
            return
        logger.info("Container services eagerly warmed up successfully.")


def _setup_default_executor() -> ThreadPoolExecutor:
    """Configures and binds a ThreadPoolExecutor to the active event loop."""
    concurrency_limit_str = os.environ.get("CONTAINER_CONCURRENCY", "128")
    try:
        concurrency_limit = int(concurrency_limit_str)
    except ValueError as e:
        logger.warning(
            "Invalid CONTAINER_CONCURRENCY value: %r (%s). Falling back to 128.",
            concurrency_limit_str,
            e,
        )
        concurrency_limit = 128

    if concurrency_limit <= 0:
        logger.warning(
            "Invalid CONTAINER_CONCURRENCY value: %d must be greater than 0. Falling back to 128.",
            concurrency_limit,
        )
        concurrency_limit = 128

    executor = ThreadPoolExecutor(
        max_workers=concurrency_limit,
        thread_name_prefix="asyncio_default_executor",
    )
    asyncio.get_running_loop().set_default_executor(executor)
    logger.info(
        "Configured event loop default executor with %d threads.",
        concurrency_limit,
    )
    return executor


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None]:
    """Warms up container services on startup and resets/closes them on shutdown."""
    executor = _setup_default_executor()
    container = TranscriptionServiceContainer()
    try:
        container.eager_warmup()
        if container.processor:
            app.state.processor = container.processor
        yield
    finally:
        # Clean up client connection pools/channels on exit
        if container.processor:
            processor = container.processor
            if processor.transcriber:
                try:
                    await processor.transcriber.close()
                except Exception:
                    logger.exception(
                        "Failed to close transcriber client on lifespan shutdown"
                    )
            if processor.audio_segments_client:
                try:
                    await processor.audio_segments_client.close()
                except Exception:
                    logger.exception(
                        "Failed to close audio segments client on lifespan shutdown"
                    )
        container.reset_clients()
        executor.shutdown(wait=False)


app = FastAPI(title="Transcription Service ASGI", lifespan=lifespan)


@app.post("/", status_code=status.HTTP_204_NO_CONTENT)
async def transcribe_claim_check(envelope: dict, request: Request) -> Response:
    """Entry point for Pub/Sub push HTTP POST requests."""
    setup_tracing(service_name="transcription-service", use_batch=False)

    processor = getattr(request.app.state, "processor", None)
    if not processor:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Transcription service is not initialized",
        )

    try:
        await processor.process_event(envelope)
    except Exception as e:
        if is_transient_exception(e):
            # Suppress noisy Uvicorn tracebacks while preserving Pub/Sub retry behavior
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=f"Transient error processing message: {e}",
            )
        raise  # Re-raise permanent/unexpected bugs so they are properly logged
    return Response(status_code=status.HTTP_204_NO_CONTENT)
