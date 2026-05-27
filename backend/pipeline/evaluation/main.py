from __future__ import annotations

import logging
import math
import os
from typing import TYPE_CHECKING

import functions_framework

if TYPE_CHECKING:
    import asyncpg
    from cloudevents.http import event as cloudevent

from backend.pipeline.common.clients import pubsub_client
from backend.pipeline.common.clients.transcripts_client import TranscriptsClient
from backend.pipeline.common.logging import setup_logging
from backend.pipeline.common.tracing_utils import setup_tracing
from backend.pipeline.evaluation import service
from backend.pipeline.evaluation.processor import EvaluationEventProcessor
from backend.pipeline.evaluation.rules_evaluation import evaluator
from backend.pipeline.storage.audio_segment_store import AudioSegmentStore
from backend.pipeline.storage.connection import create_pool_with_retry

# 1. Setup Logging and Tracing
setup_logging()
setup_tracing(use_batch=False)
logger = logging.getLogger(__name__)
DEFAULT_RULES_CACHE_TTL_SECONDS = 60.0


def _get_rules_cache_ttl_seconds() -> float:
    raw_value = os.environ.get("RULES_CACHE_TTL_SECONDS")
    if raw_value is None:
        return DEFAULT_RULES_CACHE_TTL_SECONDS

    try:
        ttl_seconds = float(raw_value)
    except ValueError:
        logger.warning(
            "Invalid RULES_CACHE_TTL_SECONDS value %r. Defaulting to %.1f.",
            raw_value,
            DEFAULT_RULES_CACHE_TTL_SECONDS,
        )
        return DEFAULT_RULES_CACHE_TTL_SECONDS

    if math.isnan(ttl_seconds):
        logger.warning(
            "NaN RULES_CACHE_TTL_SECONDS value. Defaulting to %.1f.",
            DEFAULT_RULES_CACHE_TTL_SECONDS,
        )
        return DEFAULT_RULES_CACHE_TTL_SECONDS

    if ttl_seconds < 0:
        logger.warning(
            "Negative RULES_CACHE_TTL_SECONDS value %r. Defaulting to %.1f.",
            raw_value,
            DEFAULT_RULES_CACHE_TTL_SECONDS,
        )
        return DEFAULT_RULES_CACHE_TTL_SECONDS

    return ttl_seconds


# 2. Container for lazy initialization (for performance on warm starts)
class EvaluationServiceContainer:
    def __init__(self) -> None:
        self._pool: asyncpg.Pool | None = None
        self._processor: EvaluationEventProcessor | None = None
        self._evaluation_service: service.EvaluationService | None = None
        self._transcripts_client: TranscriptsClient | None = None
        self._publisher: pubsub_client.PubSubClient | None = None

    def get_transcripts_client(self) -> TranscriptsClient:
        if self._transcripts_client is None:
            url = os.environ.get("TRANSCRIPTS_API_URL")
            if not url:
                msg = "TRANSCRIPTS_API_URL environment variable is not set."
                raise ValueError(msg)
            self._transcripts_client = TranscriptsClient(api_url=url)
        return self._transcripts_client

    def get_publisher(self) -> pubsub_client.PubSubClient:
        if self._publisher is None:
            self._publisher = pubsub_client.PubSubClient()
        return self._publisher

    def get_evaluation_service(self) -> service.EvaluationService:
        if self._evaluation_service is None:
            url = os.environ.get("RULES_API_URL")
            if url:
                logger.info("Using RemoteTextEvaluator with API: %s", url)
                ttl_seconds = _get_rules_cache_ttl_seconds()
                text_evaluator = evaluator.RemoteTextEvaluator(
                    api_url=url,
                    cache_ttl_seconds=ttl_seconds,
                )
            else:
                logger.info("Using StaticTextEvaluator (no RULES_API_URL set)")
                text_evaluator = evaluator.StaticTextEvaluator()
            self._evaluation_service = service.EvaluationService(
                text_evaluator=text_evaluator
            )
        return self._evaluation_service

    async def get_processor(self) -> EvaluationEventProcessor:
        # Re-initialize the connection pool if it is None or if the event loop
        # it was created on has been closed (e.g., between Serverless/Cloud Function invocations).
        need_reinit = (
            self._pool is None
            or self._processor is None
            or getattr(self._pool, "_loop", None) is None
            or self._pool._loop.is_closed()  # noqa: SLF001
        )

        if need_reinit:
            logger.info(
                "Initializing or recreating database connection pool..."
            )
            self._pool = await create_pool_with_retry()
            store = AudioSegmentStore(self._pool)

            output_topic = os.environ.get("RULES_EVALUATION_RESULTS_TOPIC")
            if not output_topic:
                msg = "RULES_EVALUATION_RESULTS_TOPIC environment variable is not set."
                raise ValueError(msg)

            self._processor = EvaluationEventProcessor(
                evaluation_service=self.get_evaluation_service(),
                transcripts_client=self.get_transcripts_client(),
                publisher=self.get_publisher(),
                output_topic_path=output_topic,
                audio_segment_store=store,
            )
        if self._processor is None:
            msg = "Processor was not properly initialized."
            raise RuntimeError(msg)
        return self._processor


container = EvaluationServiceContainer()


@functions_framework.cloud_event  # type: ignore
async def evaluate_transcribed_audio_segment(
    cloud_event: cloudevent.CloudEvent,
) -> None:
    """
    Triggered from a message on a Cloud Pub/Sub topic.

    Args:
        cloud_event: The CloudEvent triggered by Pub/Sub.
    """
    proc = await container.get_processor()
    await proc.process_event(cloud_event)
