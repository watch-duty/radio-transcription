"""Physical Feed-batch pipeline for SID-owned Broadcastify Calls."""

from __future__ import annotations

import asyncio
import collections.abc  # noqa: TC003 - runtime dataclass annotation
import dataclasses
import datetime
import logging
import typing

from backend.pipeline.common import gcp_helper
from backend.pipeline.ingestion import (
    audio_pipeline,
    grant_control,
    models,
    retry,
    settings,
)
from backend.pipeline.ingestion.collectors import failure_classification
from backend.pipeline.ingestion.collectors.bcfy_calls import (
    bcfy_calls_collector,
    provider,
)
from backend.pipeline.ingestion.failure_classifiers import pubsub
from backend.pipeline.storage import (
    feed_store,
    ingestion_lease_contracts,
    ingestion_lease_store,
)

if typing.TYPE_CHECKING:
    from backend.pipeline.common.clients import gcs_client, pubsub_client

logger = logging.getLogger(__name__)

_GCS_UPLOAD_FAILED = "gcs_upload_failed"
_PUBLISH_AFTER_BOOKMARK_FAILED = (
    feed_store.FeedStatusReason.PIPELINE_PUBLISH_AFTER_BOOKMARK_FAILED
)


@dataclasses.dataclass(frozen=True, slots=True)
class CallWork:
    """One validated provider call admitted to a Feed batch.

    Attributes:
        payload: Raw call fields needed to construct the audio chunk.
        audio_url: Signed provider URL used for download and deduplication.
    """

    payload: collections.abc.Mapping[str, object]
    audio_url: str


@dataclasses.dataclass(frozen=True, slots=True)
class FeedBatch:
    """Sequential physical work admitted for one Feed under one SID grant.

    Attributes:
        grant: Exact parent SID ownership generation.
        member: Immutable child Feed identity and publication metadata.
        session_id: Stable logical session for this Feed and SID grant.
        starting_sequence: First create-only GCS sequence for this batch.
        calls: Provider calls in execution order.
        grant_lost: Exact parent authority-loss signal for pre-bookmark work.
    """

    grant: ingestion_lease_contracts.LeaseGrant
    member: ingestion_lease_contracts.LeaseMember
    session_id: str
    starting_sequence: int
    calls: tuple[CallWork, ...]
    grant_lost: asyncio.Event


@dataclasses.dataclass(frozen=True, slots=True)
class FeedBatchResult:
    """Compact evidence returned to the SID page settlement owner.

    Attributes:
        attempted_count: Calls whose physical execution began.
        published_count: Calls completing the full physical pipeline.
        next_sequence: First unused Feed-local GCS sequence.
        committed_urls: URLs whose fenced progress commit completed.
        terminal: Selected Feed failure or rejected parent grant.
    """

    attempted_count: int
    published_count: int
    next_sequence: int
    committed_urls: tuple[str, ...]
    terminal: (
        failure_classification.ItemFailure
        | ingestion_lease_contracts.GrantRejected
        | grant_control.RunLost
        | None
    )


def _leased_feed(
    batch: FeedBatch,
) -> feed_store.LeasedFeed:
    """Project immutable membership into the existing GCS helper shape."""
    member = batch.member
    return feed_store.LeasedFeed(
        id=member.identity.feed_id,
        name=member.name,
        source_type=member.identity.source_type,
        last_processed_filename=None,
        last_bookmark_time=member.last_bookmark_time,
        fencing_token=batch.grant.fencing_token,
        failure_count=0,
        status_reason=None,
        source_feed_id=member.identity.source_feed_id,
        tags=None,
    )


class BcfyCallsFeedBatchExecutor:
    """Execute sequential calls while preserving fenced progress ordering."""

    def __init__(
        self,
        *,
        calls_provider: provider.CallsProviderClient,
        lease_store: ingestion_lease_store.IngestionLeaseStore,
        gcs_client: gcs_client.GcsClient,
        pubsub_client: pubsub_client.PubSubClient,
        settings: settings.CollectorSettings,
        topic_path: str,
        actor_id: str,
    ) -> None:
        self._calls_provider = calls_provider
        self._lease_store = lease_store
        self._gcs_client = gcs_client
        self._pubsub_client = pubsub_client
        self._settings = settings
        self._topic_path = topic_path
        self._actor_id = actor_id

    async def execute(  # noqa: PLR0911, PLR0912, PLR0915
        self,
        batch: FeedBatch,
    ) -> FeedBatchResult:
        """Execute one Feed batch in call order."""
        outcome = failure_classification.ItemBatchOutcome()
        published_count = 0
        next_sequence = batch.starting_sequence
        committed_urls: list[str] = []
        no_stop = asyncio.Event()
        feed = _leased_feed(batch)

        for call in batch.calls:
            if batch.grant_lost.is_set():
                return self._lost_authority_result(
                    outcome,
                    published_count,
                    next_sequence,
                    committed_urls,
                )
            outcome.record_attempt()
            sequence = next_sequence
            next_sequence += 1

            try:
                chunk_result = await (
                    bcfy_calls_collector._create_chunk_from_call  # noqa: SLF001
                )(
                    dict(call.payload),
                    call.audio_url,
                    batch.grant_lost,
                    batch.session_id,
                    datetime.datetime.now(datetime.UTC),
                    self._calls_provider,
                )
            except asyncio.CancelledError:
                if batch.grant_lost.is_set():
                    return self._lost_authority_result(
                        outcome,
                        published_count,
                        next_sequence,
                        committed_urls,
                    )
                raise
            if batch.grant_lost.is_set():
                return self._lost_authority_result(
                    outcome,
                    published_count,
                    next_sequence,
                    committed_urls,
                )
            if chunk_result.failure is not None:
                logger.error(
                    "Broadcastify Calls item failed for %s: %s",
                    call.audio_url,
                    chunk_result.failure.reason,
                )
                outcome.record_failure(chunk_result.failure)
                continue
            chunk = chunk_result.chunk
            if chunk is None:
                message = "call chunk result omitted both chunk and failure"
                raise grant_control.GrantControlIntegrityError(message)

            try:
                gcs_uri = await self._upload(
                    feed,
                    chunk,
                    sequence,
                    batch.grant.fencing_token,
                    batch.grant_lost,
                    no_stop,
                )
            except retry.LeaseExpiredError:
                return self._lost_authority_result(
                    outcome,
                    published_count,
                    next_sequence,
                    committed_urls,
                )
            except asyncio.CancelledError:
                if batch.grant_lost.is_set():
                    return self._lost_authority_result(
                        outcome,
                        published_count,
                        next_sequence,
                        committed_urls,
                    )
                raise
            except Exception:
                if batch.grant_lost.is_set():
                    return self._lost_authority_result(
                        outcome,
                        published_count,
                        next_sequence,
                        committed_urls,
                    )
                logger.exception(
                    "GCS upload failed for Broadcastify Calls item %s",
                    call.audio_url,
                )
                return self._result(
                    outcome,
                    published_count,
                    next_sequence,
                    committed_urls,
                    failure_classification.ItemFailure(
                        feed_store.FeedStatusReason.SYSTEM_PIPELINE_ERROR,
                        _GCS_UPLOAD_FAILED,
                    ),
                )
            if batch.grant_lost.is_set():
                return self._lost_authority_result(
                    outcome,
                    published_count,
                    next_sequence,
                    committed_urls,
                )

            commit_result = await self._commit_progress(
                batch,
                chunk,
                gcs_uri,
            )
            if isinstance(
                commit_result,
                ingestion_lease_contracts.GrantRejected,
            ):
                return self._result(
                    outcome,
                    published_count,
                    next_sequence,
                    committed_urls,
                    commit_result,
                )

            child = self._committed_child(commit_result, batch)
            if (
                child.disposition
                is ingestion_lease_contracts.ChildDisposition.REJECTED
            ):
                return self._result(
                    outcome,
                    published_count,
                    next_sequence,
                    committed_urls,
                    None,
                )
            if (
                child.disposition
                is not ingestion_lease_contracts.ChildDisposition.COMMITTED
            ):
                message = "audio progress returned an invalid disposition"
                raise grant_control.GrantControlIntegrityError(message)

            committed_urls.append(call.audio_url)
            try:
                await self._publish(
                    batch.member,
                    chunk,
                    gcs_uri,
                    no_stop,
                )
            except asyncio.CancelledError:
                raise
            except Exception as error:
                reason = pubsub.publish_failure_reason(error)
                logger.exception(
                    "Post-bookmark publish failed for Calls item %s",
                    call.audio_url,
                )
                return self._result(
                    outcome,
                    published_count,
                    next_sequence,
                    committed_urls,
                    failure_classification.ItemFailure(
                        _PUBLISH_AFTER_BOOKMARK_FAILED,
                        reason,
                    ),
                )

            published_count += 1
            outcome.record_chunk_produced()

        return self._result(
            outcome,
            published_count,
            next_sequence,
            committed_urls,
            outcome.promoted_failure(),
        )

    async def _upload(
        self,
        feed: feed_store.LeasedFeed,
        chunk: models.CapturedChunk,
        sequence: int,
        fencing_token: int,
        grant_lost: asyncio.Event,
        no_stop: asyncio.Event,
    ) -> str:
        extension, content_type = audio_pipeline.staging_parameters(
            chunk.mime_type
        )
        return await audio_pipeline.upload_staged_audio_with_retry(
            gcp_helper.upload_staged_audio,
            gcs_client=self._gcs_client,
            chunk=chunk,
            feed=feed,
            settings=self._settings,
            sequence=sequence,
            fencing_token=fencing_token,
            extension=extension,
            content_type=content_type,
            lease_lost=grant_lost,
            shutdown=no_stop,
        )

    async def _commit_progress(
        self,
        batch: FeedBatch,
        chunk: models.CapturedChunk,
        gcs_uri: str,
    ) -> (
        ingestion_lease_contracts.GrantRejected
        | ingestion_lease_contracts.BatchCommitted
    ):
        cursor = chunk.resume_position or chunk.chunk_end_time
        mutation = ingestion_lease_contracts.AdmittedAudioProgress(
            member=batch.member.identity,
            last_processed_filename=gcs_uri,
            cursor=cursor,
        )
        command = ingestion_lease_contracts.ChildMutationBatch(
            mutations=(mutation,),
            lease_effect=ingestion_lease_contracts.NoLeaseEffect(),
        )
        try:
            return await self._lease_store.commit_child_mutations(
                batch.grant,
                command,
                actor_id=self._actor_id,
            )
        except asyncio.CancelledError as error:
            message = "audio progress commit outcome is unknown"
            raise grant_control.GrantControlIntegrityError(message) from error
        except Exception as error:
            message = "audio progress commit outcome is unknown"
            raise grant_control.GrantControlIntegrityError(message) from error

    def _committed_child(
        self,
        result: ingestion_lease_contracts.BatchCommitted,
        batch: FeedBatch,
    ) -> ingestion_lease_contracts.ChildMutationResult:
        if len(result.children) != 1:
            message = "audio progress commit returned an invalid child count"
            raise grant_control.GrantControlIntegrityError(message)
        child = result.children[0]
        if child.feed_id != batch.member.identity.feed_id:
            message = "audio progress commit returned the wrong child"
            raise grant_control.GrantControlIntegrityError(message)
        return child

    async def _publish(
        self,
        member: ingestion_lease_contracts.LeaseMember,
        chunk: models.CapturedChunk,
        gcs_uri: str,
        no_stop: asyncio.Event,
    ) -> str:
        return await audio_pipeline.publish_audio_chunk_after_bookmark(
            gcp_helper.publish_audio_chunk,
            pubsub_client=self._pubsub_client,
            topic_path=self._topic_path,
            feed_id=member.identity.feed_id,
            feed_name=member.name,
            source_type=member.identity.source_type,
            gcs_uri=gcs_uri,
            chunk=chunk,
            settings=self._settings,
            lease_lost=no_stop,
            shutdown=no_stop,
            event_logger=logger,
        )

    def _result(
        self,
        outcome: failure_classification.ItemBatchOutcome,
        published_count: int,
        next_sequence: int,
        committed_urls: list[str],
        terminal: (
            failure_classification.ItemFailure
            | ingestion_lease_contracts.GrantRejected
            | grant_control.RunLost
            | None
        ),
    ) -> FeedBatchResult:
        return FeedBatchResult(
            attempted_count=outcome.attempted_count,
            published_count=published_count,
            next_sequence=next_sequence,
            committed_urls=tuple(committed_urls),
            terminal=terminal,
        )

    def _lost_authority_result(
        self,
        outcome: failure_classification.ItemBatchOutcome,
        published_count: int,
        next_sequence: int,
        committed_urls: list[str],
    ) -> FeedBatchResult:
        """Return definitive pre-bookmark authority-loss evidence."""
        return self._result(
            outcome,
            published_count,
            next_sequence,
            committed_urls,
            grant_control.RunLost(),
        )
