"""Bounded physical pipeline for one exact Broadcastify Calls cohort."""

from __future__ import annotations

import asyncio
import collections.abc
import dataclasses
import datetime
import enum
import logging
import math
import types
import typing

import aiohttp
from google.api_core import exceptions as google_exceptions
from google.cloud.pubsub_v1.publisher import exceptions as pubsub_exceptions

from backend.pipeline.common import gcp_helper
from backend.pipeline.ingestion import feed_work_scheduler
from backend.pipeline.ingestion.collectors.bcfy_calls import (
    bcfy_calls_collector,
    gap_ledger,
    provider,
    runtime_adapters,
)
from backend.pipeline.ingestion.failure_classifiers import pubsub
from backend.pipeline.ingestion.feed_work_scheduler import (
    CohortDirectFailureFact,
    CohortRecordTerminalReason,
)
from backend.pipeline.ingestion.models import AudioMimeType, CapturedChunk
from backend.pipeline.ingestion.retry import (
    CommittedAwaitableCancelled,
    CommittedAwaitableSettlement,
    LeaseExpiredError,
    retry_with_lease_check,
    settle_committed_awaitable,
)
from backend.pipeline.ingestion.slo_contract import EVENT_TYPE_CHUNK_INGESTED
from backend.pipeline.storage import feed_store, ingestion_lease_store

if typing.TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Mapping

    from backend.pipeline.common.clients.gcs_client import GcsClient
    from backend.pipeline.common.clients.pubsub_client import PubSubClient
    from backend.pipeline.ingestion.collectors.failure_classification import (
        ItemFailure,
    )
    from backend.pipeline.ingestion.settings import CollectorSettings

logger = logging.getLogger(__name__)

_SESSION_ID_MAX_LENGTH = 512
_GCS_RETRYABLE = (
    aiohttp.ClientError,
    asyncio.TimeoutError,
    OSError,
)
_PUBSUB_RETRYABLE = (
    google_exceptions.Aborted,
    google_exceptions.DeadlineExceeded,
    google_exceptions.InternalServerError,
    google_exceptions.ResourceExhausted,
    google_exceptions.ServiceUnavailable,
    google_exceptions.Unknown,
    google_exceptions.Cancelled,
    pubsub_exceptions.PublishToPausedOrderingKeyException,
)


class _CommitAccepted(enum.Enum):
    """Private exact marker for one accepted physical cohort commit."""

    VALUE = enum.auto()


_COMMIT_ACCEPTED = _CommitAccepted.VALUE
type _CommitClassification = (
    _CommitAccepted
    | feed_work_scheduler.CallMembershipRejected
    | feed_work_scheduler.CallAuthorityLost
    | feed_work_scheduler.CallRetryable
    | feed_work_scheduler.CallIntegrityFailure
)


def _freeze_provider_value(value: object) -> object:
    """Return a recursively immutable copy of a provider payload value."""
    if isinstance(value, collections.abc.Mapping):
        frozen = {
            key: _freeze_provider_value(item)
            for key, item in value.items()
            if isinstance(key, str)
        }
        if len(frozen) != len(value):
            message = "provider call keys must be strings"
            raise TypeError(message)
        return types.MappingProxyType(frozen)
    if isinstance(value, (list, tuple)):
        return tuple(_freeze_provider_value(item) for item in value)
    if isinstance(value, (set, frozenset)):
        return frozenset(_freeze_provider_value(item) for item in value)
    if value is None or isinstance(
        value,
        (bool, bytes, datetime.datetime, float, int, str),
    ):
        return value
    message = "provider call contains an unsupported mutable value"
    raise TypeError(message)


@dataclasses.dataclass(frozen=True, slots=True)
class ScheduledCohortEntry:
    """One immutable provider item in its original source order."""

    source_order: int
    audio_url: str = dataclasses.field(repr=False)
    raw_call: Mapping[str, object] = dataclasses.field(repr=False)
    receipt_time: datetime.datetime
    obligation: gap_ledger.MissingCallObligation

    def __post_init__(self) -> None:
        if isinstance(self.source_order, bool) or not isinstance(
            self.source_order,
            int,
        ):
            message = "source_order must be an integer"
            raise TypeError(message)
        if self.source_order < 0:
            message = "source_order must be nonnegative"
            raise ValueError(message)
        if not isinstance(self.audio_url, str):
            message = "audio_url must be a string"
            raise TypeError(message)
        if not self.audio_url:
            message = "audio_url must be nonempty"
            raise ValueError(message)
        if not isinstance(self.raw_call, collections.abc.Mapping):
            message = "raw_call must be a mapping"
            raise TypeError(message)
        frozen = _freeze_provider_value(self.raw_call)
        object.__setattr__(self, "raw_call", frozen)
        if not isinstance(self.receipt_time, datetime.datetime):
            message = "receipt_time must be a datetime"
            raise TypeError(message)
        if self.receipt_time.utcoffset() != datetime.timedelta(0):
            message = "receipt_time must be UTC-aware"
            raise ValueError(message)
        if type(self.obligation) is not gap_ledger.MissingCallObligation:
            message = "obligation must be an exact MissingCallObligation"
            raise TypeError(message)


def deduplicate_exact_url_entries(
    entries: collections.abc.Iterable[ScheduledCohortEntry],
) -> tuple[ScheduledCohortEntry, ...]:
    """Freeze producer order while removing later exact-URL duplicates."""
    seen: set[str] = set()
    unique = []
    for entry in entries:
        if type(entry) is not ScheduledCohortEntry:
            message = "entries must contain exact ScheduledCohortEntry values"
            raise TypeError(message)
        if entry.audio_url in seen:
            continue
        seen.add(entry.audio_url)
        unique.append(entry)
    return tuple(unique)


@dataclasses.dataclass(frozen=True, slots=True)
class ScheduledCohortPayload:
    """Frozen physical input shared by every counted record in a cohort."""

    member: ingestion_lease_store.LeaseMember
    session_id: str
    entries: tuple[ScheduledCohortEntry, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.member, ingestion_lease_store.LeaseMember):
            message = "member must be a LeaseMember"
            raise TypeError(message)
        if not isinstance(self.session_id, str):
            message = "session_id must be a string"
            raise TypeError(message)
        if (
            not self.session_id
            or len(self.session_id) > _SESSION_ID_MAX_LENGTH
            or any(character.isspace() for character in self.session_id)
        ):
            message = (
                "session_id must be nonempty, at most 512 chars, and "
                "whitespace-free"
            )
            raise ValueError(message)
        if not isinstance(self.entries, tuple) or not self.entries:
            message = "entries must be a nonempty immutable tuple"
            raise ValueError(message)
        if any(
            type(entry) is not ScheduledCohortEntry for entry in self.entries
        ):
            message = "entries must contain exact ScheduledCohortEntry values"
            raise TypeError(message)
        urls = tuple(entry.audio_url for entry in self.entries)
        if len(set(urls)) != len(urls):
            message = "producer must deduplicate exact URLs before construction"
            raise ValueError(message)
        orders = tuple(entry.source_order for entry in self.entries)
        if orders != tuple(sorted(orders)) or len(set(orders)) != len(orders):
            message = "entries must preserve distinct provider source order"
            raise ValueError(message)
        ledgers = tuple(
            gap_ledger._validate_scheduled_entry(  # noqa: SLF001
                entry.obligation,
                member=self.member,
                source_order=entry.source_order,
                audio_url=entry.audio_url,
                provider_ts=_normalized_provider_timestamp(entry.raw_call),
            )
            for entry in self.entries
        )
        if any(ledger is not ledgers[0] for ledger in ledgers):
            message = "payload obligations must share one page ledger"
            raise feed_work_scheduler.CohortIntegrityError(message)

    def admission_hook(
        self,
        identities: tuple[feed_work_scheduler.CohortRecordIdentity, ...],
    ) -> None:
        """Atomically bind every payload obligation at scheduler admission."""
        obligations = tuple(entry.obligation for entry in self.entries)
        ledger = gap_ledger._owner_for(obligations[0])  # noqa: SLF001
        ledger.admit(obligations, identities)

    def settlement_observer(
        self,
        source_order: int,
    ) -> Callable[[feed_work_scheduler.CallSettlement], None]:
        """Return the exact record-bound known-release observer."""
        matching = tuple(
            entry
            for entry in self.entries
            if entry.source_order == source_order
        )
        if len(matching) != 1:
            message = "source_order does not identify one payload entry"
            raise feed_work_scheduler.CohortIntegrityError(message)
        obligation = matching[0].obligation
        ledger = gap_ledger._owner_for(obligation)  # noqa: SLF001

        def observe(settlement: feed_work_scheduler.CallSettlement) -> None:
            ledger.observe_settlement(obligation, settlement)

        return observe


def _normalized_provider_timestamp(
    raw_call: Mapping[str, object],
) -> datetime.datetime | None:
    """Normalize one provider timestamp without inventing missing progress."""
    if "ts" not in raw_call:
        return None
    value = raw_call["ts"]
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(value)
    ):
        message = "provider timestamp must be one finite number"
        raise feed_work_scheduler.CohortIntegrityError(message)
    try:
        return datetime.datetime.fromtimestamp(value, datetime.UTC)
    except (OSError, OverflowError, ValueError) as exc:
        message = "provider timestamp is outside the UTC range"
        raise feed_work_scheduler.CohortIntegrityError(message) from exc


@dataclasses.dataclass(frozen=True, slots=True)
class StagedChunk:
    """One successfully claimed create-only GCS object."""

    chunk: CapturedChunk
    gcs_uri: str
    attempt_count: int

    def __post_init__(self) -> None:
        if type(self.chunk) is not CapturedChunk:
            message = "chunk must be an exact CapturedChunk"
            raise TypeError(message)
        if not isinstance(self.gcs_uri, str) or not self.gcs_uri.startswith(
            "gs://"
        ):
            message = "gcs_uri must be a nonempty GCS URI"
            raise ValueError(message)
        if (
            isinstance(self.attempt_count, bool)
            or not isinstance(self.attempt_count, int)
            or self.attempt_count <= 0
        ):
            message = "attempt_count must be a positive integer"
            raise ValueError(message)


@dataclasses.dataclass(frozen=True, slots=True)
class PublishedChunk:
    """One successfully published protobuf message."""

    message_id: str
    attempt_count: int

    def __post_init__(self) -> None:
        if not isinstance(self.message_id, str) or not self.message_id:
            message = "message_id must be a nonempty string"
            raise ValueError(message)
        if (
            isinstance(self.attempt_count, bool)
            or not isinstance(self.attempt_count, int)
            or self.attempt_count <= 0
        ):
            message = "attempt_count must be a positive integer"
            raise ValueError(message)


class StageOutcomeUnknown(RuntimeError):
    """A stage implementation cannot prove whether its claim completed."""


class ControlGate(enum.StrEnum):
    """Deterministic postcommit publication control points."""

    POST_COMMIT = "post_commit"
    BETWEEN_PUBLICATIONS = "between_publications"


async def _no_control_gate(
    _gate: ControlGate,
    _settled_publications: int,
) -> None:
    return


def _mime_staging_parameters(
    mime_type: AudioMimeType | None,
) -> tuple[str, str]:
    mime_map: dict[AudioMimeType, tuple[str, str]] = {
        AudioMimeType.MPEG: ("mp3", "audio/mpeg"),
        AudioMimeType.AAC: ("aac", "audio/aac"),
        AudioMimeType.WAV: ("wav", "audio/x-wav"),
        AudioMimeType.FLAC: ("flac", "audio/flac"),
        AudioMimeType.MP4: ("m4a", "audio/mp4"),
        AudioMimeType.OGG: ("ogg", "audio/ogg"),
    }
    if mime_type is None:
        return ("flac", "audio/flac")
    return mime_map.get(mime_type, ("flac", "audio/flac"))


def _upload_feed(
    subject: ingestion_lease_store.LeaseMember | feed_store.LeasedFeed,
) -> feed_store.LeasedFeed:
    """Project a member or retain a legacy Feed upload mapping."""
    if not isinstance(subject, ingestion_lease_store.LeaseMember):
        return subject
    return typing.cast(
        "feed_store.LeasedFeed",
        {
            "id": subject.identity.feed_id,
            "name": subject.name,
            "source_type": subject.identity.source_type,
        },
    )


def _subject_identity(
    subject: ingestion_lease_store.LeaseMember | feed_store.LeasedFeed,
) -> tuple[object, str, feed_store.SourceType]:
    """Return the downstream Feed identity shared by both runtime paths."""
    if isinstance(subject, ingestion_lease_store.LeaseMember):
        return (
            subject.identity.feed_id,
            subject.name,
            subject.identity.source_type,
        )
    return subject["id"], subject["name"], subject["source_type"]


async def stage_chunk_upload(
    gcs_client: GcsClient,
    chunk: CapturedChunk,
    subject: ingestion_lease_store.LeaseMember | feed_store.LeasedFeed,
    bucket: str,
    chunk_sequence: int,
    fencing_token: int,
    *,
    lease_lost: object,
    shutdown: object,
    max_retries: int,
    base_delay_sec: float,
    max_delay_sec: float,
    retry_state_changed: Callable[[bool], None] | None = None,
    extension: str | None = None,
    content_type: str | None = None,
) -> StagedChunk:
    """Upload one chunk with the existing fenced create-only contract."""
    mime_extension, mime_content_type = _mime_staging_parameters(
        chunk.mime_type
    )
    extension = extension or mime_extension
    content_type = content_type or mime_content_type
    attempt_count = 0

    def observe_attempt(attempt: int) -> None:
        nonlocal attempt_count
        attempt_count = attempt

    gcs_uri = await retry_with_lease_check(
        gcp_helper.upload_staged_audio,
        gcs_client,
        chunk.audio_bytes,
        _upload_feed(subject),
        bucket,
        chunk_sequence,
        fencing_token,
        extension,
        content_type,
        lease_lost=typing.cast("asyncio.Event", lease_lost),
        shutdown=typing.cast("asyncio.Event", shutdown),
        max_retries=max_retries,
        base_delay_sec=base_delay_sec,
        max_delay_sec=max_delay_sec,
        retryable=_GCS_RETRYABLE,
        operation_name="GCS upload",
        retry_state_changed=retry_state_changed,
        attempt_observer=observe_attempt,
    )
    return StagedChunk(chunk, gcs_uri, attempt_count)


async def publish_staged_chunk(
    pubsub_client: PubSubClient,
    topic_path: str,
    subject: ingestion_lease_store.LeaseMember | feed_store.LeasedFeed,
    staged: StagedChunk,
    *,
    max_retries: int,
    base_delay_sec: float,
    max_delay_sec: float,
    retry_state_changed: Callable[[bool], None] | None = None,
    attempt_observer: Callable[[int], None] | None = None,
) -> CommittedAwaitableSettlement[PublishedChunk]:
    """Publish one bookmarked chunk and defer caller cancellation safely."""
    chunk = staged.chunk
    feed_id, feed_name, source_type = _subject_identity(subject)
    duration_ms = int(
        (chunk.chunk_end_time - chunk.chunk_start_time).total_seconds() * 1000
    )
    attempt_count = 0

    def observe_attempt(attempt: int) -> None:
        nonlocal attempt_count
        attempt_count = attempt
        if attempt_observer is not None:
            try:
                attempt_observer(attempt)
            except (KeyboardInterrupt, SystemExit):
                raise
            except BaseException:  # noqa: S110 - observation is nonsemantic
                pass

    committed_signal = asyncio.Event()

    async def publish() -> PublishedChunk:
        message_id = await retry_with_lease_check(
            gcp_helper.publish_audio_chunk,
            pubsub_client,
            topic_path,
            str(feed_id),
            feed_name,
            staged.gcs_uri,
            chunk.session_id,
            chunk.chunk_start_time,
            duration_ms,
            source_type,
            chunk.external_audio_segment_id,
            lease_lost=committed_signal,
            shutdown=committed_signal,
            max_retries=max_retries,
            base_delay_sec=base_delay_sec,
            max_delay_sec=max_delay_sec,
            retryable=_PUBSUB_RETRYABLE,
            operation_name="Pub/Sub publish",
            retry_state_changed=retry_state_changed,
            attempt_observer=observe_attempt,
        )
        return PublishedChunk(message_id, attempt_count)

    settlement = await settle_committed_awaitable(publish())
    if settlement.failure is None:
        published = settlement.unwrap()
        logger.info(
            "Published message %s for feed %s",
            published.message_id,
            feed_name,
        )
        _log_chunk_ingested(feed_id, source_type, chunk)
    return settlement


def _log_chunk_ingested(
    feed_id: object,
    source_type: feed_store.SourceType,
    chunk: CapturedChunk,
) -> None:
    """Emit the legacy post-publication SLO event without payload changes."""
    payload: dict[str, object] = {
        "event_type": EVENT_TYPE_CHUNK_INGESTED,
        "feed_id": str(feed_id),
        "source_type": source_type,
    }
    if chunk.receipt_time is not None:
        raw_latency_sec = (
            datetime.datetime.now(datetime.UTC) - chunk.receipt_time
        ).total_seconds()
        payload["processing_latency_sec"] = max(
            0.0,
            round(raw_latency_sec, 2),
        )
        if raw_latency_sec < 0:
            payload["latency_clamped"] = True
    if chunk.stream_interval_lag_sec is not None:
        payload["stream_interval_lag_sec"] = round(
            chunk.stream_interval_lag_sec,
            2,
        )
    logger.info("Chunk ingested", extra={"json_fields": payload})


@dataclasses.dataclass(slots=True)
class _RecordState:
    call: feed_work_scheduler.CallExecution
    entry: ScheduledCohortEntry
    participated: bool = False
    item_failure: feed_work_scheduler.CohortItemFailureFact | None = None
    staged: StagedChunk | None = None
    publication_reason: (
        feed_work_scheduler.CohortRecordTerminalReason | None
    ) = None
    direct_failure: feed_work_scheduler.CohortDirectFailureFact | None = None
    content_attempt_count: int = 0
    publication_attempt_count: int = 0


class _AttemptObservingCallsProvider:
    """Invocation-local proxy that records media attempts for one call."""

    __slots__ = ("_observe", "_provider")

    def __init__(
        self,
        calls_provider: provider.CallsProviderClient,
        observe: Callable[[object, int], None],
    ) -> None:
        self._provider = calls_provider
        self._observe = observe

    async def download_audio(
        self,
        audio_url: str,
        *,
        shutdown_event: asyncio.Event,
        out_headers: dict[str, str] | None = None,
        attempt_observer: Callable[[object, int], None] | None = None,
    ) -> bytes | ItemFailure:
        """Forward one media request while preserving any outer observer."""

        def observe(kind: object, ordinal: int) -> None:
            self._observe(kind, ordinal)
            if attempt_observer is not None:
                attempt_observer(kind, ordinal)

        return await self._provider.download_audio(
            audio_url,
            shutdown_event=shutdown_event,
            out_headers=out_headers,
            attempt_observer=observe,
        )


type _StageOperation = Callable[..., Awaitable[StagedChunk]]
type _PublishOperation = Callable[
    ...,
    Awaitable[CommittedAwaitableSettlement[PublishedChunk]],
]
type _CreateChunkOperation = Callable[..., Awaitable[object]]
type _ControlGateOperation = Callable[[ControlGate, int], Awaitable[None]]
type _CohortExecutionResult = (
    feed_work_scheduler.CallCompleted
    | feed_work_scheduler.CallFinalClosurePending
    | feed_work_scheduler.CallReplayableDirectFailure
    | feed_work_scheduler.CallRetryable
    | feed_work_scheduler.CallStopped
    | feed_work_scheduler.CallAuthorityLost
    | feed_work_scheduler.CallMembershipRejected
    | feed_work_scheduler.CallIntegrityFailure
    | feed_work_scheduler.CallOutcomeUnknown
)


class BcfyCallsCohortExecutor:
    """Stage, commit, and sequentially publish one exact Feed cohort."""

    def __init__(
        self,
        *,
        http_session: aiohttp.ClientSession,
        calls_provider: provider.CallsProviderClient,
        gcs_client: GcsClient,
        pubsub_client: PubSubClient,
        committer: runtime_adapters.PhysicalCohortCommitter,
        settings: CollectorSettings,
        topic_path: str,
        retry_state_changed: Callable[[bool], None] | None = None,
        control_gate: _ControlGateOperation = _no_control_gate,
        stage_operation: _StageOperation = stage_chunk_upload,
        publish_operation: _PublishOperation = publish_staged_chunk,
        create_chunk_operation: _CreateChunkOperation = (
            bcfy_calls_collector._create_chunk_from_call  # noqa: SLF001
        ),
    ) -> None:
        if not isinstance(topic_path, str) or not topic_path:
            message = "topic_path must be a nonempty string"
            raise ValueError(message)
        for name, operation in (
            ("committer", getattr(committer, "commit_cohort", None)),
            ("control_gate", control_gate),
            ("stage_operation", stage_operation),
            ("publish_operation", publish_operation),
            ("create_chunk_operation", create_chunk_operation),
        ):
            if not callable(operation):
                message = f"{name} must be callable"
                raise TypeError(message)
        self._http_session = http_session
        self._calls_provider = calls_provider
        self._gcs_client = gcs_client
        self._pubsub_client = pubsub_client
        self._committer = committer
        self._settings = settings
        self._topic_path = topic_path
        self._retry_state_changed = retry_state_changed
        self._control_gate = control_gate
        self._stage_operation = stage_operation
        self._publish_operation = publish_operation
        self._create_chunk_operation = create_chunk_operation

    async def execute(  # noqa: PLR0911, PLR0912, PLR0915
        self,
        execution: feed_work_scheduler.CohortExecution,
    ) -> _CohortExecutionResult:
        """Run the complete physical state machine for one exact cohort."""
        payload, states = self._validate_execution(execution)
        signal_outcome = self._precommit_signal_outcome(execution, states)
        if signal_outcome is not None:
            return signal_outcome

        for index, state in enumerate(states):
            signal_outcome = self._precommit_signal_outcome(execution, states)
            if signal_outcome is not None:
                return signal_outcome
            state.participated = True
            state.content_attempt_count = 1

            def observe_content_attempt(
                _kind: object,
                ordinal: int,
                *,
                record_state: _RecordState = state,
            ) -> None:
                if (
                    isinstance(ordinal, int)
                    and not isinstance(ordinal, bool)
                    and ordinal > record_state.content_attempt_count
                ):
                    record_state.content_attempt_count = ordinal

            try:
                result = await self._create_chunk_operation(
                    self._http_session,
                    dict(state.entry.raw_call),
                    state.entry.audio_url,
                    typing.cast(
                        "asyncio.Event",
                        execution.signals.stop_requested,
                    ),
                    payload.session_id,
                    state.entry.receipt_time,
                    calls_provider=_AttemptObservingCallsProvider(
                        self._calls_provider,
                        observe_content_attempt,
                    ),
                )
            except asyncio.CancelledError:
                if self._task_is_cancelling():
                    raise
                signal_outcome = self._precommit_signal_outcome(
                    execution,
                    states,
                )
                if signal_outcome is not None:
                    return signal_outcome
                raise
            except Exception as exc:
                return self._integrity_outcome(execution, states, exc)
            if type(result) is not bcfy_calls_collector._CallChunkResult:  # noqa: SLF001
                return self._integrity_outcome(
                    execution,
                    states,
                    RuntimeError("chunk operation returned malformed evidence"),
                )
            chunk = getattr(result, "chunk", None)
            failure = getattr(result, "failure", None)
            if chunk is None:
                if failure is None:
                    return self._integrity_outcome(
                        execution,
                        states,
                        RuntimeError("chunk result omitted chunk and failure"),
                    )
                state.item_failure = feed_work_scheduler.CohortItemFailureFact(
                    failure.status_reason,
                    failure.reason,
                )
                self._obligation_ledger(state).mark_terminal_skip(
                    state.entry.obligation,
                    state.call.identity,
                    state.item_failure,
                    state.content_attempt_count,
                )
                continue
            if failure is not None or not isinstance(chunk, CapturedChunk):
                return self._integrity_outcome(
                    execution,
                    states,
                    RuntimeError("chunk result carried contradictory evidence"),
                )
            try:
                staged_chunk = await self._stage_operation(
                    self._gcs_client,
                    chunk,
                    payload.member,
                    self._settings.audio_staging_bucket,
                    state.call.identity.local_sequence,
                    state.call.identity.grant.fencing_token,
                    lease_lost=execution.signals.grant_lost,
                    shutdown=execution.signals.stop_requested,
                    max_retries=self._settings.gcs_upload_max_retries,
                    base_delay_sec=(
                        self._settings.gcs_upload_retry_base_delay_sec
                    ),
                    max_delay_sec=(
                        self._settings.gcs_upload_retry_max_delay_sec
                    ),
                    retry_state_changed=self._retry_state_changed,
                )
                if (
                    type(staged_chunk) is not StagedChunk
                    or staged_chunk.chunk is not chunk
                    or staged_chunk.attempt_count
                    > self._settings.gcs_upload_max_retries + 1
                ):
                    return self._integrity_outcome(
                        execution,
                        states,
                        RuntimeError(
                            "stage operation returned malformed evidence"
                        ),
                    )
                state.staged = staged_chunk
            except asyncio.CancelledError:
                if self._task_is_cancelling():
                    raise
                signal_outcome = self._precommit_signal_outcome(
                    execution,
                    states,
                )
                if signal_outcome is not None:
                    return signal_outcome
                raise
            except LeaseExpiredError:
                signal_outcome = self._precommit_signal_outcome(
                    execution,
                    states,
                )
                if signal_outcome is not None:
                    return signal_outcome
                raise
            except StageOutcomeUnknown:
                return self._unknown_outcome(states)
            except _GCS_RETRYABLE:
                return self._replayable_upload_outcome(states, index)
            except Exception as exc:
                return self._integrity_outcome(execution, states, exc)

        signal_outcome = self._precommit_signal_outcome(execution, states)
        if signal_outcome is not None:
            return signal_outcome
        staged = [state.staged for state in states if state.staged is not None]
        cohort_timestamp = states[0].call.identity.cohort_timestamp
        if not staged and cohort_timestamp is None:
            return self._final_pending_outcome(states)

        commit = runtime_adapters.PhysicalCohortCommit(
            member=payload.member.identity,
            last_processed_filename=(staged[-1].gcs_uri if staged else None),
            cursor=cohort_timestamp,
        )
        try:
            commit_settlement = await settle_committed_awaitable(
                self._committer.commit_cohort(
                    states[0].call.identity.grant,
                    commit,
                    final_logical=False,
                )
            )
        except CommittedAwaitableCancelled:
            return self._unknown_outcome(states)
        cancellation = commit_settlement.cancellation
        if commit_settlement.failure is not None:
            failure = commit_settlement.failure
            if isinstance(
                failure,
                runtime_adapters.BoundaryCommitNotStarted,
            ):
                outcome = feed_work_scheduler.CallRetryable(
                    self._replay_facts(
                        states,
                        feed_work_scheduler.CohortTerminalDisposition.RETRYABLE,
                        feed_work_scheduler.CohortRecordTerminalReason.RETRYABLE,
                        participated=False,
                    )
                )
                if cancellation is not None:
                    execution.cancellation_handoff.settle(outcome)
                    raise cancellation
                return outcome
            if isinstance(
                failure,
                runtime_adapters.BoundaryAdapterIntegrityError,
            ):
                outcome = self._integrity_outcome(execution, states, failure)
                if cancellation is not None:
                    self._retain_integrity(
                        execution,
                        outcome,
                        feed_work_scheduler.OutcomeUnknownCause.COMMIT,
                    )
                    raise cancellation
                return outcome
            outcome = self._unknown_outcome(states)
            if cancellation is not None:
                self._retain_unknown(
                    execution,
                    outcome,
                    feed_work_scheduler.OutcomeUnknownCause.COMMIT,
                )
                raise cancellation
            return outcome

        commit_result = commit_settlement.unwrap()
        classified = self._classify_commit_result(
            commit,
            commit_result,
            states,
        )
        if classified is not _COMMIT_ACCEPTED:
            if cancellation is not None:
                if isinstance(
                    classified,
                    feed_work_scheduler.CallIntegrityFailure,
                ):
                    self._retain_integrity(
                        execution,
                        classified,
                        feed_work_scheduler.OutcomeUnknownCause.COMMIT,
                    )
                else:
                    execution.cancellation_handoff.settle(classified)
                raise cancellation
            return classified

        self._close_timestamped_terminal_skips(states, commit.cursor)

        gate_cancellation = await self._run_gate(
            ControlGate.POST_COMMIT,
            0,
        )
        cancellation = cancellation or gate_cancellation
        if cancellation is not None:
            self._abandon_unpublished(states)
            outcome = self._completed_outcome(states)
            execution.cancellation_handoff.settle(outcome)
            raise cancellation

        signal_outcome = self._postcommit_signal_outcome(execution, states)
        if signal_outcome is not None:
            return signal_outcome

        settled_publications = 0
        for state in states:
            if state.staged is None:
                continue
            state.publication_attempt_count = 1

            def observe_publication_attempt(
                ordinal: int,
                *,
                record_state: _RecordState = state,
            ) -> None:
                if (
                    isinstance(ordinal, int)
                    and not isinstance(ordinal, bool)
                    and ordinal > record_state.publication_attempt_count
                ):
                    record_state.publication_attempt_count = ordinal

            try:
                publication = await self._publish_operation(
                    self._pubsub_client,
                    self._topic_path,
                    payload.member,
                    state.staged,
                    max_retries=self._settings.pubsub_publish_max_retries,
                    base_delay_sec=(
                        self._settings.pubsub_publish_retry_base_delay_sec
                    ),
                    max_delay_sec=(
                        self._settings.pubsub_publish_retry_max_delay_sec
                    ),
                    retry_state_changed=self._retry_state_changed,
                    attempt_observer=observe_publication_attempt,
                )
            except CommittedAwaitableCancelled:
                return self._unknown_outcome(states)

            if type(publication) is not CommittedAwaitableSettlement:
                return self._integrity_value(
                    states,
                    RuntimeError(
                        "publication returned malformed settlement evidence"
                    ),
                )

            publication_cancel = publication.cancellation
            if publication.failure is None:
                published = publication.unwrap()
                if (
                    type(published) is not PublishedChunk
                    or published.attempt_count
                    > self._settings.pubsub_publish_max_retries + 1
                ):
                    outcome = self._integrity_value(
                        states,
                        RuntimeError(
                            "publication returned malformed result evidence"
                        ),
                    )
                    if publication_cancel is not None:
                        self._retain_integrity(
                            execution,
                            outcome,
                            feed_work_scheduler.OutcomeUnknownCause.PUBLICATION,
                        )
                        raise publication_cancel
                    return outcome
                state.publication_reason = (
                    feed_work_scheduler.CohortRecordTerminalReason.FULL_PIPELINE
                )
                self._obligation_ledger(state).resolve_success(
                    state.entry.obligation,
                    state.call.identity,
                )
            elif isinstance(publication.failure, _PUBSUB_RETRYABLE):
                state.publication_reason = (
                    CohortRecordTerminalReason.PUBLICATION_EXHAUSTED
                )
                state.direct_failure = CohortDirectFailureFact(
                    feed_store.FeedStatusReason.PIPELINE_PUBLISH_AFTER_BOOKMARK_FAILED,
                    pubsub.publish_failure_reason(publication.failure),
                )
                self._obligation_ledger(state).emit_publication_exhausted(
                    state.entry.obligation,
                    state.call.identity,
                    state.direct_failure,
                    state.publication_attempt_count,
                )
            else:
                outcome = self._unknown_outcome(states)
                if publication_cancel is not None:
                    self._retain_unknown(
                        execution,
                        outcome,
                        feed_work_scheduler.OutcomeUnknownCause.PUBLICATION,
                    )
                    raise publication_cancel
                return outcome

            settled_publications += 1
            gate_cancellation = await self._run_gate(
                ControlGate.BETWEEN_PUBLICATIONS,
                settled_publications,
            )
            cancellation = publication_cancel or gate_cancellation
            if cancellation is not None:
                self._abandon_unpublished(states)
                if (
                    execution.signals.grant_lost.is_set()
                    and self._has_abandonment(states)
                ):
                    outcome = feed_work_scheduler.CallAuthorityLost(
                        self._durable_facts(
                            states,
                            feed_work_scheduler.CohortTerminalDisposition.AUTHORITY_LOST,
                        )
                    )
                else:
                    outcome = self._completed_outcome(states)
                execution.cancellation_handoff.settle(outcome)
                raise cancellation
            signal_outcome = self._postcommit_signal_outcome(execution, states)
            if signal_outcome is not None:
                return signal_outcome

        return self._completed_outcome(states)

    def _validate_execution(  # noqa: PLR0912, PLR0915
        self,
        execution: feed_work_scheduler.CohortExecution,
    ) -> tuple[ScheduledCohortPayload, list[_RecordState]]:
        if type(execution) is not feed_work_scheduler.CohortExecution:
            message = "execution must be an exact CohortExecution"
            raise feed_work_scheduler.CohortIntegrityError(message)
        first_payload = execution.calls[0].payload
        if type(first_payload) is not ScheduledCohortPayload:
            message = "cohort payload must be an exact ScheduledCohortPayload"
            raise feed_work_scheduler.CohortIntegrityError(message)
        payload = first_payload
        if len(execution.calls) != len(payload.entries):
            message = "cohort records and payload entries disagree"
            raise feed_work_scheduler.CohortIntegrityError(message)
        if any(call.payload is not payload for call in execution.calls):
            message = "cohort records do not share one frozen payload"
            raise feed_work_scheduler.CohortIntegrityError(message)

        grant = execution.calls[0].identity.grant
        member = payload.member.identity
        try:
            ingestion_lease_store._require_member_binding(  # noqa: SLF001
                grant,
                member,
            )
        except (TypeError, ValueError) as exc:
            message = "payload member crossed complete grant identity"
            raise feed_work_scheduler.CohortIntegrityError(message) from exc
        if grant.source_type is not feed_store.SourceType.BCFY_CALLS:
            message = "cohort grant is not Broadcastify Calls"
            raise feed_work_scheduler.CohortIntegrityError(message)
        if execution.signals.grant != grant:
            message = "lane signals crossed complete grant identity"
            raise feed_work_scheduler.CohortIntegrityError(message)

        identities = tuple(call.identity for call in execution.calls)
        first = identities[0]
        if any(
            identity.grant != grant
            or identity.member is not member
            or identity.feed_id != member.feed_id
            or identity.page_sequence != first.page_sequence
            or identity.cohort_timestamp != first.cohort_timestamp
            for identity in identities
        ):
            message = "cohort records crossed grant/member/Feed/page identity"
            raise feed_work_scheduler.CohortIntegrityError(message)
        if first.cohort_timestamp is None and len(identities) != 1:
            message = "missing-timestamp cohort must be a singleton"
            raise feed_work_scheduler.CohortIntegrityError(message)
        source_orders = tuple(identity.source_order for identity in identities)
        local_sequences = tuple(
            identity.local_sequence for identity in identities
        )
        expected_orders = tuple(entry.source_order for entry in payload.entries)
        if source_orders != expected_orders:
            message = "record and provider source order disagree"
            raise feed_work_scheduler.CohortIntegrityError(message)
        if local_sequences != tuple(sorted(local_sequences)) or len(
            set(local_sequences)
        ) != len(local_sequences):
            message = "record local sequence is not strictly ordered"
            raise feed_work_scheduler.CohortIntegrityError(message)

        states = []
        for call, entry in zip(execution.calls, payload.entries, strict=True):
            try:
                gap_ledger._validate_admitted_entry(  # noqa: SLF001
                    entry.obligation,
                    call.identity,
                )
            except (
                TypeError,
                ValueError,
                gap_ledger.MissingCallIntegrityError,
            ) as exc:
                message = "scheduled obligation crossed admitted identity"
                raise feed_work_scheduler.CohortIntegrityError(message) from exc
            if entry.raw_call.get("url") != entry.audio_url:
                message = "entry URL does not match immutable provider call"
                raise feed_work_scheduler.CohortIntegrityError(message)
            if entry.raw_call.get("groupId") != member.source_feed_id:
                message = "provider call does not match issued group identity"
                raise feed_work_scheduler.CohortIntegrityError(message)
            normalized = self._normalized_call_timestamp(entry.raw_call)
            if normalized != call.identity.cohort_timestamp:
                message = "provider timestamp does not match cohort identity"
                raise feed_work_scheduler.CohortIntegrityError(message)
            states.append(_RecordState(call, entry))
        return payload, states

    @staticmethod
    def _normalized_call_timestamp(
        raw_call: Mapping[str, object],
    ) -> datetime.datetime | None:
        return _normalized_provider_timestamp(raw_call)

    async def _run_gate(
        self,
        gate: ControlGate,
        settled_publications: int,
    ) -> asyncio.CancelledError | None:
        settlement = await settle_committed_awaitable(
            self._control_gate(gate, settled_publications)
        )
        settlement.unwrap()
        return settlement.cancellation

    def _precommit_signal_outcome(
        self,
        execution: feed_work_scheduler.CohortExecution,
        states: list[_RecordState],
    ) -> (
        feed_work_scheduler.CallAuthorityLost
        | feed_work_scheduler.CallStopped
        | None
    ):
        if execution.signals.grant_lost.is_set():
            return feed_work_scheduler.CallAuthorityLost(
                self._replay_facts(
                    states,
                    feed_work_scheduler.CohortTerminalDisposition.AUTHORITY_LOST,
                    feed_work_scheduler.CohortRecordTerminalReason.AUTHORITY_LOST,
                )
            )
        if execution.signals.stop_requested.is_set():
            return feed_work_scheduler.CallStopped(
                self._replay_facts(
                    states,
                    feed_work_scheduler.CohortTerminalDisposition.STOPPED,
                    feed_work_scheduler.CohortRecordTerminalReason.STOPPED,
                )
            )
        return None

    def _postcommit_signal_outcome(
        self,
        execution: feed_work_scheduler.CohortExecution,
        states: list[_RecordState],
    ) -> (
        feed_work_scheduler.CallAuthorityLost
        | feed_work_scheduler.CallStopped
        | None
    ):
        if not self._has_unpublished(states):
            return None
        if execution.signals.grant_lost.is_set():
            self._abandon_unpublished(states)
            return feed_work_scheduler.CallAuthorityLost(
                self._durable_facts(
                    states,
                    feed_work_scheduler.CohortTerminalDisposition.AUTHORITY_LOST,
                )
            )
        if execution.signals.stop_requested.is_set():
            self._abandon_unpublished(states)
            return feed_work_scheduler.CallStopped(
                self._durable_facts(
                    states,
                    feed_work_scheduler.CohortTerminalDisposition.STOPPED,
                )
            )
        return None

    def _abandon_unpublished(self, states: list[_RecordState]) -> None:
        for state in states:
            if state.staged is not None and state.publication_reason is None:
                state.publication_reason = (
                    CohortRecordTerminalReason.PUBLICATION_ABANDONED
                )
                self._obligation_ledger(state).emit_publication_abandoned(
                    state.entry.obligation,
                    state.call.identity,
                )

    @staticmethod
    def _obligation_ledger(
        state: _RecordState,
    ) -> gap_ledger.MissingCallLedger:
        return gap_ledger._owner_for(state.entry.obligation)  # noqa: SLF001

    def _retain_unknown_states(
        self,
        states: list[_RecordState],
    ) -> None:
        for state in states:
            self._obligation_ledger(state).retain_unknown(
                state.entry.obligation,
                state.call.identity,
            )

    def _close_timestamped_terminal_skips(
        self,
        states: list[_RecordState],
        cursor: datetime.datetime | None,
    ) -> None:
        for state in states:
            if state.item_failure is None:
                continue
            if state.call.identity.cohort_timestamp is None:
                message = "missing-ts terminal skip cannot close physically"
                raise feed_work_scheduler.CohortIntegrityError(message)
            if cursor is None:
                message = "timestamped terminal skip commit omitted cursor"
                raise feed_work_scheduler.CohortIntegrityError(message)
            self._obligation_ledger(state).close_timestamped_skip(
                state.entry.obligation,
                state.call.identity,
                cursor,
            )

    @staticmethod
    def _task_is_cancelling() -> bool:
        task = asyncio.current_task()
        return task is not None and task.cancelling() > 0

    @staticmethod
    def _has_unpublished(states: list[_RecordState]) -> bool:
        return any(
            state.staged is not None and state.publication_reason is None
            for state in states
        )

    @staticmethod
    def _has_abandonment(states: list[_RecordState]) -> bool:
        abandoned = (
            feed_work_scheduler.CohortRecordTerminalReason.PUBLICATION_ABANDONED
        )
        return any(state.publication_reason is abandoned for state in states)

    def _classify_commit_result(  # noqa: PLR0911
        self,
        commit: runtime_adapters.PhysicalCohortCommit,
        result: object,
        states: list[_RecordState],
    ) -> _CommitClassification:
        if type(result) is runtime_adapters.PhysicalCohortResult:
            if result.commit is not commit:
                return self._integrity_value(
                    states,
                    RuntimeError(
                        "commit result did not retain submitted identity"
                    ),
                )
            if (
                result.disposition
                is feed_work_scheduler.BoundaryDisposition.COMMITTED
            ):
                return _COMMIT_ACCEPTED
            if (
                result.disposition
                is feed_work_scheduler.BoundaryDisposition.MEMBER_REJECTED
            ):
                return feed_work_scheduler.CallMembershipRejected(
                    self._replay_facts(
                        states,
                        feed_work_scheduler.CohortTerminalDisposition.MEMBERSHIP_REJECTED,
                        feed_work_scheduler.CohortRecordTerminalReason.MEMBERSHIP_REJECTED,
                    )
                )
            return self._integrity_value(
                states,
                RuntimeError("commit returned unsupported disposition"),
            )
        if type(result) is feed_work_scheduler.BoundaryGrantRejected:
            return feed_work_scheduler.CallAuthorityLost(
                self._replay_facts(
                    states,
                    feed_work_scheduler.CohortTerminalDisposition.AUTHORITY_LOST,
                    feed_work_scheduler.CohortRecordTerminalReason.AUTHORITY_LOST,
                )
            )
        if type(result) is feed_work_scheduler.BoundaryBatchRetryable:
            return feed_work_scheduler.CallRetryable(
                self._replay_facts(
                    states,
                    feed_work_scheduler.CohortTerminalDisposition.RETRYABLE,
                    feed_work_scheduler.CohortRecordTerminalReason.RETRYABLE,
                    participated=False,
                )
            )
        return self._integrity_value(
            states,
            RuntimeError("committer returned malformed or forged evidence"),
        )

    def _completed_outcome(
        self,
        states: list[_RecordState],
    ) -> feed_work_scheduler.CallCompleted:
        return feed_work_scheduler.CallCompleted(
            self._durable_facts(
                states,
                feed_work_scheduler.CohortTerminalDisposition.SETTLED,
            )
        )

    def _final_pending_outcome(
        self,
        states: list[_RecordState],
    ) -> feed_work_scheduler.CallFinalClosurePending:
        records = tuple(
            feed_work_scheduler.CohortRecordTerminalFact(
                state.call.identity,
                participated=state.participated,
                closure_state=feed_work_scheduler.CohortRecordClosureState.FINAL_CLOSURE_PENDING,
                full_pipeline_completed=False,
                terminal_reason=feed_work_scheduler.CohortRecordTerminalReason.TERMINAL_ITEM_SKIP,
                item_failure=state.item_failure,
            )
            for state in states
        )
        return feed_work_scheduler.CallFinalClosurePending(
            feed_work_scheduler.CohortTerminalFacts(
                records,
                feed_work_scheduler.CohortTerminalDisposition.FINAL_CLOSURE_PENDING,
            )
        )

    def _durable_facts(
        self,
        states: list[_RecordState],
        disposition: feed_work_scheduler.CohortTerminalDisposition,
    ) -> feed_work_scheduler.CohortTerminalFacts:
        full_pipeline = (
            feed_work_scheduler.CohortRecordTerminalReason.FULL_PIPELINE
        )
        records = []
        for state in states:
            if state.item_failure is not None:
                reason = CohortRecordTerminalReason.TERMINAL_ITEM_SKIP
            else:
                reason = state.publication_reason
            if reason is None:
                message = "durable record omitted publication disposition"
                raise feed_work_scheduler.CohortIntegrityError(message)
            records.append(
                feed_work_scheduler.CohortRecordTerminalFact(
                    state.call.identity,
                    participated=True,
                    closure_state=feed_work_scheduler.CohortRecordClosureState.DURABLY_CLOSED,
                    full_pipeline_completed=reason is full_pipeline,
                    terminal_reason=reason,
                    item_failure=state.item_failure,
                    direct_failure=state.direct_failure,
                )
            )
        return feed_work_scheduler.CohortTerminalFacts(
            tuple(records),
            disposition,
        )

    def _replayable_upload_outcome(
        self,
        states: list[_RecordState],
        failed_index: int,
    ) -> feed_work_scheduler.CallReplayableDirectFailure:
        direct = feed_work_scheduler.CohortDirectFailureFact(
            feed_store.FeedStatusReason.SYSTEM_PIPELINE_ERROR,
            "gcs_upload_failed",
        )
        replayable = (
            feed_work_scheduler.CohortRecordTerminalReason.REPLAYABLE_DIRECT
        )
        retryable = feed_work_scheduler.CohortRecordTerminalReason.RETRYABLE
        records = []
        for index, state in enumerate(states):
            failed = index == failed_index
            records.append(
                feed_work_scheduler.CohortRecordTerminalFact(
                    state.call.identity,
                    participated=state.participated,
                    closure_state=feed_work_scheduler.CohortRecordClosureState.REPLAY_SAFE_RELEASE,
                    full_pipeline_completed=False,
                    terminal_reason=replayable if failed else retryable,
                    direct_failure=direct if failed else None,
                )
            )
        return feed_work_scheduler.CallReplayableDirectFailure(
            feed_work_scheduler.CohortTerminalFacts(
                tuple(records),
                feed_work_scheduler.CohortTerminalDisposition.REPLAYABLE_DIRECT,
            )
        )

    @staticmethod
    def _replay_facts(
        states: list[_RecordState],
        disposition: feed_work_scheduler.CohortTerminalDisposition,
        reason: feed_work_scheduler.CohortRecordTerminalReason,
        *,
        participated: bool | None = None,
    ) -> feed_work_scheduler.CohortTerminalFacts:
        return feed_work_scheduler.CohortTerminalFacts(
            tuple(
                feed_work_scheduler.CohortRecordTerminalFact(
                    state.call.identity,
                    participated=(
                        state.participated
                        if participated is None
                        else participated
                    ),
                    closure_state=feed_work_scheduler.CohortRecordClosureState.REPLAY_SAFE_RELEASE,
                    full_pipeline_completed=False,
                    terminal_reason=reason,
                )
                for state in states
            ),
            disposition,
        )

    def _integrity_outcome(
        self,
        execution: feed_work_scheduler.CohortExecution,
        states: list[_RecordState],
        failure: BaseException,
    ) -> feed_work_scheduler.CallIntegrityFailure:
        del execution
        return self._integrity_value(states, failure)

    def _integrity_value(
        self,
        states: list[_RecordState],
        failure: BaseException,
    ) -> feed_work_scheduler.CallIntegrityFailure:
        self._retain_unknown_states(states)
        facts = self._unknown_facts(
            states,
            feed_work_scheduler.CohortTerminalDisposition.INTEGRITY_FAILURE,
            feed_work_scheduler.CohortRecordTerminalReason.INTEGRITY_FAILURE,
        )
        return feed_work_scheduler.CallIntegrityFailure(facts, failure)

    def _unknown_outcome(
        self,
        states: list[_RecordState],
    ) -> feed_work_scheduler.CallOutcomeUnknown:
        self._retain_unknown_states(states)
        facts = self._unknown_facts(
            states,
            feed_work_scheduler.CohortTerminalDisposition.OUTCOME_UNKNOWN,
            feed_work_scheduler.CohortRecordTerminalReason.OUTCOME_UNKNOWN,
        )
        return feed_work_scheduler.CallOutcomeUnknown(facts)

    @staticmethod
    def _unknown_facts(
        states: list[_RecordState],
        disposition: feed_work_scheduler.CohortTerminalDisposition,
        reason: feed_work_scheduler.CohortRecordTerminalReason,
    ) -> feed_work_scheduler.CohortTerminalFacts:
        return feed_work_scheduler.CohortTerminalFacts(
            tuple(
                feed_work_scheduler.CohortRecordTerminalFact(
                    state.call.identity,
                    participated=state.participated,
                    closure_state=feed_work_scheduler.CohortRecordClosureState.OUTCOME_UNKNOWN,
                    full_pipeline_completed=False,
                    terminal_reason=reason,
                )
                for state in states
            ),
            disposition,
        )

    @staticmethod
    def _retain_unknown(
        execution: feed_work_scheduler.CohortExecution,
        outcome: feed_work_scheduler.CallOutcomeUnknown,
        cause: feed_work_scheduler.OutcomeUnknownCause,
    ) -> None:
        execution.retention.retain(
            feed_work_scheduler.OutcomeUnknownRetentionRequest(
                cause,
                outcome.facts,
            )
        )

    @staticmethod
    def _retain_integrity(
        execution: feed_work_scheduler.CohortExecution,
        outcome: feed_work_scheduler.CallIntegrityFailure,
        cause: feed_work_scheduler.OutcomeUnknownCause,
    ) -> None:
        execution.retention.retain(
            feed_work_scheduler.OutcomeUnknownRetentionRequest(
                cause,
                outcome.facts,
                outcome.failure,
            )
        )
