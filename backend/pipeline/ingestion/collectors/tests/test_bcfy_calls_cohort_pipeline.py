"""Physical state-machine tests for exact Broadcastify Calls cohorts."""

from __future__ import annotations

import asyncio
import dataclasses
import datetime
import types
import typing
import uuid
from unittest import mock

import pytest
from google.api_core import exceptions as google_exceptions

from backend.pipeline.ingestion import feed_work_scheduler
from backend.pipeline.ingestion.collectors.bcfy_calls import (
    bcfy_calls_collector,
    gap_ledger,
    pipeline,
    runtime_adapters,
    telemetry,
)
from backend.pipeline.ingestion.collectors.failure_classification import (
    ItemFailure,
)
from backend.pipeline.ingestion.feed_work_scheduler import _types
from backend.pipeline.ingestion.models import AudioMimeType, CapturedChunk
from backend.pipeline.ingestion.retry import (
    CommittedAwaitableSettlement,
    LeaseExpiredError,
    settle_committed_awaitable,
)
from backend.pipeline.storage import feed_store, ingestion_lease_store

if typing.TYPE_CHECKING:
    from collections.abc import Callable

_NOW = datetime.datetime(2026, 7, 13, 12, 0, tzinfo=datetime.UTC)
_FEED_ID = uuid.UUID("00000000-0000-0000-0000-000000000801")
_WORKER_ID = uuid.UUID("00000000-0000-0000-0000-000000000802")


def _grant() -> ingestion_lease_store.LeaseGrant:
    return ingestion_lease_store.LeaseGrant(
        feed_store.SourceType.BCFY_CALLS,
        "123",
        _WORKER_ID,
        8,
    )


def _member(
    grant: ingestion_lease_store.LeaseGrant,
) -> ingestion_lease_store.LeaseMember:
    identity = ingestion_lease_store._issue_member_identity(
        grant,
        feed_id=_FEED_ID,
        source_type=feed_store.SourceType.BCFY_CALLS,
        source_feed_id="123-45",
        sid="123",
        group_id="45",
    )
    return ingestion_lease_store.LeaseMember(
        identity=identity,
        name="Test Calls Feed",
        status=feed_store.FeedStatus.ACTIVE,
        last_processed_filename=None,
        last_bookmark_time=None,
        failure_count=0,
        retry_after=None,
        status_reason=None,
        status_reason_detail=None,
        audit_revision=1,
    )


def _settings() -> types.SimpleNamespace:
    return types.SimpleNamespace(
        audio_staging_bucket="audio-stage",
        gcs_upload_max_retries=0,
        gcs_upload_retry_base_delay_sec=0.0,
        gcs_upload_retry_max_delay_sec=0.0,
        pubsub_publish_max_retries=0,
        pubsub_publish_retry_base_delay_sec=0.0,
        pubsub_publish_retry_max_delay_sec=0.0,
    )


def _entry(
    member: ingestion_lease_store.LeaseMember,
    source_order: int,
    ledger: gap_ledger.MissingCallLedger,
    *,
    timestamp: datetime.datetime | None = _NOW,
    audio_url: str | None = None,
) -> pipeline.ScheduledCohortEntry:
    url = audio_url or f"https://example.invalid/audio-{source_order}.flac"
    raw_call: dict[str, object] = {
        "groupId": member.identity.source_feed_id,
        "url": url,
        "start_ts": _NOW.timestamp(),
        "end_ts": (_NOW + datetime.timedelta(seconds=2)).timestamp(),
    }
    if timestamp is not None:
        raw_call["ts"] = timestamp.timestamp()
    obligation = ledger.draft(
        member,
        audio_url=url,
        provider_ts=timestamp,
        source_order=source_order,
    )
    return pipeline.ScheduledCohortEntry(
        source_order,
        url,
        raw_call,
        _NOW,
        obligation,
    )


def _execution(
    grant: ingestion_lease_store.LeaseGrant,
    payload: pipeline.ScheduledCohortPayload,
    *,
    timestamp: datetime.datetime | None = _NOW,
    stop_requested: asyncio.Event | None = None,
    grant_lost: asyncio.Event | None = None,
) -> tuple[
    feed_work_scheduler.CohortExecution,
    list[object],
    list[feed_work_scheduler.OutcomeUnknownRetentionRequest],
]:
    identities = tuple(
        feed_work_scheduler.CohortRecordIdentity(
            grant,
            payload.member.identity,
            page_sequence=3,
            feed_id=payload.member.identity.feed_id,
            cohort_timestamp=timestamp,
            source_order=entry.source_order,
            local_sequence=80 + index,
        )
        for index, entry in enumerate(payload.entries)
    )
    payload.admission_hook(identities)
    known: list[object] = []
    retained: list[feed_work_scheduler.OutcomeUnknownRetentionRequest] = []
    calls = tuple(
        feed_work_scheduler.CallExecution(identity, payload)
        for identity in identities
    )
    return (
        feed_work_scheduler.CohortExecution(
            calls,
            feed_work_scheduler.LaneSignalView(
                grant,
                stop_requested or asyncio.Event(),
                grant_lost or asyncio.Event(),
            ),
            _types._issue_retention_handle(identities, retained.append),
            _types._issue_cancellation_handoff(identities, known.append),
        ),
        known,
        retained,
    )


def _observe_terminal_outcome(
    payload: pipeline.ScheduledCohortPayload,
    outcome: object,
) -> None:
    facts = outcome.facts
    for entry, fact in zip(payload.entries, facts.records, strict=True):
        settlement = (
            feed_work_scheduler.CallSettlement.COMPLETED
            if fact.closure_state
            is feed_work_scheduler.CohortRecordClosureState.DURABLY_CLOSED
            else feed_work_scheduler.CallSettlement.REPLAY_SAFE_RELEASE
        )
        payload.settlement_observer(entry.source_order)(settlement)


class _Committer:
    def __init__(
        self,
        result: object
        | Callable[[runtime_adapters.PhysicalCohortCommit], object]
        | None = None,
    ) -> None:
        self.result = result
        self.commits: list[runtime_adapters.PhysicalCohortCommit] = []

    async def commit_cohort(
        self,
        grant: ingestion_lease_store.LeaseGrant,
        commit: runtime_adapters.PhysicalCohortCommit,
        *,
        final_logical: bool,
    ) -> object:
        del grant
        assert final_logical is False
        self.commits.append(commit)
        if callable(self.result):
            value = self.result(commit)
        elif self.result is None:
            value = runtime_adapters.PhysicalCohortResult(
                commit,
                feed_work_scheduler.BoundaryDisposition.COMMITTED,
            )
        else:
            value = self.result
        if isinstance(value, BaseException):
            raise value
        return value


class _Operations:
    def __init__(self, *, skipped: set[str] | None = None) -> None:
        self.skipped = skipped or set()
        self.created: list[str] = []
        self.staged: list[tuple[str, int]] = []
        self.published: list[str] = []
        self.publish_failures: dict[str, BaseException] = {}

    async def create(
        self,
        _session: object,
        raw_call: dict[str, object],
        audio_url: str,
        _shutdown: asyncio.Event,
        session_id: str,
        receipt_time: datetime.datetime,
        **_kwargs: object,
    ) -> object:
        self.created.append(audio_url)
        if audio_url in self.skipped:
            return bcfy_calls_collector._CallChunkResult(
                failure=ItemFailure(
                    feed_store.FeedStatusReason.SOURCE_UNREACHABLE,
                    "item_download_failed",
                )
            )
        timestamp = datetime.datetime.fromtimestamp(
            float(raw_call.get("ts", _NOW.timestamp())),
            datetime.UTC,
        )
        return bcfy_calls_collector._CallChunkResult(
            chunk=CapturedChunk(
                audio_bytes=audio_url.encode(),
                chunk_start_time=_NOW,
                chunk_end_time=_NOW + datetime.timedelta(seconds=2),
                session_id=session_id,
                receipt_time=receipt_time,
                mime_type=AudioMimeType.FLAC,
                resume_position=(timestamp if "ts" in raw_call else None),
                external_audio_segment_id=audio_url,
            )
        )

    async def stage(
        self,
        _client: object,
        chunk: CapturedChunk,
        _member: ingestion_lease_store.LeaseMember,
        _bucket: str,
        sequence: int,
        _fencing_token: int,
        **_kwargs: object,
    ) -> pipeline.StagedChunk:
        assert chunk.external_audio_segment_id is not None
        self.staged.append((chunk.external_audio_segment_id, sequence))
        return pipeline.StagedChunk(
            chunk,
            f"gs://audio-stage/token-8/{sequence}.flac",
            1,
        )

    async def publish(
        self,
        _client: object,
        _topic: str,
        _member: ingestion_lease_store.LeaseMember,
        staged: pipeline.StagedChunk,
        **_kwargs: object,
    ) -> CommittedAwaitableSettlement[pipeline.PublishedChunk]:
        url = staged.chunk.external_audio_segment_id
        assert url is not None
        self.published.append(url)
        failure = self.publish_failures.get(url)
        if failure is not None:
            return CommittedAwaitableSettlement(None, failure=failure)
        return CommittedAwaitableSettlement(
            pipeline.PublishedChunk(f"message-{len(self.published)}", 1)
        )


def _executor(
    operations: _Operations,
    committer: _Committer,
    *,
    control_gate: Callable[..., object] | None = None,
    calls_provider: object | None = None,
    create_chunk_operation: Callable[..., object] | None = None,
) -> pipeline.BcfyCallsCohortExecutor:
    async def no_gate(
        _gate: pipeline.ControlGate,
        _settled: int,
    ) -> None:
        return

    return pipeline.BcfyCallsCohortExecutor(
        http_session=mock.Mock(),
        calls_provider=(
            mock.Mock() if calls_provider is None else calls_provider
        ),
        gcs_client=mock.Mock(),
        pubsub_client=mock.Mock(),
        committer=committer,
        settings=_settings(),
        topic_path="projects/test/topics/segmented",
        control_gate=control_gate or no_gate,
        stage_operation=operations.stage,
        publish_operation=operations.publish,
        create_chunk_operation=(
            operations.create
            if create_chunk_operation is None
            else create_chunk_operation
        ),
    )


@pytest.mark.asyncio
async def test_equal_timestamp_siblings_stage_commit_then_publish() -> None:
    grant = _grant()
    member = _member(grant)
    ledger = gap_ledger.MissingCallLedger(grant, 3)
    payload = pipeline.ScheduledCohortPayload(
        member,
        "stable-session",
        (_entry(member, 4, ledger), _entry(member, 7, ledger)),
    )
    execution, known, retained = _execution(grant, payload)
    operations = _Operations()
    committer = _Committer()

    outcome = await _executor(operations, committer).execute(execution)

    assert isinstance(outcome, feed_work_scheduler.CallCompleted)
    assert operations.created == [
        payload.entries[0].audio_url,
        payload.entries[1].audio_url,
    ]
    assert operations.staged == [
        (payload.entries[0].audio_url, 80),
        (payload.entries[1].audio_url, 81),
    ]
    assert len(committer.commits) == 1
    assert committer.commits[0].cursor == _NOW
    assert committer.commits[0].last_processed_filename == (
        "gs://audio-stage/token-8/81.flac"
    )
    assert operations.published == [
        payload.entries[0].audio_url,
        payload.entries[1].audio_url,
    ]
    assert all(
        record.full_pipeline_completed for record in outcome.facts.records
    )
    assert known == []
    assert retained == []


def test_producer_deduplicates_before_payload_construction() -> None:
    grant = _grant()
    member = _member(grant)
    ledger = gap_ledger.MissingCallLedger(grant, 3)
    first = _entry(
        member,
        1,
        ledger,
        audio_url="https://example.invalid/same",
    )
    duplicate = _entry(member, 2, ledger, audio_url=first.audio_url)

    unique = pipeline.deduplicate_exact_url_entries((first, duplicate))

    assert unique == (first,)
    safe_payload = pipeline.ScheduledCohortPayload(
        member,
        "stable-session",
        unique,
    )
    assert first.audio_url not in repr(first)
    assert first.audio_url not in repr(safe_payload)
    with pytest.raises(ValueError, match="deduplicate exact URLs"):
        pipeline.ScheduledCohortPayload(
            member,
            "stable-session",
            (first, duplicate),
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("timestamp", "skip_all", "expected_fields", "expected_type"),
    [
        (_NOW, False, (True, True), feed_work_scheduler.CallCompleted),
        (None, False, (True, False), feed_work_scheduler.CallCompleted),
        (_NOW, True, (False, True), feed_work_scheduler.CallCompleted),
        (
            None,
            True,
            (False, False),
            feed_work_scheduler.CallFinalClosurePending,
        ),
    ],
    ids=(
        "timestamped-stage-path-and-cursor",
        "missing-ts-stage-path-only",
        "timestamped-all-skip-cursor-only",
        "missing-ts-all-skip-final-pending",
    ),
)
async def test_physical_commit_evidence_matrix(
    timestamp: datetime.datetime | None,
    skip_all: bool,  # noqa: FBT001
    expected_fields: tuple[bool, bool],
    expected_type: type[object],
) -> None:
    grant = _grant()
    member = _member(grant)
    ledger = gap_ledger.MissingCallLedger(grant, 3)
    entry = _entry(member, 1, ledger, timestamp=timestamp)
    payload = pipeline.ScheduledCohortPayload(member, "session", (entry,))
    execution, _known, _retained = _execution(
        grant,
        payload,
        timestamp=timestamp,
    )
    operations = _Operations(skipped={entry.audio_url} if skip_all else set())
    committer = _Committer()

    outcome = await _executor(operations, committer).execute(execution)

    assert isinstance(outcome, expected_type)
    if expected_fields == (False, False):
        assert committer.commits == []
        assert ledger.state(entry.obligation) is (
            gap_ledger.MissingCallState.FINAL_CLOSURE_PENDING
        )
    else:
        assert len(committer.commits) == 1
        commit = committer.commits[0]
        assert (commit.last_processed_filename is not None) is expected_fields[
            0
        ]
        assert (commit.cursor is not None) is expected_fields[1]
        assert ledger.state(entry.obligation) is (
            gap_ledger.MissingCallState.EMITTED
            if skip_all
            else gap_ledger.MissingCallState.RESOLVED
        )


@pytest.mark.asyncio
async def test_terminal_skip_event_reports_actual_media_attempts() -> None:
    class ThreeAttemptFailureProvider:
        async def download_audio(
            self,
            _audio_url: str,
            *,
            shutdown_event: asyncio.Event,
            out_headers: dict[str, str] | None = None,
            attempt_observer: Callable[[object, int], None] | None = None,
        ) -> ItemFailure:
            del shutdown_event, out_headers
            assert attempt_observer is not None
            for ordinal in range(1, 4):
                attempt_observer(mock.sentinel.media, ordinal)
            return ItemFailure(
                feed_store.FeedStatusReason.SOURCE_UNREACHABLE,
                "item_download_failed",
            )

    grant = _grant()
    member = _member(grant)
    ledger = gap_ledger.MissingCallLedger(grant, 3)
    entry = _entry(member, 1, ledger)
    payload = pipeline.ScheduledCohortPayload(member, "session", (entry,))
    execution, _known, _retained = _execution(grant, payload)

    with mock.patch.object(telemetry.logger, "log") as emit:
        outcome = await _executor(
            _Operations(),
            _Committer(),
            calls_provider=ThreeAttemptFailureProvider(),
            create_chunk_operation=(
                bcfy_calls_collector._create_chunk_from_call
            ),
        ).execute(execution)

    assert isinstance(outcome, feed_work_scheduler.CallCompleted)
    emit.assert_called_once()
    assert emit.call_args.kwargs["extra"]["json_fields"]["attempt_count"] == 3


@pytest.mark.asyncio
async def test_terminal_skip_and_success_share_durable_commit() -> None:
    grant = _grant()
    member = _member(grant)
    ledger = gap_ledger.MissingCallLedger(grant, 3)
    entries = (_entry(member, 1, ledger), _entry(member, 2, ledger))
    payload = pipeline.ScheduledCohortPayload(member, "session", entries)
    execution, _known, _retained = _execution(grant, payload)
    operations = _Operations(skipped={entries[0].audio_url})
    committer = _Committer()

    outcome = await _executor(operations, committer).execute(execution)

    assert isinstance(outcome, feed_work_scheduler.CallCompleted)
    assert len(committer.commits) == 1
    assert operations.published == [entries[1].audio_url]
    reasons = tuple(record.terminal_reason for record in outcome.facts.records)
    assert reasons == (
        feed_work_scheduler.CohortRecordTerminalReason.TERMINAL_ITEM_SKIP,
        feed_work_scheduler.CohortRecordTerminalReason.FULL_PIPELINE,
    )
    assert tuple(ledger.state(entry.obligation) for entry in entries) == (
        gap_ledger.MissingCallState.EMITTED,
        gap_ledger.MissingCallState.RESOLVED,
    )


@pytest.mark.asyncio
async def test_publication_exhaustion_does_not_suppress_later_sibling() -> None:
    grant = _grant()
    member = _member(grant)
    ledger = gap_ledger.MissingCallLedger(grant, 3)
    entries = (_entry(member, 1, ledger), _entry(member, 2, ledger))
    payload = pipeline.ScheduledCohortPayload(member, "session", entries)
    execution, _known, _retained = _execution(grant, payload)
    operations = _Operations()
    operations.publish_failures[entries[0].audio_url] = (
        google_exceptions.ServiceUnavailable("unavailable")
    )

    outcome = await _executor(operations, _Committer()).execute(execution)

    assert isinstance(outcome, feed_work_scheduler.CallCompleted)
    assert operations.published == [entry.audio_url for entry in entries]
    assert outcome.facts.records[0].terminal_reason is (
        feed_work_scheduler.CohortRecordTerminalReason.PUBLICATION_EXHAUSTED
    )
    assert outcome.facts.records[0].direct_failure is not None
    assert outcome.facts.records[1].full_pipeline_completed
    assert tuple(ledger.state(entry.obligation) for entry in entries) == (
        gap_ledger.MissingCallState.EMITTED,
        gap_ledger.MissingCallState.RESOLVED,
    )


@pytest.mark.asyncio
async def test_publication_gap_event_reports_actual_owner_attempts() -> None:
    grant = _grant()
    member = _member(grant)
    ledger = gap_ledger.MissingCallLedger(grant, 3)
    entry = _entry(member, 1, ledger)
    payload = pipeline.ScheduledCohortPayload(member, "session", (entry,))
    execution, _known, _retained = _execution(grant, payload)
    executor = _executor(_Operations(), _Committer())

    async def failed_publish(
        *_args: object,
        attempt_observer: Callable[[int], None],
        **_kwargs: object,
    ) -> CommittedAwaitableSettlement[pipeline.PublishedChunk]:
        for ordinal in range(1, 4):
            attempt_observer(ordinal)
        return CommittedAwaitableSettlement(
            None,
            failure=google_exceptions.ServiceUnavailable("unavailable"),
        )

    executor._publish_operation = failed_publish
    with mock.patch.object(telemetry.logger, "log") as emit:
        outcome = await executor.execute(execution)

    assert isinstance(outcome, feed_work_scheduler.CallCompleted)
    emit.assert_called_once()
    assert emit.call_args.kwargs["extra"]["json_fields"]["attempt_count"] == 3


@pytest.mark.asyncio
async def test_gcs_exhaustion_is_replayable_before_commit() -> None:
    grant = _grant()
    member = _member(grant)
    ledger = gap_ledger.MissingCallLedger(grant, 3)
    payload = pipeline.ScheduledCohortPayload(
        member,
        "session",
        (_entry(member, 1, ledger), _entry(member, 2, ledger)),
    )
    execution, _known, _retained = _execution(grant, payload)
    operations = _Operations()

    async def failed_stage(*_args: object, **_kwargs: object) -> object:
        message = "upload failed"
        raise OSError(message)

    executor = _executor(operations, _Committer())
    executor._stage_operation = failed_stage

    outcome = await executor.execute(execution)

    assert isinstance(outcome, feed_work_scheduler.CallReplayableDirectFailure)
    assert operations.published == []
    assert outcome.facts.disposition is (
        feed_work_scheduler.CohortTerminalDisposition.REPLAYABLE_DIRECT
    )
    assert all(
        ledger.state(entry.obligation) is gap_ledger.MissingCallState.ADMITTED
        for entry in payload.entries
    )
    _observe_terminal_outcome(payload, outcome)
    assert all(
        ledger.state(entry.obligation)
        is gap_ledger.MissingCallState.REPLAY_RELEASED
        for entry in payload.entries
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("signal_kind", "expected_type"),
    [
        ("stop", feed_work_scheduler.CallStopped),
        ("loss", feed_work_scheduler.CallAuthorityLost),
    ],
    ids=("stop-during-stage", "authority-loss-during-stage"),
)
async def test_precommit_signal_interrupt_is_neutral(
    signal_kind: str,
    expected_type: type[object],
) -> None:
    grant = _grant()
    member = _member(grant)
    ledger = gap_ledger.MissingCallLedger(grant, 3)
    payload = pipeline.ScheduledCohortPayload(
        member,
        "session",
        (_entry(member, 1, ledger),),
    )
    stop = asyncio.Event()
    loss = asyncio.Event()
    execution, known, retained = _execution(
        grant,
        payload,
        stop_requested=stop,
        grant_lost=loss,
    )
    operations = _Operations()
    committer = _Committer()

    async def interrupted_stage(
        *_args: object,
        **_kwargs: object,
    ) -> object:
        if signal_kind == "stop":
            stop.set()
            raise asyncio.CancelledError
        loss.set()
        message = "grant lost"
        raise LeaseExpiredError(message)

    executor = _executor(operations, committer)
    executor._stage_operation = interrupted_stage

    outcome = await executor.execute(execution)

    assert isinstance(outcome, expected_type)
    assert committer.commits == []
    assert operations.published == []
    assert known == []
    assert retained == []
    _observe_terminal_outcome(payload, outcome)
    assert ledger.state(payload.entries[0].obligation) is (
        gap_ledger.MissingCallState.REPLAY_RELEASED
    )


@pytest.mark.asyncio
async def test_raw_cancellation_before_commit_releases_replay_safely() -> None:
    grant = _grant()
    member = _member(grant)
    ledger = gap_ledger.MissingCallLedger(grant, 3)
    payload = pipeline.ScheduledCohortPayload(
        member,
        "session",
        (_entry(member, 1, ledger),),
    )
    execution, known, retained = _execution(grant, payload)
    operations = _Operations()
    started = asyncio.Event()

    async def blocked_create(*_args: object, **_kwargs: object) -> object:
        started.set()
        await asyncio.Event().wait()
        raise AssertionError

    committer = _Committer()
    executor = _executor(operations, committer)
    executor._create_chunk_operation = blocked_create
    task = asyncio.create_task(executor.execute(execution))
    await started.wait()
    task.cancel("before-commit")

    with pytest.raises(asyncio.CancelledError) as raised:
        await task

    assert raised.value.args == ("before-commit",)
    assert committer.commits == []
    assert operations.published == []
    assert known == []
    assert retained == []
    payload.settlement_observer(payload.entries[0].source_order)(
        feed_work_scheduler.CallSettlement.REPLAY_SAFE_RELEASE
    )
    assert ledger.state(payload.entries[0].obligation) is (
        gap_ledger.MissingCallState.REPLAY_RELEASED
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("signal_name", "expected_type", "expected_disposition"),
    [
        (
            "stop_requested",
            feed_work_scheduler.CallStopped,
            feed_work_scheduler.CohortTerminalDisposition.STOPPED,
        ),
        (
            "grant_lost",
            feed_work_scheduler.CallAuthorityLost,
            feed_work_scheduler.CohortTerminalDisposition.AUTHORITY_LOST,
        ),
    ],
    ids=("postcommit-stop", "postcommit-authority-loss"),
)
async def test_postcommit_gate_abandons_every_unpublished_record(
    signal_name: str,
    expected_type: type[object],
    expected_disposition: feed_work_scheduler.CohortTerminalDisposition,
) -> None:
    grant = _grant()
    member = _member(grant)
    ledger = gap_ledger.MissingCallLedger(grant, 3)
    payload = pipeline.ScheduledCohortPayload(
        member,
        "session",
        (_entry(member, 1, ledger), _entry(member, 2, ledger)),
    )
    stop = asyncio.Event()
    loss = asyncio.Event()
    execution, _known, _retained = _execution(
        grant,
        payload,
        stop_requested=stop,
        grant_lost=loss,
    )

    async def gate(point: pipeline.ControlGate, _settled: int) -> None:
        if point is pipeline.ControlGate.POST_COMMIT:
            (stop if signal_name == "stop_requested" else loss).set()

    operations = _Operations()
    outcome = await _executor(
        operations,
        _Committer(),
        control_gate=gate,
    ).execute(execution)

    assert isinstance(outcome, expected_type)
    assert outcome.facts.disposition is expected_disposition
    assert operations.published == []
    assert all(
        record.terminal_reason
        is feed_work_scheduler.CohortRecordTerminalReason.PUBLICATION_ABANDONED
        for record in outcome.facts.records
    )
    assert all(
        ledger.state(entry.obligation) is gap_ledger.MissingCallState.EMITTED
        for entry in payload.entries
    )


@pytest.mark.asyncio
async def test_between_publication_gate_starts_no_later_publish() -> None:
    grant = _grant()
    member = _member(grant)
    ledger = gap_ledger.MissingCallLedger(grant, 3)
    payload = pipeline.ScheduledCohortPayload(
        member,
        "session",
        (_entry(member, 1, ledger), _entry(member, 2, ledger)),
    )
    stop = asyncio.Event()
    execution, _known, _retained = _execution(
        grant,
        payload,
        stop_requested=stop,
    )

    async def gate(point: pipeline.ControlGate, settled: int) -> None:
        if point is pipeline.ControlGate.BETWEEN_PUBLICATIONS and settled == 1:
            stop.set()

    operations = _Operations()
    outcome = await _executor(
        operations,
        _Committer(),
        control_gate=gate,
    ).execute(execution)

    assert isinstance(outcome, feed_work_scheduler.CallStopped)
    assert operations.published == [payload.entries[0].audio_url]
    assert outcome.facts.records[0].full_pipeline_completed
    assert outcome.facts.records[1].terminal_reason is (
        feed_work_scheduler.CohortRecordTerminalReason.PUBLICATION_ABANDONED
    )
    assert tuple(
        ledger.state(entry.obligation) for entry in payload.entries
    ) == (
        gap_ledger.MissingCallState.RESOLVED,
        gap_ledger.MissingCallState.EMITTED,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("result_kind", "expected_handoff", "expected_retention"),
    [
        ("accepted", feed_work_scheduler.CallCompleted, None),
        ("grant-rejected", feed_work_scheduler.CallAuthorityLost, None),
        ("member-rejected", feed_work_scheduler.CallMembershipRejected, None),
        ("no-start", feed_work_scheduler.CallRetryable, None),
        (
            "malformed",
            None,
            feed_work_scheduler.CohortTerminalDisposition.INTEGRITY_FAILURE,
        ),
        (
            "unknown",
            None,
            feed_work_scheduler.CohortTerminalDisposition.OUTCOME_UNKNOWN,
        ),
    ],
    ids=(
        "cancel-during-accepted-commit",
        "cancel-during-grant-rejection",
        "cancel-during-member-rejection",
        "cancel-during-proven-no-start",
        "cancel-during-malformed-result",
        "cancel-during-unknown-result",
    ),
)
async def test_commit_cancellation_mapping_and_original_error(
    result_kind: str,
    expected_handoff: type[object] | None,
    expected_retention: (feed_work_scheduler.CohortTerminalDisposition | None),
) -> None:
    grant = _grant()
    member = _member(grant)
    ledger = gap_ledger.MissingCallLedger(grant, 3)
    payload = pipeline.ScheduledCohortPayload(
        member,
        "session",
        (_entry(member, 1, ledger),),
    )
    execution, known, retained = _execution(grant, payload)
    started = asyncio.Event()
    release = asyncio.Event()

    class BlockingCommitter(_Committer):
        async def commit_cohort(self, grant, commit, *, final_logical):
            del grant, final_logical
            self.commits.append(commit)
            started.set()
            await release.wait()
            if result_kind == "accepted":
                return runtime_adapters.PhysicalCohortResult(
                    commit,
                    feed_work_scheduler.BoundaryDisposition.COMMITTED,
                )
            if result_kind == "grant-rejected":
                return feed_work_scheduler.BoundaryGrantRejected()
            if result_kind == "member-rejected":
                return runtime_adapters.PhysicalCohortResult(
                    commit,
                    feed_work_scheduler.BoundaryDisposition.MEMBER_REJECTED,
                )
            if result_kind == "no-start":
                return feed_work_scheduler.BoundaryBatchRetryable()
            if result_kind == "malformed":
                return object()
            message = "commit acknowledgement unknown"
            raise OSError(message)

    task = asyncio.create_task(
        _executor(_Operations(), BlockingCommitter()).execute(execution)
    )
    await started.wait()
    task.cancel("first-cancel")
    await asyncio.sleep(0)
    task.cancel("repeated-cancel")
    await asyncio.sleep(0)
    release.set()

    with pytest.raises(asyncio.CancelledError) as raised:
        await task

    assert raised.value.args == ("first-cancel",)
    if expected_handoff is not None:
        assert len(known) == 1
        assert isinstance(known[0], expected_handoff)
        assert retained == []
        states = tuple(
            ledger.state(entry.obligation) for entry in payload.entries
        )
        if result_kind == "accepted":
            assert states == (gap_ledger.MissingCallState.EMITTED,)
        else:
            assert states == (gap_ledger.MissingCallState.ADMITTED,)
        _observe_terminal_outcome(payload, known[0])
        assert ledger.state(payload.entries[0].obligation) is (
            gap_ledger.MissingCallState.EMITTED
            if result_kind == "accepted"
            else gap_ledger.MissingCallState.REPLAY_RELEASED
        )
    else:
        assert known == []
        assert len(retained) == 1
        assert retained[0].terminal_facts.disposition is expected_retention
        if expected_retention is (
            feed_work_scheduler.CohortTerminalDisposition.INTEGRITY_FAILURE
        ):
            assert retained[0].failure is not None
        assert ledger.state(payload.entries[0].obligation) is (
            gap_ledger.MissingCallState.UNKNOWN_RETAINED
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("publication_kind", "expected_disposition"),
    [
        ("success", None),
        ("exhausted", None),
        (
            "unknown",
            feed_work_scheduler.CohortTerminalDisposition.OUTCOME_UNKNOWN,
        ),
        (
            "malformed",
            feed_work_scheduler.CohortTerminalDisposition.INTEGRITY_FAILURE,
        ),
    ],
    ids=(
        "cancel-during-publish-success",
        "cancel-during-publish-exhaustion",
        "cancel-during-publish-unknown",
        "cancel-during-publish-malformed",
    ),
)
async def test_publication_cancellation_settles_or_retains(
    publication_kind: str,
    expected_disposition: (
        feed_work_scheduler.CohortTerminalDisposition | None
    ),
) -> None:
    grant = _grant()
    member = _member(grant)
    ledger = gap_ledger.MissingCallLedger(grant, 3)
    payload = pipeline.ScheduledCohortPayload(
        member,
        "session",
        (_entry(member, 1, ledger), _entry(member, 2, ledger)),
    )
    execution, known, retained = _execution(grant, payload)
    operations = _Operations()
    started = asyncio.Event()
    release = asyncio.Event()

    async def publish(
        *_args: object,
        **_kwargs: object,
    ) -> CommittedAwaitableSettlement[object]:
        async def child() -> object:
            started.set()
            await release.wait()
            if publication_kind == "exhausted":
                message = "unavailable"
                raise google_exceptions.ServiceUnavailable(message)
            if publication_kind == "unknown":
                message = "may have published"
                raise ValueError(message)
            if publication_kind == "malformed":
                return object()
            return pipeline.PublishedChunk("message", 1)

        return await settle_committed_awaitable(child())

    executor = _executor(operations, _Committer())
    executor._publish_operation = publish
    task = asyncio.create_task(executor.execute(execution))
    await started.wait()
    task.cancel("publish-cancel")
    await asyncio.sleep(0)
    release.set()

    with pytest.raises(asyncio.CancelledError) as raised:
        await task

    assert raised.value.args == ("publish-cancel",)
    if expected_disposition is not None:
        assert known == []
        assert len(retained) == 1
        assert retained[0].cause is (
            feed_work_scheduler.OutcomeUnknownCause.PUBLICATION
        )
        assert retained[0].terminal_facts.disposition is expected_disposition
        if expected_disposition is (
            feed_work_scheduler.CohortTerminalDisposition.INTEGRITY_FAILURE
        ):
            assert retained[0].failure is not None
        assert all(
            ledger.state(entry.obligation)
            is gap_ledger.MissingCallState.UNKNOWN_RETAINED
            for entry in payload.entries
        )
    else:
        assert len(known) == 1
        assert isinstance(known[0], feed_work_scheduler.CallCompleted)
        assert retained == []
        assert all(
            ledger.state(entry.obligation)
            is not gap_ledger.MissingCallState.ADMITTED
            for entry in payload.entries
        )


@pytest.mark.asyncio
async def test_validation_precedes_every_physical_side_effect() -> None:
    grant = _grant()
    member = _member(grant)
    ledger = gap_ledger.MissingCallLedger(grant, 3)
    entry = _entry(member, 2, ledger)
    payload = pipeline.ScheduledCohortPayload(member, "session", (entry,))
    execution, _known, _retained = _execution(grant, payload)
    forged_identity = dataclasses.replace(
        execution.calls[0].identity,
        source_order=3,
    )
    forged_execution = feed_work_scheduler.CohortExecution(
        (feed_work_scheduler.CallExecution(forged_identity, payload),),
        execution.signals,
        _types._issue_retention_handle((forged_identity,), mock.Mock()),
        _types._issue_cancellation_handoff((forged_identity,), mock.Mock()),
    )
    operations = _Operations()

    with pytest.raises(
        feed_work_scheduler.CohortIntegrityError,
        match="source order",
    ):
        await _executor(operations, _Committer()).execute(forged_execution)

    assert operations.created == []
    assert operations.staged == []
    assert operations.published == []
