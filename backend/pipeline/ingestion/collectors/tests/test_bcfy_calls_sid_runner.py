"""Behavioral tests for page-sequential Broadcastify Calls SID ingestion."""

from __future__ import annotations

import asyncio
import datetime
import logging
import typing
import uuid
from unittest import mock

import asyncpg
import pytest

from backend.pipeline.ingestion import failure_policy, grant_control, models
from backend.pipeline.ingestion.collectors import failure_classification
from backend.pipeline.ingestion.collectors.bcfy_calls import (
    pipeline,
    provider,
    sid_runner,
    work_pool,
)
from backend.pipeline.storage import feed_store, ingestion_lease_store

if typing.TYPE_CHECKING:
    import collections.abc

_NOW = datetime.datetime(2026, 7, 19, 12, tzinfo=datetime.UTC)


def _grant() -> ingestion_lease_store.LeaseGrant:
    return ingestion_lease_store.LeaseGrant(
        feed_store.SourceType.BCFY_CALLS,
        "7017",
        uuid.uuid4(),
        8,
    )


def _member(
    group_id: str,
    *,
    bookmark: datetime.datetime | None,
    retry_after: datetime.datetime | None = None,
) -> ingestion_lease_store.LeaseMember:
    return ingestion_lease_store.LeaseMember(
        identity=ingestion_lease_store.LeaseMemberIdentity(
            uuid.uuid4(),
            feed_store.SourceType.BCFY_CALLS,
            f"7017-{group_id}",
        ),
        name=f"Talkgroup {group_id}",
        last_bookmark_time=bookmark,
        retry_after=retry_after,
    )


def _snapshot(
    grant: ingestion_lease_store.LeaseGrant,
    *members: ingestion_lease_store.LeaseMember,
) -> ingestion_lease_store.MembershipSnapshot:
    return ingestion_lease_store.MembershipSnapshot(grant, 1, members)


def _result(
    batch: pipeline.FeedBatch,
    *,
    attempted: int | None = None,
    published: int | None = None,
    terminal: (
        failure_classification.ItemFailure
        | ingestion_lease_store.GrantRejected
        | None
    ) = None,
) -> pipeline.FeedBatchResult:
    attempted_count = len(batch.calls) if attempted is None else attempted
    published_count = attempted_count if published is None else published
    return pipeline.FeedBatchResult(
        attempted_count=attempted_count,
        published_count=published_count,
        next_sequence=batch.starting_sequence + attempted_count,
        committed_urls=tuple(call.audio_url for call in batch.calls),
        terminal=terminal,
    )


class _Store:
    def __init__(
        self,
        snapshot: ingestion_lease_store.MembershipSnapshot,
    ) -> None:
        self.snapshot = snapshot
        self.batches: list[ingestion_lease_store.ChildMutationBatch] = []

    async def load_membership(
        self,
        grant: ingestion_lease_store.LeaseGrant,
    ) -> ingestion_lease_store.MembershipSnapshot:
        assert grant == self.snapshot.grant
        return self.snapshot

    async def commit_child_mutations(
        self,
        grant: ingestion_lease_store.LeaseGrant,
        batch: ingestion_lease_store.ChildMutationBatch,
        *,
        actor_id: str,
    ) -> (
        ingestion_lease_store.BatchCommitted
        | ingestion_lease_store.GrantRejected
    ):
        assert grant == self.snapshot.grant
        assert actor_id == "test"
        self.batches.append(batch)
        children = tuple(
            ingestion_lease_store.ChildMutationResult(
                mutation.member.feed_id,
                ingestion_lease_store.ChildDisposition.COMMITTED,
            )
            for mutation in batch.mutations
        )
        return ingestion_lease_store.BatchCommitted(children)


class _ScriptedMembershipStore(_Store):
    def __init__(
        self,
        snapshot: ingestion_lease_store.MembershipSnapshot,
        *membership_results: (
            ingestion_lease_store.MembershipSnapshot | BaseException
        ),
    ) -> None:
        super().__init__(snapshot)
        self.membership_results = list(membership_results)
        self.membership_calls = 0

    async def load_membership(
        self,
        grant: ingestion_lease_store.LeaseGrant,
    ) -> ingestion_lease_store.MembershipSnapshot:
        assert grant == self.snapshot.grant
        self.membership_calls += 1
        result = self.membership_results.pop(0)
        if isinstance(result, BaseException):
            raise result
        return result


class _Provider:
    def __init__(
        self,
        pages: list[provider.CallsPageEnvelope],
        context: grant_control.RunContext,
    ) -> None:
        self.pages = pages
        self.context = context
        self.positions: list[datetime.datetime | None] = []

    async def fetch_sid_page(
        self,
        sid: str,
        pos: datetime.datetime | None,
        *,
        shutdown_event: asyncio.Event,
    ) -> provider.CallsPageEnvelope:
        assert sid == "7017"
        assert not shutdown_event.is_set()
        self.positions.append(pos)
        page = self.pages.pop(0)
        if not self.pages:
            self.context.stop_requested.set()
        return page


class _Pool:
    def __init__(
        self,
        result_factory: mock.Mock | None = None,
    ) -> None:
        self.batches: list[pipeline.FeedBatch] = []
        self.result_factory = result_factory or mock.Mock(side_effect=_result)

    async def settle_batches(
        self,
        batches: collections.abc.Sequence[pipeline.FeedBatch],
    ) -> work_pool.SettledBatches[
        pipeline.FeedBatch,
        pipeline.FeedBatchResult,
    ]:
        self.batches.extend(batches)
        return work_pool.SettledBatches(
            results=tuple(
                (batch, self.result_factory(batch)) for batch in batches
            ),
            failure=None,
            cancellation=None,
        )


def _failure_planner(
    status_reason: feed_store.FeedStatusReason,
    reason: str | None,
) -> failure_policy.FailurePersistencePlan:
    return failure_policy.plan_failure(
        status_reason,
        reason,
        budgeted=failure_policy.ConsumeFailureBudget(5, 60, 900),
        non_budgeted=_retry_without_budget,
    )


def _retry_without_budget() -> failure_policy.RetryWithoutBudget:
    return failure_policy.RetryWithoutBudget(
        _NOW + datetime.timedelta(minutes=5)
    )


def _context() -> grant_control.RunContext:
    return grant_control.RunContext(asyncio.Event(), asyncio.Event())


def test_provider_timestamp_preserves_fractional_seconds() -> None:
    timestamp = _NOW.timestamp() + 0.75

    assert sid_runner._utc_timestamp(timestamp) == (
        datetime.datetime.fromtimestamp(timestamp, datetime.UTC)
    )


def test_page_boundary_rejects_epoch_without_requested_position() -> None:
    assert sid_runner._valid_page_boundary(0, None, _NOW) is None


def test_page_boundary_uses_the_integer_position_sent_to_provider() -> None:
    requested = _NOW + datetime.timedelta(microseconds=800_000)
    boundary = _NOW + datetime.timedelta(microseconds=100_000)

    assert (
        sid_runner._valid_page_boundary(
            boundary.timestamp(),
            requested,
            _NOW + datetime.timedelta(seconds=1),
        )
        == boundary
    )


@pytest.mark.parametrize(
    "failure_type",
    [
        OSError,
        asyncpg.TooManyConnectionsError,
        asyncpg.AdminShutdownError,
        asyncpg.CrashShutdownError,
        asyncpg.CannotConnectNowError,
        asyncpg.QueryCanceledError,
    ],
)
@pytest.mark.asyncio
async def test_transient_membership_failure_retries_before_fetch(
    failure_type: type[Exception],
) -> None:
    grant = _grant()
    member = _member("100", bookmark=_NOW - datetime.timedelta(minutes=1))
    snapshot = _snapshot(grant, member)
    context = _context()
    store = _ScriptedMembershipStore(
        snapshot,
        failure_type("backend temporarily unavailable"),
        snapshot,
    )
    page = provider.CallsPageEnvelope({}, (), _NOW.timestamp())
    calls_provider = _Provider([page], context)
    runner = sid_runner.BcfyCallsSidRunner(
        store,
        calls_provider,
        _Pool(),
        _failure_planner,
        actor_id="test",
        poll_interval_sec=0,
        clock=lambda: _NOW,
    )

    outcome = await runner.run(
        grant,
        grant_control.ClaimMode.PRIMARY,
        context,
    )

    assert isinstance(outcome, grant_control.RunCompleted)
    assert store.membership_calls == 2
    assert calls_provider.positions == [member.last_bookmark_time]


@pytest.mark.asyncio
async def test_tenth_transient_membership_failure_closes_sid_run() -> None:
    grant = _grant()
    snapshot = _snapshot(grant)
    context = _context()
    store = _ScriptedMembershipStore(
        snapshot,
        *(OSError("database unavailable") for _ in range(10)),
    )
    calls_provider = _Provider([], context)
    runner = sid_runner.BcfyCallsSidRunner(
        store,
        calls_provider,
        _Pool(),
        _failure_planner,
        actor_id="test",
        poll_interval_sec=0,
        clock=lambda: _NOW,
    )

    outcome = await runner.run(
        grant,
        grant_control.ClaimMode.PRIMARY,
        context,
    )

    assert outcome == grant_control.RunFailed(
        feed_store.FeedStatusReason.SYSTEM_COLLECTOR_ERROR,
        "bcfy_calls_sid_membership_refresh_failed",
    )
    assert store.membership_calls == 10
    assert calls_provider.positions == []


@pytest.mark.asyncio
async def test_successful_backoff_polls_reset_membership_failure_streak() -> (
    None
):
    grant = _grant()
    deferred = _member(
        "100",
        bookmark=_NOW - datetime.timedelta(minutes=1),
        retry_after=_NOW + datetime.timedelta(minutes=1),
    )
    due = ingestion_lease_store.LeaseMember(
        identity=deferred.identity,
        name=deferred.name,
        last_bookmark_time=deferred.last_bookmark_time,
        retry_after=None,
    )
    deferred_snapshot = _snapshot(grant, deferred)
    due_snapshot = _snapshot(grant, due)
    membership_results: list[
        ingestion_lease_store.MembershipSnapshot | BaseException
    ] = []
    for _ in range(10):
        membership_results.extend(
            (OSError("database unavailable"), deferred_snapshot)
        )
    membership_results.append(due_snapshot)

    context = _context()
    store = _ScriptedMembershipStore(
        due_snapshot,
        *membership_results,
    )
    calls_provider = _Provider(
        [provider.CallsPageEnvelope({}, (), _NOW.timestamp())],
        context,
    )
    runner = sid_runner.BcfyCallsSidRunner(
        store,
        calls_provider,
        _Pool(),
        _failure_planner,
        actor_id="test",
        poll_interval_sec=0,
        clock=lambda: _NOW,
    )

    outcome = await runner.run(
        grant,
        grant_control.ClaimMode.PRIMARY,
        context,
    )

    assert isinstance(outcome, grant_control.RunCompleted)
    assert store.membership_calls == 21
    assert calls_provider.positions == [due.last_bookmark_time]


@pytest.mark.asyncio
async def test_membership_success_does_not_reset_metadata_failure_streak() -> (
    None
):
    grant = _grant()
    member = _member("100", bookmark=_NOW - datetime.timedelta(minutes=1))
    context = _context()

    class FailingProvider:
        def __init__(self) -> None:
            self.calls = 0

        async def fetch_sid_page(
            self,
            sid: str,
            pos: datetime.datetime | None,
            *,
            shutdown_event: asyncio.Event,
        ) -> provider.CallsPageEnvelope:
            assert sid == grant.lease_key
            assert pos == member.last_bookmark_time
            assert shutdown_event is context.stop_requested
            self.calls += 1
            if self.calls > 10:
                raise provider.TokenLoadStopped
            raise models.FeedFailure(
                feed_store.FeedStatusReason.SYSTEM_AUTHENTICATION_FAILED,
                "calls_api_http_401",
            )

    calls_provider = FailingProvider()
    runner = sid_runner.BcfyCallsSidRunner(
        _Store(_snapshot(grant, member)),
        calls_provider,
        _Pool(),
        _failure_planner,
        actor_id="test",
        poll_interval_sec=0,
        clock=lambda: _NOW,
    )

    outcome = await runner.run(
        grant,
        grant_control.ClaimMode.PRIMARY,
        context,
    )

    assert outcome == grant_control.RunFailed(
        feed_store.FeedStatusReason.SYSTEM_AUTHENTICATION_FAILED,
        "calls_api_http_401",
    )
    assert calls_provider.calls == 10


@pytest.mark.asyncio
async def test_unknown_membership_failure_propagates() -> None:
    grant = _grant()
    snapshot = _snapshot(grant)
    context = _context()
    store = _ScriptedMembershipStore(
        snapshot,
        RuntimeError("malformed membership result"),
    )
    runner = sid_runner.BcfyCallsSidRunner(
        store,
        _Provider([], context),
        _Pool(),
        _failure_planner,
        actor_id="test",
        poll_interval_sec=0,
        clock=lambda: _NOW,
    )

    with pytest.raises(RuntimeError, match="malformed membership result"):
        await runner.run(
            grant,
            grant_control.ClaimMode.PRIMARY,
            context,
        )


@pytest.mark.asyncio
async def test_forced_cancellation_persists_settled_publish_gap(
    caplog: pytest.LogCaptureFixture,
) -> None:
    grant = _grant()
    member = _member("100", bookmark=_NOW - datetime.timedelta(minutes=1))
    context = _context()
    page = provider.CallsPageEnvelope(
        {},
        (
            {
                "groupId": "7017-100",
                "url": "https://audio/gap",
                "ts": _NOW.timestamp(),
            },
        ),
        _NOW.timestamp(),
    )
    failure = failure_classification.ItemFailure(
        feed_store.FeedStatusReason.PIPELINE_PUBLISH_AFTER_BOOKMARK_FAILED,
        "publish failed",
    )

    class DelayedPool:
        def __init__(self) -> None:
            self.batch: pipeline.FeedBatch | None = None
            self.submitted = asyncio.Event()
            self.completion = asyncio.get_running_loop().create_future()

        async def settle_batches(
            self,
            batches: collections.abc.Sequence[pipeline.FeedBatch],
        ) -> work_pool.SettledBatches[
            pipeline.FeedBatch,
            pipeline.FeedBatchResult,
        ]:
            assert len(batches) == 1
            batch = batches[0]
            self.batch = batch
            self.submitted.set()
            cancellation: asyncio.CancelledError | None = None
            while not self.completion.done():
                try:
                    await asyncio.wait((self.completion,))
                except asyncio.CancelledError as error:
                    if cancellation is None:
                        cancellation = error
            return work_pool.SettledBatches(
                results=((batch, self.completion.result()),),
                failure=None,
                cancellation=cancellation,
            )

    store = _Store(_snapshot(grant, member))
    pool = DelayedPool()
    runner = sid_runner.BcfyCallsSidRunner(
        store,
        _Provider([page], context),
        pool,
        _failure_planner,
        actor_id="test",
        poll_interval_sec=0,
        clock=lambda: _NOW,
    )
    run = asyncio.create_task(
        runner.run(grant, grant_control.ClaimMode.PRIMARY, context)
    )
    await pool.submitted.wait()
    assert pool.batch is not None

    with caplog.at_level(logging.INFO, logger=sid_runner.logger.name):
        run.cancel()
        await asyncio.sleep(0)
        pool.completion.set_result(
            _result(pool.batch, published=0, terminal=failure)
        )
        with pytest.raises(asyncio.CancelledError):
            await run

    mutation = store.batches[0].mutations[0]
    assert isinstance(mutation, ingestion_lease_store.FeedFailureTransition)
    events = [
        record.__dict__.get("json_fields", {}).get("event_type")
        for record in caplog.records
    ]
    assert "feed_failure_policy_decision" in events
    assert "post_bookmark_publish_failure" in events


@pytest.mark.asyncio
async def test_admission_error_drains_already_accepted_batch() -> None:
    grant = _grant()
    first = _member("100", bookmark=_NOW - datetime.timedelta(minutes=1))
    second = _member("200", bookmark=_NOW - datetime.timedelta(minutes=1))
    context = _context()
    page = provider.CallsPageEnvelope(
        {},
        (
            {
                "groupId": "7017-100",
                "url": "https://audio/1",
                "ts": _NOW.timestamp(),
            },
            {
                "groupId": "7017-200",
                "url": "https://audio/2",
                "ts": _NOW.timestamp(),
            },
        ),
        _NOW.timestamp(),
    )

    class PartiallyRejectingPool:
        def __init__(self) -> None:
            self.batches: list[pipeline.FeedBatch] = []
            self.completion = asyncio.get_running_loop().create_future()

        async def settle_batches(
            self,
            batches: collections.abc.Sequence[pipeline.FeedBatch],
        ) -> work_pool.SettledBatches[
            pipeline.FeedBatch,
            pipeline.FeedBatchResult,
        ]:
            self.batches.extend(batches)
            result = await self.completion
            return work_pool.SettledBatches(
                results=((batches[0], result),),
                failure=RuntimeError("admission closed"),
                cancellation=None,
            )

    pool = PartiallyRejectingPool()
    store = _Store(_snapshot(grant, first, second))
    runner = sid_runner.BcfyCallsSidRunner(
        store,
        _Provider([page], context),
        pool,
        _failure_planner,
        actor_id="test",
        poll_interval_sec=0,
        clock=lambda: _NOW,
    )

    run = asyncio.create_task(
        runner.run(
            grant,
            grant_control.ClaimMode.PRIMARY,
            context,
        )
    )
    await asyncio.sleep(0)
    assert not run.done()
    pool.completion.set_result(_result(pool.batches[0]))

    with pytest.raises(RuntimeError, match="admission closed"):
        await run
    assert store.batches[0].mutations == (
        ingestion_lease_store.SourceObservation(first.identity, _NOW),
    )
    assert isinstance(
        store.batches[0].lease_effect,
        ingestion_lease_store.NoLeaseEffect,
    )


@pytest.mark.asyncio
async def test_page_commit_grant_loss_does_not_hide_batch_failure() -> None:
    grant = _grant()
    first = _member("100", bookmark=_NOW - datetime.timedelta(minutes=1))
    second = _member("200", bookmark=_NOW - datetime.timedelta(minutes=1))
    context = _context()
    page = provider.CallsPageEnvelope(
        {},
        (
            {
                "groupId": "7017-100",
                "url": "https://audio/1",
                "ts": _NOW.timestamp(),
            },
            {
                "groupId": "7017-200",
                "url": "https://audio/2",
                "ts": _NOW.timestamp(),
            },
        ),
        _NOW.timestamp(),
    )
    batch_failure = RuntimeError("batch execution failed")
    rejection = ingestion_lease_store.GrantRejected(
        ingestion_lease_store.GrantRejectionReason.FENCE_MISMATCH
    )

    class PartiallyFailingPool:
        async def settle_batches(
            self,
            batches: collections.abc.Sequence[pipeline.FeedBatch],
        ) -> work_pool.SettledBatches[
            pipeline.FeedBatch,
            pipeline.FeedBatchResult,
        ]:
            return work_pool.SettledBatches(
                results=((batches[0], _result(batches[0])),),
                failure=batch_failure,
                cancellation=None,
            )

    class RejectingStore(_Store):
        async def commit_child_mutations(
            self,
            grant: ingestion_lease_store.LeaseGrant,
            batch: ingestion_lease_store.ChildMutationBatch,
            *,
            actor_id: str,
        ) -> ingestion_lease_store.GrantRejected:
            assert grant == self.snapshot.grant
            assert actor_id == "test"
            self.batches.append(batch)
            return rejection

    store = RejectingStore(_snapshot(grant, first, second))
    runner = sid_runner.BcfyCallsSidRunner(
        store,
        _Provider([page], context),
        PartiallyFailingPool(),
        _failure_planner,
        actor_id="test",
        poll_interval_sec=0,
        clock=lambda: _NOW,
    )

    with pytest.raises(RuntimeError, match="batch execution failed") as raised:
        await runner.run(
            grant,
            grant_control.ClaimMode.PRIMARY,
            context,
        )

    assert raised.value is batch_failure
    assert len(store.batches) == 1


@pytest.mark.asyncio
async def test_partial_cancellation_commits_only_proven_feed_evidence() -> None:
    grant = _grant()
    settled_member = _member(
        "100",
        bookmark=_NOW - datetime.timedelta(minutes=1),
    )
    unresolved_member = _member(
        "200",
        bookmark=_NOW - datetime.timedelta(minutes=1),
    )
    quiet_member = _member(
        "300",
        bookmark=_NOW - datetime.timedelta(minutes=1),
    )
    context = _context()
    page = provider.CallsPageEnvelope(
        {},
        (
            {
                "groupId": "7017-100",
                "url": "https://audio/1",
                "ts": _NOW.timestamp(),
            },
            {
                "groupId": "7017-200",
                "url": "https://audio/2",
                "ts": _NOW.timestamp(),
            },
        ),
        _NOW.timestamp(),
    )
    cancellation = asyncio.CancelledError("stop after first admission")

    class PartiallyCancelledPool:
        async def settle_batches(
            self,
            batches: collections.abc.Sequence[pipeline.FeedBatch],
        ) -> work_pool.SettledBatches[
            pipeline.FeedBatch,
            pipeline.FeedBatchResult,
        ]:
            return work_pool.SettledBatches(
                results=((batches[0], _result(batches[0])),),
                failure=None,
                cancellation=cancellation,
            )

    store = _Store(
        _snapshot(
            grant,
            settled_member,
            unresolved_member,
            quiet_member,
        )
    )
    runner = sid_runner.BcfyCallsSidRunner(
        store,
        _Provider([page], context),
        PartiallyCancelledPool(),
        _failure_planner,
        actor_id="test",
        poll_interval_sec=0,
        clock=lambda: _NOW,
    )

    with pytest.raises(asyncio.CancelledError) as raised:
        await runner.run(
            grant,
            grant_control.ClaimMode.PRIMARY,
            context,
        )

    assert raised.value is cancellation
    assert store.batches[0].mutations == (
        ingestion_lease_store.SourceObservation(
            settled_member.identity,
            _NOW,
        ),
        ingestion_lease_store.SourceObservation(
            quiet_member.identity,
            _NOW,
        ),
    )
    assert isinstance(
        store.batches[0].lease_effect,
        ingestion_lease_store.NoLeaseEffect,
    )


@pytest.mark.asyncio
async def test_cancellation_before_acceptance_starts_no_page_commit() -> None:
    grant = _grant()
    routed = _member("100", bookmark=_NOW - datetime.timedelta(minutes=1))
    quiet = _member("200", bookmark=_NOW - datetime.timedelta(minutes=1))
    context = _context()
    page = provider.CallsPageEnvelope(
        {},
        (
            {
                "groupId": "7017-100",
                "url": "https://audio/1",
                "ts": _NOW.timestamp(),
            },
        ),
        _NOW.timestamp(),
    )
    cancellation = asyncio.CancelledError("cancel before admission")

    class CancelledPool:
        async def settle_batches(
            self,
            batches: collections.abc.Sequence[pipeline.FeedBatch],
        ) -> work_pool.SettledBatches[
            pipeline.FeedBatch,
            pipeline.FeedBatchResult,
        ]:
            assert len(batches) == 1
            return work_pool.SettledBatches(
                results=(),
                failure=None,
                cancellation=cancellation,
            )

    store = _Store(_snapshot(grant, routed, quiet))
    runner = sid_runner.BcfyCallsSidRunner(
        store,
        _Provider([page], context),
        CancelledPool(),
        _failure_planner,
        actor_id="test",
        poll_interval_sec=0,
        clock=lambda: _NOW,
    )

    with pytest.raises(asyncio.CancelledError) as raised:
        await runner.run(
            grant,
            grant_control.ClaimMode.PRIMARY,
            context,
        )

    assert raised.value is cancellation
    assert store.batches == []


@pytest.mark.asyncio
async def test_accepted_child_cancellation_retains_caller_cancellation() -> (
    None
):
    grant = _grant()
    member = _member("100", bookmark=_NOW - datetime.timedelta(minutes=1))
    context = _context()
    page = provider.CallsPageEnvelope(
        {},
        (
            {
                "groupId": "7017-100",
                "url": "https://audio/1",
                "ts": _NOW.timestamp(),
            },
        ),
        _NOW.timestamp(),
    )
    child_failure = asyncio.CancelledError("executor cancellation")
    caller_cancellation = asyncio.CancelledError("runner cancellation")

    class CancelledPool:
        async def settle_batches(
            self,
            batches: collections.abc.Sequence[pipeline.FeedBatch],
        ) -> work_pool.SettledBatches[
            pipeline.FeedBatch,
            pipeline.FeedBatchResult,
        ]:
            assert len(batches) == 1
            return work_pool.SettledBatches(
                results=(),
                failure=child_failure,
                cancellation=caller_cancellation,
            )

    store = _Store(_snapshot(grant, member))
    runner = sid_runner.BcfyCallsSidRunner(
        store,
        _Provider([page], context),
        CancelledPool(),
        _failure_planner,
        actor_id="test",
        poll_interval_sec=0,
        clock=lambda: _NOW,
    )

    with pytest.raises(grant_control.GrantControlIntegrityError) as raised:
        await runner.run(
            grant,
            grant_control.ClaimMode.PRIMARY,
            context,
        )

    assert raised.value.__cause__ is child_failure
    assert child_failure.__cause__ is caller_cancellation
    assert store.batches == []


@pytest.mark.asyncio
async def test_routes_due_members_and_adopts_null_cursor_at_live_edge(
    caplog: pytest.LogCaptureFixture,
) -> None:
    grant = _grant()
    due = _member(
        "100",
        bookmark=_NOW - datetime.timedelta(seconds=30),
    )
    adopter = _member("200", bookmark=None)
    deferred = _member(
        "300",
        bookmark=_NOW - datetime.timedelta(minutes=2),
        retry_after=_NOW + datetime.timedelta(minutes=1),
    )
    context = _context()
    calls = (
        {
            "groupId": "7017-100",
            "url": "https://audio/1",
            "ts": _NOW.timestamp(),
        },
        {
            "groupId": "7017-100",
            "url": "https://audio/1",
            "ts": _NOW.timestamp(),
        },
        {
            "groupId": "7017-200",
            "url": "https://audio/2",
            "ts": _NOW.timestamp(),
        },
        {
            "groupId": "7017-300",
            "url": "https://audio/3",
            "ts": _NOW.timestamp(),
        },
    )
    historical_boundary = _NOW - datetime.timedelta(seconds=10)
    page = provider.CallsPageEnvelope(
        {},
        calls,
        historical_boundary.timestamp(),
    )
    store = _Store(_snapshot(grant, due, adopter, deferred))
    calls_provider = _Provider([page], context)
    pool = _Pool()
    request_started = _NOW + datetime.timedelta(seconds=2)
    clock = mock.Mock(
        side_effect=(
            _NOW,
            request_started,
            _NOW + datetime.timedelta(seconds=3),
        )
    )
    runner = sid_runner.BcfyCallsSidRunner(
        store,
        calls_provider,
        pool,
        _failure_planner,
        actor_id="test",
        poll_interval_sec=0,
        clock=clock,
    )

    with caplog.at_level(logging.INFO, sid_runner.__name__):
        outcome = await runner.run(
            grant,
            grant_control.ClaimMode.PRIMARY,
            context,
        )

    assert isinstance(outcome, grant_control.RunCompleted)
    assert calls_provider.positions == [due.last_bookmark_time]
    assert [batch.member for batch in pool.batches] == [due]
    assert len(pool.batches[0].calls) == 1
    assert len(store.batches) == 1
    committed_ids = {
        mutation.member.feed_id for mutation in store.batches[0].mutations
    }
    assert committed_ids == {
        due.identity.feed_id,
        adopter.identity.feed_id,
    }
    observations = {
        mutation.member.feed_id: mutation.cursor
        for mutation in store.batches[0].mutations
        if isinstance(mutation, ingestion_lease_store.SourceObservation)
    }
    assert observations == {
        due.identity.feed_id: historical_boundary,
        adopter.identity.feed_id: request_started,
    }
    assert isinstance(
        store.batches[0].lease_effect,
        ingestion_lease_store.FinalizeLeaseRecovery,
    )
    settled = [
        record
        for record in caplog.records
        if getattr(record, "json_fields", {}).get("event_type")
        == "bcfy_calls_sid_poll_settled"
    ]
    assert len(settled) == 1
    assert getattr(settled[0], "json_fields", {}).get("status") == "completed"


def test_departed_member_state_is_pruned() -> None:
    current = _member("100", bookmark=_NOW)
    departed = _member("200", bookmark=_NOW)
    current_state = sid_runner._FeedState()
    states = {
        current.identity.feed_id: current_state,
        departed.identity.feed_id: sid_runner._FeedState(),
    }

    sid_runner._prune_departed_states(states, (current,))

    assert states == {current.identity.feed_id: current_state}


@pytest.mark.asyncio
async def test_routes_only_new_well_formed_calls_for_current_members() -> None:
    grant = _grant()
    bookmark = _NOW - datetime.timedelta(seconds=30)
    member = _member("100", bookmark=bookmark)
    context = _context()
    page = provider.CallsPageEnvelope(
        {},
        (
            {
                "groupId": "7017-100",
                "url": "https://audio/equal",
                "ts": bookmark.timestamp(),
            },
            {
                "groupId": "7017-100",
                "url": "https://audio/older",
                "ts": (bookmark - datetime.timedelta(seconds=1)).timestamp(),
            },
            {
                "groupId": "7017-999",
                "url": "https://audio/untracked",
                "ts": _NOW.timestamp(),
            },
            {
                "groupId": True,
                "url": "https://audio/bool-group",
                "ts": _NOW.timestamp(),
            },
            {
                "groupId": "7017-100",
                "url": "",
                "ts": _NOW.timestamp(),
            },
            "not-a-call",
            {
                "groupId": "7017-100",
                "url": "https://audio/new",
                "ts": _NOW.timestamp(),
            },
        ),
        _NOW.timestamp(),
    )
    pool = _Pool()
    runner = sid_runner.BcfyCallsSidRunner(
        _Store(_snapshot(grant, member)),
        _Provider([page], context),
        pool,
        _failure_planner,
        actor_id="test",
        poll_interval_sec=0,
        clock=lambda: _NOW,
    )

    outcome = await runner.run(
        grant,
        grant_control.ClaimMode.PRIMARY,
        context,
    )

    assert isinstance(outcome, grant_control.RunCompleted)
    assert len(pool.batches) == 1
    assert [call.audio_url for call in pool.batches[0].calls] == [
        "https://audio/new"
    ]


@pytest.mark.asyncio
async def test_all_null_members_start_at_live_edge_without_routing() -> None:
    grant = _grant()
    first = _member("100", bookmark=None)
    second = _member("200", bookmark=None)
    context = _context()
    page = provider.CallsPageEnvelope(
        {},
        (
            {
                "groupId": "7017-100",
                "url": "https://audio/first-page",
                "ts": _NOW.timestamp(),
            },
        ),
        0,
    )
    store = _Store(_snapshot(grant, first, second))
    calls_provider = _Provider([page], context)
    pool = _Pool()
    runner = sid_runner.BcfyCallsSidRunner(
        store,
        calls_provider,
        pool,
        _failure_planner,
        actor_id="test",
        poll_interval_sec=0,
        clock=lambda: _NOW,
    )

    outcome = await runner.run(
        grant,
        grant_control.ClaimMode.PRIMARY,
        context,
    )

    assert isinstance(outcome, grant_control.RunCompleted)
    assert calls_provider.positions == [None]
    assert pool.batches == []
    assert {
        mutation.member.feed_id for mutation in store.batches[0].mutations
    } == {first.identity.feed_id, second.identity.feed_id}
    assert {
        mutation.cursor
        for mutation in store.batches[0].mutations
        if isinstance(mutation, ingestion_lease_store.SourceObservation)
    } == {_NOW}


@pytest.mark.asyncio
async def test_metadata_fetch_uses_supervisor_stop_event(
    caplog: pytest.LogCaptureFixture,
) -> None:
    grant = _grant()
    member = _member(
        "100",
        bookmark=_NOW - datetime.timedelta(seconds=30),
    )
    context = _context()
    page = provider.CallsPageEnvelope({}, (), _NOW.timestamp())

    class StopAwareProvider:
        def __init__(self) -> None:
            self.shutdown_event: asyncio.Event | None = None

        async def fetch_sid_page(
            self,
            sid: str,
            pos: datetime.datetime | None,
            *,
            shutdown_event: asyncio.Event,
        ) -> provider.CallsPageEnvelope:
            del sid, pos
            self.shutdown_event = shutdown_event
            context.stop_requested.set()
            if shutdown_event.is_set():
                raise provider.TokenLoadStopped
            return page

    calls_provider = StopAwareProvider()
    runner = sid_runner.BcfyCallsSidRunner(
        _Store(_snapshot(grant, member)),
        calls_provider,
        _Pool(),
        _failure_planner,
        actor_id="test",
        poll_interval_sec=0,
        clock=lambda: _NOW,
    )

    with caplog.at_level(logging.INFO, sid_runner.__name__):
        outcome = await runner.run(
            grant,
            grant_control.ClaimMode.PRIMARY,
            context,
        )

    assert isinstance(outcome, grant_control.RunCompleted)
    assert calls_provider.shutdown_event is context.stop_requested
    settled = [
        record
        for record in caplog.records
        if getattr(record, "json_fields", {}).get("event_type")
        == "bcfy_calls_sid_poll_settled"
    ]
    assert len(settled) == 1
    assert getattr(settled[0], "json_fields", {}).get("status") == "stopped"


@pytest.mark.asyncio
async def test_authentication_failure_retries_the_owned_sid() -> None:
    grant = _grant()
    member = _member(
        "100",
        bookmark=_NOW - datetime.timedelta(seconds=30),
    )
    context = _context()
    page = provider.CallsPageEnvelope({}, (), _NOW.timestamp())

    class RefreshingProvider(_Provider):
        def __init__(self) -> None:
            super().__init__([page], context)
            self.failed_once = False

        async def fetch_sid_page(
            self,
            sid: str,
            pos: datetime.datetime | None,
            *,
            shutdown_event: asyncio.Event,
        ) -> provider.CallsPageEnvelope:
            if not self.failed_once:
                self.failed_once = True
                raise models.FeedFailure(
                    feed_store.FeedStatusReason.SYSTEM_AUTHENTICATION_FAILED,
                    "calls_api_http_401",
                )
            return await super().fetch_sid_page(
                sid,
                pos,
                shutdown_event=shutdown_event,
            )

    store = _Store(_snapshot(grant, member))
    calls_provider = RefreshingProvider()
    runner = sid_runner.BcfyCallsSidRunner(
        store,
        calls_provider,
        _Pool(),
        _failure_planner,
        actor_id="test",
        poll_interval_sec=0,
        clock=lambda: _NOW,
    )

    outcome = await runner.run(
        grant,
        grant_control.ClaimMode.PRIMARY,
        context,
    )

    assert isinstance(outcome, grant_control.RunCompleted)
    assert calls_provider.failed_once
    assert calls_provider.positions == [member.last_bookmark_time]
    assert len(store.batches) == 1


@pytest.mark.asyncio
async def test_backoff_poll_wait_stops_without_waiting_full_interval() -> None:
    grant = _grant()
    member = _member(
        "100",
        bookmark=_NOW - datetime.timedelta(minutes=1),
        retry_after=_NOW + datetime.timedelta(minutes=1),
    )
    context = _context()
    runner = sid_runner.BcfyCallsSidRunner(
        _Store(_snapshot(grant, member)),
        mock.MagicMock(),
        _Pool(),
        _failure_planner,
        actor_id="test",
        poll_interval_sec=60,
        clock=lambda: _NOW,
    )
    run = asyncio.create_task(
        runner.run(grant, grant_control.ClaimMode.PRIMARY, context)
    )
    await asyncio.sleep(0)

    context.stop_requested.set()

    outcome = await asyncio.wait_for(run, timeout=1)
    assert isinstance(outcome, grant_control.RunCompleted)


@pytest.mark.asyncio
async def test_invalid_boundary_replays_bookmark_and_deduplicates_url() -> None:
    grant = _grant()
    bookmark = _NOW - datetime.timedelta(seconds=30)
    member = _member("100", bookmark=bookmark)
    context = _context()
    call_time = _NOW - datetime.timedelta(seconds=10)
    call = {
        "groupId": "7017-100",
        "url": "https://audio/duplicate",
        "ts": call_time.timestamp(),
    }
    pages = [
        provider.CallsPageEnvelope({}, (call,), bookmark.timestamp() - 1),
        provider.CallsPageEnvelope({}, (call,), _NOW.timestamp()),
    ]
    store = _Store(_snapshot(grant, member))
    calls_provider = _Provider(pages, context)
    pool = _Pool()
    runner = sid_runner.BcfyCallsSidRunner(
        store,
        calls_provider,
        pool,
        _failure_planner,
        actor_id="test",
        poll_interval_sec=0,
        clock=lambda: _NOW,
    )

    outcome = await runner.run(
        grant,
        grant_control.ClaimMode.RECOVERY,
        context,
    )

    assert isinstance(outcome, grant_control.RunCompleted)
    # Durable membership is deliberately reloaded every poll. This fake store
    # keeps returning the old bookmark, so it remains the safe request floor.
    assert calls_provider.positions == [bookmark, bookmark]
    assert len(pool.batches) == 1
    assert len(store.batches) == 2
    assert store.batches[0].mutations == (
        ingestion_lease_store.SourceObservation(member.identity, None),
    )
    assert len(store.batches[1].mutations) == 1


@pytest.mark.asyncio
async def test_future_page_boundary_does_not_advance_member() -> None:
    grant = _grant()
    member = _member(
        "100",
        bookmark=_NOW - datetime.timedelta(seconds=30),
    )
    context = _context()
    page = provider.CallsPageEnvelope(
        {},
        (),
        (_NOW + datetime.timedelta(seconds=1)).timestamp(),
    )
    store = _Store(_snapshot(grant, member))
    runner = sid_runner.BcfyCallsSidRunner(
        store,
        _Provider([page], context),
        _Pool(),
        _failure_planner,
        actor_id="test",
        poll_interval_sec=0,
        clock=lambda: _NOW,
    )

    outcome = await runner.run(
        grant,
        grant_control.ClaimMode.PRIMARY,
        context,
    )

    assert isinstance(outcome, grant_control.RunCompleted)
    assert len(store.batches) == 1
    assert store.batches[0].mutations == (
        ingestion_lease_store.SourceObservation(member.identity, None),
    )


@pytest.mark.asyncio
async def test_grant_loss_during_fetch_still_settles_admitted_page() -> None:
    grant = _grant()
    member = _member(
        "100",
        bookmark=_NOW - datetime.timedelta(minutes=1),
    )
    context = _context()

    class LosingProvider(_Provider):
        async def fetch_sid_page(
            self,
            sid: str,
            pos: datetime.datetime | None,
            *,
            shutdown_event: asyncio.Event,
        ) -> provider.CallsPageEnvelope:
            page = await super().fetch_sid_page(
                sid,
                pos,
                shutdown_event=shutdown_event,
            )
            context.grant_lost.set()
            return page

    page = provider.CallsPageEnvelope(
        {},
        (
            {
                "groupId": "7017-100",
                "url": "https://audio/admitted",
                "ts": _NOW.timestamp(),
            },
        ),
        _NOW.timestamp(),
    )
    store = _Store(_snapshot(grant, member))
    calls_provider = LosingProvider([page], context)
    pool = _Pool()
    runner = sid_runner.BcfyCallsSidRunner(
        store,
        calls_provider,
        pool,
        _failure_planner,
        actor_id="test",
        poll_interval_sec=0,
        clock=lambda: _NOW,
    )

    outcome = await runner.run(
        grant,
        grant_control.ClaimMode.PRIMARY,
        context,
    )

    assert isinstance(outcome, grant_control.RunLost)
    assert len(pool.batches) == 1
    assert len(store.batches) == 1


@pytest.mark.asyncio
async def test_batch_grant_rejection_stops_sid_without_page_commit() -> None:
    grant = _grant()
    member = _member(
        "100",
        bookmark=_NOW - datetime.timedelta(minutes=1),
    )
    context = _context()
    page = provider.CallsPageEnvelope(
        {},
        (
            {
                "groupId": "7017-100",
                "url": "https://audio/rejected",
                "ts": _NOW.timestamp(),
            },
        ),
        _NOW.timestamp(),
    )
    rejection = ingestion_lease_store.GrantRejected(
        ingestion_lease_store.GrantRejectionReason.FENCE_MISMATCH
    )

    def rejected_result(
        batch: pipeline.FeedBatch,
    ) -> pipeline.FeedBatchResult:
        return _result(batch, terminal=rejection)

    store = _Store(_snapshot(grant, member))
    runner = sid_runner.BcfyCallsSidRunner(
        store,
        _Provider([page], context),
        _Pool(mock.Mock(side_effect=rejected_result)),
        _failure_planner,
        actor_id="test",
        poll_interval_sec=0,
        clock=lambda: _NOW,
    )

    outcome = await runner.run(
        grant,
        grant_control.ClaimMode.PRIMARY,
        context,
    )

    assert isinstance(outcome, grant_control.RunLost)
    assert store.batches == []


@pytest.mark.asyncio
async def test_unanimous_multi_feed_failure_promotes_only_to_sid() -> None:
    grant = _grant()
    first = _member("100", bookmark=_NOW - datetime.timedelta(minutes=1))
    second = _member("200", bookmark=_NOW - datetime.timedelta(minutes=1))
    quiet = _member("300", bookmark=_NOW - datetime.timedelta(minutes=1))
    context = _context()
    page = provider.CallsPageEnvelope(
        {},
        (
            {
                "groupId": "7017-100",
                "url": "https://audio/1",
                "ts": _NOW.timestamp(),
            },
            {
                "groupId": "7017-100",
                "url": "https://audio/1b",
                "ts": _NOW.timestamp(),
            },
            {
                "groupId": "7017-200",
                "url": "https://audio/2",
                "ts": _NOW.timestamp(),
            },
        ),
        _NOW.timestamp(),
    )
    first_failure = failure_classification.ItemFailure(
        feed_store.FeedStatusReason.SOURCE_UNREACHABLE,
        "first",
    )
    second_failure = failure_classification.ItemFailure(
        feed_store.FeedStatusReason.SOURCE_UNREACHABLE,
        "second",
    )
    failures = iter((first_failure, second_failure))

    def failed_result(batch: pipeline.FeedBatch) -> pipeline.FeedBatchResult:
        return _result(
            batch,
            published=0,
            terminal=next(failures),
        )

    store = _Store(_snapshot(grant, first, second, quiet))
    calls_provider = _Provider([page], context)
    pool = _Pool(mock.Mock(side_effect=failed_result))
    runner = sid_runner.BcfyCallsSidRunner(
        store,
        calls_provider,
        pool,
        _failure_planner,
        actor_id="test",
        poll_interval_sec=0,
        clock=lambda: _NOW,
    )

    outcome = await runner.run(
        grant,
        grant_control.ClaimMode.PRIMARY,
        context,
    )

    assert outcome == grant_control.RunFailed(
        feed_store.FeedStatusReason.SOURCE_UNREACHABLE,
        "first",
    )
    assert store.batches[0].mutations == (
        ingestion_lease_store.SourceObservation(
            quiet.identity,
            _NOW,
        ),
    )
    assert isinstance(
        store.batches[0].lease_effect,
        ingestion_lease_store.NoLeaseEffect,
    )


@pytest.mark.asyncio
async def test_mixed_multi_feed_failures_promote_generic_sid_failure() -> None:
    grant = _grant()
    first = _member("100", bookmark=_NOW - datetime.timedelta(minutes=1))
    second = _member("200", bookmark=_NOW - datetime.timedelta(minutes=1))
    context = _context()
    page = provider.CallsPageEnvelope(
        {},
        (
            {
                "groupId": "7017-100",
                "url": "https://audio/1",
                "ts": _NOW.timestamp(),
            },
            {
                "groupId": "7017-200",
                "url": "https://audio/2",
                "ts": _NOW.timestamp(),
            },
        ),
        _NOW.timestamp(),
    )
    failures = iter(
        (
            failure_classification.ItemFailure(
                feed_store.FeedStatusReason.SOURCE_UNREACHABLE,
                "source unavailable",
            ),
            failure_classification.ItemFailure(
                feed_store.FeedStatusReason.SYSTEM_PIPELINE_ERROR,
                "upload failed",
            ),
        )
    )

    def failed_result(batch: pipeline.FeedBatch) -> pipeline.FeedBatchResult:
        return _result(batch, published=0, terminal=next(failures))

    store = _Store(_snapshot(grant, first, second))
    runner = sid_runner.BcfyCallsSidRunner(
        store,
        _Provider([page], context),
        _Pool(mock.Mock(side_effect=failed_result)),
        _failure_planner,
        actor_id="test",
        poll_interval_sec=0,
        clock=lambda: _NOW,
    )

    outcome = await runner.run(
        grant,
        grant_control.ClaimMode.PRIMARY,
        context,
    )

    assert outcome == grant_control.RunFailed(
        feed_store.FeedStatusReason.SYSTEM_COLLECTOR_ERROR,
        "mixed_feed_failures",
    )
    assert store.batches == []


@pytest.mark.asyncio
async def test_promoted_sid_failure_outranks_cancellation_and_logs_gap(
    caplog: pytest.LogCaptureFixture,
) -> None:
    grant = _grant()
    first = _member("100", bookmark=_NOW - datetime.timedelta(minutes=1))
    second = _member("200", bookmark=_NOW - datetime.timedelta(minutes=1))
    quiet = _member("300", bookmark=_NOW - datetime.timedelta(minutes=1))
    context = _context()
    page = provider.CallsPageEnvelope(
        {},
        (
            {
                "groupId": "7017-100",
                "url": "https://audio/1",
                "ts": _NOW.timestamp(),
            },
            {
                "groupId": "7017-200",
                "url": "https://audio/2",
                "ts": _NOW.timestamp(),
            },
        ),
        _NOW.timestamp(),
    )
    publish_gap = failure_classification.ItemFailure(
        feed_store.FeedStatusReason.PIPELINE_PUBLISH_AFTER_BOOKMARK_FAILED,
        "publish failed",
    )
    source_failure = failure_classification.ItemFailure(
        feed_store.FeedStatusReason.SOURCE_UNREACHABLE,
        "source unavailable",
    )
    cancellation = asyncio.CancelledError("shutdown")

    class PromotedPool:
        async def settle_batches(
            self,
            batches: collections.abc.Sequence[pipeline.FeedBatch],
        ) -> work_pool.SettledBatches[
            pipeline.FeedBatch,
            pipeline.FeedBatchResult,
        ]:
            return work_pool.SettledBatches(
                results=(
                    (
                        batches[0],
                        _result(
                            batches[0],
                            published=0,
                            terminal=publish_gap,
                        ),
                    ),
                    (
                        batches[1],
                        _result(
                            batches[1],
                            published=0,
                            terminal=source_failure,
                        ),
                    ),
                ),
                failure=None,
                cancellation=cancellation,
            )

    store = _Store(_snapshot(grant, first, second, quiet))
    runner = sid_runner.BcfyCallsSidRunner(
        store,
        _Provider([page], context),
        PromotedPool(),
        _failure_planner,
        actor_id="test",
        poll_interval_sec=0,
        clock=lambda: _NOW,
    )

    with caplog.at_level(logging.INFO, logger=sid_runner.logger.name):
        outcome = await runner.run(
            grant,
            grant_control.ClaimMode.PRIMARY,
            context,
        )

    assert outcome == grant_control.RunFailed(
        feed_store.FeedStatusReason.SYSTEM_COLLECTOR_ERROR,
        "mixed_feed_failures",
    )
    assert store.batches[0].mutations == (
        ingestion_lease_store.SourceObservation(quiet.identity, _NOW),
    )
    assert isinstance(
        store.batches[0].lease_effect,
        ingestion_lease_store.NoLeaseEffect,
    )
    events = [
        record.__dict__.get("json_fields", {}).get("event_type")
        for record in caplog.records
    ]
    assert events.count("post_bookmark_publish_failure") == 1
    assert "feed_failure_policy_decision" not in events


@pytest.mark.asyncio
async def test_successful_participant_suppresses_sid_promotion() -> None:
    grant = _grant()
    failed = _member("100", bookmark=_NOW - datetime.timedelta(minutes=1))
    succeeded = _member("200", bookmark=_NOW - datetime.timedelta(minutes=1))
    context = _context()
    page = provider.CallsPageEnvelope(
        {},
        (
            {
                "groupId": "7017-100",
                "url": "https://audio/1",
                "ts": _NOW.timestamp(),
            },
            {
                "groupId": "7017-200",
                "url": "https://audio/2",
                "ts": _NOW.timestamp(),
            },
        ),
        _NOW.timestamp(),
    )
    failure = failure_classification.ItemFailure(
        feed_store.FeedStatusReason.SOURCE_UNREACHABLE,
        "source unavailable",
    )

    def mixed_result(batch: pipeline.FeedBatch) -> pipeline.FeedBatchResult:
        if batch.member.identity.feed_id == failed.identity.feed_id:
            return _result(batch, published=0, terminal=failure)
        return _result(batch)

    store = _Store(_snapshot(grant, failed, succeeded))
    runner = sid_runner.BcfyCallsSidRunner(
        store,
        _Provider([page], context),
        _Pool(mock.Mock(side_effect=mixed_result)),
        _failure_planner,
        actor_id="test",
        poll_interval_sec=0,
        clock=lambda: _NOW,
    )

    outcome = await runner.run(
        grant,
        grant_control.ClaimMode.PRIMARY,
        context,
    )

    assert isinstance(outcome, grant_control.RunCompleted)
    assert isinstance(
        store.batches[0].mutations[0],
        ingestion_lease_store.FeedFailureTransition,
    )
    assert store.batches[0].mutations[1] == (
        ingestion_lease_store.SourceObservation(succeeded.identity, _NOW)
    )
    assert isinstance(
        store.batches[0].lease_effect,
        ingestion_lease_store.FinalizeLeaseRecovery,
    )


@pytest.mark.asyncio
async def test_single_feed_failure_stays_child_local() -> None:
    grant = _grant()
    member = _member("100", bookmark=_NOW - datetime.timedelta(minutes=1))
    context = _context()
    page = provider.CallsPageEnvelope(
        {},
        (
            {
                "groupId": "7017-100",
                "url": "https://audio/1",
                "ts": _NOW.timestamp(),
            },
            {
                "groupId": "7017-100",
                "url": "https://audio/2",
                "ts": _NOW.timestamp(),
            },
        ),
        _NOW.timestamp(),
    )
    item_failure = failure_classification.ItemFailure(
        feed_store.FeedStatusReason.SOURCE_UNREACHABLE,
        "audio unavailable",
    )

    def failed_result(batch: pipeline.FeedBatch) -> pipeline.FeedBatchResult:
        return _result(batch, published=0, terminal=item_failure)

    store = _Store(_snapshot(grant, member))
    calls_provider = _Provider([page], context)
    runner = sid_runner.BcfyCallsSidRunner(
        store,
        calls_provider,
        _Pool(mock.Mock(side_effect=failed_result)),
        _failure_planner,
        actor_id="test",
        poll_interval_sec=0,
        clock=lambda: _NOW,
    )

    outcome = await runner.run(
        grant,
        grant_control.ClaimMode.PRIMARY,
        context,
    )

    assert isinstance(outcome, grant_control.RunCompleted)
    mutation = store.batches[0].mutations[0]
    assert isinstance(
        mutation,
        ingestion_lease_store.FeedFailureTransition,
    )
    assert mutation.completion_cursor is None
    assert isinstance(
        mutation.action,
        ingestion_lease_store.NonBudgetedFailure,
    )


@pytest.mark.asyncio
async def test_committed_publish_gap_emits_runtime_telemetry(
    caplog: pytest.LogCaptureFixture,
) -> None:
    grant = _grant()
    member = _member("100", bookmark=_NOW - datetime.timedelta(minutes=1))
    store = _Store(_snapshot(grant, member))
    runner = sid_runner.BcfyCallsSidRunner(
        store,
        mock.MagicMock(),
        mock.MagicMock(),
        _failure_planner,
        actor_id="test",
        poll_interval_sec=0,
        clock=lambda: _NOW,
    )
    failure = failure_classification.ItemFailure(
        feed_store.FeedStatusReason.PIPELINE_PUBLISH_AFTER_BOOKMARK_FAILED,
        "publish failed",
    )
    result = pipeline.FeedBatchResult(
        attempted_count=1,
        published_count=0,
        next_sequence=1,
        committed_urls=(),
        terminal=failure,
    )

    with caplog.at_level(logging.INFO, logger=sid_runner.logger.name):
        committed, cancellation = await runner._commit_page(
            grant,
            (member,),
            frozenset((member.identity.feed_id,)),
            {member.identity.feed_id: result},
            _NOW,
            _NOW,
            complete=True,
            promoted=None,
        )

    assert isinstance(committed, ingestion_lease_store.BatchCommitted)
    assert cancellation is None
    records = [
        record.__dict__["json_fields"]
        for record in caplog.records
        if record.__dict__.get("json_fields", {}).get("event_type")
        in {
            "feed_failure_policy_decision",
            "post_bookmark_publish_failure",
        }
    ]
    assert [record["event_type"] for record in records] == [
        "feed_failure_policy_decision",
        "post_bookmark_publish_failure",
    ]
    for record in records:
        assert record["feed_id"] == str(member.identity.feed_id)
        assert record["source_type"] == feed_store.SourceType.BCFY_CALLS.value
        assert record["replay_missing"] is True
        assert record["data_gap_known"] is True


@pytest.mark.asyncio
async def test_page_commit_settles_before_cancellation_propagates() -> None:
    grant = _grant()
    member = _member("100", bookmark=_NOW - datetime.timedelta(minutes=1))
    context = _context()
    page = provider.CallsPageEnvelope({}, (), _NOW.timestamp())

    class DelayedStore(_Store):
        def __init__(self) -> None:
            super().__init__(_snapshot(grant, member))
            self.commit_started = asyncio.Event()
            self.release_commit = asyncio.Event()

        async def commit_child_mutations(
            self,
            grant: ingestion_lease_store.LeaseGrant,
            batch: ingestion_lease_store.ChildMutationBatch,
            *,
            actor_id: str,
        ) -> (
            ingestion_lease_store.BatchCommitted
            | ingestion_lease_store.GrantRejected
        ):
            self.commit_started.set()
            await self.release_commit.wait()
            return await super().commit_child_mutations(
                grant,
                batch,
                actor_id=actor_id,
            )

    store = DelayedStore()
    runner = sid_runner.BcfyCallsSidRunner(
        store,
        _Provider([page], context),
        _Pool(),
        _failure_planner,
        actor_id="test",
        poll_interval_sec=0,
        clock=lambda: _NOW,
    )
    run = asyncio.create_task(
        runner.run(grant, grant_control.ClaimMode.PRIMARY, context)
    )
    await store.commit_started.wait()

    run.cancel("cancel during issued commit")
    await asyncio.sleep(0)
    assert not run.done()
    store.release_commit.set()

    with pytest.raises(asyncio.CancelledError) as raised:
        await run
    assert raised.value.args == ("cancel during issued commit",)
    assert len(store.batches) == 1


@pytest.mark.asyncio
async def test_inner_page_commit_cancellation_is_outcome_unknown() -> None:
    grant = _grant()
    member = _member("100", bookmark=_NOW - datetime.timedelta(minutes=1))
    context = _context()
    page = provider.CallsPageEnvelope({}, (), _NOW.timestamp())

    class InternallyCancelledStore(_Store):
        async def commit_child_mutations(
            self,
            grant: ingestion_lease_store.LeaseGrant,
            batch: ingestion_lease_store.ChildMutationBatch,
            *,
            actor_id: str,
        ) -> ingestion_lease_store.BatchCommitted:
            del grant, batch, actor_id
            message = "database operation cancelled"
            raise asyncio.CancelledError(message)

    store = InternallyCancelledStore(_snapshot(grant, member))
    runner = sid_runner.BcfyCallsSidRunner(
        store,
        _Provider([page], context),
        _Pool(),
        _failure_planner,
        actor_id="test",
        poll_interval_sec=0,
        clock=lambda: _NOW,
    )

    with pytest.raises(
        grant_control.GrantControlIntegrityError,
        match="outcome unknown",
    ):
        await runner.run(
            grant,
            grant_control.ClaimMode.PRIMARY,
            context,
        )


@pytest.mark.asyncio
async def test_page_commit_failure_retains_concurrent_cancellation() -> None:
    grant = _grant()
    member = _member("100", bookmark=_NOW - datetime.timedelta(minutes=1))
    context = _context()
    page = provider.CallsPageEnvelope({}, (), _NOW.timestamp())
    commit_started = asyncio.Event()
    release_commit = asyncio.Event()
    child_failure = asyncio.CancelledError("inner commit cancellation")

    class FailingStore(_Store):
        async def commit_child_mutations(
            self,
            grant: ingestion_lease_store.LeaseGrant,
            batch: ingestion_lease_store.ChildMutationBatch,
            *,
            actor_id: str,
        ) -> ingestion_lease_store.BatchCommitted:
            del grant, batch, actor_id
            commit_started.set()
            await release_commit.wait()
            raise child_failure

    runner = sid_runner.BcfyCallsSidRunner(
        FailingStore(_snapshot(grant, member)),
        _Provider([page], context),
        _Pool(),
        _failure_planner,
        actor_id="test",
        poll_interval_sec=0,
        clock=lambda: _NOW,
    )
    run = asyncio.create_task(
        runner.run(grant, grant_control.ClaimMode.PRIMARY, context)
    )
    await commit_started.wait()
    outer_marker = object()
    run.cancel(outer_marker)
    await asyncio.sleep(0)
    release_commit.set()

    with pytest.raises(grant_control.GrantControlIntegrityError) as raised:
        await run

    assert raised.value.__cause__ is child_failure
    outer_cancellation = typing.cast(
        "asyncio.CancelledError",
        child_failure.__cause__,
    )
    assert outer_cancellation.args[0] is outer_marker
