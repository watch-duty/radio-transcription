"""Exact-grant Event coordinator for counted trailing boundaries."""

# Private sibling modules deliberately share closed scheduler internals.
# ruff: noqa: SLF001

from __future__ import annotations

import asyncio
import collections
import dataclasses
import math
import typing

from backend.pipeline.ingestion.feed_work_scheduler import _shard, _types
from backend.pipeline.storage import ingestion_lease_store

_BOUNDARY_BATCH_SIZE = 100
_MAX_FINITE_SECONDS = float.fromhex("0x1.fffffffffffffp+1023")


class _BoundaryCoordinatorError(RuntimeError):
    """The exact boundary coordinator can no longer settle requests."""


class _BoundaryAuthorityLostError(_BoundaryCoordinatorError):
    """The exact boundary committer rejected this grant generation."""


type _SelectedBoundaryBatch = tuple[
    tuple[_shard._Shard, _types._BoundaryRecord],
    ...,
]


class _BoundaryEvidenceSink(typing.Protocol):
    """One captured page-local destination for a started flush attempt."""

    @property
    def boundary_capture_open(self) -> bool: ...

    @property
    def boundary_capture_complete(self) -> bool: ...

    def close_boundary_capture(self, *, complete: bool) -> None: ...

    def tracks_boundary(self, record: _types._BoundaryRecord) -> bool: ...

    def observe_flush_attempt(
        self,
        *,
        final_logical: bool,
        latency_seconds: float,
    ) -> None: ...

    def observe_fence_rejection(self) -> None: ...

    def observe_member_rejections(
        self,
        selected: _SelectedBoundaryBatch,
        dispositions: tuple[_types.BoundaryDisposition, ...],
    ) -> None: ...


@dataclasses.dataclass(slots=True)
class _DeferredBoundaryEvidence:
    """One unowned automatic attempt awaiting generation serialization."""

    final_logical: bool
    latency_seconds: float
    fence_rejected: bool = False
    selected: _SelectedBoundaryBatch = ()
    dispositions: tuple[_types.BoundaryDisposition, ...] = ()


class _DefaultBoundaryCommitter:
    """Deterministic Phase 4 adapter used until Phase 5 wires storage."""

    async def commit(
        self,
        grant: ingestion_lease_store.LeaseGrant,
        boundaries: tuple[_types.BoundaryWork, ...],
        *,
        final_logical: bool,
    ) -> _types.BoundaryCommitResult:
        del grant, final_logical
        return _types.BoundaryBatchCommitted(
            tuple(
                _types.BoundaryResult(
                    boundary,
                    _types.BoundaryDisposition.COMMITTED,
                )
                for boundary in boundaries
            )
        )


async def _settle_final_page(
    awaitable: typing.Awaitable[_types.FinalPageResult],
) -> _types.FinalPageResult:
    """Settle one started FinalPage call despite caller cancellation."""
    task = asyncio.ensure_future(awaitable)
    while True:
        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError:
            if not task.done():
                continue
            if task.cancelled():
                message = "started FinalPage call lost its typed outcome"
                raise _BoundaryCoordinatorError(message) from None
            return task.result()


class _BoundaryCoordinator:
    """One bounded Event/generation flusher for one complete grant."""

    def __init__(
        self,
        grant: ingestion_lease_store.LeaseGrant,
        shards: tuple[_shard._Shard, ...],
        committer: _types.BoundaryCommitter,
        *,
        authority_lost: typing.Callable[[], object],
        fatal_observer: typing.Callable[[BaseException], None],
        monotonic: typing.Callable[[], float],
        evidence_sink_for: typing.Callable[
            [_SelectedBoundaryBatch, bool],
            _BoundaryEvidenceSink | None,
        ],
        batch_size: int = _BOUNDARY_BATCH_SIZE,
    ) -> None:
        if not isinstance(grant, ingestion_lease_store.LeaseGrant):
            message = "grant must be a LeaseGrant"
            raise TypeError(message)
        if not shards:
            message = "boundary coordinator requires at least one shard"
            raise ValueError(message)
        if not callable(getattr(committer, "commit", None)):
            message = "committer must provide async commit"
            raise TypeError(message)
        for name, callback in (
            ("authority_lost", authority_lost),
            ("fatal_observer", fatal_observer),
            ("monotonic", monotonic),
            ("evidence_sink_for", evidence_sink_for),
        ):
            if not callable(callback):
                message = f"{name} must be callable"
                raise TypeError(message)
        _types._require_positive_integer(batch_size, "batch_size")
        self._grant = grant
        self._shards = shards
        self._committer = committer
        self._authority_lost = authority_lost
        self._fatal_observer = fatal_observer
        self._monotonic = monotonic
        self._evidence_sink_for = evidence_sink_for
        self._last_now: float | None = None
        self._batch_size = batch_size
        self._signal = asyncio.Event()
        self._generation_changed = asyncio.Condition()
        self._requested_generation = 0
        self._completed_generation = 0
        self._requested_final = False
        self._retryable_generation: int | None = None
        self._attempt_active = False
        self._active_evidence_sink: _BoundaryEvidenceSink | None = None
        self._requested_evidence_sink: _BoundaryEvidenceSink | None = None
        self._evidence_cutoff = False
        self._closing = False
        self._abandoned = False
        self._authority_rejected = False
        self._fatal: BaseException | None = None
        self._close_changed = asyncio.Event()
        self._task = asyncio.create_task(
            self._run(),
            name=(
                f"feed-boundary-flusher-{grant.lease_key}-{grant.fencing_token}"
            ),
        )
        self._task.add_done_callback(self._task_settled)

    @property
    def task(self) -> asyncio.Task[None]:
        """Return the one registered flusher task for bounded inspection."""
        return self._task

    @property
    def signal(self) -> asyncio.Event:
        """Return the one coalescing flusher signal for inspection."""
        return self._signal

    @property
    def requested_generation(self) -> int:
        return self._requested_generation

    @property
    def completed_generation(self) -> int:
        return self._completed_generation

    def notify_ready(self) -> None:
        """Coalesce a newly ready physical-boundary wake."""
        if not self._closing and self._fatal is None:
            self._signal.set()

    async def request_relief(self) -> _types._BoundaryPressureResult:
        """Request one bounded pressure-relief generation."""
        return await self._request_generation(final_logical=False)

    async def request_final(self) -> _types._BoundaryPressureResult:
        """Request the page's one required final logical attempt."""
        return await self._request_generation(final_logical=True)

    async def settle_page_evidence(
        self,
        evidence_sink: _BoundaryEvidenceSink,
    ) -> bool:
        """Wait only started attempts, then close page evidence capture.

        Returns:
            Whether every attempt captured before the cutoff settled. An
            abandoned active mutation returns false without blocking forever.
        """
        if not evidence_sink.boundary_capture_open:
            return evidence_sink.boundary_capture_complete
        try:
            async with self._generation_changed:
                self._evidence_cutoff = True
                self._generation_changed.notify_all()
                await self._generation_changed.wait_for(
                    lambda: (
                        self._abandoned
                        or (
                            not self._attempt_active
                            and (
                                self._requested_generation
                                == self._completed_generation
                                or self._terminally_unavailable()
                            )
                        )
                    )
                )
                incomplete_capture = (
                    self._abandoned
                    and self._attempt_active
                    and self._active_evidence_sink is evidence_sink
                )
                complete = not incomplete_capture
                evidence_sink.close_boundary_capture(complete=complete)
                return complete
        finally:
            async with self._generation_changed:
                self._evidence_cutoff = False
                self._generation_changed.notify_all()

    async def close(self) -> None:
        """Stop selection and await any already-committed batch settlement."""
        self._closing = True
        self._signal.set()
        await self._notify_generation_waiters()
        if not self._task.done() and not self._abandoned:
            await self._close_changed.wait()
        if self._task.done():
            await self._task

    async def abandon(self, failure: BaseException) -> None:
        """Fail closed without waiting forever on an unsettled mutation."""
        if not isinstance(failure, BaseException):
            message = "failure must be a BaseException"
            raise TypeError(message)
        if self._task.done():
            return
        self._abandoned = True
        self._closing = True
        self._signal.set()
        await self._fail(failure)
        self._close_changed.set()

    async def _request_generation(
        self,
        *,
        final_logical: bool,
    ) -> _types._BoundaryPressureResult:
        """Own and await the coordinator's sole outstanding generation.

        Args:
            final_logical: Whether the generation is a page's required final
                attempt, including an empty physical batch.

        Returns:
            The closed pressure result correlated to this generation.

        Raises:
            _BoundaryCoordinatorError: The coordinator closes or fails before
                the generation settles.
            _BoundaryAuthorityLostError: The exact grant was rejected.

        The condition admits only one requester beyond the completed counter,
        so the boolean final marker always belongs to that exact generation.
        """
        async with self._generation_changed:
            self._raise_unavailable()
            await self._generation_changed.wait_for(
                lambda: (
                    self._requested_generation == self._completed_generation
                    or self._fatal is not None
                    or self._closing
                )
            )
            self._raise_unavailable()
            self._requested_generation += 1
            generation = self._requested_generation
            self._requested_final = final_logical
            self._requested_evidence_sink = self._capture_evidence_sink(
                (),
                logical=True,
            )
            if (
                not final_logical
                and self._attempt_active
                and self._active_evidence_sink is None
            ):
                self._active_evidence_sink = self._requested_evidence_sink
            self._signal.set()
            await self._generation_changed.wait_for(
                lambda: (
                    self._completed_generation >= generation
                    or self._fatal is not None
                    or self._closing
                )
            )
            self._raise_unavailable()
            if self._retryable_generation == generation:
                return _types._BoundaryPressureResult.RETRYABLE
            return _types._BoundaryPressureResult.COMPLETED

    async def _run(self) -> None:  # noqa: PLR0915
        """Flush signaled batches and complete each bounded generation.

        Returns:
            None after close stops selection and wakes generation waiters.

        Cancellation is converted to persistent coordinator failure so a
        committed mutation cannot disappear with an unobserved task exit.
        """
        try:
            while True:
                await self._signal.wait()
                self._signal.clear()
                async with self._generation_changed:
                    await self._generation_changed.wait_for(
                        lambda: (
                            not self._evidence_cutoff
                            or self._closing
                            or self._requested_generation
                            > self._completed_generation
                        )
                    )
                    if self._closing:
                        self._generation_changed.notify_all()
                        return
                    generation = self._requested_generation
                    logical = generation > self._completed_generation
                    final_logical = logical and self._requested_final
                    self._attempt_active = True
                    self._generation_changed.notify_all()
                try:
                    selected = await self._select_batch(
                        include_suspended=logical,
                    )
                    if not selected and not logical:
                        async with self._generation_changed:
                            self._attempt_active = False
                            self._active_evidence_sink = None
                            self._generation_changed.notify_all()
                        continue
                    retryable, deferred_evidence = await self._commit_selected(
                        selected,
                        logical=logical,
                        final_logical=final_logical,
                    )
                except BaseException:
                    async with self._generation_changed:
                        self._attempt_active = False
                        self._active_evidence_sink = None
                        self._generation_changed.notify_all()
                    raise
                async with self._generation_changed:
                    adopted_relief = False
                    if (
                        not logical
                        and self._requested_generation
                        > self._completed_generation
                        and not self._requested_final
                    ):
                        generation = self._requested_generation
                        logical = True
                        adopted_relief = True
                    if adopted_relief and deferred_evidence is not None:
                        evidence_sink = (
                            self._requested_evidence_sink
                            or self._active_evidence_sink
                            or self._capture_evidence_sink((), logical=True)
                        )
                        self._publish_deferred_evidence(
                            evidence_sink,
                            deferred_evidence,
                        )
                    self._attempt_active = False
                    self._active_evidence_sink = None
                    self._generation_changed.notify_all()
                    if logical:
                        self._completed_generation = generation
                        self._retryable_generation = (
                            generation if retryable else None
                        )
                        self._requested_evidence_sink = None
                        self._generation_changed.notify_all()
                    pending_final = (
                        self._requested_generation > self._completed_generation
                        and self._requested_final
                    )
                    if retryable and not pending_final:
                        self._signal.clear()
                if not retryable and await self._has_ready_boundary():
                    self._signal.set()
        except asyncio.CancelledError:
            failure = _BoundaryCoordinatorError(
                "boundary flusher was cancelled before settlement"
            )
            await self._fail(failure)
        except BaseException as exc:
            await self._fail(exc)

    async def _select_batch(
        self,
        *,
        include_suspended: bool,
    ) -> tuple[tuple[_shard._Shard, _types._BoundaryRecord], ...]:
        selected = []
        remaining = self._batch_size
        for shard in self._shards:
            if remaining <= 0:
                break
            records = await shard.select_boundary_batch(
                self._grant,
                remaining,
                include_suspended=include_suspended,
            )
            selected.extend((shard, record) for record in records)
            remaining -= len(records)
        return tuple(selected)

    async def _commit_selected(  # noqa: PLR0912, PLR0915
        self,
        selected: _SelectedBoundaryBatch,
        *,
        logical: bool,
        final_logical: bool,
    ) -> tuple[bool, _DeferredBoundaryEvidence | None]:
        """Commit and correlate one immutable batch outside shard locks.

        Args:
            selected: Detached records paired with their owning shards.
            logical: Whether a caller owns this attempt, including when its
                physical batch is empty.
            final_logical: Whether this is the page's required final attempt.

        Returns:
            Whether a target must be retried and any still-unowned automatic
            attempt evidence for generation serialization.

        Raises:
            _BoundaryCoordinatorError: The committed outcome is lost,
                malformed, or outside the closed result vocabulary.
        """
        targets = tuple(
            record.detached_work() for _shard_value, record in selected
        )
        evidence_sink = self._capture_evidence_sink(
            selected,
            logical=logical,
        )
        if evidence_sink is None and logical:
            evidence_sink = self._requested_evidence_sink
        self._active_evidence_sink = evidence_sink
        started = self._clock_sample()
        try:
            result, cancelled = await self._settle_committed(
                self._committer.commit(
                    self._grant,
                    targets,
                    final_logical=final_logical,
                )
            )
        except BaseException:
            flush_latency_seconds = self._elapsed_since(started)
            failure_sink = evidence_sink
            if failure_sink is None and self._pending_relief_request():
                failure_sink = (
                    self._requested_evidence_sink
                    or self._capture_evidence_sink((), logical=True)
                )
            if failure_sink is not None and self._evidence_sink_is_open(
                failure_sink
            ):
                self._notify_observer(
                    failure_sink.observe_flush_attempt,
                    final_logical=final_logical,
                    latency_seconds=flush_latency_seconds,
                )
            raise
        flush_latency_seconds = self._elapsed_since(started)
        deferred_evidence = None
        if evidence_sink is not None and self._evidence_sink_is_open(
            evidence_sink
        ):
            self._notify_observer(
                evidence_sink.observe_flush_attempt,
                final_logical=final_logical,
                latency_seconds=flush_latency_seconds,
            )
        elif evidence_sink is None:
            deferred_evidence = _DeferredBoundaryEvidence(
                final_logical=final_logical,
                latency_seconds=flush_latency_seconds,
            )
        try:
            if type(result) is _types.BoundaryGrantRejected:
                if evidence_sink is not None and self._evidence_sink_is_open(
                    evidence_sink
                ):
                    self._notify_observer(evidence_sink.observe_fence_rejection)
                elif deferred_evidence is not None:
                    deferred_evidence.fence_rejected = True
                await self._discard_selected(selected)
                self._authority_rejected = True
                self._authority_lost()
                self._raise_if_cancelled(cancelled=cancelled)
                return False, deferred_evidence
            if type(result) is _types.BoundaryBatchRetryable:
                await self._restore_selected(selected)
                self._raise_if_cancelled(cancelled=cancelled)
                return True, deferred_evidence
            dispositions = self._correlate_dispositions(result, targets)

            member_rejections = dispositions.count(
                _types.BoundaryDisposition.MEMBER_REJECTED
            )
            if member_rejections:
                if evidence_sink is not None and self._evidence_sink_is_open(
                    evidence_sink
                ):
                    self._notify_observer(
                        evidence_sink.observe_member_rejections,
                        selected,
                        dispositions,
                    )
                elif deferred_evidence is not None:
                    deferred_evidence.selected = selected
                    deferred_evidence.dispositions = dispositions

            by_shard: dict[
                _shard._Shard,
                list[
                    tuple[
                        _types._BoundaryRecord,
                        _types.BoundaryDisposition,
                    ]
                ],
            ] = collections.defaultdict(list)
            for (shard, record), disposition in zip(
                selected,
                dispositions,
                strict=True,
            ):
                by_shard[shard].append((record, disposition))
            retryable = False
            for shard, shard_results in by_shard.items():
                retryable = (
                    await shard.apply_boundary_results(tuple(shard_results))
                    or retryable
                )
            self._raise_if_cancelled(cancelled=cancelled)
        except BaseException:
            if deferred_evidence is not None and (
                self._pending_relief_request()
            ):
                late_sink = (
                    self._requested_evidence_sink
                    or self._capture_evidence_sink((), logical=True)
                )
                self._publish_deferred_evidence(
                    late_sink,
                    deferred_evidence,
                )
            raise
        return retryable, deferred_evidence

    @staticmethod
    def _correlate_dispositions(
        result: object,
        targets: tuple[_types.BoundaryWork, ...],
    ) -> tuple[_types.BoundaryDisposition, ...]:
        """Validate one closed batch result and preserve target order."""
        if type(result) is not _types.BoundaryBatchCommitted:
            message = "committer returned outside the closed vocabulary"
            raise _BoundaryCoordinatorError(message)
        if len(result.results) != len(targets):
            message = "committer result cardinality does not match targets"
            raise _BoundaryCoordinatorError(message)
        dispositions = []
        for target, correlated in zip(targets, result.results, strict=True):
            if type(correlated) is not _types.BoundaryResult:
                message = "committer returned an unknown per-target result"
                raise _BoundaryCoordinatorError(message)
            if correlated.boundary is not target:
                message = "committer result identity/order is not correlated"
                raise _BoundaryCoordinatorError(message)
            if not isinstance(
                correlated.disposition,
                _types.BoundaryDisposition,
            ):
                message = "committer returned an unknown disposition"
                raise _BoundaryCoordinatorError(message)
            dispositions.append(correlated.disposition)
        return tuple(dispositions)

    def _clock_sample(self) -> float:
        try:
            raw = self._monotonic()
        except BaseException:
            if self._last_now is None:
                self._last_now = 0.0
            return self._last_now
        if isinstance(raw, bool) or not isinstance(raw, (int, float)):
            if self._last_now is None:
                self._last_now = 0.0
            return self._last_now
        try:
            sampled = float(raw)
        except (OverflowError, ValueError):
            sampled = math.nan
        if not math.isfinite(sampled):
            if self._last_now is None:
                self._last_now = 0.0
            return self._last_now
        if self._last_now is None or sampled > self._last_now:
            self._last_now = sampled
        return self._last_now

    def _elapsed_since(self, started: float) -> float:
        elapsed = self._clock_sample() - started
        if elapsed <= 0.0:
            return 0.0
        if not math.isfinite(elapsed):
            return _MAX_FINITE_SECONDS
        return elapsed

    def _capture_evidence_sink(
        self,
        selected: _SelectedBoundaryBatch,
        *,
        logical: bool,
    ) -> _BoundaryEvidenceSink | None:
        try:
            return self._evidence_sink_for(selected, logical)
        except BaseException:
            return None

    def _pending_relief_request(self) -> bool:
        """Return whether this automatic attempt will satisfy page relief."""
        return (
            self._requested_generation > self._completed_generation
            and not self._requested_final
        )

    def _publish_deferred_evidence(
        self,
        evidence_sink: _BoundaryEvidenceSink | None,
        evidence: _DeferredBoundaryEvidence,
    ) -> None:
        """Publish one settled automatic attempt after relief adopts it."""
        if evidence_sink is None or not self._evidence_sink_is_open(
            evidence_sink
        ):
            return
        self._notify_observer(
            evidence_sink.observe_flush_attempt,
            final_logical=evidence.final_logical,
            latency_seconds=evidence.latency_seconds,
        )
        if evidence.fence_rejected:
            self._notify_observer(evidence_sink.observe_fence_rejection)
        if evidence.selected:
            self._notify_observer(
                evidence_sink.observe_member_rejections,
                evidence.selected,
                evidence.dispositions,
            )

    @staticmethod
    def _evidence_sink_is_open(
        evidence_sink: _BoundaryEvidenceSink | None,
    ) -> bool:
        if evidence_sink is None:
            return False
        try:
            return evidence_sink.boundary_capture_open
        except BaseException:
            return False

    @staticmethod
    def _notify_observer(
        observer: typing.Callable[..., None],
        *args: object,
        **kwargs: object,
    ) -> None:
        try:
            observer(*args, **kwargs)
        except BaseException:  # noqa: S110 - evidence is observational
            pass

    async def _discard_selected(
        self,
        selected: tuple[
            tuple[_shard._Shard, _types._BoundaryRecord],
            ...,
        ],
    ) -> None:
        by_shard: dict[_shard._Shard, list[_types._BoundaryRecord]] = (
            collections.defaultdict(list)
        )
        for shard, record in selected:
            by_shard[shard].append(record)
        for shard, records in by_shard.items():
            await shard.discard_boundary_batch(tuple(records))

    async def _restore_selected(
        self,
        selected: tuple[
            tuple[_shard._Shard, _types._BoundaryRecord],
            ...,
        ],
    ) -> None:
        by_shard: dict[_shard._Shard, list[_types._BoundaryRecord]] = (
            collections.defaultdict(list)
        )
        for shard, record in selected:
            by_shard[shard].append(record)
        for shard, records in by_shard.items():
            await shard.restore_boundary_batch(tuple(records))

    async def _has_ready_boundary(self) -> bool:
        for shard in self._shards:
            if await shard.has_ready_boundary(self._grant):
                return True
        return False

    def _terminally_unavailable(self) -> bool:
        return (
            self._fatal is not None or self._closing or self._authority_rejected
        )

    async def _settle_committed[ResultT](
        self, awaitable: typing.Awaitable[ResultT]
    ) -> tuple[ResultT, bool]:
        """Settle a started commit despite coordinator-task cancellation.

        Args:
            awaitable: External mutation whose closed outcome is now required.

        Returns:
            The settled result and whether cancellation arrived while waiting.

        Raises:
            _BoundaryCoordinatorError: Cancellation erased the committed
                mutation's closed outcome.

        Shielding is bounded to this one commit task; repeated cancellation
        cannot detach mutation-capable work from its counted boundary records.
        """
        task = asyncio.ensure_future(awaitable)
        cancelled = False
        while True:
            try:
                return await asyncio.shield(task), cancelled
            except asyncio.CancelledError:
                cancelled = True
                if not task.done():
                    continue
                if task.cancelled():
                    message = "committed boundary mutation lost its outcome"
                    raise _BoundaryCoordinatorError(message) from None
                return task.result(), cancelled

    @staticmethod
    def _raise_if_cancelled(*, cancelled: bool) -> None:
        if cancelled:
            message = "boundary flusher was cancelled during settlement"
            raise _BoundaryCoordinatorError(message)

    async def _fail(self, failure: BaseException) -> None:
        if self._fatal is None:
            self._fatal = failure
            self._fatal_observer(failure)
        await self._notify_generation_waiters()

    async def _notify_generation_waiters(self) -> None:
        async with self._generation_changed:
            self._generation_changed.notify_all()

    def _raise_unavailable(self) -> None:
        if self._fatal is not None:
            message = "boundary coordinator integrity failed"
            raise _BoundaryCoordinatorError(message) from self._fatal
        if self._authority_rejected:
            message = "boundary committer rejected the exact grant"
            raise _BoundaryAuthorityLostError(message)
        if self._closing:
            message = "boundary coordinator is closing"
            raise _BoundaryCoordinatorError(message)

    def _task_settled(self, task: asyncio.Task[None]) -> None:
        del task
        self._close_changed.set()
