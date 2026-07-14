"""Controlled exact-lane lifecycle for one Broadcastify Calls SID grant."""

from __future__ import annotations

import asyncio
import dataclasses
import enum
import typing

from backend.pipeline.ingestion import (
    feed_work_scheduler,
    grant_control,
    sid_grant_control,
)
from backend.pipeline.ingestion.collectors.bcfy_calls import sid_processor

if typing.TYPE_CHECKING:
    from backend.pipeline.storage import ingestion_lease_store


__all__ = [
    "BcfyCallsSidRunner",
    "ProcessorCancellationHandoff",
    "RunnerCancellationCause",
    "SidProcessorFactory",
    "SidRunnerIntegrityError",
]

ProcessorCancellationHandoff = sid_processor.ProcessorCancellationHandoff
RunnerCancellationCause = sid_processor.RunnerCancellationCause


class SidRunnerIntegrityError(grant_control.GrantControlIntegrityError):
    """The SID runner cannot prove mutation closure for its exact lane."""


class _TerminalSignal(enum.IntEnum):
    """Deterministic precedence for monotonic runner signals."""

    PROCESSOR_FAILURE = 0
    STOP = 1
    LOSS = 2
    INTEGRITY = 3


class _SidProcessor(typing.Protocol):
    """Invocation-local processor task owned by the shallow runner."""

    async def run(
        self,
        stop_requested: asyncio.Event,
        *,
        signals: feed_work_scheduler.LaneSignalView,
        cancellation_handoff: ProcessorCancellationHandoff,
    ) -> None:
        """Run source work until stopped or a typed outcome is raised."""
        ...


class SidProcessorFactory(typing.Protocol):
    """Construct one processor after its exact scheduler lane opens."""

    def __call__(
        self,
        grant: ingestion_lease_store.LeaseGrant,
        payload: sid_grant_control.SidClaimPayload,
        lane: feed_work_scheduler.GrantLane,
    ) -> _SidProcessor:
        """Return one invocation-local processor for the exact grant."""
        ...


@dataclasses.dataclass(frozen=True, slots=True)
class _TerminalSelection:
    """One classified wake result below monotonic lifecycle signals."""

    signal: _TerminalSignal
    failure: sid_processor.SidProcessorFailure | None = None
    integrity: SidRunnerIntegrityError | None = None


class BcfyCallsSidRunner:
    """Open and settle one invocation-local scheduler lane per exact grant."""

    __slots__ = ("_processor_factory", "_scheduler")

    def __init__(
        self,
        scheduler: feed_work_scheduler.FeedWorkScheduler,
        processor_factory: SidProcessorFactory,
    ) -> None:
        """Bind the shared scheduler and invocation processor factory."""
        required = (
            "open_lane",
            "integrity_failure_event",
            "raise_if_failed",
        )
        if any(not hasattr(scheduler, name) for name in required):
            message = "scheduler does not provide the SID runner contract"
            raise TypeError(message)
        if not callable(processor_factory):
            message = "processor_factory must be callable"
            raise TypeError(message)
        self._scheduler = scheduler
        self._processor_factory = processor_factory

    async def run(  # noqa: PLR0915
        self,
        grant: ingestion_lease_store.LeaseGrant,
        payload: sid_grant_control.SidClaimPayload,
        context: grant_control.RunContext,
    ) -> grant_control.RunOutcome:
        """Supervise one processor and acknowledge only a settled exact lane."""
        if type(payload) is not sid_grant_control.SidClaimPayload:
            message = "payload must be an exact SidClaimPayload"
            raise TypeError(message)
        try:
            lane = self._scheduler.open_lane(
                grant,
                stop_requested=context.stop_requested,
                grant_lost=context.grant_lost,
            )
        except feed_work_scheduler.SchedulerIntegrityError as exc:
            message = "scheduler failed before the exact lane opened"
            raise SidRunnerIntegrityError(message) from exc

        work_stop = asyncio.Event()
        cancellation_handoff = ProcessorCancellationHandoff(grant)
        processor_task: asyncio.Task[None] | None = None
        signal_waiters: tuple[asyncio.Task[bool], ...] = ()
        close_tasks: list[asyncio.Task[object]] = []
        try:
            processor = self._require_processor(
                self._processor_factory(grant, payload, lane)
            )
            processor_task = asyncio.create_task(
                processor.run(
                    work_stop,
                    signals=lane.signals,
                    cancellation_handoff=cancellation_handoff,
                ),
                name="bcfy-calls-sid-processor",
            )
            stop_wait = asyncio.create_task(
                context.stop_requested.wait(),
                name="bcfy-calls-sid-runner-stop-wait",
            )
            loss_wait = asyncio.create_task(
                context.grant_lost.wait(),
                name="bcfy-calls-sid-runner-loss-wait",
            )
            integrity_wait = asyncio.create_task(
                self._scheduler.integrity_failure_event.wait(),
                name="bcfy-calls-sid-runner-integrity-wait",
            )
            signal_waiters = (stop_wait, loss_wait, integrity_wait)
            await asyncio.wait(
                (*signal_waiters, processor_task),
                return_when=asyncio.FIRST_COMPLETED,
            )
            selection = self._select_terminal(context, processor_task)
            await self._stop_and_settle_processor(
                processor_task,
                work_stop,
            )

            close_signal, result = await self._close_for_signal(
                lane,
                context,
                selection.signal,
                stop_wait=stop_wait,
                loss_wait=loss_wait,
                integrity_wait=integrity_wait,
                close_tasks=close_tasks,
            )
            self._validate_close_result(grant, close_signal, result)

            return self._closed_outcome(selection, close_signal)
        except asyncio.CancelledError as first_cancelled:
            cancellation_handoff._record(  # noqa: SLF001
                self._runner_cancellation_cause(lane.signals),
                first_cancelled,
            )
            reason = self._cancellation_reason(context)
            cleanup = asyncio.create_task(
                self._cleanup_cancelled_run(
                    lane,
                    reason,
                    processor_task,
                    work_stop,
                ),
                name="bcfy-calls-sid-runner-cancel-cleanup",
            )
            await self._settle_cancelled_cleanup(cleanup)
            raise
        except SidRunnerIntegrityError:
            if processor_task is None:
                cleanup = self._create_close_task(
                    lane,
                    feed_work_scheduler.LaneCloseReason.SCHEDULER_SHUTDOWN,
                )
                close_tasks.append(cleanup)
                await cleanup
            raise
        except feed_work_scheduler.SchedulerIntegrityError as exc:
            message = "scheduler failed while the exact lane was closing"
            raise SidRunnerIntegrityError(message) from exc
        except Exception as exc:
            if processor_task is None:
                cleanup = self._create_close_task(
                    lane,
                    feed_work_scheduler.LaneCloseReason.SCHEDULER_SHUTDOWN,
                )
                close_tasks.append(cleanup)
                await cleanup
            message = "exact lane close failed before settlement was proved"
            raise SidRunnerIntegrityError(message) from exc
        finally:
            await self._cancel_and_settle(signal_waiters)
            if close_tasks:
                await asyncio.gather(*close_tasks, return_exceptions=True)

    async def _close_for_signal(
        self,
        lane: feed_work_scheduler.GrantLane,
        context: grant_control.RunContext,
        signal: _TerminalSignal,
        *,
        stop_wait: asyncio.Task[bool],
        loss_wait: asyncio.Task[bool],
        integrity_wait: asyncio.Task[bool],
        close_tasks: list[asyncio.Task[object]],
    ) -> tuple[_TerminalSignal, object]:
        """Close strongly while retaining every stronger monotonic signal."""
        close_task = self._create_close_task(
            lane,
            self._close_reason(signal, context),
        )
        close_tasks.append(close_task)
        close_signal = signal

        while True:
            waiters: set[asyncio.Task[object]] = {close_task}
            if close_signal is not _TerminalSignal.INTEGRITY:
                waiters.add(typing.cast("asyncio.Task[object]", integrity_wait))
            if close_signal < _TerminalSignal.LOSS:
                waiters.add(typing.cast("asyncio.Task[object]", loss_wait))
            if close_signal < _TerminalSignal.STOP:
                waiters.add(typing.cast("asyncio.Task[object]", stop_wait))
            done, _pending = await asyncio.wait(
                waiters,
                return_when=asyncio.FIRST_COMPLETED,
            )
            latest = self._current_signal(context)
            if latest is not None and latest > close_signal:
                close_signal = latest
                close_task = self._create_close_task(
                    lane,
                    self._close_reason(latest, context),
                )
                close_tasks.append(close_task)
                continue
            if close_task in done:
                return close_signal, await close_task

    def _current_signal(
        self,
        context: grant_control.RunContext,
    ) -> _TerminalSignal | None:
        """Recheck monotonic predicates in deterministic precedence order."""
        if self._scheduler.integrity_failure_event.is_set():
            return _TerminalSignal.INTEGRITY
        if context.grant_lost.is_set():
            return _TerminalSignal.LOSS
        if context.stop_requested.is_set():
            return _TerminalSignal.STOP
        return None

    @staticmethod
    def _require_processor(processor: object) -> _SidProcessor:
        if not callable(getattr(processor, "run", None)):
            message = "processor_factory returned an invalid processor"
            raise SidRunnerIntegrityError(message)
        return typing.cast("_SidProcessor", processor)

    def _closed_outcome(
        self,
        selection: _TerminalSelection,
        close_signal: _TerminalSignal,
    ) -> grant_control.RunOutcome:
        """Translate one settled lane selection into the generic outcome."""
        if close_signal is _TerminalSignal.INTEGRITY:
            if self._scheduler.integrity_failure_event.is_set():
                self._raise_scheduler_integrity()
            if selection.integrity is not None:
                raise selection.integrity
            message = "runner integrity selected without failure evidence"
            raise SidRunnerIntegrityError(message)
        if close_signal is _TerminalSignal.LOSS:
            return grant_control.RunLost()
        if close_signal is _TerminalSignal.STOP:
            return grant_control.RunStopped(
                grant_control.TerminalCause.PLANNED_DRAIN
            )
        if selection.failure is None:
            message = "processor failure selected without typed evidence"
            raise SidRunnerIntegrityError(message)
        return grant_control.RunFailed(
            selection.failure.status_reason,
            selection.failure.reason,
        )

    def _select_terminal(  # noqa: PLR0911
        self,
        context: grant_control.RunContext,
        processor_task: asyncio.Task[None],
    ) -> _TerminalSelection:
        """Apply integrity > loss > stop > typed processor precedence."""
        signal = self._current_signal(context)
        if signal is not None:
            return _TerminalSelection(signal)
        if not processor_task.done():
            message = "runner wait completed without a terminal predicate"
            return _TerminalSelection(
                _TerminalSignal.INTEGRITY,
                integrity=SidRunnerIntegrityError(message),
            )
        if processor_task.cancelled():
            message = "SID processor was cancelled without runner intent"
            return _TerminalSelection(
                _TerminalSignal.INTEGRITY,
                integrity=SidRunnerIntegrityError(message),
            )
        exception = processor_task.exception()
        if isinstance(exception, sid_processor.SidProcessorFailure):
            return _TerminalSelection(
                _TerminalSignal.PROCESSOR_FAILURE,
                failure=exception,
            )
        if isinstance(exception, sid_processor.SidProcessorAuthorityLost):
            return _TerminalSelection(_TerminalSignal.LOSS)
        if isinstance(exception, sid_processor.SidProcessorPlannedDrain):
            return _TerminalSelection(_TerminalSignal.STOP)
        if isinstance(exception, sid_processor.SidProcessorUndrained):
            message = "SID processor retained an undrained exact page"
            integrity = SidRunnerIntegrityError(message)
            integrity.__cause__ = exception
            return _TerminalSelection(
                _TerminalSignal.INTEGRITY,
                integrity=integrity,
            )
        if exception is None:
            message = "SID processor returned normally without a stop signal"
            return _TerminalSelection(
                _TerminalSignal.INTEGRITY,
                integrity=SidRunnerIntegrityError(message),
            )
        message = "SID processor raised an unknown terminal exception"
        integrity = SidRunnerIntegrityError(message)
        integrity.__cause__ = exception
        return _TerminalSelection(
            _TerminalSignal.INTEGRITY,
            integrity=integrity,
        )

    def _close_reason(
        self,
        signal: _TerminalSignal,
        context: grant_control.RunContext,
    ) -> feed_work_scheduler.LaneCloseReason:
        """Choose one strong reason without weakening confirmed loss."""
        if context.grant_lost.is_set() or signal is _TerminalSignal.LOSS:
            return feed_work_scheduler.LaneCloseReason.AUTHORITY_LOSS
        if signal is _TerminalSignal.INTEGRITY:
            return feed_work_scheduler.LaneCloseReason.SCHEDULER_SHUTDOWN
        return feed_work_scheduler.LaneCloseReason.PLANNED_DRAIN

    def _cancellation_reason(
        self,
        context: grant_control.RunContext,
    ) -> feed_work_scheduler.LaneCloseReason:
        """Request immediate cleanup without manufacturing a valid outcome."""
        if context.grant_lost.is_set():
            return feed_work_scheduler.LaneCloseReason.AUTHORITY_LOSS
        return feed_work_scheduler.LaneCloseReason.SCHEDULER_SHUTDOWN

    @staticmethod
    def _runner_cancellation_cause(
        signals: feed_work_scheduler.LaneSignalView,
    ) -> RunnerCancellationCause:
        """Classify exact loss only from this lane's immutable signal."""
        if signals.grant_lost.is_set():
            return RunnerCancellationCause.EXACT_GRANT_LOSS
        return RunnerCancellationCause.RAW_RUNNER_CANCELLATION

    @staticmethod
    async def _stop_and_settle_processor(
        processor_task: asyncio.Task[None],
        work_stop: asyncio.Event,
    ) -> None:
        """Set local stop before cancelling and settling the producer."""
        work_stop.set()
        if not processor_task.done():
            processor_task.cancel()
        await asyncio.gather(processor_task, return_exceptions=True)

    async def _cleanup_cancelled_run(
        self,
        lane: feed_work_scheduler.GrantLane,
        reason: feed_work_scheduler.LaneCloseReason,
        processor_task: asyncio.Task[None] | None,
        work_stop: asyncio.Event,
    ) -> object:
        """Settle producer mutation capability before raw-cancel close."""
        work_stop.set()
        if processor_task is not None:
            await self._stop_and_settle_processor(
                processor_task,
                work_stop,
            )
        return await lane.close(reason)

    @staticmethod
    def _create_close_task(
        lane: feed_work_scheduler.GrantLane,
        reason: feed_work_scheduler.LaneCloseReason,
    ) -> asyncio.Task[object]:
        return asyncio.create_task(
            lane.close(reason),
            name="bcfy-calls-sid-runner-lane-close",
        )

    @staticmethod
    async def _settle_cancelled_cleanup(task: asyncio.Task[object]) -> None:
        """Keep raw cancellation pending until the cleanup task settles."""
        while not task.done():
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                continue
            except Exception:
                return
        if not task.cancelled():
            task.exception()

    @staticmethod
    async def _cancel_and_settle(
        tasks: tuple[asyncio.Task[bool], ...],
    ) -> None:
        """Cancel and await every registered signal helper explicitly."""
        for task in tasks:
            if not task.done():
                task.cancel()
        for task in tasks:
            try:
                await task
            except asyncio.CancelledError:
                pass

    def _raise_scheduler_integrity(self) -> typing.Never:
        try:
            self._scheduler.raise_if_failed()
        except feed_work_scheduler.SchedulerIntegrityError as exc:
            message = "process scheduler integrity failed"
            raise SidRunnerIntegrityError(message) from exc
        message = "scheduler integrity event has no failure evidence"
        raise SidRunnerIntegrityError(message)

    @staticmethod
    def _validate_close_result(
        grant: ingestion_lease_store.LeaseGrant,
        close_signal: _TerminalSignal,
        result: object,
    ) -> None:
        if isinstance(result, feed_work_scheduler.Undrained):
            message = "exact lane did not prove mutation closure"
            raise SidRunnerIntegrityError(message)
        if not isinstance(result, feed_work_scheduler.LaneClosed):
            message = "exact lane returned an invalid close result"
            raise SidRunnerIntegrityError(message)
        if result.grant != grant:
            message = "exact lane close result crossed grant identity"
            raise SidRunnerIntegrityError(message)
        if (
            close_signal
            in (_TerminalSignal.PROCESSOR_FAILURE, _TerminalSignal.STOP)
            and result.reason
            is not feed_work_scheduler.LaneCloseReason.PLANNED_DRAIN
        ):
            message = "planned drain returned the wrong close reason"
            raise SidRunnerIntegrityError(message)
        if (
            close_signal is _TerminalSignal.LOSS
            and result.reason
            is not feed_work_scheduler.LaneCloseReason.AUTHORITY_LOSS
        ):
            message = "pending loss did not escalate exact lane cancellation"
            raise SidRunnerIntegrityError(message)
        if close_signal is _TerminalSignal.INTEGRITY and result.reason not in (
            feed_work_scheduler.LaneCloseReason.AUTHORITY_LOSS,
            feed_work_scheduler.LaneCloseReason.SCHEDULER_SHUTDOWN,
        ):
            message = "scheduler integrity did not request strong lane cleanup"
            raise SidRunnerIntegrityError(message)
