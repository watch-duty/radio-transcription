"""Pure bootstrap and progress policy for Broadcastify Calls cursors."""

from __future__ import annotations

import dataclasses
import datetime
import enum
import math
import typing

if typing.TYPE_CHECKING:
    import collections.abc

    from backend.pipeline.storage import ingestion_lease_store


__all__ = [
    "BootstrapDecision",
    "CursorIntegrityError",
    "LeaseCursor",
    "NoProgressPageCandidate",
    "PageCandidate",
    "PageCursorCandidate",
    "ReplayFloorCause",
    "ReplayFloorDecision",
    "ReplayFloorReached",
    "apply_replay_floor",
    "bootstrap_cursor",
]


class CursorIntegrityError(ValueError):
    """A Lease Cursor candidate or receipt violates its exact contract."""


class ReplayFloorCause(enum.StrEnum):
    """Closed reason why one known request start met the replay floor.

    Attributes:
        BOOTSTRAP: Durable cursor bootstrap reached the replay floor.
        RECOVERY: Recovery processing reached the replay floor.
        REPLAY_OVERRIDE: An explicit replay override reached the replay floor.
        OVERLOAD: Overload handling reached the replay floor.
    """

    BOOTSTRAP = "bootstrap"
    RECOVERY = "recovery"
    REPLAY_OVERRIDE = "replay_override"
    OVERLOAD = "overload"


@dataclasses.dataclass(frozen=True, slots=True)
class ReplayFloorReached:
    """Exact known interval removed by one five-minute floor decision.

    Attributes:
        cause: Closed provenance for the floor decision.
        requested_start: Original inclusive request start.
        floor_start: Oldest request start accepted by the provider.
        lost_span_start: Inclusive start of the known lost interval.
        lost_span_end: Exclusive end of the known lost interval.
        lost_duration_seconds: Exact duration of the known lost interval.
    """

    cause: ReplayFloorCause
    requested_start: datetime.datetime
    floor_start: datetime.datetime
    lost_span_start: datetime.datetime
    lost_span_end: datetime.datetime
    lost_duration_seconds: float

    def __post_init__(self) -> None:
        requested_start = _require_utc_datetime(
            self.requested_start,
            field_name="requested_start",
        )
        floor_start = _require_utc_datetime(
            self.floor_start,
            field_name="floor_start",
        )
        lost_span_start = _require_utc_datetime(
            self.lost_span_start,
            field_name="lost_span_start",
        )
        lost_span_end = _require_utc_datetime(
            self.lost_span_end,
            field_name="lost_span_end",
        )
        if requested_start > floor_start:
            msg = "requested_start must be at or before floor_start"
            raise ValueError(msg)
        if lost_span_start != requested_start or lost_span_end != floor_start:
            msg = (
                "lost span must exactly cover requested_start through "
                "floor_start"
            )
            raise ValueError(msg)
        if isinstance(self.lost_duration_seconds, bool):
            msg = "lost_duration_seconds must be a number"
            raise TypeError(msg)
        duration = float(self.lost_duration_seconds)
        expected = (floor_start - requested_start).total_seconds()
        if not math.isfinite(duration) or duration < 0 or duration != expected:
            msg = "lost_duration_seconds must exactly match the lost span"
            raise ValueError(msg)


@dataclasses.dataclass(frozen=True, slots=True)
class ReplayFloorDecision:
    """Pure selected start plus optional exact replay-loss evidence.

    Attributes:
        selected_start: Request start bounded to the live window, or ``None``.
        floor_start: Oldest request start accepted by the provider.
        floor_reached: Exact known loss evidence when the floor was reached.
    """

    selected_start: datetime.datetime | None
    floor_start: datetime.datetime
    floor_reached: ReplayFloorReached | None


@dataclasses.dataclass(frozen=True, slots=True)
class BootstrapDecision:
    """Evidence for one bounded durable Feed cursor bootstrap.

    Attributes:
        pos: Inclusive replay position, or ``None`` for all-null input.
        durable_minimum: Minimum non-null durable Feed cursor observed.
        replay_floor: Oldest replay position allowed for this bootstrap.
        clamped: Whether ``pos`` was raised to ``replay_floor``.
        floor_reached: Exact known loss evidence at or below the floor.
    """

    pos: datetime.datetime | None
    durable_minimum: datetime.datetime | None
    replay_floor: datetime.datetime
    clamped: bool
    floor_reached: ReplayFloorReached | None


class _CandidateSeal:
    """Unique process-local identity for one prepared page candidate."""

    __slots__ = ()


_CONSTRUCTION_KEY = object()


@dataclasses.dataclass(frozen=True, slots=True)
class PageCursorCandidate:
    """Immutable exact-next cursor candidate prepared for page coverage.

    Attributes:
        grant: Complete Lease ownership generation bound to the candidate.
        page_sequence: Exact grant-local page sequence awaiting coverage.
        last_pos: Validated monotonic page completion position.
    """

    grant: ingestion_lease_store.LeaseGrant
    page_sequence: int
    last_pos: datetime.datetime
    _seal: _CandidateSeal = dataclasses.field(repr=False, compare=False)
    _construction_key: dataclasses.InitVar[object | None] = None

    def __post_init__(self, _construction_key: object | None) -> None:
        if _construction_key is not _CONSTRUCTION_KEY:
            msg = "Page cursor candidates must be prepared by LeaseCursor"
            raise CursorIntegrityError(msg)
        _require_page_sequence(self.page_sequence)
        _require_integrity_utc_datetime(
            self.last_pos,
            field_name="last_pos",
        )
        if type(self._seal) is not _CandidateSeal:
            msg = "Page cursor candidate seal is invalid"
            raise CursorIntegrityError(msg)


@dataclasses.dataclass(frozen=True, slots=True)
class NoProgressPageCandidate:
    """Immutable exact-next candidate carrying no cursor evidence.

    Attributes:
        grant: Complete Lease ownership generation bound to the candidate.
        page_sequence: Exact grant-local page sequence awaiting settlement.
    """

    grant: ingestion_lease_store.LeaseGrant
    page_sequence: int
    _seal: _CandidateSeal = dataclasses.field(repr=False, compare=False)
    _construction_key: dataclasses.InitVar[object | None] = None

    def __post_init__(self, _construction_key: object | None) -> None:
        if _construction_key is not _CONSTRUCTION_KEY:
            msg = "No-progress candidates must be prepared by LeaseCursor"
            raise CursorIntegrityError(msg)
        _require_page_sequence(self.page_sequence)
        if type(self._seal) is not _CandidateSeal:
            msg = "No-progress candidate seal is invalid"
            raise CursorIntegrityError(msg)


@dataclasses.dataclass(frozen=True, slots=True)
class _CoveredPage:
    """Private proof that one exact page obtained bounded coverage."""

    grant: ingestion_lease_store.LeaseGrant
    page_sequence: int
    last_pos: datetime.datetime
    _seal: _CandidateSeal = dataclasses.field(repr=False, compare=False)
    _construction_key: dataclasses.InitVar[object | None] = None

    def __post_init__(self, _construction_key: object | None) -> None:
        if _construction_key is not _CONSTRUCTION_KEY:
            msg = "Covered page receipts require the private issuer"
            raise CursorIntegrityError(msg)
        _require_page_sequence(self.page_sequence)
        _require_integrity_utc_datetime(
            self.last_pos,
            field_name="last_pos",
        )
        if type(self._seal) is not _CandidateSeal:
            msg = "Covered page receipt seal is invalid"
            raise CursorIntegrityError(msg)


@dataclasses.dataclass(frozen=True, slots=True)
class _ReplayablePageSettled:
    """Private proof that one progress page settled for replay."""

    grant: ingestion_lease_store.LeaseGrant
    page_sequence: int
    last_pos: datetime.datetime
    _seal: _CandidateSeal = dataclasses.field(repr=False, compare=False)
    _construction_key: dataclasses.InitVar[object | None] = None

    def __post_init__(self, _construction_key: object | None) -> None:
        if _construction_key is not _CONSTRUCTION_KEY:
            msg = "Replayable page settlements require the private issuer"
            raise CursorIntegrityError(msg)
        _require_page_sequence(self.page_sequence)
        _require_integrity_utc_datetime(
            self.last_pos,
            field_name="last_pos",
        )
        if type(self._seal) is not _CandidateSeal:
            msg = "Replayable page settlement seal is invalid"
            raise CursorIntegrityError(msg)


@dataclasses.dataclass(frozen=True, slots=True)
class _NoProgressPageSettled:
    """Private proof that one exact no-progress page settled safely."""

    grant: ingestion_lease_store.LeaseGrant
    page_sequence: int
    _seal: _CandidateSeal = dataclasses.field(repr=False, compare=False)
    _construction_key: dataclasses.InitVar[object | None] = None

    def __post_init__(self, _construction_key: object | None) -> None:
        if _construction_key is not _CONSTRUCTION_KEY:
            msg = "No-progress settlements require the private issuer"
            raise CursorIntegrityError(msg)
        _require_page_sequence(self.page_sequence)
        if type(self._seal) is not _CandidateSeal:
            msg = "No-progress settlement seal is invalid"
            raise CursorIntegrityError(msg)


type PageCandidate = PageCursorCandidate | NoProgressPageCandidate
type PageSettlement = (
    _CoveredPage | _ReplayablePageSettled | _NoProgressPageSettled
)


def _read_seal(
    value: PageCandidate | PageSettlement,
) -> _CandidateSeal:
    """Read the module-private capability without exposing public access."""
    return object.__getattribute__(value, "_seal")


def _require_utc_datetime(
    value: datetime.datetime,
    *,
    field_name: str,
) -> datetime.datetime:
    if value.utcoffset() != datetime.timedelta(0):
        msg = f"{field_name} must be UTC-aware"
        raise ValueError(msg)
    return value


def _require_integrity_utc_datetime(
    value: datetime.datetime,
    *,
    field_name: str,
) -> datetime.datetime:
    try:
        return _require_utc_datetime(value, field_name=field_name)
    except ValueError as exc:
        raise CursorIntegrityError(str(exc)) from exc


def _require_page_sequence(value: int) -> int:
    if isinstance(value, bool):
        msg = "page_sequence must be an integer"
        raise CursorIntegrityError(msg)
    if value < 0:
        msg = "page_sequence must be nonnegative"
        raise CursorIntegrityError(msg)
    return value


def apply_replay_floor(
    requested_start: datetime.datetime | None,
    *,
    now: datetime.datetime,
    cause: ReplayFloorCause,
) -> ReplayFloorDecision:
    """Select one request start within the unconditional five-minute window.

    A known start exactly equal to the floor is intentionally evidence-bearing:
    the source window has been reached even though the known lost duration is
    zero. A future start is bounded to ``now`` without inventing loss. ``None``
    remains an omitted request position and never invents loss.

    Args:
        requested_start: Requested inclusive start, or ``None`` to omit it.
        now: Explicit UTC upper boundary of the provider's live window.
        cause: Closed provenance for any replay-floor evidence.

    Returns:
        Immutable bounded request decision and any exact replay-loss evidence.

    Raises:
        ValueError: ``now`` or ``requested_start`` is not UTC-aware.
    """
    validated_now = _require_utc_datetime(now, field_name="now")
    floor_start = validated_now - datetime.timedelta(minutes=5)
    if requested_start is None:
        return ReplayFloorDecision(
            selected_start=None,
            floor_start=floor_start,
            floor_reached=None,
        )

    validated_start = _require_utc_datetime(
        requested_start,
        field_name="requested_start",
    )
    if validated_start > floor_start:
        return ReplayFloorDecision(
            selected_start=min(validated_start, validated_now),
            floor_start=floor_start,
            floor_reached=None,
        )

    lost_duration_seconds = (floor_start - validated_start).total_seconds()
    evidence = ReplayFloorReached(
        cause=cause,
        requested_start=validated_start,
        floor_start=floor_start,
        lost_span_start=validated_start,
        lost_span_end=floor_start,
        lost_duration_seconds=lost_duration_seconds,
    )
    return ReplayFloorDecision(
        selected_start=floor_start,
        floor_start=floor_start,
        floor_reached=evidence,
    )


def bootstrap_cursor(
    cursors: collections.abc.Iterable[datetime.datetime | None],
    *,
    now: datetime.datetime,
    cause: ReplayFloorCause = ReplayFloorCause.BOOTSTRAP,
) -> BootstrapDecision:
    """Select one bounded inclusive replay position from Feed cursors.

    Args:
        cursors: Eligible independent durable Feed cursors.
        now: Explicit UTC time used to calculate the replay floor.
        cause: Closed provenance for any floor evidence.

    Returns:
        Immutable bootstrap evidence with the selected replay position.

    Raises:
        ValueError: ``now`` or a non-null Feed cursor is not UTC-aware.
    """
    validated_now = _require_utc_datetime(now, field_name="now")
    durable_minimum: datetime.datetime | None = None

    for cursor in cursors:
        if cursor is None:
            continue
        validated_cursor = _require_utc_datetime(
            cursor,
            field_name="Feed cursor",
        )
        if durable_minimum is None or validated_cursor < durable_minimum:
            durable_minimum = validated_cursor

    if durable_minimum is None:
        floor_decision = apply_replay_floor(
            None,
            now=validated_now,
            cause=cause,
        )
        return BootstrapDecision(
            pos=None,
            durable_minimum=None,
            replay_floor=floor_decision.floor_start,
            clamped=False,
            floor_reached=None,
        )

    floor_decision = apply_replay_floor(
        durable_minimum,
        now=validated_now,
        cause=cause,
    )
    return BootstrapDecision(
        pos=floor_decision.selected_start,
        durable_minimum=durable_minimum,
        replay_floor=floor_decision.floor_start,
        clamped=durable_minimum < floor_decision.floor_start,
        floor_reached=floor_decision.floor_reached,
    )


def _issue_covered_page(candidate: PageCursorCandidate) -> _CoveredPage:
    """Issue a private receipt after the scheduler proves page coverage."""
    if type(candidate) is not PageCursorCandidate:
        msg = "candidate must be an exact PageCursorCandidate"
        raise CursorIntegrityError(msg)
    seal = _read_seal(candidate)
    if type(seal) is not _CandidateSeal:
        msg = "Page cursor candidate seal is invalid"
        raise CursorIntegrityError(msg)
    return _CoveredPage(
        grant=candidate.grant,
        page_sequence=candidate.page_sequence,
        last_pos=candidate.last_pos,
        _seal=seal,
        _construction_key=_CONSTRUCTION_KEY,
    )


def _issue_replayable_page(
    candidate: PageCursorCandidate,
) -> _ReplayablePageSettled:
    """Issue a private settlement after definitive replayable finalization."""
    if type(candidate) is not PageCursorCandidate:
        msg = "candidate must be an exact PageCursorCandidate"
        raise CursorIntegrityError(msg)
    seal = _read_seal(candidate)
    if type(seal) is not _CandidateSeal:
        msg = "Page cursor candidate seal is invalid"
        raise CursorIntegrityError(msg)
    return _ReplayablePageSettled(
        grant=candidate.grant,
        page_sequence=candidate.page_sequence,
        last_pos=candidate.last_pos,
        _seal=seal,
        _construction_key=_CONSTRUCTION_KEY,
    )


def _issue_no_progress_page(
    candidate: NoProgressPageCandidate,
) -> _NoProgressPageSettled:
    """Issue a private settlement after bounded no-progress coverage."""
    if type(candidate) is not NoProgressPageCandidate:
        msg = "candidate must be an exact NoProgressPageCandidate"
        raise CursorIntegrityError(msg)
    seal = _read_seal(candidate)
    if type(seal) is not _CandidateSeal:
        msg = "No-progress candidate seal is invalid"
        raise CursorIntegrityError(msg)
    return _NoProgressPageSettled(
        grant=candidate.grant,
        page_sequence=candidate.page_sequence,
        _seal=seal,
        _construction_key=_CONSTRUCTION_KEY,
    )


class LeaseCursor:
    """Grant-local monotonic cursor advanced by exact covered receipts.

    The cursor retains one outstanding candidate and no accepted receipt
    history. Its state is process-local and must be reconstructed from durable
    Feed cursor inputs after restart.

    Attributes:
        grant: Complete immutable Lease ownership generation.
        pos: Current grant-local upstream cursor.
        next_page_sequence: Exact sequence expected for the next receipt.
        outstanding_candidate: Candidate currently awaiting coverage.
    """

    __slots__ = (
        "_grant",
        "_next_page_sequence",
        "_outstanding_candidate",
        "_pos",
    )

    def __init__(
        self,
        grant: ingestion_lease_store.LeaseGrant,
        *,
        pos: datetime.datetime | None,
    ) -> None:
        """Create fresh in-memory state for one exact grant.

        Args:
            grant: Complete Lease ownership generation for this cursor.
            pos: Bootstrap position selected from durable Feed cursors.

        Raises:
            CursorIntegrityError: The grant or position is invalid.
        """
        self._grant = grant
        self._pos = (
            None
            if pos is None
            else _require_integrity_utc_datetime(pos, field_name="pos")
        )
        self._next_page_sequence = 0
        self._outstanding_candidate: PageCandidate | None = None

    @property
    def grant(self) -> ingestion_lease_store.LeaseGrant:
        """Return the complete grant bound to this cursor."""
        return self._grant

    @property
    def pos(self) -> datetime.datetime | None:
        """Return the current grant-local upstream cursor."""
        return self._pos

    @property
    def next_page_sequence(self) -> int:
        """Return the exact sequence expected by the next receipt."""
        return self._next_page_sequence

    @property
    def outstanding_candidate(self) -> PageCandidate | None:
        """Return the candidate currently awaiting scheduler coverage."""
        return self._outstanding_candidate

    def prepare(self, last_pos: datetime.datetime) -> PageCursorCandidate:
        """Prepare one exact-next candidate without advancing progress.

        Args:
            last_pos: Valid UTC page completion position.

        Returns:
            Immutable candidate bound to this grant and next sequence.

        Raises:
            CursorIntegrityError: The position regresses, is invalid, or a
                candidate is already outstanding.
        """
        validated_last_pos = _require_integrity_utc_datetime(
            last_pos,
            field_name="last_pos",
        )
        if self._outstanding_candidate is not None:
            msg = "A page cursor candidate is already outstanding"
            raise CursorIntegrityError(msg)
        if self._pos is not None and validated_last_pos < self._pos:
            msg = "last_pos must not regress the Lease Cursor"
            raise CursorIntegrityError(msg)

        candidate = PageCursorCandidate(
            grant=self._grant,
            page_sequence=self._next_page_sequence,
            last_pos=validated_last_pos,
            _seal=_CandidateSeal(),
            _construction_key=_CONSTRUCTION_KEY,
        )
        self._outstanding_candidate = candidate
        return candidate

    def prepare_no_progress(self) -> NoProgressPageCandidate:
        """Prepare one exact-next candidate carrying no cursor evidence.

        Returns:
            Immutable candidate bound to this grant and next sequence.

        Raises:
            CursorIntegrityError: A candidate is already outstanding.
        """
        if self._outstanding_candidate is not None:
            msg = "A page cursor candidate is already outstanding"
            raise CursorIntegrityError(msg)
        candidate = NoProgressPageCandidate(
            grant=self._grant,
            page_sequence=self._next_page_sequence,
            _seal=_CandidateSeal(),
            _construction_key=_CONSTRUCTION_KEY,
        )
        self._outstanding_candidate = candidate
        return candidate

    def accept(self, receipt: _CoveredPage) -> datetime.datetime:
        """Consume the exact outstanding covered-page receipt once.

        Args:
            receipt: Private scheduler-issued proof of bounded page coverage.

        Returns:
            Current monotonic position after consuming the receipt.

        Raises:
            CursorIntegrityError: The receipt is absent, forged, crossed,
                skipped, duplicated, regressive, or for another candidate.
        """
        if type(receipt) is not _CoveredPage:
            msg = "receipt must be a private covered-page capability"
            raise CursorIntegrityError(msg)

        candidate = self._outstanding_candidate
        if type(candidate) is not PageCursorCandidate:
            msg = "No progress cursor candidate is awaiting coverage"
            raise CursorIntegrityError(msg)
        if receipt.grant != self._grant:
            msg = "Covered page receipt grant does not match"
            raise CursorIntegrityError(msg)
        if receipt.page_sequence != self._next_page_sequence:
            msg = "Covered page receipt is not the exact next sequence"
            raise CursorIntegrityError(msg)

        validated_last_pos = _require_integrity_utc_datetime(
            receipt.last_pos,
            field_name="receipt.last_pos",
        )
        if self._pos is not None and validated_last_pos < self._pos:
            msg = "Covered page receipt regresses the Lease Cursor"
            raise CursorIntegrityError(msg)
        if _read_seal(receipt) is not _read_seal(candidate):
            msg = "Covered page receipt is for another candidate"
            raise CursorIntegrityError(msg)
        if (
            receipt.grant != candidate.grant
            or receipt.page_sequence != candidate.page_sequence
            or validated_last_pos != candidate.last_pos
        ):
            msg = "Covered page receipt fields do not match its candidate"
            raise CursorIntegrityError(msg)

        self._pos = (
            validated_last_pos
            if self._pos is None
            else max(self._pos, validated_last_pos)
        )
        self._next_page_sequence += 1
        self._outstanding_candidate = None
        return validated_last_pos

    def accept_no_progress(self, settlement: _NoProgressPageSettled) -> None:
        """Consume the exact no-progress settlement without moving ``pos``.

        Args:
            settlement: Private scheduler-issued no-progress capability.

        Returns:
            None.

        Raises:
            CursorIntegrityError: The settlement is absent, forged, crossed,
                skipped, duplicated, or for another candidate.
        """
        if type(settlement) is not _NoProgressPageSettled:
            msg = "settlement must be a private no-progress capability"
            raise CursorIntegrityError(msg)

        candidate = self._outstanding_candidate
        if type(candidate) is not NoProgressPageCandidate:
            msg = "No no-progress candidate is awaiting coverage"
            raise CursorIntegrityError(msg)
        if settlement.grant != self._grant:
            msg = "No-progress settlement grant does not match"
            raise CursorIntegrityError(msg)
        if settlement.page_sequence != self._next_page_sequence:
            msg = "No-progress settlement is not the exact next sequence"
            raise CursorIntegrityError(msg)
        if _read_seal(settlement) is not _read_seal(candidate):
            msg = "No-progress settlement is for another candidate"
            raise CursorIntegrityError(msg)
        if (
            settlement.grant != candidate.grant
            or settlement.page_sequence != candidate.page_sequence
        ):
            msg = "No-progress settlement fields do not match its candidate"
            raise CursorIntegrityError(msg)

        self._next_page_sequence += 1
        self._outstanding_candidate = None

    def accept_replayable(self, settlement: _ReplayablePageSettled) -> None:
        """Consume one replayable settlement without accepting ``last_pos``.

        Args:
            settlement: Private scheduler-issued replayable capability.

        Returns:
            None.

        Raises:
            CursorIntegrityError: The settlement is absent, forged, crossed,
                skipped, duplicated, invalid, or for another candidate.
        """
        if type(settlement) is not _ReplayablePageSettled:
            msg = "settlement must be a private replayable capability"
            raise CursorIntegrityError(msg)

        candidate = self._outstanding_candidate
        if type(candidate) is not PageCursorCandidate:
            msg = "No progress cursor candidate is awaiting settlement"
            raise CursorIntegrityError(msg)
        if settlement.grant != self._grant:
            msg = "Replayable page settlement grant does not match"
            raise CursorIntegrityError(msg)
        if settlement.page_sequence != self._next_page_sequence:
            msg = "Replayable settlement is not the exact next sequence"
            raise CursorIntegrityError(msg)

        validated_last_pos = _require_integrity_utc_datetime(
            settlement.last_pos,
            field_name="settlement.last_pos",
        )
        if _read_seal(settlement) is not _read_seal(candidate):
            msg = "Replayable settlement is for another candidate"
            raise CursorIntegrityError(msg)
        if (
            settlement.grant != candidate.grant
            or settlement.page_sequence != candidate.page_sequence
            or validated_last_pos != candidate.last_pos
        ):
            msg = "Replayable settlement fields do not match its candidate"
            raise CursorIntegrityError(msg)

        self._next_page_sequence += 1
        self._outstanding_candidate = None
