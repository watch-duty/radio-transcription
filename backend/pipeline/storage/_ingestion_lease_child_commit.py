"""Private child-Feed commit capability for ingestion Lease storage."""

from __future__ import annotations

import dataclasses
import datetime
import enum
import json
import typing
import uuid

from backend.pipeline.storage import (
    feed_audit_sql,
    feed_lifecycle,
    feed_store,
    ingestion_lease_contracts,
    ingestion_lease_queries,
)

if typing.TYPE_CHECKING:
    import collections.abc

    import asyncpg


LeaseGrant = ingestion_lease_contracts.LeaseGrant
BudgetedFailure = ingestion_lease_contracts.BudgetedFailure
NonBudgetedFailure = ingestion_lease_contracts.NonBudgetedFailure
LeaseMemberIdentity = ingestion_lease_contracts.LeaseMemberIdentity
AdmittedAudioProgress = ingestion_lease_contracts.AdmittedAudioProgress
SourceObservation = ingestion_lease_contracts.SourceObservation
ClosedCohortProgress = ingestion_lease_contracts.ClosedCohortProgress
FeedFailureTransition = ingestion_lease_contracts.FeedFailureTransition
ChildMutation = ingestion_lease_contracts.ChildMutation
NoLeaseEffect = ingestion_lease_contracts.NoLeaseEffect
FinalizeLeaseRecovery = ingestion_lease_contracts.FinalizeLeaseRecovery
LeaseEffect = ingestion_lease_contracts.LeaseEffect
ChildMutationBatch = ingestion_lease_contracts.ChildMutationBatch
ChildDisposition = ingestion_lease_contracts.ChildDisposition
ChildMutationResult = ingestion_lease_contracts.ChildMutationResult
BatchCommitted = ingestion_lease_contracts.BatchCommitted


@dataclasses.dataclass(frozen=True, slots=True)
class _PreparedChildMutation:
    """Validated command plus values normalized before pool checkout."""

    mutation: ChildMutation
    reason_detail: str | None = None


@dataclasses.dataclass(frozen=True, slots=True)
class _AuditedChildState:
    """Decoded Feed state shared by locks and audited DML returns."""

    values: collections.abc.Mapping
    feed_id: uuid.UUID
    source_type: feed_store.SourceType
    status: feed_store.FeedStatus
    status_reason: feed_store.FeedStatusReason | None
    failure_count: int
    retry_after: datetime.datetime | None
    status_reason_detail: str | None
    audit_revision: int


@dataclasses.dataclass(frozen=True, slots=True)
class _LockedChildState(_AuditedChildState):
    """Decoded child state from the canonical row-lock projection."""

    last_processed_filename: str | None
    last_bookmark_time: datetime.datetime | None


@dataclasses.dataclass(frozen=True, slots=True)
class _NeutralChildState:
    """Narrow returned state for lifecycle-neutral cohort progress."""

    feed_id: uuid.UUID


type _UpdatedChildState = _AuditedChildState | _NeutralChildState


@dataclasses.dataclass(frozen=True, slots=True)
class _PlannedChildMutation:
    """Private, database-derived execution plan for one child command."""

    prepared: _PreparedChildMutation
    before_state: _LockedChildState | None
    disposition: ChildDisposition
    write_cursor: bool = False
    write_path: bool = False
    clear_lifecycle: bool = False

    @property
    def mutation(self) -> ChildMutation:
        """Return the validated caller command."""
        return self.prepared.mutation

    @property
    def feed_id(self) -> uuid.UUID:
        """Return the command's immutable Feed UUID."""
        return self.mutation.member.feed_id

    @property
    def needs_update(self) -> bool:
        """Whether this command requires a write-hot Feed update."""
        return (
            self.write_cursor
            or self.write_path
            or self.clear_lifecycle
            or (
                isinstance(self.mutation, FeedFailureTransition)
                and self.disposition is not ChildDisposition.REJECTED
            )
        )


@dataclasses.dataclass(frozen=True, slots=True)
class _ChildAuditRowset:
    """Private parallel arrays for one rowset-safe Feed audit insert."""

    feed_ids: tuple[uuid.UUID, ...]
    actions: tuple[str, ...]
    actors: tuple[str, ...]
    revisions: tuple[int, ...]
    before_values: tuple[str, ...]
    after_values: tuple[str, ...]


@dataclasses.dataclass(frozen=True, slots=True)
class PreparedChildCommit:
    """Validated child batch prepared before pool checkout."""

    grant: LeaseGrant
    mutations: tuple[_PreparedChildMutation, ...]
    lease_effect: LeaseEffect
    actor_id: str
    target_feed_ids: tuple[uuid.UUID, ...]


@dataclasses.dataclass(frozen=True, slots=True)
class AppliedChildCommit:
    """Planned children and returned rows beneath the held Lease lock."""

    prepared: PreparedChildCommit
    plans: tuple[_PlannedChildMutation, ...]
    updated_by_feed_id: dict[uuid.UUID, _UpdatedChildState]


@dataclasses.dataclass(frozen=True, slots=True)
class _PlannedMutationGroups:
    """Write-hot plans partitioned by the four closed command variants."""

    admitted_progress: tuple[_PlannedChildMutation, ...]
    source_observations: tuple[_PlannedChildMutation, ...]
    closed_cohort_progress: tuple[_PlannedChildMutation, ...]
    feed_failures: tuple[_PlannedChildMutation, ...]


@dataclasses.dataclass(frozen=True, slots=True)
class PendingChildEffects:
    """Committed result and notifications buffered until transaction exit."""

    committed: BatchCommitted
    notification_payloads: dict[uuid.UUID, object]


def _require_utc_cursor(
    value: object,
    *,
    field_name: str = "cursor",
) -> datetime.datetime | None:
    if value is None:
        return None
    if not isinstance(value, datetime.datetime):
        msg = f"{field_name} must be a datetime or None"
        raise TypeError(msg)
    if value.utcoffset() != datetime.timedelta(0):
        msg = f"{field_name} must be UTC-aware"
        raise ValueError(msg)
    return value


def _require_member_identity(
    grant: LeaseGrant,
    value: object,
) -> LeaseMemberIdentity:
    if not isinstance(value, LeaseMemberIdentity):
        msg = "child member must be a LeaseMemberIdentity"
        raise TypeError(msg)
    if not isinstance(value.feed_id, uuid.UUID):
        msg = "child member feed_id must be a UUID"
        raise TypeError(msg)
    if not isinstance(value.source_type, feed_store.SourceType):
        msg = "child member source_type must be a SourceType"
        raise TypeError(msg)
    for field_name, field_value in (
        ("sid", value.sid),
        ("group_id", value.group_id),
    ):
        if (
            not isinstance(field_value, str)
            or not field_value
            or not field_value.isascii()
            or not field_value.isdigit()
        ):
            msg = f"child member {field_name} must contain ASCII digits"
            raise ValueError(msg)
    if not isinstance(value.source_feed_id, str):
        msg = "child member source_feed_id must be a string"
        raise TypeError(msg)
    if value.source_type is not grant.source_type:
        msg = "child member source type does not match the Lease grant"
        raise ValueError(msg)
    if value.sid != grant.lease_key:
        msg = "child member SID does not match the Lease key"
        raise ValueError(msg)
    if value.source_feed_id != f"{value.sid}-{value.group_id}":
        msg = "child member source_feed_id does not match SID-group identity"
        raise ValueError(msg)
    return value


def _require_parallel_cardinality(
    rowset_name: str,
    *columns: collections.abc.Sequence[object],
) -> None:
    lengths = {len(column) for column in columns}
    if len(lengths) > 1:
        msg = f"{rowset_name} rowset columns have different cardinalities"
        raise ValueError(msg)


def _mutation_cursor(
    mutation: ChildMutation,
) -> datetime.datetime | None:
    """Return the cursor field shared semantically by child commands."""
    if isinstance(mutation, FeedFailureTransition):
        return mutation.completion_cursor
    if isinstance(
        mutation,
        (AdmittedAudioProgress, SourceObservation, ClosedCohortProgress),
    ):
        return mutation.cursor
    typing.assert_never(mutation)


def _require_closed_cohort_progress(
    mutation: ClosedCohortProgress,
) -> None:
    """Validate lifecycle-neutral progress fields before pool checkout."""
    path = mutation.last_processed_filename
    if path is not None and not isinstance(path, str):
        msg = "closed cohort last_processed_filename must be a string or None"
        raise TypeError(msg)
    if isinstance(path, str) and not path.strip():
        msg = (
            "closed cohort last_processed_filename must be nonempty "
            "when present"
        )
        raise ValueError(msg)
    if path is None and mutation.cursor is None:
        msg = "closed cohort progress requires a path or cursor"
        raise ValueError(msg)


def _prepare_child_batch(
    grant: LeaseGrant,
    value: object,
) -> tuple[tuple[_PreparedChildMutation, ...], LeaseEffect]:
    if type(value) is not ChildMutationBatch:
        msg = "batch must be a ChildMutationBatch"
        raise TypeError(msg)
    batch = value
    if not isinstance(batch.mutations, tuple):
        msg = "batch mutations must be an immutable tuple"
        raise TypeError(msg)
    if type(batch.lease_effect) not in (
        NoLeaseEffect,
        FinalizeLeaseRecovery,
    ):
        msg = "lease_effect must be NoLeaseEffect or FinalizeLeaseRecovery"
        raise TypeError(msg)

    seen_feed_ids: set[uuid.UUID] = set()
    prepared_mutations: list[_PreparedChildMutation] = []
    for mutation in batch.mutations:
        if type(mutation) not in (
            AdmittedAudioProgress,
            SourceObservation,
            ClosedCohortProgress,
            FeedFailureTransition,
        ):
            msg = f"unsupported child mutation {type(mutation).__name__}"
            raise TypeError(msg)
        member = _require_member_identity(grant, mutation.member)
        if member.feed_id in seen_feed_ids:
            msg = f"duplicate Feed UUID {member.feed_id}"
            raise ValueError(msg)
        seen_feed_ids.add(member.feed_id)
        _require_utc_cursor(
            _mutation_cursor(mutation),
            field_name=(
                "completion_cursor"
                if isinstance(mutation, FeedFailureTransition)
                else "cursor"
            ),
        )
        if isinstance(mutation, AdmittedAudioProgress):
            if (
                not isinstance(mutation.last_processed_filename, str)
                or not mutation.last_processed_filename.strip()
            ):
                msg = "last_processed_filename must be nonempty"
                raise ValueError(msg)
        elif isinstance(mutation, SourceObservation):
            pass
        elif isinstance(mutation, ClosedCohortProgress):
            _require_closed_cohort_progress(mutation)
        elif isinstance(mutation, FeedFailureTransition):
            ingestion_lease_contracts._require_failure_action(  # noqa: SLF001
                mutation.action
            )
            ingestion_lease_contracts._require_status_reason(  # noqa: SLF001
                mutation.status_reason
            )
            reason_detail = (
                ingestion_lease_contracts._status_reason_detail_storage_value(  # noqa: SLF001
                    mutation.reason
                )
            )
            prepared_mutations.append(
                _PreparedChildMutation(
                    mutation=mutation,
                    reason_detail=reason_detail,
                )
            )
            continue
        else:
            typing.assert_never(mutation)
        prepared_mutations.append(_PreparedChildMutation(mutation=mutation))
    return tuple(prepared_mutations), batch.lease_effect


def _child_feed_id_from_row(row: collections.abc.Mapping) -> uuid.UUID:
    feed_id = row["id"]
    if not isinstance(feed_id, uuid.UUID):
        msg = "child Feed row contains a non-UUID id"
        raise TypeError(msg)
    return feed_id


def _child_source_type_from_row(
    row: collections.abc.Mapping,
) -> feed_store.SourceType:
    value = row["source_type"]
    try:
        return feed_store.SourceType(value)
    except ValueError as error:
        msg = f"Unknown child Feed source type {value!r}"
        raise ValueError(msg) from error


def _should_write_cursor(
    current: datetime.datetime | None,
    requested: datetime.datetime | None,
) -> bool:
    """Return whether ``requested`` advances the durable Feed cursor."""
    return requested is not None and (current is None or requested > current)


def _has_dirty_child_lifecycle(
    state: _LockedChildState,
) -> bool:
    return (
        state.status is feed_store.FeedStatus.FAILING
        or state.failure_count != 0
        or state.retry_after is not None
        or state.status_reason is not None
        or state.status_reason_detail is not None
    )


def _effective_prior_status(
    state: _LockedChildState,
) -> feed_store.FeedStatus:
    """Apply the canonical dirty-active compatibility interpretation."""
    if state.status is feed_store.FeedStatus.ACTIVE and (
        state.failure_count > 0 or state.status_reason is not None
    ):
        return feed_store.FeedStatus.FAILING
    return state.status


def _child_audit_action(
    plan: _PlannedChildMutation,
    after_state: _AuditedChildState,
) -> str | None:
    """Select a canonical action from the locked before and returned after."""
    if isinstance(plan.mutation, ClosedCohortProgress):
        return None
    before_state = plan.before_state
    if before_state is None:
        msg = "updated child lacks locked before state"
        raise ValueError(msg)
    before_status = _effective_prior_status(before_state)
    after_status = after_state.status
    if isinstance(plan.mutation, FeedFailureTransition):
        if (
            after_status is feed_store.FeedStatus.QUARANTINED
            and before_status is not feed_store.FeedStatus.QUARANTINED
            and after_state.failure_count > before_state.failure_count
        ):
            return "feed.quarantined"
        if after_status is feed_store.FeedStatus.FAILING:
            if (
                before_status is not feed_store.FeedStatus.FAILING
                or before_state.status_reason != after_state.status_reason
            ):
                return "feed.failure_reported"
            return None
        msg = "committed Feed failure returned an invalid lifecycle state"
        raise ValueError(msg)
    if (
        before_status
        in (feed_store.FeedStatus.FAILING, feed_store.FeedStatus.QUARANTINED)
        and after_status
        not in (
            feed_store.FeedStatus.FAILING,
            feed_store.FeedStatus.QUARANTINED,
        )
        and after_state.status_reason is None
        and after_state.failure_count == 0
    ):
        return "feed.recovered"
    return None


def _json_compatible(value: object) -> object:
    if isinstance(value, datetime.datetime):
        return value.isoformat()
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, enum.Enum):
        return value.value
    msg = f"unsupported Feed audit JSON value {type(value).__name__}"
    raise TypeError(msg)


def _tags_from_property(value: object) -> object:
    if value is None:
        return []
    if isinstance(value, str):
        parsed = json.loads(value)
        if not isinstance(parsed, list):
            msg = "Feed audit tags must be a JSON array"
            raise TypeError(msg)
        return parsed
    if isinstance(value, list):
        return value
    msg = "Feed audit tags must be a JSON array"
    raise TypeError(msg)


def _child_audit_snapshot(
    state: _AuditedChildState,
    property_row: collections.abc.Mapping,
) -> str:
    source = dict(state.values)
    source["source_feed_id"] = property_row["source_feed_id"]
    source["tags"] = _tags_from_property(property_row["tags"])
    snapshot = {
        key: source[column]
        for key, column in feed_audit_sql.AUDITED_FEED_STATE_FIELDS
    }
    return json.dumps(
        snapshot,
        default=_json_compatible,
        separators=(",", ":"),
    )


def _audit_properties_by_id(
    expected_feed_ids: set[uuid.UUID],
    rows: collections.abc.Sequence[collections.abc.Mapping],
) -> dict[uuid.UUID, collections.abc.Mapping]:
    properties_by_id: dict[uuid.UUID, collections.abc.Mapping] = {}
    for row in rows:
        feed_id = row["feed_id"]
        if not isinstance(feed_id, uuid.UUID):
            msg = "Feed audit property row has a non-UUID id"
            raise TypeError(msg)
        if feed_id not in expected_feed_ids or feed_id in properties_by_id:
            msg = "Feed audit properties returned an unexpected row"
            raise ValueError(msg)
        properties_by_id[feed_id] = row
    if set(properties_by_id) != expected_feed_ids:
        msg = "Feed audit properties are missing an actual candidate"
        raise ValueError(msg)
    return properties_by_id


def _build_child_audit_rowset(
    candidates: collections.abc.Sequence[tuple[_PlannedChildMutation, str]],
    updated_by_feed_id: collections.abc.Mapping[
        uuid.UUID,
        _AuditedChildState,
    ],
    properties_by_id: dict[uuid.UUID, collections.abc.Mapping],
    actor_id: str,
) -> _ChildAuditRowset:
    feed_ids: list[uuid.UUID] = []
    actions: list[str] = []
    revisions: list[int] = []
    before_values: list[str] = []
    after_values: list[str] = []
    for plan, action in sorted(
        candidates,
        key=lambda item: item[0].feed_id.int,
    ):
        before_state = plan.before_state
        after_state = updated_by_feed_id.get(plan.feed_id)
        if before_state is None or after_state is None:
            msg = "Feed audit candidate lacks before or after state"
            raise ValueError(msg)
        property_row = properties_by_id[plan.feed_id]
        if property_row["source_feed_id"] != (
            plan.mutation.member.source_feed_id
        ):
            msg = "Feed audit property identity does not match the member"
            raise ValueError(msg)
        if after_state.audit_revision != before_state.audit_revision + 1:
            msg = "Feed audit revision did not advance exactly once"
            raise ValueError(msg)
        feed_ids.append(plan.feed_id)
        actions.append(action)
        revisions.append(after_state.audit_revision)
        before_values.append(_child_audit_snapshot(before_state, property_row))
        after_values.append(_child_audit_snapshot(after_state, property_row))

    actors = [actor_id] * len(feed_ids)
    _require_parallel_cardinality(
        "child audit",
        feed_ids,
        actions,
        actors,
        revisions,
        before_values,
        after_values,
    )
    return _ChildAuditRowset(
        feed_ids=tuple(feed_ids),
        actions=tuple(actions),
        actors=tuple(actors),
        revisions=tuple(revisions),
        before_values=tuple(before_values),
        after_values=tuple(after_values),
    )


def _status_from_row(
    row: collections.abc.Mapping,
    prefix: str = "",
) -> feed_store.FeedStatus:
    value = row[f"{prefix}status"]
    try:
        return feed_store.FeedStatus(value)
    except ValueError as error:
        msg = f"Unknown Lease status {value!r}"
        raise ValueError(msg) from error


def _status_reason_from_row(
    row: collections.abc.Mapping,
    prefix: str = "",
) -> feed_store.FeedStatusReason | None:
    value = row[f"{prefix}status_reason"]
    if value is None:
        return None
    try:
        return feed_store.FeedStatusReason(value)
    except ValueError as error:
        msg = f"Unknown Lease status reason {value!r}"
        raise ValueError(msg) from error


def _nonnegative_row_integer(
    row: collections.abc.Mapping,
    column: str,
) -> int:
    value = row[column]
    if isinstance(value, bool) or not isinstance(value, int):
        msg = f"child Feed {column} must be an integer"
        raise TypeError(msg)
    if value < 0:
        msg = f"child Feed {column} must be nonnegative"
        raise ValueError(msg)
    return value


def _optional_row_datetime(
    row: collections.abc.Mapping,
    column: str,
) -> datetime.datetime | None:
    value = row[column]
    if value is not None and not isinstance(value, datetime.datetime):
        msg = f"child Feed {column} must be a datetime or None"
        raise TypeError(msg)
    return value


def _optional_row_string(
    row: collections.abc.Mapping,
    column: str,
) -> str | None:
    value = row[column]
    if value is not None and not isinstance(value, str):
        msg = f"child Feed {column} must be a string or None"
        raise TypeError(msg)
    return value


def _audited_child_state_from_row(
    row: collections.abc.Mapping,
) -> _AuditedChildState:
    """Decode the projection shared by auditable child mutations."""
    return _AuditedChildState(
        values=row,
        feed_id=_child_feed_id_from_row(row),
        source_type=_child_source_type_from_row(row),
        status=_status_from_row(row),
        status_reason=_status_reason_from_row(row),
        failure_count=_nonnegative_row_integer(row, "failure_count"),
        retry_after=_optional_row_datetime(row, "retry_after"),
        status_reason_detail=_optional_row_string(
            row,
            "status_reason_detail",
        ),
        audit_revision=_nonnegative_row_integer(row, "audit_revision"),
    )


def _locked_child_state_from_row(
    row: collections.abc.Mapping,
) -> _LockedChildState:
    """Decode the canonical child lock projection once at the boundary."""
    audited = _audited_child_state_from_row(row)
    return _LockedChildState(
        values=audited.values,
        feed_id=audited.feed_id,
        source_type=audited.source_type,
        status=audited.status,
        status_reason=audited.status_reason,
        failure_count=audited.failure_count,
        retry_after=audited.retry_after,
        status_reason_detail=audited.status_reason_detail,
        audit_revision=audited.audit_revision,
        last_processed_filename=_optional_row_string(
            row,
            "last_processed_filename",
        ),
        last_bookmark_time=_optional_row_datetime(
            row,
            "last_bookmark_time",
        ),
    )


def _neutral_child_state_from_row(
    row: collections.abc.Mapping,
) -> _NeutralChildState:
    """Decode the intentionally narrow neutral DML projection."""
    return _NeutralChildState(feed_id=_child_feed_id_from_row(row))


def _locked_children_by_id(
    requested_feed_ids: set[uuid.UUID],
    rows: collections.abc.Sequence[collections.abc.Mapping],
) -> dict[uuid.UUID, _LockedChildState]:
    by_id: dict[uuid.UUID, _LockedChildState] = {}
    for row in rows:
        state = _locked_child_state_from_row(row)
        feed_id = state.feed_id
        if feed_id not in requested_feed_ids or feed_id in by_id:
            msg = "child Feed lock returned an unexpected or duplicate row"
            raise ValueError(msg)
        by_id[feed_id] = state
    return by_id


def _plan_non_failure_child_mutation(
    prepared: _PreparedChildMutation,
    mutation: AdmittedAudioProgress | SourceObservation | ClosedCohortProgress,
    before_state: _LockedChildState,
    *,
    write_cursor: bool,
) -> _PlannedChildMutation:
    """Plan progress or observation separately from failure charging."""
    if isinstance(mutation, ClosedCohortProgress):
        write_path = (
            mutation.last_processed_filename is not None
            and before_state.last_processed_filename
            != mutation.last_processed_filename
        )
        return _PlannedChildMutation(
            prepared=prepared,
            before_state=before_state,
            disposition=ChildDisposition.COMMITTED,
            write_cursor=write_cursor,
            write_path=write_path,
        )

    clear_lifecycle = _has_dirty_child_lifecycle(before_state)

    write_path = False
    if isinstance(mutation, AdmittedAudioProgress):
        write_path = write_cursor or (
            mutation.cursor is None
            and before_state.last_processed_filename
            != mutation.last_processed_filename
        )

    return _PlannedChildMutation(
        prepared=prepared,
        before_state=before_state,
        disposition=ChildDisposition.COMMITTED,
        write_cursor=write_cursor,
        write_path=write_path,
        clear_lifecycle=clear_lifecycle,
    )


def _plan_prepared_child_mutation(
    prepared: _PreparedChildMutation,
    before_state: _LockedChildState | None,
) -> _PlannedChildMutation:
    mutation = prepared.mutation
    if before_state is None:
        return _PlannedChildMutation(
            prepared=prepared,
            before_state=None,
            disposition=ChildDisposition.REJECTED,
        )

    write_cursor = _should_write_cursor(
        before_state.last_bookmark_time,
        _mutation_cursor(mutation),
    )
    if before_state.source_type is not mutation.member.source_type:
        return _PlannedChildMutation(
            prepared=prepared,
            before_state=before_state,
            disposition=ChildDisposition.REJECTED,
        )

    is_progress = isinstance(
        mutation,
        (AdmittedAudioProgress, ClosedCohortProgress),
    )
    allowed_statuses = (
        (
            feed_store.FeedStatus.ACTIVE,
            feed_store.FeedStatus.FAILING,
            feed_store.FeedStatus.DEACTIVATED,
        )
        if is_progress
        else (
            feed_store.FeedStatus.ACTIVE,
            feed_store.FeedStatus.FAILING,
        )
    )
    if before_state.status not in allowed_statuses:
        return _PlannedChildMutation(
            prepared=prepared,
            before_state=before_state,
            disposition=ChildDisposition.REJECTED,
        )

    if isinstance(mutation, FeedFailureTransition):
        failure = mutation
        quarantined = isinstance(failure.action, BudgetedFailure) and (
            before_state.failure_count + 1 >= failure.action.failure_threshold
        )
        return _PlannedChildMutation(
            prepared=prepared,
            before_state=before_state,
            disposition=(
                ChildDisposition.COMMITTED_AND_QUARANTINED
                if quarantined
                else ChildDisposition.COMMITTED
            ),
            write_cursor=write_cursor,
        )

    if isinstance(
        mutation,
        (AdmittedAudioProgress, SourceObservation, ClosedCohortProgress),
    ):
        return _plan_non_failure_child_mutation(
            prepared,
            mutation,
            before_state,
            write_cursor=write_cursor,
        )
    typing.assert_never(mutation)


def _group_planned_mutations(
    plans: tuple[_PlannedChildMutation, ...],
) -> _PlannedMutationGroups:
    """Partition write-hot plans with an exhaustive closed-union switch."""
    admitted_progress: list[_PlannedChildMutation] = []
    source_observations: list[_PlannedChildMutation] = []
    closed_cohort_progress: list[_PlannedChildMutation] = []
    feed_failures: list[_PlannedChildMutation] = []
    for plan in plans:
        if not plan.needs_update:
            continue
        mutation = plan.mutation
        if isinstance(mutation, AdmittedAudioProgress):
            admitted_progress.append(plan)
        elif isinstance(mutation, SourceObservation):
            source_observations.append(plan)
        elif isinstance(mutation, ClosedCohortProgress):
            closed_cohort_progress.append(plan)
        elif isinstance(mutation, FeedFailureTransition):
            feed_failures.append(plan)
        else:
            typing.assert_never(mutation)
    return _PlannedMutationGroups(
        admitted_progress=tuple(admitted_progress),
        source_observations=tuple(source_observations),
        closed_cohort_progress=tuple(closed_cohort_progress),
        feed_failures=tuple(feed_failures),
    )


def _updated_children_by_id(
    expected: collections.abc.Sequence[_PlannedChildMutation],
    rows: collections.abc.Sequence[collections.abc.Mapping],
) -> dict[uuid.UUID, _AuditedChildState]:
    expected_feed_ids = {plan.feed_id for plan in expected}
    rows_by_feed_id: dict[uuid.UUID, _AuditedChildState] = {}
    for row in rows:
        state = _audited_child_state_from_row(row)
        feed_id = state.feed_id
        if feed_id not in expected_feed_ids or feed_id in rows_by_feed_id:
            msg = "child DML returned an unexpected or duplicate row"
            raise ValueError(msg)
        rows_by_feed_id[feed_id] = state
    if set(rows_by_feed_id) != expected_feed_ids:
        msg = "child DML did not return every locked eligible Feed"
        raise ValueError(msg)
    return rows_by_feed_id


def _updated_neutral_children_by_id(
    expected: collections.abc.Sequence[_PlannedChildMutation],
    rows: collections.abc.Sequence[collections.abc.Mapping],
) -> dict[uuid.UUID, _NeutralChildState]:
    """Correlate lifecycle-neutral DML without reading lifecycle columns."""
    expected_feed_ids = {plan.feed_id for plan in expected}
    rows_by_feed_id: dict[uuid.UUID, _NeutralChildState] = {}
    for row in rows:
        state = _neutral_child_state_from_row(row)
        feed_id = state.feed_id
        if feed_id not in expected_feed_ids or feed_id in rows_by_feed_id:
            msg = "neutral child DML returned an unexpected or duplicate row"
            raise ValueError(msg)
        rows_by_feed_id[feed_id] = state
    if set(rows_by_feed_id) != expected_feed_ids:
        msg = "neutral child DML did not return every locked eligible Feed"
        raise ValueError(msg)
    return rows_by_feed_id


async def _apply_admitted_progress(
    connection: asyncpg.Connection,
    plans: tuple[_PlannedChildMutation, ...],
) -> collections.abc.Sequence[collections.abc.Mapping]:
    """Apply one static admitted-progress rowset."""
    mutations = [
        typing.cast("AdmittedAudioProgress", plan.mutation) for plan in plans
    ]
    feed_ids = [plan.feed_id for plan in plans]
    paths = [mutation.last_processed_filename for mutation in mutations]
    cursors = [mutation.cursor for mutation in mutations]
    write_cursors = [plan.write_cursor for plan in plans]
    write_paths = [plan.write_path for plan in plans]
    clear_lifecycle = [plan.clear_lifecycle for plan in plans]
    _require_parallel_cardinality(
        "admitted progress",
        feed_ids,
        paths,
        cursors,
        write_cursors,
        write_paths,
        clear_lifecycle,
    )
    return await connection.fetch(
        ingestion_lease_queries.APPLY_ADMITTED_PROGRESS_SQL,
        feed_ids,
        paths,
        cursors,
        write_cursors,
        write_paths,
        clear_lifecycle,
    )


async def _apply_source_observations(
    connection: asyncpg.Connection,
    plans: tuple[_PlannedChildMutation, ...],
) -> collections.abc.Sequence[collections.abc.Mapping]:
    """Apply one static source-observation rowset."""
    mutations = [
        typing.cast("SourceObservation", plan.mutation) for plan in plans
    ]
    feed_ids = [plan.feed_id for plan in plans]
    cursors = [mutation.cursor for mutation in mutations]
    write_cursors = [plan.write_cursor for plan in plans]
    clear_lifecycle = [plan.clear_lifecycle for plan in plans]
    _require_parallel_cardinality(
        "source observation",
        feed_ids,
        cursors,
        write_cursors,
        clear_lifecycle,
    )
    return await connection.fetch(
        ingestion_lease_queries.APPLY_SOURCE_OBSERVATIONS_SQL,
        feed_ids,
        cursors,
        write_cursors,
        clear_lifecycle,
    )


async def _apply_closed_cohort_progress(
    connection: asyncpg.Connection,
    plans: tuple[_PlannedChildMutation, ...],
) -> collections.abc.Sequence[collections.abc.Mapping]:
    """Apply one static lifecycle-neutral cohort progress rowset."""
    mutations = [
        typing.cast("ClosedCohortProgress", plan.mutation) for plan in plans
    ]
    feed_ids = [plan.feed_id for plan in plans]
    paths = [mutation.last_processed_filename for mutation in mutations]
    cursors = [mutation.cursor for mutation in mutations]
    write_cursors = [plan.write_cursor for plan in plans]
    write_paths = [plan.write_path for plan in plans]
    _require_parallel_cardinality(
        "closed cohort progress",
        feed_ids,
        paths,
        cursors,
        write_cursors,
        write_paths,
    )
    return await connection.fetch(
        ingestion_lease_queries.APPLY_CLOSED_COHORT_PROGRESS_SQL,
        feed_ids,
        paths,
        cursors,
        write_cursors,
        write_paths,
    )


async def _apply_feed_failures(
    connection: asyncpg.Connection,
    plans: tuple[_PlannedChildMutation, ...],
) -> collections.abc.Sequence[collections.abc.Mapping]:
    """Apply one static one-shot Feed failure rowset."""
    mutations = [
        typing.cast("FeedFailureTransition", plan.mutation) for plan in plans
    ]
    actions = [mutation.action for mutation in mutations]
    feed_ids = [plan.feed_id for plan in plans]
    cursors = [mutation.completion_cursor for mutation in mutations]
    write_cursors = [plan.write_cursor for plan in plans]
    is_budgeted = [isinstance(action, BudgetedFailure) for action in actions]
    thresholds = [
        action.failure_threshold
        if isinstance(action, BudgetedFailure)
        else feed_lifecycle.DEFAULT_FAILURE_THRESHOLD
        for action in actions
    ]
    backoff_maxima = [
        action.backoff_max_sec
        if isinstance(action, BudgetedFailure)
        else feed_lifecycle.DEFAULT_BACKOFF_MAX_SEC
        for action in actions
    ]
    backoff_bases = [
        action.backoff_base_sec
        if isinstance(action, BudgetedFailure)
        else feed_lifecycle.DEFAULT_BACKOFF_BASE_SEC
        for action in actions
    ]
    retry_times = [
        action.retry_after if isinstance(action, NonBudgetedFailure) else None
        for action in actions
    ]
    status_reasons = [mutation.status_reason.value for mutation in mutations]
    reason_details = [plan.prepared.reason_detail for plan in plans]
    _require_parallel_cardinality(
        "Feed failure",
        feed_ids,
        cursors,
        write_cursors,
        is_budgeted,
        thresholds,
        backoff_maxima,
        backoff_bases,
        retry_times,
        status_reasons,
        reason_details,
    )
    return await connection.fetch(
        ingestion_lease_queries.APPLY_FEED_FAILURES_SQL,
        feed_ids,
        cursors,
        write_cursors,
        is_budgeted,
        thresholds,
        backoff_maxima,
        backoff_bases,
        retry_times,
        status_reasons,
        reason_details,
    )


async def _write_child_audits(
    connection: asyncpg.Connection,
    plans: tuple[_PlannedChildMutation, ...],
    updated_by_feed_id: collections.abc.Mapping[
        uuid.UUID,
        _UpdatedChildState,
    ],
    actor_id: str,
) -> dict[uuid.UUID, object]:
    """Insert actual child audit candidates as one canonical rowset."""
    candidates: list[tuple[_PlannedChildMutation, str]] = []
    audited_by_feed_id: dict[uuid.UUID, _AuditedChildState] = {}
    for plan in plans:
        if not plan.needs_update or isinstance(
            plan.mutation,
            ClosedCohortProgress,
        ):
            continue
        after_state = updated_by_feed_id.get(plan.feed_id)
        if after_state is None:
            msg = "updated child lacks returned after state"
            raise ValueError(msg)
        if not isinstance(after_state, _AuditedChildState):
            msg = "audited child returned lifecycle-neutral state"
            raise TypeError(msg)
        action = _child_audit_action(plan, after_state)
        if action is not None:
            candidates.append((plan, action))
            audited_by_feed_id[plan.feed_id] = after_state
    if not candidates:
        return {}

    sorted_feed_ids = sorted(
        (plan.feed_id for plan, _action in candidates),
        key=lambda feed_id: feed_id.int,
    )
    property_rows = await connection.fetch(
        ingestion_lease_queries.LOAD_CHILD_AUDIT_PROPERTIES_SQL,
        sorted_feed_ids,
    )
    properties_by_id = _audit_properties_by_id(
        set(sorted_feed_ids),
        property_rows,
    )
    rowset = _build_child_audit_rowset(
        tuple(candidates),
        audited_by_feed_id,
        properties_by_id,
        actor_id,
    )
    audit_rows = await connection.fetch(
        ingestion_lease_queries.INSERT_CHILD_AUDIT_EVENTS_SQL,
        rowset.feed_ids,
        rowset.actions,
        rowset.actors,
        rowset.revisions,
        rowset.before_values,
        rowset.after_values,
    )
    payloads: dict[uuid.UUID, object] = {}
    expected_feed_ids = set(rowset.feed_ids)
    for row in audit_rows:
        feed_id = row["feed_id"]
        if (
            not isinstance(feed_id, uuid.UUID)
            or feed_id not in expected_feed_ids
            or feed_id in payloads
            or row["feed_audit_event"] is None
        ):
            msg = "Feed audit insert returned an unexpected row"
            raise ValueError(msg)
        payloads[feed_id] = row["feed_audit_event"]
    if set(payloads) != expected_feed_ids:
        msg = "Feed audit insert did not return every candidate"
        raise ValueError(msg)
    return payloads


def prepare_child_commit(
    grant: LeaseGrant,
    batch: object,
    actor_id: str,
) -> PreparedChildCommit:
    """Validate a child batch before pool checkout."""
    mutations, lease_effect = _prepare_child_batch(grant, batch)
    target_feed_ids = tuple(
        sorted(
            (prepared.mutation.member.feed_id for prepared in mutations),
            key=lambda feed_id: feed_id.int,
        )
    )
    return PreparedChildCommit(
        grant=grant,
        mutations=mutations,
        lease_effect=lease_effect,
        actor_id=actor_id,
        target_feed_ids=target_feed_ids,
    )


async def apply_child_mutations(
    connection: asyncpg.Connection,
    prepared: PreparedChildCommit,
) -> AppliedChildCommit:
    """Lock, plan, and apply the child portion of one prepared batch."""
    locked_rows = []
    if prepared.target_feed_ids:
        locked_rows = await connection.fetch(
            ingestion_lease_queries.LOCK_CHILD_FEEDS_SQL,
            list(prepared.target_feed_ids),
        )
    locked_by_id = _locked_children_by_id(
        set(prepared.target_feed_ids),
        locked_rows,
    )
    plans = tuple(
        _plan_prepared_child_mutation(
            prepared_mutation,
            locked_by_id.get(prepared_mutation.mutation.member.feed_id),
        )
        for prepared_mutation in prepared.mutations
    )

    updated_by_feed_id: dict[uuid.UUID, _UpdatedChildState] = {}
    groups = _group_planned_mutations(plans)
    if groups.admitted_progress:
        rows = await _apply_admitted_progress(
            connection,
            groups.admitted_progress,
        )
        updated_by_feed_id.update(
            _updated_children_by_id(groups.admitted_progress, rows)
        )
    if groups.source_observations:
        rows = await _apply_source_observations(
            connection,
            groups.source_observations,
        )
        updated_by_feed_id.update(
            _updated_children_by_id(groups.source_observations, rows)
        )
    if groups.closed_cohort_progress:
        rows = await _apply_closed_cohort_progress(
            connection,
            groups.closed_cohort_progress,
        )
        updated_by_feed_id.update(
            _updated_neutral_children_by_id(
                groups.closed_cohort_progress,
                rows,
            )
        )
    if groups.feed_failures:
        rows = await _apply_feed_failures(connection, groups.feed_failures)
        updated_by_feed_id.update(
            _updated_children_by_id(groups.feed_failures, rows)
        )

    return AppliedChildCommit(
        prepared=prepared,
        plans=plans,
        updated_by_feed_id=updated_by_feed_id,
    )


async def prepare_pending_effects(
    connection: asyncpg.Connection,
    applied: AppliedChildCommit,
) -> PendingChildEffects:
    """Write audits and assemble caller/post-commit effects."""
    notification_payloads = await _write_child_audits(
        connection,
        applied.plans,
        applied.updated_by_feed_id,
        applied.prepared.actor_id,
    )
    children = tuple(
        ChildMutationResult(
            feed_id=plan.feed_id,
            disposition=plan.disposition,
        )
        for plan in applied.plans
    )
    return PendingChildEffects(
        committed=BatchCommitted(children=children),
        notification_payloads=notification_payloads,
    )
