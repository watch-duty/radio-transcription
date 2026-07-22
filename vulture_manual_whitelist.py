# Manual Vulture Whitelist
# This file contains unused code/symbols that we intentionally keep in the codebase.
# Unlike vulture_whitelist.py, this file is maintained manually and WILL NOT be
# overwritten when running 'mise run lint:dead-code:update'.
# Please add detailed comments for why each entry is kept.

# MockCacheProvider is a mock implementation of CacheProvider used in integration and unit tests.
# Vulture excludes test directories (**/tests/**, **/test_*.py) from analysis, so it misses
# the imports of this class in tests, falsely flagging it as dead code.
from backend.pipeline.common.storage.mock_cache_provider import MockCacheProvider
MockCacheProvider
_.get_value

# Calls batch execution lands before the later stacked runtime-composition PR
# wires these public classes. Vulture excludes their focused tests.
from backend.pipeline.ingestion.collectors.bcfy_calls import pipeline
from backend.pipeline.ingestion.collectors.bcfy_calls import sid_runner
from backend.pipeline.ingestion.collectors.bcfy_calls import work_pool
pipeline.BcfyCallsFeedBatchExecutor
sid_runner.BcfyCallsSidRunner
work_pool.BcfyCallsWorkPool

# These PRs intentionally introduce the Lease lifecycle and membership storage
# boundaries before the generic runtime starts calling them. Vulture excludes
# the focused tests, so keep only dormant public methods, returned result fields,
# and lifecycle telemetry causes allowlisted until the runtime wiring lands.
from backend.pipeline.storage.ingestion_lease_store import (
    IngestionLeaseStore,
    LeaseHeartbeatResult,
    LeaseOperationResult,
    LeaseReleaseCause,
)
IngestionLeaseStore.claim_unclaimed
IngestionLeaseStore.claim_recoverable
IngestionLeaseStore.renew_heartbeats
IngestionLeaseStore.release
IngestionLeaseStore.finalize_failure
IngestionLeaseStore.load_membership
IngestionLeaseStore.commit_child_mutations
LeaseOperationResult.disposition
LeaseHeartbeatResult.disposition
LeaseReleaseCause.SHUTDOWN
LeaseReleaseCause.REBALANCE
LeaseReleaseCause.CANCELLATION
LeaseReleaseCause.ABANDONMENT

# The exact Feed-grant heartbeat storage boundary lands before the generic
# Feed/SID runtime adapter calls it. Vulture excludes the focused tests.
from backend.pipeline.storage.feed_store import FeedStore
FeedStore.renew_grant_heartbeats

# The typed Feed/SID control contracts and immutable worker profiles land as a
# reviewable foundation before CollectorRuntime composition. Vulture excludes
# their focused tests and cannot see Protocol member use through structural
# typing. Remove these entries as the supervisor and startup wiring land.
from backend.pipeline.ingestion import (
    failure_policy,
    feed_grant_control,
    grant_control,
    grant_supervisor,
    sid_grant_control,
    worker_profiles,
)
failure_policy.plan_failure
feed_grant_control.FeedGrantControl
feed_grant_control.FeedGrantControl.heartbeat
feed_grant_control.FeedGrantControl.finalize
sid_grant_control.SidGrantControl
sid_grant_control.SidGrantControl.heartbeat
sid_grant_control.SidGrantControl.finalize
grant_control.ClaimMode.RECOVERY
grant_control.RunContext.stop_requested
grant_control.RunContext.grant_lost
grant_control.GrantControl
grant_control.GrantControl.heartbeat
grant_control.GrantControl.finalize
grant_control.GrantRunner
worker_profiles.derive_bcfy_calls_authority
worker_profiles.resolve_worker_profile

# The pure Calls cursor policy lands before the scheduler and SID runner that
# consume it. Vulture excludes its focused tests.
from backend.pipeline.ingestion.collectors.bcfy_calls import cursor_policy
cursor_policy.ReplayFloorCause.REPLAY_OVERRIDE
cursor_policy.ReplayFloorCause.OVERLOAD
cursor_policy.BootstrapDecision.replay_floor
cursor_policy._issue_covered_page
cursor_policy._issue_replayable_page
cursor_policy._issue_no_progress_page
cursor_policy.LeaseCursor.next_page_sequence
cursor_policy.LeaseCursor.outstanding_candidate
cursor_policy.LeaseCursor.prepare
cursor_policy.LeaseCursor.prepare_no_progress
cursor_policy.LeaseCursor.accept
cursor_policy.LeaseCursor.accept_no_progress
cursor_policy.LeaseCursor.accept_replayable

# The generic supervisor lands before CollectorRuntime delegates its legacy
# Feed lifecycle to it. Vulture excludes the focused contract tests, so retain
# only the public handoff surface until the next runtime-migration PR.
grant_supervisor.GrantSupervisor
grant_supervisor.GrantSupervisor.admission_enabled
grant_supervisor.GrantSupervisor.integrity_failure_event
grant_supervisor.GrantSupervisor.integrity_failure
grant_supervisor.GrantSupervisor.admit_cycle
grant_supervisor.GrantSupervisor.heartbeat_cycle
grant_supervisor.GrantSupervisor.active_count

# The private Feed-affine shard lands before the public scheduler facade that
# consumes it. Vulture excludes its focused shard tests, so allowlist this
# dormant package only until the next stacked scheduler PR wires the facade.
from backend.pipeline.ingestion.feed_work_scheduler import _shard, _types
_shard._Shard
_shard._Shard.fatal_failure
_shard._Shard.admit
_shard._Shard.purge_exact
_shard._Shard.cancel_active_exact
_shard._Shard.abandon_cancellation
_shard._Shard.wait_for_held
_types._shard_index
cohort_timestamp

# The shared Calls provider lands before the SID polling runtime consumes its
# SID-specific selector. Vulture excludes the focused provider tests.
from backend.pipeline.ingestion.collectors.bcfy_calls.provider import (
    CallsProviderClient,
)
CallsProviderClient.fetch_sid_page

# FeedChangeNotificationPayload fields are consumed by Pydantic model validation
# and schema reflection, which Vulture cannot trace through direct Python
# references.
event_type
schema_version
event_id
action
occurred_at
actor_id
feed_revision
before_values
after_values

# FastAPI discovers this route handler through decorator registration.
receive_feed_change_notification

# StitcherDlqPayload TypedDict fields consumed by structure definition and dictionary creation.
error_message
