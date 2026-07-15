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
    feed_grant_control,
    grant_control,
    sid_grant_control,
    worker_profiles,
)
feed_grant_control.FeedGrantControl
feed_grant_control.FeedGrantControl.heartbeat
feed_grant_control.FeedGrantControl.finalize
sid_grant_control.SidGrantControl
sid_grant_control.SidGrantControl.heartbeat
sid_grant_control.SidGrantControl.finalize
grant_control.ClaimMode.RECOVERY
grant_control.RunContext.stop_requested
grant_control.RunContext.grant_lost
grant_control.RunContext.set_retrying
grant_control.GrantControl
grant_control.GrantControl.heartbeat
grant_control.GrantControl.finalize
grant_control.GrantRunner
worker_profiles.derive_bcfy_calls_authority
worker_profiles.resolve_worker_profile
worker_profiles.profile_digest

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
