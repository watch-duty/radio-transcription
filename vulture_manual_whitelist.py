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

# This PR intentionally introduces the Lease lifecycle storage boundary before
# the generic runtime starts calling it. Vulture excludes the focused tests,
# so keep only the dormant public methods and release telemetry causes added by
# this change allowlisted until the runtime wiring lands.
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
LeaseOperationResult.disposition
LeaseHeartbeatResult.disposition
LeaseReleaseCause.SHUTDOWN
LeaseReleaseCause.REBALANCE
LeaseReleaseCause.CANCELLATION
LeaseReleaseCause.ABANDONMENT

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
