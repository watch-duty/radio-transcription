import type {
  BackendFeedStatus,
  BackendFeedStatusReason,
  FeedStatus,
} from '../types/feeds.js';

const BACKEND_FEED_STATUS_REASONS = new Set<BackendFeedStatusReason>([
  'pipeline_publish_after_bookmark_failed',
  'source_offline',
  'source_unreachable',
  'source_rate_limited',
  'system_authentication_failed',
  'system_configuration_invalid',
  'system_source_configuration_invalid',
  'system_runtime_configuration_invalid',
  'system_credential_access_failed',
  'system_source_payload_invalid',
  'system_collector_error',
  'system_pipeline_error',
  'system_unexpected_error',
]);

export function convertFeedStatusReason(
  reason: string | null | undefined
): BackendFeedStatusReason | undefined {
  if (!reason) return undefined;
  return BACKEND_FEED_STATUS_REASONS.has(reason as BackendFeedStatusReason)
    ? (reason as BackendFeedStatusReason)
    : 'unknown';
}

export const FEED_STATUS_BUCKETS: Record<BackendFeedStatus, FeedStatus> = {
  active: 'active',
  failing: 'error',
  quarantined: 'error',
  unclaimed: 'inactive',
  deactivated: 'inactive',
};

const FEED_STATUS_OPTION_ORDER: FeedStatus[] = ['active', 'inactive', 'error'];

export function convertFeedStatusBackend(status: BackendFeedStatus): FeedStatus {
  return FEED_STATUS_BUCKETS[status] ?? 'inactive';
}

export function normalizeFeedStatusOptions(statuses: string[]): FeedStatus[] {
  const buckets = new Set(
    statuses.map((s) => convertFeedStatusBackend(s as BackendFeedStatus))
  );
  return FEED_STATUS_OPTION_ORDER.filter((bucket) => buckets.has(bucket));
}

export function mapFeedStatusToBackendStatuses(status: string): BackendFeedStatus[] {
  const bucket = status.toLowerCase();
  return (Object.keys(FEED_STATUS_BUCKETS) as BackendFeedStatus[]).filter(
    (backend) => FEED_STATUS_BUCKETS[backend] === bucket
  );
}
