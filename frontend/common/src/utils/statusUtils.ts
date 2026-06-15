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

export function convertFeedStatusBackend(status: BackendFeedStatus): FeedStatus {
  switch (status) {
    case 'active':
      return 'active';
    case 'quarantined':
    case 'failing':
      return 'error';
    case 'deactivated':
    case 'unclaimed':
    default:
      return 'inactive';
  }
}

export function mapFeedStatusToBackendStatuses(status: string): BackendFeedStatus[] {
  const s = status.toLowerCase();
  switch (s) {
    case 'active':
      return ['active'];
    case 'error':
      return ['failing', 'quarantined'];
    case 'inactive':
      return ['unclaimed', 'deactivated'];
    default:
      return [];
  }
}
