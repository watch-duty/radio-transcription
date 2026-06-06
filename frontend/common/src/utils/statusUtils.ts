import type { BackendFeedStatus, FeedStatus } from '../types/feeds.js';

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
