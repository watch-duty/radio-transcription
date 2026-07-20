export enum SourceType {
  BCFY_FEEDS = 'bcfy_feeds',
  BCFY_CALLS = 'bcfy_calls',
  ECHO = 'echo',
  OPENMHZ = 'openmhz',
  FIRE_NOTIFICATIONS = 'fire_notifications',
}

export type BackendFeedStatus =
  | 'unclaimed'
  | 'active'
  | 'failing'
  | 'quarantined'
  | 'deactivated';

export type FeedStatus = 'active' | 'inactive' | 'error';

export type BackendFeedStatusReason =
  | 'unknown'
  | 'pipeline_publish_after_bookmark_failed'
  | 'source_offline'
  | 'source_unreachable'
  | 'source_rate_limited'
  | 'system_authentication_failed'
  | 'system_configuration_invalid'
  | 'system_source_configuration_invalid'
  | 'system_runtime_configuration_invalid'
  | 'system_credential_access_failed'
  | 'system_source_payload_invalid'
  | 'system_collector_error'
  | 'system_pipeline_error'
  | 'system_unexpected_error';

export interface Tag {
  key: string;
  value: string;
}

export interface BaseFeed {
  name: string;
  sourceType: SourceType;
}

export interface Feed extends BaseFeed {
  id: string;
  sourceFeedId?: string;
  sourceUrl?: string;
  archiveUrl?: string;
  status: FeedStatus;
  substatus: BackendFeedStatus;
  lastHeartbeat?: number;
  tags?: Tag[];
  statusReason?: BackendFeedStatusReason;
  lastSpeechSegmentTimestamp?: number;
  statusReasonDetail?: string;
}

export interface FeedCreate extends BaseFeed {
  sourceFeedId: string;
  tags?: Tag[];
}

export interface FeedUpdate {
  name: string;
  tags?: Tag[];
}

export interface ListFeedsResponse {
  feeds: Feed[];
  nextToken?: string;
  total: number;
}

export interface FeedSearchOptionsResponse {
  sourceTypes: string[];
  statuses: string[];
  tags: Tag[];
}

export interface AuditTrailValues { 
  id?: string;
  name?: string;
  sourceType?: SourceType;
  status?: BackendFeedStatus;
  failureCount?: number;
  retryAfter?: string;
  statusReason?: BackendFeedStatusReason | null;
  statusReasonUpdatedAt?: string;
  statusReasonDetail?: string | null;
  createdAt?: string;
  sourceFeedId?: string;
  tags?: Tag[];
}

export interface FeedHistoryEvent {
  id: string;
  feedId: string;
  action: string;
  actor: string;
  occurredAt: number;
  feedRevision: number;
  beforeValues: AuditTrailValues;
  afterValues: AuditTrailValues;
}

export interface ListFeedHistoryResponse {
  historyEvents: FeedHistoryEvent[];
  nextToken?: string;
  total: number;
}
