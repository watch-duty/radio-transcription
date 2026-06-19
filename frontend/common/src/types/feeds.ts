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
  lastHeartbeat?: string;
  tags?: Tag[];
  statusReason?: BackendFeedStatusReason;
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
