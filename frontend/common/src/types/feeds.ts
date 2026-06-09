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
  | 'source_offline'
  | 'source_unreachable'
  | 'source_rate_limited'
  | 'system_authentication_failed'
  | 'system_configuration_invalid'
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
  quarantineReason?: string;
  statusReason?: BackendFeedStatusReason;
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
}

