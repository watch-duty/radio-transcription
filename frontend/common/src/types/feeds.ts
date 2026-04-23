export type SourceType = 'bcfy_feeds' | 'bcfy_calls' | 'echo' | 'openmhz';

export interface BaseFeed {
  name: string;
  sourceType: SourceType;
}

export interface Feed extends BaseFeed {
  id: string;
  sourceFeedId?: string;
  externalId?: string;
  sourceUrl?: string;
  archiveUrl?: string;
}

export interface FeedCreate extends BaseFeed {
  sourceFeedId: string;
  externalId: string;
}
