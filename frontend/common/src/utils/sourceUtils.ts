import { SourceType } from '../types/feeds.js';

/**
 * Determines whether an audio source type represents a continuous stream feed.
 * Defaults to false if audioSource is omitted or not SourceType.BCFY_FEEDS.
 */
export function isContinuousSource(audioSource?: SourceType): boolean {
  return audioSource === SourceType.BCFY_FEEDS;
}
