import { useMemo } from 'react';

import {
  AudioClassification,
  type AudioSegment,
  type FeedHistoryEvent,
  SourceType,
  isContinuousSource,
} from '@transcription/common';

import { segmentHasSpeech } from '../utils/annotationUtils';

export { isContinuousSource };

/**
 * Tolerance threshold to distinguish between minor timestamp rounding errors
 * and actual missing audio (outages) in continuous feeds.
 */
const MIN_GAP_FOR_OUTAGE_MS = 10;

export interface RenderableAudioSegment extends AudioSegment {
  /**
   * Indicates whether this segment represents a consolidated bundle of consecutive
   * non-speech (silence) segments. If true, the row displays a placeholder in the UI
   * rather than active transcript text.
   */
  isSilenceBundle?: boolean;
  /**
   * Indicates whether this segment represents a virtual outage bundle
   * when physical audio ingestion was interrupted.
   */
  isOutageBundle?: boolean;
  /**
   * The list of individual raw segment IDs that have been consolidated
   * into this silence bundle.
   */
  bundledSegmentIds?: string[];
}

export interface TimeInterval {
  startMs: number;
  endMs: number;
}

/**
 * Derives time windows during which the feed was offline or failing, based on
 * audit state history events.
 *
 * @param historyEvents Optional list of feed state history audit events.
 * @param nowMs Optional reference timestamp for ongoing offline state (defaults to Date.now()).
 */
export function deriveOfflineWindows(
  historyEvents?: FeedHistoryEvent[],
  nowMs: number = Date.now()
): TimeInterval[] {
  if (!historyEvents || historyEvents.length === 0) return [];

  // Sort events chronologically ascending (oldest first)
  const sorted = [...historyEvents].sort((a, b) => a.occurredAt - b.occurredAt);
  const windows: TimeInterval[] = [];
  let offlineStartMs: number | null = null;

  for (const event of sorted) {
    const status = event.afterValues?.status;
    const reason = event.afterValues?.statusReason;

    const isExplicitOnline =
      status === 'active' && (!reason || reason === 'unknown');
    const isExplicitOffline =
      (status !== undefined && status !== 'active') ||
      (reason !== undefined && reason !== null && reason !== 'unknown');

    if (isExplicitOffline && offlineStartMs === null) {
      offlineStartMs = event.occurredAt;
    } else if (isExplicitOnline && offlineStartMs !== null) {
      windows.push({ startMs: offlineStartMs, endMs: event.occurredAt });
      offlineStartMs = null;
    }
  }

  if (offlineStartMs !== null) {
    windows.push({ startMs: offlineStartMs, endMs: nowMs });
  }

  return windows;
}

/**
 * Efficiently checks if the timestamp interval [gapStartMs, gapEndMs] overlaps with any
 * derived offline windows using binary search.
 */
export function isOverlapWithOfflineWindows(
  gapStartMs: number,
  gapEndMs: number,
  windows: TimeInterval[]
): boolean {
  if (windows.length === 0) return false;

  let low = 0;
  let high = windows.length - 1;
  let candidateIdx = windows.length;

  while (low <= high) {
    const mid = (low + high) >> 1;
    if (windows[mid].endMs > gapStartMs) {
      candidateIdx = mid;
      high = mid - 1;
    } else {
      low = mid + 1;
    }
  }

  for (let i = candidateIdx; i < windows.length; i++) {
    const w = windows[i];
    if (w.startMs >= gapEndMs) break;
    if (gapStartMs < w.endMs && gapEndMs > w.startMs) {
      return true;
    }
  }

  return false;
}

export function consolidateAudioSegments(
  segments: AudioSegment[],
  audioSource?: SourceType,
  historyEvents?: FeedHistoryEvent[],
  nowMs: number = Date.now()
): RenderableAudioSegment[] {
  if (segments.length === 0) return [];

  const isContinuousAudioSource = isContinuousSource(audioSource);
  const offlineWindows = deriveOfflineWindows(historyEvents, nowMs);

  // Sort chronologically (ascending) to group consecutive segments in time order
  const chronologicalSegments = [...segments].sort(
    (a, b) => Date.parse(a.startTimestamp) - Date.parse(b.startTimestamp)
  );

  const consolidated: RenderableAudioSegment[] = [];
  let activeSilenceBundle: RenderableAudioSegment | null = null;

  const flushSilenceBundle = () => {
    if (activeSilenceBundle) {
      consolidated.push(activeSilenceBundle);
      activeSilenceBundle = null;
    }
  };

  for (let i = 0; i < chronologicalSegments.length; i++) {
    const segment = chronologicalSegments[i];
    const prevSegment = i > 0 ? chronologicalSegments[i - 1] : null;

    // Detect if there is a gap between the previous segment and this segment
    if (prevSegment) {
      const prevEnd = Date.parse(prevSegment.endTimestamp);
      const currStart = Date.parse(segment.startTimestamp);
      const gapMs = currStart - prevEnd;

      // Tolerance for rounding errors and minor overlaps
      if (gapMs > MIN_GAP_FOR_OUTAGE_MS) {
        // Check if the gap falls within an offline window from audit history
        const isOfflineInHistory = isOverlapWithOfflineWindows(
          prevEnd,
          currStart,
          offlineWindows
        );

        const isOutage =
          isOfflineInHistory ||
          (isContinuousAudioSource &&
            (segment.missingPriorContext || prevSegment.missingPostContext));

        if (isOutage) {
          flushSilenceBundle();

          // Inject virtual outage segment
          consolidated.push({
            id: `outage-${prevSegment.id}-${segment.id}`,
            feedId: segment.feedId,
            classification: AudioClassification.UNSPECIFIED,
            startTimestamp: prevSegment.endTimestamp,
            endTimestamp: segment.startTimestamp,
            missingPriorContext: false,
            missingPostContext: false,
            sourceAudioUris: [],
            canonicalAudioUri: '',
            playbackAudioUri: '',
            startAudioOffset: '0',
            endAudioOffset: '0',
            createdAt: segment.createdAt,
            annotations: [],
            isOutageBundle: true,
          } as RenderableAudioSegment);
        } else {
          // Continuous or non-continuous feed gap where feed was active:
          // flush silence bundle so silence does not bridge across unrecorded time gaps
          flushSilenceBundle();
        }
      }
    }

    const isSpeech = segmentHasSpeech(segment);

    if (isSpeech) {
      flushSilenceBundle();
      consolidated.push({ ...segment });
    } else {
      activeSilenceBundle = extendOrCreateSilenceBundle(
        activeSilenceBundle,
        segment
      );
    }
  }

  flushSilenceBundle();

  // Return sorted descending (newest at the top)
  return consolidated.sort(
    (a, b) => Date.parse(b.startTimestamp) - Date.parse(a.startTimestamp)
  );
}

function extendOrCreateSilenceBundle(
  activeBundle: RenderableAudioSegment | null,
  segment: AudioSegment
): RenderableAudioSegment {
  if (!activeBundle) {
    return {
      ...segment,
      isSilenceBundle: true,
      bundledSegmentIds: [segment.id],
    };
  }
  return {
    ...activeBundle,
    endTimestamp: segment.endTimestamp,
    bundledSegmentIds: [...(activeBundle.bundledSegmentIds || []), segment.id],
  };
}

/**
 * Custom hook to consolidate consecutive non-speech (silence) segments into bundles
 * and sort them descending (newest at the top).
 *
 * @param segments List of raw audio segments.
 * @param audioSource Source type or feed source of the audio (e.g. SourceType.BCFY_FEEDS).
 * @param historyEvents Optional list of feed state history audit events.
 * @returns List of renderable audio segments with consolidated silence bundles.
 */
export function useConsolidatedAudioSegments(
  segments: AudioSegment[],
  audioSource?: SourceType,
  historyEvents?: FeedHistoryEvent[]
): RenderableAudioSegment[] {
  return useMemo(() => {
    return consolidateAudioSegments(segments, audioSource, historyEvents);
  }, [segments, audioSource, historyEvents]);
}
