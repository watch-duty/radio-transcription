import { useMemo } from 'react';

import { AudioClassification, type AudioSegment } from '@transcription/common';

import { findTranscriptAnnotationData } from '../utils/annotationUtils';

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

export function consolidateAudioSegments(
  segments: AudioSegment[],
  isContinuousAudioSource: boolean = true
): RenderableAudioSegment[] {
  if (segments.length === 0) return [];

  // Sort chronologically (ascending) to group consecutive segments in time order
  const chronologicalSegments = [...segments].sort(
    (a, b) =>
      new Date(a.startTimestamp).getTime() -
      new Date(b.startTimestamp).getTime()
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
    if (prevSegment && isContinuousAudioSource) {
      const prevEnd = new Date(prevSegment.endTimestamp).getTime();
      const currStart = new Date(segment.startTimestamp).getTime();
      const gapMs = currStart - prevEnd;

      // Tolerance for rounding errors and minor overlaps
      if (gapMs > MIN_GAP_FOR_OUTAGE_MS) {
        const isOutage =
          segment.missingPriorContext || prevSegment.missingPostContext;

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
        }
      }
    }

    const hasTranscript = !!findTranscriptAnnotationData(segment.annotations);
    const isSpeech =
      segment.classification === AudioClassification.SPEECH || hasTranscript;

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
    (a, b) =>
      new Date(b.startTimestamp).getTime() -
      new Date(a.startTimestamp).getTime()
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
 * @param isContinuousAudioSource Whether the source feed is continuous.
 * @returns List of renderable audio segments with consolidated silence bundles.
 */
export function useConsolidatedAudioSegments(
  segments: AudioSegment[],
  isContinuousAudioSource: boolean = true
): RenderableAudioSegment[] {
  return useMemo(() => {
    return consolidateAudioSegments(segments, isContinuousAudioSource);
  }, [segments, isContinuousAudioSource]);
}
