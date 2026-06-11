import { useMemo } from 'react';

import { AudioClassification, type AudioSegment } from '@transcription/common';

export interface RenderableAudioSegment extends AudioSegment {
  isSilenceBundle?: boolean;
  bundledSegmentIds?: string[];
}

export function consolidateAudioSegments(
  segments: AudioSegment[]
): RenderableAudioSegment[] {
  // Sort chronologically (ascending) to group consecutive segments in time order
  const chronologicalSegments = [...segments].sort(
    (a, b) =>
      new Date(a.startTimestamp).getTime() -
      new Date(b.startTimestamp).getTime()
  );

  const consolidated: RenderableAudioSegment[] = [];
  let activeSilenceBundle: RenderableAudioSegment | null = null;

  for (const segment of chronologicalSegments) {
    const isSpeech = segment.classification === AudioClassification.SPEECH;

    if (isSpeech) {
      if (activeSilenceBundle) {
        consolidated.push(activeSilenceBundle);
        activeSilenceBundle = null;
      }
      consolidated.push({ ...segment });
    } else {
      activeSilenceBundle = extendOrCreateSilenceBundle(
        activeSilenceBundle,
        segment
      );
    }
  }

  if (activeSilenceBundle) {
    consolidated.push(activeSilenceBundle);
  }

  // Return sorted descending (newest at the top)
  return consolidated.sort(
    (a, b) =>
      new Date(b.endTimestamp).getTime() - new Date(a.endTimestamp).getTime()
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
 * @returns List of renderable audio segments with consolidated silence bundles.
 */
export function useConsolidatedAudioSegments(
  segments: AudioSegment[]
): RenderableAudioSegment[] {
  return useMemo(() => {
    return consolidateAudioSegments(segments);
  }, [segments]);
}
