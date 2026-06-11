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
  const sorted = [...segments].sort(
    (a, b) =>
      new Date(a.startTimestamp).getTime() -
      new Date(b.startTimestamp).getTime()
  );

  const consolidated: RenderableAudioSegment[] = [];
  let currentBundle: RenderableAudioSegment | null = null;

  for (const segment of sorted) {
    const isSpeech = segment.classification === AudioClassification.SPEECH;

    if (isSpeech) {
      if (currentBundle) {
        consolidated.push(currentBundle);
        currentBundle = null;
      }
      consolidated.push({ ...segment });
    } else {
      if (!currentBundle) {
        currentBundle = {
          ...segment,
          isSilenceBundle: true,
          bundledSegmentIds: [segment.id],
        };
      } else {
        currentBundle.endTimestamp = segment.endTimestamp;
        currentBundle.bundledSegmentIds?.push(segment.id);
      }
    }
  }

  if (currentBundle) {
    consolidated.push(currentBundle);
  }

  // Return sorted descending (newest at the top)
  return consolidated.sort(
    (a, b) =>
      new Date(b.endTimestamp).getTime() - new Date(a.endTimestamp).getTime()
  );
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
