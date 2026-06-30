import { AudioClassification, type AudioSegment } from '@transcription/common';

import { type RenderableAudioSegment } from '../hooks/useConsolidatedAudioSegments';

/**
 * Matches a consolidated segment by its own id or, for silence bundles, by any
 * of the raw segment ids it contains.
 */
export function isWithinSegment(
  segment: RenderableAudioSegment,
  id: string
): boolean {
  if (segment.id === id) {
    return true;
  }
  if (segment.isSilenceBundle && segment.bundledSegmentIds) {
    return segment.bundledSegmentIds.includes(id);
  }
  return false;
}

export function getSegmentDuration(segment: {
  startTimestamp: string;
  endTimestamp: string;
}): number {
  const diffMs =
    new Date(segment.endTimestamp).getTime() -
    new Date(segment.startTimestamp).getTime();
  return Math.max(0, diffMs / 1000);
}

/**
 * Finds the adjacent audio segment in the specified direction.
 * Newer segments have lower indices, older segments have higher indices.
 */
export function findNextAudioSegment(
  rawAudioSegments: AudioSegment[],
  currentId: string,
  direction: 'forward' | 'backward'
): { id: string; uri: string } | null {
  const currentIdx = rawAudioSegments.findIndex((s) => s.id === currentId);
  if (currentIdx === -1) return null;

  const step = direction === 'forward' ? -1 : 1; // forward = next newer = index decreasing; backward = next older = index increasing
  for (
    let i = currentIdx + step;
    i >= 0 && i < rawAudioSegments.length;
    i += step
  ) {
    const nextSegment = rawAudioSegments[i];
    if (nextSegment?.playbackAudioUri) {
      return { id: nextSegment.id, uri: nextSegment.playbackAudioUri };
    }
  }
  return null;
}

/**
 * Finds the adjacent speech segment in the specified direction.
 * Newer segments have lower indices, older segments have higher indices.
 */
export function findAdjacentSpeechSegment(
  audioSegments: RenderableAudioSegment[],
  currentId: string,
  direction: 'forward' | 'backward'
): { id: string; uri: string; index: number } | null {
  const currentConsolidatedIdx = audioSegments.findIndex((t) =>
    isWithinSegment(t, currentId)
  );
  if (currentConsolidatedIdx === -1) return null;

  const step = direction === 'forward' ? -1 : 1; // forward = next newer = index decreasing; backward = next older = index increasing
  for (
    let i = currentConsolidatedIdx + step;
    i >= 0 && i < audioSegments.length;
    i += step
  ) {
    const segment = audioSegments[i];
    if (
      segment.classification === AudioClassification.SPEECH &&
      segment.playbackAudioUri
    ) {
      return { id: segment.id, uri: segment.playbackAudioUri, index: i };
    }
  }
  return null;
}

export interface SkipTimeDestination {
  segmentId: string;
  playbackAudioUri: string;
  seekTime: number;
}

/**
 * Calculates the destination segment and relative seek time when skipping forward or backward by an offset,
 * accounting for segment boundaries and overshooting.
 *
 * @returns The destination skip target, or null if:
 *          - The active segment is not found in rawAudioSegments.
 *          - The target segment lacks a playable audio URI (e.g. it is a non-audio segment).
 *          - There are no playable segments available in the skip direction.
 */
export function calculateSkipTimeDestination(
  rawAudioSegments: AudioSegment[],
  activeSegmentId: string,
  currentTime: number,
  offsetSeconds: number
): SkipTimeDestination | null {
  const currentIdx = rawAudioSegments.findIndex(
    (s) => s.id === activeSegmentId
  );
  if (currentIdx === -1) return null;
  const activeSegment = rawAudioSegments[currentIdx];

  const activeDuration = getSegmentDuration(activeSegment);
  const targetTime = currentTime + offsetSeconds;

  // CASE 1: Within the bounds of the current segment
  if (targetTime >= 0 && targetTime <= activeDuration) {
    if (activeSegment.playbackAudioUri) {
      return {
        segmentId: activeSegment.id,
        playbackAudioUri: activeSegment.playbackAudioUri,
        seekTime: targetTime,
      };
    }
    return null;
  }

  // CASE 2: Overshot backwards (Replay / older segments)
  if (targetTime < 0) {
    let remainingOvershoot = -targetTime;

    for (
      let nextIdx = currentIdx + 1;
      nextIdx < rawAudioSegments.length;
      nextIdx++
    ) {
      const segment = rawAudioSegments[nextIdx];
      if (segment.playbackAudioUri) {
        const duration = getSegmentDuration(segment);
        if (duration >= remainingOvershoot) {
          return {
            segmentId: segment.id,
            playbackAudioUri: segment.playbackAudioUri,
            seekTime: duration - remainingOvershoot,
          };
        } else {
          remainingOvershoot -= duration;
        }
      }
    }

    // Fallback: If we overshot the oldest segment, play the oldest from the start (0)
    for (let i = rawAudioSegments.length - 1; i >= 0; i--) {
      const segment = rawAudioSegments[i];
      if (segment.playbackAudioUri) {
        return {
          segmentId: segment.id,
          playbackAudioUri: segment.playbackAudioUri,
          seekTime: 0,
        };
      }
    }
  }

  // CASE 3: Overshot forwards (Forward / newer segments)
  if (targetTime > activeDuration) {
    let remainingOvershoot = targetTime - activeDuration;

    for (let nextIdx = currentIdx - 1; nextIdx >= 0; nextIdx--) {
      const segment = rawAudioSegments[nextIdx];
      if (segment.playbackAudioUri) {
        const duration = getSegmentDuration(segment);
        if (duration >= remainingOvershoot) {
          return {
            segmentId: segment.id,
            playbackAudioUri: segment.playbackAudioUri,
            seekTime: remainingOvershoot,
          };
        } else {
          remainingOvershoot -= duration;
        }
      }
    }

    // Fallback: If we overshot the newest segment, seek to the end of the newest segment
    for (let i = 0; i < rawAudioSegments.length; i++) {
      const segment = rawAudioSegments[i];
      if (segment.playbackAudioUri) {
        return {
          segmentId: segment.id,
          playbackAudioUri: segment.playbackAudioUri,
          seekTime: getSegmentDuration(segment),
        };
      }
    }
  }

  return null;
}

/**
 * Determines the next audio segment to auto-play when a segment ends.
 * Since the audio segment list is sorted newest-first, the next transmission in time is at `currentIndex - 1`.
 */
export function getNextContinuousSegment(
  audioSegments: RenderableAudioSegment[],
  rawAudioSegments: AudioSegment[],
  endedSegmentId: string
): { id: string; uri: string } | null {
  // 1. First check if the ended segment was part of a silence bundle, and if there is a next newer segment in that same bundle!
  const parentBundle = audioSegments.find(
    (t) => t.isSilenceBundle && t.bundledSegmentIds?.includes(endedSegmentId)
  );

  if (parentBundle && parentBundle.bundledSegmentIds) {
    const endedIdx = parentBundle.bundledSegmentIds.indexOf(endedSegmentId);
    if (
      endedIdx !== -1 &&
      endedIdx < parentBundle.bundledSegmentIds.length - 1
    ) {
      const nextSegmentId = parentBundle.bundledSegmentIds[endedIdx + 1];
      const nextSegment = rawAudioSegments.find((s) => s.id === nextSegmentId);
      if (nextSegment && nextSegment.playbackAudioUri) {
        return { id: nextSegment.id, uri: nextSegment.playbackAudioUri };
      }
    }
  }

  // 2. If it was a Speech segment, or the last segment in a silence bundle, advance to the next newer audio segment row
  const currentIndex = audioSegments.findIndex((t) =>
    isWithinSegment(t, endedSegmentId)
  );

  let nextIndex = currentIndex - 1;
  while (nextIndex >= 0) {
    const nextAudioSegment = audioSegments[nextIndex];
    if (nextAudioSegment.playbackAudioUri) {
      // If the next audio segment is a silence bundle, play its first segment
      if (
        nextAudioSegment.isSilenceBundle &&
        nextAudioSegment.bundledSegmentIds &&
        nextAudioSegment.bundledSegmentIds.length > 0
      ) {
        const firstId = nextAudioSegment.bundledSegmentIds[0];
        const firstSegment = rawAudioSegments.find((s) => s.id === firstId);
        if (firstSegment && firstSegment.playbackAudioUri) {
          return { id: firstSegment.id, uri: firstSegment.playbackAudioUri };
        }
      } else {
        return {
          id: nextAudioSegment.id,
          uri: nextAudioSegment.playbackAudioUri,
        };
      }
    }
    // Skip outage bundles or segments without audio and check the next one
    nextIndex--;
  }

  return null;
}
