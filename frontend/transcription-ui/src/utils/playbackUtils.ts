import { type AudioSegment } from '@transcription/common';

import { type RenderableAudioSegment } from '../hooks/useConsolidatedAudioSegments';

/**
 * Matches a consolidated segment by its own id or, for silence bundles, by any
 * of the raw segment ids it contains.
 */
export function isWithinSegment(
  segment: RenderableAudioSegment,
  id: string
): boolean {
  return (
    segment.id === id || (segment.bundledSegmentIds?.includes(id) ?? false)
  );
}

/**
 * Gets the duration of a segment in seconds based on its start and end timestamps.
 */
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
export function findAdjacentAudioSegment(
  rawAudioSegments: AudioSegment[],
  currentId: string,
  direction: 'forward' | 'backward'
): { id: string; uri: string } | null {
  const currentIdx = rawAudioSegments.findIndex((s) => s.id === currentId);
  if (currentIdx === -1) return null;

  const step = direction === 'forward' ? -1 : 1; // forward = next newer = index decreasing; backward = next older = index increasing
  let nextIdx = currentIdx + step;

  while (nextIdx >= 0 && nextIdx < rawAudioSegments.length) {
    const nextSegment = rawAudioSegments[nextIdx];
    if (nextSegment?.playbackAudioUri) {
      return { id: nextSegment.id, uri: nextSegment.playbackAudioUri };
    }
    nextIdx += step;
  }
  return null;
}

/**
 * Finds the adjacent speech (non-silence) segment in the consolidated list.
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
  let nextIdx = currentConsolidatedIdx + step;

  while (nextIdx >= 0 && nextIdx < audioSegments.length) {
    const nextSegment = audioSegments[nextIdx];
    if (!nextSegment.isSilenceBundle && nextSegment.playbackAudioUri) {
      return {
        id: nextSegment.id,
        uri: nextSegment.playbackAudioUri,
        index: nextIdx,
      };
    }
    nextIdx += step;
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
    let nextIdx = currentIdx + 1; // Older segments have higher indexes

    while (nextIdx < rawAudioSegments.length) {
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
      nextIdx++;
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
    let nextIdx = currentIdx - 1; // Newer segments have lower indexes

    while (nextIdx >= 0) {
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
      nextIdx--;
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
