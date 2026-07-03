import { type AudioSegment } from '@transcription/common';

import { type RenderableAudioSegment } from '../../hooks/useConsolidatedAudioSegments';
import { type PlaybackState, isWithinSegment } from '../../utils/playbackUtils';
import { formatClockTime } from '../../utils/timeUtils';

interface Playhead {
  // False when the marker falls outside the visible window (or there's no audio).
  show: boolean;
  state: PlaybackState;
  // Horizontal position within the timeline, as a percentage (0–100).
  leftOffsetPct: number;
  label: string;
}

interface ComputePlayheadParams {
  audioSegments: RenderableAudioSegment[];
  rawAudioSegments: AudioSegment[];
  currentlyPlayingSegmentId: string | null;
  state: PlaybackState;
  localCurrentTimeSeconds: number;
  startTime: number;
  windowDurationMs: number;
}

// Listening rests at the live edge; playing follows the clip; paused freezes
// there (or at the live edge if paused straight from listening).
export function computePlayhead({
  audioSegments,
  rawAudioSegments,
  currentlyPlayingSegmentId,
  state,
  localCurrentTimeSeconds,
  startTime,
  windowDurationMs,
}: ComputePlayheadParams): Playhead {
  // Match via isWithinSegment so a raw id playing inside a non-speech bundle
  // resolves to its consolidated entry rather than falling back to the live edge.
  const playingSegment = currentlyPlayingSegmentId
    ? audioSegments.find((s) => isWithinSegment(s, currentlyPlayingSegmentId))
    : undefined;
  const firstAudioSegment = audioSegments[0];
  const liveEdge = firstAudioSegment
    ? new Date(firstAudioSegment.endTimestamp).getTime()
    : null;

  // localCurrentTimeSeconds is relative to the raw clip being played, so anchor
  // on that clip — not the bundle it may be consolidated into.
  const anchorSegment =
    (currentlyPlayingSegmentId
      ? rawAudioSegments.find((s) => s.id === currentlyPlayingSegmentId)
      : undefined) ?? playingSegment;

  const playbackPosition =
    playingSegment && anchorSegment
      ? Math.min(
          new Date(anchorSegment.startTimestamp).getTime() +
            localCurrentTimeSeconds * 1000,
          new Date(anchorSegment.endTimestamp).getTime()
        )
      : null;

  const time =
    state === 'listening' ? liveEdge : (playbackPosition ?? liveEdge);

  const leftOffsetPct =
    time !== null ? ((time - startTime) / windowDurationMs) * 100 : null;
  const show =
    audioSegments.length > 0 &&
    leftOffsetPct !== null &&
    leftOffsetPct >= 0 &&
    leftOffsetPct <= 100;

  const label =
    state === 'listening'
      ? 'Listening'
      : time !== null
        ? formatClockTime(time, true)
        : '';

  return { show, state, leftOffsetPct: leftOffsetPct ?? 0, label };
}
