import { type RenderableAudioSegment } from '../../hooks/useConsolidatedAudioSegments';
import { type PlaybackState, isWithinSegment } from '../../utils/playbackUtils';
import { formatClockTime } from '../../utils/timeUtils';

interface Playhead {
  // False when the marker falls outside the visible window (or there's no audio).
  show: boolean;
  state: PlaybackState;
  // Horizontal position within the timeline, as a percentage (0–100).
  left: number;
  label: string;
}

interface ComputePlayheadParams {
  audioSegments: RenderableAudioSegment[];
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
  currentlyPlayingSegmentId,
  state,
  localCurrentTimeSeconds,
  startTime,
  windowDurationMs,
}: ComputePlayheadParams): Playhead {
  // Match via isWithinSegment so a raw id playing inside a silence bundle
  // resolves to its consolidated entry rather than falling back to the live edge.
  const playingSegment = currentlyPlayingSegmentId
    ? audioSegments.find((s) => isWithinSegment(s, currentlyPlayingSegmentId))
    : undefined;
  const firstAudioSegment = audioSegments[0];
  const liveEdge = firstAudioSegment
    ? new Date(firstAudioSegment.endTimestamp).getTime()
    : null;

  const playbackPosition = playingSegment
    ? Math.min(
        new Date(playingSegment.startTimestamp).getTime() +
          localCurrentTimeSeconds * 1000,
        new Date(playingSegment.endTimestamp).getTime()
      )
    : null;

  const time =
    state === 'listening' ? liveEdge : (playbackPosition ?? liveEdge);

  const left =
    time !== null ? ((time - startTime) / windowDurationMs) * 100 : null;
  const show =
    audioSegments.length > 0 && left !== null && left >= 0 && left <= 100;

  const label =
    state === 'listening'
      ? 'Listening'
      : time !== null
        ? formatClockTime(time, true)
        : '';

  return { show, state, left: left ?? 0, label };
}
