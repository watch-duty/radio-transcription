import { type RenderableAudioSegment } from '../../hooks/useConsolidatedAudioSegments';
import { type PlaybackState, isWithinSegment } from '../../utils/playbackUtils';

interface DeriveTimelineStateParams {
  isAudioPlaying: boolean;
  playbackIntent: 'playing' | 'paused';
  // Window's right edge is at the newest loaded audio (from useAudioTimelineWindow).
  isLatestTimeWindow: boolean;
  // A date/time filter is active (the window is parked at a chosen time).
  hasDateFilter: boolean;
  currentlyPlayingSegmentId: string | null;
  // Consolidated (rendered) segments, newest-first.
  audioSegments: RenderableAudioSegment[];
  // The query knows of newer audio not yet in the window.
  hasNewerAudioSegments: boolean;
}

interface TimelineState {
  // Playhead lozenge color/label.
  playbackState: PlaybackState;
  // "Jump to live" has nothing to do — fully live in every sense.
  isViewingLive: boolean;
}

// Single source of truth for the two view-level signals the timeline derives
// from window + playback state. Truth table:
//   playing (any window)                 → 'playing',   jump enabled unless at edge
//   idle, following live, intent playing → 'listening', jump disabled
//   idle, parked (date filter / scrubbed back) or intent paused → 'paused', jump enabled
// "Listening" (green) is reserved for idle AND following live; parking away from
// live reads as paused (amber) even though nothing was hand-paused.
export function deriveTimelineState({
  isAudioPlaying,
  playbackIntent,
  isLatestTimeWindow,
  hasDateFilter,
  currentlyPlayingSegmentId,
  audioSegments,
  hasNewerAudioSegments,
}: DeriveTimelineStateParams): TimelineState {
  const isFollowingLive = isLatestTimeWindow && !hasDateFilter;

  const playbackState: PlaybackState = isAudioPlaying
    ? 'playing'
    : playbackIntent === 'paused' || !isFollowingLive
      ? 'paused'
      : 'listening';

  // Playback is at the live edge: idle-and-listening, or playing the newest clip.
  // Match by containment (like computePlayhead): the playing id is often a raw
  // segment inside the newest consolidated entry (a non-speech bundle), so a strict
  // id equality would miss it and leave the button wrongly enabled at the edge.
  const isPlaybackAtLiveEdge =
    playbackState === 'listening' ||
    (playbackState === 'playing' &&
      currentlyPlayingSegmentId !== null &&
      audioSegments.length > 0 &&
      isWithinSegment(audioSegments[0], currentlyPlayingSegmentId));

  const isViewingLive =
    !hasNewerAudioSegments &&
    isLatestTimeWindow &&
    !hasDateFilter &&
    isPlaybackAtLiveEdge;

  return { playbackState, isViewingLive };
}
