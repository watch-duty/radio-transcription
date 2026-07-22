import { describe, expect, it } from 'vitest';

import { type RenderableAudioSegment } from '../../hooks/useConsolidatedAudioSegments';
import { deriveTimelineState } from './deriveTimelineState';

// deriveTimelineState only reads audioSegments[0] and its id/bundle fields.
const seg = (
  id: string,
  extra: Partial<RenderableAudioSegment> = {}
): RenderableAudioSegment => ({ id, ...extra }) as RenderableAudioSegment;

const base = {
  isAudioPlaying: false,
  playbackIntent: 'playing' as const,
  isLatestTimeWindow: true,
  hasDateFilter: false,
  currentlyPlayingSegmentId: null,
  audioSegments: [seg('head')],
  hasNewerAudioSegments: false,
};

describe('deriveTimelineState', () => {
  it('idle, following live, intent playing → listening + viewing live', () => {
    const { playbackState, isViewingLive } = deriveTimelineState(base);
    expect(playbackState).toBe('listening');
    expect(isViewingLive).toBe(true);
  });

  it('idle but a date filter is active → parked (paused), not viewing live', () => {
    const s = deriveTimelineState({ ...base, hasDateFilter: true });
    expect(s.playbackState).toBe('paused');
    expect(s.isViewingLive).toBe(false);
  });

  it('idle but window is scrubbed back → parked (paused), not viewing live', () => {
    const s = deriveTimelineState({ ...base, isLatestTimeWindow: false });
    expect(s.playbackState).toBe('paused');
    expect(s.isViewingLive).toBe(false);
  });

  it('idle with intent paused → paused, not viewing live', () => {
    const s = deriveTimelineState({ ...base, playbackIntent: 'paused' });
    expect(s.playbackState).toBe('paused');
    expect(s.isViewingLive).toBe(false);
  });

  it('listening but newer audio exists → still not viewing live', () => {
    const s = deriveTimelineState({ ...base, hasNewerAudioSegments: true });
    expect(s.playbackState).toBe('listening');
    expect(s.isViewingLive).toBe(false);
  });

  it('playing the head clip → playing + viewing live', () => {
    const s = deriveTimelineState({
      ...base,
      isAudioPlaying: true,
      currentlyPlayingSegmentId: 'head',
    });
    expect(s.playbackState).toBe('playing');
    expect(s.isViewingLive).toBe(true);
  });

  it('playing a past clip (not the head) → playing, jump-to-live enabled', () => {
    const s = deriveTimelineState({
      ...base,
      isAudioPlaying: true,
      currentlyPlayingSegmentId: 'older',
    });
    expect(s.playbackState).toBe('playing');
    expect(s.isViewingLive).toBe(false);
  });

  it('playing a raw id inside the head non-speech bundle → viewing live', () => {
    const s = deriveTimelineState({
      ...base,
      isAudioPlaying: true,
      currentlyPlayingSegmentId: 'raw-2',
      audioSegments: [
        seg('bundle', {
          isNonSpeechBundle: true,
          bundledSegmentIds: ['raw-1', 'raw-2'],
        }),
      ],
    });
    expect(s.playbackState).toBe('playing');
    expect(s.isViewingLive).toBe(true);
  });
});
