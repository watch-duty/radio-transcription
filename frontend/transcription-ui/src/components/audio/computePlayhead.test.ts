import { describe, expect, it } from 'vitest';

import { type RenderableAudioSegment } from '../../hooks/useConsolidatedAudioSegments';
import { computePlayhead } from './computePlayhead';

function seg(
  id: string,
  startMs: number,
  endMs: number,
  extra: Partial<RenderableAudioSegment> = {}
): RenderableAudioSegment {
  return {
    id,
    startTimestamp: new Date(startMs).toISOString(),
    endTimestamp: new Date(endMs).toISOString(),
    ...extra,
  } as unknown as RenderableAudioSegment;
}

// Window covers [0, 1000), so left% === time / 10.
const START_TIME = 0;
const WINDOW_MS = 1000;

// Newest first; live edge (newest end) is 800 → 80%.
const SEGMENTS = [seg('newest', 700, 800), seg('p', 400, 600)];

const base = {
  audioSegments: SEGMENTS,
  startTime: START_TIME,
  windowDurationMs: WINDOW_MS,
  localCurrentTimeSeconds: 0,
  currentlyPlayingSegmentId: null,
};

describe('computePlayhead', () => {
  it('rests at the live edge while listening', () => {
    const p = computePlayhead({ ...base, state: 'listening' });
    expect(p.show).toBe(true);
    expect(p.leftOffsetPct).toBe(80);
    expect(p.label).toBe('Listening');
  });

  it('follows the playback position while playing', () => {
    const p = computePlayhead({
      ...base,
      state: 'playing',
      currentlyPlayingSegmentId: 'p',
      localCurrentTimeSeconds: 0.1, // 400ms start + 100ms = 500 → 50%
    });
    expect(p.leftOffsetPct).toBe(50);
    expect(p.label).toMatch(/^\d{2}:\d{2}:\d{2}$/);
  });

  it('resolves a raw id playing inside a silence bundle to its consolidated entry', () => {
    const bundle = seg('bundle', 400, 600, {
      isSilenceBundle: true,
      bundledSegmentIds: ['r1', 'r2'],
    });
    const p = computePlayhead({
      ...base,
      audioSegments: [seg('newest', 700, 800), bundle],
      state: 'playing',
      currentlyPlayingSegmentId: 'r2', // a raw member, not the bundle's own id
      localCurrentTimeSeconds: 0.1, // positioned within the bundle, not at live edge
    });
    expect(p.leftOffsetPct).toBe(50);
  });

  it('freezes at the playback position when paused mid-clip', () => {
    const p = computePlayhead({
      ...base,
      state: 'paused',
      currentlyPlayingSegmentId: 'p',
      localCurrentTimeSeconds: 0.1,
    });
    expect(p.leftOffsetPct).toBe(50);
    expect(p.label).toMatch(/^\d{2}:\d{2}:\d{2}$/);
  });

  it('clamps to the clip end when playback overshoots', () => {
    const p = computePlayhead({
      ...base,
      state: 'playing',
      currentlyPlayingSegmentId: 'p', // ends at 600 → 60%
      localCurrentTimeSeconds: 10, // 400ms + 10s overshoots well past the end
    });
    expect(p.leftOffsetPct).toBe(60);
  });

  it('falls back to the live edge when paused straight from listening', () => {
    const p = computePlayhead({
      ...base,
      state: 'paused',
      currentlyPlayingSegmentId: null,
    });
    expect(p.leftOffsetPct).toBe(80);
  });

  it('hides when there are no segments', () => {
    const p = computePlayhead({
      ...base,
      audioSegments: [],
      state: 'listening',
    });
    expect(p.show).toBe(false);
  });

  it('hides when the marker is after the visible window', () => {
    // Live edge at 2000 but the window only covers [0, 1000) → 200%.
    const p = computePlayhead({
      ...base,
      audioSegments: [seg('future', 1900, 2000)],
      state: 'listening',
    });
    expect(p.show).toBe(false);
  });

  it('hides when the marker is before the visible window', () => {
    // Window covers [1000, 2000); the only segment ends at 500 → negative left.
    const p = computePlayhead({
      ...base,
      audioSegments: [seg('past', 400, 500)],
      startTime: 1000,
      state: 'listening',
    });
    expect(p.show).toBe(false);
  });
});
