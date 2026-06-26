import { describe, expect, it } from 'vitest';

import { type AudioSegment } from '@transcription/common';

import { computePlayhead } from './computePlayhead';

// The helper only reads id/startTimestamp/endTimestamp.
function seg(id: string, startMs: number, endMs: number): AudioSegment {
  return {
    id,
    startTimestamp: new Date(startMs).toISOString(),
    endTimestamp: new Date(endMs).toISOString(),
  } as unknown as AudioSegment;
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
    expect(p.left).toBe(80);
    expect(p.label).toBe('Listening');
  });

  it('follows the playback position while playing', () => {
    const p = computePlayhead({
      ...base,
      state: 'playing',
      currentlyPlayingSegmentId: 'p',
      localCurrentTimeSeconds: 0.1, // 400ms start + 100ms = 500 → 50%
    });
    expect(p.left).toBe(50);
    expect(p.label).toMatch(/^\d{2}:\d{2}:\d{2}$/);
  });

  it('freezes at the playback position when paused mid-clip', () => {
    const p = computePlayhead({
      ...base,
      state: 'paused',
      currentlyPlayingSegmentId: 'p',
      localCurrentTimeSeconds: 0.1,
    });
    expect(p.left).toBe(50);
    expect(p.label).toMatch(/^\d{2}:\d{2}:\d{2}$/);
  });

  it('falls back to the live edge when paused straight from listening', () => {
    const p = computePlayhead({
      ...base,
      state: 'paused',
      currentlyPlayingSegmentId: null,
    });
    expect(p.left).toBe(80);
  });

  it('hides when there are no segments', () => {
    const p = computePlayhead({
      ...base,
      audioSegments: [],
      state: 'listening',
    });
    expect(p.show).toBe(false);
  });

  it('hides when the marker is outside the visible window', () => {
    // Live edge at 2000 but the window only covers [0, 1000) → 200%.
    const p = computePlayhead({
      ...base,
      audioSegments: [seg('future', 1900, 2000)],
      state: 'listening',
    });
    expect(p.show).toBe(false);
  });
});
