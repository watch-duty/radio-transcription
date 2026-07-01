// @vitest-environment jsdom
import { describe, expect, it } from 'vitest';

import { act, renderHook } from '@testing-library/react';
import { type AudioSegment } from '@transcription/common';

import { MAX_WINDOW_DURATION_MS } from '../utils/timeUtils';
import {
  isSegmentOutsideWindow,
  useAudioTimelineWindow,
} from './useAudioTimelineWindow';

describe('isSegmentOutsideWindow', () => {
  // window covers (500, 1000]
  const END = 1000;
  const DUR = 500;

  it('is false when the segment is fully inside the window', () => {
    expect(isSegmentOutsideWindow(600, 900, END, DUR)).toBe(false);
  });

  it('is false at the exact window edges', () => {
    expect(isSegmentOutsideWindow(500, 1000, END, DUR)).toBe(false);
  });

  it('is true when the segment starts before the window', () => {
    expect(isSegmentOutsideWindow(400, 900, END, DUR)).toBe(true);
  });

  it('is true when the segment ends after the window', () => {
    expect(isSegmentOutsideWindow(600, 1100, END, DUR)).toBe(true);
  });
});

// The hook only reads id/startTimestamp/endTimestamp.
function seg(id: string, start: string, end: string): AudioSegment {
  return {
    id,
    startTimestamp: new Date(start).toISOString(),
    endTimestamp: new Date(end).toISOString(),
  } as unknown as AudioSegment;
}

const NEWEST = seg('a', '2026-04-20T09:00:00Z', '2026-04-20T09:00:05Z');
const OLDER = seg('b', '2026-04-20T08:00:00Z', '2026-04-20T08:00:05Z');

interface SetupProps {
  audioSegments: AudioSegment[];
  currentlyPlayingSegmentId?: string | null;
  highlightedSegmentId?: string | null;
  resetKey?: string;
}

function setup(initialProps: SetupProps) {
  return renderHook(
    (props: SetupProps) =>
      useAudioTimelineWindow({
        audioSegments: props.audioSegments,
        currentlyPlayingSegmentId: props.currentlyPlayingSegmentId ?? null,
        highlightedSegmentId: props.highlightedSegmentId ?? null,
        resetKey: props.resetKey ?? 'feed-1',
      }),
    { initialProps }
  );
}

describe('useAudioTimelineWindow', () => {
  it('defaults to a latest, 15-minute window', () => {
    const { result } = setup({ audioSegments: [] });
    expect(result.current.windowDurationMs).toBe(MAX_WINDOW_DURATION_MS);
    expect(result.current.windowEndTime).toBeNull();
    expect(result.current.isLatestTimeWindow).toBe(true);
  });

  it('follows the live edge as the head segment advances while pinned', () => {
    const { result, rerender } = setup({ audioSegments: [NEWEST] });
    expect(result.current.windowEndTime).toBe(
      new Date(NEWEST.endTimestamp).getTime()
    );

    const newer = seg('c', '2026-04-20T09:10:00Z', '2026-04-20T09:10:05Z');
    rerender({ audioSegments: [newer, NEWEST] });

    expect(result.current.windowEndTime).toBe(
      new Date(newer.endTimestamp).getTime()
    );
    expect(result.current.isLatestTimeWindow).toBe(true);
  });

  it('recenters and leaves the latest window when a highlighted clip is outside it', () => {
    const { result, rerender } = setup({ audioSegments: [NEWEST, OLDER] });
    expect(result.current.isLatestTimeWindow).toBe(true);

    rerender({ audioSegments: [NEWEST, OLDER], highlightedSegmentId: 'b' });

    expect(result.current.windowEndTime).toBeLessThan(
      new Date(NEWEST.endTimestamp).getTime()
    );
    expect(result.current.isLatestTimeWindow).toBe(false);
  });

  it('does not follow the live edge once viewing back', () => {
    const { result, rerender } = setup({ audioSegments: [NEWEST, OLDER] });
    rerender({ audioSegments: [NEWEST, OLDER], highlightedSegmentId: 'b' });
    const pastEnd = result.current.windowEndTime;
    expect(result.current.isLatestTimeWindow).toBe(false);

    const newer = seg('c', '2026-04-20T09:10:00Z', '2026-04-20T09:10:05Z');
    rerender({
      audioSegments: [newer, NEWEST, OLDER],
      highlightedSegmentId: 'b',
    });

    expect(result.current.windowEndTime).toBe(pastEnd);
    expect(result.current.isLatestTimeWindow).toBe(false);
  });

  it('jumpToLive returns to the latest window', () => {
    const { result, rerender } = setup({ audioSegments: [NEWEST, OLDER] });
    rerender({ audioSegments: [NEWEST, OLDER], highlightedSegmentId: 'b' });
    expect(result.current.isLatestTimeWindow).toBe(false);

    act(() => {
      result.current.jumpToLive();
    });

    expect(result.current.windowEndTime).toBeNull();
    expect(result.current.isLatestTimeWindow).toBe(true);
  });

  it('follows the live edge when the head segment extends (same id, later end)', () => {
    const bundle = seg('a', '2026-04-20T09:00:00Z', '2026-04-20T09:00:05Z');
    const { result, rerender } = setup({ audioSegments: [bundle] });
    expect(result.current.windowEndTime).toBe(
      new Date(bundle.endTimestamp).getTime()
    );

    const extended = seg('a', '2026-04-20T09:00:00Z', '2026-04-20T09:00:30Z');
    rerender({ audioSegments: [extended] });

    expect(result.current.windowEndTime).toBe(
      new Date(extended.endTimestamp).getTime()
    );
  });

  it('recenters on the playing segment when nothing is highlighted', () => {
    const { result, rerender } = setup({ audioSegments: [NEWEST, OLDER] });
    expect(result.current.isLatestTimeWindow).toBe(true);

    rerender({
      audioSegments: [NEWEST, OLDER],
      currentlyPlayingSegmentId: 'b',
    });

    expect(result.current.windowEndTime).toBeLessThan(
      new Date(NEWEST.endTimestamp).getTime()
    );
    expect(result.current.isLatestTimeWindow).toBe(false);
  });

  it('resets to the live edge when the resetKey changes (feed / date / filter switch)', () => {
    const { result, rerender } = setup({ audioSegments: [NEWEST, OLDER] });
    rerender({ audioSegments: [NEWEST, OLDER], highlightedSegmentId: 'b' });
    expect(result.current.isLatestTimeWindow).toBe(false);

    // A different feed replaces the list wholesale.
    rerender({ audioSegments: [NEWEST, OLDER], resetKey: 'feed-2' });

    expect(result.current.windowEndTime).toBeNull();
    expect(result.current.isLatestTimeWindow).toBe(true);
  });
});
