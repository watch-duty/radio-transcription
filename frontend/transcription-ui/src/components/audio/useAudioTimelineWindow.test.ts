// @vitest-environment jsdom
import { afterEach, describe, expect, it } from 'vitest';

import { act, cleanup, renderHook } from '@testing-library/react';
import { AudioClassification, type AudioSegment } from '@transcription/common';

import { DEFAULT_AUDIO_WINDOW_DURATION_MS } from '../../utils/timeUtils';
import { useAudioTimelineWindow } from './useAudioTimelineWindow';

const iso = (clock: string) => new Date(`2026-04-20T${clock}Z`).toISOString();
const ms = (clock: string) => new Date(`2026-04-20T${clock}Z`).getTime();

const seg = (id: string, start: string, end: string): AudioSegment => ({
  id,
  feedId: 'feed1',
  classification: AudioClassification.SPEECH,
  startTimestamp: iso(start),
  endTimestamp: iso(end),
  missingPriorContext: false,
  missingPostContext: false,
  sourceAudioUris: [],
  createdAt: iso(start),
  annotations: [],
});

const base = {
  audioSegments: [] as AudioSegment[],
  currentlyPlayingSegmentId: null as string | null,
  highlightedSegmentId: null as string | null,
  // Anchor the 24h overview at the segments' live edge so its range contains
  // them (otherwise the live "now" sits far from these fixed test timestamps).
  overviewAnchorMs: ms('10:00:05') as number | null,
};

afterEach(cleanup);

describe('useAudioTimelineWindow', () => {
  it('defaults to the standard window duration', () => {
    const { result } = renderHook(() => useAudioTimelineWindow(base));
    expect(result.current.windowDurationMs).toBe(
      DEFAULT_AUDIO_WINDOW_DURATION_MS
    );
  });

  it('honors a custom window duration', () => {
    const custom = 5 * 60 * 1000;
    const { result } = renderHook(() =>
      useAudioTimelineWindow({ ...base, windowDurationMs: custom })
    );
    expect(result.current.windowDurationMs).toBe(custom);
  });

  it('follows the live edge as the head segment changes', () => {
    const s1 = seg('1', '10:00:00', '10:00:05');
    const { result, rerender } = renderHook(
      (props: Parameters<typeof useAudioTimelineWindow>[0]) =>
        useAudioTimelineWindow(props),
      { initialProps: { ...base, audioSegments: [s1] } }
    );
    expect(result.current.windowEndTime).toBe(ms('10:00:05'));

    const s2 = seg('2', '11:00:00', '11:00:05');
    rerender({ ...base, audioSegments: [s2, s1] });
    expect(result.current.windowEndTime).toBe(ms('11:00:05'));
  });

  it('recenters the window on the playing segment when it leaves the window', () => {
    const recent = seg('recent', '10:00:00', '10:00:05');
    const old = seg('old', '08:00:00', '08:00:05');
    const { result, rerender } = renderHook(
      (props: Parameters<typeof useAudioTimelineWindow>[0]) =>
        useAudioTimelineWindow(props),
      { initialProps: { ...base, audioSegments: [recent, old] } }
    );
    const liveEnd = result.current.windowEndTime;

    rerender({
      ...base,
      audioSegments: [recent, old],
      currentlyPlayingSegmentId: 'old',
    });
    expect(result.current.windowEndTime).toBeLessThan(liveEnd!);
  });

  it('scrubToCenter moves the window and marks it scrubbed', () => {
    const a = seg('a', '10:00:00', '10:00:05');
    const b = seg('b', '08:00:00', '08:00:05');
    const { result } = renderHook(
      (props: Parameters<typeof useAudioTimelineWindow>[0]) =>
        useAudioTimelineWindow(props),
      { initialProps: { ...base, audioSegments: [a, b] } }
    );

    act(() => {
      result.current.scrubToCenter(ms('08:00:02'));
    });

    expect(result.current.isScrubbed).toBe(true);
    expect(result.current.windowEndTime).toBeLessThan(ms('10:00:05'));
  });

  it('does not recenter on playback while scrubbed', () => {
    const a = seg('a', '10:00:00', '10:00:05');
    const b = seg('b', '08:00:00', '08:00:05');
    const { result, rerender } = renderHook(
      (props: Parameters<typeof useAudioTimelineWindow>[0]) =>
        useAudioTimelineWindow(props),
      { initialProps: { ...base, audioSegments: [a, b] } }
    );
    act(() => {
      result.current.scrubToCenter(ms('08:00:02'));
    });
    const scrubbedEnd = result.current.windowEndTime;

    rerender({
      ...base,
      audioSegments: [a, b],
      currentlyPlayingSegmentId: 'a',
    });
    expect(result.current.windowEndTime).toBe(scrubbedEnd);
  });

  it('does not exit scrub when the highlight follows the playing clip', () => {
    const a = seg('a', '10:00:00', '10:00:05');
    const b = seg('b', '08:00:00', '08:00:05');
    const { result, rerender } = renderHook(
      (props: Parameters<typeof useAudioTimelineWindow>[0]) =>
        useAudioTimelineWindow(props),
      { initialProps: { ...base, audioSegments: [a, b] } }
    );
    act(() => {
      result.current.scrubToCenter(ms('08:00:02'));
    });
    const scrubbedEnd = result.current.windowEndTime;

    // Playback advancing sets both the playing and highlighted ids to the same
    // clip; that must not yank the scrubbed window.
    rerender({
      ...base,
      audioSegments: [a, b],
      currentlyPlayingSegmentId: 'a',
      highlightedSegmentId: 'a',
    });
    expect(result.current.windowEndTime).toBe(scrubbedEnd);
    expect(result.current.isScrubbed).toBe(true);
  });

  it('recenters on an explicitly highlighted (non-playing) clip while scrubbed', () => {
    const a = seg('a', '10:00:00', '10:00:05');
    const b = seg('b', '08:00:00', '08:00:05');
    const { result, rerender } = renderHook(
      (props: Parameters<typeof useAudioTimelineWindow>[0]) =>
        useAudioTimelineWindow(props),
      { initialProps: { ...base, audioSegments: [a, b] } }
    );
    act(() => {
      result.current.scrubToCenter(ms('08:00:02'));
    });
    expect(result.current.isScrubbed).toBe(true);

    // Clicking a clip that isn't playing is an explicit pick: recenter to it.
    rerender({
      ...base,
      audioSegments: [a, b],
      highlightedSegmentId: 'a',
    });
    expect(result.current.windowEndTime).toBe(ms('10:00:05'));
    expect(result.current.isScrubbed).toBe(false);
  });

  it('keeps the scrubbed window when the list blanks and reloads (scrub re-anchor)', () => {
    const a = seg('a', '10:00:00', '10:00:05');
    const b = seg('b', '08:00:00', '08:00:05');
    const { result, rerender } = renderHook(
      (props: Parameters<typeof useAudioTimelineWindow>[0]) =>
        useAudioTimelineWindow(props),
      { initialProps: { ...base, audioSegments: [a, b] } }
    );
    act(() => {
      result.current.scrubToCenter(ms('08:00:02'));
    });
    const scrubbedEnd = result.current.windowEndTime;
    expect(result.current.isScrubbed).toBe(true);

    // Scrubbing re-anchors the segment query: the list blanks mid-refetch, then
    // reloads with the newly anchored page. Neither must yank the window.
    rerender({ ...base, audioSegments: [] });
    const reanchored = seg('c', '09:00:00', '09:00:05');
    rerender({ ...base, audioSegments: [reanchored] });

    expect(result.current.windowEndTime).toBe(scrubbedEnd);
    expect(result.current.isScrubbed).toBe(true);
  });

  it('jumpToLive returns to the live edge', () => {
    const a = seg('a', '10:00:00', '10:00:05');
    const b = seg('b', '08:00:00', '08:00:05');
    const { result } = renderHook(
      (props: Parameters<typeof useAudioTimelineWindow>[0]) =>
        useAudioTimelineWindow(props),
      { initialProps: { ...base, audioSegments: [a, b] } }
    );
    act(() => {
      result.current.scrubToCenter(ms('08:00:02'));
    });
    expect(result.current.isScrubbed).toBe(true);

    act(() => {
      result.current.jumpToLive();
    });
    expect(result.current.windowEndTime).toBeNull();
    expect(result.current.isScrubbed).toBe(false);
  });
});
