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

  it('scrubToCenter moves the window, marks it scrubbed, and reports the edge segment', () => {
    const a = seg('a', '10:00:00', '10:00:05');
    const b = seg('b', '08:00:00', '08:00:05');
    const { result } = renderHook(
      (props: Parameters<typeof useAudioTimelineWindow>[0]) =>
        useAudioTimelineWindow(props),
      { initialProps: { ...base, audioSegments: [a, b] } }
    );

    let repId: string | null = null;
    act(() => {
      repId = result.current.scrubToCenter(ms('08:00:02'));
    });

    expect(repId).toBe('b');
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
