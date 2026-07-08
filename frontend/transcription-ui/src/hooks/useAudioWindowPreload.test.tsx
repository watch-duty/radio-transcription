// @vitest-environment jsdom
import { afterEach, describe, expect, it, vi } from 'vitest';

import { renderHook } from '@testing-library/react';
import { AudioClassification, type AudioSegment } from '@transcription/common';

import { useAudioWindowPreload } from './useAudioWindowPreload';

const WINDOW_MS = 24 * 60 * 60 * 1000;
const HALF = WINDOW_MS / 2;

function seg(startMs: number): AudioSegment {
  const iso = new Date(startMs).toISOString();
  return {
    id: `s-${startMs}`,
    feedId: 'feed-1',
    classification: AudioClassification.SPEECH,
    startTimestamp: iso,
    endTimestamp: iso,
    missingPriorContext: false,
    missingPostContext: false,
    sourceAudioUris: [],
    createdAt: iso,
    annotations: [],
  };
}

type Options = Parameters<typeof useAudioWindowPreload>[0];

function baseOptions(overrides: Partial<Options>): Options {
  return {
    enabled: true,
    windowMs: WINDOW_MS,
    anchorTimestamp: null,
    segments: [],
    hasOlder: true,
    hasNewer: true,
    isFetchingOlder: false,
    isFetchingNewer: false,
    fetchOlder: vi.fn(),
    fetchNewer: vi.fn(),
    resetKey: 'feed-1||all',
    ...overrides,
  };
}

function setup(overrides: Partial<Options>) {
  const opts = baseOptions(overrides);
  renderHook(() => useAudioWindowPreload(opts));
  return { fetchOlder: opts.fetchOlder, fetchNewer: opts.fetchNewer };
}

describe('useAudioWindowPreload', () => {
  afterEach(() => vi.restoreAllMocks());

  const ANCHOR = new Date('2026-06-20T12:00:00.000Z');
  const T = ANCHOR.getTime();

  it('pages older when the live window is not yet covered', () => {
    // Newest-first; oldest loaded is only 1h back, window wants 24h.
    const { fetchOlder, fetchNewer } = setup({
      segments: [seg(Date.now()), seg(Date.now() - 60 * 60 * 1000)],
    });
    expect(fetchOlder).toHaveBeenCalledTimes(1);
    expect(fetchNewer).not.toHaveBeenCalled();
  });

  it('pages older relative to the live edge, not wall-clock now, on a stale feed', () => {
    // Feed went quiet: newest clip is well behind Date.now(). Anchoring on the
    // live edge (not now) leaves the 24h span uncovered, so it pages older.
    const staleEdge = new Date('2026-06-20T12:00:00.000Z').getTime();
    const { fetchOlder } = setup({
      segments: [seg(staleEdge), seg(staleEdge - 60 * 60 * 1000)],
    });
    expect(fetchOlder).toHaveBeenCalledTimes(1);
  });

  it('does not page when the live window is already covered', () => {
    const { fetchOlder } = setup({
      segments: [seg(Date.now()), seg(Date.now() - 2 * WINDOW_MS)],
    });
    expect(fetchOlder).not.toHaveBeenCalled();
  });

  it('date mode pages older toward T - window/2 first', () => {
    const { fetchOlder, fetchNewer } = setup({
      anchorTimestamp: ANCHOR,
      segments: [seg(T), seg(T - 60 * 60 * 1000)],
    });
    expect(fetchOlder).toHaveBeenCalledTimes(1);
    expect(fetchNewer).not.toHaveBeenCalled();
  });

  it('date mode pages newer once the older side is covered', () => {
    // Oldest already past T-12h, newest only at T (window wants up to T+12h).
    const { fetchOlder, fetchNewer } = setup({
      anchorTimestamp: ANCHOR,
      segments: [seg(T), seg(T - HALF - 1000)],
    });
    expect(fetchOlder).not.toHaveBeenCalled();
    expect(fetchNewer).toHaveBeenCalledTimes(1);
  });

  it('does not page while disabled or with no window', () => {
    const a = setup({ enabled: false, segments: [seg(Date.now())] });
    expect(a.fetchOlder).not.toHaveBeenCalled();
    const b = setup({ windowMs: undefined, segments: [seg(Date.now())] });
    expect(b.fetchOlder).not.toHaveBeenCalled();
  });

  it('does not page older while an older fetch is in flight', () => {
    const { fetchOlder } = setup({
      isFetchingOlder: true,
      segments: [seg(Date.now()), seg(Date.now() - 60 * 60 * 1000)],
    });
    expect(fetchOlder).not.toHaveBeenCalled();
  });

  it('caps the older preload and warns once when the window stays uncovered', () => {
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
    const fetchOlder = vi.fn();
    // Window never covered: oldest stays only 1h back across every render.
    const uncovered = () => [seg(Date.now()), seg(Date.now() - 60 * 60 * 1000)];

    const { rerender } = renderHook(
      (props: Options) => useAudioWindowPreload(props),
      { initialProps: baseOptions({ fetchOlder, segments: uncovered() }) }
    );

    // Fresh segments array each render so the effect re-runs and the page
    // counter advances; stop once past the cap.
    for (let i = 0; i < 35; i++) {
      rerender(baseOptions({ fetchOlder, segments: uncovered() }));
    }

    expect(fetchOlder.mock.calls.length).toBe(30);
    expect(warn).toHaveBeenCalledTimes(1);
    expect(warn.mock.calls[0][0]).toContain('30-page cap');
  });

  it('warns once per side when date mode caps both directions', () => {
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
    const fetchOlder = vi.fn();
    const fetchNewer = vi.fn();
    // Centered window uncovered on both sides: oldest inside T-12h, newest at T.
    const uncovered = () => [seg(T), seg(T - 60 * 60 * 1000)];

    const { rerender } = renderHook(
      (props: Options) => useAudioWindowPreload(props),
      {
        initialProps: baseOptions({
          anchorTimestamp: ANCHOR,
          fetchOlder,
          fetchNewer,
          segments: uncovered(),
        }),
      }
    );
    // Older caps first (paged before newer each render), then newer caps.
    for (let i = 0; i < 65; i++) {
      rerender(
        baseOptions({
          anchorTimestamp: ANCHOR,
          fetchOlder,
          fetchNewer,
          segments: uncovered(),
        })
      );
    }

    const sides = warn.mock.calls.map((c) => c[0]);
    expect(sides.some((m) => m.includes('paging older'))).toBe(true);
    expect(sides.some((m) => m.includes('paging newer'))).toBe(true);
    expect(warn).toHaveBeenCalledTimes(2);
  });

  it('resets the page counters on a resetKey change so a new query preloads again', () => {
    vi.spyOn(console, 'warn').mockImplementation(() => {});
    const fetchOlder = vi.fn();
    const uncovered = () => [seg(Date.now()), seg(Date.now() - 60 * 60 * 1000)];

    const { rerender } = renderHook(
      (props: Options) => useAudioWindowPreload(props),
      {
        initialProps: baseOptions({
          fetchOlder,
          segments: uncovered(),
          resetKey: 'feed-1||all',
        }),
      }
    );
    for (let i = 0; i < 35; i++) {
      rerender(
        baseOptions({
          fetchOlder,
          segments: uncovered(),
          resetKey: 'feed-1||all',
        })
      );
    }
    expect(fetchOlder.mock.calls.length).toBe(30);

    // Query changes: counters must reset, so the fresh query pages again.
    rerender(
      baseOptions({
        fetchOlder,
        segments: uncovered(),
        resetKey: 'feed-2||all',
      })
    );
    expect(fetchOlder.mock.calls.length).toBe(31);
  });
});
