// @vitest-environment jsdom
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { act, renderHook } from '@testing-library/react';
import { type AudioSegment } from '@transcription/common';

import { useAudioPlayback } from './useAudioPlayback';
import { type RenderableAudioSegment } from './useConsolidatedAudioSegments';

const engineMock = vi.hoisted(() => ({
  contextCreated: 0,
  playersCreated: 0,
  resumeSpy: vi.fn(),
  stopSpy: vi.fn(),
  closeSpy: vi.fn(() => Promise.resolve()),
  lastSrc: null as string | null,
  lastCallbacks: null as {
    onPlay?: () => void;
    onPause?: () => void;
    onEnd?: () => void;
    onError?: () => void;
  } | null,
  playSpy: vi.fn(),
  pauseSpy: vi.fn(),
  unloadSpy: vi.fn(),
}));

vi.mock('../audio/WebAudioPlayer', () => ({
  createAudioContext: () => {
    engineMock.contextCreated += 1;
    return { close: engineMock.closeSpy };
  },
  WebAudioPlayer: class {
    constructor() {
      engineMock.playersCreated += 1;
    }
    resume() {
      engineMock.resumeSpy();
    }
    stop() {
      engineMock.stopSpy();
    }
    load(src: string, callbacks: NonNullable<typeof engineMock.lastCallbacks>) {
      engineMock.lastSrc = src;
      engineMock.lastCallbacks = callbacks;
      return {
        play: () => {
          engineMock.playSpy();
          callbacks.onPlay?.();
        },
        pause: () => {
          engineMock.pauseSpy();
          callbacks.onPause?.();
        },
        stop: () => {},
        getCurrentTime: () => 0,
        setCurrentTime: () => {},
        unload: () => engineMock.unloadSpy(),
        off: () => {},
      };
    }
  },
}));

vi.mock('../utils/audioUtils', () => ({
  getAudioUrl: (uri: string) => `resolved://${uri}`,
}));

function makeSegment(id: string): AudioSegment {
  return { id, playbackAudioUri: `${id}.m4a` } as AudioSegment;
}

function renderPlayback(segments: AudioSegment[] = []) {
  const onPlaySegment = vi.fn();
  const audioSegmentsRef = { current: segments as RenderableAudioSegment[] };
  const rawAudioSegmentsRef = { current: segments };
  const view = renderHook(() =>
    useAudioPlayback({
      audioSegmentsRef,
      rawAudioSegmentsRef,
      onPlaySegment,
    })
  );
  return { ...view, onPlaySegment, audioSegmentsRef, rawAudioSegmentsRef };
}

describe('useAudioPlayback', () => {
  beforeEach(() => {
    engineMock.contextCreated = 0;
    engineMock.playersCreated = 0;
    engineMock.lastSrc = null;
    engineMock.lastCallbacks = null;
    vi.clearAllMocks();
  });

  afterEach(() => {
    vi.clearAllMocks();
  });

  it('lazily builds the context and player on first togglePlay, then reuses them', () => {
    const { result } = renderPlayback([makeSegment('a'), makeSegment('b')]);

    expect(engineMock.contextCreated).toBe(0);
    expect(engineMock.playersCreated).toBe(0);

    act(() => result.current.togglePlay('b', 'b.m4a'));

    expect(engineMock.contextCreated).toBe(1);
    expect(engineMock.playersCreated).toBe(1);
    expect(engineMock.resumeSpy).toHaveBeenCalled();
    expect(engineMock.lastSrc).toBe('resolved://b.m4a');
    expect(result.current.isAudioPlaying).toBe(true);
    expect(result.current.currentlyPlayingSegmentId).toBe('b');
  });

  it('highlights and plays a new segment', () => {
    const { result, onPlaySegment } = renderPlayback([makeSegment('a')]);

    act(() => result.current.togglePlay('a', 'a.m4a'));

    expect(onPlaySegment).toHaveBeenCalledWith('a');
    expect(engineMock.playSpy).toHaveBeenCalledTimes(1);
  });

  it('pauses when toggling the same segment that is already playing', () => {
    const { result } = renderPlayback([makeSegment('a')]);

    act(() => result.current.togglePlay('a', 'a.m4a'));
    expect(result.current.isAudioPlaying).toBe(true);

    act(() => result.current.togglePlay('a', 'a.m4a'));

    expect(engineMock.pauseSpy).toHaveBeenCalled();
    expect(result.current.isAudioPlaying).toBe(false);
  });

  it('unloads the previous clip when switching segments', () => {
    const { result } = renderPlayback([makeSegment('a'), makeSegment('b')]);

    act(() => result.current.togglePlay('a', 'a.m4a'));
    act(() => result.current.togglePlay('b', 'b.m4a'));

    expect(engineMock.unloadSpy).toHaveBeenCalled();
    expect(result.current.currentlyPlayingSegmentId).toBe('b');
    expect(engineMock.lastSrc).toBe('resolved://b.m4a');
    // The new clip is loaded and played, not just unloaded.
    expect(engineMock.playSpy).toHaveBeenCalledTimes(2);
  });

  it('flags the ended segment for autoplay and clears the playing flag when it was last', () => {
    const { result } = renderPlayback([makeSegment('a')]);

    act(() => result.current.togglePlay('a', 'a.m4a'));
    act(() => engineMock.lastCallbacks?.onEnd?.());

    expect(result.current.playbackEndedForId).toBe('a');
    // 'a' is at index 0 (newest), so there is no newer segment to advance to.
    expect(result.current.isAudioPlaying).toBe(false);
    // The handle is released so AudioDisplay stops polling the finished clip.
    expect(result.current.currentAudioRef.current).toBeNull();
  });

  it('keeps playing on end when a newer segment exists', () => {
    // Newest-first: 'b' at index 0 is newer than 'a' at index 1.
    const { result } = renderPlayback([makeSegment('b'), makeSegment('a')]);

    act(() => result.current.togglePlay('a', 'a.m4a'));
    act(() => engineMock.lastCallbacks?.onEnd?.());

    // It should have transitioned synchronously to playing segment 'b'
    expect(result.current.currentlyPlayingSegmentId).toBe('b');
    expect(result.current.isAudioPlaying).toBe(true);
    expect(result.current.playbackEndedForId).toBeNull();
  });

  it('stop() halts the engine and resets playback identity', () => {
    const { result } = renderPlayback([makeSegment('a')]);

    act(() => result.current.togglePlay('a', 'a.m4a'));
    act(() => result.current.stop());

    expect(engineMock.stopSpy).toHaveBeenCalled();
    expect(result.current.currentlyPlayingSegmentId).toBeNull();
    expect(result.current.playbackEndedForId).toBeNull();
    expect(result.current.currentAudioRef.current).toBeNull();
  });

  it('closes the AudioContext on unmount', () => {
    const { result, unmount } = renderPlayback([makeSegment('a')]);
    act(() => result.current.togglePlay('a', 'a.m4a'));

    unmount();

    expect(engineMock.closeSpy).toHaveBeenCalled();
  });
});
