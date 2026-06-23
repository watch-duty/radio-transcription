// @vitest-environment jsdom
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { act, renderHook } from '@testing-library/react';

import { useAudioControls } from './useAudioControls';

const FEED = 'feed-1';

describe('useAudioControls', () => {
  beforeEach(() => {
    localStorage.clear();
  });

  afterEach(() => {
    localStorage.clear();
    vi.unstubAllGlobals();
  });

  it('defaults to unity volume, center pan, and 1x speed', () => {
    const { result } = renderHook(() => useAudioControls(FEED));

    expect(result.current.volumeDb).toBe(0);
    expect(result.current.pan).toBe(0);
    expect(result.current.speed).toBe(1);
  });

  it('persists changes under a per-feed key', () => {
    const { result } = renderHook(() => useAudioControls(FEED));

    act(() => {
      result.current.setVolumeDb(6);
      result.current.setPan(-1);
      result.current.setSpeed(1.5);
    });

    expect(localStorage.getItem('rt.audio.volumeDb.feed-1')).toBe('6');
    expect(localStorage.getItem('rt.audio.pan.feed-1')).toBe('-1');
    expect(localStorage.getItem('rt.audio.speed.feed-1')).toBe('1.5');
  });

  it('restores a feed’s persisted values on mount', () => {
    localStorage.setItem('rt.audio.volumeDb.feed-1', '12');
    localStorage.setItem('rt.audio.pan.feed-1', '1');
    localStorage.setItem('rt.audio.speed.feed-1', '2');

    const { result } = renderHook(() => useAudioControls(FEED));

    expect(result.current.volumeDb).toBe(12);
    expect(result.current.pan).toBe(1);
    expect(result.current.speed).toBe(2);
  });

  it('keeps settings independent across feeds', () => {
    localStorage.setItem('rt.audio.volumeDb.feed-1', '6');
    localStorage.setItem('rt.audio.volumeDb.feed-2', '-12');

    const a = renderHook(() => useAudioControls('feed-1'));
    const b = renderHook(() => useAudioControls('feed-2'));

    expect(a.result.current.volumeDb).toBe(6);
    expect(b.result.current.volumeDb).toBe(-12);
  });

  it('falls back to the global default when a feed has no stored value', () => {
    localStorage.setItem('rt.audio.volumeDb', '9');

    const { result } = renderHook(() => useAudioControls(FEED));

    expect(result.current.volumeDb).toBe(9);
  });

  it('prefers the per-feed value over the global default', () => {
    localStorage.setItem('rt.audio.volumeDb', '9');
    localStorage.setItem('rt.audio.volumeDb.feed-1', '3');

    const { result } = renderHook(() => useAudioControls(FEED));

    expect(result.current.volumeDb).toBe(3);
  });

  it('reloads stored settings when the feed changes', () => {
    localStorage.setItem('rt.audio.volumeDb.feed-1', '6');
    localStorage.setItem('rt.audio.volumeDb.feed-2', '-12');

    const { result, rerender } = renderHook(
      ({ feedId }) => useAudioControls(feedId),
      { initialProps: { feedId: 'feed-1' } }
    );
    expect(result.current.volumeDb).toBe(6);

    rerender({ feedId: 'feed-2' });
    expect(result.current.volumeDb).toBe(-12);
  });

  it('does not write a per-feed key just by switching to a feed', () => {
    localStorage.setItem('rt.audio.volumeDb', '9');

    const { result } = renderHook(() => useAudioControls(FEED));

    // Inherited the global default without capturing it as a per-feed override.
    expect(result.current.volumeDb).toBe(9);
    expect(localStorage.getItem('rt.audio.volumeDb.feed-1')).toBeNull();
  });

  it('clamps an out-of-range stored volume', () => {
    localStorage.setItem('rt.audio.volumeDb.feed-1', '999');
    const { result } = renderHook(() => useAudioControls(FEED));
    expect(result.current.volumeDb).toBe(20);
  });

  it('pins speed to 1x on Safari, ignoring any persisted value', () => {
    vi.stubGlobal('navigator', {
      userAgent:
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15',
    });
    localStorage.setItem('rt.audio.speed.feed-1', '2');

    const { result } = renderHook(() => useAudioControls(FEED));

    expect(result.current.speed).toBe(1);
  });

  it('falls back to defaults for invalid stored pan/speed', () => {
    localStorage.setItem('rt.audio.pan.feed-1', '0.4');
    localStorage.setItem('rt.audio.speed.feed-1', '3');
    localStorage.setItem('rt.audio.volumeDb.feed-1', 'not-a-number');

    const { result } = renderHook(() => useAudioControls(FEED));

    expect(result.current.pan).toBe(0);
    expect(result.current.speed).toBe(1);
    expect(result.current.volumeDb).toBe(0);
  });
});
