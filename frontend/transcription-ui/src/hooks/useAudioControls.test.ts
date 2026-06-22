// @vitest-environment jsdom
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { act, renderHook } from '@testing-library/react';

import { useAudioControls } from './useAudioControls';

describe('useAudioControls', () => {
  beforeEach(() => {
    localStorage.clear();
  });

  afterEach(() => {
    localStorage.clear();
    vi.unstubAllGlobals();
  });

  it('defaults to unity volume, center pan, and 1x speed', () => {
    const { result } = renderHook(() => useAudioControls());

    expect(result.current.volumeDb).toBe(0);
    expect(result.current.pan).toBe(0);
    expect(result.current.speed).toBe(1);
  });

  it('persists changes to localStorage', () => {
    const { result } = renderHook(() => useAudioControls());

    act(() => {
      result.current.setVolumeDb(6);
      result.current.setPan(-1);
      result.current.setSpeed(1.5);
    });

    expect(localStorage.getItem('rt.audio.volumeDb')).toBe('6');
    expect(localStorage.getItem('rt.audio.pan')).toBe('-1');
    expect(localStorage.getItem('rt.audio.speed')).toBe('1.5');
  });

  it('restores persisted values on mount', () => {
    localStorage.setItem('rt.audio.volumeDb', '12');
    localStorage.setItem('rt.audio.pan', '1');
    localStorage.setItem('rt.audio.speed', '2');

    const { result } = renderHook(() => useAudioControls());

    expect(result.current.volumeDb).toBe(12);
    expect(result.current.pan).toBe(1);
    expect(result.current.speed).toBe(2);
  });

  it('clamps an out-of-range stored volume', () => {
    localStorage.setItem('rt.audio.volumeDb', '999');
    const { result } = renderHook(() => useAudioControls());
    expect(result.current.volumeDb).toBe(20);
  });

  it('pins speed to 1x on Safari, ignoring any persisted value', () => {
    vi.stubGlobal('navigator', {
      userAgent:
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15',
    });
    localStorage.setItem('rt.audio.speed', '2');

    const { result } = renderHook(() => useAudioControls());

    expect(result.current.speed).toBe(1);
  });

  it('falls back to defaults for invalid stored pan/speed', () => {
    localStorage.setItem('rt.audio.pan', '0.4');
    localStorage.setItem('rt.audio.speed', '3');
    localStorage.setItem('rt.audio.volumeDb', 'not-a-number');

    const { result } = renderHook(() => useAudioControls());

    expect(result.current.pan).toBe(0);
    expect(result.current.speed).toBe(1);
    expect(result.current.volumeDb).toBe(0);
  });
});
