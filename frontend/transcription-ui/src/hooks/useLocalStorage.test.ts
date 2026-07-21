// @vitest-environment jsdom
import { afterEach, beforeEach, describe, expect, it } from 'vitest';

import { act, renderHook } from '@testing-library/react';

import { useLocalStorage } from './useLocalStorage';

const KEY = 'test:key';

describe('useLocalStorage', () => {
  beforeEach(() => {
    window.localStorage.clear();
  });

  afterEach(() => {
    window.localStorage.clear();
  });

  it('returns the initial value when nothing is stored', () => {
    const { result } = renderHook(() => useLocalStorage(KEY, 'fallback'));
    expect(result.current[0]).toBe('fallback');
  });

  it('reads an existing stored value', () => {
    window.localStorage.setItem(KEY, JSON.stringify('stored'));
    const { result } = renderHook(() => useLocalStorage(KEY, 'fallback'));
    expect(result.current[0]).toBe('stored');
  });

  it('writes updates through to localStorage', () => {
    const { result } = renderHook(() => useLocalStorage<number[]>(KEY, []));

    act(() => result.current[1]([1, 2, 3]));

    expect(result.current[0]).toEqual([1, 2, 3]);
    expect(JSON.parse(window.localStorage.getItem(KEY)!)).toEqual([1, 2, 3]);
  });

  it('falls back when stored JSON is malformed', () => {
    window.localStorage.setItem(KEY, '{not valid json');
    const { result } = renderHook(() => useLocalStorage(KEY, 'fallback'));
    expect(result.current[0]).toBe('fallback');
  });
});
