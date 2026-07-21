// @vitest-environment jsdom
import { afterEach, beforeEach, describe, expect, it } from 'vitest';

import { act, cleanup, renderHook } from '@testing-library/react';

import { SCANNER_STORAGE_KEY, useScanner } from './ScannerContext';
import { ScannerProvider } from './ScannerProvider';

const renderScanner = () =>
  renderHook(() => useScanner(), { wrapper: ScannerProvider });

describe('ScannerProvider', () => {
  beforeEach(() => {
    window.localStorage.clear();
  });

  afterEach(() => {
    cleanup();
  });

  it('adds feeds and persists them to localStorage', () => {
    const { result } = renderScanner();

    act(() => result.current.addFeed({ id: 'a', name: 'Alpha' }));

    expect(result.current.scannerFeeds).toEqual([{ id: 'a', name: 'Alpha' }]);
    expect(result.current.count).toBe(1);
    expect(result.current.isInScanner('a')).toBe(true);
    expect(
      JSON.parse(window.localStorage.getItem(SCANNER_STORAGE_KEY)!)
    ).toEqual([{ id: 'a', name: 'Alpha' }]);
  });

  it('does not add duplicate feeds', () => {
    const { result } = renderScanner();

    act(() => result.current.addFeed({ id: 'a', name: 'Alpha' }));
    act(() => result.current.addFeed({ id: 'a', name: 'Alpha renamed' }));

    expect(result.current.count).toBe(1);
    expect(result.current.scannerFeeds[0].name).toBe('Alpha');
  });

  it('removes feeds', () => {
    const { result } = renderScanner();

    act(() => result.current.addFeed({ id: 'a', name: 'Alpha' }));
    act(() => result.current.addFeed({ id: 'b', name: 'Bravo' }));
    act(() => result.current.removeFeed('a'));

    expect(result.current.scannerFeeds.map((f) => f.id)).toEqual(['b']);
    expect(result.current.isInScanner('a')).toBe(false);
  });

  it('reorders feeds', () => {
    const { result } = renderScanner();

    act(() => result.current.addFeed({ id: 'a', name: 'Alpha' }));
    act(() => result.current.addFeed({ id: 'b', name: 'Bravo' }));
    act(() => result.current.addFeed({ id: 'c', name: 'Charlie' }));

    act(() => result.current.reorderFeeds(0, 2));

    expect(result.current.scannerFeeds.map((f) => f.id)).toEqual([
      'b',
      'c',
      'a',
    ]);
  });

  it('hydrates initial state from localStorage', () => {
    window.localStorage.setItem(
      SCANNER_STORAGE_KEY,
      JSON.stringify([{ id: 'z', name: 'Zeta' }])
    );

    const { result } = renderScanner();

    expect(result.current.scannerFeeds.map((f) => f.id)).toEqual(['z']);
  });
});
