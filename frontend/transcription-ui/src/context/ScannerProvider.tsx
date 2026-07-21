import { useCallback, useMemo } from 'react';
import type { ReactNode } from 'react';

import { useLocalStorage } from '../hooks/useLocalStorage';
import {
  SCANNER_STORAGE_KEY,
  ScannerContext,
  type ScannerContextValue,
  type ScannerFeed,
} from './ScannerContext';

export function ScannerProvider({ children }: { children: ReactNode }) {
  const [scannerFeeds, setScannerFeeds] = useLocalStorage<ScannerFeed[]>(
    SCANNER_STORAGE_KEY,
    []
  );

  const isInScanner = useCallback(
    (id: string) => scannerFeeds.some((f) => f.id === id),
    [scannerFeeds]
  );

  const addFeed = useCallback(
    (feed: ScannerFeed) => {
      setScannerFeeds((prev) =>
        prev.some((f) => f.id === feed.id) ? prev : [...prev, feed]
      );
    },
    [setScannerFeeds]
  );

  const removeFeed = useCallback(
    (id: string) => {
      setScannerFeeds((prev) => prev.filter((f) => f.id !== id));
    },
    [setScannerFeeds]
  );

  const removeAll = useCallback(() => {
    setScannerFeeds([]);
  }, [setScannerFeeds]);

  const reorderFeeds = useCallback(
    (fromIndex: number, toIndex: number) => {
      setScannerFeeds((prev) => {
        if (
          fromIndex === toIndex ||
          fromIndex < 0 ||
          toIndex < 0 ||
          fromIndex >= prev.length ||
          toIndex >= prev.length
        ) {
          return prev;
        }
        const next = [...prev];
        const [moved] = next.splice(fromIndex, 1);
        next.splice(toIndex, 0, moved);
        return next;
      });
    },
    [setScannerFeeds]
  );

  const value = useMemo<ScannerContextValue>(
    () => ({
      scannerFeeds,
      count: scannerFeeds.length,
      isInScanner,
      addFeed,
      removeFeed,
      removeAll,
      reorderFeeds,
    }),
    [scannerFeeds, isInScanner, addFeed, removeFeed, removeAll, reorderFeeds]
  );

  return (
    <ScannerContext.Provider value={value}>{children}</ScannerContext.Provider>
  );
}

export default ScannerProvider;
