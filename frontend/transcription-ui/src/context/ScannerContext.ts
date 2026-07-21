import { createContext, useContext } from 'react';

export const SCANNER_STORAGE_KEY = 'transcription:scannerFeeds';

export interface ScannerFeed {
  id: string;
  name: string;
}

export interface ScannerContextValue {
  scannerFeeds: ScannerFeed[];
  count: number;
  isInScanner: (id: string) => boolean;
  addFeed: (feed: ScannerFeed) => void;
  removeFeed: (id: string) => void;
  removeAll: () => void;
  reorderFeeds: (fromIndex: number, toIndex: number) => void;
}

export const ScannerContext = createContext<ScannerContextValue>({
  scannerFeeds: [],
  count: 0,
  isInScanner: () => false,
  addFeed: () => {},
  removeFeed: () => {},
  removeAll: () => {},
  reorderFeeds: () => {},
});

export const useScanner = (): ScannerContextValue => useContext(ScannerContext);
