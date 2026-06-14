import { useCallback, useEffect, useRef, useSyncExternalStore } from 'react';

import WaveSurfer from 'wavesurfer.js';

type CachedPeaks = { peaks: (Float32Array | number[])[]; duration: number };

// Decoded peaks per URL; WaveSurfer is display-only (Howler plays), so cached
// peaks let a revisited clip render instantly without re-decoding. The cache is
// an external store: subscribers re-render via useSyncExternalStore when it
// changes, and the version bumps on every mutation.
const MAX_PEAKS_CACHE = 500;
const peaksCache = new Map<string, CachedPeaks>();
const listeners = new Set<() => void>();
let cacheVersion = 0;

const emitCacheChange = () => {
  cacheVersion += 1;
  listeners.forEach((listener) => listener());
};

const subscribeCache = (listener: () => void) => {
  listeners.add(listener);
  return () => {
    listeners.delete(listener);
  };
};

const getCacheVersion = () => cacheVersion;

const cachePeaks = (
  url: string,
  peaks: (Float32Array | number[])[],
  duration: number
) => {
  peaksCache.set(url, { peaks, duration });
  if (peaksCache.size > MAX_PEAKS_CACHE) {
    const oldest = peaksCache.keys().next().value;
    if (oldest !== undefined) peaksCache.delete(oldest);
  }
  emitCacheChange();
};

// Test seam: prime the cache so the player renders synchronously under jsdom,
// where real audio decoding is unavailable.
export const __primePeaksCacheForTest = (url: string) => {
  cachePeaks(url, [[0, 1, 0]], 1);
};
export const __resetPeaksCacheForTest = () => {
  peaksCache.clear();
  emitCacheChange();
};

// Bounded so decoding many clips at once doesn't saturate network/CPU.
const DECODE_CONCURRENCY = 6;
// Free a stuck decode's slot so the queue can't wedge on a hung fetch.
const DECODE_TIMEOUT_MS = 15000;

// Resolves true if peaks were cached; false on error/timeout/no-op (don't retry false).
const decodePeaksHeadless = (url: string): Promise<boolean> =>
  new Promise((resolve) => {
    if (peaksCache.has(url)) {
      resolve(false);
      return;
    }
    let ws: WaveSurfer;
    try {
      const container = document.createElement('div');
      container.style.width = '100px';
      container.style.height = '1px';
      ws = WaveSurfer.create({ container, height: 1, url });
    } catch {
      resolve(false);
      return;
    }
    let settled = false;
    const finish = (cached: boolean) => {
      if (settled) return;
      settled = true;
      clearTimeout(timeout);
      ws.destroy();
      resolve(cached);
    };
    const timeout = setTimeout(() => finish(false), DECODE_TIMEOUT_MS);
    ws.on('ready', () => {
      if (!peaksCache.has(url)) {
        cachePeaks(url, ws.exportPeaks(), ws.getDuration());
      }
      finish(peaksCache.has(url));
    });
    ws.on('error', () => finish(false));
  });

export interface PeaksDecodeQueue {
  // priority puts URLs ahead of hover prefetch so the visible window wins.
  enqueueDecode: (urls: string[], priority: boolean) => void;
  // Drop everything queued-but-not-started; in-flight decodes finish and cache.
  // Lets a new window abandon a window the user scrubbed past without fetching it.
  clearPending: () => void;
  getPeaks: (url: string) => CachedPeaks | undefined;
}

// A single concurrency-limited decode queue shared by clips and hover prefetch.
// Decoded peaks land in the cache above; subscribing here re-renders the caller
// so getPeaks returns the new peaks once a decode completes.
export function usePeaksDecodeQueue(): PeaksDecodeQueue {
  useSyncExternalStore(subscribeCache, getCacheVersion);

  const queueRef = useRef<string[]>([]);
  // Mirrors queueRef's contents for O(1) dedup on enqueue.
  const queuedRef = useRef<Set<string>>(new Set());
  const activeRef = useRef(0);
  const inFlightRef = useRef<Set<string>>(new Set());
  // Latest pump, so async completion re-pumps without the callback self-referencing.
  const pumpRef = useRef<() => void>(() => {});

  const pump = useCallback(() => {
    while (
      activeRef.current < DECODE_CONCURRENCY &&
      queueRef.current.length > 0
    ) {
      const url = queueRef.current.shift();
      if (!url) continue;
      queuedRef.current.delete(url);
      if (peaksCache.has(url) || inFlightRef.current.has(url)) {
        continue;
      }
      inFlightRef.current.add(url);
      activeRef.current += 1;
      decodePeaksHeadless(url).finally(() => {
        activeRef.current -= 1;
        inFlightRef.current.delete(url);
        pumpRef.current();
      });
    }
  }, []);
  useEffect(() => {
    pumpRef.current = pump;
  }, [pump]);

  const enqueueDecode = useCallback(
    (urls: string[], priority: boolean) => {
      let added = 0;
      for (const url of urls) {
        if (
          peaksCache.has(url) ||
          inFlightRef.current.has(url) ||
          queuedRef.current.has(url)
        ) {
          continue;
        }
        queuedRef.current.add(url);
        if (priority) queueRef.current.unshift(url);
        else queueRef.current.push(url);
        added += 1;
      }
      if (added > 0) pump();
    },
    [pump]
  );

  const clearPending = useCallback(() => {
    queueRef.current = [];
    queuedRef.current.clear();
  }, []);

  const getPeaks = useCallback((url: string) => peaksCache.get(url), []);

  return { enqueueDecode, clearPending, getPeaks };
}
