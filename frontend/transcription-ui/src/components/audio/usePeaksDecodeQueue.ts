import { useCallback, useEffect, useRef, useSyncExternalStore } from 'react';

type CachedPeaks = { peaks: (Float32Array | number[])[]; duration: number };

// Decoded peaks per URL, cached so a revisited clip renders without re-decoding.
// An external store so subscribers re-render when a decode lands.
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

// Test seam: prime the cache so the player renders synchronously in tests.
export const __primePeaksCacheForTest = (url: string) => {
  cachePeaks(url, [[0, 1, 0]], 1);
};
export const __resetPeaksCacheForTest = () => {
  peaksCache.clear();
  emitCacheChange();
};

// Max concurrent downloads.
const FETCH_CONCURRENCY = 20;
// Peaks are signed max-abs per bucket — the format the display player expects.
// Bucket count scales with clip length (~one per rendered pixel) with a floor.
const PEAKS_CHANNELS = 2;
const PEAKS_PER_SECOND = 4;
const MIN_PEAK_BUCKETS = 32;
const PEAKS_PRECISION = 10000;

// Shared, lazily-created AudioContext for all decodes.
let audioContext: AudioContext | null = null;
const getAudioContext = (): AudioContext => {
  audioContext ??= new AudioContext();
  return audioContext;
};

const extractPeaks = (buffer: AudioBuffer): number[][] => {
  const buckets = Math.max(
    MIN_PEAK_BUCKETS,
    Math.ceil(buffer.duration * PEAKS_PER_SECOND)
  );
  const channels = Math.min(PEAKS_CHANNELS, buffer.numberOfChannels);
  const peaks: number[][] = [];
  for (let c = 0; c < channels; c++) {
    const data = buffer.getChannelData(c);
    const bucketSize = data.length / buckets;
    const channelPeaks: number[] = [];
    for (let i = 0; i < buckets; i++) {
      let peak = 0;
      const start = Math.floor(i * bucketSize);
      const end = Math.ceil((i + 1) * bucketSize);
      for (let j = start; j < end; j++) {
        if (Math.abs(data[j]) > Math.abs(peak)) peak = data[j];
      }
      channelPeaks.push(Math.round(peak * PEAKS_PRECISION) / PEAKS_PRECISION);
    }
    peaks.push(channelPeaks);
  }
  return peaks;
};

const decodePeaks = async (url: string, buffer: ArrayBuffer): Promise<void> => {
  if (peaksCache.has(url)) return;
  try {
    const decoded = await getAudioContext().decodeAudioData(buffer);
    if (!peaksCache.has(url)) {
      cachePeaks(url, extractPeaks(decoded), decoded.duration);
    }
  } catch {
    // On failure the clip stays a placeholder.
  }
};

export interface PeaksDecodeQueue {
  enqueueDecode: (urls: string[]) => void;
  // Drop queued-but-not-started urls; in-flight fetches/decodes still finish.
  clearPending: () => void;
  getPeaks: (url: string) => CachedPeaks | undefined;
}

// Fetch-bounded peaks pipeline: at most FETCH_CONCURRENCY downloads at once, each
// freeing its slot when the bytes arrive and decoding in the background.
// Subscribing re-renders the caller when a decode lands.
export function usePeaksDecodeQueue(): PeaksDecodeQueue {
  useSyncExternalStore(subscribeCache, getCacheVersion);

  const queueRef = useRef<string[]>([]);
  // Urls anywhere in the pipeline (queued, fetching, or decoding), for dedup.
  const queuedRef = useRef<Set<string>>(new Set());
  const fetchActiveRef = useRef(0);
  // Latest pump, so async completions can re-pump.
  const pumpRef = useRef<() => void>(() => {});

  const pump = useCallback(() => {
    while (
      fetchActiveRef.current < FETCH_CONCURRENCY &&
      queueRef.current.length > 0
    ) {
      const url = queueRef.current.shift();
      if (!url) continue;
      if (peaksCache.has(url)) {
        queuedRef.current.delete(url);
        continue;
      }
      fetchActiveRef.current += 1;
      fetch(url)
        .then((res) => res.arrayBuffer())
        .then((buffer) => {
          // Decode in the background; keep the url in queuedRef until it
          // resolves so it isn't re-fetched.
          void decodePeaks(url, buffer).finally(() =>
            queuedRef.current.delete(url)
          );
        })
        .catch(() => queuedRef.current.delete(url))
        .finally(() => {
          fetchActiveRef.current -= 1;
          pumpRef.current();
        });
    }
  }, []);
  useEffect(() => {
    pumpRef.current = pump;
  }, [pump]);

  const enqueueDecode = useCallback(
    (urls: string[]) => {
      let added = 0;
      for (const url of urls) {
        if (peaksCache.has(url) || queuedRef.current.has(url)) continue;
        queuedRef.current.add(url);
        queueRef.current.push(url);
        added += 1;
      }
      if (added > 0) pump();
    },
    [pump]
  );

  const clearPending = useCallback(() => {
    for (const url of queueRef.current) queuedRef.current.delete(url);
    queueRef.current = [];
  }, []);

  const getPeaks = useCallback((url: string) => peaksCache.get(url), []);

  return { enqueueDecode, clearPending, getPeaks };
}
