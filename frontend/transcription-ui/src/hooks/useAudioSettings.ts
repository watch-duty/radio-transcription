import { useCallback, useState } from 'react';

import {
  DEFAULT_PAN,
  DEFAULT_SPEED,
  DEFAULT_VOLUME_DB,
  PAN_OPTIONS,
  SPEED_OPTIONS,
  STORAGE_KEYS,
  VOLUME_MAX_DB,
  VOLUME_MIN_DB,
  feedKey,
} from '../audio/audioSettings';
import { isSafari } from '../utils/browser';

export interface AudioSettings {
  volumeDb: number;
  setVolumeDb: (db: number) => void;
  pan: number;
  setPan: (pan: number) => void;
  speed: number;
  setSpeed: (speed: number) => void;
  // Clears this feed's overrides so it re-inherits the (global) defaults,
  // rather than pinning it to the current default values.
  reset: () => void;
}

function readStored(key: string): number | null {
  const raw = localStorage.getItem(key);
  if (raw === null) return null;
  const parsed = Number(raw);
  return Number.isFinite(parsed) ? parsed : null;
}

function readForFeed(base: string, feedId: string): number | null {
  return readStored(feedKey(base, feedId)) ?? readStored(base);
}

function initVolumeDb(feedId: string): number {
  const stored = readForFeed(STORAGE_KEYS.volumeDb, feedId);
  if (stored === null) return DEFAULT_VOLUME_DB;
  return Math.min(VOLUME_MAX_DB, Math.max(VOLUME_MIN_DB, Math.round(stored)));
}

function initFromOptions(
  base: string,
  feedId: string,
  options: readonly number[],
  fallback: number
): number {
  const stored = readForFeed(base, feedId);
  return stored !== null && options.includes(stored) ? stored : fallback;
}

function initSpeed(feedId: string): number {
  // Safari mangles playbackRate through Web Audio, so speed is pinned to 1×
  // there; ignore any persisted value (the UI also disables the control).
  // https://bugs.webkit.org/show_bug.cgi?id=311000
  if (isSafari()) return DEFAULT_SPEED;
  return initFromOptions(
    STORAGE_KEYS.speed,
    feedId,
    SPEED_OPTIONS,
    DEFAULT_SPEED
  );
}

export function useAudioSettings(feedId: string): AudioSettings {
  const [volumeDb, setVolumeDbState] = useState(() => initVolumeDb(feedId));
  const [pan, setPanState] = useState(() =>
    initFromOptions(STORAGE_KEYS.pan, feedId, PAN_OPTIONS, DEFAULT_PAN)
  );
  const [speed, setSpeedState] = useState(() => initSpeed(feedId));

  // Reload each feed's stored settings when the id changes. The view stays
  // mounted across feed switches, so this uses React's "adjust state during
  // render" pattern (not an effect) to re-read storage with no flash.
  const [loadedFeedId, setLoadedFeedId] = useState(feedId);
  if (loadedFeedId !== feedId) {
    setLoadedFeedId(feedId);
    setVolumeDbState(initVolumeDb(feedId));
    setPanState(
      initFromOptions(STORAGE_KEYS.pan, feedId, PAN_OPTIONS, DEFAULT_PAN)
    );
    setSpeedState(initSpeed(feedId));
  }

  // Persist only on user changes (not the reload above), so merely visiting a
  // feed never captures the current default as a per-feed override.
  const persist = useCallback(
    (base: string, value: number) => {
      if (feedId) localStorage.setItem(feedKey(base, feedId), String(value));
    },
    [feedId]
  );

  const setVolumeDb = useCallback(
    (db: number) => {
      setVolumeDbState(db);
      persist(STORAGE_KEYS.volumeDb, db);
    },
    [persist]
  );
  const setPan = useCallback(
    (value: number) => {
      setPanState(value);
      persist(STORAGE_KEYS.pan, value);
    },
    [persist]
  );
  const setSpeed = useCallback(
    (value: number) => {
      setSpeedState(value);
      persist(STORAGE_KEYS.speed, value);
    },
    [persist]
  );

  const reset = useCallback(() => {
    // Remove the overrides (never the bare global key) and reload, so the feed
    // falls back to the global default — now and if that default later changes.
    if (feedId) {
      localStorage.removeItem(feedKey(STORAGE_KEYS.volumeDb, feedId));
      localStorage.removeItem(feedKey(STORAGE_KEYS.pan, feedId));
      localStorage.removeItem(feedKey(STORAGE_KEYS.speed, feedId));
    }
    setVolumeDbState(initVolumeDb(feedId));
    setPanState(
      initFromOptions(STORAGE_KEYS.pan, feedId, PAN_OPTIONS, DEFAULT_PAN)
    );
    setSpeedState(initSpeed(feedId));
  }, [feedId]);

  return { volumeDb, setVolumeDb, pan, setPan, speed, setSpeed, reset };
}
