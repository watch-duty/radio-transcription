import { useCallback, useState } from 'react';

import {
  PAN_OPTIONS,
  SPEED_OPTIONS,
  VOLUME_MAX_DB,
  VOLUME_MIN_DB,
} from '../audio/WebAudioPlayer';
import { isSafari } from '../utils/browser';

// Base keys hold the global default; a future global-settings UI writes these.
// Per-feed adjustments are stored under `<base>.<feedId>` and a read falls back
// to the base key, so an unvisited feed inherits the global default.
const STORAGE_KEYS = {
  volumeDb: 'rt.audio.volumeDb',
  pan: 'rt.audio.pan',
  speed: 'rt.audio.speed',
} as const;

const DEFAULT_VOLUME_DB = 0;
const DEFAULT_PAN = 0;
const DEFAULT_SPEED = 1;

export interface AudioControls {
  volumeDb: number;
  setVolumeDb: (db: number) => void;
  pan: number;
  setPan: (pan: number) => void;
  speed: number;
  setSpeed: (speed: number) => void;
}

function feedKey(base: string, feedId: string): string {
  return feedId ? `${base}.${feedId}` : base;
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
  if (isSafari()) return DEFAULT_SPEED;
  return initFromOptions(
    STORAGE_KEYS.speed,
    feedId,
    SPEED_OPTIONS,
    DEFAULT_SPEED
  );
}

export function useAudioControls(feedId: string): AudioControls {
  const [volumeDb, setVolumeDbState] = useState(() => initVolumeDb(feedId));
  const [pan, setPanState] = useState(() =>
    initFromOptions(STORAGE_KEYS.pan, feedId, PAN_OPTIONS, DEFAULT_PAN)
  );
  const [speed, setSpeedState] = useState(() => initSpeed(feedId));

  // The view stays mounted across feed switches, so reload each feed's stored
  // settings when the id changes (re-running the initializers re-reads storage).
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

  return { volumeDb, setVolumeDb, pan, setPan, speed, setSpeed };
}
