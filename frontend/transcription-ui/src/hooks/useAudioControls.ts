import { useEffect, useState } from 'react';

import {
  PAN_OPTIONS,
  SPEED_OPTIONS,
  VOLUME_MAX_DB,
  VOLUME_MIN_DB,
} from '../audio/WebAudioPlayer';
import { isSafari } from '../utils/browser';

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

function readStored(key: string): number | null {
  const raw = localStorage.getItem(key);
  if (raw === null) return null;
  const parsed = Number(raw);
  return Number.isFinite(parsed) ? parsed : null;
}

function initVolumeDb(): number {
  const stored = readStored(STORAGE_KEYS.volumeDb);
  if (stored === null) return DEFAULT_VOLUME_DB;
  return Math.min(VOLUME_MAX_DB, Math.max(VOLUME_MIN_DB, Math.round(stored)));
}

function initFromOptions(
  key: string,
  options: readonly number[],
  fallback: number
): number {
  const stored = readStored(key);
  return stored !== null && options.includes(stored) ? stored : fallback;
}

export function useAudioControls(): AudioControls {
  const [volumeDb, setVolumeDb] = useState(initVolumeDb);
  const [pan, setPan] = useState(() =>
    initFromOptions(STORAGE_KEYS.pan, PAN_OPTIONS, DEFAULT_PAN)
  );
  const [speed, setSpeed] = useState(() =>
    // Safari mangles playbackRate through Web Audio, so speed is pinned to 1×
    // there; ignore any persisted value (the UI also disables the control).
    isSafari()
      ? DEFAULT_SPEED
      : initFromOptions(STORAGE_KEYS.speed, SPEED_OPTIONS, DEFAULT_SPEED)
  );

  useEffect(() => {
    localStorage.setItem(STORAGE_KEYS.volumeDb, String(volumeDb));
  }, [volumeDb]);

  useEffect(() => {
    localStorage.setItem(STORAGE_KEYS.pan, String(pan));
  }, [pan]);

  useEffect(() => {
    localStorage.setItem(STORAGE_KEYS.speed, String(speed));
  }, [speed]);

  return { volumeDb, setVolumeDb, pan, setPan, speed, setSpeed };
}
