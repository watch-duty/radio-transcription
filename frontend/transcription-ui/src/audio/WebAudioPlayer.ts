import {
  DEFAULT_VOLUME_DB,
  VOLUME_MIN_DB,
  VOLUME_SNAP_DB,
} from './audioSettings';

declare global {
  interface Window {
    webkitAudioContext?: typeof AudioContext;
  }
}

// Below this, snap to silence instead of an inaudible-but-nonzero gain.
const MUTE_THRESHOLD_DB = VOLUME_MIN_DB + 1;

export function dbToGain(db: number): number {
  return db < MUTE_THRESHOLD_DB ? 0 : 10 ** (db / 20);
}

export function gainToDb(gain: number): number {
  return gain <= 0 ? -Infinity : 20 * Math.log10(gain);
}

export function formatVolumeDb(db: number): string {
  if (db < MUTE_THRESHOLD_DB) return 'Muted';
  const rounded = Math.round(db);
  return rounded > 0 ? `+${rounded} dB` : `${rounded} dB`;
}

// Both the snap center (`defaultDb`) and width (`snapDb`) are parameters so a
// future global settings UI can drive them; passing snapDb 0 disables snapping.
export function snapVolumeToDefault(
  db: number,
  defaultDb: number = DEFAULT_VOLUME_DB,
  snapDb: number = VOLUME_SNAP_DB
): number {
  return Math.abs(db - defaultDb) <= snapDb ? defaultDb : db;
}

export interface AudioCallbacks {
  onPlay?: () => void;
  onPause?: () => void;
  onEnd?: () => void;
  onError?: () => void;
}

export interface PlaybackController {
  play: () => void;
  pause: () => void;
  stop: () => void;
  getCurrentTime: () => number;
  setCurrentTime: (time: number) => void;
  unload: () => void;
  off: () => void;
}

type GraphListeners = {
  play: () => void;
  pause: () => void;
  ended: () => void;
  error: () => void;
};

const RAMP_SECONDS = 0.05;

export function createAudioContext(): AudioContext {
  const AudioContextClass = window.AudioContext ?? window.webkitAudioContext;
  if (!AudioContextClass) {
    throw new Error('Web Audio API is not supported in this browser');
  }
  return new AudioContextClass();
}

/** MediaElementSource → GainNode → StereoPannerNode → destination */
export class WebAudioPlayer {
  private readonly context: AudioContext;
  private readonly audio: HTMLAudioElement;
  private readonly source: MediaElementAudioSourceNode;
  private readonly gain: GainNode;
  private readonly panner: StereoPannerNode;

  private activeListeners: GraphListeners | null = null;
  private speed = 1;

  constructor(context: AudioContext) {
    this.context = context;

    this.audio = new Audio();
    // Cross-origin GCS media must be CORS-clean here or Web Audio silences the output.
    this.audio.crossOrigin = 'anonymous';
    this.audio.preload = 'auto';

    this.source = this.context.createMediaElementSource(this.audio);
    this.gain = this.context.createGain();
    this.panner = this.context.createStereoPanner();
    this.source
      .connect(this.gain)
      .connect(this.panner)
      .connect(this.context.destination);
  }

  resume(): void {
    if (this.context.state === 'suspended') {
      this.context.resume().catch(() => {});
    }
  }

  setVolumeDb(db: number): void {
    this.gain.gain.linearRampToValueAtTime(
      dbToGain(db),
      this.context.currentTime + RAMP_SECONDS
    );
  }

  setPan(pan: number): void {
    this.panner.pan.linearRampToValueAtTime(
      pan,
      this.context.currentTime + RAMP_SECONDS
    );
  }

  setSpeed(rate: number): void {
    this.speed = rate;
    this.audio.playbackRate = rate;
  }

  load(src: string, callbacks: AudioCallbacks): PlaybackController {
    this.detachListeners();

    const listeners: GraphListeners = {
      play: () => callbacks.onPlay?.(),
      pause: () => callbacks.onPause?.(),
      ended: () => callbacks.onEnd?.(),
      error: () => callbacks.onError?.(),
    };
    this.audio.addEventListener('play', listeners.play);
    this.audio.addEventListener('pause', listeners.pause);
    this.audio.addEventListener('ended', listeners.ended);
    this.audio.addEventListener('error', listeners.error);
    this.activeListeners = listeners;

    this.audio.src = src;
    this.audio.load();
    // Changing src resets playbackRate, so re-apply the current speed.
    this.audio.playbackRate = this.speed;

    return {
      play: () => {
        this.resume();
        this.audio.play().catch(() => {});
      },
      pause: () => this.audio.pause(),
      stop: () => {
        this.audio.pause();
        this.audio.currentTime = 0;
      },
      getCurrentTime: () => this.audio.currentTime,
      setCurrentTime: (time: number) => {
        if (!Number.isFinite(time)) return;
        this.audio.currentTime = time;
      },
      unload: () => this.detachListeners(listeners),
      off: () => this.detachListeners(listeners),
    };
  }

  stop(): void {
    // Halt output before tearing down. The `pause` event is async, so callers
    // must clear their own playback state — it won't reach a detached listener.
    this.audio.pause();
    this.detachListeners();
    this.audio.removeAttribute('src');
    this.audio.load();
  }

  dispose(): void {
    this.stop();
    this.source.disconnect();
    this.gain.disconnect();
    this.panner.disconnect();
  }

  // A stale handle (already replaced by a newer `load`) is a no-op.
  private detachListeners(listeners?: GraphListeners): void {
    const target = listeners ?? this.activeListeners;
    if (!target || (listeners && listeners !== this.activeListeners)) return;
    this.audio.removeEventListener('play', target.play);
    this.audio.removeEventListener('pause', target.pause);
    this.audio.removeEventListener('ended', target.ended);
    this.audio.removeEventListener('error', target.error);
    this.activeListeners = null;
  }
}
