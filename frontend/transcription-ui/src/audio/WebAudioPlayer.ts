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

function urlsMatch(a: string, b: string): boolean {
  if (!a || !b) return false;
  try {
    const base =
      typeof window !== 'undefined' && window.location?.href
        ? window.location.href
        : 'http://localhost';
    return new URL(a, base).href === new URL(b, base).href;
  } catch {
    return a === b;
  }
}

/** Dual MediaElementSource → GainNode → StereoPannerNode → destination (A/B Ping-Pong Buffering) */
export class WebAudioPlayer {
  private readonly context: AudioContext;
  private readonly audioA: HTMLAudioElement;
  private readonly audioB: HTMLAudioElement;
  private readonly sourceA: MediaElementAudioSourceNode;
  private readonly sourceB: MediaElementAudioSourceNode;
  private readonly gain: GainNode;
  private readonly panner: StereoPannerNode;

  private activePlayer: 'A' | 'B' = 'A';
  private activeListeners: GraphListeners | null = null;
  private speed = 1;

  private get activeAudio(): HTMLAudioElement {
    return this.activePlayer === 'A' ? this.audioA : this.audioB;
  }

  private get standbyAudio(): HTMLAudioElement {
    return this.activePlayer === 'A' ? this.audioB : this.audioA;
  }

  constructor(context: AudioContext) {
    this.context = context;

    this.audioA = new Audio();
    this.audioA.crossOrigin = 'anonymous';
    this.audioA.preload = 'auto';

    this.audioB = new Audio();
    this.audioB.crossOrigin = 'anonymous';
    this.audioB.preload = 'auto';

    this.sourceA = this.context.createMediaElementSource(this.audioA);
    this.sourceB = this.context.createMediaElementSource(this.audioB);

    this.gain = this.context.createGain();
    this.panner = this.context.createStereoPanner();

    this.sourceA
      .connect(this.gain)
      .connect(this.panner)
      .connect(this.context.destination);
    this.sourceB
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
    this.audioA.playbackRate = rate;
    this.audioB.playbackRate = rate;
  }

  preloadNext(src: string): void {
    if (!src) return;
    const standby = this.standbyAudio;
    if (!urlsMatch(standby.src, src)) {
      standby.src = src;
      standby.load();
      standby.playbackRate = this.speed;
    }
  }

  load(src: string, callbacks: AudioCallbacks): PlaybackController {
    this.detachListeners();

    const standby = this.standbyAudio;
    if (urlsMatch(standby.src, src)) {
      // Flip active player to the already-loaded standby player for instant gapless handoff
      this.activePlayer = this.activePlayer === 'A' ? 'B' : 'A';
    } else {
      const active = this.activeAudio;
      if (!urlsMatch(active.src, src)) {
        active.src = src;
        active.load();
        active.playbackRate = this.speed;
      }
      if (standby.src) {
        standby.removeAttribute('src');
        standby.load();
      }
    }

    const audio = this.activeAudio;
    const listeners: GraphListeners = {
      play: () => callbacks.onPlay?.(),
      pause: () => callbacks.onPause?.(),
      ended: () => callbacks.onEnd?.(),
      error: () => callbacks.onError?.(),
    };
    audio.addEventListener('play', listeners.play);
    audio.addEventListener('pause', listeners.pause);
    audio.addEventListener('ended', listeners.ended);
    audio.addEventListener('error', listeners.error);
    this.activeListeners = listeners;

    audio.playbackRate = this.speed;

    return {
      play: () => {
        this.resume();
        audio.play().catch(() => {});
      },
      pause: () => audio.pause(),
      stop: () => {
        audio.pause();
        audio.currentTime = 0;
      },
      getCurrentTime: () => audio.currentTime,
      setCurrentTime: (time: number) => {
        if (!Number.isFinite(time)) return;
        audio.currentTime = time;
      },
      unload: () => this.detachListeners(listeners),
      off: () => this.detachListeners(listeners),
    };
  }

  stop(): void {
    this.audioA.pause();
    this.audioB.pause();
    this.detachListeners();
    this.audioA.removeAttribute('src');
    this.audioA.load();
    this.audioB.removeAttribute('src');
    this.audioB.load();
  }

  dispose(): void {
    this.stop();
    this.sourceA.disconnect();
    this.sourceB.disconnect();
    this.gain.disconnect();
    this.panner.disconnect();
  }

  private detachListeners(listeners?: GraphListeners): void {
    const target = listeners ?? this.activeListeners;
    if (!target || (listeners && listeners !== this.activeListeners)) return;
    this.audioA.removeEventListener('play', target.play);
    this.audioA.removeEventListener('pause', target.pause);
    this.audioA.removeEventListener('ended', target.ended);
    this.audioA.removeEventListener('error', target.error);
    this.audioB.removeEventListener('play', target.play);
    this.audioB.removeEventListener('pause', target.pause);
    this.audioB.removeEventListener('ended', target.ended);
    this.audioB.removeEventListener('error', target.error);
    this.activeListeners = null;
  }
}
