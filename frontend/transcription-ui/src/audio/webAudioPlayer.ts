export const VOLUME_MIN_DB = -30;
export const VOLUME_MAX_DB = 20;

export const SPEED_OPTIONS = [0.5, 1.0, 1.25, 1.5, 2.0] as const;
export const PAN_OPTIONS = [-1, 0, 1] as const;

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

export interface AudioCallbacks {
  onplay?: () => void;
  onpause?: () => void;
  onend?: () => void;
  onerror?: () => void;
}

export interface PlaybackController {
  play: () => void;
  pause: () => void;
  stop: () => void;
  seek: () => number;
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
  const Ctor =
    window.AudioContext ??
    (window as unknown as { webkitAudioContext?: typeof AudioContext })
      .webkitAudioContext;
  if (!Ctor) {
    throw new Error('Web Audio API is not supported in this browser');
  }
  return new Ctor();
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

  /** Resume the context (call from a user gesture). */
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
      play: () => callbacks.onplay?.(),
      pause: () => callbacks.onpause?.(),
      ended: () => callbacks.onend?.(),
      error: () => callbacks.onerror?.(),
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
      seek: () => this.audio.currentTime,
      unload: () => this.detachListeners(listeners),
      off: () => this.detachListeners(listeners),
    };
  }

  /** Stop and clear the source, but keep the graph/context alive for reuse. */
  stop(): void {
    this.detachListeners();
    this.audio.pause();
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
