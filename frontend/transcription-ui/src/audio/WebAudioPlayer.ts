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

const RAMP_SECONDS = 0.05;

export function createAudioContext(): AudioContext {
  const AudioContextClass = window.AudioContext ?? window.webkitAudioContext;
  if (!AudioContextClass) {
    throw new Error('Web Audio API is not supported in this browser');
  }
  return new AudioContextClass();
}

/** In-memory PCM Buffer Engine (AudioBufferSourceNode + LRU Buffer Cache for Zero-Gap Playback) */
export class WebAudioPlayer {
  private readonly context: AudioContext;
  private readonly gain: GainNode;
  private readonly panner: StereoPannerNode;
  private readonly bufferCache = new Map<string, Promise<AudioBuffer>>();

  private currentSource: AudioBufferSourceNode | null = null;
  private currentBuffer: AudioBuffer | null = null;
  private activeSrc: string | null = null;
  private activeCallbacks: AudioCallbacks | null = null;
  private isPlaying = false;
  private pausedOffset = 0;
  private playStartHardwareTime = 0;
  private playStartBufferOffset = 0;
  private speed = 1;

  constructor(context: AudioContext) {
    this.context = context;
    this.gain = this.context.createGain();
    this.panner = this.context.createStereoPanner();

    this.gain.connect(this.panner).connect(this.context.destination);
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
    if (this.isPlaying && this.currentSource && this.currentBuffer) {
      const elapsed =
        (this.context.currentTime - this.playStartHardwareTime) * this.speed;
      this.playStartBufferOffset = Math.min(
        this.currentBuffer.duration,
        this.playStartBufferOffset + elapsed
      );
      this.playStartHardwareTime = this.context.currentTime;
      this.currentSource.playbackRate.linearRampToValueAtTime(
        rate,
        this.context.currentTime + RAMP_SECONDS
      );
    }
    this.speed = rate;
  }

  preloadNext(src: string): void {
    if (!src) return;
    this.fetchAndDecode(src).catch(() => {});
  }

  private fetchAndDecode(src: string): Promise<AudioBuffer> {
    if (!src) return Promise.reject(new Error('Empty src'));
    let promise = this.bufferCache.get(src);
    if (!promise) {
      // LRU evict if cache exceeds 20 items to prevent memory leaks
      if (this.bufferCache.size >= 20) {
        const oldestKey = this.bufferCache.keys().next().value;
        if (oldestKey && oldestKey !== this.activeSrc) {
          this.bufferCache.delete(oldestKey);
        }
      }
      promise = fetch(src)
        .then((res) => {
          if (!res.ok) {
            throw new Error(
              `Failed to fetch ${src}: ${res.status} ${res.statusText}`
            );
          }
          return res.arrayBuffer();
        })
        .then((buf) => this.context.decodeAudioData(buf))
        .catch((err) => {
          this.bufferCache.delete(src);
          throw err;
        });
      this.bufferCache.set(src, promise);
    }
    return promise;
  }

  load(src: string, callbacks: AudioCallbacks): PlaybackController {
    if (this.activeSrc && this.activeSrc !== src) {
      this.stopCurrentSource();
      this.pausedOffset = 0;
    }
    this.activeSrc = src;
    this.activeCallbacks = callbacks;

    if (src) {
      this.fetchAndDecode(src).catch(() => {});
    }

    const controller: PlaybackController = {
      play: () => {
        this.resume();
        this.isPlaying = true;
        this.startBufferPlayback(src, this.pausedOffset);
      },
      pause: () => {
        if (!this.isPlaying) return;
        this.pausedOffset = controller.getCurrentTime();
        this.isPlaying = false;
        this.stopCurrentSource();
        this.activeCallbacks?.onPause?.();
      },
      stop: () => {
        this.isPlaying = false;
        this.pausedOffset = 0;
        this.stopCurrentSource();
      },
      getCurrentTime: () => {
        if (!this.isPlaying || !this.currentBuffer) {
          return this.pausedOffset;
        }
        const elapsed =
          (this.context.currentTime - this.playStartHardwareTime) * this.speed;
        return Math.min(
          this.currentBuffer.duration,
          this.playStartBufferOffset + elapsed
        );
      },
      setCurrentTime: (time: number) => {
        if (!Number.isFinite(time) || time < 0) return;
        if (!this.isPlaying) {
          this.pausedOffset = time;
        } else {
          this.startBufferPlayback(src, time);
        }
      },
      unload: () => {
        if (this.activeSrc === src) {
          this.isPlaying = false;
          this.stopCurrentSource();
          this.activeCallbacks = null;
          this.activeSrc = null;
        }
      },
      off: () => {
        if (this.activeSrc === src) {
          this.activeCallbacks = null;
        }
      },
    };
    return controller;
  }

  private startBufferPlayback(src: string, offset: number): void {
    this.stopCurrentSource();
    if (!src) {
      this.isPlaying = false;
      return;
    }

    this.fetchAndDecode(src)
      .then((buffer) => {
        if (!this.isPlaying || this.activeSrc !== src) return;
        this.currentBuffer = buffer;

        if (offset >= buffer.duration) {
          this.isPlaying = false;
          this.pausedOffset = 0;
          this.activeCallbacks?.onEnd?.();
          return;
        }

        const source = this.context.createBufferSource();
        source.buffer = buffer;
        source.playbackRate.value = this.speed;
        source.connect(this.gain);
        this.currentSource = source;
        this.playStartHardwareTime = this.context.currentTime;
        this.playStartBufferOffset = offset;

        source.onended = () => {
          if (this.currentSource !== source) return;
          this.currentSource = null;
          this.isPlaying = false;
          this.pausedOffset = 0;
          this.activeCallbacks?.onEnd?.();
        };

        try {
          source.start(0, offset);
          this.activeCallbacks?.onPlay?.();
        } catch {
          this.isPlaying = false;
          this.activeCallbacks?.onError?.();
        }
      })
      .catch(() => {
        if (this.activeSrc === src) {
          this.isPlaying = false;
          this.activeCallbacks?.onError?.();
        }
      });
  }

  private stopCurrentSource(): void {
    if (this.currentSource) {
      this.currentSource.onended = null;
      try {
        this.currentSource.stop();
      } catch {}
      try {
        this.currentSource.disconnect();
      } catch {}
      this.currentSource = null;
    }
  }

  stop(): void {
    this.isPlaying = false;
    this.pausedOffset = 0;
    this.stopCurrentSource();
  }

  dispose(): void {
    this.stop();
    this.gain.disconnect();
    this.panner.disconnect();
    this.bufferCache.clear();
  }
}
