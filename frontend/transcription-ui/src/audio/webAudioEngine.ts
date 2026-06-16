import { dbToGain } from './audioMath';

export interface AudioCallbacks {
  onplay?: () => void;
  onpause?: () => void;
  onend?: () => void;
  onerror?: () => void;
}

// Per-segment handle over the engine's shared element; `unload`/`off` only
// detach this segment's listeners, they never tear down the graph.
export interface WebAudioPlayer {
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
// Safari needs playbackRate changes deferred and bracketed by a pause/resume
// (see setSpeed); coalesce rapid slider changes within this window.
const SPEED_DEBOUNCE_MS = 150;

function createAudioContext(): AudioContext {
  const Ctor =
    window.AudioContext ??
    (window as unknown as { webkitAudioContext?: typeof AudioContext })
      .webkitAudioContext;
  if (!Ctor) {
    throw new Error('Web Audio API is not supported in this browser');
  }
  return new Ctor();
}

// Desktop Safari + iOS WebKit (which share Safari's audio quirks), not Chrome/Android.
function detectSafari(): boolean {
  return /^((?!chrome|android).)*safari/i.test(navigator.userAgent);
}

/**
 * Browser playback graph: MediaElementSource → GainNode → StereoPannerNode → destination.
 *
 * A MediaElementSourceNode can only be created once per element, so the graph is
 * built once and the source is swapped per segment via `load()`; volume/pan/speed
 * live on the graph and thus apply to every segment automatically.
 */
export class WebAudioEngine {
  private readonly context: AudioContext;
  private readonly audio: HTMLAudioElement;
  private readonly gain: GainNode;
  private readonly panner: StereoPannerNode;
  private readonly isSafari = detectSafari();
  private readonly handleVisibilityChange = () => {
    if (document.visibilityState === 'visible') this.resume();
  };

  private activeListeners: GraphListeners | null = null;
  private speed = 1;
  private speedDebounce: ReturnType<typeof setTimeout> | null = null;
  // True while we pause/resume the element internally (Safari speed change) so
  // those transient events don't surface as play/pause callbacks.
  private adjusting = false;

  constructor() {
    this.context = createAudioContext();

    this.audio = new Audio();
    // Required so the cross-origin GCS media isn't tainted when routed through
    // Web Audio (otherwise the graph output is silenced).
    this.audio.crossOrigin = 'anonymous';
    this.audio.preload = 'auto';

    const source = this.context.createMediaElementSource(this.audio);
    this.gain = this.context.createGain();
    this.panner = this.context.createStereoPanner();
    source
      .connect(this.gain)
      .connect(this.panner)
      .connect(this.context.destination);

    // Safari/iOS suspend (or "interrupt") the context when the tab is hidden;
    // resume it once the page is visible again so playback isn't left silent.
    document.addEventListener('visibilitychange', this.handleVisibilityChange);
  }

  /** Resume the context (call from a user gesture). */
  resume(): void {
    // `!== 'running'` also covers Safari's non-standard "interrupted" state.
    if (this.context.state !== 'running' && this.context.state !== 'closed') {
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

    if (!this.isSafari) {
      this.audio.playbackRate = rate;
      return;
    }

    // Safari distorts audio when playbackRate changes mid-playback on a
    // graph-routed element, so pause, apply, then resume — suppressing the
    // transient play/pause events so the UI doesn't flicker.
    if (this.speedDebounce) clearTimeout(this.speedDebounce);
    this.speedDebounce = setTimeout(() => {
      const wasPlaying = !this.audio.paused;
      this.adjusting = true;
      if (wasPlaying) this.audio.pause();
      this.audio.playbackRate = rate;
      if (wasPlaying) {
        this.audio
          .play()
          .catch(() => {})
          .finally(() => {
            this.adjusting = false;
          });
      } else {
        this.adjusting = false;
      }
    }, SPEED_DEBOUNCE_MS);
  }

  load(src: string, callbacks: AudioCallbacks): WebAudioPlayer {
    this.detachListeners();
    this.clearSpeedDebounce();

    const listeners: GraphListeners = {
      play: () => {
        if (!this.adjusting) callbacks.onplay?.();
      },
      pause: () => {
        if (!this.adjusting) callbacks.onpause?.();
      },
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
    this.clearSpeedDebounce();
    this.audio.pause();
    this.audio.removeAttribute('src');
    this.audio.load();
  }

  destroy(): void {
    document.removeEventListener(
      'visibilitychange',
      this.handleVisibilityChange
    );
    this.stop();
    this.context.close().catch(() => {});
  }

  private clearSpeedDebounce(): void {
    if (this.speedDebounce) {
      clearTimeout(this.speedDebounce);
      this.speedDebounce = null;
    }
  }

  // Detaches the given listeners, or the active ones if none specified. A
  // stale handle (already replaced by a newer load) is a no-op.
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
