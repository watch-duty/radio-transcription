import {
  DEFAULT_VOLUME_DB,
  VOLUME_MIN_DB,
  VOLUME_SNAP_DB,
} from './audioSettings';
import { inspectMp4Boxes, splitFmp4 } from './fmp4Utils';

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

export interface SequenceCallbacks {
  onPlay?: () => void;
  onPause?: () => void;
  onSegmentChange?: (segmentId: string) => void;
  onEnd?: (lastSegmentId: string) => void;
  onError?: () => void;
}

export interface SequenceOptions {
  initialSegmentId: string;
  initialUri: string;
  callbacks: SequenceCallbacks;
  getNextSegment: (currentId: string) => { id: string; uri: string } | null;
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

interface TimelineRange {
  segmentId: string;
  startTime: number;
  endTime: number;
}

class FallbackSequenceSession implements PlaybackController {
  private audio: HTMLAudioElement;
  private speed: number;
  private options: SequenceOptions;
  private currentSegmentId: string;
  private isUnloaded = false;
  private listeners: GraphListeners;

  constructor(
    audio: HTMLAudioElement,
    speed: number,
    options: SequenceOptions
  ) {
    this.audio = audio;
    this.speed = speed;
    this.options = options;
    this.currentSegmentId = options.initialSegmentId;

    this.listeners = {
      play: () => this.options.callbacks.onPlay?.(),
      pause: () => this.options.callbacks.onPause?.(),
      ended: () => this.handleEnd(),
      error: () => this.options.callbacks.onError?.(),
    };

    this.audio.addEventListener('play', this.listeners.play);
    this.audio.addEventListener('pause', this.listeners.pause);
    this.audio.addEventListener('ended', this.listeners.ended);
    this.audio.addEventListener('error', this.listeners.error);

    this.audio.src = options.initialUri;
    this.audio.load();
    this.audio.playbackRate = this.speed;
    console.info(
      '[WebAudioPlayer] Active engine: Fallback (<audio> sequential player)'
    );
  }

  private handleEnd() {
    if (this.isUnloaded) return;
    const next = this.options.getNextSegment(this.currentSegmentId);
    if (next) {
      this.currentSegmentId = next.id;
      this.options.callbacks.onSegmentChange?.(next.id);
      this.audio.src = next.uri;
      this.audio.load();
      this.audio.playbackRate = this.speed;
      this.audio.play().catch(() => {});
    } else {
      this.options.callbacks.onEnd?.(this.currentSegmentId);
    }
  }

  play(): void {
    this.audio.play().catch(() => {});
  }

  pause(): void {
    this.audio.pause();
  }

  stop(): void {
    this.audio.pause();
    this.audio.currentTime = 0;
  }

  getCurrentTime(): number {
    return this.audio.currentTime;
  }

  setCurrentTime(time: number): void {
    if (!Number.isFinite(time)) return;
    this.audio.currentTime = time;
  }

  unload(): void {
    if (this.isUnloaded) return;
    this.isUnloaded = true;
    this.audio.removeEventListener('play', this.listeners.play);
    this.audio.removeEventListener('pause', this.listeners.pause);
    this.audio.removeEventListener('ended', this.listeners.ended);
    this.audio.removeEventListener('error', this.listeners.error);
  }

  off(): void {
    this.unload();
  }
}

class MseSequenceSession implements PlaybackController {
  private audio: HTMLAudioElement;
  private speed: number;
  private options: SequenceOptions;

  private mediaSource: MediaSource | null = null;
  private sourceBuffer: SourceBuffer | null = null;
  private objectUrl: string | null = null;
  private abortController = new AbortController();

  private appendQueue: Array<{ segmentId: string; buffer: ArrayBuffer }> = [];
  private enqueuedSegmentIds = new Set<string>();
  private lastEnqueuedSegmentId: string;
  private currentActiveSegmentId: string;
  private segmentTimeline: TimelineRange[] = [];
  private inFlightAppend: { segmentId: string; startTime: number } | null =
    null;

  private isUnloaded = false;
  private hasEnded = false;
  private isPrefetching = false;
  private hasCalledEndOfStream = false;
  private hasAppendedInitSegment = false;
  private onFallback?: (session: PlaybackController) => void;

  private listeners: {
    play: () => void;
    pause: () => void;
    ended: () => void;
    error: () => void;
    timeupdate: () => void;
  };

  constructor(
    audio: HTMLAudioElement,
    speed: number,
    options: SequenceOptions,
    onFallback?: (session: PlaybackController) => void
  ) {
    this.audio = audio;
    this.speed = speed;
    this.options = options;
    this.onFallback = onFallback;
    this.lastEnqueuedSegmentId = options.initialSegmentId;
    this.currentActiveSegmentId = options.initialSegmentId;
    this.enqueuedSegmentIds.add(options.initialSegmentId);

    this.listeners = {
      play: () => this.options.callbacks.onPlay?.(),
      pause: () => {
        if (!this.hasEnded && !this.isUnloaded) {
          this.options.callbacks.onPause?.();
        }
      },
      ended: () => this.handleEnded(),
      error: () => {
        if (!this.isUnloaded) {
          const err = this.audio.error;
          console.error(
            'MSE audio element error:',
            err ? `Code ${err.code}: ${err.message}` : 'Unknown error'
          );
          this.options.callbacks.onError?.();
        }
      },
      timeupdate: () => this.handleTimeUpdate(),
    };

    this.audio.addEventListener('play', this.listeners.play);
    this.audio.addEventListener('pause', this.listeners.pause);
    this.audio.addEventListener('ended', this.listeners.ended);
    this.audio.addEventListener('error', this.listeners.error);
    this.audio.addEventListener('timeupdate', this.listeners.timeupdate);

    this.mediaSource = new MediaSource();
    this.objectUrl = URL.createObjectURL(this.mediaSource);
    this.mediaSource.addEventListener('sourceopen', this.handleSourceOpen);
    this.mediaSource.addEventListener('sourceclose', this.handleSourceClose);
    this.mediaSource.addEventListener('sourceended', this.handleSourceEnded);

    this.audio.src = this.objectUrl;
    this.audio.load();
    this.audio.playbackRate = this.speed;
    console.info(
      '[WebAudioPlayer] Active engine: MSE (SourceBuffer in sequence mode)'
    );
  }

  private fallbackToStandardAudio(): void {
    if (this.isUnloaded) return;
    this.unload();
    const fallback = new FallbackSequenceSession(
      this.audio,
      this.speed,
      this.options
    );
    if (this.onFallback) {
      this.onFallback(fallback);
    }
    fallback.play();
  }

  private handleSourceClose = (): void => {
    if (this.isUnloaded) return;
    const err = this.audio.error;
    console.warn(
      `MSE MediaSource closed. Audio error: ${err ? `${err.code} (${err.message})` : 'none'}`
    );
  };

  private handleSourceEnded = (): void => {
    if (this.isUnloaded) return;
    console.debug('MSE MediaSource ended.');
  };

  private handleSourceOpen = (): void => {
    if (
      this.isUnloaded ||
      !this.mediaSource ||
      this.mediaSource.readyState !== 'open'
    ) {
      return;
    }
    this.mediaSource.removeEventListener('sourceopen', this.handleSourceOpen);
    try {
      this.sourceBuffer = this.mediaSource.addSourceBuffer(
        'audio/mp4; codecs="mp4a.40.2"'
      );
      this.sourceBuffer.mode = 'sequence';
      this.sourceBuffer.addEventListener('updateend', this.handleUpdateEnd);
      this.sourceBuffer.addEventListener('error', this.handleSourceBufferError);

      this.fetchAndEnqueue(
        this.options.initialSegmentId,
        this.options.initialUri
      );
    } catch (err) {
      console.error('MSE sourceopen error:', err);
      this.fallbackToStandardAudio();
    }
  };

  private async fetchAndEnqueue(segmentId: string, uri: string): Promise<void> {
    if (this.isUnloaded) return;
    try {
      const response = await fetch(uri, {
        signal: this.abortController.signal,
      });
      if (!response.ok) {
        throw new Error(`Failed to fetch ${uri}: ${response.status}`);
      }
      const buffer = await response.arrayBuffer();
      if (this.isUnloaded) return;

      const boxes = inspectMp4Boxes(buffer);
      if (!boxes.includes('moof')) {
        console.warn(
          `MSE check: Segment '${segmentId}' is not fragmented (boxes found: [${boxes.join(', ')}]). Falling back to standard <audio> sequence player.`
        );
        this.fallbackToStandardAudio();
        return;
      }

      this.appendQueue.push({ segmentId, buffer });
      this.processQueue();
    } catch (err) {
      if (this.abortController.signal.aborted || this.isUnloaded) return;
      console.error('MSE fetch error:', err);
      this.unload();
      this.options.callbacks.onError?.();
    }
  }

  private processQueue(): void {
    if (
      this.isUnloaded ||
      !this.sourceBuffer ||
      !this.mediaSource ||
      this.mediaSource.readyState !== 'open' ||
      !Array.from(this.mediaSource.sourceBuffers).includes(this.sourceBuffer) ||
      this.sourceBuffer.updating ||
      this.appendQueue.length === 0
    ) {
      if (
        this.appendQueue.length === 0 &&
        !this.isUnloaded &&
        (!this.sourceBuffer || !this.sourceBuffer.updating)
      ) {
        this.checkEndOfStream();
      } else if (
        this.appendQueue.length > 0 &&
        !this.isUnloaded &&
        this.mediaSource &&
        (this.mediaSource.readyState !== 'open' ||
          (this.sourceBuffer &&
            !Array.from(this.mediaSource.sourceBuffers).includes(
              this.sourceBuffer
            )))
      ) {
        const audioErr = this.audio.error;
        console.warn(
          `MSE append aborted: MediaSource readyState='${this.mediaSource.readyState}', sourceBuffers=${this.mediaSource.sourceBuffers.length}, audio.error=${audioErr ? `${audioErr.code} (${audioErr.message})` : 'none'}`
        );
        this.appendQueue = [];
        this.unload();
        this.options.callbacks.onError?.();
      }
      return;
    }

    const next = this.appendQueue[0];
    try {
      let bufferToAppend = next.buffer;
      if (!this.hasAppendedInitSegment) {
        this.hasAppendedInitSegment = true;
      } else {
        const { mediaSegment } = splitFmp4(next.buffer);
        bufferToAppend = mediaSegment;
      }

      const startTime =
        this.segmentTimeline.length > 0
          ? this.segmentTimeline[this.segmentTimeline.length - 1].endTime
          : 0;

      this.inFlightAppend = { segmentId: next.segmentId, startTime };
      this.sourceBuffer.appendBuffer(bufferToAppend);
    } catch (err) {
      console.error('MSE appendBuffer error:', err);
      this.appendQueue.shift();
      this.inFlightAppend = null;
      this.unload();
      this.options.callbacks.onError?.();
    }
  }

  private handleUpdateEnd = (): void => {
    if (this.isUnloaded || !this.sourceBuffer) return;

    if (this.inFlightAppend) {
      const { segmentId, startTime } = this.inFlightAppend;
      const endTime =
        this.sourceBuffer.buffered.length > 0
          ? this.sourceBuffer.buffered.end(
              this.sourceBuffer.buffered.length - 1
            )
          : startTime;

      this.segmentTimeline.push({ segmentId, startTime, endTime });
      console.info(
        `[MSE] Appended segment '${segmentId}' | Buffered range: [${startTime.toFixed(3)}s -> ${endTime.toFixed(3)}s] (duration: ${(endTime - startTime).toFixed(3)}s)`
      );
      this.inFlightAppend = null;
      this.appendQueue.shift();
    }

    this.maybePrefetchNext();
    this.processQueue();
  };

  private handleSourceBufferError = (): void => {
    if (this.isUnloaded) return;
    console.error(
      'MSE SourceBuffer emitted error. Attempting fallback to standard <audio> sequence player.'
    );
    this.fallbackToStandardAudio();
  };

  private maybePrefetchNext(): void {
    if (this.isUnloaded || this.hasEnded || this.isPrefetching) return;

    const currentIndex = this.segmentTimeline.findIndex(
      (item) => item.segmentId === this.currentActiveSegmentId
    );
    const effectiveIndex = currentIndex === -1 ? 0 : currentIndex;
    const remainingInTimeline = Math.max(
      0,
      this.segmentTimeline.length - effectiveIndex
    );
    const totalBufferedAhead = remainingInTimeline + this.appendQueue.length;

    // Buffer up to 3 contiguous segments ahead of the active playhead
    if (totalBufferedAhead < 3) {
      const next = this.options.getNextSegment(this.lastEnqueuedSegmentId);
      if (next && !this.enqueuedSegmentIds.has(next.id)) {
        this.enqueuedSegmentIds.add(next.id);
        this.lastEnqueuedSegmentId = next.id;
        this.isPrefetching = true;
        this.fetchAndEnqueue(next.id, next.uri).finally(() => {
          this.isPrefetching = false;
          this.maybePrefetchNext();
        });
      } else if (
        !next &&
        this.appendQueue.length === 0 &&
        !this.sourceBuffer?.updating
      ) {
        this.checkEndOfStream();
      }
    }
  }

  private checkEndOfStream(): void {
    if (
      !this.isUnloaded &&
      !this.hasCalledEndOfStream &&
      this.appendQueue.length === 0 &&
      (!this.sourceBuffer || !this.sourceBuffer.updating) &&
      this.mediaSource &&
      this.mediaSource.readyState === 'open'
    ) {
      const next = this.options.getNextSegment(this.lastEnqueuedSegmentId);
      if (!next) {
        try {
          this.mediaSource.endOfStream();
          this.hasCalledEndOfStream = true;
        } catch {
          // ignore DOM errors when calling endOfStream
        }
      }
    }
  }

  private handleTimeUpdate = (): void => {
    if (this.isUnloaded || this.segmentTimeline.length === 0) return;

    const currentTime = this.audio.currentTime;
    for (let i = 0; i < this.segmentTimeline.length; i++) {
      const range = this.segmentTimeline[i];
      if (
        currentTime >= range.startTime &&
        (currentTime < range.endTime || i === this.segmentTimeline.length - 1)
      ) {
        if (range.segmentId !== this.currentActiveSegmentId) {
          this.currentActiveSegmentId = range.segmentId;
          this.options.callbacks.onSegmentChange?.(range.segmentId);
          this.maybePrefetchNext();
        }
        break;
      }
    }
  };

  private handleEnded = (): void => {
    if (this.isUnloaded || this.hasEnded) return;
    this.hasEnded = true;
    this.options.callbacks.onEnd?.(this.currentActiveSegmentId);
  };

  play(): void {
    this.audio.play().catch(() => {});
  }

  pause(): void {
    this.audio.pause();
  }

  stop(): void {
    this.audio.pause();
    this.audio.currentTime = 0;
  }

  getCurrentTime(): number {
    const range = this.segmentTimeline.find(
      (item) => item.segmentId === this.currentActiveSegmentId
    );
    if (!range) return this.audio.currentTime;
    return Math.max(0, this.audio.currentTime - range.startTime);
  }

  setCurrentTime(time: number): void {
    if (!Number.isFinite(time)) return;
    const range = this.segmentTimeline.find(
      (item) => item.segmentId === this.currentActiveSegmentId
    );
    const startTime = range ? range.startTime : 0;
    this.audio.currentTime = startTime + time;
  }

  unload(): void {
    if (this.isUnloaded) return;
    this.isUnloaded = true;
    this.abortController.abort();

    this.audio.removeEventListener('play', this.listeners.play);
    this.audio.removeEventListener('pause', this.listeners.pause);
    this.audio.removeEventListener('ended', this.listeners.ended);
    this.audio.removeEventListener('error', this.listeners.error);
    this.audio.removeEventListener('timeupdate', this.listeners.timeupdate);

    if (this.mediaSource) {
      this.mediaSource.removeEventListener('sourceopen', this.handleSourceOpen);
      this.mediaSource.removeEventListener(
        'sourceclose',
        this.handleSourceClose
      );
      this.mediaSource.removeEventListener(
        'sourceended',
        this.handleSourceEnded
      );
    }

    if (this.sourceBuffer) {
      try {
        this.sourceBuffer.removeEventListener(
          'updateend',
          this.handleUpdateEnd
        );
        this.sourceBuffer.removeEventListener(
          'error',
          this.handleSourceBufferError
        );
        if (this.sourceBuffer.updating) {
          this.sourceBuffer.abort();
        }
      } catch {
        // ignore DOM errors during cleanup
      }
    }

    if (this.objectUrl) {
      URL.revokeObjectURL(this.objectUrl);
      this.objectUrl = null;
    }
  }

  off(): void {
    this.unload();
  }
}

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
  private activeSession: PlaybackController | null = null;
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
    if (this.activeSession) {
      this.activeSession.unload();
      this.activeSession = null;
    }

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

  loadSequence(options: SequenceOptions): PlaybackController {
    this.detachListeners();
    if (this.activeSession) {
      this.activeSession.unload();
      this.activeSession = null;
    }

    if (
      typeof window.MediaSource !== 'undefined' &&
      typeof window.URL !== 'undefined' &&
      MediaSource.isTypeSupported('audio/mp4; codecs="mp4a.40.2"')
    ) {
      const session = new MseSequenceSession(
        this.audio,
        this.speed,
        options,
        (fallback) => {
          this.activeSession = fallback;
        }
      );
      this.activeSession = session;
      return session;
    }

    const fallback = new FallbackSequenceSession(
      this.audio,
      this.speed,
      options
    );
    this.activeSession = fallback;
    return fallback;
  }

  stop(): void {
    // Halt output before tearing down. The `pause` event is async, so callers
    // must clear their own playback state — it won't reach a detached listener.
    if (this.activeSession) {
      this.activeSession.unload();
      this.activeSession = null;
    } else {
      this.audio.pause();
      this.detachListeners();
      this.audio.removeAttribute('src');
      this.audio.load();
    }
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
