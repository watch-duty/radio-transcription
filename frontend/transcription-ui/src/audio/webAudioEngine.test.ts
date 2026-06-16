// @vitest-environment jsdom
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { dbToGain } from './audioMath';
import { WebAudioEngine } from './webAudioEngine';

class MockAudioParam {
  linearRampToValueAtTime = vi.fn();
}

class MockNode {
  connect = vi.fn((dest: unknown) => dest);
}

class MockGainNode extends MockNode {
  gain = new MockAudioParam();
}

class MockPannerNode extends MockNode {
  pan = new MockAudioParam();
}

class MockAudioContext {
  state: AudioContextState | 'interrupted' = 'suspended';
  currentTime = 0;
  destination = {};
  gain = new MockGainNode();
  panner = new MockPannerNode();
  resume = vi.fn(() => {
    this.state = 'running';
    return Promise.resolve();
  });
  close = vi.fn(() => Promise.resolve());
  createMediaElementSource = vi.fn(() => new MockNode());
  createGain = vi.fn(() => this.gain);
  createStereoPanner = vi.fn(() => this.panner);
}

class MockAudio {
  paused = true;
  playbackRate = 1;
  currentTime = 0;
  crossOrigin: string | null = null;
  preload = '';
  src = '';
  private listeners: Record<string, Array<() => void>> = {};

  play = vi.fn(() => {
    this.paused = false;
    this.emit('play');
    return Promise.resolve();
  });
  pause = vi.fn(() => {
    this.paused = true;
    this.emit('pause');
  });
  load = vi.fn();
  removeAttribute = vi.fn();

  addEventListener = (type: string, cb: () => void) => {
    (this.listeners[type] ??= []).push(cb);
  };
  removeEventListener = (type: string, cb: () => void) => {
    this.listeners[type] = (this.listeners[type] ?? []).filter((f) => f !== cb);
  };
  emit(type: string) {
    (this.listeners[type] ?? []).forEach((f) => f());
  }
}

let lastContext: MockAudioContext;
let lastAudio: MockAudio;

function setUserAgent(ua: string) {
  Object.defineProperty(window.navigator, 'userAgent', {
    value: ua,
    configurable: true,
  });
}

const SAFARI_UA =
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15';
const CHROME_UA =
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36';

beforeEach(() => {
  // Plain function expressions (unlike arrows) are valid constructors, and
  // returning an object makes `new` yield it — letting us capture each instance.
  vi.stubGlobal('AudioContext', function () {
    lastContext = new MockAudioContext();
    return lastContext;
  });
  vi.stubGlobal('Audio', function () {
    lastAudio = new MockAudio();
    return lastAudio;
  });
  setUserAgent(CHROME_UA);
});

afterEach(() => {
  vi.unstubAllGlobals();
  vi.useRealTimers();
});

describe('WebAudioEngine', () => {
  it('builds the MediaElementSource → Gain → StereoPanner → destination graph', () => {
    new WebAudioEngine();

    expect(lastContext.createMediaElementSource).toHaveBeenCalledWith(
      lastAudio
    );
    expect(lastContext.createGain).toHaveBeenCalled();
    expect(lastContext.createStereoPanner).toHaveBeenCalled();
    expect(lastAudio.crossOrigin).toBe('anonymous');
    expect(lastContext.panner.connect).toHaveBeenCalledWith(
      lastContext.destination
    );
  });

  it('resumes a suspended context and is a no-op when running', () => {
    const engine = new WebAudioEngine();

    engine.resume();
    expect(lastContext.resume).toHaveBeenCalledTimes(1);

    lastContext.resume.mockClear();
    engine.resume();
    expect(lastContext.resume).not.toHaveBeenCalled();
  });

  it('resumes a Safari "interrupted" context', () => {
    const engine = new WebAudioEngine();
    lastContext.state = 'interrupted';

    engine.resume();
    expect(lastContext.resume).toHaveBeenCalled();
  });

  it('maps volume in dB to gain', () => {
    const engine = new WebAudioEngine();

    engine.setVolumeDb(-6);
    expect(lastContext.gain.gain.linearRampToValueAtTime).toHaveBeenCalledWith(
      dbToGain(-6),
      expect.any(Number)
    );
  });

  it('applies pan to the StereoPanner', () => {
    const engine = new WebAudioEngine();

    engine.setPan(-1);
    expect(lastContext.panner.pan.linearRampToValueAtTime).toHaveBeenCalledWith(
      -1,
      expect.any(Number)
    );
  });

  it('sets playbackRate immediately on non-Safari browsers', () => {
    const engine = new WebAudioEngine();

    engine.setSpeed(1.5);
    expect(lastAudio.playbackRate).toBe(1.5);
  });

  it('loads a clip, wires callbacks, and re-applies the current speed', () => {
    const engine = new WebAudioEngine();
    engine.setSpeed(1.25);

    const onplay = vi.fn();
    const onend = vi.fn();
    const player = engine.load('https://example.com/clip.m4a', {
      onplay,
      onend,
    });

    expect(lastAudio.src).toBe('https://example.com/clip.m4a');
    expect(lastAudio.load).toHaveBeenCalled();
    expect(lastAudio.playbackRate).toBe(1.25);

    player.play();
    expect(lastContext.resume).toHaveBeenCalled();
    expect(lastAudio.play).toHaveBeenCalled();
    expect(onplay).toHaveBeenCalled();

    lastAudio.emit('ended');
    expect(onend).toHaveBeenCalled();
  });

  describe('on Safari', () => {
    beforeEach(() => {
      setUserAgent(SAFARI_UA);
      vi.useFakeTimers();
    });

    it('debounces playbackRate and brackets it with pause/resume while playing', async () => {
      const engine = new WebAudioEngine();
      const onplay = vi.fn();
      const onpause = vi.fn();
      engine.load('https://example.com/clip.m4a', { onplay, onpause });

      lastAudio.paused = false;
      onplay.mockClear();
      onpause.mockClear();

      engine.setSpeed(2);
      expect(lastAudio.playbackRate).toBe(1);

      await vi.advanceTimersByTimeAsync(200);

      expect(lastAudio.playbackRate).toBe(2);
      expect(lastAudio.pause).toHaveBeenCalled();
      expect(lastAudio.play).toHaveBeenCalled();
      // The internal pause/resume must not surface as play/pause callbacks.
      expect(onpause).not.toHaveBeenCalled();
      expect(onplay).not.toHaveBeenCalled();
    });

    it('coalesces rapid speed changes into a single applied value', async () => {
      const engine = new WebAudioEngine();
      engine.load('https://example.com/clip.m4a', {});

      engine.setSpeed(1.5);
      engine.setSpeed(2);

      await vi.advanceTimersByTimeAsync(200);
      expect(lastAudio.playbackRate).toBe(2);
    });
  });

  it('resumes on visibilitychange when the page becomes visible', () => {
    Object.defineProperty(document, 'visibilityState', {
      value: 'visible',
      configurable: true,
    });
    const engine = new WebAudioEngine();
    engine.resume();
    lastContext.state = 'suspended';
    lastContext.resume.mockClear();

    document.dispatchEvent(new Event('visibilitychange'));
    expect(lastContext.resume).toHaveBeenCalled();
  });

  it('removes the visibility listener and closes the context on destroy', () => {
    const engine = new WebAudioEngine();
    engine.destroy();

    expect(lastContext.close).toHaveBeenCalled();

    lastContext.resume.mockClear();
    document.dispatchEvent(new Event('visibilitychange'));
    expect(lastContext.resume).not.toHaveBeenCalled();
  });
});
