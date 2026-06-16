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
  state: AudioContextState = 'suspended';
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
});

afterEach(() => {
  vi.unstubAllGlobals();
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

  it('sets playbackRate', () => {
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

  it('closes the context on destroy', () => {
    const engine = new WebAudioEngine();
    engine.destroy();

    expect(lastContext.close).toHaveBeenCalled();
  });
});
