// @vitest-environment jsdom
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import {
  WebAudioPlayer,
  dbToGain,
  formatVolumeDb,
  gainToDb,
  snapVolumeToDefault,
} from './WebAudioPlayer';
import { VOLUME_MAX_DB, VOLUME_MIN_DB } from './audioSettings';

describe('audioMath', () => {
  describe('dbToGain', () => {
    it('maps 0 dB to unity gain', () => {
      expect(dbToGain(0)).toBeCloseTo(1);
    });

    it('maps +20 dB to 10x gain', () => {
      expect(dbToGain(VOLUME_MAX_DB)).toBeCloseTo(10);
    });

    it('maps -6 dB to ~0.5 gain', () => {
      expect(dbToGain(-6)).toBeCloseTo(0.501, 2);
    });

    it('snaps to 0 at and below the mute threshold', () => {
      expect(dbToGain(VOLUME_MIN_DB)).toBe(0);
      expect(dbToGain(VOLUME_MIN_DB + 0.5)).toBe(0);
    });
  });

  describe('gainToDb', () => {
    it('is the inverse of dbToGain for audible values', () => {
      expect(gainToDb(dbToGain(-6))).toBeCloseTo(-6);
      expect(gainToDb(1)).toBeCloseTo(0);
    });

    it('returns -Infinity for zero gain', () => {
      expect(gainToDb(0)).toBe(-Infinity);
    });
  });

  describe('formatVolumeDb', () => {
    it('formats unity as 0 dB', () => {
      expect(formatVolumeDb(0)).toBe('0 dB');
    });

    it('prefixes positive values with +', () => {
      expect(formatVolumeDb(12)).toBe('+12 dB');
    });

    it('formats negative values', () => {
      expect(formatVolumeDb(-12)).toBe('-12 dB');
    });

    it('shows Muted at the floor', () => {
      expect(formatVolumeDb(VOLUME_MIN_DB)).toBe('Muted');
    });
  });

  describe('snapVolumeToDefault', () => {
    it('snaps values within the zone to the default', () => {
      expect(snapVolumeToDefault(1)).toBe(0);
      expect(snapVolumeToDefault(-1)).toBe(0);
      expect(snapVolumeToDefault(0)).toBe(0);
    });

    it('leaves values outside the zone untouched', () => {
      expect(snapVolumeToDefault(2)).toBe(2);
      expect(snapVolumeToDefault(-6)).toBe(-6);
    });

    it('snaps toward a non-zero default', () => {
      expect(snapVolumeToDefault(5, 6)).toBe(6);
      expect(snapVolumeToDefault(3, 6)).toBe(3);
    });

    it('honors a custom snap width', () => {
      expect(snapVolumeToDefault(2, 0, 3)).toBe(0);
      expect(snapVolumeToDefault(4, 0, 3)).toBe(4);
    });

    it('disables snapping at width 0', () => {
      expect(snapVolumeToDefault(1, 0, 0)).toBe(1);
    });
  });
});

class MockAudioParam {
  linearRampToValueAtTime = vi.fn();
}

class MockNode {
  connect = vi.fn((dest: unknown) => dest);
  disconnect = vi.fn();
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
let audioInstances: MockAudio[] = [];

beforeEach(() => {
  audioInstances = [];
  vi.stubGlobal('AudioContext', function () {
    lastContext = new MockAudioContext();
    return lastContext;
  });
  vi.stubGlobal('Audio', function () {
    const audio = new MockAudio();
    audioInstances.push(audio);
    return audio;
  });
});

afterEach(() => {
  vi.unstubAllGlobals();
});

describe('WebAudioPlayer', () => {
  it('builds the MediaElementSource → Gain → StereoPanner → destination graph for both players', () => {
    new WebAudioPlayer(new AudioContext());

    expect(lastContext.createMediaElementSource).toHaveBeenCalledWith(
      audioInstances[0]
    );
    expect(lastContext.createMediaElementSource).toHaveBeenCalledWith(
      audioInstances[1]
    );
    expect(lastContext.createGain).toHaveBeenCalled();
    expect(lastContext.createStereoPanner).toHaveBeenCalled();
    expect(audioInstances[0].crossOrigin).toBe('anonymous');
    expect(audioInstances[1].crossOrigin).toBe('anonymous');
    expect(lastContext.panner.connect).toHaveBeenCalledWith(
      lastContext.destination
    );
  });

  it('resumes a suspended context and is a no-op when running', () => {
    const player = new WebAudioPlayer(new AudioContext());

    player.resume();
    expect(lastContext.resume).toHaveBeenCalledTimes(1);

    lastContext.resume.mockClear();
    player.resume();
    expect(lastContext.resume).not.toHaveBeenCalled();
  });

  it('maps volume in dB to gain', () => {
    const player = new WebAudioPlayer(new AudioContext());

    player.setVolumeDb(-6);
    expect(lastContext.gain.gain.linearRampToValueAtTime).toHaveBeenCalledWith(
      dbToGain(-6),
      expect.any(Number)
    );
  });

  it('applies pan to the StereoPanner', () => {
    const player = new WebAudioPlayer(new AudioContext());

    player.setPan(-1);
    expect(lastContext.panner.pan.linearRampToValueAtTime).toHaveBeenCalledWith(
      -1,
      expect.any(Number)
    );
  });

  it('sets playbackRate on both A and B players', () => {
    const player = new WebAudioPlayer(new AudioContext());

    player.setSpeed(1.5);
    expect(audioInstances[0].playbackRate).toBe(1.5);
    expect(audioInstances[1].playbackRate).toBe(1.5);
  });

  it('loads a clip, wires callbacks, and re-applies the current speed', () => {
    const player = new WebAudioPlayer(new AudioContext());
    player.setSpeed(1.25);

    const onPlay = vi.fn();
    const onEnd = vi.fn();
    const handle = player.load('https://example.com/clip.m4a', {
      onPlay,
      onEnd,
    });

    expect(audioInstances[0].src).toBe('https://example.com/clip.m4a');
    expect(audioInstances[0].load).toHaveBeenCalled();
    expect(audioInstances[0].playbackRate).toBe(1.25);

    handle.play();
    expect(lastContext.resume).toHaveBeenCalled();
    expect(audioInstances[0].play).toHaveBeenCalled();
    expect(onPlay).toHaveBeenCalled();

    audioInstances[0].emit('ended');
    expect(onEnd).toHaveBeenCalled();
  });

  it('preloads the next clip onto standby player and flips active player on load for gapless handoff', () => {
    const player = new WebAudioPlayer(new AudioContext());
    const onPlayB = vi.fn();

    // Load clip 1 onto active player A
    player.load('https://example.com/1.m4a', {});
    expect(audioInstances[0].src).toBe('https://example.com/1.m4a');
    expect(audioInstances[1].src).toBe('');

    // Preload clip 2 onto standby player B
    player.preloadNext('https://example.com/2.m4a');
    expect(audioInstances[1].src).toBe('https://example.com/2.m4a');
    expect(audioInstances[1].load).toHaveBeenCalled();

    // Now load clip 2: since it is preloaded on player B, it flips active player to B
    const handleB = player.load('https://example.com/2.m4a', {
      onPlay: onPlayB,
    });
    handleB.play();

    expect(audioInstances[1].play).toHaveBeenCalled();
    expect(onPlayB).toHaveBeenCalled();
  });

  it('reads and writes the playback position', () => {
    const player = new WebAudioPlayer(new AudioContext());
    const handle = player.load('https://example.com/clip.m4a', {});

    audioInstances[0].currentTime = 3.2;
    expect(handle.getCurrentTime()).toBe(3.2);

    handle.setCurrentTime(7.5);
    expect(audioInstances[0].currentTime).toBe(7.5);
  });

  it('ignores a non-finite seek target', () => {
    const player = new WebAudioPlayer(new AudioContext());
    const handle = player.load('https://example.com/clip.m4a', {});

    audioInstances[0].currentTime = 4;
    handle.setCurrentTime(Infinity);
    handle.setCurrentTime(NaN);
    expect(audioInstances[0].currentTime).toBe(4);
  });

  it('does not detach the current clip when a stale handle is unloaded', () => {
    const player = new WebAudioPlayer(new AudioContext());
    const stale = player.load('https://example.com/first.m4a', {});

    const onPause = vi.fn();
    player.load('https://example.com/second.m4a', { onPause });

    stale.unload();

    audioInstances[0].emit('pause');
    expect(onPause).toHaveBeenCalled();
  });

  it('clears the source on stop, firing pause, but leaves the context open', () => {
    const player = new WebAudioPlayer(new AudioContext());
    const onPause = vi.fn();
    player.load('https://example.com/clip.m4a', { onPause });
    player.stop();

    expect(audioInstances[0].pause).toHaveBeenCalled();
    expect(audioInstances[1].pause).toHaveBeenCalled();
    expect(onPause).toHaveBeenCalled();
    expect(audioInstances[0].removeAttribute).toHaveBeenCalledWith('src');
    expect(audioInstances[1].removeAttribute).toHaveBeenCalledWith('src');
    expect(lastContext.close).not.toHaveBeenCalled();
  });

  it('disconnects its own nodes on dispose without closing the context', () => {
    const player = new WebAudioPlayer(new AudioContext());
    player.dispose();

    expect(lastContext.gain.disconnect).toHaveBeenCalled();
    expect(lastContext.panner.disconnect).toHaveBeenCalled();
    expect(lastContext.close).not.toHaveBeenCalled();
  });
});
