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

class MockAudioBuffer {
  duration = 10;
  sampleRate = 44100;
  length = 441000;
  numberOfChannels = 1;
}

class MockBufferSourceNode extends MockNode {
  buffer: MockAudioBuffer | null = null;
  playbackRate = new MockAudioParam();
  start = vi.fn();
  stop = vi.fn();
  onended: (() => void) | null = null;
  addEventListener = (type: string, cb: () => void) => {
    if (type === 'ended') this.onended = cb;
  };
  removeEventListener = (type: string, cb: () => void) => {
    if (type === 'ended' && this.onended === cb) this.onended = null;
  };
  emitEnded() {
    this.onended?.();
  }
}

class MockAudioContext {
  state: AudioContextState = 'suspended';
  currentTime = 0;
  destination = {};
  gain = new MockGainNode();
  panner = new MockPannerNode();
  sources: MockBufferSourceNode[] = [];
  resume = vi.fn(() => {
    this.state = 'running';
    return Promise.resolve();
  });
  close = vi.fn(() => Promise.resolve());
  createGain = vi.fn(() => this.gain);
  createStereoPanner = vi.fn(() => this.panner);
  decodeAudioData = vi.fn(() => Promise.resolve(new MockAudioBuffer()));
  createBufferSource = vi.fn(() => {
    const src = new MockBufferSourceNode();
    this.sources.push(src);
    return src;
  });
}

let lastContext: MockAudioContext;
let fetchSpy: ReturnType<typeof vi.fn>;

beforeEach(() => {
  fetchSpy = vi.fn(() =>
    Promise.resolve({
      ok: true,
      status: 200,
      statusText: 'OK',
      arrayBuffer: () => Promise.resolve(new ArrayBuffer(8)),
    } as Response)
  );
  vi.stubGlobal('fetch', fetchSpy);
  vi.stubGlobal('AudioContext', function () {
    lastContext = new MockAudioContext();
    return lastContext;
  });
});

afterEach(() => {
  vi.unstubAllGlobals();
});

describe('WebAudioPlayer', () => {
  it('builds the Gain → StereoPanner → destination graph', () => {
    new WebAudioPlayer(new AudioContext());

    expect(lastContext.createGain).toHaveBeenCalled();
    expect(lastContext.createStereoPanner).toHaveBeenCalled();
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

  it('sets playbackRate on current and future buffer sources', async () => {
    const player = new WebAudioPlayer(new AudioContext());
    const handle = player.load('https://example.com/clip.m4a', {});
    handle.play();
    await vi.waitFor(() => expect(lastContext.sources.length).toBe(1));

    player.setSpeed(1.5);
    expect(
      lastContext.sources[0].playbackRate.linearRampToValueAtTime
    ).toHaveBeenCalledWith(1.5, expect.any(Number));
  });

  it('loads a clip, fetches arrayBuffer, decodes, and plays', async () => {
    const player = new WebAudioPlayer(new AudioContext());
    const onPlay = vi.fn();
    const onEnd = vi.fn();
    const handle = player.load('https://example.com/clip.m4a', {
      onPlay,
      onEnd,
    });

    handle.play();
    expect(lastContext.resume).toHaveBeenCalled();
    await vi.waitFor(() =>
      expect(lastContext.createBufferSource).toHaveBeenCalled()
    );

    expect(fetchSpy).toHaveBeenCalledWith('https://example.com/clip.m4a');
    expect(lastContext.decodeAudioData).toHaveBeenCalled();
    expect(lastContext.sources[0].start).toHaveBeenCalledWith(0, 0);
    expect(onPlay).toHaveBeenCalled();

    lastContext.sources[0].emitEnded();
    expect(onEnd).toHaveBeenCalled();
  });

  it('preloads the next clip into buffer cache for instant zero-gap handoff', async () => {
    const player = new WebAudioPlayer(new AudioContext());
    const onPlayB = vi.fn();

    // Preload clip 2
    player.preloadNext('https://example.com/2.m4a');
    expect(fetchSpy).toHaveBeenCalledWith('https://example.com/2.m4a');

    // Wait for decode to finish
    await vi.waitFor(() =>
      expect(lastContext.decodeAudioData).toHaveBeenCalled()
    );

    // Now load clip 2: since it is already decoded in bufferCache, play() starts without re-fetching
    fetchSpy.mockClear();
    lastContext.decodeAudioData.mockClear();

    const handleB = player.load('https://example.com/2.m4a', {
      onPlay: onPlayB,
    });
    handleB.play();

    await vi.waitFor(() =>
      expect(lastContext.createBufferSource).toHaveBeenCalled()
    );
    expect(fetchSpy).not.toHaveBeenCalled();
    expect(lastContext.decodeAudioData).not.toHaveBeenCalled();
    expect(onPlayB).toHaveBeenCalled();
  });

  it('reads and writes the playback position', async () => {
    const player = new WebAudioPlayer(new AudioContext());
    const handle = player.load('https://example.com/clip.m4a', {});
    handle.play();
    await vi.waitFor(() => expect(lastContext.sources.length).toBe(1));

    lastContext.currentTime = 3.2;
    expect(handle.getCurrentTime()).toBeCloseTo(3.2);

    handle.setCurrentTime(7.5);
    await vi.waitFor(() => expect(lastContext.sources.length).toBe(2));
    expect(lastContext.sources[1].start).toHaveBeenCalledWith(0, 7.5);
  });

  it('ignores a non-finite or negative seek target', async () => {
    const player = new WebAudioPlayer(new AudioContext());
    const handle = player.load('https://example.com/clip.m4a', {});
    handle.play();
    await vi.waitFor(() => expect(lastContext.sources.length).toBe(1));

    handle.setCurrentTime(Infinity);
    handle.setCurrentTime(NaN);
    handle.setCurrentTime(-5);
    expect(lastContext.sources.length).toBe(1);
  });

  it('does not fire callbacks when a stale handle is unloaded', async () => {
    const player = new WebAudioPlayer(new AudioContext());
    const stale = player.load('https://example.com/first.m4a', {});
    stale.play();
    await vi.waitFor(() => expect(lastContext.sources.length).toBe(1));

    const onEnd = vi.fn();
    player.load('https://example.com/second.m4a', { onEnd });

    stale.unload();
    lastContext.sources[0].emitEnded();
    expect(onEnd).not.toHaveBeenCalled();
  });

  it('stops the current source on stop, but leaves the context open', async () => {
    const player = new WebAudioPlayer(new AudioContext());
    const handle = player.load('https://example.com/clip.m4a', {});
    handle.play();
    await vi.waitFor(() => expect(lastContext.sources.length).toBe(1));

    player.stop();
    expect(lastContext.sources[0].stop).toHaveBeenCalled();
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
