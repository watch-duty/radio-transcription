// @vitest-environment jsdom
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import {
  WebAudioPlayer,
  dbToGain,
  formatVolumeDb,
  gainToDb,
  snapVolumeToDefault,
} from './WebAudioPlayer';
import {
  DEFAULT_VOLUME_DB,
  VOLUME_MAX_DB,
  VOLUME_MIN_DB,
} from './audioSettings';

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
  setValueAtTime = vi.fn();
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

function createBox(type: string, payloadSize: number): ArrayBuffer {
  const size = 8 + payloadSize;
  const buffer = new ArrayBuffer(size);
  const view = new DataView(buffer);
  view.setUint32(0, size, false);
  for (let i = 0; i < 4; i++) view.setUint8(4 + i, type.charCodeAt(i));
  return buffer;
}
function concatBuffers(buffers: ArrayBuffer[]): ArrayBuffer {
  const total = buffers.reduce((acc, b) => acc + b.byteLength, 0);
  const result = new Uint8Array(total);
  let offset = 0;
  for (const b of buffers) {
    result.set(new Uint8Array(b), offset);
    offset += b.byteLength;
  }
  return result.buffer;
}
function createContainerBox(type: string, children: ArrayBuffer[]) {
  const payload = concatBuffers(children);
  const box = createBox(type, payload.byteLength);
  new Uint8Array(box).set(new Uint8Array(payload), 8);
  return box;
}
function createMdhdBox(timescale: number): ArrayBuffer {
  const payloadSize = 4 + 4 * 2 + 4 + 4 + 2 + 2; // version-0 field layout
  const box = createBox('mdhd', payloadSize);
  new DataView(box).setUint32(8 + 4 + 4 * 2, timescale, false);
  return box;
}
// Mirrors real ffmpeg `frag_keyframe+empty_moov+default_base_moof` output shape:
// ftyp+moov(with mdhd timescale)+moof+mdat+mfra, one independent file per segment.
function createFmp4File(timescale: number): ArrayBuffer {
  const moov = createContainerBox('moov', [
    createContainerBox('trak', [
      createContainerBox('mdia', [createMdhdBox(timescale)]),
    ]),
  ]);
  return concatBuffers([
    createBox('ftyp', 8),
    moov,
    createBox('moof', 20),
    createBox('mdat', 100),
    createBox('mfra', 16),
  ]);
}

class MockSourceBuffer {
  mode = '';
  appendWindowStart = 0;
  appendWindowEnd = Infinity;
  updating = false;
  bufferedEnd = 0;
  buffered = { length: 1, end: () => this.bufferedEnd };
  listeners: Record<string, Array<() => void>> = {};
  appendWindowStartAtCall: number[] = [];
  addEventListener = (type: string, cb: () => void) => {
    (this.listeners[type] ??= []).push(cb);
  };
  removeEventListener = vi.fn();
  appendBuffer = vi.fn(() => {
    // Capture synchronously, since appendWindowStart is mutated in place before the
    // next call — reading it later wouldn't reflect what was in effect at append time.
    this.appendWindowStartAtCall.push(this.appendWindowStart);
    this.bufferedEnd += 1;
    setTimeout(() => {
      (this.listeners['updateend'] ?? []).forEach((f) => f());
    }, 0);
  });
  abort = vi.fn();
}

let lastSourceBuffer: MockSourceBuffer;

class MockMediaSource {
  static current: MockMediaSource | undefined;
  readyState = 'open';
  listeners: Record<string, Array<() => void>> = {};
  sourceBuffers: MockSourceBuffer[] = [];
  static isTypeSupported = vi.fn(() => true);
  constructor() {
    MockMediaSource.current = this;
  }
  addSourceBuffer = vi.fn(() => {
    const sb = new MockSourceBuffer();
    this.sourceBuffers.push(sb);
    lastSourceBuffer = sb;
    return sb;
  });
  endOfStream = vi.fn();
  addEventListener = (type: string, cb: () => void) => {
    (this.listeners[type] ??= []).push(cb);
  };
  removeEventListener = vi.fn();
  emit(type: string) {
    (this.listeners[type] ?? []).forEach((f) => f());
  }
}

function stubMseGlobals() {
  vi.stubGlobal('MediaSource', MockMediaSource);
  vi.stubGlobal('URL', {
    createObjectURL: vi.fn(() => 'blob:mock-media-source'),
    revokeObjectURL: vi.fn(),
  });
}

function stubFetchForTwoSegments(
  seg1Bytes: ArrayBuffer,
  seg2Bytes: ArrayBuffer
) {
  vi.stubGlobal(
    'fetch',
    vi.fn((url: string) => {
      const buffer = url.includes('seg-2') ? seg2Bytes : seg1Bytes;
      return Promise.resolve({
        ok: true,
        arrayBuffer: () => Promise.resolve(buffer),
      });
    })
  );
}

let lastContext: MockAudioContext;
let lastAudio: MockAudio;

beforeEach(() => {
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

describe('WebAudioPlayer', () => {
  it('builds the MediaElementSource → Gain → StereoPanner → destination graph', () => {
    new WebAudioPlayer(new AudioContext());

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

  it('sets playbackRate', () => {
    const player = new WebAudioPlayer(new AudioContext());

    player.setSpeed(1.5);
    expect(lastAudio.playbackRate).toBe(1.5);
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

    expect(lastAudio.src).toBe('https://example.com/clip.m4a');
    expect(lastAudio.load).toHaveBeenCalled();
    expect(lastAudio.playbackRate).toBe(1.25);

    handle.play();
    expect(lastContext.resume).toHaveBeenCalled();
    expect(lastAudio.play).toHaveBeenCalled();
    expect(onPlay).toHaveBeenCalled();

    lastAudio.emit('ended');
    expect(onEnd).toHaveBeenCalled();
  });

  it('reads and writes the playback position', () => {
    const player = new WebAudioPlayer(new AudioContext());
    const handle = player.load('https://example.com/clip.m4a', {});

    lastAudio.currentTime = 3.2;
    expect(handle.getCurrentTime()).toBe(3.2);

    handle.setCurrentTime(7.5);
    expect(lastAudio.currentTime).toBe(7.5);
  });

  it('ignores a non-finite seek target', () => {
    const player = new WebAudioPlayer(new AudioContext());
    const handle = player.load('https://example.com/clip.m4a', {});

    lastAudio.currentTime = 4;
    handle.setCurrentTime(Infinity);
    handle.setCurrentTime(NaN);
    expect(lastAudio.currentTime).toBe(4);
  });

  it('does not detach the current clip when a stale handle is unloaded', () => {
    const player = new WebAudioPlayer(new AudioContext());
    const stale = player.load('https://example.com/first.m4a', {});

    const onPause = vi.fn();
    player.load('https://example.com/second.m4a', { onPause });

    stale.unload();

    lastAudio.emit('pause');
    expect(onPause).toHaveBeenCalled();
  });

  it('clears the source on stop, firing pause, but leaves the context open', () => {
    const player = new WebAudioPlayer(new AudioContext());
    const onPause = vi.fn();
    player.load('https://example.com/clip.m4a', { onPause });
    player.stop();

    expect(lastAudio.pause).toHaveBeenCalled();
    expect(onPause).toHaveBeenCalled();
    expect(lastAudio.removeAttribute).toHaveBeenCalledWith('src');
    expect(lastContext.close).not.toHaveBeenCalled();
  });

  it('disconnects its own nodes on dispose without closing the context', () => {
    const player = new WebAudioPlayer(new AudioContext());
    player.dispose();

    expect(lastContext.gain.disconnect).toHaveBeenCalled();
    expect(lastContext.panner.disconnect).toHaveBeenCalled();
    expect(lastContext.close).not.toHaveBeenCalled();
  });

  it('loadSequence in fallback mode loads initial clip and advances when ended', () => {
    const player = new WebAudioPlayer(new AudioContext());
    const onPlay = vi.fn();
    const onSegmentChange = vi.fn();
    const onEnd = vi.fn();

    const getNextSegment = vi
      .fn()
      .mockReturnValueOnce({
        id: 'seg-2',
        uri: 'https://example.com/seg-2.m4a',
      })
      .mockReturnValueOnce(null);

    const handle = player.loadSequence({
      initialSegmentId: 'seg-1',
      initialUri: 'https://example.com/seg-1.m4a',
      callbacks: { onPlay, onSegmentChange, onEnd },
      getNextSegment,
    });

    expect(lastAudio.src).toBe('https://example.com/seg-1.m4a');
    handle.play();
    expect(onPlay).toHaveBeenCalled();

    // Clip 1 ends -> should advance to seg-2
    lastAudio.emit('ended');
    expect(getNextSegment).toHaveBeenCalledWith('seg-1');
    expect(onSegmentChange).toHaveBeenCalledWith('seg-2');
    expect(lastAudio.src).toBe('https://example.com/seg-2.m4a');
    expect(lastAudio.play).toHaveBeenCalled();

    // Clip 2 ends -> getNextSegment returns null -> should call onEnd
    lastAudio.emit('ended');
    expect(getNextSegment).toHaveBeenCalledWith('seg-2');
    expect(onEnd).toHaveBeenCalledWith('seg-2');
  });

  it('loadSequence in MSE mode creates MediaSource and manages sequence timeline', () => {
    class MockSourceBuffer {
      mode = '';
      buffered = {
        length: 1,
        end: () => 5.0,
      };
      updating = false;
      listeners: Record<string, Array<() => void>> = {};
      addEventListener = (type: string, cb: () => void) => {
        (this.listeners[type] ??= []).push(cb);
      };
      removeEventListener = vi.fn();
      appendBuffer = vi.fn(() => {
        // simulate async append completion
        setTimeout(() => {
          (this.listeners['updateend'] ?? []).forEach((f) => f());
        }, 0);
      });
      abort = vi.fn();
    }

    class MockMediaSource {
      readyState = 'open';
      listeners: Record<string, Array<() => void>> = {};
      static isTypeSupported = vi.fn(() => true);
      addSourceBuffer = vi.fn(() => new MockSourceBuffer());
      endOfStream = vi.fn();
      addEventListener = (type: string, cb: () => void) => {
        (this.listeners[type] ??= []).push(cb);
      };
      removeEventListener = vi.fn();
      emit(type: string) {
        (this.listeners[type] ?? []).forEach((f) => f());
      }
    }

    vi.stubGlobal('MediaSource', MockMediaSource);
    vi.stubGlobal('URL', {
      createObjectURL: vi.fn(() => 'blob:mock-media-source'),
      revokeObjectURL: vi.fn(),
    });

    const player = new WebAudioPlayer(new AudioContext());
    const onSegmentChange = vi.fn();
    const getNextSegment = vi.fn().mockReturnValue(null);

    const handle = player.loadSequence({
      initialSegmentId: 'seg-1',
      initialUri: 'https://example.com/seg-1.m4a',
      callbacks: { onSegmentChange },
      getNextSegment,
    });

    expect(lastAudio.src).toBe('blob:mock-media-source');
    expect(handle.getCurrentTime()).toBe(0);
  });

  it("trims each subsequent segment's AAC encoder priming via appendWindowStart", async () => {
    stubMseGlobals();
    stubFetchForTwoSegments(createFmp4File(16000), createFmp4File(16000));

    const player = new WebAudioPlayer(new AudioContext());
    const getNextSegment = vi
      .fn()
      .mockReturnValueOnce({
        id: 'seg-2',
        uri: 'https://example.com/seg-2.m4a',
      })
      .mockReturnValue(null);

    player.loadSequence({
      initialSegmentId: 'seg-1',
      initialUri: 'https://example.com/seg-1.m4a',
      callbacks: {},
      getNextSegment,
    });

    MockMediaSource.current!.emit('sourceopen');

    await vi.waitFor(() =>
      expect(lastSourceBuffer.appendBuffer).toHaveBeenCalledTimes(2)
    );
    await new Promise((resolve) => setTimeout(resolve, 0));

    // First (initial) segment is appended untrimmed; the second is trimmed by exactly one
    // AAC frame (1024 samples) at the parsed 16kHz timescale (=64ms), starting right where
    // the first segment's buffered range ended.
    expect(lastSourceBuffer.appendWindowStartAtCall[0]).toBe(0);
    expect(lastSourceBuffer.appendWindowStartAtCall[1]).toBeCloseTo(
      1 + 1024 / 16000
    );
  });

  it('schedules a brief gain dip at each segment boundary to mask splice artifacts', async () => {
    stubMseGlobals();
    stubFetchForTwoSegments(createFmp4File(16000), createFmp4File(16000));

    const player = new WebAudioPlayer(new AudioContext());
    const getNextSegment = vi
      .fn()
      .mockReturnValueOnce({
        id: 'seg-2',
        uri: 'https://example.com/seg-2.m4a',
      })
      .mockReturnValue(null);

    player.loadSequence({
      initialSegmentId: 'seg-1',
      initialUri: 'https://example.com/seg-1.m4a',
      callbacks: {},
      getNextSegment,
    });

    MockMediaSource.current!.emit('sourceopen');

    await vi.waitFor(() =>
      expect(lastSourceBuffer.appendBuffer).toHaveBeenCalledTimes(2)
    );
    await new Promise((resolve) => setTimeout(resolve, 0));

    // seg-2's boundary is queued once its append lands (see previous test): startTime =
    // 1 + 1024/16000. Move playback to just inside the declick lookahead window and fire
    // 'timeupdate', which is what schedules the dip.
    const boundary = 1 + 1024 / 16000;
    lastAudio.currentTime = boundary - 0.2;
    lastContext.currentTime = 10;
    lastAudio.emit('timeupdate');

    const gainParam = lastContext.gain.gain;
    const baseGain = dbToGain(DEFAULT_VOLUME_DB);
    expect(gainParam.setValueAtTime).toHaveBeenCalledWith(
      baseGain,
      expect.closeTo(10.192, 5)
    );
    expect(gainParam.linearRampToValueAtTime).toHaveBeenCalledWith(
      0,
      expect.closeTo(10.2, 5)
    );
    expect(gainParam.linearRampToValueAtTime).toHaveBeenCalledWith(
      baseGain,
      expect.closeTo(10.208, 5)
    );
  });
});
