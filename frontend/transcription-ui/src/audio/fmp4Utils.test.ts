// @vitest-environment jsdom
import { describe, expect, it } from 'vitest';

import {
  inspectMp4Boxes,
  parseAudioTimescale,
  parseTrunTotalDuration,
  splitFmp4,
} from './fmp4Utils';

function createBox(type: string, payloadSize: number): ArrayBuffer {
  const size = 8 + payloadSize;
  const buffer = new ArrayBuffer(size);
  const view = new DataView(buffer);
  view.setUint32(0, size, false);
  for (let i = 0; i < 4; i++) {
    view.setUint8(4 + i, type.charCodeAt(i));
  }
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

function createContainerBox(
  type: string,
  children: ArrayBuffer[]
): ArrayBuffer {
  const payload = concatBuffers(children);
  const box = createBox(type, payload.byteLength);
  new Uint8Array(box).set(new Uint8Array(payload), 8);
  return box;
}

function createMdhdBox(timescale: number, version: 0 | 1 = 0): ArrayBuffer {
  const fieldSize = version === 1 ? 8 : 4;
  // version(1) + flags(3) + creation(fieldSize) + modification(fieldSize) + timescale(4)
  // + duration(fieldSize) + language(2) + pre_defined(2)
  const payloadSize = 4 + fieldSize * 2 + 4 + fieldSize + 2 + 2;
  const box = createBox('mdhd', payloadSize);
  const view = new DataView(box);
  view.setUint8(8, version);
  const timescaleOffset = 8 + 4 + fieldSize * 2; // skip version/flags + creation + modification
  view.setUint32(timescaleOffset, timescale, false);
  return box;
}

// flags: data_offset_present(0x1) + sample_duration_present(0x100) + sample_size_present(0x200)
const TRUN_FLAGS_DURATION_AND_SIZE = 0x000301;

function createTrunBox(
  sampleDurations: number[],
  flags = TRUN_FLAGS_DURATION_AND_SIZE
): ArrayBuffer {
  const hasDataOffset = Boolean(flags & 0x000001);
  const hasSampleDuration = Boolean(flags & 0x000100);
  const hasSampleSize = Boolean(flags & 0x000200);
  const fieldsPerSample = (hasSampleDuration ? 1 : 0) + (hasSampleSize ? 1 : 0);

  const payloadSize =
    4 + // version + flags
    4 + // sample_count
    (hasDataOffset ? 4 : 0) +
    sampleDurations.length * fieldsPerSample * 4;
  const box = createBox('trun', payloadSize);
  const view = new DataView(box);
  view.setUint32(8, flags, false); // version(0) + flags
  view.setUint32(12, sampleDurations.length, false);

  let offset = 16;
  if (hasDataOffset) {
    view.setInt32(offset, 0, false);
    offset += 4;
  }
  for (const duration of sampleDurations) {
    if (hasSampleDuration) {
      view.setUint32(offset, duration, false);
      offset += 4;
    }
    if (hasSampleSize) {
      view.setUint32(offset, 0, false);
      offset += 4;
    }
  }
  return box;
}

describe('splitFmp4', () => {
  it('splits an fMP4 buffer into initSegment (ftyp+moov) and mediaSegment (moof+mdat)', () => {
    const ftyp = createBox('ftyp', 16);
    const moov = createBox('moov', 100);
    const moof = createBox('moof', 50);
    const mdat = createBox('mdat', 200);

    const fullBuffer = concatBuffers([ftyp, moov, moof, mdat]);
    const { initSegment, mediaSegment } = splitFmp4(fullBuffer);

    expect(initSegment).not.toBeNull();
    expect(initSegment?.byteLength).toBe(ftyp.byteLength + moov.byteLength);
    expect(mediaSegment.byteLength).toBe(moof.byteLength + mdat.byteLength);
  });

  it('returns null initSegment when moof is at the beginning of the buffer', () => {
    const moof = createBox('moof', 50);
    const mdat = createBox('mdat', 200);

    const buffer = concatBuffers([moof, mdat]);
    const { initSegment, mediaSegment } = splitFmp4(buffer);

    expect(initSegment).toBeNull();
    expect(mediaSegment.byteLength).toBe(buffer.byteLength);
  });

  it('returns full buffer as mediaSegment when no moof box is present', () => {
    const ftyp = createBox('ftyp', 16);
    const moov = createBox('moov', 100);

    const buffer = concatBuffers([ftyp, moov]);
    const { initSegment, mediaSegment } = splitFmp4(buffer);

    expect(initSegment).toBeNull();
    expect(mediaSegment.byteLength).toBe(buffer.byteLength);
  });

  it('handles 64-bit largesize boxes before moof correctly', () => {
    const buffer = new ArrayBuffer(16 + 50);
    const view = new DataView(buffer);
    // ftyp with size=1 and largesize=16
    view.setUint32(0, 1, false);
    view.setUint8(4, 'f'.charCodeAt(0));
    view.setUint8(5, 't'.charCodeAt(0));
    view.setUint8(6, 'y'.charCodeAt(0));
    view.setUint8(7, 'p'.charCodeAt(0));
    view.setUint32(8, 0, false);
    view.setUint32(12, 16, false);

    // moof box immediately after
    view.setUint32(16, 50, false);
    view.setUint8(20, 'm'.charCodeAt(0));
    view.setUint8(21, 'o'.charCodeAt(0));
    view.setUint8(22, 'o'.charCodeAt(0));
    view.setUint8(23, 'f'.charCodeAt(0));

    const { initSegment, mediaSegment } = splitFmp4(buffer);
    expect(initSegment?.byteLength).toBe(16);
    expect(mediaSegment.byteLength).toBe(50);
  });

  it('drops a trailing mfra box from the media segment', () => {
    // Matches real ffmpeg `frag_keyframe+empty_moov+default_base_moof` output: each
    // independently-encoded segment file ends with its own mfra, which must not be fed
    // to the decoder as if it were part of the next segment's moof+mdat.
    const ftyp = createBox('ftyp', 16);
    const moov = createBox('moov', 100);
    const moof = createBox('moof', 50);
    const mdat = createBox('mdat', 200);
    const mfra = createBox('mfra', 40);

    const fullBuffer = concatBuffers([ftyp, moov, moof, mdat, mfra]);
    const { initSegment, mediaSegment } = splitFmp4(fullBuffer);

    expect(initSegment?.byteLength).toBe(ftyp.byteLength + moov.byteLength);
    expect(mediaSegment.byteLength).toBe(moof.byteLength + mdat.byteLength);

    const boxesInMediaSegment = inspectMp4Boxes(mediaSegment);
    expect(boxesInMediaSegment).toEqual(['moof', 'mdat']);
  });

  it('inspectMp4Boxes lists top-level box types in order', () => {
    const ftyp = createBox('ftyp', 16);
    const moov = createBox('moov', 100);
    const moof = createBox('moof', 50);
    const mdat = createBox('mdat', 200);

    const buffer = concatBuffers([ftyp, moov, moof, mdat]);
    const boxes = inspectMp4Boxes(buffer);
    expect(boxes).toEqual(['ftyp', 'moov', 'moof', 'mdat']);
  });
});

describe('parseAudioTimescale', () => {
  function buildInitSegment(mdhd: ArrayBuffer): ArrayBuffer {
    const mdia = createContainerBox('mdia', [mdhd]);
    const trak = createContainerBox('trak', [mdia]);
    const moov = createContainerBox('moov', [trak]);
    return concatBuffers([createBox('ftyp', 16), moov]);
  }

  it('reads the timescale from a version-0 mdhd box', () => {
    const initSegment = buildInitSegment(createMdhdBox(16000, 0));
    expect(parseAudioTimescale(initSegment)).toBe(16000);
  });

  it('reads the timescale from a version-1 (64-bit fields) mdhd box', () => {
    const initSegment = buildInitSegment(createMdhdBox(44100, 1));
    expect(parseAudioTimescale(initSegment)).toBe(44100);
  });

  it('returns null when moov/trak/mdia/mdhd is absent', () => {
    const buffer = concatBuffers([createBox('ftyp', 16), createBox('moov', 4)]);
    expect(parseAudioTimescale(buffer)).toBeNull();
  });
});

describe('parseTrunTotalDuration', () => {
  function buildMediaSegment(trun: ArrayBuffer): ArrayBuffer {
    const traf = createContainerBox('traf', [trun]);
    const moof = createContainerBox('moof', [traf]);
    return concatBuffers([moof, createBox('mdat', 32)]);
  }

  it('sums declared per-sample durations, matching real ffmpeg output where the final AAC frame is shorter than 1024 samples', () => {
    // Mirrors the real backend's trun: full 1024-sample frames, with a short final sample
    // representing the true (un-padded) remainder of the segment's audio.
    const durations = [1024, 1024, 1024, 1024, 944];
    const mediaSegment = buildMediaSegment(createTrunBox(durations));
    expect(parseTrunTotalDuration(mediaSegment)).toBe(
      durations.reduce((a, b) => a + b, 0)
    );
  });

  it('returns null when trun has no explicit per-sample durations', () => {
    const flagsWithoutDuration = 0x000201; // data_offset + sample_size only
    const mediaSegment = buildMediaSegment(
      createTrunBox([1024, 1024], flagsWithoutDuration)
    );
    expect(parseTrunTotalDuration(mediaSegment)).toBeNull();
  });

  it('returns null when moof/traf/trun is absent', () => {
    const mediaSegment = concatBuffers([
      createBox('moof', 8),
      createBox('mdat', 32),
    ]);
    expect(parseTrunTotalDuration(mediaSegment)).toBeNull();
  });
});
