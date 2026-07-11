// @vitest-environment jsdom
import { describe, expect, it } from 'vitest';

import { inspectMp4Boxes, splitFmp4 } from './fmp4Utils';

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
