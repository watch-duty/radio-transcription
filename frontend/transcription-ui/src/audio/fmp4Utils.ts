export interface SplitFmp4Result {
  initSegment: ArrayBuffer | null;
  mediaSegment: ArrayBuffer;
}

/**
 * Splits a fragmented MP4 (fMP4) buffer into its initialization segment (`ftyp` + `moov`)
 * and its media segment (`moof` + `mdat` and any subsequent boxes).
 *
 * This allows stripping redundant initialization segments when appending subsequent
 * fMP4 chunks to a single MediaSource `SourceBuffer` in `sequence` mode.
 */
export function splitFmp4(buffer: ArrayBuffer): SplitFmp4Result {
  const view = new DataView(buffer);
  let offset = 0;
  let firstMoofOffset = -1;

  while (offset + 8 <= buffer.byteLength) {
    const size = view.getUint32(offset, false); // big-endian
    const type = String.fromCharCode(
      view.getUint8(offset + 4),
      view.getUint8(offset + 5),
      view.getUint8(offset + 6),
      view.getUint8(offset + 7)
    );

    if (type === 'moof') {
      firstMoofOffset = offset;
      break;
    }

    let boxSize = size;
    if (size === 1) {
      if (offset + 16 > buffer.byteLength) break;
      const high = view.getUint32(offset + 8, false);
      const low = view.getUint32(offset + 12, false);
      boxSize = Number((BigInt(high) << 32n) | BigInt(low));
    } else if (size === 0) {
      boxSize = buffer.byteLength - offset;
    }

    if (boxSize <= 0 || offset + boxSize > buffer.byteLength) {
      break;
    }
    offset += boxSize;
  }

  if (firstMoofOffset > 0) {
    return {
      initSegment: buffer.slice(0, firstMoofOffset),
      mediaSegment: buffer.slice(firstMoofOffset),
    };
  } else if (firstMoofOffset === 0) {
    return {
      initSegment: null,
      mediaSegment: buffer,
    };
  }

  // If no 'moof' box was found, return the full buffer as the media segment
  return {
    initSegment: null,
    mediaSegment: buffer,
  };
}

/**
 * Scans top-level box types in an MP4/M4A buffer (e.g. ['ftyp', 'moov', 'moof', 'mdat']).
 * Useful for verifying whether a clip is an fMP4 (`moof` present) vs non-fragmented (`moof` absent).
 */
export function inspectMp4Boxes(buffer: ArrayBuffer): string[] {
  const view = new DataView(buffer);
  let offset = 0;
  const boxes: string[] = [];

  while (offset + 8 <= buffer.byteLength) {
    const size = view.getUint32(offset, false);
    const type = String.fromCharCode(
      view.getUint8(offset + 4),
      view.getUint8(offset + 5),
      view.getUint8(offset + 6),
      view.getUint8(offset + 7)
    );
    boxes.push(type);

    let boxSize = size;
    if (size === 1) {
      if (offset + 16 > buffer.byteLength) break;
      const high = view.getUint32(offset + 8, false);
      const low = view.getUint32(offset + 12, false);
      boxSize = Number((BigInt(high) << 32n) | BigInt(low));
    } else if (size === 0) {
      boxSize = buffer.byteLength - offset;
    }

    if (boxSize <= 0 || offset + boxSize > buffer.byteLength) {
      break;
    }
    offset += boxSize;
  }

  return boxes;
}
