export interface SplitFmp4Result {
  initSegment: ArrayBuffer | null;
  mediaSegment: ArrayBuffer;
}

interface TopLevelBox {
  type: string;
  offset: number;
  size: number;
  headerSize: number;
}

// ffmpeg's fragmented muxer (`frag_keyframe+empty_moov+default_base_moof`) writes each
// output as its own complete file, ending with a trailing `mfra` (movie fragment random
// access) box. Since each fMP4 here is spliced into one continuous SourceBuffer as a
// media segment, only `moof`/`mdat` (and `styp`/`sidx`/`free`/`skip`, which some muxers
// interleave) belong in the appended chunk — `mfra` must be dropped or it gets fed to the
// decoder as if it were part of every segment instead of just the end of the whole file.
const FRAGMENT_BOX_TYPES = new Set([
  'styp',
  'sidx',
  'moof',
  'mdat',
  'free',
  'skip',
]);

function walkBoxes(view: DataView, start: number, end: number): TopLevelBox[] {
  const boxes: TopLevelBox[] = [];
  let offset = start;

  while (offset + 8 <= end) {
    const size = view.getUint32(offset, false); // big-endian
    const type = String.fromCharCode(
      view.getUint8(offset + 4),
      view.getUint8(offset + 5),
      view.getUint8(offset + 6),
      view.getUint8(offset + 7)
    );

    let boxSize = size;
    let headerSize = 8;
    if (size === 1) {
      if (offset + 16 > end) break;
      const high = view.getUint32(offset + 8, false);
      const low = view.getUint32(offset + 12, false);
      boxSize = Number((BigInt(high) << 32n) | BigInt(low));
      headerSize = 16;
    } else if (size === 0) {
      boxSize = end - offset;
    }

    if (boxSize <= 0 || offset + boxSize > end) {
      break;
    }

    boxes.push({ type, offset, size: boxSize, headerSize });
    offset += boxSize;
  }

  return boxes;
}

function walkTopLevelBoxes(buffer: ArrayBuffer): TopLevelBox[] {
  return walkBoxes(new DataView(buffer), 0, buffer.byteLength);
}

/** Walks a dot-path of nested container boxes (e.g. `moov.trak.mdia.mdhd`). */
function findBoxPath(buffer: ArrayBuffer, path: string[]): TopLevelBox | null {
  const view = new DataView(buffer);
  let candidates = walkBoxes(view, 0, buffer.byteLength);
  let match: TopLevelBox | null = null;

  for (const type of path) {
    match = candidates.find((box) => box.type === type) ?? null;
    if (!match) return null;
    candidates = walkBoxes(
      view,
      match.offset + match.headerSize,
      match.offset + match.size
    );
  }

  return match;
}

/**
 * Splits a fragmented MP4 (fMP4) buffer into its initialization segment (`ftyp` + `moov`)
 * and its media segment (`moof` + `mdat`, trimmed of any trailing non-fragment boxes such
 * as a source file's `mfra`).
 *
 * This allows stripping redundant initialization segments when appending subsequent
 * fMP4 chunks to a single MediaSource `SourceBuffer` in `sequence` mode.
 */
export function splitFmp4(buffer: ArrayBuffer): SplitFmp4Result {
  const boxes = walkTopLevelBoxes(buffer);
  const moofIndex = boxes.findIndex((box) => box.type === 'moof');

  if (moofIndex === -1) {
    // No 'moof' box was found; return the full buffer as the media segment.
    return { initSegment: null, mediaSegment: buffer };
  }

  const firstMoofOffset = boxes[moofIndex].offset;
  let mediaEnd = firstMoofOffset;
  for (let i = moofIndex; i < boxes.length; i++) {
    const box = boxes[i];
    if (!FRAGMENT_BOX_TYPES.has(box.type)) break;
    mediaEnd = box.offset + box.size;
  }

  return {
    initSegment: firstMoofOffset > 0 ? buffer.slice(0, firstMoofOffset) : null,
    mediaSegment: buffer.slice(firstMoofOffset, mediaEnd),
  };
}

/**
 * Scans top-level box types in an MP4/M4A buffer (e.g. ['ftyp', 'moov', 'moof', 'mdat']).
 * Useful for verifying whether a clip is an fMP4 (`moof` present) vs non-fragmented (`moof` absent).
 */
export function inspectMp4Boxes(buffer: ArrayBuffer): string[] {
  return walkTopLevelBoxes(buffer).map((box) => box.type);
}

/**
 * Reads the audio track's timescale (== sample rate, per ISO/IEC 14496-12) from an
 * initialization segment's `moov > trak > mdia > mdhd` box. Returns null if absent/malformed.
 */
export function parseAudioTimescale(initSegment: ArrayBuffer): number | null {
  const mdhd = findBoxPath(initSegment, ['moov', 'trak', 'mdia', 'mdhd']);
  if (!mdhd) return null;

  const view = new DataView(initSegment);
  const payloadStart = mdhd.offset + mdhd.headerSize;
  if (payloadStart >= initSegment.byteLength) return null;

  const version = view.getUint8(payloadStart);
  // version 0: creation/modification times are 32-bit; version 1: 64-bit.
  const timescaleOffset = payloadStart + 4 + (version === 1 ? 16 : 8);
  if (timescaleOffset + 4 > initSegment.byteLength) return null;

  const timescale = view.getUint32(timescaleOffset, false);
  return timescale > 0 ? timescale : null;
}

const TRUN_FLAG_DATA_OFFSET = 0x000001;
const TRUN_FLAG_FIRST_SAMPLE_FLAGS = 0x000004;
const TRUN_FLAG_SAMPLE_DURATION = 0x000100;
const TRUN_FLAG_SAMPLE_SIZE = 0x000200;
const TRUN_FLAG_SAMPLE_FLAGS = 0x000400;
const TRUN_FLAG_SAMPLE_CTO = 0x000800;

/**
 * Sums the per-sample durations declared in a media segment's `moof > traf > trun` box, in
 * the track's timescale units. ffmpeg writes an accurate (short) duration for a fragment's
 * final sample when it doesn't fill a whole AAC frame, even though the frame itself still
 * decodes to a full 1024 samples — this is how a compliant player is meant to know to
 * truncate that trailing padding rather than play it. Returns null if `trun` is absent or
 * doesn't carry explicit per-sample durations (no default-duration fallback to `tfhd`/`trex`
 * is implemented, since this backend's ffmpeg output always writes them explicitly).
 */
export function parseTrunTotalDuration(
  mediaSegment: ArrayBuffer
): number | null {
  const trun = findBoxPath(mediaSegment, ['moof', 'traf', 'trun']);
  if (!trun) return null;

  const view = new DataView(mediaSegment);
  const payloadStart = trun.offset + trun.headerSize;
  if (payloadStart + 8 > mediaSegment.byteLength) return null;

  const flags = view.getUint32(payloadStart, false) & 0x00ffffff;
  if (!(flags & TRUN_FLAG_SAMPLE_DURATION)) return null;

  const sampleCount = view.getUint32(payloadStart + 4, false);
  let offset = payloadStart + 8;
  if (flags & TRUN_FLAG_DATA_OFFSET) offset += 4;
  if (flags & TRUN_FLAG_FIRST_SAMPLE_FLAGS) offset += 4;

  const fieldsPerSample =
    1 +
    (flags & TRUN_FLAG_SAMPLE_SIZE ? 1 : 0) +
    (flags & TRUN_FLAG_SAMPLE_FLAGS ? 1 : 0) +
    (flags & TRUN_FLAG_SAMPLE_CTO ? 1 : 0);
  if (offset + sampleCount * fieldsPerSample * 4 > mediaSegment.byteLength) {
    return null;
  }

  let totalDuration = 0;
  for (let i = 0; i < sampleCount; i++) {
    totalDuration += view.getUint32(offset, false);
    offset += fieldsPerSample * 4;
  }

  return totalDuration;
}
