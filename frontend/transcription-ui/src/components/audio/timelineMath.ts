import { type AudioSegment } from '@transcription/common';

import { type HistogramMark } from '../../hooks/useTimelineHistogram';

// 6h grid → ~4 marks at local 00:00/06:00/12:00/18:00 across the 24h range.
const GRID_INTERVAL_MS = 6 * 60 * 60 * 1000;

export function clamp(value: number, min: number, max: number): number {
  return Math.min(max, Math.max(min, value));
}

export function msToPct(
  ms: number,
  rangeStartMs: number,
  rangeTotalMs: number
): number {
  return ((ms - rangeStartMs) / rangeTotalMs) * 100;
}

// Bucket opacity by count: 1 clip = 50%, ramping to fully opaque at 5+.
export function opacityForCount(count: number): number {
  if (count <= 0) return 0;
  return clamp(0.5 + (0.5 * (count - 1)) / 4, 0.5, 1);
}

// Paint order: non-speech first, speech next, alerts last, so the more
// important tint stays visible where cells overlap.
export function markRank(mark: HistogramMark): number {
  if (mark.hasAlert) return 2;
  if (mark.hasSpeech) return 1;
  return 0;
}

export function markColor(mark: HistogramMark, isDarkTheme: boolean): string {
  if (mark.hasAlert) return 'warning.main';
  // Same blue as the header/controls; density opacity makes it read lighter.
  if (mark.hasSpeech) return 'primary.main';
  return isDarkTheme ? 'grey.600' : 'grey.400';
}

// Gridline times aligned to local boundaries (not UTC) so day boundaries land
// on a mark. Uses the offset at rangeStartMs; a mid-range DST shift is fine.
export function computeGridLineTimes(
  rangeStartMs: number,
  maxEnd: number
): number[] {
  const offsetMs = new Date(rangeStartMs).getTimezoneOffset() * 60 * 1000;
  const times: number[] = [];
  // Step through local-epoch space (utc - offset) so multiples fall on local rounds.
  for (
    let local =
      Math.ceil((rangeStartMs - offsetMs) / GRID_INTERVAL_MS) *
      GRID_INTERVAL_MS;
    local + offsetMs <= maxEnd;
    local += GRID_INTERVAL_MS
  ) {
    times.push(local + offsetMs);
  }
  return times;
}

/**
 * Given a clicked consolidated segment clip, the clicked timestamp (in epoch ms),
 * and the list of raw audio segments:
 * Finds the specific raw segment that contains (or is closest to) clickedTimeMs,
 * and calculates the offset in seconds relative to that raw segment's start timestamp.
 */
export function resolveClickSegmentAndOffset(
  clip: {
    id: string;
    bundledSegmentIds?: string[];
    startTimestamp: string;
    endTimestamp: string;
  },
  clickedTimeMs: number,
  rawAudioSegments: AudioSegment[]
): { segmentId: string; offsetSeconds: number } {
  if (clip.bundledSegmentIds && clip.bundledSegmentIds.length > 0) {
    const bundledRaw = clip.bundledSegmentIds
      .map((id) => rawAudioSegments.find((s) => s.id === id))
      .filter((s): s is AudioSegment => Boolean(s && s.playbackAudioUri));

    if (bundledRaw.length > 0) {
      let targetRaw = bundledRaw.find((s) => {
        const sStart = new Date(s.startTimestamp).getTime();
        const sEnd = new Date(s.endTimestamp).getTime();
        return clickedTimeMs >= sStart && clickedTimeMs <= sEnd;
      });

      if (!targetRaw) {
        let minDistance = Infinity;
        for (const s of bundledRaw) {
          const sStart = new Date(s.startTimestamp).getTime();
          const sEnd = new Date(s.endTimestamp).getTime();
          const distance = Math.max(
            0,
            sStart - clickedTimeMs,
            clickedTimeMs - sEnd
          );
          if (distance < minDistance) {
            minDistance = distance;
            targetRaw = s;
          }
        }
      }

      if (targetRaw) {
        const targetStartMs = new Date(targetRaw.startTimestamp).getTime();
        const offsetSeconds = Math.max(
          0,
          (clickedTimeMs - targetStartMs) / 1000
        );
        return { segmentId: targetRaw.id, offsetSeconds };
      }
    }
  }

  const tStart = new Date(clip.startTimestamp).getTime();
  const offsetSeconds = Math.max(0, (clickedTimeMs - tStart) / 1000);
  return { segmentId: clip.id, offsetSeconds };
}
